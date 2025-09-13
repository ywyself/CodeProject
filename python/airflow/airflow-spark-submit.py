import json
import logging
import subprocess
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

APP_NAME = "my-spark-job"
NAMESPACE = "default"
SPARK_DRIVER_LABEL = f"park-role=driver,spark-app-selector={APP_NAME}"


def get_driver_pod_names():
    """获取所有 driver pod 名称（按创建时间排序）"""
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", SPARK_DRIVER_LABEL, "-o", "json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        pod_info = json.loads(result.stdout)
        # 按创建时间排序
        sorted_pods = sorted(
            pod_info.get("items", []),
            key=lambda p: p["metadata"]["creationTimestamp"]
        )
        # 提取排序后的pod名称
        return [p["metadata"]["name"] for p in sorted_pods]
    except subprocess.CalledProcessError as e:
        logging.error(f"kubectl命令执行失败: {str(e.stderr).strip()}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"解析pod信息JSON失败: {str(e)}")
        return []
    except KeyError as e:
        logging.error(f"pod信息缺少必要字段: {str(e)}")
        return []


def get_pod_phase(pod_name):
    """获取 pod 的 phase 状态"""
    if not pod_name:
        logging.warning("pod名称为空，无法查询状态")
        return "InvalidName"

    try:
        # 只获取需要的phase字段，减少数据传输
        result = subprocess.run(
            ["kubectl", "get", "pod", pod_name, "-n", NAMESPACE, "-o", "jsonpath={.status.phase}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=30  # 超时控制，避免命令无响应
        )

        phase = result.stdout.strip()
        if not phase:
            return "Unknown"
        return phase

    except subprocess.CalledProcessError as e:
        # 区分Pod不存在和其他错误
        if "not found" in e.stderr.lower():
            logging.info(f"Pod {pod_name} 不存在")
            return "NotFound"
        else:
            logging.error(f"查询Pod {pod_name} 状态失败: {e.stderr.strip()}")
            return "Error"
    except subprocess.TimeoutExpired:
        logging.error(f"查询Pod {pod_name} 状态超时")
        return "Timeout"
    except Exception as e:
        logging.error(f"查询Pod {pod_name} 状态时发生未知错误: {str(e)}")
        return "Unknown"


def get_kubectl_pod_status(pod_name):
    """
    获取与 `kubectl get pods` STATUS 列完全一致的Pod状态
    """
    if not pod_name:
        logging.warning("pod名称为空，无法查询状态")
        return "InvalidName"

    try:
        # 拿到 Pod 的完整 JSON
        result = subprocess.run(
            ["kubectl", "get", "pod", pod_name, "-n", NAMESPACE, "-o", "json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=30
        )

        pod = json.loads(result.stdout)

        # 特殊 case: 正在删除
        if pod.get("metadata", {}).get("deletionTimestamp"):
            return "Terminating"

        # 遍历容器状态（按kubectl逻辑，通常取第一个）
        container_statuses = pod.get("status", {}).get("containerStatuses", [])
        if container_statuses:
            state = container_statuses[0].get("state", {})
            if "waiting" in state:
                return state["waiting"].get("reason", "Waiting")
            if "terminated" in state:
                return state["terminated"].get("reason", "Terminated")

        # fallback: phase
        return pod.get("status", {}).get("phase", "Unknown")

    except subprocess.CalledProcessError as e:
        if "not found" in e.stderr.lower():
            logging.info(f"Pod {pod_name} 不存在")
            return "NotFound"
        else:
            logging.error(f"查询Pod {pod_name} 状态失败: {e.stderr.strip()}")
            return "Error"
    except subprocess.TimeoutExpired:
        logging.error(f"查询Pod {pod_name} 状态超时")
        return "Timeout"
    except Exception as e:
        logging.error(f"查询Pod {pod_name} 状态时发生未知错误: {str(e)}")
        return "Unknown"


def wait_driver_and_get_name(submit_wait_timeout=3600, submit_check_interval=10, pod_wait_timeout=600,
                             pod_check_interval=5, **kwargs):
    """
    1) 等待 spark_submit 进入 RUNNING（或已 SUCCESS）状态（不直接退出）
    2) 在其开始运行后，轮询获取 driver pod name，直到 pod 出现或超时
    将 pod name（或 None）通过 XCom 推送出去
    """
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    ti.xcom_push(key='driver_namespace', value=NAMESPACE)

    # ---- 1) 等待 spark_submit 开始运行 ----
    submit_start_ts = time.time()
    seen_start = False
    while time.time() - submit_start_ts < submit_wait_timeout:
        submit_ti = dag_run.get_task_instance("spark_submit")
        if submit_ti:
            state = submit_ti.state
            print(f"[wait_driver] spark_submit state: {state}")
            # 当检测到 RUNNING 时，认为 spark 已开始运行，继续去找 pod
            if state == State.RUNNING:
                seen_start = True
                break
            # 如果已经是 SUCCESS（运行很快完成），仍然继续尝试查 pod
            if state == State.SUCCESS:
                print("[wait_driver] spark_submit already SUCCESS - proceeding to pod lookup")
                seen_start = True
                break
            # 如果是 FAILED，说明 spark_submit 在启动前就失败了，直接返回 None
            if state == State.FAILED:
                print("[wait_driver] spark_submit FAILED before running - will not lookup pod")
                ti.xcom_push(key='driver_pod_name', value=None)
                return None
        else:
            print("[wait_driver] spark_submit TaskInstance not found yet")
        time.sleep(submit_check_interval)

    if not seen_start:
        print(f"[wait_driver] timeout waiting for spark_submit to start (timeout={submit_wait_timeout}s)")
        return None

    # ---- 2) spark_submit 已开始，轮询获取 driver pod 名称 ----
    pod_start = time.time()
    while time.time() - pod_start < pod_wait_timeout:
        driver_pod_names = get_driver_pod_names()
        if driver_pod_names:
            latest_pod_name = driver_pod_names[-1]
            # 确认 phase 是否是我们关心的状态（Running/Succeeded/Failed）
            phase = get_pod_phase(latest_pod_name)  # Pod phase（固定枚举值）Pending,Running,Succeeded,Failed,Unknown
            print(f"[wait_driver] found pod {latest_pod_name} phase={phase}")
            if phase in ["Succeeded", "Failed"]:
                ti.xcom_push(key='driver_pod_name', value=latest_pod_name)
                return phase
            elif phase == "Running":
                status = get_kubectl_pod_status(latest_pod_name)
                if status == "Running":
                    ti.xcom_push(key='driver_pod_name', value=latest_pod_name)
                    return status
                else:
                    return status
            elif phase == "Pending":
                status = get_kubectl_pod_status(latest_pod_name)
                if status in ["Pending", "ContainerCreating"]:
                    # 继续循环等待
                    pass
                else:
                    # 容器启动出错了
                    return status
                pass
            else:
                # Unknown
                return phase
        time.sleep(pod_check_interval)

    # 超时仍没找到 pod
    print(f"[wait_driver] timeout waiting for driver pod to appear (timeout={pod_wait_timeout}s)")
    return None


with DAG(
    "spark_on_k8s_finalized",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1) spark-submit（正常的 shell 提交，保留 stdout 到 Airflow UI）
    spark_submit = BashOperator(
        task_id="spark_submit",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
          --master k8s://https://kubernetes.default.svc \
          --deploy-mode cluster \
          --name {APP_NAME} \
          --class org.example.Main \
          local:///opt/spark/app.jar
        """,
    )

    # 2) 等待 spark_submit 开始并获取 driver pod name（推到 XCom）
    wait_driver = PythonOperator(
        task_id="wait_driver",
        python_callable=wait_driver_and_get_name,
    )

    # 3) 日志监控 + 清理（合并）：从 XCom 取 pod name；若存在则后台跟随日志，并在 pod 结束后删除 pod
    spark_logs_and_cleanup = BashOperator(
        task_id="spark_logs_and_cleanup",
        bash_command="""
DRIVER_POD="{{ ti.xcom_pull(task_ids='wait_driver', key='driver_pod_name') }}"
NAMESPACE="{{ ti.xcom_pull(task_ids='wait_driver', key='driver_namespace') }}"
if [ -z "$DRIVER_POD" ] || [ "$DRIVER_POD" = "None" ]; then
  echo "[logs_and_cleanup] no driver pod name, skipping logs & cleanup"
  exit 0
fi

echo "[logs_and_cleanup] start following logs for pod: $DRIVER_POD"
kubectl logs -f $DRIVER_POD -n $NAMESPACE --ignore-errors=true

echo "[logs_and_cleanup] extracting SparkJobResult"
RESULT_LINE=$(kubectl logs $DRIVER_POD -n $NAMESPACE --ignore-errors=true --tail=1000 | grep "SparkJobResult:")
#RESULT_LINE=$(kubectl logs "$DRIVER_POD" -n "$NAMESPACE" --ignore-errors=true --tail=1000 | grep "SparkJobResult:" | tail -n1)


echo "[logs_and_cleanup] deleting pod $DRIVER_POD"
kubectl delete pod $DRIVER_POD -n $NAMESPACE --ignore-not-found=true

echo "[logs_and_cleanup] finished"
# show the result of spark job
if [ -n "$RESULT_LINE" ]; then
  echo "[logs_and_cleanup] final SparkJobResult: $RESULT_LINE"
else
  echo "[logs_and_cleanup] SparkJobResult not found in logs"
fi
""",
        # 如果你希望即使 wait_driver/spark_submit 失败也执行（但脚本自己会跳过无 pod 情形），可以用 ALL_DONE
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 依赖：wait_driver -> logs_and_cleanup，spark_submit 与 wait_driver 并行触发（不对 logs_and_cleanup 建立依赖）
    wait_driver >> spark_logs_and_cleanup
    # 注意：spark_submit 与 wait_driver 同级并行，不要把 spark_submit 设为 spark_logs_and_cleanup 的上游，
    # 因为我们希望日志尽早开始（日志任务会自行等待 pod 出现）

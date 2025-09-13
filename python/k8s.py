"""
| kubectl STATUS 列值            | 对应的 phase                                   |
| ---------------------------- | ------------------------------------------- |
| `Pending`                    | Pending                                     |
| `Running`                    | Running                                     |
| `Succeeded`                  | Succeeded                                   |
| `Failed`                     | Failed                                      |
| `Unknown`                    | Unknown                                     |
| `Terminating`                | （仍然是 Pending/Running，只是有 deletionTimestamp） |
| `ContainerCreating`          | Pending                                     |
| `ErrImagePull`               | Pending                                     |
| `ImagePullBackOff`           | Pending                                     |
| `InvalidImageName`           | Pending                                     |
| `CrashLoopBackOff`           | Running                                     |
| `RunContainerError`          | Pending 或 Running                           |
| `CreateContainerConfigError` | Pending                                     |
| `Completed`                  | Succeeded                                   |
| `Error`                      | Failed                                      |
| `OOMKilled`                  | Failed                                      |
"""

import time
import json
import subprocess

NAMESPACE = "default"
DRIVER_LABEL = "spark-role=driver"


def get_driver_pods():
    """获取所有 driver pod 名称（按创建时间排序）"""
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", DRIVER_LABEL, "-o", "json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        pod_info = json.loads(result.stdout)
        # 按创建时间排序而非名称
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
            ["kubectl", "get", "pod", pod_name, "-n", NAMESPACE, 
             "-o", "jsonpath={.status.phase}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=10  # 超时控制，避免命令无响应
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
            timeout=10
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

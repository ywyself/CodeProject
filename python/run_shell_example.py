import os
import signal
import subprocess
import threading
import time
from typing import Callable, Union


# 为什么不用 proc.terminate()？
# proc.terminate() 只能终止主进程；
# 如果 shell 脚本中有 sleep、python 等子子进程，它们会变“孤儿进程”继续运行；
# killpg 是发信号给“进程组”，整个 shell 命令树都能杀干净。
#
# 为什么要 preexec_fn=os.setsid？
# 这行代码是为了让子进程启动新会话（process group leader），这样我们就可以用 killpg 终止它和它的子孙。


def terminate_proc_gracefully(proc: subprocess.Popen, timeout: int = 10) -> int:
    """
    安全地终止 subprocess：
    - 如果是独立进程组，则发送 SIGTERM 给整个进程组，等待其优雅退出；
    - 否则只终止该 subprocess；
    - 如果超时仍未退出，则发送 SIGKILL 强制杀死。

    :param proc: subprocess.Popen 实例（必须设置 preexec_fn=os.setsid）
    :param timeout: 等待优雅退出的秒数
    :return: 最终的 returncode
    """
    try:
        parent_pgid = os.getpgrp()
        child_pgid = os.getpgid(proc.pid)
        is_independent_group = (child_pgid != parent_pgid)
    except Exception as e:
        print(f"[WARN] 获取 PGID 失败: {e}")
        is_independent_group = False

    if is_independent_group:
        print(f"[INFO] Sending SIGTERM to process group {child_pgid}")
        os.killpg(child_pgid, signal.SIGTERM)
    else:
        print(f"[INFO] Sending SIGTERM to PID {proc.pid}")
        proc.terminate()

    try:
        proc.wait(timeout=timeout)
        print(f"[INFO] Process exited with returncode {proc.returncode}")
    except subprocess.TimeoutExpired:
        print(f"[WARN] Timeout after {timeout}s, sending SIGKILL")
        try:
            if is_independent_group:
                os.killpg(child_pgid, signal.SIGKILL)
            else:
                proc.kill()
        except Exception as e:
            print(f"[ERROR] 强制杀死失败: {e}")
        proc.wait()
        print(f"[INFO] Process killed, returncode {proc.returncode}")

    return proc.returncode


def exec_command(command, interval=1, max_exec_timeout=30, max_terminate_timeout=5,
                 predicate_to_kill: Union[None, Callable[[str], bool]] = None, wait_before_kill=3):
    """
    运行 shell 命令，实时读取输出

    :param command: 待执行的命令
    :param interval: 检查命令是否结束
    :param max_exec_timeout: 命令执行的超时时间
    :param max_terminate_timeout: 终止命令执行的最大等待时间
    :param predicate_to_kill: 根据命令的输出，判断是否需要终止命令
    :param wait_before_kill: 终止命令前的等待时间
    :return: 最终的 returncode
    """

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        executable="/bin/bash",
        preexec_fn=os.setsid  # 非常关键！创建新进程组, 用来 later kill entire process group
    )

    output_lines = []
    kill_signal = threading.Event()  # Python 提供的线程同步原语，用于在不同线程之间安全地传递“某个状态是否发生”的信号。

    def read_proc_stdout():
        for line in iter(proc.stdout.readline, ''):  # subprocess.stdout.readline() 是阻塞的。
            print(line, end='')  # 实时输出
            output_lines.append(line)
            if predicate_to_kill and predicate_to_kill(line.lower()):
                kill_signal.set()  # 把 Event 设置为已触发（True）
                # break  # 这里可以在触发后停止打印

    thread = threading.Thread(target=read_proc_stdout)
    thread.start()

    start_time = time.time()
    while True:
        if proc.poll() is not None:
            print("[INFO] Process finished.")
            break

        if kill_signal.is_set():  # 查询 Event 是否已触发
            print(f"[INFO] Predicate True. Waiting {wait_before_kill}s then terminating process...")
            if wait_before_kill > 0:
                time.sleep(wait_before_kill)
            terminate_proc_gracefully(proc=proc, timeout=max_terminate_timeout)
            break

        if time.time() - start_time > max_exec_timeout:
            print("[INFO] Max timeout reached. Killing process.")
            terminate_proc_gracefully(proc=proc, timeout=max_terminate_timeout)
            break

        time.sleep(interval)

    # 让主线程等 read_proc_stdout 这个输出监听线程把活干完
    thread.join(timeout=5)

    returncode = proc.poll()
    return {
        "returncode": returncode,
        "output": ''.join(output_lines),
        "success": kill_signal.is_set()
    }


def _is_ok(line: str) -> bool:
    if '5' in line:
        return True
    return False


if __name__ == '__main__':
    cmd = """for i in {1..10}; do echo "times: $i"; sleep 1; done"""
    ret = exec_command(cmd, max_exec_timeout=2, predicate_to_kill=_is_ok, wait_before_kill=0)
    print(ret)
    print()
    ret = exec_command(cmd, max_exec_timeout=7, predicate_to_kill=_is_ok, wait_before_kill=2)
    print(ret)
    print()
    ret = exec_command(cmd, max_exec_timeout=7, predicate_to_kill=None)
    print(ret)
    print()
    ret = exec_command(cmd, max_exec_timeout=20, predicate_to_kill=None, wait_before_kill=1)
    print(ret)

"""output
times: 1
times: 2
[WARN] Max timeout reached. Killing process.
[INFO] Sending SIGTERM to process group 2999
[INFO] Process exited with returncode -15
{'returncode': -15, 'output': 'times: 1\ntimes: 2\n', 'success': False}

times: 1
times: 2
times: 3
times: 4
times: 5
[INFO] Predicate True. Waiting 2s then terminating process...
times: 6
times: 7
[INFO] Sending SIGTERM to process group 3003
[INFO] Process exited with returncode -15
{'returncode': -15, 'output': 'times: 1\ntimes: 2\ntimes: 3\ntimes: 4\ntimes: 5\ntimes: 6\ntimes: 7\n', 'success': True}

times: 1
times: 2
times: 3
times: 4
times: 5
times: 6
times: 7
[INFO] Max timeout reached. Killing process.
[INFO] Sending SIGTERM to process group 3015
[INFO] Process exited with returncode -15
{'returncode': -15, 'output': 'times: 1\ntimes: 2\ntimes: 3\ntimes: 4\ntimes: 5\ntimes: 6\ntimes: 7\n', 'success': False}

times: 1
times: 2
times: 3
times: 4
times: 5
times: 6
times: 7
times: 8
times: 9
times: 10
[INFO] Process finished.
{'returncode': 0, 'output': 'times: 1\ntimes: 2\ntimes: 3\ntimes: 4\ntimes: 5\ntimes: 6\ntimes: 7\ntimes: 8\ntimes: 9\ntimes: 10\n', 'success': False}
"""

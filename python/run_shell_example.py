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


def run_with_terminal(command, max_timeout=30,
                      predicate: Union[None, Callable[[str], bool]] = None, wait_after_success=3):
    """
    运行 shell 命令，实时读取输出，如果 predicate 为 True，则主动 kill 掉脚本。
    """

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        executable="/bin/bash",
        preexec_fn=os.setsid  # 用来 later kill entire process group
    )

    output_lines = []
    success_detected = threading.Event()  # Python 提供的线程同步原语，用于在不同线程之间安全地传递“某个状态是否发生”的信号。

    def read_proc_stdout():
        for line in iter(proc.stdout.readline, ''):  # subprocess.stdout.readline() 是阻塞的。
            print(line, end='')  # 实时输出
            output_lines.append(line)
            if predicate and predicate(line.lower()):
                success_detected.set()  # 把 Event 设置为已触发（True）
                break

    thread = threading.Thread(target=read_proc_stdout)
    thread.start()

    start_time = time.time()
    while True:
        if proc.poll() is not None:
            print("Process finished.")
            break

        if success_detected.is_set():  # 查询 Event 是否已触发
            print(f"Predicate True. Waiting {wait_after_success}s then terminating process...")
            if wait_after_success > 0:
                time.sleep(wait_after_success)
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            # break  # 如果直接break，可能后面的returncode取值为None

        if time.time() - start_time > max_timeout:
            print("Max timeout reached. Killing process.")
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            break

        time.sleep(1)

    # 让主线程等 read_proc_stdout 这个输出监听线程把活干完
    thread.join()

    returncode = proc.poll()
    return {
        "returncode": returncode,
        "output": ''.join(output_lines),
        "success": success_detected.is_set()
    }


def _is_ok(line: str) -> bool:
    if '5' in line:
        return True
    return False


if __name__ == '__main__':
    cmd = """for i in {1..20}; do echo "times: $i"; sleep 1; done"""
    ret = run_with_terminal(cmd, max_timeout=10, predicate=_is_ok, wait_after_success=0)
    print(ret)
    print()
    ret = run_with_terminal(cmd, max_timeout=10, predicate=_is_ok, wait_after_success=3)
    print(ret)
    print()
    ret = run_with_terminal(cmd, max_timeout=3, predicate=_is_ok, wait_after_success=0)
    print(ret)
    print()
    ret = run_with_terminal(cmd, max_timeout=30, predicate=None, wait_after_success=3)
    print(ret)

"""outputpython3 shell_tool.py
times: 1
times: 2
times: 3
times: 4
times: 5
Predicate True. Waiting 0s then terminating process...
Process finished.
{'returncode': -15, 'output': 'times: 1\ntimes: 2\ntimes: 3\ntimes: 4\ntimes: 5\n', 'success': True}

times: 1
times: 2
times: 3
times: 4
times: 5
Predicate True. Waiting 3s then terminating process...
Process finished.
{'returncode': -15, 'output': 'times: 1\ntimes: 2\ntimes: 3\ntimes: 4\ntimes: 5\n', 'success': True}

times: 1
times: 2
times: 3
Max timeout reached. Killing process.
{'returncode': -9, 'output': 'times: 1\ntimes: 2\ntimes: 3\n', 'success': False}

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
times: 11
times: 12
times: 13
times: 14
times: 15
times: 16
times: 17
times: 18
times: 19
times: 20
Process finished.
{'returncode': 0, 'output': 'times: 1\ntimes: 2\ntimes: 3\ntimes: 4\ntimes: 5\ntimes: 6\ntimes: 7\ntimes: 8\ntimes: 9\ntimes: 10\ntimes: 11\ntimes: 12\ntimes: 13\ntimes: 14\ntimes: 15\ntimes: 16\ntimes: 17\ntimes: 18\ntimes: 19\ntimes: 20\n', 'success': False}
"""

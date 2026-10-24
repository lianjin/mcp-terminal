"""内部启动器：避免在多线程父进程中使用 preexec_fn。

父进程用 start_new_session 创建独立进程组；本文件只使用标准库，
在 exec 前设置控制终端。错误管道在 exec 成功时自动关闭，供父进程确认启动。
"""

import fcntl
import json
import os
import signal
import sys
import termios


def main() -> None:
    error_fd = int(sys.argv[1])
    argv = sys.argv[2:]
    try:
        os.set_inheritable(error_fd, False)
        fcntl.ioctl(0, termios.TIOCSCTTY, 0)
        os.tcsetpgrp(0, os.getpgrp())
        for sig in (signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGTERM):
            signal.signal(sig, signal.SIG_DFL)
        os.execvpe(argv[0], argv, os.environ)
    except BaseException as exc:
        # 只发送 errno，不包含命令参数、环境值或 traceback。
        payload = json.dumps({"errno": getattr(exc, "errno", None)}).encode()
        try:
            os.write(error_fd, payload)
        finally:
            os._exit(127)


if __name__ == "__main__":
    main()

"""SessionManager 对真实 PTY 的端到端行为测试。"""

from __future__ import annotations

import asyncio
import errno
import os
import shlex
import signal
import sys
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

import mcp_terminal.sessions as sessions_module
from mcp_terminal.errors import SessionError
from mcp_terminal.sessions import SessionConfig, SessionManager

POLL_INTERVAL = 0.01


def python_command(source: str) -> str:
    """构造无需 shell 的、可被 start_session 的 shlex.split 正确解析的命令。"""
    return shlex.join([sys.executable, "-u", "-c", source])


async def wait_until(
    action: Callable[[], Awaitable[Any]],
    predicate: Callable[[Any], bool],
    *,
    timeout: float = 3.0,
) -> Any:
    """有限轮询，避免将 PTY 调度时序绑定到固定长 sleep。"""
    async with asyncio.timeout(timeout):
        while True:
            value = await action()
            if predicate(value):
                return value
            await asyncio.sleep(POLL_INTERVAL)


async def wait_for_terminal_status(manager: SessionManager, session_id: str) -> dict[str, Any]:
    return await wait_until(
        lambda: manager.get_status(session_id),
        lambda status: (
            status["status"] in {"exited", "terminated", "failed"} and status["output_complete"]
        ),
    )


async def wait_for_output(
    manager: SessionManager, session_id: str, expected: str, *, timeout: float = 3.0
) -> dict[str, Any]:
    return await wait_until(
        lambda: manager.read_output(session_id, cursor=0, max_chars=65536),
        lambda result: expected in result["output"],
        timeout=timeout,
    )


def assert_error(error: BaseException, code: str) -> SessionError:
    assert isinstance(error, SessionError)
    assert error.code == code
    assert error.message
    assert isinstance(error.details, dict)
    return error


def running_config(**overrides: Any) -> SessionConfig:
    options: dict[str, Any] = {
        "buffer_chars": 1024,
        "max_sessions": 8,
        "retention_seconds": 2,
        "cleanup_interval": 0.02,
        "write_timeout": 0.10,
        "start_timeout": 2,
        "terminate_grace": 0.10,
        "drain_timeout": 0.25,
        "max_input_bytes": 65536,
    }
    options.update(overrides)
    return SessionConfig(**options)


async def test_repl_retains_variables_and_sessions_are_isolated() -> None:
    repl = "import sys\nfor line in sys.stdin:\n    exec(line, globals())\n"
    async with SessionManager(running_config()) as manager:
        first = await manager.start_session(python_command(repl))
        second = await manager.start_session(python_command(repl))
        first_id = first["session_id"]
        second_id = second["session_id"]

        await manager.write_input(first_id, "answer = 41\n")
        await manager.write_input(first_id, "print(answer + 1, flush=True)\n")
        output = await wait_until(
            lambda: manager.read_output(first_id),
            lambda result: "42" in result["output"].replace("\r\n", "\n").splitlines(),
        )
        assert "42" in output["output"].replace("\r\n", "\n").splitlines()

        await manager.write_input(
            second_id, "print(globals().get('answer', 'UNSET'), flush=True)\n"
        )
        isolated = await wait_until(
            lambda: manager.read_output(second_id),
            lambda result: "UNSET" in result["output"].replace("\r\n", "\n").splitlines(),
        )
        assert "UNSET" in isolated["output"].replace("\r\n", "\n").splitlines()
        assert "answer = 41" not in isolated["output"]


async def test_fast_and_nonzero_natural_exit_are_exited_and_output_is_complete() -> None:
    async with SessionManager(running_config()) as manager:
        clean = await manager.start_session(python_command("pass"))
        nonzero = await manager.start_session(python_command("import sys; sys.exit(7)"))

        clean_status = await wait_for_terminal_status(manager, clean["session_id"])
        nonzero_status = await wait_for_terminal_status(manager, nonzero["session_id"])

        assert clean_status["status"] == "exited"
        assert clean_status["exit_code"] == 0
        assert nonzero_status["status"] == "exited"
        assert nonzero_status["exit_code"] == 7
        for status in (clean_status, nonzero_status):
            assert status["ended_at"] is not None
            assert status["expires_at"] is not None
            assert status["output_complete"] is True
            assert status["reason"] is not None


async def test_pty_incrementally_decodes_unicode_and_replaces_invalid_bytes() -> None:
    # 把欧元符号拆在两次 write，确保采集端不会按每个 read 单独解码；0xff 必须替换。
    source = (
        "import os, time\n"
        "os.write(1, b'A\\xffB\\xe2')\n"
        "time.sleep(0.08)\n"
        "os.write(1, b'\\x82\\xacC')\n"
    )
    async with SessionManager(running_config()) as manager:
        created = await manager.start_session(python_command(source))
        session_id = created["session_id"]
        output = await wait_for_output(manager, session_id, "A�B€C")
        assert output["output"] == "A�B€C"
        status = await wait_for_terminal_status(manager, session_id)
        assert status["output_complete"] is True


async def test_pagination_replay_cursor_expiry_and_recovery() -> None:
    source = "import os; os.write(1, b'0123456789')"
    async with SessionManager(running_config(buffer_chars=6)) as manager:
        created = await manager.start_session(python_command(source))
        session_id = created["session_id"]
        status = await wait_for_terminal_status(manager, session_id)
        assert (status["earliest_cursor"], status["latest_cursor"]) == (4, 10)

        with pytest.raises(SessionError) as caught:
            await manager.read_output(session_id, cursor=0, max_chars=2)
        expired = assert_error(caught.value, "CURSOR_EXPIRED")
        assert expired.details == {"earliest_cursor": 4, "latest_cursor": 10}

        page_one = await manager.read_output(session_id, cursor=4, max_chars=2)
        assert page_one["output"] == "45"
        assert page_one["cursor"] == 4
        assert page_one["next_cursor"] == 6
        assert page_one["has_more"] is True
        # 同一 cursor 可以重放，不会被上一次 read 消费。
        assert await manager.read_output(session_id, cursor=4, max_chars=2) == page_one

        page_two = await manager.read_output(
            session_id, cursor=page_one["next_cursor"], max_chars=2
        )
        page_three = await manager.read_output(
            session_id, cursor=page_two["next_cursor"], max_chars=2
        )
        assert page_two["output"] == "67"
        assert page_three["output"] == "89"
        assert page_three["has_more"] is False
        assert page_three["output_complete"] is True


async def test_concurrent_creation_enforces_limit_including_live_sessions() -> None:
    async with SessionManager(running_config(max_sessions=2)) as manager:
        command = python_command("import time; time.sleep(30)")
        results = await asyncio.gather(
            manager.start_session(command),
            manager.start_session(command),
            manager.start_session(command),
            return_exceptions=True,
        )
        created = [result for result in results if isinstance(result, dict)]
        rejected = [result for result in results if isinstance(result, BaseException)]
        try:
            assert len(created) == 2
            assert len(rejected) == 1
            assert_error(rejected[0], "SESSION_LIMIT_REACHED")
            assert len({item["session_id"] for item in created}) == 2
        finally:
            await asyncio.gather(
                *(manager.terminate_session(item["session_id"]) for item in created),
                return_exceptions=True,
            )


async def test_write_and_terminate_race_leaves_terminal_session_and_is_idempotent() -> None:
    async with SessionManager(running_config()) as manager:
        created = await manager.start_session(python_command("import time; time.sleep(30)"))
        session_id = created["session_id"]

        write, first_terminate = await asyncio.gather(
            manager.write_input(session_id, "race\n"),
            manager.terminate_session(session_id),
            return_exceptions=True,
        )
        if isinstance(write, BaseException):
            assert_error(write, "SESSION_NOT_RUNNING")
        else:
            assert write["session_id"] == session_id
            assert 0 < write["bytes_written"] <= len(b"race\n")
        assert not isinstance(first_terminate, BaseException)
        assert first_terminate["session_id"] == session_id
        assert first_terminate["status"] == "terminated"

        repeated = await manager.terminate_session(session_id)
        assert repeated["session_id"] == session_id
        assert repeated["status"] == "terminated"
        assert (await wait_for_terminal_status(manager, session_id))["status"] == "terminated"


async def test_write_timeout_does_not_block_another_session() -> None:
    # 子进程将 stdin 设为 raw 且不读取，稳定填满 PTY 输入缓冲，促使有限写超时。
    blocked_source = (
        "import sys, termios, time\n"
        "attrs = termios.tcgetattr(sys.stdin.fileno())\n"
        "attrs[3] &= ~(termios.ICANON | termios.ECHO)\n"
        "termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, attrs)\n"
        "print('READY', flush=True)\n"
        "time.sleep(30)\n"
    )
    async with SessionManager(running_config(write_timeout=0.08)) as manager:
        blocked = await manager.start_session(python_command(blocked_source))
        responsive = await manager.start_session(python_command("import time; time.sleep(30)"))
        blocked_id = blocked["session_id"]
        responsive_id = responsive["session_id"]
        try:
            await wait_for_output(manager, blocked_id, "READY")
            pending_write = asyncio.create_task(manager.write_input(blocked_id, "x" * 65536))
            await asyncio.sleep(0)
            # 正在等待 EAGAIN 时，其他会话 API 仍需迅速响应，不能被全局锁/阻塞写卡住。
            async with asyncio.timeout(0.30):
                other_status = await manager.get_status(responsive_id)
            assert other_status["status"] == "running"

            with pytest.raises(SessionError) as caught:
                await pending_write
            timeout_error = assert_error(caught.value, "WRITE_TIMEOUT")
            assert 0 <= timeout_error.details["bytes_written"] < 65536
        finally:
            await asyncio.gather(
                manager.terminate_session(blocked_id),
                manager.terminate_session(responsive_id),
                return_exceptions=True,
            )


async def test_expired_finished_record_is_removed_by_background_cleanup() -> None:
    async with SessionManager(
        running_config(retention_seconds=0.08, cleanup_interval=0.01)
    ) as manager:
        created = await manager.start_session(python_command("pass"))
        session_id = created["session_id"]
        finished = await wait_for_terminal_status(manager, session_id)
        assert finished["status"] == "exited"
        assert finished["expires_at"] is not None

        async def removed() -> bool:
            try:
                await manager.get_status(session_id)
            except SessionError as error:
                assert error.code == "SESSION_NOT_FOUND"
                return True
            return False

        assert await wait_until(removed, bool, timeout=1.5) is True


async def test_close_reaps_a_running_process() -> None:
    manager = SessionManager(running_config())
    pid: int | None = None
    try:
        created = await manager.start_session(python_command("import time; time.sleep(30)"))
        pid = created["pid"]
        await manager.close()
        # close 返回时必须已经 wait/reap，而不是遗留后台子进程。
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        await manager.close()
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


async def test_injected_pty_and_spawn_failures_release_capacity_and_open_fds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """openpty/spawn 任一失败均不得留下 slot 或已分配的 master/slave FD。"""
    async with SessionManager(running_config(max_sessions=1)) as manager:
        original_openpty = sessions_module.os.openpty

        def pty_failure() -> tuple[int, int]:
            raise OSError(errno.EMFILE, "injected openpty failure")

        monkeypatch.setattr(sessions_module.os, "openpty", pty_failure)
        with pytest.raises(SessionError) as caught:
            await manager.start_session(python_command("pass"))
        assert_error(caught.value, "SPAWN_FAILED")
        monkeypatch.setattr(sessions_module.os, "openpty", original_openpty)

        allocated: list[int] = []

        def recording_openpty() -> tuple[int, int]:
            pair = original_openpty()
            allocated.extend(pair)
            return pair

        async def spawn_failure(*_args: Any, **_kwargs: Any) -> Any:
            raise OSError(errno.EAGAIN, "injected subprocess spawn failure")

        monkeypatch.setattr(sessions_module.os, "openpty", recording_openpty)
        monkeypatch.setattr(sessions_module.asyncio, "create_subprocess_exec", spawn_failure)
        with pytest.raises(SessionError) as caught:
            await manager.start_session(python_command("pass"))
        assert_error(caught.value, "SPAWN_FAILED")
        assert len(allocated) == 2
        for descriptor in allocated:
            with pytest.raises(OSError):
                os.fstat(descriptor)

        monkeypatch.undo()
        # max_sessions=1；失败的 reservation 必须释放，随后创建能够成功。
        recovered = await manager.start_session(python_command("pass"))
        status = await wait_for_terminal_status(manager, recovered["session_id"])
        assert status["status"] == "exited"

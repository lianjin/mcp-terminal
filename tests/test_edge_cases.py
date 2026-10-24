"""失败注入与并发回归；全部进程只在临时测试上下文内存活。"""

from __future__ import annotations

import asyncio
import errno
import os
import shlex
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from mcp import Client

import mcp_terminal.sessions as session_module
from mcp_terminal.errors import SessionError
from mcp_terminal.server import create_server
from mcp_terminal.sessions import SessionConfig, SessionManager


def command(code: str) -> str:
    return shlex.join([sys.executable, "-u", "-c", code])


def config(**kwargs: Any) -> SessionConfig:
    return SessionConfig(**{"terminate_grace": 0.08, "drain_timeout": 0.3, **kwargs})


async def until(action: Any, predicate: Any, timeout: float = 3) -> Any:
    async with asyncio.timeout(timeout):
        while True:
            result = await action()
            if predicate(result):
                return result
            await asyncio.sleep(0.005)


async def complete(manager: SessionManager, sid: str) -> dict[str, Any]:
    return await until(lambda: manager.get_status(sid), lambda data: data["output_complete"])


async def output(manager: SessionManager, sid: str, marker: str) -> dict[str, Any]:
    return await until(lambda: manager.read_output(sid), lambda data: marker in data["output"])


@pytest.mark.parametrize(
    "name,value",
    [
        ("buffer_chars", 0),
        ("max_sessions", True),
        ("max_input_bytes", -1),
        ("retention_seconds", 0),
        ("cleanup_interval", float("nan")),
        ("write_timeout", False),
        ("start_timeout", float("inf")),
        ("terminate_grace", -1),
        ("drain_timeout", "1"),
    ],
)
def test_config_rejects_unbounded_or_invalid_limits(name: str, value: Any) -> None:
    with pytest.raises(ValueError):
        SessionConfig(**{name: value})


@pytest.mark.parametrize(
    "cmd,code",
    [
        ("", "INVALID_ARGUMENT"),
        ("   ", "INVALID_ARGUMENT"),
        ("'", "INVALID_ARGUMENT"),
        ("''", "INVALID_ARGUMENT"),
        ("python\0", "INVALID_ARGUMENT"),
        ("not-a-real-program-stage2-982747", "COMMAND_NOT_FOUND"),
        ("/not/a/real/program", "COMMAND_NOT_FOUND"),
        ("x" * 8193, "INVALID_ARGUMENT"),
    ],
)
async def test_start_failures_release_reservations(cmd: str, code: str) -> None:
    async with SessionManager(config(max_sessions=1)) as manager:
        with pytest.raises(SessionError) as caught:
            await manager.start_session(cmd)
        assert caught.value.code == code
        assert not manager._pending
        assert not manager._sessions
        created = await manager.start_session(command("pass"))
        assert (await complete(manager, created["session_id"]))["exit_code"] == 0


@pytest.mark.parametrize("directory", ["missing", "file", "", "nul", False])
async def test_invalid_cwd(directory: Any, tmp_path: Path) -> None:
    candidate: Any = directory
    if directory == "missing":
        candidate = str(tmp_path / "missing")
    elif directory == "file":
        target = tmp_path / "file"
        target.write_text("not a directory")
        candidate = str(target)
    elif directory == "nul":
        candidate = "\0"
    async with SessionManager(config()) as manager:
        with pytest.raises(SessionError) as caught:
            await manager.start_session(command("pass"), cwd=candidate)
        assert caught.value.code == "INVALID_CWD"


@pytest.mark.parametrize(
    "env",
    [
        {1: "x"},
        {"x": 1},
        {"": "x"},
        {"A=B": "x"},
        {"A\0": "x"},
        {"A": "\0"},
        {"A": "\ud800"},
        {"A": "x" * 8193},
        {"A" * 257: "x"},
        {str(i): "x" for i in range(129)},
        {str(i): "x" * 8192 for i in range(9)},
    ],
)
async def test_invalid_env_is_rejected_without_exposing_values(env: Any) -> None:
    async with SessionManager(config()) as manager:
        with pytest.raises(SessionError) as caught:
            await manager.start_session(command("pass"), env=env)
        assert caught.value.code == "INVALID_ARGUMENT"
        assert not manager._sessions


async def test_cwd_relative_path_env_isolation_and_stderr_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STAGE2_PRIVATE_SECRET", "secret-do-not-inherit")
    executable = tmp_path / "local-python"
    executable.symlink_to(sys.executable)
    code = (
        "import os; print(os.getcwd()); print(os.getenv('EXPLICIT')); "
        "print(os.getenv('STAGE2_PRIVATE_SECRET', 'not-inherited')); "
        "os.write(1,b'OUT'); os.write(2,b'ERR')"
    )
    async with SessionManager(
        config(
            allowed_cwd_roots=(str(tmp_path),),
            allowed_env_keys=("PATH", "EXPLICIT"),
            audit_log_path=str(tmp_path / "audit/sessions.jsonl"),
        )
    ) as manager:
        created = await manager.start_session(
            shlex.join(["local-python", "-u", "-c", code]),
            cwd=str(tmp_path),
            env={"PATH": "", "EXPLICIT": "visible"},
        )
        sid = created["session_id"]
        status = await complete(manager, sid)
        text = (await manager.read_output(sid))["output"]
        assert str(tmp_path) in text
        assert "visible\r\n" in text
        assert "not-inherited\r\n" in text
        assert "secret-do-not-inherit" not in text
        assert "OUTERR" in text
        assert status["cwd"] == str(tmp_path.resolve())
        assert status["exit_code"] == 0
        assert manager._sessions[sid].master_fd is None
        assert manager._sessions[sid].process is None


async def test_command_is_not_implicitly_interpreted_as_shell() -> None:
    code = "import sys; print(repr(sys.argv[1:]))"
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command(code) + " '|' '>' '&&' '$HOME'")
        await complete(manager, created["session_id"])
        text = (await manager.read_output(created["session_id"]))["output"]
        assert "['|', '>', '&&', '$HOME']" in text


@pytest.mark.parametrize(
    "operation", ["read_output", "write_input", "get_status", "terminate_session"]
)
async def test_unknown_session_for_every_operation(operation: str) -> None:
    async with SessionManager(config()) as manager:
        args = ["unknown"] + (["hi\n"] if operation == "write_input" else [])
        with pytest.raises(SessionError) as caught:
            await getattr(manager, operation)(*args)
        assert caught.value.code == "SESSION_NOT_FOUND"


@pytest.mark.parametrize("sid", ["", "s" * 129, None, 1, True])
async def test_invalid_session_id(sid: Any) -> None:
    async with SessionManager(config()) as manager:
        with pytest.raises(SessionError) as caught:
            await manager.get_status(sid)
        assert caught.value.code == "INVALID_ARGUMENT"


@pytest.mark.parametrize("text", ["", "\ud800", "x" * 65537, "界" * 22000])
async def test_invalid_input(text: str) -> None:
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command("import time; time.sleep(30)"))
        with pytest.raises(SessionError) as caught:
            await manager.write_input(created["session_id"], text)
        assert caught.value.code == "INVALID_ARGUMENT"


async def test_empty_output_is_immediate_and_reads_can_replay_concurrently() -> None:
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command("import time; time.sleep(30)"))
        sid = created["session_id"]
        async with asyncio.timeout(0.1):
            empty = await manager.read_output(sid, cursor=0)
        assert empty["output"] == "" and empty["next_cursor"] == 0
        await manager.write_input(sid, "echo-me\n")
        await output(manager, sid, "echo-me")
        reads = await asyncio.gather(*(manager.read_output(sid, cursor=0) for _ in range(10)))
        assert len({read["output"] for read in reads}) == 1
        assert "echo-me" in reads[0]["output"]


async def test_live_session_is_not_expired_and_finished_output_ttl_is_not_extended() -> None:
    async with SessionManager(config(retention_seconds=0.12, cleanup_interval=0.01)) as manager:
        created = await manager.start_session(command("import time; print('KEPT'); time.sleep(30)"))
        sid = created["session_id"]
        await output(manager, sid, "KEPT")
        await asyncio.sleep(0.15)
        assert (await manager.get_status(sid))["status"] == "running"
        terminal = await manager.terminate_session(sid)
        deadline = terminal["expires_at"]
        assert (
            abs(
                (
                    datetime.fromisoformat(deadline) - datetime.fromisoformat(terminal["ended_at"])
                ).total_seconds()
                - 0.12
            )
            < 0.001
        )
        for _ in range(3):
            assert "KEPT" in (await manager.read_output(sid))["output"]
            assert (await manager.get_status(sid))["expires_at"] == deadline
            await asyncio.sleep(0.01)
        with pytest.raises(SessionError) as caught:
            await manager.write_input(sid, "no\n")
        assert caught.value.code == "SESSION_NOT_RUNNING"
        retained = manager._sessions[sid]
        # 不再调用任何查询，证明是后台 sweep 清除，而非 get_status 的惰性删除。
        await asyncio.sleep(0.16)
        assert sid not in manager._sessions
        assert retained.buffer.size == 0
        with pytest.raises(SessionError) as caught:
            await manager.read_output(sid)
        assert caught.value.code == "SESSION_NOT_FOUND"


async def test_retained_record_counts_toward_limit_until_expiry() -> None:
    async with SessionManager(
        config(max_sessions=1, retention_seconds=0.08, cleanup_interval=0.01)
    ) as manager:
        created = await manager.start_session(command("pass"))
        await complete(manager, created["session_id"])
        with pytest.raises(SessionError) as caught:
            await manager.start_session(command("pass"))
        assert caught.value.code == "SESSION_LIMIT_REACHED"
        await asyncio.sleep(0.1)
        recovered = await manager.start_session(command("pass"))
        assert recovered["session_id"] != created["session_id"]


async def test_real_python_repl_ctrl_c_and_eof() -> None:
    async with SessionManager(config()) as manager:
        created = await manager.start_session(shlex.join([sys.executable, "-i", "-q"]))
        sid = created["session_id"]
        await output(manager, sid, ">>>")
        await manager.write_input(sid, "import time; time.sleep(30)\n")
        await asyncio.sleep(0.04)
        await manager.write_input(sid, "\x03")
        await output(manager, sid, "KeyboardInterrupt")
        assert (await manager.get_status(sid))["status"] == "running"
        await manager.write_input(sid, "\x04")
        assert (await complete(manager, sid))["status"] == "exited"


async def test_concurrent_same_session_writes_are_not_interleaved() -> None:
    code = (
        "import sys; print('READY'); "
        "[print('RESULT:'+line.strip(),flush=True) for line in sys.stdin]"
    )
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command(code))
        sid = created["session_id"]
        await output(manager, sid, "READY")
        await asyncio.gather(*(manager.write_input(sid, f"item-{i}\n") for i in range(20)))
        text = (
            await until(
                lambda: manager.read_output(sid), lambda r: r["output"].count("RESULT:") == 20
            )
        )["output"]
        lines = text.replace("\r\n", "\n").splitlines()
        assert {line for line in lines if line.startswith("RESULT:")} == {
            f"RESULT:item-{i}" for i in range(20)
        }


async def test_cancelled_termination_finishes_and_escalates_to_kill() -> None:
    code = (
        "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        "print('READY'); time.sleep(30)"
    )
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command(code))
        sid = created["session_id"]
        await output(manager, sid, "READY")
        operation = asyncio.create_task(manager.terminate_session(sid))
        await asyncio.sleep(0.01)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        terminal = await complete(manager, sid)
        assert terminal["status"] == "terminated"
        assert terminal["exit_code"] == -signal.SIGKILL
        with pytest.raises(ProcessLookupError):
            os.kill(created["pid"], 0)


@pytest.mark.parametrize("mode", ["cancel", "timeout"])
async def test_start_cancel_or_timeout_during_handshake_reaps_child(
    mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = asyncio.Event()
    pids: list[int] = []
    fds: list[int] = []
    original_spawn = asyncio.create_subprocess_exec
    original_pty = os.openpty

    async def spawn(*args: Any, **kwargs: Any) -> Any:
        process = await original_spawn(*args, **kwargs)
        pids.append(process.pid)
        return process

    def allocate() -> tuple[int, int]:
        pair = original_pty()
        fds.extend(pair)
        return pair

    async def blocked_handshake(_fd: int) -> bytes:
        entered.set()
        await asyncio.sleep(30)
        return b""

    monkeypatch.setattr(session_module.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(session_module.os, "openpty", allocate)
    async with SessionManager(config(start_timeout=0.12)) as manager:
        monkeypatch.setattr(manager, "_pipe_contents", blocked_handshake)
        operation = asyncio.create_task(
            manager.start_session(command("import time; time.sleep(30)"))
        )
        async with asyncio.timeout(2):
            await entered.wait()
        if mode == "cancel":
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation
        else:
            with pytest.raises(SessionError) as caught:
                await operation
            assert caught.value.code == "SPAWN_TIMEOUT"
        assert not manager._sessions and not manager._pending
        assert pids
        for pid in pids:
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        for fd in fds:
            with pytest.raises(OSError):
                os.fstat(fd)


async def test_cancel_after_launch_before_id_delivery_does_not_orphan_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with SessionManager(config()) as manager:
        original_launch = manager._launch
        operation: asyncio.Task[Any]

        async def launch_then_cancel(*args: Any) -> Any:
            result = await original_launch(*args)
            operation.cancel()
            return result

        monkeypatch.setattr(manager, "_launch", launch_then_cancel)
        operation = asyncio.create_task(
            manager.start_session(command("import time; time.sleep(30)"))
        )
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert len(manager._sessions) == 1
        retained = next(iter(manager._sessions.values()))
        assert retained.status == "terminated" and retained.finished.is_set()
        with pytest.raises(ProcessLookupError):
            os.kill(retained.pid, 0)


async def test_pipe_failure_closes_allocated_pty(monkeypatch: pytest.MonkeyPatch) -> None:
    fds: list[int] = []
    original = os.openpty

    def allocate() -> tuple[int, int]:
        pair = original()
        fds.extend(pair)
        return pair

    def fail_pipe() -> Any:
        raise OSError(errno.EMFILE, "injected pipe failure")

    async with SessionManager(config()) as manager:
        monkeypatch.setattr(session_module.os, "openpty", allocate)
        monkeypatch.setattr(session_module.os, "pipe", fail_pipe)
        with pytest.raises(SessionError) as caught:
            await manager.start_session(command("pass"))
        assert caught.value.code == "SPAWN_FAILED"
        for fd in fds:
            with pytest.raises(OSError):
                os.fstat(fd)
        assert not manager._pending


async def test_exec_failure_is_reported_as_start_failure(tmp_path: Path) -> None:
    target = tmp_path / "invalid-executable"
    target.write_text("invalid executable without a shebang")
    target.chmod(0o755)
    async with SessionManager(config()) as manager:
        with pytest.raises(SessionError) as caught:
            await manager.start_session(str(target))
        assert caught.value.code == "SPAWN_FAILED"
        assert caught.value.details["errno"] == errno.ENOEXEC
        assert not manager._sessions


@pytest.mark.parametrize("failure", ["zero", "error"])
async def test_input_io_failure_is_explicit(failure: str, monkeypatch: pytest.MonkeyPatch) -> None:
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command("import time; time.sleep(30)"))
        sid = created["session_id"]
        fd = manager._sessions[sid].master_fd
        original = os.write

        def write(descriptor: int, data: bytes) -> int:
            if descriptor == fd:
                if failure == "zero":
                    return 0
                raise OSError(errno.EIO, "injected input failure")
            return original(descriptor, data)

        monkeypatch.setattr(session_module.os, "write", write)
        with pytest.raises(SessionError) as caught:
            await manager.write_input(sid, "hello\n")
        assert caught.value.code == "INPUT_CLOSED"
        assert caught.value.details["bytes_written"] == 0


async def test_output_io_failure_updates_failed_status(monkeypatch: pytest.MonkeyPatch) -> None:
    async with SessionManager(config()) as manager:
        created = await manager.start_session(command("import time; time.sleep(30)"))
        sid = created["session_id"]
        fd = manager._sessions[sid].master_fd
        original = os.read

        def read(descriptor: int, count: int) -> bytes:
            if descriptor == fd:
                raise OSError(errno.EBADF, "injected output failure")
            return original(descriptor, count)

        monkeypatch.setattr(session_module.os, "read", read)
        # 输入回显令 reader 从可读回调醒来，然后遇到注入的错误。
        await manager.write_input(sid, "wake\n")
        terminal = await complete(manager, sid)
        assert terminal["status"] == "failed" and terminal["reason"] == "output_read_error"


async def test_closed_manager_rejects_new_operations_and_close_is_idempotent() -> None:
    manager = SessionManager(config())
    await asyncio.gather(manager.close(), manager.close())
    for operation in (manager.get_status("x"), manager.start_session(command("pass"))):
        with pytest.raises(SessionError) as caught:
            await operation
        assert caught.value.code == "SERVER_SHUTTING_DOWN"


async def test_mcp_ring_error_recovery_and_internal_error_sanitization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with Client(create_server(config(buffer_chars=8))) as client:
        result = await client.call_tool(
            "start_session", {"command": command("print('0123456789',end='')")}
        )
        sid = result.structured_content["data"]["session_id"]
        await until(
            lambda: client.call_tool("get_status", {"session_id": sid}),
            lambda result: result.structured_content["data"]["output_complete"],
        )
        stale = await client.call_tool("read_output", {"session_id": sid, "cursor": 0})
        envelope = stale.structured_content
        assert stale.is_error and not envelope["ok"] and envelope["data"] is None
        assert envelope["error"]["code"] == "CURSOR_EXPIRED"
        earliest = envelope["error"]["details"]["earliest_cursor"]
        recovered = await client.call_tool("read_output", {"session_id": sid, "cursor": earliest})
        assert recovered.structured_content["data"]["output"] == "23456789"
        ahead = await client.call_tool("read_output", {"session_id": sid, "cursor": 100})
        assert ahead.structured_content["error"]["code"] == "CURSOR_AHEAD"

        async def crash(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("sensitive-value-must-not-leak")

        monkeypatch.setattr(SessionManager, "get_status", crash)
        error = await client.call_tool("get_status", {"session_id": sid})
        assert error.is_error and error.structured_content["error"]["code"] == "INTERNAL_ERROR"
        assert "sensitive-value-must-not-leak" not in str(error)
        assert not (await client.call_tool("ping", {})).is_error


async def test_mcp_pending_termination_does_not_block_other_requests() -> None:
    async with Client(create_server(config(terminate_grace=0.25))) as client:
        code = (
            "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
            "print('READY'); time.sleep(30)"
        )
        created = await client.call_tool("start_session", {"command": command(code)})
        sid = created.structured_content["data"]["session_id"]
        await until(
            lambda: client.call_tool("read_output", {"session_id": sid}),
            lambda result: "READY" in result.structured_content["data"]["output"],
        )
        pending = asyncio.create_task(client.call_tool("terminate_session", {"session_id": sid}))
        await asyncio.sleep(0.01)
        started = time.monotonic()
        pong = await client.call_tool("ping", {})
        assert time.monotonic() - started < 0.15 and not pong.is_error
        assert not pending.done()
        result = await pending
        assert result.structured_content["data"]["status"] == "terminated"


async def test_close_during_creation_does_not_leave_a_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = SessionManager(config())
    entered = asyncio.Event()
    pids: list[int] = []
    original = asyncio.create_subprocess_exec

    async def spawn(*args: Any, **kwargs: Any) -> Any:
        process = await original(*args, **kwargs)
        pids.append(process.pid)
        return process

    async def hold(_fd: int) -> bytes:
        entered.set()
        await asyncio.sleep(30)
        return b""

    monkeypatch.setattr(session_module.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(manager, "_pipe_contents", hold)
    operation = asyncio.create_task(manager.start_session(command("import time; time.sleep(30)")))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            await manager.close()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert not manager._sessions and not manager._pending
        for pid in pids:
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
    finally:
        await manager.close()
        await asyncio.gather(operation, return_exceptions=True)


async def test_high_volume_output_keeps_latest_data_and_other_session_responsive() -> None:
    async with SessionManager(config(buffer_chars=128)) as manager:
        flood = await manager.start_session(command("import os\nwhile True: os.write(1,b'N'*4096)"))
        sid = flood["session_id"]
        await until(lambda: manager.get_status(sid), lambda s: s["latest_cursor"] > 8192)
        started = time.monotonic()
        other = await manager.start_session(command("import time; time.sleep(30)"))
        assert time.monotonic() - started < 1
        assert (await manager.get_status(other["session_id"]))["status"] == "running"
        latest = await manager.read_output(sid)
        assert latest["output"] == "N" * 128
        assert latest["earliest_cursor"] > 0
        assert (await manager.get_status(sid))["buffered_chars"] == 128
        with pytest.raises(SessionError) as caught:
            await manager.read_output(sid, cursor=0)
        assert caught.value.code == "CURSOR_EXPIRED"


async def test_delivered_repl_client_uses_venv_and_real_stdio(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "outputs/stage2/repl_client.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        cwd=tmp_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        text = stdout.decode()
        assert "独立输出行 200" in text
        assert '"tool": "start_session"' in text
        assert '"tool": "write_input"' in text
        assert '"tool": "read_output"' in text
        assert '"tool": "get_status"' in text
        assert '"tool": "terminate_session"' in text
        assert '"status": "terminated"' in text
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

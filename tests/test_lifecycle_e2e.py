"""生命周期真实 stdio MCP 端到端测试；不直接访问服务端会话管理器。"""

from __future__ import annotations

import asyncio
import errno
import json
import os
import shlex
import signal
import sys
from pathlib import Path
from typing import Any

from test_acceptance_e2e import Driver, connected, python_command


def lifecycle_config(root: Path, audit_path: Path, **overrides: Any) -> dict[str, Any]:
    """构造经测试启动器 JSON 注入的短生命周期配置。"""
    options: dict[str, Any] = {
        "buffer_chars": 4096,
        "max_sessions": 8,
        "retention_seconds": 4,
        "cleanup_interval": 0.02,
        "write_timeout": 0.25,
        "start_timeout": 2,
        "terminate_grace": 0.20,
        "drain_timeout": 0.30,
        "max_input_bytes": 65536,
        "allowed_cwd_roots": [str(root)],
        "allowed_env_keys": [],
        "audit_log_path": str(audit_path),
        "audit_max_bytes": 1024 * 1024,
        "audit_backup_count": 1,
        "audit_queue_size": 64,
        "audit_timeout": 0.25,
    }
    options.update(overrides)
    return options


async def wait_until_expired(driver: Driver, session_id: str) -> None:
    """通过同一真实 MCP stdio 连接轮询，直到清理器移除结束记录。"""
    async with asyncio.timeout(3):
        while True:
            result = await driver.client.call_tool("get_status", {"session_id": session_id})
            envelope = result.structured_content
            assert isinstance(envelope, dict) and set(envelope) == {"ok", "data", "error"}
            if result.is_error:
                assert envelope["error"]["code"] == "SESSION_NOT_FOUND"
                return
            # 持续读取而非只查状态，任何“读输出续期”回归都应使本循环超时失败。
            page = await driver.client.call_tool(
                "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 4096}
            )
            page_envelope = page.structured_content
            assert isinstance(page_envelope, dict)
            if page.is_error:
                assert page_envelope["error"]["code"] == "SESSION_NOT_FOUND"
                return
            assert page_envelope["data"]["output_complete"] is True
            await asyncio.sleep(0.01)


async def wait_for_expiry_audit(audit_path: Path, session_id: str) -> None:
    """等待异步审计写线程追加本会话的到期事件。"""
    async with asyncio.timeout(3):
        while True:
            if audit_path.exists():
                records = [
                    json.loads(line)
                    for line in audit_path.read_text(encoding="utf-8").splitlines()
                    if line
                ]
                if any(
                    record.get("event") == "session_expired"
                    and record.get("session_id") == session_id
                    for record in records
                ):
                    return
            await asyncio.sleep(0.01)


async def wait_for_pid_reap(pid: int) -> None:
    """只检查本测试启动并由服务报告的 PID 是否已被回收。"""
    async with asyncio.timeout(2):
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.01)


async def test_stdio_expiry_retains_output_then_removes_metadata_without_touch_refresh(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "logs" / "expiry.jsonl"
    config = lifecycle_config(
        tmp_path,
        audit_path,
        retention_seconds=0.6,
        cleanup_interval=0.01,
    )

    async with connected(tmp_path, config) as driver:
        created = await driver.start(python_command("print('TTL-NONCONSUMED', flush=True)"))
        session_id = created["session_id"]
        terminal = await driver.finished(session_id)
        first_page = await driver.request(
            "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 4096}
        )
        replayed_page = await driver.request(
            "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 4096}
        )
        after_replays = await driver.status(session_id)

        assert "TTL-NONCONSUMED" in first_page["output"].replace("\r\n", "\n").splitlines()
        assert replayed_page == first_page  # read_output 为重放，绝不消费输出。
        assert terminal["duration_seconds"] == after_replays["duration_seconds"]
        assert terminal["expires_at"] == after_replays["expires_at"]
        assert terminal["output_complete"] is True

        await wait_until_expired(driver, session_id)
        for tool, arguments in (
            ("read_output", {"session_id": session_id, "cursor": 0}),
            ("get_status", {"session_id": session_id}),
            ("write_input", {"session_id": session_id, "input": "unused"}),
            ("terminate_session", {"session_id": session_id}),
        ):
            await driver.request(tool, arguments, error="SESSION_NOT_FOUND")
        await wait_for_expiry_audit(audit_path, session_id)


async def test_stdio_termination_wait_and_session_limit_do_not_block_other_session(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "logs" / "termination.jsonl"
    config = lifecycle_config(
        tmp_path,
        audit_path,
        max_sessions=2,
        retention_seconds=5,
        terminate_grace=0.8,
    )
    ignores_term = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, lambda *_: None)\n"
        "print('TERM-READY', flush=True)\n"
        "time.sleep(30)\n"
    )
    repl = shlex.join([sys.executable, "-i", "-q"])

    async with connected(tmp_path, config) as driver:
        first_id: str | None = None
        second_id: str | None = None
        terminating: asyncio.Task[dict[str, Any]] | None = None
        try:
            first = await driver.start(python_command(ignores_term))
            second = await driver.start(repl)
            first_id, second_id = first["session_id"], second["session_id"]
            await driver.until_line(first_id, "TERM-READY")
            await driver.request(
                "start_session",
                {"command": python_command("print('THIRD-MUST-NOT-START')")},
                error="SESSION_LIMIT_REACHED",
            )

            terminating = asyncio.create_task(
                driver.request("terminate_session", {"session_id": first_id})
            )
            # 让请求已经跨越 stdio 边界并向忽略 SIGTERM 的实际进程发出终止信号。
            await asyncio.sleep(0.05)
            assert not terminating.done()

            async with asyncio.timeout(0.5):
                pong, live_status, assignment = await asyncio.gather(
                    driver.request("ping", {}),
                    driver.status(second_id),
                    driver.request(
                        "write_input",
                        {"session_id": second_id, "input": "x = 21", "append_newline": True},
                    ),
                )
            assert pong["message"] == "pong"
            assert live_status["status"] == "running"
            assert assignment["bytes_written"] > 0
            await driver.request(
                "write_input",
                {"session_id": second_id, "input": "print(x * 2)", "append_newline": True},
            )
            first_result, cursor = await driver.until_line(second_id, "42")
            assert "42" in first_result.replace("\r\n", "\n").splitlines()
            assert not terminating.done()

            terminated = await terminating
            assert terminated["status"] == "terminated" and terminated["output_complete"] is True
            repeated = await driver.request("terminate_session", {"session_id": first_id})
            assert repeated == terminated
            # 已结束记录仍在保留期内，因而也仍占据 max_sessions 配额。
            await driver.request(
                "start_session",
                {"command": python_command("print('LIMIT-MUST-REMAIN')")},
                error="SESSION_LIMIT_REACHED",
            )

            await driver.request(
                "write_input",
                {"session_id": second_id, "input": "print(x + 1)", "append_newline": True},
            )
            continuation, _ = await driver.until_line(second_id, "22", cursor)
            assert "22" in continuation.replace("\r\n", "\n").splitlines()
        finally:
            cleanup = []
            if first_id is not None:
                cleanup.append(driver.request("terminate_session", {"session_id": first_id}))
            if second_id is not None:
                cleanup.append(driver.request("terminate_session", {"session_id": second_id}))
            if terminating is not None:
                cleanup.append(terminating)
            if cleanup:
                try:
                    async with asyncio.timeout(3):
                        await asyncio.gather(*cleanup, return_exceptions=True)
                except TimeoutError:
                    pass


async def test_stdio_exec_failure_and_signal_exit_are_structured_and_recoverable(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "logs" / "spawn.jsonl"
    broken = tmp_path / "invalid-executable"
    broken.write_text("this has no shebang and is not an executable format\n", encoding="utf-8")
    os.chmod(broken, 0o700)

    async with connected(tmp_path, lifecycle_config(tmp_path, audit_path)) as driver:
        failure = await driver.request(
            "start_session",
            {"command": shlex.join([str(broken)])},
            error="SPAWN_FAILED",
        )
        assert failure["details"] == {"errno": errno.ENOEXEC}
        assert (await driver.request("ping", {}))["message"] == "pong"

        signalled = await driver.start(
            python_command("import os, signal; os.kill(os.getpid(), signal.SIGTERM)")
        )
        terminal = await driver.finished(signalled["session_id"])
        assert terminal["status"] == "exited"
        assert terminal["exit_code"] == -signal.SIGTERM
        assert terminal["reason"] == "process_exit"


async def test_stdio_audit_failure_is_fail_closed_but_termination_still_reaps(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "logs" / "permissions.jsonl"
    marker = tmp_path / "must-not-be-created"
    fault_gate = tmp_path / "activate-audit-fault"
    config = lifecycle_config(tmp_path, audit_path, retention_seconds=5)
    weakens_audit = (
        "import os, sys, time\n"
        f"while not os.path.exists({str(fault_gate)!r}):\n"
        "    time.sleep(0.005)\n"
        f"os.chmod({str(audit_path)!r}, 0o666)\n"
        "print('AUDIT-WEAK-READY', flush=True)\n"
        "sys.stdin.readline()\n"
    )

    async with connected(tmp_path, config) as driver:
        created = await driver.start(python_command(weakens_audit))
        session_id, pid = created["session_id"], created["pid"]
        try:
            # start 已返回意味着 session_created 已落盘，之后才触发受控文件权限故障。
            fault_gate.touch()
            await driver.until_line(session_id, "AUDIT-WEAK-READY")
            assert (audit_path.stat().st_mode & 0o777) == 0o666

            blocked_start = await driver.request(
                "start_session",
                {
                    "command": python_command(
                        f"from pathlib import Path; Path({str(marker)!r}).write_text('side effect')"
                    )
                },
                error="AUDIT_UNAVAILABLE",
            )
            assert blocked_start["details"]["execution_started"] is False
            assert not marker.exists()
            blocked_write = await driver.request(
                "write_input",
                {"session_id": session_id, "input": "must not arrive", "append_newline": True},
                error="AUDIT_UNAVAILABLE",
            )
            assert blocked_write["details"]["bytes_written"] == 0

            before_termination = await driver.status(session_id)
            visible_output = await driver.request(
                "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 4096}
            )
            assert before_termination["audit_error"] == "AUDIT_UNAVAILABLE"
            assert "AUDIT-WEAK-READY" in visible_output["output"].replace("\r\n", "\n")

            terminated = await driver.request("terminate_session", {"session_id": session_id})
            status = await driver.status(session_id)
            after_termination = await driver.request(
                "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 4096}
            )
            assert terminated["status"] == "terminated" and terminated["output_complete"] is True
            assert status["audit_error"] == "AUDIT_UNAVAILABLE"
            assert "AUDIT-WEAK-READY" in after_termination["output"].replace("\r\n", "\n")
            await wait_for_pid_reap(pid)
        finally:
            # 让服务的正常 lifespan 退出可安全关闭其审计线程；不改变测试目录以外文件。
            if audit_path.exists():
                os.chmod(audit_path, 0o600)

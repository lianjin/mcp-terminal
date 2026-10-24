"""审计超时后的迟到错误与交互写入取消的资源/事件回归。"""

import asyncio
import gc
import json
import os
import shlex
import sys
import threading

import pytest

from mcp_terminal.audit import AuditLog
from mcp_terminal.errors import SessionError
from mcp_terminal.sessions import SessionConfig, SessionManager


async def test_late_audit_failure_after_ack_timeout_is_consumed(tmp_path, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    exceptions = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: exceptions.append(context))
    log = AuditLog(tmp_path / "private/events.jsonl", timeout=0.03)

    def late_failure(self, data):
        entered.set()
        release.wait(2)
        raise OSError("late injected file failure")

    monkeypatch.setattr(AuditLog, "_write_line", late_failure)
    try:
        await log.start()
        task = asyncio.create_task(log.record({"event": "test"}))
        assert await asyncio.to_thread(entered.wait, 1)
        with pytest.raises(SessionError) as caught:
            await task
        assert caught.value.code == "AUDIT_UNAVAILABLE"
        release.set()
        async with asyncio.timeout(2):
            await log.close()
        del task
        gc.collect()
        await asyncio.sleep(0.02)
        assert not exceptions
    finally:
        release.set()
        await log.close()
        loop.set_exception_handler(previous_handler)


async def test_cancelled_input_records_submitted_bytes_without_payload(tmp_path, monkeypatch):
    log = tmp_path / "private/events.jsonl"
    config = SessionConfig(allowed_cwd_roots=(str(tmp_path),), audit_log_path=str(log))
    async with SessionManager(config) as manager:
        created = await manager.start_session(
            shlex.join(
                [
                    sys.executable,
                    "-u",
                    "-c",
                    "import time; time.sleep(30)",
                ]
            )
        )
        sid = created["session_id"]
        session = manager._sessions[sid]
        original = os.write
        first_write = asyncio.Event()
        sent = False

        def partial_write(fd, data):
            nonlocal sent
            if session.master_fd is not None and fd == session.master_fd:
                if sent:
                    raise BlockingIOError()
                sent = True
                count = original(fd, data[:2])
                first_write.set()
                return count
            return original(fd, data)

        with monkeypatch.context() as patch:
            patch.setattr(os, "write", partial_write)
            task = asyncio.create_task(manager.write_input(sid, "PRIVATE_INPUT_VALUE"))
            async with asyncio.timeout(3):
                await first_write.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await manager.terminate_session(sid)
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    cancelled = [row for row in rows if row["event"] == "input_cancelled"]
    assert len(cancelled) == 1 and cancelled[0]["bytes_written"] == 2
    assert "PRIVATE_INPUT_VALUE" not in log.read_text()

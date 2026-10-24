"""AuditLog 的线程、文件安全与 JSONL 集成测试。"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import stat
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from mcp_terminal.audit import AuditLog
from mcp_terminal.errors import SessionError


def _modes(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _process_writer(path: str, prefix: str, ready: Any, start: Any, result: Any) -> None:
    """Spawn-safe target used to exercise flock and rotation across processes."""

    async def write() -> None:
        logger = AuditLog(path, max_bytes=300, backup_count=8, timeout=2)
        await logger.start()
        ready.set()
        start.wait(5)
        for number in range(12):
            await logger.record({"event": "process", "writer": prefix, "number": number})
            await asyncio.sleep(0.002)
        await logger.close()

    try:
        asyncio.run(write())
    except BaseException as exc:  # pragma: no cover - sent through the child result queue
        result.put(f"{type(exc).__name__}: {exc}")
    else:
        result.put(None)


def assert_unavailable(caught: pytest.ExceptionInfo[SessionError]) -> None:
    error = caught.value
    assert error.code == "AUDIT_UNAVAILABLE"
    assert error.message == "Audit logging is unavailable."
    assert error.details == {}


async def test_record_lazy_start_adds_only_audit_envelope_and_uses_secure_files(
    tmp_path: Path,
) -> None:
    target = tmp_path / "private-audit" / "sessions.jsonl"
    logger = AuditLog(target)

    assert logger.path.is_absolute()
    assert logger.error is None and logger.instance_id
    await logger.record({"event": "session_created", "session_id": "one", "secret": "provided"})
    await logger.close()
    await logger.close()

    records = _read_records(target)
    assert len(records) == 1
    record = records[0]
    assert record == {
        "event": "session_created",
        "session_id": "one",
        "secret": "provided",
        "schema_version": 1,
        "server_instance_id": logger.instance_id,
        "event_seq": 1,
    }
    assert _modes(target.parent) == 0o700
    assert _modes(target) == _modes(target.with_name("sessions.jsonl.lock")) == 0o600


async def test_parallel_records_are_acked_once_with_contiguous_instance_sequences(
    tmp_path: Path,
) -> None:
    target = tmp_path / "logs" / "events.jsonl"
    logger = AuditLog(target, queue_size=128)
    await logger.start()

    await asyncio.gather(
        *(logger.record({"event": "input", "number": number}) for number in range(60))
    )
    await logger.close()

    records = _read_records(target)
    assert len(records) == 60
    assert {entry["server_instance_id"] for entry in records} == {logger.instance_id}
    assert sorted(entry["event_seq"] for entry in records) == list(range(1, 61))
    assert {entry["number"] for entry in records} == set(range(60))


async def test_emit_nowait_never_does_io_and_consumes_writer_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logger = AuditLog(tmp_path / "logs" / "events.jsonl")
    await logger.start()

    def fail_write(self: AuditLog, line: bytes) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(AuditLog, "_write_line", fail_write)
    assert logger.emit_nowait({"event": "expired"}) is True
    async with asyncio.timeout(2):
        while logger.error is None:
            await asyncio.sleep(0.005)
    assert logger.error == "AUDIT_UNAVAILABLE"
    await logger.close()
    assert logger._worker is not None and not logger._worker.is_alive()


async def test_ack_timeout_is_observable_and_close_reclaims_slow_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = threading.Event()
    release = threading.Event()
    original = AuditLog._write_line

    def slow_write(self: AuditLog, line: bytes) -> None:
        entered.set()
        release.wait(2)
        original(self, line)

    monkeypatch.setattr(AuditLog, "_write_line", slow_write)
    logger = AuditLog(tmp_path / "logs" / "events.jsonl", timeout=0.03)
    task = asyncio.create_task(logger.record({"event": "slow"}))
    assert await asyncio.to_thread(entered.wait, 1)

    with pytest.raises(SessionError) as caught:
        await task
    assert_unavailable(caught)
    assert logger.error == "AUDIT_UNAVAILABLE"

    release.set()
    async with asyncio.timeout(2):
        await logger.close()
    assert logger._worker is not None and not logger._worker.is_alive()


async def test_nowait_queue_full_fails_closed_and_does_not_leave_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = threading.Event()
    release = threading.Event()
    original = AuditLog._write_line

    def slow_write(self: AuditLog, line: bytes) -> None:
        entered.set()
        release.wait(2)
        original(self, line)

    monkeypatch.setattr(AuditLog, "_write_line", slow_write)
    logger = AuditLog(tmp_path / "logs" / "events.jsonl", queue_size=1, timeout=0.1)
    await logger.start()
    assert logger.emit_nowait({"event": "first"}) is True
    assert await asyncio.to_thread(entered.wait, 1)
    assert logger.emit_nowait({"event": "second"}) is True
    assert logger.emit_nowait({"event": "third"}) is False
    assert logger.error == "AUDIT_UNAVAILABLE"

    release.set()
    await logger.close()
    assert logger._worker is not None and not logger._worker.is_alive()
    with pytest.raises(SessionError) as caught:
        await logger.record({"event": "later"})
    assert_unavailable(caught)


@pytest.mark.parametrize("target_kind", ["log_symlink", "lock_symlink", "weak_log"])
async def test_start_rejects_unsafe_existing_files_without_leaking_path(
    tmp_path: Path, target_kind: str
) -> None:
    directory = tmp_path / "logs"
    directory.mkdir(mode=0o700)
    os.chmod(directory, 0o700)
    target = directory / "events.jsonl"
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    os.chmod(outside, 0o600)

    if target_kind == "log_symlink":
        target.symlink_to(outside)
    elif target_kind == "lock_symlink":
        target.write_text("", encoding="utf-8")
        os.chmod(target, 0o600)
        target.with_name("events.jsonl.lock").symlink_to(outside)
    else:
        target.write_text("", encoding="utf-8")
        os.chmod(target, 0o644)

    logger = AuditLog(target)
    with pytest.raises(SessionError) as caught:
        await logger.start()
    assert_unavailable(caught)
    assert logger.error == "AUDIT_UNAVAILABLE"
    assert str(target) not in caught.value.message
    assert outside.read_text(encoding="utf-8") == "outside"
    await logger.close()


async def test_rotation_retains_secure_parseable_backups(tmp_path: Path) -> None:
    target = tmp_path / "logs" / "events.jsonl"
    logger = AuditLog(target, max_bytes=180, backup_count=3)
    for number in range(10):
        await logger.record({"event": "rotation", "number": number, "padding": "x" * 35})
    await logger.close()

    files = [target, *(target.with_name(f"events.jsonl.{number}") for number in range(1, 4))]
    existing = [path for path in files if path.exists()]
    assert len(existing) == 4
    assert all(_modes(path) == 0o600 for path in existing)
    entries = [entry for path in existing for entry in _read_records(path)]
    assert entries
    assert all(entry["schema_version"] == 1 for entry in entries)
    assert max(entry["event_seq"] for entry in entries) == 10


def test_two_processes_serialize_rotation_and_leave_jsonl_parseable(tmp_path: Path) -> None:
    """The lock file must protect rotation as well as individual O_APPEND writes."""

    directory = tmp_path / "logs"
    directory.mkdir(mode=0o700)
    os.chmod(directory, 0o700)
    target = directory / "shared.jsonl"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    start = context.Event()
    result: Any = context.Queue()
    processes = [
        context.Process(target=_process_writer, args=(str(target), name, ready, start, result))
        for name in ("left", "right")
    ]
    for process in processes:
        process.start()
    try:
        assert ready.wait(10)
        # The first process signals readiness; giving both a brief opportunity to
        # prepare makes this a real overlapping write/rotation exercise.
        time.sleep(0.1)
        start.set()
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        assert [result.get(timeout=2) for _ in processes] == [None, None]
    finally:
        start.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)

    candidates = [target, *(target.with_name(f"shared.jsonl.{number}") for number in range(1, 9))]
    lines = [line for path in candidates if path.exists() for line in path.read_text().splitlines()]
    assert lines
    entries = [json.loads(line) for line in lines]
    assert all(entry["event"] == "process" for entry in entries)
    assert all(_modes(path) == 0o600 for path in candidates if path.exists())

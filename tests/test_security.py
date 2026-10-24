"""安全阶段的 SessionManager 回归测试。

这些测试仅使用公开的 SessionConfig/SessionManager API；对审计不可用场景，
仅替换 AuditLog.record 的行为，以验证会话层在边界处的 fail-closed 策略。
"""

from __future__ import annotations

import asyncio
import json
import os
import pwd
import shlex
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from mcp_terminal.errors import SessionError
from mcp_terminal.sessions import SessionConfig, SessionManager

POLL_INTERVAL = 0.01
TERMINAL_STATUSES = {"exited", "terminated", "failed"}


def python_command(source: str) -> str:
    """Build an absolute, shell-free Python command suitable for a PTY session."""
    return shlex.join([sys.executable, "-u", "-c", source])


def security_config(tmp_path: Path, **overrides: Any) -> SessionConfig:
    """Use a short-lived, explicitly rooted manager and a test-private audit log."""
    options: dict[str, Any] = {
        "buffer_chars": 4096,
        "max_sessions": 8,
        "retention_seconds": 0.20,
        "cleanup_interval": 0.01,
        "write_timeout": 0.20,
        "start_timeout": 2,
        "terminate_grace": 0.10,
        "drain_timeout": 0.25,
        "max_input_bytes": 65536,
        "allowed_cwd_roots": (str(tmp_path),),
        "allowed_env_keys": (),
        "audit_log_path": str(tmp_path / "logs" / "sessions.jsonl"),
        "audit_max_bytes": 1024 * 1024,
        "audit_backup_count": 1,
        "audit_queue_size": 64,
        "audit_timeout": 0.25,
    }
    options.update(overrides)
    return SessionConfig(**options)


async def wait_until(
    action: Callable[[], Awaitable[Any]], predicate: Callable[[Any], bool], *, timeout: float = 3.0
) -> Any:
    async with asyncio.timeout(timeout):
        while True:
            value = await action()
            if predicate(value):
                return value
            await asyncio.sleep(POLL_INTERVAL)


async def terminal_status(manager: SessionManager, session_id: str) -> dict[str, Any]:
    return await wait_until(
        lambda: manager.get_status(session_id),
        lambda status: status["status"] in TERMINAL_STATUSES and status["output_complete"],
    )


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


async def wait_for_event(path: Path, event: str, *, timeout: float = 3.0) -> list[dict[str, Any]]:
    return await wait_until(
        lambda: asyncio.sleep(0, result=read_jsonl(path)),
        lambda rows: any(row.get("event") == event for row in rows),
        timeout=timeout,
    )


def assert_session_error(caught: pytest.ExceptionInfo[SessionError], code: str) -> SessionError:
    error = caught.value
    assert error.code == code
    assert error.message
    assert isinstance(error.details, dict)
    return error


async def test_minimal_child_environment_is_fixed_and_never_inherits_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The parent process may be credentialed; the child starts from only three fixed keys."""
    monkeypatch.setenv("PATH", "/parent/path-that-must-not-be-inherited")
    monkeypatch.setenv("HOME", "/parent/home-that-must-not-be-inherited")
    monkeypatch.setenv("USER", "parent-user-that-must-not-be-inherited")
    monkeypatch.setenv("SECURITY_TEST_PARENT_TOKEN", "parent-token-that-must-not-be-inherited")
    keys = ["PATH", "TERM", "LANG", "HOME", "USER", "SECURITY_TEST_PARENT_TOKEN"]
    source = f"import json, os; print(json.dumps({{k: os.environ.get(k) for k in {keys!r}}}))"

    async with SessionManager(security_config(tmp_path)) as manager:
        created = await manager.start_session(python_command(source))
        await terminal_status(manager, created["session_id"])
        output = await manager.read_output(created["session_id"], cursor=0, max_chars=4096)

    child_env = json.loads(output["output"].strip())
    assert child_env == {
        "PATH": f"{Path(sys.executable).parent}{os.pathsep}{os.defpath}",
        "TERM": "xterm-256color",
        "LANG": "C.UTF-8",
        "HOME": None,
        "USER": None,
        "SECURITY_TEST_PARENT_TOKEN": None,
    }


async def test_environment_is_default_deny_then_explicitly_allowed_without_parent_leakage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rejected_value = "ENV_VALUE_MUST_NEVER_APPEAR"
    audit_path = tmp_path / "blocked.jsonl"
    blocked_config = security_config(tmp_path, audit_log_path=str(audit_path))
    async with SessionManager(blocked_config) as manager:
        with pytest.raises(SessionError) as caught:
            await manager.start_session(
                python_command("print('must not start')"),
                env={"APP_MODE": rejected_value},
            )
        error = assert_session_error(caught, "ENV_NOT_ALLOWED")
        assert error.details["keys"] == ["APP_MODE"]
        assert error.details["hint"]
        assert rejected_value not in str(error)
        assert rejected_value not in json.dumps(error.details)
        await wait_for_event(audit_path, "start_rejected")

    assert rejected_value not in audit_path.read_text(encoding="utf-8")

    # Even an explicitly permitted key must come from the MCP request, not os.environ.
    monkeypatch.setenv("APP_MODE", "parent-value-must-not-leak")
    allowed_config = security_config(
        tmp_path,
        allowed_env_keys=("APP_MODE",),
        audit_log_path=str(tmp_path / "allowed.jsonl"),
    )
    source = "import os; print(repr(os.environ.get('APP_MODE')), flush=True)"
    async with SessionManager(allowed_config) as manager:
        absent = await manager.start_session(python_command(source))
        permitted = await manager.start_session(
            python_command(source), env={"APP_MODE": "request-only-value"}
        )
        await terminal_status(manager, absent["session_id"])
        await terminal_status(manager, permitted["session_id"])
        absent_output = await manager.read_output(absent["session_id"], cursor=0, max_chars=4096)
        permitted_output = await manager.read_output(
            permitted["session_id"], cursor=0, max_chars=4096
        )

    assert absent_output["output"].strip() == "None"
    assert permitted_output["output"].strip() == "'request-only-value'"


async def test_cwd_policy_allows_canonical_root_and_subtree_but_rejects_escape_routes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "approved"
    nested = root / "nested"
    outside = tmp_path / "outside"
    same_prefix = tmp_path / "approved-looking-but-not-approved"
    root.mkdir()
    nested.mkdir()
    outside.mkdir()
    same_prefix.mkdir()
    outside_link = root / "outside-link"
    outside_link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError):
        security_config(tmp_path, allowed_cwd_roots=())

    config = security_config(tmp_path, allowed_cwd_roots=(str(root),))
    async with SessionManager(config) as manager:
        root_session = await manager.start_session(python_command("pass"), cwd=str(root))
        nested_session = await manager.start_session(python_command("pass"), cwd=str(nested))
        assert root_session["cwd"] == str(root.resolve())
        assert nested_session["cwd"] == str(nested.resolve())
        await terminal_status(manager, root_session["session_id"])
        await terminal_status(manager, nested_session["session_id"])

        escapes = (
            str(outside),
            str(root / ".." / outside.name),
            str(same_prefix),
            str(outside_link),
        )
        for candidate in escapes:
            with pytest.raises(SessionError) as caught:
                await manager.start_session(python_command("pass"), cwd=candidate)
            assert_session_error(caught, "CWD_NOT_ALLOWED")


async def test_default_cwd_root_is_manager_start_directory_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager_cwd = tmp_path / "manager-cwd"
    descendant = manager_cwd / "descendant"
    outside = tmp_path / "other-directory"
    manager_cwd.mkdir()
    descendant.mkdir()
    outside.mkdir()
    monkeypatch.chdir(manager_cwd)

    config = security_config(
        tmp_path,
        allowed_cwd_roots=None,
        audit_log_path=str(tmp_path / "default-root.jsonl"),
    )
    async with SessionManager(config) as manager:
        created = await manager.start_session(python_command("pass"), cwd=str(descendant))
        assert created["cwd"] == str(descendant.resolve())
        await terminal_status(manager, created["session_id"])
        with pytest.raises(SessionError) as caught:
            await manager.start_session(python_command("pass"), cwd=str(outside))
        assert_session_error(caught, "CWD_NOT_ALLOWED")


async def test_os_identity_metadata_comes_from_effective_ids_not_user_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("USER", "forged-parent-user")
    source = "import os; print(os.environ['USER'], flush=True)"
    config = security_config(tmp_path, allowed_env_keys=("USER",))
    async with SessionManager(config) as manager:
        created = await manager.start_session(
            python_command(source), env={"USER": "forged-request-user"}
        )
        status = await terminal_status(manager, created["session_id"])
        output = await manager.read_output(created["session_id"], cursor=0, max_chars=4096)

    assert output["output"].strip() == "forged-request-user"
    expected_uid = os.geteuid()
    assert status["uid"] == expected_uid
    assert status["gid"] == os.getegid()
    assert status["user"] == pwd.getpwuid(expected_uid).pw_name
    assert status["user"] not in {"forged-parent-user", "forged-request-user"}


async def test_audit_lifecycle_records_fixed_terminal_duration_and_expiry(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "lifecycle.jsonl"
    config = security_config(
        tmp_path,
        audit_log_path=str(audit_path),
        retention_seconds=0.08,
        cleanup_interval=0.01,
    )
    async with SessionManager(config) as manager:
        created = await manager.start_session(
            python_command("import sys, time; time.sleep(0.06); sys.exit(7)")
        )
        session_id = created["session_id"]
        first_running = await manager.get_status(session_id)
        await asyncio.sleep(0.02)
        second_running = await manager.get_status(session_id)
        assert first_running["status"] == second_running["status"] == "running"
        assert second_running["duration_seconds"] >= first_running["duration_seconds"]

        terminal = await terminal_status(manager, session_id)
        repeated = await manager.get_status(session_id)
        assert terminal["status"] == "exited"
        assert terminal["exit_code"] == 7
        assert terminal["duration_seconds"] == repeated["duration_seconds"]
        assert terminal["duration_seconds"] >= second_running["duration_seconds"]

        rows = await wait_for_event(audit_path, "session_finished")
        finished = next(row for row in rows if row.get("event") == "session_finished")
        assert finished["session_id"] == session_id
        assert finished["status"] == "exited"
        assert finished["exit_code"] == 7
        assert finished["duration_seconds"] == terminal["duration_seconds"]

        async def expired() -> bool:
            try:
                await manager.get_status(session_id)
            except SessionError as error:
                assert error.code == "SESSION_NOT_FOUND"
                return True
            return False

        assert await wait_until(expired, bool, timeout=2) is True
        await wait_for_event(audit_path, "session_expired")

    events = [row["event"] for row in read_jsonl(audit_path)]
    assert "start_requested" in events
    assert "session_created" in events
    assert "session_finished" in events
    assert "session_expired" in events


async def test_audit_never_records_input_output_or_environment_values(tmp_path: Path) -> None:
    audit_path = tmp_path / "redaction.jsonl"
    env_secret = "ENV_SECRET_NOT_IN_AUDIT"
    input_secret = "INPUT_SECRET_NOT_IN_AUDIT"
    config = security_config(
        tmp_path,
        audit_log_path=str(audit_path),
        allowed_env_keys=("APP_TOKEN",),
    )
    async with SessionManager(config) as manager:
        emitted = await manager.start_session(
            python_command("import os; print(os.environ['APP_TOKEN'], flush=True)"),
            env={"APP_TOKEN": env_secret},
        )
        echoed = await manager.start_session(python_command("import sys; sys.stdin.readline()"))
        await manager.write_input(echoed["session_id"], input_secret, append_newline=True)
        await terminal_status(manager, emitted["session_id"])
        await terminal_status(manager, echoed["session_id"])
        emitted_output = await manager.read_output(emitted["session_id"], cursor=0, max_chars=4096)
        echoed_output = await manager.read_output(echoed["session_id"], cursor=0, max_chars=4096)

    assert env_secret in emitted_output["output"]
    assert input_secret in echoed_output["output"]  # PTY echo proves it was actually sent.
    audit_text = audit_path.read_text(encoding="utf-8")
    assert env_secret not in audit_text
    assert input_secret not in audit_text


async def test_audit_failures_block_create_and_input_but_not_termination_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only pre-side-effect operations fail closed; termination still reaps its process."""
    import mcp_terminal.audit as audit_module

    original_record = audit_module.AuditLog.record
    audit_path = tmp_path / "injected-failure.jsonl"
    marker = tmp_path / "must-not-exist"
    config = security_config(tmp_path, audit_log_path=str(audit_path))

    async with SessionManager(config) as manager:

        async def reject_create(self: Any, event: dict[str, Any]) -> None:
            if event.get("event") == "start_requested":
                raise SessionError("AUDIT_UNAVAILABLE", "injected audit failure")
            await original_record(self, event)

        monkeypatch.setattr(audit_module.AuditLog, "record", reject_create)
        marker_source = f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')"
        with pytest.raises(SessionError) as caught:
            await manager.start_session(python_command(marker_source))
        assert_session_error(caught, "AUDIT_UNAVAILABLE")
        assert not marker.exists()

        monkeypatch.setattr(audit_module.AuditLog, "record", original_record)
        reader_source = (
            "import sys, time; sys.stdin.readline(); print('INPUT_RAN', flush=True); time.sleep(30)"
        )
        reader = await manager.start_session(python_command(reader_source))
        session_id = reader["session_id"]

        async def reject_input(self: Any, event: dict[str, Any]) -> None:
            if event.get("event") == "input_requested":
                raise SessionError("AUDIT_UNAVAILABLE", "injected audit failure")
            await original_record(self, event)

        monkeypatch.setattr(audit_module.AuditLog, "record", reject_input)
        with pytest.raises(SessionError) as caught:
            await manager.write_input(session_id, "MUST_NOT_REACH_PTY", append_newline=True)
        assert_session_error(caught, "AUDIT_UNAVAILABLE")
        await asyncio.sleep(0.03)
        before_termination = await manager.read_output(session_id, cursor=0, max_chars=4096)
        assert "INPUT_RAN" not in before_termination["output"]

        async def reject_termination(self: Any, event: dict[str, Any]) -> None:
            if event.get("event") == "terminate_requested":
                raise SessionError("AUDIT_UNAVAILABLE", "injected audit failure")
            await original_record(self, event)

        monkeypatch.setattr(audit_module.AuditLog, "record", reject_termination)
        terminated = await manager.terminate_session(session_id)
        assert terminated["status"] == "terminated"
        assert terminated["output_complete"] is True
        assert terminated["audit_error"] == "AUDIT_UNAVAILABLE"
        with pytest.raises(ProcessLookupError):
            os.kill(terminated["pid"], 0)

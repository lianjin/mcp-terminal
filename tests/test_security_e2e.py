"""通过真实 stdio MCP 连接验证安全配置，而非直接调用服务端对象。"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import signal
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from mcp import Client, StdioServerParameters
from mcp.types import TextContent

TOOLS = {"ping", "start_session", "write_input", "read_output", "get_status", "terminate_session"}
TERMINAL_STATUSES = {"exited", "terminated", "failed"}


def python_command(source: str) -> str:
    return shlex.join([sys.executable, "-u", "-c", source])


def stdio_config(root: Path, audit_path: Path, **overrides: Any) -> dict[str, Any]:
    """JSON-serializable SessionConfig suitable for tests/stdio_test_server.py."""
    options: dict[str, Any] = {
        "buffer_chars": 4096,
        "max_sessions": 8,
        "retention_seconds": 0.25,
        "cleanup_interval": 0.01,
        "write_timeout": 0.20,
        "start_timeout": 2,
        "terminate_grace": 0.10,
        "drain_timeout": 0.25,
        "max_input_bytes": 65536,
        "allowed_cwd_roots": [str(root)],
        "allowed_env_keys": ["APP_FLAG"],
        "audit_log_path": str(audit_path),
        "audit_max_bytes": 1024 * 1024,
        "audit_backup_count": 1,
        "audit_queue_size": 64,
        "audit_timeout": 0.25,
    }
    options.update(overrides)
    return options


class Driver:
    """Small client-side assertion helper; it never reaches server internals."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.pids: set[int] = set()

    async def request(
        self, tool: str, arguments: dict[str, Any], error: str | None = None
    ) -> dict[str, Any]:
        result = await self.client.call_tool(tool, arguments)
        envelope = result.structured_content
        assert isinstance(envelope, dict) and set(envelope) == {"ok", "data", "error"}
        text = [part for part in result.content if isinstance(part, TextContent)]
        assert len(text) == 1 and json.loads(text[0].text) == envelope
        if error is not None:
            assert result.is_error and envelope["ok"] is False
            assert envelope["data"] is None
            assert envelope["error"]["code"] == error
            assert isinstance(envelope["error"]["details"], dict)
            return envelope["error"]
        assert not result.is_error and envelope["ok"] is True, envelope
        assert envelope["error"] is None and isinstance(envelope["data"], dict)
        return envelope["data"]

    async def start(self, command: str, **arguments: Any) -> dict[str, Any]:
        created = await self.request("start_session", {"command": command, **arguments})
        assert created["status"] == "running"
        self.pids.add(created["pid"])
        return created

    async def status(self, session_id: str) -> dict[str, Any]:
        return await self.request("get_status", {"session_id": session_id})

    async def finished(self, session_id: str) -> dict[str, Any]:
        async with asyncio.timeout(5):
            while True:
                status = await self.status(session_id)
                if status["status"] in TERMINAL_STATUSES and status["output_complete"]:
                    return status
                await asyncio.sleep(0.01)

    async def output(self, session_id: str) -> str:
        result = await self.request(
            "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 65536}
        )
        return result["output"]


@asynccontextmanager
async def connected(cwd: Path, config: dict[str, Any]) -> AsyncIterator[Driver]:
    """Start the actual production server through the existing stdio test launcher."""
    launcher = Path(__file__).with_name("stdio_test_server.py")
    params = StdioServerParameters(
        command=sys.executable,
        args=[str(launcher), json.dumps(config)],
        cwd=str(cwd),
    )
    driver: Driver | None = None
    try:
        async with asyncio.timeout(20):
            async with Client(params, read_timeout_seconds=5) as client:
                assert client.server_info is not None
                assert client.server_info.name == "mcp-terminal"
                assert {tool.name for tool in (await client.list_tools()).tools} == TOOLS
                driver = Driver(client)
                yield driver
    finally:
        if driver is not None:
            # Client exit invokes server lifespan; fallback only handles failed tests.
            try:
                async with asyncio.timeout(4):
                    while driver.pids:
                        pid = driver.pids.pop()
                        while True:
                            try:
                                os.kill(pid, 0)
                            except ProcessLookupError:
                                break
                            await asyncio.sleep(0.01)
            except TimeoutError:
                for pid in driver.pids:
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                raise AssertionError("stdio security test left a session process running") from None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


async def test_stdio_security_success_uses_approved_env_and_writes_lifecycle_log(
    tmp_path: Path,
) -> None:
    root = tmp_path / "approved-root"
    nested = root / "nested"
    root.mkdir()
    nested.mkdir()
    audit_path = tmp_path / "logs" / "sessions.jsonl"
    config = stdio_config(root, audit_path)
    source = (
        "import os; print(os.environ['APP_FLAG'], flush=True); print(os.getenv('HOME'), flush=True)"
    )

    async with connected(root, config) as driver:
        created = await driver.start(
            python_command(source), cwd=str(nested), env={"APP_FLAG": "stdio-approved-value"}
        )
        finished = await driver.finished(created["session_id"])
        output = await driver.output(created["session_id"])
        assert finished["status"] == "exited"
        assert finished["exit_code"] == 0
        assert finished["cwd"] == str(nested.resolve())
        assert "stdio-approved-value" in output
        assert "None" in output.replace("\r\n", "\n").splitlines()
        # A terminal duration is a snapshot, not a query-time counter.
        assert (
            finished["duration_seconds"]
            == (await driver.status(created["session_id"]))["duration_seconds"]
        )

    rows = read_jsonl(audit_path)
    events = [row["event"] for row in rows]
    assert "start_requested" in events
    assert "session_created" in events
    assert "session_finished" in events
    created_log = next(row for row in rows if row.get("event") == "session_created")
    finished_log = next(row for row in rows if row.get("event") == "session_finished")
    assert created_log["session_id"] == finished_log["session_id"] == created["session_id"]
    assert finished_log["exit_code"] == 0
    assert finished_log["duration_seconds"] == finished["duration_seconds"]
    assert "stdio-approved-value" not in audit_path.read_text(encoding="utf-8")


async def test_stdio_security_rejects_forbidden_environment_and_cwd_without_leaking_value(
    tmp_path: Path,
) -> None:
    root = tmp_path / "approved-root"
    outside = tmp_path / "outside-root"
    root.mkdir()
    outside.mkdir()
    audit_path = tmp_path / "rejections.jsonl"
    rejected_value = "STDIO_ENV_SECRET_MUST_NOT_APPEAR"

    async with connected(root, stdio_config(root, audit_path)) as driver:
        env_error = await driver.request(
            "start_session",
            {"command": python_command("pass"), "env": {"UNAPPROVED_TOKEN": rejected_value}},
            error="ENV_NOT_ALLOWED",
        )
        assert env_error["details"]["keys"] == ["UNAPPROVED_TOKEN"]
        assert env_error["details"]["hint"]
        assert rejected_value not in json.dumps(env_error)
        await driver.request(
            "start_session",
            {"command": python_command("pass"), "cwd": str(outside)},
            error="CWD_NOT_ALLOWED",
        )
        # Errors must not poison the same real MCP connection.
        assert (await driver.request("ping", {}))["message"] == "pong"

    audit_text = audit_path.read_text(encoding="utf-8")
    assert rejected_value not in audit_text
    rows = read_jsonl(audit_path)
    rejected = [row for row in rows if row.get("event") == "start_rejected"]
    assert len(rejected) >= 2
    assert {row.get("error_code") for row in rejected} >= {"ENV_NOT_ALLOWED", "CWD_NOT_ALLOWED"}


async def test_stdio_security_parallel_sessions_receive_distinct_audited_lifecycles(
    tmp_path: Path,
) -> None:
    root = tmp_path / "approved-root"
    root.mkdir()
    audit_path = tmp_path / "parallel.jsonl"
    config = stdio_config(root, audit_path)

    async with connected(root, config) as driver:
        first_source = "import time; print('FIRST', flush=True); time.sleep(0.04)"
        second_source = "import time; print('SECOND', flush=True); time.sleep(0.04)"
        first, second = await asyncio.gather(
            driver.start(python_command(first_source)),
            driver.start(python_command(second_source)),
        )
        assert first["session_id"] != second["session_id"]
        first_status, second_status = await asyncio.gather(
            driver.finished(first["session_id"]), driver.finished(second["session_id"])
        )
        first_output, second_output = await asyncio.gather(
            driver.output(first["session_id"]), driver.output(second["session_id"])
        )
        assert first_status["status"] == second_status["status"] == "exited"
        assert "FIRST" in first_output and "SECOND" not in first_output
        assert "SECOND" in second_output and "FIRST" not in second_output

    rows = read_jsonl(audit_path)
    ids = {first["session_id"], second["session_id"]}
    for session_id in ids:
        session_rows = [row for row in rows if row.get("session_id") == session_id]
        events = {row["event"] for row in session_rows}
        assert {"start_requested", "session_created", "session_finished"} <= events
        terminal = next(row for row in session_rows if row["event"] == "session_finished")
        assert terminal["status"] == "exited"
        assert terminal["exit_code"] == 0

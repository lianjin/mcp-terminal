"""MCP 对外协议的统一响应、校验错误与真实 stdio 链路测试。"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

from mcp import Client, StdioServerParameters
from mcp.types import TextContent

from mcp_terminal.server import create_server
from mcp_terminal.sessions import SessionConfig

EXPECTED_TOOLS = {
    "ping",
    "start_session",
    "write_input",
    "read_output",
    "get_status",
    "terminate_session",
}


def python_command(source: str) -> str:
    return shlex.join([sys.executable, "-u", "-c", source])


def assert_envelope(result: Any, *, ok: bool, code: str | None = None) -> dict[str, Any]:
    """验证工具结果的 structured_content 和唯一文本 JSON 严格一致。"""
    envelope = result.structured_content
    assert isinstance(envelope, dict)
    assert set(envelope) == {"ok", "data", "error"}
    assert envelope["ok"] is ok
    assert result.is_error is (not ok)

    text = [block for block in result.content if isinstance(block, TextContent)]
    assert len(text) == 1
    assert json.loads(text[0].text) == envelope

    if ok:
        assert isinstance(envelope["data"], dict)
        assert envelope["error"] is None
    else:
        assert envelope["data"] is None
        assert isinstance(envelope["error"], dict)
        assert set(envelope["error"]) == {"code", "message", "details"}
        assert isinstance(envelope["error"]["code"], str)
        assert envelope["error"]["code"]
        if code is not None:
            assert envelope["error"]["code"] == code
        assert isinstance(envelope["error"]["message"], str)
        assert envelope["error"]["message"]
        assert isinstance(envelope["error"]["details"], dict)
    return envelope


async def wait_for_output(
    client: Client, session_id: str, expected: str, *, timeout: float = 3.0
) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        while True:
            result = await client.call_tool(
                "read_output", {"session_id": session_id, "cursor": 0, "max_chars": 65536}
            )
            data = assert_envelope(result, ok=True)["data"]
            if expected in data["output"].replace("\r\n", "\n").splitlines():
                return data
            await asyncio.sleep(0.01)


async def wait_for_process_exit(pid: int, *, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.01)


def small_config() -> SessionConfig:
    return SessionConfig(
        buffer_chars=1024,
        max_sessions=4,
        retention_seconds=2,
        cleanup_interval=0.02,
        write_timeout=0.2,
        start_timeout=2,
        terminate_grace=0.1,
        drain_timeout=0.25,
        max_input_bytes=65536,
    )


async def test_tool_discovery_advertises_all_tools_and_envelope_output_schema() -> None:
    async with Client(create_server(small_config())) as client:
        tools = (await client.list_tools()).tools

    assert {tool.name for tool in tools} == EXPECTED_TOOLS
    assert len(tools) == len(EXPECTED_TOOLS)
    for tool in tools:
        assert tool.description
        assert tool.input_schema["type"] == "object"
        assert tool.output_schema is not None
        assert {"ok", "data", "error"} <= set(tool.output_schema["properties"])

    ping = next(tool for tool in tools if tool.name == "ping")
    assert ping.input_schema.get("properties", {}) == {}
    assert not ping.input_schema.get("required")
    assert ping.annotations is not None
    assert ping.annotations.read_only_hint is True
    assert ping.annotations.open_world_hint is False


async def test_validation_extra_and_unknown_tool_errors_are_enveloped_and_recoverable() -> None:
    async with Client(create_server(small_config())) as client:
        # 缺参数、类型错误和额外参数均是 tools/call 层面的统一业务错误，而非连接中断。
        missing = await client.call_tool("start_session", {})
        assert_envelope(missing, ok=False, code="INVALID_ARGUMENT")

        wrong_type = await client.call_tool("read_output", {"session_id": 12})
        assert_envelope(wrong_type, ok=False, code="INVALID_ARGUMENT")

        extra = await client.call_tool(
            "start_session", {"command": python_command("pass"), "surplus": "forbidden"}
        )
        assert_envelope(extra, ok=False, code="INVALID_ARGUMENT")

        unknown = await client.call_tool("not_a_real_terminal_tool", {})
        assert_envelope(unknown, ok=False)

        recovered = await client.call_tool("ping", {})
        assert_envelope(recovered, ok=True)
        assert recovered.structured_content == {
            "ok": True,
            "data": {"status": "ok", "message": "pong"},
            "error": None,
        }


async def test_in_process_tools_preserve_envelope_for_success_and_session_errors() -> None:
    server = create_server(small_config())
    async with Client(server) as client:
        started = await client.call_tool(
            "start_session", {"command": python_command("import time; time.sleep(30)")}
        )
        session = assert_envelope(started, ok=True)["data"]
        session_id = session["session_id"]
        assert session["status"] == "running"
        assert isinstance(session["pid"], int)

        empty_input = await client.call_tool("write_input", {"session_id": session_id, "input": ""})
        assert_envelope(empty_input, ok=False, code="INVALID_ARGUMENT")

        not_found = await client.call_tool("get_status", {"session_id": "not-an-issued-id"})
        assert_envelope(not_found, ok=False, code="SESSION_NOT_FOUND")

        terminated = await client.call_tool("terminate_session", {"session_id": session_id})
        terminated_data = assert_envelope(terminated, ok=True)["data"]
        assert terminated_data["session_id"] == session_id
        assert terminated_data["status"] == "terminated"

        again = await client.call_tool("terminate_session", {"session_id": session_id})
        assert assert_envelope(again, ok=True)["data"]["status"] == "terminated"


async def test_real_stdio_chain_works_from_other_cwd_and_shutdown_reaps_live_pty(
    tmp_path: Path,
) -> None:
    """验证模块入口、stdio JSON-RPC、六个工具及服务关闭时的 manager.close。"""
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_terminal"],
        cwd=str(tmp_path),
    )
    pid: int | None = None
    try:
        async with asyncio.timeout(20):
            async with Client(server) as client:
                assert client.server_info is not None
                assert client.server_info.name == "mcp-terminal"
                assert client.protocol_version
                assert {tool.name for tool in (await client.list_tools()).tools} == EXPECTED_TOOLS

                pong = await client.call_tool("ping", {})
                assert_envelope(pong, ok=True)

                repl = "import sys\nfor line in sys.stdin:\n    exec(line, globals())\n"
                started = await client.call_tool("start_session", {"command": python_command(repl)})
                session = assert_envelope(started, ok=True)["data"]
                session_id = session["session_id"]
                pid = session["pid"]

                wrote = await client.call_tool(
                    "write_input", {"session_id": session_id, "input": "value = 'stdio'\n"}
                )
                wrote_data = assert_envelope(wrote, ok=True)["data"]
                assert wrote_data["bytes_written"] == len(b"value = 'stdio'\n")

                wrote = await client.call_tool(
                    "write_input", {"session_id": session_id, "input": "print(value, flush=True)\n"}
                )
                assert_envelope(wrote, ok=True)
                output = await wait_for_output(client, session_id, "stdio")
                assert output["session_id"] == session_id
                assert output["output_complete"] is False

                status = await client.call_tool("get_status", {"session_id": session_id})
                status_data = assert_envelope(status, ok=True)["data"]
                assert status_data["status"] == "running"
                assert status_data["buffered_chars"] >= len("stdio")

                # 故意不终止该会话；Client/stdio server 正常退出必须进入 lifespan cleanup。
    finally:
        if pid is not None:
            try:
                await wait_for_process_exit(pid)
            finally:
                try:
                    os.kill(pid, 9)
                except ProcessLookupError:
                    pass

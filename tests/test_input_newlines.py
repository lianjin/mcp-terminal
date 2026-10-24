"""Inspector 字符串字段与真实换行的专用回归测试。"""

import asyncio
import shlex
import sys

import pytest
from mcp import Client

from mcp_terminal.errors import SessionError
from mcp_terminal.server import create_server
from mcp_terminal.sessions import SessionConfig, SessionManager


async def read_until(client, sid, predicate):
    async with asyncio.timeout(3):
        while True:
            result = await client.call_tool("read_output", {"session_id": sid, "cursor": 0})
            assert not result.is_error
            text = result.structured_content["data"]["output"]
            if predicate(text):
                return text
            await asyncio.sleep(0.005)


async def start_repl(client):
    result = await client.call_tool(
        "start_session", {"command": shlex.join([sys.executable, "-i", "-q"])}
    )
    assert not result.is_error
    return result.structured_content["data"]["session_id"]


async def test_literal_backslash_n_reproduces_inspector_problem_without_execution():
    async with Client(create_server()) as client:
        sid = await start_repl(client)
        for text, count in (("x = 100\\n", 9), ("print(x * 2)\\n", 14)):
            result = await client.call_tool("write_input", {"session_id": sid, "input": text})
            assert result.structured_content["data"]["bytes_written"] == count
        text = await read_until(client, sid, lambda text: "print(x * 2)\\n" in text)
        assert "x = 100\\nprint(x * 2)\\n" in text
        assert "\n" not in text
        assert "200" not in text
        assert (await client.call_tool("get_status", {"session_id": sid})).structured_content[
            "data"
        ]["status"] == "running"


async def test_append_newline_executes_exact_acceptance_case():
    async with Client(create_server()) as client:
        sid = await start_repl(client)
        for text, count in (("x = 100", 8), ("print(x * 2)", 13)):
            result = await client.call_tool(
                "write_input", {"session_id": sid, "input": text, "append_newline": True}
            )
            assert not result.is_error
            assert result.structured_content["data"]["bytes_written"] == count
        text = await read_until(
            client, sid, lambda text: "200" in text.replace("\r\n", "\n").splitlines()
        )
        assert "200" in text.replace("\r\n", "\n").splitlines()
        ended = await client.call_tool("terminate_session", {"session_id": sid})
        assert ended.structured_content["data"]["status"] == "terminated"


async def test_append_newline_preserves_backslashes_inside_python_source():
    async with Client(create_server()) as client:
        sid = await start_repl(client)
        result = await client.call_tool(
            "write_input",
            {"session_id": sid, "input": "print(r'A\\nB')", "append_newline": True},
        )
        assert not result.is_error
        text = await read_until(
            client, sid, lambda text: "A\\nB" in text.replace("\r\n", "\n").splitlines()
        )
        assert "A\\nB" in text.replace("\r\n", "\n").splitlines()
        assert "SyntaxError" not in text


@pytest.mark.parametrize("flag", [1, "true", None])
async def test_append_newline_requires_real_boolean(flag):
    async with Client(create_server()) as client:
        result = await client.call_tool(
            "write_input", {"session_id": "unknown", "input": "x", "append_newline": flag}
        )
        assert result.is_error
        assert result.structured_content["error"]["code"] == "INVALID_ARGUMENT"


async def test_appended_newline_counts_toward_input_size_limit():
    async with SessionManager(SessionConfig(max_input_bytes=3)) as manager:
        created = await manager.start_session(
            shlex.join([sys.executable, "-c", "import time; time.sleep(30)"])
        )
        sid = created["session_id"]
        with pytest.raises(SessionError) as caught:
            await manager.write_input(sid, "abc", append_newline=True)
        assert caught.value.code == "INVALID_ARGUMENT"
        assert (await manager.write_input(sid, "ab", append_newline=True))["bytes_written"] == 3

"""ping 工具的统一 MCP envelope 契约测试。"""

import json

from mcp import Client
from mcp.types import TextContent

from mcp_terminal.server import mcp

EXPECTED = {
    "ok": True,
    "data": {"status": "ok", "message": "pong"},
    "error": None,
}


async def test_ping_returns_unified_envelope_in_structured_and_text_content() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("ping", {})

    assert result.is_error is False
    assert result.structured_content == EXPECTED
    text = [block for block in result.content if isinstance(block, TextContent)]
    assert len(text) == 1
    assert json.loads(text[0].text) == EXPECTED


async def test_ping_is_repeatable_without_state_or_side_effects() -> None:
    async with Client(mcp) as client:
        first = await client.call_tool("ping", {})
        second = await client.call_tool("ping", {})

    assert first.is_error is False
    assert second.is_error is False
    assert first.structured_content == second.structured_content == EXPECTED

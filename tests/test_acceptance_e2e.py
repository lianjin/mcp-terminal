"""六项验收：全部通过独立服务进程的真实 stdio JSON-RPC 调用，非进程内单元测试。"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import signal
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from mcp import Client, StdioServerParameters
from mcp.types import TextContent

TOOLS = {"ping", "start_session", "write_input", "read_output", "get_status", "terminate_session"}


def python_command(code: str) -> str:
    return shlex.join([sys.executable, "-u", "-c", code])


class Driver:
    """仅封装客户端请求和断言，不直接访问 SessionManager 或服务端内存。"""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.pids: list[int] = []

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

    async def start(self, command: str) -> dict[str, Any]:
        data = await self.request("start_session", {"command": command})
        assert data["status"] == "running"
        self.pids.append(data["pid"])
        return data

    async def status(self, sid: str) -> dict[str, Any]:
        return await self.request("get_status", {"session_id": sid})

    async def finished(self, sid: str) -> dict[str, Any]:
        async with asyncio.timeout(5):
            while True:
                state = await self.status(sid)
                if state["status"] != "running" and state["output_complete"]:
                    return state
                await asyncio.sleep(0.01)

    async def page(self, sid: str, cursor: int, size: int = 17) -> dict[str, Any]:
        page = await self.request(
            "read_output", {"session_id": sid, "cursor": cursor, "max_chars": size}
        )
        assert page["cursor"] == cursor
        assert page["next_cursor"] == cursor + len(page["output"])
        assert page["earliest_cursor"] <= cursor <= page["next_cursor"] <= page["latest_cursor"]
        assert page["has_more"] is (page["next_cursor"] < page["latest_cursor"])
        return page

    async def until_line(self, sid: str, line: str, cursor: int = 0) -> tuple[str, int]:
        captured = ""
        async with asyncio.timeout(5):
            while True:
                page = await self.page(sid, cursor, 11)
                captured += page["output"]
                cursor = page["next_cursor"]
                if line in captured.replace("\r\n", "\n").splitlines():
                    return captured, cursor
                if not page["has_more"]:
                    await asyncio.sleep(0.01)


@asynccontextmanager
async def connected(cwd: Path, options: dict[str, Any] | None = None) -> AsyncIterator[Driver]:
    # 常规case验证正式模块入口；只有小buffer case使用测试启动器注入配置。
    args = ["-m", "mcp_terminal"]
    if options is not None:
        args = [str(Path(__file__).with_name("stdio_test_server.py")), json.dumps(options)]
    params = StdioServerParameters(command=sys.executable, args=args, cwd=str(cwd))
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
            try:
                # Client正常断开应使服务执行lifespan清理并回收其所有会话主进程。
                async with asyncio.timeout(4):
                    for pid in driver.pids:
                        while True:
                            try:
                                os.kill(pid, 0)
                            except ProcessLookupError:
                                break
                            await asyncio.sleep(0.01)
            except TimeoutError:
                # 测试失败时也只清理本测试创建的进程组，不留下长任务。
                for pid in driver.pids:
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                raise AssertionError("stdio连接关闭后仍有会话主进程未回收") from None


async def test_e2e_long_running_output_cursors_are_contiguous_without_overlap(
    tmp_path: Path, record_property: Callable[[str, Any], None]
) -> None:
    expected = "".join(f"SEQ-{i:03d}:界:{i * 7}\r\n" for i in range(60))
    source = (
        "import os,time\n"
        "for i in range(60):\n"
        "    os.write(1,f'SEQ-{i:03d}:界:{i*7}\\n'.encode('utf-8'))\n"
        "    time.sleep(0.015)\n"
        "time.sleep(30)\n"
    )
    async with connected(tmp_path) as driver:
        created = await driver.start(python_command(source))
        sid = created["session_id"]
        cursor = 0
        captured = ""
        intervals: list[tuple[int, int]] = []
        running_pages = 0
        empty_reads = 0
        async with asyncio.timeout(8):
            while len(captured) < len(expected):
                page = await driver.page(sid, cursor, size=13)
                if page["output"]:
                    intervals.append((cursor, page["next_cursor"]))
                    running_pages += int(page["status"] == "running")
                else:
                    empty_reads += 1
                captured += page["output"]
                cursor = page["next_cursor"]
                if not page["has_more"]:
                    await asyncio.sleep(0.01)
        assert captured == expected  # 不仅校验数字，还逐字符证明无重复、无缺失、无乱码。
        assert cursor == len(expected) and running_pages > 10 and len(intervals) > 10
        assert intervals[0][0] == 0
        assert all(before[1] == after[0] for before, after in zip(intervals, intervals[1:]))
        assert sum(end - start for start, end in intervals) == len(expected)
        assert (await driver.status(sid))["status"] == "running"
        tail = await driver.page(sid, cursor)
        assert tail["output"] == "" and tail["next_cursor"] == cursor
        ended = await driver.request("terminate_session", {"session_id": sid})
        assert ended["status"] == "terminated" and ended["output_complete"]
        record_property("frames", 60)
        record_property("characters", len(expected))
        record_property("nonempty_pages", len(intervals))
        record_property("empty_reads", empty_reads)
        record_property("cursor_intervals_contiguous", True)


@pytest.mark.parametrize("exit_code", [0, 7])
async def test_e2e_exit_code_is_visible_in_status(
    tmp_path: Path, exit_code: int, record_property: Callable[[str, Any], None]
) -> None:
    code = (
        f"import os,sys; os.write(1,b'EXIT-{exit_code}\\n'); "
        f"os.write(2,b'STDERR-MERGED\\n'); sys.exit({exit_code})"
    )
    async with connected(tmp_path) as driver:
        created = await driver.start(python_command(code))
        sid = created["session_id"]
        status = await driver.finished(sid)
        assert status["status"] == "exited"
        assert status["exit_code"] == exit_code and status["reason"] == "process_exit"
        assert status["ended_at"] and status["expires_at"] and status["output_complete"]
        output = (await driver.page(sid, 0, 4096))["output"]
        assert f"EXIT-{exit_code}" in output.replace("\r\n", "\n").splitlines()
        assert "STDERR-MERGED" in output.replace("\r\n", "\n").splitlines()
        error = await driver.request(
            "write_input",
            {"session_id": sid, "input": "no", "append_newline": True},
            error="SESSION_NOT_RUNNING",
        )
        assert error["details"]["bytes_written"] == 0
        record_property("status", status["status"])
        record_property("exit_code", status["exit_code"])


async def test_e2e_parallel_sessions_survive_other_exit_and_termination(
    tmp_path: Path, record_property: Callable[[str, Any], None]
) -> None:
    repl = shlex.join([sys.executable, "-i", "-q"])
    async with connected(tmp_path) as driver:
        exited, first, second = await asyncio.gather(
            driver.start(python_command("import sys; print('OTHER-EXIT'); sys.exit(7)")),
            driver.start(repl),
            driver.start(repl),
        )
        ids = {item["session_id"] for item in (exited, first, second)}
        assert len(ids) == 3 and len(set(driver.pids)) == 3
        a, b = first["session_id"], second["session_id"]
        await asyncio.gather(
            driver.request(
                "write_input", {"session_id": a, "input": "x = 100", "append_newline": True}
            ),
            driver.request(
                "write_input", {"session_id": b, "input": "x = 7", "append_newline": True}
            ),
        )
        await asyncio.gather(
            *(
                driver.request(
                    "write_input",
                    {"session_id": sid, "input": "print(x * 2)", "append_newline": True},
                )
                for sid in (a, b)
            )
        )
        first_output, second_output = await asyncio.gather(
            driver.until_line(a, "200"), driver.until_line(b, "14")
        )
        assert "200" not in second_output[0].replace("\r\n", "\n").splitlines()
        assert "14" not in first_output[0].replace("\r\n", "\n").splitlines()
        failed_command = await driver.finished(exited["session_id"])
        assert failed_command["status"] == "exited" and failed_command["exit_code"] == 7
        assert (await driver.status(a))["status"] == (await driver.status(b))["status"] == "running"
        terminated = await driver.request("terminate_session", {"session_id": a})
        assert terminated["status"] == "terminated"
        assert (await driver.status(b))["status"] == "running"
        await driver.request(
            "write_input", {"session_id": b, "input": "print(x + 1)", "append_newline": True}
        )
        continuation, _ = await driver.until_line(b, "8", cursor=second_output[1])
        assert "8" in continuation.replace("\r\n", "\n").splitlines()
        await driver.request("terminate_session", {"session_id": b})
        record_property("parallel_sessions", 3)
        record_property("isolated_results", "200,14")
        record_property("surviving_session_result_after_other_termination", 8)


async def test_e2e_small_ringbuffer_evicts_old_data_and_cursor_recovers(
    tmp_path: Path, record_property: Callable[[str, Any], None]
) -> None:
    payload = "0123456789" * 8
    source = (
        f"import os,time; data={payload!r}.encode(); "
        "os.write(1,data[:32]); time.sleep(0.04); os.write(1,data[32:])"
    )
    async with connected(tmp_path, {"buffer_chars": 24}) as driver:
        created = await driver.start(python_command(source))
        sid = created["session_id"]
        status = await driver.finished(sid)
        assert status["capacity_chars"] == status["buffered_chars"] == 24
        assert status["earliest_cursor"] == status["dropped_chars"] == 56
        assert status["latest_cursor"] == 80
        error = await driver.request(
            "read_output", {"session_id": sid, "cursor": 0}, error="CURSOR_EXPIRED"
        )
        assert error["details"] == {"earliest_cursor": 56, "latest_cursor": 80}
        await driver.request("read_output", {"session_id": sid, "cursor": 81}, error="CURSOR_AHEAD")
        cursor = error["details"]["earliest_cursor"]
        first = await driver.page(sid, cursor, 5)
        assert await driver.page(sid, cursor, 5) == first  # 重放，不消费。
        captured = ""
        intervals = []
        while True:
            page = await driver.page(sid, cursor, 5)
            intervals.append((cursor, page["next_cursor"]))
            captured += page["output"]
            cursor = page["next_cursor"]
            if not page["has_more"]:
                break
        assert captured == payload[-24:] and cursor == 80
        assert all(left[1] == right[0] for left, right in zip(intervals, intervals[1:]))
        record_property("capacity_chars", 24)
        record_property("emitted_chars", 80)
        record_property("evicted_chars", 56)
        record_property("recovery_cursor", 56)
        record_property("retained_output", captured)


async def test_e2e_invalid_inputs_are_structured_and_connection_stays_usable(
    tmp_path: Path, record_property: Callable[[str, Any], None]
) -> None:
    unknown = "not-an-issued-session-id"
    cases = [
        ("read_output", {"session_id": unknown}, "SESSION_NOT_FOUND"),
        ("write_input", {"session_id": unknown, "input": "x"}, "SESSION_NOT_FOUND"),
        ("get_status", {"session_id": unknown}, "SESSION_NOT_FOUND"),
        ("terminate_session", {"session_id": unknown}, "SESSION_NOT_FOUND"),
        ("get_status", {"session_id": ""}, "INVALID_ARGUMENT"),
        ("get_status", {"session_id": 123}, "INVALID_ARGUMENT"),
        ("read_output", {"session_id": unknown, "cursor": -1}, "INVALID_ARGUMENT"),
        ("read_output", {"session_id": unknown, "cursor": True}, "INVALID_ARGUMENT"),
        ("read_output", {"session_id": unknown, "max_chars": 0}, "INVALID_ARGUMENT"),
        ("write_input", {"session_id": unknown, "input": ""}, "INVALID_ARGUMENT"),
        (
            "write_input",
            {"session_id": unknown, "input": "x", "append_newline": "true"},
            "INVALID_ARGUMENT",
        ),
        ("start_session", {}, "INVALID_ARGUMENT"),
        ("start_session", {"command": "not-a-real-command-acceptance-67923"}, "COMMAND_NOT_FOUND"),
        (
            "start_session",
            {"command": python_command("pass"), "cwd": str(tmp_path / "missing")},
            "INVALID_CWD",
        ),
        ("ping", {"extra": True}, "INVALID_ARGUMENT"),
        ("unknown-tool", {}, "UNKNOWN_TOOL"),
    ]
    async with connected(tmp_path) as driver:
        for tool, args, code in cases:
            await driver.request(tool, args, error=code)
            assert (await driver.request("ping", {}))["message"] == "pong"
        # 失败后仍能创建真实会话，并自然退出，不只是ping还可用。
        valid = await driver.start(python_command("print('RECOVERED')"))
        assert (await driver.finished(valid["session_id"]))["exit_code"] == 0
        assert "RECOVERED" in (await driver.page(valid["session_id"], 0, 4096))["output"]
        record_property("validated_error_cases", len(cases))
        record_property("connection_recovery", True)


async def test_e2e_complete_repl_workflow_and_stdio_shutdown_cleanup(
    tmp_path: Path, record_property: Callable[[str, Any], None]
) -> None:
    async with connected(tmp_path) as driver:
        created = await driver.start(shlex.join([sys.executable, "-i", "-q"]))
        sid = created["session_id"]
        for source, count in (("x = 100", 8), ("print(x * 2)", 13)):
            ack = await driver.request(
                "write_input", {"session_id": sid, "input": source, "append_newline": True}
            )
            assert ack["bytes_written"] == count
        captured, cursor = await driver.until_line(sid, "200")
        assert "200" in captured.replace("\r\n", "\n").splitlines()
        live = await driver.status(sid)
        assert live["status"] == "running" and live["exit_code"] is None
        assert not live["output_complete"]
        await driver.request(
            "write_input",
            {
                "session_id": sid,
                "input": "x += 1; print(x)",
                "append_newline": True,
            },
        )
        continuation, next_cursor = await driver.until_line(sid, "101", cursor)
        assert "101" in continuation.replace("\r\n", "\n").splitlines()
        assert next_cursor > cursor
        ended = await driver.request("terminate_session", {"session_id": sid})
        repeated = await driver.request("terminate_session", {"session_id": sid})
        status = await driver.status(sid)
        assert ended == repeated == status
        assert status["status"] == "terminated" and status["output_complete"]
        assert type(status["exit_code"]) is int
        retention = datetime.fromisoformat(status["expires_at"]) - datetime.fromisoformat(
            status["ended_at"]
        )
        assert retention.total_seconds() == 600
        history = await driver.page(sid, 0, 4096)
        lines = history["output"].replace("\r\n", "\n").splitlines()
        assert "200" in lines and "101" in lines
        await driver.request(
            "write_input", {"session_id": sid, "input": "no"}, error="SESSION_NOT_RUNNING"
        )
        # 故意留下一个活跃会话；connected退出时验证stdio关闭确实回收该主进程。
        survivor = await driver.start(python_command("import time; print('LIVE'); time.sleep(30)"))
        assert (await driver.status(survivor["session_id"]))["status"] == "running"
        record_property("repl_results", "200,101")
        record_property("terminal_status", status["status"])
        record_property("retention_seconds", retention.total_seconds())
        record_property("shutdown_live_session_pid", survivor["pid"])

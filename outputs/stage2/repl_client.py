#!/usr/bin/env python3
"""通过同一真实 stdio MCP 连接验收 Python REPL 会话。

不使用 LLM，也不依赖额外运行包。请使用项目 .venv 中的 Python 运行本脚本：

    .venv/bin/python outputs/stage2/repl_client.py

脚本刻意把每个工具调用的 structured_content 与 isError 打印出来，供人工复核。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shlex
import sys
from pathlib import Path
from typing import Any

from mcp import Client, StdioServerParameters

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SERVER_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
POLL_ATTEMPTS = 60
POLL_INTERVAL_SECONDS = 0.05


def _json(value: Any) -> str:
    """产生适合人工检查的、不会因未知 SDK 对象而失败的 JSON。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _require_int(value: Any, field: str) -> int:
    """严格区分 bool 与 int，因为 bool 是 int 的子类。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise AssertionError(f"{field} 必须是整数，实际为 {value!r}")
    return value


def _require_envelope(result: Any, tool: str) -> dict[str, Any]:
    """打印并验证 MCP 工具统一 envelope 与 SDK isError 的一致性。"""
    payload = result.structured_content
    print(
        _json(
            {
                "tool": tool,
                "isError": result.is_error,
                "structured_content": payload,
            }
        ),
        flush=True,
    )

    if not isinstance(payload, dict):
        raise AssertionError(f"{tool} 未返回对象 structured_content：{payload!r}")
    if set(payload) != {"ok", "data", "error"}:
        raise AssertionError(f"{tool} envelope 字段不符：{payload!r}")
    if not isinstance(payload["ok"], bool):
        raise AssertionError(f"{tool} envelope 的 ok 不是布尔值：{payload!r}")
    if result.is_error != (not payload["ok"]):
        raise AssertionError(f"{tool} 的 isError 与 envelope.ok 不一致：{payload!r}")

    if payload["ok"]:
        if not isinstance(payload["data"], dict) or payload["error"] is not None:
            raise AssertionError(f"{tool} 成功 envelope 不符：{payload!r}")
    else:
        error = payload["error"]
        if payload["data"] is not None or not isinstance(error, dict):
            raise AssertionError(f"{tool} 失败 envelope 不符：{payload!r}")
        if not isinstance(error.get("code"), str) or not isinstance(error.get("message"), str):
            raise AssertionError(f"{tool} 错误对象缺少 code/message：{payload!r}")
    return payload


async def _call(client: Client, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """调用一个工具并检查其 envelope；不将业务失败误认为传输异常。"""
    result = await client.call_tool(tool, arguments)
    return _require_envelope(result, tool)


async def _call_ok(client: Client, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """调用预期成功的工具并取得 data。"""
    envelope = await _call(client, tool, arguments)
    if not envelope["ok"]:
        raise AssertionError(f"{tool} 失败：{_json(envelope['error'])}")
    data = envelope["data"]
    assert isinstance(data, dict)  # 已由 _require_envelope 验证，帮助静态检查。
    return data


def _contains_independent_200_line(text: str) -> bool:
    """只接受独立输出行 200，避免将 PTY 回显误判为计算结果。"""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return any(line.strip() == "200" for line in normalized.split("\n"))


def _check_status_shape(status: dict[str, Any], session_id: str, *, terminal: bool) -> None:
    """检查契约要求的状态及退出码字段结构。"""
    required = {
        "session_id",
        "status",
        "command",
        "argv",
        "cwd",
        "pid",
        "exit_code",
        "reason",
        "created_at",
        "ended_at",
        "expires_at",
        "earliest_cursor",
        "latest_cursor",
        "buffered_chars",
        "capacity_chars",
        "dropped_chars",
        "output_complete",
    }
    missing = sorted(required - set(status))
    if missing:
        raise AssertionError(f"get_status 缺少字段：{missing}")
    if status["session_id"] != session_id:
        raise AssertionError(f"get_status 的 session_id 不一致：{status!r}")
    if status["status"] not in {"running", "exited", "terminated", "failed"}:
        raise AssertionError(f"非法会话状态：{status!r}")
    for name in (
        "earliest_cursor",
        "latest_cursor",
        "buffered_chars",
        "capacity_chars",
        "dropped_chars",
    ):
        if _require_int(status[name], name) < 0:
            raise AssertionError(f"{name} 不能为负：{status!r}")
    if not isinstance(status["output_complete"], bool):
        raise AssertionError(f"output_complete 不是布尔值：{status!r}")

    exit_code = status["exit_code"]
    if exit_code is not None and (isinstance(exit_code, bool) or not isinstance(exit_code, int)):
        raise AssertionError(f"exit_code 必须为整数或 null：{status!r}")
    if terminal:
        if status["status"] not in {"terminated", "exited", "failed"}:
            raise AssertionError(f"终止后仍非终态：{status!r}")
        if isinstance(exit_code, bool) or not isinstance(exit_code, int):
            raise AssertionError(f"终态必须有整数 exit_code：{status!r}")
        if not status["output_complete"]:
            raise AssertionError(f"终态尚未完成输出 drain：{status!r}")


async def _read_until_200(client: Client, session_id: str, initial_cursor: int) -> str:
    """从保存的 next_cursor 有界轮询；必要时按 CURSOR_EXPIRED 的建议恢复。"""
    cursor = initial_cursor
    captured = ""

    for attempt in range(1, POLL_ATTEMPTS + 1):
        envelope = await _call(
            client,
            "read_output",
            {"session_id": session_id, "cursor": cursor, "max_chars": 4096},
        )
        if not envelope["ok"]:
            error = envelope["error"]
            assert isinstance(error, dict)  # 已由 _require_envelope 验证。
            if error.get("code") != "CURSOR_EXPIRED":
                raise AssertionError(f"read_output 失败：{_json(error)}")

            # 环形窗口已覆盖旧文本；只能从服务报告的 earliest_cursor 继续，不能补回丢失内容。
            details = error.get("details")
            if not isinstance(details, dict):
                raise AssertionError(f"CURSOR_EXPIRED 缺少 details：{_json(error)}")
            cursor = _require_int(details.get("earliest_cursor"), "error.details.earliest_cursor")
            latest = _require_int(details.get("latest_cursor"), "error.details.latest_cursor")
            if cursor > latest:
                raise AssertionError(f"CURSOR_EXPIRED 的游标范围非法：{_json(error)}")
            print(f"CURSOR_EXPIRED：从 earliest_cursor={cursor} 恢复读取。", flush=True)
            continue

        data = envelope["data"]
        assert isinstance(data, dict)
        if data.get("session_id") != session_id:
            raise AssertionError(f"read_output 返回错误会话：{data!r}")
        returned_cursor = _require_int(data.get("cursor"), "read_output.cursor")
        next_cursor = _require_int(data.get("next_cursor"), "read_output.next_cursor")
        if returned_cursor != cursor or next_cursor < cursor:
            raise AssertionError(f"read_output 游标未连续推进：{data!r}")
        output = data.get("output")
        if not isinstance(output, str):
            raise AssertionError(f"read_output.output 不是文本：{data!r}")

        captured += output
        # 后续每次都使用本次 next_cursor；读取不消费也不由服务端隐式推进。
        cursor = next_cursor
        if _contains_independent_200_line(captured):
            print(f"已在第 {attempt} 次轮询观察到独立输出行 200。", flush=True)
            return captured
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    raise AssertionError(
        f"在 {POLL_ATTEMPTS} 次、每次 {POLL_INTERVAL_SECONDS:.2f}s 的有界轮询内未见独立输出行 200；"
        f"已收集输出：{captured!r}"
    )


async def _exercise(server_python: Path) -> None:
    """在一个持久 MCP 连接中完成启动、写入、轮询、状态与终止。"""
    # command 与 cwd 均为绝对路径；启动服务不依赖调用脚本时的当前工作目录。
    server = StdioServerParameters(
        command=str(server_python),
        args=["-m", "mcp_terminal", "--config", str(PROJECT_ROOT / "server-config.json")],
        cwd=str(PROJECT_ROOT),
    )
    session_id: str | None = None
    terminate_requested = False

    async with asyncio.timeout(12):
        async with Client(server) as client:
            try:
                # -q 仅压低 banner；它仍是经 PTY 交互的 Python REPL。
                repl_command = shlex.join([str(server_python), "-i", "-q"])
                started = await _call_ok(client, "start_session", {"command": repl_command})
                session_id_value = started.get("session_id")
                if not isinstance(session_id_value, str) or not session_id_value:
                    raise AssertionError(f"start_session 未返回 session_id：{started!r}")
                session_id = session_id_value
                if started.get("status") != "running":
                    raise AssertionError(f"Python REPL 没有进入 running：{started!r}")
                initial_cursor = _require_int(
                    started.get("earliest_cursor"), "start_session.earliest_cursor"
                )

                first_write = await _call_ok(
                    client, "write_input", {"session_id": session_id, "input": "x=100\n"}
                )
                second_write = await _call_ok(
                    client, "write_input", {"session_id": session_id, "input": "print(x*2)\n"}
                )
                for write_data, source in ((first_write, "x=100"), (second_write, "print(x*2)")):
                    if write_data.get("session_id") != session_id:
                        raise AssertionError(f"write_input({source}) 返回错误会话：{write_data!r}")
                    if (
                        _require_int(write_data.get("bytes_written"), "write_input.bytes_written")
                        <= 0
                    ):
                        raise AssertionError(
                            f"write_input({source}) 未提交任何字节：{write_data!r}"
                        )

                await _read_until_200(client, session_id, initial_cursor)

                before_termination = await _call_ok(
                    client, "get_status", {"session_id": session_id}
                )
                _check_status_shape(before_termination, session_id, terminal=False)
                if before_termination["status"] != "running":
                    raise AssertionError(f"REPL 在显式终止前意外结束：{before_termination!r}")

                terminated = await _call_ok(client, "terminate_session", {"session_id": session_id})
                if terminated.get("session_id") != session_id:
                    raise AssertionError(f"terminate_session 返回错误会话：{terminated!r}")
                terminate_requested = True

                after_termination = await _call_ok(client, "get_status", {"session_id": session_id})
                _check_status_shape(after_termination, session_id, terminal=True)
                print(
                    "验收完成：REPL 输出、状态字段与终止后的 exit_code 结构均符合预期。", flush=True
                )
            finally:
                # 即使断言、超时或某个工具调用失败，也尝试收回仍处于本连接中的 PTY 进程组。
                if session_id is not None and not terminate_requested:
                    try:
                        await _call(client, "terminate_session", {"session_id": session_id})
                        print(f"finally 已请求清理会话 {session_id}。", flush=True)
                    except (
                        Exception
                    ) as cleanup_error:  # 连接本身失败时，Client 退出仍会关闭 stdio 服务。
                        print(
                            f"finally 清理会话 {session_id} 失败：{cleanup_error!r}",
                            file=sys.stderr,
                            flush=True,
                        )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="在同一 MCP stdio 连接中验收 Python REPL 会话。")
    parser.add_argument(
        "--server-python",
        default=str(DEFAULT_SERVER_PYTHON),
        help="启动 MCP 服务与 REPL 的 Python 解释器；默认是本项目 .venv/bin/python 的绝对路径。",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    # 不 resolve：.venv/bin/python 常是指向基础解释器的符号链接，解析后会失去 venv。
    server_python = Path(args.server_python).expanduser().absolute()
    if not server_python.is_file():
        print(f"解释器不存在或不是文件：{server_python}", file=sys.stderr)
        return 2

    try:
        asyncio.run(_exercise(server_python))
    except Exception as error:
        print(f"验收失败：{error!r}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

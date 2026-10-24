"""用户控制的启动配置与正式CLI入口的回归验证。"""

import asyncio
import json
import os
import shlex
import stat
import sys
from pathlib import Path

import pytest
from mcp import Client, StdioServerParameters

from mcp_terminal.config import load_config


def write_config(path: Path, data) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")
    path.chmod(0o600)


def test_relative_config_paths_are_resolved_from_config_directory(tmp_path):
    path = tmp_path / "server.json"
    write_config(
        path,
        {
            "allowed_cwd_roots": ["."],
            "allowed_env_keys": ["APP_MODE"],
            "audit_log_path": "private/events.jsonl",
        },
    )
    config = load_config(path)
    assert config.allowed_cwd_roots == (str(tmp_path),)
    assert config.allowed_env_keys == ("APP_MODE",)
    assert config.audit_log_path == str(tmp_path / "private/events.jsonl")


@pytest.mark.parametrize(
    "data",
    [
        [],
        {"unrecognized": True},
        {"allowed_cwd_roots": []},
        {"allowed_env_keys": "APP_MODE"},
        {"audit_timeout": False},
    ],
)
def test_bad_config_rejected(tmp_path, data):
    path = tmp_path / "server.json"
    write_config(path, data)
    with pytest.raises((ValueError, TypeError)):
        load_config(path)


def test_config_other_user_write_permission_is_rejected(tmp_path):
    path = tmp_path / "server.json"
    write_config(path, {})
    path.chmod(0o666)
    with pytest.raises(ValueError):
        load_config(path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o666


async def test_production_config_cli_enforces_roots_and_os_identity_and_writes_jsonl(tmp_path):
    root = tmp_path / "approved"
    root.mkdir()
    launch_cwd = tmp_path / "launch-outside-approved-root"
    launch_cwd.mkdir()
    path = tmp_path / "server.json"
    log = tmp_path / "private/events.jsonl"
    write_config(
        path,
        {
            "allowed_cwd_roots": ["approved"],
            "allowed_env_keys": ["APP_MODE"],
            "audit_log_path": "private/events.jsonl",
        },
    )
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_terminal", "--config", str(path)],
        cwd=str(launch_cwd),
        env={"DO_NOT_INHERIT": "parent-private-value"},
    )
    source = (
        "import os,json; print(json.dumps({'uid':os.geteuid(),'gid':os.getegid(),"
        "'cwd':os.getcwd(),'mode':os.getenv('APP_MODE'),'private':os.getenv('DO_NOT_INHERIT')}))"
    )
    async with asyncio.timeout(15):
        async with Client(params) as client:
            assert client.server_info.version == "0.3.0"
            result = await client.call_tool(
                "start_session",
                {
                    "command": shlex.join([sys.executable, "-u", "-c", source]),
                    "env": {"APP_MODE": "explicit-request-value"},
                },
            )
            assert not result.is_error, result.structured_content
            sid = result.structured_content["data"]["session_id"]
            async with asyncio.timeout(5):
                while True:
                    status = (
                        await client.call_tool("get_status", {"session_id": sid})
                    ).structured_content["data"]
                    if status["output_complete"]:
                        break
                    await asyncio.sleep(0.01)
            assert status["uid"] == os.geteuid() and status["gid"] == os.getegid()
            text = (await client.call_tool("read_output", {"session_id": sid})).structured_content[
                "data"
            ]["output"]
            actual = json.loads(text.strip())
            assert actual == {
                "uid": os.geteuid(),
                "gid": os.getegid(),
                "cwd": str(root),
                "mode": "explicit-request-value",
                "private": None,
            }
            denied = await client.call_tool(
                "start_session",
                {
                    "command": shlex.join([sys.executable, "-c", "pass"]),
                    "cwd": str(launch_cwd),
                },
            )
            assert (
                denied.is_error and denied.structured_content["error"]["code"] == "CWD_NOT_ALLOWED"
            )
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert any(row["event"] == "session_created" and row["session_id"] == sid for row in rows)
    assert any(row["event"] == "session_finished" and row["exit_code"] == 0 for row in rows)
    assert any(
        row["event"] == "start_rejected" and row["error_code"] == "CWD_NOT_ALLOWED" for row in rows
    )
    assert "parent-private-value" not in log.read_text()
    assert "explicit-request-value" not in log.read_text()
    assert stat.S_IMODE(log.stat().st_mode) == 0o600

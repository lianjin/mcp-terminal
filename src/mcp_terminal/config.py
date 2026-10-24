"""可信本地启动配置：与MCP请求参数隔离，不能由工具调用修改安全策略。"""

from __future__ import annotations

import json
import os
from pathlib import Path

from mcp_terminal.sessions import SessionConfig


def load_config(path: str | Path) -> SessionConfig:
    filename = Path(path).expanduser().resolve(strict=True)
    info = filename.stat()
    if not filename.is_file() or info.st_size > 65536:
        raise ValueError("配置必须是最多 65536 字节的 JSON 文件")
    if info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise ValueError("配置必须属于当前用户，且不能允许其他用户或组成员修改")
    with filename.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("配置 JSON 必须是对象")
    roots = data.get("allowed_cwd_roots")
    if isinstance(roots, list) and all(isinstance(item, str) and item for item in roots):
        data["allowed_cwd_roots"] = [
            str(filename.parent / Path(item).expanduser()) for item in roots
        ]
    audit_path = data.get("audit_log_path")
    if isinstance(audit_path, str) and audit_path:
        data["audit_log_path"] = str(filename.parent / Path(audit_path).expanduser())
    return SessionConfig(**data)

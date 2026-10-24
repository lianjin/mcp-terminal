"""启动边界策略；不声称提供进程运行后的文件系统或网络沙箱。"""

from __future__ import annotations

import os
import pwd
import sys
from dataclasses import dataclass
from pathlib import Path

from mcp_terminal.errors import SessionError


@dataclass(frozen=True)
class OSIdentity:
    user: str
    uid: int
    gid: int

    @classmethod
    def current(cls) -> OSIdentity:
        uid = os.geteuid()
        return cls(pwd.getpwuid(uid).pw_name, uid, os.getegid())


def minimal_environment() -> dict[str, str]:
    """固定且可运行的三项基线；绝不读取 os.environ（连 PATH 也不继承）。"""
    executable_dir = str(Path(sys.executable).parent)
    return {
        "PATH": os.pathsep.join(dict.fromkeys([executable_dir, *os.defpath.split(os.pathsep)])),
        "TERM": "xterm-256color",
        "LANG": "C.UTF-8",
    }


def normalize_roots(roots: tuple[str, ...] | None, startup_cwd: str) -> tuple[Path, ...]:
    candidates = roots if roots is not None else (startup_cwd,)
    result = []
    for value in candidates:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = Path(startup_cwd) / path
        try:
            path = path.resolve(strict=True)
            if not path.is_dir():
                raise ValueError()
        except (OSError, ValueError, RuntimeError):
            raise ValueError("allowed_cwd_roots 必须是已经存在且可访问的目录") from None
        result.append(path)
    return tuple(dict.fromkeys(result))


def check_directory(cwd: str | None, default_cwd: str, roots: tuple[Path, ...]) -> str:
    if cwd is not None and (not isinstance(cwd, str) or not cwd or "\0" in cwd):
        raise SessionError("INVALID_CWD", "cwd 必须是有效目录路径。")
    path = Path(cwd or default_cwd).expanduser()
    if not path.is_absolute():
        path = Path(default_cwd) / path
    try:
        path = path.resolve(strict=True)
        if not path.is_dir():
            raise OSError()
    except (OSError, ValueError, RuntimeError):
        raise SessionError("INVALID_CWD", "cwd 不存在、不是目录或不可访问。") from None
    if not any(path.is_relative_to(root) for root in roots):
        raise SessionError(
            "CWD_NOT_ALLOWED",
            "cwd 不在用户配置的允许目录内。",
            {"allowed_roots": [str(root) for root in roots]},
        )
    return str(path)


def check_environment_keys(env: dict[str, str], allowed: tuple[str, ...]) -> None:
    denied = sorted(set(env) - set(allowed))
    if denied:
        raise SessionError(
            "ENV_NOT_ALLOWED",
            "新增或覆盖环境变量需要用户先在服务启动配置中允许。",
            {
                "keys": denied,
                "hint": "请用户在 server-config.json 的 allowed_env_keys 添加所需键后重启服务。",
            },
        )

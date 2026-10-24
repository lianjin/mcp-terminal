"""本地 POSIX PTY 会话管理；所有进程/FD 等待都在异步任务中进行。"""

from __future__ import annotations

import asyncio
import codecs
import errno
import json
import logging
import math
import os
import shlex
import shutil
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from mcp_terminal.audit import AuditLog
from mcp_terminal.buffer import RingBuffer
from mcp_terminal.errors import SessionError
from mcp_terminal.security import (
    OSIdentity,
    check_directory,
    check_environment_keys,
    minimal_environment,
    normalize_roots,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionConfig:
    buffer_chars: int = 1048576
    max_sessions: int = 32
    retention_seconds: float = 600
    cleanup_interval: float = 1
    write_timeout: float = 2
    start_timeout: float = 5
    terminate_grace: float = 1
    drain_timeout: float = 1
    max_input_bytes: int = 65536
    allowed_cwd_roots: tuple[str, ...] | None = None
    allowed_env_keys: tuple[str, ...] = ()
    audit_log_path: str | None = None
    audit_max_bytes: int = 10485760
    audit_backup_count: int = 3
    audit_queue_size: int = 2048
    audit_timeout: float = 2

    def __post_init__(self) -> None:
        for name in (
            "buffer_chars",
            "max_sessions",
            "max_input_bytes",
            "audit_max_bytes",
            "audit_backup_count",
            "audit_queue_size",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in (
            "retention_seconds",
            "cleanup_interval",
            "write_timeout",
            "start_timeout",
            "terminate_grace",
            "drain_timeout",
            "audit_timeout",
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        for name in ("allowed_cwd_roots", "allowed_env_keys"):
            values = getattr(self, name)
            if name == "allowed_cwd_roots" and values is None:
                continue
            if not isinstance(values, (tuple, list)) or any(
                not isinstance(item, str) or not item or "\0" in item for item in values
            ):
                raise ValueError(f"{name} must be a sequence of nonempty strings")
            if name == "allowed_cwd_roots" and not values:
                raise ValueError("allowed_cwd_roots must not be empty")
            if name == "allowed_env_keys" and any(
                "=" in item or len(item) > 256 for item in values
            ):
                raise ValueError("allowed_env_keys contains invalid names")
            object.__setattr__(self, name, tuple(dict.fromkeys(values)))
        if self.audit_log_path is not None and (
            not isinstance(self.audit_log_path, str)
            or not self.audit_log_path
            or "\0" in self.audit_log_path
        ):
            raise ValueError("audit_log_path must be a nonempty path string")


@dataclass
class _Session:
    session_id: str
    command: str
    argv: list[str]
    cwd: str
    pid: int
    process: asyncio.subprocess.Process | None
    master_fd: int | None
    buffer: RingBuffer
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    started_mono: float = field(default_factory=time.monotonic)
    ended_mono: float | None = None
    user: str = ""
    uid: int = -1
    gid: int = -1
    audit_error: str | None = None
    status: str = "running"
    exit_code: int | None = None
    reason: str | None = None
    ended_at: str | None = None
    expires_at: str | None = None
    expires_mono: float | None = None
    output_complete: bool = False
    terminate_requested: bool = False
    failure_reason: str | None = None
    input_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    reader_task: asyncio.Task[None] | None = None
    monitor_task: asyncio.Task[None] | None = None
    terminate_task: asyncio.Task[None] | None = None
    read_waiter: asyncio.Future[None] | None = None
    write_waiter: asyncio.Future[None] | None = None


class SessionManager:
    def __init__(self, config: SessionConfig | None = None) -> None:
        self.config = config or SessionConfig()
        startup_cwd = os.getcwd()
        self.allowed_cwd_roots = normalize_roots(self.config.allowed_cwd_roots, startup_cwd)
        self.default_cwd = str(self.allowed_cwd_roots[0])
        self.identity = OSIdentity.current()
        path = Path(self.config.audit_log_path or str(Path(startup_cwd) / "logs/sessions.jsonl"))
        self.audit = AuditLog(
            path,
            max_bytes=self.config.audit_max_bytes,
            backup_count=self.config.audit_backup_count,
            queue_size=self.config.audit_queue_size,
            timeout=self.config.audit_timeout,
        )
        self.audit_log_path = str(self.audit.path)
        self._sessions: dict[str, _Session] = {}
        self._pending: set[str] = set()
        self._start_tasks: set[asyncio.Task[dict[str, Any]]] = set()
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> SessionManager:
        self._ensure_open()
        await self.audit.start()
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="session-expiry")
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionError("SERVER_SHUTTING_DOWN", "服务正在关闭，无法处理会话请求。")

    def _prune(self) -> None:
        now = time.monotonic()
        for sid, session in list(self._sessions.items()):
            if session.finished.is_set() and session.expires_mono is not None:
                if now >= session.expires_mono:
                    if not self.audit.emit_nowait(self._event("session_expired", session)):
                        session.audit_error = "AUDIT_UNAVAILABLE"
                    session.buffer.clear()
                    del self._sessions[sid]

    def _lookup(self, session_id: str) -> _Session:
        self._ensure_open()
        if not isinstance(session_id, str) or not session_id or len(session_id) > 128:
            raise SessionError("INVALID_ARGUMENT", "session_id 必须是长度 1..128 的字符串。")
        self._prune()
        try:
            return self._sessions[session_id]
        except KeyError:
            raise SessionError("SESSION_NOT_FOUND", "会话不存在或保留期已结束。") from None

    def _snapshot(self, session: _Session) -> dict[str, Any]:
        return {
            "session_id": session.session_id,
            "status": session.status,
            "command": session.command,
            "argv": list(session.argv),
            "cwd": session.cwd,
            "pid": session.pid,
            "exit_code": session.exit_code,
            "reason": session.reason,
            "created_at": session.created_at,
            "ended_at": session.ended_at,
            "expires_at": session.expires_at,
            "earliest_cursor": session.buffer.earliest_cursor,
            "latest_cursor": session.buffer.latest_cursor,
            "buffered_chars": session.buffer.size,
            "capacity_chars": session.buffer.capacity_chars,
            "dropped_chars": session.buffer.earliest_cursor,
            "output_complete": session.output_complete,
            "user": session.user,
            "uid": session.uid,
            "gid": session.gid,
            "duration_seconds": round(
                max(0, (session.ended_mono or time.monotonic()) - session.started_mono), 6
            ),
            "audit_error": session.audit_error or self.audit.error,
        }

    def _event(self, event: str, session: _Session | None = None, **extra: Any) -> dict[str, Any]:
        now = datetime.now(UTC).isoformat()
        data = (
            self._snapshot(session)
            if session is not None
            else {
                "session_id": None,
                "created_at": now,
                "user": self.identity.user,
                "uid": self.identity.uid,
                "gid": self.identity.gid,
                "command": None,
                "argv": [],
                "cwd": None,
                "pid": None,
                "status": "starting",
                "exit_code": None,
                "duration_seconds": 0,
            }
        )
        return {**data, "timestamp": now, "event": event, **extra}

    async def _audit_event(
        self,
        event: str,
        session: _Session | None = None,
        *,
        required: bool = False,
        **extra: Any,
    ) -> None:
        try:
            await self.audit.record(self._event(event, session, **extra))
        except SessionError:
            if session is not None:
                session.audit_error = "AUDIT_UNAVAILABLE"
            if required:
                raise
            logger.error("Session audit unavailable; event=%s", event)

    def _prepare(
        self, command: str, cwd: str | None, env: dict[str, str] | None
    ) -> tuple[list[str], str, dict[str, str]]:
        if not isinstance(command, str) or not command.strip() or len(command) > 8192:
            raise SessionError("INVALID_ARGUMENT", "command 必须是非空字符串，最长 8192 字符。")
        if "\x00" in command:
            raise SessionError("INVALID_ARGUMENT", "command 不可含 NUL。")
        try:
            argv = shlex.split(command)
        except ValueError:
            raise SessionError("INVALID_ARGUMENT", "command 的引号不匹配。") from None
        if not argv or not argv[0]:
            raise SessionError("INVALID_ARGUMENT", "command 缺少可执行程序。")
        child_env = minimal_environment()
        if env is not None:
            if not isinstance(env, dict) or len(env) > 128:
                raise SessionError("INVALID_ARGUMENT", "env 必须是最多 128 项的字符串映射。")
            total = 0
            for key, value in env.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    raise SessionError("INVALID_ARGUMENT", "env 的键和值必须是字符串。")
                if not key or "=" in key or "\x00" in key or "\x00" in value:
                    raise SessionError("INVALID_ARGUMENT", "env 的键或值格式无效。")
                if len(key) > 256 or len(value) > 8192:
                    raise SessionError("INVALID_ARGUMENT", "env 的键或值超过长度限制。")
                try:
                    total += len(key.encode("utf-8")) + len(value.encode("utf-8"))
                except UnicodeError:
                    raise SessionError(
                        "INVALID_ARGUMENT", "env 必须是有效 Unicode 文本。"
                    ) from None
            if total > 65536:
                raise SessionError("INVALID_ARGUMENT", "env 的编码长度超过 65536 字节。")
            check_environment_keys(env, self.config.allowed_env_keys)
            child_env.update(env)
        directory = Path(check_directory(cwd, self.default_cwd, self.allowed_cwd_roots))
        executable = argv[0]
        if os.sep in executable:
            candidate = Path(executable)
            if not candidate.is_absolute():
                candidate = directory / candidate
            executable = str(candidate)
        else:
            # 相对 PATH 项必须相对目标 cwd，而不是 MCP 服务的 cwd。
            search_path = os.pathsep.join(
                str(directory / entry) if not os.path.isabs(entry) else entry
                for entry in child_env.get("PATH", os.defpath).split(os.pathsep)
            )
            executable = shutil.which(executable, path=search_path) or ""
        if not executable or not os.path.isfile(executable) or not os.access(executable, os.X_OK):
            raise SessionError("COMMAND_NOT_FOUND", "找不到可执行程序或程序没有执行权限。")
        argv[0] = executable
        return argv, str(directory), child_env

    async def start_session(
        self, command: str, cwd: str | None = None, env: dict[str, str] | None = None
    ) -> dict[str, Any]:
        self._ensure_open()
        sid = uuid.uuid4().hex
        async with self._lock:
            self._ensure_open()
            self._prune()
            if len(self._sessions) + len(self._pending) >= self.config.max_sessions:
                raise SessionError(
                    "SESSION_LIMIT_REACHED", "会话记录达到上限，请等待结束记录过期。"
                )
            self._pending.add(sid)
        # 文件系统检查放到线程，避免慢磁盘阻塞所有会话。
        task = asyncio.create_task(self._launch(sid, command, cwd, env), name=f"start-{sid}")
        self._start_tasks.add(task)
        try:
            async with asyncio.timeout(self.config.start_timeout):
                return await asyncio.shield(task)
        except (asyncio.CancelledError, TimeoutError) as exc:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            # 启动已完成但调用者尚未收到 ID 时取消，也不能遗留无人控制的进程。
            session = self._sessions.get(sid)
            if session is not None:
                cleanup = asyncio.create_task(self._terminate(session))
                await asyncio.shield(cleanup)
            if isinstance(exc, TimeoutError):
                raise SessionError("SPAWN_TIMEOUT", "创建会话超过启动期限。") from None
            raise
        finally:
            self._pending.discard(sid)
            self._start_tasks.discard(task)

    @staticmethod
    async def _pipe_contents(fd: int) -> bytes:
        loop = asyncio.get_running_loop()
        parts = bytearray()
        while True:
            try:
                chunk = os.read(fd, 4096)
                if not chunk:
                    return bytes(parts)
                parts.extend(chunk)
                if len(parts) > 4096:
                    raise SessionError("SPAWN_FAILED", "启动器返回了无效结果。")
            except BlockingIOError:
                future = loop.create_future()

                def ready() -> None:
                    if not future.done():
                        future.set_result(None)

                loop.add_reader(fd, ready)
                try:
                    await future
                finally:
                    loop.remove_reader(fd)

    @staticmethod
    def _signal_group(pid: int, sig: signal.Signals) -> None:
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass

    async def _abort_process(self, process: asyncio.subprocess.Process) -> None:
        self._signal_group(process.pid, signal.SIGKILL)
        await process.wait()

    async def _launch(
        self, sid: str, command: str, cwd: str | None, env: dict[str, str] | None
    ) -> dict[str, Any]:
        master = slave = error_read = error_write = None
        process: asyncio.subprocess.Process | None = None
        spawn_task: asyncio.Task[asyncio.subprocess.Process] | None = None
        transferred = False
        try:
            await self._audit_event(
                "start_requested",
                required=True,
                session_id=sid,
                command=command,
                env_keys=sorted(key for key in (env or {}) if isinstance(key, str))[:128]
                if isinstance(env, dict)
                else [],
            )
            argv, directory, child_env = await asyncio.to_thread(self._prepare, command, cwd, env)
            self._ensure_open()
            master, slave = os.openpty()
            error_read, error_write = os.pipe()
            os.set_blocking(master, False)
            os.set_blocking(error_read, False)
            launcher = str(Path(__file__).with_name("_pty_child.py"))
            spawn_task = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    launcher,
                    str(error_write),
                    *argv,
                    stdin=slave,
                    stdout=slave,
                    stderr=slave,
                    cwd=directory,
                    env=child_env,
                    start_new_session=True,
                    pass_fds=(error_write,),
                )
            )
            process = await asyncio.shield(spawn_task)
            os.close(slave)
            slave = None
            os.close(error_write)
            error_write = None
            try:
                async with asyncio.timeout(self.config.start_timeout):
                    launch_error = await self._pipe_contents(error_read)
            except TimeoutError:
                raise SessionError("SPAWN_TIMEOUT", "创建会话超过启动期限。") from None
            if launch_error:
                try:
                    detail = json.loads(launch_error)
                except (ValueError, UnicodeError):
                    detail = {}
                raise SessionError("SPAWN_FAILED", "子进程无法设置终端或执行命令。", detail)
            self._ensure_open()
            session = _Session(
                sid,
                command,
                argv,
                directory,
                process.pid,
                process,
                master,
                RingBuffer(self.config.buffer_chars),
                user=self.identity.user,
                uid=self.identity.uid,
                gid=self.identity.gid,
            )
            await self._audit_event("session_created", session, required=True)
            self._sessions[sid] = session
            session.reader_task = asyncio.create_task(self._pump(session), name=f"output-{sid}")
            session.monitor_task = asyncio.create_task(self._monitor(session), name=f"exit-{sid}")
            transferred = True
            return self._snapshot(session)
        except asyncio.CancelledError:
            await self._audit_event(
                "start_cancelled", session_id=sid, command=command, status="cancelled"
            )
            raise
        except SessionError as exc:
            if exc.code == "AUDIT_UNAVAILABLE":
                exc.details.setdefault("execution_started", process is not None)
            await self._audit_event(
                "start_rejected",
                session_id=sid,
                command=command,
                status="rejected",
                error_code=exc.code,
            )
            raise
        except OSError:
            await self._audit_event(
                "start_rejected",
                session_id=sid,
                command=command,
                status="rejected",
                error_code="SPAWN_FAILED",
            )
            raise SessionError("SPAWN_FAILED", "无法创建 PTY 或启动进程。") from None
        finally:
            if not transferred:
                # shield 保证取消发生在 Popen 创建期间时仍能取得进程并回收。
                if process is None and spawn_task is not None:
                    try:
                        process = await spawn_task
                    except Exception:
                        pass
                if process is not None:
                    await self._abort_process(process)
            for fd in (slave, error_write, error_read, master if not transferred else None):
                if fd is not None:
                    os.close(fd)

    @staticmethod
    async def _ready(session: _Session, write: bool = False) -> None:
        fd = session.master_fd
        if fd is None:
            raise SessionError("SESSION_NOT_RUNNING", "会话终端已关闭。")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        attr = "write_waiter" if write else "read_waiter"
        setattr(session, attr, future)

        def ready() -> None:
            if not future.done():
                future.set_result(None)

        if write:
            loop.add_writer(fd, ready)
        else:
            loop.add_reader(fd, ready)
        try:
            await future
        finally:
            # 不允许旧会话的回调删除已重用 FD 的新会话回调。
            if session.master_fd == fd and getattr(session, attr) is future:
                if write:
                    loop.remove_writer(fd)
                else:
                    loop.remove_reader(fd)
            if getattr(session, attr) is future:
                setattr(session, attr, None)

    async def _pump(self, session: _Session) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            while session.master_fd is not None:
                try:
                    chunk = os.read(session.master_fd, 65536)
                except BlockingIOError:
                    await self._ready(session)
                    continue
                except OSError as exc:
                    if exc.errno == errno.EIO:  # POSIX PTY EOF，Linux 常以 EIO 表示。
                        break
                    raise
                if not chunk:
                    break
                session.buffer.append(decoder.decode(chunk))
                await asyncio.sleep(0)  # 高频输出也不能垄断事件循环。
        except asyncio.CancelledError:
            raise
        except (OSError, SessionError):
            session.failure_reason = "output_read_error"
            if session.process is not None:
                self._signal_group(session.pid, signal.SIGKILL)
        finally:
            session.buffer.append(decoder.decode(b"", final=True))

    def _close_master(self, session: _Session) -> None:
        fd = session.master_fd
        if fd is None:
            return
        loop = asyncio.get_running_loop()
        loop.remove_reader(fd)
        loop.remove_writer(fd)
        session.master_fd = None
        for future in (session.read_waiter, session.write_waiter):
            if future is not None and not future.done():
                future.set_exception(SessionError("SESSION_NOT_RUNNING", "会话终端已关闭。"))
        os.close(fd)

    async def _monitor(self, session: _Session) -> None:
        assert session.process is not None
        process = session.process
        try:
            session.exit_code = await process.wait()
            ended = datetime.now(UTC)
            session.ended_mono = time.monotonic()
            session.ended_at = ended.isoformat()
            session.expires_at = (
                ended + timedelta(seconds=self.config.retention_seconds)
            ).isoformat()
            session.expires_mono = session.ended_mono + self.config.retention_seconds
            if session.failure_reason:
                session.status, session.reason = "failed", session.failure_reason
            elif session.terminate_requested:
                session.status, session.reason = "terminated", "user_terminated"
            else:
                session.status, session.reason = "exited", "process_exit"
            # 主进程退出后不让同组子进程继续持有PTY或成为无人管理的任务。
            self._signal_group(session.pid, signal.SIGKILL)
            if session.reader_task is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(session.reader_task), self.config.drain_timeout
                    )
                except TimeoutError:
                    session.reader_task.cancel()
                    await asyncio.gather(session.reader_task, return_exceptions=True)
                    session.reason = f"{session.reason};output_drain_timeout"
            if session.failure_reason:
                session.status, session.reason = "failed", session.failure_reason
        finally:
            self._close_master(session)
            session.output_complete = True
            session.process = None
            session.reader_task = None
            await self._audit_event("session_finished", session)
            session.finished.set()

    async def write_input(
        self, session_id: str, input: str, append_newline: bool = False
    ) -> dict[str, Any]:
        session = self._lookup(session_id)
        if not isinstance(input, str) or not input:
            raise SessionError("INVALID_ARGUMENT", "input 必须是非空字符串。")
        if type(append_newline) is not bool:
            raise SessionError("INVALID_ARGUMENT", "append_newline 必须是布尔值。")
        try:
            payload = (input + ("\n" if append_newline else "")).encode("utf-8")
        except UnicodeError:
            raise SessionError("INVALID_ARGUMENT", "input 必须是有效 Unicode 文本。") from None
        if len(payload) > self.config.max_input_bytes:
            raise SessionError(
                "INVALID_ARGUMENT",
                "input 超过单次字节限制。",
                {"max_input_bytes": self.config.max_input_bytes},
            )
        try:
            await self._audit_event(
                "input_requested",
                session,
                required=True,
                bytes_requested=len(payload),
                append_newline=append_newline,
            )
        except SessionError as exc:
            exc.details.setdefault("bytes_written", 0)
            raise
        written = 0
        try:
            async with asyncio.timeout(self.config.write_timeout):
                async with session.input_lock:
                    while written < len(payload):
                        if session.status != "running" or session.master_fd is None:
                            raise SessionError(
                                "SESSION_NOT_RUNNING",
                                "会话已经结束，不能继续写入。",
                                {"bytes_written": written},
                            )
                        if session.process is None or session.process.returncode is not None:
                            raise SessionError(
                                "SESSION_NOT_RUNNING", "进程已经退出。", {"bytes_written": written}
                            )
                        try:
                            count = os.write(session.master_fd, payload[written:])
                        except BlockingIOError:
                            await self._ready(session, write=True)
                            continue
                        except OSError:
                            raise SessionError(
                                "INPUT_CLOSED", "会话输入已不可写。", {"bytes_written": written}
                            ) from None
                        if count == 0:
                            raise SessionError(
                                "INPUT_CLOSED", "会话输入已不可写。", {"bytes_written": written}
                            )
                        written += count
                        await asyncio.sleep(0)
        except asyncio.CancelledError:
            await self._audit_event("input_cancelled", session, bytes_written=written)
            raise
        except TimeoutError:
            await self._audit_event(
                "input_failed", session, bytes_written=written, error_code="WRITE_TIMEOUT"
            )
            raise SessionError(
                "WRITE_TIMEOUT", "写入超时；请勿盲目重试已提交的内容。", {"bytes_written": written}
            ) from None
        except SessionError as exc:
            exc.details.setdefault("bytes_written", written)
            await self._audit_event(
                "input_failed", session, bytes_written=written, error_code=exc.code
            )
            raise
        try:
            await self._audit_event("input_written", session, required=True, bytes_written=written)
        except SessionError as exc:
            exc.details.setdefault("bytes_written", written)
            raise
        return {"session_id": session_id, "status": session.status, "bytes_written": written}

    async def read_output(
        self, session_id: str, cursor: int | None = None, max_chars: int = 16384
    ) -> dict[str, Any]:
        session = self._lookup(session_id)
        return {
            "session_id": session_id,
            "status": session.status,
            **session.buffer.read(cursor, max_chars),
            "output_complete": session.output_complete,
        }

    async def get_status(self, session_id: str) -> dict[str, Any]:
        return self._snapshot(self._lookup(session_id))

    async def _terminate(self, session: _Session) -> None:
        process = session.process
        if process is not None and process.returncode is None:
            if not session.terminate_requested:
                session.terminate_requested = True
                await self._audit_event("terminate_requested", session)
            self._signal_group(session.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(asyncio.shield(process.wait()), self.config.terminate_grace)
            except TimeoutError:
                self._signal_group(session.pid, signal.SIGKILL)
                await process.wait()
        await session.finished.wait()

    async def terminate_session(self, session_id: str) -> dict[str, Any]:
        session = self._lookup(session_id)
        if session.terminate_task is None:
            session.terminate_task = asyncio.create_task(
                self._terminate(session), name=f"terminate-{session_id}"
            )
        await asyncio.shield(session.terminate_task)
        return self._snapshot(session)

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.cleanup_interval)
            self._prune()

    async def _close(self) -> None:
        self._closed = True
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
            self._cleanup_task = None
        starts = list(self._start_tasks)
        for task in starts:
            task.cancel()
        await asyncio.gather(*starts, return_exceptions=True)
        sessions = list(self._sessions.values())
        for session in sessions:
            if session.terminate_task is None:
                session.terminate_task = asyncio.create_task(self._terminate(session))
        await asyncio.gather(*(s.terminate_task for s in sessions), return_exceptions=True)
        for session in sessions:
            if session.monitor_task is not None:
                await asyncio.gather(session.monitor_task, return_exceptions=True)
            session.buffer.clear()
        self._sessions.clear()
        self._pending.clear()
        self._start_tasks.clear()
        await self._audit_event("server_shutdown", status="closed", session_count=len(sessions))
        await self.audit.close()

    async def close(self) -> None:
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close(), name="session-manager-close")
        try:
            await asyncio.shield(self._close_task)
        except asyncio.CancelledError:
            await self._close_task
            raise

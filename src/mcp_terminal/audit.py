"""安全、异步的 JSONL 会话审计日志。"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import json
import math
import os
import queue
import stat
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp_terminal.errors import SessionError

_AUDIT_ERROR = "AUDIT_UNAVAILABLE"
_AUDIT_MESSAGE = "Audit logging is unavailable."
_STOP = object()


class _AuditWriteFailed(Exception):
    """Private marker placed on queued acknowledgements after a writer failure."""


@dataclass(frozen=True)
class _QueuedRecord:
    line: bytes
    acknowledgement: Future[None]


class AuditLog:
    """Append-only audit logger with a bounded producer queue.

    The public methods intentionally expose no operating-system failure details.  The
    ``error`` attribute is the observable, stable error code for callers which need
    to include audit health in a session snapshot.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_bytes: int = 10_485_760,
        backup_count: int = 3,
        queue_size: int = 2048,
        timeout: float = 2,
    ) -> None:
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        if type(backup_count) is not int or backup_count < 0:
            raise ValueError("backup_count must be a non-negative integer")
        if type(queue_size) is not int or queue_size <= 0:
            raise ValueError("queue_size must be a positive integer")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and positive")

        # Path.absolute is lexical, rather than resolve(): resolving here would follow
        # a final symlink before the worker has had a chance to reject it.
        self.path = Path(path).expanduser().absolute()
        self.error: str | None = None
        self.instance_id = uuid.uuid4().hex
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.timeout = float(timeout)

        self._lock_path = self.path.with_name(f"{self.path.name}.lock")
        self._queue: queue.Queue[_QueuedRecord | object] = queue.Queue(maxsize=queue_size)
        self._state_lock = threading.Lock()
        self._start_ready = threading.Event()
        self._state = "new"
        self._worker: threading.Thread | None = None
        self._next_sequence = 0
        self._sequence_lock = threading.Lock()

    async def start(self) -> None:
        """Prepare secure files and start the writer exactly once."""
        try:
            await asyncio.to_thread(self._ensure_started)
        except _AuditWriteFailed:
            raise self._unavailable() from None

    async def record(self, event: dict[str, Any]) -> None:
        """Queue an event and wait for the worker's durable-write acknowledgement."""
        await self.start()
        queued = self._make_record(event)
        if queued is None:
            raise self._unavailable()

        accepted = await asyncio.to_thread(self._put_with_timeout, queued)
        if not accepted:
            self._mark_failed()
            raise self._unavailable()

        acknowledgement = asyncio.wrap_future(queued.acknowledgement)
        acknowledgement.add_done_callback(self._consume_future_exception)
        try:
            await asyncio.wait_for(asyncio.shield(acknowledgement), timeout=self.timeout)
        except TimeoutError:
            # Do not cancel the thread-owned Future.  Its done callback consumes any
            # late exception, while the sticky error prevents future business actions.
            self._mark_failed()
            raise self._unavailable() from None
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except BaseException:
            self._mark_failed()
            raise self._unavailable() from None

    def emit_nowait(self, event: dict[str, Any]) -> bool:
        """Attempt a non-blocking enqueue without doing file I/O.

        This is used by expiry/cleanup code where awaiting is not appropriate.  Every
        acknowledgement has a done callback which retrieves its exception, even though
        this method deliberately does not await it.
        """
        with self._state_lock:
            ready = self._state == "started" and self.error is None
        if not ready:
            return False

        queued = self._make_record(event)
        if queued is None:
            return False
        with self._state_lock:
            if self._state != "started" or self.error is not None:
                return False
            try:
                self._queue.put_nowait(queued)
            except queue.Full:
                self._mark_failed_locked()
                return False
        return True

    async def close(self) -> None:
        """Drain the queue and stop the worker; safe to call repeatedly.

        Shutdown must not replace a primary session/business exception, so writer
        failures are retained in ``error`` and are not raised from this method.
        """
        worker = await asyncio.to_thread(self._begin_close)
        if worker is not None and worker is not threading.current_thread():
            await asyncio.to_thread(worker.join)
        # A writer which fails while close is blocked behind a full queue can
        # exit just before the stop marker is admitted.  Drain that marker (and
        # reject any theoretical record) so close never leaves queue work or an
        # unobserved Future behind.
        await asyncio.to_thread(self._discard_remaining)
        with self._state_lock:
            if self._state != "failed":
                self._state = "closed"

    def _ensure_started(self) -> None:
        leader = False
        with self._state_lock:
            if self.error is not None or self._state in {"failed", "closed", "closing"}:
                raise _AuditWriteFailed()
            if self._state == "started":
                return
            if self._state == "new":
                self._state = "starting"
                leader = True

        if not leader:
            self._start_ready.wait()
            with self._state_lock:
                if self._state == "started" and self.error is None:
                    return
            raise _AuditWriteFailed()

        try:
            self._prepare_storage()
            worker = threading.Thread(
                target=self._worker_main,
                name=f"audit-log-{self.instance_id[:8]}",
                daemon=True,
            )
            with self._state_lock:
                if self._state != "starting":
                    raise _AuditWriteFailed()
                self._worker = worker
                self._state = "started"
            worker.start()
        except BaseException:
            self._mark_failed()
            raise _AuditWriteFailed() from None
        finally:
            self._start_ready.set()

    def _make_record(self, event: dict[str, Any]) -> _QueuedRecord | None:
        if not isinstance(event, dict):
            self._mark_failed()
            return None
        try:
            payload = dict(event)
            with self._sequence_lock:
                self._next_sequence += 1
                sequence = self._next_sequence
            # Reserved audit fields are authoritative; callers cannot forge an
            # instance id or sequence by putting keys in their event dictionary.
            payload["schema_version"] = 1
            payload["server_instance_id"] = self.instance_id
            payload["event_seq"] = sequence
            encoded = (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                + b"\n"
            )
        except (TypeError, ValueError, UnicodeError):
            self._mark_failed()
            return None

        acknowledgement: Future[None] = Future()
        acknowledgement.add_done_callback(self._consume_future_exception)
        return _QueuedRecord(encoded, acknowledgement)

    @staticmethod
    def _consume_future_exception(
        acknowledgement: Future[None] | asyncio.Future[None],
    ) -> None:
        """Retrieve a background exception even when no coroutine awaits it."""
        try:
            acknowledgement.exception()
        except BaseException:
            # Cancelled futures also raise here; there is nothing public to report.
            pass

    def _put_with_timeout(self, queued: _QueuedRecord) -> bool:
        with self._state_lock:
            if self._state != "started" or self.error is not None:
                return False
            try:
                self._queue.put(queued, block=True, timeout=self.timeout)
            except queue.Full:
                return False
        return True

    def _begin_close(self) -> threading.Thread | None:
        # A concurrent start performs preparation on another thread.  Waiting here is
        # safe (this helper itself is called through asyncio.to_thread).
        with self._state_lock:
            starting = self._state == "starting"
        if starting:
            self._start_ready.wait()

        enqueue_stop = False
        with self._state_lock:
            if self._state in {"new", "closed"}:
                self._state = "closed"
                return None
            worker = self._worker
            if self._state == "closing":
                return worker
            if self._state == "failed":
                # A caller can mark the logger failed after an acknowledgement
                # timeout while the worker is still blocked in a slow write.  It
                # still needs a sentinel so close can join it instead of leaving a
                # live thread blocked in Queue.get().
                if worker is not None and worker.is_alive():
                    self._state = "closing"
                    enqueue_stop = True
            else:
                self._state = "closing"
                # State becomes closing before the sentinel is put, making close a
                # queue barrier: no producer can appear behind it.  Do not keep the
                # state lock while Queue.put waits, because a failed writer also needs
                # it to publish its error.
                enqueue_stop = True
        if enqueue_stop:
            self._queue.put(_STOP)
        return worker

    def _worker_main(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is _STOP:
                    return
                assert isinstance(item, _QueuedRecord)
                if self.error is not None:
                    self._reject_pending(item)
                    return
                try:
                    self._write_line(item.line)
                except BaseException:
                    self._mark_failed()
                    if not item.acknowledgement.done():
                        item.acknowledgement.set_exception(_AuditWriteFailed())
                    self._reject_pending()
                    return
                if not item.acknowledgement.done():
                    item.acknowledgement.set_result(None)
            finally:
                self._queue.task_done()

    def _reject_pending(self, first: _QueuedRecord | None = None) -> None:
        if first is not None and not first.acknowledgement.done():
            first.acknowledgement.set_exception(_AuditWriteFailed())
        while True:
            try:
                pending = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                if isinstance(pending, _QueuedRecord) and not pending.acknowledgement.done():
                    pending.acknowledgement.set_exception(_AuditWriteFailed())
            finally:
                self._queue.task_done()

    def _discard_remaining(self) -> None:
        """Empty residual queue items after the worker has been joined."""
        while True:
            try:
                pending = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                if isinstance(pending, _QueuedRecord) and not pending.acknowledgement.done():
                    pending.acknowledgement.set_exception(_AuditWriteFailed())
            finally:
                self._queue.task_done()

    def _mark_failed(self) -> None:
        with self._state_lock:
            self._mark_failed_locked()

    def _mark_failed_locked(self) -> None:
        self.error = _AUDIT_ERROR
        if self._state != "closed":
            self._state = "failed"

    @staticmethod
    def _unavailable() -> SessionError:
        return SessionError(_AUDIT_ERROR, _AUDIT_MESSAGE)

    def _prepare_storage(self) -> None:
        self._ensure_log_directory()
        log_fd = self._open_secure(self.path, os.O_WRONLY | os.O_APPEND)
        try:
            pass
        finally:
            os.close(log_fd)
        lock_fd = self._open_secure(self._lock_path, os.O_RDWR)
        try:
            pass
        finally:
            os.close(lock_fd)

    def _ensure_log_directory(self) -> None:
        directory = self.path.parent
        created = False
        try:
            os.mkdir(directory, 0o700)
            created = True
        except FileExistsError:
            pass
        except FileNotFoundError:
            # ``mkdir(..., parents=True)`` only occurs for a missing configured log
            # directory; the final direct parent is explicitly hardened afterwards.
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            created = True
        if created:
            os.chmod(directory, 0o700)
        info = os.lstat(directory)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise OSError(errno.ELOOP, "unsafe audit directory")
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise OSError(errno.EACCES, "unsafe audit directory")

    @staticmethod
    def _validate_secure_regular(info: os.stat_result) -> None:
        if not stat.S_ISREG(info.st_mode):
            raise OSError(errno.EINVAL, "audit target is not a regular file")
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise OSError(errno.EACCES, "unsafe audit file permissions")

    def _open_secure(self, target: Path, flags: int) -> int:
        """Open a user-owned 0600 regular file without following a final symlink."""
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        create_flags = flags | os.O_CREAT | os.O_EXCL | nofollow
        existing_info: os.stat_result | None = None
        try:
            descriptor = os.open(target, create_flags, 0o600)
        except FileExistsError:
            # lstat makes a symlink rejection deterministic, including before an
            # O_NOFOLLOW error which varies by platform.
            existing_info = os.lstat(target)
            self._validate_secure_regular(existing_info)
            descriptor = os.open(target, flags | nofollow)
        try:
            # Explicitly set the new file's mode; umask may only remove bits, but the
            # public contract calls for exactly 0600.  Existing files are never chmod'd.
            opened_info = os.fstat(descriptor)
            if existing_info is not None and (
                opened_info.st_dev != existing_info.st_dev
                or opened_info.st_ino != existing_info.st_ino
            ):
                raise OSError(errno.EAGAIN, "audit target changed during open")
            if existing_info is None and stat.S_IMODE(opened_info.st_mode) != 0o600:
                # This branch can only be a newly-created file with a restrictive
                # umask, because existing files were validated before opening.
                os.fchmod(descriptor, 0o600)
            self._validate_secure_regular(os.fstat(descriptor))
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _acquire_rotation_lock(self) -> int:
        descriptor = self._open_secure(self._lock_path, os.O_RDWR)
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return descriptor
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("audit lock acknowledgement timed out")
                    time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        except BaseException:
            os.close(descriptor)
            raise

    def _write_line(self, line: bytes) -> None:
        lock_descriptor = self._acquire_rotation_lock()
        try:
            log_descriptor = self._open_secure(self.path, os.O_WRONLY | os.O_APPEND)
            try:
                current_size = os.fstat(log_descriptor).st_size
                if current_size and current_size + len(line) > self.max_bytes:
                    os.close(log_descriptor)
                    log_descriptor = -1
                    self._rotate_locked()
                    log_descriptor = self._open_secure(self.path, os.O_WRONLY | os.O_APPEND)
                self._write_all(log_descriptor, line)
                # An acknowledgement means the event was flushed to the filesystem,
                # not merely copied into Python's queue or a buffered file object.
                os.fsync(log_descriptor)
            finally:
                if log_descriptor >= 0:
                    os.close(log_descriptor)
        finally:
            try:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(lock_descriptor)

    @staticmethod
    def _write_all(descriptor: int, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError(errno.EIO, "short audit write")
            view = view[written:]

    def _rotate_locked(self) -> None:
        backups = [
            self.path.with_name(f"{self.path.name}.{number}")
            for number in range(1, self.backup_count + 1)
        ]
        # Validate every file which might be replaced before mutating any names.  This
        # rejects attacker-supplied symlinks and non-owner/weak-mode backups.
        for backup in backups:
            try:
                self._validate_secure_regular(os.lstat(backup))
            except FileNotFoundError:
                continue
        self._validate_secure_regular(os.lstat(self.path))

        if self.backup_count == 0:
            os.unlink(self.path)
            return
        for number in range(self.backup_count - 1, 0, -1):
            source = self.path.with_name(f"{self.path.name}.{number}")
            destination = self.path.with_name(f"{self.path.name}.{number + 1}")
            try:
                os.replace(source, destination)
            except FileNotFoundError:
                continue
        os.replace(self.path, backups[0])

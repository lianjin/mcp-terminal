"""按 Unicode 字符计数的有界输出窗口；读取不会消费内容。"""

from collections import deque
from typing import Any

from mcp_terminal.errors import SessionError


class RingBuffer:
    def __init__(self, capacity_chars: int) -> None:
        if type(capacity_chars) is not int or capacity_chars <= 0:
            raise ValueError("capacity_chars must be a positive integer")
        self.capacity_chars = capacity_chars
        self._chunks: deque[str] = deque()
        self.earliest_cursor = 0
        self.latest_cursor = 0

    @property
    def size(self) -> int:
        return self.latest_cursor - self.earliest_cursor

    def append(self, text: str) -> None:
        if not isinstance(text, str):
            raise TypeError("text must be str")
        if not text:
            return
        self.latest_cursor += len(text)
        # 超大单次输出只保留尾部，避免将它作为长期 chunk 留在内存。
        if len(text) >= self.capacity_chars:
            self._chunks.clear()
            self._chunks.append(text[-self.capacity_chars :])
            self.earliest_cursor = self.latest_cursor - self.capacity_chars
            return
        if self._chunks and len(self._chunks[-1]) < 4096:
            self._chunks.append(self._chunks.pop() + text)
        else:
            self._chunks.append(text)
        excess = self.size - self.capacity_chars
        while excess > 0:
            first = self._chunks.popleft()
            count = min(excess, len(first))
            if count < len(first):
                self._chunks.appendleft(first[count:])
            self.earliest_cursor += count
            excess -= count

    def read(self, cursor: int | None = None, max_chars: int = 16384) -> dict[str, Any]:
        if type(max_chars) is not int or not 1 <= max_chars <= 65536:
            raise SessionError("INVALID_ARGUMENT", "max_chars 必须是 1..65536 的整数。")
        if cursor is None:
            cursor = self.earliest_cursor
        if type(cursor) is not int or cursor < 0:
            raise SessionError("INVALID_ARGUMENT", "cursor 必须是非负整数。")
        window = {"earliest_cursor": self.earliest_cursor, "latest_cursor": self.latest_cursor}
        if cursor < self.earliest_cursor:
            raise SessionError(
                "CURSOR_EXPIRED", "cursor 已被新输出覆盖，请从最早 cursor 恢复。", window
            )
        if cursor > self.latest_cursor:
            raise SessionError("CURSOR_AHEAD", "cursor 超过当前输出末尾。", window)
        skip = cursor - self.earliest_cursor
        remaining = max_chars
        parts: list[str] = []
        for chunk in self._chunks:
            if skip >= len(chunk):
                skip -= len(chunk)
                continue
            part = chunk[skip : skip + remaining]
            parts.append(part)
            remaining -= len(part)
            skip = 0
            if remaining == 0:
                break
        output = "".join(parts)
        next_cursor = cursor + len(output)
        return {
            "output": output,
            "cursor": cursor,
            "next_cursor": next_cursor,
            **window,
            "has_more": next_cursor < self.latest_cursor,
        }

    def clear(self) -> None:
        self._chunks.clear()
        self.earliest_cursor = self.latest_cursor

"""RingBuffer 的 Unicode 游标、分页及错误边界测试。"""

import pytest

from mcp_terminal.buffer import RingBuffer
from mcp_terminal.errors import SessionError


def assert_session_error(exc_info: pytest.ExceptionInfo[SessionError], code: str) -> SessionError:
    error = exc_info.value
    assert error.code == code
    assert error.message
    assert isinstance(error.details, dict)
    return error


def test_unicode_offsets_are_characters_and_pages_are_replayable() -> None:
    buffer = RingBuffer(capacity_chars=5)
    buffer.append("a😀界bc")

    assert buffer.earliest_cursor == 0
    assert buffer.latest_cursor == 5
    assert buffer.size == 5
    assert buffer.capacity_chars == 5

    first = buffer.read(cursor=0, max_chars=2)
    assert first == {
        "output": "a😀",
        "cursor": 0,
        "next_cursor": 2,
        "earliest_cursor": 0,
        "latest_cursor": 5,
        "has_more": True,
    }
    # read 不消费输出；相同 cursor 可以被多个调用者重放。
    assert buffer.read(cursor=0, max_chars=2) == first

    second = buffer.read(cursor=first["next_cursor"], max_chars=2)
    assert second["output"] == "界b"
    assert second["cursor"] == 2
    assert second["next_cursor"] == 4
    assert second["has_more"] is True

    last = buffer.read(cursor=second["next_cursor"], max_chars=2)
    assert last["output"] == "c"
    assert last["next_cursor"] == 5
    assert last["has_more"] is False

    tail = buffer.read(cursor=buffer.latest_cursor, max_chars=1)
    assert tail["output"] == ""
    assert tail["cursor"] == tail["next_cursor"] == 5
    assert tail["has_more"] is False


def test_overwrite_expires_old_cursor_and_default_starts_at_earliest() -> None:
    buffer = RingBuffer(capacity_chars=5)
    buffer.append("a😀界bc")
    buffer.append("é")

    assert buffer.earliest_cursor == 1
    assert buffer.latest_cursor == 6
    assert buffer.size == 5

    with pytest.raises(SessionError) as caught:
        buffer.read(cursor=0)
    error = assert_session_error(caught, "CURSOR_EXPIRED")
    assert error.details == {"earliest_cursor": 1, "latest_cursor": 6}

    default = buffer.read()
    assert default["cursor"] == 1
    assert default["output"] == "😀界bcé"
    assert default["next_cursor"] == 6
    assert default["has_more"] is False

    with pytest.raises(SessionError) as caught:
        buffer.read(cursor=7)
    error = assert_session_error(caught, "CURSOR_AHEAD")
    assert error.details == {"earliest_cursor": 1, "latest_cursor": 6}


def test_invalid_cursor_and_page_size_types_are_rejected_strictly() -> None:
    buffer = RingBuffer(capacity_chars=8)
    buffer.append("text")

    for cursor in (-1, True, False, 1.5, "1"):
        with pytest.raises(SessionError) as caught:
            buffer.read(cursor=cursor)  # type: ignore[arg-type]
        assert_session_error(caught, "INVALID_ARGUMENT")

    for max_chars in (0, -1, True, False, 1.5, "2", 65537):
        with pytest.raises(SessionError) as caught:
            buffer.read(max_chars=max_chars)  # type: ignore[arg-type]
        assert_session_error(caught, "INVALID_ARGUMENT")


def test_clear_resets_window_without_rewinding_absolute_offsets() -> None:
    buffer = RingBuffer(capacity_chars=4)
    buffer.append("abcd")
    buffer.append("e")
    assert (buffer.earliest_cursor, buffer.latest_cursor) == (1, 5)

    buffer.clear()
    assert buffer.size == 0
    assert buffer.earliest_cursor == buffer.latest_cursor == 5
    assert buffer.read() == {
        "output": "",
        "cursor": 5,
        "next_cursor": 5,
        "earliest_cursor": 5,
        "latest_cursor": 5,
        "has_more": False,
    }

    buffer.append("中")
    assert buffer.read(cursor=5)["output"] == "中"
    assert buffer.latest_cursor == 6

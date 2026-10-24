"""不会泄漏内部异常或敏感输入的公共业务错误。"""

from typing import Any


class SessionError(Exception):
    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

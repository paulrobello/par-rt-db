"""Error envelope, codes, and the precondition-retry helper."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    """Wire error codes (SCREAMING_SNAKE_CASE), each mapped to an HTTP status."""

    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    CONFLICT = "CONFLICT"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    BAD_REQUEST = "BAD_REQUEST"
    INTERNAL = "INTERNAL"


_STATUS: dict[ErrorCode, int] = {
    ErrorCode.BAD_REQUEST: 400,
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.FORBIDDEN: 403,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.PRECONDITION_FAILED: 409,
    ErrorCode.CONFLICT: 409,
    ErrorCode.SCHEMA_VIOLATION: 422,
    ErrorCode.INTERNAL: 500,
}


class RtDbError(Exception):
    """The single client error type. Mirrors the server's ``{code, message}`` envelope."""

    code: ErrorCode
    message: str

    def __init__(self, code: ErrorCode, message: str) -> None:
        self.code = code if isinstance(code, ErrorCode) else ErrorCode(code)
        self.message = message
        super().__init__(f"{self.code.value}: {message}")

    @property
    def status_code(self) -> int:
        """HTTP status this code maps to."""
        return _STATUS[self.code]

    @classmethod
    def from_envelope(cls, envelope: dict[str, Any]) -> RtDbError:
        """Build from a parsed ``{code, message}`` body."""
        try:
            code = ErrorCode(envelope.get("code", "INTERNAL"))
        except ValueError:
            code = ErrorCode.INTERNAL
        return cls(code, str(envelope.get("message", "")))

    @classmethod
    def from_http(cls, status: int, body: bytes | str | None) -> RtDbError:
        """Non-2xx response -> RtDbError. Parses ``{code,message}`` if present."""
        if body is None:
            return cls(ErrorCode.INTERNAL, f"request failed with status {status}")
        raw = body.decode("utf-8") if isinstance(body, bytes) else body
        try:
            env = json.loads(raw)
        except (ValueError, TypeError):
            return cls(ErrorCode.INTERNAL, f"request failed with status {status}")
        if isinstance(env, dict) and "code" in env:
            # The body's {code, message} envelope is authoritative.
            return cls.from_envelope(env)
        return cls(ErrorCode.INTERNAL, f"request failed with status {status}")


async def retry_on_precondition[T](
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 5,
) -> T:
    """Call ``fn`` until it succeeds, retrying only on PRECONDITION_FAILED (OCC)."""
    last: RtDbError | None = None
    for _ in range(max_attempts):
        try:
            return await fn()
        except RtDbError as err:
            last = err
            if err.code is not ErrorCode.PRECONDITION_FAILED:
                raise
    assert last is not None
    raise last

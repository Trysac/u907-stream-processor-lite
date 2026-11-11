"""Error definitions for stream_processor_lite."""

from __future__ import annotations

from typing import Literal

ErrorCode = Literal[
    "invalid_window",
    "bad_event",
    "line_too_long",
    "state_too_large",
    "corrupt_checkpoint",
]


class StreamError(Exception):
    """Base exception raised by stream_processor_lite operations."""

    def __init__(self, code: ErrorCode, message: str = "", *, line: int | None = None) -> None:
        self.code: ErrorCode = code
        self.line: int | None = line
        safe_msg = message[:200] if message else code
        if line is not None and f"line {line}" not in safe_msg:
            full_msg = f"[{code}] (line {line}): {safe_msg}"
        else:
            full_msg = f"[{code}]: {safe_msg}"
        super().__init__(full_msg)

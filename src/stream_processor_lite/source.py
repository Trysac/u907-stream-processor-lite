"""Replayable JSONL input source."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .errors import StreamError


class JsonlSource:
    """Line-oriented JSON source supporting byte limits and line-offset replay."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        max_line_bytes: int = 65_536,
    ) -> None:
        if (
            isinstance(max_line_bytes, bool)
            or not isinstance(max_line_bytes, int)
            or max_line_bytes < 1
        ):
            raise ValueError("max_line_bytes must be a positive integer")
        self.path: Path = Path(path)
        self.max_line_bytes: int = max_line_bytes

    def iter_from(self, start_line: int = 0) -> Iterator[tuple[int, str, dict[str, Any]]]:
        """Yield (line_no, raw_line, parsed_dict) skipping first start_line lines."""
        if start_line < 0:
            raise ValueError("start_line must be >= 0")
        if not self.path.exists():
            raise FileNotFoundError(f"Source file not found: {self.path}")

        with self.path.open("rb") as f:
            line_no = 0
            for raw_bytes in f:
                line_no += 1
                if line_no <= start_line:
                    continue
                # Strip trailing CR/LF before checking byte length against limit
                stripped_bytes = raw_bytes.rstrip(b"\r\n")
                if len(stripped_bytes) > self.max_line_bytes:
                    raise StreamError(
                        "line_too_long",
                        f"Line {line_no} exceeds {self.max_line_bytes} bytes",
                        line=line_no,
                    )
                try:
                    text = stripped_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise StreamError(
                        "bad_event",
                        f"Invalid UTF-8 on line {line_no}",
                        line=line_no,
                    ) from exc

                if not text.strip():
                    raise StreamError(
                        "bad_event",
                        f"Empty line {line_no}",
                        line=line_no,
                    )

                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise StreamError(
                        "bad_event",
                        f"Malformed JSON on line {line_no}",
                        line=line_no,
                    ) from exc

                if not isinstance(parsed, dict):
                    raise StreamError(
                        "bad_event",
                        f"Event on line {line_no} must be a JSON object",
                        line=line_no,
                    )

                yield line_no, text, parsed

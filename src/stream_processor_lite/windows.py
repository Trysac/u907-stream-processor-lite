"""Window specification dataclasses."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import StreamError


def _is_valid_int(val: object) -> bool:
    return isinstance(val, int) and not isinstance(val, bool)


@dataclass(frozen=True)
class Tumbling:
    """Fixed-size, non-overlapping, contiguous time intervals."""

    size: int

    def __post_init__(self) -> None:
        if not _is_valid_int(self.size) or self.size < 1:
            raise StreamError(
                "invalid_window",
                "Tumbling window size must be an integer >= 1",
            )


@dataclass(frozen=True)
class Sliding:
    """Fixed-size, potentially overlapping time intervals advancing by slide."""

    size: int
    slide: int

    def __post_init__(self) -> None:
        if not _is_valid_int(self.size) or not _is_valid_int(self.slide):
            raise StreamError(
                "invalid_window",
                "Sliding window size and slide must be integers",
            )
        if self.size < 1 or self.slide < 1 or self.slide > self.size:
            raise StreamError(
                "invalid_window",
                "Sliding window requires size >= 1 and 1 <= slide <= size",
            )


@dataclass(frozen=True)
class Session:
    """Dynamic per-key windows separated by periods of inactivity >= gap."""

    gap: int

    def __post_init__(self) -> None:
        if not _is_valid_int(self.gap) or self.gap < 1:
            raise StreamError(
                "invalid_window",
                "Session window gap must be an integer >= 1",
            )


WindowSpec = Tumbling | Sliding | Session

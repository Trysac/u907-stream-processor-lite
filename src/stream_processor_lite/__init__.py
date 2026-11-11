"""Event-time stream windows with watermarks, late-data side output and atomic checkpointing."""

from __future__ import annotations

import logging

from .errors import ErrorCode, StreamError
from .pipeline import Pipeline, batch_compute, read_output
from .source import JsonlSource
from .windows import Session, Sliding, Tumbling, WindowSpec

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "ErrorCode",
    "JsonlSource",
    "Pipeline",
    "Session",
    "Sliding",
    "StreamError",
    "Tumbling",
    "WindowSpec",
    "__version__",
    "batch_compute",
    "read_output",
]

__version__ = "0.1.0"

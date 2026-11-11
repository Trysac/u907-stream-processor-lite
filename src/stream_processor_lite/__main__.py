"""Command-line interface for stream_processor_lite."""

from __future__ import annotations

import argparse
import sys
from typing import NoReturn

from .errors import StreamError
from .pipeline import Pipeline
from .source import JsonlSource
from .windows import Session, Sliding, Tumbling, WindowSpec


class _CliArgumentParser(argparse.ArgumentParser):
    """ArgumentParser that raises ValueError on usage errors instead of calling sys.exit."""

    def error(self, message: str) -> NoReturn:
        raise ValueError(message)


def _parse_window_arg(spec: str) -> WindowSpec:
    parts = spec.split(":")
    kind = parts[0]
    try:
        if kind == "tumbling" and len(parts) == 2:
            size = int(parts[1])
            return Tumbling(size=size)
        if kind == "sliding" and len(parts) == 3:
            size = int(parts[1])
            slide = int(parts[2])
            return Sliding(size=size, slide=slide)
        if kind == "session" and len(parts) == 2:
            gap = int(parts[1])
            return Session(gap=gap)
    except ValueError as exc:
        raise ValueError(f"Invalid window integer parameter in '{spec}'") from exc
    raise ValueError(f"Invalid window specification '{spec}'")


def _build_aggregations(
    count_flag: bool,
    sums: list[str],
    mins: list[str],
    maxs: list[str],
    means: list[str],
) -> dict[str, str]:
    aggs: dict[str, str] = {}
    if count_flag:
        aggs["count"] = "count"

    op_groups = [
        ("sum", sums),
        ("min", mins),
        ("max", maxs),
        ("mean", means),
    ]
    for op_name, fields in op_groups:
        if len(fields) == 1:
            field = fields[0]
            if not field:
                raise ValueError(f"Empty field name for --{op_name}")
            aggs[op_name] = f"{op_name}:{field}"
        elif len(fields) > 1:
            for field in fields:
                if not field:
                    raise ValueError(f"Empty field name for --{op_name}")
                out_key = f"{op_name}_{field}"
                aggs[out_key] = f"{op_name}:{field}"

    if not aggs:
        raise ValueError(
            "At least one aggregation flag (--count, --sum, --min, --max, --mean) is required"
        )
    return aggs


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint. Returns 0 on success, 1 on StreamError, 2 on usage error."""
    args_list = sys.argv[1:] if argv is None else argv

    parser = _CliArgumentParser(prog="stream_processor_lite", add_help=True)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run stream processor on a JSONL file")
    run_parser.add_argument("source", help="Path to input JSONL file")
    run_parser.add_argument(
        "--window",
        required=True,
        help="Window spec: tumbling:<ms>, sliding:<size>:<slide>, session:<gap>",
    )
    run_parser.add_argument("--key", required=True, help="Event key field name")
    run_parser.add_argument(
        "--time-field", default="t", help="Event timestamp field name (default: t)"
    )
    run_parser.add_argument("--count", action="store_true", help="Include event count aggregation")
    run_parser.add_argument(
        "--sum", action="append", default=[], metavar="FIELD", help="Sum aggregation on FIELD"
    )
    run_parser.add_argument(
        "--min", action="append", default=[], metavar="FIELD", help="Min aggregation on FIELD"
    )
    run_parser.add_argument(
        "--max", action="append", default=[], metavar="FIELD", help="Max aggregation on FIELD"
    )
    run_parser.add_argument(
        "--mean", action="append", default=[], metavar="FIELD", help="Mean aggregation on FIELD"
    )
    run_parser.add_argument(
        "--bound", type=int, default=0, help="Max out-of-orderness in ms (default: 0)"
    )
    run_parser.add_argument("--out", required=True, help="Output directory path")

    try:
        parsed = parser.parse_args(args_list)
        if parsed.command != "run":
            return 2
        window_spec = _parse_window_arg(parsed.window)
        aggs = _build_aggregations(
            count_flag=bool(parsed.count),
            sums=list(parsed.sum),
            mins=list(parsed.min),
            maxs=list(parsed.max),
            means=list(parsed.mean),
        )
        if parsed.bound < 0:
            return 2
    except (ValueError, SystemExit) as exc:
        if isinstance(exc, SystemExit) and exc.code == 0:
            return 0
        return 2
    except StreamError:
        return 2

    try:
        pipeline = Pipeline(
            window_spec,
            key_field=parsed.key,
            time_field=parsed.time_field,
            aggregations=aggs,
            max_out_of_orderness=parsed.bound,
            output_dir=parsed.out,
        )
        source = JsonlSource(parsed.source)
        written = pipeline.run(source)
        print(written)
        return 0
    except StreamError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except OSError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

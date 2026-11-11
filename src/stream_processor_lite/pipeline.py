"""Core stream processing pipeline, window state, checkpointing and batch oracle."""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import StreamError
from .source import JsonlSource
from .windows import Session, Sliding, Tumbling, WindowSpec

logger = logging.getLogger(__name__)

_VALID_AGG_OPS = frozenset({"count", "sum", "min", "max", "mean"})
_CHECKPOINT_RE = re.compile(r"^checkpoint-(\d+)\.json$")
_PART_RE = re.compile(r"^part-(\d+)\.jsonl$")
_LATE_PART_RE = re.compile(r"^late-(\d+)\.jsonl$")


@dataclass(frozen=True)
class AggSpec:
    """Parsed specification for a single aggregation."""

    out_name: str
    op: str
    field: str | None


def _parse_aggregations(aggregations: Mapping[str, str]) -> list[AggSpec]:
    if not aggregations:
        raise StreamError("invalid_window", "At least one aggregation must be specified")
    specs: list[AggSpec] = []
    for out_name, spec_str in aggregations.items():
        if not out_name or not isinstance(out_name, str) or not isinstance(spec_str, str):
            raise StreamError("invalid_window", "Invalid aggregation name or specification")
        if out_name in ("key", "start", "end"):
            raise StreamError("invalid_window", f"Aggregation output name '{out_name}' is reserved")
        if spec_str == "count":
            specs.append(AggSpec(out_name=out_name, op="count", field=None))
        elif ":" in spec_str:
            op, field = spec_str.split(":", 1)
            if op not in ("sum", "min", "max", "mean") or not field:
                raise StreamError("invalid_window", f"Invalid aggregation spec '{spec_str}'")
            specs.append(AggSpec(out_name=out_name, op=op, field=field))
        else:
            raise StreamError("invalid_window", f"Invalid aggregation spec '{spec_str}'")
    return specs


class AggAccumulator:
    """Mutable accumulator for a single aggregation inside a window."""

    def __init__(self, op: str) -> None:
        self.op: str = op
        self.count: int = 0
        self.sum_val: int | float = 0
        self.has_float: bool = False
        self.min_val: int | float | None = None
        self.max_val: int | float | None = None

    def add(self, value: int | float | None) -> None:
        self.count += 1
        if self.op == "count":
            return
        if value is None:
            raise ValueError("AggAccumulator value cannot be None for numeric operations")
        if isinstance(value, float):
            self.has_float = True
        self.sum_val += value
        if self.min_val is None or value < self.min_val:
            self.min_val = value
        if self.max_val is None or value > self.max_val:
            self.max_val = value

    def merge(self, other: AggAccumulator) -> None:
        self.count += other.count
        if self.op == "count":
            return
        self.has_float = self.has_float or other.has_float
        self.sum_val += other.sum_val
        if other.min_val is not None and (self.min_val is None or other.min_val < self.min_val):
            self.min_val = other.min_val
        if other.max_val is not None and (self.max_val is None or other.max_val > self.max_val):
            self.max_val = other.max_val

    def result(self) -> int | float:
        if self.op == "count":
            return self.count
        if self.op == "sum":
            return float(self.sum_val) if self.has_float else int(self.sum_val)
        if self.op == "min":
            if self.min_val is None:
                raise ValueError("Accumulator has no min value")
            return self.min_val
        if self.op == "max":
            if self.max_val is None:
                raise ValueError("Accumulator has no max value")
            return self.max_val
        if self.op == "mean":
            return float(self.sum_val) / float(self.count)
        raise AssertionError(f"Unexpected op: {self.op}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "count": self.count,
            "sum_val": self.sum_val,
            "has_float": self.has_float,
            "min_val": self.min_val,
            "max_val": self.max_val,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AggAccumulator:
        op = data.get("op")
        count = data.get("count")
        sum_val = data.get("sum_val")
        has_float = data.get("has_float")
        min_val = data.get("min_val")
        max_val = data.get("max_val")
        if (
            not isinstance(op, str)
            or op not in _VALID_AGG_OPS
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 1
            or isinstance(sum_val, bool)
            or not isinstance(sum_val, (int, float))
            or not isinstance(has_float, bool)
        ):
            raise StreamError("corrupt_checkpoint", "Invalid accumulator data in checkpoint")
        if op != "count" and (
            min_val is None
            or isinstance(min_val, bool)
            or not isinstance(min_val, (int, float))
            or max_val is None
            or isinstance(max_val, bool)
            or not isinstance(max_val, (int, float))
        ):
            raise StreamError("corrupt_checkpoint", "Invalid min/max in checkpoint")
        acc = cls(op=op)
        acc.count = count
        acc.sum_val = float(sum_val) if has_float else int(sum_val)
        acc.has_float = has_float
        acc.min_val = min_val
        acc.max_val = max_val
        return acc


class WindowState:
    """State of a single open window (key, start, end)."""

    def __init__(
        self,
        key: str | int,
        start: int,
        end: int,
        agg_specs: list[AggSpec],
        accs: dict[str, AggAccumulator] | None = None,
    ) -> None:
        self.key: str | int = key
        self.start: int = start
        self.end: int = end
        self.accs: dict[str, AggAccumulator] = (
            accs
            if accs is not None
            else {spec.out_name: AggAccumulator(spec.op) for spec in agg_specs}
        )

    def add_event(
        self, extracted_fields: dict[str, int | float | None], agg_specs: list[AggSpec]
    ) -> None:
        for spec in agg_specs:
            val = extracted_fields[spec.field] if spec.field is not None else None
            self.accs[spec.out_name].add(val)

    def merge(self, other: WindowState) -> None:
        self.start = min(self.start, other.start)
        self.end = max(self.end, other.end)
        for out_name, acc in self.accs.items():
            acc.merge(other.accs[out_name])

    def to_output_dict(self, agg_specs: list[AggSpec]) -> dict[str, Any]:
        res: dict[str, Any] = {
            "key": self.key,
            "start": self.start,
            "end": self.end,
        }
        for spec in agg_specs:
            res[spec.out_name] = self.accs[spec.out_name].result()
        return res

    def to_checkpoint_dict(self) -> dict[str, Any]:
        return {
            "key_type": "int" if isinstance(self.key, int) else "str",
            "key": self.key,
            "start": self.start,
            "end": self.end,
            "accs": {k: v.to_dict() for k, v in self.accs.items()},
        }

    @classmethod
    def from_checkpoint_dict(cls, data: dict[str, Any], agg_specs: list[AggSpec]) -> WindowState:
        if not isinstance(data, dict):
            raise StreamError("corrupt_checkpoint", "Window entry must be a dict")
        key_type = data.get("key_type")
        key = data.get("key")
        start = data.get("start")
        end = data.get("end")
        accs_raw = data.get("accs")
        if (
            key_type not in ("int", "str")
            or isinstance(start, bool)
            or not isinstance(start, int)
            or isinstance(end, bool)
            or not isinstance(end, int)
            or end <= start
            or not isinstance(accs_raw, dict)
        ):
            raise StreamError("corrupt_checkpoint", "Invalid window metadata in checkpoint")
        if key_type == "int":
            if isinstance(key, bool) or not isinstance(key, int):
                raise StreamError("corrupt_checkpoint", "Mismatched int key in checkpoint")
        else:
            if not isinstance(key, str):
                raise StreamError("corrupt_checkpoint", "Mismatched str key in checkpoint")

        accs: dict[str, AggAccumulator] = {}
        for spec in agg_specs:
            if spec.out_name not in accs_raw or not isinstance(accs_raw[spec.out_name], dict):
                raise StreamError("corrupt_checkpoint", f"Missing accumulator '{spec.out_name}'")
            acc = AggAccumulator.from_dict(accs_raw[spec.out_name])
            if acc.op != spec.op:
                raise StreamError("corrupt_checkpoint", "Accumulator op mismatch in checkpoint")
            accs[spec.out_name] = acc

        return cls(key=key, start=start, end=end, agg_specs=agg_specs, accs=accs)


def _InternalKey(key: str | int) -> tuple[int, str | int]:
    """Distinguish int 1 and str '1' cleanly in dictionaries and sort keys."""
    # Type tag: 0 for int, 1 for str
    return (0 if isinstance(key, int) else 1, key)


def _output_sort_key(w: WindowState) -> tuple[int, int, str, int]:
    """Deterministic sort key: (end, start, str(key), type_tiebreaker)."""
    return (w.end, w.start, str(w.key), 0 if isinstance(w.key, int) else 1)


def _validate_and_extract_event(
    event: Mapping[str, Any],
    key_field: str,
    time_field: str,
    agg_specs: list[AggSpec],
    line: int | None,
) -> tuple[int, str | int, dict[str, int | float | None]]:
    if time_field not in event:
        raise StreamError("bad_event", f"Missing time field '{time_field}'", line=line)
    t = event[time_field]
    if isinstance(t, bool) or not isinstance(t, int) or t < 0:
        raise StreamError(
            "bad_event",
            f"Field '{time_field}' must be an integer >= 0",
            line=line,
        )

    if key_field not in event:
        raise StreamError("bad_event", f"Missing key field '{key_field}'", line=line)
    k = event[key_field]
    if isinstance(k, bool) or not isinstance(k, (str, int)):
        raise StreamError(
            "bad_event",
            f"Field '{key_field}' must be str or int",
            line=line,
        )

    extracted: dict[str, int | float | None] = {}
    for spec in agg_specs:
        if spec.field is not None and spec.field not in extracted:
            if spec.field not in event:
                raise StreamError(
                    "bad_event",
                    f"Missing aggregation field '{spec.field}'",
                    line=line,
                )
            val = event[spec.field]
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                raise StreamError(
                    "bad_event",
                    f"Aggregation field '{spec.field}' must be int or float",
                    line=line,
                )
            extracted[spec.field] = val

    return t, k, extracted


def _fsync_file(path: Path) -> None:
    with path.open("rb") as f:
        os.fsync(f.fileno())


def _write_and_fsync(path: Path, content: str) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())


class _StreamEngine:
    """In-memory state manager for open windows and watermark progression."""

    def __init__(
        self,
        window: WindowSpec,
        agg_specs: list[AggSpec],
        max_out_of_orderness: int,
        max_open_windows: int,
    ) -> None:
        self.window = window
        self.agg_specs = agg_specs
        self.max_out_of_orderness = max_out_of_orderness
        self.max_open_windows = max_open_windows
        self.watermark: int | None = None
        # Map from ((type_tag, key), start, end) -> WindowState
        self.open_windows: dict[tuple[tuple[int, str | int], int, int], WindowState] = {}

    def process_event(
        self,
        t: int,
        key: str | int,
        extracted: dict[str, int | float | None],
        line: int | None,
    ) -> bool:
        """Process event; return True if accepted into windows, False if late."""
        if self.watermark is not None and t < self.watermark:
            return False

        candidate_watermark = t - self.max_out_of_orderness
        if self.watermark is None or candidate_watermark > self.watermark:
            self.watermark = candidate_watermark

        ik = _InternalKey(key)

        if isinstance(self.window, Tumbling):
            size = self.window.size
            start = (t // size) * size
            end = start + size
            w_id = (ik, start, end)
            w = self.open_windows.get(w_id)
            if w is None:
                if len(self.open_windows) >= self.max_open_windows:
                    raise StreamError(
                        "state_too_large",
                        f"Exceeded max_open_windows ({self.max_open_windows})",
                        line=line,
                    )
                w = WindowState(key=key, start=start, end=end, agg_specs=self.agg_specs)
                self.open_windows[w_id] = w
            w.add_event(extracted, self.agg_specs)

        elif isinstance(self.window, Sliding):
            size = self.window.size
            slide = self.window.slide
            last_start = (t // slide) * slide
            first_start = last_start
            while first_start - slide >= 0 and first_start - slide + size > t:
                first_start -= slide

            starts = range(first_start, last_start + 1, slide)
            new_needed = sum(1 for s in starts if (ik, s, s + size) not in self.open_windows)
            if len(self.open_windows) + new_needed > self.max_open_windows:
                raise StreamError(
                    "state_too_large",
                    f"Exceeded max_open_windows ({self.max_open_windows})",
                    line=line,
                )
            for s in starts:
                end = s + size
                w_id = (ik, s, end)
                w = self.open_windows.get(w_id)
                if w is None:
                    w = WindowState(key=key, start=s, end=end, agg_specs=self.agg_specs)
                    self.open_windows[w_id] = w
                w.add_event(extracted, self.agg_specs)

        elif isinstance(self.window, Session):
            gap = self.window.gap
            new_start = t
            new_end = t + gap
            new_w = WindowState(key=key, start=new_start, end=new_end, agg_specs=self.agg_specs)
            new_w.add_event(extracted, self.agg_specs)

            overlapping_ids: list[tuple[tuple[int, str | int], int, int]] = []
            for w_id, existing in self.open_windows.items():
                if w_id[0] == ik and existing.start < new_w.end and new_w.start < existing.end:
                    overlapping_ids.append(w_id)

            if not overlapping_ids:
                if len(self.open_windows) >= self.max_open_windows:
                    raise StreamError(
                        "state_too_large",
                        f"Exceeded max_open_windows ({self.max_open_windows})",
                        line=line,
                    )
                self.open_windows[(ik, new_w.start, new_w.end)] = new_w
            else:
                for w_id in overlapping_ids:
                    existing = self.open_windows.pop(w_id)
                    new_w.merge(existing)
                self.open_windows[(ik, new_w.start, new_w.end)] = new_w

        return True

    def pop_ready_windows(self, *, flush_all: bool = False) -> list[WindowState]:
        """Remove and return windows where watermark >= end (or all if flush_all)."""
        if not flush_all and self.watermark is None:
            return []
        wm = self.watermark
        ready: list[WindowState] = []
        to_delete: list[tuple[tuple[int, str | int], int, int]] = []
        for w_id, w in self.open_windows.items():
            if flush_all or (wm is not None and wm >= w.end):
                ready.append(w)
                to_delete.append(w_id)
        for w_id in to_delete:
            del self.open_windows[w_id]
        ready.sort(key=_output_sort_key)
        return ready


class Pipeline:
    """Stateful event-time streaming pipeline with atomic file commits and checkpoint recovery."""

    def __init__(
        self,
        window: WindowSpec,
        *,
        key_field: str,
        time_field: str = "t",
        aggregations: Mapping[str, str],
        max_out_of_orderness: int,
        output_dir: str | os.PathLike[str],
        checkpoint_interval: int = 1000,
        max_open_windows: int = 100_000,
        on_event: Callable[[dict[str, Any]], None] | None = None,
        fail_at: Callable[[str, int], None] | None = None,
    ) -> None:
        if not isinstance(window, (Tumbling, Sliding, Session)):
            raise StreamError("invalid_window", "window must be Tumbling, Sliding, or Session")
        if not key_field or not isinstance(key_field, str):
            raise StreamError("invalid_window", "key_field must be a non-empty string")
        if not time_field or not isinstance(time_field, str):
            raise StreamError("invalid_window", "time_field must be a non-empty string")
        if (
            isinstance(max_out_of_orderness, bool)
            or not isinstance(max_out_of_orderness, int)
            or max_out_of_orderness < 0
        ):
            raise StreamError("invalid_window", "max_out_of_orderness must be an integer >= 0")
        if (
            isinstance(checkpoint_interval, bool)
            or not isinstance(checkpoint_interval, int)
            or checkpoint_interval < 1
        ):
            raise StreamError("invalid_window", "checkpoint_interval must be an integer >= 1")
        if (
            isinstance(max_open_windows, bool)
            or not isinstance(max_open_windows, int)
            or max_open_windows < 1
        ):
            raise StreamError("invalid_window", "max_open_windows must be an integer >= 1")

        self.window: WindowSpec = window
        self.key_field: str = key_field
        self.time_field: str = time_field
        self.aggregations: dict[str, str] = dict(aggregations)
        self.agg_specs: list[AggSpec] = _parse_aggregations(aggregations)
        self.max_out_of_orderness: int = max_out_of_orderness
        self.output_dir: Path = Path(output_dir)
        self.checkpoint_interval: int = checkpoint_interval
        self.max_open_windows: int = max_open_windows
        self.on_event: Callable[[dict[str, Any]], None] | None = on_event
        self.fail_at: Callable[[str, int], None] | None = fail_at

    def _emit_hook(self, kind: str, epoch: int) -> None:
        if self.on_event is not None:
            self.on_event({"kind": kind, "epoch": epoch})

    def _trigger_fail_at(self, step_name: str, epoch: int) -> None:
        if self.fail_at is not None:
            self.fail_at(step_name, epoch)

    def _recover(self, engine: _StreamEngine) -> tuple[int, int, bool]:
        """Recover from output_dir. Returns (last_epoch, offset, completed)."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        highest_epoch = 0
        highest_ckpt_path: Path | None = None

        for entry in self.output_dir.iterdir():
            if not entry.is_file():
                continue
            m = _CHECKPOINT_RE.match(entry.name)
            if m:
                ep = int(m.group(1))
                if ep > highest_epoch:
                    highest_epoch = ep
                    highest_ckpt_path = entry

        offset = 0
        completed = False

        if highest_ckpt_path is not None:
            try:
                raw_text = highest_ckpt_path.read_text(encoding="utf-8")
                ckpt_data = json.loads(raw_text)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StreamError(
                    "corrupt_checkpoint",
                    f"Failed to decode checkpoint {highest_ckpt_path.name}",
                ) from exc

            if not isinstance(ckpt_data, dict):
                raise StreamError("corrupt_checkpoint", "Checkpoint root must be a JSON object")

            ep_val = ckpt_data.get("epoch")
            off_val = ckpt_data.get("offset")
            wm_val = ckpt_data.get("watermark")
            comp_val = ckpt_data.get("completed", False)
            wins_val = ckpt_data.get("windows")

            if (
                ep_val != highest_epoch
                or isinstance(off_val, bool)
                or not isinstance(off_val, int)
                or off_val < 0
                or (
                    wm_val is not None and (isinstance(wm_val, bool) or not isinstance(wm_val, int))
                )
                or not isinstance(comp_val, bool)
                or not isinstance(wins_val, list)
            ):
                raise StreamError("corrupt_checkpoint", "Invalid fields in checkpoint")

            offset = off_val
            completed = comp_val
            engine.watermark = wm_val
            engine.open_windows.clear()

            for w_dict in wins_val:
                w_state = WindowState.from_checkpoint_dict(w_dict, self.agg_specs)
                w_id = (_InternalKey(w_state.key), w_state.start, w_state.end)
                if w_id in engine.open_windows:
                    raise StreamError("corrupt_checkpoint", "Duplicate window in checkpoint")
                engine.open_windows[w_id] = w_state

            if len(engine.open_windows) > self.max_open_windows:
                raise StreamError(
                    "state_too_large",
                    f"Restored state exceeds max_open_windows ({self.max_open_windows})",
                )

            # Roll forward part-e and late-e if their final files are missing
            part_tmp = self.output_dir / f"part-{highest_epoch}.jsonl.tmp"
            part_final = self.output_dir / f"part-{highest_epoch}.jsonl"
            if not part_final.exists():
                if part_tmp.exists():
                    os.replace(part_tmp, part_final)
                else:
                    raise StreamError(
                        "corrupt_checkpoint",
                        f"Missing part file for committed epoch {highest_epoch}",
                    )
            elif part_tmp.exists():
                part_tmp.unlink()

            late_tmp = self.output_dir / f"late-{highest_epoch}.jsonl.tmp"
            late_final = self.output_dir / f"late-{highest_epoch}.jsonl"
            if not late_final.exists():
                if late_tmp.exists():
                    os.replace(late_tmp, late_final)
                else:
                    raise StreamError(
                        "corrupt_checkpoint",
                        f"Missing late file for committed epoch {highest_epoch}",
                    )
            elif late_tmp.exists():
                late_tmp.unlink()

        # Clean up any leftover temp files and any parts/checkpoints above highest_epoch
        for entry in list(self.output_dir.iterdir()):
            if not entry.is_file():
                continue
            name = entry.name
            if name.endswith(".tmp"):
                entry.unlink()
                continue
            for regex in (_PART_RE, _LATE_PART_RE, _CHECKPOINT_RE):
                m = regex.match(name)
                if m and int(m.group(1)) > highest_epoch:
                    entry.unlink()
                    break

        if highest_epoch > 0:
            self._rebuild_late_jsonl()
            self._emit_hook("recovered", highest_epoch)
            logger.debug("Recovered pipeline state at epoch %d (offset %d)", highest_epoch, offset)

        return highest_epoch, offset, completed

    def _rebuild_late_jsonl(self) -> None:
        """Rebuild late.jsonl as the exact concatenation of committed late-e.jsonl parts."""
        late_parts: list[tuple[int, Path]] = []
        for entry in self.output_dir.iterdir():
            if entry.is_file():
                m = _LATE_PART_RE.match(entry.name)
                if m:
                    late_parts.append((int(m.group(1)), entry))
        late_parts.sort(key=lambda item: item[0])

        combined_tmp = self.output_dir / "late.jsonl.tmp"
        combined_final = self.output_dir / "late.jsonl"
        with combined_tmp.open("wb") as out_f:
            for _, part_path in late_parts:
                data = part_path.read_bytes()
                if data:
                    out_f.write(data)
            out_f.flush()
            os.fsync(out_f.fileno())
        os.replace(combined_tmp, combined_final)

    def _commit_epoch(
        self,
        epoch: int,
        offset: int,
        engine: _StreamEngine,
        ready_windows: list[WindowState],
        late_lines: list[str],
        *,
        completed: bool,
    ) -> int:
        """Execute the 4-step atomic commit protocol for epoch e."""
        part_tmp = self.output_dir / f"part-{epoch}.jsonl.tmp"
        part_final = self.output_dir / f"part-{epoch}.jsonl"
        late_tmp = self.output_dir / f"late-{epoch}.jsonl.tmp"
        late_final = self.output_dir / f"late-{epoch}.jsonl"
        ckpt_tmp = self.output_dir / f"checkpoint-{epoch}.json.tmp"
        ckpt_final = self.output_dir / f"checkpoint-{epoch}.json"

        # Step 1: write out/part-e.jsonl.tmp and late-e.jsonl.tmp and fsync
        part_lines = [
            json.dumps(w.to_output_dict(self.agg_specs), separators=(",", ":")) + "\n"
            for w in ready_windows
        ]
        _write_and_fsync(part_tmp, "".join(part_lines))
        _write_and_fsync(late_tmp, "".join(line + "\n" for line in late_lines))
        self._trigger_fail_at("part_written", epoch)

        # Step 2: write checkpoint-e.json.tmp with offset, watermark, open windows, fsync
        ckpt_payload = {
            "epoch": epoch,
            "offset": offset,
            "watermark": engine.watermark,
            "completed": completed,
            "windows": [w.to_checkpoint_dict() for w in engine.open_windows.values()],
        }
        _write_and_fsync(ckpt_tmp, json.dumps(ckpt_payload, separators=(",", ":")))
        self._trigger_fail_at("checkpoint_written", epoch)

        # Step 3: os.replace to checkpoint-e.json (the commit point)
        os.replace(ckpt_tmp, ckpt_final)
        self._trigger_fail_at("committed", epoch)

        # Step 4: os.replace part and late temp files to final names
        os.replace(part_tmp, part_final)
        os.replace(late_tmp, late_final)
        self._rebuild_late_jsonl()
        self._trigger_fail_at("renamed", epoch)

        self._emit_hook("commit", epoch)
        return len(ready_windows)

    def run(self, source: JsonlSource) -> int:
        """Run pipeline from source, recovering from prior checkpoint. Return results written."""
        engine = _StreamEngine(
            window=self.window,
            agg_specs=self.agg_specs,
            max_out_of_orderness=self.max_out_of_orderness,
            max_open_windows=self.max_open_windows,
        )
        last_epoch, offset, completed = self._recover(engine)

        # If already completed and no lines after offset, return 0
        current_epoch = last_epoch + 1
        lines_since_commit = 0
        ready_buffer: list[WindowState] = []
        late_buffer: list[str] = []
        results_written = 0
        current_offset = offset
        saw_any_new_line = False

        for line_no, raw_line, event in source.iter_from(offset):
            saw_any_new_line = True
            t, key, extracted = _validate_and_extract_event(
                event,
                self.key_field,
                self.time_field,
                self.agg_specs,
                line=line_no,
            )
            accepted = engine.process_event(t, key, extracted, line=line_no)
            if not accepted:
                late_buffer.append(raw_line)
                self._emit_hook("late", current_epoch)
            else:
                ready_buffer.extend(engine.pop_ready_windows(flush_all=False))

            current_offset = line_no
            lines_since_commit += 1

            if lines_since_commit >= self.checkpoint_interval:
                ready_buffer.sort(key=_output_sort_key)
                results_written += self._commit_epoch(
                    epoch=current_epoch,
                    offset=current_offset,
                    engine=engine,
                    ready_windows=ready_buffer,
                    late_lines=late_buffer,
                    completed=False,
                )
                current_epoch += 1
                lines_since_commit = 0
                ready_buffer = []
                late_buffer = []

        if completed and not saw_any_new_line:
            return 0

        # End of input: watermark becomes +infinity, firing all remaining open windows
        ready_buffer.extend(engine.pop_ready_windows(flush_all=True))
        ready_buffer.sort(key=_output_sort_key)
        results_written += self._commit_epoch(
            epoch=current_epoch,
            offset=current_offset,
            engine=engine,
            ready_windows=ready_buffer,
            late_lines=late_buffer,
            completed=True,
        )

        return results_written


def read_output(output_dir: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """Read committed output parts in epoch order and ensure late.jsonl is synchronized."""
    out_path = Path(output_dir)
    if not out_path.exists():
        return []

    parts: list[tuple[int, Path]] = []
    late_parts: list[tuple[int, Path]] = []
    for entry in out_path.iterdir():
        if not entry.is_file():
            continue
        m_part = _PART_RE.match(entry.name)
        if m_part:
            parts.append((int(m_part.group(1)), entry))
            continue
        m_late = _LATE_PART_RE.match(entry.name)
        if m_late:
            late_parts.append((int(m_late.group(1)), entry))

    parts.sort(key=lambda item: item[0])
    late_parts.sort(key=lambda item: item[0])

    # Ensure late.jsonl exists as the concatenation of late parts if late parts exist
    if late_parts:
        combined_late = out_path / "late.jsonl"
        expected_bytes = b"".join(p.read_bytes() for _, p in late_parts)
        if not combined_late.exists() or combined_late.read_bytes() != expected_bytes:
            combined_late.write_bytes(expected_bytes)

    results: list[dict[str, Any]] = []
    for _, part_file in parts:
        with part_file.open("r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    results.append(json.loads(stripped))
    return results


def batch_compute(
    events: Iterable[Mapping[str, Any]],
    pipeline_spec: Pipeline | Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Reference batch computation oracle matching Pipeline semantics and ordering."""
    if isinstance(pipeline_spec, Pipeline):
        window = pipeline_spec.window
        key_field = pipeline_spec.key_field
        time_field = pipeline_spec.time_field
        agg_specs = pipeline_spec.agg_specs
        max_ooo = pipeline_spec.max_out_of_orderness
        max_open = pipeline_spec.max_open_windows
        ckpt_interval = pipeline_spec.checkpoint_interval
    elif isinstance(pipeline_spec, Mapping):
        window = pipeline_spec["window"]
        if not isinstance(window, (Tumbling, Sliding, Session)):
            raise StreamError("invalid_window", "Invalid window in pipeline_spec")
        key_field = str(pipeline_spec["key_field"])
        time_field = str(pipeline_spec.get("time_field", "t"))
        agg_specs = _parse_aggregations(pipeline_spec["aggregations"])
        max_ooo = int(pipeline_spec.get("max_out_of_orderness", 0))
        max_open = int(pipeline_spec.get("max_open_windows", 100_000))
        ckpt_interval = int(pipeline_spec.get("checkpoint_interval", 1000))
    else:
        raise StreamError("invalid_window", "pipeline_spec must be a Pipeline or Mapping")

    engine = _StreamEngine(
        window=window,
        agg_specs=agg_specs,
        max_out_of_orderness=max_ooo,
        max_open_windows=max_open,
    )

    all_results: list[dict[str, Any]] = []
    epoch_buffer: list[WindowState] = []
    lines_since_commit = 0

    for idx, ev in enumerate(events, start=1):
        if not isinstance(ev, Mapping):
            raise StreamError("bad_event", f"Event {idx} must be a mapping", line=idx)
        t, key, extracted = _validate_and_extract_event(
            ev,
            key_field,
            time_field,
            agg_specs,
            line=idx,
        )
        accepted = engine.process_event(t, key, extracted, line=idx)
        if accepted:
            epoch_buffer.extend(engine.pop_ready_windows(flush_all=False))

        lines_since_commit += 1
        if lines_since_commit >= ckpt_interval:
            epoch_buffer.sort(key=_output_sort_key)
            all_results.extend(w.to_output_dict(agg_specs) for w in epoch_buffer)
            epoch_buffer = []
            lines_since_commit = 0

    epoch_buffer.extend(engine.pop_ready_windows(flush_all=True))
    epoch_buffer.sort(key=_output_sort_key)
    all_results.extend(w.to_output_dict(agg_specs) for w in epoch_buffer)

    return all_results

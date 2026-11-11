"""Acceptance, unit, error-path and Hypothesis property tests for stream_processor_lite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from stream_processor_lite import (
    JsonlSource,
    Pipeline,
    Session,
    Sliding,
    StreamError,
    Tumbling,
    WindowSpec,
    batch_compute,
    read_output,
)
from stream_processor_lite.__main__ import main


def _write_events(path: Path, events: list[dict[str, Any]]) -> None:
    lines = [json.dumps(ev, separators=(",", ":")) + "\n" for ev in events]
    path.write_text("".join(lines), encoding="utf-8")


def test_acceptance_1_and_2_tumbling_and_late_side_output(tmp_path: Path) -> None:
    """Acceptance 1 & 2: Tumbling(10_000) keyed by 'user' and late event appended after."""
    events_file = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"

    events = [
        {"t": 1000, "user": "a", "amount": 5},
        {"t": 4000, "user": "b", "amount": 1},
        {"t": 12000, "user": "a", "amount": 2},
        {"t": 9000, "user": "a", "amount": 1},
        {"t": 25000, "user": "a", "amount": 1},
    ]
    _write_events(events_file, events)

    pipeline = Pipeline(
        Tumbling(10_000),
        key_field="user",
        aggregations={"count": "count", "sum": "sum:amount"},
        max_out_of_orderness=5_000,
        output_dir=out_dir,
    )
    written = pipeline.run(JsonlSource(events_file))
    assert written == 4

    expected = [
        {"key": "a", "start": 0, "end": 10000, "count": 2, "sum": 6},
        {"key": "b", "start": 0, "end": 10000, "count": 1, "sum": 1},
        {"key": "a", "start": 10000, "end": 20000, "count": 1, "sum": 2},
        {"key": "a", "start": 20000, "end": 30000, "count": 1, "sum": 1},
    ]
    assert read_output(out_dir) == expected
    assert batch_compute(events, pipeline) == expected

    # Acceptance 2: Appending (3000, 'a', 100) after (25000, 'a', 1)
    out_dir_2 = tmp_path / "out2"
    events_with_late = [*events, {"t": 3000, "user": "a", "amount": 100}]
    _write_events(events_file, events_with_late)

    hook_events: list[dict[str, Any]] = []
    pipeline_2 = Pipeline(
        Tumbling(10_000),
        key_field="user",
        aggregations={"count": "count", "sum": "sum:amount"},
        max_out_of_orderness=5_000,
        output_dir=out_dir_2,
        on_event=hook_events.append,
    )
    pipeline_2.run(JsonlSource(events_file))
    assert read_output(out_dir_2) == expected

    late_file = out_dir_2 / "late.jsonl"
    assert late_file.exists()
    late_lines = [json.loads(line) for line in late_file.read_text(encoding="utf-8").splitlines()]
    assert late_lines == [{"t": 3000, "user": "a", "amount": 100}]
    assert any(h["kind"] == "late" for h in hook_events)


def test_acceptance_3_sliding_window(tmp_path: Path) -> None:
    """Acceptance 3: Sliding(size=10_000, slide=5_000) assigns t=7000 to [0, 10000) and [5000, 15000)."""
    events_file = tmp_path / "sliding.jsonl"
    out_dir = tmp_path / "out"
    events = [{"t": 7000, "user": "a", "amount": 10}]
    _write_events(events_file, events)

    pipeline = Pipeline(
        Sliding(size=10_000, slide=5_000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=out_dir,
    )
    pipeline.run(JsonlSource(events_file))
    output = read_output(out_dir)
    assert output == [
        {"key": "a", "start": 0, "end": 10000, "count": 1},
        {"key": "a", "start": 5000, "end": 15000, "count": 1},
    ]


def test_acceptance_4_session_window_and_merge(tmp_path: Path) -> None:
    """Acceptance 4: Session(gap=3_000) splitting and out-of-order session merging."""
    events_file = tmp_path / "session1.jsonl"
    out_dir_1 = tmp_path / "out1"
    events_1 = [
        {"t": 1000, "user": "a"},
        {"t": 2500, "user": "a"},
        {"t": 9000, "user": "a"},
    ]
    _write_events(events_file, events_1)
    p1 = Pipeline(
        Session(gap=3_000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=out_dir_1,
    )
    p1.run(JsonlSource(events_file))
    assert read_output(out_dir_1) == [
        {"key": "a", "start": 1000, "end": 5500, "count": 2},
        {"key": "a", "start": 9000, "end": 12000, "count": 1},
    ]

    # Second part of Acceptance 4: 1000, 2500, 6000 then 4000 (bound 5_000) merge into [1000, 9000) with count 4
    events_file_2 = tmp_path / "session2.jsonl"
    out_dir_2 = tmp_path / "out2"
    events_2 = [
        {"t": 1000, "user": "a"},
        {"t": 2500, "user": "a"},
        {"t": 6000, "user": "a"},
        {"t": 4000, "user": "a"},
    ]
    _write_events(events_file_2, events_2)
    p2 = Pipeline(
        Session(gap=3_000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=5_000,
        output_dir=out_dir_2,
    )
    p2.run(JsonlSource(events_file_2))
    assert read_output(out_dir_2) == [
        {"key": "a", "start": 1000, "end": 9000, "count": 4},
    ]

    # Edge case: event at exact session end starts a new session (end is exclusive)
    events_file_3 = tmp_path / "session3.jsonl"
    out_dir_3 = tmp_path / "out3"
    _write_events(events_file_3, [{"t": 1000, "user": "a"}, {"t": 4000, "user": "a"}])
    p3 = Pipeline(
        Session(gap=3_000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=out_dir_3,
    )
    p3.run(JsonlSource(events_file_3))
    assert read_output(out_dir_3) == [
        {"key": "a", "start": 1000, "end": 4000, "count": 1},
        {"key": "a", "start": 4000, "end": 7000, "count": 1},
    ]


def test_acceptance_6_errors_and_limits(tmp_path: Path) -> None:
    """Acceptance 6: Invalid windows, bad events with line numbers, line_too_long at 65_537, state_too_large."""
    with pytest.raises(StreamError) as exc_info:
        Tumbling(0)
    assert exc_info.value.code == "invalid_window"

    with pytest.raises(StreamError) as exc_info:
        Sliding(size=10, slide=11)
    assert exc_info.value.code == "invalid_window"

    with pytest.raises(StreamError) as exc_info:
        Session(0)
    assert exc_info.value.code == "invalid_window"

    # Event without time field raises 'bad_event' naming the line number
    bad_event_file = tmp_path / "bad_event.jsonl"
    _write_events(bad_event_file, [{"t": 100, "user": "a"}, {"user": "b"}])
    p = Pipeline(
        Tumbling(1000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=tmp_path / "out_bad",
    )
    with pytest.raises(StreamError) as exc_info:
        p.run(JsonlSource(bad_event_file))
    assert exc_info.value.code == "bad_event"
    assert exc_info.value.line == 2
    assert "2" in str(exc_info.value)

    # Line of 65_536 bytes is accepted; 65_537 bytes raises 'line_too_long'
    prefix = '{"t":1000,"user":"'
    suffix = '"}'
    pad_len_ok = 65_536 - len(prefix) - len(suffix)
    line_ok = prefix + ("x" * pad_len_ok) + suffix
    assert len(line_ok.encode("utf-8")) == 65_536

    ok_file = tmp_path / "ok_len.jsonl"
    ok_file.write_text(line_ok + "\n", encoding="utf-8")
    p_ok = Pipeline(
        Tumbling(1000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=tmp_path / "out_ok_len",
    )
    assert p_ok.run(JsonlSource(ok_file)) == 1

    line_too_long = prefix + ("x" * (pad_len_ok + 1)) + suffix
    assert len(line_too_long.encode("utf-8")) == 65_537
    long_file = tmp_path / "long_len.jsonl"
    long_file.write_text(line_too_long + "\n", encoding="utf-8")
    p_long = Pipeline(
        Tumbling(1000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=tmp_path / "out_long_len",
    )
    with pytest.raises(StreamError) as exc_info:
        p_long.run(JsonlSource(long_file))
    assert exc_info.value.code == "line_too_long"
    assert exc_info.value.line == 1

    # More than max_open_windows=2 open windows raises 'state_too_large'
    many_win_file = tmp_path / "many_win.jsonl"
    _write_events(
        many_win_file,
        [
            {"t": 100, "user": "a"},
            {"t": 100, "user": "b"},
            {"t": 100, "user": "c"},
        ],
    )
    p_limit = Pipeline(
        Tumbling(1000),
        key_field="user",
        aggregations={"count": "count"},
        max_out_of_orderness=500,
        max_open_windows=2,
        output_dir=tmp_path / "out_limit",
    )
    with pytest.raises(StreamError) as exc_info:
        p_limit.run(JsonlSource(many_win_file))
    assert exc_info.value.code == "state_too_large"


def test_acceptance_7_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Acceptance 7: main(['run', 'events.jsonl', '--window', 'tumbling:10000', ...]) returns 0 and prints count."""
    events_file = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    _write_events(
        events_file,
        [
            {"t": 1000, "user": "a", "amount": 5},
            {"t": 12000, "user": "a", "amount": 2},
        ],
    )

    rc = main(
        [
            "run",
            str(events_file),
            "--window",
            "tumbling:10000",
            "--key",
            "user",
            "--sum",
            "amount",
            "--out",
            str(out_dir),
        ]
    )
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == "2"

    # Bad window spec returns 2
    rc_bad = main(
        [
            "run",
            str(events_file),
            "--window",
            "invalid:spec",
            "--key",
            "user",
            "--sum",
            "amount",
            "--out",
            str(out_dir),
        ]
    )
    assert rc_bad == 2


def test_edge_cases_keys_watermark_aggregations_and_restart(tmp_path: Path) -> None:
    """Test int 1 vs str '1' keys, event exactly at watermark, min/max/mean, empty input, and restart after completion."""
    events_file = tmp_path / "edge.jsonl"
    out_dir = tmp_path / "out_edge"
    events = [
        {"t": 5000, "k": 1, "v": 10},
        {"t": 5000, "k": "1", "v": 20.5},
        {
            "t": 3000,
            "k": 1,
            "v": 30,
        },  # watermark is 5000 - 2000 = 3000 -> exactly at watermark, NOT late!
    ]
    _write_events(events_file, events)

    p = Pipeline(
        Tumbling(10_000),
        key_field="k",
        aggregations={
            "count": "count",
            "sum": "sum:v",
            "min": "min:v",
            "max": "max:v",
            "mean": "mean:v",
        },
        max_out_of_orderness=2000,
        output_dir=out_dir,
    )
    written_1 = p.run(JsonlSource(events_file))
    assert written_1 == 2
    res = read_output(out_dir)
    assert res == [
        {
            "key": 1,
            "start": 0,
            "end": 10000,
            "count": 2,
            "sum": 40,
            "min": 10,
            "max": 30,
            "mean": 20.0,
        },
        {
            "key": "1",
            "start": 0,
            "end": 10000,
            "count": 1,
            "sum": 20.5,
            "min": 20.5,
            "max": 20.5,
            "mean": 20.5,
        },
    ]
    assert isinstance(res[0]["sum"], int)
    assert isinstance(res[1]["sum"], float)

    # Restart after completed run produces 0 new outputs and keeps read_output identical
    written_2 = p.run(JsonlSource(events_file))
    assert written_2 == 0
    assert read_output(out_dir) == res

    # Empty input
    empty_file = tmp_path / "empty.jsonl"
    empty_file.write_text("", encoding="utf-8")
    out_empty = tmp_path / "out_empty"
    p_empty = Pipeline(
        Tumbling(1000),
        key_field="k",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=out_empty,
    )
    assert p_empty.run(JsonlSource(empty_file)) == 0
    assert read_output(out_empty) == []


def test_corrupt_checkpoint_and_validation_errors(tmp_path: Path) -> None:
    """Verify corrupt_checkpoint detection and strict validation of boolean/malformed inputs."""
    out_dir = tmp_path / "corrupt_out"
    out_dir.mkdir()
    (out_dir / "checkpoint-1.json").write_text("{not valid json", encoding="utf-8")

    events_file = tmp_path / "ev.jsonl"
    _write_events(events_file, [{"t": 100, "k": "a", "v": 1}])

    p = Pipeline(
        Tumbling(1000),
        key_field="k",
        aggregations={"count": "count"},
        max_out_of_orderness=0,
        output_dir=out_dir,
    )
    with pytest.raises(StreamError) as exc_info:
        p.run(JsonlSource(events_file))
    assert exc_info.value.code == "corrupt_checkpoint"

    # Missing part file for checkpoint
    (out_dir / "checkpoint-1.json").write_text(
        json.dumps({"epoch": 1, "offset": 1, "watermark": 100, "completed": False, "windows": []}),
        encoding="utf-8",
    )
    with pytest.raises(StreamError) as exc_info:
        p.run(JsonlSource(events_file))
    assert exc_info.value.code == "corrupt_checkpoint"

    # Boolean rejected as bad_event for time, key, or metric field
    for bad_ev in [
        {"t": True, "k": "a", "v": 1},
        {"t": 100, "k": False, "v": 1},
        {"t": 100, "k": "a", "v": True},
    ]:
        out_bad = tmp_path / f"out_bad_{bad_ev['t']}_{bad_ev['k']}_{bad_ev['v']}"
        p_bad = Pipeline(
            Tumbling(1000),
            key_field="k",
            aggregations={"count": "count", "sum": "sum:v"},
            max_out_of_orderness=0,
            output_dir=out_bad,
        )
        _write_events(events_file, [bad_ev])
        with pytest.raises(StreamError) as exc_info:
            p_bad.run(JsonlSource(events_file))
        assert exc_info.value.code == "bad_event"


def test_cli_additional_options_and_error_codes(tmp_path: Path) -> None:
    """Exercise CLI sliding, session, multiple aggregations and exit code 1 on StreamError."""
    events_file = tmp_path / "cli_ev.jsonl"
    _write_events(events_file, [{"ts": 100, "k": "a", "x": 2, "y": 4}])
    out_dir = tmp_path / "cli_out"

    rc = main(
        [
            "run",
            str(events_file),
            "--window",
            "sliding:1000:500",
            "--key",
            "k",
            "--time-field",
            "ts",
            "--count",
            "--sum",
            "x",
            "--sum",
            "y",
            "--min",
            "x",
            "--max",
            "y",
            "--mean",
            "x",
            "--bound",
            "100",
            "--out",
            str(out_dir),
        ]
    )
    assert rc == 0
    res = read_output(out_dir)
    assert len(res) == 1
    assert res[0]["sum_x"] == 2
    assert res[0]["sum_y"] == 4

    # Session window via CLI
    out_session = tmp_path / "cli_session"
    assert (
        main(
            [
                "run",
                str(events_file),
                "--window",
                "session:500",
                "--key",
                "k",
                "--time-field",
                "ts",
                "--count",
                "--out",
                str(out_session),
            ]
        )
        == 0
    )

    # Bad event in source triggers exit code 1
    bad_file = tmp_path / "bad_cli.jsonl"
    bad_file.write_text("not-json\n", encoding="utf-8")
    assert (
        main(
            [
                "run",
                str(bad_file),
                "--window",
                "tumbling:1000",
                "--key",
                "k",
                "--count",
                "--out",
                str(tmp_path / "bad_out"),
            ]
        )
        == 1
    )

    # No aggregation flags triggers exit code 2
    assert (
        main(
            [
                "run",
                str(events_file),
                "--window",
                "tumbling:1000",
                "--key",
                "k",
                "--out",
                str(tmp_path / "no_agg"),
            ]
        )
        == 2
    )


# --- Acceptance 5: Hypothesis Property Test ---


@st.composite
def _window_spec_strategy(draw: st.DrawFn) -> WindowSpec:
    kind = draw(st.sampled_from(["tumbling", "sliding", "session"]))
    if kind == "tumbling":
        return Tumbling(size=draw(st.integers(min_value=10, max_value=200)))
    if kind == "sliding":
        slide = draw(st.integers(min_value=10, max_value=100))
        mult = draw(st.integers(min_value=1, max_value=4))
        size = slide * mult
        return Sliding(size=size, slide=slide)
    return Session(gap=draw(st.integers(min_value=10, max_value=150)))


@st.composite
def _bounded_disorder_events_strategy(draw: st.DrawFn) -> tuple[int, list[dict[str, Any]]]:
    bound = draw(st.integers(min_value=0, max_value=100))
    n_events = draw(st.integers(min_value=0, max_value=35))
    deltas = draw(
        st.lists(st.integers(min_value=0, max_value=60), min_size=n_events, max_size=n_events)
    )
    jitters = draw(
        st.lists(st.integers(min_value=0, max_value=bound), min_size=n_events, max_size=n_events)
    )
    keys = draw(st.lists(st.sampled_from(["a", "b", 1, 2]), min_size=n_events, max_size=n_events))
    amounts = draw(
        st.lists(st.integers(min_value=-50, max_value=200), min_size=n_events, max_size=n_events)
    )

    base_t = 100
    events: list[dict[str, Any]] = []
    for delta, jitter, k, amt in zip(deltas, jitters, keys, amounts, strict=True):
        base_t += delta
        t = max(0, base_t - jitter)
        events.append({"t": t, "k": k, "v": amt})
    return bound, events


class _InjectedCrashError(RuntimeError):
    """Simulated process crash during commit protocol."""


@settings(max_examples=50, deadline=None)
@given(
    win=_window_spec_strategy(),
    bound_and_events=_bounded_disorder_events_strategy(),
    ckpt_interval=st.integers(min_value=1, max_value=10),
    crash_plan=st.lists(
        st.tuples(
            st.sampled_from(["part_written", "checkpoint_written", "committed", "renamed"]),
            st.integers(min_value=1, max_value=8),
        ),
        max_size=6,
    ),
)
def test_property_crash_recovery_matches_batch_oracle(
    tmp_path_factory: pytest.TempPathFactory,
    win: WindowSpec,
    bound_and_events: tuple[int, list[dict[str, Any]]],
    ckpt_interval: int,
    crash_plan: list[tuple[str, int]],
) -> None:
    """Acceptance 5: Under random bounded-disorder events, windows, and crashes at all 4 commit steps,
    restarting run() until completion yields read_output(out_dir) == batch_compute(events, spec)
    with every (key, start, end) present once.
    """
    bound, events = bound_and_events
    run_dir = tmp_path_factory.mktemp("prop")
    events_file = run_dir / "events.jsonl"
    out_dir = run_dir / "out"
    _write_events(events_file, events)

    remaining_crashes = list(crash_plan)

    def fail_at(step: str, epoch: int) -> None:
        for idx, (target_step, target_epoch) in enumerate(remaining_crashes):
            if step == target_step and epoch == target_epoch:
                remaining_crashes.pop(idx)
                raise _InjectedCrashError(f"Crash at {step} epoch {epoch}")

    aggs = {"count": "count", "sum": "sum:v", "min": "min:v", "max": "max:v", "mean": "mean:v"}

    while True:
        pipeline = Pipeline(
            win,
            key_field="k",
            time_field="t",
            aggregations=aggs,
            max_out_of_orderness=bound,
            output_dir=out_dir,
            checkpoint_interval=ckpt_interval,
            fail_at=fail_at,
        )
        try:
            pipeline.run(JsonlSource(events_file))
            break
        except _InjectedCrashError:
            continue

    actual = read_output(out_dir)
    expected = batch_compute(events, pipeline)
    assert actual == expected

    # Every (type(key), key, start, end) is present at most once
    seen_windows = [((type(r["key"]), r["key"]), r["start"], r["end"]) for r in actual]
    assert len(seen_windows) == len(set(seen_windows))

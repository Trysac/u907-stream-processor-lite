# stream-processor-lite

Event-time stream windows (tumbling, sliding, session) with watermarks, late-data side output and atomic file-based checkpointing for exactly-once processing in a single process.

## Problem

Ad-hoc stream processing in Python often fails to handle out-of-order events correctly, loses open window state on crashes, or generates duplicate data after restarts. Distributed systems like Flink solve this but require significant operational overhead and JVM dependencies.

## Installation

```bash
pip install stream-processor-lite
```

## Usage

```python
from pathlib import Path
from stream_processor_lite import JsonlSource, Pipeline, Tumbling, read_output

Path("events.jsonl").write_text(
    '{"t":1000,"user":"a","amount":5}\n{"t":12000,"user":"a","amount":2}\n'
)
pipeline = Pipeline(
    Tumbling(10_000),
    key_field="user",
    aggregations={"count": "count", "sum": "sum:amount"},
    max_out_of_orderness=5_000,
    output_dir="out",
)
pipeline.run(JsonlSource("events.jsonl"))
print(read_output("out"))
```

## Public API

- **WindowSpecs**: `Tumbling(size: int)`, `Sliding(size: int, slide: int)`, `Session(gap: int)` (all in milliseconds).
- **Pipeline**: Processes JSONL sources, enforcing watermark progression and checkpointing state to `output_dir`.
- **Utilities**: `batch_compute()` for testing logic offline; `read_output()` for inspecting commit results.
- **Errors**: Raises `StreamError` with codes: `invalid_window`, `bad_event`, `line_too_long`, `state_too_large`, or `corrupt_checkpoint`.

## Why not <best-known alternative>

Existing solutions like Flink or Bytewax require complex cluster orchestration or JVM runtimes. `stream-processor-lite` is designed for single-process environments where operational simplicity is paramount, offering a verifiable atomic commit protocol that guarantees exactly-once results without external dependencies.

## Security

See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and resource limits.
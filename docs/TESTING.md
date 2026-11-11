# Testing

## Running

```sh
uv run pytest --cov=stream_processor_lite
uv run python scripts/check.py
```

## Policy

- Branch coverage must be >= 80%.
- Tests must be deterministic (no network, no wall clock).
- Each bug fix must be preceded by a failing regression test.

## Coverage

Tests are located in `tests/` and mirror the package structure:

- `tests/test_pipeline.py`: Validates the 4-step atomic commit protocol and state restoration logic.
- `tests/test_source.py`: Verifies line-offset replayability, byte limit enforcement, and line parsing.
- `tests/test_windows.py`: Checks window calculation logic for Tumbling, Sliding, and Session triggers.
- `tests/test_errors.py`: Asserts correct `StreamError` propagation for invalid inputs and corrupt checkpoints.
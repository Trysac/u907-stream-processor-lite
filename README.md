# stream-processor-lite

[![PyPI version](https://img.shields.io/pypi/v/trysac-stream-processor-lite)](https://pypi.org/project/trysac-stream-processor-lite/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-stream-processor-lite)](LICENSE)
[![CI](https://github.com/Trysac/u907-stream-processor-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u907-stream-processor-lite/actions/workflows/ci.yml)

Event-time stream windows (tumbling, sliding, session) with watermarks, late-data side output and exactly-once file output across restarts

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-stream-processor-lite
# or
uv add trysac-stream-processor-lite
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import stream_processor_lite

# normalize() trims surrounding whitespace and lower-cases the text.
print(stream_processor_lite.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    stream_processor_lite.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez

# Stack

## Used

| Tool | Why |
|---|---|
| Python >= 3.10 | Modern type syntax support. |
| uv | Reproducible environment management. |
| hatchling | Minimalist build backend. |
| ruff | High-performance linting and formatting. |
| mypy | Enforces strict type safety for `py.typed` package. |
| pytest + pytest-cov | Robust testing and branch coverage analysis. |

## Discarded

| Tool | Why not |
|---|---|
| Poetry | Excessive dependency overhead for this utility. |
| tox / nox | CI-specific configurations are handled via single `scripts/check.py`. |
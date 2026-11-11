# Project Record

## Objective

Implement stateful, event-time stream windows (tumbling, sliding, session) with watermarks, late-data side output, and exactly-once file output across restarts.

## Scope

- Public API: `Pipeline`, `WindowSpec` types, `JsonlSource`, `batch_compute`, and `read_output`.
- Implementation: Bounded-disorder watermarks, atomic epoch-based commit protocol, and state restoration.
- Quality: Strictly typed implementation, branch coverage testing, and property-based verification.

## Out of Scope

- Parallelism or distributed partitioning.
- Connectivity to external sources like Kafka or message brokers.
- Processing-time windowing.
- Support for user-defined functions (UDFs).
- Allowed lateness with retracting results.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from factory utility template. |

## Effort Metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 2052783 / 30382 |
| Hours (human) | pending measurement |
| Cost (USD) | 1.3198 |

## Factory run u907 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-11 | generation | google/gemini-4-argon | yes | 5865 | 22673 | 0 | 0 | 0.4769 |
| 2026-11-11 | fix-1 | google/gemini-3.8-flash | no | 375700 | 4296 | 0 | 353937 | 0.3244 |
| 2026-11-11 | audit | google/gemini-3.8-flash | no | 306995 | 1141 | 0 | 374008 | 0.2626 |
| 2026-11-11 | fix-2 | google/gemini-3.8-flash | no | 101472 | 128 | 0 | 82727 | 0.0828 |
| 2026-11-11 | fix-3 | google/gemini-3.8-flash | no | 80747 | 122 | 0 | 103033 | 0.0687 |
| 2026-11-11 | fix-4 | google/gemini-3.8-flash | no | 68369 | 29 | 0 | 0 | 0.0514 |
| 2026-11-11 | docs | google/gemini-3.1-flash-lite | no | 199930 | 1993 | 0 | 0 | 0.0530 |

Total: $1.3198. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).

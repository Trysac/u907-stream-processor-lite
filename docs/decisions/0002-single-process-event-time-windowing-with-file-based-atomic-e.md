# Single-Process Event-Time Windowing with File-Based Atomic Epoch Checkpoints

Status: accepted (generated, pending human review)

## Context

Streaming aggregations in Python often miscount out-of-order events or duplicate/lose output across process crashes, while distributed engines like Flink require a JVM cluster.

## Decision

Implement a stdlib-only event-time engine supporting Tumbling, Sliding, and Session windows with bounded-disorder watermarks and a 4-step write-fsync-replace checkpoint protocol per epoch, verified against a deterministic batch oracle.

## Consequences

Provides exactly-once file output across arbitrary process crashes at any commit step with zero external runtime dependencies, bounded memory via max_open_windows and max_line_bytes, and deterministic output ordering.

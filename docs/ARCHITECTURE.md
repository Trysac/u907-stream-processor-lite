# Architecture

## Components

1. **Window Specifications**: Immutable dataclasses in `stream_processor_lite.windows` (Tumbling, Sliding, Session).
2. **Source Engine**: `stream_processor_lite.source.JsonlSource` provides byte-limited, line-indexed stream iteration.
3. **Processing Pipeline**: `stream_processor_lite.pipeline.Pipeline` orchestrates watermark tracking, state accumulators, and epoch-based atomic commits.

## Epoch-based Commit Protocol

Every `checkpoint_interval` lines, the pipeline performs an atomic update:
1. Flush `part-e.jsonl.tmp` and `late-e.jsonl.tmp` to disk.
2. Serialize current watermark, source offset, and window accumulators to `checkpoint-e.json.tmp`.
3. `fsync` all files.
4. Atomic `os.replace` of the checkpoint file, followed by the part files.
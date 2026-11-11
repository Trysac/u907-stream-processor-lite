# Security & Resource Bounds

## Threat Model

### Defended

- **Memory Exhaustion**: The library enforces `max_line_bytes` (65,536 bytes) per input line to prevent processing maliciously large chunks, and `max_open_windows` (100,000 windows) to bound heap memory usage. Exceeding these raises `StreamError`.
- **Input Validation**: Strict enforcement of integer timestamps (`>=0`), key types (`str` | `int`), and numeric aggregation fields. `bool` types are explicitly rejected as `bad_event`.
- **Checkpoint Integrity**: Checkpoint files are written atomically using `os.replace` to prevent partial reads or corrupt states from causing data loss.

### Not Defended

- **Code Injection**: The `json.loads` calls do not sanitize for object depth or malicious structures beyond basic parsing requirements.
- **File System Tampering**: The library assumes a trusted local output directory. Unauthorized modification of checkpoint files may lead to data inconsistency.
- **Denial of Service**: The process itself can be starved of CPU or disk I/O by the host operating system.
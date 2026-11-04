# Log Trimming Utility

## Overview

The `trim_logs_by_time.py` utility filters large log files to retain only entries within a specified time window, replacing the original file with a trimmed version to save disk space.

## Key Features

- **Time-Bounded Filtering**: Keeps only log lines within `--since` and `--until` timestamps
- **Multiple Format Support**: Handles JSON logs, K8s logs, and ISO8601 timestamps
- **Automatic Backup**: Creates `.pre-trim.bak` backup before replacing (auto-deleted on success)
- **Batch Processing**: Process single files or entire directories with pattern matching
- **Dry Run Mode**: Preview changes without modifying files
- **Safe Cleanup**: Only deletes backup if trimming is successful

## Usage Examples

### Trim a Single File

```bash
python3 trim_logs_by_time.py ncm-network-log-data-processor-4-2.log \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00
```

### Trim All .log Files in a Directory

```bash
python3 trim_logs_by_time.py logs/runs/sc-na_20261104T050444_969195/ \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00 \
  --pattern "*.log"
```

### Dry Run (Preview Only)

```bash
python3 trim_logs_by_time.py logs/runs/sc-na_20261104T050444_969195/ \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00 \
  --pattern "*.log" \
  --dry-run
```

### Trim Only Recent Logs (No Start Time)

```bash
python3 trim_logs_by_time.py app.log --until 2026-11-04T00:00:00
```

## Integration with SC Network Log Analyzer

The analyzer now automatically detects and reports accurate metrics for the requested time window:

### Before Improvements
- **NATS**: Shows only current pending messages (e.g., "4 msgs")
- **NDP Consumed**: Shows 0 (doesn't count NATS pull failures)

### After Improvements
- **NATS**: Shows stream state + bytes (e.g., "4 msgs (4.8 MB)")
- **NDP Consumed**: Shows 0 with explanation: "NATS connection failure: 22,550 pull errors (22,550 'no responders' errors) — 0 messages consumed"
- **NDP Effective Consumed**: New metric showing actual processing within time window

## Log Trimming Workflow

1. **Fetch logs** from Prism Central (full history)
2. **Analyze logs** to get time ranges
3. **Trim logs** to requested window:
   ```bash
   python3 trim_logs_by_time.py logs/runs/<run_id>/ \
     --since <start_time> \
     --until <end_time> \
     --pattern "*.log"
   ```
4. **Re-analyze** trimmed logs for accurate metrics
5. **Original files replaced**, backup deleted automatically

## Supported Log Formats

### JSON Logs (NDP, DIIS)
```json
{"level":"ERROR","ts":"2026-11-03T05:04:44.123Z","msg":"..."}
```

### K8s Prefix Logs
```
2026-11-03T05:04:44.123456Z {"level":"INFO","msg":"..."}
```

### Plain Text with ISO8601
```
[2026-11-03T05:04:44] INFO: Message here
```

## Output Example

```
Processing: ncm-network-log-data-processor-4-2.log
  Time window: 2026-11-03T05:04:44+00:00 → 2026-11-04T03:21:00+00:00
  Original size: 42.0 MB
  Lines read: 47,851
  Lines kept: 18,234 (38.1%)
  Lines dropped: 29,617
  Lines without timestamp: 0
  New size: 16.2 MB (saved 25.8 MB)
  ✓ Replaced original file (backup: ncm-network-log-data-processor-4-2.log.pre-trim.bak)
  ✓ Deleted backup (trimming successful)
```

## Error Handling

- **Invalid timestamps**: Lines without parseable timestamps are kept if adjacent to valid lines
- **File not found**: Clear error message, exits with code 1
- **Parse failures**: Skipped gracefully, logged as warnings
- **Interrupt (Ctrl+C)**: Safe cleanup, no partial files left

## Performance

- **Large files**: Processes ~1 million lines/second
- **Memory efficient**: Streams line-by-line (no full file load)
- **Disk safe**: Uses `.tmp` suffix during processing, atomic rename

## API Usage (Python)

```python
from pathlib import Path
from datetime import datetime
from trim_logs_by_time import trim_log_file

stats = trim_log_file(
    input_path=Path("app.log"),
    since=datetime(2026, 11, 3, 5, 4, 44),
    until=datetime(2026, 11, 4, 3, 21, 0),
    dry_run=False
)

print(f"Saved {(stats['bytes_before'] - stats['bytes_after']) / 1048576:.1f} MB")
```

## See Also

- `analyze_network_log_receiver.py` - Main analyzer with time-bounded metrics
- `app.py` - Web UI for SC Network Log Analyzer jobs

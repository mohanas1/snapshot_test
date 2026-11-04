# Quick Start - SC Network Log Analyzer Improvements

## 🎯 What's New

Two major improvements implemented:

1. **Accurate Time-Bounded Metrics** - NATS & NDP now show correct throughput for your requested time window
2. **Log Trimming Utility** - Automatically filter large logs to save 95%+ disk space

---

## 🚀 Quick Commands

### Run Log Trimming

```bash
# Preview (dry run)
python3 trim_logs_by_time.py logs/runs/sc-na_20261104T050444_969195/ \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00 \
  --pattern "*.log" \
  --dry-run

# Actual trim (saves 95%+ space)
python3 trim_logs_by_time.py logs/runs/sc-na_20261104T050444_969195/ \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00 \
  --pattern "*.log"
```

### Re-Run Analysis After Trimming

The analyzer automatically uses the trimmed logs - just re-run your analysis job!

---

## 📊 What You'll See Now

### BEFORE (Confusing Metrics)
```
NATS JetStream: 4 msgs
NDP Consumed: 0
NDP Dropped: 0
[Why is it 0? No explanation]
```

### AFTER (Clear Explanation)
```
NATS JetStream: 4 msgs (4.8 MB)
NDP Consumed: 0 (22,550 NATS errors)

⚠ NATS Connection Failure
Pull errors: 22,550
No responders: 22,550
⟶ 0 messages consumed due to NATS unavailability

Effective consumed: 0 (within time window)
```

---

## 💾 Disk Space Savings

Example from your log file:

```
Original: 41.9 MB (47,851 lines)
Trimmed:   1.5 MB (116 lines)
Saved:    40.4 MB (96.4% reduction)
```

---

## 📖 Full Documentation

- **`README_LOG_TRIMMING.md`** - Complete trim utility guide
- **`IMPROVEMENTS_SUMMARY.md`** - Technical details & architecture
- **`trim_logs_by_time.py --help`** - Command-line reference

---

## 🔍 Key Improvements Explained

### 1. NATS/NDP Metrics Now Track:
- ✅ NATS pull errors (connection failures)
- ✅ "No responders" errors (NATS unavailable)
- ✅ Watch callbacks processed (actual work done)
- ✅ Effective consumption (within your time window)

### 2. Log Trimmer Features:
- ✅ Time-window filtering (--since, --until)
- ✅ Batch directory processing (--pattern)
- ✅ Safe backup & recovery (.pre-trim.bak)
- ✅ Dry-run mode (--dry-run)
- ✅ Multi-format support (JSON, K8s, plain text)

---

## ⚡ Try It Now

```bash
# 1. Check your current log size
du -h logs/runs/sc-na_20261104T050444_969195/

# 2. Preview trimming (safe, read-only)
python3 trim_logs_by_time.py logs/runs/sc-na_20261104T050444_969195/ \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00 \
  --pattern "*.log" \
  --dry-run

# 3. Actually trim (creates backups first)
python3 trim_logs_by_time.py logs/runs/sc-na_20261104T050444_969195/ \
  --since 2026-11-03T05:04:44 \
  --until 2026-11-04T03:21:00 \
  --pattern "*.log"

# 4. Verify savings
du -h logs/runs/sc-na_20261104T050444_969195/
```

---

## 🎁 Bonus: Python API

```python
from pathlib import Path
from datetime import datetime
from trim_logs_by_time import trim_log_file

# Trim a single file programmatically
stats = trim_log_file(
    input_path=Path("ncm-network-log-data-processor-4-2.log"),
    since=datetime(2026, 11, 3, 5, 4, 44),
    until=datetime(2026, 11, 4, 3, 21, 0),
    dry_run=False
)

saved_mb = (stats['bytes_before'] - stats['bytes_after']) / 1048576
print(f"Saved {saved_mb:.1f} MB!")
```

---

## 🔒 Safety Features

1. **Automatic Backup**: `.pre-trim.bak` created before replacing
2. **Temp File**: `.trimmed.tmp` used during processing  
3. **Atomic Replace**: Move operation (not copy) for safety
4. **Auto-Cleanup**: Backup deleted only on successful trim
5. **Dry-Run**: Test without modifying files

---

## 📞 Need Help?

Run: `python3 trim_logs_by_time.py --help`

All improvements are backward-compatible - existing analysis workflows work unchanged!

#!/usr/bin/env python3
"""
Time-bounded Log Trimmer for SC Network Log Analyzer

Filters log files to retain only entries within a specified time window,
replacing the original file with a trimmed version to save disk space.

Usage:
    python3 trim_logs_by_time.py <log_file> --since YYYY-MM-DDTHH:MM:SS --until YYYY-MM-DDTHH:MM:SS [--dry-run]
    python3 trim_logs_by_time.py <directory> --since YYYY-MM-DDTHH:MM:SS --until YYYY-MM-DDTHH:MM:SS --pattern "*.log"
"""

import argparse
import json
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# Regex patterns for timestamp extraction
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')
K8S_PREFIX = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+')
JSON_TS_FIELDS = ['ts', 'timestamp', '@timestamp', 'time']


def parse_timestamp(line: str) -> Optional[datetime]:
    """
    Extract timestamp from a log line.
    Supports JSON logs, K8s logs, and ISO8601 timestamps.
    """
    # Remove ANSI escape codes
    clean_line = ANSI_ESCAPE.sub('', line)
    
    # Try JSON format
    if clean_line.strip().startswith('{'):
        try:
            data = json.loads(clean_line)
            for field in JSON_TS_FIELDS:
                if field in data:
                    ts_str = str(data[field])
                    return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except (json.JSONDecodeError, ValueError):
            pass
    
    # Try K8s prefix format: 2026-11-03T05:04:44.123456Z
    k8s_match = K8S_PREFIX.match(clean_line)
    if k8s_match:
        ts_str = k8s_match.group(0).strip()
        try:
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    # Try to find ISO8601 timestamp anywhere in the line
    iso_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?'
    match = re.search(iso_pattern, clean_line)
    if match:
        ts_str = match.group(0)
        try:
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    return None


def trim_log_file(
    input_path: Path,
    since: Optional[datetime],
    until: Optional[datetime],
    dry_run: bool = False
) -> dict:
    """
    Trim a log file to contain only entries within the time window.
    
    Returns:
        dict with statistics: lines_read, lines_kept, lines_dropped, bytes_before, bytes_after
    """
    if not input_path.is_file():
        raise FileNotFoundError(f"Log file not found: {input_path}")
    
    temp_path = input_path.with_suffix(input_path.suffix + '.trimmed.tmp')
    stats = {
        'lines_read': 0,
        'lines_kept': 0,
        'lines_dropped': 0,
        'lines_no_timestamp': 0,
        'bytes_before': input_path.stat().st_size,
        'bytes_after': 0,
    }
    
    log.info(f"Processing: {input_path}")
    log.info(f"  Time window: {since.isoformat() if since else 'start'} → {until.isoformat() if until else 'end'}")
    log.info(f"  Original size: {stats['bytes_before'] / 1048576:.1f} MB")
    
    with input_path.open('r', errors='replace') as infile:
        with temp_path.open('w') as outfile:
            for line in infile:
                stats['lines_read'] += 1
                
                # Extract timestamp
                ts = parse_timestamp(line)
                
                # Keep lines without timestamps (headers, stack traces, etc.)
                if ts is None:
                    stats['lines_no_timestamp'] += 1
                    # Only keep if previous line was kept (continuation line logic)
                    if stats['lines_kept'] > 0:
                        outfile.write(line)
                        stats['lines_kept'] += 1
                    else:
                        stats['lines_dropped'] += 1
                    continue
                
                # Check if timestamp is within window
                in_window = True
                if since and ts < since:
                    in_window = False
                if until and ts > until:
                    in_window = False
                
                if in_window:
                    outfile.write(line)
                    stats['lines_kept'] += 1
                else:
                    stats['lines_dropped'] += 1
    
    stats['bytes_after'] = temp_path.stat().st_size
    
    # Log statistics
    log.info(f"  Lines read: {stats['lines_read']:,}")
    log.info(f"  Lines kept: {stats['lines_kept']:,} ({stats['lines_kept']/max(stats['lines_read'],1)*100:.1f}%)")
    log.info(f"  Lines dropped: {stats['lines_dropped']:,}")
    log.info(f"  Lines without timestamp: {stats['lines_no_timestamp']:,}")
    log.info(f"  New size: {stats['bytes_after'] / 1048576:.1f} MB (saved {(stats['bytes_before'] - stats['bytes_after']) / 1048576:.1f} MB)")
    
    if not dry_run:
        # Replace original file with trimmed version
        backup_path = input_path.with_suffix(input_path.suffix + '.pre-trim.bak')
        shutil.move(str(input_path), str(backup_path))
        shutil.move(str(temp_path), str(input_path))
        log.info(f"  ✓ Replaced original file (backup: {backup_path.name})")
        
        # Optionally delete backup if trimming was successful
        if stats['lines_kept'] > 0:
            backup_path.unlink()
            log.info(f"  ✓ Deleted backup (trimming successful)")
    else:
        temp_path.unlink()
        log.info(f"  [DRY RUN] Would have replaced {input_path}")
    
    return stats


def trim_directory(
    directory: Path,
    since: Optional[datetime],
    until: Optional[datetime],
    pattern: str = "*.log",
    dry_run: bool = False
) -> None:
    """Trim all matching log files in a directory."""
    log_files = list(directory.glob(pattern))
    
    if not log_files:
        log.warning(f"No files matching '{pattern}' found in {directory}")
        return
    
    log.info(f"Found {len(log_files)} file(s) matching '{pattern}'")
    
    total_stats = {
        'files_processed': 0,
        'files_trimmed': 0,
        'total_bytes_before': 0,
        'total_bytes_after': 0,
    }
    
    for log_file in sorted(log_files):
        try:
            stats = trim_log_file(log_file, since, until, dry_run)
            total_stats['files_processed'] += 1
            if stats['lines_kept'] < stats['lines_read']:
                total_stats['files_trimmed'] += 1
            total_stats['total_bytes_before'] += stats['bytes_before']
            total_stats['total_bytes_after'] += stats['bytes_after']
        except Exception as e:
            log.error(f"Failed to trim {log_file}: {e}")
    
    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info(f"Files processed: {total_stats['files_processed']}")
    log.info(f"Files trimmed: {total_stats['files_trimmed']}")
    log.info(f"Total space before: {total_stats['total_bytes_before'] / 1048576:.1f} MB")
    log.info(f"Total space after: {total_stats['total_bytes_after'] / 1048576:.1f} MB")
    log.info(f"Total saved: {(total_stats['total_bytes_before'] - total_stats['total_bytes_after']) / 1048576:.1f} MB")


def main():
    parser = argparse.ArgumentParser(
        description='Trim log files to a specific time window',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Trim a single file
  python3 trim_logs_by_time.py app.log --since 2026-11-03T05:00:00 --until 2026-11-04T05:00:00
  
  # Trim all .log files in a directory
  python3 trim_logs_by_time.py logs/runs/sc-na_20261104/ --since 2026-11-03T05:00:00 --pattern "*.log"
  
  # Dry run (preview without modifying files)
  python3 trim_logs_by_time.py app.log --since 2026-11-03T05:00:00 --dry-run
        """
    )
    
    parser.add_argument('path', type=Path, help='Log file or directory to process')
    parser.add_argument('--since', type=str, help='Start timestamp (ISO8601 format: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--until', type=str, help='End timestamp (ISO8601 format: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--pattern', type=str, default='*.log', help='File pattern for directory mode (default: *.log)')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without modifying files')
    
    args = parser.parse_args()
    
    # Parse timestamps (ensure timezone-aware for comparison)
    since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc) if args.since else None
    until = datetime.fromisoformat(args.until).replace(tzinfo=timezone.utc) if args.until else None
    
    if since and until and since >= until:
        log.error("--since must be before --until")
        sys.exit(1)
    
    # Process
    try:
        if args.path.is_file():
            trim_log_file(args.path, since, until, args.dry_run)
        elif args.path.is_dir():
            trim_directory(args.path, since, until, args.pattern, args.dry_run)
        else:
            log.error(f"Path not found: {args.path}")
            sys.exit(1)
    except KeyboardInterrupt:
        log.warning("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        log.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

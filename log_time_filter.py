#!/usr/bin/env python3
"""Fast time-window filtering for large Fluentd/Kubernetes log trees."""

from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


# Supports:
# - 2026-08-10T15:30:00
# - 2026-08-10 15:30:00
# - 2026/08/10 15:30:00
TIMESTAMP_RE = re.compile(
    rb"(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
)

# Optional folder-level pruning for common date tags in path names.
FOLDER_DATE_RE = re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})")


@dataclass(frozen=True)
class FileTimeRange:
    path: str
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]


def _parse_ts_match(match: re.Match[bytes]) -> Optional[datetime]:
    try:
        parts = [int(g) for g in match.groups()]
        # Keep naive UTC for fast comparisons; consistent with fromtimestamp(..., UTC)
        return datetime(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], tzinfo=timezone.utc)
    except Exception:
        return None


def _extract_first_ts(block: bytes) -> Optional[datetime]:
    m = TIMESTAMP_RE.search(block)
    if not m:
        return None
    return _parse_ts_match(m)


def _extract_last_ts(block: bytes) -> Optional[datetime]:
    matches = list(TIMESTAMP_RE.finditer(block))
    if not matches:
        return None
    return _parse_ts_match(matches[-1])


def _path_date_in_range(path: Path, start: datetime, end: datetime) -> bool:
    """
    Optional pruning:
    - If no date in folder names, keep it.
    - If date exists, only keep if day overlaps target day-window.
    """
    for part in path.parts:
        m = FOLDER_DATE_RE.search(part)
        if not m:
            continue
        try:
            y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            day = datetime(y, mm, dd, tzinfo=timezone.utc)
            if start.date() <= day.date() <= end.date():
                return True
            return False
        except Exception:
            return True
    return True


def _read_start_end_timestamps(file_path: Path, block_size: int = 8192) -> FileTimeRange:
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None

    with file_path.open("rb") as fh:
        # Start timestamp from the first block.
        head = fh.read(block_size)
        start_ts = _extract_first_ts(head)

        # End timestamp from the last block.
        size = fh.seek(0, os.SEEK_END)
        if size == 0:
            return FileTimeRange(str(file_path), start_ts, end_ts)
        tail_start = max(0, size - block_size)
        fh.seek(tail_start, os.SEEK_SET)
        tail = fh.read(block_size)
        end_ts = _extract_last_ts(tail)

    return FileTimeRange(str(file_path), start_ts, end_ts)


def _file_overlaps_range(
    file_path: Path,
    range_start: datetime,
    range_end: datetime,
    block_size: int,
) -> Tuple[str, bool]:
    try:
        st = file_path.stat()
        # 1) Metadata-first prefilter.
        if st.st_mtime < range_start.timestamp():
            return str(file_path), False

        ts_range = _read_start_end_timestamps(file_path, block_size=block_size)

        # If timestamps are absent/unparseable, keep by conservative policy
        # because file mtime already says it may be relevant.
        if ts_range.start_ts is None or ts_range.end_ts is None:
            return str(file_path), True

        # 2) O(1) range rejection using first/last timestamps.
        if ts_range.end_ts < range_start:
            return str(file_path), False
        if ts_range.start_ts > range_end:
            return str(file_path), False

        return str(file_path), True
    except (OSError, PermissionError):
        # 3) Graceful exception handling: skip unreadable files.
        return str(file_path), False


def _iter_candidate_files(log_directory: Path) -> Iterable[Path]:
    # os.walk is kept for compatibility; scandir in the walk internals is efficient.
    for root, dirs, files in os.walk(log_directory):
        root_path = Path(root)
        for name in files:
            # Adjust this suffix filter if your logs differ.
            if name.endswith(".log") or ".log." in name:
                yield root_path / name


def _filter_logs_between_datetimes(
    log_directory: str,
    range_start: datetime,
    range_end: datetime,
    max_workers: int = 16,
    block_size: int = 8192,
    enable_directory_pruning: bool = True,
) -> List[str]:
    root = Path(log_directory)
    if not root.exists() or not root.is_dir():
        return []

    candidates: List[Path] = []
    for file_path in _iter_candidate_files(root):
        if enable_directory_pruning and not _path_date_in_range(file_path.parent, range_start, range_end):
            continue
        candidates.append(file_path)

    if not candidates:
        return []

    selected: List[str] = []
    workers = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                _file_overlaps_range,
                path,
                range_start,
                range_end,
                block_size,
            )
            for path in candidates
        ]
        for fut in as_completed(futures):
            path, keep = fut.result()
            if keep:
                selected.append(path)

    selected.sort()
    return selected


def filter_logs_between_epochs(
    log_directory: str,
    range_start_epoch: int,
    range_end_epoch: int,
    max_workers: int = 16,
    block_size: int = 8192,
    enable_directory_pruning: bool = True,
) -> List[str]:
    """
    Return log files whose time span overlaps [range_start_epoch, range_end_epoch].
    """
    range_start = datetime.fromtimestamp(int(range_start_epoch), tz=timezone.utc)
    range_end = datetime.fromtimestamp(int(range_end_epoch), tz=timezone.utc)
    return _filter_logs_between_datetimes(
        log_directory=log_directory,
        range_start=range_start,
        range_end=range_end,
        max_workers=max_workers,
        block_size=block_size,
        enable_directory_pruning=enable_directory_pruning,
    )


def filter_logs_in_time_range(
    log_directory: str,
    hours: int = 24,
    max_workers: int = 16,
    block_size: int = 8192,
    enable_directory_pruning: bool = True,
) -> List[str]:
    """
    Return log files whose time span overlaps the last `hours`.

    Strategy:
    1) Metadata pre-filter by mtime.
    2) Binary O(1) timestamp probing from first + last block.
    3) Parallel file inspection via ThreadPoolExecutor.
    4) Optional directory-name date pruning.
    """
    now = datetime.now(timezone.utc)
    range_end = now
    range_start = now - timedelta(hours=hours)

    return _filter_logs_between_datetimes(
        log_directory=log_directory,
        range_start=range_start,
        range_end=range_end,
        max_workers=max_workers,
        block_size=block_size,
        enable_directory_pruning=enable_directory_pruning,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fast time-window filter for Fluentd logs")
    parser.add_argument("log_directory", help="Root log directory to scan")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    parser.add_argument("--from-epoch", type=int, default=None, help="Explicit start epoch (UTC seconds)")
    parser.add_argument("--to-epoch", type=int, default=None, help="Explicit end epoch (UTC seconds)")
    parser.add_argument("--workers", type=int, default=16, help="Parallel worker count")
    parser.add_argument("--block-size", type=int, default=8192, help="Head/tail read size in bytes")
    parser.add_argument("--no-dir-prune", action="store_true", help="Disable optional directory-name pruning")
    args = parser.parse_args()

    if args.from_epoch is not None or args.to_epoch is not None:
        if args.from_epoch is None or args.to_epoch is None:
            raise SystemExit("--from-epoch and --to-epoch must be provided together")
        result = filter_logs_between_epochs(
            log_directory=args.log_directory,
            range_start_epoch=args.from_epoch,
            range_end_epoch=args.to_epoch,
            max_workers=args.workers,
            block_size=args.block_size,
            enable_directory_pruning=not args.no_dir_prune,
        )
    else:
        result = filter_logs_in_time_range(
            log_directory=args.log_directory,
            hours=args.hours,
            max_workers=args.workers,
            block_size=args.block_size,
            enable_directory_pruning=not args.no_dir_prune,
        )
    for p in result:
        print(p)

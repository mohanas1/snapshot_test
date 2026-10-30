"""Append-only JSONL store for completed snapshot runs (timing + config hints)."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, List

_lock = threading.Lock()


def append_record(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
    with _lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)


def load_records(path: Path, max_lines: int = 2000) -> List[Dict[str, Any]]:
    """Return the last *max_lines* JSON records from an append-only JSONL file.

    Reads the file in reverse chunks from the end so it never loads the entire
    file into memory — critical when individual records can be large (e.g. SC
    analysis results).
    """
    if not path.is_file():
        return []

    # Each chunk is 256 KB; for large records we read more chunks until we
    # have collected enough complete lines.
    CHUNK = 262144  # 256 KB
    collected: List[bytes] = []
    newline_count = 0

    with _lock:
        try:
            with open(path, "rb") as fh:
                fh.seek(0, 2)
                file_size = fh.tell()
                pos = file_size
                while pos > 0 and newline_count <= max_lines:
                    read_size = min(CHUNK, pos)
                    pos -= read_size
                    fh.seek(pos)
                    chunk = fh.read(read_size)
                    collected.append(chunk)
                    newline_count += chunk.count(b"\n")
        except OSError:
            return []

    raw = b"".join(reversed(collected)).decode("utf-8", errors="replace")
    lines = raw.splitlines()
    tail = lines[-max_lines:] if len(lines) > max_lines else lines
    out: List[Dict[str, Any]] = []
    for line in tail:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out

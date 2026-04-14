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
    if not path.is_file():
        return []
    with _lock:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return []
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

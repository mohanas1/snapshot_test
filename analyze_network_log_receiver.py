#!/usr/bin/env python3
"""Build a detailed network-log receiver report from a fluentd-aggregated log.

Supports fetching the log directly from Prism Central via SSH + kubectl cp.

Reads FlowBatch receive lines and JetStream publish lines, then writes:

  network_receiver_analysis.json   full structured dataset
  network_receiver_hourly.csv      one row per hour
  network_receiver_batches.csv     one row per received batch, joined to its publish
  network_receiver_analysis.md     readable report

If netwrk_los.txt sits next to the receiver log, the script also reconciles
published batches with ncm-data-processor "Message Terminated" events.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import re
import shutil
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
#  ★  CONFIG — edit these two blocks and run
# ─────────────────────────────────────────────────────────────────────────────

# Prism Central connection
PC_IP: str = "10.120.10.9"
PC_SSH_USER: str = "nutanix"
PC_SSH_PASS: str = "nutanix/4u"

# If True  → fetch a fresh kubeconfig + log from the PC before analysing.
# If False → use whatever files are already on disk (error if missing).
FETCH_FRESH: bool = True

# Local paths
KUBECONFIG_FILE: Path = Path("pc_kube.txt")   # kubeconfig saved / reused
RECEIVER_LOG_FILE: Path = Path("file.log")    # fetched log (receiver input)
PROCESSOR_LOG_FILE: Path = Path("netwrk_los.txt")  # optional processor log

# Fluentd pod details (used when FETCH_FRESH=True)
FLUENTD_NAMESPACE: str = "ntnx-system"
FLUENTD_POD: str = "fluentd-aggregator-0"
FLUENTD_LOG_BASE: str = "/fluentd/data/logs"
LOG_RECEIVER_KEYWORD: str = "ncm-network-log-receiver"
# Additional pod keyword names to fetch when FETCH_FRESH=True.
# Logs for each keyword are saved as <keyword>.log in the output directory.
EXTRA_POD_KEYWORDS: list[str] = [
    "ncm-network-log-data-processor",
    "diis",
    "nats",
]
LOG_RECEIVER_FILENAME: str = "file.log.log"

# MSP kubeconfig cluster name
MSP_CLUSTER_NAME: str = "nc"

# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TS_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


# ─────────────────────────────────────────────────────────────────────────────
#  Fetch helpers
# ─────────────────────────────────────────────────────────────────────────────

def _run(cmd: list[str], *, check: bool = True, capture: bool = False, timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a command, streaming output unless capture=True."""
    log.info("$ %s", " ".join(str(c) for c in cmd))
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )


def fetch_kubeconfig(pc_ip: str, ssh_user: str, ssh_pass: str,
                     kubeconfig: Path, cluster: str = MSP_CLUSTER_NAME) -> None:
    """
    Fetch a fresh kubeconfig from Prism Central via SSH and save it locally.

    Equivalent to:
      sshpass -p '<pass>' ssh <user>@<ip> -- /usr/local/nutanix/cluster/bin/mspctl cls kubeconfig <cluster> > kubeconfig
    """
    if not shutil.which("sshpass"):
        raise RuntimeError(
            "sshpass is not installed — cannot fetch kubeconfig automatically. "
            "Manually copy the kubeconfig to %s, then re-run with FETCH_FRESH=False." % kubeconfig
        )
    cmd = [
        "sshpass", "-p", ssh_pass,
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        f"{ssh_user}@{pc_ip}",
        "--",
        "/usr/local/nutanix/cluster/bin/mspctl", "cls", "kubeconfig", cluster,
    ]
    result = _run(cmd, capture=True, timeout=30)
    # Strip the "Prism Central VM" banner line that sshd injects
    lines = [l for l in result.stdout.splitlines() if not l.strip().startswith("Prism Central")]
    kubeconfig.write_text("\n".join(lines) + "\n")
    log.info("Kubeconfig saved → %s (%d bytes)", kubeconfig, kubeconfig.stat().st_size)


def discover_log_path(kubeconfig: Path, namespace: str, pod: str,
                      base: str, keyword: str, filename: str) -> str:
    """
    Find the remote path of the receiver log inside the fluentd pod.

    Runs: kubectl exec <pod> -- find <base> -name <filename> -path '*<keyword>*'
    Returns the first match.
    """
    paths = discover_all_log_paths(kubeconfig, namespace, pod, base, keyword, filename)
    if not paths:
        raise RuntimeError(
            f"Could not find '{filename}' under '{base}' matching '{keyword}' "
            f"in pod {pod}. Check FLUENTD_LOG_BASE / LOG_RECEIVER_KEYWORD."
        )
    chosen = paths[-1]
    if len(paths) > 1:
        log.warning(
            "Multiple log paths found for '%s' — only first replica used by this caller.\n"
            "  Use discover_all_log_paths() to fetch all %d replicas:\n  %s",
            keyword, len(paths), "\n  ".join(paths),
        )
    log.info("Discovered log path: %s", chosen)
    return chosen


def discover_all_log_paths(kubeconfig: Path, namespace: str, pod: str,
                           base: str, keyword: str, filename: str) -> list[str]:
    """
    Return ALL remote log paths inside the fluentd pod that match the keyword.

    Handles multiple replicas: e.g. two pods ncm-network-log-receiver-xxx-a
    and ncm-network-log-receiver-xxx-b each produce a separate log file under
    FLUENTD_LOG_BASE.
    """
    cmd = [
        "kubectl", "--kubeconfig", str(kubeconfig),
        "exec", "-n", namespace, pod,
        "--", "find", base, "-name", filename, "-path", f"*{keyword}*",
    ]
    result = _run(cmd, capture=True, timeout=30)
    paths = [p.strip() for p in result.stdout.splitlines() if p.strip()]
    log.info("Found %d log path(s) for keyword '%s': %s", len(paths), keyword, paths)
    return paths


def fetch_log_from_pc(kubeconfig: Path, namespace: str, pod: str,
                      remote_path: str, local_path: Path) -> None:
    """
    Copy the log file out of the fluentd pod.

    Equivalent to:
      kubectl --kubeconfig=<kc> cp -n <ns> <pod>:<remote> <local>
    """
    cmd = [
        "kubectl", "--kubeconfig", str(kubeconfig),
        "cp", "-n", namespace,
        f"{pod}:{remote_path}",
        str(local_path),
    ]
    _run(cmd, timeout=120)
    size = local_path.stat().st_size
    log.info("Log fetched → %s (%.1f KB)", local_path, size / 1024)


def _pod_name_from_path(remote_path: str) -> str:
    """Extract the pod name from a fluentd log remote path.

    Fluentd paths look like:
      /fluentd/data/logs/kube.<namespace>.<pod-name>.<uid>.<container>.<node>/file.log.log

    Returns the pod-name component, or a fallback slug if parsing fails.
    """
    # The directory name is kube.<ns>.<pod>.<uid>.<container>.<node>
    dir_name = remote_path.rstrip("/").split("/")[-2]  # second-to-last component
    parts = dir_name.split(".")
    if len(parts) >= 3 and parts[0] == "kube":
        return parts[2]  # pod name is always at index 2
    # Fallback: use last path segment minus extension
    return Path(remote_path).parent.name.replace(".", "-")[:60]


def _iter_log_lines(*paths: "Path | None") -> "Iterator[str]":
    """Yield all text lines from one or more log files in sequence.

    Silently skips paths that are None or do not exist.
    """
    from typing import Iterator  # noqa: F401
    for p in paths:
        if not p or not p.is_file():
            continue
        with p.open(errors="replace") as fh:
            yield from fh


def fetch_replica_logs(
    kubeconfig: Path,
    namespace: str,
    pod: str,
    remote_paths: list[str],
    out_dir: Path,
) -> "tuple[list[Path], int]":
    """Fetch each replica log into its own file named after the pod.

    Files are saved as ``<out_dir>/<pod_name>.log``.

    Returns ``(list_of_local_paths, total_bytes)``.
    """
    if not remote_paths:
        raise RuntimeError("fetch_replica_logs: remote_paths is empty")

    import tempfile

    out_dir.mkdir(parents=True, exist_ok=True)
    local_paths: list[Path] = []
    total_bytes = 0

    for rp in remote_paths:
        pod_name = _pod_name_from_path(rp)
        local_path = out_dir / f"{pod_name}.log"
        # Avoid overwriting if two replicas happen to have the same pod-name slug
        if local_path.exists():
            suffix = 1
            while (out_dir / f"{pod_name}-{suffix}.log").exists():
                suffix += 1
            local_path = out_dir / f"{pod_name}-{suffix}.log"

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            cmd = [
                "kubectl", "--kubeconfig", str(kubeconfig),
                "cp", "-n", namespace,
                f"{pod}:{rp}",
                str(tmp_path),
            ]
            _run(cmd, timeout=120)
            chunk = tmp_path.read_bytes()
            local_path.write_bytes(chunk)
            total_bytes += len(chunk)
            log.info(
                "Saved replica log → %s (%.1f KB)",
                local_path.name, len(chunk) / 1024,
            )
            local_paths.append(local_path)
        except Exception as exc:
            log.warning("Could not fetch replica %s: %s — skipping", rp, exc)
        finally:
            tmp_path.unlink(missing_ok=True)

    log.info(
        "Fetched %d replica log(s) into %s (%.1f KB total)",
        len(local_paths), out_dir, total_bytes / 1024,
    )
    return local_paths, total_bytes


# ─────────────────────────────────────────────────────────────────────────────
#  Time-window pruning of replica log files
# ─────────────────────────────────────────────────────────────────────────────

# Broad timestamp regex — matches the most common log timestamp formats:
#   ISO-8601 : 2026-10-23T10:05:00   2026-10-23 10:05:00
#   NATS     : 2026/10/23 10:05:00
_BROAD_TS = re.compile(
    r'(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}):(\d{2}):(\d{2})'
)
# glog: [IWEF]MMDD HH:MM:SS  (no year; we assume current year)
_GLOG_TS = re.compile(
    r'^[IWEF](\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})',
    re.MULTILINE,
)
_CURRENT_YEAR = str(datetime.now(timezone.utc).year)


def _extract_timestamps_from_chunk(chunk: str) -> list[datetime]:
    """Return all parseable timestamps found in a text chunk."""
    found: list[datetime] = []
    for m in _BROAD_TS.finditer(chunk):
        try:
            found.append(datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6)),
                tzinfo=timezone.utc,
            ))
        except ValueError:
            pass
    for m in _GLOG_TS.finditer(chunk):
        try:
            ts_str = f"{_CURRENT_YEAR}-{m.group(1)}-{m.group(2)}T{m.group(3)}:{m.group(4)}:{m.group(5)}"
            found.append(datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc))
        except ValueError:
            pass
    return found


def _prune_replica_files_outside_window(
    paths: list[Path],
    since: "datetime | None",
    until: "datetime | None",
    chunk_size: int = 8192,
) -> tuple[list[Path], list[Path]]:
    """Delete replica log files that contain no data within [since, until].

    Strategy (logs are written chronologically):
    - If the last timestamp in the file < since  → whole file predates window → delete.
    - If the first timestamp in the file > until → whole file postdates window → delete.
    - Files with no parseable timestamp are kept (safe default).

    Returns (kept_paths, deleted_paths).
    """
    if not since and not until:
        return paths, []

    kept: list[Path] = []
    deleted: list[Path] = []

    for p in paths:
        if not p.is_file():
            kept.append(p)
            continue
        file_size = p.stat().st_size
        if file_size == 0:
            try:
                p.unlink()
            except OSError:
                pass
            deleted.append(p)
            continue

        with p.open("rb") as fh:
            head_bytes = fh.read(min(chunk_size, file_size))
            if file_size > chunk_size * 2:
                fh.seek(-chunk_size, 2)
                tail_bytes = fh.read(chunk_size)
            else:
                tail_bytes = head_bytes

        head_ts = _extract_timestamps_from_chunk(head_bytes.decode("utf-8", errors="replace"))
        tail_ts = _extract_timestamps_from_chunk(tail_bytes.decode("utf-8", errors="replace"))

        if not head_ts and not tail_ts:
            # Can't determine time coverage — keep it (safe)
            kept.append(p)
            continue

        first_ts = min(head_ts) if head_ts else None
        last_ts  = max(tail_ts) if tail_ts else (min(head_ts) if head_ts else None)

        out_of_range = False
        if since and last_ts and last_ts < since:
            out_of_range = True   # entire file is before the window
        if until and first_ts and first_ts > until:
            out_of_range = True   # entire file is after the window

        if out_of_range:
            try:
                p.unlink()
                deleted.append(p)
                log.info(
                    "Pruned out-of-window replica: %s  (last=%s, since=%s)",
                    p.name,
                    last_ts.isoformat() if last_ts else "?",
                    since.isoformat() if since else "—",
                )
            except OSError as exc:
                log.warning("Could not delete %s: %s", p.name, exc)
                kept.append(p)
        else:
            kept.append(p)

    return kept, deleted


def fetch_and_merge_logs(
    kubeconfig: Path,
    namespace: str,
    pod: str,
    remote_paths: list[str],
    local_path: Path,
) -> int:
    """Fetch log files for ALL matching remote_paths and concatenate into *local_path*.

    Returns the total bytes written.
    """
    if not remote_paths:
        raise RuntimeError("fetch_and_merge_logs: remote_paths is empty")

    import tempfile

    local_path.parent.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    with local_path.open("wb") as out_fh:
        for rp in remote_paths:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                cmd = [
                    "kubectl", "--kubeconfig", str(kubeconfig),
                    "cp", "-n", namespace,
                    f"{pod}:{rp}",
                    str(tmp_path),
                ]
                _run(cmd, timeout=120)
                chunk = tmp_path.read_bytes()
                # Ensure the chunk ends with a newline before appending the next
                if chunk and not chunk.endswith(b"\n"):
                    chunk += b"\n"
                out_fh.write(chunk)
                total_bytes += len(chunk)
                log.info("Appended replica log %s → %s (%.1f KB)", rp, local_path.name, len(chunk) / 1024)
            finally:
                tmp_path.unlink(missing_ok=True)

    log.info(
        "Merged %d replica log(s) → %s (%.1f KB total)",
        len(remote_paths), local_path, total_bytes / 1024,
    )
    return total_bytes


def ensure_log_file(fetch_fresh: bool) -> Path:
    """
    Either fetch a fresh copy of the receiver log or verify the local one exists.
    Returns the local Path to pass to the analyser.
    """
    local = RECEIVER_LOG_FILE

    if not fetch_fresh:
        if not local.exists():
            log.error(
                "FETCH_FRESH=False but '%s' does not exist.  "
                "Either set FETCH_FRESH=True or manually copy the log here.", local
            )
            sys.exit(1)
        log.info("Using existing log file: %s (%.1f KB)", local, local.stat().st_size / 1024)
        return local

    # ── FETCH_FRESH=True ────────────────────────────────────────────────────
    kc = KUBECONFIG_FILE
    if kc.exists():
        log.info("Re-using existing kubeconfig: %s", kc)
    else:
        log.info("Fetching kubeconfig from PC %s …", PC_IP)
        fetch_kubeconfig(PC_IP, PC_SSH_USER, PC_SSH_PASS, kc)

    log.info("Discovering receiver log path inside %s/%s …", FLUENTD_NAMESPACE, FLUENTD_POD)
    remote_paths = discover_all_log_paths(
        kc, FLUENTD_NAMESPACE, FLUENTD_POD,
        FLUENTD_LOG_BASE, LOG_RECEIVER_KEYWORD, LOG_RECEIVER_FILENAME,
    )
    if not remote_paths:
        log.error("No log files found for keyword '%s'.", LOG_RECEIVER_KEYWORD)
        sys.exit(1)

    log.info("Fetching and merging %d replica log(s) …", len(remote_paths))
    fetch_and_merge_logs(kc, FLUENTD_NAMESPACE, FLUENTD_POD, remote_paths, local)
    return local


# ─────────────────────────────────────────────────────────────────────────────
#  Core analysis (unchanged public API + new helpers)
# ─────────────────────────────────────────────────────────────────────────────

def parse_ts(value: str) -> datetime:
    if value.endswith("Z") and "." in value:
        return datetime.strptime(value, TS_FMT).replace(tzinfo=timezone.utc)
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    raise ValueError(f"unsupported timestamp: {value}")


def parse_time_arg(value: str | None) -> datetime | None:
    """Parse a --since/--until CLI value.

    Accepts:
      • Relative offsets  : "24h", "6h", "1h", "7d", "30d", "90m"
      • ISO 8601 timestamp: "2026-10-21T00:00:00Z" or "2026-10-21T00:00:00+00:00"
                            or bare date "2026-10-21" (treated as 00:00:00 UTC)

    Returns a timezone-aware UTC datetime, or None if value is empty/None.
    """
    if not value:
        return None
    value = value.strip()
    # Relative offset: e.g. "24h", "6h", "1h", "7d", "30m"
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(h|d|m|s)", value, re.IGNORECASE)
    if m:
        from datetime import timedelta
        amount = float(m.group(1))
        unit = m.group(2).lower()
        seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        return datetime.now(timezone.utc) - timedelta(seconds=amount * seconds)
    # Bare date "YYYY-MM-DD"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # ISO timestamp
    ts = value.replace("Z", "+00:00")
    try:
        from datetime import timedelta as _td  # noqa
        dt_obj = datetime.fromisoformat(ts)
    except ValueError:
        raise ValueError(f"Cannot parse time argument: {value!r}  "
                         f"(expected ISO timestamp or relative offset like '24h')")
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj


def percentile(sorted_values: list[float], p: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * p
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    weight = rank - low
    return sorted_values[low] * (1 - weight) + sorted_values[high] * weight


def summary(values: list[float]) -> dict:
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "sum": round(sum(ordered), 3),
        "min": ordered[0],
        "max": ordered[-1],
        "mean": round(statistics.fmean(ordered), 3),
        "median": round(percentile(ordered, 0.5), 3),
        "p90": round(percentile(ordered, 0.90), 3),
        "p95": round(percentile(ordered, 0.95), 3),
        "p99": round(percentile(ordered, 0.99), 3),
    }


def load_json_lines(path: Path) -> tuple[list[tuple[int, dict]], list[tuple[int, str]]]:
    # Matches Kubernetes log-stream prefixes like "2026-10-15T13:50:34.042809330Z "
    _K8S_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+")
    records: list[tuple[int, dict]] = []
    plain: list[tuple[int, str]] = []
    with path.open(errors="replace") as handle:
        for line_no, line in enumerate(handle, 1):
            text = ANSI.sub("", line.strip())
            if not text:
                continue
            # Strip Kubernetes pod-log timestamp prefix if present
            if not text.startswith("{"):
                text = _K8S_PREFIX.sub("", text)
            if not text.startswith("{"):
                plain.append((line_no, text))
                continue
            try:
                records.append((line_no, json.loads(text)))
            except json.JSONDecodeError:
                plain.append((line_no, text))
    return records, plain


_MSG_RECEIVED   = "Received FlowBatch"
_MSG_PUBLISHED  = "Published batch to jet-stream"
_MSG_SPLIT      = (
    "FlowBatch exceeds max_flows for a single JetStream message; "
    "splitting into multiple publishes"
)
_MSG_STATS      = "Flow throughput stats"

def _make_publish(record: dict, received_at: str, *,
                  size_bytes: int | None = None) -> dict:
    """Build a publish dict from a log record, normalising the size field name."""
    # Field was "size in bytes" in older builds; "compressedBytes" in newer builds.
    raw_size = (
        record.get("compressedBytes")
        or record.get("size in bytes")
        if size_bytes is None
        else size_bytes
    )
    pub: dict = {
        "line": None,           # caller fills in
        "published_at": record.get("timestamp", ""),
        "size_bytes": raw_size,
        "uncompressed_bytes": record.get("uncompressedBytes"),
        "subject": record.get("subject"),
        "ack": record.get("ack"),
    }
    try:
        lag = round(
            (parse_ts(pub["published_at"]) - parse_ts(received_at)).total_seconds()
            * 1000,
            3,
        )
    except Exception:
        lag = None
    pub["lag_ms"] = lag
    return pub


def _close_split(pending: dict) -> dict:
    """Merge accumulated split publish parts into one publish dict."""
    parts = pending.pop("_split_parts", [])
    total_bytes = sum(p["size_bytes"] or 0 for p in parts)
    total_uncompressed = sum(p.get("uncompressed_bytes") or 0 for p in parts)
    last = parts[-1] if parts else {}
    flows = pending.get("num_flows") or 0
    return {
        "line": last.get("line"),
        "published_at": last.get("published_at", ""),
        "lag_ms": last.get("lag_ms"),
        "size_bytes": total_bytes,
        "uncompressed_bytes": total_uncompressed,
        "subject": last.get("subject"),
        "ack": all(p.get("ack") for p in parts),
        "bytes_per_flow": round(total_bytes / flows, 3) if flows else None,
        "is_split": True,
        "split_count": len(parts),
    }


def pair_batches(records: list[tuple[int, dict]]) -> tuple[list[dict], dict]:
    """Join each Received FlowBatch with its one or more publish lines.

    Handles two message formats for the publish size field:
      • old logs  → "size in bytes"
      • new logs  → "compressedBytes" / "uncompressedBytes"

    Handles split batches: when the receiver log contains
      "FlowBatch exceeds max_flows for a single JetStream message; splitting…"
    the single receive is followed by N publish lines (N = ceil(numFlows/maxFlows)).
    All N publish sizes are summed into one merged publish entry so downstream
    aggregations continue to work without changes.

    Any unrecognised top-level message keys are captured in startup["unknown_msgs"]
    so new log patterns surface immediately in the report.
    """
    batches: list[dict] = []
    pending: dict | None = None
    orphan_publishes = 0
    startup: dict[str, object] = {}
    unknown_msgs: Counter = Counter()

    def _flush_pending(reason: str) -> None:
        """Close a pending batch that never received its publish(es)."""
        nonlocal pending
        if pending is None:
            return
        if "_split_parts" in pending and pending["_split_parts"]:
            # End-of-log or new-receive arrived while split still accumulating.
            pending["publish"] = _close_split(pending)
            pending["publish"]["incomplete_split"] = True
        else:
            pending["publish"] = None
        batches.append(pending)
        pending = None

    for line_no, record in records:
        message = record.get("msg", "")

        # ── Receive ──────────────────────────────────────────────────────────
        if message == _MSG_RECEIVED:
            _flush_pending("new receive arrived")
            pending = {
                "line": line_no,
                "received_at": record["timestamp"],
                "batch_uuid": record.get("batchUuid"),
                "host": record.get("host"),
                "cluster": record.get("cluster"),
                "num_flows": record.get("numFlows"),
                "flow_record_count": record.get("flowRecordCount"),
            }
            continue

        # ── Split warning ─────────────────────────────────────────────────────
        if message == _MSG_SPLIT:
            if pending is not None:
                max_flows = record.get("maxFlows") or 1
                num_flows = pending.get("num_flows") or record.get("numFlows") or 1
                pending["_split_expected"] = max(2, math.ceil(num_flows / max_flows))
                pending["_split_parts"] = []
            continue

        # ── Publish ───────────────────────────────────────────────────────────
        if message == _MSG_PUBLISHED:
            if pending is None:
                orphan_publishes += 1
                continue

            pub = _make_publish(record, pending["received_at"])
            pub["line"] = line_no

            if "_split_parts" in pending:
                # Accumulate split pieces
                pending["_split_parts"].append(pub)
                if len(pending["_split_parts"]) < pending.get("_split_expected", 2):
                    continue  # more pieces expected
                # Got them all — merge and close
                pending["publish"] = _close_split(pending)
                batches.append(pending)
                pending = None
                continue

            # Normal (non-split) publish
            flows = pending["num_flows"] or 0
            size  = pub["size_bytes"] or 0
            pub["bytes_per_flow"] = round(size / flows, 3) if flows else None
            pending["publish"] = pub
            batches.append(pending)
            pending = None
            continue

        # ── Skip known non-batch messages ─────────────────────────────────────
        if message == _MSG_STATS:
            continue

        # ── Everything else → startup / unknown ───────────────────────────────
        unknown_msgs[message] += 1
        startup.setdefault("events", [])
        if len(startup["events"]) < 40:
            extra = {
                key: value
                for key, value in record.items()
                if key not in ("level", "timestamp", "caller", "msg")
            }
            startup["events"].append(
                {
                    "line": line_no,
                    "timestamp": record.get("timestamp"),
                    "caller": record.get("caller"),
                    "msg": message,
                    "fields": extra,
                }
            )

    # Close any dangling pending at end of file
    _flush_pending("end of file")

    # Throughput-stats pass (unchanged)
    stats = []
    for line_no, record in records:
        if record.get("msg") != _MSG_STATS:
            continue
        stats.append(
            {
                "line": line_no,
                "timestamp": record["timestamp"],
                "interval_seconds": record.get("interval in seconds"),
                "flows_in_interval": record.get("flows in last interval"),
                "total_flows_since_start": record.get("total flows since start"),
            }
        )
    startup["throughput_stats"] = stats
    startup["orphan_publishes"] = orphan_publishes
    startup["unpaired_receives"] = sum(1 for batch in batches if batch["publish"] is None)

    # Surface any new/unrecognised message patterns seen in this log
    known = {_MSG_RECEIVED, _MSG_PUBLISHED, _MSG_SPLIT, _MSG_STATS}
    startup["unknown_msgs"] = [
        {"msg": msg, "count": cnt}
        for msg, cnt in unknown_msgs.most_common()
        if msg not in known
    ]

    return batches, startup


def load_processor_terminations(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    terminations = []
    with path.open(errors="replace") as handle:
        for line_no, line in enumerate(handle, 1):
            text = ANSI.sub("", line.strip())
            if not text.startswith("{") or "Message Terminated" not in text:
                continue
            record = json.loads(text)
            if record.get("msg") != "Message Terminated":
                continue
            terminations.append(
                {
                    "line": line_no,
                    "timestamp": record.get("ts") or record.get("timestamp"),
                    "pipeline": record.get("pipeline-name"),
                    "worker": record.get("worker-name"),
                    "subject": record.get("subject"),
                    "error": record.get("error"),
                }
            )
    return terminations


def bucket_key(timestamp: str, width: str) -> str:
    if width == "day":
        return timestamp[:10]
    if width == "hour":
        return timestamp[:13]
    raise ValueError(width)


def aggregate(batches: list[dict], width: str) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for batch in batches:
        grouped[bucket_key(batch["received_at"], width)].append(batch)
    rows = []
    for key in sorted(grouped):
        items = grouped[key]
        flows = [item["num_flows"] for item in items if item["num_flows"] is not None]
        sizes = [
            item["publish"]["size_bytes"]
            for item in items
            if item["publish"] and item["publish"]["size_bytes"] is not None
        ]
        lags = [
            item["publish"]["lag_ms"]
            for item in items
            if item["publish"] and item["publish"]["lag_ms"] is not None
        ]
        bpf = [
            item["publish"]["bytes_per_flow"]
            for item in items
            if item["publish"] and item["publish"]["bytes_per_flow"] is not None
        ]
        rows.append(
            {
                width: key,
                "batches": len(items),
                "flows": int(sum(flows)),
                "flows_summary": summary(flows),
                "bytes": int(sum(sizes)),
                "bytes_summary": summary(sizes),
                "bytes_per_flow_summary": summary(bpf),
                "lag_ms_summary": summary(lags),
                "acks": sum(1 for item in items if item["publish"] and item["publish"]["ack"] is True),
                "empty_cluster": sum(1 for item in items if item["cluster"] == ""),
            }
        )
    return rows


def gaps(batches: list[dict], min_seconds: float, max_seconds: float) -> list[dict]:
    found = []
    for previous, current in zip(batches, batches[1:]):
        delta = (parse_ts(current["received_at"]) - parse_ts(previous["received_at"])).total_seconds()
        if delta < min_seconds or delta > max_seconds:
            found.append(
                {
                    "gap_seconds": round(delta, 3),
                    "from": previous["received_at"],
                    "to": current["received_at"],
                    "from_batch_uuid": previous["batch_uuid"],
                    "to_batch_uuid": current["batch_uuid"],
                    "from_flows": previous["num_flows"],
                    "to_flows": current["num_flows"],
                }
            )
    return found


def detect_regime(hourly: list[dict], jump_ratio: float = 1.5) -> dict | None:
    """Find the first hour whose mean flows/batch jumps well above the prior hour."""
    previous = None
    for row in hourly:
        mean = row["flows_summary"].get("mean")
        if previous and previous["flows_summary"].get("mean") and mean:
            before = previous["flows_summary"]["mean"]
            if before > 0 and mean / before >= jump_ratio:
                return {
                    "from_hour": previous["hour"],
                    "to_hour": row["hour"],
                    "from_mean_flows": before,
                    "to_mean_flows": mean,
                }
        previous = row
    return None


def split_regimes(batches: list[dict], jump_hour: str | None) -> list[dict]:
    if not jump_hour:
        return []
    boundary = jump_hour + ":00:00.000Z"
    before = [batch for batch in batches if batch["received_at"] < boundary]
    after = [batch for batch in batches if batch["received_at"] >= boundary]

    def pack(name: str, items: list[dict]) -> dict:
        flows = [item["num_flows"] for item in items]
        return {
            "window": name,
            "batches": len(items),
            "flows": int(sum(flows)),
            "flows_summary": summary(flows),
            "start": items[0]["received_at"] if items else None,
            "end": items[-1]["received_at"] if items else None,
        }

    return [pack(f"before {jump_hour}:00Z", before), pack(f"from {jump_hour}:00Z", after)]


def reconcile(batches: list[dict], terminations: list[dict]) -> dict | None:
    if not terminations:
        return None
    last_processor = max(item["timestamp"] for item in terminations)
    overlap = [batch for batch in batches if batch["received_at"] <= last_processor]
    after = [batch for batch in batches if batch["received_at"] > last_processor]
    errors = Counter(item["error"] for item in terminations)
    subjects = Counter(item["subject"] for item in terminations)
    first_batch = batches[0]
    first_term = min(terminations, key=lambda item: item["timestamp"])
    lag_ms = None
    if first_batch["publish"]:
        lag_ms = round(
            (
                parse_ts(first_term["timestamp"]) - parse_ts(first_batch["publish"]["published_at"])
            ).total_seconds()
            * 1000,
            3,
        )
    return {
        "processor_terminations": len(terminations),
        "processor_first": first_term["timestamp"],
        "processor_last": last_processor,
        "receiver_batches_in_overlap": len(overlap),
        "receiver_flows_in_overlap": int(sum(batch["num_flows"] for batch in overlap)),
        "receiver_batches_after_processor_log": len(after),
        "receiver_flows_after_processor_log": int(sum(batch["num_flows"] for batch in after)),
        "count_delta_published_minus_terminated": len(batches) - len(terminations),
        "first_publish_to_first_terminate_ms": lag_ms,
        "error_signatures": errors.most_common(),
        "processor_subjects": subjects.most_common(),
        "processor_pipeline": first_term["pipeline"],
        "processor_worker": first_term["worker"],
    }


def distribution(batches: list[dict], field: str) -> list[dict]:
    counts = Counter(batch[field] for batch in batches)
    return [{"value": value if value != "" else "(empty)", "count": count} for value, count in counts.most_common()]


# ─── New: per-cluster and per-host breakdown ──────────────────────────────────

def per_group_stats(batches: list[dict], field: str) -> list[dict]:
    """Aggregate batches by a string field (e.g. 'cluster', 'host')."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for batch in batches:
        key = batch.get(field) or "(empty)"
        grouped[key].append(batch)
    rows = []
    for key in sorted(grouped, key=lambda k: -len(grouped[k])):
        items = grouped[key]
        published = [b for b in items if b["publish"]]
        flows = [b["num_flows"] for b in items if b["num_flows"] is not None]
        lags = [b["publish"]["lag_ms"] for b in published if b["publish"]["lag_ms"] is not None]
        sizes = [b["publish"]["size_bytes"] for b in published if b["publish"]["size_bytes"] is not None]
        rows.append({
            field: key,
            "batches": len(items),
            "batches_published": len(published),
            "total_flows": int(sum(flows)),
            "flows_mean": round(statistics.fmean(flows), 1) if flows else None,
            "flows_max": max(flows) if flows else None,
            "total_bytes": int(sum(sizes)),
            "lag_ms_median": round(percentile(sorted(lags), 0.5), 1) if lags else None,
            "lag_ms_p95": round(percentile(sorted(lags), 0.95), 1) if lags else None,
            "lag_ms_max": max(lags) if lags else None,
        })
    return rows


def flows_histogram(batches: list[dict], buckets: int = 10) -> list[dict]:
    """Build a simple histogram of batch flow counts."""
    values = [b["num_flows"] for b in batches if b["num_flows"] is not None]
    if not values:
        return []
    lo, hi = min(values), max(values)
    if lo == hi:
        return [{"range": f"{lo}", "count": len(values)}]
    width = (hi - lo) / buckets
    counts: Counter = Counter()
    for v in values:
        bucket = int((v - lo) / width)
        bucket = min(bucket, buckets - 1)
        counts[bucket] += 1
    rows = []
    for i in range(buckets):
        low = round(lo + i * width)
        high = round(lo + (i + 1) * width)
        rows.append({"range": f"{low}–{high}", "count": counts[i]})
    return rows


def error_log_entries(records: list[tuple[int, dict]]) -> list[dict]:
    """Collect all error/warn level log lines.

    For each error entry, also captures numFlows from the most recent
    preceding 'Received FlowBatch' line so callers can correlate NATS
    payload-size failures with the batch size that triggered them.
    
    Filters out IPv6-related warnings as these are expected operational noise.
    """
    out = []
    last_batch_num_flows: int | None = None
    for line_no, record in records:
        msg = record.get("msg", "")
        # Track the most recent received-batch size as we scan forward
        if msg == "Received FlowBatch":
            last_batch_num_flows = record.get("numFlows") or record.get("flowRecordCount")
            continue
        # Split-batch warnings are normal operational behaviour, not errors.
        if msg == _MSG_SPLIT:
            continue
        # Filter out IPv6 skipping warnings (expected operational noise)
        if "ipv6" in msg.lower() and "skip" in msg.lower():
            continue
        lvl = record.get("level", "").lower()
        if lvl in ("error", "warn", "warning", "fatal"):
            out.append({
                "line": line_no,
                "level": record.get("level"),
                "timestamp": record.get("timestamp"),
                "caller": record.get("caller"),
                "msg": msg,
                "num_flows": last_batch_num_flows,
                "fields": {k: v for k, v in record.items()
                           if k not in ("level", "timestamp", "caller", "msg")},
            })
    return out


def rate_analysis(batches: list[dict]) -> dict:
    """Compute flows/minute, peak window, and idle periods."""
    if len(batches) < 2:
        return {}
    start = parse_ts(batches[0]["received_at"])
    end = parse_ts(batches[-1]["received_at"])
    span_minutes = (end - start).total_seconds() / 60
    total_flows = sum(b["num_flows"] or 0 for b in batches)
    avg_flows_per_min = round(total_flows / span_minutes, 2) if span_minutes else None

    # Per-minute buckets
    minute_flows: Counter = Counter()
    for b in batches:
        key = parse_ts(b["received_at"]).strftime("%Y-%m-%dT%H:%M")
        minute_flows[key] += (b["num_flows"] or 0)

    peak_minute = minute_flows.most_common(1)[0] if minute_flows else None
    zero_minutes = sum(1 for v in minute_flows.values() if v == 0)

    return {
        "span_minutes": round(span_minutes, 2),
        "avg_flows_per_minute": avg_flows_per_min,
        "peak_minute": {"minute": peak_minute[0], "flows": peak_minute[1]} if peak_minute else None,
        "active_minutes": len(minute_flows),
        "zero_flow_minutes": zero_minutes,
    }



# ─── Pipeline service parsers ──────────────────────────────────────────────────

def _categorize_nats_error(msg: str) -> str:
    """Categorize NATS errors into groups for better reporting."""
    msg_lower = msg.lower()
    if "route" in msg_lower or "connection" in msg_lower:
        return "Routing/Connection"
    if "dns" in msg_lower or "lookup" in msg_lower or "no such host" in msg_lower:
        return "DNS Resolution"
    if "consumer" in msg_lower and "offline" in msg_lower:
        return "Consumer Offline"
    if "timeout" in msg_lower or "i/o timeout" in msg_lower:
        return "Timeout"
    if "cluster" in msg_lower:
        return "Cluster"
    if "stream" in msg_lower:
        return "Stream"
    return "Other"


def parse_nats_log(path: "Path | list[Path]", run_window: "tuple[str, str] | None" = None) -> dict:
    """
    Parse a NATS server log and extract stream states, errors, cluster events.

    Log format: [<pid>] YYYY/MM/DD HH:MM:SS.ms [LEVEL] message

    *path* may be a single ``Path`` or a list of ``Path`` objects (one per replica).
    All replica files are scanned in sequence; their statistics are aggregated.
    
    *run_window* is an optional ``(start_ts, end_ts)`` tuple of ISO-8601 UTC strings
    taken from the Network Log Receiver's log window. When supplied, errors and warnings
    are filtered to only include events within this time window.
    """
    import re as _re
    LINE = _re.compile(r'^\[(\d+)\] (\S+ \S+) \[(\w+)\] (.*)')
    FS_STATE = _re.compile(
        r'Filestore \[([^\]]+)\].*?state.*?Msgs:(\d+) Bytes:(\d+)'
    )
    
    # ── Run-window helper ────────────────────────────────────────────────────
    _win_start, _win_end = (run_window or ("", ""))
    
    def _normalize_nats_ts(nats_ts: str) -> str:
        """Convert NATS timestamp (YYYY/MM/DD HH:MM:SS) to ISO format for comparison."""
        if not nats_ts or "/" not in nats_ts:
            return nats_ts
        # Convert "2026/11/04 07:15:31.123" to "2026-11-04T07:15:31.123+00:00"
        try:
            parts = nats_ts.split()
            if len(parts) >= 2:
                date_part = parts[0].replace("/", "-")
                time_part = parts[1]
                return f"{date_part}T{time_part}+00:00"
        except:
            pass
        return nats_ts
    
    def _in_window(nats_ts: str) -> bool:
        """Return True if NATS timestamp falls within [_win_start, _win_end]."""
        if not run_window or not nats_ts:
            return True
        # Normalize NATS timestamp to ISO format
        ts_norm = _normalize_nats_ts(nats_ts)
        # Normalize window timestamps (handle Z vs +00:00)
        win_start_norm = _win_start.replace("Z", "+00:00") if _win_start and _win_start.endswith("Z") else _win_start
        win_end_norm = _win_end.replace("Z", "+00:00") if _win_end and _win_end.endswith("Z") else _win_end
        if win_start_norm and ts_norm < win_start_norm:
            return False
        if win_end_norm and ts_norm > win_end_norm:
            return False
        return True

    errors: list[str] = []
    warnings: list[str] = []
    errors_in_window = 0
    warnings_in_window = 0
    streams: dict[str, dict] = {}          # stream_name → latest state
    consumer_leaders: list[str] = []
    cluster_started = False
    cluster_leader: str | None = None
    line_counts: dict[str, int] = {}
    first_ts: str | None = None
    last_ts: str | None = None

    paths = path if isinstance(path, list) else [path]
    if not any(p and p.is_file() for p in paths):
        return {"available": False}

    for line in _iter_log_lines(*paths):
            line = line.strip()
            m = LINE.match(line)
            if not m:
                continue
            ts, lvl, msg = m.group(2), m.group(3), m.group(4)
            if first_ts is None:
                first_ts = ts
            last_ts = ts
            line_counts[lvl] = line_counts.get(lvl, 0) + 1

            # Collect ALL errors/warnings for full history, but track which are in window
            if lvl == "ERR":
                errors.append(msg[:120])
                if _in_window(ts):
                    errors_in_window += 1
            elif lvl == "WRN":
                warnings.append(msg[:120])
                if _in_window(ts):
                    warnings_in_window += 1

            if "JetStream cluster bootstrapping" in msg:
                cluster_started = True
            if "JetStream cluster new metadata leader" in msg:
                cluster_leader = msg.split("leader:")[-1].strip()[:60]
            if "JetStream cluster new consumer leader" in msg:
                consumer_leaders.append(msg[msg.find("'$G"):].strip("'").strip()[:100])

            sm = FS_STATE.search(msg)
            if sm:
                name, msgs, byt = sm.group(1), int(sm.group(2)), int(sm.group(3))
                existing = streams.get(name, {})
                if msgs >= existing.get("msgs", 0):
                    streams[name] = {"name": name, "msgs": msgs, "bytes": byt, "ts": ts}

    ipfix_stream = streams.get("IPFIX-COLLECTOR-1") or streams.get("IPFIX-COLLECTOR") or {}
    # Build a summary table of all non-_meta_ streams sorted by msg count descending
    active_streams = sorted(
        [v for k, v in streams.items() if not k.startswith("_meta_") and v.get("msgs", 0) > 0],
        key=lambda x: x.get("msgs", 0), reverse=True,
    )

    # Compute new NATS health fields (using ALL errors for historical context)
    route_errors = sum(1 for e in errors if "route" in e.lower())
    dns_failures = sum(1 for e in errors if "no such host" in e.lower() or "lookup" in e.lower())
    i_o_timeout_errors = sum(1 for e in errors if "i/o timeout" in e.lower())
    consumer_offline = (
        any("consumer is offline" in e.lower() for e in errors + warnings) or
        any("consumer" in e.lower() for e in errors)
    )
    ipfix_consumer_leader_changes = sum(1 for cl in consumer_leaders if "IPFIX-COLLECTOR-1" in cl)
    
    # Filter errors/warnings for display (only show those in window if window is specified)
    if run_window:
        # For display, only show recent errors (but keep counts from all time)
        display_errors = errors[-50:] if errors_in_window == 0 else errors[-min(errors_in_window, 50):]
        display_warnings = warnings[-50:] if warnings_in_window == 0 else warnings[-min(warnings_in_window, 50):]
    else:
        display_errors = errors
        display_warnings = warnings
    
    # Extract total messages across all streams for better visibility
    total_stream_msgs = sum(s.get("msgs", 0) for s in streams.values())
    total_stream_bytes = sum(s.get("bytes", 0) for s in streams.values())

    nats_issues: list[str] = []
    if dns_failures > 0:
        nats_issues.append(
            f"NATS cluster routing broken: DNS lookup failed for {dns_failures} peer node(s) — inter-node routing unavailable"
        )
    elif route_errors > 0:
        nats_issues.append(f"NATS routing errors (transient): {route_errors}")
    if i_o_timeout_errors > 0:
        nats_issues.append(f"NATS I/O timeout errors: {i_o_timeout_errors} — network partition or slowness detected")

    # Create structured error entries for detailed reporting (use display_errors)
    error_entries = []
    for i, err in enumerate(display_errors[:50], 1):  # limit to 50 most recent
        error_entries.append({
            "line": i,
            "level": "ERROR",
            "timestamp": last_ts or "",
            "msg": err,
            "category": _categorize_nats_error(err),
        })
    
    warning_entries = []
    for i, warn in enumerate([w for w in display_warnings if "Plaintext" not in w][:50], 1):
        warning_entries.append({
            "line": i,
            "level": "WARN",
            "timestamp": last_ts or "",
            "msg": warn,
            "category": _categorize_nats_error(warn),
        })
    
    return {
        "available": True,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "line_counts": line_counts,
        "total_lines": sum(line_counts.values()),
        "errors": display_errors,  # Only errors in window (or recent if window not specified)
        "warnings": [w for w in display_warnings if "Plaintext" not in w],
        "errors_in_window": errors_in_window if run_window else len(errors),
        "warnings_in_window": warnings_in_window if run_window else len([w for w in warnings if "Plaintext" not in w]),
        "total_errors": len(errors),  # All time count
        "total_warnings": len(warnings),  # All time count
        "run_window_start": _win_start or None,
        "run_window_end": _win_end or None,
        "error_entries": error_entries,
        "warning_entries": warning_entries,
        "cluster_started": cluster_started,
        "cluster_leader": cluster_leader,
        "streams": streams,
        "active_streams": active_streams[:15],
        "total_streams": len(streams),
        "total_stream_msgs": total_stream_msgs,
        "total_stream_bytes": total_stream_bytes,
        "ipfix_stream": ipfix_stream,
        "ipfix_stream_msgs": ipfix_stream.get("msgs", 0),
        "ipfix_stream_bytes": ipfix_stream.get("bytes", 0),
        "consumer_leaders": consumer_leaders[:20],
        "route_errors": route_errors,
        "dns_failures": dns_failures,
        "i_o_timeout_errors": i_o_timeout_errors,
        "consumer_offline": consumer_offline,
        "ipfix_consumer_leader_changes": ipfix_consumer_leader_changes,
        "issues": nats_issues,
    }


def _categorize_ndp_error(msg: str) -> str:
    """Categorize NDP errors into groups for better reporting."""
    msg_lower = msg.lower()
    if "message terminated" in msg_lower:
        return "Message Termination"
    if "subscription" in msg_lower and "fail" in msg_lower:
        return "Subscription Failure"
    if "consumer" in msg_lower and "offline" in msg_lower:
        return "Consumer Offline"
    if "no responders" in msg_lower:
        return "No Responders"
    if "enrichment" in msg_lower or "agent" in msg_lower:
        return "Enrichment/Agent"
    if "watch" in msg_lower:
        return "Watch Events"
    if "worker" in msg_lower:
        return "Worker"
    return "Other"


def parse_data_processor_log(
    path: "Path | list[Path]",
    run_window: "tuple[str, str] | None" = None,
) -> dict:
    """
    Parse the ncm-network-log-data-processor structured JSON log.
    Extracts pipeline config, subscription details, worker stats, errors,
    and detailed Message Terminated error breakdown.

    *path* may be a single ``Path`` or a list of ``Path`` objects (one per replica).

    *run_window* is an optional ``(start_ts, end_ts)`` tuple of ISO-8601 UTC strings
    taken from the Network Log Receiver's log window.  When supplied, per-message
    counters (``msg_terminated``, error codes, termination events, subscription
    failure retries) are restricted to log lines whose timestamp falls within that
    window, preventing stale events from earlier runs from polluting the report.
    """
    import re as _re
    K8S = _re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+')
    ANSI = _re.compile(r'\x1b\[[0-9;]*m')

    # ── Run-window helper ────────────────────────────────────────────────────
    _win_start, _win_end = (run_window or ("", ""))

    def _in_window(ts: str) -> bool:
        """Return True if ts falls within [_win_start, _win_end] (inclusive)."""
        if not run_window or not ts:
            return True
        if _win_start and ts < _win_start:
            return False
        if _win_end and ts > _win_end:
            return False
        return True

    paths = path if isinstance(path, list) else [path]
    if not any(p and p.is_file() for p in paths):
        return {"available": False}

    pipeline_name: str | None = None
    stream_name: str | None = None
    consumer_name: str | None = None
    subject: str | None = None
    batch_size: int | None = None
    worker_count: int | None = None
    errors: list[str] = []
    warnings: list[str] = []
    msg_terminated = 0
    watch_events = 0
    pipeline_started = False
    workers_started = False
    subscription_added = False
    first_ts: str | None = None
    last_ts: str | None = None

    # NDP subscription failure tracking
    subscription_failures = 0
    consumer_offline_retries = 0
    no_responders_errors = 0
    nats_pull_errors = 0  # "Unexpected error pulling from NATS"
    nats_dispatch_errors = 0  # "Error getting and dispatching messages"
    
    # Message processing tracking
    watch_callbacks_processed = 0  # Actual processing events
    entity_updates = 0
    
    # Detailed termination error tracking
    error_codes: dict[str, int] = {}
    agent_ids: dict[str, int] = {}
    termination_events: list[dict] = []   # {ts, code, agent_id, error_msg}
    msg_terminated_outside_window = 0     # stale events from earlier runs

    for line in _iter_log_lines(*paths):
            raw = line.strip()
            if not raw:
                continue
            text = ANSI.sub("", raw)
            if not text.startswith("{"):
                text = K8S.sub("", text)
            if not text.startswith("{"):
                continue
            try:
                d = json.loads(text)
            except json.JSONDecodeError:
                continue

            ts = str(d.get("ts") or d.get("timestamp") or "")
            if ts and first_ts is None:
                first_ts = ts
            if ts:
                last_ts = ts

            msg = str(d.get("msg", ""))
            lvl = str(d.get("level", "")).lower()

            if lvl in ("error", "err"):
                errors.append(msg[:100])
            elif lvl in ("warn", "warning"):
                warnings.append(msg[:100])

            # Per-message event counters — only counted within the run window
            # so that stale events from earlier runs don't pollute the report.
            if _in_window(ts):
                # NATS connection/fetch failures
                if "Unexpected error pulling from NATS" in msg:
                    nats_pull_errors += 1
                if "Error getting and dispatching messages" in msg:
                    nats_dispatch_errors += 1
                if "consumer is offline" in str(d.get("error", "")).lower():
                    consumer_offline_retries += 1
                if ("no responders available" in str(d.get("error", "")).lower()
                        or "no responders available" in msg.lower()):
                    no_responders_errors += 1
                if "Failed to create subscription" in msg:
                    subscription_failures += 1
                
                # Successful processing events
                if "Watch callback processed" in msg:
                    watch_callbacks_processed += 1
                if "Entity watch fired kEntityUpdate" in msg:
                    entity_updates += 1

                if "Message Terminated" in msg:
                    msg_terminated += 1
                    err_text = str(d.get("error", ""))
                    # Extract error code
                    mc = _re.search(r'Code:\s*(\w+)\s*\(\d+\)', err_text)
                    code = mc.group(1) if mc else "Unknown"
                    # Extract agent ID
                    ma = _re.search(r'agentID\s+([a-f0-9\-]{36})', err_text)
                    agent_id = ma.group(1) if ma else ""
                    error_codes[code] = error_codes.get(code, 0) + 1
                    if agent_id:
                        agent_ids[agent_id] = agent_ids.get(agent_id, 0) + 1
                    # Store compact event (limit to 200 for JSON size)
                    if len(termination_events) < 200:
                        termination_events.append({
                            "ts": ts,
                            "code": code,
                            "agent_id": agent_id,
                            "error": err_text[:120],
                        })
            elif "Message Terminated" in msg:
                # Count but don't include in error_codes/termination_events
                msg_terminated_outside_window += 1
            elif "watch" in msg.lower():
                watch_events += 1

            if "Registering pipeline with fanout" in msg or "Successfully created pipeline" in msg:
                pipeline_started = True
                if not pipeline_name and ":" in msg:
                    pipeline_name = str(d.get("type") or "")

            if "All workers started successfully" in msg or "Successfully started workers" in msg:
                workers_started = True
                if not worker_count:
                    wc = d.get("worker-count") or d.get("worker_count")
                    if wc:
                        worker_count = int(wc)

            if "Added subscription to the worker" in msg:
                subscription_added = True

            if "Registering data source" in msg:
                rc = d.get("receiver-config") or {}
                if isinstance(rc, dict):
                    for cfg in rc.get("configs", []):
                        for shard in cfg.get("shards", []):
                            stream_name = stream_name or shard.get("stream-name")
                            consumer_name = consumer_name or shard.get("consumer-name")
                            subject = subject or shard.get("subjects")
                            batch_size = batch_size or shard.get("batch-size")

            if "Assigning shard to the worker" in msg:
                si = str(d.get("shard-info", ""))
                m2 = _re.search(r'streamName: (\S+),', si)
                if m2 and not stream_name: stream_name = m2.group(1)
                m3 = _re.search(r'consumerName: (\S+),', si)
                if m3 and not consumer_name: consumer_name = m3.group(1)
                m4 = _re.search(r'subjectName: (\S+),', si)
                if m4 and not subject: subject = m4.group(1)

            if "nx-network-log-pipeline" in msg and not pipeline_name:
                pipeline_name = "nx-network-log-pipeline"

    # Compute termination event rate (events per hour)
    termination_rate_per_hour: float | None = None
    if len(termination_events) >= 2:
        try:
            from datetime import datetime, timezone
            t0 = datetime.fromisoformat(termination_events[0]["ts"].replace("Z","")).replace(tzinfo=timezone.utc)
            t1 = datetime.fromisoformat(termination_events[-1]["ts"].replace("Z","")).replace(tzinfo=timezone.utc)
            span_hours = (t1 - t0).total_seconds() / 3600.0
            if span_hours > 0:
                termination_rate_per_hour = round(len(termination_events) / span_hours, 1)
        except Exception:
            pass

    dominant_error = max(error_codes, key=lambda k: error_codes[k]) if error_codes else None

    # Per-replica subscription tracking
    subscribed_replicas = 0
    total_replicas = 0
    for p in paths:
        if not (p and p.is_file()):
            continue
        total_replicas += 1
        had_subscription = False
        for line in p.open(errors="replace"):
            if "Added subscription to the worker" in line:
                had_subscription = True
                break
        if had_subscription:
            subscribed_replicas += 1

    # Create structured error entries for detailed reporting
    error_entries = []
    for i, err in enumerate(errors[:100], 1):  # limit to 100 most recent
        error_entries.append({
            "line": i,
            "level": "ERROR",
            "timestamp": last_ts or "",
            "msg": err,
            "category": _categorize_ndp_error(err),
        })
    
    warning_entries = []
    for i, warn in enumerate(warnings[:100], 1):
        warning_entries.append({
            "line": i,
            "level": "WARN",
            "timestamp": last_ts or "",
            "msg": warn,
            "category": _categorize_ndp_error(warn),
        })
    
    # Calculate effective consumption
    # If there are NATS pull errors, it means no messages were consumed
    effective_consumed = 0 if nats_pull_errors > 0 else watch_callbacks_processed
    
    issues = []
    if not pipeline_started:
        issues.append("Pipeline did not start successfully")
    if not subscription_added:
        issues.append("No NATS subscription was created")
    if nats_pull_errors > 0:
        issues.append(
            f"NATS connection failure: {nats_pull_errors} pull errors "
            f"({no_responders_errors} 'no responders' errors) — 0 messages consumed"
        )
    if error_codes:
        top_code = dominant_error or "Unknown"
        issues.append(f"ALL {msg_terminated} messages failed: {top_code} — messages are being dropped")
    if subscription_failures > 0:
        issues.append(f"{subscription_failures} subscription attempt(s) failed (consumer offline)")
    if no_responders_errors > 0 and nats_pull_errors == 0:
        issues.append(f"One or more NDP replicas cannot reach NATS: {no_responders_errors} 'no responders' errors")
    if total_replicas > 0 and subscribed_replicas < total_replicas:
        issues.append(f"Only {subscribed_replicas}/{total_replicas} replicas subscribed to NATS")
    if watch_callbacks_processed > 0:
        issues.append(f"Successfully processed {watch_callbacks_processed} watch callbacks")
    if warnings:
        issues.extend([f"Warning: {w}" for w in warnings[:3]])

    return {
        "available": True,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "pipeline_name": pipeline_name or "nx-network-log-pipeline",
        "stream_name": stream_name,
        "consumer_name": consumer_name,
        "subject": subject,
        "batch_size": batch_size,
        "worker_count": worker_count,
        "pipeline_started": pipeline_started,
        "workers_started": workers_started,
        "subscription_added": subscription_added,
        "msg_terminated": msg_terminated,
        "msg_terminated_outside_window": msg_terminated_outside_window,
        "run_window_start": _win_start or None,
        "run_window_end": _win_end or None,
        "watch_events": watch_events,
        # Message processing metrics
        "nats_pull_errors": nats_pull_errors,
        "nats_dispatch_errors": nats_dispatch_errors,
        "watch_callbacks_processed": watch_callbacks_processed,
        "entity_updates": entity_updates,
        "effective_consumed": effective_consumed,
        # Detailed error breakdown
        "error_codes": error_codes,
        "dominant_error_code": dominant_error,
        "unique_failing_agent_ids": len(agent_ids),
        "failing_agent_ids": dict(list(agent_ids.items())[:10]),
        "termination_events": termination_events[:50],  # first 50 for timeline
        "termination_rate_per_hour": termination_rate_per_hour,
        "all_messages_dropped": msg_terminated > 0 and sum(error_codes.values()) == msg_terminated,
        "subscription_failures": subscription_failures,
        "consumer_offline_retries": consumer_offline_retries,
        "no_responders_errors": no_responders_errors,
        "subscribed_replicas": subscribed_replicas,
        "total_replicas": total_replicas,
        "errors": errors,
        "warnings": warnings,
        "error_entries": error_entries,
        "warning_entries": warning_entries,
        "issues": issues,
    }


def _categorize_diis_error(msg: str) -> str:
    """Categorize DIIS errors into groups for better reporting."""
    msg_lower = msg.lower()
    if "clickhouse" in msg_lower or "ch" in msg_lower:
        return "ClickHouse"
    if "insert" in msg_lower:
        return "Insert"
    if "grpc" in msg_lower or "rpc" in msg_lower:
        return "gRPC"
    if "nats" in msg_lower or "consume" in msg_lower:
        return "NATS/Consumer"
    if "batch" in msg_lower:
        return "Batch Processing"
    if "connection" in msg_lower or "alive" in msg_lower:
        return "Connection"
    return "Other"


def parse_diis_log(path: "Path | list[Path]", run_window: "tuple[str, str] | None" = None) -> dict:
    """
    Parse the DIIS log (mixed glog + zap/logrus format).
    Extracts ClickHouse config, insert events, gRPC server info, errors,
    and CH connection gauge time series.

    *path* may be a single ``Path`` or a list of ``Path`` objects (one per replica).
    
    *run_window* is an optional ``(start_ts, end_ts)`` tuple of ISO-8601 UTC strings
    taken from the Network Log Receiver's log window. When supplied, errors and warnings
    are filtered to only include events within this time window.
    """
    import re as _re
    ANSI = _re.compile(r'\x1b\[[0-9;]*m')
    GLOG = _re.compile(r'^([IWEF])(\d{4}) (\d{2}:\d{2}:\d{2}\.\d+)\s+\d+\s+\S+:\d+\]\s*(.*)')
    TAB_ZAP = _re.compile(r'^(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\t(\w+)\t(\S+)\t([^\t]+)(.*)')
    LOGRUS = _re.compile(r'time="([^"]+)" level=(\w+) msg="([^"]+)"')
    
    # ── Run-window helper ────────────────────────────────────────────────────
    _win_start, _win_end = (run_window or ("", ""))
    
    def _normalize_ts(ts: str) -> str:
        """Normalize timestamp for comparison (handle Z vs +00:00 suffix)."""
        if not ts:
            return ts
        return ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
    
    def _in_window(ts: str) -> bool:
        """Return True if ts falls within [_win_start, _win_end] (inclusive)."""
        if not run_window or not ts:
            return True
        ts_norm = _normalize_ts(ts)
        win_start_norm = _normalize_ts(_win_start)
        win_end_norm = _normalize_ts(_win_end)
        if win_start_norm and ts_norm < win_start_norm:
            return False
        if win_end_norm and ts_norm > win_end_norm:
            return False
        return True

    paths = path if isinstance(path, list) else [path]
    if not any(p and p.is_file() for p in paths):
        return {"available": False}

    errors: list[str] = []
    warnings: list[str] = []
    errors_in_window = 0
    warnings_in_window = 0
    ch_table: str | None = None
    ch_inserts = 0
    ch_errors = 0
    grpc_port: str | None = None
    nats_consume_events = 0
    gauge_updates = 0
    service_started = False
    first_ts: str | None = None
    last_ts: str | None = None
    total_lines = 0

    # DIIS-specific ingestion tracking
    batches_created = 0
    batches_prepared = 0
    metrics_completed = 0
    per_flow_byte_missing = 0
    ch_not_alive_count = 0

    # CH connection gauge time series (downsampled to at most 200 points)
    ch_open_series: list[dict] = []     # {ts, value}
    ch_idle_series: list[dict] = []
    _ch_sample_interval = 60           # sample at most once per minute
    _ch_last_ts: str | None = None

    # Rate limiter config
    rate_limits: dict[str, dict] = {}

    for line in _iter_log_lines(*paths):
            raw = ANSI.sub("", line.strip())
            if not raw:
                continue
            total_lines += 1

            ts: str | None = None
            lvl = "info"
            msg = ""

            mg = GLOG.match(raw)
            if mg:
                lvl_char = mg.group(1)
                ts = f"2026-{mg.group(2)[:2]}-{mg.group(2)[2:]}T{mg.group(3)}"
                msg = mg.group(4)
                lvl = {"I": "info", "W": "warn", "E": "error", "F": "fatal"}.get(lvl_char, "info")
            else:
                mz = TAB_ZAP.match(raw)
                if mz:
                    ts = mz.group(1)
                    lvl = mz.group(2).lower()
                    msg = mz.group(4).strip()
                    tail = mz.group(5).strip()

                    # Gauge metric parsing
                    if msg == "Updating gauge for metric" and tail.startswith("{"):
                        try:
                            gd = json.loads(tail)
                            metric_name = gd.get("metric", "")
                            metric_val = int(gd.get("value", 0))
                            gauge_updates += 1
                            # Downsample: keep one point per interval per metric
                            if ts != _ch_last_ts:
                                _ch_last_ts = ts
                                if "open" in metric_name:
                                    if len(ch_open_series) < 200:
                                        ch_open_series.append({"ts": ts, "v": metric_val})
                                elif "idle" in metric_name:
                                    if len(ch_idle_series) < 200:
                                        ch_idle_series.append({"ts": ts, "v": metric_val})
                        except Exception:
                            pass
                        continue  # don't process further

                    # Rate limiter registration
                    if "Registering rate limiter" in msg and tail.startswith("{"):
                        try:
                            rl = json.loads(tail)
                            method = rl.get("method", "")
                            cfg = rl.get("config", {})
                            if method and isinstance(cfg, dict):
                                # get the "Medium" tier count as representative
                                med = cfg.get("Medium", cfg.get("Small", {}))
                                rate_limits[method] = {
                                    "count": med.get("Count", 0) if isinstance(med, dict) else 0,
                                    "unit": med.get("TimeUnit", "") if isinstance(med, dict) else "",
                                }
                        except Exception:
                            pass

                    # Extract CH table from 'Created new ingestion batch' tail JSON
                    if msg == "Created new ingestion batch" and tail.startswith("{"):
                        try:
                            tbl_d = json.loads(tail)
                            tbl = tbl_d.get("table", "")
                            if "ncm.nx_ipfix" in tbl and not ch_table:
                                ch_table = tbl
                        except Exception:
                            pass
                else:
                    ml = LOGRUS.match(raw)
                    if ml:
                        ts = ml.group(1)
                        lvl = ml.group(2).lower()
                        msg = ml.group(3)
                    else:
                        continue

            if ts and first_ts is None:
                first_ts = ts
            if ts:
                last_ts = ts

            if lvl in ("error", "err"):
                errors.append(msg[:120])
                if _in_window(ts):
                    errors_in_window += 1
                if "clickhouse" in msg.lower() or "insert" in msg.lower():
                    ch_errors += 1
            elif lvl in ("warn", "warning"):
                warnings.append(msg[:120])
                if _in_window(ts):
                    warnings_in_window += 1

            lmsg = msg.lower()
            if "gauge" in lmsg:
                gauge_updates += 1
            if "clickhouse table config" in lmsg:
                m2 = _re.search(r'"(ncm\.\w+)"', raw)
                if m2 and not ch_table:
                    ch_table = m2.group(1)
            if "Created new ingestion batch" in msg:
                batches_created += 1
            if "Prepared clickhouse batches" in msg:
                batches_prepared += 1
            if "Metrics ingestion completed" in msg:
                metrics_completed += 1
            if "PerFlowByte datapoints not found" in msg:
                per_flow_byte_missing += 1
            if "Clickhouse server is not alive" in msg:
                ch_not_alive_count += 1
            if "nats" in lmsg or ("consume" in lmsg and "jetstream" in lmsg):
                nats_consume_events += 1
            if "starting" in lmsg and ("server" in lmsg or "http" in lmsg or "grpc" in lmsg):
                service_started = True
                if not grpc_port:
                    m3 = _re.search(r':(\d+)', msg)
                    if m3:
                        grpc_port = m3.group(1)

    # Compute max CH connections from series
    ch_max_open = max((p["v"] for p in ch_open_series), default=0)
    ch_max_idle = max((p["v"] for p in ch_idle_series), default=0)
    
    # Filter errors/warnings for display (only show those in window if window is specified)
    if run_window:
        display_errors = errors[-50:] if errors_in_window == 0 else errors[-min(errors_in_window, 50):]
        display_warnings = warnings[-50:] if warnings_in_window == 0 else warnings[-min(warnings_in_window, 50):]
    else:
        display_errors = errors
        display_warnings = warnings

    # Create structured error entries for detailed reporting (use display_errors)
    error_entries = []
    for i, err in enumerate(display_errors[:50], 1):  # limit to 50 most recent
        error_entries.append({
            "line": i,
            "level": "ERROR",
            "timestamp": last_ts or "",
            "msg": err,
            "category": _categorize_diis_error(err),
        })
    
    warning_entries = []
    for i, warn in enumerate(display_warnings[:50], 1):
        warning_entries.append({
            "line": i,
            "level": "WARN",
            "timestamp": last_ts or "",
            "msg": warn,
            "category": _categorize_diis_error(warn),
        })
    
    issues: list[str] = []
    if ch_errors:
        issues.append(f"ClickHouse insert errors: {ch_errors}")
    if not service_started:
        issues.append("DIIS server may not have started")
    if ch_max_open == 0 and len(ch_open_series) > 0:
        issues.append("ClickHouse connection pool has 0 open connections throughout the log")
    if ch_not_alive_count > 0:
        issues.append(f"ClickHouse readiness check failed {ch_not_alive_count} time(s)")

    return {
        "available": True,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "total_lines": total_lines,
        "ch_table": ch_table,
        "ch_inserts": metrics_completed,
        "ch_errors": ch_errors,
        "grpc_port": grpc_port,
        "nats_consume_events": nats_consume_events,
        "gauge_updates": gauge_updates,
        "service_started": service_started,
        "ch_max_open_connections": ch_max_open,
        "ch_max_idle_connections": ch_max_idle,
        "ch_open_series": ch_open_series[::max(1, len(ch_open_series)//60)],  # downsample to ~60 pts
        "ch_idle_series": ch_idle_series[::max(1, len(ch_idle_series)//60)],
        "rate_limits": rate_limits,
        "batches_created": batches_created,
        "batches_prepared": batches_prepared,
        "metrics_completed": metrics_completed,
        "per_flow_byte_missing": per_flow_byte_missing,
        "ch_not_alive_count": ch_not_alive_count,
        "errors": display_errors[:20],  # Only errors in window
        "warnings": display_warnings[:20],
        "errors_in_window": errors_in_window if run_window else len(errors),
        "warnings_in_window": warnings_in_window if run_window else len(warnings),
        "total_errors": len(errors),  # All time count
        "total_warnings": len(warnings),  # All time count
        "run_window_start": _win_start or None,
        "run_window_end": _win_end or None,
        "error_entries": error_entries,
        "warning_entries": warning_entries,
        "issues": issues,
    }


# ─── Pipeline metric helpers ────────────────────────────────────────────────

def _build_e2e_latency(totals: dict, dp_result: dict | None, ch_inserts: int) -> dict:
    """Compute end-to-end latency metrics from available log data."""
    recv_lag = totals.get("publish_lag_ms_summary", {})
    # Compute average interval between NDP termination events (proxy for processing rate)
    term_events = (dp_result or {}).get("termination_events", [])
    ndp_avg_interval_sec: float | None = None
    if len(term_events) >= 2:
        from datetime import datetime, timezone as _tz
        try:
            times = []
            for ev in term_events[:50]:
                ts = ev.get("ts", "")
                if ts:
                    t = datetime.fromisoformat(ts.replace("Z", "")).replace(tzinfo=_tz.utc)
                    times.append(t)
            if len(times) >= 2:
                span = (times[-1] - times[0]).total_seconds()
                ndp_avg_interval_sec = round(span / (len(times) - 1), 1)
        except Exception:
            pass

    # Estimated full E2E: recv_lag + NATS transit (~negligible) + NDP processing (~interval/5 estimate)
    p50_ms = recv_lag.get("median")
    p95_ms = recv_lag.get("p95")
    p99_ms = recv_lag.get("p99")
    return {
        "recv_to_nats_ms": {
            "min": recv_lag.get("min"),
            "p50": p50_ms,
            "p95": p95_ms,
            "p99": p99_ms,
            "max": recv_lag.get("max"),
        },
        "ndp_avg_processing_interval_sec": ndp_avg_interval_sec,
        "full_e2e_available": ch_inserts > 0,
        "note": (
            "Full E2E latency (PE → ClickHouse) requires successful CH inserts. "
            "Showing Receiver → NATS publish lag only."
            if ch_inserts == 0 else None
        ),
    }


def _build_backpressure(
    recv_batches: int, recv_pub: int,
    nats_msgs: int,
    dp_msg_term: int, dp_all_drop: bool, dp_err_codes: dict,
    ch_max_open: int, ch_inserts: int,
    diis_result: dict | None,
) -> dict:
    """Build per-component backpressure / health indicator objects."""
    recv_ratio = recv_pub / recv_batches if recv_batches else 1.0
    ndp_success = 0 if dp_all_drop else (
        (dp_msg_term - sum(dp_err_codes.values())) / dp_msg_term if dp_msg_term else 1.0
    )

    def _status(val: float, ok_thresh: float, warn_thresh: float, higher_is_better: bool = True) -> str:
        if higher_is_better:
            return "ok" if val >= ok_thresh else ("warn" if val >= warn_thresh else "error")
        else:
            return "ok" if val <= ok_thresh else ("warn" if val <= warn_thresh else "error")

    components = [
        {
            "name": "Receiver Ingestion",
            "icon": "📥",
            "status": _status(recv_ratio, 0.99, 0.90),
            "value": f"{recv_pub}/{recv_batches} published",
            "detail": f"Publish ratio: {recv_ratio * 100:.1f}%" if recv_batches else "No batches",
            "pct": round(recv_ratio * 100, 1),
            "pct_label": "publish success",
        },
        {
            "name": "NATS Stream Lag",
            "icon": "📡",
            "status": _status(nats_msgs, 100, 10_000, higher_is_better=False),
            "value": f"{nats_msgs:,} pending",
            "detail": (
                "Stream empty — consumer draining normally" if nats_msgs == 0
                else f"IPFIX-COLLECTOR-1 has {nats_msgs:,} pending messages"
            ),
            "pct": min(100, round(nats_msgs / 10_000 * 100, 1)),
            "pct_label": "stream fill %",
        },
        {
            "name": "NDP Processing",
            "icon": "⚙️",
            "status": "error" if dp_all_drop else _status(ndp_success, 0.99, 0.90),
            "value": "100% drop rate" if dp_all_drop else (
                f"{int(ndp_success * 100)}% success" if dp_msg_term else "No messages"
            ),
            "detail": (
                f"All {dp_msg_term} msgs dropped — {max(dp_err_codes, key=lambda k: dp_err_codes[k], default='?')}"
                if dp_all_drop else (
                    f"{sum(dp_err_codes.values())} errors / {dp_msg_term} msgs" if dp_err_codes
                    else "No errors observed"
                )
            ),
            "pct": 0 if dp_all_drop else round(ndp_success * 100, 1),
            "pct_label": "success rate",
        },
        {
            "name": "DIIS/CH Health",
            "icon": "🗄",
            "status": (
                "error" if ch_max_open == 0 and (diis_result or {}).get("available")
                else ("ok" if ch_max_open > 0 else "idle")
            ),
            "value": f"{ch_max_open} open connections",
            "detail": (
                "Connection pool empty — DIIS not reaching ClickHouse" if ch_max_open == 0
                else f"{ch_max_open} open, {ch_inserts:,} inserts"
            ),
            "pct": min(100, ch_max_open * 20),
            "pct_label": "connection pool",
        },
    ]

    overall = (
        "error" if any(c["status"] == "error" for c in components)
        else ("warn" if any(c["status"] == "warn" for c in components) else "ok")
    )
    return {"components": components, "overall": overall}


def _build_error_drops(
    recv_batches: int, recv_pub: int, recv_errors: list,
    nats_result: dict | None,
    dp_result: dict | None, dp_msg_term: int, dp_all_drop: bool, dp_err_codes: dict,
    ch_inserts: int, ch_errors: int,
) -> dict:
    """Build per-stage error and drop rate summary."""
    recv_drops = recv_batches - recv_pub
    nats_errs = len((nats_result or {}).get("errors", []))
    ndp_drops = sum(dp_err_codes.values()) if dp_err_codes else 0
    ndp_errs = len((dp_result or {}).get("errors", []))

    stages = [
        {
            "stage": "Receiver",
            "icon": "📥",
            "errors": len([e for e in recv_errors if e]),
            "drops": recv_drops,
            "total": recv_batches,
            "drop_rate_pct": round(recv_drops / recv_batches * 100, 2) if recv_batches else 0.0,
            "note": "Batch exceeded NATS max_payload limit" if recv_drops > 0 else None,
            "dlq": 0,
        },
        {
            "stage": "NATS JetStream",
            "icon": "📡",
            "errors": nats_errs,
            "drops": 0,
            "total": (nats_result or {}).get("total_streams", 0),
            "drop_rate_pct": 0.0,
            "note": f"{nats_errs} routing/DNS errors (transient on startup)" if nats_errs > 0 else None,
            "dlq": 0,
        },
        {
            "stage": "Data Processor",
            "icon": "⚙️",
            "errors": ndp_errs,
            "drops": ndp_drops,
            "total": dp_msg_term,
            "drop_rate_pct": round(ndp_drops / dp_msg_term * 100, 2) if dp_msg_term else 0.0,
            "note": (
                f"100% drop: {max(dp_err_codes, key=lambda k: dp_err_codes[k], default='?')}"
                if dp_all_drop else None
            ),
            "dlq": 0,
        },
        {
            "stage": "DIIS / ClickHouse",
            "icon": "🗄",
            "errors": ch_errors,
            "drops": 0,
            "total": ch_inserts,
            "drop_rate_pct": 0.0,
            "note": "0 inserts — upstream drops prevent data reaching DIIS" if ch_inserts == 0 else None,
            "dlq": 0,
        },
    ]

    return {
        "stages": stages,
        "total_drops": recv_drops + ndp_drops,
        "total_errors": len([e for e in recv_errors if e]) + nats_errs + ndp_errs + ch_errors,
        "dlq_events": 0,
        "pipeline_drop_rate_pct": round(
            (recv_drops + ndp_drops) / max(recv_batches + dp_msg_term, 1) * 100, 2
        ),
    }



def build_pipeline_report(
    receiver_result: dict,
    nats_result: dict | None,
    dp_result: dict | None,
    diis_result: dict | None,
) -> dict:
    """
    Combine per-service analysis results into a unified pipeline health report.
    Identifies where data is dropping off and what the likely root cause is.
    """
    totals = receiver_result.get("totals", {})
    recv_flows    = totals.get("flows", 0)
    recv_batches  = totals.get("batches_received", 0)
    recv_pub      = totals.get("batches_published", 0)
    recv_bytes_total = totals.get("bytes", 0)  # Total bytes published
    recv_errors   = receiver_result.get("error_log_entries", [])

    nats_msgs     = (nats_result or {}).get("ipfix_stream_msgs", 0)
    nats_errors   = (nats_result or {}).get("errors", [])
    nats_stream_bytes = (nats_result or {}).get("ipfix_stream_bytes", 0)

    dp_msg_term   = (dp_result or {}).get("msg_terminated", 0)
    dp_msg_outside = (dp_result or {}).get("msg_terminated_outside_window", 0)
    dp_errors     = (dp_result or {}).get("errors", [])
    dp_subs       = (dp_result or {}).get("subscription_added", False)
    dp_all_drop   = (dp_result or {}).get("all_messages_dropped", False)
    dp_err_codes  = (dp_result or {}).get("error_codes", {})
    dp_agents     = (dp_result or {}).get("failing_agent_ids", {})
    # Infer msgs consumed from receiver + NATS stream:
    #   published - still_in_stream = consumed by NDP (across all replicas)
    # Successful = consumed - terminated_with_error (within run window)
    nats_remaining = (nats_result or {}).get("stream_msgs", None)
    dp_msgs_consumed = (recv_pub - nats_remaining) if (nats_remaining is not None and recv_pub > 0) else None
    dp_msgs_ok = (dp_msgs_consumed - dp_msg_term) if dp_msgs_consumed is not None else None
    # Display helpers used in both stages[] and throughput_comparison[]
    dp_effective = (dp_result or {}).get("effective_consumed", 0)
    dp_nats_errors = (dp_result or {}).get("nats_pull_errors", 0)
    dp_watch_callbacks = (dp_result or {}).get("watch_callbacks_processed", 0)

    # Determine what to display for "consumed":
    # Priority: calculated consumption > inferred from NATS > effective_consumed (watch callbacks) > terminations
    if dp_msgs_consumed is not None:
        # Best: calculated from (published - pending)
        _ndp_consumed_display = dp_msgs_consumed
        _ndp_consumed_note = f"{dp_watch_callbacks} entity updates" if dp_watch_callbacks > 0 else None
    elif recv_pub > 0 and nats_remaining is not None:
        # Fallback: inferred from NATS
        _ndp_consumed_display = recv_pub - nats_remaining
        _ndp_consumed_note = f"~{recv_pub - nats_remaining} inferred; {dp_watch_callbacks} entity updates" if dp_watch_callbacks > 0 else None
    elif dp_effective > 0:
        # Last resort: use watch callbacks but mark as such
        _ndp_consumed_display = dp_effective
        _ndp_consumed_note = f"⚠ {dp_effective} entity updates (not throughput)" if dp_watch_callbacks > 0 else None
    else:
        _ndp_consumed_display = dp_msg_term
        _ndp_consumed_note = None
    
    _ndp_dropped_display  = dp_msg_term  # only terminations within run window

    ch_inserts    = (diis_result or {}).get("ch_inserts", 0)
    ch_errors     = (diis_result or {}).get("ch_errors", 0)
    ch_max_open   = (diis_result or {}).get("ch_max_open_connections", 0)

    # ── Threshold-based alerts ─────────────────────────────────────────────
    alerts: list[dict] = []
    THRESHOLDS = {
        "recv_publish_ratio":   0.95,   # warn if < 95% of batches published
        "nats_pending_warn":    10_000, # warn if stream has > 10K pending msgs
        "ch_insert_min":        1,      # error if CH inserts == 0 while receiver active
    }

    if recv_batches > 0 and recv_pub / recv_batches < THRESHOLDS["recv_publish_ratio"]:
        alerts.append({
            "level": "warn",
            "stage": "Receiver",
            "title": "Publish ratio below threshold",
            "detail": f"{recv_pub}/{recv_batches} batches published ({recv_pub/recv_batches*100:.1f}% < {THRESHOLDS['recv_publish_ratio']*100}%)",
        })

    if any("payload" in str(e).lower() for e in recv_errors):
        alerts.append({
            "level": "error",
            "stage": "Receiver → NATS",
            "title": "NATS max_payload exceeded",
            "detail": "One or more batches exceeded the NATS message size limit. Reduce batchSize in receiver config.",
        })

    if dp_all_drop and dp_msg_term > 0:
        top_code = max(dp_err_codes, key=lambda k: dp_err_codes[k], default="Unknown")
        top_agents = list(dp_agents.keys())[:3]
        alerts.append({
            "level": "error",
            "stage": "Data Processor",
            "title": f"100% message drop — {top_code}",
            "detail": (
                f"All {dp_msg_term} messages terminated with '{top_code}'. "
                + (f"Failing agent ID(s): {', '.join(top_agents)}. " if top_agents else "")
                + "The cluster is not registered with the enrichment service."
            ),
        })
    elif dp_msg_outside > 0 and dp_msg_term == 0:
        alerts.append({
            "level": "info",
            "stage": "Data Processor",
            "title": f"{dp_msg_outside} historical termination(s) from earlier runs",
            "detail": (
                f"{dp_msg_outside} 'Message Terminated' event(s) found outside this run's time window "
                f"({(dp_result or {}).get('run_window_start', '?')[:19]} → "
                f"{(dp_result or {}).get('run_window_end', '?')[:19]}). "
                "They belong to a previous run and do not affect this run's status."
            ),
        })
    elif dp_msg_term > 0 and dp_err_codes:
        alerts.append({
            "level": "warn",
            "stage": "Data Processor",
            "title": f"{dp_msg_term} messages dropped",
            "detail": f"Error breakdown: {dict(list(dp_err_codes.items())[:3])}",
        })

    if nats_msgs > THRESHOLDS["nats_pending_warn"]:
        alerts.append({
            "level": "warn",
            "stage": "NATS",
            "title": "High pending message count",
            "detail": f"IPFIX-COLLECTOR-1 has {nats_msgs:,} pending messages — consumer may be lagging.",
        })

    if recv_pub > 0 and ch_inserts == 0 and not dp_all_drop:
        alerts.append({
            "level": "error",
            "stage": "DIIS → ClickHouse",
            "title": "No ClickHouse inserts",
            "detail": f"0 rows inserted despite {recv_pub} published batches. Check DIIS gRPC connectivity to ClickHouse.",
        })

    if ch_max_open == 0 and (diis_result or {}).get("available"):
        alerts.append({
            "level": "warn",
            "stage": "DIIS",
            "title": "ClickHouse connection pool is empty",
            "detail": "dbal_ch_open_connections_count stayed at 0 throughout the log period. DIIS may not be connecting to ClickHouse.",
        })

    # ── Per-stage status ───────────────────────────────────────────────────
    stages = [
        {
            "name": "PE → Receiver",
            "label": "PE / AHV Host",
            "icon": "🖥",
            "status": "ok" if recv_flows > 0 else "error",
            "metric": f"{recv_flows:,} flows received",
            "note": "No IPFIX data sent from PE" if recv_flows == 0 else None,
        },
        {
            "name": "Receiver",
            "label": "Network Log Receiver",
            "icon": "📥",
            "status": "ok" if recv_pub > 0 and not any("payload" in str(e) for e in recv_errors) else (
                "warn" if recv_pub > 0 else "error"
            ),
            "metric": f"{recv_batches} rx / {recv_pub} pub",
            "note": (
                "Payload exceeded NATS limit on 1+ batches"
                if any("payload" in str(e).lower() for e in recv_errors)
                else ("0 flows received from PE" if recv_flows == 0 else None)
            ),
        },
        {
            "name": "NATS",
            "label": "NATS JetStream (IPFIX-COLLECTOR-1)",
            "icon": "📡",
            "status": "ok" if nats_msgs >= 0 and recv_pub > 0 else "idle",
            "metric": (
                f"{recv_pub:,} batches published ({recv_bytes_total / 1048576:.1f} MB)"
                if recv_pub > 0
                else f"{nats_msgs:,} msgs pending"
            ),
            "note": (
                f"Current stream state: {nats_msgs} msgs pending ({nats_stream_bytes / 1048576:.1f} MB)"
                if recv_pub > 0 and nats_msgs > 0
                else ("Stream empty — consumer acks quickly" if nats_msgs == 0 and recv_pub > 0 else None)
            ),
        },
        {
            "name": "Data Processor",
            "label": "NDP (nx-network-log-pipeline)",
            "icon": "⚙️",
            "status": "error" if (dp_all_drop or dp_nats_errors > 0) else ("ok" if dp_subs and not dp_errors else ("warn" if dp_subs else "error")),
            "metric": (
                f"Consumed: {_ndp_consumed_display:,}, Dropped: {_ndp_dropped_display}"
                + (f" ({dp_msgs_ok} ok)" if dp_msgs_ok is not None and dp_msgs_ok > 0 else "")
                + (f" ({dp_nats_errors} NATS errors)" if dp_nats_errors > 0 else "")
            ),
            "note": (
                f"NATS connection failure: {dp_nats_errors} pull errors (no responders) — 0 consumed"
                if dp_nats_errors > 0
                else (
                    _ndp_consumed_note if _ndp_consumed_note
                    else (
                        f"100% drop — {max(dp_err_codes, key=lambda k: dp_err_codes[k], default='?')}"
                        if dp_all_drop
                        else (
                            f"+{dp_msg_outside} terminated in earlier runs (excluded)"
                            if dp_msg_outside > 0 and not dp_all_drop
                            else ("No NATS subscription" if not dp_subs else None)
                        )
                    )
                )
            ),
        },
        {
            "name": "DIIS → ClickHouse",
            "label": "DIIS (ncm.nx_ipfix_network_logs_3d_raw)",
            "icon": "🗄",
            "status": "ok" if ch_inserts > 0 else ("warn" if ch_errors == 0 else "error"),
            "metric": f"{ch_inserts:,} CH inserts",
            "note": "No data inserted (NDP drops all messages upstream)" if ch_inserts == 0 else None,
        },
    ]

    # ── Root cause heuristic ─────────────────────────────────────────────────
    root_cause: str
    if recv_flows == 0:
        root_cause = (
            "PE is not sending IPFIX data to the receiver's gRPC port. "
            "Check that the Network Log Exporter on AHV hosts is running and pointing at the correct receiver IP:8081."
        )
    elif recv_pub == 0:
        root_cause = "Receiver received flows but failed to publish any batches to NATS."
    elif any("payload" in str(e).lower() for e in recv_errors):
        root_cause = (
            "Receiver attempted to publish an oversized batch. "
            "NATS max_payload limit exceeded — reduce batch_size in the receiver config."
        )
    elif dp_all_drop and dp_msg_term > 0:
        top_code = max(dp_err_codes, key=lambda k: dp_err_codes[k], default="Unknown")
        agents = list(dp_agents.keys())[:2]
        root_cause = (
            f"NDP enrichment failure: all {dp_msg_term} consumed messages raised '{top_code}'. "
            + (f"Agent ID(s) not registered with enrichment service: {', '.join(agents)}. " if agents else "")
            + "Register the cluster or check the enrichment service configuration."
        )
    elif dp_msg_outside > 0 and dp_msg_term == 0 and dp_msgs_ok and dp_msgs_ok > 0:
        _win_note = ""
        _dp_win_start = (dp_result or {}).get("run_window_start") or ""
        if _dp_win_start:
            _win_note = (
                f" Note: {dp_msg_outside} 'Message Terminated' event(s) in the NDP log "
                f"pre-date this run ({_dp_win_start[:19]}) and have been excluded."
            )
        root_cause = f"Pipeline healthy: {dp_msgs_ok} message(s) processed successfully.{_win_note}"
    elif nats_msgs == 0 and recv_pub > 0 and dp_msg_term == 0:
        root_cause = (
            "Batches were published but the IPFIX-COLLECTOR-1 stream shows 0 messages and NDP consumed 0. "
            "Verify the stream's subject filter matches 'ipfix.collector.nx'."
        )
    elif not dp_subs:
        root_cause = "Data Processor has no NATS subscription — pipeline worker did not start."
    elif ch_inserts == 0 and nats_msgs > 0 and not dp_all_drop:
        root_cause = "NATS has messages but DIIS is not inserting into ClickHouse — check DIIS gRPC connectivity."
    else:
        root_cause = "Pipeline appears healthy."

    overall = "healthy" if all(s["status"] == "ok" for s in stages) else (
        "degraded" if any(s["status"] == "error" for s in stages) else "warn"
    )

    # ── Cross-pipeline throughput comparison ──────────────────────────────
    throughput_comparison = [
        {"stage": "Received from PE",   "value": recv_flows,            "unit": "flows", "bytes": 0},
        {"stage": "Published to NATS",  "value": recv_pub,              "unit": "batches", 
         "bytes": recv_bytes_total,
         "note": f"{recv_bytes_total / 1048576:.1f} MB published"},
        {"stage": "NATS Stream Peak",   "value": nats_msgs,             "unit": "msgs",
         "bytes": nats_stream_bytes,
         "note": f"{nats_stream_bytes / 1048576:.1f} MB in stream" if nats_stream_bytes > 0 else None},
        {"stage": "NDP Consumed",       "value": _ndp_consumed_display, "unit": "msgs", "bytes": 0},
        {"stage": "NDP Dropped",        "value": _ndp_dropped_display,  "unit": "msgs", "bytes": 0},
        {"stage": "CH Inserts",         "value": ch_inserts,            "unit": "rows", "bytes": 0},
    ]

    return {
        "overall_status": overall,
        "root_cause": root_cause,
        "stages": stages,
        "alerts": alerts,
        "thresholds": THRESHOLDS,
        "throughput_comparison": throughput_comparison,
        "receiver": {
            "flows": recv_flows,
            "batches_received": recv_batches,
            "batches_published": recv_pub,
            "publish_ratio": round(recv_pub / recv_batches, 4) if recv_batches else 0,
            "errors": [str(e) for e in recv_errors][:10],
        },
        "nats": {
            "available": bool(nats_result and nats_result.get("available")),
            "stream_msgs": nats_msgs,
            "errors": nats_errors[:5],
            "warnings": (nats_result or {}).get("warnings", [])[:5],
            "cluster_leader": (nats_result or {}).get("cluster_leader"),
            "active_streams": (nats_result or {}).get("active_streams", []),
            "total_streams": (nats_result or {}).get("total_streams", 0),
            "route_errors": (nats_result or {}).get("route_errors", 0),
            "dns_failures": (nats_result or {}).get("dns_failures", 0),
            "consumer_offline": (nats_result or {}).get("consumer_offline", False),
            "ipfix_consumer_leader_changes": (nats_result or {}).get("ipfix_consumer_leader_changes", 0),
        },
        "data_processor": {
            "available": bool(dp_result and dp_result.get("available")),
            "pipeline": (dp_result or {}).get("pipeline_name"),
            "stream": (dp_result or {}).get("stream_name"),
            "consumer": (dp_result or {}).get("consumer_name"),
            "subject": (dp_result or {}).get("subject"),
            "workers": (dp_result or {}).get("worker_count"),
            "subscription_active": dp_subs,
            "msgs_consumed": dp_msgs_consumed,
            "msgs_successfully_processed": dp_msgs_ok,
            "msg_terminated": dp_msg_term,
            "msg_terminated_outside_window": dp_msg_outside,
            "all_messages_dropped": dp_all_drop,
            "error_codes": dp_err_codes,
            "dominant_error_code": (dp_result or {}).get("dominant_error_code"),
            "unique_failing_agent_ids": (dp_result or {}).get("unique_failing_agent_ids", 0),
            "failing_agent_ids": dict(list(dp_agents.items())[:5]),
            "termination_events": (dp_result or {}).get("termination_events", [])[:20],
            "termination_rate_per_hour": (dp_result or {}).get("termination_rate_per_hour"),
            "subscription_failures": (dp_result or {}).get("subscription_failures", 0),
            "consumer_offline_retries": (dp_result or {}).get("consumer_offline_retries", 0),
            "no_responders_errors": (dp_result or {}).get("no_responders_errors", 0),
            "subscribed_replicas": (dp_result or {}).get("subscribed_replicas", 0),
            "total_replicas": (dp_result or {}).get("total_replicas", 0),
            "errors": dp_errors[:5],
            "warnings": (dp_result or {}).get("warnings", [])[:5],
        },
        "diis": {
            "available": bool(diis_result and diis_result.get("available")),
            "ch_table": (diis_result or {}).get("ch_table"),
            "ch_inserts": ch_inserts,
            "ch_errors": ch_errors,
            "grpc_port": (diis_result or {}).get("grpc_port"),
            "gauge_updates": (diis_result or {}).get("gauge_updates", 0),
            "ch_max_open_connections": ch_max_open,
            "ch_max_idle_connections": (diis_result or {}).get("ch_max_idle_connections", 0),
            "ch_open_series": (diis_result or {}).get("ch_open_series", []),
            "ch_idle_series": (diis_result or {}).get("ch_idle_series", []),
            "rate_limits": (diis_result or {}).get("rate_limits", {}),
            "batches_created": (diis_result or {}).get("batches_created", 0),
            "batches_prepared": (diis_result or {}).get("batches_prepared", 0),
            "metrics_completed": (diis_result or {}).get("metrics_completed", 0),
            "per_flow_byte_missing": (diis_result or {}).get("per_flow_byte_missing", 0),
            "ch_not_alive_count": (diis_result or {}).get("ch_not_alive_count", 0),
            "errors": (diis_result or {}).get("errors", [])[:5],
            "warnings": (diis_result or {}).get("warnings", [])[:5],
        },
        # ── E2E Latency ─────────────────────────────────────────────────────────
        "e2e_latency": _build_e2e_latency(totals, dp_result, ch_inserts),
        # ── Backpressure & Health ────────────────────────────────────────────────
        "backpressure": _build_backpressure(
            recv_batches, recv_pub, nats_msgs,
            dp_msg_term, dp_all_drop, dp_err_codes,
            ch_max_open, ch_inserts, diis_result,
        ),
        # ── Error & Drop Rates ───────────────────────────────────────────────────
        "error_drops": _build_error_drops(
            recv_batches, recv_pub, recv_errors,
            nats_result, dp_result, dp_msg_term, dp_all_drop, dp_err_codes,
            ch_inserts, ch_errors,
        ),
    }
# ─── Build the full report ────────────────────────────────────────────────────

def build(receiver_path: Path, processor_path: Path | None,
          fetch_meta: dict | None = None,
          nats_log: "Path | list[Path] | None" = None,
          data_processor_log: "Path | list[Path] | None" = None,
          diis_log: "Path | list[Path] | None" = None,
          since: "datetime | None" = None,
          until: "datetime | None" = None) -> dict:
    records, plain = load_json_lines(receiver_path)

    # ── Time window filter ──────────────────────────────────────────────────
    if since or until:
        before_count = len(records)

        def _in_window(rec: dict) -> bool:
            ts_str = rec.get("timestamp")
            if not ts_str:
                return True  # keep records with no timestamp (startup events etc.)
            try:
                ts = parse_ts(ts_str)
            except ValueError:
                return True
            if since and ts < since:
                return False
            if until and ts > until:
                return False
            return True

        records = [(ln, rec) for ln, rec in records if _in_window(rec)]
        log.info(
            "Time window [%s → %s]: kept %d / %d records",
            since.isoformat() if since else "start",
            until.isoformat() if until else "end",
            len(records),
            before_count,
        )

    batches, startup = pair_batches(records)
    levels = Counter(record.get("level", "").lower() for _, record in records)
    messages = Counter(record.get("msg", "") for _, record in records)
    published = [batch for batch in batches if batch["publish"]]
    flows = [batch["num_flows"] for batch in batches if batch["num_flows"] is not None]
    sizes = [batch["publish"]["size_bytes"] for batch in published
             if batch["publish"]["size_bytes"] is not None]
    lags  = [batch["publish"]["lag_ms"] for batch in published
             if batch["publish"]["lag_ms"] is not None]
    hourly = aggregate(batches, "hour")
    daily = aggregate(batches, "day")
    regime = detect_regime(hourly)
    jump_hour = regime["to_hour"] if regime else None
    terminations = load_processor_terminations(processor_path)
    uuids = [batch["batch_uuid"] for batch in batches]
    mismatch = [
        batch["batch_uuid"]
        for batch in batches
        if batch["num_flows"] != batch["flow_record_count"]
    ]
    stats = startup.pop("throughput_stats")
    stats_check = None
    if stats and batches:
        last = stats[-1]
        summed = sum(
            batch["num_flows"]
            for batch in batches
            if batch["received_at"] <= last["timestamp"]
        )
        stats_check = {
            "intervals": len(stats),
            "zero_intervals": sum(1 for item in stats if item["flows_in_interval"] == 0),
            "first": stats[0],
            "last": last,
            "sum_num_flows_at_or_before_last_stats": summed,
            "counter_minus_sum_num_flows": last["total_flows_since_start"] - summed,
        }
    first_received = batches[0]["received_at"] if batches else None
    quiet_seconds = None
    if records and first_received:
        start = next(record.get("timestamp") for _, record in records if record.get("timestamp"))
        quiet_seconds = round((parse_ts(first_received) - parse_ts(start)).total_seconds(), 3)

    result = {
        "fetch": fetch_meta or {},
        "source": {
            "receiver_log": str(receiver_path),
            "processor_log": str(processor_path) if processor_path and processor_path.exists() else None,
            "json_lines": len(records),
            "plain_lines": [{"line": line_no, "text": text} for line_no, text in plain],
            "levels": dict(levels),
            "messages": messages.most_common(),
            "log_start": records[0][1].get("timestamp") if records else None,
            "log_end": records[-1][1].get("timestamp") if records else None,
            "analysis_since": since.isoformat() if since else None,
            "analysis_until": until.isoformat() if until else None,
        },
        "startup": startup,
        "identity": {
            "hosts": distribution(batches, "host"),
            "clusters": distribution(batches, "cluster"),
            "subjects": distribution(
                [{"subject": batch["publish"]["subject"] if batch["publish"] else None} for batch in batches],
                "subject",
            ),
            "ack_values": Counter(
                str(batch["publish"]["ack"]) if batch["publish"] else "missing" for batch in batches
            ).most_common(),
            "unique_batch_uuids": len(set(uuids)),
            "duplicate_batch_uuids": len(uuids) - len(set(uuids)),
            "num_flows_mismatch_flow_record_count": len(mismatch),
        },
        "totals": {
            "batches_received": len(batches),
            "batches_published": len(published),
            "flows": int(sum(flows)),
            "bytes": int(sum(sizes)),
            "gibibytes": round(sum(sizes) / (1024**3), 6) if sizes else 0,
            "flows_summary": summary(flows),
            "bytes_summary": summary(sizes),
            "bytes_per_flow_summary": summary(
                [batch["publish"]["bytes_per_flow"] for batch in published
                 if batch["publish"]["bytes_per_flow"] is not None]
            ),
            "publish_lag_ms_summary": summary(lags),
            "quiet_seconds_before_first_batch": quiet_seconds,
        },
        "daily": daily,
        "hourly": hourly,
        "per_cluster": per_group_stats(batches, "cluster"),
        "per_host": per_group_stats(batches, "host"),
        "flows_histogram": flows_histogram(batches),
        "rate_analysis": rate_analysis(batches),
        "error_log_entries": error_log_entries(records),
        "receive_gaps_outside_100_to_130_seconds": gaps(batches, 100, 130),
        "volume_regime": {"jump": regime, "windows": split_regimes(batches, jump_hour)},
        "throughput_stats_check": stats_check,
        "processor_reconciliation": reconcile(batches, terminations),
        "samples": {
            "first": batches[0] if batches else None,
            "largest_by_flows": max(batches, key=lambda batch: batch["num_flows"] or 0) if batches else None,
            "largest_by_bytes": max(
                published, key=lambda batch: batch["publish"]["size_bytes"] or 0
            )
            if published
            else None,
            "last": batches[-1] if batches else None,
        },
    }

    # Build pipeline report post-hoc (needs full result dict for receiver totals)
    def _has_files(arg: "Path | list[Path] | None") -> bool:
        if arg is None:
            return False
        paths_to_check = arg if isinstance(arg, list) else [arg]
        return any(p and p.is_file() for p in paths_to_check)

    has_extra = any(_has_files(x) for x in [nats_log, data_processor_log, diis_log])
    if has_extra:
        # Pass the receiver log's time window to all parsers so stale events
        # from earlier runs are excluded from counters and error lists
        _recv_start = result["source"].get("log_start") or ""
        _recv_end   = result["source"].get("log_end")   or ""
        _run_window = (_recv_start, _recv_end) if _recv_start else None
        
        _nats_r = parse_nats_log(nats_log, run_window=_run_window) if _has_files(nats_log) else None
        _dp_r   = parse_data_processor_log(data_processor_log, run_window=_run_window) \
                  if _has_files(data_processor_log) else None
        _diis_r = parse_diis_log(diis_log, run_window=_run_window) if _has_files(diis_log) else None
        result["pipeline"] = build_pipeline_report(result, _nats_r, _dp_r, _diis_r)
        
        # Store stage-specific parsing results for template error boxes
        result["_nats_parse"] = _nats_r or {}
        result["_dp_parse"] = _dp_r or {}
        result["_diis_parse"] = _diis_r or {}

    return result



# ─────────────────────────────────────────────────────────────────────────────
#  Output writers
# ─────────────────────────────────────────────────────────────────────────────

def write_hourly_csv(path: Path, hourly: list[dict]) -> None:
    fields = [
        "hour", "batches", "flows",
        "flows_min", "flows_mean", "flows_max",
        "bytes", "bytes_per_flow_mean",
        "lag_ms_median", "lag_ms_p95", "lag_ms_max",
        "acks", "empty_cluster",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in hourly:
            writer.writerow(
                {
                    "hour": row["hour"],
                    "batches": row["batches"],
                    "flows": row["flows"],
                    "flows_min": row["flows_summary"].get("min"),
                    "flows_mean": row["flows_summary"].get("mean"),
                    "flows_max": row["flows_summary"].get("max"),
                    "bytes": row["bytes"],
                    "bytes_per_flow_mean": row["bytes_per_flow_summary"].get("mean"),
                    "lag_ms_median": row["lag_ms_summary"].get("median"),
                    "lag_ms_p95": row["lag_ms_summary"].get("p95"),
                    "lag_ms_max": row["lag_ms_summary"].get("max"),
                    "acks": row["acks"],
                    "empty_cluster": row["empty_cluster"],
                }
            )


def write_batches(path: Path, report_batches: list[dict]) -> None:
    fields = [
        "received_at", "published_at", "lag_ms",
        "batch_uuid", "host", "cluster",
        "num_flows", "flow_record_count",
        "size_bytes", "bytes_per_flow", "subject", "ack",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for batch in report_batches:
            publish = batch.get("publish") or {}
            writer.writerow(
                {
                    "received_at": batch["received_at"],
                    "published_at": publish.get("published_at"),
                    "lag_ms": publish.get("lag_ms"),
                    "batch_uuid": batch["batch_uuid"],
                    "host": batch["host"],
                    "cluster": batch["cluster"],
                    "num_flows": batch["num_flows"],
                    "flow_record_count": batch["flow_record_count"],
                    "size_bytes": publish.get("size_bytes"),
                    "bytes_per_flow": publish.get("bytes_per_flow"),
                    "subject": publish.get("subject"),
                    "ack": publish.get("ack"),
                }
            )


def fmt_num(value) -> str:
    if isinstance(value, float):
        return f"{value:,.3f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def bar(value: int, max_value: int, width: int = 20) -> str:
    if not max_value:
        return ""
    filled = round(value / max_value * width)
    return "█" * filled + "░" * (width - filled)


def write_markdown(path: Path, report: dict) -> None:  # noqa: C901
    totals = report["totals"]
    identity = report["identity"]
    src = report["source"]
    fetch = report.get("fetch") or {}
    rate = report.get("rate_analysis") or {}
    errors = report.get("error_log_entries") or []

    lines: list[str] = [
        "# Network Log Receiver — Analysis Report",
        "",
    ]

    # ── Fetch / source info ──────────────────────────────────────────────────
    if fetch:
        lines += [
            "## Fetch Info",
            "",
            f"| Key | Value |",
            f"|---|---|",
        ]
        for k, v in fetch.items():
            lines.append(f"| {k} | `{v}` |")
        lines.append("")

    lines += [
        "## Source",
        "",
        f"- **Log file:** `{src['receiver_log']}`",
        f"- **Window:** {src['log_start']}  →  {src['log_end']}",
        f"- **JSON lines:** {fmt_num(src['json_lines'])}",
    ]
    if src["processor_log"]:
        lines.append(f"- **Processor log:** `{src['processor_log']}`")
    level_str = "  ".join(f"{k}: {v}" for k, v in src["levels"].items())
    lines += [f"- **Log levels:** {level_str}", ""]

    # ── Totals ───────────────────────────────────────────────────────────────
    lines += [
        "## Totals",
        "",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Batches received | {fmt_num(totals['batches_received'])} |",
        f"| Batches published | {fmt_num(totals['batches_published'])} |",
        f"| Unpaired (no publish) | {fmt_num(totals['batches_received'] - totals['batches_published'])} |",
        f"| Flow records | {fmt_num(totals['flows'])} |",
        f"| Bytes published | {fmt_num(totals['bytes'])} ({totals['gibibytes']} GiB) |",
        f"| Quiet seconds before first batch | {fmt_num(totals['quiet_seconds_before_first_batch'])} |",
        "",
        "### Flows per batch",
        "",
        f"| min | mean | median | p90 | p95 | p99 | max |",
        f"|---:|---:|---:|---:|---:|---:|---:|",
        "| {} | {} | {} | {} | {} | {} | {} |".format(
            fmt_num(totals['flows_summary'].get('min')),
            fmt_num(totals['flows_summary'].get('mean')),
            fmt_num(totals['flows_summary'].get('median')),
            fmt_num(totals['flows_summary'].get('p90')),
            fmt_num(totals['flows_summary'].get('p95')),
            fmt_num(totals['flows_summary'].get('p99')),
            fmt_num(totals['flows_summary'].get('max')),
        ),
        "",
        "### Publish lag (ms)",
        "",
        f"| min | mean | median | p90 | p95 | p99 | max |",
        f"|---:|---:|---:|---:|---:|---:|---:|",
        "| {} | {} | {} | {} | {} | {} | {} |".format(
            fmt_num(totals['publish_lag_ms_summary'].get('min')),
            fmt_num(totals['publish_lag_ms_summary'].get('mean')),
            fmt_num(totals['publish_lag_ms_summary'].get('median')),
            fmt_num(totals['publish_lag_ms_summary'].get('p90')),
            fmt_num(totals['publish_lag_ms_summary'].get('p95')),
            fmt_num(totals['publish_lag_ms_summary'].get('p99')),
            fmt_num(totals['publish_lag_ms_summary'].get('max')),
        ),
        "",
        "### Bytes per flow",
        "",
        f"| min | mean | median | p95 | max |",
        f"|---:|---:|---:|---:|---:|",
        "| {} | {} | {} | {} | {} |".format(
            fmt_num(totals['bytes_per_flow_summary'].get('min')),
            fmt_num(totals['bytes_per_flow_summary'].get('mean')),
            fmt_num(totals['bytes_per_flow_summary'].get('median')),
            fmt_num(totals['bytes_per_flow_summary'].get('p95')),
            fmt_num(totals['bytes_per_flow_summary'].get('max')),
        ),
        "",
    ]

    # ── Rate analysis ────────────────────────────────────────────────────────
    if rate:
        peak = rate.get("peak_minute") or {}
        lines += [
            "## Rate Analysis",
            "",
            f"| Metric | Value |",
            f"|---|---|",
            f"| Log span | {fmt_num(rate.get('span_minutes'))} minutes |",
            f"| Avg flows / minute | {fmt_num(rate.get('avg_flows_per_minute'))} |",
            f"| Active minutes | {fmt_num(rate.get('active_minutes'))} |",
            f"| Zero-flow minutes | {fmt_num(rate.get('zero_flow_minutes'))} |",
            f"| Peak minute | {peak.get('minute', '—')} ({fmt_num(peak.get('flows', 0))} flows) |",
            "",
        ]

    # ── Batch size histogram ─────────────────────────────────────────────────
    hist = report.get("flows_histogram") or []
    if hist:
        max_count = max(row["count"] for row in hist) if hist else 1
        lines += ["## Batch-size Distribution (flows per batch)", ""]
        lines.append("| Flow range | Count | Bar |")
        lines.append("|---|---:|---|")
        for row in hist:
            lines.append(f"| {row['range']} | {row['count']} | {bar(row['count'], max_count)} |")
        lines.append("")

    # ── Identity ─────────────────────────────────────────────────────────────
    lines += [
        "## Identity",
        "",
        f"- **Unique batch UUIDs:** {identity['unique_batch_uuids']} "
        f"(duplicates: **{identity['duplicate_batch_uuids']}**)",
        f"- **numFlows ≠ flowRecordCount:** {identity['num_flows_mismatch_flow_record_count']} batches",
        f"- **Ack values:** {dict(identity['ack_values'])}",
        "",
    ]

    # ── Per-cluster breakdown ────────────────────────────────────────────────
    per_cluster = report.get("per_cluster") or []
    if per_cluster:
        lines += ["## Per-Cluster Breakdown", ""]
        lines.append("| Cluster | Batches | Published | Total flows | Mean flows | Max flows | Total bytes | Median lag ms | P95 lag ms | Max lag ms |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for row in per_cluster:
            lines.append(
                f"| {row['cluster']} "
                f"| {fmt_num(row['batches'])} "
                f"| {fmt_num(row['batches_published'])} "
                f"| {fmt_num(row['total_flows'])} "
                f"| {fmt_num(row['flows_mean'])} "
                f"| {fmt_num(row['flows_max'])} "
                f"| {fmt_num(row['total_bytes'])} "
                f"| {fmt_num(row['lag_ms_median'])} "
                f"| {fmt_num(row['lag_ms_p95'])} "
                f"| {fmt_num(row['lag_ms_max'])} |"
            )
        lines.append("")

    # ── Per-host breakdown ───────────────────────────────────────────────────
    per_host = report.get("per_host") or []
    if per_host:
        lines += ["## Per-Host Breakdown", ""]
        lines.append("| Host | Batches | Total flows | Mean flows | Median lag ms | P95 lag ms |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for row in per_host:
            lines.append(
                f"| {row['host']} "
                f"| {fmt_num(row['batches'])} "
                f"| {fmt_num(row['total_flows'])} "
                f"| {fmt_num(row['flows_mean'])} "
                f"| {fmt_num(row['lag_ms_median'])} "
                f"| {fmt_num(row['lag_ms_p95'])} |"
            )
        lines.append("")

    # ── Subjects ─────────────────────────────────────────────────────────────
    lines += ["## JetStream Subjects", ""]
    for entry in identity["subjects"]:
        lines.append(f"- `{entry['value']}`: **{fmt_num(entry['count'])}** batches")
    lines.append("")

    # ── Daily ────────────────────────────────────────────────────────────────
    lines += [
        "## Daily",
        "",
        "| Day | Batches | Flows | Mean flows/batch | Total bytes | Median lag ms | Acks |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["daily"]:
        lines.append(
            f"| {row['day']} | {row['batches']} | {fmt_num(row['flows'])} | "
            f"{row['flows_summary'].get('mean')} | {fmt_num(row['bytes'])} | "
            f"{row['lag_ms_summary'].get('median')} | {row['acks']} |"
        )
    lines.append("")

    # ── Hourly ───────────────────────────────────────────────────────────────
    lines += [
        "## Hourly",
        "",
        "| Hour (UTC) | Batches | Flows | Flows mean | Bytes | Lag median ms | Lag p95 ms | Acks |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["hourly"]:
        lines.append(
            f"| {row['hour']} "
            f"| {row['batches']} "
            f"| {fmt_num(row['flows'])} "
            f"| {row['flows_summary'].get('mean')} "
            f"| {fmt_num(row['bytes'])} "
            f"| {row['lag_ms_summary'].get('median')} "
            f"| {row['lag_ms_summary'].get('p95')} "
            f"| {row['acks']} |"
        )
    lines.append("")

    # ── Volume regime ────────────────────────────────────────────────────────
    lines += ["## Volume Regime", ""]
    jump = report["volume_regime"]["jump"]
    if jump:
        lines.append(
            f"- Mean flows/batch jumped from **{jump['from_mean_flows']}** at {jump['from_hour']} "
            f"to **{jump['to_mean_flows']}** at {jump['to_hour']}."
        )
    else:
        lines.append("- No significant volume regime change detected.")
    for window in report["volume_regime"]["windows"]:
        lines.append(
            f"- {window['window']}: {fmt_num(window['batches'])} batches, "
            f"{fmt_num(window['flows'])} flows, mean {window['flows_summary'].get('mean')}"
        )
    lines.append("")

    # ── Receive gaps ─────────────────────────────────────────────────────────
    gap_rows = report["receive_gaps_outside_100_to_130_seconds"]
    lines += ["## Receive Gaps (outside 100–130 s)", ""]
    if not gap_rows:
        lines.append("- None — all inter-batch intervals were within 100–130 s.")
    else:
        lines += [
            f"**{len(gap_rows)} gap(s) detected.**",
            "",
            "| Gap (s) | From | To | Flows before | Flows after |",
            "|---:|---|---|---:|---:|",
        ]
        for gap in gap_rows[:50]:
            lines.append(
                f"| {gap['gap_seconds']} "
                f"| {gap['from']} "
                f"| {gap['to']} "
                f"| {gap['from_flows']} "
                f"| {gap['to_flows']} |"
            )
        if len(gap_rows) > 50:
            lines.append(f"\n_(showing first 50 of {len(gap_rows)} gaps)_")
    lines.append("")

    # ── Errors ───────────────────────────────────────────────────────────────
    lines += ["## Error / Warning Log Entries", ""]
    if not errors:
        lines.append("- No error or warning lines found. ✓")
    else:
        lines.append(f"**{len(errors)} error/warn line(s):**")
        lines.append("")
        lines.append("| Line | Level | Timestamp | Caller | Message | Fields |")
        lines.append("|---:|---|---|---|---|---|")
        for e in errors[:100]:
            fields_str = ", ".join(f"{k}={v}" for k, v in (e.get("fields") or {}).items())
            lines.append(
                f"| {e['line']} "
                f"| **{e['level']}** "
                f"| {e['timestamp']} "
                f"| `{e['caller']}` "
                f"| {e['msg']} "
                f"| {fields_str} |"
            )
    lines.append("")

    # ── Processor reconciliation ─────────────────────────────────────────────
    recon = report["processor_reconciliation"]
    lines += ["## Processor Reconciliation", ""]
    if not recon:
        lines.append("- No processor log was provided.")
    else:
        lines += [
            f"| Metric | Value |",
            f"|---|---|",
            f"| Terminations | {fmt_num(recon['processor_terminations'])} |",
            f"| First | {recon['processor_first']} |",
            f"| Last | {recon['processor_last']} |",
            f"| Overlap batches | {fmt_num(recon['receiver_batches_in_overlap'])} |",
            f"| Overlap flows | {fmt_num(recon['receiver_flows_in_overlap'])} |",
            f"| After processor log | {fmt_num(recon['receiver_batches_after_processor_log'])} batches, "
            f"{fmt_num(recon['receiver_flows_after_processor_log'])} flows |",
            f"| Published − terminated | {recon['count_delta_published_minus_terminated']} |",
            f"| First publish→first terminate | {recon['first_publish_to_first_terminate_ms']} ms |",
            f"| Error signatures | {recon['error_signatures']} |",
            f"| Pipeline / worker | {recon['processor_pipeline']} / {recon['processor_worker']} |",
        ]
    lines.append("")

    # ── Throughput counter ───────────────────────────────────────────────────
    check = report["throughput_stats_check"]
    if check:
        lines += [
            "## Throughput Counter",
            "",
            f"| Metric | Value |",
            f"|---|---|",
            f"| Intervals | {check['intervals']} ({check['zero_intervals']} at zero flows) |",
            f"| First | {check['first']['timestamp']} — {fmt_num(check['first']['total_flows_since_start'])} flows |",
            f"| Last | {check['last']['timestamp']} — {fmt_num(check['last']['total_flows_since_start'])} flows |",
            f"| Counter − summed numFlows | {check['counter_minus_sum_num_flows']} |",
            "",
        ]

    # ── Startup events ───────────────────────────────────────────────────────
    startup = report.get("startup") or {}
    events = startup.get("events") or []
    if events:
        lines += ["## Startup Events (first 40)", ""]
        lines.append("| Line | Timestamp | Caller | Message | Key fields |")
        lines.append("|---:|---|---|---|---|")
        for ev in events:
            fields_str = ", ".join(f"{k}={v}" for k, v in (ev.get("fields") or {}).items())
            lines.append(
                f"| {ev['line']} | {ev['timestamp']} | `{ev['caller']}` | {ev['msg']} | {fields_str} |"
            )
        lines.append("")
        orphans = startup.get("orphan_publishes", 0)
        unpaired = startup.get("unpaired_receives", 0)
        lines += [
            f"- Orphan publishes (no preceding receive): **{orphans}**",
            f"- Unpaired receives (no following publish): **{unpaired}**",
            "",
        ]

    # ── Unknown / new message patterns ───────────────────────────────────────
    unknown = (report.get("startup") or {}).get("unknown_msgs") or []
    if unknown:
        lines += ["## ⚠ Unrecognised Message Patterns (new in this log)", ""]
        lines.append("These messages were not matched by the parser. "
                     "A schema update may have introduced new log keys.")
        lines.append("")
        lines.append("| Message | Count |")
        lines.append("|---|---:|")
        for entry in unknown:
            lines.append(f"| `{entry['msg']}` | {entry['count']} |")
        lines.append("")

    # ── Samples ──────────────────────────────────────────────────────────────
    lines += ["## Sample Batches", ""]
    for label, sample in report["samples"].items():
        if not sample:
            continue
        publish = sample.get("publish") or {}
        lines.append(
            f"**{label}** — {sample['received_at']}  "
            f"uuid `{sample['batch_uuid']}`  "
            f"host `{sample['host']}`  "
            f"cluster `{sample['cluster']}`  "
            f"flows {sample['num_flows']}  "
            f"bytes {publish.get('size_bytes')}  "
            f"lag {publish.get('lag_ms')} ms  "
            f"subject `{publish.get('subject')}`  "
            f"ack {publish.get('ack')}"
        )
    lines.append("")

    path.write_text("\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pc-ip", default=PC_IP, help="Prism Central IP (default: %(default)s)")
    parser.add_argument("--pc-user", default=PC_SSH_USER, help="PC SSH user (default: %(default)s)")
    parser.add_argument("--pc-pass", default=PC_SSH_PASS, help="PC SSH password")
    parser.add_argument(
        "--fetch-fresh",
        action=argparse.BooleanOptionalAction,
        default=FETCH_FRESH,
        help="Fetch kubeconfig + log from PC before analysing (default: %(default)s)",
    )
    parser.add_argument("--kubeconfig", type=Path, default=KUBECONFIG_FILE,
                        help="Path to save/reuse kubeconfig (default: %(default)s)")
    parser.add_argument("--receiver", type=Path, default=RECEIVER_LOG_FILE,
                        help="Local receiver log path (default: %(default)s)")
    parser.add_argument("--processor", type=Path, default=PROCESSOR_LOG_FILE,
                        help="Optional ncm-data-processor log (default: %(default)s)")
    parser.add_argument("--out-dir", type=Path, default=Path("."),
                        help="Directory for json, csv, and markdown output")
    parser.add_argument(
        "--pod-keywords",
        default=",".join([LOG_RECEIVER_KEYWORD] + EXTRA_POD_KEYWORDS),
        help=(
            "Comma-separated pod-name keywords to fetch when --fetch-fresh is active. "
            "The first keyword is used as the primary receiver log; "
            "the rest are fetched and saved to <out-dir>/<keyword>.log. "
            "(default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--since", default=None,
        help=(
            "Only analyse log records at or after this time. "
            "Accepts an ISO 8601 timestamp (e.g. '2026-10-21T00:00:00Z') "
            "or a relative offset (e.g. '24h', '6h', '1h', '7d')."
        ),
    )
    parser.add_argument(
        "--until", default=None,
        help=(
            "Only analyse log records at or before this time. "
            "Accepts an ISO 8601 timestamp or a relative offset. "
            "Defaults to now (no upper bound)."
        ),
    )
    args = parser.parse_args()

    _since = parse_time_arg(args.since)
    _until = parse_time_arg(args.until)

    # Resolved config (CLI overrides module-level defaults)
    _pc_ip       = args.pc_ip
    _pc_user     = args.pc_user
    _pc_pass     = args.pc_pass
    _fetch_fresh = args.fetch_fresh
    _kubeconfig  = args.kubeconfig
    receiver_path = args.receiver

    fetch_meta: dict = {
        "fetch_fresh": _fetch_fresh,
        "pc_ip": _pc_ip,
        "analysed_at": datetime.now(timezone.utc).isoformat(),
    }

    if _fetch_fresh:
        log.info("FETCH_FRESH=True — fetching log from PC %s", _pc_ip)
        kc = _kubeconfig
        if kc.exists():
            log.info("Re-using existing kubeconfig: %s", kc)
            fetch_meta["kubeconfig_source"] = "reused"
        else:
            log.info("Fetching kubeconfig …")
            fetch_kubeconfig(_pc_ip, _pc_user, _pc_pass, kc)
            fetch_meta["kubeconfig_source"] = "fresh"
        fetch_meta["kubeconfig_file"] = str(kc)

        # Parse pod keywords: first = primary receiver, rest = additional pods
        all_keywords = [k.strip() for k in args.pod_keywords.split(",") if k.strip()]
        primary_keyword = all_keywords[0] if all_keywords else LOG_RECEIVER_KEYWORD
        extra_keywords  = all_keywords[1:]

        log.info("Discovering receiver log path inside pod (keyword: %s) …", primary_keyword)
        remote_paths = discover_all_log_paths(
            kc, FLUENTD_NAMESPACE, FLUENTD_POD,
            FLUENTD_LOG_BASE, primary_keyword, LOG_RECEIVER_FILENAME,
        )
        if not remote_paths:
            log.error(
                "No log files found for primary keyword '%s' under '%s' in pod %s.",
                primary_keyword, FLUENTD_LOG_BASE, FLUENTD_POD,
            )
            sys.exit(1)
        fetch_meta["remote_log_paths"] = remote_paths
        fetch_meta["replica_count"] = len(remote_paths)

        log.info(
            "Fetching primary receiver log from %d replica(s) …",
            len(remote_paths),
        )
        # Save each receiver replica as its own file for inspection
        receiver_replica_paths, _recv_bytes = fetch_replica_logs(
            kc, FLUENTD_NAMESPACE, FLUENTD_POD, remote_paths, args.out_dir,
        )
        if _since or _until:
            receiver_replica_paths, _pruned_recv = _prune_replica_files_outside_window(
                receiver_replica_paths, _since, _until,
            )
            if _pruned_recv:
                log.info(
                    "Pruned %d/%d receiver replica(s) outside time window",
                    len(_pruned_recv), len(receiver_replica_paths) + len(_pruned_recv),
                )
        fetch_meta["log_size_bytes"] = _recv_bytes
        fetch_meta["replica_files"] = [str(p) for p in receiver_replica_paths]
        # Also create the merged file.log that the analyser reads from
        log.info("Merging %d receiver replica(s) → %s …", len(receiver_replica_paths), receiver_path)
        import tempfile as _tmpmod
        receiver_path.parent.mkdir(parents=True, exist_ok=True)
        with receiver_path.open("wb") as _rfh:
            for _rp in receiver_replica_paths:
                _chunk = _rp.read_bytes()
                if _chunk and not _chunk.endswith(b"\n"):
                    _chunk += b"\n"
                _rfh.write(_chunk)

        # Fetch additional pod logs (best-effort; non-fatal if not found)
        args.out_dir.mkdir(parents=True, exist_ok=True)
        fetched_extra: list[dict] = []
        for kw in extra_keywords:
            try:
                log.info("Discovering log path(s) for extra pod keyword '%s' …", kw)
                extra_paths = discover_all_log_paths(
                    kc, FLUENTD_NAMESPACE, FLUENTD_POD,
                    FLUENTD_LOG_BASE, kw, LOG_RECEIVER_FILENAME,
                )
                if not extra_paths:
                    log.warning("No log files found for keyword '%s' — skipping", kw)
                    continue
                log.info(
                    "Fetching extra log '%s' from %d replica(s) as individual files …",
                    kw, len(extra_paths),
                )
                replica_local_paths, total_extra_bytes = fetch_replica_logs(
                    kc, FLUENTD_NAMESPACE, FLUENTD_POD, extra_paths, args.out_dir,
                )
                if _since or _until:
                    replica_local_paths, _pruned_extra = _prune_replica_files_outside_window(
                        replica_local_paths, _since, _until,
                    )
                    if _pruned_extra:
                        log.info(
                            "Pruned %d/%d '%s' replica(s) outside time window",
                            len(_pruned_extra),
                            len(replica_local_paths) + len(_pruned_extra),
                            kw,
                        )
                fetched_extra.append({
                    "keyword": kw,
                    "replica_files": [str(p) for p in replica_local_paths],
                    "size_bytes": total_extra_bytes,
                    "replicas": len(replica_local_paths),
                    "replica_paths": extra_paths,
                })
                log.info(
                    "Extra pod log '%s' saved → %d file(s) in %s",
                    kw, len(replica_local_paths), args.out_dir,
                )
            except Exception as exc:
                log.warning("Could not fetch log for '%s': %s — skipping", kw, exc)
        if fetched_extra:
            fetch_meta["extra_pod_logs"] = fetched_extra
    else:
        log.info("FETCH_FRESH=False — using local files")
        if not receiver_path.exists():
            log.error(
                "FETCH_FRESH=False but '%s' does not exist.  "
                "Set FETCH_FRESH=True or copy the log here manually.",
                receiver_path,
            )
            sys.exit(1)
        log.info("Using existing log: %s (%.1f KB)", receiver_path, receiver_path.stat().st_size / 1024)
        fetch_meta["log_size_bytes"] = receiver_path.stat().st_size

    processor_path = args.processor if args.processor.exists() else None

    # Auto-discover extra service logs from the output directory (if already fetched).
    # New layout: individual per-replica files (no merged file).
    # Legacy layout: single merged <keyword>.log (backward compat for --no-fetch-fresh).
    _out = args.out_dir

    def _find_extra_list(keyword: str) -> "list[Path]":
        """Return list of log Paths for *keyword*, preferring per-replica files."""
        # 1. Per-replica files recorded in fetch_meta (most accurate)
        if "extra_pod_logs" in fetch_meta:
            for entry in fetch_meta["extra_pod_logs"]:
                if entry.get("keyword") == keyword:
                    paths_from_meta = [Path(p) for p in entry.get("replica_files", []) if Path(p).is_file()]
                    if paths_from_meta:
                        return paths_from_meta
        # 2. Fall back to legacy merged file (backward compat)
        safe = keyword.replace("/", "_").replace(" ", "_")
        for candidate in [_out / f"{safe}.log", receiver_path.parent / f"{safe}.log"]:
            if candidate.is_file():
                return [candidate]
        return []

    _nats_paths = _find_extra_list("nats")
    _dp_paths   = _find_extra_list("ncm-network-log-data-processor")
    _diis_paths = _find_extra_list("diis")

    if _since or _until:
        log.info(
            "Time range: %s → %s",
            _since.isoformat() if _since else "start of log",
            _until.isoformat() if _until else "end of log",
        )
    log.info("Analysing …")
    report = build(
        receiver_path, processor_path, fetch_meta,
        nats_log=_nats_paths or None,
        data_processor_log=_dp_paths or None,
        diis_log=_diis_paths or None,
        since=_since,
        until=_until,
    )

    # Pipeline report is now built inside build() — just log the root cause if present
    pipeline_report = report.get("pipeline") or {}
    if pipeline_report.get("root_cause"):
        log.info("Pipeline root cause: %s", pipeline_report["root_cause"])


    # Re-load batches for the CSV (build() doesn't expose the full list)
    records, _plain = load_json_lines(receiver_path)
    if _since or _until:
        def _in_win(rec: dict) -> bool:
            ts_str = rec.get("timestamp")
            if not ts_str:
                return True
            try:
                ts = parse_ts(ts_str)
            except ValueError:
                return True
            if _since and ts < _since:
                return False
            if _until and ts > _until:
                return False
            return True
        records = [(ln, rec) for ln, rec in records if _in_win(rec)]
    batches, _startup = pair_batches(records)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path  = args.out_dir / "network_receiver_analysis.json"
    hourly_path= args.out_dir / "network_receiver_hourly.csv"
    batch_path = args.out_dir / "network_receiver_batches.csv"
    md_path    = args.out_dir / "network_receiver_analysis.md"

    json_path.write_text(json.dumps(report, indent=2) + "\n")
    write_hourly_csv(hourly_path, report["hourly"])
    write_batches(batch_path, batches)
    write_markdown(md_path, report)

    totals = report["totals"]
    print(f"\nbatches received={totals['batches_received']}  published={totals['batches_published']}")
    print(f"flows {fmt_num(totals['flows'])}  bytes {fmt_num(totals['bytes'])}")
    rate = report.get("rate_analysis") or {}
    if rate.get("avg_flows_per_minute"):
        print(f"avg rate {fmt_num(rate['avg_flows_per_minute'])} flows/min  span {rate['span_minutes']} min")
    errs = report.get("error_log_entries") or []
    if errs:
        print(f"⚠  {len(errs)} error/warn line(s) found — see report for details")
    print(f"\nwrote {json_path}")
    print(f"wrote {hourly_path}")
    print(f"wrote {batch_path}")
    print(f"wrote {md_path}")


if __name__ == "__main__":
    main()

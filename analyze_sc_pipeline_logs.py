#!/usr/bin/env python3
"""Analyse ncm-sc namespace pod logs collected by collect_pod_logs.sh."""

import argparse
import datetime as _dt_module
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any

ANSI = re.compile(r'\x1b\[[0-9;]*m')

# ── MSP / Fluentd constants (same cluster as Network Log Analyzer) ────────────
MSP_CLUSTER_NAME  = "nc"              # mspctl cluster name — same as Network Log Analyzer
NC_SC_NAMESPACE   = "ncm-sc"
FLUENTD_NAMESPACE = "ntnx-system"
FLUENTD_POD       = "fluentd-aggregator-0"
FLUENTD_LOG_BASE  = "/fluentd/data/logs"

# ── Error-line detection ──────────────────────────────────────────────────────
# Match ERROR at word boundaries, but not inside URLs (://) or JSON keys
_ERR_RE = re.compile(r'(?<![:/\w])(ERROR|Exception|Traceback)(?!://)')

# ── Log-level pattern for counting WARN lines ────────────────────────────────
_WARN_RE = re.compile(r'\bWARN\b')


def _strip_ansi(line: str) -> str:
    return ANSI.sub('', line)


def _run(cmd: list[str], *, capture: bool = False, timeout: int = 60) -> "subprocess.CompletedProcess[str]":
    """Run a subprocess, raise on non-zero exit."""
    result = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()[:500]
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd[:4])}\n{err}")
    return result


def fetch_kubeconfig(pc_ip: str, ssh_user: str, ssh_pass: str,
                     kubeconfig: "Path", cluster: str = MSP_CLUSTER_NAME) -> None:
    """Fetch kubeconfig from Prism Central via SSH and save locally."""
    if not shutil.which("sshpass"):
        raise RuntimeError(
            "sshpass is not installed — cannot fetch kubeconfig automatically."
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
    lines = [l for l in result.stdout.splitlines() if not l.strip().startswith("Prism Central")]
    kubeconfig.write_text("\n".join(lines) + "\n")
    print(f"[INFO] Kubeconfig saved → {kubeconfig} ({kubeconfig.stat().st_size} bytes)")


def _since_to_find_mmin(since_iso: str) -> str:
    """
    Convert an ISO-8601 UTC timestamp to a 'find -mmin -N' flag string.
    Returns e.g. '-mmin -1440' for a since value 24 h ago, or '' for no filter.
    """
    if not since_iso:
        return ""
    try:
        import datetime as _dt2
        since_dt = _dt2.datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
        now = _dt2.datetime.now(_dt2.timezone.utc)
        minutes_ago = int((now - since_dt).total_seconds() / 60)
        return f"-mmin -{minutes_ago}" if minutes_ago > 0 else ""
    except Exception:
        return ""


def collect_ncm_sc_pod_logs_from_fluentd(
    kubeconfig: "Path",
    out_dir: "Path",
    namespace: str = NC_SC_NAMESPACE,
    since: str = "",
) -> "Path":
    """
    Collect ncm-sc namespace pod logs from the Fluentd aggregator pod.

    Identical approach to the Network Log Analyzer:
      1. kubectl exec fluentd-aggregator-0 -- find /fluentd/data/logs -name 'kube.<ns>.*'
      2. Bulk-stream all log files in one tar via kubectl exec -- sh -c '... | tar -cf -'
      3. Parse the tar locally and write per-pod .log files.

    Fluentd log path format:
      /fluentd/data/logs/kube.<ns>.<pod-name>.<uid>.<container>.<node>/file.log.log
    """
    import datetime as _dt
    import io
    import tarfile

    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir = out_dir / f"pod-logs_{timestamp}"
    logs_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Collecting {namespace} pod logs from Fluentd → {logs_dir}")

    # ── Single kubectl exec: find all ncm-sc log files and tar them in one shot ─
    # No directory listing step — avoids "argument list too long" with 1000+ dirs.
    # Runs entirely inside the fluentd pod: find matching file.log.log files by
    # path pattern (+ optional mtime filter), then pipes to tar and streams back.
    time_filter = _since_to_find_mmin(since)
    if time_filter:
        print(f"[INFO] Filtering Fluentd dirs by modification time: {time_filter}")
    remote_script = (
        f"find '{FLUENTD_LOG_BASE}' -maxdepth 2 -name 'file.log.log' "
        f"-path '*/kube.{namespace}.*/*' {time_filter} 2>/dev/null "
        f"| tar -cf - --files-from=- 2>/dev/null"
    )
    tar_cmd = [
        "kubectl", "--kubeconfig", str(kubeconfig),
        "exec", "-n", FLUENTD_NAMESPACE, FLUENTD_POD,
        "--", "sh", "-c", remote_script,
    ]
    print(f"[INFO] Streaming {namespace} logs from {FLUENTD_POD} via tar "
          f"(single kubectl exec — no arg-list limit)...")
    proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    try:
        stdout_data, stderr_data = proc.communicate(timeout=600)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise RuntimeError("Timed out streaming logs from Fluentd (>10 min)")

    if not stdout_data:
        err = stderr_data.decode("utf-8", errors="replace")[:400]
        raise RuntimeError(f"No data received from Fluentd tar stream.\nstderr: {err}")

    print(f"[INFO] Received {len(stdout_data) / 1024 / 1024:.1f} MB — parsing tar...")

    # ── Step 3: Parse tar → group by pod name → write per-pod .log files ─────
    # Path inside tar: .../kube.<ns>.<pod-name>.<uid>.<container>.<node>/file.log.log
    pod_contents: "dict[str, list[tuple[str, bytes]]]" = defaultdict(list)

    try:
        with tarfile.open(fileobj=io.BytesIO(stdout_data)) as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                path_parts = member.name.replace("\\", "/").rstrip("/").split("/")
                if len(path_parts) < 2:
                    continue
                dir_name = path_parts[-2]           # kube.<ns>.<pod>.<uid>.<ctr>.<node>
                dir_parts = dir_name.split(".")
                if len(dir_parts) < 5 or dir_parts[0] != "kube":
                    continue
                pod_name      = dir_parts[2]        # index 2 = pod name
                container_name = dir_parts[4] if len(dir_parts) > 4 else "default"
                fobj = tf.extractfile(member)
                if fobj:
                    pod_contents[pod_name].append((container_name, fobj.read()))
    except Exception as exc:
        raise RuntimeError(f"Failed to parse tar from Fluentd: {exc}") from exc

    print(f"[INFO] Parsed {len(pod_contents)} unique pod(s)")

    # Write per-pod .log files with the standard header format
    now_str = _dt.datetime.now().strftime("%a %b %d %H:%M:%S UTC %Y")
    collected = 0
    for pod_name in sorted(pod_contents):
        entries = pod_contents[pod_name]  # [(container, bytes), ...]
        containers_str = " ".join(sorted({c for c, _ in entries}))
        log_file = logs_dir / f"{pod_name}.log"
        with open(log_file, "w", encoding="utf-8", errors="replace") as f:
            f.write(
                "========================================\n"
                f"Pod: {pod_name}\n"
                f"Namespace: {namespace}\n"
                f"Status: Unknown\n"
                f"Containers: {containers_str}\n"
                f"Collected at: {now_str}\n"
                "========================================\n\n"
            )
            for container_name, content in sorted(entries):
                if len(entries) > 1:
                    f.write(f"\n--- Container: {container_name} ---\n")
                f.write(content.decode("utf-8", errors="replace"))
        print(f"  ✓ {pod_name}  ({len(entries)} container file(s))")
        collected += 1

    print(f"[INFO] Wrote {collected} pod log file(s) to {logs_dir}")
    return logs_dir


def collect_ncm_sc_pod_logs_via_ssh(
    pc_ip: str,
    ssh_user: str,
    ssh_pass: str,
    out_dir: "Path",
    namespace: str = NC_SC_NAMESPACE,
    since: str = "",
) -> "Path":
    """
    Collect pod logs from ncm-sc namespace by SSHing into PC and running kubectl
    there directly — no local kubeconfig or cluster-name management needed.

    A single SSH session runs a bash script that:
      1. Lists all pods in the namespace
      2. Runs kubectl logs for each pod
      3. Emits ===PODHEADER=== / ===PODEND=== delimiters
    The output is parsed locally and written into per-pod .log files.
    """
    import datetime as _dt
    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir = out_dir / f"pod-logs_{timestamp}"
    logs_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Collecting {namespace} pod logs via SSH into PC {pc_ip} → {logs_dir}")

    if not shutil.which("sshpass"):
        raise RuntimeError("sshpass is not installed — cannot SSH into PC.")

    ssh_base = [
        "sshpass", "-p", ssh_pass,
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-o", "ConnectTimeout=15",
        f"{ssh_user}@{pc_ip}",
    ]

    since_flag = f'--since="{since}"' if since else ""

    # Single bash script — runs entirely on PC, streams back delimited output
    bash_script = f"""#!/bin/bash
NAMESPACE="{namespace}"
SINCE_FLAG="{f"--since={since}" if since else ""}"
SEP_HDR="===PODHEADER==="
SEP_END="===PODEND==="

PODS=$(kubectl get pods -n "$NAMESPACE" \\
  -o jsonpath='{{range .items[*]}}{{.metadata.name}}|{{.status.phase}}|{{range .spec.containers[*]}}{{.name}},{{end}}{{"|\\n"}}{{end}}' 2>/dev/null)

if [ -z "$PODS" ]; then
  echo "[WARN] No pods found in namespace $NAMESPACE" >&2
  exit 0
fi

echo "[INFO] Got pod list from namespace $NAMESPACE"

while IFS= read -r line; do
  [ -z "$line" ] && continue
  POD_NAME=$(echo "$line" | cut -d'|' -f1)
  POD_STATUS=$(echo "$line" | cut -d'|' -f2)
  CONTAINERS=$(echo "$line" | cut -d'|' -f3 | sed 's/,$//')
  [ -z "$POD_NAME" ] && continue

  printf '%s %s %s %s\\n' "$SEP_HDR" "$POD_NAME" "$POD_STATUS" "$CONTAINERS"
  kubectl logs -n "$NAMESPACE" "$POD_NAME" \\
    --all-containers=true --prefix=false $SINCE_FLAG 2>&1 || true
  echo "$SEP_END"
done <<< "$PODS"
"""

    try:
        result = subprocess.run(
            ssh_base + ["--", "bash -s"],
            input=bash_script,
            capture_output=True,
            text=True,
            timeout=600,  # up to 10 min for large namespaces
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("SSH kubectl collection timed out after 10 minutes")

    # Strip "Prism Central VM" banner lines injected by sshd
    output = "\n".join(
        l for l in result.stdout.splitlines()
        if not l.strip().startswith("Prism Central")
    )

    # Log any stderr for diagnostics
    if result.stderr:
        for l in result.stderr.splitlines():
            if l.strip() and not l.strip().startswith("Prism Central"):
                print(f"[SSH] {l}")

    # ── Parse the delimited output into per-pod files ──────────────────────────
    HEADER_MARKER = "===PODHEADER==="
    END_MARKER    = "===PODEND==="

    # Forward any [INFO]/[WARN] lines from the bash script
    collected = 0
    failed    = 0
    all_lines = output.splitlines()
    i = 0
    while i < len(all_lines):
        line = all_lines[i]
        if line.startswith("[INFO]") or line.startswith("[WARN]"):
            print(line)
            i += 1
            continue
        if not line.startswith(HEADER_MARKER):
            i += 1
            continue

        # Parse header: "===PODHEADER=== <pod_name> <status> <containers>"
        parts = line[len(HEADER_MARKER):].strip().split(None, 2)
        if not parts:
            i += 1
            continue
        pod_name      = parts[0]
        pod_status    = parts[1] if len(parts) > 1 else "Unknown"
        containers_raw = parts[2].rstrip(",") if len(parts) > 2 else ""
        containers    = [c for c in containers_raw.split(",") if c]

        # Collect lines until ===PODEND===
        i += 1
        pod_log_lines = []
        while i < len(all_lines) and not all_lines[i].startswith(END_MARKER):
            pod_log_lines.append(all_lines[i])
            i += 1
        i += 1  # skip ===PODEND===

        # Write log file in the same header format as collect_pod_logs.sh
        log_file = logs_dir / f"{pod_name}.log"
        try:
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(
                    "========================================\n"
                    f"Pod: {pod_name}\n"
                    f"Namespace: {namespace}\n"
                    f"Status: {pod_status}\n"
                    f"Containers: {' '.join(containers)}\n"
                    f"Collected at: {_dt.datetime.now().strftime('%a %b %d %H:%M:%S UTC %Y')}\n"
                    "========================================\n\n"
                )
                f.write("\n".join(pod_log_lines))
            print(f"  ✓ {pod_name} ({pod_status})")
            collected += 1
        except Exception as exc:
            print(f"  ✗ {pod_name}: {exc}")
            failed += 1

    if collected == 0 and failed == 0:
        print(f"[WARN] No pod output parsed — raw SSH output ({len(all_lines)} lines):")
        for l in all_lines[:20]:
            print(f"  {l}")

    print(f"[INFO] Collected: {collected}, Failed: {failed}")
    return logs_dir


# Keep old kubeconfig-based function available as fallback
def collect_ncm_sc_pod_logs(
    kubeconfig: "Path",
    out_dir: "Path",
    namespace: str = NC_SC_NAMESPACE,
    since: str = "",
) -> "Path":
    """Legacy: collect via local kubectl + kubeconfig. Use collect_ncm_sc_pod_logs_via_ssh instead."""
    import datetime as _dt
    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir = out_dir / f"pod-logs_{timestamp}"
    logs_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "kubectl", "--kubeconfig", str(kubeconfig),
        "get", "pods", "-n", namespace,
        "-o", "jsonpath={range .items[*]}{.metadata.name}|{.status.phase}|{range .spec.containers[*]}{.name},{end}{\"|\\n\"}{end}",
    ]
    result = _run(cmd, capture=True, timeout=30)
    collected = 0
    for line in result.stdout.splitlines():
        parts = line.split("|", 2)
        if len(parts) < 1 or not parts[0].strip():
            continue
        pod_name = parts[0].strip()
        pod_status = parts[1].strip() if len(parts) > 1 else "Unknown"
        containers_raw = parts[2].rstrip(",") if len(parts) > 2 else ""
        containers = [c for c in containers_raw.split(",") if c]
        log_file = logs_dir / f"{pod_name}.log"
        log_file.write_text(
            "========================================\n"
            f"Pod: {pod_name}\nNamespace: {namespace}\nStatus: {pod_status}\n"
            f"Containers: {' '.join(containers)}\n"
            f"Collected at: {_dt.datetime.now().strftime('%a %b %d %H:%M:%S UTC %Y')}\n"
            "========================================\n\n",
            encoding="utf-8",
        )
        log_cmd = ["kubectl", "--kubeconfig", str(kubeconfig),
                   "logs", "-n", namespace, pod_name, "--all-containers=true"]
        if since:
            log_cmd += ["--since", since]
        r = subprocess.run(log_cmd, capture_output=True, text=True, timeout=60, check=False)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(r.stdout)
        collected += 1
    print(f"[INFO] Collected: {collected}")
    return logs_dir


def parse_header(lines: list[str]) -> dict:
    """Extract Pod/Namespace/Status/Containers/Restarts/Collected-at from first 9 lines."""
    info: dict[str, str] = {}
    for line in lines[:10]:
        line = _strip_ansi(line).strip()
        if line.startswith('Pod:'):
            info['pod_name'] = line[4:].strip()
        elif line.startswith('Namespace:'):
            info['namespace'] = line[10:].strip()
        elif line.startswith('Status:'):
            info['status'] = line[7:].strip()
        elif line.startswith('Containers:'):
            info['containers'] = line[11:].strip()
        elif line.startswith('Restarts:'):
            info['restarts'] = line[9:].strip()
        elif line.startswith('Collected at:'):
            info['collected_at'] = line[13:].strip()
    return info


def family_of(pod_name: str) -> str:
    """Map pod name to its family by stripping the trailing hash/job-id suffixes.

    Examples:
      sc-sp-ml-worker-29877240-2pt5d       -> sc-sp-ml-worker
      common-sc-realtime-service-6949bbbd9f-m6h6v -> common-sc-realtime-service
      ncm-sc-metrics-job-29887200-jmj7b    -> ncm-sc-metrics-job
      sc-copy-graphdb-secret-job-132821-2jjkt -> sc-copy-secret-jobs
    """
    # Normalise all sc-copy-*-secret-job variants to one family
    if re.match(r'^sc-copy-.+-secret-job', pod_name):
        return 'sc-copy-secret-jobs'

    name = pod_name
    # Kubernetes pod names follow: {deploy}-{rs-hash}-{pod-hash}
    # or for jobs: {job}-{job-id}-{pod-hash}
    # We strip up to 2 such trailing segments.
    #
    # A suffix is a Kubernetes hash/id if it:
    #   a) Contains at least one digit (clearly an ID/hash), OR
    #   b) Is exactly 5 chars with NO vowels — Kubernetes pod names avoid vowels in
    #      random suffixes (e.g. "jvnsn", "cbxxc") whereas real words contain them
    #      (e.g. "image", "service", "thanos").
    _VOWELS = frozenset('aeiou')
    _HASH_ANY = re.compile(r'-([a-z0-9]{5,10})$')
    _NUM_SUFFIX = re.compile(r'-\d+$')

    def _is_k8s_suffix(seg: str) -> bool:
        if any(c.isdigit() for c in seg):
            return True   # contains a digit → definitely a hash/ID
        # 5-char pure-consonant string → Kubernetes pod random suffix
        return len(seg) == 5 and not any(c in _VOWELS for c in seg)

    for _ in range(2):
        m = _HASH_ANY.search(name)
        if m and _is_k8s_suffix(m.group(1)):
            name = name[: m.start()]
        else:
            break

    # Phase 2: strip any remaining pure-numeric job-id / replica suffixes (-1-1, -29887200)
    for _ in range(2):
        m = _NUM_SUFFIX.search(name)
        if m:
            name = name[: m.start()]
        else:
            break
    return name


def _is_error_line(line: str) -> bool:
    """Return True if line looks like a genuine error (not a URL mention)."""
    return bool(_ERR_RE.search(line))


# ── Time-series and window filtering helpers ─────────────────────────────────

def parse_time_boundary(value: "str | Path | None") -> "_dt_module.datetime | None":
    """
    Parse a since/until value into a UTC datetime.
    Accepts:
      • Relative durations: "24h", "1h", "6h", "7d", "30m", "90s"
      • ISO-8601 strings:  "2026-10-31T12:22:49.787Z", "2026-10-31 12:22:49", "2026-10-31"
    """
    if not value:
        return None
    val_str = str(value).strip()
    if not val_str:
        return None

    # Relative duration (e.g. 24h, 30m, 7d)
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(h|d|m|s)", val_str, re.IGNORECASE)
    if m:
        amount = float(m.group(1))
        unit = m.group(2).lower()
        seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        return _dt_module.datetime.now(_dt_module.timezone.utc) - _dt_module.timedelta(seconds=amount * seconds)

    # ISO / RFC3339 string
    try:
        clean = val_str.replace("Z", "+00:00")
        if "T" not in clean and " " in clean:
            clean = clean.replace(" ", "T")
        if len(clean) == 10:  # "2026-10-31"
            clean += "T00:00:00+00:00"
        dt = _dt_module.datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt_module.timezone.utc)
        else:
            dt = dt.astimezone(_dt_module.timezone.utc)
        return dt
    except Exception:
        return None


_ISO_DT_RE = re.compile(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)')
_TIME_ONLY_RE = re.compile(r'\b(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?\b')


def _parse_line_dt(line: str, base_dt: "_dt_module.datetime | None" = None) -> "_dt_module.datetime | None":
    """
    Extract UTC datetime from a log line.
    Handles:
      • Full ISO timestamps (Flink, Python, Go logrus, etc.)
      • Time-only HH:MM:SS (Spring Boot logs), using base_dt date if available
    """
    if not line:
        return None

    # 1. Full ISO timestamp
    m = _ISO_DT_RE.search(line)
    if m:
        raw = m.group(1).replace('Z', '+00:00').replace(' ', 'T')
        if '.' in raw:
            base, frac_tz = raw.split('.', 1)
            tz_match = re.search(r'[+-]\d{2}:?\d{2}$', frac_tz)
            if tz_match:
                frac = frac_tz[:tz_match.start()]
                tz = frac_tz[tz_match.start():]
            else:
                frac = frac_tz
                tz = ''
            raw = f'{base}.{frac[:6]}{tz}'
        try:
            dt = _dt_module.datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt_module.timezone.utc)
            return dt.astimezone(_dt_module.timezone.utc)
        except Exception:
            pass

    # 2. Time-only HH:MM:SS with base_dt (collected_at date)
    if base_dt:
        m2 = _TIME_ONLY_RE.search(line)
        if m2:
            try:
                hr, mn, sec = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
                micro = int((m2.group(4) or '0')[:6].ljust(6, '0'))
                return base_dt.replace(hour=hr, minute=mn, second=sec, microsecond=micro)
            except Exception:
                pass
    return None


def _in_time_window(
    line_dt: "_dt_module.datetime | None",
    since_dt: "_dt_module.datetime | None",
    until_dt: "_dt_module.datetime | None",
) -> bool:
    """Return True if line_dt falls within [since_dt, until_dt]."""
    if line_dt is None:
        # If line has no timestamp, keep it so non-timestamped error outputs aren't lost
        return True
    if since_dt and line_dt < since_dt:
        return False
    if until_dt and line_dt > until_dt:
        return False
    return True


# ── Per-family extraction functions ──────────────────────────────────────────

def _extract_realtime_service(body_lines: list[str],
                             since_dt: "_dt_module.datetime | None" = None,
                             until_dt: "_dt_module.datetime | None" = None,
                             base_dt:  "_dt_module.datetime | None" = None) -> dict:
    pg_pools_ready = 0
    bql_queries_ok = 0
    bql_results: list[int] = []
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if 'Postgres Pool - Start completed' in clean:
            pg_pools_ready += 1
        if 'SuccessFully Executed query' in clean:
            bql_queries_ok += 1
        m = re.search(r'with\s+(\d+)\s+results', clean, re.IGNORECASE)
        if m:
            bql_results.append(int(m.group(1)))
        if _is_error_line(clean):
            error_count += 1
    return {
        'pg_pools_ready': pg_pools_ready,
        'bql_queries_ok': bql_queries_ok,
        'bql_total_results': sum(bql_results),
        'error_count': error_count,
    }


def _extract_bql_thanos(body_lines: list[str],
                        since_dt: "_dt_module.datetime | None" = None,
                        until_dt: "_dt_module.datetime | None" = None,
                        base_dt:  "_dt_module.datetime | None" = None) -> dict:
    queries_received  = 0
    queries_responded = 0
    rows_list: list[int] = []      # per-query row counts (for percentiles)
    hourly_q: dict[str, dict] = {} # {hour: {requests, responses, rows}}
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if 'request for execute with query' in clean:
            queries_received += 1
            h = _ts_to_hour(_err_ts(clean))
            if h:
                r = hourly_q.setdefault(h, {'requests': 0, 'responses': 0, 'rows': 0})
                r['requests'] += 1
        if 'returning response for query' in clean:
            queries_responded += 1
        m = re.search(r'with no of rows\s+(\d+)', clean)
        if m:
            n = int(m.group(1))
            if len(rows_list) < 2000:
                rows_list.append(n)
            h = _ts_to_hour(_err_ts(clean))
            if h:
                r = hourly_q.setdefault(h, {'requests': 0, 'responses': 0, 'rows': 0})
                r['responses'] += 1
                r['rows']      += n
        if _is_error_line(clean):
            error_count += 1
    total_rows = sum(rows_list)
    avg_rows   = round(total_rows / len(rows_list), 2) if rows_list else 0
    return {
        'queries_received':  queries_received,
        'queries_responded': queries_responded,
        'total_rows_returned': total_rows,
        'avg_rows':          avg_rows,
        'error_count':       error_count,
        'rows_per_query':    rows_list,       # kept for percentile computation
        'hourly_queries':    hourly_q,
    }


def _extract_flink_taskmanager(body_lines: list[str],
                                since_dt: "_dt_module.datetime | None" = None,
                                until_dt: "_dt_module.datetime | None" = None,
                                base_dt:  "_dt_module.datetime | None" = None) -> dict:
    missing_by_type: Counter = Counter()
    warn_count = 0
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        m = re.search(r'No account id found with entity type:\s*(\w+)', clean)
        if m:
            missing_by_type[m.group(1)] += 1
        if _WARN_RE.search(clean):
            warn_count += 1
        if _is_error_line(clean):
            error_count += 1
    return {
        'missing_account_by_type': dict(missing_by_type),
        'warn_count': warn_count,
        'error_count': error_count,
    }


def _extract_flink_jobmanager(body_lines: list[str],
                               since_dt: "_dt_module.datetime | None" = None,
                               until_dt: "_dt_module.datetime | None" = None,
                               base_dt:  "_dt_module.datetime | None" = None) -> dict:
    plugin_downloads = 0
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if "saved" in clean and ".jar" in clean:
            plugin_downloads += 1
        if _is_error_line(clean):
            error_count += 1
    return {
        'plugin_downloads': plugin_downloads,
        'error_count': error_count,
    }


def _extract_sp_ml_worker(body_lines: list[str],
                          since_dt: "_dt_module.datetime | None" = None,
                          until_dt: "_dt_module.datetime | None" = None,
                          base_dt:  "_dt_module.datetime | None" = None) -> dict:
    ch_connected = False
    worker_started = False
    groupings_started = 0
    errors: list[str] = []
    for line in body_lines:
        clean = _strip_ansi(line).strip()
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if 'connected to Clickhouse DB' in clean:
            ch_connected = True
        if 'Started SC Security Planning worker' in clean:
            worker_started = True
        if 'Starting grouping for tenant' in clean:
            groupings_started += 1
        if _is_error_line(clean):
            errors.append(clean[:200])
    return {
        'ch_connected': ch_connected,
        'worker_started': worker_started,
        'groupings_started': groupings_started,
        'errors': errors[:10],  # cap to keep output sane
    }


def _extract_cron_flow_stats(body_lines: list[str],
                            since_dt: "_dt_module.datetime | None" = None,
                            until_dt: "_dt_module.datetime | None" = None,
                            base_dt:  "_dt_module.datetime | None" = None) -> dict:
    ch_pool_ready = 0
    bql_queries_ok = 0
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if 'ClickHouse-Pool - Start completed' in clean:
            ch_pool_ready += 1
        if 'SuccessFully Executed query' in clean:
            bql_queries_ok += 1
        if _is_error_line(clean):
            error_count += 1
    return {
        'ch_pool_ready': ch_pool_ready,
        'bql_queries_ok': bql_queries_ok,
        'error_count': error_count,
    }


def _extract_cron_spring(body_lines: list[str],
                         since_dt: "_dt_module.datetime | None" = None,
                         until_dt: "_dt_module.datetime | None" = None,
                         base_dt:  "_dt_module.datetime | None" = None) -> dict:
    """Generic Spring Boot cron worker extractor (issue-stats, cleanup, etc.)."""
    started = False
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if 'Started' in clean and 'Application' in clean:
            started = True
        if _is_error_line(clean):
            error_count += 1
    return {
        'spring_started': started,
        'error_count': error_count,
    }


def _extract_metrics_job(body_lines: list[str],
                         since_dt: "_dt_module.datetime | None" = None,
                         until_dt: "_dt_module.datetime | None" = None,
                         base_dt:  "_dt_module.datetime | None" = None) -> dict:
    collectors: dict[str, int] = {}
    metrics_inserted = 0
    total_ingested = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        # Go/logrus format: msg="Collector \"X\" returned N metric(s)" (escaped quotes)
        # or unescaped: Collector "X" returned N metric(s)
        m = re.search(r'Collector\s+\\?"([^"\\]+)\\?"\s+returned\s+(\d+)\s+metric', clean)
        if m:
            collectors[m.group(1)] = int(m.group(2))
        if 'Inserted metric' in clean and 'into IDF' in clean:
            metrics_inserted += 1
        m2 = re.search(r'Done\s*[—\-]+\s*ingested\s+(\d+)\s+metric', clean)
        if m2:
            total_ingested = int(m2.group(1))
    return {
        'collectors': collectors,
        'metrics_inserted': metrics_inserted,
        'total_ingested': total_ingested,
    }


def _extract_flink_operator(body_lines: list[str],
                            since_dt: "_dt_module.datetime | None" = None,
                            until_dt: "_dt_module.datetime | None" = None,
                            base_dt:  "_dt_module.datetime | None" = None) -> dict:
    reconciliations = 0
    fully_reconciled = 0
    error_count = 0
    for line in body_lines:
        clean = _strip_ansi(line)
        if not _in_time_window(_parse_line_dt(clean, base_dt), since_dt, until_dt):
            continue
        if 'Starting reconciliation' in clean:
            reconciliations += 1
        if 'Resource fully reconciled' in clean:
            fully_reconciled += 1
        if _is_error_line(clean):
            error_count += 1
    return {
        'reconciliations': reconciliations,
        'fully_reconciled': fully_reconciled,
        'error_count': error_count,
    }


def _extract_generic(body_lines: list[str],
                     since_dt: "_dt_module.datetime | None" = None,
                     until_dt: "_dt_module.datetime | None" = None,
                     base_dt:  "_dt_module.datetime | None" = None) -> dict:
    error_count = sum(
        1 for l in body_lines
        if _in_time_window(_parse_line_dt(_strip_ansi(l), base_dt), since_dt, until_dt)
        and _is_error_line(_strip_ansi(l))
    )
    return {'error_count': error_count}


# ── Dispatcher ────────────────────────────────────────────────────────────────

_FAMILY_EXTRACTORS: dict[str, Any] = {
    'common-sc-realtime-service': _extract_realtime_service,
    'sc-bql-thanos': _extract_bql_thanos,
    'fsc-flink-cluster-taskmanager': _extract_flink_taskmanager,
    'fsc-flink-cluster': _extract_flink_jobmanager,
    'sc-sp-ml-worker': _extract_sp_ml_worker,
    'sc-cron-flow-stats-worker': _extract_cron_flow_stats,
    'sc-cron-issue-stats-worker': _extract_cron_spring,
    'sc-cron-cleanup-worker': _extract_cron_spring,
    'ncm-sc-metrics-job': _extract_metrics_job,
    'ncm-platform-flink-kubernetes-operator': _extract_flink_operator,
}


# ── Error entry extraction ────────────────────────────────────────────────────

_TS_ISO_RE    = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
_TS_TIME_RE   = re.compile(r'\b(\d{2}:\d{2}:\d{2})\b')
_TS_GOTIME_RE = re.compile(r'time="(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')

# Normalisation: strip UUIDs, long numbers, hex, timestamps → keep core message
_SIG_STRIP_RE = re.compile(
    r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'  # UUID
    r'|0x[0-9a-fA-F]+'                # hex literal
    r'|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?'              # ISO ts
    r'|\b\d{2}:\d{2}:\d{2}\.\d+\b'   # HH:mm:ss.nnn
    r'|\b\d{10,}\b',                  # epoch / long number
    re.IGNORECASE,
)


def _err_ts(line: str) -> str:
    m = _TS_ISO_RE.search(line)
    if m: return m.group(0)
    m = _TS_GOTIME_RE.search(line)
    if m: return m.group(1)
    m = _TS_TIME_RE.search(line)
    if m: return m.group(1)
    return ''


def _err_msg(line: str, level_token: str) -> str:
    """Strip log prefix up to and including the level token."""
    idx = line.upper().find(level_token.upper())
    if idx < 0:
        return line[:300]
    tail = line[idx + len(level_token):].strip()
    # Java: "com.example.Foo -- actual message" or "ClassName : msg"
    m = re.match(r'^[\w.$<>@/]+(?:\s*--\s*|\s*:\s*|\s+)(.*)', tail)
    if m and len(m.group(1)) > 8:
        return m.group(1).strip()[:300]
    return tail[:300]


def _normalize_sig(message: str) -> str:
    """Canonical signature for grouping similar errors."""
    sig = _SIG_STRIP_RE.sub('…', message)
    sig = re.sub(r'\b\d+\b', 'N', sig)           # replace remaining numbers
    sig = re.sub(r'(…\s*)+', '…', sig)           # collapse repeated …
    return sig.strip()[:150]


# ── Time-series helpers ───────────────────────────────────────────────────────

def _parse_ca_hour(collected_at: str) -> str:
    """
    Parse 'Sat Oct 22 13:34:00 UTC 2026' or 'Sun Nov  1 17:32:00 UTC 2026'
    → '2026-10-22 13:00'.  Returns '' on failure.
    """
    if not collected_at:
        return ''
    try:
        # Normalise whitespace (double space for single-digit days) + strip UTC
        s = re.sub(r'\s+', ' ', collected_at.replace(' UTC ', ' ')).strip()
        parts = s.split()
        if len(parts) == 5:
            parts[2] = parts[2].zfill(2)   # ensure zero-padded day
            s = ' '.join(parts)
        dt = _dt_module.datetime.strptime(s, '%a %b %d %H:%M:%S %Y')
        return dt.strftime('%Y-%m-%d %H:00')
    except Exception:
        return ''


def _pct(sorted_vals: list, p: float) -> float:
    """p-th percentile (0‒100) of a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = max(0, int(len(sorted_vals) * p / 100) - 1)
    return float(sorted_vals[idx])


def _ts_to_hour(ts: str) -> str:
    """Convert ISO timestamp '2026-10-31T13:34:...' or '13:34:...' → '2026-10-31 13:00'."""
    if not ts:
        return ''
    ts = ts.strip()
    # ISO full: 2026-10-31T13:34:00
    if len(ts) >= 13 and 'T' in ts:
        return ts[:10] + ' ' + ts[11:13] + ':00'
    # Date-only prefix: 2026-10-31 13:34:00
    if len(ts) >= 13 and ' ' in ts:
        return ts[:10] + ' ' + ts[11:13] + ':00'
    return ''


def build_time_series(
    pods: list[dict],
    error_entries: list[dict],
    family_summaries: dict,
) -> dict:
    """
    Build hourly time-series data for charts:
      hourly_activity  – pods started per hour per family + error counts
      bql_percentiles  – P50/P95/P99 of rows-per-query
      bql_hourly       – BQL query counts per hour
      worker_gaps      – detected gaps in sc-sp-ml-worker activity
    """
    # ── 1. Hourly pod activity (from collected_at header) ─────────────────
    hourly_pods: dict[str, dict[str, int]] = {}
    for pod in pods:
        hour = _parse_ca_hour(pod.get('collected_at', ''))
        if not hour:
            continue
        if hour not in hourly_pods:
            hourly_pods[hour] = {}
        fam = pod['family']
        hourly_pods[hour][fam] = hourly_pods[hour].get(fam, 0) + 1

    # ── 2. Hourly error counts (from error_entries timestamps) ────────────
    hourly_errors: dict[str, int] = {}
    for e in error_entries:
        h = _ts_to_hour(e.get('ts', ''))
        if h:
            hourly_errors[h] = hourly_errors.get(h, 0) + 1

    # ── 3. Merge into sorted hourly rows ──────────────────────────────────
    bql_fam     = family_summaries.get('sc-bql-thanos', {})
    bql_hourly  = bql_fam.get('hourly_queries', {})
    all_hours   = sorted(
        set(list(hourly_pods.keys()))
        | set(hourly_errors.keys())
        | set(bql_hourly.keys())
    )

    hourly_rows: list[dict] = []
    for h in all_hours:
        pc = hourly_pods.get(h, {})
        bql = bql_hourly.get(h, {})
        hourly_rows.append({
            'hour':           h,
            'pod_counts':     pc,
            'total_pods':     sum(pc.values()),
            'errors':         hourly_errors.get(h, 0),
            'bql_requests':   bql.get('requests', 0),
            'bql_responses':  bql.get('responses', 0),
        })

    # ── 4. BQL rows-per-query percentiles ─────────────────────────────────
    bql_pcts = bql_fam.get('latency_pcts', {})

    # ── 5. ML worker activity gap detection ───────────────────────────────
    ml_hours = sorted(
        h for h, c in hourly_pods.items()
        if c.get('sc-sp-ml-worker', 0) > 0
    )
    worker_gaps: list[dict] = []
    for i in range(1, len(ml_hours)):
        prev_h, curr_h = ml_hours[i - 1], ml_hours[i]
        try:
            prev_dt = _dt_module.datetime.strptime(prev_h, '%Y-%m-%d %H:00')
            curr_dt = _dt_module.datetime.strptime(curr_h, '%Y-%m-%d %H:00')
            gap_h   = (curr_dt - prev_dt).total_seconds() / 3600
            if gap_h > 1.5:
                worker_gaps.append({
                    'from_hour': prev_h,
                    'to_hour':   curr_h,
                    'gap_hours': round(gap_h, 1),
                    'severity':  'critical' if gap_h > 6 else 'warn' if gap_h > 3 else 'ok',
                })
        except Exception:
            pass

    # ── 6. Hourly BQL rows ────────────────────────────────────────────────
    bql_hourly_rows = [
        {'hour': h, **v}
        for h, v in sorted(bql_hourly.items())
    ]

    return {
        'hourly_activity': hourly_rows[-96:],   # last 4 days max
        'bql_percentiles': bql_pcts,
        'bql_hourly':      bql_hourly_rows[-96:],
        'worker_gaps':     worker_gaps,
        'total_hours':     len(all_hours),
    }


def extract_error_entries(
    lines: list[str],
    max_err:  int = 50,
    max_warn: int = 20,
    max_info: int = 10,
    since_dt: "_dt_module.datetime | None" = None,
    until_dt: "_dt_module.datetime | None" = None,
    base_dt:  "_dt_module.datetime | None" = None,
) -> list[dict]:
    """
    Scan body lines and return structured error/warn/info entries.

    Handles:
      • Spring Boot: HH:mm:ss.SSS [thread] ERROR class -- msg
      • Flink:       [ISO_TS] ERROR thread class:line msg
      • Python:      [ISO_TS] ERROR N [thread] module:line msg
      • Go logrus:   time="ISO" level=error msg="msg"
      • Tracebacks:  Traceback … / ExceptionType: message
    """
    entries: list[dict] = []
    err_c = warn_c = info_c = 0
    n = len(lines)
    i = 0

    while i < n:
        raw  = lines[i]
        line = _strip_ansi(raw).strip()
        i += 1
        if not line:
            continue

        # Check line timestamp against [since_dt, until_dt] window
        line_dt = _parse_line_dt(line, base_dt)
        if not _in_time_window(line_dt, since_dt, until_dt):
            continue

        # ── Python Traceback ────────────────────────────────────────────
        if 'Traceback (most recent call last)' in line and err_c < max_err:
            ts = _err_ts(line)
            j  = i
            while j < n and j < i + 40:
                exc_line = _strip_ansi(lines[j]).rstrip()
                j += 1
                # Exception line: non-indented, contains ": ", not "File "
                if (exc_line
                        and not exc_line.startswith((' ', '\t', 'File '))
                        and ': ' in exc_line):
                    entries.append({'ts': ts, 'level': 'ERROR',
                                    'message': exc_line.strip()[:300],
                                    'type': 'exception'})
                    err_c += 1
                    i = j
                    break
            continue

        lup = line.upper()
        ll  = line.lower()

        # ── ERROR / FATAL ───────────────────────────────────────────────
        if err_c < max_err and (
            ' ERROR ' in lup or '[ERROR]' in lup
            or ' FATAL ' in lup or '[FATAL]' in lup
            or 'level=error' in ll
        ):
            token = 'ERROR' if 'ERROR' in lup else 'FATAL'
            entries.append({'ts': _err_ts(line), 'level': 'ERROR',
                            'message': _err_msg(line, token), 'type': 'log'})
            err_c += 1

        # ── WARN ────────────────────────────────────────────────────────
        elif warn_c < max_warn and (
            ' WARN ' in lup or '[WARN]' in lup or 'level=warn' in ll
        ):
            if any(k in ll for k in ('clickhouse', 'connection', 'fail', 'timeout',
                                      'refused', 'exception', 'invalid', 'account',
                                      'retry', 'lost', 'disconnect', 'error')):
                entries.append({'ts': _err_ts(line), 'level': 'WARN',
                                'message': _err_msg(line, 'WARN'), 'type': 'log'})
                warn_c += 1

        # ── INFO (health-critical only) ──────────────────────────────────
        elif info_c < max_info and (' INFO ' in lup or '[INFO]' in lup):
            if any(k in ll for k in ('connected to clickhouse', 'started sc security',
                                      'starting grouping', 'inserted metric',
                                      'postgres pool', 'fully reconciled')):
                entries.append({'ts': _err_ts(line), 'level': 'INFO',
                                'message': _err_msg(line, 'INFO'), 'type': 'info'})
                info_c += 1

    return entries


def parse_pod_log(
    path: Path,
    since_dt: "_dt_module.datetime | None" = None,
    until_dt: "_dt_module.datetime | None" = None,
) -> dict:
    """Return a dict with header info + extracted signals for one pod log."""
    try:
        text = path.read_text(encoding='utf-8', errors='replace')
    except Exception as exc:
        return {
            'pod_name': path.stem,
            'family': family_of(path.stem),
            'status': 'Unknown',
            'error': str(exc),
            'signals': {},
        }

    all_lines = text.splitlines()
    header = parse_header(all_lines)
    pod_name = header.get('pod_name') or path.stem
    fam = family_of(pod_name)

    # Parse collected_at header date into base_dt for time-only log lines
    ca_str = header.get('collected_at', '')
    collected_at_dt = None
    if ca_str:
        try:
            s = re.sub(r'\s+', ' ', ca_str.replace(' GMT ', ' ').replace(' UTC ', ' ')).strip()
            parts = s.split()
            if len(parts) == 5:
                parts[2] = parts[2].zfill(2)
                s = ' '.join(parts)
            try:
                collected_at_dt = _dt_module.datetime.strptime(s, '%a %b %d %I:%M:%S %p %Y').replace(tzinfo=_dt_module.timezone.utc)
            except Exception:
                collected_at_dt = _dt_module.datetime.strptime(s, '%a %b %d %H:%M:%S %Y').replace(tzinfo=_dt_module.timezone.utc)
        except Exception:
            pass

    # Body = everything after the closing ======= line (line index 7)
    body_start = 0
    for i, line in enumerate(all_lines[:10]):
        if line.strip().startswith('===') and i > 0:
            body_start = i + 1
    body_lines = all_lines[body_start:]

    extractor = _FAMILY_EXTRACTORS.get(fam, _extract_generic)
    signals = extractor(body_lines, since_dt=since_dt, until_dt=until_dt, base_dt=collected_at_dt)

    # Extract structured error/warn entries for the Error Visibility section
    error_entries = extract_error_entries(
        body_lines,
        since_dt=since_dt,
        until_dt=until_dt,
        base_dt=collected_at_dt,
    )

    # Extract or infer restart count
    restarts = 0
    if 'restarts' in header:
        try: restarts = int(header['restarts'])
        except Exception: pass
    if restarts == 0:
        for l in body_lines[:150]:
            cl = _strip_ansi(l).lower()
            if 'switched from state restarting' in cl or 'tasks will be restarted' in cl or 'terminating worker' in cl or 'retrying failed for' in cl:
                restarts += 1

    status = header.get('status', 'Unknown')
    if status == 'Unknown':
        # Smart status inference for Fluentd logs
        if any(e.get('level') == 'ERROR' and e.get('type') == 'exception' for e in error_entries):
            status = 'Failed'
        elif any('fatal' in e.get('message', '').lower() or 'terminating' in e.get('message', '').lower() for e in error_entries):
            status = 'Failed'
        elif fam in ('common-sc-realtime-service', 'sc-bql-thanos', 'fsc-flink-cluster', 'sc-account-sync-service'):
            status = 'Running'
        elif signals.get('spring_started') or signals.get('total_ingested', 0) > 0 or signals.get('bql_queries_ok', 0) > 0:
            status = 'Succeeded'
        elif any(e.get('level') == 'ERROR' for e in error_entries):
            status = 'Failed'
        else:
            status = 'Succeeded'

    result: dict[str, Any] = {
        'pod_name': pod_name,
        'family': fam,
        'status': status,
        'namespace': header.get('namespace', 'ncm-sc'),
        'containers': header.get('containers', ''),
        'collected_at': header.get('collected_at', ''),
        'restarts': restarts,
        'signals': signals,
        'error_entries': error_entries,
    }
    # Expose top-level errors flag for quick filtering
    errs = signals.get('errors') or (signals.get('error_count', 0) > 0 and ['errors present'])
    if errs:
        result['errors'] = True
    return result


# ── Family-level aggregation ──────────────────────────────────────────────────

def aggregate_family(pods: list[dict]) -> dict:
    """Aggregate per-pod results into a family-level summary."""
    if not pods:
        return {}

    res = _aggregate_family_internal(pods)

    # Attach pod instance summaries to the family dictionary
    pod_instances = []
    for p in pods:
        p_errs = sum(1 for e in p.get('error_entries', []) if e.get('level') == 'ERROR')
        p_warns = sum(1 for e in p.get('error_entries', []) if e.get('level') == 'WARN')
        p_last_err = next((e.get('message', '') for e in p.get('error_entries', []) if e.get('level') == 'ERROR'), '')
        pod_instances.append({
            'pod_name':     p['pod_name'],
            'status':       p.get('status', 'Unknown'),
            'containers':   p.get('containers', ''),
            'collected_at': p.get('collected_at', ''),
            'restarts':     p.get('restarts', 0),
            'error_count':  p_errs,
            'warn_count':   p_warns,
            'last_error':   p_last_err[:200],
        })

    # Sort pod instances: Failed first, then by error_count descending, then pod_name
    _status_order = {'Failed': 0, 'Unknown': 1, 'Running': 2, 'Succeeded': 3}
    pod_instances.sort(key=lambda x: (_status_order.get(x['status'], 9), -x['error_count'], x['pod_name']))

    res['pods'] = pod_instances
    return res


def _aggregate_family_internal(pods: list[dict]) -> dict:
    fam = pods[0]['family']
    status_counts = Counter(p['status'] for p in pods)

    # Merge signals depending on family
    if fam == 'common-sc-realtime-service':
        total_pg = sum(p['signals'].get('pg_pools_ready', 0) for p in pods)
        total_bql = sum(p['signals'].get('bql_queries_ok', 0) for p in pods)
        total_res = sum(p['signals'].get('bql_total_results', 0) for p in pods)
        total_err = sum(p['signals'].get('error_count', 0) for p in pods)
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'pg_pools_ready': total_pg,
            'bql_queries_ok': total_bql,
            'bql_total_results': total_res,
            'error_count': total_err,
        }

    if fam == 'sc-bql-thanos':
        total_recv = sum(p['signals'].get('queries_received', 0) for p in pods)
        total_resp = sum(p['signals'].get('queries_responded', 0) for p in pods)
        total_rows = sum(p['signals'].get('total_rows_returned', 0) for p in pods)
        total_err  = sum(p['signals'].get('error_count', 0) for p in pods)
        avg_rows   = round(total_rows / total_resp, 2) if total_resp else 0

        # Aggregate rows-per-query for percentile computation
        all_rpq: list[int] = []
        for p in pods:
            all_rpq.extend(p['signals'].get('rows_per_query', []))
        all_rpq.sort()
        latency_pcts = {
            'p50':    _pct(all_rpq, 50),
            'p95':    _pct(all_rpq, 95),
            'p99':    _pct(all_rpq, 99),
            'max':    float(all_rpq[-1]) if all_rpq else 0.0,
            'sample': len(all_rpq),
        } if all_rpq else {}

        # Merge hourly query data
        merged_hourly: dict[str, dict] = {}
        for p in pods:
            for h, v in (p['signals'].get('hourly_queries') or {}).items():
                r = merged_hourly.setdefault(h, {'requests': 0, 'responses': 0, 'rows': 0})
                r['requests']  += v.get('requests', 0)
                r['responses'] += v.get('responses', 0)
                r['rows']      += v.get('rows', 0)

        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'queries_received':    total_recv,
            'queries_responded':   total_resp,
            'total_rows_returned': total_rows,
            'avg_rows':            avg_rows,
            'error_count':         total_err,
            'latency_pcts':        latency_pcts,
            'hourly_queries':      merged_hourly,
        }

    if fam == 'fsc-flink-cluster-taskmanager':
        merged_missing: Counter = Counter()
        for p in pods:
            for k, v in (p['signals'].get('missing_account_by_type') or {}).items():
                merged_missing[k] += v
        total_warn = sum(p['signals'].get('warn_count', 0) for p in pods)
        total_err = sum(p['signals'].get('error_count', 0) for p in pods)
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'missing_account_by_type': dict(merged_missing),
            'total_missing_account_warnings': sum(merged_missing.values()),
            'warn_count': total_warn,
            'error_count': total_err,
        }

    if fam == 'sc-sp-ml-worker':
        ch_connected = sum(1 for p in pods if p['signals'].get('ch_connected'))
        workers_started = sum(1 for p in pods if p['signals'].get('worker_started'))
        groupings = sum(p['signals'].get('groupings_started', 0) for p in pods)
        all_errors: list[str] = []
        for p in pods:
            all_errors.extend(p['signals'].get('errors') or [])
        # Deduplicate by truncated message
        seen: set[str] = set()
        top_errors: list[str] = []
        for e in all_errors:
            key = e[:100]
            if key not in seen:
                seen.add(key)
                top_errors.append(e)
            if len(top_errors) >= 20:
                break
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'ch_connected_pods': ch_connected,
            'workers_started': workers_started,
            'groupings_started': groupings,
            'top_errors': top_errors,
        }

    if fam == 'sc-cron-flow-stats-worker':
        ch_ready = sum(p['signals'].get('ch_pool_ready', 0) for p in pods)
        bql_ok = sum(p['signals'].get('bql_queries_ok', 0) for p in pods)
        err = sum(p['signals'].get('error_count', 0) for p in pods)
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'ch_pool_ready': ch_ready,
            'bql_queries_ok': bql_ok,
            'error_count': err,
        }

    if fam in ('sc-cron-issue-stats-worker', 'sc-cron-cleanup-worker'):
        started = sum(1 for p in pods if p['signals'].get('spring_started'))
        err = sum(p['signals'].get('error_count', 0) for p in pods)
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'spring_started_count': started,
            'error_count': err,
        }

    if fam == 'ncm-sc-metrics-job':
        merged_collectors: Counter = Counter()
        for p in pods:
            for k, v in (p['signals'].get('collectors') or {}).items():
                merged_collectors[k] += v
        total_inserted = sum(p['signals'].get('metrics_inserted', 0) for p in pods)
        total_ingested = sum(p['signals'].get('total_ingested', 0) for p in pods)
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'collectors': dict(merged_collectors),
            'metrics_inserted': total_inserted,
            'total_ingested': total_ingested,
        }

    if fam == 'ncm-platform-flink-kubernetes-operator':
        rec = sum(p['signals'].get('reconciliations', 0) for p in pods)
        full_rec = sum(p['signals'].get('fully_reconciled', 0) for p in pods)
        err = sum(p['signals'].get('error_count', 0) for p in pods)
        return {
            'pod_count': len(pods),
            'status_counts': dict(status_counts),
            'reconciliations': rec,
            'fully_reconciled': full_rec,
            'error_count': err,
        }

    # Generic fallback
    err = sum(p['signals'].get('error_count', 0) for p in pods)
    return {
        'pod_count': len(pods),
        'status_counts': dict(status_counts),
        'error_count': err,
    }


# ── Issue collection ──────────────────────────────────────────────────────────

def _collect_issues(family_summaries: dict) -> list[str]:
    issues: list[str] = []

    ml = family_summaries.get('sc-sp-ml-worker', {})
    if ml:
        failed = (ml.get('status_counts') or {}).get('Failed', 0)
        total = ml.get('pod_count', 0)
        if failed and total:
            issues.append(
                f"sc-sp-ml-worker: {failed}/{total} pods Failed "
                f"(workers_started={ml.get('workers_started', 0)})"
            )

    bql = family_summaries.get('sc-bql-thanos', {})
    if bql and bql.get('error_count', 0) > 0:
        issues.append(
            f"sc-bql-thanos: {bql['error_count']} ERROR line(s) across "
            f"{bql.get('pod_count', '?')} pod(s)"
        )

    tm = family_summaries.get('fsc-flink-cluster-taskmanager', {})
    if tm:
        missing_total = tm.get('total_missing_account_warnings', 0)
        if missing_total > 100:
            by_type = tm.get('missing_account_by_type', {})
            top = sorted(by_type.items(), key=lambda x: -x[1])[:3]
            top_str = ', '.join(f'{t}={c}' for t, c in top)
            issues.append(
                f"fsc-flink-cluster-taskmanager: {missing_total} "
                f"'No account id found' warnings ({top_str})"
            )

    mj = family_summaries.get('ncm-sc-metrics-job', {})
    if mj and mj.get('total_ingested', 0) == 0 and mj.get('pod_count', 0) > 0:
        issues.append('ncm-sc-metrics-job: total_ingested == 0 — no metrics pushed to IDF')

    return issues


# ── Main build ────────────────────────────────────────────────────────────────

def build_report(
    logs_dir: Path,
    since_dt: "_dt_module.datetime | None" = None,
    until_dt: "_dt_module.datetime | None" = None,
) -> dict:
    """Parse all *.log files and build the full report dict."""
    pods: list[dict] = []
    for p in sorted(logs_dir.glob('*.log')):
        pods.append(parse_pod_log(p, since_dt=since_dt, until_dt=until_dt))

    families: dict[str, list[dict]] = defaultdict(list)
    for pod in pods:
        families[pod['family']].append(pod)

    family_summaries: dict[str, dict] = {}
    for fam, fam_pods in sorted(families.items()):
        family_summaries[fam] = aggregate_family(fam_pods)

    status_counts = Counter(p['status'] for p in pods)
    error_pods = [p['pod_name'] for p in pods if p.get('errors')]

    # ── Aggregate error entries across all pods ───────────────────────────
    all_entries: list[dict] = []
    for pod in pods:
        for e in pod.get('error_entries', []):
            all_entries.append({
                'pod_name': pod['pod_name'],
                'family':   pod['family'],
                'status':   pod.get('status', 'Unknown'),
                **e,
            })

    # Sort: ERROR first, WARN second, INFO last; within each level newest ts first.
    # Use two stable passes (Python sort is guaranteed stable).
    _level_order = {'ERROR': 0, 'WARN': 1, 'INFO': 2}
    all_entries.sort(key=lambda x: x.get('ts', ''), reverse=True)  # newest first
    all_entries.sort(key=lambda x: _level_order.get(x.get('level', 'INFO'), 9))  # level order

    # Signature grouping
    sig_map: dict[str, dict] = {}
    for e in all_entries:
        sig = _normalize_sig(e['message'])
        if not sig:
            continue
        if sig not in sig_map:
            sig_map[sig] = {
                'signature': sig,
                'level':     e['level'],
                'count':     0,
                '_pods':     set(),
                'first_ts':  e.get('ts', ''),
                'last_ts':   e.get('ts', ''),
                'example':   e['message'],
                'type':      e.get('type', 'log'),
            }
        rec = sig_map[sig]
        rec['count'] += 1
        rec['_pods'].add(e['pod_name'])
        ts = e.get('ts', '')
        if ts:
            if not rec['first_ts'] or ts < rec['first_ts']:
                rec['first_ts'] = ts
            if not rec['last_ts'] or ts > rec['last_ts']:
                rec['last_ts'] = ts

    error_signatures = sorted(
        [{k: v for k, v in rec.items() if k != '_pods'} | {
            'pod_count':   len(rec['_pods']),
            'pods_sample': sorted(rec['_pods'])[:5],
         }
         for rec in sig_map.values()],
        key=lambda x: (-x['count'], _level_order.get(x['level'], 9)),
    )[:100]

    err_level_counts = Counter(e.get('level', '') for e in all_entries)

    # ── Pod summaries ─────────────────────────────────────────────────────
    pod_summaries: list[dict] = []
    for pod in pods:
        p_errs = sum(1 for e in pod.get('error_entries', []) if e.get('level') == 'ERROR')
        p_warns = sum(1 for e in pod.get('error_entries', []) if e.get('level') == 'WARN')
        p_infos = sum(1 for e in pod.get('error_entries', []) if e.get('level') == 'INFO')
        last_err = next((e.get('message', '') for e in pod.get('error_entries', []) if e.get('level') == 'ERROR'), '')
        pod_summaries.append({
            'pod_name':     pod['pod_name'],
            'family':       pod['family'],
            'status':       pod.get('status', 'Unknown'),
            'containers':   pod.get('containers', ''),
            'collected_at': pod.get('collected_at', ''),
            'error_count':  p_errs,
            'warn_count':   p_warns,
            'info_count':   p_infos,
            'last_error':   last_err[:150],
        })

    # Build balanced sample of log entries (so WARN and INFO are not crowded out by thousands of errors)
    err_items  = [e for e in all_entries if e.get('level') == 'ERROR'][:500]
    warn_items = [e for e in all_entries if e.get('level') == 'WARN'][:300]
    info_items = [e for e in all_entries if e.get('level') == 'INFO'][:200]
    balanced_entries = err_items + warn_items + info_items

    # Build time-series data (hourly activity, percentiles, worker gaps)
    time_series = build_time_series(pods, all_entries, family_summaries)

    return {
        'logs_dir': str(logs_dir),
        'pod_logs_dir': str(logs_dir),
        'analysis_since': since_dt.isoformat() if since_dt else None,
        'analysis_until': until_dt.isoformat() if until_dt else None,
        'total_pods': len(pods),
        'status_counts': dict(status_counts),
        'families': family_summaries,
        'error_pods': error_pods,
        'issues': _collect_issues(family_summaries),
        'pod_summaries':    pod_summaries,
        'error_entries':    balanced_entries,
        'error_signatures': error_signatures,
        'error_counts':     dict(err_level_counts),
        'time_series':      time_series,
    }


def ingest_sc_pipeline_run_logs(
    target: "str | Path",
    runs_dir: "Path | None" = None,
    force: bool = False,
    since: "str | None" = None,
    until: "str | None" = None,
    level: "str | None" = None,
    pod: "str | None" = None,
    family: "str | None" = None,
    search: "str | None" = None,
    limit: int = 200,
    offset: int = 0,
) -> dict:
    """
    Ingest target run logs from an sc-pl_* run directory or logs folder.
    Extracts timestamps, log levels, pod names, error counts, and recurring
    signatures, structuring them into a clean JSON payload.
    """
    target_str = str(target).strip()
    target_path = Path(target_str)
    run_dir: Path | None = None
    run_id: str = target_path.name

    since_dt = parse_time_boundary(since)
    until_dt = parse_time_boundary(until)

    if target_path.is_dir():
        run_dir = target_path
    else:
        # Search inside runs_dir
        if not runs_dir:
            runs_dir = Path("/home/mohan.as1/projects/snapshot_test/logs/runs")
        candidates = [
            runs_dir / target_str,
            runs_dir / target_str.replace("sc_pl_", "sc-pl_"),
            runs_dir / target_str.replace("sc-pl_", "sc_pl_"),
        ]
        for c in candidates:
            if c.is_dir():
                run_dir = c
                run_id = c.name
                break

    if not run_dir or not run_dir.is_dir():
        raise FileNotFoundError(f"Run directory not found for target '{target}'")

    analysis_file = run_dir / "sc_pipeline_analysis.json"
    report: dict[str, Any] | None = None

    # Load cached report if valid, matching window, and not force-reparsing
    if not force and analysis_file.is_file():
        try:
            cached = json.loads(analysis_file.read_text(encoding="utf-8"))
            c_since = cached.get("analysis_since")
            c_until = cached.get("analysis_until")
            req_since = since_dt.isoformat() if since_dt else None
            req_until = until_dt.isoformat() if until_dt else None

            if (cached.get("error_signatures") and cached.get("pod_summaries")
                    and (not since or c_since == req_since)
                    and (not until or c_until == req_until)):
                report = cached
        except Exception:
            report = None

    if report is None:
        # Find pod logs directory in run_dir
        pod_log_dirs = sorted(run_dir.glob("pod-logs_*"))
        if pod_log_dirs and pod_log_dirs[-1].is_dir():
            target_logs_dir = pod_log_dirs[-1]
        elif any(run_dir.glob("*.log")):
            target_logs_dir = run_dir
        else:
            raise FileNotFoundError(f"No pod logs found in run directory '{run_dir}'")

        report = build_report(target_logs_dir, since_dt=since_dt, until_dt=until_dt)
        try:
            analysis_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass

    # ── Filter and paginate parsed logs ──────────────────────────────────
    all_entries = report.get("error_entries", [])
    filtered = []
    level_filter = level.upper() if level and level.upper() != "ALL" else None
    search_q = search.lower().strip() if search else None
    pod_q = pod.lower().strip() if pod else None
    family_q = family.lower().strip() if family else None

    for e in all_entries:
        if level_filter and e.get("level") != level_filter:
            continue
        if pod_q and pod_q not in e.get("pod_name", "").lower():
            continue
        if family_q and family_q != e.get("family", "").lower():
            continue
        if search_q:
            combined = f"{e.get('message','')} {e.get('pod_name','')} {e.get('signature','')}".lower()
            if search_q not in combined:
                continue
        filtered.append({
            "timestamp": e.get("ts") or "",
            "level": e.get("level", "INFO"),
            "pod_name": e.get("pod_name", ""),
            "family": e.get("family", ""),
            "message": e.get("message", ""),
            "type": e.get("type", "log"),
            "signature": e.get("signature") or _normalize_sig(e.get("message", "")),
        })

    total_matching = len(filtered)
    paginated_entries = filtered[offset:offset + limit]

    return {
        "ok": True,
        "run_id": run_id,
        "target_dir": str(run_dir),
        "pod_logs_dir": report.get("pod_logs_dir") or report.get("logs_dir"),
        "total_pods": report.get("total_pods", 0),
        "status_counts": report.get("status_counts", {}),
        "error_counts": report.get("error_counts", {}),
        "total_errors": sum(report.get("error_counts", {}).values()),
        "issues": report.get("issues", []),
        "families": report.get("families", {}),
        "pod_summaries": report.get("pod_summaries", []),
        "error_signatures": report.get("error_signatures", []),
        "time_series": report.get("time_series", {}),
        "total_matching_logs": total_matching,
        "parsed_logs": paginated_entries,
        "limit": limit,
        "offset": offset,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyse ncm-sc pod logs")
    # Fetch-fresh mode (SSH into PC + kubectl)
    ap.add_argument("--fetch-fresh", action="store_true", default=False,
                    help="Fetch pod logs from PC via SSH + kubectl before analysing")
    ap.add_argument("--pc-ip",   default="", help="Prism Central IP (required with --fetch-fresh)")
    ap.add_argument("--pc-user", default="nutanix", help="SSH username (default: nutanix)")
    ap.add_argument("--pc-pass", default="", help="SSH password")
    ap.add_argument("--cluster", default=MSP_CLUSTER_NAME,
                    help=f"MSP cluster name for mspctl kubeconfig fetch (default: {MSP_CLUSTER_NAME})")
    ap.add_argument("--kubeconfig", type=Path, default=None,
                    help="Path to save/reuse kubeconfig (default: <out-dir>/pc_kube.txt)")
    ap.add_argument("--since",  default="", help="kubectl --since duration or ISO timestamp (e.g. 2h, 30m, 2026-10-31T12:00:00Z)")
    ap.add_argument("--until",  default="", help="Until ISO timestamp filter (e.g. 2026-10-31T18:00:00Z)")
    # Local mode
    ap.add_argument("--pod-logs-dir", default="", help="Directory containing *.log files (local mode)")
    # Output
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="Output directory (default: beside pod-logs-dir or cwd)")
    ap.add_argument("--out", default="sc_pipeline_analysis.json", help="Output JSON filename")
    args = ap.parse_args()

    since_dt = parse_time_boundary(args.since or None)
    until_dt = parse_time_boundary(args.until or None)

    if args.fetch_fresh:
        if not args.pc_ip:
            print("ERROR: --pc-ip is required with --fetch-fresh", file=sys.stderr)
            sys.exit(1)
        out_dir = args.out_dir or Path.cwd()
        out_dir.mkdir(parents=True, exist_ok=True)
        kc_path = args.kubeconfig or (out_dir / "pc_kube.txt")
        # Reuse existing kubeconfig if present (avoids re-fetching)
        if kc_path.is_file() and kc_path.stat().st_size > 100:
            print(f"[INFO] Re-using existing kubeconfig: {kc_path}")
        else:
            print(f"[INFO] Fetching kubeconfig from PC {args.pc_ip} (cluster: {args.cluster}) …")
            fetch_kubeconfig(args.pc_ip, args.pc_user, args.pc_pass, kc_path, cluster=args.cluster)
        print("[INFO] Collecting ncm-sc pod logs from Fluentd aggregator pod …")
        logs_dir = collect_ncm_sc_pod_logs_from_fluentd(kc_path, out_dir, since=args.since)
    else:
        if not args.pod_logs_dir:
            print("ERROR: --pod-logs-dir is required (or use --fetch-fresh)", file=sys.stderr)
            sys.exit(1)
        logs_dir = Path(args.pod_logs_dir)
        if not logs_dir.is_dir():
            print(f"ERROR: {logs_dir} is not a directory", file=sys.stderr)
            sys.exit(1)
        out_dir = args.out_dir or logs_dir.parent

    report = build_report(logs_dir, since_dt=since_dt, until_dt=until_dt)

    out_path = out_dir / args.out
    out_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({
        "total_pods": report["total_pods"],
        "status_counts": report["status_counts"],
        "issues": len(report["issues"]),
        "output": str(out_path),
        "pod_logs_dir": str(logs_dir),
    }, indent=2))


if __name__ == '__main__':
    main()

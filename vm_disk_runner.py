"""Guest VM dummy data churn over SSH: create / append / overwrite dd, or rm glob (snapshot test helpers)."""

from __future__ import annotations

import logging
import math
import os
import random
import re
import signal
import tempfile
import concurrent.futures
import shlex
import threading
import shutil
import subprocess
import time
import warnings
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from snapshot_runner import SnapshotConfig, TLS_VERIFY, RunCancelled, _vm_name_should_skip
from vm_inventory import fetch_vm_inventory_rows

# Prism inventory + name filters only; work runs inside guest via SSH.
DISK_CREATE = "create"
DISK_ADD = "add"
DISK_UPDATE = "update"
DISK_DELETE = "delete"
DISK_RANDOM_MIX = "random_mix"

ALL_MODES = frozenset({DISK_CREATE, DISK_ADD, DISK_UPDATE, DISK_DELETE, DISK_RANDOM_MIX})

# GNU ``dd`` ``bs=`` suffix bytes (coreutils): ``K``/``M``/… alone = IEC; ``*B`` = SI for k/M/G/T.
_GUEST_DD_BS_SUFFIX_BYTES: Dict[str, int] = {
    "": 1,
    "c": 1,
    "w": 2,
    "b": 512,
    "kb": 1000,
    "k": 1024,
    "mb": 10**6,
    "m": 1024**2,
    "gb": 10**9,
    "g": 1024**3,
    "tb": 10**12,
    "t": 1024**4,
}

_GUEST_DD_BS_RE = re.compile(r"^[1-9]\d{0,15}([A-Za-z]+)?$")


def guest_dd_bs_to_bytes(spec: str) -> int:
    """Bytes per ``dd`` block for ``bs=`` (GNU-style suffixes: ``1M``, ``10MB``, ``512``, …)."""
    s = (spec or "").strip()
    if not s or _GUEST_DD_BS_RE.match(s) is None:
        raise ValueError(f"invalid guest_dd_bs {spec!r}")
    i = 0
    while i < len(s) and s[i].isdigit():
        i += 1
    num_s, suf = s[:i], s[i:].lower()
    mult = _GUEST_DD_BS_SUFFIX_BYTES.get(suf)
    if mult is None:
        raise ValueError(f"unsupported dd bs suffix in {spec!r}")
    return int(num_s) * mult


def normalize_guest_dd_bs(spec: str, *, default: str = "1M") -> str:
    """Validate guest ``dd`` block size; return stripped ``bs`` string."""
    raw = (spec or "").strip() or default
    guest_dd_bs_to_bytes(raw)
    return raw


def guest_dd_transfer_bytes(bs: str, count: int) -> int:
    """Total transfer size for ``count`` blocks of ``bs``."""
    return guest_dd_bs_to_bytes(bs) * max(0, int(count))


# When ``cluster_adaptive_ssh_ceiling`` is 0, per-cluster adaptive cap is
# ``max(guest_ssh_parallel, this)`` (then capped by VMs in the shard).
ADAPTIVE_GUEST_SSH_AUTO_CEILING = 30

# PE Prism Element ``/cluster/stats`` metrics (values are ppm; %% = ppm / 10_000).
PE_STATS_CPU_METRIC = "hypervisor_cpu_usage_ppm"
PE_STATS_MEM_METRIC = "aggregate_hypervisor_memory_usage_ppm"
PE_STATS_CPU_INTERVAL_SEC = 60
PE_STATS_MEM_INTERVAL_SEC = 30
# Query window must cover at least a few intervals so the latest bucket is populated.
PE_STATS_HISTORY_SEC = 900
PE_STATS_PATH = "/PrismGateway/services/rest/v1/cluster/stats"

_DEFAULT_ADAPTIVE_RAMP: Tuple[Tuple[float, int], ...] = ((180.0, 5), (300.0, 3))


def _parse_adaptive_ramp_schedule(raw: Optional[str]) -> List[Tuple[float, int]]:
    """
    Parse ``sec/inc`` phases (comma-separated). After ``sec`` seconds *consecutively* below CPU and
    memory ramp limits, increase effective concurrency by ``inc`` (capped by ceiling). Timer restarts
    after each bump. Example: ``180/5,300/3`` → +5 after 3 min, then +3 after 5 min more.
    """
    if not (raw or "").strip():
        return list(_DEFAULT_ADAPTIVE_RAMP)
    out: List[Tuple[float, int]] = []
    for part in str(raw).split(","):
        p = part.strip()
        if not p or "/" not in p:
            continue
        a, b = p.split("/", 1)
        try:
            sec = float(a.strip())
            inc = int(float(b.strip()))
        except (TypeError, ValueError):
            continue
        if sec <= 0 or inc <= 0:
            continue
        out.append((sec, inc))
    return out if out else list(_DEFAULT_ADAPTIVE_RAMP)


def is_vm_powered_on(power_state: Optional[str]) -> bool:
    """Treat Prism ``power_state`` as on for guest SSH (typical values: on, ON)."""
    if power_state is None:
        return False
    p = str(power_state).strip().upper()
    return p in ("ON", "POWERED_ON", "POWER_ON")


def build_guest_disk_worklist(
    rows: List[Dict[str, Any]],
    snap_cfg: SnapshotConfig,
    *,
    require_power_on: bool = True,
    min_memory_mib: int = 0,
) -> Tuple[List[Tuple[str, Optional[str], str, str]], Dict[str, int]]:
    """
    VMs eligible for guest disk churn: pass name rules, have IP, (by default) power ON,
    and optional minimum **configured RAM** from Prism (``memory_mib``; MiB from ``memory_size_bytes``).

    If ``min_memory_mib`` > 0, only VMs with ``memory_mib`` strictly greater than that threshold qualify
    (unknown RAM counts as not qualifying).

    Returns (worklist of (uuid, name, first_ip, cluster_name), counts).
    """
    ignored_name = 0
    skipped_no_ip = 0
    skipped_power_off = 0
    skipped_below_min_memory = 0
    worklist: List[Tuple[str, Optional[str], str, str]] = []
    floor = max(0, int(min_memory_mib or 0))

    for row in rows:
        name = row.get("name")
        uid = str(row.get("uuid") or "")
        if not uid:
            continue
        if _vm_name_should_skip(name, snap_cfg):
            ignored_name += 1
            continue
        ips = row.get("ips") or []
        if not ips:
            skipped_no_ip += 1
            continue
        if require_power_on and not is_vm_powered_on(row.get("power_state")):
            skipped_power_off += 1
            continue
        if floor > 0:
            mem = row.get("memory_mib")
            try:
                mem_i = int(mem) if mem is not None else None
            except (TypeError, ValueError):
                mem_i = None
            if mem_i is None or mem_i <= floor:
                skipped_below_min_memory += 1
                continue
        cname = str(row.get("cluster_name") or "—").strip() or "—"
        worklist.append((uid, name, str(ips[0]).strip(), cname))

    counts = {
        "ignored_name": ignored_name,
        "skipped_no_ip": skipped_no_ip,
        "skipped_power_off": skipped_power_off,
        "skipped_below_min_memory": skipped_below_min_memory,
        "inventory_distinct_vms": len(rows),
    }
    return worklist, counts


def resolve_disk_run_limit(spec: str, eligible: int) -> int:
    """
    How many VMs to run against, capped by ``eligible``.

    ``spec``: empty / \"all\" / \"100%\" → all eligible; \"50\" → 50 VMs; \"25%\" → ceil(25% * eligible).
    """
    if eligible <= 0:
        return 0
    s = (spec or "").strip()
    if not s or s == "100%" or s.lower() in ("all", "max", "*"):
        return eligible
    if s.endswith("%"):
        pct = float(s[:-1].strip())
        pct = max(0.0, min(100.0, pct))
        return max(0, min(eligible, int(math.ceil(eligible * pct / 100.0))))
    try:
        n = int(float(s))
        return max(0, min(eligible, n))
    except ValueError as e:
        raise ValueError(
            f"Invalid disk run limit {spec!r}; use an integer (e.g. 100) or percent (e.g. 50%)."
        ) from e


@dataclass
class DiskOpConfig:
    """PC creds list VMs; guest SSH runs dd/rm on each VM’s first reported IP."""

    base_url: str
    pc_user: str
    pc_password: str
    group_member_page: int = 500
    skip_substrings: Tuple[str, ...] = ()
    skip_regex_patterns: Tuple[str, ...] = ()
    random_seed: Optional[int] = None
    # One of: create, add, update, delete, random_mix
    mode: str = DISK_UPDATE

    guest_ssh_user: str = "root"
    guest_ssh_password: str = ""
    guest_ssh_port: int = 22
    guest_ssh_connect_timeout: float = 30.0
    guest_ssh_command_timeout: float = 300.0 # 5 minutes for disk operations

    guest_target_file: str = "/root/dummy_snapshot_data_1.img"
    guest_delete_glob: str = "/root/dummy_snapshot_data_*.img"
    # ``dd`` block size (GNU ``bs=``); see ``normalize_guest_dd_bs``.
    guest_dd_bs: str = "1M"
    # Block counts for create / add / update (with ``guest_dd_bs``, total bytes = bs × count).
    create_count_mib: int = 1024
    churn_count_mib: int = 500
    # Cap guest SSH runs: "", "all", "100", "50%", etc. (see ``resolve_disk_run_limit``).
    disk_run_limit: str = ""
    # Only VMs with Prism ``memory_mib`` **>** this (configured RAM). 0 = no minimum.
    guest_min_memory_mib: int = 250
    # Max concurrent ``ssh`` sessions to guests (default 10).
    guest_ssh_parallel: int = 10
    # When True: run each cluster in its own thread pool shard; within a cluster, up to
    # ``guest_ssh_parallel`` concurrent guest SSH sessions. Optional ``vm_per_cluster`` caps how many
    # VMs are *selected* per cluster (0 = no cap — all eligible VMs in that cluster).
    parallel_clusters: bool = False
    # Max VMs to *select* per cluster when ``parallel_clusters`` (0 = all eligible in that cluster).
    vm_per_cluster: int = 0
    # When True (default): poll each PE Prism ``cluster/stats`` before guest SSH; pause until CPU is
    # below ``cluster_cpu_max_pct`` when that limit is > 0 (see ``pe_cvm_ips_multiline``). Applies to
    # all disk job modes (parallel shards or single pool).
    cluster_pe_top_monitor: bool = True
    # Pause when PE stats report CPU usage %% >= this (0 = do not check CPU).
    cluster_cpu_max_pct: float = 85.0
    # Pause when PE stats report memory usage %% >= this (0 = do not check memory; UI/API use 0).
    cluster_mem_max_pct: float = 0.0
    # Optional: with PE CVM IPs, raise per-cluster guest SSH concurrency via ``cluster_adaptive_ramp``
    # while PE CPU stays below limits; overload resets baseline. Independent of ``parallel_clusters``.
    cluster_adaptive_ssh_parallel: bool = False
    # PE CPU%% from cluster stats; at or above → reset to baseline (no ramp increases while hot).
    cluster_adaptive_cpu_threshold_pct: float = 90.0
    # Comma-separated ``seconds/slots`` phases (see ``_parse_adaptive_ramp_schedule``). Default 180/5,300/3.
    cluster_adaptive_ramp: str = "180/5,300/3"
    # Legacy: per-sample step (no longer used; adaptive uses ``cluster_adaptive_ramp`` only).
    cluster_adaptive_ssh_step: int = 2
    # Max concurrent guest SSH when adaptive is on (0 = max(guest_ssh_parallel,
    # ADAPTIVE_GUEST_SSH_AUTO_CEILING); set explicit value to override).
    cluster_adaptive_ssh_ceiling: int = 0
    # If PE CPU%% jumps by at least this many points vs the prior stats sample on this cluster,
    # reset adaptive concurrency to ``guest_ssh_parallel`` even when still below
    # ``cluster_adaptive_cpu_threshold_pct`` (0 = disable spike detection).
    cluster_adaptive_cpu_spike_delta_pct: float = 10.0
    # When adaptive SSH hits overload (CPU/mem over limit or spike): sleep this many seconds before
    # proceeding with guest work (per cluster; holds that cluster's stats lock).
    cluster_adaptive_overload_pause_sec: float = 10.0
    # After overload or spike: no ramp increases for this many seconds (concurrency stays at baseline).
    cluster_adaptive_cooldown_sec: float = 300.0
    # Seconds to sleep between PE stats checks while over limit.
    cluster_util_pause_sec: float = 30.0
    # After first over-limit sample, keep pausing/retrying for at most this many seconds, then continue anyway.
    cluster_util_max_retry_sec: float = 1800.0
    # One line per PE CVM IP (Prism Element host for :9440 stats), **same order as cluster names**
    # (sorted, unnamed last), or ``ClusterName=ip`` lines.
    pe_cvm_ips_multiline: str = ""
    # HTTPS port for PE ``PrismGateway`` (cluster stats); default 9440.
    pe_prism_rest_port: int = 9440
    # SSH to PE CVM (defaults: ``BULK_SNAP_CURATOR_PE_SSH_*`` env); unused for CPU/mem metrics (REST only).
    pe_cvm_ssh_user: str = ""
    pe_cvm_ssh_password: str = ""
    pe_cvm_ssh_port: int = 22

    _compiled_regexes: Tuple[Any, ...] = field(default_factory=tuple, repr=False)

    def compile_regexes(self) -> None:
        import re

        object.__setattr__(
            self,
            "_compiled_regexes",
            tuple(re.compile(p, re.IGNORECASE) for p in self.skip_regex_patterns),
        )

    def to_snapshot_cfg(self) -> SnapshotConfig:
        return SnapshotConfig(
            base_url=self.base_url.rstrip("/"),
            pc_user=self.pc_user,
            pc_password=self.pc_password,
            batch_size=10,
            group_member_page=self.group_member_page,
            poll_interval=4.0,
            task_timeout_sec=300,
            sleep_before_task_poll_sec=0.0,
            skip_substrings=self.skip_substrings,
            skip_regex_patterns=self.skip_regex_patterns,
        )


def vm_per_cluster_cap(cfg: DiskOpConfig) -> Optional[int]:
    """Max VMs to take from each cluster when ``parallel_clusters`` is enabled; ``None`` = all eligible."""
    n = int(cfg.vm_per_cluster or 0)
    if n <= 0:
        return None
    return max(1, min(n, 500))


def partition_guest_disk_worklist_by_cluster(
    worklist: List[Tuple[str, Optional[str], str, str]],
    cfg: DiskOpConfig,
) -> Tuple[List[Tuple[str, Optional[str], str, str]], Dict[str, Any]]:
    """
    Group eligible VMs by ``cluster_name``, take up to ``vm_per_cluster_cap`` per cluster
    (or all eligible in each cluster when that is ``None``), stable cluster name order,
    then apply global ``disk_run_limit`` (``resolve_disk_run_limit``) to the concatenated list.
    """
    vm_cap = vm_per_cluster_cap(cfg)
    by_cluster: Dict[str, List[Tuple[str, Optional[str], str, str]]] = {}
    for uid, name, ip, cname in worklist:
        by_cluster.setdefault(cname, []).append((uid, name, ip, cname))
    sorted_names = sorted(by_cluster.keys(), key=lambda x: (x == "—", x))
    chunks: List[Tuple[str, Optional[str], str, str]] = []
    for c in sorted_names:
        part = by_cluster[c]
        if vm_cap is not None:
            part = part[:vm_cap]
        chunks.extend(part)
    eligible = len(worklist)
    max_total = resolve_disk_run_limit(cfg.disk_run_limit, eligible)
    if len(chunks) > max_total:
        chunks = chunks[:max_total]
    per_cluster = Counter(t[3] for t in chunks)
    meta = {
        "vm_per_cluster_cap": vm_cap,
        "clusters_in_inventory": len(by_cluster),
        "per_cluster_planned": {
            k: per_cluster[k]
            for k in sorted(per_cluster.keys(), key=lambda x: (x == "—", x))
        },
    }
    return chunks, meta


def _ssh_host_for_socket(field: str) -> str:
    s = (field or "").strip()
    if not s:
        return ""
    s = s.removeprefix("https://").removeprefix("http://")
    s = s.split("/")[0].strip()
    if not s.startswith("[") and s.count(":") == 1:
        host, port = s.rsplit(":", 1)
        if port.isdigit():
            s = host
    if len(s) >= 2 and s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return s


def _terminate_ssh_process(p: subprocess.Popen) -> None:
    if p.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    except (ProcessLookupError, OSError, AttributeError):
        try:
            p.terminate()
        except ProcessLookupError:
            pass


def _guest_sshpass_run(
    host: str,
    username: str,
    password: str,
    remote_cmd: str,
    *,
    port: int = 22,
    connect_timeout: float = 30.0,
    command_timeout: float = 7200.0,
    cancel_event: Optional[Any] = None,
) -> Tuple[int, str, str]:
    if os.environ.get("BULK_SNAP_GUEST_NO_SSHPASS", "").strip().lower() in ("1", "true", "yes"):
        return -1, "", "sshpass disabled by BULK_SNAP_GUEST_NO_SSHPASS"
    if not shutil.which("sshpass") or not shutil.which("ssh"):
        return -1, "", "sshpass or ssh not found on this server"

    host = _ssh_host_for_socket(host)
    if not host:
        return -1, "", "Guest IP/host is empty"

    ct = int(max(5, min(120, connect_timeout)))
    # Default ``-T``: no PTY so ``BULK_SNAP_T`` lines are not mixed with terminal init / CR noise.
    # Set ``BULK_SNAP_GUEST_SSH_TTY=1`` to force ``-tt`` (e.g. if a guest requires a TTY).
    want_tty = os.environ.get("BULK_SNAP_GUEST_SSH_TTY", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "tt",
    )
    ssh_mode = ["-tt"] if want_tty else ["-T"]
    cmd = [
        "sshpass",
        "-p",
        password,
        "ssh",
        *ssh_mode,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-o",
        "PreferredAuthentications=password,keyboard-interactive",
        "-o",
        f"ConnectTimeout={ct}",
        "-p",
        str(int(port)),
        f"{username}@{host}",
        remote_cmd,
    ]
    max_sec = float(connect_timeout) + float(command_timeout)
    try:
        if cancel_event is None:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max_sec,
            )
            return p.returncode, (p.stdout or ""), (p.stderr or "")

        with tempfile.TemporaryDirectory(prefix="bulk_snap_guest_ssh_") as td:
            out_path = os.path.join(td, "stdout.txt")
            err_path = os.path.join(td, "stderr.txt")
            fout = open(out_path, "wb")
            ferr = open(err_path, "wb")
            try:
                p = subprocess.Popen(
                    cmd,
                    stdout=fout,
                    stderr=ferr,
                    start_new_session=True,
                )
            finally:
                fout.close()
                ferr.close()
            deadline = time.monotonic() + max_sec
            while p.poll() is None:
                if cancel_event.is_set():
                    _terminate_ssh_process(p)
                    try:
                        p.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                        except (ProcessLookupError, OSError, AttributeError):
                            p.kill()
                        p.wait()
                    raise RunCancelled()
                if time.monotonic() >= deadline:
                    _terminate_ssh_process(p)
                    try:
                        p.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                        except (ProcessLookupError, OSError, AttributeError):
                            p.kill()
                        p.wait()
                    return -1, "", "ssh command timed out"
                time.sleep(0.25)
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            with open(out_path, "rb") as rf_out:
                out_b = rf_out.read()
            with open(err_path, "rb") as rf_err:
                err_b = rf_err.read()
        return (
            int(p.returncode if p.returncode is not None else 0),
            out_b.decode("utf-8", errors="replace"),
            err_b.decode("utf-8", errors="replace"),
        )
    except RunCancelled:
        raise
    except subprocess.TimeoutExpired:
        return -1, "", "ssh command timed out"
    except OSError as e:
        return -1, "", f"Could not run ssh/sshpass: {e}"


_cvm_top_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_cvm_top_lock = threading.Lock()
_CVM_TOP_CACHE_TTL_SEC = 5.0


def _top_process_header_indices(header_line: str) -> Optional[dict[str, int]]:
    """Map ``top -bn1`` header tokens to indices for ``%CPU``, ``%MEM``, optional ``RES``, ``COMMAND``."""
    parts = header_line.split()
    if "PID" not in parts or "%CPU" not in parts or "%MEM" not in parts:
        return None
    try:
        i_cpu = parts.index("%CPU")
        i_mem = parts.index("%MEM")
    except ValueError:
        return None
    i_cmd: Optional[int] = None
    if "COMMAND" in parts:
        i_cmd = parts.index("COMMAND")
    elif "CMD" in parts:
        i_cmd = parts.index("CMD")
    if i_cmd is None:
        return None
    i_res: Optional[int] = None
    if "RES" in parts:
        try:
            i_res = parts.index("RES")
        except ValueError:
            i_res = None
    return {"cpu": i_cpu, "mem": i_mem, "res": i_res, "cmd": i_cmd}


def _parse_top_res_token(tok: str) -> Optional[float]:
    """Parse ``top`` RES column into **KiB** (plain KiB or suffix ``k``/``m``/``g``/…)."""
    t = (tok or "").strip()
    if not t:
        return None
    m = re.match(r"^(\d+\.?\d*)\s*([kmgtpezy])?.*$", t, re.I)
    if not m:
        try:
            return float(t.replace(",", ""))
        except ValueError:
            return None
    val = float(m.group(1))
    suf = (m.group(2) or "").lower()
    if suf in ("", "k"):
        return val
    if suf == "m":
        return val * 1024.0
    if suf == "g":
        return val * 1024.0 * 1024.0
    if suf == "t":
        return val * 1024.0**3
    if suf in ("p", "e", "z", "y"):
        return val * 1024.0**4
    return val


def _stargate_metrics_from_top_data_line(
    line: str, col: dict[str, int]
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Stargate row: ``%CPU``, ``%MEM``, RES (KiB). Header indices avoid wrong-column bugs vs argv offsets."""
    parts = line.split()
    i_cmd = int(col["cmd"])
    if len(parts) <= i_cmd:
        return None, None, None
    joined_low = " ".join(p.lower() for p in parts[i_cmd:])
    if "stargate" not in joined_low:
        return None, None, None
    try:
        cpu = float(parts[int(col["cpu"])].replace(",", "."))
        mem = float(parts[int(col["mem"])].replace(",", "."))
    except (ValueError, IndexError):
        return None, None, None
    if math.isnan(cpu) or math.isnan(mem):
        return None, None, None
    if not (0.0 <= mem <= 100.0):
        return None, None, None
    if cpu < 0.0 or cpu > 1_000_000.0:
        return None, None, None
    res_kib: Optional[float] = None
    ri = col.get("res")
    if ri is not None and int(ri) < len(parts):
        res_kib = _parse_top_res_token(parts[int(ri)])
    return cpu, mem, res_kib


def _parse_ps_stargate_metrics(text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """``ps -C stargate -o rss=,%cpu=,%mem=`` — RSS in KiB on Linux. Prefer row with largest RSS."""
    best_rss = -1.0
    pick_cpu: Optional[float] = None
    pick_mem: Optional[float] = None
    pick_rss: Optional[float] = None
    for raw in (text or "").splitlines():
        s = raw.strip()
        if not s:
            continue
        parts = s.split()
        if len(parts) < 3:
            continue
        try:
            rss = float(parts[0].replace(",", "."))
            cpu = float(parts[1].replace(",", "."))
            mem = float(parts[2].replace(",", "."))
        except ValueError:
            continue
        if rss >= best_rss:
            best_rss = rss
            pick_cpu = cpu
            pick_mem = mem
            pick_rss = rss
    if best_rss < 0:
        return None, None, None
    return pick_cpu, pick_mem, pick_rss


def _parse_top_bn1_pe_cvm(text: str) -> Dict[str, Any]:
    """Extract CVM ``us`` / ``wa`` from the summary line and stargate ``%CPU`` / ``%MEM`` / RES (KiB)."""
    out: Dict[str, Any] = {}
    if not (text or "").strip():
        return out
    header_line = ""
    for raw in text.splitlines():
        s = raw.strip()
        if "PID" in s and "%CPU" in s and ("COMMAND" in s or re.search(r"\bCMD\b", s)):
            header_line = s
            break
    col = _top_process_header_indices(header_line)
    for raw in text.splitlines():
        s = raw.strip()
        if "Cpu(s)" in s and "wa" in s.lower():
            m_us = re.search(r"(\d+\.?\d*)\s*us\b", s, re.I)
            m_wa = re.search(r"(\d+\.?\d*)\s*wa\b", s, re.I)
            if m_us:
                try:
                    out["pe_cvm_cpu_us_pct"] = round(float(m_us.group(1)), 1)
                except ValueError:
                    pass
            if m_wa:
                try:
                    out["pe_cvm_cpu_wa_pct"] = round(float(m_wa.group(1)), 1)
                except ValueError:
                    pass
            break
    st_cpu: Optional[float] = None
    st_mem: Optional[float] = None
    st_res: Optional[float] = None
    for raw in text.splitlines():
        s = raw.strip()
        if "PID" in s and "%CPU" in s and ("COMMAND" in s or re.search(r"\bCMD\b", s)):
            continue
        if col is None:
            continue
        c_v, m_v, r_v = _stargate_metrics_from_top_data_line(s, col)
        if c_v is not None:
            st_cpu = c_v if st_cpu is None else max(st_cpu, c_v)
        if m_v is not None:
            st_mem = m_v if st_mem is None else max(st_mem, m_v)
        if r_v is not None:
            st_res = r_v if st_res is None else max(st_res, r_v)
    if st_cpu is not None:
        out["pe_stargate_cpu_pct"] = round(st_cpu, 1)
    if st_mem is not None:
        out["pe_stargate_mem_pct"] = round(st_mem, 1)
    if st_res is not None:
        out["pe_stargate_res_kib"] = int(round(float(st_res)))
    return out


_CVM_TOP_PS_MARKER = "BULK_SNAP_STG"


def _pe_cvm_top_metrics_raw(
    host: str,
    port: int,
    user: str,
    password: str,
    cfg: DiskOpConfig,
) -> Dict[str, Any]:
    ct = min(20.0, float(cfg.guest_ssh_connect_timeout or 30.0))
    top_extra = (os.environ.get("BULK_SNAP_PE_TOP_BATCH_ARGS") or "").strip()
    if top_extra:
        top_extra = " " + top_extra
    # ``ps`` RSS (resident) supplements RES; ``ps`` %cpu only if ``top`` lacks stargate row.
    remote_cmd = (
        f"LC_ALL=C top -bn1{top_extra}; "
        f"printf '\\n{_CVM_TOP_PS_MARKER}\\n'; "
        "LC_ALL=C ps -C stargate -o rss=,%cpu=,%mem= --no-headers 2>/dev/null || true"
    )
    ec, out, err = _guest_sshpass_run(
        host,
        user,
        password,
        remote_cmd,
        port=port,
        connect_timeout=ct,
        command_timeout=25.0,
        cancel_event=None,
    )
    if ec != 0:
        msg = ((err or "").strip() or f"top exit {ec}")[:500]
        return {"pe_cvm_top_err": msg}
    top_text = out or ""
    ps_block = ""
    if _CVM_TOP_PS_MARKER in top_text:
        top_text, ps_block = top_text.split(_CVM_TOP_PS_MARKER, 1)
        ps_block = ps_block.lstrip("\n")
    parsed = _parse_top_bn1_pe_cvm(top_text)
    ps_cpu, ps_mem, ps_rss = _parse_ps_stargate_metrics(ps_block)
    if ps_cpu is not None and parsed.get("pe_stargate_cpu_pct") is None:
        parsed["pe_stargate_cpu_pct"] = round(ps_cpu, 1)
    if ps_mem is not None and parsed.get("pe_stargate_mem_pct") is None:
        parsed["pe_stargate_mem_pct"] = round(ps_mem, 1)
    if ps_rss is not None and parsed.get("pe_stargate_res_kib") is None:
        parsed["pe_stargate_res_kib"] = int(round(float(ps_rss)))
    has_any = any(
        k in parsed
        for k in (
            "pe_cvm_cpu_us_pct",
            "pe_cvm_cpu_wa_pct",
            "pe_stargate_cpu_pct",
            "pe_stargate_mem_pct",
            "pe_stargate_res_kib",
        )
    )
    if not has_any:
        parsed["pe_cvm_top_err"] = "could not parse top (us/wa/stargate)"
    return parsed


def _pe_cvm_top_metrics_cached(pe_ip: str, cfg: DiskOpConfig) -> Dict[str, Any]:
    if os.environ.get("BULK_SNAP_PE_CVM_NO_TOP", "").strip().lower() in ("1", "true", "yes"):
        return {}
    user = (cfg.pe_cvm_ssh_user or "").strip()
    pwd = (cfg.pe_cvm_ssh_password or "").strip()
    if not user or not pwd:
        return {}
    host = _ssh_host_for_socket(pe_ip)
    if not host:
        return {}
    port = max(1, min(int(cfg.pe_cvm_ssh_port or 22), 65535))
    hk = f"{host}:{port}"
    now = time.monotonic()
    with _cvm_top_lock:
        hit = _cvm_top_cache.get(hk)
        if hit and (now - hit[0]) < _CVM_TOP_CACHE_TTL_SEC:
            return dict(hit[1])
    parsed = _pe_cvm_top_metrics_raw(host, port, user, pwd, cfg)
    with _cvm_top_lock:
        _cvm_top_cache[hk] = (now, parsed)
    return dict(parsed)


def _last_valid_ppm_as_percent(values: Any) -> Optional[float]:
    """Latest valid sample from Prism ``cluster/stats`` ``values`` list (ppm → %%). ``-1`` = missing."""
    if not isinstance(values, list) or not values:
        return None
    for raw in reversed(values):
        try:
            n = int(raw)
        except (TypeError, ValueError):
            continue
        if n < 0:
            continue
        return max(0.0, min(100.0, n / 10_000.0))
    return None


def _parse_cluster_stats_block(data: Any, metric_name: str) -> Tuple[Optional[float], str]:
    if not isinstance(data, dict):
        return None, "response is not a JSON object"
    arr = data.get("statsSpecificResponses")
    if not isinstance(arr, list) or not arr:
        return None, "missing statsSpecificResponses"
    block = arr[0]
    if not isinstance(block, dict):
        return None, "invalid stats block"
    if not block.get("successful"):
        msg = block.get("message")
        return None, str(msg or "stats query unsuccessful")
    got_metric = block.get("metric")
    if got_metric and str(got_metric) != metric_name:
        return None, f"unexpected metric {got_metric!r} (wanted {metric_name!r})"
    pct = _last_valid_ppm_as_percent(block.get("values"))
    if pct is None:
        return None, f"no valid samples for {metric_name}"
    return pct, ""


def _pe_cluster_stats_one_metric(
    pe_host: str,
    metric: str,
    interval_secs: int,
    rest_port: int,
    user: str,
    password: str,
    *,
    timeout: float = 25.0,
) -> Tuple[Optional[float], str]:
    host = (pe_host or "").strip()
    if not host:
        return None, "PE host empty"
    port = max(1, min(int(rest_port), 65535))
    url = f"https://{host}:{port}{PE_STATS_PATH}"
    end_u = int(time.time() * 1_000_000)
    span_u = max(int(PE_STATS_HISTORY_SEC) * 1_000_000, int(interval_secs) * 3 * 1_000_000)
    start_u = end_u - span_u
    params = {
        "metrics": metric,
        "startTimeInUsecs": str(start_u),
        "endTimeInUsecs": str(end_u),
        "intervalInSecs": str(max(1, int(interval_secs))),
    }
    try:
        r = requests.get(
            url,
            params=params,
            auth=(user, password),
            verify=TLS_VERIFY,
            timeout=timeout,
        )
    except requests.RequestException as e:
        return None, str(e)
    if r.status_code != 200:
        body = (r.text or "")[:400]
        return None, f"HTTP {r.status_code}: {body or 'no body'}"
    try:
        data = r.json()
    except ValueError:
        return None, "response is not JSON"
    return _parse_cluster_stats_block(data, metric)


def _pe_cluster_stats_snapshot(pe_host: str, cfg: DiskOpConfig) -> Tuple[Optional[float], Optional[float], str]:
    """
    Prism Element ``GET .../cluster/stats`` for hypervisor CPU and aggregate memory (ppm in API; %% here).
    Uses ``pc_user`` / ``pc_password`` (same as Prism Central inventory).
    """
    user = (cfg.pc_user or "").strip()
    pwd = (cfg.pc_password or "").strip()
    if not user or not pwd:
        return None, None, "Prism user/password empty (required for PE cluster stats API)"
    port = int(cfg.pe_prism_rest_port or 9440)
    cpu, err_cpu = _pe_cluster_stats_one_metric(
        pe_host,
        PE_STATS_CPU_METRIC,
        PE_STATS_CPU_INTERVAL_SEC,
        port,
        user,
        pwd,
    )
    mem, err_mem = _pe_cluster_stats_one_metric(
        pe_host,
        PE_STATS_MEM_METRIC,
        PE_STATS_MEM_INTERVAL_SEC,
        port,
        user,
        pwd,
    )
    errs = [x for x in (err_cpu, err_mem) if x]
    if cpu is None or mem is None:
        return None, None, "; ".join(errs) if errs else "could not read CPU/mem stats"
    return cpu, mem, ""


class _ClusterConcurrencyGate:
    """Limit in-flight guest SSH tasks per cluster; effective limit can change (adaptive concurrency)."""

    __slots__ = (
        "baseline",
        "ceiling",
        "_current",
        "_active",
        "_lock",
        "_cond",
        "_ramp_phases",
        "_ramp_phase_idx",
        "_ramp_below_since",
        "_cooldown_until",
        "_cooldown_sec",
        "_prev_overload_sample",
    )

    def __init__(
        self,
        baseline: int,
        ceiling: int,
        ramp_phases: List[Tuple[float, int]],
        *,
        cooldown_sec: float = 300.0,
    ) -> None:
        self.baseline = max(1, baseline)
        self.ceiling = max(self.baseline, max(1, ceiling))
        self._current = self.baseline
        self._active = 0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._ramp_phases = tuple(ramp_phases) if ramp_phases else _DEFAULT_ADAPTIVE_RAMP
        self._ramp_phase_idx = 0
        self._ramp_below_since: Optional[float] = None
        self._cooldown_until: Optional[float] = None
        self._cooldown_sec = max(0.0, float(cooldown_sec))
        self._prev_overload_sample = False

    @property
    def current(self) -> int:
        with self._lock:
            return self._current

    def apply_adaptive(
        self,
        cpu_pct: Optional[float],
        mem_pct: Optional[float],
        cpu_threshold_pct: float,
        mem_pause_threshold_pct: float,
        *,
        prev_cpu_pct: Optional[float] = None,
        spike_delta_pct: float = 0.0,
    ) -> bool:
        """Returns True if caller should sleep ``cluster_adaptive_overload_pause_sec`` (overload edge or spike)."""
        if cpu_pct is None:
            return False
        now = time.monotonic()
        pause_after = False
        with self._cond:
            spike = (
                spike_delta_pct > 0.0
                and prev_cpu_pct is not None
                and (float(cpu_pct) - float(prev_cpu_pct)) >= spike_delta_pct
            )
            cpu_hot = float(cpu_pct) >= float(cpu_threshold_pct)
            mem_hot = (
                float(mem_pause_threshold_pct) > 0.0
                and mem_pct is not None
                and float(mem_pct) >= float(mem_pause_threshold_pct)
            )
            overload_now = cpu_hot or mem_hot
            if overload_now or spike:
                self._current = self.baseline
                self._ramp_phase_idx = 0
                self._ramp_below_since = None
                if self._cooldown_sec > 0.0:
                    self._cooldown_until = now + self._cooldown_sec
                else:
                    self._cooldown_until = None
                edge_overload = overload_now and not self._prev_overload_sample
                pause_after = bool(spike or edge_overload)
                self._prev_overload_sample = overload_now
            else:
                self._prev_overload_sample = False
                if self._cooldown_until is not None and now >= self._cooldown_until:
                    self._cooldown_until = None
                in_cooldown = self._cooldown_until is not None and now < self._cooldown_until
                if in_cooldown:
                    self._ramp_below_since = None
                else:
                    if self._ramp_below_since is None:
                        self._ramp_below_since = now
                    elapsed = now - self._ramp_below_since
                    if self._ramp_phase_idx < len(self._ramp_phases):
                        need_sec, inc = self._ramp_phases[self._ramp_phase_idx]
                        if elapsed >= need_sec:
                            self._current = min(self.ceiling, self._current + max(1, int(inc)))
                            self._ramp_phase_idx += 1
                            self._ramp_below_since = now
            self._cond.notify_all()
        return pause_after

    def acquire(self) -> None:
        with self._cond:
            while self._active >= self._current:
                self._cond.wait()
            self._active += 1

    def release(self) -> None:
        with self._cond:
            self._active -= 1
            self._cond.notify_all()


def _build_cluster_pe_ip_map(sorted_cluster_names: List[str], multiline: str) -> Dict[str, str]:
    lines = [
        ln.strip()
        for ln in (multiline or "").splitlines()
        if ln.strip() and not ln.lstrip().startswith("#")
    ]
    if not lines:
        return {}
    by_name: Dict[str, str] = {}
    plain_ips: List[str] = []
    for ln in lines:
        if "=" in ln:
            k, _, v = ln.partition("=")
            k, v = k.strip(), v.strip()
            if k and v:
                by_name[k] = v
        else:
            plain_ips.append(ln)
    out: Dict[str, str] = {}
    lower_to_key = {k.lower(): k for k in by_name}
    for cn in sorted_cluster_names:
        if cn in by_name:
            out[cn] = by_name[cn]
        elif cn.lower() in lower_to_key:
            bk = lower_to_key[cn.lower()]
            out[cn] = by_name[bk]
    missing = [cn for cn in sorted_cluster_names if cn not in out]
    if plain_ips:
        if len(plain_ips) == len(sorted_cluster_names):
            for cn, ip in zip(sorted_cluster_names, plain_ips):
                out[cn] = ip
        elif missing and len(plain_ips) == len(missing):
            for cn, ip in zip(missing, plain_ips):
                out[cn] = ip
    return out


def _throttle_pe_before_guest_ssh(
    cluster_name: str,
    pe_ip: str,
    cfg: DiskOpConfig,
    log: logging.Logger,
    cancel_event: Optional[Any],
    top_lock: threading.Lock,
    *,
    gate: Optional[_ClusterConcurrencyGate] = None,
    pe_metrics: Optional[Dict[str, Dict[str, Any]]] = None,
    pe_metrics_lock: Optional[threading.Lock] = None,
    last_adaptive_pe_cpu: Optional[Dict[str, Optional[float]]] = None,
    emit_progress: Optional[Callable[[], None]] = None,
) -> None:
    """
    Optionally poll PE Prism ``cluster/stats`` for metrics UI and adaptive concurrency; optionally pause
    guest work when CPU/mem are at or above configured limits.
    """
    if not pe_ip.strip():
        return
    cpu_lim = float(cfg.cluster_cpu_max_pct or 0)
    mem_lim = float(cfg.cluster_mem_max_pct or 0)
    check_cpu = cpu_lim > 0
    check_mem = mem_lim > 0
    need_pause_loop = check_cpu or check_mem
    need_sample = bool(cfg.cluster_pe_top_monitor or cfg.cluster_adaptive_ssh_parallel)
    if not need_sample:
        return

    def _abort() -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise RunCancelled()

    pause = max(1.0, float(cfg.cluster_util_pause_sec or 30.0))
    thr_adapt = float(cfg.cluster_adaptive_cpu_threshold_pct or 90.0)
    mem_ramp_lim = float(cfg.cluster_mem_max_pct or 0)
    spike_d = float(cfg.cluster_adaptive_cpu_spike_delta_pct or 0.0)
    overload_pause = max(0.0, float(cfg.cluster_adaptive_overload_pause_sec or 10.0))

    def _set_cluster_pause(paused: bool, reason: Optional[str]) -> None:
        if pe_metrics is None or pe_metrics_lock is None:
            return
        with pe_metrics_lock:
            cur = dict(pe_metrics.get(cluster_name) or {})
            cur["cluster_paused"] = paused
            cur["cluster_pause_reason"] = reason if paused else None
            pe_metrics[cluster_name] = cur
        if emit_progress is not None:
            emit_progress()

    while True:
        _abort()
        pause_after = False
        top_cvm = _pe_cvm_top_metrics_cached(pe_ip, cfg)
        with top_lock:
            cpu, mem, err = _pe_cluster_stats_snapshot(pe_ip, cfg)
            prev_cpu: Optional[float] = None
            if last_adaptive_pe_cpu is not None:
                prev_cpu = last_adaptive_pe_cpu.get(cluster_name)
            if gate is not None and cfg.cluster_adaptive_ssh_parallel:
                if (
                    spike_d > 0.0
                    and prev_cpu is not None
                    and cpu is not None
                    and not err
                    and (float(cpu) - float(prev_cpu)) >= spike_d
                ):
                    log.info(
                        "Cluster %r: adaptive concurrency reset (CPU jump %.1f%% → %.1f%%, Δ≥%.1f%%).",
                        cluster_name,
                        float(prev_cpu),
                        float(cpu),
                        spike_d,
                    )
                prev_eff = gate.current
                pause_after = gate.apply_adaptive(
                    cpu,
                    mem,
                    thr_adapt,
                    mem_ramp_lim,
                    prev_cpu_pct=prev_cpu,
                    spike_delta_pct=spike_d,
                )
                if not err and gate.current > prev_eff:
                    m_s = "—" if mem is None else f"{float(mem):.1f}"
                    log.info(
                        "Cluster %r: adaptive SSH %d → %d (time-based ramp; CPU %.1f%%, mem %s%%).",
                        cluster_name,
                        prev_eff,
                        gate.current,
                        float(cpu),
                        m_s,
                    )
            if (
                last_adaptive_pe_cpu is not None
                and cpu is not None
                and not err
            ):
                last_adaptive_pe_cpu[cluster_name] = float(cpu)
            row_m: Dict[str, Any] = {
                "pe_cpu_pct": None,
                "pe_mem_pct": None,
                "pe_top_err": err or None,
                "cluster_paused": False,
                "cluster_pause_reason": None,
            }
            if not err:
                if cpu is not None:
                    row_m["pe_cpu_pct"] = round(float(cpu), 1)
                if mem is not None:
                    row_m["pe_mem_pct"] = round(float(mem), 1)
            if gate is not None:
                row_m["guest_ssh_parallel_effective"] = gate.current
                row_m["guest_ssh_parallel_baseline"] = gate.baseline
                row_m["guest_ssh_parallel_ceiling"] = gate.ceiling
            for _k in (
                "pe_cvm_cpu_us_pct",
                "pe_cvm_cpu_wa_pct",
                "pe_stargate_cpu_pct",
                "pe_stargate_mem_pct",
                "pe_stargate_res_kib",
            ):
                if top_cvm.get(_k) is not None:
                    row_m[_k] = top_cvm[_k]
            if top_cvm.get("pe_cvm_top_err"):
                row_m["pe_cvm_top_err"] = top_cvm["pe_cvm_top_err"]
            if pe_metrics is not None and pe_metrics_lock is not None:
                with pe_metrics_lock:
                    pe_metrics[cluster_name] = row_m
        if err:
            log.warning("PE stats cluster=%r pe_ip=%s: %s (continuing guest work)", cluster_name, pe_ip, err)
            return
        if pause_after and cfg.cluster_adaptive_ssh_parallel and overload_pause > 0:
            cd = float(cfg.cluster_adaptive_cooldown_sec or 300.0)
            log.info(
                "Cluster %r: adaptive overload edge or spike — pausing %.0fs (concurrency at baseline; "
                "%.0fs ramp cooldown).",
                cluster_name,
                overload_pause,
                cd,
            )
            _set_cluster_pause(
                True,
                (
                    f"Adaptive overload or CPU spike — pausing {overload_pause:.0f}s "
                    f"(ramp cooldown {cd:.0f}s)"
                ),
            )
            t_end = time.monotonic() + overload_pause
            while time.monotonic() < t_end:
                _abort()
                time.sleep(min(1.0, t_end - time.monotonic()))
            _set_cluster_pause(False, None)
        if not need_pause_loop:
            return
        cpu_s = "—" if cpu is None else f"{cpu:.1f}"
        mem_s = "—" if mem is None else f"{mem:.1f}"
        log.debug(
            "PE stats cluster=%r pe_ip=%s → CPU=%s%% MEM=%s%% (pause if cpu≥%s%% or mem≥%s%%)",
            cluster_name,
            pe_ip,
            cpu_s,
            mem_s,
            f"{cpu_lim:.0f}" if check_cpu else "off",
            f"{mem_lim:.0f}" if check_mem else "off",
        )
        over_cpu = check_cpu and cpu is not None and cpu >= cpu_lim
        over_mem = check_mem and mem is not None and mem >= mem_lim
        if not over_cpu and not over_mem:
            return
        log.debug(
            "PE util cluster=%r over limit (cpu_high=%s mem_high=%s); pausing %.0fs.",
            cluster_name,
            over_cpu,
            over_mem,
            pause,
        )
        r_parts: List[str] = []
        if over_cpu and cpu is not None:
            r_parts.append(f"CPU {float(cpu):.1f}% ≥ throttle {cpu_lim:.0f}%")
        if over_mem and mem is not None:
            r_parts.append(f"mem {float(mem):.1f}% ≥ throttle {mem_lim:.0f}%")
        _set_cluster_pause(
            True,
            "PE stats throttle: " + "; ".join(r_parts) + f" — sleeping {pause:.0f}s",
        )
        time.sleep(pause)


def _guest_ssh_output_snippet(text: str, max_len: int = 220) -> str:
    if not (text or "").strip():
        return "(no output)"
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return "(no output)"
    low_needle = ("denied", "failed", "error", "timed out", "refused", "unreachable", "warning")
    for ln in reversed(lines):
        l = ln.lower()
        if any(n in l for n in low_needle):
            return ln[:max_len]
    return lines[-1][:max_len]


def _guest_ssh_failure_bucket(ec: int, combined: str) -> Tuple[str, str]:
    """
    Roughly classify a failed guest SSH / remote run for end-of-job summaries.

    Returns ``(category, reason_snippet)``. Category is one of:
    ``auth``, ``timeout``, ``refused``, ``unreachable``, ``dns``, ``hostkey``, ``disk_op``, ``other``.
    """
    text = (combined or "").strip()
    low = text.lower()
    snippet = _guest_ssh_output_snippet(text)

    auth_markers = (
        "permission denied",
        "authentication failed",
        "too many authentication failures",
        "login incorrect",
        "access denied",
        "password authentication failed",
    )
    if any(m in low for m in auth_markers):
        return "auth", snippet

    if "connection timed out" in low or "operation timed out" in low:
        return "timeout", snippet
    if "ssh command timed out" in low:
        return "timeout", snippet

    if "connection refused" in low:
        return "refused", snippet

    if "no route to host" in low or "network is unreachable" in low:
        return "unreachable", snippet

    if "could not resolve" in low or "name or service not known" in low:
        return "dns", snippet

    if "host key verification failed" in low:
        return "hostkey", snippet

    if ec == 0:
        return "ok", snippet

    if "=== disk op ===" in text:
        return "disk_op", snippet

    return "other", snippet


def _log_guest_ssh_failure_summary(
    log: logging.Logger,
    failures_by_cat: Dict[str, List[Dict[str, str]]],
) -> None:
    if not failures_by_cat:
        return
    total = sum(len(v) for v in failures_by_cat.values())
    log.info("=== Guest SSH / disk churn failure summary: %d failing VM(s) ===", total)

    labels = {
        "auth": "Authentication / permission denied",
        "timeout": "Connection or SSH timeout",
        "refused": "Connection refused",
        "unreachable": "Network unreachable / no route",
        "dns": "DNS / hostname resolution",
        "hostkey": "Host key verification",
        "disk_op": "Reached guest; disk command failed (non-zero exit)",
        "other": "Other",
    }
    order = ("auth", "timeout", "refused", "unreachable", "dns", "hostkey", "disk_op", "other")

    for cat in order:
        rows = failures_by_cat.get(cat) or []
        if not rows:
            continue
        log.info("-- %s — %d host(s) --", labels.get(cat, cat), len(rows))
        for r in rows:
            log.info(
                "  ip=%s  vm=%r  uuid=%s…  %s",
                r.get("ip", ""),
                r.get("vm_name", ""),
                (r.get("vm_uuid") or "")[:8],
                r.get("reason", ""),
            )


# Single-pass guest pipeline (same as a manual run): stream from /dev/zero through openssl into ``dd``.
_GUEST_OPENSSL_ZERO_PIPE = (
    'openssl enc -aes-256-ctr -pass pass:"testing" -nosalt < /dev/zero 2>/dev/null'
)

# Split-stage mode: encrypt to ``$TMP`` then ``dd`` to target — doubles sequential disk I/O but enables ossl/dd columns.
_GUEST_OPENSSL_ENC_TO_TMP = (
    'openssl enc -aes-256-ctr -pass pass:"testing" -nosalt -out "$TMP"'
)


def _guest_disk_split_stages_for_timing() -> bool:
    return os.environ.get("BULK_SNAP_GUEST_DISK_SPLIT_STAGES", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


_BULK_SNAP_T_RE = re.compile(
    r"BULK_SNAP_T[ \t]+(\S+)[ \t]+(\d+(?:\.\d+)?)",
    re.MULTILINE,
)


def _parse_guest_timing_output(text: str) -> Dict[str, Any]:
    """
    Parse ``BULK_SNAP_T <step> <epoch>`` markers (epoch from ``date +%s.%N`` or ``date +%s``).

    Uses regex so markers still parse if SSH/PTY adds ANSI or CR noise. Returns interval
    seconds between named steps for UI + ``guest_span_sec`` / ``parse_ok``.
    """
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    raw = re.sub(r"\x1b\[[0-9;?]*[0-9A-Za-z]", "", raw)
    marks: List[Tuple[str, float]] = []
    for m in _BULK_SNAP_T_RE.finditer(raw):
        name, ts_s = m.group(1), m.group(2)
        try:
            ts = float(ts_s)
        except ValueError:
            continue
        marks.append((name, ts))
    if len(marks) < 2:
        return {"parse_ok": False}

    def span(a: str, b: str) -> Optional[float]:
        try:
            i0 = next(i for i, (n, _) in enumerate(marks) if n == a)
            i1 = next(i for i, (n, _) in enumerate(marks) if n == b and i > i0)
            return round(marks[i1][1] - marks[i0][1], 3)
        except StopIteration:
            return None

    g0 = marks[0][1]
    g1 = marks[-1][1]
    guest_span = round(g1 - g0, 3) if g1 >= g0 else None

    su_stream = span("guest_start", "openssl_start")
    su_del = span("guest_start", "pre_rm")
    setup_sec = su_stream if su_stream is not None else su_del

    cl_stream = span("dd_done", "guest_end")
    cl_del = span("post_rm", "guest_end")
    cleanup_sec = cl_stream if cl_stream is not None else cl_del

    out: Dict[str, Any] = {
        "parse_ok": True,
        "guest_span_sec": guest_span,
        "setup_sec": setup_sec,
        "mktemp_sec": span("mktemp_done", "openssl_start"),
        "openssl_sec": span("openssl_start", "openssl_done"),
        "dd_sec": span("dd_start", "dd_done"),
        "cleanup_sec": cleanup_sec,
        "rm_sec": span("pre_rm", "post_rm"),
    }
    # Single-pipeline create/add/update (no split stages): openssl|dd bounded by pipeline_* markers.
    if any(n == "pipeline_start" for n, _ in marks) and any(n == "pipeline_done" for n, _ in marks):
        ps = span("guest_start", "pipeline_start")
        pi = span("pipeline_start", "pipeline_done")
        pc = span("pipeline_done", "guest_end")
        if ps is not None:
            out["setup_sec"] = ps
        if pi is not None:
            out["dd_sec"] = pi
        if pc is not None:
            out["cleanup_sec"] = pc
    return out


def _guest_timed_remote_body(op: str, cfg: DiskOpConfig) -> str:
    """
    Bash script body (for ``bash -lc``) with ``BULK_SNAP_T`` timing lines.

    **Default:** create/add/update use one **openssl | dd** pipeline (same I/O as a manual shell test).
    **Optional:** set ``BULK_SNAP_GUEST_DISK_SPLIT_STAGES=1`` to encrypt to a temp file then ``dd`` to the
    target (about **2×** sequential disk work, but populates ossl / dd timing columns).
    """
    _bs = (
        "_bs(){ printf 'BULK_SNAP_T %s %s\\n' \"$1\" "
        "\"$(date +%s.%N 2>/dev/null || date +%s)\"; }; "
    )
    if op == DISK_DELETE:
        g = cfg.guest_delete_glob.replace('"', "")
        return (
            "set -e; "
            + _bs
            + "_bs guest_start; "
            + "_bs pre_rm; "
            + f"rm -f {g}; "
            + "_bs post_rm; "
            + "_bs pre_fstrim; "
            + "fstrim -v / 2>&1 || echo 'fstrim not available or failed'; "
            + "_bs post_fstrim; "
            + "_bs guest_end; "
            "exit 0"
        )

    of = shlex.quote(cfg.guest_target_file)
    if op == DISK_CREATE:
        n = max(1, int(cfg.create_count_mib))
    elif op == DISK_ADD:
        n = max(1, int(cfg.churn_count_mib))
    elif op == DISK_UPDATE:
        n = max(1, int(cfg.churn_count_mib))
    else:
        raise ValueError(f"Unknown op {op!r}")

    bs_arg = shlex.quote(cfg.guest_dd_bs)

    if not _guest_disk_split_stages_for_timing():
        if op == DISK_CREATE:
            inner = (
                f"{_GUEST_OPENSSL_ZERO_PIPE} | "
                f"dd of={of} bs={bs_arg} count={n} iflag=fullblock status=progress"
            )
        elif op == DISK_ADD:
            inner = (
                f"{_GUEST_OPENSSL_ZERO_PIPE} | "
                f"dd of={of} bs={bs_arg} count={n} iflag=fullblock oflag=append "
                "conv=notrunc status=progress"
            )
        else:
            inner = (
                f"{_GUEST_OPENSSL_ZERO_PIPE} | "
                f"dd of={of} bs={bs_arg} count={n} iflag=fullblock conv=notrunc status=progress"
            )
        # No pipefail: ``openssl | dd`` stops ``dd`` after ``count=N``; ``openssl`` then gets
        # SIGPIPE — with pipefail the pipeline is treated as failed even when ``dd`` succeeded.
        # ``pipeline_start`` / ``pipeline_done`` bound the full openssl|dd span for UI (dd_sec, etc.).
        return (
            "set -e; "
            + _bs
            + "_bs guest_start; "
            + "_bs pipeline_start; "
            + inner
            + "; ec=$?; "
            + "_bs pipeline_done; "
            + "_bs guest_end; "
            "exit $ec"
        )

    out_assign = f"OUT={shlex.quote(cfg.guest_target_file)}"
    if op == DISK_CREATE:
        dd_tail = (
            'dd if="$TMP" of="$OUT" bs=$BS count=$N iflag=fullblock status=progress'
        )
    elif op == DISK_ADD:
        dd_tail = (
            'dd if="$TMP" of="$OUT" bs=$BS count=$N iflag=fullblock oflag=append '
            "conv=notrunc status=progress"
        )
    else:
        dd_tail = (
            'dd if="$TMP" of="$OUT" bs=$BS count=$N iflag=fullblock conv=notrunc status=progress'
        )

    return (
        "set -eo pipefail; "
        + _bs
        + f"{out_assign}; N={n}; BS={bs_arg}; "
        + "_bs guest_start; "
        + "TMP=$(mktemp /tmp/bulk_snap_XXXXXX); "
        + "_bs mktemp_done; "
        + "_bs openssl_start; "
        + "dd if=/dev/zero bs=$BS count=$N status=none 2>/dev/null | "
        + _GUEST_OPENSSL_ENC_TO_TMP
        + "; "
        + "_bs openssl_done; "
        + "_bs dd_start; "
        + dd_tail
        + "; ec=$?; "
        + "_bs dd_done; "
        + "rm -f \"$TMP\"; "
        + "_bs guest_end; "
        + "exit $ec"
    )


def _remote_shell_line(op: str, cfg: DiskOpConfig) -> str:
    """
    Remote argv for ``ssh`` — ``bash -lc`` running the timed guest disk script.

    Exit status is the disk op’s status (final ``dd`` or ``rm``).
    """
    body = _guest_timed_remote_body(op, cfg)
    return "bash -lc " + shlex.quote(body)


def _resolve_op(mode: str, rng: random.Random) -> str:
    if mode != DISK_RANDOM_MIX:
        return mode
    return rng.choice([DISK_CREATE, DISK_ADD, DISK_UPDATE, DISK_DELETE])


def preview_guest_disk_targets(
    cfg: DiskOpConfig,
    rows: Optional[List[Dict[str, Any]]] = None,
    duplicate_inventory_rows: int = 0,
    *,
    from_cache: bool = False,
) -> Dict[str, Any]:
    """
    Count VMs eligible for guest disk churn (name rules, IP, powered on, RAM floor). No SSH.

    Pass ``rows`` to skip Prism refetch (e.g. inventory cache from index page load).
    """
    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    cfg.compile_regexes()
    base = cfg.base_url.rstrip("/")
    snap_cfg = cfg.to_snapshot_cfg()
    snap_cfg.compile_regexes()

    dup_rows = duplicate_inventory_rows
    if rows is None:
        session = requests.Session()
        session.auth = (cfg.pc_user, cfg.pc_password)
        session.headers["Content-Type"] = "application/json"
        rows, dup_rows = fetch_vm_inventory_rows(session, base, page_size=cfg.group_member_page)

    worklist, counts = build_guest_disk_worklist(
        rows,
        snap_cfg,
        min_memory_mib=cfg.guest_min_memory_mib,
    )
    eligible = len(worklist)

    try:
        if cfg.parallel_clusters:
            _candidates, pmeta = partition_guest_disk_worklist_by_cluster(worklist, cfg)
            planned = len(_candidates)
        else:
            pmeta = {}
            planned = resolve_disk_run_limit(cfg.disk_run_limit, eligible)
    except ValueError as e:
        return {"ok": False, "message": str(e)}

    out: Dict[str, Any] = {
        "ok": True,
        "inventory_distinct_vms": counts["inventory_distinct_vms"],
        "eligible_for_guest_ssh": eligible,
        "skipped_by_name_rules": counts["ignored_name"],
        "no_ip_in_pc_inventory": counts["skipped_no_ip"],
        "skipped_powered_off": counts["skipped_power_off"],
        "skipped_below_min_memory": counts["skipped_below_min_memory"],
        "guest_min_memory_mib": int(cfg.guest_min_memory_mib or 0),
        "duplicate_inventory_rows": dup_rows,
        "disk_run_limit": (cfg.disk_run_limit or "").strip(),
        "planned_guest_ssh_runs": planned,
        "inventory_from_cache": from_cache,
        "guest_ssh_parallel": max(1, min(int(cfg.guest_ssh_parallel or 10), 500)),
        "parallel_clusters": bool(cfg.parallel_clusters),
        "vm_per_cluster": int(cfg.vm_per_cluster or 0),
        "cluster_pe_top_monitor": bool(cfg.cluster_pe_top_monitor),
        "cluster_cpu_max_pct": float(cfg.cluster_cpu_max_pct or 0),
        "cluster_mem_max_pct": float(cfg.cluster_mem_max_pct or 0),
        "cluster_adaptive_ssh_parallel": bool(cfg.cluster_adaptive_ssh_parallel),
        "cluster_adaptive_cpu_threshold_pct": float(cfg.cluster_adaptive_cpu_threshold_pct or 90.0),
        "cluster_adaptive_ramp": (cfg.cluster_adaptive_ramp or "180/5,300/3").strip(),
        "cluster_adaptive_ssh_step": int(cfg.cluster_adaptive_ssh_step or 2),
        "cluster_adaptive_ssh_ceiling": int(cfg.cluster_adaptive_ssh_ceiling or 0),
        "cluster_adaptive_cpu_spike_delta_pct": float(cfg.cluster_adaptive_cpu_spike_delta_pct or 0.0),
        "cluster_adaptive_overload_pause_sec": float(cfg.cluster_adaptive_overload_pause_sec or 10.0),
        "cluster_adaptive_cooldown_sec": float(cfg.cluster_adaptive_cooldown_sec or 300.0),
        "pe_prism_rest_port": int(cfg.pe_prism_rest_port or 9440),
    }
    if cfg.parallel_clusters:
        out["vm_per_cluster_cap"] = pmeta.get("vm_per_cluster_cap")
        out["clusters_in_inventory"] = pmeta.get("clusters_in_inventory")
        out["per_cluster_planned"] = pmeta.get("per_cluster_planned") or {}
        if (cfg.cluster_pe_top_monitor or cfg.cluster_adaptive_ssh_parallel) and pmeta.get(
            "per_cluster_planned"
        ):
            out["pe_top_cluster_order"] = sorted(
                (pmeta.get("per_cluster_planned") or {}).keys(),
                key=lambda x: (x == "—", x),
            )
    return out


_DISK_METRICS_TIMELINE_MAX = 64
_DISK_METRICS_TIMELINE_MIN_INTERVAL_SEC = 12.0


def _mean_float(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    return sum(xs) / float(len(xs))


def _fallback_avg_wall_per_vm_cluster(completed: List[Dict[str, Any]], cluster: str) -> Optional[float]:
    vals: List[float] = []
    for r in completed:
        if str(r.get("cluster") or "") != str(cluster):
            continue
        try:
            vals.append(float(r["seconds"]))
        except (KeyError, TypeError, ValueError):
            continue
    return _mean_float(vals)


def _fallback_avg_wall_per_vm_all(completed: List[Dict[str, Any]]) -> Optional[float]:
    vals: List[float] = []
    for r in completed:
        try:
            vals.append(float(r["seconds"]))
        except (KeyError, TypeError, ValueError):
            continue
    return _mean_float(vals)


def _eta_for_progress_row(
    row: Dict[str, Any],
    cluster_key: str,
    completed: List[Dict[str, Any]],
) -> Tuple[Optional[float], Optional[float]]:
    """Rough remaining wall for this shard.

    When ``done > 0``, the row's ``avg_wall_sec_per_vm`` is ``cluster_wall_sec / done`` — seconds of
    shard wall clock per completion — which **already** reflects parallel guest SSH. Throughput is
    ``~ done / wall`` completions/s, so remaining time ``≈ pending / (done/wall) = pending * avg``.
    Multiplying by ``(pending / SSH slots)`` would incorrectly divide by parallelism twice.

    When ``done == 0``, fall back to ``(pending / SSH slots) *`` mean per-VM duration from completed
    VMs (no shard throughput sample yet).
    """
    pending = int(row.get("pending") or 0)
    if pending <= 0:
        return 0.0, None
    done = int(row.get("done") or 0)
    eff = max(1, int(row.get("guest_ssh_parallel_effective") or 1))

    avg_shard: Optional[float] = None
    if done > 0 and row.get("avg_wall_sec_per_vm") is not None:
        try:
            avg_shard = float(row["avg_wall_sec_per_vm"])
        except (TypeError, ValueError):
            avg_shard = None
    if avg_shard is not None and avg_shard >= 0.0 and done > 0:
        basis = round(float(avg_shard), 2)
        return round(float(pending) * float(avg_shard), 1), basis

    avg_fb: Optional[float] = None
    if cluster_key == "_all":
        avg_fb = _fallback_avg_wall_per_vm_all(completed)
    else:
        avg_fb = _fallback_avg_wall_per_vm_cluster(completed, cluster_key)
    if avg_fb is None or avg_fb < 0:
        return None, None
    basis = round(float(avg_fb), 2)
    return round((float(pending) / float(eff)) * float(avg_fb), 1), basis


_PE_VM_SNAPSHOT_KEYS = (
    "pe_cpu_pct",
    "pe_mem_pct",
    "pe_cvm_cpu_us_pct",
    "pe_cvm_cpu_wa_pct",
    "pe_stargate_cpu_pct",
    "pe_stargate_mem_pct",
    "pe_stargate_res_kib",
    "guest_ssh_parallel_effective",
    "cluster_paused",
)


def _pe_metrics_row_snapshot(
    pe_metrics: Optional[Dict[str, Dict[str, Any]]],
    cluster: str,
    lock: threading.Lock,
) -> Dict[str, Any]:
    """Copy live PE / CVM / stargate fields from the shard row (same source as Disk job progress)."""
    if not pe_metrics:
        return {}
    ck = str(cluster or "")
    with lock:
        row = pe_metrics.get(ck)
        if not row:
            return {}
        out: Dict[str, Any] = {}
        for k in _PE_VM_SNAPSHOT_KEYS:
            if k in row and row[k] is not None:
                out[k] = row[k]
        for ek in ("pe_cvm_top_err", "pe_top_err"):
            if row.get(ek):
                out[ek] = str(row[ek])[:500]
        return dict(out)


def run_disk_ops(
    cfg: DiskOpConfig,
    log: logging.Logger,
    cancel_event: Optional[Any] = None,
    rows: Optional[List[Dict[str, Any]]] = None,
    duplicate_inventory_rows: int = 0,
    *,
    from_cache: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    cfg.compile_regexes()
    base = cfg.base_url.rstrip("/")
    mode = (cfg.mode or DISK_UPDATE).strip().lower()
    if mode not in ALL_MODES:
        raise ValueError(f"Invalid mode {mode!r}; expected one of {sorted(ALL_MODES)}")

    if not (cfg.guest_ssh_password or "").strip():
        raise ValueError("Guest SSH password is empty (sshpass requires a password).")

    rng = random.Random(cfg.random_seed)

    session = requests.Session()
    session.auth = (cfg.pc_user, cfg.pc_password)
    session.headers["Content-Type"] = "application/json"

    snap_cfg = cfg.to_snapshot_cfg()
    snap_cfg.compile_regexes()

    def _abort() -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise RunCancelled()

    _abort()

    dup_rows = duplicate_inventory_rows
    if rows is None:
        _abort()
        log.debug("Fetching VM inventory from Prism (guest disk churn, mode=%s)…", mode)
        rows, dup_rows = fetch_vm_inventory_rows(session, base, page_size=cfg.group_member_page)
    else:
        log.debug(
            "Using cached VM inventory (%d rows, %s); guest disk churn mode=%s.",
            len(rows),
            "no Prism refetch" if from_cache else "caller-supplied",
            mode,
        )
    _abort()

    worklist, counts = build_guest_disk_worklist(
        rows,
        snap_cfg,
        min_memory_mib=cfg.guest_min_memory_mib,
    )
    ignored_name = counts["ignored_name"]
    skipped_no_ip = counts["skipped_no_ip"]
    skipped_power_off = counts["skipped_power_off"]
    skipped_below_min_memory = counts["skipped_below_min_memory"]

    eligible_total = len(worklist)
    try:
        if cfg.parallel_clusters:
            candidates, pc_meta = partition_guest_disk_worklist_by_cluster(worklist, cfg)
            n_run = len(candidates)
        else:
            pc_meta = {}
            n_run = resolve_disk_run_limit(cfg.disk_run_limit, eligible_total)
            candidates = worklist[:n_run]
    except ValueError as e:
        raise ValueError(str(e)) from e

    floor = int(cfg.guest_min_memory_mib or 0)
    ram_elig = f" + configured RAM > {floor} MiB" if floor > 0 else ""
    log.debug(
        "Eligible for guest disk ops (IP + powered on + name rules%s): %d "
        "(VMs in inventory: %d, name-skip: %d, no IP: %d, powered off: %d, RAM≤%s/unknown: %d, dup rows: %d).",
        ram_elig,
        eligible_total,
        len(rows),
        ignored_name,
        skipped_no_ip,
        skipped_power_off,
        f"{floor} MiB" if floor > 0 else "n/a",
        skipped_below_min_memory,
        dup_rows,
    )
    lim = (cfg.disk_run_limit or "").strip()
    parallel = max(1, min(int(cfg.guest_ssh_parallel or 10), 500))
    if cfg.parallel_clusters:
        cap = pc_meta.get("vm_per_cluster_cap")
        cap_s = "all eligible per cluster" if cap is None else f"at most {cap} VM(s) per cluster"
        log.debug(
            "Parallel-by-cluster: %s; %s cluster name(s) in inventory; "
            "up to %d concurrent guest SSH session(s) per cluster; %d total VM run(s) planned.",
            cap_s,
            pc_meta.get("clusters_in_inventory"),
            parallel,
            n_run,
        )
        if cfg.cluster_pe_top_monitor:
            log.debug(
                "PE stats monitor: pause guest work per cluster when cluster/stats shows CPU≥%.0f%% or mem≥%.0f%% "
                "(0=off per metric); retry up to %.0fs in %.0fs steps (Prism user=%s, PE :%s).",
                float(cfg.cluster_cpu_max_pct or 0),
                float(cfg.cluster_mem_max_pct or 0),
                float(cfg.cluster_util_max_retry_sec or 1800.0),
                float(cfg.cluster_util_pause_sec or 30.0),
                (cfg.pc_user or "?"),
                int(cfg.pe_prism_rest_port or 9440),
            )
    elif lim and lim.lower() not in ("all", "100%", "max", "*"):
        log.debug(
            "Will run guest SSH on %d of %d eligible VMs (disk_run_limit=%r).",
            n_run,
            eligible_total,
            lim,
        )
    else:
        log.debug(
            "Will run guest SSH on all %d eligible VMs (no limit).",
            n_run,
        )
    log.info(
        "Disk job: %d guest VM(s) to process (parallel-by-cluster=%s, concurrent SSH per shard=%d).",
        n_run,
        cfg.parallel_clusters,
        parallel,
    )
    adaptive_ssh = bool(cfg.cluster_adaptive_ssh_parallel)
    if adaptive_ssh:
        ceil_h = int(cfg.cluster_adaptive_ssh_ceiling or 0)
        if ceil_h > 0:
            ceil_s = str(ceil_h)
        else:
            ceil_s = f"max({parallel}, {ADAPTIVE_GUEST_SSH_AUTO_CEILING})"
        spike_d = float(cfg.cluster_adaptive_cpu_spike_delta_pct or 0.0)
        phases = _parse_adaptive_ramp_schedule(cfg.cluster_adaptive_ramp)
        ramp_desc = ", ".join(f"{int(s)}s→+{i}" for s, i in phases)
        mem_thr = float(cfg.cluster_mem_max_pct or 0)
        mem_tail = f" and mem < {mem_thr:.0f}%%" if mem_thr > 0.0 else ""
        log.info(
            "Adaptive per-cluster guest SSH: ramp [%s]; grow only while CPU < %.0f%%%s — "
            "reset to %d on overload or spike; overload edge/spike pauses %.0fs; ramp frozen %.0fs after "
            "reset; ceiling %s.%s",
            ramp_desc,
            float(cfg.cluster_adaptive_cpu_threshold_pct or 90.0),
            mem_tail,
            parallel,
            float(cfg.cluster_adaptive_overload_pause_sec or 10.0),
            float(cfg.cluster_adaptive_cooldown_sec or 300.0),
            ceil_s,
            (
                f" Spike guard: CPU Δ≥{spike_d:.0f}%% vs prior sample resets."
                if spike_d > 0.0
                else ""
            ),
        )

    jobs_by_cluster: Dict[str, List[Tuple[int, str, str, str, str, str]]] = defaultdict(list)
    shard_keys_sorted: List[str] = []
    if cfg.parallel_clusters:
        for i, (vm_uuid, vm_name, guest_ip, cname) in enumerate(candidates):
            _abort()
            op = _resolve_op(mode, rng) if mode == DISK_RANDOM_MIX else mode
            cn = str(cname or "—").strip() or "—"
            jobs_by_cluster[cn].append((i, vm_uuid, vm_name or "", guest_ip, op, cn))
        shard_keys_sorted = sorted(jobs_by_cluster.keys(), key=lambda x: (x == "—", x))

    progress_lock = threading.Lock()
    progress_by_cluster: Dict[str, Dict[str, int]] = {}
    cluster_timing_lock = threading.Lock()
    cluster_timing: Dict[str, Dict[str, Optional[float]]] = {}
    pe_metrics_lock = threading.Lock()
    pe_metrics_by_cluster: Dict[str, Dict[str, Any]] = {}
    cluster_gates: Dict[str, _ClusterConcurrencyGate] = {}
    last_adaptive_pe_cpu: Dict[str, Optional[float]] = {}
    vm_activity_lock = threading.Lock()
    vm_inflight: Dict[str, Dict[str, Any]] = {}
    vm_completed: List[Dict[str, Any]] = []
    _vm_activity_completed_cap = 8000
    _metrics_timeline: List[Dict[str, Any]] = []
    _tl_state: Dict[str, Any] = {"last_mono": 0.0, "last_done": -1}
    if cfg.parallel_clusters:
        for ck in shard_keys_sorted:
            progress_by_cluster[ck] = {
                "total": len(jobs_by_cluster[ck]),
                "done": 0,
                "ok": 0,
                "fail": 0,
            }
    else:
        progress_by_cluster["_all"] = {
            "total": n_run,
            "done": 0,
            "ok": 0,
            "fail": 0,
        }

    def _snapshot_disk_progress() -> Dict[str, Any]:
        by_c: Dict[str, Any] = {}
        overall_done = 0
        for k, st in progress_by_cluster.items():
            tot = int(st["total"])
            done = int(st["done"])
            overall_done += done
            row: Dict[str, Any] = {
                "total": tot,
                "done": done,
                "pending": max(0, tot - done),
                "ok": int(st["ok"]),
                "fail": int(st["fail"]),
            }
            with cluster_timing_lock:
                ct = cluster_timing.get(k)
            if ct and ct.get("t0") is not None:
                t0 = float(ct["t0"])
                dur = ct.get("duration_sec")
                if dur is not None:
                    wall = float(dur)
                else:
                    wall = time.perf_counter() - t0
                row["cluster_wall_sec"] = round(wall, 1)
                if done > 0:
                    if done >= tot:
                        row["avg_wall_sec_per_vm"] = round(wall / tot, 2)
                    else:
                        row["avg_wall_sec_per_vm"] = round(wall / done, 2)
                        row["avg_wall_inflight"] = True
            with pe_metrics_lock:
                pm = pe_metrics_by_cluster.get(k)
            if pm:
                if pm.get("pe_cpu_pct") is not None:
                    row["pe_cpu_pct"] = pm["pe_cpu_pct"]
                for _pk in (
                    "pe_cvm_cpu_us_pct",
                    "pe_cvm_cpu_wa_pct",
                    "pe_stargate_cpu_pct",
                    "pe_stargate_mem_pct",
                    "pe_stargate_res_kib",
                ):
                    if pm.get(_pk) is not None:
                        row[_pk] = pm[_pk]
                if pm.get("pe_cvm_top_err"):
                    row["pe_cvm_top_err"] = str(pm["pe_cvm_top_err"])
                row["cluster_paused"] = bool(pm.get("cluster_paused"))
                if row["cluster_paused"] and pm.get("cluster_pause_reason"):
                    row["cluster_pause_reason"] = str(pm["cluster_pause_reason"])
            cg = cluster_gates.get(k)
            if cg is not None:
                row["guest_ssh_parallel_effective"] = cg.current
                row["guest_ssh_parallel_baseline"] = cg.baseline
                row["guest_ssh_parallel_ceiling"] = cg.ceiling
                row["cluster_adaptive_ssh_parallel"] = True
            else:
                cap_pf = max(1, min(parallel, tot))
                row["guest_ssh_parallel_effective"] = cap_pf
                row["guest_ssh_parallel_baseline"] = cap_pf
            by_c[k] = row
        if not cfg.parallel_clusters and "_all" in by_c and pe_metrics_by_cluster:
            ra = by_c["_all"]
            cpus: List[float] = []
            paused_any = False
            reasons: List[str] = []
            for _pk, pm in pe_metrics_by_cluster.items():
                v = pm.get("pe_cpu_pct")
                if v is not None:
                    try:
                        cpus.append(float(v))
                    except (TypeError, ValueError):
                        pass
                if pm.get("cluster_paused"):
                    paused_any = True
                    pr = pm.get("cluster_pause_reason")
                    if pr:
                        reasons.append(str(pr))
            if cpus:
                ra["pe_cpu_pct"] = round(max(cpus), 1)
            for _mk in (
                "pe_cvm_cpu_us_pct",
                "pe_cvm_cpu_wa_pct",
                "pe_stargate_cpu_pct",
                "pe_stargate_mem_pct",
            ):
                acc: List[float] = []
                for _pk, pm in pe_metrics_by_cluster.items():
                    v = pm.get(_mk)
                    if v is not None:
                        try:
                            acc.append(float(v))
                        except (TypeError, ValueError):
                            pass
                if acc:
                    ra[_mk] = round(max(acc), 1)
            acc_r: List[float] = []
            for _pk, pm in pe_metrics_by_cluster.items():
                v = pm.get("pe_stargate_res_kib")
                if v is not None:
                    try:
                        acc_r.append(float(v))
                    except (TypeError, ValueError):
                        pass
            if acc_r:
                ra["pe_stargate_res_kib"] = int(round(max(acc_r)))
            for _pk, pm in pe_metrics_by_cluster.items():
                te = pm.get("pe_cvm_top_err")
                if te:
                    ra["pe_cvm_top_err"] = str(te)
                    break
            if paused_any:
                ra["cluster_paused"] = True
                ra["cluster_pause_reason"] = reasons[0] if reasons else "Paused"
            if cluster_gates:
                ra["guest_ssh_parallel_effective"] = max(g.current for g in cluster_gates.values())
                ra["guest_ssh_parallel_baseline"] = min(g.baseline for g in cluster_gates.values())
                ra["guest_ssh_parallel_ceiling"] = max(g.ceiling for g in cluster_gates.values())
                ra["cluster_adaptive_ssh_parallel"] = adaptive_ssh
        with vm_activity_lock:
            running: List[Dict[str, Any]] = []
            for rec in vm_inflight.values():
                t0v = float(rec["t0"])
                running.append(
                    {
                        "vm_name": rec["vm_name"],
                        "guest_ip": rec.get("guest_ip") or "",
                        "cluster": rec["cluster"],
                        "op": rec.get("op") or "",
                        "state": "running",
                        "seconds": round(time.perf_counter() - t0v, 1),
                        "pe_cluster_before": rec.get("pe_cluster_before") or {},
                    }
                )
            running.sort(key=lambda r: (str(r["cluster"]), str(r["vm_name"]).lower()))
            completed_copy = list(vm_completed)
        pred: Dict[str, Any] = {
            "eta_rem_sec_by_cluster": {},
            "eta_rem_sec_total": None,
            "avg_wall_sec_basis": {},
            "completed_vm_count": len(completed_copy),
            # Mean of each finished VM's own ``seconds`` (SSH+guest); larger than Avg s/VM when many
            # guests run in parallel — ETA uses shard Avg s/VM × pending, not mean per-VM × pending/SSH.
            "mean_wall_sec_per_completed_vm": None,
            "note": "ETA per cluster with progress: pending × (shard wall ÷ done) = pending × Avg s/VM; "
            "Avg s/VM already includes parallel SSH throughput (do not divide by slots again). "
            "With done=0, fallback uses (pending ÷ SSH) × mean per-VM time from finished VMs. "
            "Multi-cluster parallel job total ETA ≈ max(shard ETAs). Throttle/adaptive SSH can stretch real time.",
        }
        _secs_done: List[float] = []
        for r in completed_copy:
            try:
                _secs_done.append(float(r["seconds"]))
            except (KeyError, TypeError, ValueError):
                pass
        if _secs_done:
            pred["mean_wall_sec_per_completed_vm"] = round(
                sum(_secs_done) / float(len(_secs_done)), 2
            )
        etas_nonneg: List[float] = []
        any_pending_no_eta = False
        for ck, row in by_c.items():
            eta, basis = _eta_for_progress_row(row, ck, completed_copy)
            pred["eta_rem_sec_by_cluster"][ck] = eta
            if basis is not None:
                pred["avg_wall_sec_basis"][ck] = round(float(basis), 2)
            if int(row.get("pending") or 0) > 0 and eta is None:
                any_pending_no_eta = True
            if eta is not None:
                etas_nonneg.append(float(eta))
        if cfg.parallel_clusters:
            pred["eta_rem_sec_total"] = (
                None
                if any_pending_no_eta
                else (round(max(etas_nonneg), 1) if etas_nonneg else None)
            )
        else:
            pred["eta_rem_sec_total"] = pred["eta_rem_sec_by_cluster"].get("_all")
            if any_pending_no_eta:
                pred["eta_rem_sec_total"] = None

        now_m = time.monotonic()
        if (
            now_m - float(_tl_state["last_mono"]) >= _DISK_METRICS_TIMELINE_MIN_INTERVAL_SEC
            or int(overall_done) != int(_tl_state["last_done"])
        ):
            _tl_state["last_mono"] = now_m
            _tl_state["last_done"] = int(overall_done)
            slim_bc: Dict[str, Any] = {}
            for ck, rw in by_c.items():
                ent: Dict[str, Any] = {
                    "done": rw.get("done"),
                    "pending": rw.get("pending"),
                }
                for _key in (
                    "pe_cpu_pct",
                    "pe_cvm_cpu_us_pct",
                    "pe_cvm_cpu_wa_pct",
                    "pe_stargate_cpu_pct",
                    "pe_stargate_mem_pct",
                    "pe_stargate_res_kib",
                    "guest_ssh_parallel_effective",
                ):
                    if rw.get(_key) is not None:
                        ent[_key] = rw[_key]
                slim_bc[ck] = ent
            _metrics_timeline.append(
                {
                    "t_monotonic_sec": round(now_m, 2),
                    "overall_done": int(overall_done),
                    "by_cluster": slim_bc,
                }
            )
            while len(_metrics_timeline) > _DISK_METRICS_TIMELINE_MAX:
                del _metrics_timeline[0]

        return {
            "overall_total": n_run,
            "overall_done": overall_done,
            "overall_pending": max(0, n_run - overall_done),
            "by_cluster": by_c,
            "parallel_clusters": bool(cfg.parallel_clusters),
            "disk_adaptive_ssh_parallel": adaptive_ssh,
            "guest_ssh_parallel_config": parallel,
            "vm_activity": {"running": running, "completed": completed_copy},
            "prediction": pred,
            "metrics_timeline": list(_metrics_timeline),
        }

    def _emit_disk_progress() -> None:
        if progress_callback:
            progress_callback(_snapshot_disk_progress())

    _emit_disk_progress()

    failure_lock = threading.Lock()
    failures_by_cat: Dict[str, List[Dict[str, str]]] = {}

    def _note_ssh_failure(cat: str, ip: str, name: str, uuid: str, reason: str) -> None:
        if cat == "ok":
            return
        rec = {
            "ip": ip,
            "vm_name": name or "?",
            "vm_uuid": uuid or "",
            "reason": reason or "",
        }
        with failure_lock:
            failures_by_cat.setdefault(cat, []).append(rec)

    def _ssh_task(
        args: Tuple[Any, ...],
        cluster_tag: Optional[str] = None,
        *,
        pe_before: Optional[Dict[str, Any]] = None,
    ) -> int:
        if len(args) == 6:
            i, vm_uuid, vm_name, guest_ip, op, cname = args
            tag = cluster_tag if cluster_tag is not None else str(cname or "_all")
        else:
            i, vm_uuid, vm_name, guest_ip, op = args  # type: ignore[misc]
            tag = cluster_tag if cluster_tag is not None else "_all"
        _abort()
        act_key = f"{i}:{vm_uuid}"
        t_vm0 = time.perf_counter()
        pe_b = dict(pe_before) if pe_before else {}
        with vm_activity_lock:
            vm_inflight[act_key] = {
                "vm_name": vm_name or "?",
                "guest_ip": guest_ip or "",
                "cluster": tag,
                "op": op,
                "t0": t_vm0,
                "pe_cluster_before": pe_b,
            }
        _emit_disk_progress()

        ec = -1
        combined = ""
        try:
            remote = _remote_shell_line(op, cfg)
            log.debug(
                "[%d/%d] VM %s… (%s) via %s op=%s",
                i + 1,
                n_run,
                vm_uuid[:8],
                vm_name or "?",
                guest_ip,
                op,
            )
            log.debug("  remote: %s", remote)

            ec, out, err = _guest_sshpass_run(
                guest_ip,
                cfg.guest_ssh_user,
                cfg.guest_ssh_password,
                remote,
                port=cfg.guest_ssh_port,
                connect_timeout=cfg.guest_ssh_connect_timeout,
                command_timeout=cfg.guest_ssh_command_timeout,
                cancel_event=cancel_event,
            )
            combined = ((out or "") + (err or "")).rstrip()
            if combined:
                for line in combined.splitlines():
                    log.debug("  %s", line.rstrip("\r"))
            if ec == 0:
                log.debug("  OK (disk op exit 0)")
            else:
                log.error(
                    "Guest disk op failed ec=%s vm=%s ip=%s — %s",
                    ec,
                    vm_name or "?",
                    guest_ip,
                    _guest_ssh_output_snippet(combined, max_len=400),
                )
                bucket, reason_snip = _guest_ssh_failure_bucket(ec, combined)
                _note_ssh_failure(bucket, guest_ip, vm_name, vm_uuid, reason_snip)
        except RunCancelled:
            with vm_activity_lock:
                vm_inflight.pop(act_key, None)
            _emit_disk_progress()
            raise
        except Exception:
            log.exception("Guest disk op crashed vm=%s ip=%s", vm_name or "?", guest_ip)
            ec = 255

        dur_vm = time.perf_counter() - t_vm0
        guest_timing = _parse_guest_timing_output(combined)
        ssh_overhead_sec: Optional[float] = None
        guest_cmd_sec: Optional[float] = None
        if guest_timing.get("parse_ok") and guest_timing.get("guest_span_sec") is not None:
            gs = float(guest_timing["guest_span_sec"])
            guest_timing["overhead_sec"] = round(max(0.0, float(dur_vm) - gs), 3)
            ssh_overhead_sec = float(guest_timing["overhead_sec"])
            guest_cmd_sec = float(gs)
        else:
            guest_timing["overhead_sec"] = None
        if dur_vm >= 30.0:
            log.error(
                "⚠️  SLOW VM: Guest disk op wall %.1fs (vm=%r ip=%s op=%s ec=%s) — EXCEEDED 30s THRESHOLD! "
                "includes SSH + guest disk command; if still high, check guest disk throughput vs. MiB settings.",
                dur_vm,
                vm_name or "?",
                guest_ip,
                op,
                ec,
            )
        elif dur_vm >= 20.0:
            log.warning(
                "Guest disk op wall %.1fs (vm=%r ip=%s op=%s ec=%s) — includes SSH + "
                "guest disk command; if still high, check guest disk throughput vs. MiB settings.",
                dur_vm,
                vm_name or "?",
                guest_ip,
                op,
                ec,
            )
        pe_after = _pe_metrics_row_snapshot(pe_metrics_by_cluster, tag, pe_metrics_lock)
        with vm_activity_lock:
            vm_inflight.pop(act_key, None)
            vm_completed.append(
                {
                    "vm_name": vm_name or "?",
                    "guest_ip": guest_ip or "",
                    "cluster": tag,
                    "op": op,
                    "state": "ok" if ec == 0 else "fail",
                    "seconds": round(dur_vm, 1),
                    "guest_timing": guest_timing,
                    # UI: SSH/connect + shell startup + trailing I/O vs guest-measured script (BULK_SNAP_T span).
                    "ssh_overhead_sec": round(ssh_overhead_sec, 2)
                    if ssh_overhead_sec is not None
                    else None,
                    "guest_cmd_sec": round(guest_cmd_sec, 2)
                    if guest_cmd_sec is not None
                    else None,
                    # Log command output for debugging
                    "exit_code": ec,
                    "remote_cmd": remote,
                    "stdout": (out or "")[:2000],  # Limit to 2000 chars to avoid excessive data
                    "stderr": (err or "")[:2000],
                    "combined_output": combined[:2000] if combined else "",
                    # Same live PE/CVM/stargate fields as Disk job progress, captured after throttle (pre)
                    # and after guest SSH returns (post); for ETA / analysis (concurrent shards may interleave).
                    "pe_cluster_before": pe_b,
                    "pe_cluster_after": pe_after,
                }
            )
            while len(vm_completed) > _vm_activity_completed_cap:
                del vm_completed[0]

        prog_key = "_all" if not cfg.parallel_clusters else tag
        with progress_lock:
            st = progress_by_cluster.get(prog_key)
            if st is not None:
                st["done"] += 1
                if ec == 0:
                    st["ok"] += 1
                else:
                    st["fail"] += 1
        _emit_disk_progress()
        return int(ec)

    tally_s = 0
    tally_f = 0
    t0 = time.perf_counter()

    if cfg.parallel_clusters:
        pe_map: Dict[str, str] = {}
        top_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        want_pe = cfg.cluster_pe_top_monitor or cfg.cluster_adaptive_ssh_parallel
        if want_pe:
            pe_map = _build_cluster_pe_ip_map(shard_keys_sorted, cfg.pe_cvm_ips_multiline)
            missing_pe = [c for c in shard_keys_sorted if c not in pe_map]
            if missing_pe:
                log.warning(
                    "PE CVM map: no IP for cluster(s) %s — PE top/adaptive skipped there "
                    "(add lines: one IP per cluster in sort order, or Name=ip).",
                    missing_pe,
                )

        remaining = {"n": len(jobs_by_cluster)}
        rem_lock = threading.Lock()

        def _run_one_cluster(
            cname: str, cjobs: List[Tuple[int, str, str, str, str, str]]
        ) -> Tuple[int, int]:
            ct0 = time.perf_counter()
            with cluster_timing_lock:
                cluster_timing[cname] = {"t0": ct0, "duration_sec": None}
            ts = 0
            tf = 0
            try:
                pe_ip = pe_map.get(cname, "") if want_pe else ""
                tlk = top_locks[cname]
                use_pe = bool(pe_ip.strip()) and (
                    cfg.cluster_pe_top_monitor or cfg.cluster_adaptive_ssh_parallel
                )

                gate: Optional[_ClusterConcurrencyGate] = None
                if adaptive_ssh and pe_ip.strip():
                    ceil_cfg = int(cfg.cluster_adaptive_ssh_ceiling or 0)
                    auto_ceil = max(parallel, ADAPTIVE_GUEST_SSH_AUTO_CEILING)
                    auto_ceil = min(auto_ceil, 500)
                    cap = ceil_cfg if ceil_cfg > 0 else auto_ceil
                    base_pf = max(1, min(parallel, len(cjobs)))
                    cap = max(base_pf, max(1, min(cap, len(cjobs))))
                    ramp_phases = _parse_adaptive_ramp_schedule(cfg.cluster_adaptive_ramp)
                    gate = _ClusterConcurrencyGate(
                        base_pf,
                        cap,
                        ramp_phases,
                        cooldown_sec=float(cfg.cluster_adaptive_cooldown_sec or 300.0),
                    )
                    cluster_gates[cname] = gate
                    log.info(
                        "Cluster %r: adaptive guest SSH baseline=%d ceiling=%d (CPU ramp thresh=%.0f%%); %d VM(s).",
                        cname,
                        base_pf,
                        cap,
                        float(cfg.cluster_adaptive_cpu_threshold_pct or 90.0),
                        len(cjobs),
                    )

                def _guest_op(args: Tuple[int, str, str, str, str, str]) -> int:
                    if use_pe:
                        _throttle_pe_before_guest_ssh(
                            cname,
                            pe_ip,
                            cfg,
                            log,
                            cancel_event,
                            tlk,
                            gate=gate,
                            pe_metrics=pe_metrics_by_cluster,
                            pe_metrics_lock=pe_metrics_lock,
                            last_adaptive_pe_cpu=last_adaptive_pe_cpu
                            if adaptive_ssh
                            else None,
                            emit_progress=_emit_disk_progress,
                        )
                    snap_b = _pe_metrics_row_snapshot(pe_metrics_by_cluster, cname, pe_metrics_lock)
                    if gate is not None:
                        gate.acquire()
                        try:
                            return _ssh_task(args, pe_before=snap_b)
                        finally:
                            gate.release()
                    return _ssh_task(args, pe_before=snap_b)

                inner = max(1, min(parallel, len(cjobs)))
                if gate is not None:
                    pool_workers = gate.ceiling
                    log.info(
                        "Cluster %r: thread pool %d; effective concurrent guest SSH is limited adaptively "
                        "(%d–%d) by PE CPU.",
                        cname,
                        pool_workers,
                        gate.baseline,
                        gate.ceiling,
                    )
                    with concurrent.futures.ThreadPoolExecutor(max_workers=pool_workers) as ex:
                        futures = [ex.submit(_guest_op, a) for a in cjobs]
                        try:
                            for fut in concurrent.futures.as_completed(futures):
                                _abort()
                                ec = fut.result()
                                if ec == 0:
                                    ts += 1
                                else:
                                    tf += 1
                        except RunCancelled:
                            try:
                                ex.shutdown(wait=False, cancel_futures=True)
                            except TypeError:
                                ex.shutdown(wait=False)
                            raise
                elif inner == 1:
                    for args in cjobs:
                        ec = _guest_op(args)
                        if ec == 0:
                            ts += 1
                        else:
                            tf += 1
                else:
                    log.info(
                        "Cluster %r: up to %d concurrent guest SSH session(s); %d VM(s) in this shard.",
                        cname,
                        inner,
                        len(cjobs),
                    )
                    with concurrent.futures.ThreadPoolExecutor(max_workers=inner) as ex:
                        futures = [ex.submit(_guest_op, a) for a in cjobs]
                        try:
                            for fut in concurrent.futures.as_completed(futures):
                                _abort()
                                ec = fut.result()
                                if ec == 0:
                                    ts += 1
                                else:
                                    tf += 1
                        except RunCancelled:
                            try:
                                ex.shutdown(wait=False, cancel_futures=True)
                            except TypeError:
                                ex.shutdown(wait=False)
                            raise
                with rem_lock:
                    remaining["n"] -= 1
                    rem = remaining["n"]
                log.info(
                    "Cluster %r finished: processed %d VM(s) in this shard. %d cluster shard(s) left.",
                    cname,
                    len(cjobs),
                    rem,
                )
                return ts, tf
            finally:
                with cluster_timing_lock:
                    rec = cluster_timing.get(cname)
                    if rec is not None:
                        rec["duration_sec"] = time.perf_counter() - ct0
                _emit_disk_progress()

        shard_keys = shard_keys_sorted
        if not shard_keys:
            pass
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(shard_keys))) as outer:
                outer_futs = [
                    outer.submit(_run_one_cluster, ck, jobs_by_cluster[ck]) for ck in shard_keys
                ]
                try:
                    for fut in concurrent.futures.as_completed(outer_futs):
                        _abort()
                        ts, tf = fut.result()
                        tally_s += ts
                        tally_f += tf
                except RunCancelled:
                    try:
                        outer.shutdown(wait=False, cancel_futures=True)
                    except TypeError:
                        outer.shutdown(wait=False)
                    raise
    else:
        jobs_np: List[Tuple[int, str, str, str, str, str]] = []
        for i, (vm_uuid, vm_name, guest_ip, _c) in enumerate(candidates):
            _abort()
            op = _resolve_op(mode, rng) if mode == DISK_RANDOM_MIX else mode
            cn = str(_c or "—").strip() or "—"
            jobs_np.append((i, vm_uuid, vm_name or "", guest_ip, op, cn))

        job_counts = Counter(j[5] for j in jobs_np)
        cluster_names_np = sorted(job_counts.keys(), key=lambda x: (x == "—", x))
        top_locks_np: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        pe_map_np: Dict[str, str] = {}
        want_pe_np = cfg.cluster_pe_top_monitor or cfg.cluster_adaptive_ssh_parallel
        if want_pe_np:
            pe_map_np = _build_cluster_pe_ip_map(list(cluster_names_np), cfg.pe_cvm_ips_multiline)
            miss_np = [c for c in cluster_names_np if c not in pe_map_np]
            if miss_np:
                log.warning(
                    "PE CVM map: no IP for cluster(s) %s — PE CPU throttle / adaptive skipped there "
                    "(add lines: one IP per cluster in sort order, or Name=ip).",
                    miss_np,
                )

        for cname in cluster_names_np:
            pe_ip_g = pe_map_np.get(cname, "")
            if adaptive_ssh and pe_ip_g.strip():
                n_c = int(job_counts[cname])
                ceil_cfg = int(cfg.cluster_adaptive_ssh_ceiling or 0)
                auto_ceil = max(parallel, ADAPTIVE_GUEST_SSH_AUTO_CEILING)
                auto_ceil = min(auto_ceil, 500)
                cap = ceil_cfg if ceil_cfg > 0 else auto_ceil
                base_pf = max(1, min(parallel, n_c))
                cap = max(base_pf, max(1, min(cap, n_c)))
                ramp_phases = _parse_adaptive_ramp_schedule(cfg.cluster_adaptive_ramp)
                g_np = _ClusterConcurrencyGate(
                    base_pf,
                    cap,
                    ramp_phases,
                    cooldown_sec=float(cfg.cluster_adaptive_cooldown_sec or 300.0),
                )
                cluster_gates[cname] = g_np
                log.info(
                    "Cluster %r (single pool): adaptive guest SSH baseline=%d ceiling=%d "
                    "(CPU ramp thresh=%.0f%%); %d VM(s).",
                    cname,
                    base_pf,
                    cap,
                    float(cfg.cluster_adaptive_cpu_threshold_pct or 90.0),
                    n_c,
                )

        def _guest_op_np(args: Tuple[int, str, str, str, str, str]) -> int:
            cname = args[5]
            pe_ip = pe_map_np.get(cname, "") if want_pe_np else ""
            use_pe = bool(pe_ip.strip()) and (
                cfg.cluster_pe_top_monitor or cfg.cluster_adaptive_ssh_parallel
            )
            tlk = top_locks_np[cname]
            gate = cluster_gates.get(cname)
            if use_pe:
                _throttle_pe_before_guest_ssh(
                    cname,
                    pe_ip,
                    cfg,
                    log,
                    cancel_event,
                    tlk,
                    gate=gate,
                    pe_metrics=pe_metrics_by_cluster,
                    pe_metrics_lock=pe_metrics_lock,
                    last_adaptive_pe_cpu=last_adaptive_pe_cpu if adaptive_ssh else None,
                    emit_progress=_emit_disk_progress,
                )
            snap_b = _pe_metrics_row_snapshot(pe_metrics_by_cluster, cname, pe_metrics_lock)
            if gate is not None:
                gate.acquire()
                try:
                    return _ssh_task(args, pe_before=snap_b)
                finally:
                    gate.release()
            return _ssh_task(args, pe_before=snap_b)

        pool_t0 = time.perf_counter()
        with cluster_timing_lock:
            cluster_timing["_all"] = {"t0": pool_t0, "duration_sec": None}
        try:
            pool_workers_np = max(1, min(parallel, len(jobs_np)))
            if cluster_gates:
                pool_workers_np = min(
                    500,
                    max(parallel, sum(g.ceiling for g in cluster_gates.values())),
                )
                pool_workers_np = max(1, min(pool_workers_np, len(jobs_np)))
            if parallel == 1 and not cluster_gates:
                for args in jobs_np:
                    ec = _guest_op_np(args)
                    if ec == 0:
                        tally_s += 1
                    else:
                        tally_f += 1
            else:
                log.debug(
                    "Guest SSH parallelism (single pool): up to %d worker thread(s).",
                    pool_workers_np,
                )
                with concurrent.futures.ThreadPoolExecutor(max_workers=pool_workers_np) as ex:
                    futures = [ex.submit(_guest_op_np, a) for a in jobs_np]
                    try:
                        for fut in concurrent.futures.as_completed(futures):
                            _abort()
                            ec = fut.result()
                            if ec == 0:
                                tally_s += 1
                            else:
                                tally_f += 1
                    except RunCancelled:
                        try:
                            ex.shutdown(wait=False, cancel_futures=True)
                        except TypeError:
                            ex.shutdown(wait=False)
                        raise
        finally:
            with cluster_timing_lock:
                rec = cluster_timing.get("_all")
                if rec is not None:
                    rec["duration_sec"] = time.perf_counter() - pool_t0
            _emit_disk_progress()

    elapsed = time.perf_counter() - t0
    log.info(
        "Guest churn done in %.1fs: ok=%d failed=%d",
        elapsed,
        tally_s,
        tally_f,
    )
    _log_guest_ssh_failure_summary(log, failures_by_cat)
    failure_counts = {k: len(v) for k, v in failures_by_cat.items() if v}
    ret: Dict[str, Any] = {
        "duration_sec": elapsed,
        "n_vms": n_run,
        "eligible_for_guest_ssh": eligible_total,
        "planned_guest_ssh_runs": n_run,
        "disk_run_limit": (cfg.disk_run_limit or "").strip(),
        "skipped_powered_off": skipped_power_off,
        "skipped_below_min_memory": skipped_below_min_memory,
        "succeeded": tally_s,
        "failed": tally_f,
        "other": 0,
        "ignored": ignored_name + skipped_no_ip + skipped_power_off + skipped_below_min_memory,
        "ignored_name": ignored_name,
        "skipped_no_ip": skipped_no_ip,
        "duplicate_rows_skipped": dup_rows,
        "mode": mode,
        "guest_ssh_parallel": parallel,
        "guest_ssh_failure_count_by_category": failure_counts,
        "parallel_clusters": bool(cfg.parallel_clusters),
        "vm_per_cluster": int(cfg.vm_per_cluster or 0),
        "cluster_pe_top_monitor": bool(cfg.cluster_pe_top_monitor),
        "cluster_cpu_max_pct": float(cfg.cluster_cpu_max_pct or 0),
        "cluster_mem_max_pct": float(cfg.cluster_mem_max_pct or 0),
        "cluster_adaptive_ssh_parallel": bool(cfg.cluster_adaptive_ssh_parallel),
        "cluster_adaptive_cpu_threshold_pct": float(cfg.cluster_adaptive_cpu_threshold_pct or 90.0),
        "cluster_adaptive_ramp": (cfg.cluster_adaptive_ramp or "180/5,300/3").strip(),
        "cluster_adaptive_ssh_step": int(cfg.cluster_adaptive_ssh_step or 2),
        "cluster_adaptive_ssh_ceiling": int(cfg.cluster_adaptive_ssh_ceiling or 0),
        "cluster_adaptive_cpu_spike_delta_pct": float(cfg.cluster_adaptive_cpu_spike_delta_pct or 0.0),
        "cluster_adaptive_overload_pause_sec": float(cfg.cluster_adaptive_overload_pause_sec or 10.0),
        "cluster_adaptive_cooldown_sec": float(cfg.cluster_adaptive_cooldown_sec or 300.0),
        "pe_prism_rest_port": int(cfg.pe_prism_rest_port or 9440),
        "disk_progress": _snapshot_disk_progress(),
    }
    if cfg.parallel_clusters:
        ret["vm_per_cluster_cap"] = pc_meta.get("vm_per_cluster_cap")
        ret["clusters_in_inventory"] = pc_meta.get("clusters_in_inventory")
        ret["per_cluster_planned"] = pc_meta.get("per_cluster_planned") or {}
    return ret

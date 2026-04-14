"""Web UI to configure and run bulk VM snapshots; logs per run with download."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import socket
import statistics
import subprocess
import threading
import time
import uuid
import datetime as dt
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from flask import Flask, abort, jsonify, redirect, render_template, request, send_file, url_for

from snapshot_runner import RANDOM_CRASH_OR_APP, RunCancelled, SnapshotConfig, run_snapshots
from vm_disk_runner import (
    DiskOpConfig,
    build_guest_disk_worklist,
    normalize_guest_dd_bs,
    preview_guest_disk_targets,
    run_disk_ops,
)
from vm_inventory import fetch_vm_inventory_rows, summarize_inventory_rows

_DISK_OP_MODES = frozenset({"create", "add", "update", "delete", "random_mix"})
from run_history import append_record, load_records

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024

PROJECT_DIR = Path(__file__).resolve().parent
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR = PROJECT_DIR / "data"
HISTORY_FILE = DATA_DIR / "run_history.jsonl"
SCHEDULES_FILE = DATA_DIR / "schedules.json"
DATA_DIR.mkdir(exist_ok=True)

runs_lock = threading.Lock()
# run_id -> dict
runs: dict[str, dict] = {}

# Disk job progress snapshots updated on a tight loop; kept out of ``runs_lock`` so ``/api/job`` polls
# are not starved when many workers call the progress callback.
_disk_progress_hot: dict[str, dict] = {}
_disk_progress_hot_lock = threading.Lock()


def _set_disk_progress_hot(run_id: str, snap: dict) -> None:
    with _disk_progress_hot_lock:
        _disk_progress_hot[run_id] = snap


def _pop_disk_progress_hot(run_id: str) -> None:
    with _disk_progress_hot_lock:
        _disk_progress_hot.pop(run_id, None)

schedules_lock = threading.Lock()
# normalized_pc_host_key -> schedule record (see _persist_schedules)
schedules: dict[str, dict] = {}

# Rows from **Fetch VMs**, reused for guest disk preview/run (no second Prism pass when possible).
INVENTORY_CACHE_TTL_SEC = int(os.environ.get("BULK_SNAP_INVENTORY_CACHE_TTL", str(4 * 3600)))
inventory_cache_lock = threading.Lock()
# cache_id -> { rows, pc_host_key, deadline_epoch }
inventory_cache: dict[str, dict] = {}

PC_SCHEME = "https"
PC_PORT = 9440

# Curator: Prism Central vs PE CVM often use different ``nutanix`` passwords — split so discover + PE runs both work.
# Override without editing code: BULK_SNAP_CURATOR_PC_SSH_PASSWORD, BULK_SNAP_CURATOR_PE_SSH_PASSWORD, etc.
CURATOR_PC_SSH_USER = os.environ.get("BULK_SNAP_CURATOR_PC_SSH_USER", "nutanix")
CURATOR_PC_SSH_PASSWORD = os.environ.get("BULK_SNAP_CURATOR_PC_SSH_PASSWORD", "nutanix/4u")
CURATOR_PC_SSH_PORT = int(os.environ.get("BULK_SNAP_CURATOR_PC_SSH_PORT", "22"))

CURATOR_PE_SSH_USER = os.environ.get("BULK_SNAP_CURATOR_PE_SSH_USER", "nutanix")
CURATOR_PE_SSH_PASSWORD = os.environ.get("BULK_SNAP_CURATOR_PE_SSH_PASSWORD", "RDMCluster.123")
CURATOR_PE_SSH_PORT = int(os.environ.get("BULK_SNAP_CURATOR_PE_SSH_PORT", "22"))
# Same as mohan_helpers/*.sh: full path to ncli on PC CVM.
_CURATOR_REMOTE_MULTICLUSTER_STATE = (
    "source /etc/profile 2>/dev/null; "
    "/home/nutanix/prism/cli/ncli multicluster get-cluster-state"
)
# On PE CVMs after profile (adjust path in this constant if your cluster uses a different curator_cli location).
_CURATOR_REMOTE_CLI_GET_SCANS = "source /etc/profile 2>/dev/null; curator_cli get_last_successful_scans"
_CURATOR_REMOTE_CLI_GET_BG_TASK_QUEUE_INFO = "source /etc/profile 2>/dev/null; curator_cli get_bg_task_queue_info"
_CURATOR_REMOTE_CLI_START_CURATOR_TASK = "source /etc/profile 2>/dev/null;  curl " + "http://$(curator_cli get_master_location | grep Using | awk '{print $4}')/master/api/client/StartCuratorTasks?task_type=2"

# Poll get_bg_task_queue_info: stop early if queue shows task rows, or if table stays empty after this many polls (0 = abort on first empty table).
CURATOR_BG_QUEUE_MAX_POLLS = int(os.environ.get("BULK_SNAP_CURATOR_BG_MAX_POLLS", "60"))
CURATOR_BG_QUEUE_POLL_INTERVAL_SEC = int(os.environ.get("BULK_SNAP_CURATOR_BG_POLL_INTERVAL_SEC", "10"))
CURATOR_BG_QUEUE_EMPTY_ABORT_MIN_POLLS = int(os.environ.get("BULK_SNAP_CURATOR_BG_EMPTY_ABORT_MIN_POLLS", "5"))
# After StartCuratorTasks, wait before polling queue (set 0 for quick tests).
CURATOR_POST_START_SLEEP_SEC = int(os.environ.get("BULK_SNAP_CURATOR_POST_START_SLEEP_SEC", "300"))

curator_run_lock = threading.Lock()
curator_run_state: dict[str, Any] = {
    "status": "idle",
    "started_at": "",
    "finished_at": "",
    "pe_count": 0,
    "results": None,
    "message": "",
    "top_level_ok": None,
}

CURATOR_LOG = logging.getLogger("bulk_snap.curator")

# ncli multicluster get-cluster-state — "Controller VM IP Address : [x]" or truncated "Controller VM IP Addre..."
_CURATOR_CONTROLLER_VM_IP_RE = re.compile(
    r"^\s*Controller VM IP[^:\r\n]*:\s*\[([^\]]*)\]\s*$",
    re.MULTILINE | re.IGNORECASE,
)


def _extract_controller_vm_ips_from_ncli_output(text: str) -> list[str]:
    """Collect unique PE CVM IPs from ncli multicluster get-cluster-state output (order preserved)."""
    seen: set[str] = set()
    order: list[str] = []
    for m in _CURATOR_CONTROLLER_VM_IP_RE.finditer(text or ""):
        inner = (m.group(1) or "").strip()
        if not inner:
            continue
        for part in re.split(r",\s*", inner):
            ip = part.strip()
            if not ip:
                continue
            if ip not in seen:
                seen.add(ip)
                order.append(ip)
    return order


def _curator_cli_bg_queue_has_task_rows(stdout: str) -> bool:
    """
    True if ``curator_cli get_bg_task_queue_info`` output includes at least one job data row
    (not only the header line and ``+---`` table frame).
    """
    for raw in (stdout or "").splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.split("|")]
        cells = [c for c in cells if c]
        if len(cells) < 2:
            continue
        if cells[0] == "Job Name" and cells[1] == "Priority":
            continue
        return True
    return False


def _curator_cli_bg_queue_output_empty(stdout: str) -> bool:
    """No usable stdout, or only an empty queue table (no task rows)."""
    if not (stdout or "").strip():
        return True
    return not _curator_cli_bg_queue_has_task_rows(stdout)


def _ssh_host_for_socket(field: str) -> str:
    """Host string for TCP/SSH (no https://, no :9440). IPv6 without brackets."""
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


def _nutanix_cvm_ssh_authenticate(transport, username: str, password: str) -> None:
    """
    Nutanix CVM keyboard-interactive logins: answer every prompt with the password (same as sshpass).
    """
    import paramiko

    def _interactive_handler(_title, _instructions, prompt_list) -> list[str]:
        if not prompt_list:
            return []
        return [password] * len(prompt_list)

    for _attempt in range(2):
        try:
            transport.auth_interactive(username, _interactive_handler)
        except paramiko.AuthenticationException:
            pass
        if transport.is_authenticated():
            return
        try:
            transport.auth_password(username, password)
        except paramiko.AuthenticationException:
            pass
        if transport.is_authenticated():
            return

    raise paramiko.AuthenticationException(
        "SSH authentication failed. Use the CVM Linux user (usually nutanix) and the same password "
        "that works with: ssh nutanix@<pc-ip> (not necessarily the Prism admin API password)."
    )


def _sshpass_ssh_run_remote(
    host: str,
    username: str,
    password: str,
    remote_cmd: str,
    *,
    port: int = 22,
    connect_timeout: float = 20.0,
    command_timeout: float = 120.0,
) -> tuple[int, str, str]:
    """
    Run ``remote_cmd`` on a host via ``sshpass`` + ``ssh -tt``.
    Returns (exit_code, stdout, stderr). On missing sshpass/ssh returns (-1, "", "sshpass or ssh not found").
    """
    if os.environ.get("BULK_SNAP_CURATOR_NO_SSHPASS", "").strip().lower() in ("1", "true", "yes"):
        return -1, "", "sshpass disabled by BULK_SNAP_CURATOR_NO_SSHPASS"
    if not shutil.which("sshpass") or not shutil.which("ssh"):
        return -1, "", "sshpass or ssh not found on this server"

    host = _ssh_host_for_socket(host)
    if not host:
        return -1, "", "Host/IP is empty"

    ct = int(max(5, min(120, connect_timeout)))
    cmd = [
        "sshpass",
        "-p",
        password,
        "ssh",
        "-tt",
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
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=float(connect_timeout) + float(command_timeout),
        )
    except subprocess.TimeoutExpired:
        return -1, "", "ssh command timed out"
    except OSError as e:
        return -1, "", f"Could not run ssh/sshpass: {e}"

    return p.returncode, (p.stdout or ""), (p.stderr or "")


def _ncli_multicluster_state_via_sshpass_ssh(
    host: str,
    username: str,
    password: str,
    *,
    port: int = 22,
    connect_timeout: float = 20.0,
    command_timeout: float = 120.0,
) -> tuple[str | None, str | None]:
    """
    ``sshpass`` + OpenSSH path for ncli multicluster state on PC CVM.
    Returns (None, None) if sshpass path is unavailable so caller can try Paramiko.
    """
    ec, out, err = _sshpass_ssh_run_remote(
        host,
        username,
        password,
        _CURATOR_REMOTE_MULTICLUSTER_STATE,
        port=port,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
    )
    if ec == -1:
        if "not found" in err or "disabled" in err:
            return None, None
        if "Host/IP is empty" in err:
            return None, "Prism Central host/IP is empty."
        return None, err or "ssh/ncli failed before remote command ran."

    err = err.strip()
    out_s = (out or "").strip("\n")
    if ec != 0:
        tail = (err + "\n" + out_s)[-2500:].strip()
        return None, f"ssh/ncli exited {ec}. {tail}" if tail else f"ssh/ncli exited {ec}."
    if not out_s and err:
        return None, f"ncli produced no stdout. stderr: {err[:2000]}"
    return (out or ""), None


def _ncli_multicluster_state_via_ssh(
    host: str,
    username: str,
    password: str,
    *,
    port: int = 22,
    connect_timeout: float = 20.0,
    command_timeout: float = 120.0,
) -> tuple[str | None, str | None]:
    """
    Run ``ncli multicluster get-cluster-state`` on a Prism Central CVM via SSH.
    Prefers OpenSSH + sshpass when installed (matches working manual flows); otherwise Paramiko.
    """
    host_key = _ssh_host_for_socket(host)
    if not host_key:
        return None, "Prism Central host/IP is empty."

    out_ss, err_ss = _ncli_multicluster_state_via_sshpass_ssh(
        host,
        username,
        password,
        port=port,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
    )
    if err_ss is not None:
        return None, err_ss
    if out_ss is not None:
        return out_ss, None

    try:
        import paramiko
    except ImportError:
        return None, "Neither sshpass+openssh nor paramiko is available (install sshpass or pip install paramiko)."

    sock = None
    transport = None
    channel = None
    try:
        sock = socket.create_connection((host_key, int(port)), timeout=float(connect_timeout))
        transport = paramiko.Transport(sock)
        transport.start_client(timeout=float(connect_timeout))
        _nutanix_cvm_ssh_authenticate(transport, username, password)

        cmd = _CURATOR_REMOTE_MULTICLUSTER_STATE
        channel = transport.open_session()
        channel.settimeout(max(5.0, float(command_timeout)))
        channel.exec_command(cmd)
        stdout = channel.makefile("rb", -1)
        stderr = channel.makefile_stderr("rb", -1)
        out_b = stdout.read() or b""
        err_b = stderr.read() or b""
        out = out_b.decode("utf-8", errors="replace")
        err = err_b.decode("utf-8", errors="replace").strip()
        if not out.strip() and err:
            return None, f"ncli produced no stdout. stderr: {err[:2000]}"
        if err and "not found" in err.lower() and "ncli" in err.lower():
            return None, f"ncli may be missing or not in PATH on the CVM. stderr: {err[:2000]}"
        return out, None
    except paramiko.AuthenticationException as e:
        msg = str(e).strip() or (
            "SSH authentication failed — use CVM user nutanix and the password that works from this host with ssh."
        )
        return None, msg
    except paramiko.SSHException as e:
        return None, f"SSH error: {e}"
    except OSError as e:
        return None, f"Could not reach {host_key}:{port} ({e}). Ensure this app’s host can SSH to the Prism Central CVM."
    except Exception as e:
        return None, f"Unexpected error: {e}"
    finally:
        try:
            if channel is not None:
                channel.close()
        except Exception:
            pass
        try:
            if transport is not None:
                transport.close()
        except Exception:
            pass
        try:
            if sock is not None:
                sock.close()
        except Exception:
            pass


def _pc_base_url(field: str) -> str:
    """Build https://<host>:9440 from UI text (IP, hostname, or pasted URL)."""
    s = (field or "").strip()
    if not s:
        return ""
    s = s.removeprefix("https://").removeprefix("http://")
    s = s.split("/")[0].strip()
    if not s:
        return ""
    # Drop trailing :port if IPv4 / hostname with numeric port (we force 9440).
    if not s.startswith("[") and s.count(":") == 1:
        host, port = s.rsplit(":", 1)
        if port.isdigit():
            s = host
    # Bare IPv6 literal → bracketed for URL
    if ":" in s and not s.startswith("["):
        if s.count(":") > 1:
            s = f"[{s}]"
    return f"{PC_SCHEME}://{s}:{PC_PORT}".rstrip("/")


def _pc_host_key(field: str) -> str:
    """Normalize PC IP/hostname from the form (one active schedule per host)."""
    s = (field or "").strip().lower()
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


def _inventory_cache_prune_unlocked() -> None:
    now = time.time()
    dead = [k for k, v in inventory_cache.items() if float(v.get("deadline", 0)) < now]
    for k in dead:
        inventory_cache.pop(k, None)


def _inventory_cache_store(rows: list, pc_host_key: str, duplicate_rows_skipped: int = 0) -> str:
    cid = uuid.uuid4().hex
    deadline = time.time() + INVENTORY_CACHE_TTL_SEC
    with inventory_cache_lock:
        _inventory_cache_prune_unlocked()
        inventory_cache[cid] = {
            "rows": rows,
            "pc_host_key": pc_host_key,
            "deadline": deadline,
            "duplicate_rows_skipped": int(duplicate_rows_skipped or 0),
        }
    return cid


def _inventory_cache_get(cache_id: str, pc_host_key: str) -> tuple[list | None, int, str | None]:
    """Return (rows, duplicate_rows_skipped, error_message). ``error_message`` is set when rows is None."""
    cid = (cache_id or "").strip()
    if not cid:
        return None, 0, None
    with inventory_cache_lock:
        _inventory_cache_prune_unlocked()
        ent = inventory_cache.get(cid)
        if not ent:
            return None, 0, "inventory_cache_id expired or unknown — click Fetch VMs again."
        if ent["pc_host_key"] != pc_host_key:
            return None, 0, "inventory cache is for a different Prism Central host — Fetch VMs again."
        if time.time() > float(ent["deadline"]):
            inventory_cache.pop(cid, None)
            return None, 0, "inventory cache expired — click Fetch VMs again."
        return ent["rows"], int(ent.get("duplicate_rows_skipped") or 0), None


def _parse_guest_min_memory_mib(mapping: dict) -> int:
    """
    Minimum configured RAM (MiB from Prism ``memory_size_bytes``) for guest disk eligibility.
    Eligible VMs must have ``memory_mib`` **strictly greater** than this. ``0`` disables the filter.
    Default **250** (i.e. only VMs with more than 250 MiB RAM).
    """
    raw = mapping.get("guest_min_memory_mib")
    if raw is None or raw == "":
        n = 250
    else:
        try:
            n = int(raw)
        except (TypeError, ValueError):
            n = 250
    return max(0, min(n, 1_000_000))


def _guest_ssh_parallel_from_payload(payload: dict) -> int:
    """Max concurrent guest SSH sessions for disk churn (default 10, clamped 1–500)."""
    raw = payload.get("guest_ssh_parallel")
    if raw is None or raw == "":
        n = 10
    else:
        try:
            n = int(raw)
        except (TypeError, ValueError):
            n = 10
    return max(1, min(n, 500))


def _truthy_payload(val: Any, default: bool = False) -> bool:
    if val is None or val == "":
        return default
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "on")


def _float_payload(payload: dict, key: str, default: float) -> float:
    raw = payload.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _int_nonneg_payload(payload: dict, key: str, default: int) -> int:
    raw = payload.get(key)
    if raw is None or raw == "":
        return default
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return default


def _disk_cluster_fields_from_payload(payload: dict) -> dict[str, Any]:
    """Parallel clusters, PE Prism stats, adaptive SSH — guest disk jobs only."""
    pe_port = int(payload.get("pe_cvm_ssh_port") or 22)
    try:
        raw_pp = payload.get("pe_prism_rest_port")
        if raw_pp is None or raw_pp == "":
            pe_prism_rest_port = int(os.environ.get("BULK_SNAP_PE_PRISM_REST_PORT", "9440") or "9440")
        else:
            pe_prism_rest_port = int(raw_pp)
    except (TypeError, ValueError):
        pe_prism_rest_port = 9440
    return {
        "parallel_clusters": _truthy_payload(payload.get("parallel_clusters")),
        "vm_per_cluster": _int_nonneg_payload(payload, "vm_per_cluster", 0),
        "cluster_pe_top_monitor": _truthy_payload(payload.get("cluster_pe_top_monitor")),
        "cluster_cpu_max_pct": _float_payload(payload, "cluster_cpu_max_pct", 85.0),
        "cluster_mem_max_pct": _float_payload(payload, "cluster_mem_max_pct", 85.0),
        "cluster_adaptive_ssh_parallel": _truthy_payload(payload.get("cluster_adaptive_ssh_parallel")),
        "cluster_adaptive_cpu_threshold_pct": _float_payload(
            payload, "cluster_adaptive_cpu_threshold_pct", 90.0
        ),
        "cluster_adaptive_ramp": (
            str(payload.get("cluster_adaptive_ramp") or "").strip() or "180/5,300/3"
        ),
        "cluster_adaptive_ssh_step": max(
            1, _int_nonneg_payload(payload, "cluster_adaptive_ssh_step", 2) or 2
        ),
        "cluster_adaptive_ssh_ceiling": _int_nonneg_payload(
            payload, "cluster_adaptive_ssh_ceiling", 0
        ),
        "cluster_adaptive_cpu_spike_delta_pct": _float_payload(
            payload, "cluster_adaptive_cpu_spike_delta_pct", 10.0
        ),
        "cluster_adaptive_overload_pause_sec": max(
            0.0, _float_payload(payload, "cluster_adaptive_overload_pause_sec", 10.0)
        ),
        "cluster_adaptive_cooldown_sec": max(
            0.0, _float_payload(payload, "cluster_adaptive_cooldown_sec", 300.0)
        ),
        "cluster_util_pause_sec": max(1.0, _float_payload(payload, "cluster_util_pause_sec", 30.0)),
        "cluster_util_max_retry_sec": max(
            1.0, _float_payload(payload, "cluster_util_max_retry_sec", 1800.0)
        ),
        "pe_cvm_ips_multiline": str(payload.get("pe_cvm_ips_multiline") or ""),
        "pe_cvm_ssh_user": str(payload.get("pe_cvm_ssh_user") or "").strip(),
        "pe_cvm_ssh_password": str(payload.get("pe_cvm_ssh_password") or ""),
        "pe_cvm_ssh_port": max(1, min(pe_port, 65535)),
        "pe_prism_rest_port": max(1, min(pe_prism_rest_port, 65535)),
    }


def _disk_run_limit_from_payload(payload: dict) -> str:
    raw = payload.get("disk_run_limit")
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip()
    legacy = payload.get("disk_max_vms")
    if legacy is None or legacy == "":
        return ""
    try:
        n = int(legacy)
        return str(max(0, min(n, 1_000_000)))
    except (TypeError, ValueError):
        return ""


def _pc_slug_for_run_id(pc_host_display: str) -> str:
    """PC hostname/IP as a single log/URL-safe token (no slashes or spaces)."""
    key = _pc_host_key(pc_host_display)
    if not key:
        return "pc"
    parts: list[str] = []
    for c in key:
        if c.isalnum():
            parts.append(c)
        else:
            parts.append("-")
    slug = "".join(parts).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return (slug[:80] if len(slug) > 80 else slug) or "pc"


def _allocate_run_id_and_path(pc_label: str) -> tuple[str, Path]:
    """run_id = pc_slug_YYYYMMDDTHHMMSS_microseconds; extend if same host collides in one tick."""
    slug = _pc_slug_for_run_id(pc_label)
    with runs_lock:
        for n in range(100_000):
            now = dt.datetime.now(dt.timezone.utc)
            ts = now.strftime("%Y%m%dT%H%M%S_%f")
            rid = f"{slug}_{ts}"
            if n:
                rid = f"{rid}_{n}"
            path = LOG_DIR / f"run_{rid}.log"
            if rid not in runs and not path.is_file():
                return rid, path
        rid = f"{slug}_{uuid.uuid4().hex}"
        path = LOG_DIR / f"run_{rid}.log"
        if rid not in runs and not path.is_file():
            return rid, path
    raise RuntimeError("Could not allocate a unique run_id for log file.")


def _cfg_to_dict(cfg: SnapshotConfig) -> dict:
    return {
        "base_url": cfg.base_url,
        "pc_user": cfg.pc_user,
        "pc_password": cfg.pc_password,
        "batch_size": cfg.batch_size,
        "recovery_point_type": cfg.recovery_point_type,
        "expiration_days": cfg.expiration_days,
        "poll_interval": cfg.poll_interval,
        "task_timeout_sec": cfg.task_timeout_sec,
        "group_member_page": cfg.group_member_page,
        "sleep_before_task_poll_sec": cfg.sleep_before_task_poll_sec,
        "snapshot_trigger_mode": cfg.snapshot_trigger_mode,
        "skip_substrings": list(cfg.skip_substrings),
        "skip_regex_patterns": list(cfg.skip_regex_patterns),
    }


def _cfg_from_dict(d: dict) -> SnapshotConfig:
    return SnapshotConfig(
        base_url=d["base_url"],
        pc_user=d["pc_user"],
        pc_password=d["pc_password"],
        batch_size=int(d["batch_size"]),
        recovery_point_type=d["recovery_point_type"],
        expiration_days=int(d["expiration_days"]),
        poll_interval=float(d["poll_interval"]),
        task_timeout_sec=int(d["task_timeout_sec"]),
        group_member_page=int(d["group_member_page"]),
        sleep_before_task_poll_sec=float(d["sleep_before_task_poll_sec"]),
        snapshot_trigger_mode=d["snapshot_trigger_mode"],
        skip_substrings=tuple(d.get("skip_substrings") or ()),
        skip_regex_patterns=tuple(d.get("skip_regex_patterns") or ()),
    )


def _load_schedules_from_disk() -> None:
    global schedules
    if not SCHEDULES_FILE.is_file():
        schedules = {}
        return
    try:
        data = json.loads(SCHEDULES_FILE.read_text(encoding="utf-8"))
        schedules = data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        schedules = {}


def _persist_schedules() -> None:
    with schedules_lock:
        blob = json.dumps(schedules, indent=2)
    tmp = SCHEDULES_FILE.with_suffix(".json.tmp")
    tmp.write_text(blob, encoding="utf-8")
    tmp.replace(SCHEDULES_FILE)


def _in_progress_runs_for_pc(host_key: str) -> list[dict]:
    """Queued or running snapshot jobs for this Prism host (for schedule list + cancel UX)."""
    rows: list[dict] = []
    with runs_lock:
        for rid, info in runs.items():
            ik = info.get("pc_host_key") or _pc_host_key(info.get("pc_host") or "")
            if ik != host_key:
                continue
            st = str(info.get("status") or "")
            if st not in ("queued", "running"):
                continue
            try:
                job_url = url_for("job_status", run_id=rid)
            except RuntimeError:
                job_url = f"/job/{rid}"
            rows.append({"run_id": rid, "status": st, "job_url": job_url})
    return rows


def _schedule_summaries() -> list[dict]:
    """Rows for index template (no passwords). Browser formats next_run_utc locally."""
    out: list[dict] = []
    with schedules_lock:
        items = list(schedules.items())
    for key, rec in items:
        pc = rec.get("pc_ip") or key
        kind = rec.get("kind") or "?"
        nr = (rec.get("next_run_utc") or "").strip()
        extra = ""
        if kind == "recurring":
            extra = f"every {int(rec.get('recurring_interval_minutes') or 60)} min"
        out.append(
            {
                "pc_ip": pc,
                "pc_host_key": key,
                "kind": kind,
                "next_run_utc": nr,
                "detail": extra,
                "in_progress": _in_progress_runs_for_pc(key),
                "_sort": nr,
            }
        )
    out.sort(key=lambda r: r["_sort"])
    for row in out:
        row.pop("_sort", None)
    return out


def _enqueue_snapshot_run(cfg: SnapshotConfig, pc_host_display: str = "") -> str:
    """Start a background job; return run_id (log file run_<pc>_<utc_ts>.log)."""
    disp = (pc_host_display or "").strip()
    if not disp:
        host = urlparse(cfg.base_url).hostname
        disp = host or ""
    pc_for_name = disp or (urlparse(cfg.base_url).hostname or "pc")
    run_id, log_path = _allocate_run_id_and_path(pc_for_name)
    host_key = _pc_host_key(disp) or _pc_host_key(urlparse(cfg.base_url).hostname or "")
    queued_at = dt.datetime.now(dt.timezone.utc).isoformat()
    cancel_ev = threading.Event()
    with runs_lock:
        runs[run_id] = {
            "status": "queued",
            "log_path": str(log_path),
            "error": "",
            "base_url": cfg.base_url.rstrip("/"),
            "pc_host": disp,
            "pc_host_key": host_key,
            "queued_at": queued_at,
            "cancel_event": cancel_ev,
        }
    t = threading.Thread(
        target=_job,
        args=(run_id, cfg, log_path),
        daemon=True,
    )
    t.start()
    return run_id


def _scheduler_loop() -> None:
    utc = dt.timezone.utc
    log = logging.getLogger("bulk_snap.scheduler")
    while True:
        time.sleep(20)
        try:
            now = dt.datetime.now(utc)
            due_keys: list[str] = []
            with schedules_lock:
                for key, rec in list(schedules.items()):
                    nr_s = rec.get("next_run_utc")
                    if not nr_s:
                        continue
                    try:
                        nr = dt.datetime.fromisoformat(nr_s.replace("Z", "+00:00"))
                        if nr.tzinfo is None:
                            nr = nr.replace(tzinfo=utc)
                    except (TypeError, ValueError):
                        continue
                    if nr <= now:
                        due_keys.append(key)

            for key in due_keys:
                cfg_payload = None
                pc_disp = ""
                with schedules_lock:
                    cur = schedules.get(key)
                    if not cur:
                        continue
                    nr_s = cur.get("next_run_utc")
                    if not nr_s:
                        continue
                    try:
                        nr = dt.datetime.fromisoformat(nr_s.replace("Z", "+00:00"))
                        if nr.tzinfo is None:
                            nr = nr.replace(tzinfo=utc)
                    except (TypeError, ValueError):
                        continue
                    if nr > dt.datetime.now(utc):
                        continue
                    cfg_payload = cur.get("cfg")
                    pc_disp = str(cur.get("pc_ip") or "")
                    kind = cur.get("kind")
                    if kind == "one_time":
                        del schedules[key]
                    else:
                        interval = int(cur.get("recurring_interval_minutes") or 60)
                        interval = max(1, interval)
                        cur["next_run_utc"] = (
                            dt.datetime.now(utc) + dt.timedelta(minutes=interval)
                        ).isoformat()

                if cfg_payload:
                    _persist_schedules()
                    try:
                        cfg = _cfg_from_dict(cfg_payload)
                        _enqueue_snapshot_run(cfg, pc_disp)
                    except Exception:
                        log.exception("Scheduled run failed to start for %s", key)
        except Exception:
            log.exception("Scheduler loop error")


def _parse_lines(text: str) -> tuple[str, ...]:
    out = []
    for line in (text or "").replace(",", "\n").splitlines():
        s = line.strip()
        if s:
            out.append(s)
    return tuple(out)


def _validate_regexes(patterns: tuple[str, ...]):
    for p in patterns:
        try:
            re.compile(p, re.IGNORECASE)
        except re.error as e:
            return f"Invalid regex {p!r}: {e}"
    return None


def _job(run_id: str, cfg: SnapshotConfig, log_path: Path) -> None:
    logger = logging.getLogger(f"bulk_snap.{run_id}")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    logger.addHandler(fh)

    with runs_lock:
        cancel_ev = runs[run_id].get("cancel_event")
        if not isinstance(cancel_ev, threading.Event):
            cancel_ev = threading.Event()
            runs[run_id]["cancel_event"] = cancel_ev

    try:
        with runs_lock:
            runs[run_id]["status"] = "running"
            runs[run_id]["error"] = ""
            runs[run_id]["running_started_at"] = dt.datetime.now(
                dt.timezone.utc
            ).isoformat()

        try:
            result = run_snapshots(cfg, logger, cancel_ev)
        except RunCancelled:
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            logger.info("Run cancelled by user.")
            with runs_lock:
                runs[run_id]["status"] = "aborted"
                runs[run_id]["error"] = "Cancelled by user."
                runs[run_id]["finished_at"] = finished_at
            return

        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        with runs_lock:
            pc_h = runs[run_id].get("pc_host", "")
            pc_k = runs[run_id].get("pc_host_key", "") or _pc_host_key(pc_h)
        rec = {
            "run_id": run_id,
            "pc_host": pc_h,
            "pc_host_key": pc_k,
            "at": finished_at,
            "duration_sec": float(result["duration_sec"]),
            "n_vms": int(result["n_vms"]),
            "snapshot_trigger_mode": cfg.snapshot_trigger_mode,
            "batch_size": cfg.batch_size,
            "recovery_point_type": cfg.recovery_point_type,
            "succeeded": int(result["succeeded"]),
            "failed": int(result["failed"]),
            "other": int(result["other"]),
            "ignored": int(result["ignored"]),
            "rp_random_crash": int(result.get("rp_random_crash") or 0),
            "rp_random_app": int(result.get("rp_random_app") or 0),
        }
        append_record(HISTORY_FILE, rec)
        summary = {
            "n_vms": int(result["n_vms"]),
            "duration_sec": float(result["duration_sec"]),
            "succeeded": int(result["succeeded"]),
            "failed": int(result["failed"]),
            "other": int(result["other"]),
            "ignored": int(result["ignored"]),
            "recovery_point_type": cfg.recovery_point_type,
            "snapshot_trigger_mode": cfg.snapshot_trigger_mode,
            "batch_size": cfg.batch_size,
            "rp_random_crash": int(result.get("rp_random_crash") or 0),
            "rp_random_app": int(result.get("rp_random_app") or 0),
        }
        with runs_lock:
            runs[run_id]["status"] = "complete"
            runs[run_id]["finished_at"] = finished_at
            runs[run_id]["timing"] = {
                "duration_sec": result["duration_sec"],
                "n_vms": result["n_vms"],
            }
            runs[run_id]["summary"] = summary
    except Exception as e:
        logger.exception("Run failed")
        with runs_lock:
            runs[run_id]["status"] = "error"
            runs[run_id]["error"] = str(e)
            runs[run_id]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    finally:
        with runs_lock:
            if run_id in runs:
                runs[run_id].pop("cancel_event", None)


def _enqueue_disk_run(
    dcfg: DiskOpConfig,
    pc_host_display: str = "",
    *,
    inventory_rows: list | None = None,
    duplicate_inventory_rows: int = 0,
    inventory_from_cache: bool = False,
) -> str:
    """Background guest disk SSH batch; returns run_id."""
    disp = (pc_host_display or "").strip()
    if not disp:
        host = urlparse(dcfg.base_url).hostname
        disp = host or ""
    pc_for_name = disp or (urlparse(dcfg.base_url).hostname or "pc")
    run_id, log_path = _allocate_run_id_and_path(f"{pc_for_name}_disk")
    host_key = _pc_host_key(disp) or _pc_host_key(urlparse(dcfg.base_url).hostname or "")
    queued_at = dt.datetime.now(dt.timezone.utc).isoformat()
    cancel_ev = threading.Event()
    with runs_lock:
        runs[run_id] = {
            "status": "queued",
            "log_path": str(log_path),
            "error": "",
            "base_url": dcfg.base_url.rstrip("/"),
            "pc_host": disp,
            "pc_host_key": host_key,
            "queued_at": queued_at,
            "cancel_event": cancel_ev,
            "job_kind": "disk",
            "disk_progress": None,
        }
    t = threading.Thread(
        target=_disk_job,
        args=(run_id, dcfg, log_path),
        kwargs={
            "inventory_rows": inventory_rows,
            "duplicate_inventory_rows": duplicate_inventory_rows,
            "inventory_from_cache": inventory_from_cache,
        },
        daemon=True,
    )
    t.start()
    return run_id


def _disk_job(
    run_id: str,
    dcfg: DiskOpConfig,
    log_path: Path,
    *,
    inventory_rows: list | None = None,
    duplicate_inventory_rows: int = 0,
    inventory_from_cache: bool = False,
) -> None:
    logger = logging.getLogger(f"bulk_disk.{run_id}")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)

    with runs_lock:
        cancel_ev = runs[run_id].get("cancel_event")
        if not isinstance(cancel_ev, threading.Event):
            cancel_ev = threading.Event()
            runs[run_id]["cancel_event"] = cancel_ev

    try:
        with runs_lock:
            runs[run_id]["status"] = "running"
            runs[run_id]["error"] = ""
            runs[run_id]["running_started_at"] = dt.datetime.now(
                dt.timezone.utc
            ).isoformat()

        def _on_disk_progress(snap: dict) -> None:
            _set_disk_progress_hot(run_id, snap)

        try:
            result = run_disk_ops(
                dcfg,
                logger,
                cancel_ev,
                rows=inventory_rows,
                duplicate_inventory_rows=duplicate_inventory_rows,
                from_cache=inventory_from_cache,
                progress_callback=_on_disk_progress,
            )
        except RunCancelled:
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            logger.info("Disk job cancelled by user.")
            with _disk_progress_hot_lock:
                last_dp = _disk_progress_hot.get(run_id)
            with runs_lock:
                runs[run_id]["status"] = "aborted"
                runs[run_id]["error"] = "Cancelled by user."
                runs[run_id]["finished_at"] = finished_at
                if last_dp is not None:
                    runs[run_id]["disk_progress"] = last_dp
            return

        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        summary = {
            "job_kind": "disk",
            "n_vms": int(result["n_vms"]),
            "eligible_for_guest_ssh": int(result.get("eligible_for_guest_ssh") or 0),
            "planned_guest_ssh_runs": int(result.get("planned_guest_ssh_runs") or 0),
            "disk_run_limit": str(result.get("disk_run_limit") or ""),
            "skipped_powered_off": int(result.get("skipped_powered_off") or 0),
            "skipped_below_min_memory": int(result.get("skipped_below_min_memory") or 0),
            "guest_min_memory_mib": int(dcfg.guest_min_memory_mib or 0),
            "guest_ssh_parallel": int(result.get("guest_ssh_parallel") or 10),
            "duration_sec": float(result["duration_sec"]),
            "succeeded": int(result["succeeded"]),
            "failed": int(result["failed"]),
            "other": int(result["other"]),
            "ignored": int(result["ignored"]),
            "disk_op_mode": str(result.get("mode") or ""),
            "guest_ssh_failure_count_by_category": result.get("guest_ssh_failure_count_by_category")
            or {},
            "parallel_clusters": bool(result.get("parallel_clusters")),
            "vm_per_cluster": int(result.get("vm_per_cluster") or 0),
            "cluster_pe_top_monitor": bool(result.get("cluster_pe_top_monitor")),
            "cluster_adaptive_ssh_parallel": bool(result.get("cluster_adaptive_ssh_parallel")),
        }
        with runs_lock:
            runs[run_id]["status"] = "complete"
            runs[run_id]["finished_at"] = finished_at
            runs[run_id]["timing"] = {
                "duration_sec": result["duration_sec"],
                "n_vms": result["n_vms"],
            }
            runs[run_id]["summary"] = summary
            runs[run_id]["disk_progress"] = result.get("disk_progress")
    except Exception as e:
        logger.exception("Disk job failed")
        with runs_lock:
            runs[run_id]["status"] = "error"
            runs[run_id]["error"] = str(e)
            runs[run_id]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    finally:
        _pop_disk_progress_hot(run_id)
        with runs_lock:
            if run_id in runs:
                runs[run_id].pop("cancel_event", None)


def _get_run_info(run_id: str):
    """In-memory run row, or minimal row if log file still on disk (after restart)."""
    with runs_lock:
        mem = runs.get(run_id)
        if mem:
            out = dict(mem)
        else:
            out = None
    if out is not None:
        with _disk_progress_hot_lock:
            hot = _disk_progress_hot.get(run_id)
        if hot is not None:
            out["disk_progress"] = hot
        return out
    path = LOG_DIR / f"run_{run_id}.log"
    if path.is_file():
        return {
            "status": "complete",
            "log_path": str(path),
            "error": "",
            "base_url": "",
            "pc_host": "",
            "pc_host_key": "",
            "queued_at": "",
            "finished_at": "",
            "summary": None,
        }
    return None


def _summary_from_history_rec(rec: dict) -> dict:
    return {
        "n_vms": int(rec.get("n_vms", 0)),
        "duration_sec": float(rec.get("duration_sec", 0)),
        "succeeded": int(rec.get("succeeded", 0)),
        "failed": int(rec.get("failed", 0)),
        "other": int(rec.get("other", 0)),
        "ignored": int(rec.get("ignored", 0)),
        "recovery_point_type": str(rec.get("recovery_point_type") or ""),
        "snapshot_trigger_mode": str(rec.get("snapshot_trigger_mode") or ""),
        "batch_size": int(rec.get("batch_size", 0)),
        "rp_random_crash": int(rec.get("rp_random_crash") or 0),
        "rp_random_app": int(rec.get("rp_random_app") or 0),
    }


@app.context_processor
def _inject_active_schedules():
    return {
        "active_schedules": _schedule_summaries(),
        "RANDOM_CRASH_OR_APP": RANDOM_CRASH_OR_APP,
    }


@app.route("/")
def index():
    return render_template(
        "index.html",
        success=request.args.get("success"),
    )


@app.route("/cancel_schedule", methods=["POST"])
def cancel_schedule():
    pc_ip = request.form.get("pc_ip") or ""
    key = _pc_host_key(pc_ip)
    abort_jobs = request.form.get("abort_in_progress") == "1"
    if key:
        if abort_jobs:
            with runs_lock:
                for _rid, info in list(runs.items()):
                    ik = info.get("pc_host_key") or _pc_host_key(info.get("pc_host") or "")
                    if ik != key:
                        continue
                    if info.get("status") not in ("queued", "running"):
                        continue
                    ev = info.get("cancel_event")
                    if isinstance(ev, threading.Event):
                        ev.set()
        with schedules_lock:
            schedules.pop(key, None)
        _persist_schedules()
    msg = (
        "Schedule removed and abort requested for in-progress job(s)."
        if abort_jobs
        else "Schedule removed."
    )
    return redirect(url_for("index", success=msg))


@app.route("/start", methods=["POST"])
def start():
    pc_ip = request.form.get("pc_ip") or ""
    base_url = _pc_base_url(pc_ip)
    pc_user = (request.form.get("pc_user") or "").strip()
    pc_password = request.form.get("pc_password") or ""

    if not base_url or not pc_user:
        return render_template(
            "index.html",
            error="PC IP / hostname and username are required.",
        ), 400

    skip_subs = _parse_lines(request.form.get("skip_substrings", ""))
    skip_rx = _parse_lines(request.form.get("skip_regexes", ""))
    err = _validate_regexes(skip_rx)
    if err:
        return render_template("index.html", error=err), 400

    try:
        batch_size = int(request.form.get("batch_size") or 10)
        expiration_days = int(request.form.get("expiration_days") or 30)
        task_timeout_sec = int(request.form.get("task_timeout_sec") or 300)
        group_member_page = int(request.form.get("group_member_page") or 500)
    except ValueError:
        return render_template(
            "index.html",
            error="Batch size, expiration days, task timeout, and group page must be integers.",
        ), 400

    try:
        poll_interval = float(request.form.get("poll_interval") or 4)
        sleep_before = float(request.form.get("sleep_before_task_poll_sec") or 0)
    except ValueError:
        return render_template(
            "index.html",
            error="Poll interval and pre-poll sleep must be numbers.",
        ), 400

    snap_mode = (request.form.get("snapshot_trigger_mode") or "series").lower()
    if snap_mode not in ("series", "parallel"):
        return render_template(
            "index.html",
            error="Snapshot trigger mode must be series or parallel.",
        ), 400

    rpt = (request.form.get("recovery_point_type") or "CRASH_CONSISTENT").strip()
    if rpt not in (
        "CRASH_CONSISTENT",
        "APPLICATION_CONSISTENT",
        RANDOM_CRASH_OR_APP,
    ):
        return render_template(
            "index.html",
            error="Invalid recovery point type.",
        ), 400

    cfg = SnapshotConfig(
        base_url=base_url.rstrip("/"),
        pc_user=pc_user,
        pc_password=pc_password,
        batch_size=max(1, batch_size),
        snapshot_trigger_mode=snap_mode,
        recovery_point_type=rpt,
        expiration_days=max(1, expiration_days),
        poll_interval=max(0.5, poll_interval),
        task_timeout_sec=max(60, task_timeout_sec),
        group_member_page=max(1, group_member_page),
        sleep_before_task_poll_sec=max(0.0, sleep_before),
        skip_substrings=skip_subs,
        skip_regex_patterns=skip_rx,
    )

    if request.form.get("schedule_enabled") == "1":
        host_key = _pc_host_key(pc_ip)
        if not host_key:
            return (
                render_template(
                    "index.html",
                    error="Prism Central address is required for a schedule.",
                ),
                400,
            )
        with schedules_lock:
            conflict = host_key in schedules
        if conflict:
            return (
                render_template(
                    "index.html",
                    error=(
                        "An active schedule already exists for this Prism Central host. "
                        "Cancel it in the list below before adding another."
                    ),
                    error_schedule_conflict=True,
                ),
                400,
            )
        sk = (request.form.get("schedule_kind") or "one_time").strip()
        if sk not in ("one_time", "recurring"):
            return (
                render_template("index.html", error="Schedule type must be one-time or recurring."),
                400,
            )
        utc = dt.timezone.utc
        now = dt.datetime.now(utc)
        if sk == "one_time":
            utc_raw = (request.form.get("schedule_one_time_utc") or "").strip()
            if not utc_raw:
                return (
                    render_template(
                        "index.html",
                        error=(
                            "Choose a one-time run time, or turn off scheduling "
                            "to start immediately."
                        ),
                    ),
                    400,
                )
            try:
                run_at = dt.datetime.fromisoformat(utc_raw.replace("Z", "+00:00"))
                if run_at.tzinfo is None:
                    run_at = run_at.replace(tzinfo=utc)
                else:
                    run_at = run_at.astimezone(utc)
            except (TypeError, ValueError):
                return (
                    render_template("index.html", error="Invalid one-time schedule timestamp."),
                    400,
                )
            if run_at <= now:
                return (
                    render_template(
                        "index.html",
                        error="One-time schedule must be in the future.",
                    ),
                    400,
                )
            next_run = run_at
            interval_minutes = None
        else:
            try:
                interval_minutes = int(request.form.get("recurring_interval_minutes") or 60)
            except ValueError:
                interval_minutes = 60
            interval_minutes = max(1, min(interval_minutes, 7 * 24 * 60))
            next_run = now + dt.timedelta(minutes=interval_minutes)

        rec = {
            "schedule_id": str(uuid.uuid4()),
            "kind": sk,
            "pc_ip": pc_ip.strip(),
            "next_run_utc": next_run.isoformat(),
            "recurring_interval_minutes": interval_minutes,
            "cfg": _cfg_to_dict(cfg),
        }
        with schedules_lock:
            schedules[host_key] = rec
        _persist_schedules()
        return render_template(
            "index.html",
            success=(
                "Schedule saved. Snapshots will run automatically at the chosen time "
                "(check the list below)."
            ),
        )

    run_id = _enqueue_snapshot_run(cfg, pc_ip.strip())
    return redirect(url_for("job_status", run_id=run_id))


@app.route("/api/start_snapshot_run", methods=["POST"])
def api_start_snapshot_run():
    """
    Start an immediate snapshot run from JSON (same parameters as the snapshot portion of the main form).
    Used by the unified pipeline button. Schedules are not supported here — use POST /start with the form.
    """
    p = request.get_json(silent=True) or {}
    pc_ip = str(p.get("pc_ip") or "").strip()
    base_url = _pc_base_url(pc_ip)
    pc_user = str(p.get("pc_user") or "").strip()
    pc_password = str(p.get("pc_password") or "")
    if not base_url or not pc_user:
        return jsonify({"ok": False, "message": "pc_ip and pc_user are required."}), 400

    skip_subs = _parse_lines(str(p.get("skip_substrings") or ""))
    skip_rx = _parse_lines(str(p.get("skip_regexes") or ""))
    err = _validate_regexes(skip_rx)
    if err:
        return jsonify({"ok": False, "message": err}), 400

    try:
        batch_size = int(p.get("batch_size") or 10)
        expiration_days = int(p.get("expiration_days") or 30)
        task_timeout_sec = int(p.get("task_timeout_sec") or 300)
        group_member_page = int(p.get("group_member_page") or 500)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "batch_size, expiration_days, task_timeout_sec, group_member_page must be integers."}), 400

    try:
        poll_interval = float(p.get("poll_interval") or 4)
        sleep_before = float(p.get("sleep_before_task_poll_sec") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "poll_interval and sleep_before_task_poll_sec must be numbers."}), 400

    snap_mode = str(p.get("snapshot_trigger_mode") or "series").lower()
    if snap_mode not in ("series", "parallel"):
        return jsonify({"ok": False, "message": "snapshot_trigger_mode must be series or parallel."}), 400

    rpt = str(p.get("recovery_point_type") or "CRASH_CONSISTENT").strip()
    if rpt not in ("CRASH_CONSISTENT", "APPLICATION_CONSISTENT", RANDOM_CRASH_OR_APP):
        return jsonify({"ok": False, "message": "Invalid recovery_point_type."}), 400

    cfg = SnapshotConfig(
        base_url=base_url.rstrip("/"),
        pc_user=pc_user,
        pc_password=pc_password,
        batch_size=max(1, batch_size),
        snapshot_trigger_mode=snap_mode,
        recovery_point_type=rpt,
        expiration_days=max(1, expiration_days),
        poll_interval=max(0.5, poll_interval),
        task_timeout_sec=max(60, task_timeout_sec),
        group_member_page=max(1, group_member_page),
        sleep_before_task_poll_sec=max(0.0, sleep_before),
        skip_substrings=skip_subs,
        skip_regex_patterns=skip_rx,
    )
    run_id = _enqueue_snapshot_run(cfg, pc_ip.strip())
    return jsonify({"ok": True, "run_id": run_id, "job_url": url_for("job_status", run_id=run_id)})


_JOB_API_LOG_MAX_COMPLETE = 500_000
_JOB_API_LOG_MAX_RUNNING = 180_000
_JOB_API_LOG_MAX_RUNNING_DISK = 96_000


def _read_log_text_for_job_api(path: Path, *, status: str, job_kind: str = "") -> tuple[str, bool]:
    """
    Body of ``log`` for ``/api/job``. While status is *running*, return only a tail so polls
    stay small (less I/O) and overlap less on the console when ``threaded=True``.
    """
    if job_kind == "disk":
        # Disk jobs surface per-VM lines via ``disk_progress.vm_activity`` only; full log is download.
        return "", False
    running = status == "running"
    if running and job_kind == "disk":
        max_bytes = _JOB_API_LOG_MAX_RUNNING_DISK
    elif running:
        max_bytes = _JOB_API_LOG_MAX_RUNNING
    else:
        max_bytes = _JOB_API_LOG_MAX_COMPLETE
    if not path.is_file():
        return "", False
    try:
        size = path.stat().st_size
    except OSError:
        return "(could not read log file)", False
    try:
        with open(path, "rb") as f:
            if size <= max_bytes:
                raw = f.read()
                truncated = False
            else:
                truncated = True
                f.seek(max(0, size - max_bytes))
                raw = f.read()
        text = raw.decode("utf-8", errors="replace")
        if truncated and raw:
            nl = text.find("\n")
            if nl != -1:
                text = text[nl + 1 :]
            prefix = (
                "...[live tail while running — use Download for the full log]\n"
                if running
                else "...[truncated]\n"
            )
            text = prefix + text
        return text, truncated
    except OSError:
        return "(could not read log file)", False


def _elapsed_running_seconds(info: dict) -> int | None:
    if info.get("status") != "running":
        return None
    rs = info.get("running_started_at")
    if not rs:
        return None
    try:
        t0 = dt.datetime.fromisoformat(str(rs).replace("Z", "+00:00"))
        if t0.tzinfo is None:
            t0 = t0.replace(tzinfo=dt.timezone.utc)
        return max(0, int((dt.datetime.now(dt.timezone.utc) - t0).total_seconds()))
    except (TypeError, ValueError):
        return None


@app.route("/job/<run_id>")
def job_status(run_id: str):
    info = _get_run_info(run_id)
    if not info:
        abort(404)
    return render_template(
        "job.html",
        run_id=run_id,
        info=info,
        api_url=url_for("api_job", run_id=run_id),
        cancel_api_url=url_for("api_job_cancel", run_id=run_id),
    )


@app.route("/api/job/<run_id>")
def api_job(run_id: str):
    info = _get_run_info(run_id)
    if not info:
        return jsonify({"error": "not found"}), 404
    st = info.get("status", "")
    path = Path(info["log_path"])
    jk = str(info.get("job_kind") or "")
    text, log_truncated = _read_log_text_for_job_api(path, status=st, job_kind=jk)
    return jsonify(
        {
            "status": st,
            "error": info.get("error", ""),
            "log": text,
            "log_truncated": log_truncated,
            "elapsed_running_sec": _elapsed_running_seconds(info),
            "queued_at": info.get("queued_at", ""),
            "job_kind": jk,
            "summary": info.get("summary"),
            "disk_progress": info.get("disk_progress"),
        }
    )


@app.route("/api/job/<run_id>/cancel", methods=["POST"])
def api_job_cancel(run_id: str):
    with runs_lock:
        info = runs.get(run_id)
        if not info:
            return jsonify({"ok": False, "message": "Run not found (only active session jobs can be cancelled)."}), 404
        ev = info.get("cancel_event")
        if isinstance(ev, threading.Event):
            ev.set()
        else:
            return jsonify({"ok": False, "message": "This run cannot be cancelled."}), 400
    return jsonify({"ok": True, "message": "Cancel requested."})


@app.route("/download/<run_id>")
def download(run_id: str):
    info = _get_run_info(run_id)
    if not info:
        abort(404)
    path = Path(info["log_path"])
    if not path.is_file():
        abort(404)
    return send_file(
        path,
        as_attachment=True,
        download_name=f"bulk_snapshots_{run_id}.log",
        mimetype="text/plain",
    )


@app.route("/api/runs_for_pc")
def api_runs_for_pc():
    """Completed runs from JSONL + live session rows for this Prism Central host."""
    pc_ip = (request.args.get("pc_ip") or "").strip()
    key = _pc_host_key(pc_ip)
    if not key:
        return jsonify({"ok": False, "message": "pc_ip is required"}), 400

    by_id: dict[str, dict] = {}
    with runs_lock:
        snapshot = [(rid, dict(info)) for rid, info in runs.items()]

    for rid, info in snapshot:
        ik = info.get("pc_host_key") or _pc_host_key(info.get("pc_host") or "")
        if ik != key:
            continue
        queued = info.get("queued_at") or ""
        sort_at = info.get("finished_at") or queued
        by_id[rid] = {
            "run_id": rid,
            "source": "session",
            "status": info.get("status", ""),
            "at_utc": queued,
            "finished_at_utc": info.get("finished_at") or "",
            "job_url": url_for("job_status", run_id=rid),
            "summary": info.get("summary"),
            "error": info.get("error") or "",
            "sort_at": sort_at,
        }

    for rec in load_records(HISTORY_FILE, max_lines=8000):
        rid = rec.get("run_id")
        if not rid:
            continue
        rec_key = rec.get("pc_host_key") or _pc_host_key(rec.get("pc_host") or "")
        if rec_key != key:
            continue
        if rid in by_id:
            continue
        at = rec.get("at") or ""
        by_id[str(rid)] = {
            "run_id": str(rid),
            "source": "history",
            "status": "complete",
            "at_utc": at,
            "finished_at_utc": at,
            "job_url": url_for("job_status", run_id=str(rid)),
            "summary": _summary_from_history_rec(rec),
            "error": "",
            "sort_at": at,
        }

    rows = sorted(by_id.values(), key=lambda r: r.get("sort_at") or "", reverse=True)
    for r in rows:
        r.pop("sort_at", None)
    return jsonify(
        {
            "ok": True,
            "pc_ip": pc_ip,
            "pc_host_key": key,
            "runs": rows[:200],
        }
    )


def _fmt_duration(sec: float) -> str:
    if sec < 90:
        return f"{sec:.0f} s"
    m = sec / 60.0
    if m < 120:
        return f"{m:.1f} min"
    return f"{m / 60.0:.1f} h"


def _history_rpt(r: dict) -> str:
    return str(r.get("recovery_point_type") or "CRASH_CONSISTENT")


def _rates_from_records(subset: list) -> list[float]:
    rates: list[float] = []
    for r in subset:
        n = int(r.get("n_vms", 0))
        d = float(r.get("duration_sec", 0))
        if n > 0 and d > 0:
            rates.append(d / n)
    return rates


@app.route("/api/estimate")
def api_estimate():
    mode = (request.args.get("snapshot_trigger_mode") or "series").lower()
    if mode not in ("series", "parallel"):
        mode = "series"
    try:
        batch_size = int(request.args.get("batch_size") or 10)
    except ValueError:
        batch_size = 10
    rpt = (request.args.get("recovery_point_type") or "CRASH_CONSISTENT").strip()
    if rpt not in (
        "CRASH_CONSISTENT",
        "APPLICATION_CONSISTENT",
        "RANDOM_CRASH_OR_APP",
    ):
        rpt = "CRASH_CONSISTENT"

    records = load_records(HISTORY_FILE, max_lines=2000)
    if not records:
        return jsonify(
            {
                "ok": False,
                "message": "No completed runs on this server yet. After a successful run, median time per VM is shown here.",
            }
        )

    # Prefer tighter config match first: (trigger, batch, recovery type) down to all runs.
    tiers: list[tuple[str, list]] = [
        (
            "exact_triple",
            [
                r
                for r in records
                if r.get("snapshot_trigger_mode") == mode
                and int(r.get("batch_size", -1)) == batch_size
                and _history_rpt(r) == rpt
            ],
        ),
        (
            "trigger_and_batch",
            [
                r
                for r in records
                if r.get("snapshot_trigger_mode") == mode
                and int(r.get("batch_size", -1)) == batch_size
            ],
        ),
        (
            "trigger_and_recovery_type",
            [
                r
                for r in records
                if r.get("snapshot_trigger_mode") == mode and _history_rpt(r) == rpt
            ],
        ),
        (
            "batch_and_recovery_type",
            [
                r
                for r in records
                if int(r.get("batch_size", -1)) == batch_size and _history_rpt(r) == rpt
            ],
        ),
        (
            "trigger_only",
            [r for r in records if r.get("snapshot_trigger_mode") == mode],
        ),
        (
            "recovery_type_only",
            [r for r in records if _history_rpt(r) == rpt],
        ),
        ("all_runs", list(records)),
    ]

    match_kind = "all_runs"
    rates: list[float] = []
    for label, subset in tiers:
        rates_try = _rates_from_records(subset)
        if rates_try:
            match_kind = label
            rates = rates_try
            break

    if not rates:
        return jsonify(
            {
                "ok": False,
                "message": "No usable samples (need runs where n_vms is greater than 0).",
            }
        )

    med_rate = statistics.median(rates)
    mean_rate = statistics.mean(rates)
    return jsonify(
        {
            "ok": True,
            "sample_count": len(rates),
            "match": match_kind,
            "per_vm_seconds_median": round(med_rate, 2),
            "per_vm_seconds_mean": round(mean_rate, 2),
            "per_vm_human": _fmt_duration(med_rate),
            "examples": {
                "10_vms": _fmt_duration(med_rate * 10),
                "50_vms": _fmt_duration(med_rate * 50),
                "100_vms": _fmt_duration(med_rate * 100),
            },
        }
    )


@app.route("/api/vm_inventory", methods=["POST"])
def api_vm_inventory():
    """List non-CVM mh_vm VMs from Prism Central groups API and return aggregate stats."""
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        pc_ip = str(payload.get("pc_ip") or "").strip()
        pc_user = str(payload.get("pc_user") or "").strip()
        pc_password = str(payload.get("pc_password") or "")
        try:
            group_member_page = int(payload.get("group_member_page") or 500)
        except (TypeError, ValueError):
            group_member_page = 500
        skip_subs = _parse_lines(str(payload.get("skip_substrings") or ""))
        skip_rx = _parse_lines(str(payload.get("skip_regexes") or ""))
        guest_src = payload
    else:
        pc_ip = str(request.form.get("pc_ip") or "").strip()
        pc_user = str(request.form.get("pc_user") or "").strip()
        pc_password = str(request.form.get("pc_password") or "")
        try:
            group_member_page = int(request.form.get("group_member_page") or 500)
        except (TypeError, ValueError):
            group_member_page = 500
        skip_subs = _parse_lines(str(request.form.get("skip_substrings") or ""))
        skip_rx = _parse_lines(str(request.form.get("skip_regexes") or ""))
        guest_src = dict(request.form)

    base_url = _pc_base_url(pc_ip).rstrip("/")
    if not base_url or not pc_user:
        return jsonify({"ok": False, "message": "pc_ip and pc_user are required."}), 400

    rx_err = _validate_regexes(skip_rx)
    if rx_err:
        return jsonify({"ok": False, "message": rx_err}), 400

    group_member_page = max(1, min(group_member_page, 2000))
    guest_mm = _parse_guest_min_memory_mib(guest_src)

    sess = requests.Session()
    sess.auth = (pc_user, pc_password)
    sess.headers["Content-Type"] = "application/json"

    try:
        rows, dups = fetch_vm_inventory_rows(sess, base_url, page_size=group_member_page)
        summary = summarize_inventory_rows(rows)
        summary["ok"] = True
        summary["duplicate_rows_skipped"] = dups
        summary["base_url"] = base_url
        snap_cfg = SnapshotConfig(
            base_url=base_url.rstrip("/"),
            pc_user=pc_user,
            pc_password=pc_password,
            group_member_page=group_member_page,
            skip_substrings=tuple(skip_subs),
            skip_regex_patterns=tuple(skip_rx),
        )
        snap_cfg.compile_regexes()
        _wl, gd = build_guest_disk_worklist(rows, snap_cfg, min_memory_mib=guest_mm)
        summary["guest_disk_eligible"] = len(_wl)
        summary["guest_disk_skipped_name"] = gd["ignored_name"]
        summary["guest_disk_skipped_no_ip"] = gd["skipped_no_ip"]
        summary["guest_disk_skipped_power_off"] = gd["skipped_power_off"]
        summary["guest_disk_skipped_below_min_memory"] = gd["skipped_below_min_memory"]
        summary["guest_disk_min_memory_mib"] = guest_mm
        summary["inventory_cache_id"] = _inventory_cache_store(
            rows,
            _pc_host_key(pc_ip),
            duplicate_rows_skipped=dups,
        )
        return jsonify(summary)
    except requests.HTTPError as e:
        detail = str(e)
        if e.response is not None:
            detail = f"HTTP {e.response.status_code}: {(e.response.text or '')[:800]}"
        return jsonify({"ok": False, "message": detail})
    except requests.RequestException as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/disk_targets_preview", methods=["POST"])
def api_disk_targets_preview():
    """
    Count VMs eligible for guest disk ops (name rules, IP in PC inventory, powered on).
    With ``inventory_cache_id`` from **Fetch VMs**, reuses cached rows (no second Prism groups fetch).
    ``disk_run_limit`` may be an integer or percent (e.g. ``50%``); legacy ``disk_max_vms`` is still accepted.
    """
    payload = request.get_json(silent=True) or {}
    pc_ip = str(payload.get("pc_ip") or "").strip()
    pc_user = str(payload.get("pc_user") or "").strip()
    pc_password = str(payload.get("pc_password") or "")

    base_url = _pc_base_url(pc_ip).rstrip("/")
    if not base_url or not pc_user:
        return jsonify({"ok": False, "message": "pc_ip and pc_user are required."}), 400

    skip_subs = _parse_lines(str(payload.get("skip_substrings") or ""))
    skip_rx = _parse_lines(str(payload.get("skip_regexes") or ""))
    err = _validate_regexes(skip_rx)
    if err:
        return jsonify({"ok": False, "message": err}), 400

    try:
        group_member_page = int(payload.get("group_member_page") or 500)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "group_member_page must be an integer."}), 400

    group_member_page = max(1, min(group_member_page, 2000))
    disk_run_limit = _disk_run_limit_from_payload(payload)
    guest_mm = _parse_guest_min_memory_mib(payload)
    guest_ssh_parallel = _guest_ssh_parallel_from_payload(payload)
    dclus = _disk_cluster_fields_from_payload(payload)
    cache_id = str(payload.get("inventory_cache_id") or "").strip()
    rows: list | None = None
    dup_rows = 0
    from_cache = False
    if cache_id:
        rows, dup_rows, cache_err = _inventory_cache_get(cache_id, _pc_host_key(pc_ip))
        if cache_err:
            return jsonify({"ok": False, "message": cache_err}), 400
        from_cache = True

    preview_cfg = DiskOpConfig(
        base_url=base_url,
        pc_user=pc_user,
        pc_password=pc_password,
        mode="update",
        group_member_page=group_member_page,
        skip_substrings=skip_subs,
        skip_regex_patterns=skip_rx,
        guest_ssh_password="",
        disk_run_limit=disk_run_limit,
        guest_min_memory_mib=guest_mm,
        guest_ssh_parallel=guest_ssh_parallel,
        **dclus,
    )

    try:
        out = preview_guest_disk_targets(
            preview_cfg,
            rows=rows,
            duplicate_inventory_rows=dup_rows,
            from_cache=from_cache,
        )
    except requests.HTTPError as e:
        detail = str(e)
        if e.response is not None:
            detail = f"HTTP {e.response.status_code}: {(e.response.text or '')[:800]}"
        return jsonify({"ok": False, "message": detail})
    except requests.RequestException as e:
        return jsonify({"ok": False, "message": str(e)})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})
    if not out.get("ok"):
        return jsonify(out), 400
    return jsonify(out)


@app.route("/api/start_disk_ops", methods=["POST"])
def api_start_disk_ops():
    """
    Queue a background job that SSHs into each listed guest (first PC-reported IP) and runs:

    - **create** — ``openssl … < /dev/zero | dd`` to the target (``bs`` and ``count`` from the UI; default ``bs=1M``). Optional: ``BULK_SNAP_GUEST_DISK_SPLIT_STAGES=1`` for temp-file + second ``dd`` (2× disk, finer timings).
    - **add** — same pipeline with ``oflag=append conv=notrunc`` (or split-stages if env set)
    - **update** — same pipeline with ``conv=notrunc`` (or split-stages if env set)
    - **delete** — ``rm -f`` on ``guest_delete_glob``
    - **random_mix** — random choice of the four per VM

    Prism creds are only used for inventory; guests need ``guest_ssh_password`` (``sshpass``).
    """
    payload = request.get_json(silent=True) or {}
    pc_ip = str(payload.get("pc_ip") or "").strip()
    pc_user = str(payload.get("pc_user") or "").strip()
    pc_password = str(payload.get("pc_password") or "")
    mode = str(payload.get("disk_op_mode") or "update").strip().lower()

    if mode not in _DISK_OP_MODES:
        return jsonify(
            {
                "ok": False,
                "message": "disk_op_mode must be one of: "
                + ", ".join(sorted(_DISK_OP_MODES)),
            }
        ), 400

    base_url = _pc_base_url(pc_ip).rstrip("/")
    if not base_url or not pc_user:
        return jsonify({"ok": False, "message": "pc_ip and pc_user are required."}), 400

    guest_ssh_user = str(payload.get("guest_ssh_user") or "root").strip() or "root"
    guest_ssh_password = str(payload.get("guest_ssh_password") or "")
    if not guest_ssh_password.strip():
        return jsonify(
            {"ok": False, "message": "guest_ssh_password is required (sshpass to each VM)."}
        ), 400

    skip_subs = _parse_lines(str(payload.get("skip_substrings") or ""))
    skip_rx = _parse_lines(str(payload.get("skip_regexes") or ""))
    err = _validate_regexes(skip_rx)
    if err:
        return jsonify({"ok": False, "message": err}), 400

    try:
        group_member_page = int(payload.get("group_member_page") or 500)
        guest_ssh_port = int(payload.get("guest_ssh_port") or 22)
        guest_ssh_connect_timeout = float(payload.get("guest_ssh_connect_timeout") or 30)
        guest_ssh_command_timeout = float(payload.get("guest_ssh_command_timeout") or 7200)
        create_count_mib = int(payload.get("create_count_mib") or 1024)
        churn_count_mib = int(payload.get("churn_count_mib") or 500)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "Numeric guest/VM parameters invalid."}), 400

    try:
        guest_dd_bs = normalize_guest_dd_bs(str(payload.get("guest_dd_bs") or ""))
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400

    disk_run_limit = _disk_run_limit_from_payload(payload)
    guest_mm = _parse_guest_min_memory_mib(payload)
    guest_ssh_parallel = _guest_ssh_parallel_from_payload(payload)
    dclus = _disk_cluster_fields_from_payload(payload)
    cache_id = str(payload.get("inventory_cache_id") or "").strip()
    inventory_rows: list | None = None
    dup_rows = 0
    inventory_from_cache = False
    if cache_id:
        inventory_rows, dup_rows, cache_err = _inventory_cache_get(cache_id, _pc_host_key(pc_ip))
        if cache_err:
            return jsonify({"ok": False, "message": cache_err}), 400
        inventory_from_cache = True

    guest_target_file = str(payload.get("guest_target_file") or "").strip() or "/root/dummy_snapshot_data_1.img"
    guest_delete_glob = str(payload.get("guest_delete_glob") or "").strip() or "/root/dummy_snapshot_data_*.img"

    seed_raw = payload.get("random_seed")
    random_seed: int | None
    if seed_raw is None or seed_raw == "":
        random_seed = None
    else:
        try:
            random_seed = int(seed_raw)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "message": "random_seed must be an integer or empty."}), 400

    dcfg = DiskOpConfig(
        base_url=base_url,
        pc_user=pc_user,
        pc_password=pc_password,
        mode=mode,
        group_member_page=max(1, min(group_member_page, 2000)),
        skip_substrings=skip_subs,
        skip_regex_patterns=skip_rx,
        random_seed=random_seed,
        guest_ssh_user=guest_ssh_user,
        guest_ssh_password=guest_ssh_password,
        guest_ssh_port=max(1, min(guest_ssh_port, 65535)),
        guest_ssh_connect_timeout=max(5.0, guest_ssh_connect_timeout),
        guest_ssh_command_timeout=max(60.0, guest_ssh_command_timeout),
        guest_target_file=guest_target_file,
        guest_delete_glob=guest_delete_glob,
        guest_dd_bs=guest_dd_bs,
        create_count_mib=max(1, create_count_mib),
        churn_count_mib=max(1, churn_count_mib),
        disk_run_limit=disk_run_limit,
        guest_min_memory_mib=guest_mm,
        guest_ssh_parallel=guest_ssh_parallel,
        **dclus,
    )

    run_id = _enqueue_disk_run(
        dcfg,
        pc_ip,
        inventory_rows=inventory_rows,
        duplicate_inventory_rows=dup_rows,
        inventory_from_cache=inventory_from_cache,
    )
    return jsonify(
        {
            "ok": True,
            "run_id": run_id,
            "job_url": url_for("job_status", run_id=run_id),
        }
    )


@app.route("/api/curator_pe_ips", methods=["POST"])
def api_curator_pe_ips():
    """
    SSH to Prism Central CVM, run ``ncli multicluster get-cluster-state``, return Controller VM IPs.
    Accepts JSON or form: ``pc_ip`` only. Uses ``CURATOR_PC_SSH_*`` (Prism Central CVM).
    """
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        pc_ip = str(payload.get("pc_ip") or "").strip()
    else:
        pc_ip = str(request.form.get("pc_ip") or "").strip()

    if not pc_ip:
        return jsonify({"ok": False, "message": "Prism Central host/IP (pc_ip) is required."}), 400

    raw, err = _ncli_multicluster_state_via_ssh(
        pc_ip,
        CURATOR_PC_SSH_USER,
        CURATOR_PC_SSH_PASSWORD,
        port=CURATOR_PC_SSH_PORT,
    )
    if err:
        return jsonify({"ok": False, "message": err})

    ips = _extract_controller_vm_ips_from_ncli_output(raw or "")
    if not ips:
        return jsonify(
            {
                "ok": False,
                "message": "SSH and ncli succeeded but no Controller VM IP lines were found. "
                "Check multicluster registration on Prism Central.",
                "raw_preview": (raw or "")[:1200],
            }
        )

    return jsonify(
        {
            "ok": True,
            "ips": ips,
            "count": len(ips),
        }
    )


def _execute_curator_run_on_pes(pe_ips: list[str]) -> tuple[list[dict], str | None]:
    """
    Blocking SSH work: scans, optional start task, sleep, poll bg queue per PE.
    Returns ``(results, error_message)`` where ``error_message`` is set if sshpass/ssh is missing.
    """
    results: list[dict] = []
    for pe in pe_ips:
        for cmd in [
            _CURATOR_REMOTE_CLI_GET_SCANS,
            _CURATOR_REMOTE_CLI_GET_BG_TASK_QUEUE_INFO,
            _CURATOR_REMOTE_CLI_START_CURATOR_TASK,
        ]:
            CURATOR_LOG.info("Running command: %s on %s", cmd, pe)
            ec, out, err = _sshpass_ssh_run_remote(
                pe,
                CURATOR_PE_SSH_USER,
                CURATOR_PE_SSH_PASSWORD,
                cmd,
                port=CURATOR_PE_SSH_PORT,
                connect_timeout=20.0,
                command_timeout=180.0,
            )
            if ec == -1 and "not found" in (err or ""):
                return results, (
                    "This server needs OpenSSH and sshpass in PATH to run curator_cli on PEs."
                )
            results.append(
                {
                    "pe": pe,
                    "ok": ec == 0,
                    "exit_code": ec,
                    "stdout": (out or "")[:12000],
                    "stderr": (err or "")[:6000],
                }
            )
            CURATOR_LOG.info("Result: %s", results[-1])
        sleep_sec = max(0, CURATOR_POST_START_SLEEP_SEC)
        if sleep_sec:
            CURATOR_LOG.info("Waiting %s seconds for curator task to start…", sleep_sec)
            time.sleep(sleep_sec)
        i = 0
        while i < CURATOR_BG_QUEUE_MAX_POLLS:
            ec, out, err = _sshpass_ssh_run_remote(
                pe,
                CURATOR_PE_SSH_USER,
                CURATOR_PE_SSH_PASSWORD,
                _CURATOR_REMOTE_CLI_GET_BG_TASK_QUEUE_INFO,
                port=CURATOR_PE_SSH_PORT,
                connect_timeout=20.0,
                command_timeout=180.0,
            )
            if ec != 0:
                CURATOR_LOG.info(
                    "curator_cli get_bg_task_queue_info: exit %s on %s, aborting poll loop.",
                    ec,
                    pe,
                )
                break
            if not (out or "").strip():
                CURATOR_LOG.info(
                    "curator_cli get_bg_task_queue_info: empty stdout on %s, aborting poll loop.",
                    pe,
                )
                break
            if _curator_cli_bg_queue_has_task_rows(out):
                CURATOR_LOG.info(
                    "curator_cli get_bg_task_queue_info: queue has tasks on %s, stopping poll loop.",
                    pe,
                )
                break
            if _curator_cli_bg_queue_output_empty(out) and i >= CURATOR_BG_QUEUE_EMPTY_ABORT_MIN_POLLS:
                CURATOR_LOG.info(
                    "curator_cli get_bg_task_queue_info: still no task rows on %s after %s poll(s), aborting.",
                    pe,
                    i + 1,
                )
                break
            time.sleep(CURATOR_BG_QUEUE_POLL_INTERVAL_SEC)
            i += 1

    return results, None


def _curator_run_background(pe_ips: list[str]) -> None:
    try:
        results, err = _execute_curator_run_on_pes(pe_ips)
        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        with curator_run_lock:
            curator_run_state["finished_at"] = finished_at
            if err:
                curator_run_state["status"] = "error"
                curator_run_state["message"] = err
                curator_run_state["results"] = results
                curator_run_state["top_level_ok"] = False
            else:
                curator_run_state["status"] = "complete"
                curator_run_state["message"] = ""
                curator_run_state["results"] = results
                curator_run_state["top_level_ok"] = bool(results) and all(
                    r.get("ok") for r in results
                )
    except Exception as e:
        logging.getLogger("bulk_snap.curator").exception("Curator background run failed")
        with curator_run_lock:
            curator_run_state["status"] = "error"
            curator_run_state["message"] = str(e)
            curator_run_state["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            curator_run_state["top_level_ok"] = False


@app.route("/api/curator_run_status", methods=["GET"])
def api_curator_run_status():
    """Snapshot of the last / in-progress curator run (for async mode)."""
    with curator_run_lock:
        return jsonify(dict(curator_run_state))


@app.route("/api/curator_run_scans", methods=["POST"])
def api_curator_run_scans():
    """
    On each PE CVM in ``pe_ips``, SSH as ``nutanix`` and run curator_cli steps.

    **Non-blocking:** returns immediately with ``{\"async\": true}``. Poll ``GET /api/curator_run_status``
    until ``status`` is ``complete`` or ``error`` (then use ``results`` / ``message``). Long waits
    (``CURATOR_POST_START_SLEEP_SEC``, SSH) run only in a background thread.

    **Blocking (legacy):** pass ``\"sync\": true`` in JSON or ``?sync=true`` — response includes full
    ``results`` after all work finishes.

    JSON is parsed with ``force=True`` so a body works even if ``Content-Type`` is not set.

    Uses ``CURATOR_PE_SSH_*`` on each PE (not PC credentials).
    """
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        payload = {}
    pe_ips = payload.get("pe_ips")
    if not isinstance(pe_ips, list) or not [x for x in pe_ips if str(x).strip()]:
        pe_ips = list(request.form.getlist("pe_ips"))

    want_sync = str(payload.get("sync") or "").lower() in ("1", "true", "yes")
    if str(request.args.get("sync") or "").lower() in ("1", "true", "yes"):
        want_sync = True

    if not isinstance(pe_ips, list):
        pe_ips = []
    pe_ips = [str(x).strip() for x in pe_ips if str(x).strip()]
    if not pe_ips:
        return jsonify({"ok": False, "message": "Send a JSON body: {\"pe_ips\": [\"10.0.0.1\", ...]}."}), 400

    use_async = not want_sync

    if use_async:
        with curator_run_lock:
            if curator_run_state.get("status") == "running":
                return jsonify(
                    {
                        "ok": False,
                        "async": True,
                        "message": "A curator run is already in progress. Poll /api/curator_run_status.",
                    }
                ), 409
            curator_run_state["status"] = "running"
            curator_run_state["started_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            curator_run_state["finished_at"] = ""
            curator_run_state["pe_count"] = len(pe_ips)
            curator_run_state["results"] = None
            curator_run_state["message"] = ""
            curator_run_state["top_level_ok"] = None

        t = threading.Thread(
            target=_curator_run_background,
            args=(list(pe_ips),),
            daemon=True,
            name="curator-run",
        )
        t.start()
        return jsonify(
            {
                "ok": True,
                "async": True,
                "pe_count": len(pe_ips),
                "status_url": url_for("api_curator_run_status"),
                "message": "Curator run started in background. Poll GET /api/curator_run_status until status is complete or error.",
            }
        )

    results, err = _execute_curator_run_on_pes(pe_ips)
    if err:
        return jsonify({"ok": False, "async": False, "message": err, "results": results})
    return jsonify(
        {
            "ok": all(r["ok"] for r in results) if results else True,
            "async": False,
            "results": results,
        }
    )


def _start_scheduler_worker() -> None:
    _load_schedules_from_disk()
    t = threading.Thread(target=_scheduler_loop, daemon=True, name="bulk-snap-scheduler")
    t.start()


_start_scheduler_worker()

if __name__ == "__main__":
    # 0.0.0.0: reachable via host FQDN/IP from other machines (firewall permitting).
    # Local-only: BULK_SNAP_HOST=127.0.0.1 python app.py
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    host = os.environ.get("BULK_SNAP_HOST", "0.0.0.0")
    port = int(os.environ.get("BULK_SNAP_PORT", "8765"))
    # threaded=False serializes HTTP handlers so Werkzeug access lines don't interleave on the terminal.
    threaded = os.environ.get("BULK_SNAP_THREADED", "1").strip().lower() not in ("0", "false", "no")
    app.run(host=host, port=port, debug=False, threaded=threaded)

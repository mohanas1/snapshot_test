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
    _build_cluster_pe_ip_map,
    build_guest_disk_worklist,
    normalize_guest_dd_bs,
    preview_guest_disk_targets,
    run_disk_ops,
)
from vm_power_runner import PowerOpConfig, run_power_operations
from vm_inventory import fetch_vm_inventory_rows, summarize_inventory_rows
import recovery_points_analyzer
import recovery_points_cache

_DISK_OP_MODES = frozenset({"create", "add", "update", "delete", "random_mix"})
from run_history import append_record, load_records

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024

PROJECT_DIR = Path(__file__).resolve().parent
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Setup SINGLE consolidated log file for entire application
MAIN_LOG_FILE = LOG_DIR / "bulk_snapshots_ui.log"

# Configure root logger to write everything to one file
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Remove any existing handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Single file handler for all logs
main_file_handler = logging.FileHandler(MAIN_LOG_FILE)
main_file_handler.setLevel(logging.DEBUG)
main_file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)-8s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
root_logger.addHandler(main_file_handler)

# Also configure Flask app logger
app.logger.setLevel(logging.DEBUG)
app.logger.propagate = True  # Propagate to root logger

# Log all HTTP requests and responses
@app.before_request
def log_request_info():
    app.logger.info(f">>> REQUEST: {request.method} {request.path} from {request.remote_addr}")
    if request.is_json:
        # Mask passwords in logs
        data = request.get_json()
        if data:
            masked_data = {k: '***' if 'password' in k.lower() else v for k, v in data.items()}
            app.logger.debug(f"    Request JSON: {json.dumps(masked_data, indent=2)}")
    if request.args:
        app.logger.debug(f"    Request Args: {dict(request.args)}")

@app.after_request
def log_response_info(response):
    app.logger.info(f"<<< RESPONSE: {request.method} {request.path} - Status: {response.status_code}")
    return response

app.logger.info("="*100)
app.logger.info("BULK SNAPSHOTS UI APPLICATION STARTED")
app.logger.info(f"Single Log File: {MAIN_LOG_FILE}")
app.logger.info(f"Python Version: {os.sys.version}")
app.logger.info(f"Working Directory: {PROJECT_DIR}")
app.logger.info("="*100)
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


_snapshot_progress_hot: dict[str, dict] = {}
_snapshot_progress_hot_lock = threading.Lock()


def _set_snapshot_progress_hot(run_id: str, snap: dict) -> None:
    with _snapshot_progress_hot_lock:
        _snapshot_progress_hot[run_id] = dict(snap)


def _pop_snapshot_progress_hot(run_id: str) -> None:
    with _snapshot_progress_hot_lock:
        _snapshot_progress_hot.pop(run_id, None)


_power_progress_hot: dict[str, dict] = {}
_power_progress_hot_lock = threading.Lock()


def _set_power_progress_hot(run_id: str, snap: dict) -> None:
    with _power_progress_hot_lock:
        _power_progress_hot[run_id] = dict(snap)


def _pop_power_progress_hot(run_id: str) -> None:
    with _power_progress_hot_lock:
        _power_progress_hot.pop(run_id, None)


schedules_lock = threading.Lock()
# normalized_pc_host_key -> schedule record (see _persist_schedules)
schedules: dict[str, dict] = {}

# Rows from index **VM inventory** - stored on disk as JSON files (one per PC)
INVENTORY_CACHE_DIR = os.path.join(os.path.dirname(__file__), "data", "inventory_cache")
os.makedirs(INVENTORY_CACHE_DIR, exist_ok=True)
inventory_cache_lock = threading.Lock()

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
    # Progress tracking
    "current_pe_index": 0,
    "current_pe_ip": "",
    "completed_pes": 0,
    "pe_ips": [],
    # Per-PE detailed progress
    "pe_progress": {},  # {pe_ip: {"status": "...", "step": "...", "started_at": "...", "finished_at": "..."}}
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


def _get_inventory_cache_file(pc_host_key: str) -> str:
    """Get the JSON file path for a given PC host key."""
    # Sanitize pc_host_key to make it filesystem-safe
    safe_key = pc_host_key.replace(":", "_").replace("/", "_").replace("\\", "_")
    return os.path.join(INVENTORY_CACHE_DIR, f"{safe_key}.json")


def _inventory_cache_store(rows: list, pc_host_key: str, duplicate_rows_skipped: int = 0) -> str:
    """
    Store inventory rows to a JSON file using PC IP as the filename.
    Returns the pc_host_key for consistency with existing code.
    """
    cache_file = _get_inventory_cache_file(pc_host_key)
    
    cache_data = {
        "rows": rows,
        "pc_host_key": pc_host_key,
        "timestamp": time.time(),
        "duplicate_rows_skipped": int(duplicate_rows_skipped or 0),
    }
    
    with inventory_cache_lock:
        try:
            with open(cache_file, "w") as f:
                json.dump(cache_data, f, indent=2)
            app.logger.info(f"Stored VM inventory cache for {pc_host_key}: {len(rows)} rows")
        except Exception as e:
            app.logger.error(f"Failed to write inventory cache to {cache_file}: {e}")
    
    return pc_host_key


def _inventory_cache_get(cache_id: str, pc_host_key: str) -> tuple[list | None, int, str | None]:
    """
    Load inventory rows from JSON file using PC IP as the filename.
    Returns (rows, duplicate_rows_skipped, error_message).
    """
    # Use pc_host_key as the cache key
    cache_key = pc_host_key if pc_host_key else (cache_id or "").strip()
    
    if not cache_key:
        return None, 0, None
    
    cache_file = _get_inventory_cache_file(cache_key)
    
    with inventory_cache_lock:
        try:
            if not os.path.exists(cache_file):
                return None, 0, None
            
            with open(cache_file, "r") as f:
                cache_data = json.load(f)
            
            rows = cache_data.get("rows", [])
            dup_rows = int(cache_data.get("duplicate_rows_skipped", 0))
            
            app.logger.info(f"Loaded VM inventory cache for {cache_key}: {len(rows)} rows")
            return rows, dup_rows, None
            
        except Exception as e:
            app.logger.error(f"Failed to read inventory cache from {cache_file}: {e}")
            return None, 0, None


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
    try:
        pe_port = int(payload.get("pe_cvm_ssh_port") or CURATOR_PE_SSH_PORT)
    except (TypeError, ValueError):
        pe_port = CURATOR_PE_SSH_PORT
    try:
        raw_pp = payload.get("pe_prism_rest_port")
        if raw_pp is None or raw_pp == "":
            pe_prism_rest_port = int(os.environ.get("BULK_SNAP_PE_PRISM_REST_PORT", "9440") or "9440")
        else:
            pe_prism_rest_port = int(raw_pp)
    except (TypeError, ValueError):
        pe_prism_rest_port = 9440
    parallel = _truthy_payload(payload.get("parallel_clusters"))
    # PE CPU throttle is always enabled for disk jobs; payload flag is ignored. Memory is not used.
    return {
        "parallel_clusters": parallel,
        "vm_per_cluster": _int_nonneg_payload(payload, "vm_per_cluster", 0),
        "cluster_pe_top_monitor": True,
        "cluster_cpu_max_pct": _float_payload(payload, "cluster_cpu_max_pct", 85.0),
        "cluster_mem_max_pct": 0.0,
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
        "pe_cvm_ssh_user": (
            str(payload.get("pe_cvm_ssh_user") or "").strip() or CURATOR_PE_SSH_USER
        ),
        "pe_cvm_ssh_password": (
            str(payload.get("pe_cvm_ssh_password") or "").strip() or CURATOR_PE_SSH_PASSWORD
        ),
        "pe_cvm_ssh_port": max(1, min(pe_port, 65535)),
        "pe_prism_rest_port": max(1, min(pe_prism_rest_port, 65535)),
    }


def _resolve_pe_cvm_ips_multiline_for_pc(
    pc_ip: str, payload_multiline: str
) -> tuple[str, str | None]:
    """
    Use non-empty ``pe_cvm_ips_multiline`` from the client as-is; otherwise SSH to Prism Central
    CVM and run ``ncli multicluster get-cluster-state`` (same as ``/api/curator_pe_ips``).
    """
    if (payload_multiline or "").strip():
        return (payload_multiline or "").strip(), None
    host = (pc_ip or "").strip()
    if not host:
        return "", "pc_ip is required to discover PE CVM IPs (ncli on Prism Central)."
    raw, err = _ncli_multicluster_state_via_ssh(
        host,
        CURATOR_PC_SSH_USER,
        CURATOR_PC_SSH_PASSWORD,
        port=CURATOR_PC_SSH_PORT,
    )
    if err:
        return "", err
    ips = _extract_controller_vm_ips_from_ncli_output(raw or "")
    if not ips:
        return (
            "",
            "SSH and ncli on Prism Central succeeded but no Controller VM IP lines were found.",
        )
    return "\n".join(ips), None


def _merge_disk_cluster_with_resolved_pe(
    dclus: dict[str, Any], pc_ip: str
) -> tuple[dict[str, Any], str | None]:
    # Skip PE discovery if parallel_clusters or cluster_pe_top_monitor aren't enabled
    if not dclus.get("parallel_clusters") and not dclus.get("cluster_pe_top_monitor"):
        return dclus, None
    
    line, err = _resolve_pe_cvm_ips_multiline_for_pc(
        pc_ip, str(dclus.get("pe_cvm_ips_multiline") or "")
    )
    if err:
        return dclus, err
    out = dict(dclus)
    out["pe_cvm_ips_multiline"] = line
    return out, None


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


def _active_disk_job_for_pc_key(pc_host_key: str) -> dict | None:
    """If a guest disk job is queued or running for this Prism host, return its run row."""
    key = (pc_host_key or "").strip().lower()
    if not key:
        return None
    with runs_lock:
        for rid, info in runs.items():
            if info.get("job_kind") != "disk":
                continue
            ik = str(info.get("pc_host_key") or "").strip().lower()
            if not ik:
                ik = _pc_host_key(str(info.get("pc_host") or ""))
            if ik != key:
                continue
            st = str(info.get("status") or "").lower()
            if st not in ("queued", "running"):
                continue
            try:
                ju = url_for("job_status", run_id=rid)
            except RuntimeError:
                ju = f"/job/{rid}"
            return {
                "run_id": rid,
                "status": st,
                "job_url": ju,
                "pc_host": str(info.get("pc_host") or ""),
            }
    return None


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

        def _on_snapshot_progress(snap: dict) -> None:
            _set_snapshot_progress_hot(run_id, snap)

        try:
            result = run_snapshots(
                cfg, logger, cancel_ev, progress_callback=_on_snapshot_progress
            )
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
            runs[run_id]["snapshot_progress"] = {
                "overall_done": int(result["n_vms"]),
                "overall_total": int(result["n_vms"]),
            }
    except Exception as e:
        logger.exception("Run failed")
        with runs_lock:
            runs[run_id]["status"] = "error"
            runs[run_id]["error"] = str(e)
            runs[run_id]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    finally:
        _pop_snapshot_progress_hot(run_id)
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
            "disk_op_mode": str(getattr(dcfg, "mode", "") or ""),
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
        dp_final = result.get("disk_progress") or {}
        vmc = (dp_final.get("vm_activity") or {}).get("completed") or []
        with runs_lock:
            rrow = runs.get(run_id, {})
            pc_h_rec = str(rrow.get("pc_host", "") or "")
            pc_k_rec = str(rrow.get("pc_host_key", "") or "") or _pc_host_key(pc_h_rec)
        hist_rec = {
            "job_kind": "disk",
            "run_id": run_id,
            "pc_host": pc_h_rec,
            "pc_host_key": pc_k_rec,
            "at": finished_at,
            "duration_sec": float(result["duration_sec"]),
            "n_vms": int(result["n_vms"]),
            "succeeded": int(result["succeeded"]),
            "failed": int(result["failed"]),
            "disk_op_mode": str(result.get("mode") or ""),
            "parallel_clusters": bool(result.get("parallel_clusters")),
            "guest_ssh_parallel": int(result.get("guest_ssh_parallel") or 0),
            "median_wall_sec_per_vm": _median_disk_vm_wall_sec(vmc),
            "metrics_timeline": dp_final.get("metrics_timeline"),
        }
        append_record(HISTORY_FILE, hist_rec)
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
        with _snapshot_progress_hot_lock:
            shot = _snapshot_progress_hot.get(run_id)
        if shot is not None:
            out["snapshot_progress"] = shot
        with _power_progress_hot_lock:
            power = _power_progress_hot.get(run_id)
        if power is not None:
            out["power_progress"] = power
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


def _median_disk_vm_wall_sec(completed: list) -> float | None:
    vals: list[float] = []
    for r in completed or []:
        if not isinstance(r, dict):
            continue
        try:
            vals.append(float(r.get("seconds")))
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    m = n // 2
    if n % 2:
        return float(s[m])
    return float(s[m - 1] + s[m]) / 2.0


def _summary_from_history_rec(rec: dict) -> dict:
    if str(rec.get("job_kind") or "") == "disk":
        return {
            "job_kind": "disk",
            "n_vms": int(rec.get("n_vms", 0)),
            "duration_sec": float(rec.get("duration_sec", 0)),
            "succeeded": int(rec.get("succeeded", 0)),
            "failed": int(rec.get("failed", 0)),
            "disk_op_mode": str(rec.get("disk_op_mode") or ""),
            "parallel_clusters": bool(rec.get("parallel_clusters")),
            "guest_ssh_parallel": int(rec.get("guest_ssh_parallel") or 0),
            "median_wall_sec_per_vm": rec.get("median_wall_sec_per_vm"),
        }
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


def _index_recent_runs(limit: int = 5) -> list[dict[str, Any]]:
    """Newest-first rows for the index status dashboard (no secrets)."""
    rows = load_records(HISTORY_FILE, max_lines=400)
    if not rows:
        return []
    tail = rows[-limit:]
    tail.reverse()
    out: list[dict[str, Any]] = []
    for r in tail:
        jk = str(r.get("job_kind") or "").strip() or "snapshot"
        out.append(
            {
                "run_id": str(r.get("run_id") or ""),
                "job_kind": jk,
                "at": str(r.get("at") or ""),
                "pc_host": str(r.get("pc_host") or ""),
                "succeeded": r.get("succeeded"),
                "failed": r.get("failed"),
                "duration_sec": r.get("duration_sec"),
            }
        )
    return out


@app.context_processor
def _inject_active_schedules():
    out = {
        "active_schedules": _schedule_summaries(),
        "RANDOM_CRASH_OR_APP": RANDOM_CRASH_OR_APP,
    }
    # History tail only for pages that render index.html (skip on /job/… etc.)
    ep = getattr(request, "endpoint", None)
    if ep in ("index", "start"):
        out["recent_runs"] = _index_recent_runs(5)
    else:
        out["recent_runs"] = []
    return out


@app.route("/")
def index():
    return render_template("index.html", success=request.args.get("success"))


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


def _read_log_text_for_job_api(
    path: Path,
    *,
    status: str,
    job_kind: str = "",
    disk_log_for_console: bool = False,
) -> tuple[str, bool]:
    """
    Body of ``log`` for ``/api/job``. While status is *running*, return only a tail so polls
    stay small (less I/O) and overlap less on the console when ``threaded=True``.
    """
    if job_kind == "disk" and not disk_log_for_console:
        # Disk jobs surface per-VM lines via ``disk_progress.vm_activity`` only; full log is download.
        return "", False
    if job_kind == "power":
        # Power jobs surface per-VM lines via ``power_progress.vm_activity`` only; full log is download.
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
    console_q = request.args.get("console") == "1"
    disk_log_for_console = bool(console_q and jk == "disk")
    text, log_truncated = _read_log_text_for_job_api(
        path,
        status=st,
        job_kind=jk,
        disk_log_for_console=disk_log_for_console,
    )
    summ = info.get("summary")
    disk_mode = str(info.get("disk_op_mode") or "")
    if not disk_mode and isinstance(summ, dict):
        disk_mode = str(summ.get("disk_op_mode") or "")
    return jsonify(
        {
            "status": st,
            "error": info.get("error", ""),
            "log": text,
            "log_truncated": log_truncated,
            "elapsed_running_sec": _elapsed_running_seconds(info),
            "queued_at": info.get("queued_at", ""),
            "running_started_at": info.get("running_started_at") or "",
            "job_kind": jk,
            "disk_op_mode": disk_mode,
            "summary": summ,
            "disk_progress": info.get("disk_progress"),
            "snapshot_progress": info.get("snapshot_progress"),
            "power_progress": info.get("power_progress"),
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

    records = [
        r
        for r in load_records(HISTORY_FILE, max_lines=2000)
        if str(r.get("job_kind") or "") != "disk"
    ]
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


@app.route("/api/pc_reachable", methods=["POST"])
def api_pc_reachable():
    """Quick HTTPS probe to Prism Central for inline connection validation in the UI."""
    payload = request.get_json(silent=True) or {}
    pc_ip = str(payload.get("pc_ip") or "").strip()
    pc_user = str(payload.get("pc_user") or "").strip()
    pc_password = str(payload.get("pc_password") or "")
    base = _pc_base_url(pc_ip)
    if not base:
        return jsonify(
            {"ok": True, "reachable": False, "message": "Enter a Prism Central address."}
        )
    urls = (
        f"{base}/api/nutanix/v3/cluster",
        f"{base}/PrismGateway/services/rest/v1/cluster",
    )
    last_detail = ""
    for url in urls:
        try:
            kw: dict[str, Any] = {"verify": False, "timeout": 8}
            if pc_user:
                kw["auth"] = (pc_user, pc_password)
            r = requests.get(url, **kw)
            if r.status_code < 500:
                return jsonify(
                    {
                        "ok": True,
                        "reachable": True,
                        "http_status": r.status_code,
                    }
                )
            last_detail = f"HTTP {r.status_code}"
        except requests.exceptions.RequestException as e:
            last_detail = str(e)[:220]
            continue
    return jsonify(
        {
            "ok": True,
            "reachable": False,
            "message": last_detail or "No response from Prism Central.",
        }
    )


@app.route("/api/vm_inventory_table", methods=["POST"])
def api_vm_inventory_table():
    """
    Paginated, searchable VM rows from cached inventory.
    Now uses PC IP as cache key - inventory_cache_id is optional.
    """
    payload = request.get_json(silent=True) or {}
    pc_ip = str(payload.get("pc_ip") or "").strip()
    cache_id = str(payload.get("inventory_cache_id") or "").strip()  # Optional, kept for backward compat
    q = str(payload.get("q") or "").strip().lower()
    sort_column = str(payload.get("sort_column") or "").strip()
    sort_direction = str(payload.get("sort_direction") or "").strip()
    try:
        page = int(payload.get("page") or 1)
    except (TypeError, ValueError):
        page = 1
    try:
        page_size = int(payload.get("page_size") or 25)
    except (TypeError, ValueError):
        page_size = 25
    page = max(1, page)
    page_size = max(5, min(100, page_size))

    if not pc_ip:
        return jsonify({"ok": False, "message": "pc_ip is required."}), 400

    # Fetch from cache using PC IP as key (inventory_cache_id is ignored)
    pc_host_key = _pc_host_key(pc_ip)
    rows, _dups, err = _inventory_cache_get(cache_id, pc_host_key)
    
    if not rows:
        return jsonify({"ok": False, "message": "No inventory data cached for this PC. Click 'Fetch Latest VMs Info' to load data."}), 400
    
    # Apply PE cluster filters if provided
    skip_clusters_raw = str(payload.get("skip_clusters") or "").strip()
    if skip_clusters_raw:
        skip_clusters = [c.strip() for c in skip_clusters_raw.split(",") if c.strip()]
        if skip_clusters:
            original_count = len(rows)
            rows = [r for r in rows if str(r.get("cluster_name") or "") not in skip_clusters]
            filtered_count = len(rows)
            app.logger.info(f"Applied cluster filter: {original_count} -> {filtered_count} rows (skipped clusters: {skip_clusters})")
    
    if not rows:
        return jsonify({"ok": False, "message": "No VMs match the selected cluster filters."}), 400

    cluster_names_pe = sorted(
        {str(r.get("cluster_name") or "—") for r in rows},
        key=lambda x: (str(x) == "—", str(x)),
    )
    line_pe, pe_disc_err = _resolve_pe_cvm_ips_multiline_for_pc(pc_ip, "")
    pe_map: dict[str, str] = {}
    if not pe_disc_err and (line_pe or "").strip():
        pe_map = _build_cluster_pe_ip_map(cluster_names_pe, line_pe)

    def row_to_table(r: dict) -> dict:
        ips = r.get("ips") or []
        cn = str(r.get("cluster_name") or "—")
        return {
            "name": r.get("name") or "—",
            "power_state": r.get("power_state") or "unknown",
            "cluster_name": cn,
            "pe_cvm_ip": pe_map.get(cn) or "",
            "ip": ips[0] if ips else "",
            "num_vcpus": r.get("num_vcpus"),
            "memory_mib": r.get("memory_mib"),
            "uuid": str(r.get("uuid") or ""),
        }

    out_rows = [row_to_table(r) for r in rows]

    if q:

        def _match(row: dict) -> bool:
            parts = [
                str(row.get("name") or ""),
                str(row.get("cluster_name") or ""),
                str(row.get("ip") or ""),
                str(row.get("power_state") or ""),
                str(row.get("pe_cvm_ip") or ""),
            ]
            return q in " ".join(parts).lower()

        out_rows = [r for r in out_rows if _match(r)]

    # Apply sorting
    if sort_column and sort_direction in ('asc', 'desc'):
        def get_sort_key(row):
            val = row.get(sort_column)
            
            # Handle None/empty values
            if val is None:
                return ('', 0) if sort_column in ('num_vcpus', 'memory_mib') else ''
            
            # Numeric columns
            if sort_column in ('num_vcpus', 'memory_mib'):
                try:
                    return ('', int(val))
                except (TypeError, ValueError):
                    return ('', 0)
            
            # IP address columns
            if sort_column in ('ip', 'pe_cvm_ip'):
                ip_str = str(val).strip()
                if not ip_str or ip_str == '—':
                    return (0, 0, 0, 0)
                parts = ip_str.split('.')
                if len(parts) == 4:
                    try:
                        return tuple(int(p) for p in parts)
                    except (TypeError, ValueError):
                        return (0, 0, 0, 0)
                return (0, 0, 0, 0)
            
            # String columns (name, power_state, cluster_name)
            return str(val).lower()
        
        out_rows.sort(key=get_sort_key, reverse=(sort_direction == 'desc'))
    else:
        # Default sort by name
        out_rows.sort(key=lambda r: str(r.get("name") or "").lower())
    
    total = len(out_rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    start = (page - 1) * page_size
    slice_rows = out_rows[start : start + page_size]

    return jsonify(
        {
            "ok": True,
            "rows": slice_rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "pe_cvm_ip_discovery_error": pe_disc_err,
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
    
    # Read fetch-time filter options
    fetch_skip_no_ip = bool(payload.get("fetch_skip_no_ip")) if request.is_json else False
    fetch_skip_powered_off = bool(payload.get("fetch_skip_powered_off")) if request.is_json else False
    fetch_skip_low_ram = bool(payload.get("fetch_skip_low_ram")) if request.is_json else False
    fetch_ram_threshold = int(payload.get("fetch_ram_threshold") or 250) if request.is_json else 250
    fetch_skip_clusters_str = str(payload.get("fetch_skip_clusters") or "") if request.is_json else ""
    fetch_skip_clusters = [c.strip() for c in fetch_skip_clusters_str.split(",") if c.strip()]

    sess = requests.Session()
    sess.auth = (pc_user, pc_password)
    sess.headers["Content-Type"] = "application/json"

    try:
        rows, dups = fetch_vm_inventory_rows(sess, base_url, page_size=group_member_page)
        
        # Apply fetch-time filters to reduce returned dataset
        original_count = len(rows)
        filtered_counts = {"no_ip": 0, "powered_off": 0, "low_ram": 0, "clusters": 0}
        
        if fetch_skip_no_ip or fetch_skip_powered_off or fetch_skip_low_ram or fetch_skip_clusters:
            filtered_rows = []
            for row in rows:
                # Check cluster filter
                if fetch_skip_clusters:
                    cluster_name = str(row.get("cluster_name") or "")
                    if cluster_name in fetch_skip_clusters:
                        filtered_counts["clusters"] += 1
                        continue
                
                # Check IP filter
                if fetch_skip_no_ip and not row.get("ips"):
                    filtered_counts["no_ip"] += 1
                    continue
                
                # Check power state filter
                if fetch_skip_powered_off:
                    power = str(row.get("power_state") or "").upper()
                    if power in ("OFF", "UNKNOWN", ""):
                        filtered_counts["powered_off"] += 1
                        continue
                
                # Check RAM filter
                if fetch_skip_low_ram:
                    mem_mib = row.get("memory_mib")
                    if mem_mib is None or int(mem_mib) <= fetch_ram_threshold:
                        filtered_counts["low_ram"] += 1
                        continue
                
                filtered_rows.append(row)
            
            rows = filtered_rows
        
        summary = summarize_inventory_rows(rows)
        summary["fetch_filtered"] = {
            "enabled": fetch_skip_no_ip or fetch_skip_powered_off or fetch_skip_low_ram or bool(fetch_skip_clusters),
            "original_count": original_count,
            "filtered_count": len(rows),
            "skipped_no_ip": filtered_counts["no_ip"],
            "skipped_powered_off": filtered_counts["powered_off"],
            "skipped_low_ram": filtered_counts["low_ram"],
            "skipped_clusters": filtered_counts["clusters"],
            "skipped_cluster_names": fetch_skip_clusters if fetch_skip_clusters else [],
        }
        cluster_names_pe = sorted(
            (r.get("cluster") for r in summary.get("by_cluster") or []),
            key=lambda x: (str(x) == "—", str(x)),
        )
        line_pe, pe_disc_err = _resolve_pe_cvm_ips_multiline_for_pc(pc_ip, "")
        pe_map: dict[str, str] = {}
        if not pe_disc_err and (line_pe or "").strip():
            pe_map = _build_cluster_pe_ip_map(cluster_names_pe, line_pe)
        for row in summary.get("by_cluster") or []:
            if isinstance(row, dict):
                cn = str(row.get("cluster") or "")
                row["pe_cvm_ip"] = pe_map.get(cn) or ""
        summary["pe_cvm_ips_ordered"] = [
            ln.strip() for ln in (line_pe or "").splitlines() if ln.strip()
        ]
        if pe_disc_err:
            summary["pe_cvm_ip_discovery_error"] = pe_disc_err
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
    With ``inventory_cache_id`` from index VM inventory, reuses cached rows (no second Prism groups fetch).
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
    dclus, pe_err = _merge_disk_cluster_with_resolved_pe(dclus, pc_ip)
    if pe_err:
        return jsonify({"ok": False, "message": pe_err}), 400
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
        guest_ssh_password="",  # Preview doesn't need SSH access to guest VMs
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
    dclus, pe_err = _merge_disk_cluster_with_resolved_pe(dclus, pc_ip)
    if pe_err:
        return jsonify({"ok": False, "message": pe_err}), 400
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

    pc_key = _pc_host_key(pc_ip)
    active_disk = _active_disk_job_for_pc_key(pc_key)
    if active_disk:
        return (
            jsonify(
                {
                    "ok": False,
                    "message": (
                        "A disk job is already "
                        + active_disk["status"]
                        + " for this Prism Central (run "
                        + active_disk["run_id"]
                        + "). Open that job or wait for it to finish before starting another."
                    ),
                    "blocked_by_run_id": active_disk["run_id"],
                    "job_url": active_disk["job_url"],
                }
            ),
            409,
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


@app.route("/api/active_disk_job", methods=["POST"])
def api_active_disk_job():
    """Return whether a disk job is queued or running for the given Prism Central host (in-memory session)."""
    payload = request.get_json(silent=True) or {}
    pc_ip = str(payload.get("pc_ip") or "").strip()
    if not pc_ip:
        return jsonify({"ok": False, "message": "pc_ip is required."}), 400
    row = _active_disk_job_for_pc_key(_pc_host_key(pc_ip))
    if not row:
        return jsonify({"ok": True, "busy": False})
    return jsonify(
        {
            "ok": True,
            "busy": True,
            "run_id": row["run_id"],
            "status": row["status"],
            "job_url": row["job_url"],
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


def _update_pe_progress(pe_ip: str, status: str, step: str = "") -> None:
    """Update progress for a specific PE."""
    with curator_run_lock:
        if pe_ip not in curator_run_state["pe_progress"]:
            curator_run_state["pe_progress"][pe_ip] = {
                "status": status,
                "step": step,
                "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "finished_at": "",
                "last_update": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        else:
            curator_run_state["pe_progress"][pe_ip]["status"] = status
            curator_run_state["pe_progress"][pe_ip]["step"] = step
            curator_run_state["pe_progress"][pe_ip]["last_update"] = dt.datetime.now(dt.timezone.utc).isoformat()
            if status in ("complete", "error"):
                curator_run_state["pe_progress"][pe_ip]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()


def _execute_curator_on_single_pe(pe: str, pe_idx: int, total_pes: int) -> tuple[list[dict], str | None]:
    """
    Execute curator commands on a single PE.
    Returns (results_list, error_message).
    """
    CURATOR_LOG.info("=== Starting PE %d/%d: %s ===", pe_idx + 1, total_pes, pe)
    _update_pe_progress(pe, "running", "Connecting via SSH...")
    
    results: list[dict] = []
    cmd_names = ["get_scans", "get_bg_task_queue_info", "start_curator_task"]
    for cmd, cmd_name in zip([
        _CURATOR_REMOTE_CLI_GET_SCANS,
        _CURATOR_REMOTE_CLI_GET_BG_TASK_QUEUE_INFO,
        _CURATOR_REMOTE_CLI_START_CURATOR_TASK,
    ], cmd_names):
        _update_pe_progress(pe, "running", f"Running {cmd_name}...")
        CURATOR_LOG.info("Running command on %s: %s", pe, cmd_name)
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
            _update_pe_progress(pe, "error", "SSH/sshpass not found")
            return results, (
                "This server needs OpenSSH and sshpass in PATH to run curator_cli on PEs."
            )
        results.append(
            {
                "pe": pe,
                "command": cmd_name,
                "ok": ec == 0,
                "exit_code": ec,
                "stdout": (out or "")[:12000],
                "stderr": (err or "")[:6000],
            }
        )
        CURATOR_LOG.info("Result on %s (%s): exit_code=%d", pe, cmd_name, ec)
    sleep_sec = max(0, CURATOR_POST_START_SLEEP_SEC)
    if sleep_sec:
        _update_pe_progress(pe, "waiting", f"Waiting {sleep_sec}s for curator task to start...")
        CURATOR_LOG.info("Waiting %s seconds for curator task to start on %s", sleep_sec, pe)
        time.sleep(sleep_sec)
    
    _update_pe_progress(pe, "polling", "Polling background task queue...")
    i = 0
    while i < CURATOR_BG_QUEUE_MAX_POLLS:
        _update_pe_progress(pe, "polling", f"Poll {i+1}/{CURATOR_BG_QUEUE_MAX_POLLS}...")
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
    
    # Mark this PE as complete
    _update_pe_progress(pe, "complete", "Finished")
    with curator_run_lock:
        curator_run_state["completed_pes"] += 1
    CURATOR_LOG.info("=== Completed PE: %s ===", pe)
    
    return results, None


def _execute_curator_run_on_pes(pe_ips: list[str]) -> tuple[list[dict], str | None]:
    """
    Execute curator commands on all PEs in PARALLEL using ThreadPoolExecutor.
    Returns ``(results, error_message)`` where ``error_message`` is set if sshpass/ssh is missing.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    CURATOR_LOG.info("Starting PARALLEL curator execution on %d PEs", len(pe_ips))
    
    all_results: list[dict] = []
    error_msg: str | None = None
    
    # Initialize PE progress for all PEs
    for pe in pe_ips:
        _update_pe_progress(pe, "pending", "Waiting to start...")
    
    # Run PEs in parallel with max 4 concurrent workers
    max_workers = min(len(pe_ips), 4)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="curator-pe") as executor:
        # Submit all PE jobs
        future_to_pe = {
            executor.submit(_execute_curator_on_single_pe, pe, idx, len(pe_ips)): pe
            for idx, pe in enumerate(pe_ips)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_pe):
            pe = future_to_pe[future]
            try:
                pe_results, pe_error = future.result()
                all_results.extend(pe_results)
                if pe_error and not error_msg:
                    error_msg = pe_error
                    # If sshpass/ssh is missing, mark all pending PEs as error
                    for other_pe in pe_ips:
                        with curator_run_lock:
                            if curator_run_state["pe_progress"].get(other_pe, {}).get("status") == "pending":
                                _update_pe_progress(other_pe, "error", "Cancelled due to SSH error")
            except Exception as e:
                CURATOR_LOG.exception("Exception processing PE %s", pe)
                _update_pe_progress(pe, "error", f"Exception: {str(e)}")
                all_results.append({
                    "pe": pe,
                    "ok": False,
                    "error": str(e)
                })
    
    CURATOR_LOG.info("PARALLEL curator execution complete - processed %d PEs", len(pe_ips))
    return all_results, error_msg


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
        pc_for_pe = str(payload.get("pc_ip") or "").strip()
        line, cerr = _resolve_pe_cvm_ips_multiline_for_pc(pc_for_pe, "")
        if cerr:
            return jsonify(
                {
                    "ok": False,
                    "message": cerr
                    + ' Or pass {"pe_ips": ["10.0.0.1", ...]} with an explicit list.',
                }
            ), 400
        pe_ips = [x.strip() for x in line.splitlines() if x.strip()]
    if not pe_ips:
        return jsonify(
            {
                "ok": False,
                "message": "No PE CVM IPs after ncli discovery. Send pc_ip or pe_ips in JSON.",
            }
        ), 400

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
            # Progress tracking
            curator_run_state["current_pe_index"] = 0
            curator_run_state["current_pe_ip"] = ""
            curator_run_state["completed_pes"] = 0
            curator_run_state["pe_ips"] = list(pe_ips)
            curator_run_state["pe_progress"] = {}  # Reset per-PE progress

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


@app.route("/recovery_points")
def recovery_points():
    """Render the Recovery Points Analysis page."""
    return render_template("recovery_points.html")


@app.route("/fetch_logs")
def fetch_logs():
    """Render the Fluentd Log Fetcher page."""
    return render_template("fetch_logs.html")


@app.route("/api/list_filer_folders", methods=["GET"])
def api_list_filer_folders():
    """List folders on filer."""
    path = request.args.get("path", "/home/nutanix/data/Bugs")
    filer_password = request.args.get("filer_password", "nutanix/4u")
    filer_ip = request.args.get("filer_ip", "10.46.1.165")
    filer_user = request.args.get("filer_user", "nutanix")
    
    try:
        # Use SSH to list directories
        import base64
        credentials = f"{filer_user}:{filer_password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        
        # Execute SSH command to list directories
        cmd = f"sshpass -p '{filer_password}' ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR {filer_user}@{filer_ip} 'ls -d {path}/*/ 2>/dev/null | xargs -n1 basename'"
        
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            folders = [f.strip() for f in result.stdout.split('\n') if f.strip()]
            return jsonify({"success": True, "folders": folders})
        else:
            return jsonify({"success": False, "error": "Failed to list folders", "folders": []})
    
    except Exception as e:
        app_logger.exception("Error listing filer folders")
        return jsonify({"success": False, "error": str(e), "folders": []})


@app.route("/api/check_recovery_cache")
def api_check_recovery_cache():
    """Check if cached recovery points data exists for a PC IP."""
    pc_ip = request.args.get("pc_ip", "").strip()
    
    if not pc_ip:
        return jsonify({"has_cache": False}), 400
    
    cached_result = recovery_points_cache.get_cached_result(pc_ip)
    
    if cached_result:
        return jsonify({
            "has_cache": True,
            "cached_at": cached_result.get("cached_at"),
            "total_vms": cached_result.get("summary", {}).get("total_vms", 0)
        })
    else:
        return jsonify({"has_cache": False})


@app.route("/api/vm_snapshot", methods=["POST"])
def api_vm_snapshot():
    """Create a snapshot for a single VM."""
    import requests
    
    data = request.get_json()
    app.logger.info(f"=== VM Snapshot Request Started ===")
    app.logger.info(f"Request data: {json.dumps({k: v for k, v in data.items() if k != 'pc_password'})}")
    
    pc_ip = data.get("pc_ip", "").strip()
    pc_user = data.get("pc_user", "").strip()
    pc_password = data.get("pc_password", "").strip()
    vm_uuid = data.get("vm_uuid", "").strip()
    vm_name = data.get("vm_name", "").strip()
    expiration_days = int(data.get("expiration_days", 30))
    recovery_point_type = data.get("recovery_point_type", "CRASH_CONSISTENT")
    task_timeout_sec = int(data.get("task_timeout_sec", 300))
    
    if not all([pc_ip, pc_user, pc_password, vm_uuid]):
        return jsonify({
            "ok": False,
            "error": "Missing required fields: pc_ip, pc_user, pc_password, vm_uuid"
        }), 400
    
    try:
        # Use v3 API (same as bulk snapshot runner) - PROVEN TO WORK
        base_url = f"https://{pc_ip}:9440"
        
        # Calculate expiration time
        import datetime
        
        expiration_time = (datetime.datetime.utcnow() + datetime.timedelta(days=expiration_days)).isoformat() + "Z"
        snapshot_name = f"Snapshot_{vm_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Use v3 API payload format (same as bulk snapshot runner)
        snapshot_payload = {
            "name": snapshot_name,
            "recovery_point_type": recovery_point_type,  # v3 uses recovery_point_type not consistencyType
            "expiration_time": expiration_time,          # v3 uses expiration_time not expirationTime
        }
        
        api_endpoint = f"{base_url}/api/nutanix/v3/vms/{vm_uuid}/snapshot"
        
        app.logger.info(f"=== SINGLE VM SNAPSHOT (v3 API - same as bulk) ===")
        app.logger.info(f"VM UUID: {vm_uuid}")
        app.logger.info(f"VM Name: {vm_name}")
        app.logger.info(f"API Endpoint: {api_endpoint}")
        app.logger.info(f"Payload: {json.dumps(snapshot_payload, indent=2)}")
        
        response = requests.post(
            api_endpoint,
            auth=(pc_user, pc_password),
            json=snapshot_payload,
            verify=False,
            timeout=task_timeout_sec
        )
        
        app.logger.info(f"Response Status: {response.status_code}")
        app.logger.info(f"Response Body: {response.text}")
        
        if response.status_code in (200, 201, 202):
            result = response.json()
            
            # Extract task_uuid from response (v3 API format)
            task_uuid = result.get("task_uuid") or (
                (result.get("status") or {}).get("execution_context") or {}
            ).get("task_uuid")
            
            app.logger.info(f"✅ Snapshot task submitted successfully for {vm_name}")
            app.logger.info(f"Task UUID: {task_uuid}")
            
            return jsonify({
                "ok": True,
                "message": f"Snapshot created successfully for {vm_name}",
                "task_uuid": task_uuid,
                "details": result
            })
        else:
            app.logger.error(f"❌ Snapshot failed with status {response.status_code}")
            app.logger.error(f"Error response: {response.text}")
            return jsonify({
                "ok": False,
                "error": f"API returned status {response.status_code}: {response.text}"
            }), response.status_code
            
    except Exception as e:
        app.logger.error(f"❌ Exception in snapshot: {str(e)}", exc_info=True)
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500


@app.route("/api/vm_disk_operation", methods=["POST"])
def api_vm_disk_operation():
    """Perform disk operation on a single VM."""
    import subprocess
    
    data = request.get_json()
    app.logger.info(f"=== VM Disk Operation Request Started ===")
    app.logger.info(f"Request data: {json.dumps({k: v for k, v in data.items() if 'password' not in k.lower()})}")
    
    pc_ip = data.get("pc_ip", "").strip()
    pc_user = data.get("pc_user", "").strip()
    pc_password = data.get("pc_password", "").strip()
    vm_uuid = data.get("vm_uuid", "").strip()
    vm_name = data.get("vm_name", "").strip()
    vm_ip = data.get("vm_ip", "").strip()  # Try to get IP from frontend
    disk_op_mode = data.get("disk_op_mode", "update").strip()
    guest_ssh_user = data.get("guest_ssh_user", "root").strip()
    guest_ssh_password = data.get("guest_ssh_password", "").strip()
    guest_target_file = data.get("guest_target_file", "/root/dummy_snapshot_data_1.img").strip()
    guest_dd_bs = data.get("guest_dd_bs", "1M").strip()
    churn_count_mib = int(data.get("churn_count_mib", 500))
    guest_ssh_command_timeout = int(data.get("guest_ssh_command_timeout", 7200))
    
    if not all([pc_ip, pc_user, pc_password, vm_uuid, guest_ssh_password]):
        return jsonify({
            "ok": False,
            "error": "Missing required fields"
        }), 400
    
    try:
        # If VM IP not provided by frontend, fetch it from Prism Central
        if not vm_ip:
            base_url = f"https://{pc_ip}:9440"
            
            # Get VM details via v3 API (simpler than Groups API)
            vm_response = requests.get(
                f"{base_url}/api/nutanix/v3/vms/{vm_uuid}",
                auth=(pc_user, pc_password),
                verify=False,
                timeout=30
            )
            
            if vm_response.status_code != 200:
                app.logger.error(f"Failed to fetch VM {vm_uuid}: {vm_response.status_code} - {vm_response.text}")
                return jsonify({
                    "ok": False,
                    "error": f"Failed to fetch VM details (HTTP {vm_response.status_code})"
                }), 500
            
            vm_data = vm_response.json()
            
            # Extract IP from NIC list
            vm_ip = None
            nic_list = vm_data.get("status", {}).get("resources", {}).get("nic_list", [])
            for nic in nic_list:
                ip_list = nic.get("ip_endpoint_list", [])
                if ip_list and ip_list[0].get("ip"):
                    vm_ip = ip_list[0]["ip"]
                    break
        
        if not vm_ip:
            return jsonify({
                "ok": False,
                "error": f"No IP address found for VM {vm_name}"
            }), 404
        
        # Execute disk operation via SSH
        if disk_op_mode == "delete":
            cmd = f"rm -f {guest_target_file}"
        elif disk_op_mode == "create":
            cmd = f"openssl enc -aes-256-ctr -pass pass:$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64) -nosalt < /dev/zero | dd of={guest_target_file} bs={guest_dd_bs} count={churn_count_mib} conv=fsync 2>&1"
        elif disk_op_mode == "add":
            cmd = f"openssl enc -aes-256-ctr -pass pass:$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64) -nosalt < /dev/zero | dd of={guest_target_file} bs={guest_dd_bs} count={churn_count_mib} oflag=append conv=notrunc,fsync 2>&1"
        elif disk_op_mode == "update":
            cmd = f"openssl enc -aes-256-ctr -pass pass:$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64) -nosalt < /dev/zero | dd of={guest_target_file} bs={guest_dd_bs} count={churn_count_mib} conv=notrunc,fsync 2>&1"
        else:
            return jsonify({
                "ok": False,
                "error": f"Invalid disk operation mode: {disk_op_mode}"
            }), 400
        
        # Use sshpass to execute command
        ssh_cmd = [
            "sshpass", "-p", guest_ssh_password,
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-o", f"ConnectTimeout=300",
            f"{guest_ssh_user}@{vm_ip}",
            cmd
        ]
        
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=guest_ssh_command_timeout
        )
        
        if result.returncode == 0:
            return jsonify({
                "ok": True,
                "message": f"Disk operation '{disk_op_mode}' completed successfully on {vm_name}",
                "details": result.stdout,
                "vm_ip": vm_ip
            })
        else:
            return jsonify({
                "ok": False,
                "error": f"Disk operation failed: {result.stderr or result.stdout}"
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({
            "ok": False,
            "error": f"Operation timed out after {guest_ssh_command_timeout} seconds"
        }), 500
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500


@app.route("/api/analyze_recovery_points", methods=["POST"])
def api_analyze_recovery_points():
    """API endpoint to analyze recovery points for all VMs with streaming logs."""
    from flask import Response
    import queue
    
    data = request.get_json()
    
    pc_ip = data.get("pc_ip", "").strip()
    pc_user = data.get("pc_user", "").strip()
    pc_password = data.get("pc_password", "").strip()
    concurrency = int(data.get("concurrency", 5))
    fetch_latest = data.get("fetch_latest", False)
    
    # Validate inputs
    if not pc_ip or not pc_user or not pc_password:
        return jsonify({
            "ok": False,
            "error": "Missing required fields: pc_ip, pc_user, pc_password"
        }), 400
    
    # Validate concurrency
    if concurrency < 1 or concurrency > 20:
        return jsonify({
            "ok": False,
            "error": "Concurrency must be between 1 and 20"
        }), 400
    
    def generate():
        """Generator for Server-Sent Events"""
        log_queue = queue.Queue()
        result = [None]
        error = [None]
        from_cache = [False]
        
        def progress_callback(message):
            log_queue.put(f"data: {json.dumps({'type': 'log', 'message': message})}\n\n")
        
        def run_analysis():
            try:
                # Check cache if fetch_latest is False
                if not fetch_latest:
                    cached_result = recovery_points_cache.get_cached_result(pc_ip)
                    if cached_result:
                        progress_callback(f"✅ Loaded cached results from {cached_result['cached_at']}")
                        progress_callback(f"📌 Use 'Fetch Latest' to refresh data from Prism Central")
                        result[0] = cached_result['summary']
                        from_cache[0] = True
                        return
                
                # Run fresh analysis
                if fetch_latest:
                    progress_callback("🔄 Fetching latest recovery points from Prism Central...")
                
                summary = recovery_points_analyzer.analyze_recovery_points(
                    pc_ip=pc_ip,
                    pc_user=pc_user,
                    pc_password=pc_password,
                    concurrency=concurrency,
                    progress_callback=progress_callback
                )
                result[0] = summary
                
                # Save to cache after successful analysis
                recovery_points_cache.save_result(pc_ip, summary)
                progress_callback(f"💾 Results cached for future use")
                
            except Exception as e:
                error[0] = str(e)
            finally:
                log_queue.put(None)  # Signal completion
        
        # Start analysis in background thread
        analysis_thread = threading.Thread(target=run_analysis, daemon=True)
        analysis_thread.start()
        
        # Stream logs
        while True:
            try:
                msg = log_queue.get(timeout=30)
                if msg is None:  # Completion signal
                    break
                yield msg
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
        
        # Send final result or error
        if error[0]:
            yield f"data: {json.dumps({'type': 'error', 'error': error[0]})}\n\n"
        else:
            yield f"data: {json.dumps({'type': 'complete', 'summary': result[0], 'from_cache': from_cache[0]})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream')



def _start_scheduler_worker() -> None:
    _load_schedules_from_disk()
    t = threading.Thread(target=_scheduler_loop, daemon=True, name="bulk-snap-scheduler")
    t.start()


_start_scheduler_worker()


@app.route("/api/fetch_fluentd_logs", methods=["POST"])
def api_fetch_fluentd_logs():
    """API endpoint to fetch fluentd logs from PC and upload to filer with streaming logs."""
    from flask import Response
    import queue
    
    data = request.get_json()
    pc_ip = data.get("pc_ip", "").strip()
    bug_folder = data.get("bug_folder", "").strip()
    pc_password = data.get("pc_password", "nutanix/4u").strip()
    filer_password = data.get("filer_password", "nutanix/4u").strip()
    filer_ip = data.get("filer_ip", "10.46.1.165").strip()
    filer_user = data.get("filer_user", "nutanix").strip()
    filer_base_path = data.get("filer_base_path", "/home/nutanix/data/Bugs").strip()
    
    # Get filter selections
    fluentd_namespaces = data.get("fluentd_namespaces", [])
    logbay_services = data.get("logbay_services", [])
    logbay_duration = data.get("logbay_duration", {})
    
    if not pc_ip:
        return jsonify({"success": False, "error": "PC IP is required"}), 400
    
    if not bug_folder:
        bug_folder = f"temp_{pc_ip}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    log_queue = queue.Queue()
    
    def send_log(message, level="INFO"):
        """Helper to send log messages to the queue and project log."""
        timestamp = dt.datetime.now().strftime("%H:%M:%S")
        log_queue.put({"timestamp": timestamp, "level": level, "message": message})
        
        # Also write to project log file
        log_prefix = f"[fetch_fluentd_logs|{pc_ip}]"
        if level == "ERROR":
            app.logger.error(f"{log_prefix} {message}")
        elif level == "WARNING":
            app.logger.warning(f"{log_prefix} {message}")
        else:
            app.logger.info(f"{log_prefix} {message}")
    
    def run_fetch_script():
        """Run the fetch_and_upload_fluentd_logs.sh script."""
        try:
            script_path = PROJECT_DIR / "fetch_and_upload_fluentd_logs.sh"
            
            if not script_path.exists():
                send_log(f"Script not found: {script_path}", "ERROR")
                send_log("END", "INFO")
                return
            
            send_log(f"Starting log fetch for PC: {pc_ip}", "INFO")
            send_log(f"Bug folder: {bug_folder}", "INFO")
            send_log("=" * 80, "INFO")
            
            # Build command
            cmd = [
                "bash",
                str(script_path),
                pc_ip,
                pc_password,
                bug_folder
            ]
            
            # Run the script
            env = os.environ.copy()
            env["FILER_PASSWORD"] = filer_password
            env["FILER_HOST"] = filer_ip  # Script expects FILER_HOST
            env["FILER_USER"] = filer_user
            env["FILER_BASE_PATH"] = filer_base_path
            
            # Pass selected namespaces if any are specified
            if fluentd_namespaces:
                env["FLUENTD_NAMESPACES"] = ",".join(fluentd_namespaces)
                send_log(f"Selected namespaces: {', '.join(fluentd_namespaces)}", "INFO")
            else:
                # No namespaces selected - signal to skip fluentd compression
                env["FLUENTD_NAMESPACES"] = "NONE"
                send_log("No fluentd namespaces selected - skipping fluentd log collection", "INFO")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env
            )
            
            # Stream output
            for line in iter(process.stdout.readline, ''):
                if line:
                    clean_line = line.rstrip()
                    
                    # Determine log level based on content
                    if "ERROR" in clean_line or "✗" in clean_line or "Failed" in clean_line:
                        level = "ERROR"
                    elif "WARNING" in clean_line or "⚠" in clean_line:
                        level = "WARNING"
                    elif "SUCCESS" in clean_line or "✓" in clean_line:
                        level = "SUCCESS"
                    else:
                        level = "INFO"
                    
                    send_log(clean_line, level)
            
            process.wait()
            
            if process.returncode == 0:
                send_log("=" * 80, "INFO")
                send_log("✅ Fluentd log fetch completed successfully!", "SUCCESS")
                
                # Run logbay collection if services are selected
                if logbay_services:
                    send_log("=" * 80, "INFO")
                    send_log("Starting Logbay collection...", "INFO")
                    run_logbay_collection()
            else:
                send_log("=" * 80, "INFO")
                send_log(f"❌ Script exited with code {process.returncode}", "ERROR")
            
        except Exception as e:
            send_log(f"Exception during log fetch: {str(e)}", "ERROR")
            app_logger.exception("Error in fetch_fluentd_logs")
        finally:
            send_log("END", "INFO")
    
    def run_logbay_collection():
        """Run logbay collection on PC for selected services."""
        total_services = len(logbay_services)
        successful_collections = 0
        failed_collections = 0
        
        try:
            send_log("=" * 80, "INFO")
            send_log(f"Step 8/8: Logbay Collection ({total_services} service(s))", "INFO")
            send_log("=" * 80, "INFO")
            
            # Build logbay command with password in ftp URL
            # Format: ftp://user:password@host/path
            from urllib.parse import quote
            # URL-encode the password to handle special characters
            encoded_password = quote(filer_password, safe='')
            filer_dest = f"ftp://{filer_user}:{encoded_password}@{filer_ip}/{filer_base_path}/{bug_folder}"
            
            # Build duration parameter
            if logbay_duration.get('type') == 'recent':
                hours = logbay_duration.get('hours', 24)
                duration_param = f"--duration=-{hours}h"
                send_log(f"Duration: Last {hours} hours", "INFO")
            else:
                # Custom duration
                from_date = logbay_duration.get('from_date')
                from_time = logbay_duration.get('from_time', '09:00')
                duration_hours = logbay_duration.get('duration_hours', 2)
                # Format: 2025/12/16-09:00:00
                from_datetime = f"{from_date.replace('-', '/')}-{from_time}:00"
                duration_param = f"--from={from_datetime} --duration=+{duration_hours}h"
                send_log(f"Duration: From {from_datetime}, +{duration_hours} hours", "INFO")
            
            # Run logbay for each selected service
            for idx, service in enumerate(logbay_services, 1):
                send_log("", "INFO")
                send_log(f"Substep {idx}/{total_services}: Collecting {service} logs", "INFO")
                send_log("-" * 80, "INFO")
                
                # Build logbay command - use expect with heredoc to handle password prompt
                filer_dest = f"ftp://{filer_user}@{filer_ip}/{filer_base_path}/{bug_folder}"
                
                # Use heredoc to pass expect script directly (avoids /tmp noexec issue)
                logbay_cmd = f"""expect << 'EXPECT_EOF'
set timeout 600
spawn ~/ncc/bin/logbay collect -D={filer_dest} -t {service} -O run_all=true,msp_pod=true,msp_systemd=true,kubectl_cmds=true,persistent=true {duration_param}
expect {{
    "password:" {{
        send "{filer_password}\\r"
        exp_continue
    }}
    "Password:" {{
        send "{filer_password}\\r"
        exp_continue
    }}
    "Enter password:" {{
        send "{filer_password}\\r"
        exp_continue
    }}
    eof
}}
wait
EXPECT_EOF
"""
                
                send_log(f"Command: ~/ncc/bin/logbay collect -D={filer_dest} -t {service} -O run_all=true,msp_pod=true,msp_systemd=true,kubectl_cmds=true,persistent=true {duration_param}", "INFO")
                
                # Execute via SSH
                ssh_cmd = [
                    "sshpass", "-p", pc_password,
                    "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null",
                    f"nutanix@{pc_ip}",
                    logbay_cmd
                ]
                
                process = subprocess.Popen(
                    ssh_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                
                # Stream output and detect errors
                command_not_found = False
                permission_denied = False
                has_error = False
                error_messages = []
                for line in iter(process.stdout.readline, ''):
                    if line:
                        clean_line = line.rstrip()
                        
                        # Detect command not found
                        if "command not found" in clean_line.lower() or "logbay: command not found" in clean_line:
                            command_not_found = True
                            has_error = True
                            error_messages.append(clean_line)
                            send_log(clean_line, "ERROR")
                        # Detect permission denied
                        elif "permission denied" in clean_line.lower():
                            permission_denied = True
                            has_error = True
                            error_messages.append(clean_line)
                            send_log(clean_line, "ERROR")
                        # Detect SFTP/FTP connection errors
                        elif "unable to connect" in clean_line.lower() or "connection refused" in clean_line.lower() or "connection failed" in clean_line.lower():
                            has_error = True
                            error_messages.append(clean_line)
                            send_log(clean_line, "ERROR")
                        # Detect other errors
                        elif "error" in clean_line.lower() or "failed" in clean_line.lower():
                            # Track errors but filter out harmless messages
                            if not any(ignore in clean_line.lower() for ignore in ["warning", "info", "debug"]):
                                has_error = True
                                error_messages.append(clean_line)
                            send_log(clean_line, "ERROR")
                        elif "warning" in clean_line.lower():
                            send_log(clean_line, "WARNING")
                        else:
                            send_log(clean_line, "INFO")
                
                process.wait()
                
                # Check for errors
                if command_not_found:
                    send_log(f"❌ Logbay command not found on PC {pc_ip}", "ERROR")
                    send_log(f"   Please ensure logbay is installed and available in PATH", "ERROR")
                    failed_collections += 1
                elif permission_denied:
                    send_log(f"❌ Logbay collection for {service} failed: Permission denied", "ERROR")
                    send_log(f"   Check SFTP permissions or ensure expect is installed on the PC", "ERROR")
                    failed_collections += 1
                elif process.returncode == 127:
                    send_log(f"❌ Logbay collection for {service} failed: command not found (exit code 127)", "ERROR")
                    failed_collections += 1
                elif has_error or process.returncode != 0:
                    # Failed due to errors in output or non-zero exit code
                    if error_messages:
                        send_log(f"❌ Logbay collection for {service} failed: {error_messages[0][:100]}", "ERROR")
                    else:
                        send_log(f"❌ Logbay collection for {service} failed with exit code {process.returncode}", "ERROR")
                    failed_collections += 1
                else:
                    send_log(f"✅ Logbay collection for {service} completed successfully", "SUCCESS")
                    successful_collections += 1
            
            # Summary
            send_log("=" * 80, "INFO")
            if failed_collections == 0:
                send_log(f"✅ All logbay collections completed successfully! ({successful_collections}/{total_services})", "SUCCESS")
            elif successful_collections == 0:
                send_log(f"❌ All logbay collections failed! ({failed_collections}/{total_services})", "ERROR")
            else:
                send_log(f"⚠️  Logbay collections completed with errors: {successful_collections} succeeded, {failed_collections} failed", "WARNING")
            
        except Exception as e:
            send_log(f"Exception during logbay collection: {str(e)}", "ERROR")
            app.logger.exception("Error in logbay collection")
    
    # Start the fetch in background thread
    thread = threading.Thread(target=run_fetch_script, daemon=True)
    thread.start()
    
    def generate():
        """Generator for Server-Sent Events."""
        while True:
            try:
                msg = log_queue.get(timeout=1)
                if msg["message"] == "END":
                    break
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping': True})}\n\n"
    
    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/start_power_ops", methods=["POST"])
def api_start_power_ops():
    """
    Start VM power on/off operations.
    
    Expected JSON payload:
    {
        "pc_ip": "10.46.117.165",
        "pc_user": "admin",
        "pc_password": "Nutanix.123",
        "power_action": "on",  // or "off"
        "vm_uuids": ["uuid1", "uuid2", ...],
        "concurrent_ops": 5,
        "check_interval": 5,
        "max_retries": 12
    }
    """
    payload = request.get_json(silent=True) or {}
    pc_ip = str(payload.get("pc_ip") or "").strip()
    pc_user = str(payload.get("pc_user") or "").strip()
    pc_password = str(payload.get("pc_password") or "")
    power_action = str(payload.get("power_action") or "on").strip().lower()
    vm_uuids = payload.get("vm_uuids", [])
    
    if not pc_ip or not pc_user or not pc_password:
        return jsonify({"ok": False, "message": "pc_ip, pc_user, and pc_password are required."}), 400
    
    if power_action not in ("on", "off"):
        return jsonify({"ok": False, "message": "power_action must be 'on' or 'off'."}), 400
    
    if not vm_uuids or not isinstance(vm_uuids, list):
        return jsonify({"ok": False, "message": "vm_uuids must be a non-empty list."}), 400
    
    try:
        concurrent_ops = int(payload.get("concurrent_ops", 5))
        check_interval = int(payload.get("check_interval", 5))
        max_retries = int(payload.get("max_retries", 12))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "Numeric parameters invalid."}), 400
    
    pc_key = _pc_host_key(pc_ip)
    
    # Check if there's already an active power job for this PC
    with runs_lock:
        for rid, rdict in runs.items():
            if (
                rdict.get("job_kind") == "power"
                and rdict.get("pc_host_key") == pc_key
                and rdict.get("status") in ("queued", "running")
            ):
                return jsonify({
                    "ok": False,
                    "message": f"A power job is already {rdict['status']} for this PC (run {rid}).",
                    "blocked_by_run_id": rid,
                    "job_url": url_for("job_status", run_id=rid, _external=True)
                }), 409
    
    # Create config
    try:
        config = PowerOpConfig(
            pc_host=pc_ip,
            pc_user=pc_user,
            pc_password=pc_password,
            power_action=power_action,
            vm_uuids=vm_uuids,
            concurrent_ops=max(1, min(concurrent_ops, 20)),
            check_interval=max(1, check_interval),
            max_retries=max(1, max_retries),
        )
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400
    
    # Queue the job
    run_id = str(uuid.uuid4())
    log_file = LOG_DIR / f"power_{run_id}.log"
    
    run_record = {
        "run_id": run_id,
        "job_kind": "power",
        "pc_host_key": pc_key,
        "status": "queued",
        "queued_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "log_path": str(log_file),
        "power_action": power_action,
        "total_vms": len(vm_uuids),
        "successful": 0,
        "failed": 0,
    }
    
    with runs_lock:
        runs[run_id] = run_record
    
    cancel_event = threading.Event()
    run_record["cancel_event"] = cancel_event
    
    def progress_callback(progress_snap: dict):
        _set_power_progress_hot(run_id, progress_snap)
    
    def worker():
        with runs_lock:
            runs[run_id]["status"] = "running"
            runs[run_id]["running_started_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        
        try:
            summary = run_power_operations(
                config,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
                log_file_path=str(log_file)
            )
            
            with runs_lock:
                runs[run_id]["status"] = "complete"
                runs[run_id]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
                runs[run_id]["successful"] = summary["successful"]
                runs[run_id]["failed"] = summary["failed"]
                runs[run_id]["summary"] = summary
            
            app.logger.info(f"Power {power_action} job {run_id} completed: {summary['successful']}/{summary['total']} successful")
            
        except Exception as e:
            app.logger.error(f"Power job {run_id} failed with error: {e}", exc_info=True)
            with runs_lock:
                runs[run_id]["status"] = "error"
                runs[run_id]["error"] = str(e)
                runs[run_id]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        finally:
            _pop_power_progress_hot(run_id)
    
    threading.Thread(target=worker, daemon=True).start()
    
    return jsonify({
        "ok": True,
        "run_id": run_id,
        "job_url": url_for("job_status", run_id=run_id, _external=True)
    })


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

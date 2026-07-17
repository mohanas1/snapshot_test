"""
Module for deleting recovery points with filtering and concurrency control.
"""

import requests
import base64
import threading
import uuid
from typing import Dict, List, Optional, Callable
import time
import fnmatch

from pc_api_auth import COOKIE_REFRESH_SEC, get_cookie


def make_auth_header(username: str, password: str) -> str:
    """Create basic auth header."""
    credentials = f"{username}:{password}"
    b64_credentials = base64.b64encode(credentials.encode()).decode()
    return f"Basic {b64_credentials}"


def delete_recovery_point(
    session: requests.Session,
    base_url: str,
    pc_user: str,
    pc_password: str,
    rp_ext_id: str,
) -> Dict:
    """
    Delete a single recovery point.
    Returns task UUID for tracking.
    """
    # Generate random UUID for ntnx-request-id header (required by API)
    request_id = str(uuid.uuid4())
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'ntnx-request-id': request_id
    }
    
    url = f"{base_url}/api/dataprotection/v4.3/config/recovery-points/{rp_ext_id}"
    
    try:
        get_cookie(
            session,
            base_url,
            pc_user,
            pc_password,
            refresh_sec=COOKIE_REFRESH_SEC,
        )
        response = session.delete(url, headers=headers, verify=False, timeout=60)
        response.raise_for_status()
        
        # DELETE returns 202 with task info
        data = response.json()
        task_ext_id = data.get('data', {}).get('extId', '')
        
        return {
            'ok': True,
            'task_ext_id': task_ext_id,
            'request_id': request_id,
            'status_code': response.status_code
        }
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_json = e.response.json()
                if 'message' in error_json:
                    error_detail = error_json.get('message', error_detail)
            except:
                pass
        
        return {
            'ok': False,
            'error': error_detail,
            'request_id': request_id,
            'status_code': getattr(e.response, 'status_code', 0) if hasattr(e, 'response') else 0
        }


def force_delete_all_recovery_points_for_protected_resource(
    session: requests.Session,
    base_url: str,
    pc_user: str,
    pc_password: str,
    protected_resource_ext_id: str,
) -> Dict:
    """
    Trigger force-delete-all recovery points for a protected resource (VM).
    Returns task ext id for tracking.
    """
    request_id = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'ntnx-request-id': request_id,
    }
    url = (
        f"{base_url}/api/dataprotection/v4.3/config/protected-resources/"
        f"{protected_resource_ext_id}/$actions/force-delete-all-recovery-points"
    )
    try:
        get_cookie(
            session,
            base_url,
            pc_user,
            pc_password,
            refresh_sec=COOKIE_REFRESH_SEC,
        )
        response = session.post(url, headers=headers, json={}, verify=False, timeout=60)
        response.raise_for_status()
        data = response.json() or {}
        task_ext_id = (
            data.get('data', {}).get('extId')
            or data.get('data', {}).get('ext_id')
            or ''
        )
        return {
            'ok': True,
            'task_ext_id': task_ext_id,
            'request_id': request_id,
            'status_code': response.status_code,
        }
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_json = e.response.json()
                if 'message' in error_json:
                    error_detail = error_json.get('message', error_detail)
            except Exception:
                pass
        return {
            'ok': False,
            'error': error_detail,
            'request_id': request_id,
            'status_code': getattr(e.response, 'status_code', 0) if hasattr(e, 'response') else 0
        }


def check_task_status(
    session: requests.Session,
    base_url: str,
    pc_user: str,
    pc_password: str,
    task_ext_id: str,
) -> Dict:
    """
    Check the status of a delete task.
    """
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    url = f"{base_url}/api/prism/v4.3/config/tasks/{task_ext_id}"
    
    try:
        get_cookie(
            session,
            base_url,
            pc_user,
            pc_password,
            refresh_sec=COOKIE_REFRESH_SEC,
        )
        response = session.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        task_data = data.get('data', {})
        
        status = task_data.get('status', 'UNKNOWN')
        progress_percentage = task_data.get('progressPercentage', 0)
        
        return {
            'ok': True,
            'status': status,  # RUNNING, SUCCEEDED, FAILED
            'progress_percentage': progress_percentage,
            'completed_time': task_data.get('completedTime'),
            'error_messages': task_data.get('errorMessages', [])
        }
    except requests.exceptions.RequestException as e:
        return {
            'ok': False,
            'error': str(e)
        }


def filter_recovery_points_by_size(recovery_points: List[Dict], size_filter: str) -> List[Dict]:
    """
    Filter recovery points based on size criteria.
    
    size_filter options:
    - "all": No filtering
    - "zero": Only 0 bytes
    - "0-100KB": 0 to 100KB
    - "100KB-1MB": 100KB to 1MB  
    - "1MB-10MB": 1MB to 10MB
    - "10MB-50MB": 10MB to 50MB
    - "1MB-50MB": 1MB to 50MB (legacy alias)
    - "50MB-500MB": 50MB to 500MB
    - "500MB+": 500MB and above
    """
    if size_filter == "all":
        return recovery_points
    
    filtered = []
    
    for rp in recovery_points:
        size_bytes = rp.get('size_bytes', 0)
        
        if size_filter == "zero":
            if size_bytes == 0:
                filtered.append(rp)
        elif size_filter == "0-100KB":
            if 0 <= size_bytes <= 100 * 1024:
                filtered.append(rp)
        elif size_filter == "100KB-1MB":
            if 100 * 1024 < size_bytes <= 1024 * 1024:
                filtered.append(rp)
        elif size_filter == "1MB-10MB":
            if 1024 * 1024 < size_bytes <= 10 * 1024 * 1024:
                filtered.append(rp)
        elif size_filter == "10MB-50MB":
            if 10 * 1024 * 1024 < size_bytes <= 50 * 1024 * 1024:
                filtered.append(rp)
        elif size_filter == "1MB-50MB":
            if 1024 * 1024 < size_bytes <= 50 * 1024 * 1024:
                filtered.append(rp)
        elif size_filter == "50MB-500MB":
            if 50 * 1024 * 1024 < size_bytes <= 500 * 1024 * 1024:
                filtered.append(rp)
        elif size_filter == "500MB+":
            if size_bytes > 500 * 1024 * 1024:
                filtered.append(rp)
    
    return filtered


def filter_recovery_points(
    recovery_points: List[Dict],
    size_filter: str = "all",
    cluster_names: Optional[List[str]] = None,
    vm_name_patterns: Optional[List[str]] = None,
    min_vm_recovery_points: int = 0,
    max_vm_recovery_points: int = 0,
) -> List[Dict]:
    """
    Apply all delete filters.

    Args:
        recovery_points: Recovery point records.
        size_filter: Size bucket selector.
        cluster_names: Optional list of cluster names to include.
        vm_name_patterns: Optional list of case-insensitive glob patterns for VM names.
        min_vm_recovery_points: Optional minimum number of recovery points the VM must have.
        max_vm_recovery_points: Optional maximum number of recovery points the VM must have.
    """
    filtered = filter_recovery_points_by_size(recovery_points, size_filter)

    clusters = {
        str(c).strip().lower()
        for c in (cluster_names or [])
        if str(c).strip()
    }
    patterns = [
        str(p).strip().lower()
        for p in (vm_name_patterns or [])
        if str(p).strip()
    ]
    threshold = max(0, int(min_vm_recovery_points or 0))
    max_threshold = max(0, int(max_vm_recovery_points or 0))

    if not clusters and not patterns and threshold <= 0 and max_threshold <= 0:
        return filtered

    vm_counts: Dict[str, int] = {}
    for rp in filtered:
        vm_uuid = str(rp.get("vm_uuid") or "").strip().lower()
        vm_name = str(rp.get("vm_name") or "").strip().lower()
        vm_key = vm_uuid or vm_name
        if not vm_key:
            continue
        vm_counts[vm_key] = vm_counts.get(vm_key, 0) + 1

    out: List[Dict] = []
    for rp in filtered:
        cluster_norm = str(rp.get("cluster_name") or rp.get("pe_cluster") or "Unknown").strip().lower()
        if clusters and cluster_norm not in clusters:
            continue
        vm_name_raw = str(rp.get("vm_name") or "").strip()
        vm_name_norm = vm_name_raw.lower()
        vm_uuid = str(rp.get("vm_uuid") or "").strip().lower()
        vm_key = vm_uuid or vm_name_norm

        if patterns:
            if not vm_name_norm:
                continue
            if not any(fnmatch.fnmatch(vm_name_norm, pattern) for pattern in patterns):
                continue

        if threshold > 0 and vm_counts.get(vm_key, 0) < threshold:
            continue
        if max_threshold > 0 and vm_counts.get(vm_key, 0) > max_threshold:
            continue

        out.append(rp)

    return out


def bulk_delete_recovery_points(
    pc_ip: str,
    pc_user: str,
    pc_password: str,
    recovery_points: List[Dict],
    size_filter: str = "all",
    cluster_names: Optional[List[str]] = None,
    vm_name_patterns: Optional[List[str]] = None,
    min_vm_recovery_points: int = 0,
    max_vm_recovery_points: int = 0,
    concurrency: int = 5,
    progress_callback: Optional[Callable[[str], None]] = None,
    cancel_event: Optional[threading.Event] = None,
) -> Dict:
    """
    Delete multiple recovery points with concurrency control and size filtering.
    
    Args:
        pc_ip: Prism Central IP
        pc_user: PC username
        pc_password: PC password
        recovery_points: List of recovery point dicts with 'ext_id', 'name', 'size_bytes'
        size_filter: Size filter criteria
        cluster_names: Optional cluster names to include
        vm_name_patterns: Optional VM name glob patterns (e.g. ["prod-*", "*db*"])
        min_vm_recovery_points: Delete only from VMs with at least this many RPs
        max_vm_recovery_points: Delete only from VMs with at most this many RPs
        concurrency: Max concurrent delete operations (default 5, max 5)
        progress_callback: Optional callback for progress updates
    
    Returns:
        Dict with deletion results
    """
    base_url = f"https://{pc_ip}:9440"
    
    # Apply all filters
    filtered_rps = filter_recovery_points(
        recovery_points,
        size_filter=size_filter,
        cluster_names=cluster_names,
        vm_name_patterns=vm_name_patterns,
        min_vm_recovery_points=min_vm_recovery_points,
        max_vm_recovery_points=max_vm_recovery_points,
    )
    
    if not filtered_rps:
        cluster_filter_summary = ", ".join([c for c in (cluster_names or []) if str(c).strip()]) or "all"
        vm_filter_summary = ", ".join([p for p in (vm_name_patterns or []) if str(p).strip()]) or "none"
        return {
            'ok': True,
            'message': (
                "No recovery points match selected filters: "
                f"size={size_filter}, clusters={cluster_filter_summary}, vm_patterns={vm_filter_summary}, "
                f"min_vm_recovery_points={int(min_vm_recovery_points or 0)}, "
                f"max_vm_recovery_points={int(max_vm_recovery_points or 0)}"
            ),
            'total': 0,
            'deleted': 0,
            'failed': 0,
            'results': []
        }
    
    if progress_callback:
        cluster_text = ", ".join([c for c in (cluster_names or []) if str(c).strip()]) or "all"
        pattern_text = ", ".join([p for p in (vm_name_patterns or []) if str(p).strip()]) or "none"
        progress_callback(
            "Starting bulk delete: "
            f"{len(filtered_rps)} recovery points "
            f"(size filter: {size_filter}, clusters: {cluster_text}, vm patterns: {pattern_text}, "
            f"min VM recovery points: {int(min_vm_recovery_points or 0)}, "
            f"max VM recovery points: {int(max_vm_recovery_points or 0)})"
        )
        progress_callback(f"Concurrency: {min(concurrency, len(filtered_rps))} workers")

    # Fast-path optimization:
    # For full VM delete (all sizes, no extra VM filters, no min/max thresholds),
    # call protected-resource force-delete-all once instead of per-RP deletes.
    vm_keys = {
        str((rp.get('vm_uuid') or '')).strip()
        for rp in filtered_rps
        if isinstance(rp, dict)
    }
    vm_keys.discard("")
    can_force_delete_all = (
        size_filter == "all"
        and not [p for p in (vm_name_patterns or []) if str(p).strip()]
        and int(min_vm_recovery_points or 0) <= 0
        and int(max_vm_recovery_points or 0) <= 0
        and len(vm_keys) == 1
    )
    if can_force_delete_all:
        vm_ext_id = next(iter(vm_keys))
        if progress_callback:
            progress_callback(
                f"Using protected-resource force-delete-all API for VM extId={vm_ext_id}"
            )
        session = requests.Session()
        force_result = force_delete_all_recovery_points_for_protected_resource(
            session,
            base_url,
            pc_user,
            pc_password,
            vm_ext_id,
        )
        if not force_result.get('ok'):
            return {
                'ok': False,
                'total': len(filtered_rps),
                'deleted': 0,
                'failed': len(filtered_rps),
                'results': [],
                'size_filter': size_filter,
                'vm_name_patterns': [p for p in (vm_name_patterns or []) if str(p).strip()],
                'min_vm_recovery_points': int(min_vm_recovery_points or 0),
                'max_vm_recovery_points': int(max_vm_recovery_points or 0),
                'cancelled': False,
                'message': force_result.get('error') or 'force-delete-all failed',
            }
        task_ext_id = str(force_result.get('task_ext_id') or '').strip()
        # Poll task to terminal.
        status = "UNKNOWN"
        error = ""
        started = time.time()
        while True:
            if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                return {
                    'ok': False,
                    'total': len(filtered_rps),
                    'deleted': 0,
                    'failed': 0,
                    'results': [],
                    'size_filter': size_filter,
                    'vm_name_patterns': [p for p in (vm_name_patterns or []) if str(p).strip()],
                    'min_vm_recovery_points': int(min_vm_recovery_points or 0),
                    'max_vm_recovery_points': int(max_vm_recovery_points or 0),
                    'cancelled': True,
                    'message': 'Cancelled by user',
                }
            if (time.time() - started) >= 900:
                status = "TIMEOUT"
                error = "Task wait timed out for force-delete-all"
                break
            st = check_task_status(session, base_url, pc_user, pc_password, task_ext_id)
            if not st.get('ok'):
                time.sleep(2)
                continue
            status = str(st.get('status') or 'UNKNOWN').upper()
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "CANCELLED"):
                if status != "SUCCEEDED":
                    em = st.get('error_messages') or []
                    error = str(em[0] if em else f"Task status: {status}")
                break
            time.sleep(2)
        success = status == "SUCCEEDED"
        return {
            'ok': success,
            'total': len(filtered_rps),
            'deleted': len(filtered_rps) if success else 0,
            'failed': 0 if success else len(filtered_rps),
            'results': [],
            'size_filter': size_filter,
            'vm_name_patterns': [p for p in (vm_name_patterns or []) if str(p).strip()],
            'min_vm_recovery_points': int(min_vm_recovery_points or 0),
            'max_vm_recovery_points': int(max_vm_recovery_points or 0),
            'cancelled': False,
            'task_ext_id': task_ext_id,
            'message': ("Force-delete-all completed" if success else (error or f"Task status: {status}")),
        }
    
    # Limit concurrency to max 5
    concurrency = min(concurrency, 5, len(filtered_rps))
    
    # Shared data structures
    results = []
    lock = threading.Lock()
    processed_count = [0]
    
    def _is_cancelled() -> bool:
        return isinstance(cancel_event, threading.Event) and cancel_event.is_set()

    def _wait_task_terminal(
        session: requests.Session,
        task_ext_id: str,
        rp_name: str,
        max_wait_sec: int = 600,
    ) -> Dict:
        started = time.time()
        last_status = "UNKNOWN"
        last_progress_emit_status = ""
        last_progress_emit_ts = 0.0
        while True:
            if _is_cancelled():
                return {"ok": False, "status": "CANCELLED", "error": "Cancelled by user"}
            if (time.time() - started) >= max_wait_sec:
                return {"ok": False, "status": "TIMEOUT", "error": "Task status wait timed out"}

            st = check_task_status(session, base_url, pc_user, pc_password, task_ext_id)
            if not st.get("ok"):
                time.sleep(2)
                continue

            last_status = str(st.get("status") or "UNKNOWN").upper()
            if last_status in ("SUCCEEDED", "FAILED", "ABORTED", "CANCELLED"):
                return st
            if progress_callback:
                now = time.time()
                # Reduce log noise: emit only on status change or periodic heartbeat.
                if (
                    last_status != last_progress_emit_status
                    or (now - last_progress_emit_ts) >= 15.0
                ):
                    progress_callback(f"    ↻ Task {task_ext_id[:12]} for {rp_name}: {last_status}")
                    last_progress_emit_status = last_status
                    last_progress_emit_ts = now
            time.sleep(2)

    def delete_single_rp(rp: Dict, idx: int):
        """Delete a single recovery point."""
        if _is_cancelled():
            with lock:
                processed_count[0] += 1
            return
        rp_ext_id = rp.get('ext_id', '')
        rp_name = rp.get('name', 'Unknown')
        rp_size = rp.get('size_formatted', 'Unknown')

        if not rp_ext_id:
            with lock:
                results.append({
                    'rp_name': rp_name,
                    'rp_ext_id': rp_ext_id,
                    'cluster_name': rp.get('cluster_name', 'Unknown'),
                    'vm_name': rp.get('vm_name', ''),
                    'success': False,
                    'error': 'Missing ext_id'
                })
                processed_count[0] += 1
            return

        if progress_callback:
            progress_callback(f"[{idx}/{len(filtered_rps)}] Deleting: {rp_name} ({rp_size})")

        session = requests.Session()
        get_cookie(
            session,
            base_url,
            pc_user,
            pc_password,
            force=True,
            refresh_sec=COOKIE_REFRESH_SEC,
        )

        delete_result = None
        for attempt in range(1, 6):
            if _is_cancelled():
                break
            delete_result = delete_recovery_point(
                session,
                base_url,
                pc_user,
                pc_password,
                rp_ext_id,
            )
            if delete_result.get("ok"):
                break
            if int(delete_result.get("status_code") or 0) == 429:
                backoff = min(30, 2 ** attempt)
                if progress_callback:
                    progress_callback(
                        f"  ⚠️ 429 for {rp_name}; retry {attempt}/5 after {backoff}s"
                    )
                slept = 0
                while slept < backoff:
                    if _is_cancelled():
                        break
                    time.sleep(1)
                    slept += 1
                if _is_cancelled():
                    break
                continue
            break
        if delete_result is None:
            delete_result = {'ok': False, 'error': 'Delete did not run', 'status_code': 0}
        task_terminal = None
        if delete_result.get('ok') and delete_result.get('task_ext_id'):
            task_terminal = _wait_task_terminal(session, delete_result['task_ext_id'], rp_name)
            if not task_terminal.get('ok') or str(task_terminal.get('status', '')).upper() != 'SUCCEEDED':
                delete_result = {
                    'ok': False,
                    'error': (
                        task_terminal.get('error')
                        or f"Task status: {task_terminal.get('status', 'UNKNOWN')}"
                    ),
                    'task_ext_id': delete_result.get('task_ext_id', ''),
                    'status_code': 0,
                }

        with lock:
            processed_count[0] += 1
            cluster_name = str(rp.get('cluster_name', 'Unknown') or 'Unknown')
            results.append({
                'rp_name': rp_name,
                'rp_ext_id': rp_ext_id,
                'rp_size': rp_size,
                'cluster_name': cluster_name,
                'vm_name': rp.get('vm_name', ''),
                'success': delete_result['ok'],
                'task_ext_id': delete_result.get('task_ext_id', ''),
                'error': delete_result.get('error', '')
            })

            if progress_callback:
                if delete_result['ok']:
                    progress_callback(
                        f"  ✓ Deleted: {rp_name} (cluster={cluster_name}) "
                        f"(Task: {delete_result.get('task_ext_id', 'N/A')})"
                    )
                else:
                    progress_callback(
                        f"  ✗ Failed: {rp_name} (cluster={cluster_name}) - "
                        f"{delete_result.get('error', 'Unknown error')}"
                    )

    # Fixed-size worker pool (avoid spawning one thread per RP).
    next_idx = {"i": 0}
    idx_lock = threading.Lock()

    def worker_loop():
        while True:
            if _is_cancelled():
                return
            with idx_lock:
                i = next_idx["i"]
                if i >= len(filtered_rps):
                    return
                next_idx["i"] = i + 1
                rp = filtered_rps[i]
                idx = i + 1
            delete_single_rp(rp, idx)

    # Create bounded number of threads only.
    threads = []
    for _ in range(concurrency):
        # Non-daemon workers + full join so we never report completion early.
        t = threading.Thread(target=worker_loop, daemon=False)
        t.start()
        threads.append(t)
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Calculate statistics
    deleted_count = sum(1 for r in results if r['success'])
    failed_count = len(results) - deleted_count
    cancelled = _is_cancelled()
    
    if progress_callback:
        progress_callback("=" * 80)
        progress_callback("Bulk delete completed:" if not cancelled else "Bulk delete cancelled by user:")
        progress_callback(f"  Total recovery points: {len(filtered_rps)}")
        progress_callback(f"  Successfully deleted: {deleted_count}")
        progress_callback(f"  Failed: {failed_count}")
        progress_callback("=" * 80)
    
    return {
        'ok': not cancelled,
        'total': len(filtered_rps),
        'deleted': deleted_count,
        'failed': failed_count,
        'results': results,
        'size_filter': size_filter,
        'vm_name_patterns': [p for p in (vm_name_patterns or []) if str(p).strip()],
        'min_vm_recovery_points': int(min_vm_recovery_points or 0),
        'max_vm_recovery_points': int(max_vm_recovery_points or 0),
        'cancelled': cancelled,
    }

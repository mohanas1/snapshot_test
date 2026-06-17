"""
Module for deleting recovery points with filtering and concurrency control.
"""

import requests
import base64
import threading
import uuid
from typing import Dict, List, Optional, Callable
import time


def make_auth_header(username: str, password: str) -> str:
    """Create basic auth header."""
    credentials = f"{username}:{password}"
    b64_credentials = base64.b64encode(credentials.encode()).decode()
    return f"Basic {b64_credentials}"


def delete_recovery_point(base_url: str, auth_header: str, rp_ext_id: str) -> Dict:
    """
    Delete a single recovery point.
    Returns task UUID for tracking.
    """
    # Generate random UUID for ntnx-request-id header (required by API)
    request_id = str(uuid.uuid4())
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': auth_header,
        'Accept': 'application/json',
        'ntnx-request-id': request_id
    }
    
    url = f"{base_url}/api/dataprotection/v4.3/config/recovery-points/{rp_ext_id}"
    
    try:
        response = requests.delete(url, headers=headers, verify=False, timeout=60)
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


def check_task_status(base_url: str, auth_header: str, task_ext_id: str) -> Dict:
    """
    Check the status of a delete task.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': auth_header,
        'Accept': 'application/json'
    }
    
    url = f"{base_url}/api/prism/v4.1/config/tasks/{task_ext_id}"
    
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
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
    - "1MB-50MB": 1MB to 50MB
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


def bulk_delete_recovery_points(
    pc_ip: str,
    pc_user: str,
    pc_password: str,
    recovery_points: List[Dict],
    size_filter: str = "all",
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
        concurrency: Max concurrent delete operations (default 5, max 5)
        progress_callback: Optional callback for progress updates
    
    Returns:
        Dict with deletion results
    """
    base_url = f"https://{pc_ip}:9440"
    auth_header = make_auth_header(pc_user, pc_password)
    
    # Apply size filter
    filtered_rps = filter_recovery_points_by_size(recovery_points, size_filter)
    
    if not filtered_rps:
        return {
            'ok': True,
            'message': f'No recovery points match the size filter: {size_filter}',
            'total': 0,
            'deleted': 0,
            'failed': 0,
            'results': []
        }
    
    if progress_callback:
        progress_callback(f"Starting bulk delete: {len(filtered_rps)} recovery points (size filter: {size_filter})")
        progress_callback(f"Concurrency: {min(concurrency, len(filtered_rps))} workers")
    
    # Limit concurrency to max 5
    concurrency = min(concurrency, 5, len(filtered_rps))
    
    # Shared data structures
    results = []
    lock = threading.Lock()
    semaphore = threading.Semaphore(concurrency)
    processed_count = [0]
    
    def _is_cancelled() -> bool:
        return isinstance(cancel_event, threading.Event) and cancel_event.is_set()

    def _wait_task_terminal(task_ext_id: str, rp_name: str, max_wait_sec: int = 600) -> Dict:
        started = time.time()
        last_status = "UNKNOWN"
        while True:
            if _is_cancelled():
                return {"ok": False, "status": "CANCELLED", "error": "Cancelled by user"}
            if (time.time() - started) >= max_wait_sec:
                return {"ok": False, "status": "TIMEOUT", "error": "Task status wait timed out"}

            st = check_task_status(base_url, auth_header, task_ext_id)
            if not st.get("ok"):
                time.sleep(2)
                continue

            last_status = str(st.get("status") or "UNKNOWN").upper()
            if last_status in ("SUCCEEDED", "FAILED", "ABORTED", "CANCELLED"):
                return st
            if progress_callback:
                progress_callback(f"    ↻ Task {task_ext_id[:12]} for {rp_name}: {last_status}")
            time.sleep(2)

    def delete_single_rp(rp: Dict, idx: int):
        """Delete a single recovery point with semaphore control."""
        with semaphore:
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

            delete_result = None
            for attempt in range(1, 6):
                if _is_cancelled():
                    break
                delete_result = delete_recovery_point(base_url, auth_header, rp_ext_id)
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
                task_terminal = _wait_task_terminal(delete_result['task_ext_id'], rp_name)
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
                results.append({
                    'rp_name': rp_name,
                    'rp_ext_id': rp_ext_id,
                    'rp_size': rp_size,
                    'cluster_name': rp.get('cluster_name', 'Unknown'),
                    'vm_name': rp.get('vm_name', ''),
                    'success': delete_result['ok'],
                    'task_ext_id': delete_result.get('task_ext_id', ''),
                    'error': delete_result.get('error', '')
                })
                
                if progress_callback:
                    if delete_result['ok']:
                        progress_callback(f"  ✓ Deleted: {rp_name} (Task: {delete_result.get('task_ext_id', 'N/A')})")
                    else:
                        progress_callback(f"  ✗ Failed: {rp_name} - {delete_result.get('error', 'Unknown error')}")
    
    # Create threads
    threads = []
    for idx, rp in enumerate(filtered_rps, 1):
        if _is_cancelled():
            break
        t = threading.Thread(target=delete_single_rp, args=(rp, idx))
        t.daemon = True
        t.start()
        threads.append(t)
    
    # Wait for all threads to complete
    for t in threads:
        t.join(timeout=300)  # 5 minute timeout per thread
    
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
        'cancelled': cancelled,
    }

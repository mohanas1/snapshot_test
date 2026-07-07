"""
Backend module for analyzing VM Recovery Points and calculating reclaimable space.
Adapted from analyze_recovery_points.py for Flask integration.
"""

import requests
import json
import urllib3
import base64
import random
from datetime import datetime
from typing import Dict, List, Optional, Callable
import threading
import time

from pc_api_auth import COOKIE_REFRESH_SEC, get_cookie

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Pagination settings
GROUPS_PAGE_SIZE = 60
RECOVERY_POINTS_PAGE_SIZE = 100
RECOVERY_POINTS_429_RETRIES = 3
RECOVERY_POINTS_429_BACKOFF_BASE_SEC = 0.8

_HEALTH_LOCK = threading.Lock()
_RECOVERY_HEALTH: Dict[str, object] = {
    "last_pc_ip": "",
    "last_update_iso": "",
    "network_status": "idle",
    "last_http_status": None,
    "last_latency_ms": None,
    "throttle_429_count": 0,
    "cookie_status": "unknown",
    "cookie_last_refresh_iso": "",
    "cookie_last_error": "",
    "cookie_fingerprint": "",
    "cookie_source": "",
    "cookie_changed": False,
    "last_error": "",
}


def _iso_now() -> str:
    return datetime.now().isoformat()


def _update_health(**fields: object) -> None:
    with _HEALTH_LOCK:
        _RECOVERY_HEALTH.update(fields)
        _RECOVERY_HEALTH["last_update_iso"] = _iso_now()


def get_recovery_health() -> Dict[str, object]:
    with _HEALTH_LOCK:
        return dict(_RECOVERY_HEALTH)


def _request_with_cookie_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    base_url: str,
    pc_user: str,
    pc_password: str,
    max_retries: int = 1,
    **kwargs,
) -> requests.Response:
    """
    Execute API call with cookie bootstrap and one forced refresh retry on 401/429.

    This handles cookie expiry race conditions where a previously valid session
    cookie expires between calls.
    """
    get_cookie(
        session,
        base_url,
        pc_user,
        pc_password,
        refresh_sec=COOKIE_REFRESH_SEC,
    )
    _update_health(
        cookie_fingerprint=str(getattr(session, "_pc_cookie_fingerprint", "") or ""),
        cookie_source=str(getattr(session, "_pc_cookie_source", "") or ""),
    )
    response = session.request(method, url, **kwargs)
    if response.status_code not in (401, 429) or max_retries <= 0:
        return response

    # Backoff slightly on 429 before forced cookie refresh + one retry.
    if response.status_code == 429:
        sleep_s = 0.35 + random.uniform(0.05, 0.4)
        time.sleep(sleep_s)

    before_fp = str(getattr(session, "_pc_cookie_fingerprint", "") or "")
    get_cookie(
        session,
        base_url,
        pc_user,
        pc_password,
        force=True,
        refresh_sec=COOKIE_REFRESH_SEC,
    )
    after_fp = str(getattr(session, "_pc_cookie_fingerprint", "") or "")
    _update_health(
        cookie_fingerprint=after_fp,
        cookie_source=str(getattr(session, "_pc_cookie_source", "") or ""),
        cookie_changed=bool(before_fp and after_fp and before_fp != after_fp),
    )
    return session.request(method, url, **kwargs)


def format_bytes(bytes_value):
    """Convert bytes to human-readable format."""
    if bytes_value == 0:
        return "0 B"
    
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    k = 1024
    
    for i, unit in enumerate(units):
        if bytes_value < k ** (i + 1) or i == len(units) - 1:
            value = bytes_value / (k ** i)
            return f"{value:.2f} {unit}"
    
    return f"{bytes_value} B"


def make_auth_header(username: str, password: str) -> str:
    """Create basic auth header."""
    credentials = f"{username}:{password}"
    b64_credentials = base64.b64encode(credentials.encode()).decode()
    return f"Basic {b64_credentials}"


def _normalize_recovery_point_type(rp: Dict) -> str:
    """Best-effort normalize recovery point consistency type."""
    raw = (
        rp.get("recoveryPointType")
        or rp.get("recovery_point_type")
        or rp.get("consistencyType")
        or rp.get("consistency_type")
        or ((rp.get("status") or {}).get("recoveryPointType"))
        or ((rp.get("status") or {}).get("recovery_point_type"))
        or ""
    )
    s = str(raw).strip().upper()
    if s in {"CRASH_CONSISTENT", "APPLICATION_CONSISTENT"}:
        return s
    return "UNKNOWN"


def get_all_vms_with_recovery_points(
    session: requests.Session,
    base_url: str,
    pc_user: str,
    pc_password: str,
                                     progress_callback: Optional[Callable[[str], None]] = None) -> List[Dict]:
    """Get all VMs with recovery points using v3 groups API."""
    headers = {'Content-Type': 'application/json'}
    
    all_vms = []
    offset = 0
    
    v3_groups_url = f"{base_url}/api/nutanix/v3/groups"
    
    while True:
        payload = {
            "entity_type": "vm_recovery_point",
            "group_count": GROUPS_PAGE_SIZE,
            "group_offset": offset,
            "grouping_attribute": "entity_uuid",
            "group_sort_attribute": "entity_uuid",
            "group_sort_order": "ASCENDING",
            "group_member_count": 0,
            "filter_criteria": "snapshot_type!=LIVE"
        }
        
        try:
            cookie_t0 = time.perf_counter()
            _update_health(
                cookie_status="ok",
                cookie_last_refresh_iso=_iso_now(),
                cookie_last_error="",
                network_status="requesting",
                last_pc_ip=base_url.replace("https://", "").replace(":9440", ""),
                last_latency_ms=round((time.perf_counter() - cookie_t0) * 1000, 2),
            )
            req_t0 = time.perf_counter()
            response = _request_with_cookie_retry(
                session,
                "POST",
                v3_groups_url,
                base_url=base_url,
                pc_user=pc_user,
                pc_password=pc_password,
                headers=headers,
                data=json.dumps(payload),
                verify=False,
                timeout=60,
            )
            req_latency_ms = round((time.perf_counter() - req_t0) * 1000, 2)
            _update_health(
                network_status="ok",
                last_http_status=response.status_code,
                last_latency_ms=req_latency_ms,
            )
            if response.status_code == 429:
                with _HEALTH_LOCK:
                    _RECOVERY_HEALTH["throttle_429_count"] = int(_RECOVERY_HEALTH.get("throttle_429_count", 0) or 0) + 1
            response.raise_for_status()
            data = response.json()
            
            group_results = data.get('group_results', [])
            
            if not group_results:
                break
            
            for group in group_results:
                vm_uuid = group.get('group_by_column_value')
                recovery_point_count = group.get('total_entity_count', 0)
                
                if vm_uuid and recovery_point_count > 0:
                    all_vms.append({
                        'vm_uuid': vm_uuid,
                        'recovery_point_count': recovery_point_count
                    })
            
            total_group_count = data.get('filtered_group_count', 0)
            
            if progress_callback:
                progress_callback(f"  📥 Fetched {len(group_results)} VMs (offset: {offset})")
            
            offset += GROUPS_PAGE_SIZE
            
            if offset >= total_group_count:
                break
                
        except requests.exceptions.RequestException as e:
            status_code = None
            if getattr(e, "response", None) is not None:
                status_code = e.response.status_code
                if status_code == 429:
                    with _HEALTH_LOCK:
                        _RECOVERY_HEALTH["throttle_429_count"] = int(_RECOVERY_HEALTH.get("throttle_429_count", 0) or 0) + 1
            _update_health(
                network_status="error",
                last_http_status=status_code,
                last_error=str(e),
                cookie_status="error" if status_code == 401 else str(_RECOVERY_HEALTH.get("cookie_status", "unknown")),
                cookie_last_error=str(e) if status_code == 401 else str(_RECOVERY_HEALTH.get("cookie_last_error", "")),
            )
            if progress_callback:
                progress_callback(f"  ❌ Failed while fetching VM groups at offset={offset}: {str(e)}")
            raise Exception(f"Error fetching VMs: {str(e)}")
    
    if progress_callback:
        progress_callback(f"  ✅ Completed fetching all {len(all_vms)} VMs")
    
    return all_vms


def get_vm_recovery_points_details(
    session: requests.Session,
    base_url: str,
    pc_user: str,
    pc_password: str,
    vm_uuid: str,
    max_pages: int = 50,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> List[Dict]:
    """Fetch detailed recovery points for a specific VM using v4 API with improved timeout handling."""
    headers = {
        'Content-Type': 'application/json',
        'NTNX-Request-Id': f'recovery-points-{vm_uuid}'
    }
    
    v4_recovery_points_url = f"{base_url}/api/dataprotection/v4.3/config/recovery-points"
    all_recovery_points = []
    page = 0
    
    while page < max_pages:  # Add safety limit on pages
        params = {
            '$page': page,
            '$limit': RECOVERY_POINTS_PAGE_SIZE,
            '$filter': f"vmRecoveryPoints/any(a:a/vmExtId eq '{vm_uuid}')",
            '$orderby': 'creationTime desc',
            '$select': '*'
        }
        
        try:
            last_req_exc: requests.exceptions.RequestException | None = None
            data: dict = {}
            response: requests.Response | None = None
            request_ok = False

            for attempt in range(1, RECOVERY_POINTS_429_RETRIES + 1):
                try:
                    response = _request_with_cookie_retry(
                        session,
                        "GET",
                        v4_recovery_points_url,
                        base_url=base_url,
                        pc_user=pc_user,
                        pc_password=pc_password,
                        headers=headers,
                        params=params,
                        verify=False,
                        timeout=15,  # Reduced from 30s to 15s per page
                    )
                    response.raise_for_status()
                    data = response.json()
                    request_ok = True
                    break
                except requests.exceptions.RequestException as e:
                    last_req_exc = e
                    status_code = getattr(getattr(e, "response", None), "status_code", None)
                    if status_code != 429 or attempt >= RECOVERY_POINTS_429_RETRIES:
                        break

                    sleep_s = RECOVERY_POINTS_429_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0.05, 0.35)
                    if progress_callback:
                        progress_callback(
                            "  🔁 VM "
                            f"{vm_uuid[:8]} page={page}: 429 retry {attempt}/{RECOVERY_POINTS_429_RETRIES} "
                            f"in {sleep_s:.2f}s | cookie={getattr(session, '_pc_cookie_fingerprint', '') or 'na'} "
                            f"source={getattr(session, '_pc_cookie_source', '') or 'na'}"
                        )
                    time.sleep(sleep_s)

            if not request_ok:
                if last_req_exc is not None:
                    raise last_req_exc
                raise Exception(f"Unknown request failure for VM {vm_uuid} page {page}")
            
            recovery_points = data.get('data', [])
            
            if not recovery_points:
                break
            
            all_recovery_points.extend(recovery_points)
            
            # Check if we've fetched all recovery points
            metadata = data.get('metadata', {})
            total_available = metadata.get('totalAvailableResults', 0)
            
            if (page + 1) * RECOVERY_POINTS_PAGE_SIZE >= total_available:
                break
            
            page += 1
            
        except requests.exceptions.Timeout:
            # On timeout, return what we have so far
            if all_recovery_points:
                return all_recovery_points
            raise Exception(f"Timeout fetching recovery points for VM {vm_uuid} (page {page})")
        except requests.exceptions.RequestException as e:
            # On other errors, return what we have if any, otherwise raise
            if all_recovery_points:
                return all_recovery_points
            raise Exception(f"Error fetching recovery points for VM {vm_uuid}: {str(e)}")
    
    return all_recovery_points


def get_vm_metadata(
    session: requests.Session,
    base_url: str,
    pc_user: str,
    pc_password: str,
    vm_uuid: str,
) -> Dict[str, str]:
    """Get VM metadata (name + cluster hints) from UUID using v3 API."""
    headers = {'Content-Type': 'application/json'}
    
    v3_vm_url = f"{base_url}/api/nutanix/v3/vms/{vm_uuid}"
    
    try:
        response = _request_with_cookie_retry(
            session,
            "GET",
            v3_vm_url,
            base_url=base_url,
            pc_user=pc_user,
            pc_password=pc_password,
            headers=headers,
            verify=False,
            timeout=5,  # Reduced from 10s to 5s
        )
        response.raise_for_status()
        data = response.json()
        vm_name = data.get('spec', {}).get('name', f"VM-{vm_uuid[:8]}")

        # Prefer v3 status.cluster_reference, fallback to other common locations.
        status = data.get('status', {}) or {}
        spec = data.get('spec', {}) or {}
        status_cluster_ref = status.get('cluster_reference', {}) or {}
        status_resources = status.get('resources', {}) or {}
        spec_resources = spec.get('resources', {}) or {}
        res_cluster_ref = status_resources.get('cluster_reference', {}) or spec_resources.get('cluster_reference', {}) or {}

        cluster_name = (
            status_cluster_ref.get('name')
            or res_cluster_ref.get('name')
            or ""
        )
        cluster_uuid = (
            status_cluster_ref.get('uuid')
            or res_cluster_ref.get('uuid')
            or ""
        )

        return {
            'vm_name': vm_name,
            'cluster_name': cluster_name,
            'cluster_uuid': cluster_uuid,
            # Keep dedicated field for UI compatibility.
            'pe_cluster': cluster_name or cluster_uuid or 'Unknown',
        }
    except:
        fallback = f"VM-{vm_uuid[:8]}"
        return {
            'vm_name': fallback,
            'cluster_name': "",
            'cluster_uuid': "",
            'pe_cluster': "Unknown",
        }


def analyze_recovery_points(pc_ip: str, pc_user: str, pc_password: str, 
                           concurrency: int = 5,
                           progress_callback: Optional[Callable[[str], None]] = None) -> Dict:
    """
    Main function to analyze recovery points with concurrent processing.
    Returns a summary dictionary with results.
    """
    import threading
    from queue import Queue, Empty
    
    base_url = f"https://{pc_ip}:9440"
    session = requests.Session()
    _update_health(
        last_pc_ip=pc_ip,
        network_status="running",
        last_error="",
    )
    
    start_time = datetime.now()
    
    if progress_callback:
        progress_callback("=" * 80)
        progress_callback("VM RECOVERY POINTS ANALYSIS")
        progress_callback("=" * 80)
        progress_callback(f"Prism Central: {pc_ip}")
        progress_callback("=" * 80)
        progress_callback("")
        progress_callback("🔍 Fetching VMs with recovery points using v3 groups API...")
    
    # Get all VMs with recovery points
    vms = get_all_vms_with_recovery_points(
        session,
        base_url,
        pc_user,
        pc_password,
        progress_callback,
    )
    
    if progress_callback:
        progress_callback(f"✅ Found {len(vms)} VMs with recovery points")
        progress_callback("")
        progress_callback("📊 Fetching detailed recovery points for each VM...")
        progress_callback("")
        progress_callback(f"⚡ Starting analysis with {concurrency} concurrent workers...")
    
    # Shared data structures
    vm_details = []
    total_reclaimable_bytes = 0
    total_recovery_points = 0
    rp_type_counts = {
        "CRASH_CONSISTENT": 0,
        "APPLICATION_CONSISTENT": 0,
    }
    lock = threading.Lock()
    processed_count = [0]  # Use list for mutable counter
    
    def process_vm(vm, idx, thread_session: requests.Session):
        """Process a single VM with timeout protection"""
        nonlocal total_reclaimable_bytes, total_recovery_points
        
        vm_uuid = vm['vm_uuid']
        expected_count = vm['recovery_point_count']
        
        try:
            if progress_callback:
                progress_callback(f"[{idx}/{len(vms)}] Processing VM: {vm_uuid[:8]}... (Expected: {expected_count} recovery points)")
            
            # Get VM metadata (with shorter timeout)
            try:
                vm_meta = get_vm_metadata(thread_session, base_url, pc_user, pc_password, vm_uuid)
            except:
                vm_meta = {
                    'vm_name': f"VM-{vm_uuid[:8]}",
                    'cluster_name': "",
                    'cluster_uuid': "",
                    'pe_cluster': "Unknown",
                }
            
            # Get recovery point details (with timeout handling in the function)
            recovery_points = get_vm_recovery_points_details(
                thread_session,
                base_url,
                pc_user,
                pc_password,
                vm_uuid,
                progress_callback=progress_callback,
            )
            
            # Calculate reclaimable space
            vm_reclaimable = 0
            for rp in recovery_points:
                size_bytes = rp.get('totalExclusiveUsageBytes', 0)
                vm_reclaimable += size_bytes
            
            # Format recovery points with individual sizes and extIds
            formatted_recovery_points = []
            for rp in recovery_points:
                size_bytes = rp.get('totalExclusiveUsageBytes', 0)
                rp_type = _normalize_recovery_point_type(rp)
                formatted_recovery_points.append({
                    'ext_id': rp.get('extId', ''),  # UUID for delete operations
                    'name': rp.get('name', 'Unnamed'),
                    'created_time': rp.get('creationTime', 'Unknown'),
                    'size_bytes': size_bytes,
                    'size_formatted': format_bytes(size_bytes),
                    'recovery_point_type': rp_type,
                    'expiration_time': rp.get('expirationTime', 'N/A'),
                    'status': rp.get('status', 'UNKNOWN'),
                    'cluster_name': vm_meta.get('cluster_name') or 'Unknown',
                    'pe_cluster': vm_meta.get('pe_cluster') or 'Unknown',
                    'vm_name': vm_meta.get('vm_name') or f"VM-{vm_uuid[:8]}",
                })
            
            with lock:
                total_reclaimable_bytes += vm_reclaimable
                total_recovery_points += len(recovery_points)
                for rp in formatted_recovery_points:
                    tp = str(rp.get("recovery_point_type") or "").upper()
                    if tp in rp_type_counts:
                        rp_type_counts[tp] += 1
                processed_count[0] += 1
                
                vm_details.append({
                    'vm_name': vm_meta.get('vm_name') or f"VM-{vm_uuid[:8]}",
                    'vm_uuid': vm_uuid,
                    'cluster_name': vm_meta.get('cluster_name') or 'Unknown',
                    'cluster_uuid': vm_meta.get('cluster_uuid') or '',
                    'pe_cluster': vm_meta.get('pe_cluster') or 'Unknown',
                    'recovery_point_count': len(recovery_points),
                    'reclaimable_bytes': vm_reclaimable,
                    'reclaimable_formatted': format_bytes(vm_reclaimable),
                    'recovery_points': formatted_recovery_points  # Include individual recovery points
                })
                
                if progress_callback:
                    progress_callback(
                        f"  ✓ VM: {vm_meta.get('vm_name', vm_uuid[:8])} "
                        f"[{vm_meta.get('cluster_name') or 'Unknown'}] "
                        f"({len(recovery_points)} RPs, {format_bytes(vm_reclaimable)})"
                    )
        
        except Exception as e:
            # Log error but continue processing other VMs
            with lock:
                processed_count[0] += 1  # Count as processed even if failed
            if progress_callback:
                progress_callback(f"  ⚠️ Skipped VM {vm_uuid[:8]}: {str(e)}")
    
    # Create work queue
    work_queue = Queue()
    for idx, vm in enumerate(vms, 1):
        work_queue.put((vm, idx))
    
    # Worker thread function
    def worker():
        thread_session = requests.Session()
        # Seed worker session from primary session to avoid concurrent forced
        # cookie bootstraps that can invalidate each other on some PC setups.
        thread_session.cookies.update(session.cookies)
        setattr(
            thread_session,
            "_pc_cookie_refreshed_at",
            getattr(session, "_pc_cookie_refreshed_at", 0.0),
        )
        setattr(
            thread_session,
            "_pc_cookie_base_url",
            getattr(session, "_pc_cookie_base_url", base_url),
        )
        setattr(
            thread_session,
            "_pc_cookie_username",
            getattr(session, "_pc_cookie_username", pc_user),
        )
        setattr(
            thread_session,
            "_pc_cookie_password",
            getattr(session, "_pc_cookie_password", pc_password),
        )
        while True:
            try:
                vm, idx = work_queue.get(timeout=1)
            except Empty:
                # Queue is empty, exit worker
                break
            
            try:
                process_vm(vm, idx, thread_session)
            except Exception as e:
                if progress_callback:
                    progress_callback(f"❌ Worker thread error: {str(e)}")
            finally:
                work_queue.task_done()
    
    # Start worker threads
    threads = []
    num_workers = min(concurrency, len(vms))
    
    if progress_callback:
        progress_callback(f"🚀 Starting {num_workers} concurrent workers...")
        progress_callback("")
    
    for _ in range(num_workers):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)
    
    # Wait for all tasks to complete
    work_queue.join()
    
    # Wait for all threads to finish
    for t in threads:
        t.join(timeout=1)
    
    # Sort by reclaimable space (highest first)
    vm_details.sort(key=lambda x: x['reclaimable_bytes'], reverse=True)
    
    end_time = datetime.now()
    analysis_duration = (end_time - start_time).total_seconds()
    
    summary = {
        'cluster_ip': pc_ip,
        'analysis_time': datetime.now().isoformat(),
        'duration_seconds': analysis_duration,
        'total_vms': len(vms),
        'total_recovery_points': total_recovery_points,
        'crash_consistent_count': int(rp_type_counts.get("CRASH_CONSISTENT", 0)),
        'application_consistent_count': int(rp_type_counts.get("APPLICATION_CONSISTENT", 0)),
        'total_reclaimable_bytes': total_reclaimable_bytes,
        'total_reclaimable_formatted': format_bytes(total_reclaimable_bytes),
        'vms': vm_details,
        'concurrency_used': min(concurrency, len(vms))
    }
    
    if progress_callback:
        progress_callback("")
        progress_callback("=" * 80)
        progress_callback("ANALYSIS COMPLETE")
        progress_callback("=" * 80)
        progress_callback(f"Total VMs Analyzed: {len(vms)}")
        progress_callback(f"Total Recovery Points: {total_recovery_points}")
        progress_callback(f"Total Reclaimable Space: {format_bytes(total_reclaimable_bytes)}")
        progress_callback(f"Analysis Duration: {analysis_duration:.2f} seconds")
        progress_callback(f"Concurrent Workers Used: {min(concurrency, len(vms))}")
        progress_callback("=" * 80)
    
    _update_health(network_status="complete")
    return summary

"""
Backend module for analyzing VM Recovery Points and calculating reclaimable space.
Adapted from analyze_recovery_points.py for Flask integration.
"""

import requests
import json
import urllib3
import base64
from datetime import datetime
from typing import Dict, List, Optional, Callable

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Pagination settings
GROUPS_PAGE_SIZE = 60
RECOVERY_POINTS_PAGE_SIZE = 100


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


def get_all_vms_with_recovery_points(base_url: str, auth_header: str, 
                                     progress_callback: Optional[Callable[[str], None]] = None) -> List[Dict]:
    """Get all VMs with recovery points using v3 groups API."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': auth_header
    }
    
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
            response = requests.post(
                v3_groups_url,
                headers=headers,
                data=json.dumps(payload),
                verify=False,
                timeout=60
            )
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
            raise Exception(f"Error fetching VMs: {str(e)}")
    
    if progress_callback:
        progress_callback(f"  ✅ Completed fetching all {len(all_vms)} VMs")
    
    return all_vms


def get_vm_recovery_points_details(base_url: str, auth_header: str, vm_uuid: str, max_pages: int = 50) -> List[Dict]:
    """Fetch detailed recovery points for a specific VM using v4 API with improved timeout handling."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': auth_header,
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
            response = requests.get(
                v4_recovery_points_url,
                headers=headers,
                params=params,
                verify=False,
                timeout=15  # Reduced from 30s to 15s per page
            )
            response.raise_for_status()
            data = response.json()
            
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


def get_vm_name(base_url: str, auth_header: str, vm_uuid: str) -> str:
    """Get VM name from UUID using v3 API with short timeout."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': auth_header
    }
    
    v3_vm_url = f"{base_url}/api/nutanix/v3/vms/{vm_uuid}"
    
    try:
        response = requests.get(
            v3_vm_url,
            headers=headers,
            verify=False,
            timeout=5  # Reduced from 10s to 5s
        )
        response.raise_for_status()
        data = response.json()
        return data.get('spec', {}).get('name', f"VM-{vm_uuid[:8]}")
    except:
        return f"VM-{vm_uuid[:8]}"


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
    auth_header = make_auth_header(pc_user, pc_password)
    
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
    vms = get_all_vms_with_recovery_points(base_url, auth_header, progress_callback)
    
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
    lock = threading.Lock()
    processed_count = [0]  # Use list for mutable counter
    
    def process_vm(vm, idx):
        """Process a single VM with timeout protection"""
        nonlocal total_reclaimable_bytes, total_recovery_points
        
        vm_uuid = vm['vm_uuid']
        expected_count = vm['recovery_point_count']
        
        try:
            if progress_callback:
                progress_callback(f"[{idx}/{len(vms)}] Processing VM: {vm_uuid[:8]}... (Expected: {expected_count} recovery points)")
            
            # Get VM name (with shorter timeout)
            try:
                vm_name = get_vm_name(base_url, auth_header, vm_uuid)
            except:
                vm_name = f"VM-{vm_uuid[:8]}"
            
            # Get recovery point details (with timeout handling in the function)
            recovery_points = get_vm_recovery_points_details(base_url, auth_header, vm_uuid)
            
            # Calculate reclaimable space
            vm_reclaimable = 0
            for rp in recovery_points:
                size_bytes = rp.get('totalExclusiveUsageBytes', 0)
                vm_reclaimable += size_bytes
            
            # Format recovery points with individual sizes and extIds
            formatted_recovery_points = []
            for rp in recovery_points:
                size_bytes = rp.get('totalExclusiveUsageBytes', 0)
                formatted_recovery_points.append({
                    'ext_id': rp.get('extId', ''),  # UUID for delete operations
                    'name': rp.get('name', 'Unnamed'),
                    'created_time': rp.get('creationTime', 'Unknown'),
                    'size_bytes': size_bytes,
                    'size_formatted': format_bytes(size_bytes),
                    'expiration_time': rp.get('expirationTime', 'N/A'),
                    'status': rp.get('status', 'UNKNOWN')
                })
            
            with lock:
                total_reclaimable_bytes += vm_reclaimable
                total_recovery_points += len(recovery_points)
                processed_count[0] += 1
                
                vm_details.append({
                    'vm_name': vm_name,
                    'vm_uuid': vm_uuid,
                    'recovery_point_count': len(recovery_points),
                    'reclaimable_bytes': vm_reclaimable,
                    'reclaimable_formatted': format_bytes(vm_reclaimable),
                    'recovery_points': formatted_recovery_points  # Include individual recovery points
                })
                
                if progress_callback:
                    progress_callback(f"  ✓ VM: {vm_name} ({len(recovery_points)} RPs, {format_bytes(vm_reclaimable)})")
        
        except Exception as e:
            # Log error but continue processing other VMs
            with lock:
                processed_count[0] += 1  # Count as processed even if failed
            if progress_callback:
                progress_callback(f"  ⚠️ Skipped VM {vm_uuid[:8]}: {str(e)[:100]}")
    
    # Create work queue
    work_queue = Queue()
    for idx, vm in enumerate(vms, 1):
        work_queue.put((vm, idx))
    
    # Worker thread function
    def worker():
        while True:
            try:
                vm, idx = work_queue.get(timeout=1)
            except Empty:
                # Queue is empty, exit worker
                break
            
            try:
                process_vm(vm, idx)
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
    
    return summary

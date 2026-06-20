"""VM power operations (on/off) for bulk operations using v1/v2 APIs."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import requests
import urllib3

from pc_api_auth import COOKIE_REFRESH_SEC, get_cookie

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class PowerOpConfig:
    """Configuration for VM power operations."""
    
    def __init__(
        self,
        pc_host: str,
        pc_user: str,
        pc_password: str,
        power_action: str,  # "on" or "off"
        vm_uuids: List[str],
        concurrent_ops: int = 5,
        check_interval: int = 5,
        max_retries: int = 12,
        timeout: int = 60,
    ):
        self.pc_host = pc_host
        self.pc_user = pc_user
        self.pc_password = pc_password
        self.power_action = power_action.lower()
        self.vm_uuids = vm_uuids
        self.concurrent_ops = max(1, min(concurrent_ops, 20))
        self.check_interval = check_interval
        self.max_retries = max_retries
        self.timeout = timeout
        
        if self.power_action not in ("on", "off"):
            raise ValueError(f"power_action must be 'on' or 'off', got: {power_action}")


class RunCancelled(Exception):
    """Raised when a power operation run is cancelled."""
    pass


# Global state for tracking VM power operations
power_activity_lock = threading.Lock()
vm_power_inflight: Dict[str, Dict[str, Any]] = {}
vm_power_completed: List[Dict[str, Any]] = []
_vm_power_completed_cap = 8000


def _new_pc_session(config: PowerOpConfig, *, force: bool = False) -> requests.Session:
    """Create a session with fresh PC cookie auth."""
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    get_cookie(
        session,
        f"https://{config.pc_host}:9440",
        config.pc_user,
        config.pc_password,
        force=force,
        refresh_sec=COOKIE_REFRESH_SEC,
    )
    return session


def _power_operation_vm(
    vm_uuid: str,
    vm_name: str,
    power_action: str,
    config: PowerOpConfig,
    progress_callback: Optional[Callable] = None,
    cancel_event: Optional[threading.Event] = None,
) -> Dict[str, Any]:
    """
    Perform power operation on a single VM.
    
    Returns dict with:
        - success: bool
        - vm_uuid: str
        - vm_name: str
        - message: str
        - duration: float
    """
    base_url = f"https://{config.pc_host}:9440"
    session = _new_pc_session(config)
    
    act_key = vm_uuid
    t_start = time.perf_counter()
    
    try:
        # Record as in-flight
        with power_activity_lock:
            vm_power_inflight[act_key] = {
                "vm_name": vm_name,
                "vm_uuid": vm_uuid,
                "action": power_action,
                "t0": t_start,
            }
        
        if progress_callback:
            progress_callback()
        
        # Check for cancellation
        if cancel_event and cancel_event.is_set():
            raise RunCancelled("Operation cancelled by user")
        
        # Initiate power operation using v2 API
        power_url = f"{base_url}/PrismGateway/services/rest/v2.0/vms/{vm_uuid}/set_power_state"
        payload = {"transition": power_action}
        
        logger.info(f"Sending power {power_action} request for VM: {vm_name} ({vm_uuid})")
        
        get_cookie(
            session,
            base_url,
            config.pc_user,
            config.pc_password,
            refresh_sec=COOKIE_REFRESH_SEC,
        )
        response = session.post(
            power_url,
            json=payload,
            verify=False,
            timeout=config.timeout
        )
        
        if response.status_code not in [200, 201]:
            error_msg = f"Power {power_action} request failed: HTTP {response.status_code}"
            logger.error(f"{error_msg} - {response.text[:200]}")
            return {
                "success": False,
                "vm_uuid": vm_uuid,
                "vm_name": vm_name,
                "message": error_msg,
                "duration": time.perf_counter() - t_start
            }
        
        result = response.json()
        task_uuid = result.get('task_uuid')
        
        if not task_uuid:
            error_msg = "No task_uuid in response"
            logger.error(error_msg)
            return {
                "success": False,
                "vm_uuid": vm_uuid,
                "vm_name": vm_name,
                "message": error_msg,
                "duration": time.perf_counter() - t_start
            }
        
        logger.info(f"Power {power_action} task initiated for {vm_name}: {task_uuid}")
        
        # Wait for task completion
        task_success = _wait_for_task_completion(
            task_uuid,
            vm_name,
            config,
            session,
            base_url,
            cancel_event
        )
        
        duration = time.perf_counter() - t_start
        
        if task_success:
            logger.info(f"✓ VM {vm_name} powered {power_action} successfully ({duration:.1f}s)")
            return {
                "success": True,
                "vm_uuid": vm_uuid,
                "vm_name": vm_name,
                "message": f"Powered {power_action} successfully",
                "duration": duration
            }
        else:
            logger.error(f"✗ Power {power_action} task failed for {vm_name}")
            return {
                "success": False,
                "vm_uuid": vm_uuid,
                "vm_name": vm_name,
                "message": f"Task failed or timed out",
                "duration": duration
            }
            
    except RunCancelled:
        logger.warning(f"Power operation cancelled for {vm_name}")
        return {
            "success": False,
            "vm_uuid": vm_uuid,
            "vm_name": vm_name,
            "message": "Cancelled by user",
            "duration": time.perf_counter() - t_start
        }
    except Exception as e:
        logger.error(f"Error powering {power_action} VM {vm_name}: {e}", exc_info=True)
        return {
            "success": False,
            "vm_uuid": vm_uuid,
            "vm_name": vm_name,
            "message": f"Error: {str(e)}",
            "duration": time.perf_counter() - t_start
        }
    finally:
        # Move from inflight to completed
        duration = time.perf_counter() - t_start
        with power_activity_lock:
            vm_power_inflight.pop(act_key, None)
            vm_power_completed.append({
                "vm_name": vm_name,
                "vm_uuid": vm_uuid,
                "action": power_action,
                "state": "ok" if act_key not in vm_power_inflight else "fail",
                "seconds": round(duration, 1),
            })
            while len(vm_power_completed) > _vm_power_completed_cap:
                del vm_power_completed[0]
        
        if progress_callback:
            progress_callback()


def _wait_for_task_completion(
    task_uuid: str,
    vm_name: str,
    config: PowerOpConfig,
    session: requests.Session,
    base_url: str,
    cancel_event: Optional[threading.Event] = None,
) -> bool:
    """
    Wait for task to complete using v1 progress_monitors API.
    Returns True if succeeded, False otherwise.
    """
    progress_url = f"{base_url}/PrismGateway/services/rest/v1/progress_monitors"
    
    for attempt in range(1, config.max_retries + 1):
        if cancel_event and cancel_event.is_set():
            raise RunCancelled("Operation cancelled during task wait")
        
        time.sleep(config.check_interval)
        
        try:
            params = {'filterCriteria': f'uuid=={task_uuid}'}
            get_cookie(
                session,
                base_url,
                config.pc_user,
                config.pc_password,
                refresh_sec=COOKIE_REFRESH_SEC,
            )
            response = session.get(
                progress_url,
                params=params,
                verify=False,
                timeout=config.timeout
            )
            
            if response.status_code != 200:
                logger.warning(f"Task status check failed: HTTP {response.status_code}")
                continue
            
            data = response.json()
            entities = data.get('entities', [])
            
            if not entities:
                logger.warning(f"Task {task_uuid} not found in progress monitors")
                continue
            
            task = entities[0]
            status = task.get('status', 'unknown')
            percentage = task.get('percentageCompleted', 0)
            
            logger.debug(f"Task check {attempt}/{config.max_retries}: {vm_name} - {status} ({percentage}%)")
            
            if status == 'succeeded':
                return True
            elif status in ['failed', 'error', 'aborted']:
                logger.error(f"Task failed for {vm_name}: {status}")
                return False
                
        except Exception as e:
            logger.warning(f"Error checking task status (attempt {attempt}): {e}")
    
    logger.warning(f"Task timeout for {vm_name} after {config.max_retries} checks")
    return False


def _get_power_progress(
    total_vms: int,
    successful: int,
    failed: int,
    action: str
) -> Dict[str, Any]:
    """Build current power operation progress snapshot."""
    with power_activity_lock:
        running: List[Dict[str, Any]] = []
        for rec in vm_power_inflight.values():
            t0v = float(rec["t0"])
            running.append({
                "vm_name": rec["vm_name"],
                "vm_uuid": rec["vm_uuid"],
                "action": rec["action"],
                "state": "running",
                "seconds": round(time.perf_counter() - t0v, 1),
            })
        running.sort(key=lambda r: str(r["vm_name"]).lower())
        completed_copy = list(vm_power_completed)
    
    return {
        "total_vms": total_vms,
        "successful": successful,
        "failed": failed,
        "in_progress": len(running),
        "action": action,
        "vm_activity": {
            "running": running,
            "completed": completed_copy,
        }
    }


def run_power_operations(
    config: PowerOpConfig,
    progress_callback: Optional[Callable[[Dict], None]] = None,
    cancel_event: Optional[threading.Event] = None,
    log_file_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run power operations on VMs concurrently.
    
    Args:
        config: PowerOpConfig with operation settings
        progress_callback: Optional callback for progress updates
        cancel_event: Optional threading.Event to signal cancellation
        log_file_path: Optional path for detailed logging
        
    Returns:
        Dict with summary: total, successful, failed, results
    """
    logger.info("=" * 80)
    logger.info(f"VM POWER {config.power_action.upper()} OPERATION STARTED")
    logger.info(f"PC Host: {config.pc_host}")
    logger.info(f"Total VMs: {len(config.vm_uuids)}")
    logger.info(f"Concurrent Operations: {config.concurrent_ops}")
    logger.info("=" * 80)
    
    total_vms = len(config.vm_uuids)
    successful = 0
    failed = 0
    results = []
    
    def progress_update():
        """Emit progress update."""
        if progress_callback:
            progress = _get_power_progress(total_vms, successful, failed, config.power_action)
            progress_callback(progress)
    
    # Fetch VM names
    vm_info_map = _fetch_vm_names(config)
    
    # Initial progress
    progress_update()
    
    # Execute power operations concurrently
    with ThreadPoolExecutor(max_workers=config.concurrent_ops) as executor:
        futures = []
        
        for vm_uuid in config.vm_uuids:
            if cancel_event and cancel_event.is_set():
                logger.warning("Power operation cancelled before starting all VMs")
                break
            
            vm_name = vm_info_map.get(vm_uuid, vm_uuid[:8])
            
            future = executor.submit(
                _power_operation_vm,
                vm_uuid,
                vm_name,
                config.power_action,
                config,
                progress_update,
                cancel_event
            )
            futures.append(future)
        
        # Collect results
        for future in futures:
            try:
                result = future.result(timeout=config.timeout * config.max_retries)
                results.append(result)
                
                if result["success"]:
                    successful += 1
                else:
                    failed += 1
                    
                progress_update()
                
            except Exception as e:
                logger.error(f"Future execution error: {e}")
                failed += 1
                progress_update()
    
    # Final summary
    summary = {
        "total": total_vms,
        "successful": successful,
        "failed": failed,
        "action": config.power_action,
        "results": results
    }
    
    logger.info("=" * 80)
    logger.info(f"POWER {config.power_action.upper()} OPERATION COMPLETED")
    logger.info(f"Total: {total_vms}, Success: {successful}, Failed: {failed}")
    logger.info(f"Success Rate: {(successful/total_vms*100):.1f}%" if total_vms > 0 else "N/A")
    logger.info("=" * 80)
    
    return summary


def _fetch_vm_names(config: PowerOpConfig) -> Dict[str, str]:
    """Fetch VM names for given UUIDs."""
    vm_map = {}
    
    base_url = f"https://{config.pc_host}:9440"
    session = _new_pc_session(config, force=True)
    
    try:
        # Use v1 API to fetch VM names
        vms_url = f"{base_url}/PrismGateway/services/rest/v1/vms"
        params = {
            'count': 500,
            'projection': 'basicInfo'
        }
        
        get_cookie(
            session,
            base_url,
            config.pc_user,
            config.pc_password,
            refresh_sec=COOKIE_REFRESH_SEC,
        )
        response = session.get(
            vms_url,
            params=params,
            verify=False,
            timeout=60
        )
        
        if response.status_code == 200:
            data = response.json()
            entities = data.get('entities', [])
            
            for entity in entities:
                vm_uuid = entity.get('uuid')
                vm_name = entity.get('vmName', 'Unknown')
                if vm_uuid:
                    vm_map[vm_uuid] = vm_name
            
            logger.info(f"Fetched {len(vm_map)} VM names from Prism")
        else:
            logger.warning(f"Failed to fetch VM names: HTTP {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error fetching VM names: {e}")
    
    return vm_map

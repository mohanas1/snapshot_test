"""
Cache management for recovery points analysis results.
Stores analysis results by PC IP to avoid repeated expensive API calls.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

# Cache directory
CACHE_DIR = Path(__file__).parent / "data" / "recovery_points_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _get_cache_file(pc_ip: str) -> Path:
    """Get cache file path for a specific PC IP."""
    # Sanitize IP for filename
    safe_ip = pc_ip.replace('.', '_').replace(':', '_')
    return CACHE_DIR / f"recovery_points_{safe_ip}.json"


def get_cached_result(pc_ip: str) -> Optional[Dict]:
    """
    Retrieve cached recovery points analysis for a PC IP.
    
    Args:
        pc_ip: Prism Central IP address
        
    Returns:
        Cached analysis result dict with 'summary' and 'cached_at' keys,
        or None if no cache exists
    """
    cache_file = _get_cache_file(pc_ip)
    
    if not cache_file.exists():
        return None
    
    try:
        with open(cache_file, 'r') as f:
            data = json.load(f)
            return data
    except Exception as e:
        print(f"Error reading cache for {pc_ip}: {e}")
        return None


def save_result(pc_ip: str, summary: Dict) -> None:
    """
    Save recovery points analysis result to cache.
    
    Args:
        pc_ip: Prism Central IP address
        summary: Analysis summary dict to cache
    """
    cache_file = _get_cache_file(pc_ip)
    
    cache_data = {
        'pc_ip': pc_ip,
        'cached_at': datetime.now().isoformat(),
        'summary': summary
    }
    
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"Error saving cache for {pc_ip}: {e}")


def clear_cache(pc_ip: str) -> bool:
    """
    Clear cached result for a specific PC IP.
    
    Args:
        pc_ip: Prism Central IP address
        
    Returns:
        True if cache was cleared, False if no cache existed
    """
    cache_file = _get_cache_file(pc_ip)
    
    if cache_file.exists():
        try:
            cache_file.unlink()
            return True
        except Exception as e:
            print(f"Error clearing cache for {pc_ip}: {e}")
            return False
    
    return False


def list_cached_clusters() -> list:
    """
    List all clusters that have cached results.
    
    Returns:
        List of dicts with 'pc_ip', 'cached_at', 'total_vms', 'total_reclaimable' keys
    """
    cached_clusters = []
    
    for cache_file in CACHE_DIR.glob("recovery_points_*.json"):
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                summary = data.get('summary', {})
                
                cached_clusters.append({
                    'pc_ip': data.get('pc_ip'),
                    'cached_at': data.get('cached_at'),
                    'total_vms': summary.get('total_vms', 0),
                    'total_recovery_points': summary.get('total_recovery_points', 0),
                    'total_reclaimable_formatted': summary.get('total_reclaimable_formatted', '0 B')
                })
        except Exception as e:
            print(f"Error reading cache file {cache_file}: {e}")
    
    # Sort by cached_at descending (most recent first)
    cached_clusters.sort(key=lambda x: x.get('cached_at', ''), reverse=True)
    
    return cached_clusters

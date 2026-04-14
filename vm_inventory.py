"""mh_vm inventory via Prism groups API — summaries for the bulk snapshots UI."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import requests

from snapshot_runner import GROUPS_PATH, TLS_VERIFY

GROUP_MEMBER_ATTRIBUTES: List[Dict[str, str]] = [
    {"attribute": "vm_name"},
    {"attribute": "ip_addresses"},
    {"attribute": "power_state"},
    {"attribute": "num_vcpus"},
    {"attribute": "memory_size_bytes"},
    {"attribute": "cluster_name"},
]


def _group_body(page: int, offset: int) -> Dict[str, Any]:
    return {
        "entity_type": "mh_vm",
        "query_name": "",
        "grouping_attribute": " ",
        "group_count": 20,
        "group_offset": 0,
        "group_attributes": [],
        "group_member_count": page,
        "group_member_offset": offset,
        "group_member_sort_attribute": "vm_name",
        "group_member_sort_order": "ASCENDING",
        "group_member_attributes": GROUP_MEMBER_ATTRIBUTES,
        "filter_criteria": "is_cvm==0",
    }


def _first_scalar(block: Dict[str, Any]) -> Optional[str]:
    for tv in block.get("values") or []:
        vals = tv.get("values") or []
        if vals:
            return str(vals[0])
    return None


def _ip_list(block: Dict[str, Any]) -> List[str]:
    ips: List[str] = []
    for tv in block.get("values") or []:
        for ip in tv.get("values") or []:
            if ip:
                ips.append(str(ip))
    return ips


def _safe_int(raw: Optional[str]) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(float(str(raw).strip()))
    except (ValueError, TypeError):
        return None


def _memory_mib_from_bytes(bytes_val: Optional[int]) -> Optional[int]:
    """Exact RAM size in MiB (1024² bytes), matching common VM memory sizing."""
    if bytes_val is None or bytes_val < 0:
        return None
    return bytes_val // (1024 * 1024)


def _parse_row(ent: Dict[str, Any]) -> Dict[str, Any]:
    name: Optional[str] = None
    ips: List[str] = []
    power: Optional[str] = None
    vcpus: Optional[int] = None
    mem_bytes: Optional[int] = None
    cluster: Optional[str] = None

    for block in ent.get("data") or []:
        bname = block.get("name")
        if bname == "vm_name":
            name = _first_scalar(block)
        elif bname == "ip_addresses":
            ips = _ip_list(block)
        elif bname == "power_state":
            power = _first_scalar(block)
        elif bname == "num_vcpus":
            vcpus = _safe_int(_first_scalar(block))
        elif bname == "memory_size_bytes":
            mem_bytes = _safe_int(_first_scalar(block))
        elif bname == "cluster_name":
            cluster = _first_scalar(block)

    mib = _memory_mib_from_bytes(mem_bytes)
    return {
        "name": name,
        "ips": ips,
        "power_state": power,
        "num_vcpus": vcpus,
        "memory_mib": mib,
        "cluster_name": cluster or "—",
    }


def fetch_vm_inventory_rows(
    session: requests.Session,
    base_url: str,
    *,
    page_size: int = 500,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Page through all non-CVM mh_vm entities; return one row per VM (deduped by entity_id)
    and the number of duplicate rows skipped (should normally be 0).
    """
    base = base_url.rstrip("/")
    url = base + GROUPS_PATH
    page = max(1, page_size)
    seen: Dict[str, Dict[str, Any]] = {}
    dup = 0
    offset = 0

    while True:
        body = _group_body(page, offset)
        r = session.post(url, json=body, verify=TLS_VERIFY, timeout=120)
        r.raise_for_status()
        data = r.json()
        groups = data.get("group_results") or []
        if not groups:
            break

        page_n = 0
        for group in groups:
            for ent in group.get("entity_results") or []:
                eid = ent.get("entity_id")
                if not eid:
                    continue
                page_n += 1
                key = str(eid)
                if key in seen:
                    dup += 1
                    continue
                row = _parse_row(ent)
                row["uuid"] = key
                seen[key] = row

        if not page_n or page_n < page:
            break
        offset += page

    return list(seen.values()), dup


def summarize_inventory_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(rows)
    with_ip = sum(1 for r in rows if r.get("ips"))
    power_c = Counter((r.get("power_state") or "unknown") for r in rows)
    vcpu_c = Counter()
    for r in rows:
        v = r.get("num_vcpus")
        vcpu_c[str(v) if v is not None else "unknown"] += 1
    mem_c: Counter[Any] = Counter()
    for r in rows:
        m = r.get("memory_mib")
        if m is None:
            mem_c[None] += 1
        else:
            mem_c[int(m)] += 1
    cluster_c = Counter((r.get("cluster_name") or "—") for r in rows)

    def _vcpu_sort_key(k: str) -> Tuple[int, str]:
        if k == "unknown":
            return (10**9, k)
        try:
            return (int(k), k)
        except ValueError:
            return (10**9 - 1, k)

    vcpu_breakdown = [
        {"vcpus": k, "count": vcpu_c[k]}
        for k in sorted(vcpu_c.keys(), key=_vcpu_sort_key)
    ]
    cluster_breakdown = [
        {"cluster": name, "count": cluster_c[name]}
        for name in sorted(cluster_c.keys(), key=lambda c: (-cluster_c[c], c))
    ]
    power_breakdown = [
        {"power_state": k, "count": power_c[k]}
        for k in sorted(power_c.keys(), key=lambda x: (-power_c[x], x))
    ]
    def _mem_sort_key(k: Any) -> Tuple[int, int]:
        if k is None:
            return (1, 0)
        return (0, int(k))

    memory_breakdown = [
        {
            "mb": k,
            "label": "unknown" if k is None else f"{int(k)} MB",
            "count": mem_c[k],
        }
        for k in sorted(mem_c.keys(), key=_mem_sort_key)
    ]

    return {
        "total_vms": n,
        "with_ip": with_ip,
        "without_ip": n - with_ip,
        "power_state": power_breakdown,
        "num_vcpus": vcpu_breakdown,
        "memory": memory_breakdown,
        "by_cluster": cluster_breakdown,
    }

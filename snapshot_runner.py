"""Bulk VM snapshot run — library entry: run_snapshots(cfg, logger[, cancel_event])."""

from __future__ import annotations

import base64
import concurrent.futures
import datetime as dt
import json
import logging
import random
import re
import threading
import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from pc_api_auth import COOKIE_REFRESH_SEC, get_cookie

GROUPS_PATH = "/api/nutanix/v3/groups"
TASKS_LIST_PATH = "/api/prism/v4.1/config/tasks"
VERSIONS_PATH = "/api/nutanix/v3/versions"
ERGON_PREFIX = base64.b64encode(b"ergon").decode("ascii")
TERMINAL = frozenset({"SUCCEEDED", "FAILED", "ABORTED", "CANCELED", "CANCELLED"})
# Lab/self-signed PC: never verify TLS for requests.
TLS_VERIFY = False

RECOVERY_CRASH = "CRASH_CONSISTENT"
RECOVERY_APP = "APPLICATION_CONSISTENT"
# Config / form value: pick CRASH vs APP independently per VM.
RANDOM_CRASH_OR_APP = "RANDOM_CRASH_OR_APP"
SNAPSHOT_API_RETRY_HTTP_CODES = frozenset({401, 429})
SNAPSHOT_API_MAX_RETRIES = 1
SNAPSHOT_API_429_BACKOFF_BASE_SEC = 0.8


class RunCancelled(Exception):
    """Raised when the UI requests an abort (cancel_event is set)."""


def _ensure_fresh_pc_cookie(
    session: requests.Session,
    base: str,
    cfg: "SnapshotConfig",
    *,
    force: bool = False,
) -> None:
    get_cookie(
        session,
        base.rstrip("/"),
        cfg.pc_user,
        cfg.pc_password,
        force=force,
        refresh_sec=COOKIE_REFRESH_SEC,
    )


def _request_with_cookie_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    base: str,
    cfg: "SnapshotConfig",
    log: Optional[logging.Logger] = None,
    timeout: float = 120,
    **kwargs: Any,
) -> requests.Response:
    """Issue request with one forced-cookie retry on 401/429."""
    _ensure_fresh_pc_cookie(session, base, cfg)
    resp = session.request(method, url, timeout=timeout, **kwargs)
    if resp.status_code not in SNAPSHOT_API_RETRY_HTTP_CODES:
        return resp
    if log:
        log.warning(
            "Snapshot API %s %s got HTTP %s; forcing cookie refresh and retrying once.",
            method.upper(),
            url,
            resp.status_code,
        )
    if resp.status_code == 429:
        backoff = SNAPSHOT_API_429_BACKOFF_BASE_SEC * (1.0 + random.random() * 0.25)
        if log:
            log.warning("429 backoff %.2fs before retry.", backoff)
        time.sleep(backoff)
    _ensure_fresh_pc_cookie(session, base, cfg, force=True)
    return session.request(method, url, timeout=timeout, **kwargs)


@dataclass
class SnapshotConfig:
    base_url: str
    pc_user: str
    pc_password: str
    batch_size: int = 10
    recovery_point_type: str = "CRASH_CONSISTENT"
    expiration_days: int = 1
    poll_interval: float = 2.0
    task_timeout_sec: int = 30
    group_member_page: int = 500
    sleep_before_task_poll_sec: float = 4.0
    # "series" = one snapshot POST at a time; "parallel" = all POSTs in a batch concurrently.
    snapshot_trigger_mode: str = "series"
    skip_substrings: Tuple[str, ...] = ()
    skip_regex_patterns: Tuple[str, ...] = ()
    # Optional explicit VM UUID allow-list (used by scheduled full pipeline handoff).
    target_vm_uuids: Tuple[str, ...] = ()

    _compiled_regexes: Tuple[Any, ...] = field(default_factory=tuple, repr=False)

    def compile_regexes(self) -> None:
        object.__setattr__(
            self,
            "_compiled_regexes",
            tuple(re.compile(p, re.IGNORECASE) for p in self.skip_regex_patterns),
        )


def _pick_recovery_point_type(
    cfg: SnapshotConfig,
    random_counts: Dict[str, int],
    random_lock: Optional[threading.Lock],
) -> str:
    if cfg.recovery_point_type != RANDOM_CRASH_OR_APP:
        return cfg.recovery_point_type
    choice = random.choice([RECOVERY_CRASH, RECOVERY_APP])

    def _bump() -> None:
        if choice == RECOVERY_CRASH:
            random_counts["crash"] = random_counts.get("crash", 0) + 1
        else:
            random_counts["app"] = random_counts.get("app", 0) + 1

    if random_lock:
        with random_lock:
            _bump()
    else:
        _bump()
    return choice


def _group_payload(page: int) -> Dict[str, Any]:
    return {
        "entity_type": "mh_vm",
        "query_name": "",
        "grouping_attribute": " ",
        "group_count": 20,
        "group_offset": 0,
        "group_attributes": [],
        "group_member_count": page,
        "group_member_offset": 0,
        "group_member_sort_attribute": "vm_name",
        "group_member_sort_order": "ASCENDING",
        "group_member_attributes": [{"attribute": "vm_name"}],
        "filter_criteria": "is_cvm==0",
    }


def _vm_name_should_skip(name: Optional[str], cfg: SnapshotConfig) -> bool:
    if not name:
        return False
    low = name.lower()
    if any(s.lower() in low for s in cfg.skip_substrings):
        return True
    return any(rx.search(name) for rx in cfg._compiled_regexes)


def list_all_vm_uuids(
    session: requests.Session,
    base: str,
    cfg: SnapshotConfig,
    log: Optional[logging.Logger] = None,
) -> Tuple[List[Tuple[str, Optional[str]]], int]:
    url = base + GROUPS_PATH
    seen: Dict[str, Optional[str]] = {}
    allow: Optional[set[str]] = None
    if cfg.target_vm_uuids:
        allow = {str(v).strip() for v in cfg.target_vm_uuids if str(v).strip()}
    group_member_offset = 0
    ignored_by_name = 0
    page = cfg.group_member_page

    while True:
        body = dict(_group_payload(page))
        body["group_member_offset"] = group_member_offset
        body["group_member_count"] = page
        r = _request_with_cookie_retry(
            session,
            "POST",
            url,
            base=base,
            cfg=cfg,
            log=log,
            json=body,
            verify=TLS_VERIFY,
        )
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
                if allow is not None and key not in allow:
                    continue
                if key in seen:
                    continue
                name = None
                for block in ent.get("data") or []:
                    if block.get("name") != "vm_name":
                        continue
                    for tv in block.get("values") or []:
                        vals = tv.get("values") or []
                        if vals:
                            name = str(vals[0])
                            break
                    break
                if _vm_name_should_skip(name, cfg):
                    ignored_by_name += 1
                    continue
                seen[key] = name

        if not page_n or page_n < page:
            break
        group_member_offset += page

    return list(seen.items()), ignored_by_name


def take_snapshot(
    session: requests.Session,
    base: str,
    cfg: SnapshotConfig,
    vms_snap_prefix: str,
    vm_uuid: str,
    snap_name: str,
    expiration_iso: str,
    recovery_point_type: str,
    log: Optional[logging.Logger] = None,
) -> str:
    r = _request_with_cookie_retry(
        session,
        "POST",
        f"{vms_snap_prefix}{vm_uuid}/snapshot",
        base=base,
        cfg=cfg,
        log=log,
        json={
            "name": snap_name,
            "recovery_point_type": recovery_point_type,
            "expiration_time": expiration_iso,
        },
        verify=TLS_VERIFY,
    )
    if not r.ok:
        raise RuntimeError(f"{r.status_code} {r.text[:500]}")
    body = r.json()
    tu = body.get("task_uuid") or (
        (body.get("status") or {}).get("execution_context") or {}
    ).get("task_uuid")
    if not tu:
        raise RuntimeError(json.dumps(body)[:500])
    return str(tu)


def wait_batch(
    session: requests.Session,
    base: str,
    tasks: List[Dict[str, Any]],
    tally: Dict[str, int],
    cfg: SnapshotConfig,
    log: logging.Logger,
    cancel_event: Optional[threading.Event] = None,
) -> None:
    pending = {f"{ERGON_PREFIX}:{t['task_uuid'].strip()}": t for t in tasks}
    tasks_url = base + TASKS_LIST_PATH
    deadline = time.monotonic() + cfg.task_timeout_sec

    while pending:
        if cancel_event is not None and cancel_event.is_set():
            tally["other"] += len(pending)
            log.warning("Cancelled while waiting: %d task(s) left pending.", len(pending))
            raise RunCancelled()
        if time.monotonic() > deadline:
            tally["other"] += len(pending)
            raise TimeoutError(
                f"{len(pending)} task(s) still pending after {cfg.task_timeout_sec}s"
            )

        filt = "(" + " or ".join(
            "extId eq '" + e.replace("'", "''") + "'" for e in pending
        ) + ")"
        r = _request_with_cookie_retry(
            session,
            "GET",
            tasks_url,
            base=base,
            cfg=cfg,
            log=log,
            params={
                "$page": 0,
                "$limit": max(100, len(pending)),
                "$orderBy": "lastUpdatedTime desc",
                "$filter": filt,
            },
            verify=TLS_VERIFY,
        )
        r.raise_for_status()
        for row in r.json().get("data") or []:
            eid = row.get("extId")
            if not eid:
                continue
            ext = str(eid)
            if ext not in pending:
                continue
            st = str(row.get("status") or "UNKNOWN").upper()
            if st not in TERMINAL:
                continue
            meta = pending.pop(ext)
            tu, vm = meta["task_uuid"], meta["vm_uuid"]
            nm = meta.get("vm_name") or ""
            if st == "SUCCEEDED":
                tally["succeeded"] += 1
            elif st == "FAILED":
                tally["failed"] += 1
            else:
                tally["other"] += 1
            log.info(
                "  task %s… VM %s… (%s) -> %s",
                str(tu)[:8],
                str(vm)[:8],
                nm,
                st,
            )
            if st != "SUCCEEDED":
                log.warning("    WARNING: VM %s", vm)

        if pending:
            log.info(
                "  %d task(s) pending, sleeping %.1fs",
                len(pending),
                cfg.poll_interval,
            )
            time.sleep(cfg.poll_interval)


def run_snapshots(
    cfg: SnapshotConfig,
    log: logging.Logger,
    cancel_event: Optional[threading.Event] = None,
    *,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    t_wall0 = time.perf_counter()
    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    cfg.compile_regexes()
    base = cfg.base_url.rstrip("/")
    vms_snap_prefix = f"{base}/api/nutanix/v3/vms/"

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    setattr(session, "_pc_user", cfg.pc_user)
    setattr(session, "_pc_password", cfg.pc_password)
    _ensure_fresh_pc_cookie(session, base, cfg, force=True)

    def _abort_if_needed() -> None:
        if cancel_event is not None and cancel_event.is_set():
            log.warning("Run cancelled by user; stopping.")
            raise RunCancelled()

    _abort_if_needed()
    log.info("Listing VMs…")
    vms, ignored_name = list_all_vm_uuids(session, base, cfg, log=log)
    _abort_if_needed()
    tally: Dict[str, int] = {
        "ignored": ignored_name,
        "succeeded": 0,
        "failed": 0,
        "other": 0,
    }
    log.info(
        "Found %d VMs to snapshot (ignored by name rules: %d).",
        len(vms),
        ignored_name,
    )
    for eid, name in vms[:15]:
        log.info("  %s  %s", eid, name or "")
    if len(vms) > 15:
        log.info("  … +%d more", len(vms) - 15)

    n_vms = len(vms)

    def _emit_sp() -> None:
        if progress_callback is None:
            return
        done = int(tally["succeeded"]) + int(tally["failed"]) + int(tally["other"])
        done = max(0, min(done, n_vms))
        try:
            progress_callback(
                {"overall_done": done, "overall_total": int(n_vms)}
            )
        except Exception:
            pass

    exp = (
        dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=cfg.expiration_days)
    ).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S.00Z")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    mode = (cfg.snapshot_trigger_mode or "series").lower()
    log.info("Snapshot API trigger mode: %s", mode)
    random_rp: Dict[str, int] = {"crash": 0, "app": 0}
    rp_random_lock = threading.Lock() if mode == "parallel" else None

    def _wait_and_clear(batch: List[Dict[str, Any]], label: str) -> None:
        log.info("--- %s: wait %d tasks (v4 list) ---", label, len(batch))
        try:
            _abort_if_needed()
            if cfg.sleep_before_task_poll_sec > 0:
                log.info(
                    "waiting %.1fs before polling task status",
                    cfg.sleep_before_task_poll_sec,
                )
                time.sleep(cfg.sleep_before_task_poll_sec)
                _abort_if_needed()
            wait_batch(session, base, batch, tally, cfg, log, cancel_event)
        except TimeoutError as e:
            log.error("  %s", e)
        batch.clear()
        _emit_sp()

    _emit_sp()

    if mode == "parallel":

        def _parallel_snap_one(
            tup: Tuple[int, str, Optional[str]],
        ) -> Tuple[bool, Dict[str, Any], Optional[Exception]]:
            i, vm_uuid, vm_name = tup
            snap = f"bulk_{stamp}_{i + 1}"
            rpt = _pick_recovery_point_type(cfg, random_rp, rp_random_lock)
            log.info(
                "[%d/%d] Snapshot %s… (%s) [%s]",
                i + 1,
                n_vms,
                vm_uuid[:8],
                vm_name or "?",
                rpt,
            )
            thread_sess = requests.Session()
            thread_sess.headers["Content-Type"] = "application/json"
            setattr(thread_sess, "_pc_user", cfg.pc_user)
            setattr(thread_sess, "_pc_password", cfg.pc_password)
            _ensure_fresh_pc_cookie(thread_sess, base, cfg, force=True)
            try:
                task_uuid = take_snapshot(
                    thread_sess, base, cfg, vms_snap_prefix, vm_uuid, snap, exp, rpt, log=log
                )
                return (
                    True,
                    {
                        "task_uuid": task_uuid,
                        "vm_uuid": vm_uuid,
                        "vm_name": vm_name,
                        "snapshot_name": snap,
                    },
                    None,
                )
            except Exception as e:
                return False, {}, e

        wave_start = 0
        while wave_start < n_vms:
            _abort_if_needed()
            wave_end = min(wave_start + cfg.batch_size, n_vms)
            wave_tuples = [
                (wave_start + j, vms[wave_start + j][0], vms[wave_start + j][1])
                for j in range(wave_end - wave_start)
            ]
            log.info(
                "Parallel snapshot API wave: VMs %d–%d (%d POSTs)",
                wave_start + 1,
                wave_end,
                len(wave_tuples),
            )
            batch: List[Dict[str, Any]] = []
            workers = max(1, len(wave_tuples))
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                for ok, meta, err in pool.map(_parallel_snap_one, wave_tuples):
                    if ok:
                        batch.append(meta)
                    else:
                        tally["failed"] += 1
                        log.error("  FAILED: %s", err)
            _abort_if_needed()
            if batch:
                _wait_and_clear(batch, "wave")
            _emit_sp()
            wave_start = wave_end
    else:
        batch: List[Dict[str, Any]] = []
        for i, (vm_uuid, vm_name) in enumerate(vms):
            _abort_if_needed()
            snap = f"bulk_{stamp}_{i + 1}"
            rpt = _pick_recovery_point_type(cfg, random_rp, None)
            log.info(
                "[%d/%d] Snapshot %s… (%s) [%s]",
                i + 1,
                n_vms,
                vm_uuid[:8],
                vm_name or "?",
                rpt,
            )
            try:
                task_uuid = take_snapshot(
                    session, base, cfg, vms_snap_prefix, vm_uuid, snap, exp, rpt, log=log
                )
            except Exception as e:
                tally["failed"] += 1
                log.error("  FAILED: %s", e)
                _emit_sp()
                continue

            batch.append(
                {
                    "task_uuid": task_uuid,
                    "vm_uuid": vm_uuid,
                    "vm_name": vm_name,
                    "snapshot_name": snap,
                }
            )

            if len(batch) >= cfg.batch_size:
                _wait_and_clear(batch, "batch")

        if batch:
            _wait_and_clear(batch, "final")

    log.info("Done.")
    if cfg.recovery_point_type == RANDOM_CRASH_OR_APP:
        log.info(
            "\n=== Summary ===\n"
            "  recovery types (random per VM): "
            "CRASH_CONSISTENT=%d, APPLICATION_CONSISTENT=%d\n"
            "  ignored (name rules):  %d\n"
            "  succeeded (tasks):     %d\n"
            "  failed (API or task): %d\n"
            "  other (task status / timed out): %d",
            random_rp.get("crash", 0),
            random_rp.get("app", 0),
            tally["ignored"],
            tally["succeeded"],
            tally["failed"],
            tally["other"],
        )
    else:
        log.info(
            "\n=== Summary ===\n"
            "  ignored (name rules):  %d\n"
            "  succeeded (tasks):     %d\n"
            "  failed (API or task): %d\n"
            "  other (task status / timed out): %d",
            tally["ignored"],
            tally["succeeded"],
            tally["failed"],
            tally["other"],
        )
    duration_sec = round(time.perf_counter() - t_wall0, 2)
    log.info(
        "Wall clock: %.1fs for %d VM(s) targeted (mode=%s, batch_size=%d)",
        duration_sec,
        n_vms,
        mode,
        cfg.batch_size,
    )
    result: Dict[str, Any] = dict(tally)
    result["duration_sec"] = duration_sec
    result["n_vms"] = n_vms
    result["batch_size"] = cfg.batch_size
    result["snapshot_trigger_mode"] = mode
    if cfg.recovery_point_type == RANDOM_CRASH_OR_APP:
        result["rp_random_crash"] = random_rp.get("crash", 0)
        result["rp_random_app"] = random_rp.get("app", 0)
    else:
        result["rp_random_crash"] = 0
        result["rp_random_app"] = 0
    _emit_sp()
    return result

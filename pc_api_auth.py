"""Shared Prism Central cookie authentication utilities."""

from __future__ import annotations

import copy
import hashlib
import time
from threading import Lock

import requests

TLS_VERIFY = False
COOKIE_REFRESH_SEC = 15 * 60
VERSIONS_PATH = "/api/nutanix/v3/versions"

_COOKIE_CACHE_LOCK = Lock()
_COOKIE_REFRESH_LOCKS: dict[tuple[str, str, str], Lock] = {}
_COOKIE_CACHE: dict[tuple[str, str, str], dict[str, object]] = {}
_COOKIE_DIAG: dict[tuple[str, str], dict[str, object]] = {}


def _get_refresh_lock(key: tuple[str, str, str]) -> Lock:
    with _COOKIE_CACHE_LOCK:
        lock = _COOKIE_REFRESH_LOCKS.get(key)
        if lock is None:
            lock = Lock()
            _COOKIE_REFRESH_LOCKS[key] = lock
        return lock


def _apply_cached_cookie(
    session: requests.Session,
    *,
    cached: dict[str, object],
    base: str,
    user: str,
    pwd: str,
    refreshed_at: float,
) -> None:
    cached_cookies = cached.get("cookies")
    if isinstance(cached_cookies, requests.cookies.RequestsCookieJar):
        session.cookies = copy.deepcopy(cached_cookies)
    setattr(session, "_pc_cookie_refreshed_at", float(refreshed_at))
    setattr(session, "_pc_cookie_base_url", base)
    setattr(session, "_pc_cookie_username", user)
    setattr(session, "_pc_cookie_password", pwd)
    session.auth = None

    fp = str(cached.get("fingerprint", "") or "")
    if fp:
        setattr(session, "_pc_cookie_fingerprint", fp)
        setattr(session, "_pc_cookie_source", "cache")


def _cookie_fingerprint(jar: requests.cookies.RequestsCookieJar) -> str:
    parts: list[str] = []
    for c in jar:
        parts.append(f"{c.domain}|{c.path}|{c.name}|{c.value}")
    payload = "||".join(sorted(parts))
    if not payload:
        return ""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def get_cookie(
    session: requests.Session,
    base_url: str,
    username: str,
    password: str,
    *,
    force: bool = False,
    refresh_sec: int = COOKIE_REFRESH_SEC,
) -> None:
    """Ensure the session has a fresh PC cookie, refreshing every `refresh_sec` seconds."""
    now = time.time()
    last = float(getattr(session, "_pc_cookie_refreshed_at", 0.0) or 0.0)
    if (not force) and last > 0 and (now - last) < max(1, int(refresh_sec)):
        return

    user = str(username or "").strip()
    pwd = str(password or "")
    if not user or not pwd:
        raise ValueError("PC username/password required for cookie bootstrap")

    base = (base_url or "").rstrip("/")
    if not base:
        raise ValueError("PC base URL is required for cookie bootstrap")

    cache_key = (base, user, pwd)
    cookie_lock = _get_refresh_lock(cache_key)
    now = time.time()

    # Fast-path: reuse cookie from shared cache if another session refreshed recently.
    if not force:
        with _COOKIE_CACHE_LOCK:
            cached = _COOKIE_CACHE.get(cache_key)
        if cached is not None:
            cached_refreshed_at = float(cached.get("refreshed_at", 0.0) or 0.0)
            if cached_refreshed_at > 0 and (now - cached_refreshed_at) < max(1, int(refresh_sec)):
                _apply_cached_cookie(
                    session,
                    cached=cached,
                    base=base,
                    user=user,
                    pwd=pwd,
                    refreshed_at=cached_refreshed_at,
                )
                return

    # Serialize refresh/bootstrap per (base,user,password) to avoid cookie churn/races
    # where concurrent sessions invalidate each other's cookie.
    with cookie_lock:
        now = time.time()
        last = float(getattr(session, "_pc_cookie_refreshed_at", 0.0) or 0.0)
        if (not force) and last > 0 and (now - last) < max(1, int(refresh_sec)):
            return

        with _COOKIE_CACHE_LOCK:
            cached = _COOKIE_CACHE.get(cache_key)

        # Even when force=True, avoid immediate duplicate refresh if a parallel thread
        # has just refreshed this credential tuple.
        if cached is not None:
            cached_refreshed_at = float(cached.get("refreshed_at", 0.0) or 0.0)
            just_refreshed = cached_refreshed_at > 0 and (now - cached_refreshed_at) < 5
            cache_fresh = cached_refreshed_at > 0 and (now - cached_refreshed_at) < max(1, int(refresh_sec))
            if cache_fresh or (force and just_refreshed):
                _apply_cached_cookie(
                    session,
                    cached=cached,
                    base=base,
                    user=user,
                    pwd=pwd,
                    refreshed_at=cached_refreshed_at,
                )
                return

    # Bootstrap cookie with basic auth once; subsequent API calls use cookie on the session.
        r = session.get(
            base + VERSIONS_PATH,
            auth=(user, pwd),
            verify=TLS_VERIFY,
            timeout=120,
        )
        r.raise_for_status()

        refreshed_at = time.time()
        setattr(session, "_pc_cookie_refreshed_at", refreshed_at)
        setattr(session, "_pc_cookie_base_url", base)
        setattr(session, "_pc_cookie_username", user)
        setattr(session, "_pc_cookie_password", pwd)
        # Keep requests cookie-only after bootstrap.
        session.auth = None

        with _COOKIE_CACHE_LOCK:
            _COOKIE_CACHE[cache_key] = {
                "cookies": copy.deepcopy(session.cookies),
                "refreshed_at": refreshed_at,
                "fingerprint": _cookie_fingerprint(session.cookies),
            }

            diag_key = (base, user)
            prev_fp = str((_COOKIE_DIAG.get(diag_key) or {}).get("fingerprint", "") or "")
            cur_fp = str(_COOKIE_CACHE[cache_key].get("fingerprint", "") or "")
            _COOKIE_DIAG[diag_key] = {
                "fingerprint": cur_fp,
                "previous_fingerprint": prev_fp,
                "changed": bool(prev_fp and cur_fp and prev_fp != cur_fp),
                "refreshed_at": refreshed_at,
            }

        cur_fp = str(_COOKIE_CACHE[cache_key].get("fingerprint", "") or "")
        setattr(session, "_pc_cookie_fingerprint", cur_fp)
        setattr(session, "_pc_cookie_source", "bootstrap")


def refresh_cookie_if_needed(
    session: requests.Session,
    *,
    force: bool = False,
    refresh_sec: int = COOKIE_REFRESH_SEC,
) -> None:
    """Refresh cookie using credentials/base stored on the session."""
    base = getattr(session, "_pc_cookie_base_url", "")
    user = getattr(session, "_pc_cookie_username", "")
    pwd = getattr(session, "_pc_cookie_password", "")
    if not base or not user:
        raise ValueError("Session is missing cookie bootstrap details")
    get_cookie(
        session,
        str(base),
        str(user),
        str(pwd),
        force=force,
        refresh_sec=refresh_sec,
    )

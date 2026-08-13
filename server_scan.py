"""
Background Real-Debrid CDN scan — benchmark public hosts and pick the fastest.

When RD_PREFERRED_CDN (or REAL_DEBRID_PREFERRED_CDN) is set, the scan is skipped.
When auto-scan is enabled, results are cached on disk with TTL and applied via
realdebrid.set_runtime_preferred_cdn() so no restart is required.
"""
from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from copy import deepcopy

import realdebrid

_STATE_LOCK = threading.Lock()
_SCAN_LOCK = threading.Lock()
_STARTED = False

_state: dict = {
    "state": "disabled",
    "target": None,
    "eta_sec": None,
    "winner": None,
    "mbps": None,
    "at": None,
    "error": None,
}


def _resolve_cache_dir() -> str:
    raw = (os.environ.get("CACHE_DIR") or "").strip()
    if raw:
        return os.path.realpath(raw)

    cache_file = (os.environ.get("CACHE_FILE") or "cache/scenes.json").strip()
    parent = os.path.dirname(cache_file)
    if parent:
        return os.path.realpath(parent)
    return os.path.realpath("cache")


CACHE_DIR = _resolve_cache_dir()


def _env_yes(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _scan_auto_enabled(pinned: bool) -> bool:
    default = not pinned
    if os.environ.get("RD_AUTO_SCAN") is not None:
        return _env_yes("RD_AUTO_SCAN", default=default)
    if os.environ.get("REAL_DEBRID_AUTO_SCAN") is not None:
        return _env_yes("REAL_DEBRID_AUTO_SCAN", default=default)
    return default


def _split_hosts(raw: str) -> list[str]:
    parts = re.split(r"[,;\s]+", raw or "")
    return [p.strip() for p in parts if p.strip()]


def _pinned_cdn_raw() -> str:
    return (
        (os.environ.get("RD_PREFERRED_CDN") or "").strip()
        or (os.environ.get("REAL_DEBRID_PREFERRED_CDN") or "").strip()
    )


def _cache_path(name: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, name)


def _load_cache(name: str) -> dict | None:
    path = _cache_path(name)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _write_cache(name: str, payload: dict) -> None:
    path = _cache_path(name)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def _set_state(**updates) -> None:
    with _STATE_LOCK:
        _state.update(updates)


def get_scan_status() -> dict:
    with _STATE_LOCK:
        return deepcopy(_state)


def _eligible_hosts_for_scan() -> list[str]:
    raw_hosts = (os.environ.get("RD_SCAN_HOSTS") or "").strip()
    if not raw_hosts:
        raw_hosts = (os.environ.get("REAL_DEBRID_SCAN_HOSTS") or "").strip()
    if raw_hosts:
        return [realdebrid.normalize_cdn_host(h) for h in _split_hosts(raw_hosts)]
    return list(realdebrid.DEFAULT_HOSTS)


def _cache_ttl_sec() -> float:
    ttl = _env_float("RD_SCAN_CACHE_TTL_SEC", default=-1.0)
    if ttl >= 0:
        return ttl
    return _env_float("REAL_DEBRID_SCAN_CACHE_TTL_SEC", default=86400.0)


def _start_scan_thread() -> None:
    def run() -> None:
        try:
            hosts = [h for h in _eligible_hosts_for_scan() if h]
            if not hosts:
                raise RuntimeError("No Real-Debrid CDN hosts to scan")

            _set_state(
                state="scanning",
                target={"type": "benchmark_cdn_hosts", "count": len(hosts)},
            )

            seconds = _env_float("RD_SCAN_SECONDS", default=-1.0)
            if seconds < 0:
                seconds = _env_float("REAL_DEBRID_SCAN_SECONDS", default=6.0)

            workers = _env_int("RD_SCAN_WORKERS", default=-1)
            if workers < 1:
                workers = max(1, _env_int("REAL_DEBRID_SCAN_WORKERS", default=8))

            connect_to = _env_float("RD_SCAN_CONNECT_TIMEOUT_SEC", default=-1.0)
            if connect_to < 0:
                connect_to = _env_float("REAL_DEBRID_SCAN_CONNECT_TIMEOUT_SEC", default=8.0)

            results = realdebrid.benchmark_hosts(
                hosts,
                seconds=seconds,
                workers=workers,
                connect_to=connect_to,
            )
            ok = [r for r in results if r.get("ok")]
            if not ok:
                raise RuntimeError("Real-Debrid CDN scan returned no successful results")

            ok.sort(key=lambda r: r["mbps"], reverse=True)
            top = _env_int("RD_SCAN_TOP", default=-1)
            if top < 1:
                top = max(1, _env_int("REAL_DEBRID_SCAN_TOP", default=20))
            winner = ok[:top][0]
            realdebrid.set_runtime_preferred_cdn(winner["host"])

            cache_payload = {
                "host": winner["host"],
                "mbps": winner.get("mbps"),
                "at": time.time(),
            }
            _write_cache("rd-best-cdn.json", cache_payload)

            _set_state(
                state="done",
                target=None,
                winner=winner["host"],
                mbps=winner.get("mbps"),
                at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cache_payload["at"])),
                error=None,
            )
        except Exception as e:
            _set_state(state="error", error=str(e))

    threading.Thread(target=run, daemon=True, name="server-scan-realdebrid").start()


def _maybe_apply_from_cache() -> bool:
    cached = _load_cache("rd-best-cdn.json")
    if not cached:
        return False
    ttl = _cache_ttl_sec()
    if ttl <= 0:
        return False
    at = cached.get("at")
    if not isinstance(at, (int, float)):
        return False
    if time.time() - float(at) > ttl:
        return False

    host = cached.get("host")
    if not host:
        return False
    realdebrid.set_runtime_preferred_cdn(host)
    _set_state(
        state="cached",
        target=None,
        winner=host,
        mbps=cached.get("mbps"),
        at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(at))),
        error=None,
    )
    return True


def start_background_server_scan() -> None:
    """Start the background CDN scan once per process. Safe to call multiple times."""
    with _SCAN_LOCK:
        global _STARTED
        if _STARTED:
            return
        _STARTED = True

        pinned = _pinned_cdn_raw()
        auto = _scan_auto_enabled(pinned)

        if pinned:
            _set_state(state="pinned", winner=pinned, error=None)
            return

        if not auto:
            _set_state(state="disabled", error=None)
            return

        if _maybe_apply_from_cache():
            return

        hosts = [h for h in _eligible_hosts_for_scan() if h]
        seconds = _env_float("RD_SCAN_SECONDS", default=-1.0)
        if seconds < 0:
            seconds = _env_float("REAL_DEBRID_SCAN_SECONDS", default=6.0)
        workers = _env_int("RD_SCAN_WORKERS", default=-1)
        if workers < 1:
            workers = max(1, _env_int("REAL_DEBRID_SCAN_WORKERS", default=8))
        eta_sec = float(seconds) * float(math.ceil(len(hosts) / float(workers)))
        _set_state(
            state="scanning",
            target={"type": "benchmark_cdn_hosts", "count": len(hosts)},
            eta_sec=round(eta_sec, 1),
        )
        _start_scan_thread()

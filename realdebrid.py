"""
Real-Debrid CDN host pinning and speedtest helpers.

After /unrestrict/link, Real-Debrid returns a geo-routed CDN URL. The same
``/d/<id>/file`` path works on every CDN node, so we can rewrite the hostname
to a faster or pinned server (same approach as zurg / apu).
"""
from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable
from urllib.parse import urlparse, urlunparse

import requests

log = logging.getLogger("heremio.realdebrid")

_CDN_HOST_RE = re.compile(
    r"(?:^|\.)("
    r"download\.real-debrid\.(?:com|cloud)"
    r"|(?:f)?cdn\.real-debrid\.com"
    r"|rdeb\.io"
    r")$",
    re.IGNORECASE,
)

DEFAULT_HOSTS = [
    *[f"{n}.download.real-debrid.com" for n in range(20, 24)],
    *[f"{n}.download.real-debrid.com" for n in range(30, 35)],
    *[f"{n}.download.real-debrid.com" for n in range(40, 46)],
    *[f"{n}.download.real-debrid.com" for n in range(50, 70)],
    "rbx.download.real-debrid.com",
    "den1.download.real-debrid.com",
    "sea1.download.real-debrid.com",
    "nyk1.download.real-debrid.com",
    "chi1.download.real-debrid.com",
    "lax1.download.real-debrid.com",
    "mia1.download.real-debrid.com",
    "dal1.download.real-debrid.com",
    "qro1.download.real-debrid.com",
    "sao1.download.real-debrid.com",
    "scl1.download.real-debrid.com",
    "lon1.download.real-debrid.com",
    "hkg1.download.real-debrid.com",
    "sgp1.download.real-debrid.com",
    "tyo1.download.real-debrid.com",
    "mum1.download.real-debrid.com",
    "tlv1.download.real-debrid.com",
    "jnb1.download.real-debrid.com",
    "45.download.real-debrid.cloud",
]

SPEEDTEST_PATHS = ("/speedtest/test.rar", "/speedtest/testDefault.rar")


def is_cdn_link(url: str) -> bool:
    """True when the URL already targets a Real-Debrid CDN host."""
    raw = (url or "").strip()
    if not raw:
        return False
    host = (urlparse(raw).hostname or "").lower()
    if not host:
        return False
    return bool(_CDN_HOST_RE.search(host))


def _normalize_cdn_host(host: str) -> str:
    """Accept ``nyk7-4`` or full ``nyk7-4.download.real-debrid.com``."""
    h = (host or "").strip().lower()
    if not h:
        return ""
    if "real-debrid" in h or h.endswith(".rdeb.io"):
        return h
    return f"{h}.download.real-debrid.com"


def normalize_cdn_host(raw: str) -> str:
    """Public wrapper for normalizing speedtest host inputs."""
    return _normalize_cdn_host(raw)


def _preferred_cdn_raw(explicit: str | None = None) -> str:
    if explicit is not None:
        return explicit.strip()
    return (
        (os.environ.get("RD_PREFERRED_CDN") or "").strip()
        or (os.environ.get("REAL_DEBRID_PREFERRED_CDN") or "").strip()
    )


def preferred_cdn_hosts(*, preferred_cdn: str | None = None) -> list[str]:
    """Preferred CDN hosts from explicit arg or env (RD_PREFERRED_CDN)."""
    raw = _preferred_cdn_raw(preferred_cdn)
    if not raw:
        return []
    parts = re.split(r"[,;\s]+", raw)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        norm = _normalize_cdn_host(part)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def apply_preferred_cdn(
    url: str,
    *,
    preferred_cdn: str | None = None,
    on_log: Callable[[str], None] | None = None,
) -> str:
    """
    Rewrite the CDN hostname on an unrestricted link.

    Real-Debrid mirrors the same ``/d/<id>/file`` path on every CDN node.
    """
    raw = (url or "").strip()
    if not raw or not is_cdn_link(raw):
        return raw

    prefs = preferred_cdn_hosts(preferred_cdn=preferred_cdn)
    if not prefs:
        return raw

    parsed = urlparse(raw)
    orig = (parsed.hostname or "").lower()
    pref = prefs[0]
    if orig == pref:
        return raw

    netloc = pref
    if parsed.port:
        netloc = f"{pref}:{parsed.port}"
    rewritten = urlunparse(parsed._replace(netloc=netloc))
    msg = f"CDN host {orig} → {pref}"
    if on_log:
        on_log(msg)
    else:
        log.debug("RD %s", msg)
    return rewritten


def _mbps(bytes_read: int, elapsed: float) -> float:
    if elapsed <= 0 or bytes_read <= 0:
        return 0.0
    return (bytes_read * 8) / elapsed / 1_000_000


def _probe_host(host: str, seconds: float, connect_to: float) -> dict:
    session = requests.Session()
    session.headers["User-Agent"] = "heremio-rd-cdn-speedtest/1.0"
    last_err = ""

    for path in SPEEDTEST_PATHS:
        url = f"https://{host}{path}"
        started = time.monotonic()
        bytes_read = 0
        try:
            with session.get(url, stream=True, timeout=(connect_to, seconds + 5)) as r:
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}"
                    continue
                for chunk in r.iter_content(chunk_size=256 * 1024):
                    if not chunk:
                        continue
                    bytes_read += len(chunk)
                    if time.monotonic() - started >= seconds:
                        break
        except requests.RequestException as e:
            last_err = str(e)[:120]
            continue

        elapsed = time.monotonic() - started
        if bytes_read > 0 and elapsed > 0:
            return {
                "host": host,
                "ok": True,
                "mbps": _mbps(bytes_read, elapsed),
                "mib_s": bytes_read / elapsed / (1024 * 1024),
                "bytes": bytes_read,
                "seconds": round(elapsed, 2),
                "path": path,
            }
        last_err = last_err or "no data"

    return {"host": host, "ok": False, "error": last_err or "unreachable"}


def benchmark_hosts(
    hosts: list[str],
    *,
    seconds: float,
    workers: int,
    connect_to: float,
) -> list[dict]:
    """Benchmark Real-Debrid CDN hosts by downloading public speedtest files."""
    norm_hosts = []
    seen: set[str] = set()
    for h in hosts:
        norm = normalize_cdn_host(h)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        norm_hosts.append(norm)

    if not norm_hosts:
        return []

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {
            pool.submit(_probe_host, host, seconds, connect_to): host
            for host in norm_hosts
        }
        for fut in as_completed(futs):
            results.append(fut.result())

    ok = [r for r in results if r.get("ok")]
    fail = [r for r in results if not r.get("ok")]
    ok.sort(key=lambda r: r["mbps"], reverse=True)
    return [*ok, *fail]

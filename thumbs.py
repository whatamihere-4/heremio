# -*- coding: utf-8 -*-
"""
Thumbnail proxy helpers.

HereSphere fetches thumbnails itself, one request at a time per list row.
Proxying them through the bridge lets us fetch concurrently, warm ahead of
time, and (in later layers) cache and convert formats the headset can't render.
"""

import hashlib
import logging
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger("heremio.thumbs")

_SESSION = requests.Session()
_FETCH_TIMEOUT = 15

# Small in-memory LRU so prefetched thumbs are available without a disk cache.
_MEM_MAX = 512
_mem = OrderedDict()
_mem_lock = threading.Lock()

# Optional on-disk cache (opt-in via ``configure``). Files are named by URL hash;
# mtime doubles as "last accessed" and is bumped on every hit.
_cache_dir = ""

# One lock per URL so concurrent requests for the same thumb fetch it once.
_locks = {}
_locks_lock = threading.Lock()


def _key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def _lock_for(key: str) -> threading.Lock:
    with _locks_lock:
        return _locks.setdefault(key, threading.Lock())


def sniff_mime(data: bytes) -> str:
    """Detect the image type from magic bytes (upstream headers are unreliable)."""
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    return "application/octet-stream"


def _mem_get(key: str):
    with _mem_lock:
        data = _mem.get(key)
        if data is not None:
            _mem.move_to_end(key)
        return data


def _mem_put(key: str, data: bytes) -> None:
    with _mem_lock:
        _mem[key] = data
        _mem.move_to_end(key)
        while len(_mem) > _MEM_MAX:
            _mem.popitem(last=False)


def configure(cache_dir: str, ttl_days: float) -> None:
    """Enable the disk cache and, if ``ttl_days`` > 0, the stale-file sweeper."""
    global _cache_dir
    if not cache_dir:
        return
    os.makedirs(cache_dir, exist_ok=True)
    _cache_dir = cache_dir
    log.info("🖼️  Thumbnail disk cache: %s (purge after %s days)",
             cache_dir, ttl_days if ttl_days > 0 else "never")
    if ttl_days > 0:
        threading.Thread(target=_sweep_loop, args=(ttl_days * 86400,), daemon=True).start()


def _disk_path(key: str) -> str:
    return os.path.join(_cache_dir, key)


def _disk_get(key: str):
    if not _cache_dir:
        return None
    path = _disk_path(key)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError:
        return None
    _disk_touch(key)
    return data


def _disk_touch(key: str) -> None:
    """Record an access (for the TTL sweep) even when served from memory."""
    if not _cache_dir:
        return
    try:
        os.utime(_disk_path(key))
    except OSError:
        pass


def _disk_put(key: str, data: bytes) -> None:
    if not _cache_dir:
        return
    path = _disk_path(key)
    tmp = f"{path}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except OSError as e:
        log.warning("Thumb cache write failed: %s", e)


def purge_stale(max_age_s: float) -> int:
    """Delete cached thumbs not accessed within ``max_age_s``. Returns count removed."""
    cutoff = time.time() - max_age_s
    removed = 0
    for entry in os.scandir(_cache_dir):
        try:
            if entry.is_file() and entry.stat().st_mtime < cutoff:
                os.remove(entry.path)
                removed += 1
        except OSError:
            pass
    return removed


def _sweep_loop(max_age_s: float) -> None:
    interval = min(3600, max(60, max_age_s / 2))
    while True:
        try:
            n = purge_stale(max_age_s)
            if n:
                log.info("🖼️  Purged %d stale thumbnail(s)", n)
        except Exception as e:
            log.warning("Thumbnail sweep failed: %s", e)
        time.sleep(interval)


def _fetch(url: str) -> bytes:
    resp = _SESSION.get(url, timeout=_FETCH_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def get_thumb(url: str):
    """Return ``(bytes, mime)`` for ``url``, fetching upstream on a miss."""
    key = _key(url)
    data = _mem_get(key)
    if data is not None:
        _disk_touch(key)
    else:
        with _lock_for(key):
            data = _mem_get(key)
            if data is None:
                data = _disk_get(key)
                if data is None:
                    data = _fetch(url)
                    _disk_put(key, data)
                _mem_put(key, data)
    return data, sniff_mime(data)


def _prefetch_one(url: str) -> None:
    try:
        get_thumb(url)
    except Exception as e:
        log.debug("Thumb prefetch failed for %s: %s", url, e)


def prefetch(urls, workers: int) -> None:
    """Warm thumbnails for ``urls`` in parallel on a background thread."""
    urls = [u for u in dict.fromkeys(urls) if u]
    if not urls:
        return

    def run():
        log.info("🖼️  Prefetching %d thumbnails (%d workers)...", len(urls), workers)
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            list(pool.map(_prefetch_one, urls))
        log.info("🖼️  Thumbnail prefetch done")

    threading.Thread(target=run, daemon=True).start()

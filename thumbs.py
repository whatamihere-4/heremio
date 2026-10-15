# -*- coding: utf-8 -*-
"""
Thumbnail proxy helpers.

HereSphere fetches thumbnails itself, one request at a time per list row.
Proxying them through the bridge lets us fetch concurrently, warm ahead of
time, and (in later layers) cache and convert formats the headset can't render.
"""

import hashlib
import logging
import threading
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


def _fetch(url: str) -> bytes:
    resp = _SESSION.get(url, timeout=_FETCH_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def get_thumb(url: str):
    """Return ``(bytes, mime)`` for ``url``, fetching upstream on a miss."""
    key = _key(url)
    data = _mem_get(key)
    if data is None:
        with _lock_for(key):
            data = _mem_get(key)
            if data is None:
                data = _fetch(url)
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

# -*- coding: utf-8 -*-
"""
Stream resolution — PTube, Real-Debrid, and HereSphere media formatting.

Uses ``requests.Session`` with automatic retries on 5xx / 429 for both
the Real-Debrid API and PTube addon endpoints.
"""

import logging
import re
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("heremio.streams")

# ---------------------------------------------------------------------------
# Shared HTTP session with retry logic
# ---------------------------------------------------------------------------
_retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(max_retries=_retry_strategy))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry_strategy))

# ---------------------------------------------------------------------------
# Pre-compiled regex patterns
# ---------------------------------------------------------------------------
_RE_RESOLUTION = re.compile(r'(\d{3,4})p', re.IGNORECASE)
_RE_4K_UHD = re.compile(r'4k|uhd', re.IGNORECASE)
_RE_FULL_HD = re.compile(r'full\s*hd', re.IGNORECASE)
_RE_HD = re.compile(r'\bHD\b')
_RE_SIZE = re.compile(r'([\d.]+)\s*(GB|MB)', re.IGNORECASE)


# ---------------------------------------------------------------------------
# Real-Debrid API
# ---------------------------------------------------------------------------
def rd_api(method, endpoint, *, rd_token, data=None):
    """Low-level Real-Debrid REST call (uses shared session)."""
    url = f"https://api.real-debrid.com/rest/1.0{endpoint}"
    headers = {"Authorization": f"Bearer {rd_token}"}

    try:
        resp = SESSION.request(method, url, headers=headers, data=data, timeout=15)
        if resp.status_code == 204:
            return True
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        log.warning("RD API Error on %s: %s – %s", endpoint, e, e.response.text if e.response else "")
        return None
    except Exception as e:
        log.warning("RD API Error on %s: %s", endpoint, e)
        return None


def resolve_infohash_rd(info_hash, *, rd_token):
    """Adds magnet to RD, selects files, waits for links, and unrestricts them."""
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    log.info("  🧲 Starting Real-Debrid resolution for infoHash: %s", info_hash)
    log.debug("Magnet link generated: %s", magnet)

    log.debug("Calling RD API to add magnet...")
    add_resp = rd_api("POST", "/torrents/addMagnet", rd_token=rd_token, data={"magnet": magnet})
    if not add_resp or "id" not in add_resp:
        log.debug("Failed to add magnet to RD. Response missing 'id'.")
        return []

    torrent_id = add_resp["id"]
    log.debug("Successfully added magnet. RD Torrent ID: %s", torrent_id)

    log.debug("Fetching torrent info for ID: %s", torrent_id)
    info = rd_api("GET", f"/torrents/info/{torrent_id}", rd_token=rd_token)
    if not info:
        log.debug("Failed to get torrent info from RD.")
        return []

    files = info.get("files", [])
    if files:
        log.debug("Found %d files in torrent. Filtering > 50MB...", len(files))
        file_ids = ",".join(str(f["id"]) for f in files if f["bytes"] > 50*1024*1024)
        if not file_ids:
            log.debug("No files > 50MB found. Selecting 'all' files.")
            file_ids = "all"
        else:
            log.debug("Selected file IDs: %s", file_ids)
    else:
        log.debug("No files metadata found in info. Selecting 'all'.")
        file_ids = "all"

    log.info("  🧲 Instructing RD to select files: %s", file_ids)
    rd_api("POST", f"/torrents/selectFiles/{torrent_id}", rd_token=rd_token, data={"files": file_ids})

    log.debug("Starting wait loop for links to be generated...")
    for attempt in range(15):
        log.debug("Wait attempt %d/15 for torrent ID %s...", attempt + 1, torrent_id)
        info = rd_api("GET", f"/torrents/info/{torrent_id}", rd_token=rd_token)
        if info and info.get("status") == "downloaded" and info.get("links"):
            log.debug("Torrent is downloaded and links are available!")
            break
        log.info("  🧲 Waiting for RD download (status: %s)...", info.get('status') if info else 'unknown')
        time.sleep(1)

    if not info or not info.get("links"):
        log.info("  🧲 RD failed to generate links in time, or torrent not cached.")
        log.debug("Aborting RD resolution due to missing links.")
        return []

    streams = []
    log.debug("Found %d links. Proceeding to unrestrict...", len(info.get('links', [])))
    for link in info["links"]:
        log.debug("Unrestricting link: %s", link)
        unrestrict = rd_api("POST", "/unrestrict/link", rd_token=rd_token, data={"link": link})
        if unrestrict and unrestrict.get("download"):
            size_mb = unrestrict.get("filesize", 0) / (1024*1024)
            filename = unrestrict.get("filename", "")
            log.debug("Successfully unrestricted! Final direct URL: %s", unrestrict['download'])
            streams.append({
                "url": unrestrict["download"],
                "name": "Debrid",
                "title": f"📂 {size_mb:.0f} MB  🖥️ RD Torrent\n{filename}"
            })
        else:
            log.debug("Failed to unrestrict link.")

    log.info("  ✅ Unrestricted %d links via RD", len(streams))
    return streams


# ---------------------------------------------------------------------------
# PTube streams
# ---------------------------------------------------------------------------
def get_ptube_streams(url):
    """Fetch stream list from a PTube addon endpoint."""
    log.info("  📡 Fetching PTube streams: %s", url)
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("streams", [])
    except Exception as e:
        log.warning("  ⚠️ PTube fetch failed: %s", e)
        return []


def resolve_streams(stremio_id: str, *, ptube_base, ptube_fallback_base, rd_token):
    """
    Ask the PTube addon for streams for a given Stremio content ID.
    Falls back to torrent resolution via Real-Debrid when no direct URLs exist.
    """
    url = f"{ptube_base}/stream/movie/{stremio_id}.json"
    log.debug("Resolving streams for Stremio ID: %s. Primary URL: %s", stremio_id, url)
    streams = get_ptube_streams(url)

    # 1. Prioritize HTTP streams
    http_streams = [s for s in streams if "url" in s or "externalUrl" in s]
    if http_streams:
        log.info("  ✅ Got %d direct HTTP streams from PTube", len(http_streams))
        log.debug("HTTP streams found: %s", http_streams)
        return http_streams

    tried_hashes = set()

    # 2. Check if we already have torrent infoHashes
    torrent_streams = [s for s in streams if "infoHash" in s]
    if torrent_streams:
        log.info("  ⚠️ No direct URLs found, but torrents are present. Resolving via Real-Debrid...")
        log.debug("Found %d torrent streams.", len(torrent_streams))
        for s in torrent_streams:
            h = s["infoHash"].lower()
            if h in tried_hashes:
                continue
            tried_hashes.add(h)
            log.debug("Trying infoHash: %s", h)
            rd_streams = resolve_infohash_rd(h, rd_token=rd_token)
            if rd_streams:
                log.debug("Successfully resolved RD streams from primary torrents.")
                return rd_streams

    # 3. Try fallback manifest
    if ptube_fallback_base != ptube_base:
        fallback_url = f"{ptube_fallback_base}/stream/movie/{stremio_id}.json"
        log.info("  ⚠️ No direct URLs or torrents found, trying fallback with torrents enabled...")
        log.debug("Fetching fallback URL: %s", fallback_url)
        fallback_streams = get_ptube_streams(fallback_url)
        for s in fallback_streams:
            if "infoHash" in s:
                h = s["infoHash"].lower()
                if h in tried_hashes:
                    continue
                tried_hashes.add(h)
                log.debug("Fallback torrent found. Trying infoHash: %s", h)
                rd_streams = resolve_infohash_rd(h, rd_token=rd_token)
                if rd_streams:
                    log.debug("Successfully resolved RD streams from fallback torrents.")
                    return rd_streams

    log.info("  ❌ No usable streams found for %s", stremio_id)
    log.debug("Exhausted all stream resolution methods.")
    return []


def streams_to_media(streams: list) -> list:
    """Convert stream list into HereSphere media format."""
    media = []
    for s in streams:
        stream_url = s.get("url") or s.get("externalUrl")
        if not stream_url:
            continue

        name = s.get("name", "") or ""
        title = s.get("title", "") or ""
        description = s.get("description", "") or ""
        display_name = title or name or "Stream"
        combined = f"{name} {title} {description}"

        res_match = _RE_RESOLUTION.search(combined)
        height = int(res_match.group(1)) if res_match else 0
        if not height and _RE_4K_UHD.search(f"{name} {title}"):
            height = 2160
        if not height and _RE_FULL_HD.search(f"{name} {title}"):
            height = 1080
        if not height and _RE_HD.search(f"{name} {title}"):
            height = 720
        if not height:
            height = 1080

        width = int(height * (16/9))

        size_match = _RE_SIZE.search(combined)
        size_bytes = 0
        if size_match:
            val = float(size_match.group(1))
            unit = size_match.group(2).upper()
            size_bytes = int(val * (1073741824 if unit == "GB" else 1048576))

        media.append({
            "name": display_name[:80],
            "sources": [{
                "resolution": height,
                "height": height,
                "width": width,
                "size": size_bytes,
                "url": stream_url
            }]
        })
    return media

"""
Stream resolution — PTube, Real-Debrid, and HereSphere media formatting.
"""

import re
import json
import ssl
import time
import urllib.request
import urllib.parse
import urllib.error


# ---------------------------------------------------------------------------
# Real-Debrid API
# ---------------------------------------------------------------------------
def rd_api(method, endpoint, *, rd_token, debug_print, data=None):
    """Low-level Real-Debrid REST call."""
    url = f"https://api.real-debrid.com/rest/1.0{endpoint}"
    headers = {"Authorization": f"Bearer {rd_token}"}

    data_encoded = None
    if data:
        data_encoded = urllib.parse.urlencode(data).encode("utf-8")

    req = urllib.request.Request(url, headers=headers, method=method, data=data_encoded)
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            if resp.status == 204:
                return True
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"RD API Error on {endpoint}: {e} - {e.read().decode('utf-8')}")
        return None
    except Exception as e:
        print(f"RD API Error on {endpoint}: {e}")
        return None


def resolve_infohash_rd(info_hash, *, rd_token, debug_print):
    """Adds magnet to RD, selects files, waits for links, and unrestricts them."""
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    print(f"  🧲 Starting Real-Debrid resolution for infoHash: {info_hash}")
    debug_print(f"Magnet link generated: {magnet}")

    debug_print("Calling RD API to add magnet...")
    add_resp = rd_api("POST", "/torrents/addMagnet", rd_token=rd_token,
                      debug_print=debug_print, data={"magnet": magnet})
    if not add_resp or "id" not in add_resp:
        debug_print("Failed to add magnet to RD. Response missing 'id'.")
        return []

    torrent_id = add_resp["id"]
    debug_print(f"Successfully added magnet. RD Torrent ID: {torrent_id}")

    debug_print(f"Fetching torrent info for ID: {torrent_id}")
    info = rd_api("GET", f"/torrents/info/{torrent_id}",
                  rd_token=rd_token, debug_print=debug_print)
    if not info:
        debug_print("Failed to get torrent info from RD.")
        return []

    files = info.get("files", [])
    if files:
        debug_print(f"Found {len(files)} files in torrent. Filtering > 50MB...")
        file_ids = ",".join(str(f["id"]) for f in files if f["bytes"] > 50*1024*1024)
        if not file_ids:
            debug_print("No files > 50MB found. Selecting 'all' files.")
            file_ids = "all"
        else:
            debug_print(f"Selected file IDs: {file_ids}")
    else:
        debug_print("No files metadata found in info. Selecting 'all'.")
        file_ids = "all"

    print(f"  🧲 Instructing RD to select files: {file_ids}")
    rd_api("POST", f"/torrents/selectFiles/{torrent_id}",
           rd_token=rd_token, debug_print=debug_print, data={"files": file_ids})

    debug_print("Starting wait loop for links to be generated...")
    for attempt in range(15):
        debug_print(f"Wait attempt {attempt+1}/15 for torrent ID {torrent_id}...")
        info = rd_api("GET", f"/torrents/info/{torrent_id}",
                      rd_token=rd_token, debug_print=debug_print)
        if info and info.get("status") == "downloaded" and info.get("links"):
            debug_print("Torrent is downloaded and links are available!")
            break
        print(f"  🧲 Waiting for RD download (status: {info.get('status') if info else 'unknown'})...")
        time.sleep(1)

    if not info or not info.get("links"):
        print("  🧲 RD failed to generate links in time, or torrent not cached.")
        debug_print("Aborting RD resolution due to missing links.")
        return []

    streams = []
    debug_print(f"Found {len(info.get('links', []))} links. Proceeding to unrestrict...")
    for link in info["links"]:
        debug_print(f"Unrestricting link: {link}")
        unrestrict = rd_api("POST", "/unrestrict/link",
                            rd_token=rd_token, debug_print=debug_print, data={"link": link})
        if unrestrict and unrestrict.get("download"):
            size_mb = unrestrict.get("filesize", 0) / (1024*1024)
            filename = unrestrict.get("filename", "")
            debug_print(f"Successfully unrestricted! Final direct URL: {unrestrict['download']}")
            streams.append({
                "url": unrestrict["download"],
                "name": "Debrid",
                "title": f"📂 {size_mb:.0f} MB  🖥️ RD Torrent\n{filename}"
            })
        else:
            debug_print("Failed to unrestrict link.")

    print(f"  ✅ Unrestricted {len(streams)} links via RD")
    return streams


# ---------------------------------------------------------------------------
# PTube streams
# ---------------------------------------------------------------------------
def get_ptube_streams(url, debug_print):
    """Fetch stream list from a PTube addon endpoint."""
    print(f"  📡 Fetching PTube streams: {url}")
    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            data = json.loads(resp.read().decode())
        return data.get("streams", [])
    except Exception as e:
        print(f"  ⚠️ PTube fetch failed: {e}")
        return []


def resolve_streams(stremio_id: str, *, ptube_base, ptube_fallback_base, rd_token, debug_print):
    """
    Ask the PTube addon for streams for a given Stremio content ID.
    Falls back to torrent resolution via Real-Debrid when no direct URLs exist.
    """
    url = f"{ptube_base}/stream/movie/{stremio_id}.json"
    debug_print(f"Resolving streams for Stremio ID: {stremio_id}. Primary URL: {url}")
    streams = get_ptube_streams(url, debug_print)

    # 1. Prioritize HTTP streams
    http_streams = [s for s in streams if "url" in s or "externalUrl" in s]
    if http_streams:
        print(f"  ✅ Got {len(http_streams)} direct HTTP streams from PTube")
        debug_print(f"HTTP streams found: {http_streams}")
        return http_streams

    tried_hashes = set()

    # 2. Check if we already have torrent infoHashes
    torrent_streams = [s for s in streams if "infoHash" in s]
    if torrent_streams:
        print("  ⚠️ No direct URLs found, but torrents are present. Resolving via Real-Debrid...")
        debug_print(f"Found {len(torrent_streams)} torrent streams.")
        for s in torrent_streams:
            h = s["infoHash"].lower()
            if h in tried_hashes:
                continue
            tried_hashes.add(h)
            debug_print(f"Trying infoHash: {h}")
            rd_streams = resolve_infohash_rd(h, rd_token=rd_token, debug_print=debug_print)
            if rd_streams:
                debug_print("Successfully resolved RD streams from primary torrents.")
                return rd_streams

    # 3. Try fallback manifest
    if ptube_fallback_base != ptube_base:
        fallback_url = f"{ptube_fallback_base}/stream/movie/{stremio_id}.json"
        print("  ⚠️ No direct URLs or torrents found, trying fallback with torrents enabled...")
        debug_print(f"Fetching fallback URL: {fallback_url}")
        fallback_streams = get_ptube_streams(fallback_url, debug_print)
        for s in fallback_streams:
            if "infoHash" in s:
                h = s["infoHash"].lower()
                if h in tried_hashes:
                    continue
                tried_hashes.add(h)
                debug_print(f"Fallback torrent found. Trying infoHash: {h}")
                rd_streams = resolve_infohash_rd(h, rd_token=rd_token, debug_print=debug_print)
                if rd_streams:
                    debug_print("Successfully resolved RD streams from fallback torrents.")
                    return rd_streams

    print(f"  ❌ No usable streams found for {stremio_id}")
    debug_print("Exhausted all stream resolution methods.")
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

        res_match = re.search(r'(\d{3,4})p', f"{name} {title} {description}", re.IGNORECASE)
        height = int(res_match.group(1)) if res_match else 0
        if not height and re.search(r'4k|uhd', f"{name} {title}", re.IGNORECASE):
            height = 2160
        if not height and re.search(r'full\s*hd', f"{name} {title}", re.IGNORECASE):
            height = 1080
        if not height and re.search(r'\bHD\b', f"{name} {title}"):
            height = 720
        if not height:
            height = 1080

        width = int(height * (16/9))

        size_match = re.search(r'([\d.]+)\s*(GB|MB)', f"{name} {title} {description}", re.IGNORECASE)
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

# -*- coding: utf-8 -*-
"""
Stremio → HereSphere Bridge Server
Serves your Stremio library over LAN to the HereSphere VR player app.
"""

import json
import logging
import re
import threading
import time

import requests
from datetime import datetime
from flask import Flask, request, jsonify, make_response, render_template

from config import settings
from parse_filename import parse_filename
from stashdb import (
    STASH_CACHE, _cache_lock, load_cache, save_cache,
    query_stashdb, query_stashdb_by_id, fill_stash_cache_background,
    _RE_STASHDB_URL,
)
from streams import resolve_streams, streams_to_media

log = logging.getLogger("heremio.server")

# ---------------------------------------------------------------------------
# Stremio authentication
# ---------------------------------------------------------------------------

def get_auth_key():
    """Log in to Stremio and return an auth key."""
    log.info("Logging in to Stremio as %s...", settings.STREMIO_USER)
    url = "https://api.strem.io/api/login"
    payload = {"email": settings.STREMIO_USER, "password": settings.STREMIO_PASS}

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

        if "authKey" in data:
            auth_key = data["authKey"]
        elif "result" in data and "authKey" in data["result"]:
            auth_key = data["result"]["authKey"]
        elif "result" in data and "user" in data["result"] and "authKey" in data["result"]["user"]:
            auth_key = data["result"]["user"]["authKey"]
        else:
            log.warning("Login successful, but authKey was not found in the response.")
            log.debug("Response preview: %s", str(data)[:500])
            return None

        log.info("=== SUCCESS ===")
        log.debug("Your STREMIO_AUTH key is: %s", auth_key)
        return auth_key

    except requests.exceptions.HTTPError as e:
        log.error("HTTP Error: %s", e)
        if e.response is not None:
            try:
                error_data = e.response.json()
                if "error" in error_data:
                    log.error("API Error details: %s", error_data['error'])
            except ValueError:
                log.error("Response: %s", e.response.text)
        return None
    except Exception as e:
        log.error("An error occurred: %s", e)
        return None


STREMIO_AUTH = get_auth_key()
STREMIO_API  = "https://api.strem.io/api"

# Load StashDB cache from disk
load_cache(debug_mode=settings.DEBUG_MODE)


# ---------------------------------------------------------------------------
# Stremio library
# ---------------------------------------------------------------------------
library_items = []
item_by_idx   = {}


def fetch_library():
    """Pull the full Stremio library and pre-cache parsed filenames."""
    global library_items, item_by_idx

    log.info("📡 Fetching library item IDs from Stremio...")
    meta = requests.post(f"{STREMIO_API}/datastoreMeta", json={
        "authKey": STREMIO_AUTH,
        "collection": "libraryItem"
    }, headers={"Content-Type": "application/json"})
    meta.raise_for_status()

    ids = [e[0] for e in meta.json().get("result", []) if isinstance(e, list) and len(e) >= 1]
    log.info("  ↳ Got %d IDs", len(ids))

    log.info("📡 Fetching full item details...")
    data = requests.post(f"{STREMIO_API}/datastoreGet", json={
        "authKey": STREMIO_AUTH,
        "collection": "libraryItem",
        "ids": ids
    }, headers={"Content-Type": "application/json"})
    data.raise_for_status()

    items = data.json().get("result", [])
    movies = [it for it in items if isinstance(it, dict)
              and it.get("type") == "movie" and not it.get("removed")]

    # Cache parsed filenames on every item (eliminates repeated parsing)
    for item in movies:
        item["_parsed"] = parse_filename(item.get("name", ""))

    library_items = movies
    item_by_idx = {i: m for i, m in enumerate(movies)}
    log.info("✅ Library loaded: %d movies", len(movies))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Pre-compiled VR detection patterns
_RE_VR_KEYWORDS = re.compile(r'vr|180|360|oculus|vive|quest', re.IGNORECASE)
_RE_360 = re.compile(r'360')

def parse_date(iso_str: str) -> str:
    """Convert ISO timestamp to YYYY-MM-DD."""
    if not iso_str:
        return "2000-01-01"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "2000-01-01"


# ---------------------------------------------------------------------------
# In-memory stream cache  (~4 h TTL)
# ---------------------------------------------------------------------------
_STREAM_CACHE = {}          # stremio_id → (streams_list, timestamp)
_STREAM_TTL   = 4 * 3600   # seconds


def _resolve(stremio_id):
    """Resolve streams with in-memory caching."""
    now = time.time()
    cached = _STREAM_CACHE.get(stremio_id)
    if cached:
        streams, ts = cached
        if now - ts < _STREAM_TTL:
            log.debug("Stream cache hit for %s", stremio_id)
            return streams

    streams = resolve_streams(
        stremio_id,
        ptube_base=settings.PTUBE_BASE,
        ptube_fallback_base=settings.PTUBE_FALLBACK_BASE,
        rd_token=settings.RD_TOKEN,
    )
    _STREAM_CACHE[stremio_id] = (streams, now)
    return streams


def _query_stashdb(site, title):
    """Wrapper that passes config into stashdb.query_stashdb."""
    return query_stashdb(site, title, api_key=settings.STASHDB_API_KEY)


def _query_stashdb_by_id(scene_id):
    """Wrapper that passes config into stashdb.query_stashdb_by_id."""
    return query_stashdb_by_id(scene_id, api_key=settings.STASHDB_API_KEY)


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)


def hs_response(data: dict, status=200):
    """Return a JSON response with the HereSphere header."""
    resp = make_response(jsonify(data), status)
    resp.headers["HereSphere-JSON-Version"] = "1"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/heresphere", methods=["GET", "POST"])
def heresphere_index():
    """Library index — lists all videos."""
    base = request.host_url.rstrip("/")
    video_list = [f"{base}/heresphere/{i}" for i in range(len(library_items))]

    return hs_response({
        "access": 1,
        "library": [
            {
                "name": "Stremio Library",
                "list": video_list
            }
        ]
    })


@app.route("/heresphere/<int:idx>", methods=["GET", "POST"])
def heresphere_video(idx: int):
    """Video data endpoint — returns metadata and optionally media sources."""
    item = item_by_idx.get(idx)
    if not item:
        return hs_response({"error": "not found"}, 404)

    raw_name = item.get("name", "Unknown")
    poster = item.get("poster", "")
    date_added = parse_date(item.get("_ctime", ""))
    stremio_id = item.get("_id", "")

    parsed = item.get("_parsed") or parse_filename(raw_name)
    title = parsed["title"]
    site = parsed["site"]

    # Read tags from StashDB cache (background thread fills this)
    cache_key = f"{site} - {title}"
    stash_data = STASH_CACHE.get(cache_key)

    hs_tags = []
    if stash_data:
        images = stash_data.get("images", [])
        if images:
            poster = images[0].get("url", poster)
        
        for performer in stash_data.get("performers", []):
            p_name = performer.get("performer", {}).get("name")
            if p_name:
                hs_tags.append({"name": p_name, "start": 0, "end": 0})
        for tag in stash_data.get("tags", []):
            t_name = tag.get("name")
            if t_name:
                hs_tags.append({"name": t_name, "start": 0, "end": 0})
    else:
        if settings.STASHDB_API_KEY:
            log.debug("Using local backup tags for: %s (StashDB cache miss/pending)", title)
        else:
            log.debug("Using local backup tags for: %s", title)
        hs_tags = [{"name": t, "start": 0, "end": 0} for t in parsed["tags"]]

    duration_ms = 0
    state = item.get("state", {})
    if state.get("duration"):
        duration_ms = state["duration"]

    needs_media = False
    if request.method == "POST":
        try:
            body = request.get_json(force=True, silent=True) or {}
            needs_media = body.get("needsMediaSource", True)
        except Exception:
            needs_media = True

    resp = {
        "access": 1,
        "title": title,
        "description": "",
        "thumbnailImage": poster,
        "dateReleased": date_added,
        "dateAdded": date_added,
        "duration": duration_ms,
        "isFavorite": False,
        "projection": "perspective",
        "stereo": "mono",
        "fov": 180,
        "lens": "Linear",
        "tags": hs_tags,
        "media": [],
        "writeFavorite": False,
        "writeRating": False,
        "writeTags": False,
        "writeHSP": False
    }

    name_lower = raw_name.lower()
    if _RE_VR_KEYWORDS.search(name_lower):
        resp["projection"] = "equirectangular"
        resp["stereo"] = "sbs"
    if _RE_360.search(name_lower):
        resp["projection"] = "equirectangular360"

    if needs_media:
        raw_streams = _resolve(stremio_id)
        resp["media"] = streams_to_media(raw_streams)

    return hs_response(resp)


@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    """Re-fetch the Stremio library."""
    fetch_library()
    return jsonify({"status": "ok", "count": len(library_items)})


# ---------------------------------------------------------------------------
# /api/match endpoint (used by both /match and /library UIs)
# ---------------------------------------------------------------------------
@app.route("/api/match", methods=["POST"])
def api_match():
    req = request.json
    if not req:
        return jsonify({"success": False, "error": "Invalid request"})

    cache_key = req.get("cache_key")
    stashdb_url = req.get("stashdb_url", "")

    match = _RE_STASHDB_URL.search(stashdb_url)
    if not match:
        return jsonify({"success": False, "error": "Invalid StashDB Scene URL"})

    scene_id = match.group(1)
    scene_data = _query_stashdb_by_id(scene_id)
    if not scene_data:
        return jsonify({"success": False, "error": "Could not fetch scene from StashDB"})

    with _cache_lock:
        STASH_CACHE[cache_key] = scene_data
    save_cache()

    return jsonify({"success": True, "title": scene_data.get("title")})


# ---------------------------------------------------------------------------
# /library — full library browser with integrated matching
# ---------------------------------------------------------------------------
@app.route("/library", methods=["GET"])
def library_ui():
    """Web UI to browse video library with StashDB data."""
    all_items = []

    for i, item in enumerate(library_items):
        raw_name = item.get("name", "")
        if not raw_name:
            continue
        parsed = item.get("_parsed") or parse_filename(raw_name)
        site = parsed.get("site", "")
        title = parsed.get("title", "")
        cache_key = f"{site} - {title}"
        stash = STASH_CACHE.get(cache_key)

        performers = []
        tags = []
        stash_title = ""
        stash_studio = ""
        stash_date = ""
        status = "pending"

        if cache_key in STASH_CACHE:
            if stash is None:
                status = "unmatched"
            else:
                status = "matched"
                stash_title = stash.get("title", "")
                stash_studio = (stash.get("studio") or {}).get("name", "")
                stash_date = stash.get("date", "")
                performers = [p.get("performer", {}).get("name", "") for p in stash.get("performers", [])]
                tags = [t.get("name", "") for t in stash.get("tags", [])]
                images = stash.get("images", [])
                if images:
                    stash_poster = images[0].get("url", "")

        all_items.append({
            "idx": i,
            "raw_name": raw_name,
            "parsed_title": title,
            "parsed_site": site,
            "poster": item.get("poster", ""),
            "cache_key": cache_key,
            "status": status,
            "stash_title": stash_title,
            "stash_studio": stash_studio,
            "stash_date": stash_date,
            "performers": performers,
            "tags": tags,
        })

    return render_template("library.html", items=all_items, items_json=json.dumps(all_items))


@app.route("/", methods=["GET"])
def root():
    """Simple status page."""
    return jsonify({
        "status": "running",
        "library_size": len(library_items),
        "heresphere_url": f"{request.host_url}heresphere"
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    fetch_library()

    # Start background StashDB caching thread
    threading.Thread(
        target=fill_stash_cache_background,
        args=(library_items, parse_filename),
        kwargs={"api_key": settings.STASHDB_API_KEY, "debug_mode": settings.DEBUG_MODE},
        daemon=True,
    ).start()

    log.info("🚀 HereSphere bridge running on http://0.0.0.0:%d/heresphere", settings.PORT)
    log.info("   Point HereSphere to:  http://<YOUR_LAN_IP>:%d/heresphere", settings.PORT)
    app.run(host="0.0.0.0", port=settings.PORT, debug=False, threaded=True)

# -*- coding: utf-8 -*-
"""
Stremio → HereSphere Bridge Server
Serves your Stremio library over LAN to the HereSphere VR player app.
"""

import os
import re
import json
import base64
import threading
import requests
from datetime import datetime
from flask import Flask, request, jsonify, make_response, render_template_string
from dotenv import load_dotenv

from parse_filename import parse_filename
from stashdb import (
    STASH_CACHE, load_cache, save_cache,
    query_stashdb, query_stashdb_by_id, fill_stash_cache_background,
)
from streams import resolve_streams, streams_to_media

load_dotenv()

# ---------------------------------------------------------------------------
# Config & Authentication
# ---------------------------------------------------------------------------
STASHDB_API_KEY = os.environ.get("STASHDB_API_KEY", "")
DEBUG_MODE      = os.environ.get("DEBUG_MODE", "True").lower() == "true"


def debug_print(msg):
    if DEBUG_MODE:
        print(f"🛠️ [DEBUG] {msg}", flush=True)


def get_auth_key():
    email = os.environ.get("STREMIO_USER")
    password = os.environ.get("STREMIO_PASS")

    if not email or not password:
        print("Error: Missing STREMIO_USER or STREMIO_PASS in .env file.")
        print("Please add them to your .env file and try again.")
        return None

    print("Logging in to Stremio as {}...".format(email))

    url = "https://api.strem.io/api/login"
    payload = {"email": email, "password": password}

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
            if DEBUG_MODE:
                print("Login successful, but authKey was not found in the response.")
                print("Response preview: {}".format(str(data)[:500]))
            return None

        print("=== SUCCESS ===\n")
        if DEBUG_MODE:
            print("Your STREMIO_AUTH key is:")
            print(auth_key)
            print("===============\n")

        return auth_key

    except requests.exceptions.HTTPError as e:
        print("HTTP Error: {}".format(e))
        if e.response is not None:
            try:
                error_data = e.response.json()
                if "error" in error_data:
                    print("API Error details: {}".format(error_data['error']))
            except ValueError:
                print("Response: {}".format(e.response.text))
        return None
    except Exception as e:
        print("An error occurred: {}".format(e))
        return None


STREMIO_AUTH   = get_auth_key() or os.environ.get("STREMIO_AUTH", "")
PTUBE_MANIFEST = os.environ.get("PTUBE_MANIFEST", "")
RD_TOKEN       = os.environ.get("RD_TOKEN", "")

# Derive the PTube addon base URLs from the manifest URL
PTUBE_BASE = PTUBE_MANIFEST.rsplit("/manifest.json", 1)[0]

# Automatically generate a fallback PTube URL with hideTorrents = false
try:
    ptube_url_parts = PTUBE_BASE.split("/")
    ptube_config_b64 = ptube_url_parts[-1]
    ptube_host = "/".join(ptube_url_parts[:-1])

    pad = len(ptube_config_b64) % 4
    b64_str = ptube_config_b64 + "=" * ((4 - pad) % 4)
    if "-" in b64_str or "_" in b64_str:
        config_json = base64.urlsafe_b64decode(b64_str).decode("utf-8")
    else:
        config_json = base64.b64decode(b64_str).decode("utf-8")

    config = json.loads(config_json)
    config["hideTorrents"] = False

    new_json_bytes = json.dumps(config).encode("utf-8")
    new_b64 = base64.urlsafe_b64encode(new_json_bytes).decode("utf-8").rstrip("=")
    PTUBE_FALLBACK_BASE = f"{ptube_host}/{new_b64}"
except Exception as e:
    print(f"Failed to generate fallback PTube base: {e}")
    PTUBE_FALLBACK_BASE = PTUBE_BASE

STREMIO_API = "https://api.strem.io/api"
PORT = 9000

# Load StashDB cache from disk
load_cache(debug_mode=DEBUG_MODE)


# ---------------------------------------------------------------------------
# Stremio library
# ---------------------------------------------------------------------------
library_items = []
item_by_idx = {}


def fetch_library():
    """Pull the full Stremio library."""
    global library_items, item_by_idx

    print("📡 Fetching library item IDs from Stremio...")
    meta = requests.post(f"{STREMIO_API}/datastoreMeta", json={
        "authKey": STREMIO_AUTH,
        "collection": "libraryItem"
    }, headers={"Content-Type": "application/json"})
    meta.raise_for_status()

    ids = [e[0] for e in meta.json().get("result", []) if isinstance(e, list) and len(e) >= 1]
    print(f"  ↳ Got {len(ids)} IDs")

    print("📡 Fetching full item details...")
    data = requests.post(f"{STREMIO_API}/datastoreGet", json={
        "authKey": STREMIO_AUTH,
        "collection": "libraryItem",
        "ids": ids
    }, headers={"Content-Type": "application/json"})
    data.raise_for_status()

    items = data.json().get("result", [])
    movies = [it for it in items if isinstance(it, dict)
              and it.get("type") == "movie" and not it.get("removed")]

    library_items = movies
    item_by_idx = {i: m for i, m in enumerate(movies)}
    print(f"✅ Library loaded: {len(movies)} movies")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_date(iso_str: str) -> str:
    """Convert ISO timestamp to YYYY-MM-DD."""
    if not iso_str:
        return "2000-01-01"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "2000-01-01"


def _resolve(stremio_id):
    """Wrapper that passes config into streams.resolve_streams."""
    return resolve_streams(
        stremio_id,
        ptube_base=PTUBE_BASE,
        ptube_fallback_base=PTUBE_FALLBACK_BASE,
        rd_token=RD_TOKEN,
        debug_print=debug_print,
    )


def _query_stashdb(site, title):
    """Wrapper that passes config into stashdb.query_stashdb."""
    return query_stashdb(site, title, api_key=STASHDB_API_KEY, debug_print=debug_print)


def _query_stashdb_by_id(scene_id):
    """Wrapper that passes config into stashdb.query_stashdb_by_id."""
    return query_stashdb_by_id(scene_id, api_key=STASHDB_API_KEY, debug_print=debug_print)


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

    parsed = parse_filename(raw_name)
    title = parsed["title"]
    site = parsed["site"]

    # Read tags from StashDB cache (background thread fills this)
    cache_key = f"{site} - {title}"
    stash_data = STASH_CACHE.get(cache_key)

    hs_tags = []
    if stash_data:
        for performer in stash_data.get("performers", []):
            p_name = performer.get("performer", {}).get("name")
            if p_name:
                hs_tags.append({"name": p_name, "start": 0, "end": 0})
        for tag in stash_data.get("tags", []):
            t_name = tag.get("name")
            if t_name:
                hs_tags.append({"name": t_name, "start": 0, "end": 0})
    else:
        if STASHDB_API_KEY:
            debug_print(f"Using local backup tags for: {title} (StashDB cache miss/pending)")
        else:
            debug_print(f"Using local backup tags for: {title}")
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
    if any(kw in name_lower for kw in ["vr", "180", "360", "oculus", "vive", "quest"]):
        resp["projection"] = "equirectangular"
        resp["stereo"] = "sbs"
    if "360" in name_lower:
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

    match = re.search(r'stashdb\.org/scenes/([a-f0-9\-]+)', stashdb_url)
    if not match:
        return jsonify({"success": False, "error": "Invalid StashDB Scene URL"})

    scene_id = match.group(1)
    scene_data = _query_stashdb_by_id(scene_id)
    if not scene_data:
        return jsonify({"success": False, "error": "Could not fetch scene from StashDB"})

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
        parsed = parse_filename(raw_name)
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

    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Library Browser</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f0f0f; color: #e0e0e0; }
            .header { padding: 20px 24px; background: #1a1a2e; border-bottom: 1px solid #222; display: flex; align-items: center; justify-content: space-between; }
            .header h1 { font-size: 1.4em; color: #4facfe; }
            .header .stats { font-size: 0.9em; color: #888; }
            .filter-bar { padding: 12px 24px; background: #141420; display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
            .filter-bar input[type=text] { flex: 1; min-width: 200px; padding: 8px 12px; background: #222; color: #fff; border: 1px solid #333; border-radius: 6px; font-size: 0.9em; }
            .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 16px; padding: 20px 24px; }
            .card { background: #1a1a2e; border-radius: 10px; overflow: hidden; cursor: pointer; transition: transform .15s, box-shadow .15s; position: relative; }
            .card:hover { transform: translateY(-3px); box-shadow: 0 8px 24px rgba(79,172,254,.15); }
            .card img { width: 100%; aspect-ratio: 2/3; object-fit: cover; display: block; background: #222; }
            .card .info { padding: 10px 12px; }
            .card .info .title { font-size: 0.85em; font-weight: 600; color: #fff; margin-bottom: 4px; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
            .card .info .site { font-size: 0.75em; color: #4facfe; }
            .card .badge { position: absolute; top: 8px; right: 8px; font-size: 0.65em; padding: 3px 8px; border-radius: 20px; font-weight: 600; text-transform: uppercase; }
            .badge.matched { background: #0a3d0a; color: #4caf50; }
            .badge.unmatched { background: #3d0a0a; color: #f44336; }
            .badge.pending { background: #3d3d0a; color: #ff9800; }
            .overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,.8); z-index: 100; justify-content: center; align-items: flex-start; padding: 40px 20px; overflow-y: auto; }
            .overlay.active { display: flex; }
            .modal { background: #1a1a2e; border-radius: 12px; max-width: 700px; width: 100%; overflow: hidden; }
            .modal-header { display: flex; gap: 16px; padding: 20px; border-bottom: 1px solid #2a2a3e; }
            .modal-header img { width: 140px; border-radius: 6px; object-fit: cover; }
            .modal-header .meta { flex: 1; }
            .modal-header .meta h2 { font-size: 1.1em; color: #4facfe; margin-bottom: 6px; }
            .modal-header .meta .sub { font-size: 0.85em; color: #888; margin-bottom: 4px; }
            .modal-body { padding: 16px 20px; }
            .modal-body h3 { font-size: 0.9em; color: #999; margin: 12px 0 6px; text-transform: uppercase; letter-spacing: 0.05em; }
            .modal-body h3:first-child { margin-top: 0; }
            .tag-list { display: flex; flex-wrap: wrap; gap: 6px; }
            .tag { background: #2a2a3e; padding: 4px 10px; border-radius: 20px; font-size: 0.8em; color: #ccc; }
            .tag.performer { background: #1a3a5c; color: #4facfe; }
            .raw-name { font-size: 0.75em; color: #555; word-break: break-all; margin-top: 12px; padding-top: 12px; border-top: 1px solid #2a2a3e; }
            .match-row { display: flex; gap: 8px; margin-top: 16px; padding-top: 12px; border-top: 1px solid #2a2a3e; }
            .match-row input { flex: 1; padding: 8px 10px; background: #222; color: #fff; border: 1px solid #333; border-radius: 6px; font-size: 0.85em; }
            .match-row button { padding: 8px 16px; background: #4facfe; color: #000; border: none; border-radius: 6px; cursor: pointer; font-weight: 600; font-size: 0.85em; white-space: nowrap; }
            .match-row button:hover { background: #00f2fe; }
            .match-msg { font-size: 0.85em; margin-top: 8px; }
            .close-btn { position: absolute; top: 12px; right: 16px; background: none; border: none; color: #888; font-size: 1.6em; cursor: pointer; z-index: 10; }
            .close-btn:hover { color: #fff; }
            .hidden { display: none !important; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📚 Library Browser</h1>
            <div class="stats" id="stats">{{ items|length }} videos</div>
        </div>
        <div class="filter-bar">
            <input type="text" id="search" placeholder="Search titles, performers, sites..." oninput="filterCards()">
            <label style="display:flex;align-items:center;gap:6px;color:#ccc;font-size:0.9em;cursor:pointer;white-space:nowrap;">
                <input type="checkbox" id="unmatchedOnly" onchange="filterCards()" style="accent-color:#4facfe;"> Unmatched only
            </label>
        </div>
        <div class="grid" id="grid">
            {% for v in items %}
            <div class="card" data-idx="{{ v.idx }}" data-status="{{ v.status }}" data-search="{{ v.parsed_site }} {{ v.parsed_title }} {{ v.performers|join(' ') }}" onclick="openModal({{ v.idx }})">
                <img src="{{ v.poster }}" loading="lazy" onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 width=%22200%22 height=%22300%22><rect fill=%22%23222%22 width=%22200%22 height=%22300%22/><text x=%2250%25%22 y=%2250%25%22 fill=%22%23555%22 font-size=%2214%22 text-anchor=%22middle%22 dy=%22.3em%22>No Image</text></svg>'">
                <span class="badge {{ v.status }}">{{ v.status }}</span>
                <div class="info">
                    <div class="title">{{ v.parsed_title }}</div>
                    <div class="site">{{ v.parsed_site }}</div>
                </div>
            </div>
            {% endfor %}
        </div>

        <div class="overlay" id="overlay" onclick="if(event.target===this)closeModal()">
            <div class="modal" style="position:relative" id="modal"></div>
        </div>

        <script>
            const items = {{ items_json|safe }};

            function filterCards() {
                const q = document.getElementById('search').value.toLowerCase();
                const unmatchedOnly = document.getElementById('unmatchedOnly').checked;
                let visibleCount = 0;
                document.querySelectorAll('.card').forEach(c => {
                    const matchQ = !q || c.dataset.search.toLowerCase().includes(q);
                    const matchS = !unmatchedOnly || c.dataset.status !== 'matched';
                    const isVisible = matchQ && matchS;
                    c.classList.toggle('hidden', !isVisible);
                    if (isVisible) visibleCount++;
                });
                document.getElementById('stats').textContent = visibleCount + ' videos';
            }

            function openModal(idx) {
                const v = items.find(i => i.idx === idx);
                if (!v) return;
                const perfHtml = v.performers.length ? v.performers.map(p => `<span class="tag performer">${p}</span>`).join('') : '<span style="color:#555">None</span>';
                const tagsHtml = v.tags.length ? v.tags.map(t => `<span class="tag">${t}</span>`).join('') : '<span style="color:#555">None</span>';
                const stashInfo = v.status === 'matched'
                    ? `<div class="sub"><strong>StashDB:</strong> ${v.stash_title}</div>
                       <div class="sub"><strong>Studio:</strong> ${v.stash_studio || '—'}</div>
                       <div class="sub"><strong>Date:</strong> ${v.stash_date || '—'}</div>`
                    : `<div class="sub" style="color:${v.status==='unmatched'?'#f44336':'#ff9800'}">
                         ${v.status==='unmatched' ? '❌ No StashDB match' : '⏳ Pending lookup'}
                       </div>`;

                document.getElementById('modal').innerHTML = `
                    <button class="close-btn" onclick="closeModal()">&times;</button>
                    <div class="modal-header">
                        <img src="${v.poster}" onerror="this.style.display='none'">
                        <div class="meta">
                            <h2>${v.parsed_title}</h2>
                            <div class="sub" style="color:#4facfe">${v.parsed_site}</div>
                            ${stashInfo}
                        </div>
                    </div>
                    <div class="modal-body">
                        <h3>Performers</h3>
                        <div class="tag-list">${perfHtml}</div>
                        <h3>Tags</h3>
                        <div class="tag-list">${tagsHtml}</div>
                        <div class="match-row">
                            <input type="text" id="matchUrl-${idx}" placeholder="Paste StashDB scene URL to re-match...">
                            <button onclick="doMatch(${idx})">Match</button>
                        </div>
                        <div class="match-msg" id="matchMsg-${idx}"></div>
                        <div class="raw-name">${v.raw_name}</div>
                    </div>`;
                document.getElementById('overlay').classList.add('active');
            }

            function closeModal() {
                document.getElementById('overlay').classList.remove('active');
            }
            document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

            async function doMatch(idx) {
                const v = items.find(i => i.idx === idx);
                const url = document.getElementById('matchUrl-' + idx).value;
                const msg = document.getElementById('matchMsg-' + idx);
                if (!url) { msg.innerHTML = '❌ Paste a URL first'; msg.style.color = '#f44336'; return; }
                msg.innerHTML = 'Saving...'; msg.style.color = '#aaa';
                try {
                    const res = await fetch('/api/match', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ cache_key: v.cache_key, stashdb_url: url })
                    });
                    const data = await res.json();
                    if (data.success) {
                        msg.innerHTML = '✅ Matched: ' + data.title;
                        msg.style.color = '#4caf50';
                        const card = document.querySelector(`.card[data-idx="${idx}"]`);
                        if (card) {
                            card.querySelector('.badge').className = 'badge matched';
                            card.querySelector('.badge').textContent = 'matched';
                            card.dataset.status = 'matched';
                        }
                    } else {
                        msg.innerHTML = '❌ ' + data.error;
                        msg.style.color = '#f44336';
                    }
                } catch(e) {
                    msg.innerHTML = '❌ Network error';
                    msg.style.color = '#f44336';
                }
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html, items=all_items, items_json=json.dumps(all_items))


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
        kwargs={"api_key": STASHDB_API_KEY, "debug_mode": DEBUG_MODE, "debug_print": debug_print},
        daemon=True,
    ).start()

    print(f"\n🚀 HereSphere bridge running on http://0.0.0.0:{PORT}/heresphere")
    print(f"   Point HereSphere to:  http://<YOUR_LAN_IP>:{PORT}/heresphere\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

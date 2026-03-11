"""
Stremio → HereSphere Bridge Server
Serves your Stremio library over LAN to the HereSphere VR player app.
"""

import os
import re
import json
import base64
import time
import requests
import urllib.request
import urllib.parse
import urllib.error
import ssl
from flask import Flask, request, jsonify, make_response
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config & Authentication
# ---------------------------------------------------------------------------
STASHDB_API_KEY = os.environ.get("STASHDB_API_KEY", "")
DEBUG_MODE      = os.environ.get("DEBUG_MODE", "True").lower() == "true"

def get_auth_key():
    email = os.environ.get("STREMIO_USER")
    password = os.environ.get("STREMIO_PASS")

    if not email or not password:
        print("Error: Missing STREMIO_USER or STREMIO_PASS in .env file.")
        print("Please add them to your .env file and try again.")
        return None

    #if DEBUG_MODE:
    print("Logging in to Stremio as {}...".format(email))

    url = "https://api.strem.io/api/login"
    payload = {
        "email": email,
        "password": password
    }

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
            print("You can copy this key and update STREMIO_AUTH in your .env file.")
        
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

def debug_print(msg):
    if DEBUG_MODE:
        print(f"🛠️ [DEBUG] {msg}")

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

# ---------------------------------------------------------------------------
# In-memory library & Cache stores
# ---------------------------------------------------------------------------
library_items = []   # list of dicts from Stremio
item_by_idx = {}     # idx → item dict

STASH_CACHE = {}     # Cache for StashDB queries {"Site - Title": stashdb_dict}

def fetch_library():
    """Pull the full Stremio library (same logic as getlibrary.py)."""
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
    # Keep only non-removed movies
    movies = [it for it in items if isinstance(it, dict)
              and it.get("type") == "movie" and not it.get("removed")]

    library_items = movies
    item_by_idx = {i: m for i, m in enumerate(movies)}
    print(f"✅ Library loaded: {len(movies)} movies")


# ---------------------------------------------------------------------------
# StashDB Integration
# ---------------------------------------------------------------------------
def query_stashdb(site: str, clean_title: str):
    """
    Search StashDB using a basic text query and return structured HereSphere metadata if found.
    Uses in-memory STASH_CACHE to prevent repeated API calls.
    Returns None if no match or if API request fails.
    """
    if not STASHDB_API_KEY:
        return None
        
    cache_key = f"{site} - {clean_title}"
    if cache_key in STASH_CACHE:
        debug_print(f"Loaded {cache_key} from STASH_CACHE")
        return STASH_CACHE[cache_key]

    debug_print(f"Querying StashDB for: '{cache_key}'")
    url = "https://stashdb.org/graphql"
    headers = {
        "Content-Type": "application/json",
        "ApiKey": STASHDB_API_KEY
    }

    query = """
    query SearchScenes($input: SceneQueryInput!) {
      queryScenes(input: $input) {
        scenes {
          title
          date
          studio { name }
          performers { performer { name } }
          tags { name }
        }
      }
    }
    """
    
    # Text query approach using Site + Clean title
    payload = {
        "query": query,
        "variables": {
            "input": {
                "text": clean_title
            }
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            scenes = data.get("data", {}).get("queryScenes", {}).get("scenes", [])
            if scenes:
                first_match = scenes[0]
                STASH_CACHE[cache_key] = first_match
                debug_print(f"✅ StashDB match found: {first_match.get('title')}")
                return first_match
            else:
                debug_print("❌ No StashDB results found.")
                STASH_CACHE[cache_key] = None
        else:
            debug_print(f"⚠️ StashDB HTTP Error {response.status_code}: {response.text}")
    except Exception as e:
        debug_print(f"⚠️ StashDB Query Exception: {e}")
        
    return None

# ---------------------------------------------------------------------------
# Stream Resolution & Real-Debrid API
# ---------------------------------------------------------------------------
def rd_api(method, endpoint, data=None):
    url = f"https://api.real-debrid.com/rest/1.0{endpoint}"
    headers = {"Authorization": f"Bearer {RD_TOKEN}"}
    
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

def resolve_infohash_rd(info_hash):
    """Adds magnet to RD, selects files, waits for links, and unrestricts them."""
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    print(f"  🧲 Starting Real-Debrid resolution for infoHash: {info_hash}")
    debug_print(f"Magnet link generated: {magnet}")
    
    debug_print("Calling RD API to add magnet...")
    add_resp = rd_api("POST", "/torrents/addMagnet", {"magnet": magnet})
    if not add_resp or "id" not in add_resp:
        debug_print("Failed to add magnet to RD. Response missing 'id'.")
        return []
        
    torrent_id = add_resp["id"]
    debug_print(f"Successfully added magnet. RD Torrent ID: {torrent_id}")
    
    debug_print(f"Fetching torrent info for ID: {torrent_id}")
    info = rd_api("GET", f"/torrents/info/{torrent_id}")
    if not info:
        debug_print("Failed to get torrent info from RD.")
        return []
        
    files = info.get("files", [])
    if files:
        debug_print(f"Found {len(files)} files in torrent. Filtering > 50MB...")
        # Select files larger than 50MB (likely VR video files)
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
    rd_api("POST", f"/torrents/selectFiles/{torrent_id}", {"files": file_ids})
    
    debug_print("Starting wait loop for links to be generated...")
    # Wait for links to be generated
    for attempt in range(15):
        debug_print(f"Wait attempt {attempt+1}/15 for torrent ID {torrent_id}...")
        info = rd_api("GET", f"/torrents/info/{torrent_id}")
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
        unrestrict = rd_api("POST", "/unrestrict/link", {"link": link})
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

def get_ptube_streams(url):
    print(f"  📡 Fetching PTube streams: {url}")
    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            data = json.loads(resp.read().decode())
        streams = data.get("streams", [])
        return streams
    except Exception as e:
        print(f"  ⚠️ PTube fetch failed: {e}")
        return []

def resolve_streams(stremio_id: str):
    """
    Ask the PTube addon for streams for a given Stremio content ID.
    If no URLs are found but torrents are hidden, retry with torrents enabled
    and manually resolve via Real-Debrid API.
    """
    url = f"{PTUBE_BASE}/stream/movie/{stremio_id}.json"
    debug_print(f"Resolving streams for Stremio ID: {stremio_id}. Primary URL: {url}")
    streams = get_ptube_streams(url)
    
    # 1. Prioritize HTTP streams
    http_streams = [s for s in streams if "url" in s or "externalUrl" in s]
    if http_streams:
        print(f"  ✅ Got {len(http_streams)} direct HTTP streams from PTube")
        debug_print(f"HTTP streams found: {http_streams}")
        return http_streams

    tried_hashes = set()
        
    # 2. Check if we already have torrent infoHashes without fallback
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
            rd_streams = resolve_infohash_rd(h)
            if rd_streams:
                debug_print("Successfully resolved RD streams from primary torrents.")
                return rd_streams
                
    # 3. No usable streams found, try fallback manifest
    if PTUBE_FALLBACK_BASE != PTUBE_BASE:
        fallback_url = f"{PTUBE_FALLBACK_BASE}/stream/movie/{stremio_id}.json"
        print("  ⚠️ No direct URLs or torrents found, trying fallback with torrents enabled...")
        debug_print(f"Fetching fallback URL: {fallback_url}")
        fallback_streams = get_ptube_streams(fallback_url)
        for s in fallback_streams:
            if "infoHash" in s:
                h = s["infoHash"].lower()
                if h in tried_hashes:
                    continue
                tried_hashes.add(h)
                debug_print(f"Fallback torrent found. Trying infoHash: {h}")
                rd_streams = resolve_infohash_rd(h)
                if rd_streams:
                    debug_print("Successfully resolved RD streams from fallback torrents.")
                    return rd_streams
                    
    print(f"  ❌ No usable streams found for {stremio_id}")
    debug_print("Exhausted all stream resolution methods.")
    return []


def streams_to_media(streams: list):
    """Convert stream list into HereSphere media format."""
    media = []
    for s in streams:
        # Each stream usually has a "url" (direct link) or "externalUrl"
        stream_url = s.get("url") or s.get("externalUrl")
        if not stream_url:
            continue

        # Try to parse resolution from the stream name/title
        name = s.get("name", "") or ""
        title = s.get("title", "") or ""
        description = s.get("description", "") or ""
        display_name = title or name or "Stream"

        # Attempt to extract resolution (e.g. "1080p", "4K", "2160p")
        res_match = re.search(r'(\d{3,4})p', f"{name} {title} {description}", re.IGNORECASE)
        height = int(res_match.group(1)) if res_match else 0
        if not height and re.search(r'4k|uhd', f"{name} {title}", re.IGNORECASE):
            height = 2160
        if not height and re.search(r'full\s*hd', f"{name} {title}", re.IGNORECASE):
            height = 1080
        if not height and re.search(r'\bHD\b', f"{name} {title}"):
            height = 720
        if not height:
            height = 1080  # fallback

        width = int(height * (16/9))

        # Guess file size from description if available
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

    name = item.get("name", "Unknown")
    poster = item.get("poster", "")
    date_added = parse_date(item.get("_ctime", ""))
    stremio_id = item.get("_id", "")
    
    # -----------------------------------------------------------------------
    # Parse tags and clean title for VR PTube videos
    # Expected format: "[SiteName] Video Title [YYYY-MM-DD, Tag1, Tag2, Res]"
    # -----------------------------------------------------------------------
    clean_title = name
    tags = []
    
    # Stremio truncates very long titles and some titles have unclosed internal brackets/parenthesis.
    # We find the LAST occurrence of ' [' which consistently marks the start of the tag array.
    last_bracket = name.rfind(' [')
    if last_bracket != -1 and name.startswith('['):
        title_part = name[:last_bracket]
        tags_part = name[last_bracket+2:]
        
        # Parse the title part for the site [SiteName] Title
        site_match = re.search(r'^\[(.*?)\]\s+(.*)$', title_part)
        if site_match:
            site = site_match.group(1).strip()
            clean_title = f"{site} - {site_match.group(2).strip()}"
        else:
            site = ""
            clean_title = title_part.strip()
            
        tags_clean = tags_part.replace(']', ',').replace('[', ',')
        
        # 1. Attempt to fetch clean metadata from StashDB
        # Stremio PTube titles often bury the TRUE video title inside parentheses
        # e.g., "[Site] Actors || Actors (The Actual Video Title! / 12345) [Tags...]"
        paren_match = re.search(r'\((.*?)\)', clean_title)
        
        if paren_match:
            search_term = paren_match.group(1)
            # Remove trailing ID injections from stremio addons like " / 31453"
            search_term = search_term.split(' / ')[0].strip()
        else:
            # Fallback to standard stripping if no parentheses
            search_term = clean_title.split(' - ')[-1] if ' - ' in clean_title else clean_title
            if ' || ' in search_term:
                search_term = search_term.split(' || ')[0]
            search_term = re.sub(r'\(.*?\)', '', search_term).strip()
            
        stash_data = query_stashdb(site, search_term)
        
        if stash_data:
            # Override title & date from StashDB
            clean_title = f"{stash_data.get('studio', {}).get('name', site)} - {stash_data.get('title', clean_title)}"
            if stash_data.get('date'):
                date_added = stash_data['date']
                
            # Map Performers and Tags
            for performer in stash_data.get('performers', []):
                p_name = performer.get('performer', {}).get('name')
                if p_name:
                    tags.append({"name": p_name, "start": 0, "end": 0})
                    
            for tag in stash_data.get('tags', []):
                t_name = tag.get('name')
                if t_name:
                    tags.append({"name": t_name, "start": 0, "end": 0})
        else:
            # 2. Fallback to extracting tags from Stremio title if StashDB misses
            import re as regex
            for t in tags_clean.split(','):
                t = t.strip()
                if t and not regex.match(r'^\d{3,4}p$', t) and not regex.match(r'^\dK$', t, regex.IGNORECASE) and not regex.match(r'^\d{4}-\d{2}-\d{2}$', t) and not regex.match(r'^\d{2}\.\d{2}\.\d{4}$', t) and t.lower() != "siterip" and not "fps" in t.lower() and "г." not in t.lower():
                    tags.append({"name": t, "start": 0, "end": 0})
    else:
        # Fallback to existing stremio tags if any
        stremio_tags = item.get("tags", [])
        if isinstance(stremio_tags, list):
            for t in stremio_tags:
                tags.append({"name": str(t), "start": 0, "end": 0})

    # Duration from state (in ms); Stremio stores in ms already
    duration_ms = 0
    state = item.get("state", {})
    if state.get("duration"):
        duration_ms = state["duration"]

    # Check if HereSphere wants media sources (user clicked play)
    # GET requests = library scan (no media needed)
    # POST requests = check needsMediaSource field (default True per spec)
    needs_media = False
    if request.method == "POST":
        try:
            body = request.get_json(force=True, silent=True) or {}
            needs_media = body.get("needsMediaSource", True)
        except Exception:
            needs_media = True

    # Build base response
    resp = {
        "access": 1,
        "title": clean_title,
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
        "tags": tags,
        "media": [],
        "writeFavorite": False,
        "writeRating": False,
        "writeTags": False,
        "writeHSP": False
    }

    # If the title hints at VR content, set appropriate projection defaults
    title_lower = name.lower()
    if any(kw in title_lower for kw in ["vr", "180", "360", "oculus", "vive", "quest"]):
        resp["projection"] = "equirectangular"
        resp["stereo"] = "sbs"
    if "360" in title_lower:
        resp["projection"] = "equirectangular360"

    # Resolve streams only when media is actually needed
    if needs_media:
        streams = resolve_streams(stremio_id)
        resp["media"] = streams_to_media(streams)

    return hs_response(resp)


@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    """Re-fetch the Stremio library."""
    fetch_library()
    return jsonify({"status": "ok", "count": len(library_items)})


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
    print(f"\n🚀 HereSphere bridge running on http://0.0.0.0:{PORT}/heresphere")
    print(f"   Point HereSphere to:  http://<YOUR_LAN_IP>:{PORT}/heresphere\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

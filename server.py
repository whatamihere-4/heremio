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
import threading
from flask import Flask, request, jsonify, make_response, render_template_string
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config & Authentication
# ---------------------------------------------------------------------------
STASHDB_API_KEY = os.environ.get("STASHDB_API_KEY", "")
DEBUG_MODE      = os.environ.get("DEBUG_MODE", "True").lower() == "true"

# ---------------------------------------------------------------------------
# Filename Parsing Logic
# ---------------------------------------------------------------------------
def parse_filename(filename: str) -> dict:
    """
    Extract title and tags from a filename string.

    Handles these common formats:
      1. [Site] Performer (Title / date) [year, tag1, tag2, ...]
      2. [Site] Performer - Title [date, tag1, tag2, ...]
      3. SiteName YY MM DD Performer Title XXX ...
      4. Site - Performer - Title resolution

    Returns:
        dict with keys:
            "title"   : str   — the cleaned scene/video title
            "tags"    : list  — list of individual tag strings
            "site"    : str   — source site name (if found)
            "performers" : list — performer names (best effort)
    """
    filename = filename.strip()
    if not filename:
        return {"title": "", "tags": [], "site": "", "performers": []}

    # -------------------------------------------------------------------
    # FORMAT 1 & 2:  Starts with [Site]
    # -------------------------------------------------------------------
    if filename.startswith("["):
        return _parse_bracketed(filename)

    # -------------------------------------------------------------------
    # FORMAT 3:  Scene-release style  e.g.
    #   DirtyWivesClub 24 01 17 Jessica Rex REMASTERED XXX VR180 ...
    # -------------------------------------------------------------------
    scene_match = re.match(
        r'^([A-Za-z]+(?:[A-Z][a-z]+)*)\s+'       # SiteName (CamelCase)
        r'(\d{2})\s+(\d{2})\s+(\d{2})\s+'        # YY MM DD
        r'(.+?)\s+XXX\b',                         # everything until XXX
        filename
    )
    if scene_match:
        return _parse_scene_release(filename, scene_match)

    # -------------------------------------------------------------------
    # FORMAT 4:  Simple dash-separated
    #   WankzVR - Abella Danger, Yhivi - Director's Cut - Threesomes, ...
    #   VRPorn - Ember Snow, ... - The Pussycat Girls 1920p
    #   SexLikeReal - Keaw - Hot Thai Bargirl Loves To Get Creampied 4096p
    # -------------------------------------------------------------------
    if " - " in filename:
        return _parse_dash_separated(filename)

    # Fallback: use the whole string as the title
    return {"title": filename, "tags": [], "site": "", "performers": []}


def _parse_bracketed(filename: str) -> dict:
    """
    Parse filenames that start with [Site] and contain bracket-delimited
    metadata sections.
    """
    # --- Extract site name(s) from leading bracket(s) ---
    site_match = re.match(r'^\[([^\]]+)\]', filename)
    site = site_match.group(1).strip() if site_match else ""
    # Strip .com from the site name for better StashDB matching
    site = re.sub(r'\.com$', '', site, flags=re.IGNORECASE)

    # Remove the leading [site] bracket to work with the rest
    rest = filename[site_match.end():].strip() if site_match else filename

    # --- Extract tag brackets (also match unclosed trailing brackets) ---
    all_brackets = re.findall(r'\[([^\]]+)(?:\]|$)', rest)

    tags = []
    for bracket_content in all_brackets:
        items = [item.strip() for item in bracket_content.split(",")]
        for item in items:
            cleaned = _clean_tag(item)
            if cleaned:
                tags.append(cleaned)

    # --- Strip all square-bracket sections to get the "core text" ---
    core = re.sub(r'\[[^\]]*\]', '', rest).strip()

    # --- Handle || separators (NaughtyAmerica pattern) ---
    # e.g. "Charlie Forde , Slimthick Vic || Sam Shock (description / ID)"
    # Performers are BEFORE ||, title is in parens AFTER ||
    if "||" in core:
        before_pipe, after_pipe = core.split("||", 1)
        performers = _extract_performers(before_pipe.strip())
        # Find the title in parens from the after-|| portion
        paren_match = re.search(r'\(([^)]*)\)', after_pipe)
        if paren_match:
            raw_title = paren_match.group(1).strip()
            title = _clean_title_from_parens(raw_title)
        else:
            title = after_pipe.strip()
        if not title:
            title = ", ".join(performers) if performers else site
        # Prepend performers as tags
        tags = performers + tags
        return {
            "title": title.strip(),
            "tags": tags,
            "site": site,
            "performers": performers,
        }

    # --- Extract title ---
    title = ""
    performers = []

    # Try to find content in parentheses (the scene title)
    paren_match = re.search(r'\(([^)]*)\)', core)
    if paren_match:
        raw_title = paren_match.group(1).strip()
        title = _clean_title_from_parens(raw_title)
        before_paren = core[:paren_match.start()].strip()

        # When there's a " - " before the parens, the dash-separated
        # part is always the real title (parens are supplementary info)
        # e.g. "Performer - Hot Strip Club Sex 8K (AI Upscaled...)"
        if " - " in before_paren:
            parts = before_paren.split(" - ", 1)
            performers = _extract_performers(parts[0])
            title = parts[1].strip() or title
        else:
            # Check if paren-extracted title is just noise (numeric ID, etc.)
            is_poor_title = (
                not title
                or re.match(r'^\d+$', title)
                or title.lower() in ("remastered", "a xxx parody", "a porn parody",
                                      "vr porn parody", "cgi")
            )
            if is_poor_title and before_paren:
                # Use the full text before parens as title
                title = before_paren
            else:
                performers = _extract_performers(before_paren)
                if performers and title:
                    # Mux NaughtyAmerica format (Performers - Series)
                    title = f"{', '.join(performers)} - {title}"
    else:
        # No parentheses — find title from text before first tag bracket
        first_bracket = re.search(r'\[', rest)
        if first_bracket:
            before_bracket = rest[:first_bracket.start()].strip()
        else:
            before_bracket = rest.strip()

        if " - " in before_bracket:
            parts = before_bracket.split(" - ", 1)
            performers = _extract_performers(parts[0])
            title = parts[1].strip()
        else:
            title = before_bracket

    # If title is empty but we have performers, build a fallback
    if not title and performers:
        title = ", ".join(performers)

    # If we still have no title, use the site name
    if not title:
        title = site or filename[:80]

    # Prepend performers as tags (HereSphere only supports tags)
    tags = performers + tags

    return {
        "title": title.strip(),
        "tags": tags,
        "site": site,
        "performers": performers,
    }


def _parse_scene_release(filename: str, match: re.Match) -> dict:
    """
    Parse scene-release style filenames like:
    DirtyWivesClub 24 01 17 Jessica Rex REMASTERED XXX VR180 3072p MP4-VACCiNE [XC]
    """
    site_raw = match.group(1)
    # Split CamelCase into words for the site name
    site = re.sub(r'([a-z])([A-Z])', r'\1 \2', site_raw)

    performer_and_title = match.group(5).strip()

    # Common keywords that signal end of performer/title info
    # Remove REMASTERED and similar suffixes
    clean_name = re.sub(r'\b(REMASTERED|Remastered)\b', '', performer_and_title).strip()

    # The rest after XXX contains tags/format info
    after_xxx = filename[match.end():]
    tags = []
    # Extract resolution, format tags
    tag_tokens = re.findall(r'(VR\d*|VR180|\d{3,4}p|MP4|SideBySide|[A-Z]{2,})', after_xxx)
    for t in tag_tokens:
        if t not in ("XXX", "MP4"):
            tags.append(t)

    # Also check for bracket content at the end
    bracket_tags = re.findall(r'\[([^\]]+)\]', after_xxx)
    for bt in bracket_tags:
        cleaned = bt.strip()
        if cleaned and cleaned not in tags:
            tags.append(cleaned)

    return {
        "title": clean_name,
        "tags": tags,
        "site": site,
        "performers": [],  # hard to separate performer from title in this format
    }


def _parse_dash_separated(filename: str) -> dict:
    """
    Parse dash-separated filenames like:
    WankzVR - Abella Danger, Yhivi - Director's Cut - Threesomes, Remastered 3456p
    """
    parts = filename.split(" - ")
    site = parts[0].strip() if len(parts) >= 1 else ""
    # Strip .com from the site name for better StashDB matching
    site = re.sub(r'\.com$', '', site, flags=re.IGNORECASE)
    performers = []
    title = ""

    if len(parts) >= 3:
        # Site - Performer(s) - Title [- extra]
        performers = _extract_performers(parts[1])
        # Rejoin remaining parts as title (may have dashes in it)
        title_raw = " - ".join(parts[2:])
        # Remove trailing resolution like "1920p" or "4096p"
        title = re.sub(r'\s+\d{3,4}p\s*$', '', title_raw).strip()
    elif len(parts) == 2:
        # Could be "Site - Title" or "Performer - Title"
        title = re.sub(r'\s+\d{3,4}p\s*$', '', parts[1]).strip()

    # Extract tags from comma-separated items in the last part
    tags = []
    last_part = parts[-1] if parts else ""
    if "," in last_part:
        items = [i.strip() for i in last_part.split(",")]
        for item in items:
            cleaned = _clean_tag(item)
            if cleaned:
                tags.append(cleaned)

    # Prepend performers as tags
    tags = performers + tags

    return {
        "title": title or filename,
        "tags": tags,
        "site": site,
        "performers": performers,
    }


def _clean_title_from_parens(raw: str) -> str:
    """
    Clean a title extracted from parentheses.
    Removes dates, IDs, and other noise while keeping the actual title.

    Examples:
      "Group Interview"                          → "Group Interview"
      "Final Fantasy XXX Parody / 20.01.2014 / 323591"  → "Final Fantasy XXX Parody"
      "Capital sins: Wrath – Halloween Special"  → "Capital sins: Wrath – Halloween Special"
      "After School / 27.06.2018"                → "After School"
      "The Unfaithful Boyfriend / 20.09.2019"    → "The Unfaithful Boyfriend"
    """
    # Split on " / " to separate title from date/ID components
    segments = [s.strip() for s in raw.split(" / ")]

    # Also handle " | " as separator
    if len(segments) == 1:
        segments = [s.strip() for s in raw.split(" | ")]

    # Keep segments that don't look like pure dates or numeric IDs
    title_parts = []
    for seg in segments:
        # Skip if it's purely a date pattern (DD.MM.YYYY, YYYY-MM-DD, etc.)
        if re.match(r'^\d{1,2}[./]\d{1,2}[./]\d{2,4}$', seg):
            continue
        # Skip if it's purely numeric (like an ID: 323591)
        if re.match(r'^\d{4,}$', seg):
            continue
        # Skip pure year patterns
        if re.match(r'^\d{4}\s*(г\.|year)?$', seg):
            continue
        title_parts.append(seg)

    title = " / ".join(title_parts) if title_parts else raw

    return title.strip()


def _extract_performers(text: str) -> list:
    """
    Extract performer names from a text segment.
    Handles comma-separated and & separated names.
    """
    text = text.strip()
    if not text:
        return []

    # Remove common prefixes/noise
    text = re.sub(r'\|\|.*$', '', text)  # Remove || narrator names
    text = text.strip()

    # Split on comma and/or &
    names = re.split(r',\s*|\s+&\s+', text)

    performers = []
    for name in names:
        name = name.strip()
        # Skip if it looks like a tag or keyword rather than a name
        if not name:
            continue
        if re.match(r'^\d+p$', name):
            continue
        if name.upper() in ("VR", "POV", "SBS", "4K", "8K", "XXX"):
            continue
        performers.append(name)

    return performers


def _clean_tag(raw: str) -> str:
    """
    Clean a single tag string. Returns empty string if the tag
    should be skipped.
    """
    tag = raw.strip()
    if not tag:
        return ""

    # Skip pure year entries like "2017 г." or "2022"
    if re.match(r'^\d{4}\s*(г\.?)?$', tag):
        return ""

    # Skip date entries
    if re.match(r'^\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}$', tag):
        return ""

    # Skip pure resolutions (we'll handle these separately if needed)
    if re.match(r'^\d{3,4}p$', tag):
        return ""

    # Skip format/container markers
    if tag.upper() in ("SITERIP", "SIDEBYIDE"):
        return ""

    return tag


def get_auth_key():
    email = os.environ.get("STREMIO_USER")
    password = os.environ.get("STREMIO_PASS")

    if not email or not password:
        print("Error: Missing STREMIO_USER or STREMIO_PASS in .env file.")
        print("Please add them to your .env file and try again.")
        return None

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
        print(f"🛠️ [DEBUG] {msg}", flush=True)

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

CACHE_FILE = "cache.json"
STASH_CACHE = {}     # Cache for StashDB queries {"Site - Title": stashdb_dict}
STASH_PENDING = set() # Track keys currently being queried

# Load existing cache from disk
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            STASH_CACHE = json.load(f)
        if DEBUG_MODE:
            print(f"Loaded {len(STASH_CACHE)} entries from {CACHE_FILE}")
    except Exception as e:
        print(f"Error loading {CACHE_FILE}: {e}")


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
        
    if cache_key in STASH_PENDING:
        return None
        
    STASH_PENDING.add(cache_key)

    # Use BOTH site and title in the text query to avoid broad mismatches
    # e.g. "The Student" vs "DarkRoomVR The Student"
    search_text = f"{site} {clean_title}" if site else clean_title

    debug_print(f"Querying StashDB for: '{search_text}'")
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
    
    payload = {
        "query": query,
        "variables": {
            "input": {
                "text": search_text
            }
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            scenes = data.get("data", {}).get("queryScenes", {}).get("scenes", [])
            if scenes:
                # We could ideally filter scenes by exact studio match, but StashDB's 
                # text search with the site name included usually ranks the right one first
                first_match = scenes[0]
                STASH_CACHE[cache_key] = first_match
                
                # Save to disk
                try:
                    with open(CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(STASH_CACHE, f, indent=2)
                except Exception as e:
                    debug_print(f"Error saving to {CACHE_FILE}: {e}")
                    
                debug_print(f"✅ StashDB match found: {first_match.get('title')} (Studio: {first_match.get('studio', {}).get('name')})")
                return first_match
            else:
                debug_print("❌ No StashDB results found.")
                STASH_CACHE[cache_key] = None
                # Save the "not found" state too so we don't spam StashDB
                try:
                    with open(CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(STASH_CACHE, f, indent=2)
                except Exception as e:
                    debug_print(f"Error saving to {CACHE_FILE}: {e}")
        else:
            debug_print(f"⚠️ StashDB HTTP Error {response.status_code}: {response.text}")
    except Exception as e:
        debug_print(f"⚠️ StashDB Query Exception: {e}")
    finally:
        STASH_PENDING.discard(cache_key)
        
    return None

def query_stashdb_by_id(scene_id: str):
    """
    Search StashDB using a specific Scene ID and return structured HereSphere metadata if found.
    """
    if not STASHDB_API_KEY:
        return None

    debug_print(f"Querying StashDB by ID: '{scene_id}'")
    url = "https://stashdb.org/graphql"
    headers = {
        "Content-Type": "application/json",
        "ApiKey": STASHDB_API_KEY
    }

    query = """
    query FindScene($id: ID!) {
      findScene(id: $id) {
        title
        date
        studio { name }
        performers { performer { name } }
        tags { name }
      }
    }
    """
    
    payload = {
        "query": query,
        "variables": {
            "id": scene_id
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            scene = data.get("data", {}).get("findScene")
            if scene:
                debug_print(f"✅ StashDB match found by ID: {scene.get('title')}")
                return scene
            else:
                debug_print("❌ No StashDB results found for ID.")
        else:
            debug_print(f"⚠️ StashDB HTTP Error {response.status_code}: {response.text}")
    except Exception as e:
        debug_print(f"⚠️ StashDB Query Exception: {e}")
        
    return None

def fill_stash_cache_background():
    """
    Background worker that runs on startup.
    Finds all movies in the library that don't have a StashDB cache entry yet,
    and queries StashDB for them one by one with a rate limit.
    """
    if not STASHDB_API_KEY:
        return
        
    to_fetch = []
    
    # Identify items missing from cache
    for item in library_items:
        raw_name = item.get("name", "")
        if not raw_name:
            continue
            
        parsed = parse_filename(raw_name)
        site = parsed.get("site", "")
        title = parsed.get("title", "")
        
        cache_key = f"{site} - {title}"
        if cache_key not in STASH_CACHE:
            to_fetch.append((site, title))
            
    # Deduplicate matching site/title combos
    to_fetch = list(set(to_fetch))
    
    if to_fetch:
        print(f"🔄 StashDB: Found {len(to_fetch)} new videos to cache. Starting background fetch (this may take a while)...", flush=True)
        # StashDB rate limits, so we add a delay
        delay_seconds = 1.5
        for i, (site, title) in enumerate(to_fetch, 1):
            if DEBUG_MODE:
                print(f"🛠️ [DEBUG] Background Fetch [{i}/{len(to_fetch)}]: {site} - {title}", flush=True)
            query_stashdb(site, title)
            # Sleep unless it's the very last item
            if i < len(to_fetch):
                time.sleep(delay_seconds)
                
        print("✅ StashDB: Background cache filling complete!", flush=True)


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

    raw_name = item.get("name", "Unknown")
    poster = item.get("poster", "")
    date_added = parse_date(item.get("_ctime", ""))
    stremio_id = item.get("_id", "")

    # Parse filename to extract clean title and tags
    parsed = parse_filename(raw_name)
    title = parsed["title"]
    site = parsed["site"]
    
    # Attempt to fetch StashDB tags from cache
    # We no longer query StashDB synchronously here to avoid making the user wait.
    # The background thread handles filling the cache.
    cache_key = f"{site} - {title}"
    stash_data = STASH_CACHE.get(cache_key)

    hs_tags = []
    if stash_data:
        # Add performers as the first tags
        for performer in stash_data.get("performers", []):
            p_name = performer.get("performer", {}).get("name")
            if p_name:
                hs_tags.append({"name": p_name, "start": 0, "end": 0})
                
        # Add the rest of the tags
        for tag in stash_data.get("tags", []):
            t_name = tag.get("name")
            if t_name:
                hs_tags.append({"name": t_name, "start": 0, "end": 0})
    else:
        # Fallback to local regex-parsed tags if StashDB data isn't cached yet (or failed)
        if STASHDB_API_KEY:
            debug_print(f"Using local backup tags for: {title} (StashDB cache miss/pending)")
        else:
            debug_print(f"Using local backup tags for: {title}")
        hs_tags = [{"name": t, "start": 0, "end": 0} for t in parsed["tags"]]

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

    # If the raw name hints at VR content, set appropriate projection defaults
    name_lower = raw_name.lower()
    if any(kw in name_lower for kw in ["vr", "180", "360", "oculus", "vive", "quest"]):
        resp["projection"] = "equirectangular"
        resp["stereo"] = "sbs"
    if "360" in name_lower:
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


@app.route("/match", methods=["GET"])
def match_ui():
    """Web UI to manually match videos that StashDB missed."""
    unmatched_items = []
    
    for item in library_items:
        raw_name = item.get("name", "")
        if not raw_name: continue
        
        parsed = parse_filename(raw_name)
        site = parsed.get("site", "")
        title = parsed.get("title", "")
        
        cache_key = f"{site} - {title}"
        stash_data = STASH_CACHE.get(cache_key)
        
        # We only want items that have been cached explicitly as None (failed matches)
        # or items not in cache at all (though background thread usually handles them)
        if stash_data is None and cache_key in STASH_CACHE:
            unmatched_items.append({
                "cache_key": cache_key,
                "raw_name": raw_name,
                "parsed_title": title,
                "parsed_site": site,
                "poster": item.get("poster", "")
            })
            
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Manual StashDB Matching</title>
        <style>
            body { font-family: system-ui, sans-serif; background: #1a1a1a; color: #eee; max-width: 800px; margin: 0 auto; padding: 20px; }
            .item { display: flex; gap: 20px; background: #2a2a2a; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
            .poster { width: 120px; border-radius: 4px; object-fit: cover; }
            .info { flex: 1; }
            h3 { margin: 0 0 10px 0; color: #4facfe; }
            .raw-name { font-size: 0.9em; color: #aaa; margin-bottom: 15px; }
            input[type="text"] { width: 100%; padding: 8px; margin-bottom: 10px; background: #333; color: white; border: 1px solid #444; border-radius: 4px; }
            button { background: #4facfe; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; }
            button:hover { background: #00f2fe; color: black; }
            .success { color: #00f2fe; display: none; margin-top: 10px; }
            .error { color: #fe4f4f; display: none; margin-top: 10px; }
        </style>
    </head>
    <body>
        <h2>Unmatched Videos ({{ items|length }})</h2>
        <p>These videos yielded no results on StashDB automatically. Paste a StashDB Scene URL below to fix them.</p>
        
        {% for item in items %}
        <div class="item" id="item-{{ loop.index }}">
            <img src="{{ item.poster }}" class="poster" onerror="this.style.display='none'">
            <div class="info">
                <h3>{{ item.parsed_site }} - {{ item.parsed_title }}</h3>
                <div class="raw-name">{{ item.raw_name }}</div>
                <input type="text" id="url-{{ loop.index }}" placeholder="Paste StashDB Scene URL here (e.g. https://stashdb.org/scenes/xyz)">
                <button onclick="submitMatch('{{ loop.index }}', '{{ item.cache_key|urlencode }}')">Save Match</button>
                <div id="msg-{{ loop.index }}"></div>
            </div>
        </div>
        {% endfor %}
        
        <script>
            async function submitMatch(idx, cacheKey) {
                const urlInput = document.getElementById('url-' + idx).value;
                const msgDiv = document.getElementById('msg-' + idx);
                msgDiv.innerHTML = "Saving...";
                msgDiv.className = "";
                msgDiv.style.display = "block";
                msgDiv.style.color = "#aaa";
                
                try {
                    const res = await fetch('/api/match', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ cache_key: decodeURIComponent(cacheKey), stashdb_url: urlInput })
                    });
                    const data = await res.json();
                    if (data.success) {
                        msgDiv.innerHTML = "✅ Matched: " + data.title;
                        msgDiv.className = "success";
                        msgDiv.style.display = "block";
                    } else {
                        msgDiv.innerHTML = "❌ Error: " + data.error;
                        msgDiv.style.color = "#fe4f4f";
                        msgDiv.style.display = "block";
                    }
                } catch(e) {
                    msgDiv.innerHTML = "❌ Network Error";
                    msgDiv.style.color = "#fe4f4f";
                    msgDiv.style.display = "block";
                }
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template, items=unmatched_items)

@app.route("/api/match", methods=["POST"])
def api_match():
    req = request.json
    if not req:
        return jsonify({"success": False, "error": "Invalid request"})
        
    cache_key = req.get("cache_key")
    stashdb_url = req.get("stashdb_url", "")
    
    # Extract ID from URL
    match = re.search(r'stashdb\.org/scenes/([a-f0-9\-]+)', stashdb_url)
    if not match:
        return jsonify({"success": False, "error": "Invalid StashDB Scene URL"})
        
    scene_id = match.group(1)
    
    # Fetch from StashDB
    scene_data = query_stashdb_by_id(scene_id)
    if not scene_data:
        return jsonify({"success": False, "error": "Could not fetch scene from StashDB"})
        
    # Save to Cache
    STASH_CACHE[cache_key] = scene_data
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(STASH_CACHE, f, indent=2)
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to save to disk: {e}"})
        
    return jsonify({"success": True, "title": scene_data.get("title")})


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
    threading.Thread(target=fill_stash_cache_background, daemon=True).start()
    
    print(f"\n🚀 HereSphere bridge running on http://0.0.0.0:{PORT}/heresphere")
    print(f"   Point HereSphere to:  http://<YOUR_LAN_IP>:{PORT}/heresphere\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

# -*- coding: utf-8 -*-
"""
StashDB integration — queries, caching, and background pre-fetch worker.
"""

import os
import re
import json
import time
import requests
import difflib


# ---------------------------------------------------------------------------
# Cache state
# ---------------------------------------------------------------------------
CACHE_FILE = "cache.json"
STASH_CACHE = {}       # {"Site - Title": stashdb_dict_or_None}
STASH_PENDING = set()  # keys currently being queried (thread safety)


def load_cache(debug_mode=False):
    """Load the StashDB cache from disk."""
    global STASH_CACHE
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                STASH_CACHE.clear()
                STASH_CACHE.update(data)
            if debug_mode:
                print(f"Loaded {len(STASH_CACHE)} entries from {CACHE_FILE}")
        except Exception as e:
            print(f"Error loading {CACHE_FILE}: {e}")


def save_cache():
    """Persist the cache to disk."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(STASH_CACHE, f, indent=2)
    except Exception as e:
        print(f"Error saving to {CACHE_FILE}: {e}")


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def _normalize(name: str) -> str:
    """Lowercase, strip spaces/punctuation for fuzzy studio comparison."""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def _studio_matches(expected_site: str, scene: dict) -> bool:
    """Check if a StashDB scene's studio loosely matches our expected site name."""
    studio = (scene.get("studio") or {}).get("name", "")
    if not studio or not expected_site:
        return False
    a = _normalize(expected_site)
    b = _normalize(studio)
    # One contains the other (handles "VRBangers" vs "VR Bangers", etc.)
    return a in b or b in a


def query_stashdb(site: str, clean_title: str, parsed_performers=None, *, api_key: str, debug_print=print):
    """
    Query StashDB with caching.
    Uses title, site (parsed directly or from stashdb studio), and potentially fallback actor names.
    Returns the metadata dict on success, or None on failure.
    """
    if not api_key:
        return None

    # 1) Use the same cache key HereSphere expects
    cache_key = f"{site} - {clean_title}"
    if cache_key in STASH_CACHE:
        return STASH_CACHE[cache_key]

    # Pre-parse title to isolate what looks like the actor vs actual title
    scene_name = clean_title
    actor_names = ""
    
    if parsed_performers:
        actor_names = " ".join(parsed_performers)
    elif " - " in clean_title:
        parts = clean_title.split(" - ", 1)
        actor_names = parts[0].strip()
        scene_name = parts[1].strip()

    # Build search text — strip parentheses, dashes, colons, etc. per user request
    search_text = re.sub(r'[()\-:\[\],_!?~]+', ' ', scene_name)
    # Strip resolution strings like '4K', '8K', '6k'
    search_text = re.sub(r'\b\d+[kK]\b', '', search_text)
    # Collapse multiple spaces
    search_text = " ".join(search_text.split())

    debug_print(f"Querying StashDB for title only: '{search_text}'")
    url = "https://stashdb.org/graphql"
    headers = {"Content-Type": "application/json", "ApiKey": api_key}

    query = """
    query SearchScenes($term: String!) {
      searchScene(term: $term) {
        title
        date
        studio { name }
        performers { performer { name } }
        tags { name }
      }
    }
    """

    def perform_query(term):
        payload = {
            "query": query,
            "variables": {"term": term},
        }
        response = requests.post(url, json=payload, headers=headers, timeout=20)
        if response.status_code == 200:
            return response.json().get("data", {}).get("searchScene", [])
        else:
            debug_print(f"⚠️ StashDB HTTP Error {response.status_code}: {response.text}")
            return None

    try:
        scenes = perform_query(search_text)
        if scenes is not None:
            best_match = None
            
            for s in scenes:
                stash_title = s.get("title", "")
                
                # Check for 90% title match
                ratio = difflib.SequenceMatcher(None, stash_title.lower(), scene_name.lower()).ratio()
                
                if ratio >= 0.90:
                    debug_print(f"✅ Title match ({ratio:.2f}): {stash_title}")
                    best_match = s
                    break
                    
                # If title isn't a near-perfect match, require the studio to match
                if site and _studio_matches(site, s):
                    debug_print(f"✅ Studio fallback match for {stash_title} (Ratio: {ratio:.2f})")
                    best_match = s
                    break

            # Fallback if no best match was found, multiple scenes were found, and we have actor names
            if not best_match and len(scenes) > 1 and actor_names:
                fallback_term = f"{search_text} {actor_names}"
                fallback_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', fallback_term)
                fallback_term = " ".join(fallback_term.split())
                debug_print(f"🔄 Multiple results but no match. Retrying with actor names appended: '{fallback_term}'")
                
                fallback_scenes = perform_query(fallback_term)
                if fallback_scenes:
                    for s in fallback_scenes:
                        stash_title = s.get("title", "")
                        ratio = difflib.SequenceMatcher(None, stash_title.lower(), scene_name.lower()).ratio()
                        if ratio >= 0.90:
                            debug_print(f"✅ Fallback title match ({ratio:.2f}): {stash_title}")
                            best_match = s
                            break
                        if site and _studio_matches(site, s):
                            debug_print(f"✅ Fallback studio match for {stash_title} (Ratio: {ratio:.2f})")
                            best_match = s
                            break

            if best_match:
                STASH_CACHE[cache_key] = best_match
                save_cache()
                debug_print(f"🎉 StashDB saved: {best_match.get('title')} "
                            f"(Studio: {best_match.get('studio', {}).get('name')})")
                debug_print("")
                return best_match
            else:
                debug_print(f"❌ No suitable StashDB results found out of {len(scenes)} candidates.")
                STASH_CACHE[cache_key] = None
                save_cache()
                debug_print("")

    except requests.exceptions.Timeout:
        debug_print(f"⚠️ StashDB Query Timeout for '{search_text}'. Will retry next session.")
    except Exception as e:
        debug_print(f"⚠️ StashDB Query Exception: {e}")
    finally:
        STASH_PENDING.discard(cache_key)

    return None


def query_stashdb_by_id(scene_id: str, *, api_key: str, debug_print):
    """Fetch a single scene from StashDB by its UUID."""
    if not api_key:
        return None

    debug_print(f"Querying StashDB by ID: '{scene_id}'")
    url = "https://stashdb.org/graphql"
    headers = {"Content-Type": "application/json", "ApiKey": api_key}

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

    payload = {"query": query, "variables": {"id": scene_id}}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=20)
        if response.status_code == 200:
            scene = response.json().get("data", {}).get("findScene")
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


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------
def fill_stash_cache_background(library_items, parse_filename, *, api_key, debug_mode, debug_print):
    """
    Pre-fetch StashDB data for every video in the library that isn't cached yet.
    Runs in a background thread on startup. Respects rate limits (0.75 s delay).
    """
    if not api_key:
        return

    to_fetch = []
    for item in library_items:
        raw_name = item.get("name", "")
        if not raw_name:
            continue
        parsed = parse_filename(raw_name)
        site = parsed.get("site", "")
        title = parsed.get("title", "")
        performers = parsed.get("performers", [])
        cache_key = f"{site} - {title}"
        if cache_key not in STASH_CACHE:
            to_fetch.append((site, title, performers, raw_name))

    # Keep track of unique (site, title) pairs but retain raw_name and performers
    unique_fetches = {}
    for site, title, performers, raw_name in to_fetch:
        unique_fetches[(site, title)] = (performers, raw_name)

    if unique_fetches:
        print(f"🔄 StashDB: Found {len(unique_fetches)} new videos to cache. "
              f"Starting background fetch (this may take a while)...", flush=True)
        delay_seconds = 0.75
        for i, ((site, title), (performers, raw_name)) in enumerate(unique_fetches.items(), 1):
            if debug_mode:
                print(f"🛠️ [DEBUG] Background Fetch [{i}/{len(unique_fetches)}]: {raw_name}", flush=True)
            query_stashdb(site, title, parsed_performers=performers, api_key=api_key, debug_print=debug_print)
            if i < len(to_fetch):
                time.sleep(delay_seconds)

        print("✅ StashDB: Background cache filling complete!", flush=True)

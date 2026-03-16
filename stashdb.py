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


def query_stashdb(sites, clean_title: str, parsed_performers=None, parsed_dates=None, *, api_key: str, debug_print=print):
    """
    Query StashDB with caching.
    Uses title, site (parsed directly or from stashdb studio), and potentially fallback actor names.
    Returns the metadata dict on success, or None on failure.
    """
    if not api_key:
        return None
    
    # Guarantee sites is a list
    if isinstance(sites, str):
        sites = [sites] if sites else []
    elif not sites:
        sites = []

    if parsed_dates:
        debug_print(f"📅 Parsed dates: {parsed_dates}")

    # Grab the primary site for the cache key
    primary_site = sites[0] if sites else ""

    # Check cache first
    cache_key = f"{primary_site} - {clean_title}"
    if cache_key in STASH_CACHE:
        return STASH_CACHE[cache_key]

    # Pre-parse title to isolate what looks like the actor vs actual title
    scene_name = clean_title
    search_title = clean_title
    actor_names = ""
    
    if parsed_performers:
        actor_names = " ".join(parsed_performers)
        # Strip actor names from the search_title to avoid poisoning the initial search
        for p in parsed_performers:
            search_title = re.sub(re.escape(p), '', search_title, flags=re.IGNORECASE)
        # Clean up dangling dashes, commas, and extra spaces
        search_title = re.sub(r'[,\-]', ' ', search_title)
        search_title = " ".join(search_title.split())
        
        # If the search title becomes empty, revert it
        if not search_title.strip():
            search_title = clean_title
            
    elif " - " in clean_title:
        parts = clean_title.split(" - ", 1)
        actor_names = parts[0].strip()
        scene_name = parts[1].strip()
        search_title = scene_name

    # Build search text using search_title (so scene_name remains intact for difflib matching)
    search_text = re.sub(r'[()\-:\[\],_!?~]+', ' ', search_title)
    
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
        def find_best_candidate(candidate_scenes, fallback_label=""):
            if not candidate_scenes: return None
            
            # Helper to check if ANY of our parsed studios match the StashDB result
            def any_studio_matches(s_dict):
                return any(_studio_matches(st, s_dict) for st in sites)

            # Pass 1: Date check (Super high priority)
            if parsed_dates:
                for s in candidate_scenes:
                    stash_date = s.get("date") or ""
                    if stash_date and stash_date in parsed_dates:
                        stash_title = s.get("title") or ""
                        stash_studio = (s.get("studio") or {}).get("name", "")
                        
                        ratio = difflib.SequenceMatcher(None, stash_title.lower(), scene_name.lower()).ratio()
                        studio_ratio = difflib.SequenceMatcher(None, stash_studio.lower(), scene_name.lower()).ratio() if stash_studio else 0.0
                        
                        # If date matches perfectly, require a tighter title/studio match to avoid false positives
                        if ratio >= 0.65 or studio_ratio >= 0.85 or (any_studio_matches(s) and ratio >= 0.35):
                            debug_print(f"✅ Exact Date match! {fallback_label} (Title: {ratio:.2f}, Studio: {studio_ratio:.2f}): {stash_title} [{stash_date}]")
                            return s
                            
            # Pass 2: Standard Title/Studio match
            for s in candidate_scenes:
                stash_title = s.get("title") or ""
                stash_studio = (s.get("studio") or {}).get("name", "")
                
                ratio = difflib.SequenceMatcher(None, stash_title.lower(), scene_name.lower()).ratio()
                studio_ratio = difflib.SequenceMatcher(None, stash_studio.lower(), scene_name.lower()).ratio() if stash_studio else 0.0
                
                if ratio >= 0.85:
                    debug_print(f"✅ Title match {fallback_label} ({ratio:.2f}): {stash_title}")
                    return s
                if len(candidate_scenes) == 1 and studio_ratio >= 0.85:
                    debug_print(f"✅ Sub-studio match! {fallback_label} '{stash_studio}' ({studio_ratio:.2f})")
                    return s
                    
            # Pass 3: Hyper-specific fallback trust
            if len(candidate_scenes) == 1:
                s = candidate_scenes[0]
                stash_title = s.get("title") or ""
                ratio = difflib.SequenceMatcher(None, stash_title.lower(), scene_name.lower()).ratio()

                if fallback_label in ["[Studio+Title]", "[Studio+Title+Actors]", "[Title+Date]", "[Studio+Date]"]:
                    if ratio >= 0.55 or any_studio_matches(s):
                        debug_print(f"✅ {fallback_label} Single exact match trusted: '{stash_title}' (Ratio: {ratio:.2f})")
                        return s

                if fallback_label in ["[Studio+Actors]", "[Studio+First Actor]", "[Title+Actors]", "[Title+First Actor]"]:
                    if any_studio_matches(s) or ratio >= 0.60:
                        debug_print(f"✅ {fallback_label} Single exact match trusted: '{stash_title}' (Ratio: {ratio:.2f})")
                        return s
                        
                # Blindly trust extremely specific combinations if they yield exactly one result
                if fallback_label in ["[Actors Only]", "[Studio+Date+Actors]"]:
                    debug_print(f"✅ {fallback_label} Single exact match trusted: '{stash_title}' (Ratio: {ratio:.2f})")
                    return s
                else:
                    debug_print(f"❌ Single result found for {fallback_label} but failed sanity check (Title Ratio: {ratio:.2f})")
                
            return None

        # 1. Primary Search
        scenes = perform_query(search_text)
        best_match = find_best_candidate(scenes, "[Primary]")

        # 2. Studio + Title
        if not best_match and sites:
            for site in sites:
                site_title_term = f"{site} {search_text}".strip()
                site_title_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', site_title_term)
                site_title_term = " ".join(site_title_term.split())
                debug_print(f"🔄 Retrying with Studio + Title: '{site_title_term}'")
                best_match = find_best_candidate(perform_query(site_title_term), "[Studio+Title]")
                if best_match:
                    break

        # 3. Studio + Date (Incredibly precise if title is mangled but date is known)
        if not best_match and sites and parsed_dates:
            for site in sites:
                for date_str in parsed_dates:
                    site_date_term = f"{site} {date_str}".strip()
                    debug_print(f"🔄 Retrying with Studio + Date: '{site_date_term}'")
                    best_match = find_best_candidate(perform_query(site_date_term), "[Studio+Date]")
                    if best_match:
                        break
                if best_match:
                    break

        # 4. Title + Date
        if not best_match and parsed_dates:
            for date_str in parsed_dates:
                date_term = f"{search_text} {date_str}".strip()
                debug_print(f"🔄 Retrying with Title + Date: '{date_term}'")
                best_match = find_best_candidate(perform_query(date_term), "[Title+Date]")
                if best_match:
                    break

        # 4.5 Studio + Date + Actors
        if not best_match and sites and parsed_dates and actor_names:
            for site in sites:
                for date_str in parsed_dates:
                    site_date_actor_term = f"{site} {date_str} {actor_names}".strip()
                    site_date_actor_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', site_date_actor_term)
                    site_date_actor_term = " ".join(site_date_actor_term.split())
                    debug_print(f"🔄 Retrying with Studio + Date + Actors: '{site_date_actor_term}'")
                    best_match = find_best_candidate(perform_query(site_date_actor_term), "[Studio+Date+Actors]")
                    if best_match:
                        break
                if best_match:
                    break

        # 5. Missing Actors Fallback (Title + Actors) - NO STUDIO LOOP NEEDED
        if not best_match and actor_names:
            missing_parts = [word for word in actor_names.split() if word.lower() not in search_text.lower()]
            if missing_parts:
                fallback_term = f"{search_text} {' '.join(missing_parts)}"
                fallback_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', fallback_term)
                fallback_term = " ".join(fallback_term.split())
                debug_print(f"🔄 Retrying with Title + Actors: '{fallback_term}'")
                best_match = find_best_candidate(perform_query(fallback_term), "[Title+Actors]")

        # 5.5 Title + First Actor - NO STUDIO LOOP NEEDED
        if not best_match and parsed_performers and len(parsed_performers) > 1:
            first_actor = parsed_performers[0]
            first_actor_term = f"{search_text} {first_actor}".strip()
            first_actor_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', first_actor_term)
            first_actor_term = " ".join(first_actor_term.split())
            debug_print(f"🔄 Retrying with Title + First Actor only: '{first_actor_term}'")
            best_match = find_best_candidate(perform_query(first_actor_term), "[Title+First Actor]")

        # 6. Studio + Title + Actors
        if not best_match and sites and actor_names:
            for site in sites:
                missing_parts = [word for word in actor_names.split() if word.lower() not in search_text.lower()]
                site_term = f"{site} {search_text} {' '.join(missing_parts)}".strip()
                site_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', site_term)
                site_term = " ".join(site_term.split())
                debug_print(f"🔄 Retrying with Studio + Title + Actors: '{site_term}'")
                best_match = find_best_candidate(perform_query(site_term), "[Studio+Title+Actors]")
                if best_match:
                    break

        # 7. Studio + Actors Only (Bypasses bad titles completely)
        if not best_match and sites and actor_names:
            for site in sites:
                site_actor_term = f"{site} {actor_names}"
                site_actor_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', site_actor_term)
                site_actor_term = " ".join(site_actor_term.split())
                debug_print(f"🔄 Retrying with Studio + Actors: '{site_actor_term}'")
                best_match = find_best_candidate(perform_query(site_actor_term), "[Studio+Actors]")
                if best_match:
                    break

        # 7.5 Studio + First Actor Only (If bad title AND bad secondary actor alias)
        if not best_match and sites and parsed_performers and len(parsed_performers) > 1:
            for site in sites:
                first_actor = parsed_performers[0]
                site_first_actor_term = f"{site} {first_actor}".strip()
                site_first_actor_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', site_first_actor_term)
                site_first_actor_term = " ".join(site_first_actor_term.split())
                debug_print(f"🔄 Retrying with Studio + First Actor only: '{site_first_actor_term}'")
                best_match = find_best_candidate(perform_query(site_first_actor_term), "[Studio+First Actor]")
                if best_match:
                    break

        # 8. Actors Only
        if not best_match and actor_names and len(actor_names.split()) >= 3:
            actor_only_term = re.sub(r'[()\-:\[\],_!?~]+', ' ', actor_names)
            actor_only_term = " ".join(actor_only_term.split())
            debug_print(f"🔄 Retrying with ONLY actor names: '{actor_only_term}'")
            best_match = find_best_candidate(perform_query(actor_only_term), "[Actors Only]")

        if best_match:
            STASH_CACHE[cache_key] = best_match
            save_cache()
            debug_print(f"🎉 StashDB saved: {best_match.get('title')} (Studio: {best_match.get('studio', {}).get('name')})")
            debug_print("")
            return best_match
        else:
            debug_print(f"❌ No suitable StashDB results found.")
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
        studios = parsed.get("studios", [])
        title = parsed.get("title", "")
        performers = parsed.get("performers", [])
        possible_dates = parsed.get("possible_dates", [])
        
        primary_site = studios[0] if studios else ""
        cache_key = f"{primary_site} - {title}"
        
        if cache_key not in STASH_CACHE:
            to_fetch.append((studios, title, performers, possible_dates, raw_name))

    # Keep track of unique (studios, title) pairs (using a tuple so it's hashable)
    unique_fetches = {}
    for studios, title, performers, possible_dates, raw_name in to_fetch:
        unique_fetches[(tuple(studios), title)] = (performers, possible_dates, raw_name)

    if unique_fetches:
        print(f"🔄 StashDB: Found {len(unique_fetches)} new videos to cache. "
              f"Starting background fetch (this may take a while)...", flush=True)
        delay_seconds = 0.75
        for i, ((studios, title), (performers, possible_dates, raw_name)) in enumerate(unique_fetches.items(), 1):
            if debug_mode:
                print(f"🛠️ [DEBUG] Background Fetch [{i}/{len(unique_fetches)}]: {raw_name}", flush=True)
            query_stashdb(list(studios), title, parsed_performers=performers, parsed_dates=possible_dates, api_key=api_key, debug_print=debug_print)
            if i < len(to_fetch):
                time.sleep(delay_seconds)

        print("✅ StashDB: Background cache filling complete!", flush=True)

# -*- coding: utf-8 -*-
"""
StashDB integration — queries, caching, and background pre-fetch worker.
"""

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import difflib

class BufferedLogger:
    def __init__(self, buffer):
        self.buffer = buffer
    def debug(self, msg, *args):
        if self.buffer is not None:
            self.buffer.append(msg % args if args else msg)
        else:
            log.debug(msg, *args)
    def warning(self, msg, *args):
        if self.buffer is not None:
            self.buffer.append("WARNING: " + (msg % args if args else msg))
        else:
            log.warning(msg, *args)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("heremio.stashdb")

# ---------------------------------------------------------------------------
# HTTP session with retry
# ---------------------------------------------------------------------------
_retry = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))

# ---------------------------------------------------------------------------
# Pre-compiled regex patterns
# ---------------------------------------------------------------------------
_RE_NORMALIZE = re.compile(r'[^a-z0-9]')
_RE_NOISE = re.compile(r'[():\[\],_!?~-]+')
_RE_RES_K = re.compile(r'\b\d+[kK]\b')
_RE_STASHDB_URL = re.compile(r'stashdb\.org/scenes/([a-f0-9\-]+)')

# ---------------------------------------------------------------------------
# Cache state
# ---------------------------------------------------------------------------
CACHE_FILE = "cache.json"
STASH_CACHE = {}        # {"Site - Title": stashdb_dict_or_None}
STASH_PENDING = set()   # keys currently being queried (thread safety)
_cache_lock = threading.Lock()


def load_cache(debug_mode=False):
    """Load the StashDB cache from disk."""
    global STASH_CACHE
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                with _cache_lock:
                    STASH_CACHE.clear()
                    STASH_CACHE.update(data)
            if debug_mode:
                log.debug("Loaded %d entries from %s", len(STASH_CACHE), CACHE_FILE)
        except Exception as e:
            log.error("Error loading %s: %s", CACHE_FILE, e)


def save_cache():
    """Persist the cache to disk (thread-safe)."""
    with _cache_lock:
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(STASH_CACHE, f, indent=2)
        except Exception as e:
            log.error("Error saving to %s: %s", CACHE_FILE, e)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def _normalize(name: str) -> str:
    """Lowercase, strip spaces/punctuation for fuzzy studio comparison."""
    return _RE_NORMALIZE.sub('', name.lower())


def _studio_matches(expected_site: str, scene: dict) -> bool:
    """Check if a StashDB scene's studio loosely matches our expected site name."""
    studio = (scene.get("studio") or {}).get("name", "")
    if not studio or not expected_site:
        return False
    a = _normalize(expected_site)
    b = _normalize(studio)
    # One contains the other (handles "VRBangers" vs "VR Bangers", etc.)
    return a in b or b in a


def _title_ratio(title1: str, title2: str, performers: list) -> float:
    """Computes string similarity while ignoring known actors to prevent score inflation."""
    t1 = title1.lower()
    t2 = title2.lower()
    if performers:
        for p in sorted(performers, key=len, reverse=True):
            pl = p.lower()
            t1 = t1.replace(pl, '')
            t2 = t2.replace(pl, '')
    t1 = " ".join(t1.split())
    t2 = " ".join(t2.split())
    # If stripping actors leaves nothing (e.g. title WAS just the actor), fallback to normal comparison
    if not t1 and not t2:
        return difflib.SequenceMatcher(None, title1.lower(), title2.lower()).ratio()
    return difflib.SequenceMatcher(None, t1, t2).ratio()


def query_stashdb(sites, clean_title: str, parsed_performers=None, parsed_dates=None, *, api_key: str, log_buffer=None):
    """
    Query StashDB with caching.
    Uses title, site (parsed directly or from stashdb studio), and potentially fallback actor names.
    Returns the metadata dict on success, or None on failure.
    """
    ___log = BufferedLogger(log_buffer)
    if not api_key:
        return None

    # Guarantee sites is a list
    if isinstance(sites, str):
        sites = [sites] if sites else []
    elif not sites:
        sites = []

    if parsed_dates:
        ___log.debug("📅 Parsed dates: %s", parsed_dates)

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
    search_text = _RE_NOISE.sub(' ', search_title)

    # Strip resolution strings like '4K', '8K', '6k'
    search_text = _RE_RES_K.sub('', search_text)
    # Collapse multiple spaces
    search_text = " ".join(search_text.split())

    # Stremio clamps titles at ~255 chars; drop trailing incomplete word
    if len(search_text) > 200:
        last_space = search_text.rfind(' ')
        if last_space > 0:
            search_text = search_text[:last_space]
        ___log.debug("⚠️ Query truncated (dropped trailing word): '%s'", search_text)

    ___log.debug("Querying StashDB for title only: '%s'", search_text)
    url = "https://stashdb.org/graphql"
    headers = {"Content-Type": "application/json", "ApiKey": api_key}

    query = """
    query SearchScenes($term: String!) {
      searchScene(term: $term) {
        title
        date
        images { url }
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
        response = SESSION.post(url, json=payload, headers=headers, timeout=20)
        if response.status_code == 200:
            return response.json().get("data", {}).get("searchScene", [])
        else:
            ___log.warning("⚠️ StashDB HTTP Error %d: %s", response.status_code, response.text)
            return None

    try:
        def find_best_candidate(candidate_scenes, fallback_label=""):
            if not candidate_scenes: return None

            def any_studio_matches(s_dict):
                return any(_studio_matches(st, s_dict) for st in sites)

            def has_actor_overlap(s_dict):
                if not parsed_performers: return False
                stash_actors = []
                for p in s_dict.get("performers", []):
                    # Handle different StashDB GraphQL performer node structures safely
                    name = p.get("name") or p.get("performer", {}).get("name") if isinstance(p, dict) else p if isinstance(p, str) else None
                    if name: stash_actors.append(name.lower())

                if not stash_actors: return False
                for pp in parsed_performers:
                    pp_lower = pp.lower()
                    for sa in stash_actors:
                        if pp_lower in sa or sa in pp_lower:
                            return True
                return False

            def matching_actor_count(s_dict):
                if not parsed_performers: return 0
                stash_actors = []
                for p in s_dict.get("performers", []):
                    name = p.get("name") or p.get("performer", {}).get("name") if isinstance(p, dict) else p if isinstance(p, str) else None
                    if name: stash_actors.append(name.lower())

                if not stash_actors: return 0
                count = 0
                for pp in parsed_performers:
                    pp_lower = pp.lower()
                    for sa in stash_actors:
                        if pp_lower in sa or sa in pp_lower:
                            count += 1
                            break
                return count

            # Compilation Safety Check
            is_compilation = "compil" in scene_name.lower() or "collection" in scene_name.lower() or len(parsed_performers or []) >= 5
            if is_compilation:
                for s in candidate_scenes:
                    stash_date = s.get("date") or ""
                    ratio = difflib.SequenceMatcher(None, s.get("title", "").lower(), scene_name.lower()).ratio()

                    if parsed_dates and stash_date in parsed_dates and any_studio_matches(s):
                        # Require a strong 70% match to avoid grabbing Vol. 2 instead of Vol. 1 on the same date!
                        if ratio >= 0.70:
                            ___log.debug("✅ Compilation Exact Date+Studio match! %s: %s [%s]", fallback_label, s.get('title'), stash_date)
                            return s
                    if ratio >= 0.95 and any_studio_matches(s):
                        ___log.debug("✅ Compilation Exact Title+Studio match! %s: %s", fallback_label, s.get('title'))
                        return s

                # EXCEPTION: If a massive compilation query yields exactly 1 result, trust it!
                if len(candidate_scenes) == 1:
                    s = candidate_scenes[0]
                    # Since titles for compilations without dashes become just the actor list, [Primary] and [Studio+Title] act like actor searches
                    if fallback_label in ["[Primary]", "[Studio+Title]", "[Studio+Actors]", "[Actors Only]"]:
                        if len(parsed_performers or []) >= 3 and has_actor_overlap(s):
                            ___log.debug("✅ Compilation absolute trust for single Actor match! %s: %s", fallback_label, s.get('title'))
                            return s

                if candidate_scenes:
                    ___log.debug("❌ Compilation safety triggered for %s. Rejecting loose matches.", fallback_label)
                return None

            # Pass 1: Date check (Super high priority - with safety nets!)
            if parsed_dates:
                for s in candidate_scenes:
                    stash_date = s.get("date") or ""
                    if stash_date and stash_date in parsed_dates:
                        stash_title = s.get("title") or ""
                        ratio = _title_ratio(stash_title, scene_name, parsed_performers)

                        if any_studio_matches(s):
                            # Actor overlap alone is weak with 1 performer; require decent title ratio too
                            if (has_actor_overlap(s) and ratio >= 0.60) or ratio >= 0.75:
                                ___log.debug("✅ Exact Date+Studio match! %s (Ratio: %.2f): %s [%s]", fallback_label, ratio, stash_title, stash_date)
                                return s
                            # If no actors were parsed, we can be a bit more lenient but not 0.40
                            if not parsed_performers and ratio >= 0.60:
                                ___log.debug("✅ Exact Date+Studio match! %s (Ratio: %.2f): %s [%s]", fallback_label, ratio, stash_title, stash_date)
                                return s

                        # If the studio doesn't match but the date does, require a higher title match
                        if ratio >= 0.80:
                            ___log.debug("✅ Exact Date match! %s (Ratio: %.2f): %s [%s]", fallback_label, ratio, stash_title, stash_date)
                            return s

            # Pass 2: Standard Title/Studio match
            for s in candidate_scenes:
                stash_title = s.get("title") or ""
                ratio = _title_ratio(stash_title, scene_name, parsed_performers)

                if any_studio_matches(s) and ratio >= 0.85:
                    ___log.debug("✅ Title+Studio match %s (%.2f): %s", fallback_label, ratio, stash_title)
                    return s

                if ratio >= 0.90:
                    # Prevent blindly matching extremely short titles (e.g. 1-2 words like "Jessica Rex")
                    # if they don't have studio or actor confirmation.
                    if len(scene_name.split()) <= 3 and not any_studio_matches(s) and not has_actor_overlap(s):
                        ___log.debug("❌ Pure Title match rejected (too short/generic): '%s'", stash_title)
                        continue
                    ___log.debug("✅ Pure Title match %s (%.2f): %s", fallback_label, ratio, stash_title)
                    return s

            # Pass 3: Hyper-specific fallback trust (Requires exactly 1 result)
            if len(candidate_scenes) == 1:
                s = candidate_scenes[0]
                stash_title = s.get("title") or ""
                ratio = _title_ratio(stash_title, scene_name, parsed_performers)
                
                # Check for "Compilation" in the stashdb title or tags
                stash_is_compilation = "compil" in stash_title.lower() or "collection" in stash_title.lower()
                for t in s.get("tags", []):
                    if "compil" in t.get("name", "").lower():
                        stash_is_compilation = True

                # The new 3+ actor Studio match exception
                if not is_compilation and not stash_is_compilation:
                    if any_studio_matches(s) or "Studio" in fallback_label or fallback_label == "[Actors Only]":
                        if matching_actor_count(s) >= 3:
                            ___log.debug("✅ Studio + 3+ Actors exception trusted: '%s'", stash_title)
                            return s
                
                # Boost ratio if the parsed title is fully contained in the StashDB title
                is_substring = len(scene_name) >= 4 and scene_name.lower() in stash_title.lower()

                is_czech = "czech vr" in scene_name.lower()
                if is_czech and any_studio_matches(s):
                    ___log.debug("✅ Czech VR exception trusted: '%s'", stash_title)
                    return s

                # ABSOLUTE TRUST: Highly specific combinations that blindly bypass standard checks
                if fallback_label == "[Studio+Date+Actors]":
                    if has_actor_overlap(s) or ratio >= 0.40:
                        ___log.debug("✅ %s Single exact match trusted: '%s' (Ratio: %.2f)", fallback_label, stash_title, ratio)
                        return s

                if fallback_label == "[Actors Only]":
                    # Absolute trust on just actors is too risky for compilations.
                    has_date = parsed_dates and s.get("date") and s.get("date") in parsed_dates
                    if has_actor_overlap(s) and ((any_studio_matches(s) and has_date) or ratio >= 0.60 or is_substring):
                        ___log.debug("✅ %s Single exact match trusted: '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)
                        return s
                    ___log.debug("❌ %s Single result rejected (needs date/substring/ratio≥0.60): '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)

                # Trust highly specific Actor combinations if the studio matches OR the title is close
                # Studio+Actors / Studio+First Actor: tightened — one actor + studio is weak
                if fallback_label in ["[Studio+Actors]", "[Studio+First Actor]"]:
                    has_date = parsed_dates and s.get("date") and s.get("date") in parsed_dates
                    if has_actor_overlap(s) and (has_date or is_substring or ratio >= 0.50):
                        ___log.debug("✅ %s Single exact match trusted (Actor+extra confirmed): '%s' (Ratio: %.2f, Substring: %s, Date: %s)", fallback_label, stash_title, ratio, is_substring, has_date)
                        return s
                    ___log.debug("❌ %s Single result rejected (needs date/substring/ratio≥0.50): '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)

                elif fallback_label in ["[Title+Actors]", "[Title+First Actor]"]:
                    if has_actor_overlap(s) and (any_studio_matches(s) or ratio >= 0.60 or is_substring):
                        ___log.debug("✅ %s Single exact match trusted (Actor overlap verified): '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)
                        return s
                    if (any_studio_matches(s) and ratio >= 0.40) or ratio >= 0.60 or is_substring:
                        ___log.debug("✅ %s Single exact match trusted: '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)
                        return s
                    ___log.debug("❌ Single result found for %s but failed sanity check (Title Ratio: %.2f, Substring: %s)", fallback_label, ratio, is_substring)

                # Studio+Date: tightened — same studio on adjacent dates can be different videos
                elif fallback_label == "[Studio+Date]":
                    if (has_actor_overlap(s) and ratio >= 0.60) or ratio >= 0.75 or is_substring:
                        ___log.debug("✅ %s Single exact match trusted: '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)
                        return s
                    ___log.debug("❌ %s Single result rejected (needs actor+ratio≥0.60 or ratio≥0.75): '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)

                # Other standard fallbacks
                elif fallback_label in ["[Studio+Title]", "[Studio+Title+Actors]", "[Title+Date]"]:
                    if ratio >= 0.60 or is_substring or (has_actor_overlap(s) and ratio >= 0.40):
                        ___log.debug("✅ %s Single exact match trusted: '%s' (Ratio: %.2f, Substring: %s)", fallback_label, stash_title, ratio, is_substring)
                        return s
                    ___log.debug("❌ Single result found for %s but failed sanity check (Title Ratio: %.2f, Substring: %s)", fallback_label, ratio, is_substring)

                else:
                    ___log.debug("❌ Single result found for %s but failed sanity check (Title Ratio: %.2f, Substring: %s)", fallback_label, ratio, is_substring)

        # 1. Primary Search
        scenes = perform_query(search_text)
        best_match = find_best_candidate(scenes, "[Primary]")

        # 2. Studio + Title
        if not best_match and sites:
            for site in sites:
                site_title_term = f"{site} {search_text}".strip()
                site_title_term = _RE_NOISE.sub(' ', site_title_term)
                site_title_term = " ".join(site_title_term.split())
                ___log.debug("🔄 Retrying with Studio + Title: '%s'", site_title_term)
                best_match = find_best_candidate(perform_query(site_title_term), "[Studio+Title]")
                if best_match:
                    break

        # 3. Studio + Date (Incredibly precise if title is mangled but date is known)
        if not best_match and sites and parsed_dates:
            for site in sites:
                for date_str in parsed_dates:
                    site_date_term = f"{site} {date_str}".strip()
                    ___log.debug("🔄 Retrying with Studio + Date: '%s'", site_date_term)
                    best_match = find_best_candidate(perform_query(site_date_term), "[Studio+Date]")
                    if best_match:
                        break
                if best_match:
                    break

        # 4. Title + Date
        if not best_match and parsed_dates:
            for date_str in parsed_dates:
                date_term = f"{search_text} {date_str}".strip()
                ___log.debug("🔄 Retrying with Title + Date: '%s'", date_term)
                best_match = find_best_candidate(perform_query(date_term), "[Title+Date]")
                if best_match:
                    break

        # 4.5 Studio + Date + Actors
        if not best_match and sites and parsed_dates and actor_names:
            clean_actors = _RE_NOISE.sub(' ', actor_names)
            for site in sites:
                clean_site = _RE_NOISE.sub(' ', site)
                for date_str in parsed_dates:
                    site_date_actor_term = f"{clean_site} {date_str} {clean_actors}".strip()
                    site_date_actor_term = " ".join(site_date_actor_term.split())
                    ___log.debug("🔄 Retrying with Studio + Date + Actors: '%s'", site_date_actor_term)
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
                fallback_term = _RE_NOISE.sub(' ', fallback_term)
                fallback_term = " ".join(fallback_term.split())
                ___log.debug("🔄 Retrying with Title + Actors: '%s'", fallback_term)
                best_match = find_best_candidate(perform_query(fallback_term), "[Title+Actors]")

        # 5.5 Title + First Actor - NO STUDIO LOOP NEEDED
        if not best_match and parsed_performers and len(parsed_performers) > 1:
            first_actor = parsed_performers[0]
            first_actor_term = f"{search_text} {first_actor}".strip()
            first_actor_term = _RE_NOISE.sub(' ', first_actor_term)
            first_actor_term = " ".join(first_actor_term.split())
            ___log.debug("🔄 Retrying with Title + First Actor only: '%s'", first_actor_term)
            best_match = find_best_candidate(perform_query(first_actor_term), "[Title+First Actor]")

        # 6. Studio + Title + Actors
        if not best_match and sites and actor_names:
            for site in sites:
                missing_parts = [word for word in actor_names.split() if word.lower() not in search_text.lower()]
                site_term = f"{site} {search_text} {' '.join(missing_parts)}".strip()
                site_term = _RE_NOISE.sub(' ', site_term)
                site_term = " ".join(site_term.split())
                ___log.debug("🔄 Retrying with Studio + Title + Actors: '%s'", site_term)
                best_match = find_best_candidate(perform_query(site_term), "[Studio+Title+Actors]")
                if best_match:
                    break

        # 7. Studio + Actors Only (Bypasses bad titles completely)
        if not best_match and sites and actor_names:
            for site in sites:
                site_actor_term = f"{site} {actor_names}"
                site_actor_term = _RE_NOISE.sub(' ', site_actor_term)
                site_actor_term = " ".join(site_actor_term.split())
                ___log.debug("🔄 Retrying with Studio + Actors: '%s'", site_actor_term)
                best_match = find_best_candidate(perform_query(site_actor_term), "[Studio+Actors]")
                if best_match:
                    break

        # 7.5 Studio + First Actor Only (If bad title AND bad secondary actor alias)
        if not best_match and sites and parsed_performers and len(parsed_performers) > 1:
            for site in sites:
                first_actor = parsed_performers[0]
                site_first_actor_term = f"{site} {first_actor}".strip()
                site_first_actor_term = _RE_NOISE.sub(' ', site_first_actor_term)
                site_first_actor_term = " ".join(site_first_actor_term.split())
                ___log.debug("🔄 Retrying with Studio + First Actor only: '%s'", site_first_actor_term)
                best_match = find_best_candidate(perform_query(site_first_actor_term), "[Studio+First Actor]")
                if best_match:
                    break

        # 8. Actors Only
        if not best_match and actor_names and len(actor_names.split()) >= 3:
            actor_only_term = _RE_NOISE.sub(' ', actor_names)
            actor_only_term = " ".join(actor_only_term.split())
            ___log.debug("🔄 Retrying with ONLY actor names: '%s'", actor_only_term)
            best_match = find_best_candidate(perform_query(actor_only_term), "[Actors Only]")

        if best_match:
            with _cache_lock:
                STASH_CACHE[cache_key] = best_match
            save_cache()
            ___log.debug("🎉 StashDB saved: %s (Studio: %s)\n\n", best_match.get('title'), best_match.get('studio', {}).get('name'))
            return best_match
        else:
            ___log.debug("❌ No suitable StashDB results found.\n\n")
            with _cache_lock:
                STASH_CACHE[cache_key] = None
            save_cache()
            return None

    except requests.exceptions.Timeout:
        ___log.warning("⚠️ StashDB Query Timeout for '%s'. Will retry next session.", search_text)
    except Exception as e:
        ___log.warning("⚠️ StashDB Query Exception: %s", e)
    finally:
        STASH_PENDING.discard(cache_key)

    return None


def query_stashdb_by_id(scene_id: str, *, api_key: str):
    """Fetch a single scene from StashDB by its UUID."""
    if not api_key:
        return None

    log.debug("Querying StashDB by ID: '%s'", scene_id)
    url = "https://stashdb.org/graphql"
    headers = {"Content-Type": "application/json", "ApiKey": api_key}

    query = """
    query FindScene($id: ID!) {
      findScene(id: $id) {
        title
        date
        images { url }
        studio { name }
        performers { performer { name } }
        tags { name }
      }
    }
    """

    payload = {"query": query, "variables": {"id": scene_id}}

    try:
        response = SESSION.post(url, json=payload, headers=headers, timeout=20)
        if response.status_code == 200:
            scene = response.json().get("data", {}).get("findScene")
            if scene:
                log.debug("✅ StashDB match found by ID: %s", scene.get('title'))
                return scene
            else:
                log.debug("❌ No StashDB results found for ID.")
        else:
            log.warning("⚠️ StashDB HTTP Error %d: %s", response.status_code, response.text)
    except Exception as e:
        log.warning("⚠️ StashDB Query Exception: %s", e)

    return None


# ---------------------------------------------------------------------------
# Background worker  (concurrent with rate-limiting)
# ---------------------------------------------------------------------------
_RATE_LIMIT = 0.75  # seconds between StashDB requests


def fill_stash_cache_background(library_items, parse_filename, *, api_key, debug_mode):
    """
    Pre-fetch StashDB data for every video in the library that isn't cached yet.
    Uses a ThreadPoolExecutor for concurrency while respecting the 0.75 s rate limit.
    """
    if not api_key:
        return

    to_fetch = []
    for item in library_items:
        raw_name = item.get("name", "")
        if not raw_name:
            continue
        parsed = item.get("_parsed") or parse_filename(raw_name)
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

    if not unique_fetches:
        return

    total = len(unique_fetches)
    log.info("🔄 StashDB: Found %d new videos to cache. Starting concurrent background fetch...", total)

    def _fetch_one(idx, studios, title, performers, possible_dates, raw_name):
        """Worker that queries StashDB for a single item."""
        buf = []
        if debug_mode:
            buf.append(f"Background Fetch [{idx}/{total}]: {raw_name}")
        query_stashdb(list(studios), title, parsed_performers=performers, parsed_dates=possible_dates, api_key=api_key, log_buffer=buf)
        if buf:
            log.debug("\n  ".join(buf))

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        for i, ((studios, title), (performers, possible_dates, raw_name)) in enumerate(unique_fetches.items(), 1):
            future = pool.submit(_fetch_one, i, studios, title, performers, possible_dates, raw_name)
            futures.append(future)
            # Rate-limit submission so we don't blast StashDB
            time.sleep(_RATE_LIMIT)

        # Wait for all to complete
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log.warning("Background StashDB fetch error: %s", e)

    log.info("✅ StashDB: Background cache filling complete!")

# -*- coding: utf-8 -*-
"""
Filename parsing logic for extracting titles, tags, performers, and site names
from various video filename formats.
"""

import datetime
import re

# ---------------------------------------------------------------------------
# Pre-compiled regex patterns  (biggest CPU win for 200+ video libraries)
# ---------------------------------------------------------------------------

# parse_filename top-level
_RE_SCENE_RELEASE = re.compile(
    r'^([A-Za-z]+(?:[A-Z][a-z]+)*)\s+'       # SiteName (CamelCase)
    r'(\d{2})\s+(\d{2})\s+(\d{2})\s+'        # YY MM DD
    r'(.+?)\s+(?:XXX|\d{3,4}p)\b',            # everything until XXX or resolution
    re.IGNORECASE,
)
_RE_CZECH_VR = re.compile(r'(Czech\s*(?:Fetish\s*)?VR)\s*[-_]?\s*(\d+)', re.IGNORECASE)
_RE_3D_NOISE = re.compile(r'\b(?:version\s*3D|3D)\b', re.IGNORECASE)
_RE_MULTI_SPACES = re.compile(r'\s{2,}')

# _parse_bracketed
_RE_LEADING_BRACKET = re.compile(r'^\[([^\]]+)\]')
_RE_ALL_BRACKETS = re.compile(r'\[([^\]]+)(?:\]|$)')
_RE_STRIP_BRACKETS = re.compile(r'\[([^\]]*)\]')
_RE_PAREN_GROUP = re.compile(r'\(([^)]*)\)')
_RE_FIRST_BRACKET = re.compile(r'\[')
_RE_DOT_COM = re.compile(r'\.com$', re.IGNORECASE)

# _parse_scene_release
_RE_CAMEL_SPLIT = re.compile(r'([a-z])([A-Z])')
_RE_REMASTERED = re.compile(r'\b(REMASTERED|Remastered)\b')
_RE_TAG_TOKENS = re.compile(r'(VR\d*|VR180|\d{3,4}p|MP4|SideBySide|[A-Z]{2,})')
_RE_BRACKET_TAGS = re.compile(r'\[([^\]]+)\]')

# _parse_dash_separated
_RE_TRAILING_RES = re.compile(r'\s+\d{3,4}p\s*$')

# _clean_tag
_RE_YEAR_TAG = re.compile(r'^\d{4}\s*(г\.?)?$')
_RE_DATE_TAG = re.compile(r'^\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}$')
_RE_RES_TAG = re.compile(r'^\d{3,4}p$')

# _clean_title_from_parens
_RE_TITLE_DATE = re.compile(r'^\d{1,2}[./]\d{1,2}[./]\d{2,4}$')
_RE_TITLE_ID = re.compile(r'^\d{4,}$')
_RE_TITLE_YEAR = re.compile(r'^\d{4}\s*(г\.?|year)?$')

# _extract_performers
_RE_STRIP_PIPE = re.compile(r'\|\|.*$')
_RE_SPLIT_PERFORMERS = re.compile(r',\s*|\s+&\s+')
_RE_RES_TOKEN = re.compile(r'^\d+p$')

# _extract_dates
_RE_DATE_YMD = re.compile(r'\b(20\d{2})[-. /](0[1-9]|1[0-2])[-. /](0[1-9]|[12]\d|3[01])\b')
_RE_DATE_DMY = re.compile(r'\b(0[1-9]|[12]\d|3[01])[-. /](0[1-9]|[12]\d|3[01])[-. /](20\d{2})\b')
_RE_DATE_SCENE = re.compile(r'\b(\d{2})\s+(0[1-9]|1[0-2])\s+(0[1-9]|[12]\d|3[01])\b')

# Shared (used in multiple helpers)
_RE_NOISE_CHARS = re.compile(r'[():\[\],_!?~-]+')
_RE_RES_K = re.compile(r'\b\d+[kK]\b')
_RE_POOR_TITLE_NUMERIC = re.compile(r'^\d+$')


def parse_filename(filename: str) -> dict:
    """
    Extract title and tags from a filename string.
    """
    filename = filename.strip()
    if not filename:
        return {"title": "", "tags": [], "site": "", "performers": []}

    res = None

    # FORMAT 1 & 2:  Starts with [Site]
    if filename.startswith("["):
        res = _parse_bracketed(filename)
    else:
        # FORMAT 3:  Scene-release style
        scene_match = _RE_SCENE_RELEASE.match(filename)
        if scene_match:
            res = _parse_scene_release(filename, scene_match)
        # FORMAT 4:  Simple dash-separated
        elif " - " in filename:
            res = _parse_dash_separated(filename)
        # Fallback
        else:
            res = {"title": filename, "tags": [], "site": "", "performers": []}

    # --- Czech VR / Czech Fetish VR Override ---
    czech_match = _RE_CZECH_VR.search(filename)
    if czech_match:
        is_fetish = "fetish" in czech_match.group(1).lower()
        prefix = "Czech Fetish VR" if is_fetish else "Czech VR"
        res["title"] = f"{prefix} {czech_match.group(2)}"
        res["site"] = prefix

    if res:
        # Guarantee a 'studios' list exists for all formats
        if "studios" not in res:
            res["studios"] = [res["site"]] if res.get("site") else []

        # Normalize across all parsed studios
        normalized_studios = []
        for s in res["studios"]:
            site_lower = s.lower().replace(" ", "")
            if "naughtyamerica" in site_lower:
                normalized_studios.append("Naughty America")
            else:
                normalized_studios.append(s)

        # Eliminate duplicates while preserving order
        res["studios"] = list(dict.fromkeys(normalized_studios))
        res["site"] = res["studios"][0] if res["studios"] else ""
        res["possible_dates"] = _extract_dates(filename)

        # Strip 3D / version 3D noise from titles
        if res.get("title"):
            res["title"] = _RE_3D_NOISE.sub('', res["title"]).strip()
            res["title"] = _RE_MULTI_SPACES.sub(' ', res["title"]).strip()

    return res


def _parse_bracketed(filename: str) -> dict:
    """
    Parse filenames that start with [Site] and contain bracket-delimited
    metadata sections.
    """
    # --- Extract site name(s) from leading bracket(s) ---
    site_match = _RE_LEADING_BRACKET.match(filename)
    raw_site = site_match.group(1).strip() if site_match else ""

    studios = []
    if raw_site:
        for s in raw_site.split(" / "):
            studios.append(_RE_DOT_COM.sub('', s.strip()))

    site = studios[0] if studios else ""

    # Remove the leading [site] bracket to work with the rest
    rest = filename[site_match.end():].strip() if site_match else filename

    # --- Extract tag brackets (also match unclosed trailing brackets) ---
    all_brackets = _RE_ALL_BRACKETS.findall(rest)

    tags = []
    for bracket_content in all_brackets:
        items = [item.strip() for item in bracket_content.split(",")]
        for item in items:
            cleaned = _clean_tag(item)
            if cleaned:
                tags.append(cleaned)

    # --- Strip all square-bracket sections to get the "core text" ---
    core = _RE_STRIP_BRACKETS.sub('', rest).strip()

    # --- Handle || separators (NaughtyAmerica pattern) ---
    if "||" in core:
        before_pipe, after_pipe = core.split("||", 1)
        performers = _extract_performers(before_pipe.strip())
        paren_match = _RE_PAREN_GROUP.search(after_pipe)
        if paren_match:
            raw_title = paren_match.group(1).strip()
            title = _clean_title_from_parens(raw_title)
        else:
            title = after_pipe.strip()
        if not title:
            title = ", ".join(performers) if performers else site
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

    paren_match = _RE_PAREN_GROUP.search(core)
    if paren_match:
        raw_title = paren_match.group(1).strip()
        title = _clean_title_from_parens(raw_title)
        before_paren = core[:paren_match.start()].strip()

        if " - " in before_paren:
            parts = before_paren.split(" - ", 1)
            performers = _extract_performers(parts[0])
            title = parts[1].strip() or title
        else:
            is_poor_title = (
                not title
                or _RE_POOR_TITLE_NUMERIC.match(title)
                or title.lower() in ("remastered", "a xxx parody", "a porn parody",
                                      "vr porn parody", "cgi")
            )
            if is_poor_title and before_paren:
                title = before_paren
            else:
                performers = _extract_performers(before_paren)
                if performers and title:
                    title = f"{', '.join(performers)} - {title}"
    else:
        first_bracket = _RE_FIRST_BRACKET.search(rest)
        if first_bracket:
            before_bracket = rest[:first_bracket.start()].strip()
        else:
            before_bracket = rest.strip()

        if " - " in before_bracket:
            parts = before_bracket.split(" - ", 1)
            performers = _extract_performers(parts[0])
            title = parts[1].strip()
        else:
            # If there's no dash but lots of commas, it's a list of performers, not a title!
            if "," in before_bracket:
                performers = _extract_performers(before_bracket)
                title = ""
            else:
                title = before_bracket

    if not title and performers:
        title = ", ".join(performers)
    if not title:
        title = site or filename[:80]

    tags = performers + tags

    return {
        "title": title.strip(),
        "tags": tags,
        "site": site,
        "performers": performers,
    }


def _parse_scene_release(filename: str, match: re.Match) -> dict:
    """Parse scene-release style filenames."""
    site_raw = match.group(1)
    site = _RE_CAMEL_SPLIT.sub(r'\1 \2', site_raw)

    performer_and_title = match.group(5).strip()
    clean_name = _RE_REMASTERED.sub('', performer_and_title).strip()

    after_xxx = filename[match.end():]
    tags = []
    tag_tokens = _RE_TAG_TOKENS.findall(after_xxx)
    for t in tag_tokens:
        if t not in ("XXX", "MP4"):
            tags.append(t)

    bracket_tags = _RE_BRACKET_TAGS.findall(after_xxx)
    for bt in bracket_tags:
        cleaned = bt.strip()
        if cleaned and cleaned not in tags:
            tags.append(cleaned)

    return {
        "title": clean_name,
        "tags": tags,
        "site": site,
        "performers": [],
    }


def _parse_dash_separated(filename: str) -> dict:
    """Parse dash-separated filenames."""
    parts = filename.split(" - ")
    site = parts[0].strip() if len(parts) >= 1 else ""
    site = _RE_DOT_COM.sub('', site)
    performers = []
    title = ""

    if len(parts) >= 3:
        performers = _extract_performers(parts[1])
        title_raw = " - ".join(parts[2:])
        title = _RE_TRAILING_RES.sub('', title_raw).strip()
    elif len(parts) == 2:
        title = _RE_TRAILING_RES.sub('', parts[1]).strip()
        # If the first part contains a comma or '&', it's highly likely a performer list
        if "," in parts[0] or "&" in parts[0]:
            performers = _extract_performers(parts[0])
            site = ""

    tags = []
    last_part = parts[-1] if parts else ""
    if "," in last_part:
        items = [i.strip() for i in last_part.split(",")]
        for item in items:
            cleaned = _clean_tag(item)
            if cleaned:
                tags.append(cleaned)

    tags = performers + tags

    return {
        "title": title or filename,
        "tags": tags,
        "site": site,
        "performers": performers,
    }


def _clean_title_from_parens(raw: str) -> str:
    """Clean a title extracted from parentheses. Removes dates, IDs, noise."""
    # Strip descriptors after a dash (e.g., "Asian Delight - Group Sex..." -> "Asian Delight")
    if " - " in raw:
        raw = raw.split(" - ")[0].strip()

    segments = [s.strip() for s in raw.split(" / ")]
    if len(segments) == 1:
        segments = [s.strip() for s in raw.split(" | ")]

    title_parts = []
    for seg in segments:
        if _RE_TITLE_DATE.match(seg):
            continue
        if _RE_TITLE_ID.match(seg):
            continue
        if _RE_TITLE_YEAR.match(seg):
            continue
        title_parts.append(seg)

    title = " / ".join(title_parts) if title_parts else raw
    return title.strip()


def _extract_performers(text: str) -> list:
    """Extract performer names from a text segment."""
    text = text.strip()
    if not text:
        return []

    text = _RE_STRIP_PIPE.sub('', text)
    text = text.strip()

    names = _RE_SPLIT_PERFORMERS.split(text)

    performers = []
    for name in names:
        name = name.strip()
        if not name:
            continue
        if _RE_RES_TOKEN.match(name):
            continue
        if name.upper() in ("VR", "POV", "SBS", "4K", "8K", "XXX"):
            continue
        performers.append(name)

    return performers


def _clean_tag(raw: str) -> str:
    """Clean a single tag string. Returns empty string if should be skipped."""
    tag = raw.strip()
    if not tag:
        return ""
    if _RE_YEAR_TAG.match(tag):
        return ""
    if _RE_DATE_TAG.match(tag):
        return ""
    if _RE_RES_TAG.match(tag):
        return ""
    if tag.upper() in ("SITERIP", "SIDEBYIDE"):
        return ""
    return tag

def _extract_dates(text: str) -> list:
    """Extract possible YYYY-MM-DD dates from a string."""
    possible_dates = []

    # 1. YYYY-MM-DD (or YYYY/MM/DD, YYYY.MM.DD)
    match = _RE_DATE_YMD.search(text)
    if match:
        possible_dates.append(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")

    # 2. MM.DD.YYYY or DD.MM.YYYY
    matches = _RE_DATE_DMY.findall(text)
    for p1, p2, y in matches:
        # Try US format first: MM-DD-YYYY
        if int(p1) <= 12:
            possible_dates.append(f"{y}-{p1}-{p2}")
        # Try Euro format: DD-MM-YYYY
        if int(p2) <= 12 and p1 != p2:
            possible_dates.append(f"{y}-{p2}-{p1}")

    # 3. Scene release format: YY MM DD (e.g., 24 01 17)
    scene_match = _RE_DATE_SCENE.search(text)
    if scene_match:
        yy, mm, dd = scene_match.groups()
        if 10 <= int(yy) <= 99:
            possible_dates.append(f"20{yy}-{mm}-{dd}")

    # Fuzz the dates by +/- 1 day to handle StashDB timezone differences
    expanded_dates = set(possible_dates)
    for d in possible_dates:
        try:
            dt = datetime.datetime.strptime(d, "%Y-%m-%d")
            expanded_dates.add((dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            expanded_dates.add((dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        except ValueError:
            pass

    return list(expanded_dates)
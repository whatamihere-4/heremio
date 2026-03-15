"""
Filename parsing logic for extracting titles, tags, performers, and site names
from various video filename formats.
"""

import re


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

    # FORMAT 1 & 2:  Starts with [Site]
    if filename.startswith("["):
        return _parse_bracketed(filename)

    # FORMAT 3:  Scene-release style  e.g.
    #   DirtyWivesClub 24 01 17 Jessica Rex REMASTERED XXX VR180 ...
    scene_match = re.match(
        r'^([A-Za-z]+(?:[A-Z][a-z]+)*)\s+'       # SiteName (CamelCase)
        r'(\d{2})\s+(\d{2})\s+(\d{2})\s+'        # YY MM DD
        r'(.+?)\s+XXX\b',                         # everything until XXX
        filename
    )
    if scene_match:
        return _parse_scene_release(filename, scene_match)

    # FORMAT 4:  Simple dash-separated
    #   WankzVR - Abella Danger, Yhivi - Director's Cut - Threesomes, ...
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
    if "||" in core:
        before_pipe, after_pipe = core.split("||", 1)
        performers = _extract_performers(before_pipe.strip())
        paren_match = re.search(r'\(([^)]*)\)', after_pipe)
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

    paren_match = re.search(r'\(([^)]*)\)', core)
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
                or re.match(r'^\d+$', title)
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
    site = re.sub(r'([a-z])([A-Z])', r'\1 \2', site_raw)

    performer_and_title = match.group(5).strip()
    clean_name = re.sub(r'\b(REMASTERED|Remastered)\b', '', performer_and_title).strip()

    after_xxx = filename[match.end():]
    tags = []
    tag_tokens = re.findall(r'(VR\d*|VR180|\d{3,4}p|MP4|SideBySide|[A-Z]{2,})', after_xxx)
    for t in tag_tokens:
        if t not in ("XXX", "MP4"):
            tags.append(t)

    bracket_tags = re.findall(r'\[([^\]]+)\]', after_xxx)
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
    site = re.sub(r'\.com$', '', site, flags=re.IGNORECASE)
    performers = []
    title = ""

    if len(parts) >= 3:
        performers = _extract_performers(parts[1])
        title_raw = " - ".join(parts[2:])
        title = re.sub(r'\s+\d{3,4}p\s*$', '', title_raw).strip()
    elif len(parts) == 2:
        title = re.sub(r'\s+\d{3,4}p\s*$', '', parts[1]).strip()

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
    segments = [s.strip() for s in raw.split(" / ")]
    if len(segments) == 1:
        segments = [s.strip() for s in raw.split(" | ")]

    title_parts = []
    for seg in segments:
        if re.match(r'^\d{1,2}[./]\d{1,2}[./]\d{2,4}$', seg):
            continue
        if re.match(r'^\d{4,}$', seg):
            continue
        if re.match(r'^\d{4}\s*(г\.?|year)?$', seg):
            continue
        title_parts.append(seg)

    title = " / ".join(title_parts) if title_parts else raw
    return title.strip()


def _extract_performers(text: str) -> list:
    """Extract performer names from a text segment."""
    text = text.strip()
    if not text:
        return []

    text = re.sub(r'\|\|.*$', '', text)
    text = text.strip()

    names = re.split(r',\s*|\s+&\s+', text)

    performers = []
    for name in names:
        name = name.strip()
        if not name:
            continue
        if re.match(r'^\d+p$', name):
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
    if re.match(r'^\d{4}\s*(г\.?)?$', tag):
        return ""
    if re.match(r'^\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}$', tag):
        return ""
    if re.match(r'^\d{3,4}p$', tag):
        return ""
    if tag.upper() in ("SITERIP", "SIDEBYIDE"):
        return ""
    return tag

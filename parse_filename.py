"""
Filename parsing logic for extracting titles, tags, performers, and site names
from various video filename formats.
"""

import re


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
        scene_match = re.match(
            r'^([A-Za-z]+(?:[A-Z][a-z]+)*)\s+'       # SiteName (CamelCase)
            r'(\d{2})\s+(\d{2})\s+(\d{2})\s+'        # YY MM DD
            r'(.+?)\s+(?:XXX|\d{3,4}p)\b',            # everything until XXX or resolution
            filename,
            re.IGNORECASE
        )
        if scene_match:
            res = _parse_scene_release(filename, scene_match)
        # FORMAT 4:  Simple dash-separated
        elif " - " in filename:
            res = _parse_dash_separated(filename)
        # Fallback
        else:
            res = {"title": filename, "tags": [], "site": "", "performers": []}

    # --- Czech VR / Czech Fetish VR Override ---
    # Looks for "Czech VR 123", "CzechFetishVR-45", etc.
    czech_match = re.search(r'(Czech\s*(?:Fetish\s*)?VR)\s*[-_]?\s*(\d+)', filename, re.IGNORECASE)
    if czech_match:
        is_fetish = "fetish" in czech_match.group(1).lower()
        prefix = "Czech Fetish VR" if is_fetish else "Czech VR"
        res["title"] = f"{prefix} {czech_match.group(2)}"
        res["site"] = prefix
        # We leave the parsed tags and performers intact but overwrite the title/site
    
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
            res["title"] = re.sub(r'\b(?:version\s*3D|3D)\b', '', res["title"], flags=re.IGNORECASE).strip()
            res["title"] = re.sub(r'\s{2,}', ' ', res["title"]).strip()

    return res


def _parse_bracketed(filename: str) -> dict:
    """
    Parse filenames that start with [Site] and contain bracket-delimited
    metadata sections.
    """
    # --- Extract site name(s) from leading bracket(s) ---
    # --- Extract site name(s) from leading bracket(s) ---
    site_match = re.match(r'^\[([^\]]+)\]', filename)
    raw_site = site_match.group(1).strip() if site_match else ""
    
    studios = []
    if raw_site:
        for s in raw_site.split(" / "):
            studios.append(re.sub(r'\.com$', '', s.strip(), flags=re.IGNORECASE))
            
    site = studios[0] if studios else ""

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

def _extract_dates(text: str) -> list:
    """Extract possible YYYY-MM-DD dates from a string."""
    possible_dates = []

    # 1. YYYY-MM-DD (or YYYY/MM/DD, YYYY.MM.DD)
    match = re.search(r'\b(20\d{2})[-. /](0[1-9]|1[0-2])[-. /](0[1-9]|[12]\d|3[01])\b', text)
    if match:
        possible_dates.append(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")

    # 2. MM.DD.YYYY or DD.MM.YYYY
    matches = re.findall(r'\b(0[1-9]|[12]\d|3[01])[-. /](0[1-9]|[12]\d|3[01])[-. /](20\d{2})\b', text)
    for p1, p2, y in matches:
        # Try US format first: MM-DD-YYYY
        if int(p1) <= 12:
            possible_dates.append(f"{y}-{p1}-{p2}")
        # Try Euro format: DD-MM-YYYY
        if int(p2) <= 12 and p1 != p2:
            possible_dates.append(f"{y}-{p2}-{p1}")

    # 3. Scene release format: YY MM DD (e.g., 24 01 17)
    scene_match = re.search(r'\b(\d{2})\s+(0[1-9]|1[0-2])\s+(0[1-9]|[12]\d|3[01])\b', text)
    if scene_match:
        yy, mm, dd = scene_match.groups()
        if 10 <= int(yy) <= 99:
            possible_dates.append(f"20{yy}-{mm}-{dd}")

    # Fuzz the dates by +/- 1 day to handle StashDB timezone differences
    import datetime
    expanded_dates = set(possible_dates)
    for d in possible_dates:
        try:
            dt = datetime.datetime.strptime(d, "%Y-%m-%d")
            expanded_dates.add((dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            expanded_dates.add((dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        except ValueError:
            pass

    return list(expanded_dates)
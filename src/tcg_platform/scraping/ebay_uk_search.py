"""UK-specific eBay search-page parser.

Parses the UK eBay search results page to extract (item_url, sold_date) pairs.
The sold date comes from the green 'Sold  D Mon YYYY' span in each card.
"""
import re
from datetime import datetime, timedelta, timezone
from html import unescape

UK_SEARCH_BASE = (
    "https://www.ebay.co.uk/sch/i.html"
    "?_nkw=One+Piece+TCG+PSA+10"
    "&_sacat=0"
    "&_from=R40"
    "&LH_PrefLoc=1"
    "&Language=English"
    "&_dcat=183454"
    "&rt=nc"
    "&LH_Sold=1"
)


def search_url_for_page(page: int) -> str:
    return f"{UK_SEARCH_BASE}&_pgn={page}"

# Match the date text in a Sold span: "Sold  3 Jun 2026"
# Note: double space after "Sold", NO period after the day.
_DATE_RE = re.compile(
    r"Sold\s+(\d{1,2})\s+(\w+)\s+(\d{4})"
)
_TODAY_RE = re.compile(r"Sold\s+Today")
_YESTERDAY_RE = re.compile(r"Sold\s+Yesterday")

_MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    # Short month names appear in real HTML (e.g. "Jun")
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Real UK search-page structure:
#   <span class="su-styled-text positive default" aria-label="Sold item">Sold  2 Jun 2026</span>
_DATE_SPAN_RE = re.compile(
    r'<span class="su-styled-text positive default" aria-label="Sold item">\s*([^<]+?)\s*</span>',
    re.DOTALL,
)

# Item URL inside an s-card__link anchor on the same page.
_ITEM_URL_RE = re.compile(
    r'<a class="s-card__link"[^>]*href="(https://www\.ebay\.co\.uk/itm/\d+[^"]+)"'
)


def _parse_date_text(text: str) -> str:
    text = text.strip()
    if _TODAY_RE.search(text):
        return datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    if _YESTERDAY_RE.search(text):
        return (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    m = _DATE_RE.search(text)
    if not m:
        return ""
    day, month_name, year = m.groups()
    month = _MONTHS_EN.get(month_name.lower())
    if not month:
        return ""
    return f"{year}-{month:02d}-{int(day):02d}"


def _strip_query(url: str) -> str:
    return re.sub(r"\?.*", "", url)


def parse_ebay_uk_search_page(html: str) -> list[tuple[str, str]]:
    if not html:
        return []
    html = unescape(html)

    date_spans = list(_DATE_SPAN_RE.finditer(html))
    item_urls = list(_ITEM_URL_RE.finditer(html))

    pairs: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    for date_m in date_spans:
        sold_date = _parse_date_text(date_m.group(1))
        if not sold_date:
            continue
        url_m = next(
            (u for u in item_urls if u.start() > date_m.end()),
            None,
        )
        if not url_m:
            continue
        raw_url = url_m.group(1)
        clean_url = _strip_query(raw_url)
        m = re.search(r"/itm/(\d+)", clean_url)
        if not m:
            continue
        item_id = m.group(1)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        pairs.append((clean_url, sold_date))

    return pairs

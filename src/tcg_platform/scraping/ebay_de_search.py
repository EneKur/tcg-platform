"""DE-specific eBay search-page parser.

Parses the German eBay search results page to extract (item_url, sold_date) pairs.
The sold date comes from the green 'Verkauft  D. Mon YYYY' span in each card.
"""
import re
from datetime import datetime, timedelta, timezone
from html import unescape

DE_SEARCH_URL = (
    "https://www.ebay.de/sch/i.html"
    "?_nkw=One+Piece+TCG+PSA+10"
    "&_sacat=0"
    "&_from=R40"
    "&Sprache=Englisch"
    "&_dcat=183454"
    "&LH_PrefLoc=1"
    "&rt=nc"
    "&LH_Sold=1"
)

# Match the date text in a Verkauft span: "Verkauft  3. Jun 2026"
# Note: double space after "Verkauft" and period after the day.
_DATE_RE = re.compile(
    r"Verkauft\s+(\d{1,2})\.\s+(\w+)\s+(\d{4})"
)
_HEUTE_RE = re.compile(r"Verkauft\s+Heute")
_GESTERN_RE = re.compile(r"Verkauft\s+Gestern")

_MONTHS_DE = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8,
    "september": 9, "oktober": 10, "november": 11, "dezember": 12,
    # Short month names also appear in real HTML (e.g. "Mai", "Jun")
    "jan": 1, "feb": 2, "mär": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "okt": 10, "nov": 11, "dez": 12,
}

# Real DE search-page structure:
#   <span class="su-styled-text positive default" aria-label="Verkaufter Artikel">Verkauft  3. Jun 2026</span>
_DATE_SPAN_RE = re.compile(
    r'<span class="su-styled-text positive default" aria-label="Verkaufter Artikel">\s*([^<]+?)\s*</span>',
    re.DOTALL,
)

# Item URL inside an s-card__link anchor on the same page. Anchoring on the
# class avoids picking up promo/related-item `ebay.com` hrefs elsewhere.
_ITEM_URL_RE = re.compile(
    r'<a class="s-card__link"[^>]*href="(https://www\.ebay\.de/itm/\d+[^"]+)"'
)


def _parse_date_text(text: str) -> str:
    """Parse a Verkauft-span text into YYYY-MM-DD. Returns '' on failure."""
    text = text.strip()
    if _HEUTE_RE.search(text):
        return datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    if _GESTERN_RE.search(text):
        return (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    m = _DATE_RE.search(text)
    if not m:
        return ""
    day, month_name, year = m.groups()
    month = _MONTHS_DE.get(month_name.lower())
    if not month:
        return ""
    return f"{year}-{month:02d}-{int(day):02d}"


def _strip_query(url: str) -> str:
    """Drop the ?... query string from a URL."""
    return re.sub(r"\?.*", "", url)


def parse_ebay_de_search_page(html: str) -> list[tuple[str, str]]:
    """Parse a DE eBay search results HTML page.

    Returns a list of (item_url, sold_date) pairs, deduped by item_id.
    sold_date is YYYY-MM-DD or '' if unparseable.
    """
    if not html:
        return []
    html = unescape(html)

    # Find all date spans and item URLs with their positions.
    date_spans = list(_DATE_SPAN_RE.finditer(html))
    item_urls = list(_ITEM_URL_RE.finditer(html))

    pairs: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    # For each date span, find the next item URL after it (within the same card).
    for date_m in date_spans:
        date_text = date_m.group(1)
        sold_date = _parse_date_text(date_text)
        if not sold_date:
            continue

        # Find the next item URL after the date span's end.
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

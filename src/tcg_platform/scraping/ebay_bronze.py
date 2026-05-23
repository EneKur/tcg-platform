import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Card ID extraction
# ---------------------------------------------------------------------------
SET_CODE_RE = re.compile(r"\b(OP\d+-\d+|EB\d+-\d+|OP\d+|EB\d+|ST\d+|PRB\d+)\b")

_STRIP_LEADING_SEP_RE = re.compile(r"^[-_\s]+")
_VERSION_INVALID_RE = re.compile(r"[^a-zA-Z0-9_]")
_PRICE_CLEAN_RE = re.compile(r"[^\d.,]")
_SOLD_DATE_RE1 = re.compile(r"Sold\s+\w+,?\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})", re.IGNORECASE)
_SOLD_DATE_RE2 = re.compile(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", re.IGNORECASE)
_NORM_PAREN_RE = re.compile(r"\s*\(.*?\)")
_NORM_BRACKET_RE = re.compile(r"\s*\[.*?\]")


def extract_card_id(title: str) -> str:
    """Extract the best card_id from a listing title.

    Priority:
      1. OP###-### (e.g. OP15-001)
      2. EB###-### (e.g. EB01-001)
      3. OP### (e.g. OP15)
      4. EB### (e.g. EB01)
      5. ST### / PRB###

    Returns empty string if no valid card_id found.
    """
    matches = SET_CODE_RE.findall(title)
    if not matches:
        return ""

    # Prefer multi-digit set+number (OP15-001 > OP15)
    for m in matches:
        if "-" in m:
            return m  # highest priority: OP###-### or EB###-###
    # Fall back to first OP/EB without dash (ST/PR also possible)
    for m in matches:
        if m.startswith(("OP", "EB")):
            return m
    return matches[0] if matches else ""


def extract_card_version(title: str, card_id: str) -> str:
    """Extract version suffix after the card_id in the title.

    E.g. "OP15-001 Alternative Art" → "_Alternative_Art"
         "OP15-001" → ""
    """
    if not card_id:
        return ""
    # Find card_id in title and take everything after it
    idx = title.find(card_id)
    if idx == -1:
        return ""
    remainder = title[idx + len(card_id):].strip()
    if not remainder:
        return ""
    # Strip leading separators
    remainder = _STRIP_LEADING_SEP_RE.sub("", remainder)
    if not remainder:
        return ""
    # Normalize: spaces to underscore, keep alphanumeric + underscore
    version = remainder.replace(" ", "_")
    version = _VERSION_INVALID_RE.sub("", version)
    return f"_{version}" if version else ""


# ---------------------------------------------------------------------------
# Currency detection
# ---------------------------------------------------------------------------
def detect_currency(price_text: str, url: str) -> str:
    """Detect currency from price text and URL domain.

    eBay domain → default currency:
      ebay.com.au → AUD
      ebay.ca     → CAD
      ebay.co.uk  → GBP
      ebay.de     → EUR
      ebay.com    → USD (default)
    Price symbols:
      A$ / AUD → AUD
      C$ / CAD → CAD
      £ / GBP  → GBP
      € / EUR  → EUR
      $ / USD  → USD (default)
    """
    price_text_lower = price_text.lower()

    if "a$" in price_text_lower or "aud" in price_text_lower or "au$" in price_text_lower:
        return "AUD"
    if "c$" in price_text_lower or "cad" in price_text_lower:
        return "CAD"
    if "£" in price_text or "gbp" in price_text_lower:
        return "GBP"
    if "€" in price_text or "eur" in price_text_lower:
        return "EUR"

    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if "ebay.com.au" in domain:
        return "AUD"
    if "ebay.ca" in domain:
        return "CAD"
    if "ebay.co.uk" in domain:
        return "GBP"
    if "ebay.de" in domain:
        return "EUR"

    return "USD"


# ---------------------------------------------------------------------------
# Price extraction
# ---------------------------------------------------------------------------
def extract_price(price_text: str) -> Optional[float]:
    """Extract float price from text like '$1,234.56' or '1,234.56'."""
    if not price_text:
        return None
    cleaned = _PRICE_CLEAN_RE.sub("", price_text)
    # Handle both comma and period as decimal separator
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(",", ".")
    elif "," in cleaned:
        parts = cleaned.split(",")
        if len(parts[-1]) <= 2:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# sold_date parsing (English)
# ---------------------------------------------------------------------------
MONTHS_EN = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}


def parse_sold_date(html: str) -> str:
    """Extract sold date from eBay listing HTML.

    Formats:
      "Sold Mon, Jan 1, 2024"
      "Sold Monday, January 1, 2024"
      "Sold Jan 1, 2024"
      "Jan 1, 2024"
    Returns YYYY-MM-DD or "" if not found.
    """
    # Primary pattern: "Sold Weekday, Month DD, YYYY"
    m = _SOLD_DATE_RE1.search(html)
    if not m:
        # Fallback: "Month DD, YYYY" (no Sold prefix)
        m = _SOLD_DATE_RE2.search(html)
    if not m:
        return ""
    month_name = m.group(1).lower()
    day = int(m.group(2))
    year = int(m.group(3))
    month_num = MONTHS_EN.get(month_name)
    if month_num is None:
        return ""
    return f"{year}-{month_num:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------
JP_INDICATORS = [
    "japan", "jap", "jp_", "japanese",
    "jp-", "japan import", "japanese version",
]


def detect_language(title: str) -> str:
    """Detect language from title."""
    title_lower = title.lower()
    return "JP" if any(ind in title_lower for ind in JP_INDICATORS) else "EN"


# ---------------------------------------------------------------------------
# Thumbnail URL extraction
# ---------------------------------------------------------------------------
def extract_thumbnail_url(html: str) -> str:
    """Extract listing thumbnail URL from HTML.

    Tries: data-old-hires attribute, src attribute on icImg, and fallback regex.
    Returns empty string if not found.
    """
    # Pattern 1: <img id="icImg" data-old-hires="https://..." src="...">
    m = re.search(r'<img[^>]+id="icImg"[^>]+data-old-hires="([^"]+)"', html)
    if m:
        return m.group(1)
    # Pattern 2: data-old-hires before src on icImg
    m = re.search(r'<img[^>]+data-old-hires="([^"]+)"[^>]+id="icImg"', html)
    if m:
        return m.group(1)
    # Pattern 3: src attribute on icImg
    m = re.search(r'<img[^>]+id="icImg"[^>]+src="([^"]+)"', html)
    if m:
        return m.group(1)
    return ""


# ---------------------------------------------------------------------------
# Title normalization for card_id extraction (helper for extract_card_id)
# ---------------------------------------------------------------------------
def normalize_title(title: str) -> str:
    """Strip noise from title before card_id extraction."""
    title = _NORM_PAREN_RE.sub("", title)
    title = _NORM_BRACKET_RE.sub("", title)
    return title.strip()


def parse_listing_page(html: str, url: str, scraped_at: datetime) -> dict:
    """Parse a full eBay item page into a listing row dict."""
    title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    raw_title = title_match.group(1).strip() if title_match else ""
    raw_title = re.sub(r"\s*-\s*eBay$", "", raw_title)
    title = raw_title

    price_match = re.search(r'data-testid="x-price-primary".*?<span[^>]*>([^<]+)</span>', html, re.DOTALL)
    price_text = price_match.group(1).strip() if price_match else ""
    price = extract_price(price_text)
    currency = detect_currency(price_text, url)

    norm_title = normalize_title(title)
    card_id = extract_card_id(norm_title)
    card_version = extract_card_version(title, card_id)

    sold_date = parse_sold_date(html)

    language = detect_language(title)

    thumbnail_url = extract_thumbnail_url(html)

    item_id_match = re.search(r"/itm/(\d+)", url)
    item_id = item_id_match.group(1) if item_id_match else ""

    return {
        "item_id": item_id,
        "source_url": url,
        "scraped_at": scraped_at.isoformat() if hasattr(scraped_at, "isoformat") else str(scraped_at),
        "region": "US",
        "card_id": card_id,
        "card_version": card_version,
        "title": title,
        "price": price,
        "currency": currency,
        "sold_date": sold_date,
        "language": language,
        "html_payload": html.encode("utf-8", errors="replace"),
        "thumbnail_url": thumbnail_url,
        "image_path": "",
    }
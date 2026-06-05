"""DE-specific eBay item-page parser.

Parses a single DE eBay item HTML page into PriceRecord(s).
Sold date is NOT parsed here — the caller attaches it from the search page.
"""
import re
from datetime import datetime
from html import unescape

from tcg_platform.scraping.models import PriceRecord

# Match OP/EB/ST/PRB/P set code in title, optionally with `-NNN` card number.
_SET_CODE_RE = re.compile(r"(OP\d+(?:-\d+)?|EB\d+(?:-\d+)?|ST\d+(?:-\d+)?|PRB\d+(?:-\d+)?|P\d+(?:-\d+)?)")

# Real DE item-page structure:
#   <div data-testid="x-price-primary"><span class="ux-textspans">EUR 12,50</span>...
_PRICE_PRIMARY_RE = re.compile(
    r'data-testid="x-price-primary"[^>]*>\s*<span class="ux-textspans"[^>]*>([^<]+)</span>',
    re.DOTALL,
)
# Real title structure: <h1 class="x-item-title__mainTitle"><span class="...">Title</span></h1>
_TITLE_RE = re.compile(
    r'<h1[^>]*>\s*<span[^>]*>(.*?)</span>',
    re.DOTALL,
)

_PROXY_INDICATORS = ["proxy", "dummy", "fake card", "replica"]


def _is_proxy(title: str) -> bool:
    title_lower = title.lower()
    return any(ind in title_lower for ind in _PROXY_INDICATORS)


def _format_card_id(raw: str) -> str:
    """Format a raw set+card string with a dash before the last 3 digits.

    Examples:
      OP01042 → OP01-042
      OP07029 → OP07-029
      ST13003 → ST13-003
      OP01    → OP01       (no card number, leave as-is)
    """
    m = re.match(r"^([A-Z]+)(\d+)$", raw)
    if not m or len(m.group(2)) <= 3:
        return raw
    set_code, digits = m.group(1), m.group(2)
    return f"{set_code}{digits[:-3]}-{digits[-3:]}"


def _normalize_card_id(title: str) -> tuple[str, str | None]:
    """Extract (card_id, card_version) from a raw eBay title.

    Strips (...) and [...] noise, normalizes spaces to underscores,
    matches the set-code regex, and reformats with a dash (OP01-042).
    """
    t = re.sub(r"\s*\(.*?\)", "", title)
    t = re.sub(r"\s*\[.*?\]", "", t)
    # Preserve dashes so card-number dashes (OP01-042) and version dashes (24-25) survive.
    t = re.sub(r"[^a-zA-Z0-9-]", "", t)
    t = t.strip().replace(" ", "_")[:100]
    m = _SET_CODE_RE.search(t)
    if not m:
        return t, None
    raw = m.group(0)
    base = _format_card_id(raw)
    version = t[m.end() :].strip("_")
    return base, (version or None)


def _detect_language(title: str) -> str:
    title_lower = title.lower()
    if any(
        ind in title_lower
        for ind in ["japan", "jap", "jp_", "japanese", "jp-"]
    ):
        return "JP"
    return "EN"


def parse_ebay_de_item_page(
    html: str,
    item_url: str,
    scraped_at: datetime,
) -> list[PriceRecord]:
    """Parse a DE eBay item page into PriceRecord(s).

    Returns [] on proxy titles or missing price.
    sold_date is left None — the caller attaches it from the search page.
    """
    if not html:
        return []
    html = unescape(html)

    # Title.
    title_m = _TITLE_RE.search(html)
    title = title_m.group(1).strip() if title_m else ""
    if not title or _is_proxy(title):
        return []

    # Card id + version.
    card_id, card_version = _normalize_card_id(title)

    # Price (DE: comma decimal, . thousands separator).
    price_m = _PRICE_PRIMARY_RE.search(html)
    if not price_m:
        return []
    price_text = price_m.group(1).strip()
    # Keep digits, comma, period. Convert DE format (1.234,56) → 1234.56.
    cleaned = re.sub(r"[^\d,.]", "", price_text)
    if "," in cleaned and "." in cleaned:
        # Period is thousands, comma is decimal.
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        # Comma is decimal.
        cleaned = cleaned.replace(",", ".")
    try:
        price = float(cleaned)
    except ValueError:
        return []

    language = _detect_language(title)

    return [
        PriceRecord(
            card_id=card_id,
            card_version=card_version,
            event_type="sale",
            price=price,
            currency="EUR",
            sold_date=None,
            scraped_from="ebay",
            source="DE",
            source_url=item_url,
            language=language,
            scraped_at=scraped_at,
            title=title,
        )
    ]

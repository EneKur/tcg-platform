"""UK-specific eBay item-page parser.

Parses a single UK eBay item HTML page into PriceRecord(s).
Sold date is NOT parsed here — the caller attaches it from the search page.
"""
import re
from datetime import datetime
from html import unescape

from tcg_platform.scraping.models import PriceRecord

# Match OP/EB/ST/PRB/P set code in title, optionally with `-NNN` card number.
_SET_CODE_RE = re.compile(r"(OP\d+(?:-\d+)?|EB\d+(?:-\d+)?|ST\d+(?:-\d+)?|PRB\d+(?:-\d+)?|P\d+(?:-\d+)?)")

# Real UK item-page structure: the price span often has class modifiers like
# --STRIKETHROUGH, so the regex matches any <span> tag after x-price-primary.
_PRICE_PRIMARY_RE = re.compile(
    r'data-testid="x-price-primary"[^>]*>\s*<span[^>]*>([^<]+)</span>',
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
      EB03031 → EB03-031
      OP01    → OP01       (no card number, leave as-is)
    """
    m = re.match(r"^([A-Z]+)(\d+)$", raw)
    if not m or len(m.group(2)) <= 3:
        return raw
    set_code, digits = m.group(1), m.group(2)
    return f"{set_code}{digits[:-3]}-{digits[-3:]}"


def _normalize_card_id(title: str) -> tuple[str, str | None]:
    t = re.sub(r"\s*\(.*?\)", "", title)
    t = re.sub(r"\s*\[.*?\]", "", t)
    # Preserve dashes so card-number dashes (OP01-042) and version dashes (24-25) survive.
    t = re.sub(r"[^a-zA-Z0-9-]", "", t)
    t = t.strip().replace(" ", "_")[:100]
    m = _SET_CODE_RE.search(t)
    if not m:
        # No recognizable set code (OP/EB/ST/PRB/P + digits) — caller should
        # skip the listing. Multi-card bundles and DON cards fall here.
        return "", None
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


def parse_ebay_uk_item_page(
    html: str,
    item_url: str,
    scraped_at: datetime,
) -> list[PriceRecord]:
    if not html:
        return []
    html = unescape(html)

    title_m = _TITLE_RE.search(html)
    title = title_m.group(1).strip() if title_m else ""
    if not title or _is_proxy(title):
        return []

    card_id, card_version = _normalize_card_id(title)
    if not card_id:
        return []

    price_m = _PRICE_PRIMARY_RE.search(html)
    if not price_m:
        return []
    price_text = price_m.group(1).strip()
    # UK: 1,234.56 → 1234.56
    cleaned = re.sub(r"[^\d,.]", "", price_text)
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", "")
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
            currency="GBP",
            sold_date=None,
            scraped_from="ebay",
            source="UK",
            source_url=item_url,
            language=language,
            scraped_at=scraped_at,
            title=title,
        )
    ]

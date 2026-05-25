import re
import html as _html_module
from datetime import datetime
from typing import Optional

from tcg_platform.scraping.models import PriceRecord


EBAY_REGION_CONFIGS = {
    "DE": {
        "base_url": (
            "https://www.ebay.de/sch/i.html"
            "?_nkw=PSA+One+Piece+TCG&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
        ),
        "currency": "EUR",
        "date_format": "german",
        "price_separator": ",",
        "locale": "de",
    },
    "UK": {
        "base_url": (
            "https://www.ebay.co.uk/sch/i.html"
            "?_nkw=PSA+One+Piece+TCG&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
        ),
        "currency": "GBP",
        "price_separator": ".",
        "date_format": "english",
        "locale": "en",
    },
    "US": {
        "base_url": (
            "https://www.ebay.com/sch/i.html"
            "?_nkw=PSA+One+Piece+TCG&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
        ),
        "currency": "USD",
        "price_separator": ".",
        "date_format": "english",
        "locale": "en",
    },
}

_ITEM_ID_RE = re.compile(r"/itm/(\d+)")
_DATE_RE_GERMAN = re.compile(
    r"verkauft am \w+,\s*(\d{1,2})\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)"
)
_DATE_RE_ENGLISH = re.compile(
    r"Sold\s+\w+\s+\d{1,2}\s+\w+\s+\d{4}|"
    r"Sold\s+\w+\s+\d{1,2},\s+\d{4}|"
    r"\d{1,2}\s+\w+\s+\d{4}"
)
_PRICE_RE = re.compile(r"data-testid=\"x-price-primary\".*?<span[^>]*>([^<]+)</span>", re.DOTALL)
_TITLE_RE = re.compile(r"<h1[^>]*>.*?<span[^>]*>(.*?)</span>", re.DOTALL)
_LISTING_LINK_RE = re.compile(
    r"href=\"(https://www\.ebay\.\w+/itm/\d+[^\"]*)\""
)
_LISTING_LINK_RE_BY_REGION = {
    "DE": re.compile(r"href=\"(https://www\.ebay\.de/itm/\d+[^\"]*)\""),
    "UK": re.compile(r"href=\"(https://www\.ebay\.co\.uk/itm/\d+[^\"]*)\""),
    "US": re.compile(r"href=\"(https://www\.ebay\.com/itm/\d+[^\"]*)\""),
}
_IMAGE_RE = re.compile(r'"image":"(https://i\.ebayimg\.com/[^"]+)"')

_MONTHS_DE = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8,
    "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}
_MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

_SET_CODE_RE = re.compile(r"(OP\d+|EB\d+|ST\d+|PRB\d+|P\d+)")


def _parse_date(html: str, region: str) -> Optional[str]:
    if region == "DE":
        match = _DATE_RE_GERMAN.search(html)
        if not match:
            return None
        day, month_name = match.groups()
        year = datetime.now().year
        month = _MONTHS_DE.get(month_name.lower(), 1)
        return f"{year}-{month:02d}-{int(day):02d}"

    match = _DATE_RE_ENGLISH.search(html)
    if not match:
        return None
    month_names = "|".join(_MONTHS_EN.keys())
    english_match = re.search(
        rf"(\d{{1,2}})\s+({month_names})\s+(\d{{4}})",
        html,
        re.IGNORECASE
    )
    if english_match:
        day, month_name, year = english_match.groups()
        month = _MONTHS_EN.get(month_name.lower(), 1)
        return f"{year}-{month:02d}-{int(day):02d}"
    return None


def _normalize_card_id(title: str) -> str:
    title = re.sub(r"\s*\(.*?\)", "", title)
    title = re.sub(r"\s*\[.*?\]", "", title)
    title = re.sub(r"[^a-zA-Z0-9\s]", "", title)
    return title.strip().replace(" ", "_")[:50]


def _split_base_version(card_id: str) -> tuple[str, str]:
    set_m = _SET_CODE_RE.search(card_id)
    if not set_m:
        return card_id, ""
    base = card_id[: set_m.end()]
    version = card_id[set_m.end() :].strip("_")
    return base, version


def _detect_language(title: str) -> str:
    title_lower = title.lower()
    jp_indicators = [
        "japan", "jap", "jp_", " japanese",
        "jp-", " japan", "japan import",
        " japan", "japanese version",
    ]
    if any(indic in title_lower for indic in jp_indicators):
        return "JP"
    return "EN"


def _is_proxy(title: str) -> bool:
    title_lower = title.lower()
    proxy_indicators = ["proxy", "dummy", "fake card", "replica"]
    return any(ind in title_lower for ind in proxy_indicators)


def _extract_item_id(url: str) -> str:
    match = _ITEM_ID_RE.search(url)
    return match.group(1) if match else url


def extract_item_image_url(html: str) -> str | None:
    match = _IMAGE_RE.search(html)
    return match.group(1) if match else None


def parse_ebay_item_page(
    html: str,
    item_url: str,
    scraped_at: datetime,
    region: str,
) -> list[PriceRecord]:
    cfg = EBAY_REGION_CONFIGS[region]

    price_match = _PRICE_RE.search(html)
    price_text = price_match.group(1).strip() if price_match else ""

    if region == "DE":
        price_value = re.sub(r"[^\d,]", "", price_text).replace(",", ".")
    else:
        price_value = re.sub(r"[^\d.]", "", price_text)

    try:
        price = float(price_value) if price_text else None
    except ValueError:
        price = None

    title_match = _TITLE_RE.search(html)
    title = title_match.group(1).strip() if title_match else ""
    raw_card_id = _normalize_card_id(title)
    card_id, card_version = _split_base_version(raw_card_id)
    sold_date = _parse_date(html, region)
    language = _detect_language(title)

    if _is_proxy(title):
        return []

    if price is not None:
        return [
            PriceRecord(
                card_id=card_id,
                card_version=card_version or None,
                event_type="sale",
                price=price,
                currency=cfg["currency"],
                sold_date=sold_date,
                scraped_from="ebay",
                source=region,
                source_url=item_url,
                language=language,
                scraped_at=scraped_at,
            )
        ]
    return []


def _parse_english_date(html: str) -> Optional[str]:
    """Parse English date format: 'Sold Mon, Jan 1, 2024' or 'Jan 1, 2024'."""
    month_names = "|".join(_MONTHS_EN.keys())
    match = re.search(rf"(\d{{1,2}})\s+({month_names})\s+(\d{{4}})", html, re.IGNORECASE)
    if not match:
        return None
    day, month_name, year = match.groups()
    month = _MONTHS_EN.get(month_name.lower(), 1)
    return f"{year}-{month:02d}-{int(day):02d}"


def parse_ebay_listings_page(html: str, base_url: str, region: str = "DE") -> list[tuple[str, str]]:
    """Extract item listing URLs from a search results page.

    UK and US pages use &amp; HTML entities and have different URL structures.
    Only yields URLs from the correct eBay domain for the region.
    """
    urls = []

    if region in ("UK", "US"):
        decoded = _html_module.unescape(html)
        if region == "UK":
            pattern = r'href="(https://www\.ebay\.co\.uk/itm/\d+[^"]*)"'
        else:
            pattern = r'href="(https://www\.ebay\.com/itm/\d+[^"]*)"'
        for match in re.finditer(pattern, decoded):
            raw_url = match.group(1).replace("&amp;", "&")
            clean_url = re.sub(r"\?.*", "", raw_url)
            if clean_url and clean_url not in [u for u, _ in urls]:
                urls.append((clean_url, clean_url))
    else:
        pattern = _LISTING_LINK_RE_BY_REGION.get(region, _LISTING_LINK_RE)
        for match in pattern.finditer(html):
            raw_url = match.group(1)
            clean_url = re.sub(r"\?.*", "", raw_url)
            if clean_url not in [u for u, _ in urls]:
                urls.append((clean_url, clean_url))

    return urls


def scrape_ebay_listings(
    client,
    region: str,
    already_seen_ids: set[str],
    max_pages: Optional[int] = None,
    max_records: Optional[int] = None,
):
    """Paginate through search results, yield new item URLs skipping known IDs.

    Stops when max_pages reached, no more URLs returned, or max_records yielded.
    """
    cfg = EBAY_REGION_CONFIGS[region]
    base_url = cfg["base_url"]
    yielded = 0

    for page in range(1, (max_pages or 9999) + 1):
        if max_records and yielded >= max_records:
            break

        if page == 1:
            url = base_url
        else:
            url = f"{base_url}&_pgn={page}"

        resp = client.get({"url": url, "browserHtml": True})
        if resp.get("statusCode") != 200:
            break
        html = resp.get("browserHtml", "")
        if not html:
            break

        urls = parse_ebay_listings_page(html, base_url, region)
        if not urls:
            if page == 1:
                continue
            break

        page_new = []
        for raw_url, _ in urls:
            item_id = _extract_item_id(raw_url)
            if item_id not in already_seen_ids:
                page_new.append(raw_url)

        for url in page_new:
            yield url
            yielded += 1
            if max_records and yielded >= max_records:
                break

        if len(urls) < 10:
            break

        if max_records and yielded >= max_records:
            break
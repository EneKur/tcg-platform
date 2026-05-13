from datetime import datetime
import re

from bs4 import BeautifulSoup

from steel import Steel

from tcg_platform.scraping.models import PriceRecord


PRICECHARTING_SEARCH_URL = (
    "https://www.pricecharting.com/search-products"
    "?q=one+piece+tcg&type=prices&ignore-preferences=true"
)
PRICECHARTING_CATEGORY_URL = "https://www.pricecharting.com/category/one-piece-cards"


def parse_pricecharting_html(html: str) -> list[PriceRecord]:
    soup = BeautifulSoup(html, "lxml")
    records = []
    scraped_at = datetime.utcnow()

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        title_cell = cells[1].get_text(strip=True)
        set_cell = cells[2].get_text(strip=True)

        if not title_cell or title_cell == "Title":
            continue

        card_name = title_cell

        low_price_text = cells[3].get_text(strip=True).replace("$", "").replace(",", "")
        high_price_text = cells[5].get_text(strip=True).replace("$", "").replace(",", "") if len(cells) > 5 else ""

        try:
            low_price = float(low_price_text) if low_price_text else None
        except ValueError:
            low_price = None

        try:
            high_price = float(high_price_text) if high_price_text else None
        except ValueError:
            high_price = None

        card_id = _normalize_card_id(card_name)

        if low_price is not None:
            records.append(PriceRecord(
                card_id=card_id,
                card_version=None,
                event_type="price_update",
                price=low_price,
                currency="USD",
                sold_date=None,
                scraped_from="pricecharting",
                source="US",
                source_url=PRICECHARTING_CATEGORY_URL,
                scraped_at=scraped_at,
            ))

        if high_price is not None:
            records.append(PriceRecord(
                card_id=card_id,
                card_version=None,
                event_type="price_update",
                price=high_price,
                currency="USD",
                sold_date=None,
                scraped_from="pricecharting",
                source="US",
                source_url=PRICECHARTING_CATEGORY_URL,
                scraped_at=scraped_at,
            ))

    return records


def _normalize_card_id(card_name: str) -> str:
    normalized = re.sub(r'\[.*?\]', '', card_name)
    normalized = re.sub(r'[^a-zA-Z0-9\s]', '', normalized)
    normalized = normalized.strip().replace(' ', '_')[:50]
    return normalized


def scrape_pricecharting(steel_api_key: str) -> list[PriceRecord]:
    client = Steel(steel_api_key=steel_api_key)
    result = client.scrape(url=PRICECHARTING_SEARCH_URL, delay=3.0)
    return parse_pricecharting_html(result.content.html)
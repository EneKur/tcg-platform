from datetime import datetime
import re

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from tcg_platform.scraping.models import CardRecord, PriceRecord


LIMITLESS_OP_BASE = "https://onepiece.limitlesstcg.com"


def _parse_card_page(html: str, set_code: str, scraped_at: datetime) -> tuple[CardRecord | None, list[PriceRecord]]:
    soup = BeautifulSoup(html, "html.parser")
    records = []

    title_tag = soup.find("meta", property="og:title")
    if not title_tag:
        return None, []

    full_title = title_tag.get("content", "")

    card_name_match = re.match(r"(.+?)\s*\(([^)]+)\)", full_title)
    if not card_name_match:
        return None, []

    card_name = card_name_match.group(1).strip()
    card_id_full = card_name_match.group(2).strip()

    card_type_elem = soup.find(string=re.compile(r"(Leader|Character|Event|Stages?)"))
    card_type = card_type_elem.strip() if card_type_elem else "Unknown"

    power_match = re.search(r"(\d+)\s+Power", full_title + soup.get_text())
    power = int(power_match.group(1)) if power_match else None

    cost_match = re.search(r"\[DON!!\s*x(\d+)\]", soup.get_text())
    cost = int(cost_match.group(1)) if cost_match else None

    color_elem = soup.find(string=re.compile(r"(Red|Green|Blue|Black|Purple|Yellow)"))
    color = color_elem.strip() if color_elem else None

    block_match = re.search(r"Block\s+(\d+)", soup.get_text())
    block = block_match.group(1) if block_match else None

    image_url = None
    img_tag = soup.find("img", class_=lambda x: x and "card-img" in x)
    if img_tag:
        image_url = img_tag.get("src")

    usd_price = None
    eur_price = None
    price_spans = soup.find_all("span", class_="card-price")
    for span in price_spans:
        text = span.get_text(strip=True)
        if span.get("class") and "usd" in span.get("class"):
            usd_price = float(text.replace("$", "").replace(",", ""))
        elif span.get("class") and "eur" in span.get("class"):
            eur_price = float(text.replace("€", "").replace(",", ""))

    card_record = CardRecord(
        card_id=card_id_full,
        card_version=None,
        card_name=card_name,
        set_code=set_code,
        rarity="",
        card_type=card_type,
        attribute=color,
        power=power,
        cost=cost,
        color=color,
        source_url=f"{LIMITLESS_OP_BASE}/cards/{card_id_full.replace(' ', '-')}",
        scraped_at=scraped_at,
    )

    if usd_price is not None:
        records.append(PriceRecord(
            card_id=card_id_full,
            card_version=None,
            event_type="price_update",
            price=usd_price,
            currency="USD",
            sold_date=None,
            scraped_from="limitlesstcg",
            source="onepiece",
            source_url=f"{LIMITLESS_OP_BASE}/cards/{card_id_full.replace(' ', '-')}",
            language="EN",
            scraped_at=scraped_at,
        ))

    if eur_price is not None:
        records.append(PriceRecord(
            card_id=card_id_full,
            card_version=None,
            event_type="price_update",
            price=eur_price,
            currency="EUR",
            sold_date=None,
            scraped_from="limitlesstcg",
            source="onepiece",
            source_url=f"{LIMITLESS_OP_BASE}/cards/{card_id_full.replace(' ', '-')}",
            language="EN",
            scraped_at=scraped_at,
        ))

    return card_record, records


def _get_all_sets() -> list[tuple[str, str]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{LIMITLESS_OP_BASE}/cards/", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=30000)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        sets = []
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if cells:
                    code = cells[0].get_text(strip=True)
                    link = cells[1].find("a")
                    href = link.get("href") if link else None
                    if href:
                        sets.append((code, href))

        browser.close()
        return sets


def extract_card_links_from_set_page(html: str) -> list[tuple[str, int | None]]:
    """Parse a Limitless set page; return [(card_id, variant), ...] deduped.

    card_id is uppercased. variant is None for base cards, int for ?v=N printings.
    """
    soup = BeautifulSoup(html, "html.parser")
    # "P" must be last; it's a 1-char prefix that would shadow any future "P*"-starting set codes.
    set_prefixes = ("OP", "EB", "ST", "PR", "P")
    raw = [
        a.get("href")
        for a in soup.find_all("a")
        if any(f"/CARDS/{p}" in (a.get("href") or "").upper() for p in set_prefixes)
    ]
    out: list[tuple[str, int | None]] = []
    seen: set[tuple[str, int | None]] = set()
    for href in raw:
        path, _, query = href.partition("?")
        card_id = path.rsplit("/", 1)[-1].upper()
        variant: int | None = None
        if query:
            for part in query.split("&"):
                if part.startswith("v="):
                    try:
                        variant = int(part[2:])
                    except ValueError:
                        variant = None
        key = (card_id, variant)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def scrape_limitless_op() -> tuple[list[CardRecord], list[PriceRecord]]:
    all_cards = []
    all_prices = []
    scraped_at = datetime.utcnow()

    sets = _get_all_sets()
    print(f"Found {len(sets)} sets to scrape")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for set_code, set_path in sets:
            print(f"Scraping set: {set_code}")
            page = browser.new_page()
            page.goto(f"{LIMITLESS_OP_BASE}{set_path}", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=30000)

            html = page.content()

            card_links = extract_card_links_from_set_page(html)

            # extract_card_links_from_set_page returns base + variant tuples.
            # scrape_limitless_op only needs base cards — variant pages are
            # covered by the separate sync_card_images asset.
            for card_id, variant in card_links:
                if variant is not None:
                    continue
                card_url = f"{LIMITLESS_OP_BASE}/cards/{card_id.lower()}"
                try:
                    card_page = browser.new_page()
                    card_page.goto(card_url, timeout=60000)
                    card_page.wait_for_load_state("networkidle", timeout=30000)

                    card_html = card_page.content()
                    card_record, price_records = _parse_card_page(card_html, set_code, scraped_at)

                    if card_record:
                        all_cards.append(card_record)
                        all_prices.extend(price_records)

                    card_page.close()
                except Exception as e:
                    print(f"Error scraping {card_id}: {e}")
                    continue

            page.close()

        browser.close()

    print(f"Total cards scraped: {len(all_cards)}")
    print(f"Total price records: {len(all_prices)}")
    return all_cards, all_prices
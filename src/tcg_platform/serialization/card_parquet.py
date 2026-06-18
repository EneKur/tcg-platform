import hashlib
import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from tcg_platform.scraping.ebay_utils import extract_item_id


LIMITLESS_HOST = "onepiece.limitlesstcg.com"


def derive_event_id(source_url: str) -> str:
    """Return a non-empty, deterministic event_id for the given source URL.

    - eBay DE/UK item pages: the eBay item_id (already a unique sold event).
    - Limitless TCG card pages: f"limitless-{card_id}" (the source has no
      sold event; we synthesize a stable id from the card_id).
    - Anything else: f"unknown-{md5(source_url)[:8]}" (deterministic
      8-char suffix, non-empty, debuggable, cross-run stable).
    """
    if not source_url:
        return "unknown-0"
    source_url = source_url.split("?", 1)[0]
    if LIMITLESS_HOST in source_url:
        parts = source_url.rstrip("/").split("/")
        return f"limitless-{parts[-1].upper()}"
    if "ebay.de" in source_url or "ebay.co.uk" in source_url:
        item_id = extract_item_id(source_url)
        if item_id and item_id.isdigit():
            return item_id
    digest = hashlib.md5(source_url.encode()).hexdigest()[:8]
    return f"unknown-{digest}"


def card_records_to_parquet(cards: list, partition_date: str) -> tuple[bytes, int]:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "card_id": card.card_id,
            "card_version": card.card_version or "",
            "card_name": card.card_name,
            "set_code": card.set_code,
            "rarity": card.rarity or "",
            "card_type": card.card_type,
            "attribute": card.attribute or "",
            "power": card.power or 0,
            "cost": card.cost or 0,
            "color": card.color or "",
            "source_url": card.source_url,
            "scraped_at": now.isoformat(),
        }
        for card in cards
    ]
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), len(rows)


def price_records_to_parquet(prices: list, partition_date: str) -> tuple[bytes, int]:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "event_id": "",
            "card_id": price.card_id,
            "card_version": price.card_version or "",
            "event_type": price.event_type,
            "price": price.price,
            "currency": price.currency,
            "sold_date": price.sold_date or "",
            "scraped_from": price.scraped_from,
            "source": price.source,
            "source_url": price.source_url,
            "scraped_at": now.isoformat(),
            "title": getattr(price, "title", None) or "",
        }
        for price in prices
    ]
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), len(rows)
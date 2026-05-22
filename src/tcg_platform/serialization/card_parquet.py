import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq


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
        }
        for price in prices
    ]
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), len(rows)
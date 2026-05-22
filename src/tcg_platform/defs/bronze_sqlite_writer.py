from datetime import datetime, timezone

import dagster as dg

from tcg_platform.resources.sqlite_client import SqliteClientResource


INSERT_CARD = """
    INSERT OR IGNORE INTO cardlist_dimension
        (card_id, card_version, card_name, set_code, rarity, card_type,
         attribute, power, cost, color, source_url, scraped_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_PRICE = """
    INSERT OR IGNORE INTO fact_events
        (card_id, card_version, event_type, price, currency,
         sold_date, scraped_from, source, source_url, language, scraped_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _upsert_cards(conn: SqliteClientResource, cards: list, scraped_at: datetime) -> int:
    rows = []
    for card in cards:
        rows.append((
            card.card_id,
            card.card_version,
            card.card_name,
            card.set_code,
            card.rarity or "",
            card.card_type,
            card.attribute or "",
            card.power or 0,
            card.cost or 0,
            card.color or "",
            card.source_url,
            scraped_at.isoformat(),
        ))
    conn.execute_many(INSERT_CARD, rows)
    return len(rows)


def _upsert_prices(conn: SqliteClientResource, prices: list, scraped_at: datetime) -> int:
    rows = []
    for price in prices:
        rows.append((
            price.card_id,
            price.card_version or "",
            price.event_type,
            price.price,
            price.currency,
            price.sold_date or "",
            price.scraped_from,
            price.source,
            price.source_url,
            price.language,
            scraped_at.isoformat(),
        ))
    conn.execute_many(INSERT_PRICE, rows)
    return len(rows)


@dg.asset
def bronze_sqlite_writer(
    context: dg.AssetExecutionContext,
    limitless_op_cards: dg.AssetOut,
    limitless_op_prices: dg.AssetOut,
) -> dg.MaterializeResult:
    """Write Limitless OP cards and prices to SQLite with idempotent upserts."""
    sqlite_client = context.resources.sqlite_client_de
    cards = limitless_op_cards
    prices = limitless_op_prices
    now = datetime.now(timezone.utc)

    cards_inserted = _upsert_cards(sqlite_client, cards, now)
    prices_inserted = _upsert_prices(sqlite_client, prices, now)

    context.log.info(f"Upserted {cards_inserted} cards and {prices_inserted} prices to SQLite")
    return dg.MaterializeResult(
        metadata={
            "num_cards": cards_inserted,
            "num_prices": prices_inserted,
        }
    )
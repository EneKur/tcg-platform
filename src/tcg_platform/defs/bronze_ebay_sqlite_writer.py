import dagster as dg


_PROXY_INDICATORS = ["proxy", "dummy", "fake card", "replica"]


def _is_proxy_title(card_id: str) -> bool:
    card_lower = card_id.lower()
    return any(ind in card_lower for ind in _PROXY_INDICATORS)


def _write_records(sqlite_client, records: list) -> int:
    rows = [
        (
            r.card_id,
            r.card_version or "",
            r.event_type,
            r.price,
            r.currency,
            r.sold_date or "",
            r.scraped_from,
            r.source,
            r.source_url,
            r.language,
            r.scraped_at.isoformat() if hasattr(r.scraped_at, "isoformat") else str(r.scraped_at),
        )
        for r in records
        if not _is_proxy_title(r.card_id)
    ]
    if rows:
        sqlite_client.execute_many(
            """
            INSERT OR IGNORE INTO fact_events
                (card_id, card_version, event_type, price, currency,
                 sold_date, scraped_from, source, source_url, language, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


@dg.asset
def bronze_ebay_de_sqlite_writer(
    context: dg.AssetExecutionContext,
    ebay_de_sold_listings: list,
) -> dg.MaterializeResult:
    sqlite_client = context.resources.sqlite_client_de
    n = _write_records(sqlite_client, ebay_de_sold_listings)
    context.log.info(f"Wrote {n} DE eBay records to SQLite")
    return dg.MaterializeResult(metadata={"num_records": n})


@dg.asset
def bronze_ebay_uk_sqlite_writer(
    context: dg.AssetExecutionContext,
    ebay_uk_sold_listings: list,
) -> dg.MaterializeResult:
    sqlite_client = context.resources.sqlite_client_uk
    n = _write_records(sqlite_client, ebay_uk_sold_listings)
    context.log.info(f"Wrote {n} UK eBay records to SQLite")
    return dg.MaterializeResult(metadata={"num_records": n})


@dg.asset
def bronze_ebay_us_sqlite_writer(
    context: dg.AssetExecutionContext,
    ebay_us_sold_listings: list,
) -> dg.MaterializeResult:
    sqlite_client = context.resources.sqlite_client_us
    n = _write_records(sqlite_client, ebay_us_sold_listings)
    context.log.info(f"Wrote {n} US eBay records to SQLite")
    return dg.MaterializeResult(metadata={"num_records": n})
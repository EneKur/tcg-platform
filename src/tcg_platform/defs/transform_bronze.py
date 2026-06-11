"""Offline transformer: read tcg-raw, parse HTML, write tcg-bronze.

This asset has no network dependencies. It reads the raw HTML and
images that the scraper just wrote and produces the structured
bronze layer (parquet files + SQLite fact_events rows).
"""
import logging
from datetime import datetime, timezone

import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
from tcg_platform.serialization.card_parquet import price_records_to_parquet

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
BRONZE_BUCKET = "tcg-bronze"

_PROXY_INDICATORS = ["proxy", "dummy", "fake card", "replica"]


def _is_proxy_title(card_id: str) -> bool:
    card_lower = card_id.lower()
    return any(ind in card_lower for ind in _PROXY_INDICATORS)


def _transform_region(
    raw_minio_client: MinioClientResource,
    bronze_minio_client: MinioClientResource,
    sqlite_client,
    region: str,
    written_items: list[dict],
    parse_item_page_fn,
) -> dict:
    """Read raw HTML for each written item, parse, write bronze parquet + SQLite.

    `written_items` is the list of {event_id, region, sold_date} dicts
    returned by the scraper asset for this run. This function does NOT
    scan tcg-raw; it processes exactly the items the scraper just wrote.
    """
    upper = region.upper()
    lower = region.lower()
    counts = {
        "read_html": 0, "read_image": 0, "wrote_parquet": 0,
        "wrote_sqlite": 0, "skipped_empty": 0, "parse_failed": 0,
        "image_missing": 0,
    }
    scraped_at = datetime.now(timezone.utc)

    for item in written_items:
        event_id = item["event_id"]
        if item.get("region", upper) != upper:
            continue
        sold_date = item.get("sold_date")

        # Read raw HTML
        try:
            html = raw_minio_client.get_object(
                RAW_BUCKET, f"ebay/{upper}/{event_id}.html"
            ).decode("utf-8")
        except Exception as e:
            _LOG.warning(f"Read html failed for {event_id}: {e}")
            continue
        counts["read_html"] += 1

        # Read raw image
        image_path = None
        try:
            image_path = f"sold_images/{lower}/{event_id}.jpg"
            raw_minio_client.get_object(RAW_BUCKET, image_path)
            counts["read_image"] += 1
        except Exception:
            counts["image_missing"] += 1
            image_path = None

        # Build source_url from event_id (eBay URL pattern is region-specific)
        item_url = (
            f"https://www.ebay.de/itm/{event_id}" if upper == "DE"
            else f"https://www.ebay.co.uk/itm/{event_id}"
        )

        # Parse HTML
        try:
            parsed = parse_item_page_fn(html, item_url, scraped_at)
        except Exception as e:
            _LOG.warning(f"Parse failed for {event_id}: {e}")
            counts["parse_failed"] += 1
            continue
        if not parsed:
            counts["skipped_empty"] += 1
            continue

        # Attach sold_date (from the search page, carried through the scraper)
        # and image_path to each record, then write bronze parquet + SQLite
        for rec in parsed:
            if sold_date and not rec.sold_date:
                rec.sold_date = sold_date
            rec.local_image_path = image_path
            parquet_bytes, _ = price_records_to_parquet(
                [rec], rec.scraped_at.strftime("%Y-%m-%d")
            )
            bronze_minio_client.put_object(
                bucket_name=BRONZE_BUCKET,
                object_name=f"sold_data/{upper}/{event_id}.parquet",
                data=parquet_bytes,
                length=len(parquet_bytes),
                content_type="application/parquet",
            )
            counts["wrote_parquet"] += 1

            if not _is_proxy_title(rec.card_id):
                sqlite_client.execute(
                    """
                    INSERT OR IGNORE INTO fact_events
                        (card_id, card_version, event_type, price, currency,
                         sold_date, scraped_from, source, source_url, language,
                         scraped_at, image_url, local_image_path, parqueted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        rec.card_id, rec.card_version or "", rec.event_type,
                        rec.price, rec.currency, rec.sold_date or "",
                        rec.scraped_from, rec.source, rec.source_url,
                        rec.language,
                        rec.scraped_at.isoformat() if hasattr(rec.scraped_at, "isoformat") else str(rec.scraped_at),
                        rec.image_url or "", rec.local_image_path or "",
                    ),
                )
                counts["wrote_sqlite"] += 1

    return counts


@dg.asset(
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_de"},
)
def transform_ebay_de_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_de_raw: list,
) -> dg.MaterializeResult:
    raw_minio_client = context.resources.tcg_raw_client
    bronze_minio_client = context.resources.minio_client
    sqlite_client = context.resources.sqlite_client_de

    counts = _transform_region(
        raw_minio_client, bronze_minio_client, sqlite_client, "DE",
        scrape_ebay_de_raw, parse_ebay_de_item_page,
    )
    context.log.info(f"DE transform: {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_uk"},
)
def transform_ebay_uk_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_uk_raw: list,
) -> dg.MaterializeResult:
    raw_minio_client = context.resources.tcg_raw_client
    bronze_minio_client = context.resources.minio_client
    sqlite_client = context.resources.sqlite_client_uk

    counts = _transform_region(
        raw_minio_client, bronze_minio_client, sqlite_client, "UK",
        scrape_ebay_uk_raw, parse_ebay_uk_item_page,
    )
    context.log.info(f"UK transform: {counts}")
    return dg.MaterializeResult(metadata=counts)

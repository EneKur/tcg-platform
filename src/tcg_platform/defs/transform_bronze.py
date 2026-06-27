"""Offline transformer: read tcg-raw, parse HTML, write tcg-bronze.

The per-item contract lives in `tcg_platform.serialization.bronze_writer.
transform_one_item`. This module is a thin wrapper that iterates over
the items the scraper just wrote and delegates per-item work to the
shared helper in `fill` mode.

This asset has no network dependencies. It reads the raw HTML and
images that the scraper just wrote and produces the structured
bronze layer (parquet files + SQLite fact_events rows).
"""
import logging

import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.bronze_writer import transform_one_item

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
BRONZE_BUCKET = "tcg-bronze"


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
    counts = {
        "read_html": 0, "read_image": 0, "wrote_parquet": 0,
        "wrote_sqlite": 0, "skipped_empty": 0, "parse_failed": 0,
        "image_missing": 0, "skipped_existing": 0,
    }
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

        # Read raw image (optional)
        image_path = None
        try:
            image_path = f"sold_images/{region.lower()}/{event_id}.jpg"
            raw_minio_client.get_object(RAW_BUCKET, image_path)
            counts["read_image"] += 1
        except Exception:
            counts["image_missing"] += 1
            image_path = None

        # Delegate per-item work to the shared helper in fill mode
        item_counts = transform_one_item(
            region=region,
            event_id=event_id,
            raw_html=html,
            image_path=image_path,
            bronze_minio_client=bronze_minio_client,
            sqlite_client=sqlite_client,
            parse_item_page_fn=parse_item_page_fn,
            mode="fill",
            sold_date=sold_date,
        )
        # Aggregate per-item counts into the region-level totals
        for k in ("wrote_parquet", "wrote_sqlite", "skipped_empty",
                  "parse_failed", "skipped_existing"):
            counts[k] = counts.get(k, 0) + item_counts.get(k, 0)

    return counts


@dg.asset(
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_de"},
)
def transform_ebay_de_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_de_raw: list,
) -> dg.MaterializeResult:
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    counts = _transform_region(
        context.resources.tcg_raw_client,
        context.resources.minio_client,
        context.resources.sqlite_client_de,
        "DE",
        scrape_ebay_de_raw,
        parse_ebay_de_item_page,
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
    from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
    counts = _transform_region(
        context.resources.tcg_raw_client,
        context.resources.minio_client,
        context.resources.sqlite_client_uk,
        "UK",
        scrape_ebay_uk_raw,
        parse_ebay_uk_item_page,
    )
    context.log.info(f"UK transform: {counts}")
    return dg.MaterializeResult(metadata=counts)
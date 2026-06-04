"""DE eBay sold-listings asset.

Fetches the DE search results page (1 Zyte call), parses (item_url, sold_date)
pairs, then for each new item URL fetches the item page (1 Zyte call) and
extracts price/currency/image. The search-page sold_date is attached to the
PriceRecord before returning.

Idempotency: re-runs skip item_ids already in the DE SQLite fact_events table.
"""
from datetime import datetime, timezone

import dagster as dg

from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
from tcg_platform.scraping.ebay_de_search import (
    DE_SEARCH_URL,
    parse_ebay_de_search_page,
)
from tcg_platform.scraping.ebay_image import (
    download_and_save_image,
    image_exists_in_minio,
)
from tcg_platform.scraping.ebay_utils import extract_item_id, extract_item_image_url
from tcg_platform.serialization.card_parquet import price_records_to_parquet


@dg.asset(
    required_resource_keys={"zyte_session_resource", "sqlite_client_de", "minio_client"},
)
def ebay_de_sold_listings(context: dg.AssetExecutionContext) -> list:
    zyte_client = context.resources.zyte_session_resource
    sqlite_client = context.resources.sqlite_client_de
    minio_client = context.resources.minio_client

    already_seen = sqlite_client.get_seen_ebay_item_ids()
    context.log.info(f"Known item IDs in DE DB: {len(already_seen)}")

    # 1. Fetch the search page (1 Zyte call).
    resp = zyte_client.get({"url": DE_SEARCH_URL, "browserHtml": True})
    if resp.get("statusCode") != 200:
        context.log.warning(f"Search page returned {resp.get('statusCode')}")
        return []
    html = resp.get("browserHtml", "")
    if not html:
        context.log.info("Empty search page")
        return []

    # 2. Parse (item_url, sold_date) pairs.
    pairs = parse_ebay_de_search_page(html)
    context.log.info(f"Search page: {len(pairs)} items")

    records = []
    scraped_at = datetime.now(timezone.utc)

    # 3. For each new item: fetch item page, parse, attach date.
    for item_url, sold_date in pairs:
        item_id = extract_item_id(item_url)
        if item_id in already_seen:
            continue

        try:
            item_resp = zyte_client.get(
                {"url": item_url, "browserHtml": True}
            )
            if item_resp.get("statusCode") != 200:
                continue
            item_html = item_resp.get("browserHtml", "")
            if not item_html:
                continue

            parsed = parse_ebay_de_item_page(item_html, item_url, scraped_at)
            if not parsed:
                continue

            # Image URL + download.
            image_url = extract_item_image_url(item_html)
            image_path = None
            if not image_exists_in_minio(minio_client, item_id, "DE"):
                image_path = download_and_save_image(
                    item_id, "DE", item_html, minio_client
                )
            else:
                image_path = f"sold_images/DE/{item_id}.jpg"

            for rec in parsed:
                rec.image_url = image_url
                rec.local_image_path = image_path
                rec.sold_date = sold_date or None

                item_id_for_rec = extract_item_id(rec.source_url)
                parquet_bytes, _ = price_records_to_parquet(
                    [rec], rec.scraped_at.strftime("%Y-%m-%d")
                )
                minio_client.put_object(
                    bucket_name=minio_client.bucket_name,
                    object_name=f"sold_data/DE/{item_id_for_rec}.parquet",
                    data=parquet_bytes,
                    length=len(parquet_bytes),
                    content_type="application/parquet",
                )

            records.extend(parsed)
        except Exception as e:
            context.log.warning(f"Failed to scrape {item_url}: {e}")
            continue

    context.log.info(f"Scraped {len(records)} new DE sold listing records")
    return records

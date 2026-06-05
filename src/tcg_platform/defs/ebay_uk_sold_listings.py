"""UK eBay sold-listings asset.

Fetches the UK search results page (1 Zyte call), parses (item_url, sold_date)
pairs, then for each new item URL fetches the item page (1 Zyte call) and
extracts price/currency/image. The search-page sold_date is attached to the
PriceRecord before returning.

Idempotency: re-runs skip item_ids already in the UK SQLite fact_events table.
SQLite is updated atomically per item after the MinIO write succeeds, so the
already_seen set stays current within a single scrape run — eBay can show the
same item on multiple search pages without causing duplicate Zyte calls.
"""
from datetime import datetime, timezone

import dagster as dg

from tcg_platform.defs.bronze_ebay_sqlite_writer import _is_proxy_title
from tcg_platform.scraping.ebay_image import (
    download_and_save_image,
    image_exists_in_minio,
)
from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
from tcg_platform.scraping.ebay_uk_search import (
    search_url_for_page,
    parse_ebay_uk_search_page,
)
from tcg_platform.scraping.ebay_utils import extract_item_id, extract_item_image_url
from tcg_platform.serialization.card_parquet import price_records_to_parquet


@dg.asset(
    required_resource_keys={"zyte_session_resource", "sqlite_client_uk", "minio_client"},
)
def ebay_uk_sold_listings(context: dg.AssetExecutionContext) -> list:
    zyte_client = context.resources.zyte_session_resource
    sqlite_client = context.resources.sqlite_client_uk
    minio_client = context.resources.minio_client

    already_seen = sqlite_client.get_seen_ebay_item_ids()
    context.log.info(f"Known item IDs in UK DB: {len(already_seen)}")

    records = []
    scraped_at = datetime.now(timezone.utc)
    page = 1
    empty_streak = 0
    EMPTY_STREAK_THRESHOLD = 5

    while True:
        search_url = search_url_for_page(page)
        context.log.info(f"Fetching UK search page {page}: {search_url}")

        resp = zyte_client.get({"url": search_url, "browserHtml": True})
        if resp.get("statusCode") != 200:
            context.log.warning(f"Search page {page} returned {resp.get('statusCode')}")
            break
        html = resp.get("browserHtml", "")
        if not html:
            context.log.info(f"Empty search page {page}")
            break

        pairs = parse_ebay_uk_search_page(html)
        context.log.info(f"Page {page}: {len(pairs)} items parsed")

        if not pairs:
            context.log.info(f"Page {page} returned no items — end of results")
            break

        page_item_ids = {extract_item_id(url) for url, _ in pairs}
        new_item_ids = page_item_ids - already_seen
        if not new_item_ids:
            empty_streak += 1
            context.log.info(
                f"Page {page}: all {len(pairs)} items already seen "
                f"(streak {empty_streak}/{EMPTY_STREAK_THRESHOLD})"
            )
            if empty_streak >= EMPTY_STREAK_THRESHOLD:
                context.log.info(
                    f"Stopping: {EMPTY_STREAK_THRESHOLD} consecutive pages with only "
                    "already-seen items — history is fully scraped"
                )
                break
            page += 1
            continue
        else:
            empty_streak = 0

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

                parsed = parse_ebay_uk_item_page(item_html, item_url, scraped_at)
                if not parsed:
                    continue

                image_url = extract_item_image_url(item_html)
                image_path = None
                if not image_exists_in_minio(minio_client, item_id, "UK"):
                    image_path = download_and_save_image(
                        item_id, "UK", item_html, minio_client
                    )
                else:
                    image_path = f"sold_images/UK/{item_id}.jpg"

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
                        object_name=f"sold_data/UK/{item_id_for_rec}.parquet",
                        data=parquet_bytes,
                        length=len(parquet_bytes),
                        content_type="application/parquet",
                    )

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
                                rec.card_id,
                                rec.card_version or "",
                                rec.event_type,
                                rec.price,
                                rec.currency,
                                rec.sold_date or "",
                                rec.scraped_from,
                                rec.source,
                                rec.source_url,
                                rec.language,
                                rec.scraped_at.isoformat() if hasattr(rec.scraped_at, "isoformat") else str(rec.scraped_at),
                                rec.image_url or "",
                                rec.local_image_path or "",
                            ),
                        )
                        already_seen.add(item_id_for_rec)

                records.extend(parsed)
            except Exception as e:
                context.log.warning(f"Failed to scrape {item_url}: {e}")
                continue

        page += 1

    context.log.info(f"Scraped {len(records)} new UK sold listing records across {page - 1} pages")
    return records

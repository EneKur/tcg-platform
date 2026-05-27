import dagster as dg
import re
from datetime import datetime, timezone

from tcg_platform.scraping.ebay import (
    extract_item_image_url,
    parse_ebay_item_page,
    scrape_ebay_listings,
)
from tcg_platform.scraping.ebay_image import (
    download_and_save_image,
    image_exists_in_minio,
)
from tcg_platform.serialization.card_parquet import price_records_to_parquet


_ITEM_ID_RE = re.compile(r"/itm/(\d+)")


def _extract_item_id(url: str) -> str:
    match = _ITEM_ID_RE.search(url)
    return match.group(1) if match else url


@dg.asset(
    required_resource_keys={"zyte_session_resource", "sqlite_client_de", "minio_client"},
)
def ebay_de_sold_listings(context: dg.AssetExecutionContext) -> list:
    zyte_client = context.resources.zyte_session_resource
    sqlite_client = context.resources.sqlite_client_de
    minio_client = context.resources.minio_client

    already_seen = sqlite_client.get_seen_ebay_item_ids()
    context.log.info(f"Known item IDs in DE DB: {len(already_seen)}")

    new_item_urls = []
    for item_url in scrape_ebay_listings(zyte_client, "DE", already_seen, max_records=100):
        new_item_urls.append(item_url)

    context.log.info(f"New DE items to scrape: {len(new_item_urls)}")

    if not new_item_urls:
        context.log.info("No new DE items found")
        return []

    records = []
    scraped_at = datetime.now(timezone.utc)

    for item_url in new_item_urls:
        try:
            resp = zyte_client.get({"url": item_url, "browserHtml": True})
            if resp.get("statusCode") != 200:
                continue
            html = resp.get("browserHtml", "")
            if not html:
                continue
            parsed = parse_ebay_item_page(html, item_url, scraped_at, "DE")
            if not parsed:
                continue

            item_id = _extract_item_id(item_url)
            image_url = extract_item_image_url(html)

            object_path = f"sold_images/DE/{item_id}.jpg"
            image_path = None
            if not image_exists_in_minio(minio_client, item_id, "DE"):
                image_path = download_and_save_image(item_id, "DE", html, minio_client)
            else:
                image_path = object_path

            for rec in parsed:
                rec.image_url = image_url
                rec.local_image_path = image_path

                item_id_for_rec = _extract_item_id(rec.source_url)
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
"""One-time backfill: fetch raw HTML for SQLite rows scraped before tcg-raw existed.

After this asset runs once, all historical fact_events rows have a
corresponding tcg-raw/ebay/{region}/{event_id}.html. The asset can
be deprecated after first use; the backfill can be triggered again
later if tcg-raw is ever wiped.
"""
import logging

import dagster as dg
import requests
from minio.error import S3Error

from tcg_platform.scraping.ebay_image import extract_item_image_url
from tcg_platform.scraping.ebay_utils import extract_item_id

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"


def _backfill_region(
    minio_client,
    zyte_client,
    sqlite_client,
    region: str,
) -> dict:
    """For each fact_event in SQLite whose source_url has no raw HTML yet,
    fetch the item page from eBay and persist raw HTML + image.

    One-time job to populate tcg-raw for rows scraped before this
    design landed.
    """
    upper = region.upper()
    lower = region.lower()
    rows = sqlite_client.execute(
        "SELECT source_url FROM fact_events WHERE scraped_from = 'ebay' AND source = ?",
        (upper,),
        fetch="all",
    )
    counts = {"checked": 0, "already_have": 0, "fetched": 0, "failed": 0}
    for row in rows:
        url = row["source_url"]
        event_id = extract_item_id(url)
        if not event_id or not event_id.isdigit():
            continue
        counts["checked"] += 1

        # Skip if already in raw
        try:
            minio_client.client.stat_object(RAW_BUCKET, f"ebay/{upper}/{event_id}.html")
            counts["already_have"] += 1
            continue
        except S3Error:
            pass

        # Fetch and persist
        try:
            resp = zyte_client.get({"url": url, "browserHtml": True})
            if resp.get("statusCode") != 200:
                counts["failed"] += 1
                continue
            html = resp.get("browserHtml", "")
            if not html:
                counts["failed"] += 1
                continue
            html_bytes = html.encode("utf-8")
            minio_client.put_object(
                RAW_BUCKET,
                f"ebay/{upper}/{event_id}.html",
                html_bytes,
                len(html_bytes),
                "text/html",
            )
            img_url = extract_item_image_url(html)
            if img_url:
                try:
                    img_data = requests.get(img_url, timeout=30).content
                    minio_client.put_object(
                        RAW_BUCKET,
                        f"sold_images/{lower}/{event_id}.jpg",
                        img_data,
                        len(img_data),
                        "image/jpeg",
                    )
                except Exception:
                    pass
            counts["fetched"] += 1
        except Exception as e:
            _LOG.warning(f"Backfill failed for {url}: {e}")
            counts["failed"] += 1
    return counts


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client", "sqlite_client_de"},
)
def backfill_raw_html_de(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    counts = _backfill_region(
        context.resources.minio_client,
        context.resources.zyte_session_resource,
        context.resources.sqlite_client_de,
        "DE",
    )
    context.log.info(f"DE backfill: {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client", "sqlite_client_uk"},
)
def backfill_raw_html_uk(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    counts = _backfill_region(
        context.resources.minio_client,
        context.resources.zyte_session_resource,
        context.resources.sqlite_client_uk,
        "UK",
    )
    context.log.info(f"UK backfill: {counts}")
    return dg.MaterializeResult(metadata=counts)

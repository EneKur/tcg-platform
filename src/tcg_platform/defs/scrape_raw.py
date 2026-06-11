"""Network-only eBay scraper. Writes raw HTML + images + logs to tcg-raw.

The transformer (`transform_bronze.py`) reads from tcg-raw and writes
the structured bronze layer. The scraper does not know what a
card_id is; it only deals with event_id (the eBay item id from the URL).
"""
import logging
from datetime import datetime, timezone
from typing import NamedTuple

import dagster as dg
import requests
from minio.error import S3Error

from tcg_platform.scraping.ebay_de_search import (
    parse_ebay_de_search_page,
    search_url_for_page as de_search_url_for_page,
)
from tcg_platform.scraping.ebay_uk_search import (
    parse_ebay_uk_search_page,
    search_url_for_page as uk_search_url_for_page,
)
from tcg_platform.scraping.ebay_utils import (
    extract_item_id,
    extract_item_image_url,
)

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
EMPTY_STREAK_THRESHOLD = 5


class WrittenItem(NamedTuple):
    event_id: str
    region: str  # "DE" or "UK"
    sold_date: str | None  # YYYY-MM-DD, or None if the search page didn't show it


def _exists_in_raw(minio_client, region: str, event_id: str) -> bool:
    """Atomic existence check against tcg-raw/ebay/{region}/{event_id}.html.

    Returns True if the object is present, False otherwise (including
    on unexpected errors — refetching is safer than silently skipping
    on a transient failure).
    """
    try:
        minio_client.client.stat_object(
            RAW_BUCKET, f"ebay/{region}/{event_id}.html"
        )
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject"):
            return False
        _LOG.warning(
            f"stat_object unexpected error for {region}/{event_id}: {e}"
        )
        return False
    except Exception as e:
        _LOG.warning(f"stat_object error for {region}/{event_id}: {e}")
        return False


def _scrape_region(
    minio_client,
    zyte_client,
    region: str,
    search_url_for_page_fn,
    parse_search_page_fn,
) -> tuple[list[WrittenItem], list[str]]:
    """Scrape one region's sold listings into tcg-raw.

    Returns (newly_written, log_lines). The caller writes log_lines
    to tcg-raw/logs/{timestamp}.log at the end of the run.
    """
    log: list[str] = []
    log.append(f"{datetime.now(timezone.utc).isoformat()} START region={region}")

    written: list[WrittenItem] = []
    page = 1
    pages_fetched = 0
    items_seen = 0
    items_skipped_already_seen = 0
    items_fetched_zyte = 0
    items_failed_zyte = 0
    items_failed_parse = 0
    images_skipped_already_seen = 0
    images_downloaded = 0
    images_failed = 0
    empty_streak = 0
    found_items_on_any_page = False

    while True:
        search_url = search_url_for_page_fn(page)
        log.append(
            f"{datetime.now(timezone.utc).isoformat()} FETCH "
            f"search_page={page} url={search_url}"
        )
        resp = zyte_client.get({"url": search_url, "browserHtml": True})
        pages_fetched += 1
        if resp.get("statusCode") != 200:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} status={resp.get('statusCode')}"
            )
            break
        html = resp.get("browserHtml", "")
        if not html:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} empty_html=true"
            )
            break
        pairs = parse_search_page_fn(html)
        items_seen += len(pairs)
        log.append(
            f"{datetime.now(timezone.utc).isoformat()} PARSED "
            f"search_page={page} items={len(pairs)}"
        )
        if not pairs:
            empty_streak += 1
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} no_items=true empty_streak={empty_streak}"
            )
            if found_items_on_any_page or empty_streak >= EMPTY_STREAK_THRESHOLD:
                break
            page += 1
            continue
        empty_streak = 0
        found_items_on_any_page = True

        for item_url, _sold_date in pairs:
            event_id = extract_item_id(item_url)
            if not event_id or not event_id.isdigit():
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} SKIP bad_event_id url={item_url}"
                )
                continue
            if _exists_in_raw(minio_client, region, event_id):
                items_skipped_already_seen += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} SKIP "
                    f"already_in_raw event_id={event_id}"
                )
                continue

            # Fetch item page
            item_resp = zyte_client.get({"url": item_url, "browserHtml": True})
            items_fetched_zyte += 1
            if item_resp.get("statusCode") != 200:
                items_failed_zyte += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL zyte "
                    f"event_id={event_id} status={item_resp.get('statusCode')}"
                )
                continue
            item_html = item_resp.get("browserHtml", "")
            if not item_html:
                items_failed_parse += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL "
                    f"empty_item_html event_id={event_id}"
                )
                continue

            # Persist raw HTML
            try:
                html_bytes = item_html.encode("utf-8")
                minio_client.put_object(
                    bucket_name=RAW_BUCKET,
                    object_name=f"ebay/{region}/{event_id}.html",
                    data=html_bytes,
                    length=len(html_bytes),
                    content_type="text/html",
                )
            except Exception as e:
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL "
                    f"put_object_html event_id={event_id} err={e}"
                )
                continue

            written.append(
                WrittenItem(event_id=event_id, region=region, sold_date=_sold_date)
            )
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} WROTE html "
                f"event_id={event_id} bytes={len(html_bytes)}"
            )

            # Persist raw image
            img_url = extract_item_image_url(item_html)
            if img_url:
                img_path = f"sold_images/{region}/{event_id}.jpg"
                try:
                    minio_client.client.stat_object(RAW_BUCKET, img_path)
                    images_skipped_already_seen += 1
                    log.append(
                        f"{datetime.now(timezone.utc).isoformat()} SKIP "
                        f"already_in_raw_image event_id={event_id}"
                    )
                except S3Error as e:
                    if e.code not in ("NoSuchKey", "NoSuchObject"):
                        log.append(
                            f"{datetime.now(timezone.utc).isoformat()} "
                            f"WARN stat_image event_id={event_id} err={e}"
                        )
                    try:
                        img_data = requests.get(img_url, timeout=30).content
                        minio_client.put_object(
                            bucket_name=RAW_BUCKET,
                            object_name=img_path,
                            data=img_data,
                            length=len(img_data),
                            content_type="image/jpeg",
                        )
                        images_downloaded += 1
                        log.append(
                            f"{datetime.now(timezone.utc).isoformat()} WROTE image "
                            f"event_id={event_id} bytes={len(img_data)}"
                        )
                    except Exception as img_e:
                        images_failed += 1
                        log.append(
                            f"{datetime.now(timezone.utc).isoformat()} FAIL "
                            f"image event_id={event_id} err={img_e}"
                        )

        page += 1

    log.append(
        f"{datetime.now(timezone.utc).isoformat()} END region={region} "
        f"pages_fetched={pages_fetched} items_seen={items_seen} "
        f"items_skipped_already_seen={items_skipped_already_seen} "
        f"items_fetched_zyte={items_fetched_zyte} items_failed_zyte={items_failed_zyte} "
        f"items_failed_parse={items_failed_parse} "
        f"images_downloaded={images_downloaded} "
        f"images_skipped_already_seen={images_skipped_already_seen} "
        f"images_failed={images_failed} written={len(written)}"
    )
    return written, log

"""Network-only eBay scraper. Writes raw HTML + images + logs to tcg-raw.

The transformer (`transform_bronze.py`) reads from tcg-raw and writes
the structured bronze layer. The scraper does not know what a
card_id is; it only deals with event_id (the eBay item id from the URL).
"""
import logging
import time
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

# Hard caps (2026-06-16 redesign): a runaway eBay category can't burn
# the API budget. Tuned to fit a healthy eBay category (~5s per page
# in 2026-06-16 live tests; 20 pages * 5s = 100s of pure page-fetch
# time, plus 0-60 item pages).
MAX_PAGES_PER_REGION = 20
MAX_WALL_CLOCK_S = 900  # 15 minutes per region


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
) -> tuple[list[WrittenItem], list[str], dict]:
    """Scrape one region's sold listings into tcg-raw.

    Returns (newly_written, log_lines, counts). The caller writes
    log_lines to tcg-raw/logs/{timestamp}.log and exposes counts
    in MaterializeResult.metadata.

    Exception tolerance: a Zyte SDK exception (timeout, request error,
    cross-loop) on the search-page call logs `STOP ... exc=...` and
    breaks. The same on a per-item call logs `FAIL zyte_exc event_id=...`
    and continues to the next item. The scraper does NOT propagate
    Zyte exceptions out of this function — a single bad page or item
    must not crash the whole `complete_eu_pipeline`.
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
    pages_timeout = 0
    items_timeout = 0
    empty_streak = 0
    found_items_on_any_page = False
    max_pages_stopped = False
    max_wall_clock_stopped = False

    start_monotonic = time.monotonic()

    while True:
        elapsed_s = time.monotonic() - start_monotonic

        # Wall-clock cap: stop the region before blowing the budget.
        if elapsed_s > MAX_WALL_CLOCK_S:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP max_wall_clock "
                f"elapsed_s={elapsed_s:.1f} max_wall_clock_s={MAX_WALL_CLOCK_S} "
                f"pages_fetched={pages_fetched}"
            )
            max_wall_clock_stopped = True
            break

        # Per-page heartbeat (emitted BEFORE the Zyte call so the operator
        # can see the page is in flight even if Zyte hangs).
        log.append(
            f"{datetime.now(timezone.utc).isoformat()} HEARTBEAT "
            f"search_page={page} elapsed_s={elapsed_s:.1f} "
            f"pages_fetched={pages_fetched} items_seen={items_seen}"
        )

        search_url = search_url_for_page_fn(page)
        log.append(
            f"{datetime.now(timezone.utc).isoformat()} FETCH "
            f"search_page={page} url={search_url}"
        )
        try:
            resp = zyte_client.get({"url": search_url, "browserHtml": True})
        except Exception as e:
            pages_timeout += 1
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} exc={type(e).__name__}: {str(e)[:200]}"
            )
            break
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
            try:
                item_resp = zyte_client.get({"url": item_url, "browserHtml": True})
            except Exception as e:
                items_timeout += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL zyte_exc "
                    f"event_id={event_id} exc={type(e).__name__}: {str(e)[:200]}"
                )
                continue
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

        # Max-pages cap: stop the region before blowing the budget.
        if page > MAX_PAGES_PER_REGION:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP max_pages "
                f"pages_fetched={pages_fetched} max_pages={MAX_PAGES_PER_REGION}"
            )
            max_pages_stopped = True
            break

    wall_clock_seconds = time.monotonic() - start_monotonic

    log.append(
        f"{datetime.now(timezone.utc).isoformat()} END region={region} "
        f"pages_fetched={pages_fetched} items_seen={items_seen} "
        f"items_skipped_already_seen={items_skipped_already_seen} "
        f"items_fetched_zyte={items_fetched_zyte} items_failed_zyte={items_failed_zyte} "
        f"items_failed_parse={items_failed_parse} "
        f"images_downloaded={images_downloaded} "
        f"images_skipped_already_seen={images_skipped_already_seen} "
        f"images_failed={images_failed} written={len(written)} "
        f"wall_clock_seconds={wall_clock_seconds} "
        f"max_pages_stopped={max_pages_stopped} "
        f"max_wall_clock_stopped={max_wall_clock_stopped}"
    )

    counts = {
        "pages_fetched": pages_fetched,
        "items_seen": items_seen,
        "items_skipped_already_seen": items_skipped_already_seen,
        "items_fetched_zyte": items_fetched_zyte,
        "items_failed_zyte": items_failed_zyte,
        "items_failed_parse": items_failed_parse,
        "images_downloaded": images_downloaded,
        "images_skipped_already_seen": images_skipped_already_seen,
        "images_failed": images_failed,
        "pages_timeout": pages_timeout,
        "items_timeout": items_timeout,
        "wall_clock_seconds": wall_clock_seconds,
        "max_pages_stopped": max_pages_stopped,
        "max_wall_clock_stopped": max_wall_clock_stopped,
        "written": len(written),
    }
    return written, log, counts


def _write_log(minio_client, log_lines: list[str]) -> bytes | None:
    """Write a run log to tcg-raw/logs/{timestamp}.log.

    Returns the written blob (also for test inspection), or None if
    the write itself failed.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M")
    log_blob = "\n".join(log_lines).encode("utf-8")
    try:
        minio_client.put_object(
            bucket_name=RAW_BUCKET,
            object_name=f"logs/{ts}.log",
            data=log_blob,
            length=len(log_blob),
            content_type="text/plain",
        )
        return log_blob
    except Exception:
        return None


@dg.asset(
    required_resource_keys={"zyte_session_resource", "tcg_raw_client"},
    metadata={"region": "DE"},
)
def scrape_ebay_de_raw(context: dg.AssetExecutionContext) -> list:
    """Scrape eBay DE sold-listings into tcg-raw.

    Writes per-item HTML to tcg-raw/ebay/DE/{event_id}.html and per-item
    images to tcg-raw/sold_images/DE/{event_id}.jpg. Skips event_ids
    that already have raw HTML persisted (atomic check on MinIO).
    Writes a run log to tcg-raw/logs/{timestamp}.log at end of run.

    The asset's MaterializeResult metadata surfaces live counters
    (pages_fetched, items_seen, written, max_pages_stopped, etc.) so
    the Dagster UI shows progress without parsing the run log file.
    """
    minio_client = context.resources.tcg_raw_client
    zyte_client = context.resources.zyte_session_resource

    written, log_lines, counts = _scrape_region(
        minio_client, zyte_client, "DE",
        de_search_url_for_page, parse_ebay_de_search_page,
    )
    _write_log(minio_client, log_lines)

    context.log.info(f"DE scrape complete: written={len(written)} counts={counts}")
    context.add_output_metadata(metadata=counts)
    return [
        {"event_id": w.event_id, "region": w.region, "sold_date": w.sold_date}
        for w in written
    ]


@dg.asset(
    required_resource_keys={"zyte_session_resource", "tcg_raw_client"},
    metadata={"region": "UK"},
)
def scrape_ebay_uk_raw(context: dg.AssetExecutionContext) -> list:
    """Scrape eBay UK sold-listings into tcg-raw. Symmetric to scrape_ebay_de_raw."""
    minio_client = context.resources.tcg_raw_client
    zyte_client = context.resources.zyte_session_resource

    written, log_lines, counts = _scrape_region(
        minio_client, zyte_client, "UK",
        uk_search_url_for_page, parse_ebay_uk_search_page,
    )
    _write_log(minio_client, log_lines)

    context.log.info(f"UK scrape complete: written={len(written)} counts={counts}")
    context.add_output_metadata(metadata=counts)
    return [
        {"event_id": w.event_id, "region": w.region, "sold_date": w.sold_date}
        for w in written
    ]

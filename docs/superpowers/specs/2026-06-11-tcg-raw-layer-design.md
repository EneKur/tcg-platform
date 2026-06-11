# Design: tcg-raw Layer — Persistent Raw HTML for Replayable Scrapes

**Date:** 2026-06-11
**Status:** Approved (pending user review)
**Author:** brainstorming session with user

## Problem

The current eBay DE/UK scraper is **single-stage**: each Zyte call
fetches HTML, immediately parses it, writes the parsed result to
`tcg-bronze/sold_data/{region}/{event_id}.parquet`, and inserts a
`fact_events` row. The raw HTML is **transient** — discarded as soon
as the function returns.

This causes three concrete problems:

1. **No replayability.** When a parser bug is found (e.g. the UK
   parser's `card_id` field is corrupted for non-TCG listings — see
   session log 2026-06-11), there is no way to fix the existing rows
   without re-paying Zyte API costs. The SQLite and MinIO contents are
   the *only* durable record, and they hold the *parsed* (broken)
   values.

2. **Idempotency is bolted onto the bronze write.** The current
   dedup check (`already_seen` set in `ebay_uk_sold_listings.py:39`)
   is derived from SQLite and updated *after* each row's processing
   (line 160). This works for the common case (an item that was
   already in SQLite from a previous run is skipped), but the
   dedup signal is **derived state**, not the durable artifact
   itself. If SQLite drifts from MinIO (vacuum gone wrong, manual
   edit, restore from backup), the scraper re-fetches items that
   already have raw HTML on disk. Moving the dedup check to a
   `stat_object` on `tcg-raw` makes the artifact itself the source
   of truth: the check is on the actual durable file, not a
   derived table.

3. **Search-page HTML is lost.** Even though search pages are
   ephemeral, the *item-page* HTML is durable: a given `event_id` URL
   always renders the same page. We pay for it once but throw it away.

## Approach

Introduce a new **`tcg-raw`** MinIO bucket that holds **bytes only**:
the raw item-page HTML and the raw item image, named by `event_id`
(the eBay item id extracted from the URL). The scraper asset's job
becomes "fetch from eBay, write to tcg-raw, log what happened." A new
**transformer** asset then reads tcg-raw, parses the HTML, and writes
the existing `tcg-bronze` parquets + SQLite. The scraper does not
know what a `card_id` is; the transformer does not know about eBay
cookies or Zyte.

```
                          tcg-raw bucket
┌──────────┐             ┌────────────────────────────────┐
│  eBay    │  ──fetch──▶ │ ebay/{DE,UK}/{event_id}.html   │ ──read──▶  parse
│  (live)  │  ──fetch──▶ │ sold_images/{DE,UK}/{event_id}.jpg│         │
└──────────┘             │ logs/{YYYY-MM-DD-HH-MM}.log   │         │
                         └────────────────────────────────┘         │
                                                                    ▼
                                                          tcg-bronze (existing)
                                                          • sold_data/{DE,UK}/{event_id}.parquet
                                                          • SQLite fact_events
```

**Key property:** `tcg-bronze` is now **rebuildable from `tcg-raw`**.
Deleting the entire `tcg-bronze/sold_data/`, `tcg-bronze/sold_images/`,
and both SQLite databases, then re-running the transformer, gives the
exact same result. This is the future-proofing: a parser bug becomes
a code change + a re-run, with no network.

### Why this is "bronze" in a new sense

`PROD.md:17-18` currently calls `tcg-bronze` "Raw scraped data —
unchanged from source." That language becomes ambiguous once
`tcg-raw` exists. After this change, `PROD.md` should be updated to
say:

> **Bronze layer:** Structured, parsed views of the source data,
> stored as parquet files in MinIO + tabular entries in SQLite. Every
> bronze row is **derivable from `tcg-raw`** by replaying the
> transformer — bronze is a cache, not a source of truth.
>
> **Raw layer (`tcg-raw`):** Bytes only — HTML, images, scrape logs.
> One object per eBay item, named by `event_id`. Write-once.

The old meaning of "bronze = raw from source" still holds in spirit
(both are derived from the source), but `tcg-raw` is now strictly
upstream.

## Design decisions (from brainstorming)

| Question | Decision |
|---|---|
| Where to store raw bytes? | New `tcg-raw` MinIO bucket. Per-item HTML at `ebay/{region}/{event_id}.html`, images at `sold_images/{region}/{item_id}.jpg`. |
| Save search-page HTML too? | No. Item pages are stable; search pages shift with every eBay sort. Not worth the storage. |
| Save item images? | Yes. Same dedup logic as HTML. Image is at `tcg-raw/sold_images/{region}/{item_id}.jpg` (replaces the current `tcg-bronze/sold_images/` location). |
| How does the scraper know if an item was already scraped? | `minio_client.stat_object("tcg-raw", f"ebay/{region}/{event_id}.html")` — single atomic check. If 200, skip. If `S3Error` / `NoSuchKey`, fetch and write. |
| Where does the dedup check happen in the loop? | **Before the Zyte call**, not after. The check is on the durable artifact (`tcg-raw/ebay/{region}/{event_id}.html`) instead of derived state (SQLite's `already_seen` set populated at run start). |
| What does the scraper return? | List of `(event_id, region)` tuples for items it wrote to tcg-raw. The transformer uses this list directly — no bucket scan needed. |
| What if the scraper crashes mid-run? | tcg-raw is partially populated, transformer reads what exists, run again to backfill the rest. Idempotent. |
| Scraper log? | One file per run at `tcg-raw/logs/YYYY-MM-DD-HH-MM.log`, written **at the end of the run**. Contains per-page and per-item fetch info. No card_id validity info — that's the transformer's job. |
| What does the log line look like? | Plain text, one line per event. Format: `2026-06-11 18:08:32 INFO search page 1 fetched 47 items from https://...`. See "Log format" below. |
| What happens to the existing 64+342 SQLite rows? | Stayed in SQLite as-is. Their raw HTML was never persisted, so they have no `event_id.html` in tcg-raw. A **one-time backfill asset** is included in this design to fetch and persist raw HTML for those URLs. |

## Components

### New module: `src/tcg_platform/defs/scrape_raw.py`

The new "scraper" — split from the existing `ebay_*_sold_listings.py`.
This asset does only network I/O. No parsing, no schema knowledge.

```python
import io
import logging
import re
from datetime import datetime, timezone
from typing import NamedTuple

import dagster as dg
from minio.error import S3Error
import requests

from tcg_platform.scraping.ebay_utils import extract_item_id
from tcg_platform.scraping.ebay_image import extract_item_image_url
from tcg_platform.scraping.ebay_de_search import (
    search_url_for_page as de_search_url_for_page,
    parse_ebay_de_search_page,
)
from tcg_platform.scraping.ebay_uk_search import (
    search_url_for_page as uk_search_url_for_page,
    parse_ebay_uk_search_page,
)

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
EMPTY_STREAK_THRESHOLD = 5  # unchanged from current scraper


class WrittenItem(NamedTuple):
    event_id: str
    region: str  # "DE" or "UK"


def _exists_in_raw(minio_client, region: str, event_id: str) -> bool:
    """Atomic existence check against tcg-raw.

    Used both for dedup (skip already-scraped items) and for skipping
    image downloads. A single stat_object call; NoSuchKey → not
    present; any other S3Error → log and treat as not present (safer
    to refetch than to silently skip).
    """
    try:
        minio_client.client.stat_object(
            RAW_BUCKET, f"ebay/{region}/{event_id}.html"
        )
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject"):
            return False
        _LOG.warning(f"stat_object unexpected error for {region}/{event_id}: {e}")
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
    lower = region.lower()
    log: list[str] = []
    log.append(f"{datetime.now(timezone.utc).isoformat()} START region={region}")

    written: list[WrittenItem] = []
    page = 1
    empty_streak = 0
    pages_fetched = 0
    items_seen = 0
    items_skipped_already_seen = 0
    items_fetched_zyte = 0
    items_failed_zyte = 0
    items_failed_parse = 0  # we don't parse, but log if HTML is empty
    images_skipped_already_seen = 0
    images_downloaded = 0
    images_failed = 0

    while True:
        search_url = search_url_for_page_fn(page)
        log.append(f"{datetime.now(timezone.utc).isoformat()} FETCH search_page={page} url={search_url}")
        resp = zyte_client.get({"url": search_url, "browserHtml": True})
        pages_fetched += 1
        if resp.get("statusCode") != 200:
            log.append(f"{datetime.now(timezone.utc).isoformat()} STOP search_page={page} status={resp.get('statusCode')}")
            break
        html = resp.get("browserHtml", "")
        if not html:
            log.append(f"{datetime.now(timezone.utc).isoformat()} STOP search_page={page} empty_html=true")
            break
        pairs = parse_search_page_fn(html)
        items_seen += len(pairs)
        log.append(f"{datetime.now(timezone.utc).isoformat()} PARSED search_page={page} items={len(pairs)}")
        if not pairs:
            log.append(f"{datetime.now(timezone.utc).isoformat()} STOP search_page={page} no_items=true")
            break

        for item_url, _sold_date in pairs:
            event_id = extract_item_id(item_url)
            if not event_id or not event_id.isdigit():
                log.append(f"{datetime.now(timezone.utc).isoformat()} SKIP bad_event_id url={item_url}")
                continue
            if _exists_in_raw(minio_client, region, event_id):
                items_skipped_already_seen += 1
                log.append(f"{datetime.now(timezone.utc).isoformat()} SKIP already_in_raw event_id={event_id}")
                continue

            # Fetch item page
            item_resp = zyte_client.get({"url": item_url, "browserHtml": True})
            items_fetched_zyte += 1
            if item_resp.get("statusCode") != 200:
                items_failed_zyte += 1
                log.append(f"{datetime.now(timezone.utc).isoformat()} FAIL zyte event_id={event_id} status={item_resp.get('statusCode')}")
                continue
            item_html = item_resp.get("browserHtml", "")
            if not item_html:
                items_failed_parse += 1
                log.append(f"{datetime.now(timezone.utc).isoformat()} FAIL empty_item_html event_id={event_id}")
                continue

            # Persist raw HTML
            try:
                minio_client.put_object(
                    bucket_name=RAW_BUCKET,
                    object_name=f"ebay/{region}/{event_id}.html",
                    data=item_html.encode("utf-8"),
                    length=len(item_html.encode("utf-8")),
                    content_type="text/html",
                )
            except Exception as e:
                log.append(f"{datetime.now(timezone.utc).isoformat()} FAIL put_object_html event_id={event_id} err={e}")
                continue

            written.append(WrittenItem(event_id=event_id, region=region))
            log.append(f"{datetime.now(timezone.utc).isoformat()} WROTE html event_id={event_id} bytes={len(item_html.encode('utf-8'))}")

            # Persist raw image
            img_url = extract_item_image_url(item_html)
            if img_url:
                img_path = f"sold_images/{region}/{event_id}.jpg"
                try:
                    minio_client.client.stat_object(RAW_BUCKET, img_path)
                    images_skipped_already_seen += 1
                    log.append(f"{datetime.now(timezone.utc).isoformat()} SKIP already_in_raw_image event_id={event_id}")
                except S3Error as e:
                    if e.code not in ("NoSuchKey", "NoSuchObject"):
                        log.append(f"{datetime.now(timezone.utc).isoformat()} WARN stat_image event_id={event_id} err={e}")
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
                        log.append(f"{datetime.now(timezone.utc).isoformat()} WROTE image event_id={event_id} bytes={len(img_data)}")
                    except Exception as img_e:
                        images_failed += 1
                        log.append(f"{datetime.now(timezone.utc).isoformat()} FAIL image event_id={event_id} err={img_e}")

        page += 1

    log.append(
        f"{datetime.now(timezone.utc).isoformat()} END region={region} "
        f"pages_fetched={pages_fetched} items_seen={items_seen} "
        f"items_skipped_already_seen={items_skipped_already_seen} "
        f"items_fetched_zyte={items_fetched_zyte} items_failed_zyte={items_failed_zyte} "
        f"images_downloaded={images_downloaded} images_skipped_already_seen={images_skipped_already_seen} "
        f"images_failed={images_failed} written={len(written)}"
    )
    return written, log


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client"},
    metadata={"region": "DE"},
)
def scrape_ebay_de_raw(context: dg.AssetExecutionContext) -> list:
    """Scrape eBay DE sold-listings into tcg-raw.

    Writes per-item HTML to tcg-raw/ebay/DE/{event_id}.html and per-item
    images to tcg-raw/sold_images/DE/{event_id}.jpg. Skips event_ids
    that already have raw HTML persisted (atomic check on MinIO).
    Writes a run log to tcg-raw/logs/{timestamp}.log at end of run.
    """
    minio_client = context.resources.minio_client
    zyte_client = context.resources.zyte_session_resource

    written, log_lines = _scrape_region(
        minio_client, zyte_client, "DE",
        de_search_url_for_page, parse_ebay_de_search_page,
    )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M")
    log_blob = "\n".join(log_lines).encode("utf-8")
    minio_client.put_object(
        bucket_name=RAW_BUCKET,
        object_name=f"logs/{ts}.log",
        data=log_blob,
        length=len(log_blob),
        content_type="text/plain",
    )

    context.log.info(f"DE scrape complete: written={len(written)}")
    return [{"event_id": w.event_id, "region": w.region} for w in written]


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client"},
    metadata={"region": "UK"},
)
def scrape_ebay_uk_raw(context: dg.AssetExecutionContext) -> list:
    """Scrape eBay UK sold-listings into tcg-raw. Symmetric to scrape_ebay_de_raw."""
    minio_client = context.resources.minio_client
    zyte_client = context.resources.zyte_session_resource

    written, log_lines = _scrape_region(
        minio_client, zyte_client, "UK",
        uk_search_url_for_page, parse_ebay_uk_search_page,
    )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M")
    log_blob = "\n".join(log_lines).encode("utf-8")
    minio_client.put_object(
        bucket_name=RAW_BUCKET,
        object_name=f"logs/{ts}.log",
        data=log_blob,
        length=len(log_blob),
        content_type="text/plain",
    )

    context.log.info(f"UK scrape complete: written={len(written)}")
    return [{"event_id": w.event_id, "region": w.region} for w in written]
```

### New module: `src/tcg_platform/defs/transform_bronze.py`

The new "transformer" — split from the existing `ebay_*_sold_listings.py`.
Reads tcg-raw, parses HTML, writes tcg-bronze parquets + SQLite. No
network. No Zyte.

```python
import io
import logging
from datetime import datetime, timezone

import dagster as dg
import pyarrow.parquet as pq

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
from tcg_platform.scraping.ebay_utils import extract_item_id
from tcg_platform.serialization.card_parquet import price_records_to_parquet

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
BRONZE_BUCKET = "tcg-bronze"


def _transform_region(
    minio_client: MinioClientResource,
    sqlite_client,
    region: str,
    written_items: list[dict],
    parse_item_page_fn,
) -> dict:
    """Read raw HTML for each written item, parse, write bronze parquet + SQLite.

    `written_items` is the list of {event_id, region} dicts returned
    by the scraper asset for this run. This function does NOT scan
    tcg-raw; it processes exactly the items the scraper just wrote.
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

        # Read raw HTML
        try:
            html = minio_client.get_object(
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
            minio_client.get_object(RAW_BUCKET, image_path)
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

        # Write bronze parquet + SQLite
        for rec in parsed:
            rec.local_image_path = image_path
            parquet_bytes, _ = price_records_to_parquet(
                [rec], rec.scraped_at.strftime("%Y-%m-%d")
            )
            minio_client.put_object(
                bucket_name=BRONZE_BUCKET,
                object_name=f"sold_data/{lower}/{event_id}.parquet",
                data=parquet_bytes,
                length=len(parquet_bytes),
                content_type="application/parquet",
            )
            counts["wrote_parquet"] += 1

            from tcg_platform.defs.bronze_ebay_sqlite_writer import _is_proxy_title
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
    required_resource_keys={"minio_client", "sqlite_client_de"},
)
def transform_ebay_de_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_de_raw: list,
) -> dg.MaterializeResult:
    minio_client = context.resources.minio_client
    sqlite_client = context.resources.sqlite_client_de

    counts = _transform_region(
        minio_client, sqlite_client, "DE",
        scrape_ebay_de_raw, parse_ebay_de_item_page,
    )
    context.log.info(f"DE transform: {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    required_resource_keys={"minio_client", "sqlite_client_uk"},
)
def transform_ebay_uk_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_uk_raw: list,
) -> dg.MaterializeResult:
    minio_client = context.resources.minio_client
    sqlite_client = context.resources.sqlite_client_uk

    counts = _transform_region(
        minio_client, sqlite_client, "UK",
        scrape_ebay_uk_raw, parse_ebay_uk_item_page,
    )
    context.log.info(f"UK transform: {counts}")
    return dg.MaterializeResult(metadata=counts)
```

### New module: `src/tcg_platform/defs/backfill_raw_html.py`

One-time backfill for the 64+342 rows that already exist in SQLite
but were scraped before tcg-raw existed.

```python
import logging
from datetime import datetime, timezone

import dagster as dg
from minio.error import S3Error

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.ebay_image import extract_item_image_url
from tcg_platform.scraping.ebay_utils import extract_item_id

_LOG = logging.getLogger(__name__)
RAW_BUCKET = "tcg-raw"


def _backfill_region(
    minio_client: MinioClientResource,
    zyte_client,
    sqlite_client,
    region: str,
) -> dict:
    """For each fact_event in SQLite whose source_url has no raw HTML yet,
    fetch the item page from eBay and persist raw HTML + image.

    This is the one-time job to populate tcg-raw for rows that were
    scraped before this design landed. After it runs once, tcg-raw
    is complete for the existing history and this asset can be deleted.
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
            minio_client.put_object(
                bucket_name=RAW_BUCKET,
                object_name=f"ebay/{upper}/{event_id}.html",
                data=html.encode("utf-8"),
                length=len(html.encode("utf-8")),
                content_type="text/html",
            )
            img_url = extract_item_image_url(html)
            if img_url:
                import requests
                try:
                    img_data = requests.get(img_url, timeout=30).content
                    minio_client.put_object(
                        bucket_name=RAW_BUCKET,
                        object_name=f"sold_images/{lower}/{event_id}.jpg",
                        data=img_data,
                        length=len(img_data),
                        content_type="image/jpeg",
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
```

### Modified: `src/tcg_platform/resources/minio_client.py`

The current `MinioClientResource` only knows about one bucket. It
needs to support the new `tcg-raw` bucket as well.

**Option chosen:** add a separate `tcg_raw_client` resource alongside
the existing `minio_client`, both pointing to the same MinIO endpoint
but different `bucket_name`. Cleanest separation; no risk of
accidentally writing to the wrong bucket.

```python
@resource
def tcg_raw_client(init_context: InitResourceContext):
    config = _get_minio_config(prefix="RAW")  # reads RAW_BUCKET env, default "tcg-raw"
    client = MinioClientResource(**config)
    return client.create_resource(init_context)
```

In `minio_resources.py`:

```python
def _get_minio_config(prefix: str = "MINIO") -> dict:
    return {
        "endpoint": os.getenv(f"{prefix}_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv(f"{prefix}_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv(f"{prefix}_SECRET_KEY", "minioadmin"),
        "bucket_name": os.getenv(f"{prefix}_BUCKET", "tcg-bronze"),
        "secure": False,
    }
```

The default `prefix="MINIO"` keeps the existing `tcg-bronze` default
working. The new resource uses `prefix="RAW"` → `RAW_BUCKET` env var
→ `tcg-raw` default.

### Modified: `src/tcg_platform/definitions.py`

Add the new resource to the resource map:

```python
from tcg_platform.defs.minio_resources import (
    minio_client,
    tcg_raw_client,  # NEW
)
...
resources={
    "currency_rates_db": currency_rates_db,
    "minio_client": minio_client,
    "tcg_raw_client": tcg_raw_client,  # NEW
    "sqlite_client_de": SqliteClientResource(db_path="./data/tcg_de.db"),
    "sqlite_client_uk": SqliteClientResource(db_path="./data/tcg_uk.db"),
    "zyte_session_resource": zyte_session_resource,
},
```

The new scraper assets use `tcg_raw_client` instead of `minio_client`.
The transformer uses `minio_client` for bronze writes and reads from
`tcg_raw_client`.

### Modified: `src/tcg_platform/defs/eu_pipeline_orchestrator.py`

The orchestrator changes. Today, the bronze side runs `ebay_de_pipeline`
and `ebay_uk_pipeline` in parallel. After this change, the equivalent
job is `scrape_de_raw + transform_de_to_bronze` per region. Same
orchestrator pattern, same parallel execution.

```python
ebay_de_raw_to_bronze_job = define_asset_job(
    name="ebay_de_raw_to_bronze",
    selection=["scrape_ebay_de_raw", "transform_ebay_de_to_bronze"],
    description="DE: scrape tcg-raw + transform to tcg-bronze",
)
ebay_uk_raw_to_bronze_job = define_asset_job(
    name="ebay_uk_raw_to_bronze",
    selection=["scrape_ebay_uk_raw", "transform_ebay_uk_to_bronze"],
    description="UK: scrape tcg-raw + transform to tcg-bronze",
)
```

The orchestrator runs these two jobs in parallel inside the existing
`bronze_eu_orchestrator`. Behavior is unchanged for downstream
assets (`backfill_de_asset`, `backfill_uk_asset`, `silver_eu_orchestrator`)
because their contract — "bronze has fresh parquets in
`tcg-bronze/sold_data/{region}/`" — is preserved.

### Modified: `src/tcg_platform/PROD.md`

Update the "Bronze layer" description (lines 17-18) per the new
contract. Add a "Raw layer" section above bronze. Update the
"MinIO Buckets" table at line 191-196 to add `tcg-raw`.

## Data flow

### Forward (new scrapes)

```
[ bronze_eu_orchestrator ]
        │
        ├──► ebay_de_raw_to_bronze_job
        │         ├── scrape_ebay_de_raw
        │         │      ├── GET ebay.de search page 1..N (Zyte)
        │         │      ├── for each new item: stat_object tcg-raw/ebay/DE/{id}.html
        │         │      ├── if missing: Zyte fetch item, put_object html
        │         │      ├── stat_object tcg-raw/sold_images/DE/{id}.jpg
        │         │      ├── if missing: requests.get(img_url), put_object jpg
        │         │      └── at end: put_object tcg-raw/logs/{ts}.log
        │         │
        │         └── transform_ebay_de_to_bronze  (deps on scrape_ebay_de_raw)
        │                ├── for each (event_id, region) returned by scraper:
        │                │   ├── get_object tcg-raw/ebay/DE/{id}.html
        │                │   ├── get_object tcg-raw/sold_images/DE/{id}.jpg (optional)
        │                │   ├── parse_ebay_de_item_page(html, ...)
        │                │   ├── put_object tcg-bronze/sold_data/DE/{id}.parquet
        │                │   └── INSERT OR IGNORE INTO fact_events
        │                └── return counts
        │
        └──► ebay_uk_raw_to_bronze_job  (parallel, same structure)
```

### Replay (future parser fix)

```
[ replay_bronze_from_raw_job ]  (one-off, when parser bug is fixed)
        │
        └── for each parquet in tcg-bronze/sold_data/{DE,UK}/:
              ├── get_object tcg-raw/ebay/{DE,UK}/{event_id}.html
              ├── re-parse with new logic
              ├── delete tcg-bronze/sold_data/{DE,UK}/{event_id}.parquet
              ├── put_object new tcg-bronze/sold_data/{DE,UK}/{event_id}.parquet
              ├── UPDATE fact_events SET ...  (or delete+insert)
              └── done
```

This is the future-proofing win. **No Zyte calls, no MinIO writes
to tcg-raw, no idempotency concerns.** Replay is a pure local job.

### Backfill (existing 64+342 rows)

```
[ backfill_raw_html_job ]  (one-off, run once after this design lands)
        │
        ├──► backfill_raw_html_de
        │         for each row in fact_events DE whose raw HTML is missing:
        │         ├── stat_object tcg-raw/ebay/DE/{id}.html
        │         ├── if missing: Zyte fetch + put_object (same as scraper)
        │         └── done
        │
        └──► backfill_raw_html_uk  (same for UK)
```

After this job runs once, the backfill assets can be deleted from
`defs/`. They're a one-time bootstrap, not a permanent pipeline
piece. (We'll keep them in the repo for at least one milestone in
case we need to re-backfill from a corrupted raw, then deprecate.)

## Log format

Each scraper run produces one file at `tcg-raw/logs/YYYY-MM-DD-HH-MM.log`.
Plain text, one line per event, ISO 8601 timestamp + level + message.

Example (`tcg-raw/logs/2026-06-11-18-08.log`):

```
2026-06-11T18:08:32+00:00 START region=DE
2026-06-11T18:08:32+00:00 FETCH search_page=1 url=https://www.ebay.de/sch/i.html?_nkw=One+Piece+TCG+PSA+10&_sacat=0&_from=R40&_rt=nc&LH_Sold=1
2026-06-11T18:08:35+00:00 PARSED search_page=1 items=47
2026-06-11T18:08:35+00:00 WROTE html event_id=117116140431 bytes=234512
2026-06-11T18:08:35+00:00 WROTE image event_id=117116140431 bytes=12480
2026-06-11T18:08:36+00:00 SKIP already_in_raw event_id=406820647572
2026-06-11T18:08:38+00:00 FAIL zyte event_id=999999999999 status=404
...
2026-06-11T18:09:12+00:00 END region=DE pages_fetched=7 items_seen=312 items_skipped_already_seen=235 items_fetched_zyte=77 items_failed_zyte=3 images_downloaded=77 images_skipped_already_seen=0 images_failed=2 written=77
2026-06-11T18:09:12+00:00 START region=UK
... (UK section) ...
```

**No card_id validity info** — that's the transformer's job, not
the scraper's. Scraper doesn't know what a "valid card" is.

## Edge cases

- **Crash mid-scrape:** tcg-raw is partially populated, the run
  returns whatever it has written, the transformer runs on what was
  written, the next scrape picks up where it left off. The crash
  log lines for the failed run are not written (we write the log at
  the end). The next run's log will show the surviving writes
  ("already_in_raw") for context.
- **Log write fails at end of run:** the scrape itself succeeded but
  we couldn't write the log. The asset's `MaterializeResult` reports
  this in metadata. No data loss — only observability loss.
- **tcg-raw bucket doesn't exist:** the `tcg_raw_client` resource's
  `_ensure_bucket_exists` creates it on first use, same pattern as
  the existing bronze bucket.
- **Image download fails (CDN 403, timeout):** the HTML is still
  written; the image is just missing. The transformer logs
  `image_missing=1` and proceeds without a `local_image_path`. Same
  behavior as the current code.
- **Item page Zyte call succeeds but page is empty (sold/removed):**
  the scraper writes an empty HTML file. The transformer will fail
  to parse it (no title), log `skipped_empty=1`, and not write to
  bronze. Same as today.
- **Two scraper instances run in parallel for the same region:**
  both see "missing" on the same event_id, both call Zyte, both
  `put_object` the same content. MinIO's PUT is idempotent (last
  write wins, content is identical). The log file timestamp differs
  by minute, so one will overwrite the other if they finish in the
  same minute — minor observability loss, no data loss. The
  transformer only runs once per region (one asset instance) and
  reads what exists.
- **Replay produces different result than original scrape:** this
  is exactly what we want — the new logic fixes the bug. The replay
  is an explicit operator action, not automated.

## Testing

### Unit tests in `tests/scraping/test_scrape_raw.py` (new)

1. `test_exists_in_raw_returns_false_for_missing_key` — `stat_object`
   raises `S3Error(code="NoSuchKey")` → `_exists_in_raw` returns
   False.
2. `test_exists_in_raw_returns_true_for_present_key` — `stat_object`
   succeeds → returns True.
3. `test_exists_in_raw_treats_other_errors_as_false` — `stat_object`
   raises non-NoSuchKey error → returns False (refetch is safer
   than skip).
4. `test_scrape_region_skips_already_in_raw` — given a fake MinIO
   client where `ebay/DE/12345.html` exists, the scraper does not
   call Zyte for that event_id. Verify via mock call counts.
5. `test_scrape_region_writes_html_and_image` — given a missing
   event_id, the scraper calls Zyte, writes the HTML to
   `ebay/DE/{id}.html`, and writes the image to
   `sold_images/DE/{id}.jpg`. Verify via captured put_object calls.
6. `test_scrape_region_stops_at_empty_streak` — 5 consecutive pages
   with no new items → loop exits.
7. `test_scrape_region_handles_failed_zyte_call` — Zyte returns
   status 500 for one item → item is skipped, others continue.
8. `test_scrape_region_writes_log_at_end` — after the loop, log
   blob is put at `logs/{ts}.log`. Verify via captured put_object
   calls.
9. `test_scrape_region_log_does_not_contain_card_ids` — the log
   only contains event_ids, status codes, byte counts. Verify by
   string search that the log blob has no `card_id` field name.

### Unit tests in `tests/scraping/test_transform_bronze.py` (new)

10. `test_transform_reads_raw_writes_bronze` — given a
    `scrape_ebay_de_raw` output of `[(12345, DE)]` and a tcg-raw
    bucket with `ebay/DE/12345.html` containing a known-good sample,
    the transformer writes a bronze parquet to
    `sold_data/DE/12345.parquet` and inserts a `fact_events` row.
    Verify via mock get_object + put_object + sqlite_client.
11. `test_transform_handles_missing_image_gracefully` — raw HTML
    exists, image does not → `local_image_path` is None, transform
    still succeeds.
12. `test_transform_handles_parse_failure` — HTML exists but parser
    returns [] (empty title, proxy, etc.) → `skipped_empty=1`,
    no bronze writes, no error.
13. `test_transform_filters_wrong_region_in_input` — if the
    scraper returns a UK event_id in its `written` list, the DE
    transformer ignores it (defensive against asset wiring bugs).

### Unit tests in `tests/scraping/test_backfill_raw_html.py` (new)

14. `test_backfill_skips_event_ids_already_in_raw` — given a
    SQLite row whose event_id is in tcg-raw, no Zyte call happens.
15. `test_backfill_fetches_and_writes_missing_event_ids` — given
    a SQLite row whose event_id is not in tcg-raw, Zyte is called
    once and raw HTML is persisted.
16. `test_backfill_counts` — return value has the right shape
    (`checked`, `already_have`, `fetched`, `failed`).

### Integration smoke test (manual, after merge)

1. `dg dev` from a clean checkout. Run `ebay_de_raw_to_bronze_job`.
   Confirm:
   - `tcg-raw/ebay/DE/{event_id}.html` exists for new items.
   - `tcg-raw/sold_images/DE/{event_id}.jpg` exists for new items.
   - `tcg-raw/logs/{ts}.log` exists with the expected format.
   - `tcg-bronze/sold_data/DE/{event_id}.parquet` exists (transformer).
   - SQLite `fact_events` has the new rows.
2. Re-run the same job. Confirm:
   - No new Zyte calls (verify by counting "WROTE" lines in the new
     log; should be ~0 if all items are already in raw).
   - Existing event_ids are skipped via "already_in_raw" log lines.
3. Run `backfill_raw_html_de_job`. Confirm:
   - All 64 DE rows in SQLite have a corresponding
     `tcg-raw/ebay/DE/{event_id}.html`.
4. (Future, after parser fix) Run a "replay" job to confirm
   `tcg-bronze` is rebuildable from `tcg-raw`. **This is the real
   future-proofing test — defer it to a follow-up design.**

## Out of scope

- **Auto-replay when parser changes.** Today, the transformer reads
  the `written` list from the scraper. A replay job needs a different
  input — it needs to read *all* of tcg-raw, not just the latest
  scraper's output. Designing that asset is its own task; for now,
  the user can re-run `ebay_de_raw_to_bronze_job` against an empty
  scraper output and a "replay" mode (out of scope).
- **TTL on raw objects.** Storage grows unboundedly as more eBay
  items are scraped. For a personal project at ~100 items/day,
  that's ~50 MB/day of HTML + ~10 MB/day of images. Negligible.
  Add a TTL only if storage becomes a concern.
- **Migrating images out of `tcg-bronze/sold_images/`.** Today,
  images live in `tcg-bronze`. After this change, they live in
  `tcg-raw/sold_images/`. The 57 DE + 65 UK images already in
  `tcg-bronze/sold_images/` stay there (they're tied to the parsed
  rows in SQLite via `local_image_path`). New images go to
  `tcg-raw/sold_images/`. Long-term, the `tcg-bronze` images
  become redundant; cleaning them up is a separate task.
- **Cross-region / cross-tenant image dedup.** Many items appear
  in both DE and UK; their images are identical bytes. We're
  storing them twice. For a personal project, this is fine — when
  the image file size becomes a real cost, switch to content-hash
  based paths (`sold_images/{sha256}.jpg`) instead of event_id paths.
- **Multi-source raw.** Today, raw is eBay-only. Limitless TCG and
  PriceCharting could be added later, but the bucket is structured
  to allow that: `tcg-raw/limitless/...` and `tcg-raw/pricecharting/...`
  slots are reserved by the design.
- **The UK parser's `card_id` corruption bug** flagged in the
  2026-06-11 session. This design makes the bug fixable via replay,
  but doesn't fix the bug itself. That's a separate task and
  benefits from a different design context (parser regex, not
  storage).

## Files modified

### New
- `src/tcg_platform/defs/scrape_raw.py` — defines `scrape_ebay_de_raw`
  and `scrape_ebay_uk_raw` assets.
- `src/tcg_platform/defs/transform_bronze.py` — defines
  `transform_ebay_de_to_bronze` and `transform_ebay_uk_to_bronze`
  assets.
- `src/tcg_platform/defs/backfill_raw_html.py` — defines
  `backfill_raw_html_de` and `backfill_raw_html_uk` assets
  (one-time, can be deprecated after first run).
- `tests/scraping/test_scrape_raw.py` — 9 unit tests.
- `tests/scraping/test_transform_bronze.py` — 4 unit tests.
- `tests/scraping/test_backfill_raw_html.py` — 3 unit tests.

### Modified
- `src/tcg_platform/defs/minio_resources.py` — add `tcg_raw_client`
  resource.
- `src/tcg_platform/resources/minio_client.py` — no change (the
  existing `MinioClientResource` is bucket-agnostic; the new
  `tcg_raw_client` resource just constructs it with a different
  `bucket_name`).
- `src/tcg_platform/definitions.py` — add `tcg_raw_client` to the
  resources map; add new jobs to the jobs list.
- `src/tcg_platform/defs/eu_pipeline_orchestrator.py` — replace
  references to `ebay_de_pipeline` / `ebay_uk_pipeline` with
  `ebay_de_raw_to_bronze` / `ebay_uk_raw_to_bronze`.
- `src/tcg_platform/PROD.md` — update the bronze description and
  add a raw layer section.
- `src/tcg_platform/.env.example` — add `RAW_BUCKET=tcg-raw`,
  `RAW_ACCESS_KEY=minioadmin`, `RAW_SECRET_KEY=minioadmin`,
  `RAW_ENDPOINT=localhost:9000` entries.

### Removed
- `src/tcg_platform/defs/ebay_de_sold_listings.py` — replaced by
  `scrape_ebay_de_raw` + `transform_ebay_de_to_bronze`. The
  scraping logic and the parsing logic are split across these two
  new assets.
- `src/tcg_platform/defs/ebay_uk_sold_listings.py` — same, replaced
  by `scrape_ebay_uk_raw` + `transform_ebay_uk_to_bronze`.
- `src/tcg_platform/scraping/ebay_image.py` — the image
  download/check helpers are inlined into `scrape_raw.py` because
  the bucket and path differ. (Or: keep `ebay_image.py` but
  parameterize the bucket; not yet decided at spec-time.)

### Asset/job discovery notes

`definitions.py` uses `@definitions` + `load_from_defs_folder` to
auto-discover all `@dg.asset` definitions in `defs/`. The 6 new
assets (2 scrapers, 2 transformers, 2 backfills) appear in the
Dagster UI automatically.

`@dg.define_asset_job` results are not auto-discovered — they must
be imported and added to the explicit `jobs=[...]` list in
`definitions.py`. The 3 new jobs (`ebay_de_raw_to_bronze`,
`ebay_uk_raw_to_bronze`, and the existing orchestrator's
`complete_eu_pipeline` and `backfill_*_job`) are wired in.

## Risk

**Low.** The change is additive in the new direction
(`tcg-raw` is new, transformer is new) and subtractive in the old
direction (the existing `ebay_de_sold_listings.py` and
`ebay_uk_sold_listings.py` are removed). Worst case: the new
pipeline doesn't run cleanly, and we revert by restoring the old
files from git. The data on disk is unaffected — `tcg-raw` is a
new bucket, `tcg-bronze` is still in its previous state until the
new transformer writes to it.

**One subtle risk:** the new `tcg-bronze` writes happen at slightly
different timestamps than the old code, so the `parqueted` column
in SQLite gets set to 1 by the new transformer (same as before).
The `backfill_sold_data_parquet` sensor (which reads
`get_unparqueted_fact_events`) will see no unbackfilled rows and
stay quiet — same as today. No behavior change.

**One open question for implementation:** when the new transformer
writes a parquet for an event_id that already has a parquet in
`tcg-bronze/sold_data/` (because the old code wrote it first), it
overwrites with the same content. The new content is parsed by the
new logic, so the file is structurally the same. **No data loss.**
The new logic might produce a different `card_id` than the old
one, but that delta is exactly what we want — it's the parser fix
expressed as data.

## Estimated effort

- `scrape_raw.py`: ~150 lines, mostly mechanical split from the
  existing scraper.
- `transform_bronze.py`: ~120 lines, mostly mechanical split.
- `backfill_raw_html.py`: ~80 lines, new logic but small.
- Tests: ~250 lines, mostly mock-driven per the existing
  `test_minio_remove_objects.py` pattern.
- `minio_resources.py` + `definitions.py` + `eu_pipeline_orchestrator.py`
  wiring: ~30 lines of changes.
- `PROD.md` + `.env.example`: ~15 lines of changes.

Net: ~650 lines of new code, ~150 lines of modified code, no
schema changes (the new tcg-raw bucket replaces the old in-bronze
sold_images/ and sold_data/ in spirit but not in storage).

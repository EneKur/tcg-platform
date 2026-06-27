"""Per-item tcg-raw → tcg-bronze writer.

Extracted from `src/tcg_platform/defs/transform_bronze.py:_transform_region`
so both the live transformer and the replay/gap-fill assets share one
implementation.
"""
from datetime import datetime, timezone
from typing import Callable

from tcg_platform.serialization.card_parquet import price_records_to_parquet


_VALID_MODES = ("fill", "overwrite")
BRONZE_BUCKET = "tcg-bronze"
_PROXY_INDICATORS = ["proxy", "dummy", "fake card", "replica"]


def _is_proxy_title(card_id: str) -> bool:
    card_lower = (card_id or "").lower()
    return any(ind in card_lower for ind in _PROXY_INDICATORS)


def transform_one_item(
    *,
    region: str,
    event_id: str,
    raw_html: str,
    image_path: str | None,
    bronze_minio_client,
    sqlite_client,
    parse_item_page_fn: Callable,
    mode: str,
    sold_date: str | None = None,
) -> dict:
    """Write one item's bronze parquet + (optionally) SQLite row.

    `image_path` is caller-supplied: this helper does NOT fetch images from
    MinIO — the caller is responsible for downloading any image and passing
    the local path (or `None`) in.

    `mode`:
      - "fill": skip if parquet exists; else write parquet + INSERT OR IGNORE SQLite
      - "overwrite": always re-parse; if parquet exists, remove + rewrite;
        SQLite row only inserted if no prior row exists (insert, not update)
    """
    if mode not in _VALID_MODES:
        raise ValueError(
            f"mode must be one of {_VALID_MODES!r}, got {mode!r}"
        )

    counts = {
        "mode": mode,
        "skipped_existing": 0,
        "wrote_parquet": 0,
        "wrote_sqlite": 0,
        "parse_failed": 0,
        "skipped_empty": 0,
        "parquet_write_failed": 0,
        "sqlite_write_failed": 0,
    }

    upper = region.upper()
    parquet_key = f"sold_data/{upper}/{event_id}.parquet"

    # fill mode: skip if parquet already exists
    if mode == "fill":
        try:
            bronze_minio_client.client.stat_object(BRONZE_BUCKET, parquet_key)
            counts["skipped_existing"] = 1
            return counts
        except Exception:
            pass  # NoSuchKey — expected, fall through to write
    else:
        # overwrite mode: if parquet exists, remove it
        try:
            bronze_minio_client.client.stat_object(BRONZE_BUCKET, parquet_key)
            bronze_minio_client.client.remove_object(BRONZE_BUCKET, parquet_key)
        except Exception:
            pass

    # Parse
    item_url = (
        f"https://www.ebay.de/itm/{event_id}" if upper == "DE"
        else f"https://www.ebay.co.uk/itm/{event_id}"
    )
    scraped_at = datetime.now(timezone.utc)
    try:
        parsed = parse_item_page_fn(raw_html, item_url, scraped_at)
    except Exception:
        counts["parse_failed"] = 1
        return counts

    if not parsed:
        counts["skipped_empty"] = 1
        return counts

    for rec in parsed:
        if sold_date and not rec.sold_date:
            rec.sold_date = sold_date
        rec.local_image_path = image_path
        try:
            parquet_bytes, _ = price_records_to_parquet(
                [rec], rec.scraped_at.strftime("%Y-%m-%d")
            )
        except ValueError:
            counts["parquet_write_failed"] += 1
            continue
        # Parquet write failures are NOT caught here — they propagate
        # out of the asset so the operator sees the failure loudly
        # rather than discovering partial state later.
        bronze_minio_client.put_object(
            bucket_name=BRONZE_BUCKET,
            object_name=parquet_key,
            data=parquet_bytes,
            length=len(parquet_bytes),
            content_type="application/parquet",
        )
        counts["wrote_parquet"] += 1

        if not _is_proxy_title(rec.card_id):
            try:
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
            except Exception:
                counts["sqlite_write_failed"] += 1

    return counts

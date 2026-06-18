import hashlib
import io

import pyarrow as pa
import pyarrow.parquet as pq

from tcg_platform.scraping.ebay_utils import extract_item_id


LIMITLESS_HOST = "onepiece.limitlesstcg.com"


def derive_event_id(source_url: str) -> str:
    """Return a non-empty, deterministic event_id for the given source URL.

    - eBay DE/UK item pages: the eBay item_id (already a unique sold event).
    - Limitless TCG card pages: f"limitless-{card_id}" (the source has no
      sold event; we synthesize a stable id from the card_id).
    - Anything else: f"unknown-{md5(source_url)[:8]}" (deterministic
      8-char suffix, non-empty, debuggable, cross-run stable).
    """
    if not source_url:
        return "unknown-0"
    source_url = source_url.split("?", 1)[0]
    if LIMITLESS_HOST in source_url:
        parts = source_url.rstrip("/").split("/")
        return f"limitless-{parts[-1].upper()}"
    if "ebay.de" in source_url or "ebay.co.uk" in source_url:
        item_id = extract_item_id(source_url)
        if item_id and item_id.isdigit():
            return item_id
    digest = hashlib.md5(source_url.encode()).hexdigest()[:8]
    return f"unknown-{digest}"


def card_records_to_parquet(
    cards: list, partition_date: str
) -> tuple[bytes, int]:
    """Serialize CardRecord list to a parquet blob.

    Changes from the 2026-06-10 pinned contract:
    - partition_date is written as a real column (was ignored).
    - scraped_at is sourced from partition_date for purity.
    """
    if not partition_date:
        raise ValueError("partition_date is required")
    scraped_at_iso = f"{partition_date}T00:00:00+00:00"
    rows = [
        {
            "card_id": c.card_id,
            "card_version": c.card_version or "",
            "card_name": c.card_name,
            "set_code": c.set_code,
            "rarity": c.rarity or "",
            "card_type": c.card_type,
            "attribute": c.attribute or "",
            "power": c.power or 0,
            "cost": c.cost or 0,
            "color": c.color or "",
            "source_url": c.source_url,
            "scraped_at": scraped_at_iso,
            "partition_date": partition_date,
        }
        for c in cards
    ]
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), len(rows)


def price_records_to_parquet(
    prices: list,
    partition_date: str,
    local_image_path_map: dict[str, str] | None = None,
) -> tuple[bytes, int]:
    """Serialize PriceRecord list to a parquet blob.

    Changes from the 2026-06-10 pinned contract:
    - event_id is derived from source_url (not always "").
    - image_url and local_image_path are passed through (not dropped).
    - local_image_path is backfilled from `local_image_path_map`
      (a {card_id: 'cards/{set}/{card_id}.webp'} dict) when the
      PriceRecord's own local_image_path is empty. Caller computes
      the map from MinIO `list_objects(prefix='cards/')`. The helper
      stays pure.
    - partition_date is written as a real column (was ignored).
    - scraped_at is sourced from partition_date for purity.
    """
    if not partition_date:
        raise ValueError("partition_date is required")
    scraped_at_iso = f"{partition_date}T00:00:00+00:00"
    path_map = local_image_path_map or {}
    rows = [
        {
            "event_id": derive_event_id(p.source_url),
            "card_id": p.card_id,
            "card_version": p.card_version or "",
            "event_type": p.event_type,
            "price": p.price,
            "currency": p.currency,
            "sold_date": p.sold_date or "",
            "scraped_from": p.scraped_from,
            "source": p.source,
            "source_url": p.source_url,
            "language": getattr(p, "language", "EN") or "EN",
            "scraped_at": scraped_at_iso,
            "image_url": getattr(p, "image_url", None) or "",
            "local_image_path": (
                getattr(p, "local_image_path", None)
                or path_map.get(p.card_id, "")
            ),
            "title": getattr(p, "title", None) or "",
            "partition_date": partition_date,
        }
        for p in prices
    ]
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), len(rows)


def build_local_image_path_map(minio_client) -> dict[str, str]:
    """Read tcg-bronze/cards/ from MinIO and return a {card_id: path} map.

    The serializer uses this to backfill `local_image_path` for
    Limitless rows. Empty dict if no images are present.

    `minio_client.list_objects(bucket, prefix)` returns a list of
    object name strings (see `MinioClientResource.list_objects`).
    """
    out: dict[str, str] = {}
    for obj_name in minio_client.list_objects("tcg-bronze", prefix="cards/"):
        parts = obj_name.split("/")
        if len(parts) != 3:
            continue
        filename = parts[2]
        for suffix in ("_v1", "_v2", "_v3", "_v4"):
            if filename.endswith(suffix + ".webp"):
                filename = filename[: -(len(suffix) + 5)]
                break
        else:
            if filename.endswith(".webp"):
                filename = filename[:-5]
        out[filename.upper()] = obj_name
    return out
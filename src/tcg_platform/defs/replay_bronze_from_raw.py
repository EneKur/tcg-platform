"""Replay bronze from raw: enumerate tcg-raw, re-parse, write bronze.

Two modes:
  - fill: skip if bronze parquet exists; else write parquet + SQLite
    row. Used to close the raw-no-bronze gap (89 DE + 61 UK rows).
  - overwrite: always re-parse; if bronze parquet exists, remove +
    rewrite. SQLite row untouched (historical record preserved).
    Used for parser-bug-driven replays.

Per-item contract lives in `tcg_platform.serialization.bronze_writer.
transform_one_item`. These assets are a thin enumeration loop.
"""
import logging
from typing import Callable

import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.bronze_writer import (
    _VALID_MODES,
    transform_one_item,
)

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"


def _enumerate_raw_keys(raw_minio_client: MinioClientResource, region: str) -> list[str]:
    """List raw HTML object names for a region. Returns sorted keys."""
    prefix = f"ebay/{region.upper()}/"
    keys = raw_minio_client.client.list_objects(RAW_BUCKET, prefix=prefix, recursive=True)
    return sorted(k for k in keys if k.endswith(".html"))


def _read_html(raw_minio_client, region: str, event_id: str) -> str | None:
    """Read raw HTML bytes → decoded str. Returns None on failure."""
    try:
        html_bytes = raw_minio_client.get_object(
            RAW_BUCKET, f"ebay/{region.upper()}/{event_id}.html"
        )
        return html_bytes.decode("utf-8")
    except Exception as e:
        _LOG.warning(f"Read html failed for {event_id}: {e}")
        return None


def _read_image_or_none(raw_minio_client, region: str, event_id: str) -> str | None:
    """Try to read raw image bytes; return the path string if present,
    None if missing or unreadable."""
    image_path = f"sold_images/{region.lower()}/{event_id}.jpg"
    try:
        raw_minio_client.get_object(RAW_BUCKET, image_path)
        return image_path
    except Exception:
        return None


def _run_replay(
    context: dg.AssetExecutionContext,
    region: str,
    parse_item_page_fn: Callable,
    sqlite_client,
) -> dg.MaterializeResult:
    """Shared asset body for DE / UK replay."""
    config = context.op_config or {}
    mode = config.get("mode", "fill")
    if mode not in _VALID_MODES:
        raise ValueError(f"mode must be one of {_VALID_MODES!r}, got {mode!r}")

    raw_client = context.resources.tcg_raw_client
    bronze_client = context.resources.minio_client

    keys = _enumerate_raw_keys(raw_client, region)
    counts = {
        "mode": mode,
        "read_html": 0,
        "read_failed": 0,
        "read_image_ok": 0,
        "read_image_missing": 0,
        "skipped_existing": 0,
        "wrote_parquet": 0,
        "wrote_sqlite": 0,
        "parse_failed": 0,
        "skipped_empty": 0,
        "parquet_write_failed": 0,
        "sqlite_write_failed": 0,
    }

    for key in keys:
        event_id = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        html = _read_html(raw_client, region, event_id)
        if html is None:
            counts["read_failed"] += 1
            continue
        counts["read_html"] += 1

        image_path = _read_image_or_none(raw_client, region, event_id)
        if image_path is not None:
            counts["read_image_ok"] += 1
        else:
            counts["read_image_missing"] += 1

        item_counts = transform_one_item(
            region=region,
            event_id=event_id,
            raw_html=html,
            image_path=image_path,
            bronze_minio_client=bronze_client,
            sqlite_client=sqlite_client,
            parse_item_page_fn=parse_item_page_fn,
            mode=mode,
            sold_date=None,
        )
        for k, v in item_counts.items():
            if k in counts and k != "mode":
                counts[k] += v

    context.log.info(f"{region.upper()} replay ({mode}): {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    config_schema={"mode": str},
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_de"},
)
def replay_bronze_from_raw_de(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Replay (or gap-fill) DE raw HTML → tcg-bronze parquet + SQLite row."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    return _run_replay(
        context, "DE", parse_ebay_de_item_page,
        context.resources.sqlite_client_de,
    )


@dg.asset(
    config_schema={"mode": str},
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_uk"},
)
def replay_bronze_from_raw_uk(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Replay (or gap-fill) UK raw HTML → tcg-bronze parquet + SQLite row."""
    from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
    return _run_replay(
        context, "UK", parse_ebay_uk_item_page,
        context.resources.sqlite_client_uk,
    )


replay_bronze_from_raw_job = dg.define_asset_job(
    name="replay_bronze_from_raw_job",
    selection=[
        dg.AssetKey("replay_bronze_from_raw_de"),
        dg.AssetKey("replay_bronze_from_raw_uk"),
    ],
)
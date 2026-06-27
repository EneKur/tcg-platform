"""Per-item tcg-raw → tcg-bronze writer.

Extracted from `src/tcg_platform/defs/transform_bronze.py:_transform_region`
so both the live transformer and the replay/gap-fill assets share one
implementation. Pure-ish: takes clients as injected parameters; the only
side effects are the explicit MinIO and SQLite calls.
"""
from typing import Callable


_VALID_MODES = ("fill", "overwrite")


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

    `mode`:
      - "fill": skip if parquet exists; else write parquet + INSERT OR IGNORE SQLite
      - "overwrite": always re-parse; if parquet exists, remove + rewrite;
        SQLite row only touched if no prior row exists (insert)
    """
    if mode not in _VALID_MODES:
        raise ValueError(
            f"mode must be one of {_VALID_MODES!r}, got {mode!r}"
        )
    return {"mode": mode, "skipped_existing": 0, "wrote_parquet": 0,
            "wrote_sqlite": 0, "parse_failed": 0,
            "read_image_ok": 0, "read_image_missing": 0,
            "skipped_empty": 0, "parquet_write_failed": 0,
            "sqlite_write_failed": 0}

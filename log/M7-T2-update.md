# M7-T2 Update: Per-Item-ID Silver Parquet Layout

**Date:** 2026-06-05
**Status:** Complete
**Branch:** `2026-06-03-uk-date-parser-investigation`

## Summary

Changed the silver layer to write one parquet file per item_id, mirroring
the bronze layer's per-item layout. Previously the silver layer wrote a
single aggregated `data.parquet` per region per bucket (`data/`, `quarantine/`).
The new layout:

- `tcg-silver/data/{region}/{event_id}.parquet` — one file per valid item
- `tcg-silver/quarantine/{region}/{event_id}.parquet` — one file per quarantined item

`{event_id}` is the eBay item_id (extracted from `source_url` via
`extract_item_id()`).

## Why

- Symmetry with bronze (`sold_data/{region}/{item_id}.parquet`).
- The `event_id` column was an empty string in bronze. With per-item-id
  filenames, populating `event_id` with the item_id makes the column
  self-describing and matches the filename.
- Per-item files make it easy to inspect/single out a specific sale
  event without reading the whole aggregated dataset.

## Collision check

When writing a row whose `event_id` already has a file in the destination
prefix, the writer compares the `(sold_date, event_id, title)` tuple:

- **All three match** → overwrite in place (re-scrape dedupe).
- **Any differ** → find next free `{event_id}_1.parquet`, `_2`, etc.

`sold_date + event_id` is already unique in normal operation (eBay never
reuses an item_id for the same sale date); `title` is included as a
robustness check to dedupe re-scrapes where title text varies slightly.

## Cleanup

On the first run after this change, the four legacy aggregated files
(`data/{region}/data.parquet` and `quarantine/{region}/data.parquet`)
are deleted. No historical migration; start clean from this point.
The new per-item files are the only state going forward.

## Files

**Modified:**
- `src/tcg_platform/defs/silver_transform.py` — new `_write_silver_parquet`,
  `_resolve_collision_path`, `_cleanup_legacy_aggregated_files`,
  `is_valid_card_id` (extracted from closure); `_run_silver_transform`
  rewired to use per-item writer.
- `src/tcg_platform/resources/minio_client.py` — new `remove_objects()`
  batch delete.
- `tests/scraping/test_silver_file_writer.py` — 14 tests covering
  per-item-id writer, collision check, suffix scheme, cleanup, NaN
  title handling, legacy title=0.0 backward compat.

**New:**
- `tests/scraping/test_silver_card_id_validation.py` — 9 tests for
  `is_valid_card_id` covering normalized and unnormalized forms.
- `scripts/smoke_silver_transform.py` — programmatic smoke test using
  Dagster's `materialize()` against real MinIO.

## Bug fixes surfaced during smoke (Task 4)

The smoke test revealed three live-data issues that the unit tests
didn't catch:

### 1. `is_valid_card_id` regex missed already-normalized card_ids

The M7-T1 silver transform used regex `(OP\d+|EB\d+|ST\d+|PRB\d+|P\d+)`
which extracts just the set code (`OP11`) from an already-normalized
card_id (`OP11-080`). The new M6.5-T1 scraper emits card_ids in their
final form, so the previous logic quarantined **every** valid record.

**Fix:** `is_valid_card_id` now tries the raw input against the
cardlist first, falling back to regex extraction for unnormalized forms
like `ST13003`. Extracted to module level for testability.

### 2. NaN title from Spark's `toPandas`

Spark's `toPandas()` converts `None` to `NaN` (a float) for str-dtype
columns. The writer's `is None` check missed NaN, and the fillna loop's
`dtype == object` check didn't match the new pandas `str` dtype. Result:
title column got stored as `float64(0.0)`, and re-runs of the collision
check saw `0.0 != ''` → spurious `_1` files on every re-run.

**Fix:** `is_valid_card_id`'s caller normalizes `pd.isna(v) → ""`; the
fillna loop now matches `object`, `str`, and `string` dtypes.

### 3. Float sentinel in legacy files

Pre-fix silver files have `title=0.0` (float). To make the collision
check tolerant of these legacy files, `_extract_identity_tuple` treats
`0.0` and `0` as `""`.

## Verification

- `pytest tests/scraping/test_silver_file_writer.py tests/scraping/test_silver_card_id_validation.py` → 23/23 pass
- `pytest tests/` → 63/65 pass (2 pre-existing `test_exchange_rate.py` failures, out of scope)
- `python -c "from tcg_platform.definitions import defs; defs(); print('OK')"` → OK
- `python scripts/smoke_silver_transform.py` × 2 → idempotent

**Live state after smoke:**

| Bucket | Count | _N suffix |
|---|---|---|
| `data/de/` | 21 | 0 |
| `data/uk/` | 33 | 0 |
| `quarantine/de/` | 6 | 0 |
| `quarantine/uk/` | 25 | 0 |

- All 85 files: `event_id` column matches filename (0 mismatches)
- Legacy `data/{region}/data.parquet` files: gone
- Re-run is a no-op: same file count, same content, no suffix files

## Out of scope

- Historical silver data preservation (explicitly opted out per spec)
- TTL/expiration of old per-item-id files
- Performance optimization of per-row `list_objects` (negligible at
  current scale; perf note in `_run_silver_transform` documents the
  batch pre-list upgrade path if row counts grow)

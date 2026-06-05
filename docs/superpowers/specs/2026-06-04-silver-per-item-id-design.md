# Per-Item-ID Silver Parquet Files — Design Spec

**Date:** 2026-06-04
**Status:** Draft (awaiting user review)
**Author:** opencode + user

## Problem

The current silver layer writes a single aggregated `data.parquet` per region
per bucket:

- `tcg-silver/data/de/data.parquet` (640 rows, all valid DE items)
- `tcg-silver/data/uk/data.parquet` (998 rows, all valid UK items)
- `tcg-silver/quarantine/de/data.parquet` (27 rows, invalid card_ids)
- `tcg-silver/quarantine/uk/data.parquet` (58 rows, invalid card_ids)

This layout diverges from the bronze layer (`sold_data/{region}/{item_id}.parquet`)
which is one-file-per-item. The user wants the silver layer to mirror bronze
granularity for consistency.

Additionally, the `event_id` column is currently an empty string in every row
since the bronze writer doesn't populate it. With per-item-id filenames, the
natural choice is to populate `event_id` with the eBay item_id so that the
column matches the filename.

## Goals

1. Silver layer output is one parquet file per item_id, mirroring the bronze layout.
2. `event_id` column is populated with the eBay item_id (same as the filename).
3. Re-runs of the silver transform are idempotent: same data → same files (overwrite in place), different data → no data loss (`_x` suffix scheme).
4. Quarantine bucket uses the same per-item-id layout.
5. No migration of historical aggregated files; old files are deleted as part of this change.

## Non-Goals

- Backfill from existing aggregated `data.parquet` files (user explicitly opted out).
- New silver logic (card_id validation, currency normalization, etc.) — only the file layout changes.
- Changes to the bronze layer (already correct).

## File layout (after)

```
tcg-silver/
├── data/
│   ├── de/
│   │   ├── 127860244828.parquet       # event_id 127860244828 (1 row, valid)
│   │   ├── 157956915219.parquet       # event_id 157956915219 (1 row, valid)
│   │   └── ...
│   └── uk/
│       ├── 157645495391.parquet
│       └── ...
└── quarantine/
    ├── de/
    │   ├── 306933963547.parquet       # event_id 306933963547 (1 row, invalid card_id)
    │   └── ...
    └── uk/
        └── ...
```

Each parquet contains exactly one row (the per-item silver view of that sale event). Quarantine files have the same per-item layout — invalid card_ids are quarantined per item, not aggregated.

## event_id column

Every silver row's `event_id` field = the eBay item_id extracted from `source_url`.

Example:
- `source_url = "https://www.ebay.de/itm/127860244828"` → `event_id = "127860244828"`

Extraction uses the existing `extract_item_id(url)` from `tcg_platform.scraping.ebay_utils` (DRY with bronze).

## Collision check algorithm (re-run behavior)

For each row from the silver transform:

1. Extract `event_id` from `source_url` via `extract_item_id()`.
2. Determine the destination prefix: `data/{region}/` or `quarantine/{region}/`.
3. Check if `{prefix}/{event_id}.parquet` already exists in MinIO.
4. **If no file exists** → write to `{prefix}/{event_id}.parquet`. Done.
5. **If file exists** → read the existing file (1 row, 14 columns).
6. Compare the tuple `(sold_date, event_id, title)` of the new row against the existing row.
7. **If all three match** → overwrite the file in place. This is a re-scrape of the same data; no data loss, no duplication.
8. **If any differ** → this is a true collision (same item_id, different sale event, or different title scrape). Find next free suffix:
   - Try `{prefix}/{event_id}_1.parquet`. If exists, read its tuple and compare.
   - If matches → overwrite.
   - If differs → try `{event_id}_2.parquet`, etc.
   - Stop at the first free slot (or first matching slot).

The `(sold_date, event_id, title)` tuple is the identity of a sale event in silver. In normal operation (eBay gives unique item_ids), `sold_date + event_id` is already unique; `title` is included as a robustness check to handle re-scrapes of the same item where the title text differs slightly (e.g. capitalization, spacing) — those should be dedupes, not collisions.

The full string compare is on a tuple of 3 short strings (max ~200 chars each), not the full row. Fast.

## Why (sold_date, event_id, title) and not just (event_id, title)?

eBay's item_id is unique per listing. Re-listing the same item gets a new item_id. So `event_id` alone is the unique identity. But the user noted that the sold_date is also stable — "ebay would definitely NOT use the same item_id for that exact date the item was sold twice." Including `sold_date` in the tuple is redundant for the normal case but makes the intent explicit: we treat the silver row as immutable per (item_id, sold_date). Title is the third axis to handle scrape-time variance.

## Cleanup of old aggregated files

On the next silver run, delete the four existing aggregated files:

- `tcg-silver/data/de/data.parquet`
- `tcg-silver/data/uk/data.parquet`
- `tcg-silver/quarantine/de/data.parquet`
- `tcg-silver/quarantine/uk/data.parquet`

Per "no migration — start clean from this point." Going forward, only per-item-id files exist.

The cleanup runs at the start of `_run_silver_transform()` for each region, before the cardlist validation step. If cleanup fails (MinIO unavailable), log a warning and continue — the new per-item-id files will still be written, but the old aggregated files will linger until manually deleted.

## Error handling

- **`source_url` has no item_id** (`extract_item_id` returns the original URL): log warning, skip the row. Can't determine event_id → no file to write to.
- **MinIO read failure during collision check** (existing file present but can't be read): log warning, treat as "no match" → write to the base path `{event_id}.parquet` with `_0` suffix. Avoids overwriting corrupt data.

## Components

### `src/tcg_platform/scraping/ebay_utils.py` (no change)

`extract_item_id(url) -> str` already exists. Returns digits or original URL.

### `src/tcg_platform/defs/silver_transform.py` (modified)

- New helper: `_write_silver_parquet(minio_client, row, dest_prefix, region)` — writes one row to a per-item-id path with collision check.
- New helper: `_cleanup_old_aggregated_files(minio_client, region)` — deletes `data/{region}/data.parquet` and `quarantine/{region}/data.parquet` if they exist.
- `_run_silver_transform()`: calls cleanup, then iterates valid + quarantine rows, calling `_write_silver_parquet` for each.

### `tests/scraping/test_silver_file_writer.py` (new)

TDD tests for the new write logic (see Testing section).

## Data flow (after)

```
bronze parquets  →  Spark read  →  cardlist validation
                                         │
                       ┌─────────────────┴─────────────────┐
                       │                                   │
                  valid rows                        quarantined rows
                       │                                   │
                       └──────────┬────────────────────────┘
                                  │
                  for each row: extract event_id
                                  │
                  ┌───────────────┼───────────────┐
                  │               │               │
            no file yet     file matches   file mismatches
                  │               │               │
              write base    overwrite in    find next _x
                             place            suffix
```

## Testing strategy

TDD throughout. New test file `tests/scraping/test_silver_file_writer.py` with these tests:

1. `test_writes_per_item_id_file` — 3 rows, 3 distinct files exist
2. `test_event_id_populated_from_source_url` — `event_id` column matches extracted item_id
3. `test_overwrites_when_tuple_matches` — write a file, re-run with same (sold_date, event_id, title) → file overwritten, no `_x` suffix
4. `test_adds_suffix_when_sold_date_differs` — same item_id, different sold_date → `{id}_1.parquet` created
5. `test_adds_suffix_when_title_differs` — same item_id+date, different title → `{id}_1.parquet` created
6. `test_increments_suffix_until_free` — write `_1` and `_2`, re-run with new tuple → `_3.parquet` created
7. `test_writes_to_quarantine_for_invalid_card_id` — invalid card_id → `quarantine/{region}/` prefix
8. `test_extract_event_id_from_ebay_url` — unit test for the extraction (or reuse `test_ebay_utils.py`)

The tests use a mocked `MinioClientResource` (the test pattern used elsewhere in the repo, e.g., `test_minio_client.py` if it exists) or a real MinIO against the running container (the existing pipeline tests use real MinIO).

## Files

**Modify:**
- `src/tcg_platform/defs/silver_transform.py` — change write logic to per-item-id

**Create:**
- `tests/scraping/test_silver_file_writer.py` — tests for new write logic
- `log/M7-T2-update.md` — log entry for this change (in line with existing log/ pattern)

**No file deletion in git** — the old `data.parquet` files in MinIO are runtime artifacts, not tracked.

## Success criteria

1. `python -c "from tcg_platform.definitions import defs; print('OK')"` passes
2. All new tests pass
3. All previously-passing tests still pass (the 2 pre-existing `test_exchange_rate.py` failures are out of scope)
4. After running `silver_de_transform` + `silver_uk_transform`:
   - `tcg-silver/data/{region}/` contains one file per valid item_id (no aggregated `data.parquet`)
   - `tcg-silver/quarantine/{region}/` contains one file per quarantined item_id
   - Each file's `event_id` column matches the filename
   - File count = row count of the corresponding (pre-change) aggregated file
5. Re-running the transform with the same bronze data is a no-op (no new files, no content changes)
6. The `_x` suffix scheme can be observed in a manual test where a row's title is artificially changed between runs

## Out of scope

- Performance optimization of the collision check (currently O(1) per file via `stat_object`; can be batched if needed)
- TTL/expiration of old files
- Migration of any historical aggregated files

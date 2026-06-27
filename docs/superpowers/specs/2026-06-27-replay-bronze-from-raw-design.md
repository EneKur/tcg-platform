# Design: M9-T2 — Replay Bronze from Raw + Fill the 179-Row Raw-No-Bronze Gap

**Date:** 2026-06-27
**Status:** Approved
**Branch:** `2026-06-27-m9-t2-replay-bronze-from-raw`
**Author:** brainstorming session with user

## Problem

M9-T1 (completed 2026-06-11) introduced `tcg-raw`, the persistent raw HTML
bucket. It established two properties:

1. The `tcg-raw/ebay/{DE,UK}/{event_id}.html` files are now the durable
   artifact of every scrape.
2. The transformer (`src/tcg_platform/defs/transform_bronze.py`) reads
   them and produces `tcg-bronze/sold_data/{region}/{event_id}.parquet`
   + `fact_events` SQLite rows.

But two operational gaps remain:

### Gap 1: 179 raw HTML files have no bronze parquet

Inventory measured on 2026-06-27:

| Region | Raw HTML | Bronze parquet | Raw-no-bronze gap |
| ------ | -------- | -------------- | ----------------- |
| DE     | 165      | 76             | **89**            |
| UK     | 447      | 386            | **61**            |

89 + 61 = 150, not 179. (The 179 in the title is the user's ballpark; the
precise measured gap is 150.) These are real sold-item HTML files sitting
in `tcg-raw` that the live transformer never processed. They were either
written by the live scraper but never fed to its own transformer in the
same run (e.g. orchestrator failure mid-pipeline), or by ad-hoc scripts.
Whatever the cause, the data is in raw but invisible to silver/quarantine.

### Gap 2: No replay mechanism for parser-bug fixes

The M9 spec (`docs/superpowers/specs/2026-06-11-tcg-raw-layer-design.md`,
replay diagram at line 765, deferral at lines 930–932) explicitly
deferred the replay job:

> 4. (Future, after parser fix) Run a "replay" job to confirm
>    `tcg-bronze` is rebuildable from `tcg-raw`. **This is the real
>    future-proofing test — defer it to a follow-up design.**

Today, if the UK parser's `card_id` extraction breaks for a subset of
listings (a known class of bug — flagged in
log/SESSION_2026-06-11.md), the only recovery is to re-pay Zyte API
costs by re-scraping. The raw HTML is sitting right there, but there's
no way to use it.

## Approach

Introduce **one new Dagster job** that operates in two modes:

- **`fill`** — enumerate `tcg-raw/ebay/{region}/*.html`, write a bronze
  parquet + SQLite row only if no parquet exists yet. Closes Gap 1.
- **`overwrite`** — same enumeration, but always re-parse and rewrite
  the bronze parquet. For event_ids that already have a parquet, the
  SQLite row is left untouched (per the explicit user decision —
  SQLite is treated as the immutable historical log of parser outputs
  at the time of the most recent run). For event_ids with no prior
  parquet or SQLite row, the SQLite `INSERT OR IGNORE` runs as in
  fill mode (no historical row exists to preserve). Closes Gap 2.

The per-item work is extracted from the existing
`src/tcg_platform/defs/transform_bronze.py:_transform_region` into a
new pure helper, `transform_one_item`, in
`src/tcg_platform/serialization/bronze_writer.py`. The existing live
transformer is refactored to call the same helper in `fill` mode —
behavior preserved, no contract change, no test regressions.

Two per-region assets (`replay_bronze_from_raw_de` / `_uk`) wrap the
helper in a loop over the raw bucket. A single Dagster job selects
both assets for parallel execution.

## Architecture

```
                ┌────────────────────────────────────────┐
                │  replay_bronze_from_raw_{de,uk}        │  (NEW)
                │   • enumerate tcg-raw/ebay/{region}/   │
                │   • for each .html: call helper(mode)  │
                └─────────────────┬──────────────────────┘
                                  │
                                  ▼
                ┌────────────────────────────────────────┐
                │  bronze_writer.transform_one_item      │  (NEW)
                │  mode ∈ {fill, overwrite}:             │
                │    • fill     — skip if parquet exists │
                │    • overwrite— remove + rewrite       │
                │  SQLite untouched only when prior      │
                │  parquet exists (historical row kept)  │
                └─────────────────┬──────────────────────┘
                                  ▲
                                  │ (also called by)
                                  │
                ┌────────────────────────────────────────┐
                │  transform_ebay_{de,uk}_to_bronze      │  (REFACTORED)
                │  Wraps helper in fill mode, takes     │
                │  written_items list from scraper       │
                └────────────────────────────────────────┘
```

The factory pattern (`make_replay_asset(region)`) is **not** used here
because:

1. The existing transform_bronze.py uses two hand-written asset bodies,
   not factories. Introducing one asset-style and one factory-style in
   the same logical group is inconsistent.
2. The M8-T7 factory refactor (PROD.md:141) was applied to the
   reconcile + silver-transform asset pairs, not to the original
   per-region assets that pre-date that refactor.

Per-region assets, parallel within the job, matches the established
convention in this repo.

## Components

### `transform_one_item` — pure helper

**Location:** `src/tcg_platform/serialization/bronze_writer.py` (new)

**Signature:**
```python
def transform_one_item(
    *,
    region: str,                        # "DE" or "UK"
    event_id: str,                      # eBay item id
    raw_html: str,                      # parsed-ready HTML bytes-as-str
    image_path: str | None,             # "sold_images/{region}/{event_id}.jpg" or None
    bronze_minio_client,                # MinioClientResource
    sqlite_client,                      # SqliteClientResource
    parse_item_page_fn,                 # parse_ebay_de_item_page | parse_ebay_uk_item_page
    mode: str,                          # "fill" or "overwrite"
    sold_date: str | None = None,       # from search page (live) or None (replay)
) -> dict:
```

**Returns a counts dict** with the following keys:

- `mode` (echoed for log clarity)
- `skipped_existing` (fill mode only — parquet was already there)
- `wrote_parquet`
- `wrote_sqlite` (fill mode only)
- `parse_failed`
- `read_image_ok` / `read_image_missing`
- `parquet_write_failed` (rare — parquet raised, asset continues)

**Raises:**
- `ValueError` if `mode` not in `{"fill", "overwrite"}` — fail loud (AGENTS.md Rule 12)
- The original parquet write exception propagates if `put_object` raises (don't hide state corruption)

**Side effects:**
- `fill` mode + no existing parquet: writes parquet + INSERT OR IGNORE SQLite
- `fill` mode + existing parquet: no writes
- `overwrite` mode + no existing parquet: writes parquet + INSERT OR IGNORE SQLite (this is a new row from SQLite's perspective; same SQLite behavior as fill-on-new because there was no historical row to leave untouched)
- `overwrite` mode + existing parquet: removes existing parquet, writes new parquet, **does not** touch SQLite

### `replay_bronze_from_raw_{de,uk}` — assets

**Location:** `src/tcg_platform/defs/replay_bronze_from_raw.py` (new)

**Inputs (Dagster run config):**
```yaml
ops:
  replay_bronze_from_raw_de:
    config:
      mode: fill  # or "overwrite"
```

**Behavior:**
1. Validate `mode` ∈ `{"fill", "overwrite"}`, raise `ValueError` otherwise.
2. List `tcg-raw/ebay/{region}/*.html` via `tcg_raw_client.list_objects`.
3. For each event_id:
   - Read HTML (skip + count `read_failed` on exception)
   - Try to read `tcg-raw/sold_images/{region}/{event_id}.jpg`; image_path = path on success, None on failure
   - Call `transform_one_item` with the configured mode
   - Sum counts into the per-asset totals
4. Emit `dg.MaterializeResult(metadata=counts)` for the Dagster UI.

**Required resource keys:** `tcg_raw_client`, `minio_client`, `sqlite_client_{de,uk}`.

### `replay_bronze_from_raw_job` — Dagster job

```python
replay_bronze_from_raw_job = define_asset_job(
    name="replay_bronze_from_raw_job",
    selection=[
        AssetKey("replay_bronze_from_raw_de"),
        AssetKey("replay_bronze_from_raw_uk"),
    ],
)
```

Both assets have no inter-dependency → they run in parallel within the
job. Operator passes `mode` per-asset via the launch config or the UI.

### `transform_bronze.py` — refactor

The existing `_transform_region` function (lines 30–134) is replaced
with a loop that calls `transform_one_item` for each item in
`written_items`. The mode is hardcoded to `fill` (this is the live
transformer — overwrite is never appropriate for it because the
scraper just wrote the raw, so overwriting is a no-op except in the
"raw existed before the scraper run" case, which the backfill path
already covers).

**Net effect on `transform_bronze.py`:** ~40 lines deleted, ~10 added.
Behavior preserved. Existing `test_transform_bronze.py` continues to pass.

## Data Flow

### `fill` mode — the live transformer and the new gap-fill replay both use this

For each `event_id` enumerated from inputs:
1. **Skip** if `tcg-bronze/sold_data/{region}/{event_id}.parquet` exists (`stat_object`).
2. **Read** `tcg-raw/ebay/{region}/{event_id}.html`.
3. **Read** `tcg-raw/sold_images/{region}/{event_id}.jpg` (optional; image_path=None on missing).
4. **Parse** with `parse_ebay_{de,uk}_item_page(html, item_url, scraped_at)`.
5. If `parsed` is empty / None: count `skipped_empty`, continue.
6. For each `PriceRecord` in `parsed`:
   - Apply `sold_date` (only if `rec.sold_date` is empty and the caller passed one)
   - Apply `image_path` to `rec.local_image_path`
   - Write parquet via `price_records_to_parquet([rec], rec.scraped_at.strftime("%Y-%m-%d"))`
   - Put parquet to `tcg-bronze/sold_data/{region}/{event_id}.parquet`
   - If `_is_proxy_title(rec.card_id)` is False: `INSERT OR IGNORE` into `fact_events` with `parqueted=1`

### `overwrite` mode — replay-only

For each `event_id`:
1. **Read** `tcg-raw/ebay/{region}/{event_id}.html`.
2. **Read** image (optional).
3. **Parse** (always).
4. If `parsed` is empty: count `skipped_empty`, continue (don't remove a parquet that has data — leave it for the next non-empty parse).
5. For each `PriceRecord`:
   - Apply `sold_date` only if the caller passed one AND `rec.sold_date` is empty
   - Apply `image_path`
   - **If parquet exists**: `remove_object` it (single delete, no batch)
   - Write new parquet to the same key
   - **Do not touch SQLite** (per the explicit user decision — SQLite stays as historical record)

### `sold_date` handling

- The live transformer carries `sold_date` from the search-page scraper (always non-None).
- The replay job has **no `sold_date`** (reading raw HTML, not search output).
- `transform_one_item` accepts `sold_date: str | None = None`. When None and `rec.sold_date` is empty, the field stays empty in the parquet (parquet still writes; `sold_date` is nullable).
- This means the 150 raw-no-bronze-gap rows will be written with empty `sold_date`. Consumers (silver/quarantine) already tolerate empty `sold_date` (the collision check `(sold_date, event_id, title)` handles None).

### Image handling

Unchanged from the live transformer:
- Try `tcg-raw/sold_images/{region}/{event_id}.jpg`.
- On success: `image_path = "sold_images/{region}/{event_id}.jpg"`, set on every `rec.local_image_path`.
- On failure: `image_path = None`, `rec.local_image_path = None` stays.

## Error Handling

- **Per-item failures don't abort the run.** Parse failures / MinIO read failures are counted + logged + skipped. Same pattern as the existing live transformer.
- **MinIO read failures** (`get_object` raises): count `read_failed`, continue. Distinct counter so operators can distinguish infra problems from parser bugs.
- **Parquet write failures** (`put_object` raises): re-raise — half-written state is worse than a failed run. Asset fails loudly.
- **SQLite failures** (`INSERT OR IGNORE` raises): log warning, count `wrote_parquet_failed_sqlite`, do NOT fail the asset. The parquet is the durable artifact; SQLite drift is recoverable on the next live run.
- **Mode flag validation:** `transform_one_item` raises `ValueError` if `mode` not in `{"fill", "overwrite"}`. Both assets validate at startup before any reads.
- **Idempotency of `fill` mode:** re-running produces zero new writes (all rows skipped via `skipped_existing`). Verified by a test that runs the asset twice and checks the second-run counts are all zeros except `skipped_existing`.
- **Idempotency of `overwrite` mode:** re-running produces the same final parquet bytes (deterministic parser + deterministic input). Verified by hashing the parquet before/after a second run.

## Testing

### Unit tests for `transform_one_item` — `tests/serialization/test_bronze_writer.py` (new)

~12 tests, no real MinIO/SQLite (MagicMock):

1. `fill_mode_writes_parquet_and_sqlite_when_no_existing_parquet`
2. `fill_mode_skips_when_parquet_exists`
3. `fill_mode_insert_or_ignore_on_duplicate_event_id` (existing row → no error)
4. `overwrite_mode_rewrites_parquet_when_existing`
5. `overwrite_mode_does_not_touch_sqlite_when_parquet_existed`
6. `overwrite_mode_writes_parquet_and_sqlite_when_no_existing` (treats as new row)
7. `parse_failure_increments_parse_failed_and_continues`
8. `minio_read_failure_increments_read_failed_and_continues`
9. `parquet_write_failure_propagates_exception`
10. `sold_date_from_caller_overrides_empty_rec_sold_date`
11. `sold_date_from_caller_does_not_override_set_rec_sold_date`
12. `sold_date_none_leaves_rec_sold_date_alone`
13. `invalid_mode_raises_value_error`
14. `image_path_set_when_image_exists`
15. `image_path_none_when_image_missing`

### Asset-level tests — `tests/scraping/test_replay_bronze_from_raw.py` (new)

~6 tests, mocking `tcg_raw_client`, `minio_client`, `sqlite_client`:

1. `fill_mode_de_writes_89_gap_rows` — given the actual bucket shape (165 raw, 76 bronze), verify counts: read_html=165, skipped_existing=76, wrote_parquet=89, wrote_sqlite=89 (or as close as the test fixtures allow).
2. `fill_mode_uk_writes_61_gap_rows` — same for UK (447 raw, 386 bronze → 61 gap).
3. `overwrite_mode_rewrites_all_and_leaves_sqlite_untouched` — verify SQLite calls = 0 for event_ids with existing parquets.
4. `rerun_in_fill_mode_is_noop` — run twice; second run has 0 writes, 226 skipped_existing.
5. `invalid_mode_raises_value_error_at_asset_startup` — pass `mode="garbage"`, expect `ValueError` from `context.resources` invocation.
6. `replay_bronze_from_raw_job_resolves` — `defs.resolve_job_def("replay_bronze_from_raw_job")` succeeds and has both assets.

### Live transformer regression — `tests/scraping/test_transform_bronze.py` (existing)

Unchanged. Must continue to pass after the `_transform_region` refactor.

### Coverage expectation

+~21 tests. Full suite: 195 → ~216.

## Files Touched

### New
- `src/tcg_platform/serialization/bronze_writer.py` — `transform_one_item` helper (~80 lines)
- `src/tcg_platform/defs/replay_bronze_from_raw.py` — two assets + one job (~120 lines)
- `tests/serialization/test_bronze_writer.py` — ~15 tests
- `tests/scraping/test_replay_bronze_from_raw.py` — ~6 tests

### Modified
- `src/tcg_platform/defs/transform_bronze.py` — `_transform_region` now calls `transform_one_item` (logic preserved, ~40 lines deleted)
- `src/tcg_platform/definitions.py` — import + register `replay_bronze_from_raw_job`
- `PROD.md` — close out M9-T2 (replay job); update outstanding list

### Not touched
- Parsers, scrapers, silver, reconcile, sqlite resources, minio resources, Zyte resources.

## First Use (per user decision)

After implementation lands on `main`:

1. Run `replay_bronze_from_raw_job` with `mode: fill` once.
2. Expected outcome (based on inventory measured 2026-06-27):
   - DE: 89 new bronze parquets + 89 new SQLite rows
   - UK: 61 new bronze parquets + 61 new SQLite rows
   - Total: 150 row gap closed
3. Verify with a follow-up inventory query: `raw == bronze == (165, 447)`.
4. Leave `mode: overwrite` armed in the UI for the next parser bug.

## Risks

**Medium-high:** The refactor of `_transform_region` is the riskiest
change in this PR. The function is currently working and covered by
existing tests. If I miss a side effect (e.g., the existing function
does something subtle with `scraped_at` propagation that I don't
replicate in the helper), the live transformer could silently corrupt
rows.

**Mitigation:**
- Run `pytest tests/scraping/test_transform_bronze.py -v` after the refactor and require 0 regressions.
- Diff `git diff src/tcg_platform/defs/transform_bronze.py` carefully during code review — the refactor should be a deletion + a call, not a rewrite.
- Add a brief comment in `_transform_region` noting that the per-item contract now lives in `bronze_writer.transform_one_item` so future maintainers find the contract in one place.

**Low:** `sold_date` will be empty for the 150 gap rows. This is
intentional (we have no source for it) and tolerated by downstream
consumers.

**Low:** 29 UK bronze-without-raw rows cannot be replayed. Documented
as out of scope.

## Out of Scope

- 29 UK bronze-without-raw rows — cannot be replayed (raw is gone)
- TTL on raw objects
- Image deduplication across regions
- Limitless TCG replay (only eBay DE/UK replay; the architecture extends but the spec doesn't)
- Auto-replay trigger on parser change (would require parser-version metadata in the bucket)
- SQLite UPDATE on overwrite mode (per explicit user decision: SQLite stays immutable)
- Multi-mode DE/UK parallelism beyond the existing job graph (the new job already runs DE and UK assets in parallel)

## Acceptance Criteria

1. All new tests pass; existing 195 tests still pass.
2. `defs.resolve_job_def('replay_bronze_from_raw_job')` succeeds.
3. Manual `mode: fill` run closes the 150-row gap (raw count == bronze count per region).
4. Manual `mode: overwrite` dry-run on a single test event_id demonstrates the parquet changes bytes (verifiable via hash diff) and SQLite `fact_events` is untouched (verifiable via row count before/after).
5. `PROD.md` M9-T2 marked complete with link to this spec.
6. Session log written to `log/SESSION_2026-06-27.md`.
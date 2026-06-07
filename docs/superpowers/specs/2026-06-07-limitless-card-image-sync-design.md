# Limitless Card Image Sync — Design

**Date:** 2026-06-07
**Branch:** `2026-06-07-card-image-sync`
**Status:** Approved (brainstorming complete, awaiting spec review)

## Goal

Provide a single-button Dagster job that keeps the `tcg-bronze/cards/` image
catalog in sync with the current One Piece TCG card catalog on Limitless TCG.
The job discovers every card Limitless lists (base + alternate-art variants
`?v=1`, `?v=2`, …), diffs against what's already in MinIO, and downloads only
the missing files. Re-runs are idempotent and exit fast when nothing has
changed.

## Why now

The M2-T3 image downloader code (`image_downloader.py`, `defs/image_download.py`)
was removed in a "remove dead code" chore, but the 3,489 webp files in
`tcg-bronze/cards/{OP01..OP15, EB01-04, ST01-29, PRB01-02, P}/` are still
there. The `limitless_op_cards` asset still scrapes Limitless for *card data*
(CardRecord) but feeds the deferred `bronze_cardlist_parquet` writer, not the
images. There is no current job that re-adds new cards or new sets Limitless
publishes (new sets arrive every few weeks).

## Scope

**In scope:**

- Two new Dagster assets and one new job, runnable from the Dagster UI as a
  single click, mirroring the `complete_eu_pipeline` pattern in
  `definitions.py:68`.
- Dynamic discovery of every set Limitless currently lists. No hardcoded set
  list anywhere in the new code. New sets (e.g. `OP16`, `EB05`, future ones)
  are picked up automatically on the next run.
- Image-only output: webp files at `tcg-bronze/cards/{set_code}/{card_id}.webp`
  (base) and `tcg-bronze/cards/{set_code}/{card_id}_v{N}.webp` (variants).
  This preserves the existing folder structure the user wants to keep.
- Idempotent re-runs: if no cards are new, the sync step completes with
  `new_count=0` after discovery finishes.

**Out of scope (explicit):**

- The `bronze/cardlist/partition_date={date}/cards.parquet` cardlist writer
  remains deferred to M7. The silver layer's `is_valid_card_id` lookup
  currently has a path-collision bug (`silver_transform.py:454,506` docstring
  says `tcg-bronze/cards/` but `cards/` is images) — fixing that is a
  separate task.
- The `limitless_op_cards` asset (which scrapes card *data*) is not modified.
- No changes to eBay, silver, SQLite, or any other pipeline.
- No automated scheduling. The job is launched manually from the Dagster UI
  on a roughly monthly cadence.

## Architecture

Two assets, one job, no orchestrator asset needed (the job's
`selection=[...]` runs them in dep order).

### Asset 1: `discover_limitless_catalog`

- **Path:** `src/tcg_platform/defs/discover_limitless_catalog.py`
- **Inputs:** `minio_client` resource (not strictly required for discovery
  itself, but consistent with other defs and future-proofs the asset for
  writing the catalog to `meta/` later if needed).
- **Output:** `list[tuple[str, str, int | None]]` — list of
  `(set_code, card_id, variant)` tuples. `variant` is `None` for base cards,
  `1`/`2`/… for `?v=N` printings.
- **Logic:**
  1. Launch headless Chromium via Playwright.
  2. Navigate to `https://onepiece.limitlesstcg.com/cards/`, wait for
     `networkidle`, parse the set table → `[(set_code, set_path), ...]`.
     Reuses the existing `_get_all_sets()` in
     `src/tcg_platform/scraping/limitlesstcg.py:108`.
  3. For each set, navigate to `https://onepiece.limitlesstcg.com{set_path}`,
     wait for `networkidle`, extract every `<a href="/cards/...">` link that
     matches a known set prefix (`OP`, `EB`, `ST`, `PR`, `P`). Parse the
     `?v=N` query param from each link to determine variant.
  4. Deduplicate by `(set_code, card_id, variant)`.
  5. Close the browser. Return the list.
- **Runtime:** ~5-10 min for ~3,500+ cards across 49+ sets.
- **Failure modes:** Per-set try/except with warning log; partial catalog is
  still returned. Empty discovery result (e.g. Limitless HTML changed) raises
  with a clear message pointing to `_get_all_sets`.

### Asset 2: `sync_card_images`

- **Path:** `src/tcg_platform/defs/sync_card_images.py`
- **Inputs:** `discover_limitless_catalog` (Dagster asset dep),
  `minio_client` resource.
- **Logic:**
  1. One `list_objects("tcg-bronze", "cards/")` → set of existing object keys.
  2. For each `(set_code, card_id, variant)` from discovery, build the
     expected MinIO key:
     - Base: `cards/{set_code}/{card_id}.webp`
     - Variant N: `cards/{set_code}/{card_id}_v{N}.webp`
  3. Diff: `missing = [(s, c, v) for (s, c, v) in discovered if key(s, c, v) not in existing]`
  4. For each missing tuple, fetch the image from
     `https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/{SET}/{card_id}{_pN}_EN.webp`
     (the `_pN` suffix is empty for base, `_p1`/`_p2`/… for variants) and
     `put_object` to MinIO.
  5. Return `MaterializeResult` with metadata: `discovered_count`,
     `existing_count`, `new_count`, `failed_count`, `new_card_ids` (list),
     `failed_card_ids` (list), `duration_seconds`.
- **Runtime:** discovery overhead only on the discover asset. The sync asset
  itself is fast (seconds) when no new cards exist; download time scales with
  the number of new cards (~1-2 sec per CDN fetch).
- **Failure modes:** Per-card try/except on CDN fetch; failures are logged
  and accumulated into `failed_card_ids`, but do not abort the job.

### Job: `sync_card_images_job`

Registered in `src/tcg_platform/definitions.py`:

```python
sync_card_images_job = define_asset_job(
    name="sync_card_images_job",
    selection=["discover_limitless_catalog", "sync_card_images"],
    description="Diff Limitless catalog against tcg-bronze/cards/, download missing images.",
)
```

Added to the `jobs=[...]` list in `Definitions(...)`. The job is launched
from the Dagster UI like `complete_eu_pipeline`. No CLI script is required.

## Data flow

```
User clicks "Launch sync_card_images_job" in Dagster UI
  │
  ├─ discover_limitless_catalog asset
  │    Playwright → /cards/                        → [(OP01, /cards/op01/), ...]
  │    For each set: Playwright → /cards/op01/     → [(OP01-001, None), (OP01-001, 1), ...]
  │    Returns: catalog = [(set_code, card_id, variant), ...]    ~3,500+ tuples
  │
  └─ sync_card_images asset
       list_objects("tcg-bronze", "cards/")        → existing_keys (set, 1 call)
       For each (s, c, v) in catalog:
           key = f"cards/{s}/{c}{'_v' + str(v) if v else ''}.webp"
           if key not in existing_keys:
               HTTP GET cdn_url(s, c, v)           → bytes
               put_object(bucket, key, bytes)
       Returns: MaterializeResult(metadata={...counts, new_card_ids, duration})
```

## Key conventions matched

- **Job registration:** `define_asset_job(name=..., selection=[...], description=...)`
  in `definitions.py:68`-style block, added to the `jobs=[...]` list.
- **Asset registration:** Dagster auto-discovers any `@dg.asset` in
  `src/tcg_platform/defs/` via `load_from_defs_folder` (`definitions.py:77`),
  no manual asset list update needed.
- **MinIO access:** through the `minio_client` resource
  (`defs/minio_resources.py`); bucket name comes from `minio_client.bucket_name`
  (env: `MINIO_BUCKET`, default `tcg-bronze`).
- **Logging:** `context.log.info` / `context.log.warning` for asset-level
  messages. No print statements in the asset functions.
- **Idempotency:** diff is in-memory against a single `list_objects` snapshot;
  re-running with no changes is a no-op on the sync step.

## Refactor: extract `extract_card_links_from_set_page`

In `src/tcg_platform/scraping/limitlesstcg.py`, the per-set card-link
extraction logic is currently inline at `limitlesstcg.py:154-155`:

```python
card_links = [a.get("href") for a in soup.find_all("a") if "/cards/OP" in a.get("href", "") or "/cards/EB" in a.get("href", "") or "/cards/PR" in a.get("href", "") or "/cards/ST" in a.get("href", "")]
card_links = list(set(card_links))
```

Extract to a module-level function:

```python
def extract_card_links_from_set_page(html: str) -> list[tuple[str, int | None]]:
    """Parse a Limitless set page; return [(card_id, variant), ...]."""
    soup = BeautifulSoup(html, "html.parser")
    raw = [
        a.get("href") for a in soup.find_all("a")
        if any(p in a.get("href", "") for p in ("/cards/OP", "/cards/EB", "/cards/PR", "/cards/ST"))
    ]
    out = []
    seen = set()
    for href in raw:
        # href like "/cards/op01-001" or "/cards/op01-001?v=2"
        path, _, query = href.partition("?")
        card_id = path.rsplit("/", 1)[-1].upper()
        variant = None
        if query:
            for part in query.split("&"):
                if part.startswith("v="):
                    try:
                        variant = int(part[2:])
                    except ValueError:
                        variant = None
        if (card_id, variant) not in seen:
            seen.add((card_id, variant))
            out.append((card_id, variant))
    return out
```

The existing `scrape_limitless_op()` is refactored to call this function
instead of the inline expression. One intentional behavior change: the
prefix set is extended from `OP|EB|PR|ST` to `OP|EB|ST|PR|P` to pick up
promo cards (the current code misses `/cards/p-001` URLs but MinIO already
contains a `cards/P/` set from the M2-T3 era, so this is a real existing
gap, not a new one). The `scrape_limitless_op` function is otherwise
unchanged.

## Error handling

- **CDN 404 (card was removed from Limitless):** Per-card try/except. Log
  warning, append to `failed_card_ids`, continue.
- **CDN timeout / network error:** Same per-card try/except, same handling.
- **Playwright timeout on a set page:** Per-set try/except in the discovery
  asset. Log warning, skip the set, continue. Partial catalog still returned.
- **Empty discovery result:** Discovery asset raises. Job fails with a clear
  message pointing at `_get_all_sets` in `limitlesstcg.py`. Fails loud per
  AGENTS.md Rule 12.
- **MinIO `put_object` failure:** Per-card try/except. Log warning, append
  to `failed_card_ids`, continue.
- **Set prefix regex drift (new set code Limitless introduces):** The current
  `OP|EB|ST|PR|P` prefix list is hardcoded in `extract_card_links_from_set_page`.
  If Limitless adds a new prefix (no historical example, but possible), it
  would silently miss those cards. Mitigation: log a warning when a set
  page returns zero `?v=N`-or-base links, since that indicates the prefix
  filter excluded them.

## Testing

### `tests/scraping/test_discover_limitless_catalog.py`

- `test_parses_set_index_html` — fixture HTML for `/cards/` with 3 sets,
  assert 3 `(set_code, set_path)` tuples in expected order.
- `test_parses_set_page_with_variants` — fixture HTML for one set page
  with 5 base cards + 2 `?v=1` + 1 `?v=2`, assert 8 `(card_id, variant)`
  tuples in expected shape.
- `test_dedupes_duplicate_links` — fixture with the same card linked twice
  (Limitless sometimes does this), assert dedup.
- `test_empty_set_page` — fixture with zero card links, assert empty list
  (not an error).
- `test_set_page_with_no_variants` — fixture with only base links (no `?v=`),
  assert all `variant=None`.

These tests are pure-Python (no Playwright, no MinIO) and run via the
existing `pytest tests/scraping/` setup.

### `tests/scraping/test_sync_card_images.py`

- `test_diff_no_new_cards` — discovery list of 3 cards + MinIO with all 3
  keys present → assert 0 download calls, `new_count=0`, no failed.
- `test_diff_with_new_cards` — discovery list of 3 + MinIO with 1 present
  → assert 2 download calls with correct keys and CDN URLs.
- `test_variant_keys` — discovery list with a variant tuple → assert
  expected key is `cards/OP01/OP01-001_v1.webp` and CDN URL contains
  `_p1_EN.webp`.
- `test_cdn_404_marks_failed` — mock HTTP returning 404 for one card →
  assert that card_id in `failed_card_ids`, others succeed, `failed_count=1`.
- `test_cdn_timeout_continues` — mock HTTP raising `requests.Timeout` for
  one card → same handling.
- `test_minio_put_failure_continues` — mock `minio_client.put_object`
  raising for one card → same handling.
- `test_list_objects_called_once` — assert the asset calls
  `list_objects("tcg-bronze", "cards/")` exactly once regardless of catalog
  size.

## Files added / modified

**New:**
- `src/tcg_platform/defs/discover_limitless_catalog.py`
- `src/tcg_platform/defs/sync_card_images.py`
- `tests/scraping/test_discover_limitless_catalog.py`
- `tests/scraping/test_sync_card_images.py`

**Modified:**
- `src/tcg_platform/definitions.py` — add `sync_card_images_job` and include
  in the `jobs=[...]` list.
- `src/tcg_platform/scraping/limitlesstcg.py` — extract
  `extract_card_links_from_set_page`; refactor `scrape_limitless_op` to call
  it; add `P` to the prefix set (currently missing).

## Verification (end of task)

Per AGENTS.md Rule 17:
1. `pytest tests/scraping/test_discover_limitless_catalog.py tests/scraping/test_sync_card_images.py -v` — new tests pass.
2. `pytest tests/ -v` — full test suite still 68+/70 passing; the 2 pre-existing `test_exchange_rate.py` failures are unrelated.
3. `python -c "from tcg_platform.definitions import defs; print('OK')"` — Dagster definitions load cleanly with the new assets + job.
4. `git status --porcelain` — only the planned files modified.

## Out-of-scope follow-ups (not this task)

- Wire `bronze_cardlist_parquet` to run against the existing Limitless
  scrape, so `silver_transform.is_valid_card_id` can validate against a
  real catalog (resolves the path-collision bug in `silver_transform.py:454`).
- Schedule `sync_card_images_job` to run monthly via Dagster sensor or cron.
- Generalize the prefix regex to detect unknown new set codes automatically.

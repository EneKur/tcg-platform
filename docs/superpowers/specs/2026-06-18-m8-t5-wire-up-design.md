# M8-T5 Wire-up: Limitless Bronze Parquet Pipeline

**Date:** 2026-06-18
**Status:** Approved
**Branch:** `2026-06-18-m8-t5-wire-up`

## Problem

The Limitless bronze parquet writers (`bronze_cardlist_parquet`,
`bronze_fact_events_parquet`) are dormant — assets exist in
`src/tcg_platform/defs/`, but no job selects them, and the
serialization helpers in `src/tcg_platform/serialization/card_parquet.py`
have three known-stale behaviors that were intentionally pinned by
`tests/serialization/test_card_parquet.py` on 2026-06-10 so any fix
is a deliberate, test-failing change:

1. **`event_id` is always `""`** in `price_records_to_parquet`. The
   `PriceRecord` model has no `event_id` field and the helper
   hardcodes an empty string. The downstream gold layer will need
   non-empty `event_id`s to join with eBay events.
2. **`image_url` and `local_image_path` are silently dropped** in
   `price_records_to_parquet`. Both fields are on the `PriceRecord`
   model but never make it into the parquet row.
3. **`partition_date` argument is a no-op** in both helpers. The
   argument is accepted but the column is not written; the
   partitioning is path-only (`bronze/cardlist/partition_date={date}/...`).

The 2026-06-10 session log (`log/SESSION_2026-06-10.md`) and
`log/M8-T5.md` document the de-scoped decision: pin the bugs as
tests, defer the fix. The 2026-06-10 log also notes that a
`limitless_pipeline` job to surface the dormant assets is a
"separate task" beyond the test-pinning PR.

## Goal

1. Fix the three schema bugs in
   `src/tcg_platform/serialization/card_parquet.py`:
   - `event_id` derived from `source_url` (non-empty, deterministic).
   - `image_url` and `local_image_path` pass through (default `""`).
   - `partition_date` written as a real column.
2. Add a `limitless_pipeline` job in `src/tcg_platform/definitions.py`
   that selects `[limitless_op_cards, bronze_cardlist_parquet,
   bronze_fact_events_parquet]`. Dagster infers the dependency
   order from the asset graph.
3. Update `tests/serialization/test_card_parquet.py` to enforce the
   new contract. Existing 14 tests change; ~6 new tests added.
4. Close M8-T5 in `PROD.md` and write a `log/SESSION_2026-06-18.md`
   session log.

## Non-goals

- No change to the Limitless source (`src/tcg_platform/scraping/limitlesstcg.py`).
  The browser-based scraper stays as-is.
- No change to the 14 Limitless CDN `failed_card_ids` (external gap).
- No `sync_card_images` integration (separate M8-T1 work, already landed).
- No daily Limitless schedule (still deferred per M5-T2).
- No backfill of historical `tcg-bronze/bronze/cardlist/` parquets
  (M8-T1 + sync_card_images already covers card image sync; the
  parquet is the new layer introduced by this spec).
- No image URL backfill for existing Limitless rows. Future work
  when Limitless gets image download.

## Design

### Change 1: `derive_event_id(source_url) -> str` helper

**File:** `src/tcg_platform/serialization/card_parquet.py` (new function)

```python
from tcg_platform.scraping.ebay_utils import extract_item_id

LIMITLESS_HOST = "onepiece.limitlesstcg.com"

def derive_event_id(source_url: str) -> str:
    """Return a non-empty, deterministic event_id for the given source URL.

    - eBay DE/UK item pages: the eBay item_id (already a unique sold event).
    - Limitless TCG card pages: f"limitless-{card_id}" (the source has no
      sold event; we synthesize a stable id from the card_id).
    - Anything else: f"unknown-{hash(source_url) % 10**8}" (deterministic
      8-digit suffix, non-empty, debuggable).
    """
    if not source_url:
        return "unknown-0"
    if LIMITLESS_HOST in source_url:
        # /cards/OP01-001 -> "OP01-001"
        parts = source_url.rstrip("/").split("/")
        return f"limitless-{parts[-1].upper()}"
    if "ebay.de" in source_url or "ebay.co.uk" in source_url:
        item_id = extract_item_id(source_url)
        if item_id and item_id.isdigit():
            return item_id
    return f"unknown-{abs(hash(source_url)) % 10**8}"
```

`extract_item_id` is already a pure function in
`src/tcg_platform/scraping/ebay_utils.py` (re-exported from
`M6.5-T1`). No new dependency.

### Change 2: `price_records_to_parquet` fix

**File:** `src/tcg_platform/serialization/card_parquet.py` (modified)

```python
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
```

### Change 3: `card_records_to_parquet` fix

**File:** `src/tcg_platform/serialization/card_parquet.py` (modified)

```python
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
```

### Change 4: `build_local_image_path_map` helper for the caller

**File:** `src/tcg_platform/serialization/card_parquet.py` (new function)

```python
def build_local_image_path_map(minio_client) -> dict[str, str]:
    """Read tcg-bronze/cards/ from MinIO and return a {card_id: path} map.

    The serializer uses this to backfill `local_image_path` for
    Limitless rows. Empty dict if no images are present.
    """
    out: dict[str, str] = {}
    for obj_name in minio_client.list_objects("tcg-bronze", prefix="cards/"):
        # obj_name is a string like "cards/OP01/OP01-001.webp"
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
```

The caller (`bronze_fact_events_parquet` asset) calls this once
before `price_records_to_parquet` and passes the map in.
`minio_client.list_objects(bucket, prefix)` returns `list[str]`
of object names (see `src/tcg_platform/resources/minio_client.py:90-97`).

### Change 5: `bronze_fact_events_parquet` asset wires the MinIO lookup

**File:** `src/tcg_platform/defs/bronze_fact_events_parquet.py` (modified)

Add a `build_local_image_path_map(minio_client.client)` call
before the `price_records_to_parquet` call. Pass the result as
`local_image_path_map`. No change to the asset's
`MaterializeResult` or the partition path.

### Change 6: `limitless_pipeline` job

**File:** `src/tcg_platform/definitions.py` (modified)

Add to the existing job list (between `sync_card_images_job` and
`ebay_de_raw_to_bronze_job` for logical grouping):

```python
limitless_pipeline = define_asset_job(
    name="limitless_pipeline",
    selection=[
        "limitless_op_cards",
        "bronze_cardlist_parquet",
        "bronze_fact_events_parquet",
    ],
    description="Scrape Limitless TCG catalog + write bronze cardlist + fact_events parquets.",
)
```

Register the job in the `jobs=[...]` list in the same file.

### Change 7: tests

**File:** `tests/serialization/test_card_parquet.py` (modified)

Update 14 existing tests:

- `test_cards_empty_input_returns_zero_row_parquet` — same
  shape, but the `partition_date` arg is required (was optional).
- `test_cards_single_card_writes_all_required_columns` — assert
  the new `partition_date` column is present and equal to the arg.
- `test_cards_optional_fields_default_to_empty_string_or_zero` —
  same, but the schema check now expects `partition_date` in the
  column list.
- `test_cards_scraped_at_stamped_at_call_time` — REWORKED:
  `scraped_at` is now derived from `partition_date`, not
  `datetime.now()`. New test: `scraped_at` equals
  `f"{partition_date}T00:00:00+00:00"` and is the same across
  two calls with the same arg.
- `test_cards_partition_date_argument_is_ignored` — DELETED. The
  contract is now that `partition_date` is honored. The new test
  `test_cards_partition_date_column_reflects_arg` replaces it.
- `test_cards_returned_row_count_matches_input` — unchanged.
- `test_prices_empty_input_returns_zero_row_parquet` — same
  shape, partition_date required.
- `test_prices_event_id_column_is_always_empty_string` — REWORKED:
  renamed `test_prices_event_id_derived_from_source_url` and
  now asserts non-empty for an eBay URL.
- `test_prices_image_url_and_local_image_path_are_dropped` — REWORKED:
  renamed `test_prices_image_url_passes_through` and
  `test_prices_local_image_path_passes_through`. Assert the column
  is present and equal to the source value (or "" if not set).
- `test_prices_title_defaults_to_empty_string` — unchanged.
- `test_prices_title_passed_through_when_set` — unchanged.
- `test_prices_card_version_none_becomes_empty_string` — unchanged.
- `test_prices_sold_date_none_becomes_empty_string` — unchanged.
- `test_prices_returned_row_count_matches_input` — unchanged.

Add 6 new tests:

- `test_derive_event_id_for_ebay_de_url`
- `test_derive_event_id_for_ebay_uk_url`
- `test_derive_event_id_for_limitless_url`
- `test_derive_event_id_for_unknown_url_is_deterministic`
- `test_derive_event_id_for_empty_string_returns_unknown_zero`
- `test_prices_partition_date_column_reflects_arg`
- `test_prices_local_image_path_backfilled_from_map`
- `test_prices_local_image_path_prefers_record_value_over_map`

Net: 14 − 2 (delete 2 outdated) + 8 (new) = 20 tests in this file,
up from 14. The full suite should grow by +6 net.

**File:** `tests/defs/test_definitions_load.py` — no change. The
job is registered; the existing load test still passes.

### Change 8: `PROD.md` close-out

**File:** `PROD.md` (modified)

Move M8-T5 from the "Outstanding" list to the M8 "Complete" list.
Update the entry to note the schema fixes (event_id derivation,
image_url/local_image_path passthrough, partition_date column) and
the new `limitless_pipeline` job.

## Success criteria

- `pytest tests/ -v` — 100% pass. Current count: 174. Expected:
  ~180 (+6 net: -2 deleted, +8 added in the test_card_parquet file = +6 net).
- `python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"`
  — OK. `limitless_pipeline` is selectable and resolves with
  `selected_asset_keys = ['limitless_op_cards', 'bronze_cardlist_parquet', 'bronze_fact_events_parquet']`.
- `bash scripts/check_minio_clock.sh` — OK.
- Live smoke: `dg launch --job limitless_pipeline` against the
  real Limitless source writes 1+ parquet files to
  `tcg-bronze/bronze/cardlist/{date}/cards.parquet` and
  `tcg-bronze/bronze/fact_events/{date}/prices.parquet`. A row
  in the fact_events parquet from a Limitless scrape has
  `event_id="limitless-OP01-001"` (or similar), `partition_date="2026-06-18"`,
  and `image_url=""` (until Limitless image download lands).
- `git status --porcelain` — clean.
- No work on `main`.

## Risks

- **`derive_event_id` hashing**: Python's `hash()` is randomized
  per-process by default (PYTHONHASHSEED). For unknown URLs, the
  resulting `unknown-NNNNNNNN` suffix would change across runs.
  Fix: use `hashlib.md5(source_url.encode()).hexdigest()[:8]`
  instead. Tests pin this.
- **`limitless_pipeline` is browser-based**: `scrape_limitless_op`
  uses `playwright` and the live test depends on a healthy network
  + Limitless website. The smoke test may take >2 minutes for the
  catalog walk. The full live smoke is not in the success criteria —
  a unit test asserting the job is selectable + assets are
  discoverable is sufficient.
- **Existing callers passing `partition_date=""`**: there are
  exactly two callers
  (`src/tcg_platform/defs/bronze_cardlist_parquet.py` and
  `src/tcg_platform/defs/bronze_fact_events_parquet.py`); both
  compute `partition_date` from `datetime.now(timezone.utc)`. The
  new `ValueError` on empty `partition_date` will not fire in
  production.
- **Pinning tests become false-positives after the fix**: the
  2026-06-10 log explicitly says "any future fix is a deliberate,
  test-failing change." The fix is the deliberate change. Tests
  are updated, not deleted. Documented in the session log.

## Out of scope (still)

- M5-T2 (Dagster schedules) — still deferred.
- 14 `failed_card_ids` from `sync_card_images` — Limitless CDN gap.
- M9 replay (`replay_bronze_from_raw_job`) — separate task.
- DE/UK arbitrage backtest — separate task.
- UK parser `card_id` corruption — separate task; replay would be
  the fix path.
- `silver_eu_orchestrator` parallelization — separate task.

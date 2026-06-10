# M8-T5: Pin tests for the Limitless bronze parquet serializer

**Date:** 2026-06-10
**Status:** Draft → User-approved
**Branch (planned):** `2026-06-10-m8-t5-card-parquet-tests`

## Context

`src/tcg_platform/serialization/card_parquet.py` exposes two pure functions
(`card_records_to_parquet`, `price_records_to_parquet`) that turn
`CardRecord` and `PriceRecord` Pydantic models into parquet bytes. They
are called by the dormant assets `bronze_cardlist_parquet` and
`bronze_fact_events_parquet` in `src/tcg_platform/defs/`. No job
currently selects those assets, and the serializer has **zero tests**.

This was logged as "outstanding" in the 2026-05-28 session
("Limitless TCG parquet... outstanding since 2026-05-28") and is
flagged in `PROD.md` as **M8-T5**. The user, on 2026-06-10, scoped
M8-T5 to *fix what's broken, no new job* — that means: add tests
that pin current behavior. Do not fix the schema, do not add a job,
do not change the serializer.

## Current state of the code

### `card_parquet.py:8` — `card_records_to_parquet`

```python
def card_records_to_parquet(cards: list, partition_date: str) -> tuple[bytes, int]:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "card_id": card.card_id,
            "card_version": card.card_version or "",
            "card_name": card.card_name,
            "set_code": card.set_code,
            "rarity": card.rarity or "",
            "card_type": card.card_type,
            "attribute": card.attribute or 0,
            "power": card.power or 0,
            "cost": card.cost or 0,
            "color": card.color or "",
            "source_url": card.source_url,
            "scraped_at": now.isoformat(),
        }
        for card in cards
    ]
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), len(rows)
```

Behavior to pin:

- `partition_date` is accepted but never used. `scraped_at` is
  stamped from `datetime.now(timezone.utc)`.
- `card_version`, `rarity`, `attribute`, `color` are all `or ""`-coerced.
- `power` and `cost` are `or 0`-coerced.
- Returned tuple is `(bytes, row_count)`.

### `card_parquet.py:33` — `price_records_to_parquet`

```python
def price_records_to_parquet(prices: list, partition_date: str) -> tuple[bytes, int]:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "event_id": "",
            "card_id": price.card_id,
            "card_version": price.card_version or "",
            "event_type": price.event_type,
            "price": price.price,
            "currency": price.currency,
            "sold_date": price.sold_date or "",
            "scraped_from": price.scraped_from,
            "source": price.source,
            "source_url": price.source_url,
            "scraped_at": now.isoformat(),
            "title": getattr(price, "title", None) or "",
        }
        for price in prices
    ]
    ...
```

Behavior to pin (the three "stale" things the user explicitly
decided to leave alone):

- `event_id` is always `""` — the writer does not synthesize one.
- `image_url` and `local_image_path` fields on `PriceRecord` are
  silently dropped.
- `title` is read via `getattr(price, "title", None) or ""` — safe
  if the Pydantic model is rebuilt without the field.

## Tests to add

New file: `tests/serialization/test_card_parquet.py`. Companion
`tests/serialization/__init__.py` so the test directory is importable.

Test cases (all read-then-assert — no real MinIO, no Dagster):

### `card_records_to_parquet`

1. **`test_empty_input`** — `[]` returns `(bytes, 0)`. The bytes are
   a valid 0-row parquet (verify by reading them back with
   `pq.read_bytes` and asserting `num_rows == 0`).
2. **`test_single_card_writes_all_required_columns`** — pass a
   fully-populated `CardRecord`; verify the resulting table has the
   expected column set in the expected order:
   `card_id, card_version, card_name, set_code, rarity, card_type,
   attribute, power, cost, color, source_url, scraped_at`.
3. **`test_optional_fields_default_to_empty_string_or_zero`** —
   `CardRecord` with `card_version=None`, `rarity=""`, `attribute=None`,
   `power=None`, `cost=None`, `color=None` → row's `card_version`,
   `rarity`, `attribute`, `color` are `""`; `power` and `cost` are `0`.
4. **`test_scraped_at_stamped_at_call_time`** — capture a "before"
   and "after" UTC ISO string; assert the row's `scraped_at` falls in
   that window. Tolerate clock drift by allowing 1 second of slack on
   each side.
5. **`test_partition_date_argument_is_ignored`** — call the function
   twice with different `partition_date` values but otherwise identical
   input; assert the produced bytes are *not* necessarily equal
   (because `scraped_at` differs), but assert the column structure is
   identical. This documents that `partition_date` is not yet wired
   into the schema — pinning, not fixing.
6. **`test_returned_row_count_matches_input`** — pass N cards; the
   returned int is N.

### `price_records_to_parquet`

7. **`test_empty_input`** — same shape as #1.
8. **`test_event_id_column_is_always_empty_string`** — pass a
   `PriceRecord`; the resulting `event_id` column is `""`. This pins
   the known-stale behavior; if it ever changes, this test catches it.
9. **`test_image_url_and_local_image_path_are_dropped`** — pass a
   `PriceRecord(image_url="https://x", local_image_path="sold_images/x.jpg")`;
   the resulting table has neither column. Pins the schema as written.
10. **`test_title_defaults_to_empty_string`** — pass a `PriceRecord`
    with no `title`; assert `title == ""`.
11. **`test_title_passed_through_when_set`** — pass a `PriceRecord(title="Monkey D Luffy")`;
    assert the row's `title == "Monkey D Luffy"`.
12. **`test_card_version_none_becomes_empty_string`** — pin the
    `or ""` coercion.
13. **`test_sold_date_none_becomes_empty_string`** — pin the
    `or ""` coercion.
14. **`test_returned_row_count_matches_input`** — pass N prices; the
    returned int is N.

## Test scaffolding notes

- Use `pyarrow.parquet.read_table` to read the returned bytes back
  and assert column values. No MinIO involvement — these are pure
  function tests.
- Use the real `CardRecord` / `PriceRecord` Pydantic models so the
  tests stay faithful to production input shape. Construct with
  `CardRecord(card_id=..., card_name=..., ...)` matching the fields
  in `src/tcg_platform/scraping/models.py:6` and `:21`.
- All assertions are equality-based; no sleeps or wall-clock checks
  beyond the captured-window assertion in test #4.

## What is NOT in scope

- **No source changes** to `card_parquet.py`, `bronze_cardlist_parquet.py`,
  or `bronze_fact_events_parquet.py`.
- **No new job** in `definitions.py`. The assets remain dormant.
- **No fix** for the dropped `image_url` / `local_image_path` fields.
- **No fix** for the always-empty `event_id` column.
- **No refactor** of the unused `partition_date` argument.
- **No change** to the `dg.AssetOut` type-hint smell in the asset
  modules.

## Verification

1. `pytest tests/serialization/ -v` — all new tests pass.
2. `pytest tests/ -v` — full suite still green (currently 118;
   +14 new = 132).
3. `python -c "from tcg_platform.definitions import defs; print('OK')"`
   — Dagster definitions still load cleanly (we're not touching
   `definitions.py`, but run it to confirm no surprise).

## Files

- **New:** `tests/serialization/__init__.py` (empty)
- **New:** `tests/serialization/test_card_parquet.py` (14 test functions)
- **Modified:** none
- **Log:** `log/M8-T5.md` (created in the implementation phase)

## Commit shape

Single commit:
```
test(card_parquet): pin serializer behavior for Limitless bronze writers (M8-T5)

Adds tests/serialization/test_card_parquet.py with 14 cases that pin
the current output schema of card_records_to_parquet and
price_records_to_parquet — the helpers behind the dormant
bronze_cardlist_parquet and bronze_fact_events_parquet assets.

Pins the known-stale behaviors (event_id always "", image_url /
local_image_path dropped, partition_date argument ignored) so any
future fix is a deliberate, test-failing change rather than a
silent regression. No source changes, no new job — per the
2026-06-10 scope decision, M8-T5 is tests-only.

Tests: 132 passing (118 → 132).
```

## Production verification

None. The assets these tests cover are dormant (no job selects
them). The tests prove the serializer is correct *if* it is ever
wired up.

# M8-T5: Pin tests for the Limitless bronze parquet serializer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `tests/serialization/test_card_parquet.py` file with 14 cases that pin the current output schema of `card_records_to_parquet` and `price_records_to_parquet` so any future schema change is a deliberate, test-failing event.

**Architecture:** Pure-function tests that read parquet bytes back with `pyarrow.parquet.read_table` and assert column values. No MinIO, no Dagster, no real wall-clock checks beyond a captured-window assertion for `scraped_at`. No source changes.

**Tech Stack:** `pytest`, `pyarrow.parquet`, the existing `CardRecord` and `PriceRecord` Pydantic models from `src/tcg_platform/scraping/models.py`.

---

## File map

| File | Action | Purpose |
|------|--------|---------|
| `tests/serialization/__init__.py` | Create (empty) | Make `tests/serialization/` importable. |
| `tests/serialization/test_card_parquet.py` | Create | 14 test cases pinning current serializer behavior. |
| `src/tcg_platform/serialization/card_parquet.py` | **Do not touch** | Source is the system under test. |
| `src/tcg_platform/defs/bronze_cardlist_parquet.py` | **Do not touch** | Dormant asset. |
| `src/tcg_platform/defs/bronze_fact_events_parquet.py` | **Do not touch** | Dormant asset. |
| `src/tcg_platform/definitions.py` | **Do not touch** | No new job. |

---

## Task 1: Create the test package directory

**Files:**
- Create: `tests/serialization/__init__.py`

- [ ] **Step 1: Verify the parent directory exists**

Run: `ls tests/`
Expected: `__init__.py __pycache__ defs fixtures resources scraping`

- [ ] **Step 2: Create the empty package file**

```python
# tests/serialization/__init__.py
```

Write that exact content (a single comment) to `tests/serialization/__init__.py`.

- [ ] **Step 3: Verify the directory and file exist**

Run: `ls tests/serialization/`
Expected: `__init__.py`

---

## Task 2: Write the first three test cases (cards helper)

**Files:**
- Create: `tests/serialization/test_card_parquet.py` (start)

- [ ] **Step 1: Write the imports and first three tests**

```python
# tests/serialization/test_card_parquet.py
"""Pin the output schema of card_records_to_parquet and price_records_to_parquet.

These two helpers serialize the Pydantic models from
src/tcg_platform/scraping/models.py into parquet bytes for the dormant
bronze_cardlist_parquet and bronze_fact_events_parquet assets. They
have no test coverage; this file pins their current behavior so any
future schema change is a deliberate, test-failing event.

The known-stale behaviors below (event_id always "", image_url /
local_image_path dropped, partition_date argument ignored) are
pinned intentionally — see docs/superpowers/specs/2026-06-10-m8-t5-card-parquet-tests-design.md.
"""

from datetime import datetime, timezone

import pyarrow.parquet as pq

from tcg_platform.scraping.models import CardRecord
from tcg_platform.serialization.card_parquet import card_records_to_parquet


def _make_card(**overrides) -> CardRecord:
    """Build a fully-populated CardRecord; override any field by name."""
    base = dict(
        card_id="OP01-001",
        card_version="v1",
        card_name="Monkey D. Luffy",
        set_code="OP01",
        rarity="L",
        card_type="Character",
        attribute="STR",
        power=5000,
        cost=4,
        color="Red",
        source_url="https://onepiece.limitlesstcg.com/cards/OP01-001",
        scraped_at=datetime(2026, 6, 10, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return CardRecord(**base)


def test_cards_empty_input_returns_zero_row_parquet():
    bytes_out, count = card_records_to_parquet([], "2026-06-10")
    assert count == 0
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.num_rows == 0


def test_cards_single_card_writes_all_required_columns():
    card = _make_card()
    bytes_out, count = card_records_to_parquet([card], "2026-06-10")
    assert count == 1
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.column_names == [
        "card_id",
        "card_version",
        "card_name",
        "set_code",
        "rarity",
        "card_type",
        "attribute",
        "power",
        "cost",
        "color",
        "source_url",
        "scraped_at",
    ]


def test_cards_optional_fields_default_to_empty_string_or_zero():
    card = _make_card(
        card_version=None,
        rarity="",       # CardRecord.rarity is non-Optional str; pass "" to be explicit
        attribute=None,
        power=None,
        cost=None,
        color=None,
    )
    bytes_out, _ = card_records_to_parquet([card], "2026-06-10")
    table = pq.read_table(pq.BufferReader(bytes_out))
    row = table.to_pylist()[0]
    assert row["card_version"] == ""
    assert row["rarity"] == ""
    assert row["attribute"] == ""
    assert row["power"] == 0
    assert row["cost"] == 0
    assert row["color"] == ""
```

- [ ] **Step 2: Run the three tests to confirm they pass**

Run: `pytest tests/serialization/test_card_parquet.py -v -k "cards_"`
Expected: 3 passed.

- [ ] **Step 3: Commit the first three tests**

```bash
git add tests/serialization/__init__.py tests/serialization/test_card_parquet.py
git commit -m "test(card_parquet): pin empty input, full schema, optional coercion (M8-T5)"
```

---

## Task 3: Add the remaining three card-helper tests

**Files:**
- Modify: `tests/serialization/test_card_parquet.py` (append)

- [ ] **Step 1: Append the next three tests**

Append (do not replace) the following block to the end of
`tests/serialization/test_card_parquet.py`:

```python


def test_cards_scraped_at_stamped_at_call_time():
    before = datetime.now(timezone.utc)
    bytes_out, _ = card_records_to_parquet([_make_card()], "2026-06-10")
    after = datetime.now(timezone.utc)

    table = pq.read_table(pq.BufferReader(bytes_out))
    stamped = datetime.fromisoformat(table.to_pylist()[0]["scraped_at"])
    # Allow 1 second of slack on each side for slow CI clocks.
    slack = __import__("datetime").timedelta(seconds=1)
    assert before - slack <= stamped <= after + slack


def test_cards_partition_date_argument_is_ignored():
    # Two calls with different partition_date values but identical input
    # produce structurally identical tables (same columns, same row count,
    # same data values except scraped_at which is call-time-stamped).
    card = _make_card()
    bytes_a, count_a = card_records_to_parquet([card], "2026-06-10")
    bytes_b, count_b = card_records_to_parquet([card], "2099-12-31")
    assert count_a == count_b == 1

    table_a = pq.read_table(pq.BufferReader(bytes_a))
    table_b = pq.read_table(pq.BufferReader(bytes_b))
    assert table_a.column_names == table_b.column_names
    # The data values other than scraped_at should be equal.
    row_a = table_a.to_pylist()[0]
    row_b = table_b.to_pylist()[0]
    for col in table_a.column_names:
        if col == "scraped_at":
            continue
        assert row_a[col] == row_b[col], f"{col} differs"


def test_cards_returned_row_count_matches_input():
    cards = [_make_card(card_id=f"OP01-{i:03d}") for i in range(1, 6)]
    _, count = card_records_to_parquet(cards, "2026-06-10")
    assert count == 5
```

- [ ] **Step 2: Run the new tests**

Run: `pytest tests/serialization/test_card_parquet.py -v -k "cards_"`
Expected: 6 passed (the 3 from Task 2 plus the 3 new).

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_card_parquet.py
git commit -m "test(card_parquet): pin scraped_at stamping, partition_date no-op, row count (M8-T5)"
```

---

## Task 4: Add the first batch of price-helper tests

**Files:**
- Modify: `tests/serialization/test_card_parquet.py` (append)

- [ ] **Step 1: Add the price helper import and `_make_price` builder**

Append the import line to the top of the existing imports block (just
below `from tcg_platform.scraping.models import CardRecord`):

```python
from tcg_platform.scraping.models import CardRecord, PriceRecord
```

And add the helper near the top of the file, after the existing
`_make_card` helper:

```python


def _make_price(**overrides) -> PriceRecord:
    """Build a fully-populated PriceRecord; override any field by name."""
    base = dict(
        card_id="OP01-001",
        card_version="v1",
        event_type="price_update",
        price=12.50,
        currency="USD",
        sold_date="2026-06-09",
        scraped_from="limitlesstcg",
        source="US",
        source_url="https://onepiece.limitlesstcg.com/cards/OP01-001",
        language="EN",
        scraped_at=datetime(2026, 6, 10, tzinfo=timezone.utc),
        image_url="https://limitlesstcg.nyc3.digitaloceanspaces.com/OP01/OP01-001.webp",
        local_image_path="cards/OP01/OP01-001.webp",
        title="Monkey D. Luffy",
    )
    base.update(overrides)
    return PriceRecord(**base)
```

Note: the import line is shown above as a replacement. If you prefer,
you can replace the existing `from tcg_platform.scraping.models import CardRecord`
line with the combined import. Either way the test file should end
up with a single import line:

```python
from tcg_platform.scraping.models import CardRecord, PriceRecord
```

- [ ] **Step 2: Add the price-helper import next to the existing card-helper import**

Append to `tests/serialization/test_card_parquet.py`, right after the
`from tcg_platform.serialization.card_parquet import card_records_to_parquet`
line:

```python
from tcg_platform.serialization.card_parquet import price_records_to_parquet
```

- [ ] **Step 3: Append the first four price tests**

```python


def test_prices_empty_input_returns_zero_row_parquet():
    bytes_out, count = price_records_to_parquet([], "2026-06-10")
    assert count == 0
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.num_rows == 0


def test_prices_event_id_column_is_always_empty_string():
    bytes_out, _ = price_records_to_parquet([_make_price()], "2026-06-10")
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert "event_id" in table.column_names
    assert table.column("event_id").to_pylist() == [""]


def test_prices_image_url_and_local_image_path_are_dropped():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(
            image_url="https://cdn.example.com/x.webp",
            local_image_path="cards/OP01/x.webp",
        )],
        "2026-06-10",
    )
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert "image_url" not in table.column_names
    assert "local_image_path" not in table.column_names


def test_prices_title_defaults_to_empty_string():
    # Build a price without the title field by deleting it post-init.
    price = _make_price()
    object.__delattr__(price, "title")  # pydantic v2 models store fields as attrs
    bytes_out, _ = price_records_to_parquet([price], "2026-06-10")
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.column("title").to_pylist() == [""]
```

- [ ] **Step 4: Run the price tests**

Run: `pytest tests/serialization/test_card_parquet.py -v -k "prices_"`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/serialization/test_card_parquet.py
git commit -m "test(card_parquet): pin price-helper empty input, event_id, dropped fields, title (M8-T5)"
```

---

## Task 5: Add the remaining price-helper tests

**Files:**
- Modify: `tests/serialization/test_card_parquet.py` (append)

- [ ] **Step 1: Append the final three price tests**

```python


def test_prices_title_passed_through_when_set():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(title="Monkey D. Luffy (Alt Art)")],
        "2026-06-10",
    )
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.column("title").to_pylist() == ["Monkey D. Luffy (Alt Art)"]


def test_prices_card_version_none_becomes_empty_string():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(card_version=None)],
        "2026-06-10",
    )
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.column("card_version").to_pylist() == [""]


def test_prices_sold_date_none_becomes_empty_string():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(sold_date=None)],
        "2026-06-10",
    )
    table = pq.read_table(pq.BufferReader(bytes_out))
    assert table.column("sold_date").to_pylist() == [""]
```

- [ ] **Step 2: Run the full test file**

Run: `pytest tests/serialization/test_card_parquet.py -v`
Expected: 14 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_card_parquet.py
git commit -m "test(card_parquet): pin price-helper title passthrough, None coercion (M8-T5)"
```

---

## Task 6: Run the full test suite and confirm Dagster still loads

**Files:** none

- [ ] **Step 1: Run the full pytest suite**

Run: `pytest tests/ -v`
Expected: 132 passed (118 prior + 14 new). 0 failures, 0 errors.

If the count is anything other than 132:
- Stop. Investigate. Do not push with a wrong count.
- Likely culprit: a test in another file was already failing on
  `main` (the 118 baseline was from 2026-06-09). Re-run that test
  on its own to confirm it was already broken before this branch
  landed; if so, surface it in the session log and continue.

- [ ] **Step 2: Confirm Dagster definitions still load cleanly**

Run: `python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK` printed, exit 0.

(We are not touching `definitions.py`, so this should be a no-op
check. Run it anyway — surface, don't hide.)

- [ ] **Step 3: Commit the log file**

```bash
git add log/M8-T5.md
git commit -m "docs(log): M8-T5 — pin tests for card_parquet serializer (14 new tests)"
```

The log content for `log/M8-T5.md`:

```markdown
# M8-T5 Log: Pin tests for the Limitless bronze parquet serializer

**Date:** 2026-06-10
**Status:** Complete
**Branch:** `2026-06-10-m8-t5-card-parquet-tests`

## Summary

Added `tests/serialization/test_card_parquet.py` with 14 cases that
pin the current output schema of `card_records_to_parquet` and
`price_records_to_parquet` — the two pure helpers behind the
dormant `bronze_cardlist_parquet` and `bronze_fact_events_parquet`
assets.

Per the 2026-06-10 scope decision: tests only, no source changes,
no new job, no schema fixes. The known-stale behaviors
(`event_id=""` always, `image_url`/`local_image_path` silently
dropped, `partition_date` argument ignored) are pinned
intentionally so any future fix is a deliberate, test-failing
change.

## What was added

- `tests/serialization/__init__.py` (empty)
- `tests/serialization/test_card_parquet.py` — 14 test functions:
  - 6 for `card_records_to_parquet`: empty input, full schema,
    optional coercion, `scraped_at` call-time stamping,
    `partition_date` no-op, row count
  - 8 for `price_records_to_parquet`: empty input, `event_id`
    always empty, dropped `image_url`/`local_image_path`,
    `title` default empty, `title` passthrough,
    `card_version` None coercion, `sold_date` None coercion,
    row count

## Test counts

132 tests collected, 132 passing. Up from 118; 14 new.

## Commits

```
<this commit>
<commit from Task 2>
<commit from Task 3>
<commit from Task 4>
<commit from Task 5>
```

## What remains

- The schema fixes for `price_records_to_parquet` (synthesize
  `event_id`, write `image_url`/`local_image_path`, wire
  `partition_date` into the schema) are still outstanding — but
  pinned by these tests, so they are now a deliberate change
  rather than a silent regression.
- The two dormant assets (`bronze_cardlist_parquet`,
  `bronze_fact_events_parquet`) still have no job. Adding a
  `limitless_pipeline` job is a separate task.
```

Fill in the actual commit SHAs from `git log --oneline` for the
Task 2-5 commits before committing.

---

## Verification summary

- **Tests:** 132 passing (118 → 132).
- **Dagster:** definitions load cleanly.
- **Source:** no changes to `card_parquet.py` or any `defs/` module.
- **Job graph:** no new jobs; the dormant assets remain dormant.
- **Spec coverage:** all 14 test cases from the spec are covered
  by Tasks 2-5.

## Self-review

1. **Spec coverage:** every numbered test case (1-14) from the spec
   is implemented: empty input (T2, T4), full schema (T2), optional
   coercion (T2), scraped_at stamping (T3), partition_date no-op
   (T3), row count (T3, T4), event_id always empty (T4), dropped
   image_url/local_image_path (T4), title default empty (T4),
   title passthrough (T5), card_version None coercion (T5),
   sold_date None coercion (T5). ✓
2. **Placeholder scan:** no "TBD", no "implement later", no
   "similar to" shortcuts — every step has full code. ✓
3. **Type consistency:** `_make_card` and `_make_price` builders
   are used consistently. The `pydantic` field-deletion trick
   (`object.__delattr__`) is used in exactly one test (T4 step 3)
   and is the standard way to remove a pydantic v2 field post-init.
   ✓
4. **No out-of-scope edits:** Tasks 1-5 only touch
   `tests/serialization/`. Task 6 adds the log file. No
   modifications to `src/tcg_platform/serialization/card_parquet.py`,
   any `defs/` module, or `definitions.py`. ✓

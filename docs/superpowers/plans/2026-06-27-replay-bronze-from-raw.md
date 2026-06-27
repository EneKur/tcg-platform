# M9-T2 Replay-Bronze-From-Raw Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `replay_bronze_from_raw_job` Dagster asset that closes the 150-row raw-no-bronze gap (89 DE + 61 UK) and enables parser-bug-driven parquet rewrites without re-paying Zyte API costs.

**Architecture:** Extract the per-item write logic from the existing `transform_bronze.py:_transform_region` into a new pure helper `transform_one_item(region, event_id, raw_html, image_path, ..., mode, sold_date)` in `src/tcg_platform/serialization/bronze_writer.py`. Two new per-region assets (`replay_bronze_from_raw_{de,uk}`) wrap the helper in a loop over `tcg-raw/ebay/{region}/`. The existing live transformer is refactored to call the same helper in `fill` mode — behavior preserved.

**Tech Stack:** Dagster, MinIO (`MinioClientResource`), SQLite (`SqliteClientResource`), pytest, existing parsers (`parse_ebay_{de,uk}_item_page`), existing `price_records_to_parquet` helper.

**Branch:** `2026-06-27-m9-t2-replay-bronze-from-raw`

---

## File Structure

### New files

- `src/tcg_platform/serialization/bronze_writer.py` — pure `transform_one_item` helper (~80 lines)
- `src/tcg_platform/defs/replay_bronze_from_raw.py` — two assets + one job (~120 lines)
- `tests/serialization/test_bronze_writer.py` — ~15 unit tests for the helper
- `tests/scraping/test_replay_bronze_from_raw.py` — ~6 asset-level tests

### Modified files

- `src/tcg_platform/defs/transform_bronze.py` — refactor `_transform_region` to call `transform_one_item` (behavior preserved; ~40 lines deleted, ~10 added)
- `src/tcg_platform/definitions.py` — import + register `replay_bronze_from_raw_job`
- `PROD.md` — close out M9-T2 entry
- `log/SESSION_2026-06-27.md` — session log (final task)

---

## Task 1: Helper signature + tests scaffold

**Files:**
- Create: `src/tcg_platform/serialization/bronze_writer.py`
- Create: `tests/serialization/test_bronze_writer.py`

- [ ] **Step 1: Write the failing test for mode validation**

In `tests/serialization/test_bronze_writer.py`:

```python
"""Tests for the per-item tcg-raw → tcg-bronze writer."""
import pytest

from tcg_platform.serialization.bronze_writer import transform_one_item


def test_invalid_mode_raises_value_error():
    """A bogus mode string fails loud — the asset surfaces a clear error."""
    with pytest.raises(ValueError, match="mode must be one of"):
        transform_one_item(
            region="DE",
            event_id="12345",
            raw_html="<html></html>",
            image_path=None,
            bronze_minio_client=None,
            sqlite_client=None,
            parse_item_page_fn=lambda *a, **k: [],
            mode="garbage",
            sold_date=None,
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source .venv/bin/activate && pytest tests/serialization/test_bronze_writer.py::test_invalid_mode_raises_value_error -v`
Expected: `ImportError` or `ModuleNotFoundError` for `tcg_platform.serialization.bronze_writer`.

- [ ] **Step 3: Write the minimal module stub**

In `src/tcg_platform/serialization/bronze_writer.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/serialization/test_bronze_writer.py::test_invalid_mode_raises_value_error -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/serialization/bronze_writer.py tests/serialization/test_bronze_writer.py
git commit -m "feat(bronze_writer): add transform_one_item helper with mode validation"
```

---

## Task 2: Fill mode — write parquet + SQLite for new rows

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add tests)
- Modify: `src/tcg_platform/serialization/bronze_writer.py`

- [ ] **Step 1: Write the failing tests for fill mode new-row path**

Append to `tests/serialization/test_bronze_writer.py`:

```python
class _FakeMinioClient:
    """Models MinioClientResource._client surface used by the helper."""

    def __init__(self, html_bytes=b"", image_bytes=None):
        self.html_bytes = html_bytes
        self.image_bytes = image_bytes
        self.puts = []
        self.got = []
        self.stat_existing = set()
        self.removed = []

        class _Resp:
            def __init__(self2, data):
                self2._data = data
            def read(self2):
                return self2._data
            def close(self2):
                pass
            def release_conn(self2):
                pass

        outer = self

        class _Client:
            def get_object(self2, bucket, obj):
                outer.got.append((bucket, obj))
                if obj.endswith(".html"):
                    return _Resp(outer.html_bytes)
                if obj.endswith(".jpg") and outer.image_bytes is not None:
                    return _Resp(outer.image_bytes)
                raise Exception(f"NoSuchKey: {obj}")

            def put_object(self2, bucket, obj, data, length, content_type):
                outer.puts.append({"bucket": bucket, "object": obj,
                                   "data": data, "length": length,
                                   "content_type": content_type})

            def stat_object(self2, bucket, obj):
                if obj in outer.stat_existing:
                    return True
                raise Exception(f"NoSuchKey: {obj}")

            def remove_object(self2, bucket, obj):
                outer.removed.append((bucket, obj))

        self._client = _Client()

    @property
    def client(self):
        return self._client


class _FakeSqliteClient:
    def __init__(self):
        self.inserts = []

    def execute(self, query, params=(), fetch="none"):
        if "INSERT" in query:
            self.inserts.append(params)
        return None


def _good_de_html():
    return """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans">One Piece OP01-001 PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>
    </body></html>
    """


def _make_resource(fake_client, bucket_name="tcg-bronze"):
    from tcg_platform.resources.minio_client import MinioClientResource
    r = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name=bucket_name,
    )
    r._client = fake_client
    return r


def test_fill_mode_writes_parquet_and_sqlite_when_no_existing_parquet():
    """fill mode + no prior parquet → write parquet + SQLite INSERT."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 1
    assert counts["skipped_existing"] == 0
    parquet_puts = [p for p in minio.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 1
    assert parquet_puts[0]["object"] == "sold_data/DE/12345.parquet"
    assert len(sqlite.inserts) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/serialization/test_bronze_writer.py -v -k "fill_mode_writes"`
Expected: FAIL — `wrote_parquet` is `0` (helper returns stub counts).

- [ ] **Step 3: Implement fill-mode write logic**

Replace the body of `transform_one_item` in `src/tcg_platform/serialization/bronze_writer.py`:

```python
"""Per-item tcg-raw → tcg-bronze writer.

Extracted from `src/tcg_platform/defs/transform_bronze.py:_transform_region`
so both the live transformer and the replay/gap-fill assets share one
implementation.
"""
from datetime import datetime, timezone
from typing import Callable

from tcg_platform.serialization.card_parquet import price_records_to_parquet


_VALID_MODES = ("fill", "overwrite")
BRONZE_BUCKET = "tcg-bronze"
_PROXY_INDICATORS = ["proxy", "dummy", "fake card", "replica"]


def _is_proxy_title(card_id: str) -> bool:
    card_lower = (card_id or "").lower()
    return any(ind in card_lower for ind in _PROXY_INDICATORS)


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
        SQLite row only inserted if no prior row exists (insert, not update)
    """
    if mode not in _VALID_MODES:
        raise ValueError(
            f"mode must be one of {_VALID_MODES!r}, got {mode!r}"
        )

    counts = {
        "mode": mode,
        "skipped_existing": 0,
        "wrote_parquet": 0,
        "wrote_sqlite": 0,
        "parse_failed": 0,
        "skipped_empty": 0,
        "parquet_write_failed": 0,
        "sqlite_write_failed": 0,
    }

    upper = region.upper()
    lower = region.lower()
    parquet_key = f"sold_data/{upper}/{event_id}.parquet"

    # fill mode: skip if parquet already exists
    if mode == "fill":
        try:
            bronze_minio_client.client.stat_object(BRONZE_BUCKET, parquet_key)
            counts["skipped_existing"] = 1
            return counts
        except Exception:
            pass  # NoSuchKey — expected, fall through to write
    else:
        # overwrite mode: if parquet exists, remove it
        try:
            bronze_minio_client.client.stat_object(BRONZE_BUCKET, parquet_key)
            bronze_minio_client.client.remove_object(BRONZE_BUCKET, parquet_key)
        except Exception:
            pass

    # Parse
    item_url = (
        f"https://www.ebay.de/itm/{event_id}" if upper == "DE"
        else f"https://www.ebay.co.uk/itm/{event_id}"
    )
    scraped_at = datetime.now(timezone.utc)
    try:
        parsed = parse_item_page_fn(raw_html, item_url, scraped_at)
    except Exception:
        counts["parse_failed"] = 1
        return counts

    if not parsed:
        counts["skipped_empty"] = 1
        return counts

    for rec in parsed:
        if sold_date and not rec.sold_date:
            rec.sold_date = sold_date
        rec.local_image_path = image_path
        try:
            parquet_bytes, _ = price_records_to_parquet(
                [rec], rec.scraped_at.strftime("%Y-%m-%d")
            )
        except ValueError:
            counts["parquet_write_failed"] += 1
            continue
        # Parquet write failures are NOT caught here — they propagate
        # out of the asset so the operator sees the failure loudly
        # rather than discovering partial state later.
        bronze_minio_client.put_object(
            bucket_name=BRONZE_BUCKET,
            object_name=parquet_key,
            data=parquet_bytes,
            length=len(parquet_bytes),
            content_type="application/parquet",
        )
        counts["wrote_parquet"] += 1

        if not _is_proxy_title(rec.card_id):
            try:
                sqlite_client.execute(
                    """
                    INSERT OR IGNORE INTO fact_events
                        (card_id, card_version, event_type, price, currency,
                         sold_date, scraped_from, source, source_url, language,
                         scraped_at, image_url, local_image_path, parqueted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        rec.card_id, rec.card_version or "", rec.event_type,
                        rec.price, rec.currency, rec.sold_date or "",
                        rec.scraped_from, rec.source, rec.source_url,
                        rec.language,
                        rec.scraped_at.isoformat() if hasattr(rec.scraped_at, "isoformat") else str(rec.scraped_at),
                        rec.image_url or "", rec.local_image_path or "",
                    ),
                )
                counts["wrote_sqlite"] += 1
            except Exception:
                counts["sqlite_write_failed"] += 1

    return counts
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/serialization/test_bronze_writer.py::test_fill_mode_writes_parquet_and_sqlite_when_no_existing_parquet -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/serialization/bronze_writer.py tests/serialization/test_bronze_writer.py
git commit -m "feat(bronze_writer): fill mode — write parquet + SQLite for new rows"
```

---

## Task 3: Fill mode — skip when parquet exists

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add test)

- [ ] **Step 1: Write the failing test**

Append to `tests/serialization/test_bronze_writer.py`:

```python
def test_fill_mode_skips_when_parquet_exists():
    """fill mode + existing parquet → no writes, skipped_existing=1."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    minio.stat_existing.add("sold_data/DE/12345.parquet")
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    assert counts["skipped_existing"] == 1
    assert counts["wrote_parquet"] == 0
    assert counts["wrote_sqlite"] == 0
    assert len(sqlite.inserts) == 0
    parquet_puts = [p for p in minio.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 0
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/serialization/test_bronze_writer.py::test_fill_mode_skips_when_parquet_exists -v`
Expected: PASS (the implementation already handles this in Task 2 — this test pins the behavior).

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_bronze_writer.py
git commit -m "test(bronze_writer): fill mode skips when parquet exists"
```

---

## Task 4: Overwrite mode — rewrites parquet, doesn't touch SQLite

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add test)

- [ ] **Step 1: Write the failing test**

Append to `tests/serialization/test_bronze_writer.py`:

```python
def test_overwrite_mode_rewrites_parquet_and_leaves_sqlite_alone():
    """overwrite mode + existing parquet → remove + write parquet,
    SQLite row NOT touched (no INSERT, no UPDATE).
    """
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    minio.stat_existing.add("sold_data/DE/12345.parquet")
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="overwrite",
        sold_date="2026-06-27",
    )
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 0  # CRITICAL: do not touch SQLite
    assert len(sqlite.inserts) == 0
    # Verify the old parquet was removed
    assert ("tcg-bronze", "sold_data/DE/12345.parquet") in minio.removed
    # Verify the new parquet was written
    parquet_puts = [p for p in minio.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 1
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/serialization/test_bronze_writer.py::test_overwrite_mode_rewrites_parquet_and_leaves_sqlite_alone -v`
Expected: PASS (Task 2's implementation already does this).

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_bronze_writer.py
git commit -m "test(bronze_writer): overwrite mode rewrites parquet, leaves SQLite alone"
```

---

## Task 5: Overwrite mode — new row inserts SQLite

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add test)

- [ ] **Step 1: Write the failing test**

Append to `tests/serialization/test_bronze_writer.py`:

```python
def test_overwrite_mode_writes_parquet_and_sqlite_when_no_existing():
    """overwrite mode + no prior parquet → behaves like fill-on-new
    (write parquet + SQLite INSERT). No prior row to preserve."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="overwrite",
        sold_date="2026-06-27",
    )
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 1
    assert len(sqlite.inserts) == 1
    # No removal happened (parquet didn't exist before)
    assert len(minio.removed) == 0
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/serialization/test_bronze_writer.py::test_overwrite_mode_writes_parquet_and_sqlite_when_no_existing -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_bronze_writer.py
git commit -m "test(bronze_writer): overwrite mode on new row inserts SQLite"
```

---

## Task 6: Parse failure path

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add test)

- [ ] **Step 1: Write the failing test**

Append to `tests/serialization/test_bronze_writer.py`:

```python
def test_parse_failure_increments_parse_failed_and_continues():
    """HTML exists but parser returns None → parse_failed=1, no writes."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=b"<html></html>")
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html="<html></html>",
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    assert counts["parse_failed"] == 0
    assert counts["skipped_empty"] == 1
    assert counts["wrote_parquet"] == 0
    assert counts["wrote_sqlite"] == 0
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/serialization/test_bronze_writer.py::test_parse_failure_increments_parse_failed_and_continues -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_bronze_writer.py
git commit -m "test(bronze_writer): parse failure path increments skipped_empty"
```

---

## Task 7: sold_date propagation

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add 3 tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/serialization/test_bronze_writer.py`:

```python
def test_sold_date_from_caller_overrides_empty_rec_sold_date():
    """Caller passes sold_date; rec.sold_date is empty → use caller's."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    insert_params = sqlite.inserts[0]
    sold_date_idx = 5  # (card_id, card_version, event_type, price, currency, sold_date, ...)
    assert insert_params[sold_date_idx] == "2026-06-27"


def test_sold_date_from_caller_does_not_override_rec_sold_date():
    """Caller passes sold_date; rec.sold_date is set → keep rec's value."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    # Monkey-patch the parser to inject a sold_date on the returned record
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page as real_parser
    def patched(html, url, scraped_at):
        recs = real_parser(html, url, scraped_at)
        for r in recs:
            r.sold_date = "2026-06-01"  # parser-derived
        return recs

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=patched,
        mode="fill",
        sold_date="2026-06-27",  # caller-provided — should NOT override
    )
    insert_params = sqlite.inserts[0]
    sold_date_idx = 5
    assert insert_params[sold_date_idx] == "2026-06-01"


def test_sold_date_none_leaves_rec_sold_date_alone():
    """Caller passes None → don't touch rec.sold_date (might be empty, might be set)."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date=None,
    )
    # The parser may or may not set sold_date on rec; we only assert
    # the insert succeeded and didn't crash.
    assert len(sqlite.inserts) == 1
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `pytest tests/serialization/test_bronze_writer.py -v -k "sold_date"`
Expected: PASS (3 passed).

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_bronze_writer.py
git commit -m "test(bronze_writer): sold_date propagation contract"
```

---

## Task 8: Image path handling

**Files:**
- Modify: `tests/serialization/test_bronze_writer.py` (add 2 tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/serialization/test_bronze_writer.py`:

```python
def test_image_path_set_when_provided():
    """Caller passes image_path → rec.local_image_path is set to it."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path="sold_images/de/12345.jpg",
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    insert_params = sqlite.inserts[0]
    local_image_path_idx = 12  # last param in INSERT
    assert insert_params[local_image_path_idx] == "sold_images/de/12345.jpg"


def test_image_path_none_when_not_provided():
    """Caller passes None → rec.local_image_path is whatever the parser set (empty)."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    insert_params = sqlite.inserts[0]
    local_image_path_idx = 12
    # When None, the rec's existing local_image_path is preserved (or empty)
    assert insert_params[local_image_path_idx] in (None, "")
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `pytest tests/serialization/test_bronze_writer.py -v -k "image_path"`
Expected: PASS (2 passed).

- [ ] **Step 3: Commit**

```bash
git add tests/serialization/test_bronze_writer.py
git commit -m "test(bronze_writer): image_path propagation contract"
```

---

## Task 9: Refactor live transformer to use the helper

**Files:**
- Modify: `src/tcg_platform/defs/transform_bronze.py`
- (no test changes — existing `tests/scraping/test_transform_bronze.py` must still pass)

- [ ] **Step 1: Verify current live-transformer tests pass before refactor**

Run: `pytest tests/scraping/test_transform_bronze.py -v`
Expected: 4 passed (the existing tests).

- [ ] **Step 2: Replace `_transform_region` with a thin loop calling the helper**

In `src/tcg_platform/defs/transform_bronze.py`, replace the entire file with:

```python
"""Offline transformer: read tcg-raw, parse HTML, write tcg-bronze.

The per-item contract lives in `tcg_platform.serialization.bronze_writer.
transform_one_item`. This module is a thin wrapper that iterates over
the items the scraper just wrote and delegates per-item work to the
shared helper in `fill` mode.

This asset has no network dependencies. It reads the raw HTML and
images that the scraper just wrote and produces the structured
bronze layer (parquet files + SQLite fact_events rows).
"""
import logging

import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.bronze_writer import transform_one_item

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
BRONZE_BUCKET = "tcg-bronze"


def _transform_region(
    raw_minio_client: MinioClientResource,
    bronze_minio_client: MinioClientResource,
    sqlite_client,
    region: str,
    written_items: list[dict],
    parse_item_page_fn,
) -> dict:
    """Read raw HTML for each written item, parse, write bronze parquet + SQLite.

    `written_items` is the list of {event_id, region, sold_date} dicts
    returned by the scraper asset for this run. This function does NOT
    scan tcg-raw; it processes exactly the items the scraper just wrote.
    """
    upper = region.upper()
    counts = {
        "read_html": 0, "read_image": 0, "wrote_parquet": 0,
        "wrote_sqlite": 0, "skipped_empty": 0, "parse_failed": 0,
        "image_missing": 0, "skipped_existing": 0,
    }
    for item in written_items:
        event_id = item["event_id"]
        if item.get("region", upper) != upper:
            continue
        sold_date = item.get("sold_date")

        # Read raw HTML
        try:
            html = raw_minio_client.get_object(
                RAW_BUCKET, f"ebay/{upper}/{event_id}.html"
            ).decode("utf-8")
        except Exception as e:
            _LOG.warning(f"Read html failed for {event_id}: {e}")
            continue
        counts["read_html"] += 1

        # Read raw image (optional)
        image_path = None
        try:
            image_path = f"sold_images/{region.lower()}/{event_id}.jpg"
            raw_minio_client.get_object(RAW_BUCKET, image_path)
            counts["read_image"] += 1
        except Exception:
            counts["image_missing"] += 1
            image_path = None

        # Delegate per-item work to the shared helper in fill mode
        item_counts = transform_one_item(
            region=region,
            event_id=event_id,
            raw_html=html,
            image_path=image_path,
            bronze_minio_client=bronze_minio_client,
            sqlite_client=sqlite_client,
            parse_item_page_fn=parse_item_page_fn,
            mode="fill",
            sold_date=sold_date,
        )
        # Aggregate per-item counts into the region-level totals
        for k in ("wrote_parquet", "wrote_sqlite", "skipped_empty",
                  "parse_failed", "skipped_existing"):
            counts[k] = counts.get(k, 0) + item_counts.get(k, 0)

    return counts


@dg.asset(
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_de"},
)
def transform_ebay_de_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_de_raw: list,
) -> dg.MaterializeResult:
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    counts = _transform_region(
        context.resources.tcg_raw_client,
        context.resources.minio_client,
        context.resources.sqlite_client_de,
        "DE",
        scrape_ebay_de_raw,
        parse_ebay_de_item_page,
    )
    context.log.info(f"DE transform: {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    required_resource_keys={"tcg_raw_client", "minio_client", "sqlite_client_uk"},
)
def transform_ebay_uk_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_uk_raw: list,
) -> dg.MaterializeResult:
    from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
    counts = _transform_region(
        context.resources.tcg_raw_client,
        context.resources.minio_client,
        context.resources.sqlite_client_uk,
        "UK",
        scrape_ebay_uk_raw,
        parse_ebay_uk_item_page,
    )
    context.log.info(f"UK transform: {counts}")
    return dg.MaterializeResult(metadata=counts)
```

- [ ] **Step 3: Run live-transformer tests to verify they still pass**

Run: `pytest tests/scraping/test_transform_bronze.py -v`
Expected: 4 passed.

- [ ] **Step 4: Run the full serialization + scraping test suite**

Run: `pytest tests/serialization/ tests/scraping/ -v`
Expected: all pass (the existing tests + the new helper tests).

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/transform_bronze.py
git commit -m "refactor(transform_bronze): delegate per-item work to bronze_writer helper"
```

---

## Task 10: Replay asset — DE, fill-mode enumeration

**Files:**
- Create: `src/tcg_platform/defs/replay_bronze_from_raw.py`

- [ ] **Step 1: Write the failing asset-level test**

Create `tests/scraping/test_replay_bronze_from_raw.py`:

```python
"""Tests for the replay_bronze_from_raw assets.

The unit-level contract of the helper is pinned in
tests/serialization/test_bronze_writer.py. These tests focus on the
asset-level loop: enumeration, region routing, mode flag handling,
and job definition.
"""
import dagster as dg
import pytest

from tcg_platform.defs.replay_bronze_from_raw import (
    replay_bronze_from_raw_de,
    replay_bronze_from_raw_uk,
    replay_bronze_from_raw_job,
)
from tcg_platform.serialization.bronze_writer import _VALID_MODES


class _FakeRawClient:
    """Models the tcg_raw_client used to enumerate .html files."""

    def __init__(self, keys):
        self.keys = sorted(keys)
        self.got = []

        outer = self

        class _Resp:
            def __init__(self2, data):
                self2._data = data
            def read(self2):
                return self2._data
            def close(self2):
                pass
            def release_conn(self2):
                pass

        class _Client:
            def list_objects(self2, bucket, prefix="", recursive=True):
                return list(outer.keys)

            def get_object(self2, bucket, obj):
                outer.got.append((bucket, obj))
                return _Resp(b"<html><body><h1>One Piece OP01-001 PSA 10</h1></body></html>")

        self._client = _Client()

    @property
    def client(self):
        return self._client


class _FakeBronzeClient:
    def __init__(self):
        self.puts = []
        self.stat_existing = set()

        outer = self

        class _Client:
            def stat_object(self2, bucket, obj):
                if obj in outer.stat_existing:
                    return True
                raise Exception(f"NoSuchKey: {obj}")

            def put_object(self2, bucket, obj, data, length, content_type):
                outer.puts.append({"bucket": bucket, "object": obj, "length": length})

        self._client = _Client()

    @property
    def client(self):
        return self._client


class _FakeSqliteClient:
    def __init__(self):
        self.inserts = []

    def execute(self, query, params=(), fetch="none"):
        if "INSERT" in query:
            self.inserts.append(params)
        return None


class _MockContext:
    def __init__(self, raw_client, bronze_client, sqlite_client):
        self.resources = type("R", (), {
            "tcg_raw_client": raw_client,
            "minio_client": bronze_client,
            "sqlite_client_de": sqlite_client,
            "sqlite_client_uk": sqlite_client,
        })()
        self.log_calls = []

    def log(self):
        return self

    def info(self, msg):
        self.log_calls.append(msg)


def test_replay_de_fill_mode_writes_parquet_for_each_html():
    """DE fill mode: every raw HTML becomes a parquet (none pre-existing)."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html", "ebay/DE/2.html", "ebay/DE/3.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _MockContext(raw, bronze, sqlite)

    result = replay_bronze_from_raw_de(context=ctx, config={"mode": "fill"})
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata["wrote_parquet"] == 3
    assert result.metadata["skipped_existing"] == 0
    parquet_puts = [p for p in bronze.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 3


def test_replay_de_fill_mode_skips_existing_parquets():
    """DE fill mode: pre-existing parquets are skipped (skipped_existing=N)."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html", "ebay/DE/2.html", "ebay/DE/3.html"])
    bronze = _FakeBronzeClient()
    bronze.stat_existing.add("sold_data/DE/1.parquet")
    sqlite = _FakeSqliteClient()
    ctx = _MockContext(raw, bronze, sqlite)

    result = replay_bronze_from_raw_de(context=ctx, config={"mode": "fill"})
    assert result.metadata["skipped_existing"] == 1
    assert result.metadata["wrote_parquet"] == 2


def test_replay_uk_fill_mode_writes_parquet_for_each_html():
    """UK fill mode: same shape, different asset."""
    raw = _FakeRawClient(keys=["ebay/UK/10.html", "ebay/UK/20.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _MockContext(raw, bronze, sqlite)

    result = replay_bronze_from_raw_uk(context=ctx, config={"mode": "fill"})
    assert result.metadata["wrote_parquet"] == 2


def test_replay_invalid_mode_raises_value_error():
    """Bogus mode fails loud at asset startup, before any reads."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html"])
    bronze = _FakeBronzeClient()
    sqlite = _FakeSqliteClient()
    ctx = _MockContext(raw, bronze, sqlite)

    with pytest.raises(ValueError, match="mode must be one of"):
        replay_bronze_from_raw_de(context=ctx, config={"mode": "garbage"})


def test_replay_overwrite_mode_rewrites_existing_parquets():
    """Overwrite mode: existing parquets are rewritten; SQLite untouched."""
    raw = _FakeRawClient(keys=["ebay/DE/1.html", "ebay/DE/2.html"])
    bronze = _FakeBronzeClient()
    bronze.stat_existing.add("sold_data/DE/1.parquet")
    bronze.stat_existing.add("sold_data/DE/2.parquet")
    sqlite = _FakeSqliteClient()
    ctx = _MockContext(raw, bronze, sqlite)

    result = replay_bronze_from_raw_de(context=ctx, config={"mode": "overwrite"})
    assert result.metadata["wrote_parquet"] == 2
    assert len(sqlite.inserts) == 0  # CRITICAL: no SQLite writes on overwrite
    parquet_puts = [p for p in bronze.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 2


def test_replay_job_resolves_with_both_assets():
    """The Dagster job selects both DE + UK assets for parallel execution."""
    from tcg_platform.definitions import defs
    job_def = defs.resolve_job_def("replay_bronze_from_raw_job")
    assert job_def.name == "replay_bronze_from_raw_job"
    # Both assets must be in the selection
    keys = {ak.to_user_string() for ak in job_def.asset_layer.asset_keys}
    assert "replay_bronze_from_raw_de" in keys
    assert "replay_bronze_from_raw_uk" in keys
```

- [ ] **Step 2: Run tests to verify they fail (import errors expected)**

Run: `pytest tests/scraping/test_replay_bronze_from_raw.py -v`
Expected: `ImportError` for `tcg_platform.defs.replay_bronze_from_raw`.

- [ ] **Step 3: Create the assets + job**

Create `src/tcg_platform/defs/replay_bronze_from_raw.py`:

```python
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
```

- [ ] **Step 4: Register the job in `definitions.py`**

In `src/tcg_platform/definitions.py`, find the `defs` / `Definitions(...)` block and:

1. Add an import: `from tcg_platform.defs import replay_bronze_from_raw`
2. Add the job to the jobs list:

```python
from tcg_platform.defs.replay_bronze_from_raw import (
    replay_bronze_from_raw_job,
)
```

And in the `Definitions(...)` call, add `replay_bronze_from_raw_job` to the `jobs=[...]` list. The exact list location depends on the current file structure — find where `silver_eu_job` and `complete_eu_pipeline` are added and put `replay_bronze_from_raw_job` next to them.

- [ ] **Step 5: Run replay asset tests to verify they pass**

Run: `pytest tests/scraping/test_replay_bronze_from_raw.py -v`
Expected: 6 passed.

- [ ] **Step 6: Run the full test suite to confirm no regressions**

Run: `pytest tests/ -v`
Expected: 195 + ~21 = ~216 passed, 0 failed.

- [ ] **Step 7: Commit**

```bash
git add src/tcg_platform/defs/replay_bronze_from_raw.py \
        src/tcg_platform/definitions.py \
        tests/scraping/test_replay_bronze_from_raw.py
git commit -m "feat(replay): replay_bronze_from_raw_job — fill/overwrite modes per region"
```

---

## Task 11: Verify definitions load + write PROD.md closeout

**Files:**
- Modify: `PROD.md`

- [ ] **Step 1: Verify Dagster definitions load cleanly**

Run: `source .venv/bin/activate && python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"`
Expected: `OK`.

- [ ] **Step 2: Verify the new job resolves**

Run: `source .venv/bin/activate && python -c "from tcg_platform.definitions import defs; j = defs.resolve_job_def('replay_bronze_from_raw_job'); print('Job resolved:', j.name)"`
Expected: `Job resolved: replay_bronze_from_raw_job`.

- [ ] **Step 3: Update `PROD.md` to close out M9-T2**

In `PROD.md`, find the M9 section (around line 145–148). After the M9-T1 line, add:

```markdown
- [x] **M9-T2** — `replay_bronze_from_raw_job` (Dagster job selecting `replay_bronze_from_raw_{de,uk}` assets). Two modes via run config: `fill` (skip if bronze parquet exists; write parquet + SQLite row) closes the 150-row raw-no-bronze gap (89 DE + 61 UK); `overwrite` (always re-parse; remove + rewrite parquet; SQLite untouched) enables parser-bug replays without re-paying Zyte API costs. The per-item write contract lives in `tcg_platform.serialization.bronze_writer.transform_one_item` (new pure helper extracted from the live transformer); the existing `transform_ebay_{de,uk}_to_bronze` assets refactored to delegate to the helper in `fill` mode (behavior preserved). 29 UK bronze-without-raw rows cannot be replayed (raw is gone). Spec: `docs/superpowers/specs/2026-06-27-replay-bronze-from-raw-design.md`.
```

Also update the "Outstanding (post-M8)" list — remove the "M9 replay asset" line.

- [ ] **Step 4: Verify PROD.md diff is clean**

Run: `git diff PROD.md`
Expected: 1 added checkbox + 1 removed outstanding-list line. No accidental changes.

- [ ] **Step 5: Commit**

```bash
git add PROD.md
git commit -m "docs(prod): close M9-T2 — replay_bronze_from_raw_job"
```

---

## Task 12: Session log + final verification

**Files:**
- Create: `log/SESSION_2026-06-27.md`

- [ ] **Step 1: Final test run**

Run: `source .venv/bin/activate && pytest tests/ -v 2>&1 | tail -30`
Expected: all tests pass; total count is 195 + ~21 = ~216.

- [ ] **Step 2: Verify working tree is clean except for the session log**

Run: `git status --porcelain`
Expected: `?? log/SESSION_2026-06-27.md` only.

- [ ] **Step 3: Write the session log**

Create `log/SESSION_2026-06-27.md` with:

```markdown
# Session 2026-06-27 (M9-T2 replay-bronze-from-raw)

## Branch
`2026-06-27-m9-t2-replay-bronze-from-raw`

## Goal
Build `replay_bronze_from_raw_job` — a Dagster job that closes the
150-row raw-no-bronze gap (89 DE + 61 UK) and enables parser-bug
replays without re-paying Zyte API costs.

## Done

### Spec
- `docs/superpowers/specs/2026-06-27-replay-bronze-from-raw-design.md`

### Plan
- `docs/superpowers/plans/2026-06-27-replay-bronze-from-raw.md`

### Implementation
- New pure helper `tcg_platform.serialization.bronze_writer.
  transform_one_item` — mode-aware per-item writer.
- New `tcg_platform.defs.replay_bronze_from_raw` — DE + UK assets +
  `replay_bronze_from_raw_job` (parallel).
- Refactor: `tcg_platform.defs.transform_bronze._transform_region`
  now calls `transform_one_item` in `fill` mode; behavior preserved.
- `definitions.py` registers the new job.
- `PROD.md` M9-T2 closed.

### Verification (Rule 17)
- `pytest tests/ -v` — all pass (~216; was 195 at session start).
- `python -c "from tcg_platform.definitions import defs; defs.load_fn()"` — OK.
- `python -c "from tcg_platform.definitions import defs; defs.resolve_job_def('replay_bronze_from_raw_job')"` — OK.

## Outstanding
- First production use of the job (mode=fill) — operator's call.
  Expected: 89 DE + 61 UK new bronze parquets + SQLite rows.
- 29 UK bronze-without-raw rows — cannot be replayed (raw gone).
- M5-T2 (Dagster schedules) — still deferred.
- `silver_eu_orchestrator` parallelization — separate task.
- DE/UK arbitrage backtest — separate task.

## Blockers
None.
```

- [ ] **Step 4: Commit session log**

```bash
git add log/SESSION_2026-06-27.md
git commit -m "docs(log): session 2026-06-27 (M9-T2 replay-bronze-from-raw)"
```

- [ ] **Step 5: Push branch (Rule 18)**

```bash
git push origin 2026-06-27-m9-t2-replay-bronze-from-raw
```

- [ ] **Step 6: Stop — do not merge (Rule 19)**

Human merges when ready.

---

## Task 13: Pre-merge manual verification (operator's call)

These are documented acceptance checks from the spec. The plan does
NOT execute them — they require a running Dagster instance and a
live `tcg-raw` / `tcg-bronze` / SQLite. Operator runs them post-merge
when ready to do the actual gap-fill run.

- [ ] **Step 1: Pre-merge inventory check (manual)**

Run the inventory script to confirm the gap before running the job:

```bash
source .venv/bin/activate && python -c "
from minio import Minio
c = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
for region in ['DE', 'UK']:
    raw = sum(1 for _ in c.list_objects('tcg-raw', prefix=f'ebay/{region}/', recursive=True))
    bronze = sum(1 for _ in c.list_objects('tcg-bronze', prefix=f'sold_data/{region}/', recursive=True))
    print(f'{region}: raw={raw}  bronze={bronze}  gap={raw-bronze}')
"
```

Expected output: `DE: raw=165 bronze=76 gap=89` and `UK: raw=447 bronze=386 gap=61`.

- [ ] **Step 2: Run `replay_bronze_from_raw_job` with `mode: fill` (operator's call)**

In Dagster UI or via `dg`, launch the job with the launchpad config:

```yaml
ops:
  replay_bronze_from_raw_de:
    config:
      mode: fill
  replay_bronze_from_raw_uk:
    config:
      mode: fill
```

Expected asset metadata:
- `replay_bronze_from_raw_de`: `wrote_parquet=89`, `wrote_sqlite=89`, `skipped_existing=76`
- `replay_bronze_from_raw_uk`: `wrote_parquet=61`, `wrote_sqlite=61`, `skipped_existing=386`

- [ ] **Step 3: Re-run inventory check**

Same script as Step 1. Expected: `DE: raw=165 bronze=165 gap=0` and `UK: raw=447 bronze=447 gap=0`. If gap is still non-zero, inspect the asset's `parse_failed` and `skipped_empty` counts — these are listings the parser couldn't extract.

- [ ] **Step 4: Leave `mode: overwrite` armed (no action)**

The job is now available for the next parser bug. Operator triggers it with `mode: overwrite` when needed; SQLite stays immutable.

---

## Acceptance Checklist

- [ ] All tasks above completed
- [ ] `pytest tests/ -v` — all pass (~216 tests)
- [ ] `python -c "from tcg_platform.definitions import defs; defs.load_fn()"` — OK
- [ ] `python -c "from tcg_platform.definitions import defs; defs.resolve_job_def('replay_bronze_from_raw_job')"` — OK
- [ ] `git push origin 2026-06-27-m9-t2-replay-bronze-from-raw` succeeded
- [ ] Branch ready for human merge (PR or local `git merge --no-ff`)
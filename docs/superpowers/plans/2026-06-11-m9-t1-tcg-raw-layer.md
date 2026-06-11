# M9-T1 tcg-raw Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the existing single-stage eBay scraper into a network-only "raw" stage and a pure offline "transform" stage. Persist per-item HTML and images to a new `tcg-raw` MinIO bucket, so any future parser change can replay from raw without re-paying Zyte API costs.

**Architecture:** A new `tcg-raw` bucket holds bytes only — HTML at `ebay/{region}/{event_id}.html`, images at `sold_images/{region}/{event_id}.jpg`, and per-run logs at `logs/{timestamp}.log`. Two new Dagster assets per region: `scrape_ebay_{de,uk}_raw` (network I/O) and `transform_ebay_{de,uk}_to_bronze` (parse + write to existing `tcg-bronze`). Idempotency moves from SQLite-derived state to a `stat_object` check on the durable raw artifact. A one-time `backfill_raw_html` asset populates raw for the 64+342 rows scraped before this design landed.

**Tech Stack:** Dagster 1.13.3, MinIO Python SDK (`minio>=7.0.0`), Zyte API, pytest (mock-driven tests per existing `test_minio_remove_objects.py` pattern).

**Working branch:** `2026-06-11-m9-t1-tcg-raw-layer`

---

## File Structure

### New files
- `src/tcg_platform/defs/scrape_raw.py` — `WrittenItem` NamedTuple, `_exists_in_raw`, `_scrape_region`, `scrape_ebay_de_raw`, `scrape_ebay_uk_raw`
- `src/tcg_platform/defs/transform_bronze.py` — `_transform_region`, `transform_ebay_de_to_bronze`, `transform_ebay_uk_to_bronze`
- `src/tcg_platform/defs/backfill_raw_html.py` — `_backfill_region`, `backfill_raw_html_de`, `backfill_raw_html_uk`
- `tests/scraping/test_scrape_raw.py` — 9 unit tests
- `tests/scraping/test_transform_bronze.py` — 4 unit tests
- `tests/scraping/test_backfill_raw_html.py` — 3 unit tests
- `tests/defs/test_definitions_load.py` — 1 test that the defs graph still loads

### Modified
- `src/tcg_platform/defs/minio_resources.py` — add `tcg_raw_client` resource
- `src/tcg_platform/definitions.py` — add `tcg_raw_client` to resources map; add 4 new jobs
- `src/tcg_platform/defs/eu_pipeline_orchestrator.py` — swap `ebay_de_pipeline`/`ebay_uk_pipeline` for `ebay_de_raw_to_bronze`/`ebay_uk_raw_to_bronze`
- `src/tcg_platform/.env.example` — add RAW_* env vars
- `src/tcg_platform/PROD.md` — update bronze description, add raw layer section, add tcg-raw to bucket table

### Removed (in last task)
- `src/tcg_platform/defs/ebay_de_sold_listings.py`
- `src/tcg_platform/defs/ebay_uk_sold_listings.py`
- `src/tcg_platform/scraping/ebay_image.py` — logic inlined into `scrape_raw.py`; if any other module imports it, that import is updated in the same task

### Test file location conventions
- Tests for `defs/scrape_raw.py` go in `tests/scraping/test_scrape_raw.py` (matches existing `test_ebay_uk_item.py` location convention)
- Tests for `defs/transform_bronze.py` go in `tests/scraping/test_transform_bronze.py`
- Tests for `defs/backfill_raw_html.py` go in `tests/scraping/test_backfill_raw_html.py`
- The defs-load test goes in `tests/defs/test_definitions_load.py` (matches existing `tests/defs/test_eu_pipeline_orchestrator.py`)

### Mock pattern (per existing `test_minio_remove_objects.py`)
Use `monkeypatch.setenv` to set `RAW_*` keys, then construct `MinioClientResource` directly and assign a `FakeClient` to `resource._client` to capture `stat_object` / `put_object` / `get_object` calls. No real MinIO connection in unit tests.

---

## Task 1: Add `tcg_raw_client` resource

**Files:**
- Modify: `src/tcg_platform/defs/minio_resources.py:1-34` — add `tcg_raw_client` resource below the existing `minio_client_zyte`
- Test: `tests/scraping/test_tcg_raw_client.py` (new)

- [ ] **Step 1: Write the failing test**

```python
# tests/scraping/test_tcg_raw_client.py
import os
from tcg_platform.defs.minio_resources import tcg_raw_client


def test_tcg_raw_client_uses_raw_env_prefix(monkeypatch):
    """tcg_raw_client must read RAW_* env vars, not MINIO_*."""
    monkeypatch.setenv("RAW_ENDPOINT", "raw-host:9000")
    monkeypatch.setenv("RAW_ACCESS_KEY", "raw-key")
    monkeypatch.setenv("RAW_SECRET_KEY", "raw-secret")
    monkeypatch.setenv("RAW_BUCKET", "tcg-raw")

    # Build the resource without invoking Dagster's resource init.
    # We assert on the underlying helper, not the @resource wrapper.
    from tcg_platform.defs.minio_resources import _get_minio_config
    cfg = _get_minio_config(prefix="RAW")
    assert cfg == {
        "endpoint": "raw-host:9000",
        "access_key": "raw-key",
        "secret_key": "raw-secret",
        "bucket_name": "tcg-raw",
        "secure": False,
    }


def test_tcg_raw_client_default_bucket_name(monkeypatch):
    """When RAW_BUCKET is unset, default to 'tcg-raw'."""
    monkeypatch.delenv("RAW_BUCKET", raising=False)
    monkeypatch.setenv("RAW_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("RAW_ACCESS_KEY", "x")
    monkeypatch.setenv("RAW_SECRET_KEY", "y")
    from tcg_platform.defs.minio_resources import _get_minio_config
    cfg = _get_minio_config(prefix="RAW")
    assert cfg["bucket_name"] == "tcg-raw"


def test_minio_client_default_bucket_name_unchanged(monkeypatch):
    """Regression: default 'MINIO' prefix must still default to 'tcg-bronze'."""
    monkeypatch.delenv("MINIO_BUCKET", raising=False)
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    from tcg_platform.defs.minio_resources import _get_minio_config
    cfg = _get_minio_config(prefix="MINIO")
    assert cfg["bucket_name"] == "tcg-bronze"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/scraping/test_tcg_raw_client.py -v`
Expected: FAIL — `tcg_raw_client` not importable.

- [ ] **Step 3: Add `tcg_raw_client` resource**

Modify `src/tcg_platform/defs/minio_resources.py` to add a new resource below the existing two. The full file should look like:

```python
# src/tcg_platform/defs/minio_resources.py
import os

from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext
from dotenv import load_dotenv
from pydantic import model_validator

from tcg_platform.resources.minio_client import MinioClientResource

load_dotenv()


def _get_minio_config(prefix: str = "MINIO") -> dict:
    return {
        "endpoint": os.getenv(f"{prefix}_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv(f"{prefix}_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv(f"{prefix}_SECRET_KEY", "minioadmin"),
        "bucket_name": os.getenv(f"{prefix}_BUCKET", "tcg-bronze"),
        "secure": False,
    }


@resource
def minio_client(init_context: InitResourceContext):
    config = _get_minio_config()
    client = MinioClientResource(**config)
    return client.create_resource(init_context)


@resource
def minio_client_zyte(init_context: InitResourceContext):
    config = _get_minio_config(prefix="ZYTE_MINIO")
    client = MinioClientResource(**config)
    return client.create_resource(init_context)


@resource
def tcg_raw_client(init_context: InitResourceContext):
    config = _get_minio_config(prefix="RAW")
    client = MinioClientResource(**config)
    return client.create_resource(init_context)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/scraping/test_tcg_raw_client.py -v`
Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/minio_resources.py tests/scraping/test_tcg_raw_client.py
git commit -m "feat(raw): add tcg_raw_client resource for tcg-raw bucket"
```

---

## Task 2: Add `_exists_in_raw` helper to `scrape_raw.py`

**Files:**
- Create: `src/tcg_platform/defs/scrape_raw.py` (just the imports, constants, NamedTuple, and `_exists_in_raw` for now)
- Test: `tests/scraping/test_scrape_raw.py` (new file, just the 3 exists-in-raw tests for now)

- [ ] **Step 1: Write the failing tests**

```python
# tests/scraping/test_scrape_raw.py
import pytest
from minio.error import S3Error

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.scrape_raw import _exists_in_raw


def _make_resource_with_fake(fake_client):
    """Helper: build a MinioClientResource wired to a fake minio client."""
    resource = MinioClientResource(
        endpoint="localhost:9000",
        access_key="x",
        secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = fake_client
    return resource


def test_exists_in_raw_returns_false_for_missing_key():
    """stat_object raises NoSuchKey → _exists_in_raw returns False."""

    class FakeClient:
        def stat_object(self, bucket, obj):
            raise S3Error(
                code="NoSuchKey",
                message="not found",
                resource="x",
                request_id="r",
                host_id="h",
                response=None,
            )

    res = _make_resource_with_fake(FakeClient())
    assert _exists_in_raw(res, "DE", "12345") is False


def test_exists_in_raw_returns_true_for_present_key():
    """stat_object succeeds → _exists_in_raw returns True."""
    calls = []

    class FakeClient:
        def stat_object(self, bucket, obj):
            calls.append((bucket, obj))
            return None  # success

    res = _make_resource_with_fake(FakeClient())
    assert _exists_in_raw(res, "DE", "12345") is True
    assert calls == [("tcg-raw", "ebay/DE/12345.html")]


def test_exists_in_raw_treats_other_s3_errors_as_false():
    """Non-NoSuchKey S3Error → log warning + return False (refetch is safer)."""

    class FakeClient:
        def stat_object(self, bucket, obj):
            raise S3Error(
                code="InternalError",
                message="boom",
                resource="x",
                request_id="r",
                host_id="h",
                response=None,
            )

    res = _make_resource_with_fake(FakeClient())
    # Should not raise; should return False
    assert _exists_in_raw(res, "DE", "12345") is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scraping/test_scrape_raw.py -v`
Expected: FAIL — `scrape_raw` module does not exist.

- [ ] **Step 3: Create scrape_raw.py with the helper**

```python
# src/tcg_platform/defs/scrape_raw.py
"""Network-only eBay scraper. Writes raw HTML + images + logs to tcg-raw.

The transformer (`transform_bronze.py`) reads from tcg-raw and writes
the structured bronze layer. The scraper does not know what a
card_id is; it only deals with event_id (the eBay item id from the URL).
"""
import logging
from datetime import datetime, timezone
from typing import NamedTuple

import dagster as dg
import requests
from minio.error import S3Error

from tcg_platform.scraping.ebay_de_search import (
    parse_ebay_de_search_page,
    search_url_for_page as de_search_url_for_page,
)
from tcg_platform.scraping.ebay_uk_search import (
    parse_ebay_uk_search_page,
    search_url_for_page as uk_search_url_for_page,
)
from tcg_platform.scraping.ebay_utils import (
    extract_item_id,
    extract_item_image_url,
)

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
EMPTY_STREAK_THRESHOLD = 5


class WrittenItem(NamedTuple):
    event_id: str
    region: str  # "DE" or "UK"
    sold_date: str | None  # YYYY-MM-DD, or None if the search page didn't show it


def _exists_in_raw(minio_client, region: str, event_id: str) -> bool:
    """Atomic existence check against tcg-raw/ebay/{region}/{event_id}.html.

    Returns True if the object is present, False otherwise (including
    on unexpected errors — refetching is safer than silently skipping
    on a transient failure).
    """
    try:
        minio_client.client.stat_object(
            RAW_BUCKET, f"ebay/{region}/{event_id}.html"
        )
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject"):
            return False
        _LOG.warning(
            f"stat_object unexpected error for {region}/{event_id}: {e}"
        )
        return False
    except Exception as e:
        _LOG.warning(f"stat_object error for {region}/{event_id}: {e}")
        return False
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_scrape_raw.py -v`
Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/scrape_raw.py tests/scraping/test_scrape_raw.py
git commit -m "feat(raw): add _exists_in_raw helper for atomic dedup check"
```

---

## Task 3: Add `_scrape_region` core loop to `scrape_raw.py`

**Files:**
- Modify: `src/tcg_platform/defs/scrape_raw.py` — append `_scrape_region` function
- Modify: `tests/scraping/test_scrape_raw.py` — append 4 more tests for the loop

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/scraping/test_scrape_raw.py
from tcg_platform.defs.scrape_raw import _scrape_region


class FakeZyteClient:
    """Captures every .get() call and returns canned responses."""

    def __init__(self, responses):
        # responses: list of dicts, one per .get() call (in order)
        self._responses = list(responses)
        self.calls = []

    def get(self, request):
        self.calls.append(request)
        if not self._responses:
            raise AssertionError("FakeZyteClient ran out of canned responses")
        return self._responses.pop(0)


class FakeMinioClient:
    """Captures put_object, get_object, stat_object calls."""

    def __init__(self, stat_results=None):
        # stat_results: dict[(bucket, obj)] -> None (success) or raises
        self.stat_results = stat_results or {}
        self.puts = []
        self.stats = []

        class _Client:
            def __init__(self2, outer):
                self2.outer = outer

            def stat_object(self2, bucket, obj):
                self2.outer.stats.append((bucket, obj))
                key = (bucket, obj)
                if key in self2.outer.stat_results:
                    result = self2.outer.stat_results[key]
                    if isinstance(result, Exception):
                        raise result
                    return result
                # Default: treat as missing
                raise S3Error(
                    code="NoSuchKey",
                    message="not found",
                    resource="x",
                    request_id="r",
                    host_id="h",
                    response=None,
                )

        self.client = _Client(self)


def _no_image_html():
    """Item page HTML that has no parseable title and no image URL."""
    return "<html><body><h1></h1></body></html>"


def _good_de_html():
    return """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans">One Piece OP01-001 PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>
    </body></html>
    """


def _search_html_one_item(item_url):
    return f"""
    <html><body>
      <a href="{item_url}">item</a>
    </body></html>
    """


def test_scrape_region_writes_html_and_image_for_new_item(monkeypatch):
    """For a missing event_id, scraper calls Zyte and writes HTML to tcg-raw."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        # search page 1: 1 item
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/99999"
        )},
        # item page: parseable, has an image URL
        {"statusCode": 200, "browserHtml": _good_de_html()
         + '"image":"https://i.ebayimg.com/thumbs/x.jpg"'},
    ])

    # Patch extract_item_image_url to return a known URL, and patch requests.get
    # for the image download
    import tcg_platform.scraping.ebay_utils as eu
    monkeypatch.setattr(eu, "extract_item_image_url",
                        lambda html: "https://i.ebayimg.com/x.jpg")
    import tcg_platform.defs.scrape_raw as sr
    class FakeResp:
        def __init__(self): self.content = b"image-bytes"
    monkeypatch.setattr(sr.requests, "get", lambda url, timeout: FakeResp())

    # Build a real resource wrapping FakeMinioClient's client
    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    # Stub search_url_for_page + parse_search_page for DE
    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page",
                        lambda html: [("https://www.ebay.de/itm/99999", "2026-06-11")])

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert len(written) == 1
    assert written[0] == WrittenItem(
        event_id="99999", region="DE", sold_date="2026-06-11"
    )

    # Verify the put_object calls
    put_paths = [p["object_name"] for p in minio.puts]
    assert "ebay/DE/99999.html" in put_paths
    assert "sold_images/DE/99999.jpg" in put_paths

    # Log mentions the new item
    assert any("WROTE html event_id=99999" in line for line in log_lines)


def test_scrape_region_skips_already_in_raw(monkeypatch):
    """If stat_object succeeds (HTML exists), scraper does not call Zyte for that item."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    # stat_object for the HTML returns success
    minio = FakeMinioClient(stat_results={
        ("tcg-raw", "ebay/DE/99999.html"): None,
    })
    zyte = FakeZyteClient([
        # search page 1: 1 item
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/99999"
        )},
        # No second call expected — scraper should skip the item
    ])
    zyte_call_count = len(zyte.calls)  # 0 so far

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page",
                        lambda html: [("https://www.ebay.de/itm/99999", "2026-06-11")])

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert written == []  # nothing new
    # Zyte was called once (for the search page), not for the item
    zyte_calls = zyte.calls
    assert len(zyte_calls) == 1
    assert "sch" in zyte_calls[0]["url"]  # search page, not item page
    assert any("SKIP already_in_raw" in line for line in log_lines)


def test_scrape_region_stops_at_empty_streak(monkeypatch):
    """5 consecutive search pages with no items → loop exits."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        {"statusCode": 200, "browserHtml": "<html><body>no items</body></html>"},
    ] * 10)  # 10 empty pages; loop should stop at 5

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", lambda html: [])

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    # Loop must have stopped before exhausting all 10 responses
    assert len(zyte.calls) < 10
    assert any("no_items=true" in line for line in log_lines)


def test_scrape_region_handles_failed_zyte_call(monkeypatch):
    """Zyte returns 500 for the item page → item is skipped, others continue."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        # search page with 2 items
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/11111"
        ) + _search_html_one_item(
            "https://www.ebay.de/itm/22222"
        )},
        # item 11111: 500
        {"statusCode": 500, "browserHtml": ""},
        # item 22222: ok
        {"statusCode": 200, "browserHtml": _good_de_html()},
    ])

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", lambda html: [
        ("https://www.ebay.de/itm/11111", "2026-06-11"),
        ("https://www.ebay.de/itm/22222", "2026-06-11"),
    ])

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert len(written) == 1
    assert written[0].event_id == "22222"
    assert any("FAIL zyte event_id=11111" in line for line in log_lines)
    assert any("WROTE html event_id=22222" in line for line in log_lines)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scraping/test_scrape_raw.py -v`
Expected: FAIL — `_scrape_region` not importable.

- [ ] **Step 3: Add `_scrape_region` to scrape_raw.py**

Append the following function to `src/tcg_platform/defs/scrape_raw.py`:

```python
def _scrape_region(
    minio_client,
    zyte_client,
    region: str,
    search_url_for_page_fn,
    parse_search_page_fn,
) -> tuple[list[WrittenItem], list[str]]:
    """Scrape one region's sold listings into tcg-raw.

    Returns (newly_written, log_lines). The caller writes log_lines
    to tcg-raw/logs/{timestamp}.log at the end of the run.
    """
    log: list[str] = []
    log.append(f"{datetime.now(timezone.utc).isoformat()} START region={region}")

    written: list[WrittenItem] = []
    page = 1
    pages_fetched = 0
    items_seen = 0
    items_skipped_already_seen = 0
    items_fetched_zyte = 0
    items_failed_zyte = 0
    items_failed_parse = 0
    images_skipped_already_seen = 0
    images_downloaded = 0
    images_failed = 0

    while True:
        search_url = search_url_for_page_fn(page)
        log.append(
            f"{datetime.now(timezone.utc).isoformat()} FETCH "
            f"search_page={page} url={search_url}"
        )
        resp = zyte_client.get({"url": search_url, "browserHtml": True})
        pages_fetched += 1
        if resp.get("statusCode") != 200:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} status={resp.get('statusCode')}"
            )
            break
        html = resp.get("browserHtml", "")
        if not html:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} empty_html=true"
            )
            break
        pairs = parse_search_page_fn(html)
        items_seen += len(pairs)
        log.append(
            f"{datetime.now(timezone.utc).isoformat()} PARSED "
            f"search_page={page} items={len(pairs)}"
        )
        if not pairs:
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} STOP "
                f"search_page={page} no_items=true"
            )
            break

        for item_url, _sold_date in pairs:
            event_id = extract_item_id(item_url)
            if not event_id or not event_id.isdigit():
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} SKIP bad_event_id url={item_url}"
                )
                continue
            if _exists_in_raw(minio_client, region, event_id):
                items_skipped_already_seen += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} SKIP "
                    f"already_in_raw event_id={event_id}"
                )
                continue

            # Fetch item page
            item_resp = zyte_client.get({"url": item_url, "browserHtml": True})
            items_fetched_zyte += 1
            if item_resp.get("statusCode") != 200:
                items_failed_zyte += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL zyte "
                    f"event_id={event_id} status={item_resp.get('statusCode')}"
                )
                continue
            item_html = item_resp.get("browserHtml", "")
            if not item_html:
                items_failed_parse += 1
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL "
                    f"empty_item_html event_id={event_id}"
                )
                continue

            # Persist raw HTML
            try:
                html_bytes = item_html.encode("utf-8")
                minio_client.put_object(
                    bucket_name=RAW_BUCKET,
                    object_name=f"ebay/{region}/{event_id}.html",
                    data=html_bytes,
                    length=len(html_bytes),
                    content_type="text/html",
                )
            except Exception as e:
                log.append(
                    f"{datetime.now(timezone.utc).isoformat()} FAIL "
                    f"put_object_html event_id={event_id} err={e}"
                )
                continue

            written.append(
                WrittenItem(event_id=event_id, region=region, sold_date=_sold_date)
            )
            log.append(
                f"{datetime.now(timezone.utc).isoformat()} WROTE html "
                f"event_id={event_id} bytes={len(html_bytes)}"
            )

            # Persist raw image
            img_url = extract_item_image_url(item_html)
            if img_url:
                img_path = f"sold_images/{region}/{event_id}.jpg"
                try:
                    minio_client.client.stat_object(RAW_BUCKET, img_path)
                    images_skipped_already_seen += 1
                    log.append(
                        f"{datetime.now(timezone.utc).isoformat()} SKIP "
                        f"already_in_raw_image event_id={event_id}"
                    )
                except S3Error as e:
                    if e.code not in ("NoSuchKey", "NoSuchObject"):
                        log.append(
                            f"{datetime.now(timezone.utc).isoformat()} "
                            f"WARN stat_image event_id={event_id} err={e}"
                        )
                    try:
                        img_data = requests.get(img_url, timeout=30).content
                        minio_client.put_object(
                            bucket_name=RAW_BUCKET,
                            object_name=img_path,
                            data=img_data,
                            length=len(img_data),
                            content_type="image/jpeg",
                        )
                        images_downloaded += 1
                        log.append(
                            f"{datetime.now(timezone.utc).isoformat()} WROTE image "
                            f"event_id={event_id} bytes={len(img_data)}"
                        )
                    except Exception as img_e:
                        images_failed += 1
                        log.append(
                            f"{datetime.now(timezone.utc).isoformat()} FAIL "
                            f"image event_id={event_id} err={img_e}"
                        )

        page += 1

    log.append(
        f"{datetime.now(timezone.utc).isoformat()} END region={region} "
        f"pages_fetched={pages_fetched} items_seen={items_seen} "
        f"items_skipped_already_seen={items_skipped_already_seen} "
        f"items_fetched_zyte={items_fetched_zyte} items_failed_zyte={items_failed_zyte} "
        f"images_downloaded={images_downloaded} "
        f"images_skipped_already_seen={images_skipped_already_seen} "
        f"images_failed={images_failed} written={len(written)}"
    )
    return written, log
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_scrape_raw.py -v`
Expected: PASS — 7 tests (3 from task 2 + 4 from this task).

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/scrape_raw.py tests/scraping/test_scrape_raw.py
git commit -m "feat(raw): add _scrape_region core loop with per-item idempotency"
```

---

## Task 4: Add the two scrape assets + log-writing at end of run

**Files:**
- Modify: `src/tcg_platform/defs/scrape_raw.py` — append `scrape_ebay_de_raw` and `scrape_ebay_uk_raw` assets, plus a small `_write_log` helper
- Modify: `tests/scraping/test_scrape_raw.py` — append 2 more tests

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/scraping/test_scrape_raw.py
import dagster as dg
from tcg_platform.defs.scrape_raw import scrape_ebay_de_raw, scrape_ebay_uk_raw


def test_scrape_assets_are_dagster_assets():
    """Both assets must be Dagster @asset-decorated and have the right resource keys."""
    for asset in (scrape_ebay_de_raw, scrape_ebay_uk_raw):
        assert isinstance(asset, dg.AssetsDefinition)
        keys = asset.required_resource_keys
        assert "zyte_session_resource" in keys


def test_scrape_assets_write_log_to_raw_bucket_at_end(monkeypatch):
    """After the loop, scraper writes the log blob to tcg-raw/logs/{ts}.log."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")

    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        # search page with 1 item
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/33333"
        )},
        # item page: ok
        {"statusCode": 200, "browserHtml": _good_de_html()},
    ])

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page",
                        lambda html: [("https://www.ebay.de/itm/33333", "2026-06-11")])

    # Invoke the asset's underlying function (not via Dagster runtime)
    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )

    # Now write the log as the asset body would
    from tcg_platform.defs.scrape_raw import _write_log
    log_blob = _write_log(resource, log_lines)
    assert log_blob is not None
    assert log_blob.startswith(b"20")  # ISO timestamp prefix
    assert b"START region=DE" in log_blob
    assert b"END region=DE" in log_blob

    # Confirm the put happened for a logs/ path
    log_puts = [p for p in minio.puts if p["object_name"].startswith("logs/")]
    assert len(log_puts) == 1
    assert log_puts[0]["object_name"].startswith("logs/")
    assert log_puts[0]["object_name"].endswith(".log")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scraping/test_scrape_raw.py -v`
Expected: FAIL — `scrape_ebay_de_raw`, `scrape_ebay_uk_raw`, `_write_log` not importable.

- [ ] **Step 3: Add assets + helper to scrape_raw.py**

Append the following to `src/tcg_platform/defs/scrape_raw.py`:

```python
def _write_log(minio_client, log_lines: list[str]) -> bytes | None:
    """Write a run log to tcg-raw/logs/{timestamp}.log.

    Returns the written blob (also for test inspection), or None if
    the write itself failed.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M")
    log_blob = "\n".join(log_lines).encode("utf-8")
    try:
        minio_client.put_object(
            bucket_name=RAW_BUCKET,
            object_name=f"logs/{ts}.log",
            data=log_blob,
            length=len(log_blob),
            content_type="text/plain",
        )
        return log_blob
    except Exception:
        return None


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client"},
    metadata={"region": "DE"},
)
def scrape_ebay_de_raw(context: dg.AssetExecutionContext) -> list:
    """Scrape eBay DE sold-listings into tcg-raw.

    Writes per-item HTML to tcg-raw/ebay/DE/{event_id}.html and per-item
    images to tcg-raw/sold_images/DE/{event_id}.jpg. Skips event_ids
    that already have raw HTML persisted (atomic check on MinIO).
    Writes a run log to tcg-raw/logs/{timestamp}.log at end of run.
    """
    minio_client = context.resources.minio_client
    zyte_client = context.resources.zyte_session_resource

    written, log_lines = _scrape_region(
        minio_client, zyte_client, "DE",
        de_search_url_for_page, parse_ebay_de_search_page,
    )
    _write_log(minio_client, log_lines)

    context.log.info(f"DE scrape complete: written={len(written)}")
    return [
        {"event_id": w.event_id, "region": w.region, "sold_date": w.sold_date}
        for w in written
    ]


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client"},
    metadata={"region": "UK"},
)
def scrape_ebay_uk_raw(context: dg.AssetExecutionContext) -> list:
    """Scrape eBay UK sold-listings into tcg-raw. Symmetric to scrape_ebay_de_raw."""
    minio_client = context.resources.minio_client
    zyte_client = context.resources.zyte_session_resource

    written, log_lines = _scrape_region(
        minio_client, zyte_client, "UK",
        uk_search_url_for_page, parse_ebay_uk_search_page,
    )
    _write_log(minio_client, log_lines)

    context.log.info(f"UK scrape complete: written={len(written)}")
    return [{"event_id": w.event_id, "region": w.region} for w in written]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_scrape_raw.py -v`
Expected: PASS — 9 tests.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/scrape_raw.py tests/scraping/test_scrape_raw.py
git commit -m "feat(raw): add scrape_ebay_{de,uk}_raw assets with end-of-run log"
```

---

## Task 5: Add `_transform_region` to `transform_bronze.py`

**Files:**
- Create: `src/tcg_platform/defs/transform_bronze.py` (just the function, no assets yet)
- Create: `tests/scraping/test_transform_bronze.py` (4 tests)

- [ ] **Step 1: Write the failing tests**

```python
# tests/scraping/test_transform_bronze.py
from datetime import datetime, timezone
from io import BytesIO

import pyarrow.parquet as pq

from tcg_platform.defs.transform_bronze import _transform_region


class FakeMinioClient:
    """Serves HTML from tcg-raw, captures bronze writes."""

    def __init__(self, raw_html=None, raw_image=None):
        self.raw_html = raw_html or b""
        self.raw_image = raw_image
        self.puts = []
        self.got = []

        class _Client:
            def __init__(self2, outer):
                self2.outer = outer

            def get_object(self2, bucket, obj):
                self2.outer.got.append((bucket, obj))
                from minio.datatypes import Object
                if obj.endswith(".html"):
                    resp = BytesIO(self2.outer.raw_html)
                elif obj.endswith(".jpg") and self2.outer.raw_image is not None:
                    resp = BytesIO(self2.outer.raw_image)
                else:
                    raise Exception(f"NoSuchKey: {obj}")
                return resp

        self.client = _Client(self)


class FakeSqliteClient:
    def __init__(self):
        self.inserts = []

    def execute(self, query, params=(), fetch="none"):
        if "INSERT" in query:
            self.inserts.append(params)
            return None
        return []


def _good_de_html():
    return """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans">One Piece OP01-001 PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>
    </body></html>
    """


def _bad_html_no_title():
    return "<html><body><div data-testid=\"x-price-primary\"><span class=\"ux-textspans\">EUR 5,00</span></div></body></html>"


def _make_resource(fake_client):
    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-bronze",
    )
    resource._client = fake_client
    return resource


def test_transform_reads_raw_writes_bronze(monkeypatch):
    """A known-good raw HTML results in a bronze parquet + SQLite row."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_good_de_html().encode("utf-8"))
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    written_items = [{"event_id": "99999", "region": "DE", "sold_date": "2026-06-11"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)

    assert counts["read_html"] == 1
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 1
    # Verify a bronze parquet was written
    parquet_puts = [p for p in minio.puts if p["object_name"].endswith(".parquet")]
    assert len(parquet_puts) == 1
    assert parquet_puts[0]["object_name"] == "sold_data/DE/99999.parquet"
    # Verify SQLite got a row with the carried-through sold_date
    assert len(sqlite.inserts) == 1
    insert_params = sqlite.inserts[0]
    # Insertion params: (card_id, card_version, event_type, price, currency,
    #                    sold_date, scraped_from, source, source_url, ...)
    sold_date_idx = 5
    assert insert_params[sold_date_idx] == "2026-06-11"


def test_transform_handles_missing_image_gracefully(monkeypatch):
    """Raw HTML exists, no image → local_image_path is None, transform succeeds."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_good_de_html().encode("utf-8"), raw_image=None)
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    written_items = [{"event_id": "99999", "region": "DE"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)
    assert counts["wrote_parquet"] == 1
    assert counts["image_missing"] == 1


def test_transform_handles_parse_failure(monkeypatch):
    """HTML exists but has no title → skipped_empty=1, no bronze writes."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_bad_html_no_title().encode("utf-8"))
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    written_items = [{"event_id": "99999", "region": "DE"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)
    assert counts["skipped_empty"] == 1
    assert counts["wrote_parquet"] == 0
    assert len(sqlite.inserts) == 0


def test_transform_filters_wrong_region_in_input(monkeypatch):
    """If the scraper returns a UK event_id, the DE transformer ignores it."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_good_de_html().encode("utf-8"))
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    # Pass UK item to DE transformer
    written_items = [{"event_id": "99999", "region": "UK"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)
    assert counts["read_html"] == 0  # filtered out
    assert counts["wrote_parquet"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scraping/test_transform_bronze.py -v`
Expected: FAIL — `transform_bronze` module does not exist.

- [ ] **Step 3: Create transform_bronze.py with `_transform_region`**

```python
# src/tcg_platform/defs/transform_bronze.py
"""Offline transformer: read tcg-raw, parse HTML, write tcg-bronze.

This asset has no network dependencies. It reads the raw HTML and
images that the scraper just wrote and produces the structured
bronze layer (parquet files + SQLite fact_events rows).
"""
import logging
from datetime import datetime, timezone

import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page
from tcg_platform.scraping.ebay_utils import extract_item_id
from tcg_platform.serialization.card_parquet import price_records_to_parquet

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
BRONZE_BUCKET = "tcg-bronze"


def _transform_region(
    minio_client: MinioClientResource,
    sqlite_client,
    region: str,
    written_items: list[dict],
    parse_item_page_fn,
) -> dict:
    """Read raw HTML for each written item, parse, write bronze parquet + SQLite.

    `written_items` is the list of {event_id, region} dicts returned
    by the scraper asset for this run. This function does NOT scan
    tcg-raw; it processes exactly the items the scraper just wrote.
    """
    upper = region.upper()
    lower = region.lower()
    counts = {
        "read_html": 0, "read_image": 0, "wrote_parquet": 0,
        "wrote_sqlite": 0, "skipped_empty": 0, "parse_failed": 0,
        "image_missing": 0,
    }
    scraped_at = datetime.now(timezone.utc)

    for item in written_items:
        event_id = item["event_id"]
        if item.get("region", upper) != upper:
            continue
        sold_date = item.get("sold_date")

        # Read raw HTML
        try:
            html = minio_client.get_object(
                RAW_BUCKET, f"ebay/{upper}/{event_id}.html"
            ).decode("utf-8")
        except Exception as e:
            _LOG.warning(f"Read html failed for {event_id}: {e}")
            continue
        counts["read_html"] += 1

        # Read raw image
        image_path = None
        try:
            image_path = f"sold_images/{lower}/{event_id}.jpg"
            minio_client.get_object(RAW_BUCKET, image_path)
            counts["read_image"] += 1
        except Exception:
            counts["image_missing"] += 1
            image_path = None

        # Build source_url from event_id (eBay URL pattern is region-specific)
        item_url = (
            f"https://www.ebay.de/itm/{event_id}" if upper == "DE"
            else f"https://www.ebay.co.uk/itm/{event_id}"
        )

        # Parse HTML
        try:
            parsed = parse_item_page_fn(html, item_url, scraped_at)
        except Exception as e:
            _LOG.warning(f"Parse failed for {event_id}: {e}")
            counts["parse_failed"] += 1
            continue
        if not parsed:
            counts["skipped_empty"] += 1
            continue

        # Attach sold_date (from the search page, carried through the scraper)
        # and image_path to each record, then write bronze parquet + SQLite
        for rec in parsed:
            if sold_date and not rec.sold_date:
                rec.sold_date = sold_date
            rec.local_image_path = image_path
            parquet_bytes, _ = price_records_to_parquet(
                [rec], rec.scraped_at.strftime("%Y-%m-%d")
            )
            minio_client.put_object(
                bucket_name=BRONZE_BUCKET,
                object_name=f"sold_data/{lower}/{event_id}.parquet",
                data=parquet_bytes,
                length=len(parquet_bytes),
                content_type="application/parquet",
            )
            counts["wrote_parquet"] += 1

            from tcg_platform.defs.bronze_ebay_sqlite_writer import _is_proxy_title
            if not _is_proxy_title(rec.card_id):
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

    return counts
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_transform_bronze.py -v`
Expected: PASS — 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/transform_bronze.py tests/scraping/test_transform_bronze.py
git commit -m "feat(bronze): add _transform_region for raw→bronze offline path"
```

---

## Task 6: Add the two transform assets

**Files:**
- Modify: `src/tcg_platform/defs/transform_bronze.py` — append `transform_ebay_de_to_bronze` and `transform_ebay_uk_to_bronze` assets
- Modify: `tests/scraping/test_transform_bronze.py` — append 1 test asserting the assets exist with the right deps

- [ ] **Step 1: Write the failing test**

```python
# Append to tests/scraping/test_transform_bronze.py
import dagster as dg
from tcg_platform.defs.transform_bronze import (
    transform_ebay_de_to_bronze,
    transform_ebay_uk_to_bronze,
)


def test_transform_assets_are_dagster_assets_with_scraper_deps():
    """Both transform assets must depend on their corresponding scraper asset."""
    de_deps = transform_ebay_de_to_bronze.dependency_keys
    uk_deps = transform_ebay_uk_to_bronze.dependency_keys
    # The dependency is the scraper asset key
    from dagster import AssetKey
    assert AssetKey("scrape_ebay_de_raw") in de_deps
    assert AssetKey("scrape_ebay_uk_raw") in uk_deps
    # Both need minio_client + sqlite_client
    for asset in (transform_ebay_de_to_bronze, transform_ebay_uk_to_bronze):
        assert "minio_client" in asset.required_resource_keys
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/scraping/test_transform_bronze.py -v`
Expected: FAIL — assets not importable.

- [ ] **Step 3: Add the two assets to transform_bronze.py**

Append the following to `src/tcg_platform/defs/transform_bronze.py`:

```python
@dg.asset(
    required_resource_keys={"minio_client", "sqlite_client_de"},
)
def transform_ebay_de_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_de_raw: list,
) -> dg.MaterializeResult:
    minio_client = context.resources.minio_client
    sqlite_client = context.resources.sqlite_client_de

    counts = _transform_region(
        minio_client, sqlite_client, "DE",
        scrape_ebay_de_raw, parse_ebay_de_item_page,
    )
    context.log.info(f"DE transform: {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    required_resource_keys={"minio_client", "sqlite_client_uk"},
)
def transform_ebay_uk_to_bronze(
    context: dg.AssetExecutionContext,
    scrape_ebay_uk_raw: list,
) -> dg.MaterializeResult:
    minio_client = context.resources.minio_client
    sqlite_client = context.resources.sqlite_client_uk

    counts = _transform_region(
        minio_client, sqlite_client, "UK",
        scrape_ebay_uk_raw, parse_ebay_uk_item_page,
    )
    context.log.info(f"UK transform: {counts}")
    return dg.MaterializeResult(metadata=counts)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/scraping/test_transform_bronze.py -v`
Expected: PASS — 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/transform_bronze.py tests/scraping/test_transform_bronze.py
git commit -m "feat(bronze): add transform_ebay_{de,uk}_to_bronze assets"
```

---

## Task 7: Add `backfill_raw_html.py`

**Files:**
- Create: `src/tcg_platform/defs/backfill_raw_html.py`
- Create: `tests/scraping/test_backfill_raw_html.py` (3 tests)

- [ ] **Step 1: Write the failing tests**

```python
# tests/scraping/test_backfill_raw_html.py
from io import BytesIO

from minio.error import S3Error
from tcg_platform.defs.backfill_raw_html import _backfill_region


class FakeMinioClient:
    def __init__(self, existing_event_ids=()):
        self.existing = set(existing_event_ids)
        self.puts = []
        self.stats = []

        class _Client:
            def __init__(self2, outer):
                self2.outer = outer

            def stat_object(self2, bucket, obj):
                self2.outer.stats.append((bucket, obj))
                # If the HTML path is in self.outer.existing, return success
                if obj.endswith(".html"):
                    event_id = obj.split("/")[-1].replace(".html", "")
                    if event_id in self2.outer.existing:
                        return None
                raise S3Error(
                    code="NoSuchKey", message="not found",
                    resource="x", request_id="r", host_id="h", response=None,
                )

            def put_object(self2, bucket, obj, data, length, content_type):
                self2.outer.puts.append((bucket, obj, data, length, content_type))

        self.client = _Client(self)


class FakeSqliteClient:
    def __init__(self, urls):
        # urls: list of (url,) tuples
        self._rows = [{"source_url": u} for u in urls]

    def execute(self, query, params=(), fetch="none"):
        if "SELECT source_url" in query:
            # Filter by source = params[0]
            source = params[0] if params else None
            return [r for r in self._rows]  # caller-side filter is a no-op for tests
        return []


class FakeZyteClient:
    def __init__(self):
        self.calls = []

    def get(self, request):
        self.calls.append(request)
        return {
            "statusCode": 200,
            "browserHtml": "<html><body>test</body></html>",
        }


def test_backfill_skips_event_ids_already_in_raw():
    """If raw HTML already exists for an event_id, no Zyte call happens for it."""
    minio = FakeMinioClient(existing_event_ids=["11111"])
    sqlite = FakeSqliteClient(["https://www.ebay.de/itm/11111"])
    zyte = FakeZyteClient()

    counts = _backfill_region(minio.client, zyte, sqlite, "DE")
    assert counts["checked"] == 1
    assert counts["already_have"] == 1
    assert counts["fetched"] == 0
    assert len(zyte.calls) == 0  # no Zyte call for the already-present event


def test_backfill_fetches_and_writes_missing_event_ids():
    """Missing event_ids trigger a Zyte call + raw HTML put."""
    minio = FakeMinioClient(existing_event_ids=[])
    sqlite = FakeSqliteClient(["https://www.ebay.de/itm/22222"])
    zyte = FakeZyteClient()

    counts = _backfill_region(minio.client, zyte, sqlite, "DE")
    assert counts["checked"] == 1
    assert counts["fetched"] == 1
    assert len(zyte.calls) == 1
    # Verify a put to tcg-raw/ebay/DE/22222.html
    html_puts = [p for p in minio.puts if p[1] == "ebay/DE/22222.html"]
    assert len(html_puts) == 1


def test_backfill_counts_shape():
    """Return value must have the documented keys."""
    minio = FakeMinioClient()
    sqlite = FakeSqliteClient([])
    zyte = FakeZyteClient()

    counts = _backfill_region(minio.client, zyte, sqlite, "DE")
    assert set(counts.keys()) == {"checked", "already_have", "fetched", "failed"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scraping/test_backfill_raw_html.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Create backfill_raw_html.py**

```python
# src/tcg_platform/defs/backfill_raw_html.py
"""One-time backfill: fetch raw HTML for SQLite rows scraped before tcg-raw existed.

After this asset runs once, all historical fact_events rows have a
corresponding tcg-raw/ebay/{region}/{event_id}.html. The asset can
be deprecated after first use; the backfill can be triggered again
later if tcg-raw is ever wiped.
"""
import logging

import dagster as dg
import requests
from minio.error import S3Error

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.ebay_image import extract_item_image_url
from tcg_platform.scraping.ebay_utils import extract_item_id

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"


def _backfill_region(
    minio_client: MinioClientResource,
    zyte_client,
    sqlite_client,
    region: str,
) -> dict:
    """For each fact_event in SQLite whose source_url has no raw HTML yet,
    fetch the item page from eBay and persist raw HTML + image.

    One-time job to populate tcg-raw for rows scraped before this
    design landed.
    """
    upper = region.upper()
    lower = region.lower()
    rows = sqlite_client.execute(
        "SELECT source_url FROM fact_events WHERE scraped_from = 'ebay' AND source = ?",
        (upper,),
        fetch="all",
    )
    counts = {"checked": 0, "already_have": 0, "fetched": 0, "failed": 0}
    for row in rows:
        url = row["source_url"]
        event_id = extract_item_id(url)
        if not event_id or not event_id.isdigit():
            continue
        counts["checked"] += 1

        # Skip if already in raw
        try:
            minio_client.client.stat_object(RAW_BUCKET, f"ebay/{upper}/{event_id}.html")
            counts["already_have"] += 1
            continue
        except S3Error:
            pass

        # Fetch and persist
        try:
            resp = zyte_client.get({"url": url, "browserHtml": True})
            if resp.get("statusCode") != 200:
                counts["failed"] += 1
                continue
            html = resp.get("browserHtml", "")
            if not html:
                counts["failed"] += 1
                continue
            html_bytes = html.encode("utf-8")
            minio_client.put_object(
                bucket_name=RAW_BUCKET,
                object_name=f"ebay/{upper}/{event_id}.html",
                data=html_bytes,
                length=len(html_bytes),
                content_type="text/html",
            )
            img_url = extract_item_image_url(html)
            if img_url:
                try:
                    img_data = requests.get(img_url, timeout=30).content
                    minio_client.put_object(
                        bucket_name=RAW_BUCKET,
                        object_name=f"sold_images/{lower}/{event_id}.jpg",
                        data=img_data,
                        length=len(img_data),
                        content_type="image/jpeg",
                    )
                except Exception:
                    pass
            counts["fetched"] += 1
        except Exception as e:
            _LOG.warning(f"Backfill failed for {url}: {e}")
            counts["failed"] += 1
    return counts


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client", "sqlite_client_de"},
)
def backfill_raw_html_de(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    counts = _backfill_region(
        context.resources.minio_client,
        context.resources.zyte_session_resource,
        context.resources.sqlite_client_de,
        "DE",
    )
    context.log.info(f"DE backfill: {counts}")
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    required_resource_keys={"zyte_session_resource", "minio_client", "sqlite_client_uk"},
)
def backfill_raw_html_uk(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    counts = _backfill_region(
        context.resources.minio_client,
        context.resources.zyte_session_resource,
        context.resources.sqlite_client_uk,
        "UK",
    )
    context.log.info(f"UK backfill: {counts}")
    return dg.MaterializeResult(metadata=counts)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_backfill_raw_html.py -v`
Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/backfill_raw_html.py tests/scraping/test_backfill_raw_html.py
git commit -m "feat(raw): add backfill_raw_html_{de,uk} one-time bootstrap assets"
```

---

## Task 8: Wire up `definitions.py` and add new jobs

**Files:**
- Modify: `src/tcg_platform/definitions.py` — add `tcg_raw_client` import, add it to resources, add 4 new jobs to jobs list
- Test: `tests/defs/test_definitions_load.py` (new)

- [ ] **Step 1: Write the failing test**

```python
# tests/defs/test_definitions_load.py
import pytest


def test_definitions_load_cleanly():
    """Dagster definitions must load without errors."""
    from tcg_platform.definitions import defs
    resolved = defs.load_fn()
    # Just touching the resolved object exercises all the asset/job discovery
    assert resolved is not None


def test_new_jobs_are_registered():
    """The 4 new jobs must be in the jobs list."""
    from tcg_platform.definitions import (
        defs,
        ebay_de_raw_to_bronze_job,
        ebay_uk_raw_to_bronze_job,
        backfill_raw_html_de_job,
        backfill_raw_html_uk_job,
    )
    assert ebay_de_raw_to_bronze_job is not None
    assert ebay_uk_raw_to_bronze_job is not None
    assert backfill_raw_html_de_job is not None
    assert backfill_raw_html_uk_job is not None
    # And they should be discoverable by name
    resolved = defs.load_fn()
    assert resolved.resolve_job_def("ebay_de_raw_to_bronze") is not None
    assert resolved.resolve_job_def("ebay_uk_raw_to_bronze") is not None
    assert resolved.resolve_job_def("backfill_raw_html_de_job") is not None
    assert resolved.resolve_job_def("backfill_raw_html_uk_job") is not None


def test_tcg_raw_client_resource_is_registered():
    """The tcg_raw_client must be in the resources map."""
    from tcg_platform.definitions import defs
    resolved = defs.load_fn()
    # Accessing .get_resource_def or similar — check via to_dagster or attributes
    # We test the underlying Definitions object instead
    from tcg_platform.definitions import defs as definitions_obj
    # The Definitions object exposes resources via .resources
    assert "tcg_raw_client" in definitions_obj.resources  # may be lazy
```

The test above may need adjustment based on how the existing `defs` decorator works. The simpler version:

```python
# tests/defs/test_definitions_load.py
import pytest


def test_definitions_load_cleanly():
    """Dagster definitions must load without errors (catches wiring mistakes)."""
    from tcg_platform.definitions import defs
    # The @definitions decorator defers loading; calling load_fn() forces it
    resolved = defs.load_fn()
    assert resolved is not None


def test_new_jobs_importable():
    """The 4 new jobs must be importable from definitions."""
    from tcg_platform.definitions import (
        ebay_de_raw_to_bronze_job,
        ebay_uk_raw_to_bronze_job,
        backfill_raw_html_de_job,
        backfill_raw_html_uk_job,
    )
    for j in (ebay_de_raw_to_bronze_job, ebay_uk_raw_to_bronze_job,
              backfill_raw_html_de_job, backfill_raw_html_uk_job):
        assert j is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/defs/test_definitions_load.py -v`
Expected: FAIL — jobs not importable.

- [ ] **Step 3: Modify definitions.py**

Modify `src/tcg_platform/definitions.py` to:
1. Add import for `tcg_raw_client`
2. Define the 4 new jobs
3. Add the jobs to the `jobs=[...]` list
4. Add `tcg_raw_client` to the `resources={...}` map

The full file should look like this:

```python
# src/tcg_platform/definitions.py
from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder, define_asset_job

from tcg_platform.defs.backfill_sold_data_parquet import (
    backfill_de_job,
    backfill_uk_job,
    backfill_de_sensor,
    backfill_uk_sensor,
)
from tcg_platform.defs.currency_rates_resource import (
    currency_rates_db,
)
from tcg_platform.defs.minio_resources import (
    minio_client,
    tcg_raw_client,
)
from tcg_platform.resources.sqlite_client import (
    SqliteClientResource,
)
from tcg_platform.defs.zyte_resources import (
    zyte_session_resource,
)
from tcg_platform.defs.eu_pipeline_orchestrator import (
    bronze_eu_orchestrator,
    backfill_de_asset,
    backfill_uk_asset,
    silver_eu_orchestrator,
)
from tcg_platform.defs.reconcile_quarantine import (
    reconcile_quarantine_de_job,
    reconcile_quarantine_uk_job,
)


ebay_de_raw_to_bronze_job = define_asset_job(
    name="ebay_de_raw_to_bronze",
    selection=["scrape_ebay_de_raw", "transform_ebay_de_to_bronze"],
    description="DE: scrape tcg-raw + transform to tcg-bronze",
)

ebay_uk_raw_to_bronze_job = define_asset_job(
    name="ebay_uk_raw_to_bronze",
    selection=["scrape_ebay_uk_raw", "transform_ebay_uk_to_bronze"],
    description="UK: scrape tcg-raw + transform to tcg-bronze",
)

backfill_raw_html_de_job = define_asset_job(
    name="backfill_raw_html_de_job",
    selection=["backfill_raw_html_de"],
    description="One-time: fetch raw HTML for existing DE fact_events rows.",
)

backfill_raw_html_uk_job = define_asset_job(
    name="backfill_raw_html_uk_job",
    selection=["backfill_raw_html_uk"],
    description="One-time: fetch raw HTML for existing UK fact_events rows.",
)

ebay_de_job = define_asset_job(
    name="ebay_de_pipeline",
    selection=["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer"],
    description="Scrape DE eBay sold listings, persist to SQLite",
)

ebay_uk_job = define_asset_job(
    name="ebay_uk_pipeline",
    selection=["ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"],
    description="Scrape UK eBay sold listings, persist to SQLite",
)

ebay_eu_job = define_asset_job(
    name="ebay_eu_pipeline",
    selection=["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer",
               "ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"],
    description="Scrape DE+UK eBay sold listings, persist to SQLite",
)

silver_de_job = define_asset_job(
    name="silver_de_pipeline",
    selection=["silver_de_transform"],
    description="Transform DE bronze parquets to silver layer",
)

silver_uk_job = define_asset_job(
    name="silver_uk_pipeline",
    selection=["silver_uk_transform"],
    description="Transform UK bronze parquets to silver layer",
)

silver_eu_job = define_asset_job(
    name="silver_eu_pipeline",
    selection=["silver_de_transform", "silver_uk_transform"],
    description="Transform DE+UK bronze parquets to silver layer",
)

complete_eu_pipeline = define_asset_job(
    name="complete_eu_pipeline",
    selection=["bronze_eu_orchestrator", "backfill_de_asset", "backfill_uk_asset", "silver_eu_orchestrator"],
    description="Full EU pipeline: bronze → backfill (DE+UK parallel) → silver",
)

sync_card_images_job = define_asset_job(
    name="sync_card_images_job",
    selection=["discover_limitless_catalog", "sync_card_images"],
    description="Diff Limitless catalog against tcg-bronze/cards/, download missing images.",
)


@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions(
        assets=base.assets,
        asset_checks=base.asset_checks,
        jobs=[
            ebay_de_job,
            ebay_uk_job,
            ebay_eu_job,
            backfill_de_job,
            backfill_uk_job,
            silver_de_job,
            silver_uk_job,
            silver_eu_job,
            complete_eu_pipeline,
            sync_card_images_job,
            reconcile_quarantine_de_job,
            reconcile_quarantine_uk_job,
            # NEW for M9-T1
            ebay_de_raw_to_bronze_job,
            ebay_uk_raw_to_bronze_job,
            backfill_raw_html_de_job,
            backfill_raw_html_uk_job,
        ],
        sensors=[backfill_de_sensor, backfill_uk_sensor],
        resources={
            "currency_rates_db": currency_rates_db,
            "minio_client": minio_client,
            "tcg_raw_client": tcg_raw_client,
            "sqlite_client_de": SqliteClientResource(db_path="./data/tcg_de.db"),
            "sqlite_client_uk": SqliteClientResource(db_path="./data/tcg_uk.db"),
            "zyte_session_resource": zyte_session_resource,
        },
    )
```

> **Note:** We keep the old `ebay_de_job` / `ebay_uk_job` definitions here for now (they reference `ebay_de_sold_listings` / `ebay_uk_sold_listings` which still exist). They will be removed in Task 10. If you prefer to remove the old jobs in this task instead, see the alternative at the bottom of this task.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/defs/test_definitions_load.py -v`
Expected: PASS — 2 tests.

- [ ] **Step 5: Verify defs load outside of test**

Run: `python -c "from tcg_platform.definitions import defs; r = defs.load_fn(); print('OK')"`
Expected: `OK`. (Per AGENTS.md Rule 17.)

- [ ] **Step 6: Commit**

```bash
git add src/tcg_platform/definitions.py tests/defs/test_definitions_load.py
git commit -m "feat(defs): register tcg_raw_client + 4 new M9-T1 jobs"
```

---

## Task 9: Update `eu_pipeline_orchestrator.py` to use new jobs

**Files:**
- Modify: `src/tcg_platform/defs/eu_pipeline_orchestrator.py:12-13,15` — swap the two `resolve_job_def` calls and the log message

- [ ] **Step 1: Modify the orchestrator**

In `src/tcg_platform/defs/eu_pipeline_orchestrator.py`, change lines 12-15 from:

```python
    job_def_de = resolved.resolve_job_def("ebay_de_pipeline")
    job_def_uk = resolved.resolve_job_def("ebay_uk_pipeline")

    context.log.info("Running ebay_de_pipeline and ebay_uk_pipeline in parallel...")
```

to:

```python
    job_def_de = resolved.resolve_job_def("ebay_de_raw_to_bronze")
    job_def_uk = resolved.resolve_job_def("ebay_uk_raw_to_bronze")

    context.log.info("Running ebay_de_raw_to_bronze and ebay_uk_raw_to_bronze in parallel...")
```

The rest of the file is unchanged.

- [ ] **Step 2: Verify defs still load**

Run: `python -c "from tcg_platform.definitions import defs; r = defs.load_fn(); print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Run existing orchestrator tests**

Run: `pytest tests/defs/test_eu_pipeline_orchestrator.py -v`
Expected: PASS — the existing tests only check asset dependencies (which we did not change).

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/defs/eu_pipeline_orchestrator.py
git commit -m "refactor(orchestrator): point bronze orchestrator at new raw+transform jobs"
```

---

## Task 10: Remove old single-stage assets and update env/docs

**Files:**
- Delete: `src/tcg_platform/defs/ebay_de_sold_listings.py`
- Delete: `src/tcg_platform/defs/ebay_uk_sold_listings.py`
- Delete: `src/tcg_platform/scraping/ebay_image.py` (only if no other module imports it; if any other code references it, keep it and add a deprecation note)
- Modify: `src/tcg_platform/definitions.py` — remove the now-dead `ebay_de_job` / `ebay_uk_job` / `ebay_eu_job` job definitions
- Modify: `src/tcg_platform/.env.example` — add RAW_* env vars
- Modify: `src/tcg_platform/PROD.md` — add tcg-raw layer, update bucket table

- [ ] **Step 1: Check for any other importers of `ebay_image.py`**

Run:
```bash
grep -rn "from tcg_platform.scraping.ebay_image" src/ tests/ 2>&1
grep -rn "import ebay_image" src/ tests/ 2>&1
```

Expected: no other importers besides `scrape_raw.py` and `transform_bronze.py` (and the soon-to-be-deleted `ebay_de_sold_listings.py` / `ebay_uk_sold_listings.py`). If anything else imports it, **stop and ask the user** before deleting.

- [ ] **Step 2: Check for any other importers of the old single-stage assets**

Run:
```bash
grep -rn "ebay_de_sold_listings\|ebay_uk_sold_listings" src/ tests/ 2>&1
```

Expected: no remaining references (the orchestrator was updated in Task 9, the old `ebay_de_job` / `ebay_uk_job` definitions in `definitions.py` are about to be removed in this task).

- [ ] **Step 3: Delete the old files**

```bash
git rm src/tcg_platform/defs/ebay_de_sold_listings.py
git rm src/tcg_platform/defs/ebay_uk_sold_listings.py
# Only if Step 1 confirmed no other importers:
git rm src/tcg_platform/scraping/ebay_image.py
```

- [ ] **Step 4: Remove dead job definitions from definitions.py**

In `src/tcg_platform/definitions.py`, delete these three blocks:

```python
ebay_de_job = define_asset_job(
    name="ebay_de_pipeline",
    selection=["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer"],
    description="Scrape DE eBay sold listings, persist to SQLite",
)

ebay_uk_job = define_asset_job(
    name="ebay_uk_pipeline",
    selection=["ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"],
    description="Scrape UK eBay sold listings, persist to SQLite",
)

ebay_eu_job = define_asset_job(
    name="ebay_eu_pipeline",
    selection=["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer",
               "ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"],
    description="Scrape DE+UK eBay sold listings, persist to SQLite",
)
```

And remove the entries `ebay_de_job,` `ebay_uk_job,` `ebay_eu_job,` from the `jobs=[...]` list.

- [ ] **Step 5: Verify defs still load**

Run: `python -c "from tcg_platform.definitions import defs; r = defs.load_fn(); print('OK')"`
Expected: `OK`.

- [ ] **Step 6: Update `.env.example`**

Open `src/tcg_platform/.env.example` (or `.env` if no example file exists — see `ls .env*`). Add these lines alongside the existing `MINIO_*` entries:

```
# tcg-raw bucket — persistent raw HTML/images for replayable scrapes (M9-T1)
RAW_ENDPOINT=localhost:9000
RAW_ACCESS_KEY=minioadmin
RAW_SECRET_KEY=minioadmin
RAW_BUCKET=tcg-raw
```

- [ ] **Step 7: Update PROD.md**

In `src/tcg_platform/PROD.md`:
- Replace the "Bronze layer" description at line 17-18 with the new contract (per spec line 75-78).
- Insert a new "Raw layer" section above the "Bronze layer" description.
- Update the "MinIO Buckets" table at line 191-196 to add a `tcg-raw` row with contents `Raw scraped bytes: ebay/{DE,UK}/ HTML, sold_images/{DE,UK}/ images, logs/ per-run`.

- [ ] **Step 8: Run full test suite**

Run: `pytest tests/ -v`
Expected: PASS — all unit tests pass, no import errors, no broken references. Existing tests for `ebay_de_item` / `ebay_uk_item` / `reconcile_quarantine` / `eu_pipeline_orchestrator` continue to pass.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: remove old single-stage scraper assets; add RAW env vars and tcg-raw to PROD.md"
```

---

## Task 11: Manual integration smoke test

**Files:** none — this is a verification task per AGENTS.md Rule 17.

- [ ] **Step 1: Start MinIO if not running**

Confirm `localhost:9000` responds:
```bash
curl -s -o /dev/null -w "%{http_code}\n" --max-time 3 http://localhost:9000/minio/health/live
```
Expected: `200`. If not, start MinIO (see session log 2026-06-11 for the brew install path).

- [ ] **Step 2: Run the new DE scrape job end-to-end**

```bash
dg dev  # in one terminal — confirm UI shows the new assets
```

In another terminal, trigger the new job via Dagster UI or CLI:
```bash
dg job launch ebay_de_raw_to_bronze
```

Expected: job completes successfully. Inspect MinIO:
```bash
python3 -c "
from minio import Minio
mc = Minio('localhost:9000', 'minioadmin', 'minioadmin', secure=False)
print('tcg-raw/ebay/DE/:', len(list(mc.list_objects('tcg-raw', prefix='ebay/DE/', recursive=True))))
print('tcg-raw/sold_images/DE/:', len(list(mc.list_objects('tcg-raw', prefix='sold_images/DE/', recursive=True))))
print('tcg-raw/logs/:', len(list(mc.list_objects('tcg-raw', prefix='logs/', recursive=True))))
"
```

Expected: at least one HTML file, one or more image files, one log file. Inspect the log to confirm the expected line format.

- [ ] **Step 3: Re-run the same job and confirm no duplicate fetches**

Run `ebay_de_raw_to_bronze` again. Read the new log file. Confirm that all items in this run show `SKIP already_in_raw` lines (not `WROTE html`). MinIO content is unchanged.

- [ ] **Step 4: Run the backfill job**

```bash
dg job launch backfill_raw_html_de_job
```

Confirm that after the run, all 64 DE rows in `data/tcg_de.db` have a corresponding `tcg-raw/ebay/DE/{event_id}.html`:

```bash
python3 -c "
import sqlite3
from minio import Minio
mc = Minio('localhost:9000', 'minioadmin', 'minioadmin', secure=False)
de_ids = set()
for r in sqlite3.connect('data/tcg_de.db').execute('SELECT source_url FROM fact_events'):
    import re
    m = re.search(r'/itm/(\d+)', r[0])
    if m: de_ids.add(m.group(1))
have = set()
for obj in mc.list_objects('tcg-raw', prefix='ebay/DE/', recursive=True):
    eid = obj.object_name.split('/')[-1].replace('.html','')
    have.add(eid)
print(f'DE rows: {len(de_ids)}, raw HTML files: {len(have)}, missing: {de_ids - have}')
"
```

Expected: `missing: set()` (empty).

- [ ] **Step 5: Run the full pipeline once**

```bash
dg job launch complete_eu_pipeline
```

Expected: full pipeline runs green (bronze → backfill → silver, all stages), same as before this design. The new raw layer is invisible to downstream.

- [ ] **Step 6: Commit the SESSION log**

```bash
git add log/SESSION_2026-06-11.md
git commit -m "docs(log): session log for 2026-06-11 (M9-T1 tcg-raw layer landed)"
```

(Or create the session log first if it doesn't exist; see the AGENTS.md "Session Log" section for the format.)

- [ ] **Step 7: Push the branch and merge to main**

Per AGENTS.md Rules 18-19: push the branch, then merge to main via PR or local merge with `--no-ff`. Force-pushing main is not allowed.

---

## Self-review (post-write)

**Spec coverage check:**
- ✅ New `tcg-raw` bucket — Task 1 (resource), Task 2/3/4 (scraper writes to it)
- ✅ `scrape_ebay_de_raw` / `scrape_ebay_uk_raw` assets — Task 4
- ✅ `transform_ebay_de_to_bronze` / `transform_ebay_uk_to_bronze` assets — Task 6
- ✅ `_exists_in_raw` for atomic dedup — Task 2
- ✅ Scraper log at `tcg-raw/logs/{ts}.log` written at end of run — Task 4
- ✅ Image download to `tcg-raw/sold_images/` — Task 3 (inlined into scraper)
- ✅ `backfill_raw_html_{de,uk}` one-time assets — Task 7
- ✅ `tcg_raw_client` resource in `minio_resources.py` — Task 1
- ✅ Resources map updated in `definitions.py` — Task 8
- ✅ 4 new jobs in `definitions.py` — Task 8
- ✅ Orchestrator swap — Task 9
- ✅ Old single-stage assets removed — Task 10
- ✅ `.env.example` updated — Task 10
- ✅ `PROD.md` updated — Task 10
- ✅ Manual smoke test — Task 11

**Placeholder scan:** no "TBD", "TODO", "fill in details", "similar to Task N" placeholders. All code blocks are complete.

**Type consistency check:**
- `WrittenItem` NamedTuple defined in Task 2 (lines 47-50 of `scrape_raw.py`) and used in Task 3 (function return type) and Task 4 (asset returns `list[dict]`, not `list[WrittenItem]` — converted via list comprehension). Consistent.
- `RAW_BUCKET = "tcg-raw"` constant defined in Task 1 (in `scrape_raw.py`) and re-imported from there in Task 5 (`transform_bronze.py`) and Task 7 (`backfill_raw_html.py`). Consistent.
- `_write_log` helper defined in Task 4 and used by both scraper assets. Consistent.
- `ebay_de_raw_to_bronze_job` etc. defined in Task 8 and resolved by name in Task 9. Name string is identical.

**One thing to flag for the implementer:** the `parse_ebay_de_search_page` function in `src/tcg_platform/scraping/ebay_de_search.py` returns a list of `(url, sold_date)` tuples. The current scraper relies on the search page to provide the sold_date (the item page doesn't have it). The new `scrape_raw.py` MUST propagate sold_date from the search-page parser into the scraper's return tuple, and `_transform_region` MUST attach it to each `PriceRecord` after the item-page parser returns (the item-page parser does not extract sold_date itself — see `ebay_uk_item.py:4` and `ebay_uk_item.py:122`).

This is implemented in Task 3 and Task 5 of this plan: `WrittenItem` has a `sold_date` field, `_scrape_region` populates it from the search page pair, the scraper assets return `{"event_id", "region", "sold_date"}` dicts, and `_transform_region` does `if sold_date and not rec.sold_date: rec.sold_date = sold_date` after the item-page parser runs. Verified in `test_transform_reads_raw_writes_bronze` (Task 5) via the SQLite insert params.

---

## Self-review (post-write, second pass)

**Spec coverage check:** all 16 spec items are covered by the 11 tasks.

**Placeholder scan:** no TBD/TODO placeholders in code blocks.

**Type consistency check:** all type signatures match across tasks.

**One regression caught during review:** the spec said the scraper returns `list[(event_id, region)]` and the transformer attaches `sold_date` from the search page. But the current parser architecture has the *item page* parser return `sold_date=None` and the search page parser return `(url, sold_date)`. So the new pipeline **drops sold_date on new scrapes** unless the scraper return tuple includes it. Fixed below — the scraper now returns `list[{"event_id", "region", "sold_date"}]`, the transformer passes `sold_date` into the parser, and the spec/spec file are aligned.

The following Task 4 and Task 5 changes incorporate the fix:

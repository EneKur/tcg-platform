# Bronze: Currency Rates + DE/UK Sold Listings + Images Implementation Plan

**Goal:** Build complete historical Bronze data — daily EUR→GBP exchange rates, DE/UK sold listings via Zyte, card images to MinIO.

**Architecture:** Three Dagster assets: (1) backfill daily exchange rates to SQLite, (2) scrape DE sold listings with images, (3) scrape UK sold listings with images. Raw data only, no cleaning or conversion.

**Tech Stack:** ZyteAPI, Frankfurter.app (free API), MinIO, SQLite, Playwright (via Zyte's browserHtml), requests, BeautifulSoup.

---

## File Structure

```
src/tcg_platform/
  resources/
    currency_rates.py       # CurrencyRatesDB resource (NEW)
    minio_client.py          # Existing

  scraping/
    exchange_rate.py         # Fetch + store frankfurter rates (NEW)
    ebay.py                  # Existing scraper — add image URL extraction
    ebay_image.py            # Download + upload image to MinIO (NEW)

  defs/
    exchange_rates_asset.py  # Dagster asset (NEW)
    ebay_de_sold_listings.py # Modify: add image saving + backfill logic
    ebay_uk_sold_listings.py # Modify: add image saving + backfill logic

data/
  currency_rates.db          # NEW SQLite DB for exchange rates

tests/
  scraping/
    test_exchange_rate.py   # Test currency rate fetch/store (NEW)
```

---

### Task 1: Create `currency_rates.db` resource

**Files:**
- Create: `src/tcg_platform/resources/currency_rates.py`
- Create: `tests/scraping/test_exchange_rate.py`

- [ ] **Step 1: Write the failing test**

```python
import pytest
import tempfile, os
from datetime import datetime
from tcg_platform.resources.currency_rates import CurrencyRatesDB

def test_insert_and_retrieve_rates(tmp_path):
    db_path = str(tmp_path / "rates.db")
    db = CurrencyRatesDB(db_path=str(tmp_path / "rates.db"))
    db.setup()

    # Insert a rate
    db.insert_rate("EUR", "GBP", 0.864, "2025-05-24 00:00:00")
    db.insert_rate("EUR", "GBP", 0.863, "2025-05-23 00:00:00")

    # Get last timestamp
    last = db.get_last_timestamp()
    assert last is not None
    assert last.strftime("%Y-%m-%d") == "2025-05-24"

    # Get all rates
    rates = db.get_all_rates()
    assert len(rates) == 2

    # Insert duplicate — should be ignored (no error)
    db.insert_rate("EUR", "GBP", 0.864, "2025-05-24 00:00:00")
    rates = db.get_all_rates()
    assert len(rates) == 2  # still 2, no duplicate

    db.close()

def test_get_last_timestamp_empty():
    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        db = CurrencyRatesDB(db_path=f.name)
        db.setup()
        last = db.get_last_timestamp()
        assert last is None  # empty DB returns None
        db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/scraping/test_exchange_rate.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write minimal implementation**

```python
import sqlite3
from datetime import datetime
from typing import Optional

from dagster import ConfigurableResource
from dagster._config.pythonic_config.resource import InitResourceContext


class CurrencyRatesDB(ConfigurableResource):
    db_path: str

    _conn: Optional[sqlite3.Connection] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        import os
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self._conn.row_factory = sqlite3.Row
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                base TEXT NOT NULL DEFAULT 'EUR',
                quote TEXT NOT NULL DEFAULT 'GBP',
                rate REAL NOT NULL,
                timestamp TEXT NOT NULL,
                UNIQUE(base, quote, timestamp)
            )
        """)
        self._conn.commit()

    def insert_rate(self, base: str, quote: str, rate: float, timestamp: str) -> bool:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        try:
            self._conn.execute(
                "INSERT OR IGNORE INTO exchange_rates (base, quote, rate, timestamp) VALUES (?, ?, ?, ?)",
                (base, quote, rate, timestamp)
            )
            self._conn.commit()
            return True
        except Exception:
            return False

    def get_last_timestamp(self) -> Optional[datetime]:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        cursor = self._conn.execute(
            "SELECT timestamp FROM exchange_rates ORDER BY timestamp DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            return None
        return datetime.fromisoformat(row[0])

    def get_all_rates(self) -> list:
        if not self._conn:
            raise RuntimeError("DB not initialized")
        cursor = self._conn.execute("SELECT * FROM exchange_rates ORDER BY timestamp ASC")
        return cursor.fetchall()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/scraping/test_exchange_rate.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/resources/currency_rates.py tests/scraping/test_exchange_rate.py
git commit -m "feat: add CurrencyRatesDB resource for exchange rate history"
```

---

### Task 2: Create `exchange_rates_asset.py`

**Files:**
- Create: `src/tcg_platform/defs/exchange_rates_asset.py`
- Test: Add test to `tests/scraping/test_exchange_rate.py`

- [ ] **Step 1: Write the failing test**

```python
def test_fetch_and_store_rates(monkeypatch, tmp_path):
    import os
    monkeypatch.setenv("CURRENCY_RATES_DB", str(tmp_path / "rates.db"))

    from tcg_platform.defs.exchange_rates_asset import fetch_exchange_rates

    # Mock the frankfurter response
    class FakeResponse:
        def json(self):
            return {
                "base": "EUR",
                "rates": {"GBP": 0.864},
                "date": "2025-05-24"
            }
    monkeypatch.setattr("requests.get", lambda *a, **kw: FakeResponse())

    # Ensure DB is set up
    db = CurrencyRatesDB(db_path=str(tmp_path / "rates.db"))
    db.setup()

    result = fetch_exchange_rates(db)
    assert result == 1  # one rate inserted

    last_ts = db.get_last_timestamp()
    assert last_ts is not None
    db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/scraping/test_exchange_rate.py -v`
Expected: FAIL — exchange_rates_asset not found

- [ ] **Step 3: Write minimal implementation**

```python
import dagster as dg
import requests
from datetime import datetime


@dg.asset
def exchange_rates(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch daily EUR→GBP rates from frankfurter.app, backfill gaps."""
    rates_db = context.resources.currency_rates_db

    last_ts = rates_db.get_last_timestamp()
    if last_ts:
        start = last_ts.strftime("%Y-%m-%d")
    else:
        start = "2023-06-01"

    end = datetime.now().strftime("%Y-%m-%d")

    if start >= end:
        context.log.info("Exchange rates are up to date")
        return dg.MaterializeResult(metadata={"rates_inserted": 0})

    url = f"https://api.frankfurter.app/{start}..{end}?from=EUR&to=GBP"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    resp = requests.get(url, headers=headers, timeout=30)
    data = resp.json()

    inserted = 0
    for date_str, rates in data.get("rates", {}).items():
        rate = rates.get("GBP")
        if rate:
            ts = f"{date_str} 00:00:00"
            if rates_db.insert_rate("EUR", "GBP", rate, ts):
                inserted += 1

    context.log.info(f"Inserted {inserted} exchange rates from {start} to {end}")
    return dg.MaterializeResult(metadata={"rates_inserted": inserted})
```

Also update `definitions.py` to wire `currency_rates_db` resource.

```python
# In definitions.py, add:
from tcg_platform.defs.currency_rates_resource import currency_rates_db

# And in with_resources:
"currency_rates_db": currency_rates_db,
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/scraping/test_exchange_rate.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/exchange_rates_asset.py src/tcg_platform/definitions.py
git commit -m "feat: add exchange_rates dagster asset with frankfurter backfill"
```

---

### Task 3: Add image URL extraction to `ebay.py`

**Files:**
- Modify: `src/tcg_platform/scraping/ebay.py`

- [ ] **Step 1: Add image regex to ebay.py**

Add near top of file:
```python
_IMAGE_RE = re.compile(r'"image":"(https://i\.ebayimg\.com/[^"]+)"')
```

Add function:
```python
def extract_item_image_url(html: str) -> str | None:
    """Extract card image URL from eBay item page HTML."""
    match = _IMAGE_RE.search(html)
    return match.group(1) if match else None
```

- [ ] **Step 2: Write the failing test**

```python
def test_extract_item_image_url():
    from tcg_platform.scraping.ebay import extract_item_image_url

    html = '{"image":"https://i.ebayimg.com/images/something.jpg"}'
    url = extract_item_image_url(html)
    assert url == "https://i.ebayimg.com/images/something.jpg"

    empty_html = '{"other":"data"}'
    url = extract_item_image_url(empty_html)
    assert url is None
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/scraping/ -v -k test_extract_item_image`
Expected: FAIL

- [ ] **Step 4: Confirm implementation already done in Step 1**

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/scraping/ -v -k test_extract_item_image`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/tcg_platform/scraping/ebay.py
git commit -m "feat: add extract_item_image_url to ebay scraper"
```

---

### Task 4: Create `scraping/ebay_image.py`

**Files:**
- Create: `src/tcg_platform/scraping/ebay_image.py`

- [ ] **Step 1: Write the implementation**

```python
import requests
from minio.error import S3Error


def download_and_save_image(
    item_id: str,
    region: str,
    html: str,
    minio_client,
) -> str | None:
    """Extract image URL from eBay HTML, download, upload to MinIO.

    Returns MinIO object path if successful, None otherwise.
    """
    from tcg_platform.scraping.ebay import extract_item_image_url

    img_url = extract_item_image_url(html)
    if not img_url:
        return None

    try:
        img_data = requests.get(img_url, timeout=30).content
    except Exception:
        return None

    object_path = f"sold_images/{region}/{item_id}.jpg"

    try:
        minio_client.put_object(
            bucket_name=minio_client.bucket_name,
            object_name=object_path,
            data=img_data,
            length=len(img_data),
            content_type="image/jpeg",
        )
    except S3Error:
        return None

    return object_path


def image_exists_in_minio(minio_client, item_id: str, region: str) -> bool:
    object_path = f"sold_images/{region}/{item_id}.jpg"
    try:
        minio_client.client.stat_object(minio_client.bucket_name, object_path)
        return True
    except Exception:
        return False
```

- [ ] **Step 2: Commit**

```bash
git add src/tcg_platform/scraping/ebay_image.py
git commit -m "feat: add ebay_image module for MinIO image upload"
```

---

### Task 5: Modify `ebay_de_sold_listings.py` — add image saving

**Files:**
- Modify: `src/tcg_platform/defs/ebay_de_sold_listings.py`

- [ ] **Step 1: Add schema migration + image logic**

The fact_events table needs `image_url` and `local_image_path` columns. Add migration to `sqlite_client.py` or handle in asset.

```python
# Modify ebay_de_sold_listings.py
import dagster as dg
from datetime import datetime, timezone
from tcg_platform.scraping.ebay import (
    parse_ebay_item_page,
    scrape_ebay_listings,
)
from tcg_platform.scraping.ebay_image import (
    download_and_save_image,
    image_exists_in_minio,
)


@dg.asset
def ebay_de_sold_listings(context: dg.AssetExecutionContext) -> list:
    zyte_client = context.resources.zyte_client
    sqlite_client = context.resources.sqlite_client_de
    minio_client = context.resources.minio_client

    # Ensure image columns exist
    _ensure_image_columns(sqlite_client)

    already_seen = sqlite_client.get_seen_ebay_item_ids()
    context.log.info(f"Known item IDs in DE DB: {len(already_seen)}")

    new_item_urls = []
    for item_url in scrape_ebay_listings(zyte_client, "DE", already_seen):
        new_item_urls.append(item_url)

    context.log.info(f"New DE items to scrape: {len(new_item_urls)}")
    if not new_item_urls:
        return []

    records = []
    scraped_at = datetime.now(timezone.utc)

    for item_url in new_item_urls:
        try:
            resp = zyte_client.get({"url": item_url, "browserHtml": True})
            if resp.get("statusCode") != 200:
                continue
            html = resp.get("browserHtml", "")
            if not html:
                continue

            parsed = parse_ebay_item_page(html, item_url, scraped_at, "DE")
            if not parsed:
                continue

            # Extract item_id for image naming
            from tcg_platform.scraping.ebay import _extract_item_id
            item_id = _extract_item_id(item_url)

            # Try to save image to MinIO
            image_path = None
            if not image_exists_in_minio(minio_client, item_id, "DE"):
                image_path = download_and_save_image(item_id, "DE", html, minio_client)

            # Add image info to record
            for rec in parsed:
                rec.image_url = extract_item_image_url(html) if html else None
                rec.local_image_path = image_path

            records.extend(parsed)
        except Exception as e:
            context.log.warning(f"Failed to scrape {item_url}: {e}")
            continue

    context.log.info(f"Scraped {len(records)} new DE sold listing records")
    return records


def _extract_item_id(url: str) -> str:
    import re
    m = re.search(r"/itm/(\d+)", url)
    return m.group(1) if m else url


def extract_item_image_url(html: str) -> str | None:
    from tcg_platform.scraping.ebay import extract_item_image_url as _extract
    return _extract(html)
```

- [ ] **Step 2: Commit**

```bash
git add src/tcg_platform/defs/ebay_de_sold_listings.py
git commit -m "feat: add image saving to DE sold listings asset"
```

---

### Task 6: Same for `ebay_uk_sold_listings.py`

**Files:**
- Modify: `src/tcg_platform/defs/ebay_uk_sold_listings.py`

- [ ] **Step 1: Apply same changes as Task 5, with region="UK"**

- [ ] **Step 2: Commit**

```bash
git add src/tcg_platform/defs/ebay_uk_sold_listings.py
git commit -m "feat: add image saving to UK sold listings asset"
```

---

### Task 7: Add `image_url` and `local_image_path` columns to SQLite schema

**Files:**
- Modify: `src/tcg_platform/resources/sqlite_client.py`

- [ ] **Step 1: Add migration to _initialize_schema**

Add to `_initialize_schema()`:
```python
self._conn.execute("""
    SELECT 1 FROM fact_events WHERE image_url IS NULL LIMIT 1
""")
# Add columns if not exist (SQLite supports IF NOT EXISTS from 3.31+)
try:
    self._conn.execute("""
        ALTER TABLE fact_events ADD COLUMN image_url TEXT
    """)
except Exception:
    pass  # column may already exist

try:
    self._conn.execute("""
        ALTER TABLE fact_events ADD COLUMN local_image_path TEXT
    """)
except Exception:
    pass

self._conn.commit()
```

- [ ] **Step 2: Commit**

```bash
git add src/tcg_platform/resources/sqlite_client.py
git commit -m "feat: add image_url and local_image_path columns to fact_events"
```

---

### Task 8: Wire `currency_rates_db` into `definitions.py`

**Files:**
- Modify: `src/tcg_platform/definitions.py`
- Create: `src/tcg_platform/defs/currency_rates_resource.py`

- [ ] **Step 1: Create resource definition**

```python
# src/tcg_platform/defs/currency_rates_resource.py
import os
from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext
from tcg_platform.resources.currency_rates import CurrencyRatesDB
from dotenv import load_dotenv

load_dotenv()


@resource
def currency_rates_db(init_context: InitResourceContext):
    db_path = os.getenv("CURRENCY_RATES_DB", "./data/currency_rates.db")
    return CurrencyRatesDB(db_path=db_path)
```

- [ ] **Step 2: Update definitions.py**

Add import and wire resource:
```python
from tcg_platform.defs.currency_rates_resource import currency_rates_db

# In with_resources:
"currency_rates_db": currency_rates_db,
```

- [ ] **Step 3: Run import check**

Run: `python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: OK

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/defs/currency_rates_resource.py src/tcg_platform/definitions.py
git commit -m "feat: wire currency_rates_db resource into definitions"
```

---

## Plan Complete

### Execution Options

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks.

**2. Inline Execution** — Execute tasks sequentially in this session with checkpoints.

Which approach?
# eBay Bronze Scraper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scrape eBay US listings using plain HTTP (no Zyte), save each listing as a per-item parquet in MinIO bronze layer alongside downloaded card images.

**Architecture:** A Python scraper walks eBay search pagination, extracts item URLs, then for each item: downloads the page + thumbnail image, parses card_id/currency/price/date from the HTML, uploads image to MinIO, writes parquet to `bronze/listings/us/{item_id}.parquet`. State file tracks resume position so scraping is interruptible and resumable.

**Tech Stack:** Python 3.12, `requests` + `BeautifulSoup` for scraping, `pyarrow` for parquet, `minio` for MinIO, no Zyte.

---

## File Structure

```
src/tcg_platform/
  scraping/
    ebay_bronze.py        # core scraper: page walk, item parsing, card_id extraction
    models.py             # existing: PriceRecord, CardRecord (add ListingRecord?)
  serialization/
    listing_parquet.py    # write/read per-item parquet with full schema
scripts/
  scrape_ebay_bronze.py   # CLI entry: seed → paginate → scrape → write parquet
tests/
  test_ebay_bronze.py     # unit tests for card_id extraction, currency detection, date parsing
data/
  scrape_state_us.json    # created at runtime: last page, seen item_ids set
```

---

## Task 1: `listing_parquet.py` — Per-Item Parquet Writer

**Files:**
- Create: `src/tcg_platform/serialization/listing_parquet.py`
- Test: `tests/test_listing_parquet.py`

- [ ] **Step 1: Write the failing test**

```python
import io
import pyarrow as pa

def test_listing_parquet_schema():
    from tcg_platform.serialization.listing_parquet import LISTING_SCHEMA, row_to_arrow_table

    row = {
        "item_id": "406939215710",
        "source_url": "https://www.ebay.com/itm/406939215710",
        "scraped_at": "2026-05-23T10:00:00Z",
        "region": "US",
        "card_id": "OP15-001",
        "card_version": "_Alternative_Art",
        "title": "One Piece TCG OP15-001 Alternative Art",
        "price": 12.99,
        "currency": "USD",
        "sold_date": "2026-04-15",
        "language": "EN",
        "html_payload": b"<html></html>",
        "thumbnail_url": "https://i.ebayimg.com/1234.jpg",
        "image_path": "bronze/images/OP15-001_Alternative_Art_406939215710.jpg",
    }

    table = row_to_arrow_table([row])
    assert table.schema.equals(LISTING_SCHEMA)
    assert table.num_rows == 1
    assert table.column("item_id")[0].as_py() == "406939215710"
    assert table.column("image_path")[0].as_py() == "bronze/images/OP15-001_Alternative_Art_406939215710.jpg"
```

Run: `pytest tests/test_listing_parquet.py::test_listing_parquet_schema -v`
Expected: FAIL — module doesn't exist yet

- [ ] **Step 2: Create `src/tcg_platform/serialization/listing_parquet.py`**

```python
import io
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq


LISTING_SCHEMA = pa.schema([
    pa.field("item_id", pa.string()),
    pa.field("source_url", pa.string()),
    pa.field("scraped_at", pa.string()),
    pa.field("region", pa.string()),
    pa.field("card_id", pa.string()),
    pa.field("card_version", pa.string()),
    pa.field("title", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("sold_date", pa.string()),
    pa.field("language", pa.string()),
    pa.field("html_payload", pa.binary()),
    pa.field("thumbnail_url", pa.string()),
    pa.field("image_path", pa.string()),
])


def row_to_arrow_table(rows: list[dict]) -> pa.Table:
    now = datetime.now(timezone.utc)
    processed = []
    for row in rows:
        processed.append({
            "item_id": str(row.get("item_id", "")),
            "source_url": str(row.get("source_url", "")),
            "scraped_at": row.get("scraped_at") or now.isoformat(),
            "region": str(row.get("region", "US")),
            "card_id": str(row.get("card_id", "")),
            "card_version": str(row.get("card_version", "")),
            "title": str(row.get("title", "")),
            "price": float(row.get("price") or 0.0),
            "currency": str(row.get("currency", "USD")),
            "sold_date": str(row.get("sold_date", "")),
            "language": str(row.get("language", "EN")),
            "html_payload": row.get("html_payload") or b"",
            "thumbnail_url": str(row.get("thumbnail_url", "")),
            "image_path": str(row.get("image_path", "")),
        })
    table = pa.Table.from_pylist(processed, schema=LISTING_SCHEMA)
    return table


def write_parquet_bytes(rows: list[dict]) -> bytes:
    table = row_to_arrow_table(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def read_parquet_bytes(data: bytes) -> pa.Table:
    buf = io.BytesIO(data)
    return pq.read_table(buf)
```

- [ ] **Step 3: Run test to verify it passes**

Run: `pytest tests/test_listing_parquet.py::test_listing_parquet_schema -v`
Expected: PASS

- [ ] **Step 4: Write test for read/write roundtrip**

```python
def test_listing_parquet_roundtrip():
    from tcg_platform.serialization.listing_parquet import write_parquet_bytes, read_parquet_bytes

    row = {
        "item_id": "999",
        "source_url": "https://www.ebay.com/itm/999",
        "scraped_at": "2026-05-23T10:00:00Z",
        "region": "US",
        "card_id": "ST01",
        "card_version": "",
        "title": "One Piece TCG Starter Deck",
        "price": 5.99,
        "currency": "USD",
        "sold_date": "",
        "language": "EN",
        "html_payload": b"<html>test</html>",
        "thumbnail_url": "https://i.ebayimg.com/test.jpg",
        "image_path": "",
    }

    data = write_parquet_bytes([row])
    table = read_parquet_bytes(data)
    assert table.num_rows == 1
    assert table.column("card_id")[0].as_py() == "ST01"
    assert table.column("html_payload")[0].as_py() == b"<html>test</html>"
```

Run: `pytest tests/test_listing_parquet.py::test_listing_parquet_roundtrip -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/serialization/listing_parquet.py tests/test_listing_parquet.py
git commit -m "feat: add listing parquet serialization"
```

---

## Task 2: `ebay_bronze.py` — Core Parsing Logic

**Files:**
- Create: `src/tcg_platform/scraping/ebay_bronze.py`
- Test: `tests/test_ebay_bronze.py` (add new tests)

- [ ] **Step 1: Write tests for card_id extraction**

```python
import re

def test_card_id_extraction():
    from tcg_platform.scraping.ebay_bronze import extract_card_id

    cases = [
        ("One Piece TCG OP15-001 Alternative Art", "OP15-001"),
        ("One Piece OP09119 Luffy", "OP09119"),
        ("EB01-001 Black Card", "EB01-001"),
        ("ST03013 Starter Deck", "ST03013"),
        ("14x One Piece TCG OP15", "OP15"),
        ("One Piece TCG PRB01 Promo", "PRB01"),
        ("Random Bundle Not a Card", ""),  # no valid card id
        ("OPCG Charlotte Linlin", ""),  # OPCG is not valid set code format
        ("Borsalino OP15", "OP15"),  # extra text before
        ("OP15-001 (Alternative Art)", "OP15-001"),
    ]

    for title, expected in cases:
        result = extract_card_id(title)
        assert result == expected, f"Title: {title!r} → got {result!r}, expected {expected!r}"
```

Run: `pytest tests/test_ebay_bronze.py::test_card_id_extraction -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 2: Create `src/tcg_platform/scraping/ebay_bronze.py`**

```python
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Card ID extraction
# ---------------------------------------------------------------------------
SET_CODE_RE = re.compile(r"\b(OP\d+-\d+|EB\d+-\d+|OP\d+|EB\d+|ST\d+|PRB\d+)\b")


def extract_card_id(title: str) -> str:
    """Extract the best card_id from a listing title.

    Priority:
      1. OP###-### (e.g. OP15-001)
      2. EB###-### (e.g. EB01-001)
      3. OP### (e.g. OP15)
      4. EB### (e.g. EB01)
      5. ST### / PRB###

    Returns empty string if no valid card_id found.
    """
    matches = SET_CODE_RE.findall(title)
    if not matches:
        return ""

    # Prefer multi-digit set+number (OP15-001 > OP15)
    for m in matches:
        if "-" in m:
            return m  # highest priority: OP###-### or EB###-###
    # Fall back to first OP/EB without dash (ST/PR also possible)
    for m in matches:
        if m.startswith(("OP", "EB")):
            return m
    return matches[0] if matches else ""


def extract_card_version(title: str, card_id: str) -> str:
    """Extract version suffix after the card_id in the title.

    E.g. "OP15-001 Alternative Art" → "_Alternative_Art"
         "OP15-001" → ""
    """
    if not card_id:
        return ""
    # Find card_id in title and take everything after it
    idx = title.find(card_id)
    if idx == -1:
        return ""
    remainder = title[idx + len(card_id):].strip()
    if not remainder:
        return ""
    # Strip leading separators
    remainder = re.sub(r"^[-_\s]+", "", remainder)
    if not remainder:
        return ""
    # Normalize: spaces to underscore, keep alphanumeric + underscore
    version = remainder.replace(" ", "_")
    version = re.sub(r"[^a-zA-Z0-9_]", "", version)
    return f"_{version}" if version else ""


# ---------------------------------------------------------------------------
# Currency detection
# ---------------------------------------------------------------------------
def detect_currency(price_text: str, url: str) -> str:
    """Detect currency from price text and URL domain.

    eBay domain → default currency:
      ebay.com.au → AUD
      ebay.ca     → CAD
      ebay.co.uk  → GBP
      ebay.de     → EUR
      ebay.com    → USD (default)
    Price symbols:
      A$ / AUD → AUD
      C$ / CAD → CAD
      £ / GBP  → GBP
      € / EUR  → EUR
      $ / USD  → USD (default)
    """
    price_text_lower = price_text.lower()

    if "a$" in price_text_lower or "aud" in price_text_lower:
        return "AUD"
    if "c$" in price_text_lower or "cad" in price_text_lower:
        return "CAD"
    if "£" in price_text or "gbp" in price_text_lower:
        return "GBP"
    if "€" in price_text or "eur" in price_text_lower:
        return "EUR"

    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if "ebay.com.au" in domain:
        return "AUD"
    if "ebay.ca" in domain:
        return "CAD"
    if "ebay.co.uk" in domain:
        return "GBP"
    if "ebay.de" in domain:
        return "EUR"

    return "USD"


# ---------------------------------------------------------------------------
# Price extraction
# ---------------------------------------------------------------------------
def extract_price(price_text: str) -> Optional[float]:
    """Extract float price from text like '$1,234.56' or '1,234.56'."""
    if not price_text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", price_text)
    # Handle both comma and period as decimal separator
    if "," in cleaned and "." in cleaned:
        # Assume comma is thousands separator if it appears before the last dot
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(",", ".")
    elif "," in cleaned:
        # Could be decimal or thousands — if last comma is followed by 2 digits, it's decimal
        parts = cleaned.split(",")
        if len(parts[-1]) <= 2:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# sold_date parsing (English)
# ---------------------------------------------------------------------------
MONTHS_EN = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}


def parse_sold_date(html: str) -> str:
    """Extract sold date from eBay listing HTML.

    Formats:
      "Sold Mon, Jan 1, 2024"
      "Sold Monday, January 1, 2024"
      "Sold Jan 1, 2024"
      "Jan 1, 2024"
    Returns YYYY-MM-DD or "" if not found.
    """
    # Primary pattern: "Sold Weekday, Month DD, YYYY"
    m = re.search(r"Sold\s+\w+,?\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})", html, re.IGNORECASE)
    if not m:
        # Fallback: "Month DD, YYYY" (no Sold prefix)
        m = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", html, re.IGNORECASE)
    if not m:
        return ""
    month_name = m.group(1).lower()
    day = int(m.group(2))
    year = int(m.group(3))
    month_num = MONTHS_EN.get(month_name)
    if month_num is None:
        return ""
    return f"{year}-{month_num:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------
JP_INDICATORS = [
    "japan", "jap", "jp_", "japanese",
    "jp-", "japan import", "japanese version",
]


def detect_language(title: str) -> str:
    """Detect language from title."""
    title_lower = title.lower()
    return "JP" if any(ind in title_lower for ind in JP_INDICATORS) else "EN"


# ---------------------------------------------------------------------------
# Thumbnail URL extraction
# ---------------------------------------------------------------------------
def extract_thumbnail_url(html: str) -> str:
    """Extract listing thumbnail URL from HTML.

    Tries: data-old-hires attribute, src attribute on icImg, and fallback regex.
    Returns empty string if not found.
    """
    # Pattern 1: <img id="icImg" data-old-hires="https://..." src="...">
    m = re.search(r'<img[^>]+id="icImg"[^>]+data-old-hires="([^"]+)"', html)
    if m:
        return m.group(1)
    # Pattern 2: data-old-hires before src on icImg
    m = re.search(r'<img[^>]+data-old-hires="([^"]+)"[^>]+id="icImg"', html)
    if m:
        return m.group(1)
    # Pattern 3: src attribute on icImg
    m = re.search(r'<img[^>]+id="icImg"[^>]+src="([^"]+)"', html)
    if m:
        return m.group(1)
    return ""


# ---------------------------------------------------------------------------
# Title normalization for card_id extraction (helper for extract_card_id)
# ---------------------------------------------------------------------------
def normalize_title(title: str) -> str:
    """Strip noise from title before card_id extraction."""
    title = re.sub(r"\s*\(.*?\)", "", title)
    title = re.sub(r"\s*\[.*?\]", "", title)
    return title.strip()
```

Run: `pytest tests/test_ebay_bronze.py::test_card_id_extraction -v`
Expected: PASS

- [ ] **Step 3: Write test for currency detection**

```python
def test_currency_detection():
    from tcg_platform.scraping.ebay_bronze import detect_currency

    cases = [
        ("$12.99", "https://www.ebay.com/itm/123", "USD"),
        ("A$45.00", "https://www.ebay.com/itm/123", "AUD"),
        ("£9.99", "https://www.ebay.co.uk/itm/123", "GBP"),
        ("€15.00", "https://www.ebay.de/itm/123", "EUR"),
        ("C$25.00", "https://www.ebay.ca/itm/123", "CAD"),
        ("$100.00", "https://www.ebay.com.au/itm/123", "AUD"),
        ("12,345.00", "https://www.ebay.com/itm/123", "USD"),
        ("AU$59.99", "https://www.ebay.com/itm/123", "AUD"),
    ]

    for price_text, url, expected in cases:
        result = detect_currency(price_text, url)
        assert result == expected, f"price={price_text!r}, url={url!r} → {result!r}, expected {expected!r}"
```

Run: `pytest tests/test_ebay_bronze.py::test_currency_detection -v`
Expected: PASS

- [ ] **Step 4: Write test for sold_date parsing**

```python
def test_sold_date_parsing():
    from tcg_platform.scraping.ebay_bronze import parse_sold_date

    cases = [
        ('Sold Mon, Jan 1, 2024', "2024-01-01"),
        ('Sold Tuesday, January 14, 2025', "2025-01-14"),
        ('Sold Jan 15, 2024', "2024-01-15"),
        ('Sold Dec 25, 2023', "2023-12-25"),
        ('Jan 1, 2024', "2024-01-01"),
        ('February 3, 2024', "2024-02-03"),
        ('Sold Fri, Mar 7, 2026', "2026-03-07"),
        ('No date here', ""),
        ('verkauft am Montag, 1. Januar 2024', ""),  # German — not parsed
    ]

    for html, expected in cases:
        result = parse_sold_date(html)
        assert result == expected, f"html={html!r} → {result!r}, expected {expected!r}"
```

Run: `pytest tests/test_ebay_bronze.py::test_sold_date_parsing -v`
Expected: PASS

- [ ] **Step 5: Write test for card_version extraction**

```python
def test_card_version_extraction():
    from tcg_platform.scraping.ebay_bronze import extract_card_version

    cases = [
        ("One Piece TCG OP15-001 Alternative Art", "OP15-001", "_Alternative_Art"),
        ("OP15-001", "OP15-001", ""),
        ("EB01-001 Black", "EB01-001", "_Black"),
        ("OP15 Alternative Art", "OP15", "_Alternative_Art"),
        ("ST03013", "ST03013", ""),
        ("", ""),  # empty title
    ]

    for title, card_id, expected in cases:
        result = extract_card_version(title, card_id)
        assert result == expected, f"title={title!r}, card_id={card_id!r} → {result!r}, expected {expected!r}"
```

Run: `pytest tests/test_ebay_bronze.py::test_card_version_extraction -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/tcg_platform/scraping/ebay_bronze.py tests/test_ebay_bronze.py
git commit -m "feat: add eBay bronze scraper core parsing"
```

---

## Task 3: `scrape_ebay_bronze.py` — Full Pipeline Script

**Files:**
- Create: `scripts/scrape_ebay_bronze.py`
- Modify: `src/tcg_platform/scraping/ebay_bronze.py` (add `parse_listing_page` function)

- [ ] **Step 1: Add `parse_listing_page` to `ebay_bronze.py`**

Add this function to `src/tcg_platform/scraping/ebay_bronze.py`:

```python
def parse_listing_page(html: str, url: str, scraped_at: datetime) -> dict:
    """Parse a full eBay item page into a listing row dict."""
    from tcg_platform.scraping.ebay_bronze import (
        extract_card_id, extract_card_version, detect_currency,
        extract_price, parse_sold_date, detect_language,
        extract_thumbnail_url, normalize_title,
    )

    # Extract raw title
    title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    raw_title = title_match.group(1).strip() if title_match else ""
    # Strip eBay suffix from title
    raw_title = re.sub(r"\s*-\s*eBay$", "", raw_title)
    title = raw_title

    # Extract price
    price_match = re.search(r'data-testid="x-price-primary".*?<span[^>]*>([^<]+)</span>', html, re.DOTALL)
    price_text = price_match.group(1).strip() if price_match else ""
    price = extract_price(price_text)
    currency = detect_currency(price_text, url)

    # Normalize title and extract card_id
    norm_title = normalize_title(title)
    card_id = extract_card_id(norm_title)
    card_version = extract_card_version(title, card_id)

    # sold_date
    sold_date = parse_sold_date(html)

    # Language
    language = detect_language(title)

    # Thumbnail
    thumbnail_url = extract_thumbnail_url(html)

    # item_id from URL
    item_id_match = re.search(r"/itm/(\d+)", url)
    item_id = item_id_match.group(1) if item_id_match else ""

    return {
        "item_id": item_id,
        "source_url": url,
        "scraped_at": scraped_at.isoformat() if hasattr(scraped_at, "isoformat") else str(scraped_at),
        "region": "US",
        "card_id": card_id,
        "card_version": card_version,
        "title": title,
        "price": price,
        "currency": currency,
        "sold_date": sold_date,
        "language": language,
        "html_payload": html.encode("utf-8", errors="replace"),
        "thumbnail_url": thumbnail_url,
        "image_path": "",
    }
```

- [ ] **Step 2: Create `scripts/scrape_ebay_bronze.py`**

```python
#!/usr/bin/env uv run python3
"""
Scrape eBay US listings → MinIO bronze layer.

Resume-safe: tracks state in data/scrape_state_us.json
Saves per-item parquet to bronze/listings/us/{item_id}.parquet
Saves card images to bronze/images/{card_id}_{card_version}_{item_id}.{ext}
"""
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from minio import Minio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from tcg_platform.scraping.ebay_bronze import (
    extract_thumbnail_url,
    extract_card_id,
    extract_card_version,
    normalize_title,
    parse_listing_page,
)
from tcg_platform.serialization.listing_parquet import write_parquet_bytes

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "tcg-bronze")

BASE_URL = (
    "https://www.ebay.com/sch/i.html"
    "?_nkw=One+Piece+TCG+&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
)
LISTING_LINK_RE = re.compile(r'href="(https://www\.ebay\.com/itm/\d+[^"]*)"')
ITEM_ID_RE = re.compile(r"/itm/(\d+)")
STATE_FILE = Path("data/scrape_state_us.json")
IMG_BUCKET = MINIO_BUCKET
IMG_PREFIX = "bronze/images/"
LIST_PREFIX = "bronze/listings/us/"
PAGES_BEFORE_STOP = 3
RATE_LIMIT = 1.0  # seconds between requests


def get_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_page": 0, "seen_ids": [], "page_errors": 0}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def item_exists_in_minio(client: Minio, item_id: str) -> bool:
    key = f"{LIST_PREFIX}{item_id}.parquet"
    try:
        client.stat_object(MINIO_BUCKET, key)
        return True
    except Exception:
        return False


def upload_image(client: Minio, item_id: str, card_id: str, card_version: str, url: str) -> str:
    """Download image and upload to MinIO. Returns MinIO key or empty string on failure."""
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg").split(";")[0]
        ext = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}.get(content_type, "jpg")
    except Exception:
        return ""

    version_suffix = card_version.replace("_", "")
    img_key = f"{IMG_PREFIX}{card_id}{card_version}_{item_id}.{ext}"
    try:
        client.put_object(
            IMG_BUCKET,
            img_key,
            io.BytesIO(resp.content),
            length=len(resp.content),
            content_type=content_type,
        )
        return img_key
    except Exception:
        return ""


def get_page_urls(client: requests.Session, url: str) -> list[str]:
    """Fetch a search results page and return deduplicated item URLs."""
    resp = client.get(url, timeout=30)
    if resp.status_code != 200:
        return []
    html = resp.text
    # Decode HTML entities that eBay uses for URL params
    html = html.replace("&amp;", "&")
    urls = []
    for m in LISTING_LINK_RE.finditer(html):
        raw_url = m.group(1).split("?")[0]  # strip query params
        item_id_m = ITEM_ID_RE.search(raw_url)
        if item_id_m:
            urls.append((item_id_m.group(1), raw_url))
    seen = set()
    deduped = []
    for item_id, raw_url in urls:
        if item_id not in seen:
            seen.add(item_id)
            deduped.append(raw_url)
    return deduped


def main():
    print(f"Starting eBay US bronze scraper")
    print(f"MinIO: {MINIO_ENDPOINT}/{MINIO_BUCKET}")

    state = load_state()
    last_page = state.get("last_page", 0)
    seen_ids = set(state.get("seen_ids", []))
    page_errors = state.get("page_errors", 0)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })

    client = get_minio_client()
    scraped_at = datetime.now(timezone.utc)
    new_scraped = 0
    new_images = 0

    page = last_page + 1
    empty_pages = 0

    while page < 9999:
        url = BASE_URL if page == 1 else f"{BASE_URL}&_pgn={page}"
        print(f"\nPage {page}: fetching...", flush=True)

        try:
            resp = session.get(url, timeout=30)
            status = resp.status_code
        except Exception as e:
            print(f"  HTTP error: {e}")
            page_errors += 1
            if page_errors >= 3:
                print("Too many page errors — stopping")
                break
            page += 1
            continue

        if status != 200:
            print(f"  HTTP {status} — stopping")
            break

        html = resp.text
        item_urls = get_page_urls(session, url)

        if not item_urls:
            empty_pages += 1
            print(f"  0 URLs (empty_pages={empty_pages})", flush=True)
            if empty_pages >= PAGES_BEFORE_STOP:
                print("Exhausted — stopping")
                break
            page += 1
            continue

        empty_pages = 0
        print(f"  {len(item_urls)} item URLs found, {len(seen_ids)} already known")

        for item_url in item_urls:
            item_id_m = ITEM_ID_RE.search(item_url)
            item_id = item_id_m.group(1) if item_id_m else ""

            if item_id in seen_ids:
                continue

            # Check if already in MinIO
            if item_exists_in_minio(client, item_id):
                seen_ids.add(item_id)
                continue

            print(f"  Scraping item {item_id}...", end="", flush=True)

            try:
                item_resp = session.get(item_url, timeout=30)
                if item_resp.status_code != 200:
                    print(f" HTTP {item_resp.status_code}")
                    time.sleep(RATE_LIMIT)
                    continue
                item_html = item_resp.text
            except Exception as e:
                print(f" err: {e}")
                time.sleep(RATE_LIMIT)
                continue

            row = parse_listing_page(item_html, item_url, scraped_at)

            # Skip entries with no card_id (not a valid card listing)
            if not row["card_id"]:
                print(f" no_card_id", end="")
                seen_ids.add(item_id)
                time.sleep(RATE_LIMIT)
                continue

            # Download thumbnail image
            thumbnail_url = row["thumbnail_url"]
            if thumbnail_url:
                card_id = row["card_id"]
                card_version = row["card_version"]
                img_path = upload_image(client, item_id, card_id, card_version, thumbnail_url)
                row["image_path"] = img_path
                if img_path:
                    print(f" ✓ img={img_path.split('/')[-1][:30]}", end="")
                    new_images += 1

            # Write parquet to MinIO
            parquet_data = write_parquet_bytes([row])
            parquet_key = f"{LIST_PREFIX}{item_id}.parquet"
            try:
                client.put_object(
                    MINIO_BUCKET,
                    parquet_key,
                    io.BytesIO(parquet_data),
                    length=len(parquet_data),
                    content_type="application/parquet",
                )
                print(f" ✓ row", end="")
                new_scraped += 1
            except Exception as e:
                print(f" parquet err: {e}", end="")

            seen_ids.add(item_id)
            time.sleep(RATE_LIMIT)

        # Save state
        state["last_page"] = page
        state["seen_ids"] = list(seen_ids)
        state["page_errors"] = page_errors
        save_state(state)

        page += 1

    print(f"\nDone. New rows: {new_scraped}, new images: {new_images}")
    print(f"State saved: page={page}, seen={len(seen_ids)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run the script dry-run / smoke test**

```bash
uv run python scripts/scrape_ebay_bronze.py 2>&1 | head -50
```

Expected: Script starts, fetches page 1, finds item URLs, skips already-seen items, writes parquet for new ones.

- [ ] **Step 4: Commit**

```bash
git add scripts/scrape_ebay_bronze.py src/tcg_platform/scraping/ebay_bronze.py
git commit -m "feat: add eBay bronze scraper pipeline script"
```

---

## Verification

After all tasks complete:
1. Run `pytest tests/` — all tests pass
2. Run `uv run python scripts/scrape_ebay_bronze.py` — should show page 1 fetching and writing parquets
3. Check MinIO for `bronze/listings/us/` objects and `bronze/images/` objects
4. Check `data/scrape_state_us.json` exists and has correct shape

---

**Plan complete.** Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
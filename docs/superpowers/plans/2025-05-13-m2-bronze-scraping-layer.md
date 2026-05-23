# M2: Bronze Scraping Layer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three bronze Dagster assets that scrape PriceCharting and Limitless TCG via Steel `scrape()` + BeautifulSoup, returning raw structured data. Storage layer (M3/M4) built separately — assets output typed dataclasses, wired to storage in M3/M4.

**Architecture:** Steel `scrape()` fetches HTML server-side. BeautifulSoup + lxml parses into Pydantic models. Assets return `list[CardRecord]` and `list[PriceRecord]`. Storage (MinIO/SQLite) wired in M3/M4.

**Tech Stack:** `steel-sdk`, `beautifulsoup4`, `lxml`, `pydantic`, `python-dotenv`

---

## File Structure

```
tcg_platform/
  scraping/
    pricecharting.py      # PriceCharting scrape asset + Pydantic models
    limitlesstcg.py      # Limitless TCG scrape asset + Pydantic models + image download
    models.py             # Shared Pydantic models (CardRecord, PriceRecord, ImageRecord)
    __init__.py
  assets/
    __init__.py
    scraping_assets.py   # Dagster asset definitions wiring to resources
  defs/
    scraping_assets.py   # Dagster definitions for scraping assets

tests/
  scraping/
    test_pricecharting.py
    test_limitlesstcg.py
    test_models.py
```

> **Note on M3/M4 dependency:** These assets output typed records but don't write to storage directly. They return `list[CardRecord]` etc. which M3/M4 assets will consume and write to MinIO parquet / SQLite.

---

### Task 1: M2-T1 — PriceCharting scraping pipeline

**Files:**
- Create: `src/tcg_platform/scraping/models.py` (if not exists from prior plan)
- Create: `src/tcg_platform/scraping/pricecharting.py`
- Create: `tests/scraping/test_pricecharting.py`
- Modify: `src/tcg_platform/scraping/__init__.py`

**Note:** Steel `scrape()` with `delay=3s` returns HTML shell only for JS-rendered sites. PriceCharting works via `search-products` endpoint: `https://www.pricecharting.com/search-products?q=one+piece+tcg&type=prices` → returns full table HTML with card names, sets, prices.

**URL to use:** `https://www.pricecharting.com/search-products?q=one+piece+tcg&type=prices&ignore-preferences=true`

**CSS selectors (verified against real HTML):**
- Table rows: `<tr>` elements within the results table
- Row structure: Cell 1 = title/set info, Cell 2 = set name, Cell 3 = low price, Cell 5 = high price
- Header row: `Title`, `Set`, `Low Price`, `Mid Price`, `High Price`

- [ ] **Step 1: Write the failing test**

```python
# tests/scraping/test_pricecharting.py
import pytest
from tcg_platform.scraping.pricecharting import parse_pricecharting_html

def test_parse_extracts_us_price():
    html = """
    <tr><td></td><td>DON!! Card</td><td>One Piece Japanese Promo</td><td>$13.40</td><td></td><td></td></tr>
    """
    records = parse_pricecharting_html(html)
    assert len(records) >= 1

def test_parse_handles_empty_html():
    records = parse_pricecharting_html("<html><body></body></html>")
    assert records == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/scraping/test_pricecharting.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write PriceCharting scraper**

```python
# src/tcg_platform/scraping/pricecharting.py
from datetime import datetime
import re
from bs4 import BeautifulSoup
from tcg_platform.scraping.models import PriceRecord

PRICECHARTING_SEARCH_URL = (
    "https://www.pricecharting.com/search-products"
    "?q=one+piece+tcg&type=prices&ignore-preferences=true"
)
PRICECHARTING_CATEGORY_URL = "https://www.pricecharting.com/category/one-piece-cards"

def parse_pricecharting_html(html: str) -> list[PriceRecord]:
    """Parse PriceCharting search results HTML into PriceRecord list.

    Verified CSS selectors:
    - Each card row is a <tr> element
    - Cell 1 (td 1): title + set name combined
    - Cell 2 (td 2): set name
    - Cell 3 (td 3): low price (USD)
    - Cell 5 (td 5): high price (USD)
    """
    soup = BeautifulSoup(html, "lxml")
    records = []
    scraped_at = datetime.utcnow()

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        # Cell 1: full title text (includes card name + set info)
        title_cell = cells[1].get_text(strip=True)
        set_cell = cells[2].get_text(strip=True)

        if not title_cell or title_cell == "Title":
            continue

        # Extract card_id from title_cell (text before set name)
        # Title format: "CardName SetCode" or "DON!! Card [One Piece Day '25]"
        card_name = title_cell

        # Get USD prices
        low_price_text = cells[3].get_text(strip=True).replace("$", "").replace(",", "")
        high_price_text = cells[5].get_text(strip=True).replace("$", "").replace(",", "") if len(cells) > 5 else ""

        try:
            low_price = float(low_price_text) if low_price_text else None
        except ValueError:
            low_price = None

        try:
            high_price = float(high_price_text) if high_price_text else None
        except ValueError:
            high_price = None

        # Determine card_id (normalize card name for matching)
        card_id = _normalize_card_id(card_name)

        # Create price record for low price if available
        if low_price is not None:
            records.append(PriceRecord(
                card_id=card_id,
                card_version=None,
                event_type="price_update",
                price=low_price,
                currency="USD",
                sold_date=None,
                scraped_from="pricecharting",
                source="US",  # PriceCharting prices in USD
                source_url=PRICECHARTING_CATEGORY_URL,
                scraped_at=scraped_at,
            ))

        # Create price record for high price if available
        if high_price is not None:
            records.append(PriceRecord(
                card_id=card_id,
                card_version=None,
                event_type="price_update",
                price=high_price,
                currency="USD",
                sold_date=None,
                scraped_from="pricecharting",
                source="US",
                source_url=PRICECHARTING_CATEGORY_URL,
                scraped_at=scraped_at,
            ))

    return records

def _normalize_card_id(card_name: str) -> str:
    """Convert card name to a usable card_id format."""
    # Remove bracketed suffixes like [One Piece Day '25]
    normalized = re.sub(r'\[.*?\]', '', card_name)
    # Remove special chars, lowercase
    normalized = re.sub(r'[^a-zA-Z0-9\s]', '', normalized)
    normalized = normalized.strip().replace(' ', '_')[:50]
    return normalized

def scrape_pricecharting(steel_api_key: str) -> list[PriceRecord]:
    """Scrape PriceCharting via search-products endpoint."""
    from steel import Steel
    client = Steel(steel_api_key=steel_api_key)
    result = client.scrape(url=PRICECHARTING_SEARCH_URL, delay=3.0)
    return parse_pricecharting_html(result.content.html)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_pricecharting.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/scraping/ tests/scraping/
git commit -m "feat: add PriceCharting scraping pipeline"
```

---

### Task 2: M2-T2 — Limitless TCG scraping pipeline (DEFERRED to M6)

Deferred — requires local Steel browser session via Podman to resolve JS rendering.

---

### Task 3: M2-T3 — Image download utilities (DEFERRED to M6)

Deferred — depends on M2-T2 (card data needed before images can be organized).

---

### Task 4: M2-T4 — Create log files

```bash
touch log/M2-T1.md log/M2-T2.md log/M2-T3.md log/M2-T4.md
```

---

## Key Findings from Site Inspection

**PriceCharting:**
- URL: `https://www.pricecharting.com/search-products?q=one+piece+tcg&type=prices&ignore-preferences=true`
- Returns full table with 100 rows (search results)
- Table columns: Title | Set | Low Price | Mid Price | High Price
- CSS selector: `tr` elements, cells in `td`
- Complication: mixed results (TCG cards + comics + books) — filter by set name
- Max 100 results per search — need pagination strategy or multiple searches

**Limitless TCG:**
- Fully JS-rendered — `scrape()` returns empty shells
- Main JS bundle hint: `/api/cards/${t}?v=${n}` — needs dynamic parameters
- Defer to M6 (local Steel browser session)

**eBay:**
- Deferred to M6

**WebSocket (M6-T1):**
- Steel cloud `wss://connect.steel.dev` returns 502 — local Steel via Podman in progress
- Image pull running in background: `ghcr.io/steel-dev/steel-browser:latest`
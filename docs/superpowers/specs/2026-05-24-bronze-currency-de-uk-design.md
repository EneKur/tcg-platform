# Bronze Layer: Currency History + DE/UK Sold Listings + Images

**Date:** 2026-05-24
**Status:** Approved

---

## Goal

Build complete historical Bronze data:
- Daily EUR→GBP exchange rates (frankfurter.app)
- DE and UK eBay sold listings via Zyte (raw, unclean)
- Card images saved to MinIO per item_id

Bronze stores raw data. No cleaning, no currency conversion, no arbitrage logic. That is Silver/Gold.

---

## 1. Exchange Rate DB — `currency_rates.db`

**API:** `https://api.frankfurter.app` — free, no auth, daily rates back to 2023-06-01.

**Schema:**
```sql
CREATE TABLE exchange_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base TEXT NOT NULL DEFAULT 'EUR',
    quote TEXT NOT NULL DEFAULT 'GBP',
    rate REAL NOT NULL,
    timestamp TEXT NOT NULL,  -- YYYY-MM-DD 00:00:00
    UNIQUE(base, quote, timestamp)
);
```

**Logic:**
- On each run: get last stored date from DB
- Fetch from last_date → today via `https://api.frankfurter.app/{start}..{end}?from=EUR&to=GBP`
- Insert daily rates with `INSERT OR IGNORE` (no duplicates)
- Store one rate per day at midnight UTC

---

## 2. eBay DE/UK Sold Listings — Historical Scrape

**Goal:** Fill historical DE (EUR) and UK (GBP) sold listings until hitting existing rows.

**Flow per region:**
1. Load known item_ids from SQLite (via `source_url` column)
2. Start paginating sold listings from newest backward
3. For each item: extract item_id from URL
4. If item_id already in DB → stop pagination for that region (no gaps, no duplicates)
5. If new: scrape detail page → get price, card_id, sold_date, image_url
6. Save image to MinIO
7. Store PriceRecord in SQLite

**Stop condition:** Scraper returns item_id already in DB → done for that region.

---

## 3. Image Saving to MinIO

**MinIO path:** `sold_images/{region}/{item_id}.jpg`

Example: `sold_images/DE/384759102394.jpg`

**Logic:**
- Extract image URL from eBay item page HTML (Zyte `browserHtml` = rendered page)
- Download image → upload to MinIO
- Save `local_image_path` to SQLite so higher layers can reference it

**Image regex from eBay HTML:**
```python
IMAGE_RE = re.compile(r'"image":"(https://i\.ebayimg\.com/[^"]+)"')
```

**Schema addition to `fact_events`:**
```sql
ALTER TABLE fact_events ADD COLUMN image_url TEXT;
ALTER TABLE fact_events ADD COLUMN local_image_path TEXT;
```

---

## File Structure

```
src/tcg_platform/
  resources/
    currency_rates.py   # CurrencyRatesDB resource (sqlite)
    minio_client.py      # Existing MinIO resource

  scraping/
    exchange_rate.py     # Fetch + store frankfurter rates
    ebay.py              # Existing scraper (add image extraction)

  defs/
    exchange_rates_asset.py  # Dagster asset: backfill daily EUR→GBP rates
    ebay_de_sold_listings.py  # DE asset (updated: save images)
    ebay_uk_sold_listings.py  # UK asset (updated: save images)
```

New resource: `currency_rates.py` wrapping `currency_rates.db`.

---

## Idempotency

- Exchange rates: `INSERT OR IGNORE` on (base, quote, timestamp)
- eBay listings: check item_id before scraping, stop on duplicate
- Images: check if object exists in MinIO before downloading

---

## Success Criteria

1. `currency_rates.db` has complete EUR→GBP daily rates from 2023-06-01 to today
2. DE/UK scraper stops when hitting existing rows (no duplicates)
3. Each scraped item has `image_url` and `local_image_path` in SQLite
4. Images stored in MinIO at `sold_images/{region}/{item_id}.jpg`
5. All assets load in Dagster without import errors
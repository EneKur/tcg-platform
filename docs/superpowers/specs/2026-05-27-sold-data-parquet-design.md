## Overview

Archive every scraped eBay sold listing as an individual Parquet file in MinIO under `sold_data/{region}/{item_id}.parquet`. Parquet is written inline during scraping, alongside the image download — no separate aggregation step. SQLite writer handles the batched accumulation as before.

## Data Flow

```
For each scraped item_id:
  ├─── scrape item → parse PriceRecord
  ├─── image → MinIO sold_images/{region}/{item_id}.jpg   (already here)
  └─── parquet → MinIO sold_data/{region}/{item_id}.parquet  (new, inline)
  └─── pass record to sqlite_writer (batch insert)

sqlite_writer accumulates records and does batch INSERT OR IGNORE to SQLite.
```

## Storage Structure

| Path | Format |
|------|--------|
| `sold_data/DE/{item_id}.parquet` | Single-row Parquet per DE eBay item |
| `sold_data/UK/{item_id}.parquet` | Single-row Parquet per UK eBay item |

## Schema

Uses existing `price_records_to_parquet()` from `tcg_platform/serialization/card_parquet.py`. 13 columns:

`event_id, card_id, card_version, event_type, price, currency, sold_date, scraped_from, source, source_url, language, scraped_at, image_url`

## Implementation

- **Modified:** `ebay_de_sold_listings.py` and `ebay_uk_sold_listings.py` — parquet write added inline in scrape loop
- **Removed:** `bronze_sold_data_parquet.py` — standalone parquet asset deleted (no longer needed)
- **No new files** — writes happen in the same loop as image downloads, per-item as soon as parsed
- **Jobs unchanged** — sqlite_writer assets still the accumulation point
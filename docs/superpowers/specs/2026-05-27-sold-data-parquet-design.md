# sold_data Parquet Archive — Design Spec

## Overview

Archive every scraped eBay sold listing as an individual Parquet file in MinIO under `sold_data/{region}/{item_id}.parquet`. This runs as a parallel path alongside the existing SQLite writers, providing a durable raw backup and enabling incremental lakehouse reads.

## Data Flow

```
ebay_de_sold_listings ──┬──→ bronze_ebay_de_sqlite_writer ──→ SQLite
                        │
                        └──→ bronze_de_sold_data_parquet ──→ MinIO sold_data/DE/{item_id}.parquet

ebay_uk_sold_listings ──┬──→ bronze_ebay_uk_sqlite_writer ──→ SQLite
                        │
                        └──→ bronze_uk_sold_data_parquet ──→ MinIO sold_data/UK/{item_id}.parquet
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

- **New file:** `src/tcg_platform/defs/bronze_sold_data_parquet.py`
- **Assets:** `bronze_de_sold_data_parquet` and `bronze_uk_sold_data_parquet`
- **Dependencies:** `minio_client` resource (via context), input list of PriceRecords
- **Per-record loop** extracts `item_id` from `source_url` (reuse `_extract_item_id` from ebay_DE/UK modules or import from `ebay.py`)
- Uses existing `price_records_to_parquet([record])` for single-row parquet per item
- **Jobs updated:** all three pipelines include new assets

## Jobs

- `ebay_de_pipeline` → `["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer", "bronze_de_sold_data_parquet"]`
- `ebay_uk_pipeline` → `["ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer", "bronze_uk_sold_data_parquet"]`
- `ebay_eu_pipeline` → all six assets
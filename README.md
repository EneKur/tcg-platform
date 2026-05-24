# TCG One Piece Card Game Data Platform

Scrapes, stores, and transforms One Piece TCG sold listing data from eBay (DE, UK, US regions) and Limitless TCG, with historical exchange rates and card images.

## Architecture

**Bronze layer** — raw data as-is from sources
**Silver layer** — cleaned card IDs, normalized data (not yet implemented)
**Gold layer** — aggregated analytics, parquet exports (not yet implemented)

## Stack

- **Scraping**: Zyte API (browser automation), `requests` / BeautifulSoup
- **Storage**: SQLite (per-region DBs), MinIO (card images), Parquet (analytics exports)
- **Orchestration**: Dagster (`dg dev`)
- **Exchange rates**: Frankfurter.app (EUR→GBP daily back to 2023-06-01)

## Getting Started

### 1. Install dependencies

```bash
uv sync
source .venv/bin/activate
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in:

```bash
cp .env.example .env
```

Required variables:

| Variable | Description |
|---|---|
| `ZYTE_API_KEY` | Zyte account key |
| `MINIO_ENDPOINT` | e.g. `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `MINIO_BUCKET` | Target bucket name |
| `SQLITE_PATH_DE` | Path to DE SQLite DB |
| `SQLITE_PATH_UK` | Path to UK SQLite DB |
| `SQLITE_PATH_US` | Path to US SQLite DB |
| `CURRENCY_RATES_DB` | Path to exchange rates DB |

### 3. Start Dagster

```bash
dg dev --host 0.0.0.0
```

Open http://localhost:3000 in your browser.

## Data Model

### SQLite — `fact_events`
| Column | Type | Description |
|---|---|---|
| `card_id` | TEXT | Normalized card identifier |
| `card_version` | TEXT | Set/version (e.g. OP01, ST14) |
| `event_type` | TEXT | Always `sale` |
| `price` | REAL | Sale price |
| `currency` | TEXT | EUR / GBP / USD |
| `sold_date` | TEXT | Sale date (YYYY-MM-DD) |
| `scraped_from` | TEXT | Always `ebay` |
| `source` | TEXT | Region: DE / UK / US |
| `source_url` | TEXT | eBay item URL |
| `language` | TEXT | EN / JP |
| `scraped_at` | TIMESTAMP | When the record was scraped |
| `image_url` | TEXT | Original eBay image URL |
| `local_image_path` | TEXT | MinIO path: `sold_images/{region}/{item_id}.jpg` |

### SQLite — `cardlist_dimension`
| Column | Type | Description |
|---|---|---|
| `card_id` | TEXT | Normalized card identifier |
| `card_version` | TEXT | Set/version |
| `card_name` | TEXT | Card name |
| `set_code` | TEXT | Set code |
| `rarity` | TEXT | Rarity |
| `card_type` | TEXT | Character / Event / Stage |
| `attribute` | TEXT | STR / DEX / QCK etc. |
| `power` | INTEGER | Power stat |
| `cost` | INTEGER | Cost stat |
| `color` | TEXT | Color |
| `source_url` | TEXT | Source card page URL |
| `scraped_at` | TIMESTAMP | When scraped |

### SQLite — `exchange_rates`
| Column | Type | Description |
|---|---|---|
| `base` | TEXT | Always `EUR` |
| `quote` | TEXT | Always `GBP` |
| `rate` | REAL | EUR→GBP rate |
| `timestamp` | TEXT | Date: `YYYY-MM-DD 00:00:00` |

## Assets

| Asset | Description |
|---|---|
| `exchange_rates` | Backfills EUR→GBP from Frankfurter.app |
| `ebay_de_sold_listings` | Scrapes new DE eBay sold listings via Zyte |
| `ebay_uk_sold_listings` | Scrapes new UK eBay sold listings via Zyte |
| `ebay_us_sold_listings` | Scrapes new US eBay sold listings via Zyte |
| `bronze_ebay_de_sqlite_writer` | Writes DE records to SQLite |
| `bronze_ebay_uk_sqlite_writer` | Writes UK records to SQLite |
| `bronze_ebay_us_sqlite_writer` | Writes US records to SQLite |
| `bronze_cardlist_parquet` | Exports cardlist to Parquet |
| `bronze_fact_events_parquet` | Exports fact_events to Parquet |
| `limitless_op_cards` | Scrapes Limitless TCG card data |
| `limitless_op_prices` | Scrapes Limitless TCG price data |
| `bronze_sqlite_writer` | Writes Limitless card data to SQLite |

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Zyte API](https://www.zyte.com/)
- [Frankfurter.app](https://www.frankfurter.app/)
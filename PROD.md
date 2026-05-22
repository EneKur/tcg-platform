# TCG Platform — Production Design

## Overview

Dagster-powered ETL platform for One Piece TCG trading card data.
Three data sources scraped via Steel.cloud anti-ban browser API.

## Architecture

```
[data sources]     →  [bronze layer]       →  [silver layer]       →  [gold layer]
Steel cloud APIs       Parquet (MinIO)          LakeSail (local Spark)    (future)
                        SQLite
```

**Bronze layer:** Raw scraped data — unchanged from source, stored as parquet files in MinIO + tabular entries in SQLite. Idempotent (re-running produces same output, no duplicates).

**Silver layer:** LakeSail-based transformations (deduplication, enrichment, type casting) — running locally as a Spark-alternative for fast, efficient in-process lakehouse processing.

## Data Sources

| Website | URL | Purpose |
|---------|-----|---------|
| eBay DE | `https://www.ebay.de/sch/i.html?_nkw=One+Piece+TCG+PSA+10&_sacat=0&_from=R40&_rt=nc&LH_Sold=1` | Sold listings with prices |
| PriceCharting | `https://www.pricecharting.com/category/one-piece-cards` | Card price reference |
| Limitless TCG | `https://onepiece.limitlesstcg.com/cards` | Card catalog / images |

## Data Model

### cardlist_dimension (sqlite + parquet)
| Field | Type | Description |
|-------|------|-------------|
| card_id | TEXT PK | Unique card identifier (e.g., OP-001) |
| card_version | TEXT | Version variant (e.g., BaseArt, AltArt, v1, v2). NULL if unversioned. |
| card_name | TEXT | Display name |
| set_code | TEXT | Set abbreviation |
| rarity | TEXT | Rarity等级 |
| card_type | TEXT | Character/Event/Stage |
| attribute | TEXT | Element/attribute |
| power | INT | Power stat (if applicable) |
| cost | INT | Cost to play |
| color | TEXT | Card color |
| source_url | TEXT | Which website scraped |
| scraped_at | TIMESTAMP | When first seen |

### fact_events (sqlite + parquet)
| Field | Type | Description |
|-------|------|-------------|
| event_id | TEXT PK | UUID |
| card_id | TEXT FK | Reference to cardlist_dimension |
| card_version | TEXT | Version variant (e.g., BaseArt, AltArt). NULL if unversioned. |
| event_type | TEXT | sale / price_update |
| price | REAL | Price in EUR |
| currency | TEXT | EUR/USD/etc |
| sold_date | DATE | Date sold (eBay) |
| scraped_from | TEXT | Which website scraped (pricecharting / ebay) |
| source | TEXT | Sub-source: US / Germany (PriceCharting), ebay (eBay) |
| source_url | TEXT | Which website |
| scraped_at | TIMESTAMP | When scraped |

## Milestones & Tasks

### Milestone 1: Anti-Ban Infrastructure (Steel.dev → Zyte)
- [x] ~~**M1-T1**~~ — Set up Steel.dev Python SDK dependency + env var (`STEEL_API_KEY`)
- [x] ~~**M1-T2**~~ — Create `steel_session` Dagster resource (session create/release, retry 3×)
- [x] ~~**M1-T3**~~ — Implement auth/profile loading from `auth/profile_{sitename}.json`
- [x] ~~**M1-T4**~~ — Session reuse via Steel Profiles API (persist cookies on close, inject on reopen)
- [x] ~~**M1-T5**~~ — ~~CAPTCHA auto-solve~~ → **CANCELLED** (Steel free tier doesn't include CAPTCHA solving; sites scraped are account-free)
- [x] ~~**M1-T6**~~ — Wire SteelSession into Dagster defs
- [x] **M1-T7** — Anti-ban connector research → **REPLACED Steel with Zyte API** (Steel sessions broken in Podman; Zyte works for eBay DE sold listings, free tier available)

### Milestone 2: Bronze Scraping Layer (per website)
> eBay M6 unblocked — Zyte API works for eBay DE sold listings (public data, no auth needed)

- [x] ~~**M2-T1**~~ — Scraping pipeline: PriceCharting (card catalog + prices → fact_events, event_type='price_update', scraped_from='pricecharting', source='US'/'Germany')
- [x] ~~**M2-T2**~~ — Scraping pipeline: Limitless TCG (card catalog → cardlist_dimension + fact_events)
- [x] ~~**M2-T3**~~ — Image download asset (Limitless TCG images → MinIO, path: `cards/{set_code}/{card_id}.webp`)
- [ ] **M2-T4** — Create `log/M2-T1.md` through `log/M2-T3.md` with progress docs

### Milestone 3: MinIO Integration (M3)
- [x] ~~**M3-T1**~~ — MinIO resource (connection, bucket creation)
- [ ] **M3-T2** — Bronze parquet writer asset (cardlist + fact_events → MinIO)
- [x] ~~**M3-T3**~~ — Create `log/M3-T1.md` through `log/M3-T2.md`

### Milestone 4: SQLite Integration (M4)
- [ ] **M4-T1** — SQLite resource (connection management)
- [ ] **M4-T2** — Bronze SQLite writer asset (cardlist + fact_events → SQLite)
- [ ] **M4-T3** — Idempotency enforcement (upsert logic, source_url + scraped_at primary key)
- [ ] **M4-T4** — Create `log/M4-T1.md` through `log/M4-T3.md`

### Milestone 5: Dagster Definitions (M5)
> Partial completion — resources wired, one asset scaffolded, but no persistence (MinIO/SQLite) or schedules yet

- [ ] **M5-T1** — Wire all resources and assets into `defs/` folder
- [ ] **M5-T2** — Schedule: daily full refresh + hourly incremental (new cards only)
- [ ] **M5-T3** — Create `log/M5-T1.md` through `log/M5-T2.md`

### Milestone 6: eBay Scraping via Zyte API (M6)
> **UNBLOCKED:** Zyte API confirmed working — eBay DE sold listings return 200 with full HTML, 62 listings parsed successfully. Zyte free tier sufficient for development.

- [ ] **M6-T1** — Create `ZyteSessionResource` for eBay (uses Zyte API instead of Steel)
- [ ] **M6-T2** — Scraping pipeline: eBay DE (PSA grades 1-10 → fact_events, event_type='sale', scraped_from='ebay', source='ebay')
- [ ] **M6-T3** — Create `log/M6-T1.md` through `log/M6-T2.md`

### Milestone 7: Silver Layer — Lakehouse Processing (M7)
> **Future milestone:** When bronze data is ready for transformation (bronze → silver → gold), use [LakeSail](https://github.com/lakesail/lakesail) as a local Spark-alternative for efficient in-process data processing.

- [ ] **M7-T1** — Evaluate and integrate LakeSail as Spark replacement for local lakehouse processing
- [ ] **M7-T2** — Define silver layer transformations (deduplication, enrichment, type casting)
- [ ] **M7-T3** — Create `log/M7-T1.md` through `log/M7-T2.md`

---

## Auth Folder Structure

```
auth/
  profile_ebay.json        # cookies + localStorage for eBay
  profile_pricecharting.json
  profile_limitlesstcg.json
```

## Dependencies

```
dagster==1.13.3
dagster-dg-cli
dagster-webserver
steel-sdk>=0.17.0       # steel.dev scrape() API for static HTML (PriceCharting)
zyte-api>=0.5.0         # Zyte API for browser-rendered scraping (eBay DE)
playwright>=1.40.0     # Playwright for JS-heavy sites (Limitless TCG)
minio>=7.0.0           # S3-compatible object store client
beautifulsoup4         # HTML parsing
lxml                   # HTML parser (faster than built-in)
pydantic               # schema validation
python-dotenv          # .env file loading for API keys
```

> **Future:** LakeSail (local Spark-alternative) for silver layer processing — TBD when M7 is reached.

> **Note:** MinIO and SQLite integration (M3/M4) must be built before M2 scraping pipelines can write data. Order: M2 → M3 → M4 for full bronze layer completion.

## Environment Variables

```
STEEL_API_KEY=<your-key>              # Steel.dev (static HTML scraping)
ZYTE_API_KEY=<your-key>               # Zyte API (browser-rendered scraping)
MINIO_ENDPOINT=<localhost:9000>
MINIO_ACCESS_KEY=<minioadmin>
MINIO_SECRET_KEY=<minioadmin>
MINIO_BUCKET=tcg-bronze
SQLITE_PATH=./data/tcg.db
```

## Conventions

- **Unique task IDs** are sequential (M1-T1, M1-T2, ...) starting from M1 for milestone 1
- **Log files** map 1:1 to task IDs: `log/M1-T1.md`, `log/M1-T2.md`, etc.
- **Idempotency**: bronze assets check `(source_url, card_id, scraped_at_date)` before insert
- **No schema changes in bronze layer** — raw field names, raw types, raw values
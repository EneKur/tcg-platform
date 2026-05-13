# TCG Platform — Production Design

## Overview

Dagster-powered ETL platform for One Piece TCG trading card data.
Three data sources scraped via Steel.cloud anti-ban browser API.

## Architecture

```
[data sources]     →  [bronze layer]       →  [silver layer]  →  [gold layer]
Steel cloud APIs       Parquet (MinIO)          (future)          (future)
                       SQLite
```

**Bronze layer:** Raw scraped data — unchanged from source, stored as parquet files in MinIO + tabular entries in SQLite. Idempotent (re-running produces same output, no duplicates).

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

### Milestone 1: Anti-Ban Infrastructure (Steel.dev)
- [ ] **M1-T1** — Set up Steel.dev Python SDK dependency + env var (`STEEL_API_KEY`)
- [ ] **M1-T2** — Create `steel_session` Dagster resource (session create/release, retry 3×)
- [ ] **M1-T3** — Implement auth/profile loading from `auth/profile_{sitename}.json`
- [ ] **M1-T4** — Session reuse via Steel Profiles API (persist cookies on close, inject on reopen)
- [ ] ~~**M1-T5** — CAPTCHA auto-solve + retry on detection~~ → **CANCELLED** (Steel free tier doesn't include CAPTCHA solving; sites scraped are account-free)
- [ ] **M1-T6** — Create `log/M1-T1.md` through `log/M1-T5.md` with progress docs

### Milestone 2: Bronze Scraping Layer (per website)
> eBay deferred to M3 (requires browser session via WebSocket — currently blocked by connectivity issue)

- [ ] **M2-T1** — Scraping pipeline: PriceCharting (card catalog + prices → fact_events, event_type='price_update', scraped_from='pricecharting', source='US'/'Germany')
- [ ] **M2-T2** — Scraping pipeline: Limitless TCG (card catalog → cardlist_dimension)
- [ ] **M2-T3** — Image download asset (Limitless TCG images → MinIO, path: `cards/{set_code}/{card_id}.jpg`)
- [ ] **M2-T4** — Create `log/M2-T1.md` through `log/M2-T3.md` with progress docs

### Milestone 3: MinIO Integration (M3)
- [ ] **M3-T1** — MinIO resource (connection, bucket creation)
- [ ] **M3-T2** — Bronze parquet writer asset (cardlist + fact_events → MinIO)
- [ ] **M3-T3** — Create `log/M3-T1.md` through `log/M3-T2.md`

### Milestone 4: SQLite Integration (M4)
- [ ] **M4-T1** — SQLite resource (connection management)
- [ ] **M4-T2** — Bronze SQLite writer asset (cardlist + fact_events → SQLite)
- [ ] **M4-T3** — Idempotency enforcement (upsert logic, source_url + scraped_at primary key)
- [ ] **M4-T4** — Create `log/M4-T1.md` through `log/M4-T3.md`

### Milestone 5: Dagster Definitions (M5)
- [ ] **M5-T1** — Wire all resources and assets into `defs/` folder
- [ ] **M5-T2** — Schedule: daily full refresh + hourly incremental (new cards only)
- [ ] **M5-T3** — Create `log/M5-T1.md` through `log/M5-T2.md`

### Milestone 6: eBay Scraping via Browser Session (M6)
- [ ] **M6-T1** — Resolve WebSocket connectivity to `wss://connect.steel.dev`
- [ ] **M6-T2** — Scraping pipeline: eBay DE (PSA grades 1-10 → fact_events, event_type='sale', scraped_from='ebay', source='ebay')
- [ ] **M6-T3** — Create `log/M6-T1.md` through `log/M6-T2.md`

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
steel-sdk>=0.17.0       # steel.dev browser API (Python SDK, not Node.js puppeteer-core)
playwright>=1.40.0     # Python browser automation via CDP (connects to Steel cloud browser)
beautifulsoup4         # HTML parsing
lxml                   # HTML parser (faster than built-in)
pydantic               # schema validation
minio                  # S3-compatible object store client
python-dotenv          # .env file loading for STEEL_API_KEY
```

> **Note:** MinIO and SQLite integration (M3/M4) must be built before M2 scraping pipelines can write data. Order: M2 → M3 → M4 for full bronze layer completion.

## Environment Variables

```
STEEL_API_KEY=<your-key>
MINIO_ENDPOINT=<localhost:9000>
MINIO_ACCESS_KEY=<key>
MINIO_SECRET_KEY=<secret>
MINIO_BUCKET=tcg-bronze
SQLITE_PATH=./data/tcg.db
```

## Conventions

- **Unique task IDs** are sequential (M1-T1, M1-T2, ...) starting from M1 for milestone 1
- **Log files** map 1:1 to task IDs: `log/M1-T1.md`, `log/M1-T2.md`, etc.
- **Idempotency**: bronze assets check `(source_url, card_id, scraped_at_date)` before insert
- **No schema changes in bronze layer** — raw field names, raw types, raw values
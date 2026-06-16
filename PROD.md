# TCG Platform — Production Design

## Overview

Dagster-powered ETL platform for One Piece TCG trading card data.
Three data sources scraped via Steel.cloud anti-ban browser API.

## Architecture

```
[data sources]     →  [bronze layer]       →  [silver layer]       →  [gold layer]
Steel cloud APIs       MinIO Parquet            LakeSail (pysail)           (future)
                        SQLite                   Spark Connect
                        eBay sold listings       sc://localhost:{port}
```

**Raw layer (`tcg-raw`):** Bytes only — HTML, images, scrape logs. One object per eBay item, named by `event_id`. Write-once.

**Bronze layer:** Structured, parsed views of the source data, stored as parquet files in MinIO + tabular entries in SQLite. Every bronze row is **derivable from `tcg-raw`** by replaying the transformer — bronze is a cache, not a source of truth.

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
- [x] ~~**M2-T4**~~ — Create `log/M2-T1.md` through `log/M2-T3.md` with progress docs

### Milestone 3: MinIO Integration (M3)
- [x] ~~**M3-T1**~~ — MinIO resource (connection, bucket creation)
- [x] ~~**M3-T2**~~ — Bronze parquet writer asset — **DONE** (inline per-item parquet in eBay scrape loop: `sold_data/DE|UK/{item_id}.parquet`). Limitless TCG parquet (bronze_cardlist_parquet, bronze_fact_events_parquet) deferred to **M7 silver layer** — data first needs cleaning/structuring before partitioning.
- [x] ~~**M3-T3**~~ — Create `log/M3-T1.md` through `log/M3-T2.md`

### Milestone 4: SQLite Integration (M4)
> **COMPLETE / OBSOLETE:** eBay item IDs are unique per sold listing — a sold item event is final on eBay side and cannot be re-sold. The current `(card_id, source_url)` primary key in SQLite is sufficient for idempotency. No further work needed.

- [x] ~~**M4-T1**~~ — SQLite resource (connection management) — `SqliteClientResource` in place
- [x] ~~**M4-T2**~~ — Bronze SQLite writer asset — `bronze_ebay_de_sqlite_writer` + `bronze_ebay_uk_sqlite_writer` in place
- [x] ~~**M4-T3**~~ — Idempotency — unnecessary; eBay item IDs are unique per sold event
- [x] ~~**M4-T4**~~ — Create log docs — not needed

### Milestone 5: Dagster Definitions (M5)
> **COMPLETE (scheduling DEFERRED):** Resources and assets wired. Scheduling (daily/hourly pipelines) deferred — not needed until project validated and frequent sniffing required for timely arbitrage reactions.

- [x] ~~**M5-T1**~~ — Wire all resources and assets into `defs/` folder
- [x] ~~**M5-T2**~~ — **DEFERRED** — Schedule: daily full refresh + hourly incremental (new cards only)
- [x] ~~**M5-T3**~~ — **DEFERRED** — Create `log/M5-T1.md` through `log/M5-T2.md`

### Milestone 6: eBay Scraping via Zyte API (M6)
> **COMPLETE:** Zyte API working for eBay DE + UK sold listings. Inline per-item parquet write per `2026-05-27-sold-data-parquet-design.md`. Backfill sensor + job also in place.

- [x] ~~**M6-T1**~~ — Create `ZyteSessionResource` for eBay (uses Zyte API instead of Steel)
- [x] ~~**M6-T2**~~ — Scraping pipeline: eBay DE + UK (PSA grades 1-10 → fact_events → SQLite + MinIO parquet inline) — **SUPERSEDED by M6.5-T1**
- [x] ~~**M6-T3**~~ — Create `log/M6-T1.md` through `log/M6-T2.md`

### Milestone 6.5: eBay Scraper Redesign — Per-Region Parsers (M6.5)
> **M6.5 COMPLETE:** Replaced the shared `ebay.py` parser with region-specific DE + UK modules. Sold date is now extracted from the search-page green "Sold D Mon YYYY" (UK) / "Verkauft D. Mon YYYY" (DE) text — was 99% null on UK rows before. Non-card listings (bundles, DON cards) are now filtered at parse time.

- [x] **M6.5-T1** — Region-specific DE + UK parsers (no shared logic):
  - `ebay_de_search.py` / `ebay_uk_search.py` — search-page parsers (URL + date)
  - `ebay_de_item.py` / `ebay_uk_item.py` — item-page parsers (price, currency, title, image)
  - `ebay_utils.py` — shared `extract_item_id` and `extract_item_image_url` utilities
  - `src/tcg_platform/scraping/ebay.py` — DELETED
  - Dead US-scrape scripts (`scripts/scrape_uk*.py`, `verify_lang_proxy*.py`) — DELETED
  - New URLs: TCG category (`_dcat=183454`), English-only, UK/DE local, sold only, sort by newest
- [x] **M6.5-T2** — Skip listings without a recognizable card_id (filter at parse time so bundles/DON cards never reach bronze, SQLite, or silver/quarantine)

### Milestone 7: Silver Layer — Lakehouse Processing (M7)
> **M7 COMPLETE:** LakeSail (pysail) integrated as local Spark-alternative via Spark Connect. PyArrow `from_pandas()` for DataFrame→Parquet conversion. P card normalization added (`P\d+` pattern, `P-XXX` format). PySpark 4.1+ via Spark Connect (`sc://localhost:{port}`). PyArrow fs for MinIO read/write (no Hadoop jars). Per-item-id silver parquet layout (one file per `event_id`, collision check via `(sold_date, event_id, title)`).

- [x] **M7-T1** — Evaluate and integrate LakeSail as Spark replacement for local lakehouse processing
- [x] **M7-T2** — Wire silver DE/UK/EU pipelines (valid card_ids → `tcg-silver/data/{region}/{event_id}.parquet`, invalid → `tcg-silver/quarantine/{region}/{event_id}.parquet`; `event_id` = eBay item_id, one file per item, collision check via `(sold_date, event_id, title)` tuple; see `log/M7-T2-update.md` for the per-item-id refactor)
- [x] **M7-T3** — Define silver layer transformations (card_id normalization, `title` field capture, quarantine logic)
- [x] **M7-T4** — Logs filed: `log/M7-T1.md` (LakeSail + initial transform), `log/M7-T2-update.md` (per-item-id refactor + collision check + live-data fixes), `log/M7-T3.md` (consolidated transformation definition). Note: `log/2026-06-02-M7-T2.md` is mislabeled — its content is the EU Pipeline Orchestrator, not M7-T2 silver wiring.

### Milestone 8: Limitless Card Image Sync + Quarantine Reconciliation (M8)
> **M8 COMPLETE:** Keeps `tcg-bronze/cards/` in sync with the Limitless TCG catalog (monthly manual run from the Dagster UI) and gives silver/quarantine rows a path to be promoted once their `card_id` becomes known. Quarantine reconciliation runs automatically as the first step of `complete_eu_pipeline`.

- [x] **M8-T1** — `sync_card_images_job` (Dagster job; `sync_card_images_discover` + `sync_card_images_download` assets). Derives each card's *home* set from its `card_id` (not the scraped set) and filters set-name alias slugs (`op16-the-time-of-battle`, `st30-ex-luffy-ace`) that would otherwise yield CDN 403s. Variants written as `cards/{set}/{card_id}__v{N}.webp`; base cards as `cards/{set}/{card_id}.webp`. Spec: `docs/superpowers/specs/2026-06-07-limitless-card-image-sync-design.md`.
- [x] **M8-T2** — Promo sub-pages walk. Extended `discover_limitless_catalog` to walk `/cards/promos` (~77 sub-pages: tournament packs, event packs, regional packs, championship packs, dash packs, gift collections, misc). Mixed-set cards that don't appear on standard set pages now get picked up. New pure helper `extract_promo_subpages(html)`.
- [x] **M8-T3** — Silver quarantine reconciliation. `reconcile_quarantine_de/uk` assets + jobs, wired into `silver_eu_orchestrator` before the silver calls. For each parquet in `tcg-silver/quarantine/{region}/`, re-validates the `card_id` against the **current** `tcg-bronze/cards/` set (loaded fresh on every call); if valid, batch-deletes the quarantine file via `MinioClientResource.remove_objects()`. The next silver run re-reads the immutable bronze parquet and lands the promoted row in `tcg-silver/data/{region}/{event_id}.parquet` via the writer's normal collision-check path. No SQLite writes, no bronze mutations — the `parqueted` column is a "bronze parquet exists" signal, not a "row is in silver" signal. Spec: `docs/superpowers/specs/2026-06-09-quarantine-reconciliation-design.md`.
- [x] **M8-T4** — Logs filed: `log/SESSION_2026-06-07.md` (M8-T1), `log/SESSION_2026-06-08.md` (M8-T1 CDN 403 fix), `log/SESSION_2026-06-08-m8-t2.md` (M8-T2 promo walk), `log/SESSION_2026-06-09.md` (M8-T3 quarantine reconciliation, PR #15 merged).

### Outstanding (post-M8)
- **M8-T5 — Cardlist parquet writer for Limitless** — `bronze_cardlist_parquet` and `bronze_fact_events_parquet` assets were deferred from M3-T2 (data needed cleaning/structuring first; M7's `silver_*_transform` did the cleaning but the writer was never built). Outstanding since 2026-05-28. **2026-06-10 partial close:** added 14 tests pinning the serializer's current schema (`tests/serialization/test_card_parquet.py`). Source untouched, no new job, dormant assets remain dormant. See `log/M8-T5.md`.
- **M8-T6 — Silver `is_valid_card_id` path bug** — flagged in 2026-06-07 session log; not root-caused. Reviewed on 2026-06-10 and removed from scope: no failing test reproduces it, and the only session-log reference is the single phrase "path bug" with no symptom description. Treat as stale unless a real failure surfaces.
- **M8-T7 — DE/UK factory refactor** — `reconcile_quarantine.py` (added in M8-T3) duplicates DE/UK across `_reconcile_region`, `reconcile_quarantine_de`, `reconcile_quarantine_uk`, mirroring the same duplication in `silver_transform.py` for `silver_de_transform` / `silver_uk_transform`. Candidate for a single factory-pattern refactor that converts all three pairs at once. Per M8-T3 spec: "do it as a single follow-up PR, don't fork patterns." **2026-06-10 complete:** added `make_reconcile_asset(region)` and `make_silver_asset(region)` factories; replaced the four hand-written asset bodies. Module-level identifiers preserved via `dg.asset(name=...)`, so `definitions.py` is unchanged. Net ~54 lines saved across the two files; 132 tests still passing. The eBay DE/UK scraper pair was de-scoped (different parser modules, not a clean factory candidate). See `log/M8-T7.md`.
- **14 `failed_card_ids` from `sync_card_images`** — CDN gaps on the Limitless side; no fix in this codebase. Outstanding.
- **M5-T2 deferred work** — Dagster schedules (daily full refresh + hourly incremental) never implemented. Still deferred.

### Milestone 9: Persistent Raw Layer (M9)
> **M9-T1 COMPLETE:** `tcg-raw` MinIO bucket holds raw HTML + images + per-run logs. Replay the transformer against `tcg-raw` to fix a parser without re-paying Zyte API costs. See `docs/superpowers/specs/2026-06-11-tcg-raw-layer-design.md` and `log/SESSION_2026-06-11.md`.

- [x] **M9-T1** — tcg-raw bucket, scraper split into network-only + offline transformer, one-time backfill for pre-existing rows.

#### Operational notes

- **MinIO clock skew will break the pipeline with `RequestTimeTooSkewed`.** Podman containers drift when the host sleeps/resumes. The S3 SDK rejects requests where local/server skew > ~15 min, and the failure is loud only at the resource init step (no pre-flight check). Run `bash scripts/check_minio_clock.sh` before launching `complete_eu_pipeline`; it's also wired into `pytest` as `tests/test_minio_clock_skew.py` (FAIL fails the suite, WARN emits a warning, SKIP if MinIO is unreachable).
- **A hung Zyte request will block the scraper forever** (no per-call timeout in the Zyte SDK by default). Symptom: the `scrape_ebay_*_raw` step shows no log output for tens of minutes; `lsof -p <pid>` shows an `ESTABLISHED` TCP to `69.41.180.81:443`. Fixed in PR #20 + #23 + the cross-loop fix (2026-06-16): `ZyteSessionResource` lets the Zyte SDK create its own short-lived `aiohttp.ClientSession` per call (no pre-created session — that triggered the aiohttp 3.13 cross-loop bug) AND applies a hard Python-level `future.result(timeout=ZYTE_API_TIMEOUT)` on every Zyte call (default **60s**, override via env). The single key is managed externally; the scraper wraps each Zyte call in a `try/except` so a hung connection raises `ZyteTimeoutError`, the scraper logs `STOP ... exc=ZyteTimeoutError` and continues — it does **not** crash the whole pipeline.

---

## Auth Folder Structure

```
auth/
  profile_ebay.json        # cookies + localStorage for eBay
  profile_pricecharting.json
  profile_limitlesstcg.json
```

> **Note:** MinIO and SQLite integration (M3/M4) must be built before M2 scraping pipelines can write data. Order: M2 → M3 → M4 for full bronze layer completion.

## Dependencies

```
dagster==1.13.3
dagster-dg-cli
dagster-webserver
steel-sdk>=0.17.0       # steel.dev scrape() API for static HTML (PriceCharting)
zyte-api>=0.5.0         # Zyte API for browser-rendered scraping (eBay DE)
playwright>=1.40.0      # Playwright for JS-heavy sites (Limitless TCG)
minio>=7.0.0            # S3-compatible object store client
beautifulsoup4          # HTML parsing
lxml                    # HTML parser (faster than built-in)
pydantic                # schema validation
python-dotenv           # .env file loading for API keys
pysail>=0.1.0           # LakeSail — local Spark-alternative via Spark Connect
pyspark>=4.1.0          # PySpark with Spark Connect support
pyarrow                 # Parquet read/write via PyArrow (not Hadoop)
pandas                  # DataFrame operations
zstandard               # Zstandard compression for SQLite
duckdb                  # SQL queries on silver layer parquet files
grpcio-status           # gRPC support for Spark Connect
```

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

## MinIO Buckets

| Bucket | Contents |
|--------|----------|
| `tcg-raw` | Persistent raw scrape bytes: `ebay/{DE\|UK}/{event_id}.html`, `sold_images/{DE\|UK}/{event_id}.jpg`, `logs/{ts}.log` |
| `tcg-bronze` | Parsed structured data: `sold_data/{DE\|UK}/` parquets, `cards/{set}/` images — derivable from `tcg-raw` |
| `tcg-silver` | Validated records: `data/{de\|uk}/`, quarantined: `quarantine/{de\|uk}/` |

## Conventions

- **Unique task IDs** are sequential (M1-T1, M1-T2, ...) starting from M1 for milestone 1
- **Log files** map 1:1 to task IDs: `log/M1-T1.md`, `log/M1-T2.md`, etc.
- **Idempotency**: bronze assets check `(source_url, card_id, scraped_at_date)` before insert
- **No schema changes in bronze layer** — raw field names, raw types, raw values
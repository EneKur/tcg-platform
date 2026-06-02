# Complete EU Pipeline Orchestrator Design

**Date:** 2026-06-02
**Status:** Approved

## Overview

Wrap the three-stage EU pipeline (bronze → backfill → silver) into a single orchestrator asset that runs sequentially, with parallel execution where applicable.

## Architecture

```
bronze_eu_orchestrator
        │
        ├── backfill_de_asset  ──┐
        │                        ├── silver_eu_orchestrator
        └── backfill_uk_asset  ──┘
```

- `bronze_eu_orchestrator` — triggers DE + UK eBay scrape + write to SQLite
- `backfill_de_asset` + `backfill_uk_asset` — run in parallel after bronze completes
  - If DE fails, UK continues (independent markets)
  - If UK fails, DE continues
- `silver_eu_orchestrator` — waits for **both** backfills to succeed before running
  - Reads from MinIO, writes to silver layer
  - Only runs if both backfills complete successfully

## Constraints

1. Existing jobs (`bronze_eu_pipeline`, `backfill_de_job`, `backfill_uk_job`, `silver_eu_job`) remain independently runnable in Dagster UI
2. The orchestrator is an additional layer on top — not a replacement
3. `backfill_de` and `backfill_uk` run in parallel (independent markets)
4. `silver_eu` waits for both backfills to complete successfully

## Implementation

### New assets

- `bronze_eu_orchestrator` — wraps `ebay_eu_pipeline` execution
- `backfill_de_asset` — deps on `bronze_eu_orchestrator`, wraps `backfill_de_sold_data_parquet`
- `backfill_uk_asset` — deps on `bronze_eu_orchestrator`, wraps `backfill_uk_sold_data_parquet`
- `silver_eu_orchestrator` — deps on `backfill_de_asset` + `backfill_uk_asset`, wraps `silver_eu_transform`

### New job

- `complete_eu_pipeline` job selects all four orchestrator assets
- DAG enforces: bronze → (de || uk) → silver

### Existing jobs unchanged

All current jobs remain in `definitions.py` and stay independently runnable.

## File changes

- `src/tcg_platform/defs/eu_pipeline_orchestrator.py` — new file with four orchestrator assets
- `src/tcg_platform/definitions.py` — add `complete_eu_pipeline` job
- `log/2026-06-02-M7-T2.md` — task log

## Success criteria

- `complete_eu_pipeline` job runs end-to-end (bronze → backfill → silver) in Dagster UI
- Both backfills start in parallel after bronze completes
- Silver starts only after both backfills succeed
- All four existing jobs still run independently
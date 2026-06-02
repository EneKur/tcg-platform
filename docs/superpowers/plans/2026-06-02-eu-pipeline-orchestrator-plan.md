# Complete EU Pipeline Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap bronze → backfill → silver into a single orchestrator asset graph, with parallel backfills and all existing jobs remaining independently runnable.

**Architecture:** Four new orchestrator assets (`bronze_eu_orchestrator`, `backfill_de_asset`, `backfill_uk_asset`, `silver_eu_orchestrator`) wired with deps. A single `complete_eu_pipeline` job selects all four. Existing jobs unchanged.

**Tech Stack:** Dagster 1.13.3, asset-based orchestration, `define_asset_job`

---

## File Structure

- Create: `src/tcg_platform/defs/eu_pipeline_orchestrator.py` — four orchestrator assets
- Modify: `src/tcg_platform/definitions.py` — add `complete_eu_pipeline` job and register it

---

## Task 1: Create eu_pipeline_orchestrator.py

**Files:**
- Create: `src/tcg_platform/defs/eu_pipeline_orchestrator.py`
- Test: `tests/defs/test_eu_pipeline_orchestrator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/defs/test_eu_pipeline_orchestrator.py
import pytest
from dagster import AssetKey


def test_bronze_eu_orchestrator_asset_exists():
    from tcg_platform.defs.eu_pipeline_orchestrator import bronze_eu_orchestrator
    assert bronze_eu_orchestrator is not None


def test_backfill_de_asset_depends_on_bronze():
    from dagster import AssetDep
    from tcg_platform.defs.eu_pipeline_orchestrator import backfill_de_asset

    deps = backfill_de_asset.dependency_keys
    assert AssetKey("bronze_eu_orchestrator") in deps


def test_backfill_uk_asset_depends_on_bronze():
    from dagster import AssetKey
    from tcg_platform.defs.eu_pipeline_orchestrator import backfill_uk_asset

    deps = backfill_uk_asset.dependency_keys
    assert AssetKey("bronze_eu_orchestrator") in deps


def test_silver_eu_orchestrator_depends_on_both_backfills():
    from dagster import AssetKey
    from tcg_platform.defs.eu_pipeline_orchestrator import silver_eu_orchestrator

    deps = silver_eu_orchestrator.dependency_keys
    assert AssetKey("backfill_de_asset") in deps
    assert AssetKey("backfill_uk_asset") in deps
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/defs/test_eu_pipeline_orchestrator.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write minimal implementation**

```python
# src/tcg_platform/defs/eu_pipeline_orchestrator.py
import dagster as dg
from dagster import AssetKey


@dg.asset
def bronze_eu_orchestrator(context: dg.AssetExecutionContext):
    """Triggers ebay_eu_pipeline — runs DE + UK scrape in sequence."""
    from tcg_platform.definitions import ebay_eu_job
    context.log.info("Starting bronze_eu_orchestrator")
    result = ebay_eu_job.execute_in_process()
    context.log.info(f"bronze_eu_orchestrator complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("bronze_eu_orchestrator")])
def backfill_de_asset(context: dg.AssetExecutionContext):
    """Backfill DE sold data from SQLite to MinIO. Runs after bronze."""
    context.log.info("Starting backfill_de_asset")
    from tcg_platform.defs.backfill_sold_data_parquet import (
        backfill_de_sold_data_parquet,
    )
    result = backfill_de_sold_data_parquet.backfill_de_job.execute_in_process()
    context.log.info(f"backfill_de_asset complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("bronze_eu_orchestrator")])
def backfill_uk_asset(context: dg.AssetExecutionContext):
    """Backfill UK sold data from SQLite to MinIO. Runs after bronze, in parallel with DE."""
    context.log.info("Starting backfill_uk_asset")
    from tcg_platform.defs.backfill_sold_data_parquet import (
        backfill_uk_sold_data_parquet,
    )
    result = backfill_uk_sold_data_parquet.backfill_uk_job.execute_in_process()
    context.log.info(f"backfill_uk_asset complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("backfill_de_asset"), AssetKey("backfill_uk_asset")])
def silver_eu_orchestrator(context: dg.AssetExecutionContext):
    """Run silver DE + UK transforms. Waits for both backfills to succeed."""
    context.log.info("Starting silver_eu_orchestrator")
    from tcg_platform.definitions import silver_eu_job
    result = silver_eu_job.execute_in_process()
    context.log.info(f"silver_eu_orchestrator complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/defs/test_eu_pipeline_orchestrator.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/eu_pipeline_orchestrator.py tests/defs/test_eu_pipeline_orchestrator.py
git commit -m "feat: add EU pipeline orchestrator assets (M7-T2)"
```

---

## Task 2: Wire complete_eu_pipeline job in definitions.py

**Files:**
- Modify: `src/tcg_platform/definitions.py:44-60`

- [ ] **Step 1: Read current definitions.py to confirm line numbers**

Run: `cat -n src/tcg_platform/definitions.py | head -90`

- [ ] **Step 2: Edit definitions.py to add import and job**

Add after existing imports:
```python
from tcg_platform.defs.eu_pipeline_orchestrator import (
    bronze_eu_orchestrator,
    backfill_de_asset,
    backfill_uk_asset,
    silver_eu_orchestrator,
)
```

Add after `silver_eu_job` definition:
```python
complete_eu_pipeline = define_asset_job(
    name="complete_eu_pipeline",
    selection=["bronze_eu_orchestrator", "backfill_de_asset", "backfill_uk_asset", "silver_eu_orchestrator"],
    description="Full EU pipeline: bronze → backfill (DE+UK parallel) → silver",
)
```

Add `complete_eu_pipeline` to the `jobs=` list in `Definitions(...)`.

- [ ] **Step 3: Verify definitions load**

Run: `python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: OK (no errors)

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/definitions.py
git commit -m "feat: add complete_eu_pipeline job (M7-T2)"
```

---

## Task 3: Verify end-to-end in dagster dev

**Files:**
- No file changes — verification only

- [ ] **Step 1: Run dagster dev**

Run: `cd /Users/enistoteles/tcg_platform && dagster dev`
Expected: Dagster starts without errors, all assets load

- [ ] **Step 2: Verify complete_eu_pipeline appears in UI job list**

Navigate to Dagster UI → Jobs → confirm `complete_eu_pipeline` listed

- [ ] **Step 3: Trigger complete_eu_pipeline manually**

Click `Materialize` on `complete_eu_pipeline` in UI. Confirm:
1. `bronze_eu_orchestrator` runs first
2. `backfill_de_asset` + `backfill_uk_asset` start in parallel
3. `silver_eu_orchestrator` runs only after both backfills succeed

- [ ] **Step 4: Verify existing jobs still run independently**

Navigate to `ebay_eu_pipeline` job → materialize → confirm it runs without needing orchestrator

- [ ] **Step 5: Commit all changes**

```bash
git push origin main
```

---

## Task 4: Create M7-T2 log

**Files:**
- Create: `log/2026-06-02-M7-T2.md`

- [ ] **Step 1: Write log entry**

```markdown
# M7-T2 Log: EU Pipeline Orchestrator

**Date:** 2026-06-02
**Status:** Complete

## Summary

Created `eu_pipeline_orchestrator.py` with four orchestrator assets:
- `bronze_eu_orchestrator` — wraps `ebay_eu_pipeline`
- `backfill_de_asset` — wraps `backfill_de_sold_data_parquet`, deps on bronze
- `backfill_uk_asset` — wraps `backfill_uk_sold_data_parquet`, deps on bronze
- `silver_eu_orchestrator` — deps on both backfills

`complete_eu_pipeline` job selects all four assets. DAG enforces:
bronze → (de || uk) → silver.

## Architecture

```
bronze_eu_orchestrator
        │
        ├── backfill_de_asset ──┐
        │                        ├── silver_eu_orchestrator
        └── backfill_uk_asset ──┘
```

## Files

- `src/tcg_platform/defs/eu_pipeline_orchestrator.py` — new
- `src/tcg_platform/definitions.py` — modified

## Verification

- `python -c "from tcg_platform.definitions import defs; print('OK')"` — OK
- `complete_eu_pipeline` runs end-to-end in Dagster UI
- All existing jobs (`ebay_eu_pipeline`, `backfill_de_job`, `backfill_uk_job`, `silver_eu_job`) remain independently runnable
```

- [ ] **Step 2: Commit log**

```bash
git add log/2026-06-02-M7-T2.md
git commit -m "docs: add M7-T2 log"
```

---

## Verification Checklist

- [ ] All four orchestrator assets exist in `eu_pipeline_orchestrator.py`
- [ ] Dependency graph: bronze → backfill_de, backfill_uk → silver
- [ ] `complete_eu_pipeline` job present in `definitions.py`
- [ ] All existing jobs remain independently runnable
- [ ] `python -c "from tcg_platform.definitions import defs; print('OK')"` passes
- [ ] End-to-end run in Dagster UI succeeds
- [ ] Logs committed
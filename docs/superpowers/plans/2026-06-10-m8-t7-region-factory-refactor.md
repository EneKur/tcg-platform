# M8-T7: DE/UK factory refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace four hand-written asset functions with two factory functions in `reconcile_quarantine.py` and `silver_transform.py`, preserving all module-level identifiers so `definitions.py` needs no change.

**Architecture:** Inner-function pattern with `dg.asset(name=...)` to preserve Dagster's asset keys. Module-level assignment of factory return value matches the existing pattern that `load_from_defs_folder` discovers.

**Tech Stack:** Dagster `@dg.asset`, no new dependencies.

---

## File map

| File | Action | Purpose |
|------|--------|---------|
| `src/tcg_platform/defs/reconcile_quarantine.py` | Modify | Add `make_reconcile_asset` factory; replace two asset functions with factory calls. |
| `src/tcg_platform/defs/silver_transform.py` | Modify | Add `make_silver_asset` factory; replace two asset functions with factory calls. |
| `src/tcg_platform/definitions.py` | **Do not touch** | Verified by post-task load check. |
| `tests/scraping/test_reconcile_quarantine.py` | **Do not touch** | Tests the helper, not the asset. |
| `log/M8-T7.md` | Create | Implementation log. |

---

## Task 1: Refactor `reconcile_quarantine.py`

**Files:**
- Modify: `src/tcg_platform/defs/reconcile_quarantine.py:76-125`

- [ ] **Step 1: Replace the two hand-written asset functions with the factory**

Open `src/tcg_platform/defs/reconcile_quarantine.py` and replace the
entire range from the first `@dg.asset` decorator (line 76) through
the end of `reconcile_quarantine_uk` (line 125) with the following
block. Keep everything before that range (imports, the
`_reconcile_region` helper, the two `define_asset_job` definitions
at the end) unchanged.

```python


def make_reconcile_asset(region: str) -> dg.AssetsDefinition:
    """Build a reconcile asset for a given region ('de' or 'uk')."""
    upper = region.upper()
    lower = region.lower()

    @dg.asset(name=f"reconcile_quarantine_{lower}",
              required_resource_keys={"minio_client"})
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Re-validate tcg-silver/quarantine/{region}/ rows against the current
        tcg-bronze/cards/ set. Deletes files whose card_id is now valid; the
        next silver pipeline run will re-evaluate the corresponding bronze
        rows and write them to tcg-silver/data/{region}/."""
        minio_client: MinioClientResource = context.resources.minio_client
        result = _reconcile_region(minio_client, lower)
        context.log.info(
            f"{upper} reconcile: scanned={result['scanned']} "
            f"promoted={result['promoted_count']} "
            f"still_quarantined={result['still_quarantined_count']} "
            f"read_errors={result['read_errors']}"
        )
        return dg.MaterializeResult(
            metadata={
                "scanned": result["scanned"],
                "promoted_count": result["promoted_count"],
                "still_quarantined_count": result["still_quarantined_count"],
                "read_errors": result["read_errors"],
                "promoted_card_ids": dg.MetadataValue.json(
                    [p["card_id"] for p in result["promoted"]]
                ),
            }
        )
    return _asset


reconcile_quarantine_de = make_reconcile_asset("de")
reconcile_quarantine_uk = make_reconcile_asset("uk")
```

- [ ] **Step 2: Run the reconciler test suite to confirm no regression**

Run: `pytest tests/scraping/test_reconcile_quarantine.py -v`
Expected: 8 passed. The tests target `_reconcile_region` directly,
which is unchanged.

- [ ] **Step 3: Run the full test suite**

Run: `pytest tests/ -v`
Expected: 132 passed (no count change from the M8-T5 baseline).

- [ ] **Step 4: Confirm Dagster definitions still load cleanly**

Run: `python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK`. If this fails, the most likely cause is a typo in
the `name=f"reconcile_quarantine_{lower}"` template — re-read
Task 1 step 1.

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/reconcile_quarantine.py
git commit -m "refactor(reconcile_quarantine): factory for DE/UK asset pair (M8-T7)

Replaces reconcile_quarantine_de and reconcile_quarantine_uk with
make_reconcile_asset(region) factory calls. Module-level identifiers
preserved via dg.asset(name=...) so definitions.py needs no change.
The helper _reconcile_region is unchanged; existing 8 reconciler
tests still pass without modification."
```

---

## Task 2: Refactor `silver_transform.py`

**Files:**
- Modify: `src/tcg_platform/defs/silver_transform.py:447-549`

- [ ] **Step 1: Replace the two hand-written asset functions with the factory**

Open `src/tcg_platform/defs/silver_transform.py` and replace the
entire range from the first `@dg.asset` decorator (line 448) through
the end of `silver_uk_transform` (line 549) with the following
block. Keep everything before that range (imports, helpers, the
`_run_silver_transform` function) unchanged.

```python


def make_silver_asset(region: str) -> dg.AssetsDefinition:
    """Build a silver transform asset for a given region ('de' or 'uk')."""
    upper = region.upper()
    lower = region.lower()

    @dg.asset(name=f"silver_{lower}_transform",
              required_resource_keys={"minio_client"})
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Transform {region} bronze parquets into silver layer.

        Valid card_ids (found in tcg-bronze/cards/) -> tcg-silver/data/{region}/
        Invalid card_ids -> tcg-silver/quarantine/{region}/
        """
        minio_client = context.resources.minio_client

        try:
            active = SparkSession.getActiveSession()
            if active:
                active.stop()
        except Exception:
            pass

        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        addr, port = server.listening_address
        context.log.info(f"SparkConnectServer started at sc://localhost:{port}")

        spark = None
        try:
            spark = (SparkSession.builder
                     .remote(f"sc://localhost:{port}")
                     .appName(f"silver_{lower}_transform")
                     .getOrCreate())
            context.log.info("Connected to Spark")
            result = _run_silver_transform(spark, minio_client, upper)
            context.log.info(f"{upper} transform done: {result}")
        finally:
            if spark:
                try:
                    spark.stop()
                except Exception:
                    pass
            server.stop()

        sample_meta = {}
        if result.get("sample"):
            for i, row in enumerate(result["sample"][:3]):
                for col, val in row.items():
                    sample_meta[f"sample_{i+1}.{col}"] = str(val) if val else ""

        return dg.MaterializeResult(
            metadata={
                "valid_records": result["valid"],
                "quarantined_records": result["quarantine"],
                **sample_meta,
            }
        )
    return _asset


silver_de_transform = make_silver_asset("de")
silver_uk_transform = make_silver_asset("uk")
```

- [ ] **Step 2: Run the full test suite**

Run: `pytest tests/ -v`
Expected: 132 passed. The silver assets have no direct unit tests;
end-to-end behavior is exercised by the production pipeline, not
the unit suite. The test count must not change.

- [ ] **Step 3: Confirm Dagster definitions still load cleanly**

Run: `python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK`. The `silver_de_job`, `silver_uk_job`, and
`complete_eu_pipeline` jobs in `definitions.py` reference the
preserved asset names. If this fails with a "selection unresolved"
error, the most likely cause is a typo in
`name=f"silver_{lower}_transform"` — re-read Task 2 step 1.

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/defs/silver_transform.py
git commit -m "refactor(silver_transform): factory for DE/UK asset pair (M8-T7)

Replaces silver_de_transform and silver_uk_transform with
make_silver_asset(region) factory calls. Module-level identifiers
preserved via dg.asset(name=...) so definitions.py needs no change.
The SparkConnectServer start/stop dance and the _run_silver_transform
call are unchanged; only the asset wrapper is DRY'd up.

Net line savings: ~80 lines (each asset was ~50 lines of
near-identical code)."
```

---

## Task 3: End-of-branch verification + log file

**Files:**
- Create: `log/M8-T7.md`

- [ ] **Step 1: Run the full verification one more time**

Run all three:
```bash
pytest tests/ -v
python -c "from tcg_platform.definitions import defs; print('OK')"
grep -rn "reconcile_quarantine_de\|reconcile_quarantine_uk\|silver_de_transform\|silver_uk_transform" src/
```

Expected:
- 132 tests passing
- `OK` printed
- The grep output must still show the four identifiers
  (`reconcile_quarantine_de`, `reconcile_quarantine_uk`,
  `silver_de_transform`, `silver_uk_transform`) at module scope in
  their respective files. This is what `load_from_defs_folder`
  relies on for `definitions.py` to find them.

- [ ] **Step 2: Get commit SHAs for the log file**

Run: `git log --oneline -5`

- [ ] **Step 3: Write the log file**

Write to `log/M8-T7.md`:

```markdown
# M8-T7 Log: DE/UK factory refactor for `silver_transform` and `reconcile_quarantine`

**Date:** 2026-06-10
**Status:** Complete
**Branch:** `2026-06-10-m8-t7-region-factory-refactor`

## Summary

Replaced four hand-written asset functions with two factory functions
in `reconcile_quarantine.py` and `silver_transform.py`. Module-level
identifiers (`reconcile_quarantine_de`, `reconcile_quarantine_uk`,
`silver_de_transform`, `silver_uk_transform`) preserved via
`dg.asset(name=...)`, so `definitions.py` needs no change.

## What was changed

- **`src/tcg_platform/defs/reconcile_quarantine.py`**: added
  `make_reconcile_asset(region)` factory; replaced
  `reconcile_quarantine_de` and `reconcile_quarantine_uk` function
  definitions with factory calls. Net: ~50 lines saved.
- **`src/tcg_platform/defs/silver_transform.py`**: added
  `make_silver_asset(region)` factory; replaced
  `silver_de_transform` and `silver_uk_transform` function
  definitions with factory calls. Net: ~80 lines saved.

`_reconcile_region`, `_run_silver_transform`, the two
`define_asset_job` definitions, and all imports are unchanged.

## Test counts

132 tests collected, 132 passing. No change from the M8-T5 baseline
(no new tests, no test edits). The existing
`test_reconcile_quarantine.py` (8 tests) covers `_reconcile_region`
directly, which is unchanged.

## Dagster

`python -c "from tcg_platform.definitions import defs; print('OK')"`
→ `OK`. The `silver_de_job`, `silver_uk_job`,
`reconcile_quarantine_de_job`, `reconcile_quarantine_uk_job`, and
`complete_eu_pipeline` jobs in `definitions.py` reference the
preserved asset names; the factory's `name=...` parameter keeps
those references valid.

## Commits

```
<commit from Task 1 step 5>
<commit from Task 2 step 4>
```

Fill in the actual commit SHAs from `git log --oneline` before
committing the log.

## What remains

- The factory *enables* a new region (FR, IT, etc.) to be added
  with one line per module. Adding a region is a separate task
  because it requires new regional scrapers, a new SQLite DB,
  and any data-source-specific changes.
- The eBay DE/UK scraper pair is explicitly out of scope per
  the 2026-06-10 scope decision: it already uses different
  parser modules and isn't a clean factory candidate.
```

- [ ] **Step 4: Commit the log file**

```bash
git add log/M8-T7.md
git commit -m "docs(log): M8-T7 — DE/UK factory refactor for silver_transform + reconcile_quarantine"
```

---

## Self-review

1. **Spec coverage:** every design point maps to a task:
   - `make_reconcile_asset` factory → Task 1
   - `make_silver_asset` factory → Task 2
   - module-level identifiers preserved → Tasks 1, 2, 3 (verified
     by grep)
   - `definitions.py` unchanged → Tasks 1, 2 (verified by load
     check), Task 3 (verified again)
   - tests unchanged → Tasks 1, 2 (count must stay 132)
2. **Placeholder scan:** no "TBD", no "fill in details", no
   "similar to" shortcuts — every step has full code.
3. **Type consistency:** `dg.AssetsDefinition` is the right
   return type for a factory that wraps `@dg.asset`; matches
   what `load_from_defs_folder` expects to find at module
   scope.
4. **No out-of-scope edits:** Tasks 1-2 only touch the two
   `defs/` modules. Task 3 adds the log file. No modifications
   to `definitions.py`, the helpers, the parsers, or any test.

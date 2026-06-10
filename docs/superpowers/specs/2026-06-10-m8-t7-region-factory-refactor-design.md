# M8-T7: DE/UK factory refactor for `silver_transform` and `reconcile_quarantine`

**Date:** 2026-06-10
**Status:** Draft
**Branch (planned):** `2026-06-10-m8-t7-region-factory-refactor`

## Context

`reconcile_quarantine.py` (M8-T3) and `silver_transform.py` (M7) both
contain the same DE/UK duplication pattern: a pair of near-identical
`@dg.asset` functions differing only in the region string ("de" vs
"uk") and a log/appName prefix. The underlying logic is already
factored into helpers (`_reconcile_region`, `_run_silver_transform`)
that take `region` as a parameter — but the asset wrappers around
those helpers are still hand-duplicated.

The M8-T3 spec already flagged this: "do it as a single follow-up
PR that converts all three pairs at once, don't fork patterns." The
"three pairs" phrasing is stale: the eBay DE/UK scraper pair is
already structured around different parser modules
(`ebay_de_item.py`, `ebay_uk_item.py`, etc.) and refactoring it
would touch 4-6 files for limited payoff. On 2026-06-10 the user
scoped M8-T7 to the **two pairs that are genuine factory
candidates**: `reconcile_quarantine` and `silver_transform`.

The payoff: 130 lines of asset body code collapse to ~10. Future
region additions (FR? IT?) become one-line.

## Goal

Replace the four hand-written asset functions with two factory
functions (`make_reconcile_asset`, `make_silver_asset`) that take
`region` and return a `dg.asset`. The existing module-level
identifiers (`reconcile_quarantine_de`, `reconcile_quarantine_uk`,
`silver_de_transform`, `silver_uk_transform`) keep their names —
nothing in `definitions.py` changes.

## Design

### Reconcile pair (simpler)

The current `_reconcile_region(minio_client, region)` helper
already does the real work. The two asset functions are
22-line wrappers that call it and return `MaterializeResult`. New
factory:

```python
def make_reconcile_asset(region: str) -> dg.AssetsDefinition:
    upper = region.upper()
    lower = region.lower()

    @dg.asset(name=f"reconcile_quarantine_{lower}",
              required_resource_keys={"minio_client"})
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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

The two `dg.define_asset_job(...)` definitions stay as-is — they
reference the asset names, which the factory preserves via
`name=f"reconcile_quarantine_{lower}"`.

### Silver pair (heavier)

The duplication here is the entire `SparkConnectServer` start/
stop dance plus the `_run_silver_transform(spark, minio_client,
region)` call. New factory:

```python
def make_silver_asset(region: str) -> dg.AssetsDefinition:
    upper = region.upper()
    lower = region.lower()

    @dg.asset(name=f"silver_{lower}_transform",
              required_resource_keys={"minio_client"})
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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

### Tests

No test changes are required. The existing
`test_reconcile_quarantine.py` (8 tests) covers `_reconcile_region`
directly — that helper is unchanged. The `silver_de_transform` /
`silver_uk_transform` assets have no direct unit tests; their
behavior is covered end-to-end via `complete_eu_pipeline`. After
the refactor:

1. `pytest tests/ -v` must continue to pass with **132 tests
   collected, 132 passing** (no change in count).
2. `python -c "from tcg_platform.definitions import defs; print('OK')"`
   must continue to load cleanly.
3. The Dagster asset graph must still expose assets named
   `reconcile_quarantine_de`, `reconcile_quarantine_uk`,
   `silver_de_transform`, `silver_uk_transform` (the job
   definitions in `definitions.py` reference these by name; the
   factory's `name=...` parameter preserves them).

### What is NOT in scope

- **No new region** (e.g. no FR/IT). The factory *enables* this in
  one line, but adding a region is a separate task (regional
  scrapers, SQLite DB, etc. don't exist yet).
- **No change to `definitions.py`** — the four existing jobs
  reference the four existing asset names and those names are
  preserved by the factory.
- **No change to `_reconcile_region` or `_run_silver_transform`** —
  the helpers are already DRY; the refactor only touches the asset
  wrappers.
- **No change to the eBay DE/UK scraper pair** — out of scope per
  the 2026-06-10 scope decision. They use different parser
  modules and aren't a clean factory candidate.
- **No change to imports** in either file beyond replacing the
  two hand-written functions with the factory call.

## Files

- **Modify:** `src/tcg_platform/defs/reconcile_quarantine.py`
  — replace `reconcile_quarantine_de` and `reconcile_quarantine_uk`
  function definitions with `make_reconcile_asset(...)` calls.
  Add the factory function. Lines saved: ~50.
- **Modify:** `src/tcg_platform/defs/silver_transform.py`
  — replace `silver_de_transform` and `silver_uk_transform`
  function definitions with `make_silver_asset(...)` calls.
  Add the factory function. Lines saved: ~80.
- **Modify:** `src/tcg_platform/definitions.py` — **no change**
  (verified by the load check below).
- **Modify:** `tests/scraping/test_reconcile_quarantine.py` —
  no change (tests the helper, not the asset).
- **New:** `log/M8-T7.md` (created in the implementation phase).

## Verification

1. `pytest tests/ -v` — 132 passing (no count change).
2. `python -c "from tcg_platform.definitions import defs; print('OK')"`
   — clean load.
3. `grep -rn "reconcile_quarantine_de\|reconcile_quarantine_uk\|silver_de_transform\|silver_uk_transform" src/`
   — must still show the four identifiers (preserved by the
   factory).
4. `wc -l src/tcg_platform/defs/reconcile_quarantine.py
      src/tcg_platform/defs/silver_transform.py`
   — line count should drop by ~50 in the first file and ~80 in
   the second (rough; the factory adds some lines but the asset
   bodies shrink dramatically).

## Risks and mitigations

- **Risk:** Dagster's `@dg.asset` decorator captures the enclosing
  function's name as the asset's key by default. The factory uses
  an inner `_asset` function, which would default to key
  `_asset` for both regions. **Mitigation:** the factory passes
  `name=...` explicitly so each region gets a distinct key.
- **Risk:** Dagster asset definitions must be discoverable at
  `defs()` time. Module-level assignment of the factory return
  value (`reconcile_quarantine_de = make_reconcile_asset("de")`)
  is the same pattern as the existing `@dg.asset` decorated
  functions — `load_from_defs_folder` walks the module and picks
  up the names. Verified by the load check.
- **Risk:** The two `define_asset_job` definitions reference the
  four assets by string name. The factory preserves the names
  via `name=f"reconcile_quarantine_{lower}"` /
  `name=f"silver_{lower}_transform"`. Verified by the load check
  (Dagster raises on unresolved selection).

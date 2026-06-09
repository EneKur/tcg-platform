# Design: Quarantine Reconciliation for Silver Layer

**Date:** 2026-06-09
**Status:** Approved
**Author:** brainstorming session with user

## Problem

`tcg-silver/quarantine/{de,uk}/{event_id}.parquet` accumulates eBay
sold-listing rows whose `card_id` was unknown at the time silver ran
(e.g. a freshly-shipping OP16 card scraped before the OP16 card
images were synced via `sync_card_images_job`). The current pipeline
has no path for these rows to ever leave quarantine:

- `silver_de_transform` / `silver_uk_transform` split bronze rows on
  `is_valid_card_id()` and write invalid rows to
  `tcg-silver/quarantine/{region}/{event_id}.parquet`.
- `sync_card_images_job` (the M8-T2 monthly card-image sync) adds new
  card_ids to `tcg-bronze/cards/`, but never re-runs silver.
- Result: quarantined rows stay stuck in quarantine forever; the
  silver `data/` layer is undercounted; the quarantine layer becomes
  a graveyard.

## Approach

Add a `reconcile_quarantine_*` step to `silver_eu_orchestrator`,
**before** the existing `silver_de_transform` / `silver_uk_transform`
calls. The step:

1. Reads every parquet in `tcg-silver/quarantine/{region}/`.
2. For each, loads the row's `card_id`.
3. Re-runs `is_valid_card_id(card_id, valid_card_ids_set)` where
   `valid_card_ids_set` is freshly loaded from `tcg-bronze/cards/`.
4. If it now passes → delete the quarantine parquet.
5. If it still fails → leave the file alone.
6. After the reconciler runs, the existing
   `silver_de_transform` / `silver_uk_transform` re-reads all bronze
   parquets and writes the now-valid row to
   `tcg-silver/data/{region}/{event_id}.parquet` via the writer's
   normal collision-check path (which sees no existing data file, so
   the row lands at the base path).

### Why no SQLite is needed

The `parqueted` column in `fact_events` is **not** a "row is in
silver" signal. Its actual lifecycle:

- **Set to `1` at scrape time** (`ebay_de_sold_listings.py:142`,
  `ebay_uk_sold_listings.py:141`): the scraper writes both the SQLite
  row and the bronze parquet in the same operation, so the flag
  indicates "this row's bronze parquet exists."
- **Read by `backfill_sold_data_parquet.py:55`** (`get_unparqueted_fact_events`):
  used by the one-shot historical loader (SQLite → MinIO backfill
  from before the bronze-pipeline existed).
- **Silver does NOT read `parqueted`.** It lists everything in
  `tcg-bronze/sold_data/{region}/` and processes every parquet
  (`silver_transform.py:359`).

Therefore the reconciler is a pure function of MinIO state. It does
not need to touch SQLite. Deleting the quarantine parquet is
sufficient: the bronze parquet stays put, the next silver run
re-evaluates the row, and the writer lands it in `data/`.

## Design decisions (from brainstorming)

| Question | Decision |
|---|---|
| Which quarantined rows to re-check? | All currently-quarantined rows. |
| Where do promoted rows go? | Move: delete from `quarantine/`, next silver run writes to `data/`. |
| What about the existing `silver_*_transform` quarantine writes? | Keep them. The reconciler is the only place that promotes. |
| How to identify the matching bronze row? | N/A — we don't touch bronze. The reconciler uses the `card_id` from inside the quarantine parquet. |
| Asset shape? | Two assets (`reconcile_quarantine_de`, `reconcile_quarantine_uk`) called from inside `silver_eu_orchestrator`'s body, matching the existing orchestrator pattern. |
| Where does the reconciler live in the DAG? | Inside `silver_eu_orchestrator`'s body, before the existing silver calls. Not downstream of `sync_card_images` in the DAG. |
| What if `sync_card_images` hasn't been run yet (no new cards)? | The reconciler is idempotent: re-running it just re-validates every row, finds nothing changed, exits. No work done, no side effects. |

## Components

### New module: `src/tcg_platform/defs/reconcile_quarantine.py`

```python
import io
import logging
import dagster as dg
import pyarrow.parquet as pq

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.silver_transform import (
    _build_card_id_set,  # reuse from silver_transform
    is_valid_card_id,    # reuse from silver_transform
)

_LOG = logging.getLogger(__name__)
SILVER_BUCKET = "tcg-silver"


def _reconcile_region(minio_client: MinioClientResource, region: str) -> dict:
    """For each parquet in tcg-silver/quarantine/{region}/, re-validate its
    card_id against the current tcg-bronze/cards/ set. Delete the file if
    the card_id now passes; otherwise leave it alone.

    Returns counts and the list of promoted card_ids for Dagster metadata.
    """
    valid_card_ids = _build_card_id_set(minio_client, "tcg-bronze")
    quarantine_prefix = f"quarantine/{region}/"
    quarantined_paths = list(minio_client.list_objects(SILVER_BUCKET, prefix=quarantine_prefix))

    promoted: list[dict] = []
    still_quarantined = 0
    read_errors = 0
    to_delete: list[str] = []

    for path in quarantined_paths:
        try:
            data = minio_client.get_object(SILVER_BUCKET, path)
            table = pq.read_table(io.BytesIO(data))
        except Exception as e:
            _LOG.warning(f"Reconcile: failed to read {path}: {e}")
            read_errors += 1
            continue

        if table.num_rows == 0:
            # Empty file — cleanup, no re-validation needed.
            to_delete.append(path)
            continue

        card_id = table.column("card_id").to_pylist()[0]
        if is_valid_card_id(card_id, valid_card_ids):
            to_delete.append(path)
            promoted.append({"path": path, "card_id": card_id})
            _LOG.info(f"Reconcile: promoted {card_id} ({path})")
        else:
            still_quarantined += 1

    if to_delete:
        from minio.deleteobjects import DeleteObject
        minio_client.remove_objects(
            SILVER_BUCKET, [DeleteObject(name=p) for p in to_delete]
        )

    return {
        "scanned": len(quarantined_paths),
        "promoted_count": len(promoted),
        "still_quarantined_count": still_quarantined,
        "read_errors": read_errors,
        "promoted": promoted,
    }


@dg.asset(required_resource_keys={"minio_client"})
def reconcile_quarantine_de(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    minio_client = context.resources.minio_client
    result = _reconcile_region(minio_client, "de")
    context.log.info(
        f"DE reconcile: scanned={result['scanned']} promoted={result['promoted_count']} "
        f"still_quarantined={result['still_quarantined_count']} read_errors={result['read_errors']}"
    )
    return dg.MaterializeResult(metadata={
        "scanned": result["scanned"],
        "promoted_count": result["promoted_count"],
        "still_quarantined_count": result["still_quarantined_count"],
        "read_errors": result["read_errors"],
        "promoted_card_ids": dg.MetadataValue.json(
            [p["card_id"] for p in result["promoted"]]
        ),
    })


@dg.asset(required_resource_keys={"minio_client"})
def reconcile_quarantine_uk(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    minio_client = context.resources.minio_client
    result = _reconcile_region(minio_client, "uk")
    context.log.info(
        f"UK reconcile: scanned={result['scanned']} promoted={result['promoted_count']} "
        f"still_quarantined={result['still_quarantined_count']} read_errors={result['read_errors']}"
    )
    return dg.MaterializeResult(metadata={
        "scanned": result["scanned"],
        "promoted_count": result["promoted_count"],
        "still_quarantined_count": result["still_quarantined_count"],
        "read_errors": result["read_errors"],
        "promoted_card_ids": dg.MetadataValue.json(
            [p["card_id"] for p in result["promoted"]]
        ),
    })


reconcile_quarantine_de_job = dg.define_asset_job(
    name="reconcile_quarantine_de_job",
    selection=["reconcile_quarantine_de"],
    description="Re-validate DE quarantined silver rows against current card set.",
)

reconcile_quarantine_uk_job = dg.define_asset_job(
    name="reconcile_quarantine_uk_job",
    selection=["reconcile_quarantine_uk"],
    description="Re-validate UK quarantined silver rows against current card set.",
)
```

### Modified: `src/tcg_platform/defs/eu_pipeline_orchestrator.py`

Add two new job invocations at the top of `silver_eu_orchestrator`'s
body, before the existing silver calls. Order: `reconcile_de` →
`reconcile_uk` → existing `silver_de` → existing `silver_uk`
(sequential, matches the existing DE → UK pattern in the orchestrator).

```python
@dg.asset(deps=[AssetKey("backfill_de_asset"), AssetKey("backfill_uk_asset")])
def silver_eu_orchestrator(context: dg.AssetExecutionContext):
    from tcg_platform.definitions import defs
    context.log.info("Starting silver_eu_orchestrator")

    resolved = defs.load_fn()

    # NEW: reconcile quarantine first so silver can re-evaluate promoted rows
    job_def_reconcile_de = resolved.resolve_job_def("reconcile_quarantine_de_job")
    job_def_reconcile_uk = resolved.resolve_job_def("reconcile_quarantine_uk_job")

    context.log.info("Running reconcile_quarantine_de_job...")
    result_reconcile_de = job_def_reconcile_de.execute_in_process(instance=context.instance)
    context.log.info(f"reconcile_de complete, run_id={result_reconcile_de.run_id}")

    context.log.info("Running reconcile_quarantine_uk_job...")
    result_reconcile_uk = job_def_reconcile_uk.execute_in_process(instance=context.instance)
    context.log.info(f"reconcile_uk complete, run_id={result_reconcile_uk.run_id}")

    # Existing: run silver DE then UK sequentially
    job_def_de = resolved.resolve_job_def("silver_de_pipeline")
    context.log.info("Running silver_de_pipeline...")
    result_de = job_def_de.execute_in_process(instance=context.instance)
    context.log.info(f"silver_de complete, run_id={result_de.run_id}")

    job_def_uk = resolved.resolve_job_def("silver_uk_pipeline")
    context.log.info("Running silver_uk_pipeline...")
    result_uk = job_def_uk.execute_in_process(instance=context.instance)
    context.log.info(f"silver_uk complete, run_id={result_uk.run_id}")

    return dg.MaterializeResult(metadata={
        "reconcile_de_run_id": result_reconcile_de.run_id,
        "reconcile_uk_run_id": result_reconcile_uk.run_id,
        "de_run_id": result_de.run_id,
        "uk_run_id": result_uk.run_id,
    })
```

## Data flow

```
                  (monthly, standalone)
[ sync_card_images_job ] ──► tcg-bronze/cards/OP16/OP16-005.webp
                                   │
                                   │  (cards folder updated)
                                   ▼
[ silver_eu_orchestrator ]   ◄── triggered by eBay bronze backfill sensors
        │
        ├─► reconcile_quarantine_de_job
        │     │
        │     ├─ read tcg-silver/quarantine/de/*.parquet
        │     ├─ for each: is_valid_card_id(card_id, current tcg-bronze/cards/)
        │     └─ if valid: delete parquet
        │
        ├─► reconcile_quarantine_uk_job
        │     └─ (same for UK)
        │
        ├─► silver_de_pipeline  (re-reads bronze, writes promoted row to data/)
        └─► silver_uk_pipeline  (same for UK)
```

The reconciler does **not** depend on `sync_card_images` in the DAG.
It is idempotent — re-running with no new cards does nothing
harmful. The DAG relationship between the two jobs is "eventually
consistent": after a monthly `sync_card_images_job` run, the next
silver_eu_orchestrator invocation picks up the new cards and
promotes the matching quarantined rows.

## Edge cases

- **Empty quarantine file:** delete, no re-validation needed.
- **Quarantine file with `_1`, `_2` suffix** (from the writer's
  collision scheme): algorithm doesn't depend on the filename, only
  on the `card_id` column inside the file. The suffix is preserved
  on delete (we delete the actual file the writer created, including
  its suffix).
- **Corrupted/parquet-unreadable file:** warning logged, file
  untouched, `read_errors` incremented. Doesn't crash the
  orchestrator. Will be re-tried on the next run.
- **Card-id that's the empty string or `None`:** `is_valid_card_id`
  returns False, file stays in quarantine. Correct.
- **Bronze row that was deleted somehow:** reconciler doesn't touch
  bronze. Bronze parquets are immutable.
- **Run the reconciler before any `sync_card_images` ever ran:**
  works correctly. The card set is whatever's in
  `tcg-bronze/cards/` at run time. Idempotent.

## Testing

### Unit tests in `tests/scraping/test_reconcile_quarantine.py` (new)

1. `test_promotes_row_whose_card_id_now_passes` — quarantine parquet
   with `card_id="OP16-005"`; `tcg-bronze/cards/OP16/OP16-005.webp`
   exists → reconciler deletes the quarantine file, returns
   `promoted_count=1`.
2. `test_leaves_row_alone_when_card_id_still_invalid` — quarantine
   parquet with `card_id="MALFORMED"`; cards folder doesn't have it
   → file untouched, `promoted_count=0`,
   `still_quarantined_count=1`.
3. `test_promotes_only_valid_in_mixed_batch` — 5 quarantine
   parquets, 2 with valid card_ids, 3 still invalid → only the 2
   valid ones deleted.
4. `test_deletes_empty_quarantine_file` — quarantine parquet with 0
   rows → deleted, no read of columns.
5. `test_read_error_leaves_file_untouched` — corrupted parquet →
   warning logged, file NOT deleted, `read_errors=1`,
   `promoted_count=0`.
6. `test_handles_zero_quarantined_files` — empty
   `quarantine/{region}/` prefix → reconciler returns zero counts,
   no errors.
7. `test_handles_card_id_with_collision_suffix` — quarantine file at
   `quarantine/de/12345_1.parquet` with `card_id="OP11-001"` →
   deletes correctly; verifies the suffix doesn't break the
   algorithm.
8. `test_uses_cardset_at_run_time_not_at_quarantine_time` —
   quarantine file says `card_id="OP17-099"`, the cards folder does
   NOT have `OP17-099.webp`; reconciler returns
   `still_quarantined=1`. Then in a follow-up test, add
   `OP17-099.webp` to the cards folder (in test setup) and re-run
   reconciler → `promoted_count=1`. This pins "re-validates against
   the *current* card set, not a snapshot."

### Integration smoke test (manual, after merge)

1. Find a real quarantined file in
   `tcg-silver/quarantine/{de,uk}/` whose `card_id` corresponds to a
   card we *know* is in the cards folder (e.g. `OP01-001`).
2. Run `reconcile_quarantine_de_job` from Dagster UI → confirm the
   file is deleted.
3. Run `silver_de_pipeline` → confirm
   `data/de/{event_id}.parquet` is created.
4. Verify quarantine file is gone and data file exists with the
   expected content.

### No regression test needed

`silver_de_transform` / `silver_uk_transform` behavior is unchanged.
The reconciler is additive.

## Out of scope

- **Cleaning up historical quarantined rows from before the cards
  folder was complete.** These have invalid card_ids that *will
  never* be valid (e.g. `BUNDLE_OF_CARDS`, `MALFORMED_TITLE` from
  old broken scrapes). The reconciler will leave them in quarantine,
  which is correct. Cleaning them up is a separate audit task.
- **TTL on quarantined files.** Not addressed. If a row is
  quarantined and never gets re-validated, it sits there forever.
  (Same as today.)
- **The 14 `failed_card_ids` from `sync_card_images`.** Different
  problem (CDN gaps, not card_id validation). Unrelated.
- **Bronze `cardlist` parquet writer for Limitless** and **silver
  `is_valid_card_id` path bug** flagged in earlier sessions. Still
  outstanding, separate tasks.
- **Spark deprecation.** Current silver transform still uses
  pysail/Spark. Out of scope.

## Files modified

- **New:** `src/tcg_platform/defs/reconcile_quarantine.py` —
  defines the 2 assets and 2 jobs. Assets are auto-discovered by
  `load_from_defs_folder` in `definitions.py:83` (no asset
  registration needed). The 2 new jobs must be added to the
  `jobs=[...]` list in `definitions.py:87-98`.
- **New:** `tests/scraping/test_reconcile_quarantine.py`
- **Modified:** `src/tcg_platform/definitions.py` — add 2 imports
  for the new jobs and 2 entries in the `jobs=[...]` list.
- **Modified:** `src/tcg_platform/defs/eu_pipeline_orchestrator.py`
  — add 2 job calls at the top of `silver_eu_orchestrator`'s body
  and 2 new fields in its returned `MaterializeResult` metadata.

### Asset/job discovery notes

`definitions.py:81-83` uses `@definitions` + `load_from_defs_folder`
to auto-discover all `@dg.asset` and `@dg.sensor` definitions in
`defs/`. So the 2 new assets will appear in the Dagster UI
automatically once the new module is on disk.

`@dg.define_asset_job` results are **not** auto-discovered — they
must be imported and added to the explicit `jobs=[...]` list in
`definitions.py:87-98`. The orchestrator's `silver_eu_orchestrator`
looks up jobs by name via `resolved.resolve_job_def(name)`, so the
exact string in `define_asset_job(name=...)` must match what the
orchestrator passes to `resolve_job_def(...)`.
- **Modified:** `src/tcg_platform/defs/eu_pipeline_orchestrator.py`
  — add 2 job calls at the top of `silver_eu_orchestrator`'s body
  and 2 new fields in its returned `MaterializeResult` metadata.

## Risk

- **Low.** The reconciler only deletes from `tcg-silver/quarantine/`.
  No bronze mutations. No data/ writes. The next silver run
  re-creates the data/ file from the immutable bronze parquet. The
  worst case: the reconciler is buggy and deletes a quarantine file
  that wouldn't have been promoted. The next silver run re-validates
  the row and either writes it to data/ (correct) or back to
  quarantine/ (correct). No data loss.
- **One subtle risk:** if the silver writer's collision check sees
  both a `data/{event_id}.parquet` and a `quarantine/{event_id}.parquet`
  for the same item, it has no logic to merge them. Today this
  doesn't happen because the writer reads each bronze row once and
  picks one destination. We don't introduce this risk — our
  reconciler only touches `quarantine/`, and we trust the next
  silver run to do the right thing.

# Quarantine Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a quarantine-reconciliation step to the silver EU orchestrator that deletes quarantined eBay rows whose `card_id` is now in the cards folder, allowing the next silver run to promote them to `data/`.

**Architecture:** New `reconcile_quarantine.py` module exposes two `@dg.asset` functions (`reconcile_quarantine_de`, `reconcile_quarantine_uk`) and two corresponding jobs. The `silver_eu_orchestrator` body is updated to call the two new jobs before the existing silver calls. Reconciler logic is a pure function of MinIO state — no SQLite writes, no bronze mutations. The reconciler reads the `card_id` from each quarantine parquet, re-validates against the current `tcg-bronze/cards/` set, and deletes matches via the `MinioClientResource.remove_objects` batch API.

**Tech Stack:** Dagster assets/jobs, MinIO (`tcg-silver`, `tcg-bronze` buckets), pyarrow for parquet I/O, pytest with `MagicMock`-based MinIO test fixtures matching the existing `test_silver_file_writer.py` pattern.

---

## File Structure

| File | Status | Responsibility |
|------|--------|----------------|
| `src/tcg_platform/defs/reconcile_quarantine.py` | **New** | `_reconcile_region()` helper, `reconcile_quarantine_de` asset, `reconcile_quarantine_uk` asset, two `define_asset_job` definitions |
| `src/tcg_platform/defs/eu_pipeline_orchestrator.py` | **Modify** | Add two new job calls + two metadata fields to `silver_eu_orchestrator` |
| `src/tcg_platform/definitions.py` | **Modify** | Import the two new jobs, add them to the `jobs=[...]` list |
| `tests/scraping/test_reconcile_quarantine.py` | **New** | 8 unit tests covering promote, leave-alone, mixed batch, empty file, read error, zero files, collision suffix, run-time cardset |

Reuse from `silver_transform.py` (no new code): `_build_card_id_set`, `is_valid_card_id`. They are module-level private helpers but accessible from a sibling module.

Reuse from `minio_client.py` (no new code): `list_objects`, `get_object`, `remove_objects` (batch API only — there is no `remove_object` singular).

---

## Task 1: Promote a quarantined row whose card_id now passes

**Files:**
- Create: `tests/scraping/test_reconcile_quarantine.py`
- Create: `src/tcg_platform/defs/reconcile_quarantine.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/scraping/test_reconcile_quarantine.py
import io
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from tcg_platform.defs.reconcile_quarantine import _reconcile_region


def _row_to_parquet_bytes(card_id: str) -> bytes:
    """Build a single-row quarantine parquet for the given card_id."""
    table = pa.Table.from_pydict({
        "event_id": ["123456789012"],
        "card_id": [card_id],
        "card_version": [None],
        "event_type": ["sale"],
        "price": [100.0],
        "currency": ["EUR"],
        "sold_date": ["2026-06-04"],
        "scraped_from": ["ebay"],
        "source": ["DE"],
        "source_url": ["https://www.ebay.de/itm/123456789012"],
        "language": ["EN"],
        "scraped_at": ["2026-06-04T09:00:00+00:00"],
        "image_url": [""],
        "title": [""],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_minio_with_files(
    cards_files: list[str],
    quarantine_files: dict,
) -> MagicMock:
    """Build a mock MinioClientResource.

    cards_files: object names in tcg-bronze/cards/ (the card set)
    quarantine_files: {object_name: parquet_bytes} in tcg-silver/quarantine/{region}/
    """
    minio = MagicMock()

    def list_objects(bucket, prefix=""):
        if bucket == "tcg-bronze":
            return list(cards_files)
        if bucket == "tcg-silver":
            return [name for name in quarantine_files if name.startswith(prefix)]
        return []

    def get_object(bucket, name):
        if bucket == "tcg-silver":
            return quarantine_files[name]
        raise RuntimeError(f"unexpected get_object({bucket}, {name})")

    deleted: list[list] = []

    def remove_objects(bucket, delete_list):
        deleted.append([d.name for d in delete_list])
        for d in delete_list:
            quarantine_files.pop(d.name, None)

    minio.list_objects = list_objects
    minio.get_object = get_object
    minio.remove_objects = remove_objects
    minio._deleted = deleted
    return minio


def test_promotes_row_whose_card_id_now_passes():
    cards_files = ["cards/OP16/OP16-005.webp"]
    quarantine_files = {
        "quarantine/de/999999999999.parquet": _row_to_parquet_bytes("OP16-005"),
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 1
    assert result["promoted_count"] == 1
    assert result["still_quarantined_count"] == 0
    assert result["read_errors"] == 0
    assert result["promoted"] == [
        {"path": "quarantine/de/999999999999.parquet", "card_id": "OP16-005"}
    ]
    # File must actually be gone from the mock's state
    assert "quarantine/de/999999999999.parquet" not in quarantine_files
    # And remove_objects must have been called for it
    assert minio._deleted == [["quarantine/de/999999999999.parquet"]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_promotes_row_whose_card_id_now_passes -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tcg_platform.defs.reconcile_quarantine'`

- [ ] **Step 3: Write the minimal implementation**

```python
# src/tcg_platform/defs/reconcile_quarantine.py
import io
import logging

import dagster as dg
import pyarrow.parquet as pq

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.silver_transform import (
    _build_card_id_set,
    is_valid_card_id,
)

_LOG = logging.getLogger(__name__)

SILVER_BUCKET = "tcg-silver"
BRONZE_BUCKET = "tcg-bronze"


def _reconcile_region(minio_client: MinioClientResource, region: str) -> dict:
    """Re-validate each parquet in tcg-silver/quarantine/{region}/.

    For each, check the row's card_id against the current tcg-bronze/cards/
    set. If valid, batch-delete the parquet. Otherwise leave it.

    Returns a dict with keys: scanned, promoted_count, still_quarantined_count,
    read_errors, promoted (list of {path, card_id}).
    """
    valid_card_ids = _build_card_id_set(minio_client, BRONZE_BUCKET)
    quarantine_prefix = f"quarantine/{region}/"
    quarantined_paths = list(
        minio_client.list_objects(SILVER_BUCKET, prefix=quarantine_prefix)
    )

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
            # Empty file: cleanup, no re-validation needed.
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_promotes_row_whose_card_id_now_passes -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/reconcile_quarantine.py tests/scraping/test_reconcile_quarantine.py
git commit -m "feat(reconcile_quarantine): promote rows whose card_id now passes"
```

---

## Task 2: Leave quarantined rows whose card_id is still invalid

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append to `tests/scraping/test_reconcile_quarantine.py`:

```python
def test_leaves_row_alone_when_card_id_still_invalid():
    cards_files = ["cards/OP16/OP16-005.webp"]  # OP16-005 exists, MALFORMED does not
    quarantine_files = {
        "quarantine/de/888888888888.parquet": _row_to_parquet_bytes("MALFORMED"),
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 1
    assert result["promoted_count"] == 0
    assert result["still_quarantined_count"] == 1
    assert result["read_errors"] == 0
    assert result["promoted"] == []
    # File must remain in quarantine
    assert "quarantine/de/888888888888.parquet" in quarantine_files
    # remove_objects must NOT have been called
    assert minio._deleted == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_leaves_row_alone_when_card_id_still_invalid -v`
Expected: FAIL — current implementation would still delete the file (since `_reconcile_region` already does the right thing, this test will likely PASS, which is the desired state). If it passes, mark step complete and move on. If it fails, fix the implementation.

Run anyway to confirm:
`uv run pytest tests/scraping/test_reconcile_quarantine.py::test_leaves_row_alone_when_card_id_still_invalid -v`
Expected: PASS

- [ ] **Step 3: No implementation change needed** — the implementation from Task 1 already handles this case. Skip this step.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_leaves_row_alone_when_card_id_still_invalid -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin still-invalid rows are not promoted"
```

---

## Task 3: Mixed batch — promote only the valid subset

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append:

```python
def test_promotes_only_valid_in_mixed_batch():
    cards_files = [
        "cards/OP01/OP01-001.webp",
        "cards/OP01/OP01-002.webp",
    ]
    quarantine_files = {
        "quarantine/de/111111111111.parquet": _row_to_parquet_bytes("OP01-001"),
        "quarantine/de/222222222222.parquet": _row_to_parquet_bytes("BUNDLE_OF_CARDS"),
        "quarantine/de/333333333333.parquet": _row_to_parquet_bytes("OP01-002"),
        "quarantine/de/444444444444.parquet": _row_to_parquet_bytes("MALFORMED_TITLE"),
        "quarantine/de/555555555555.parquet": _row_to_parquet_bytes("OP17-099"),
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 5
    assert result["promoted_count"] == 2
    assert result["still_quarantined_count"] == 3
    assert result["read_errors"] == 0
    promoted_paths = {p["path"] for p in result["promoted"]}
    assert promoted_paths == {
        "quarantine/de/111111111111.parquet",
        "quarantine/de/333333333333.parquet",
    }
    # The 2 valid ones are gone, the 3 invalid ones remain
    assert "quarantine/de/111111111111.parquet" not in quarantine_files
    assert "quarantine/de/333333333333.parquet" not in quarantine_files
    assert "quarantine/de/222222222222.parquet" in quarantine_files
    assert "quarantine/de/444444444444.parquet" in quarantine_files
    assert "quarantine/de/555555555555.parquet" in quarantine_files
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_promotes_only_valid_in_mixed_batch -v`
Expected: PASS (implementation from Task 1 already handles this)

- [ ] **Step 3: No implementation change needed**

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_promotes_only_valid_in_mixed_batch -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin mixed-batch behavior"
```

---

## Task 4: Empty quarantine file is cleaned up without crashing

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append:

```python
def test_deletes_empty_quarantine_file():
    cards_files = ["cards/OP01/OP01-001.webp"]
    # Build a parquet with the right schema but zero rows
    schema = pa.schema([
        ("event_id", pa.string()),
        ("card_id", pa.string()),
        ("card_version", pa.string()),
        ("event_type", pa.string()),
        ("price", pa.float64()),
        ("currency", pa.string()),
        ("sold_date", pa.string()),
        ("scraped_from", pa.string()),
        ("source", pa.string()),
        ("source_url", pa.string()),
        ("language", pa.string()),
        ("scraped_at", pa.string()),
        ("image_url", pa.string()),
        ("title", pa.string()),
    ])
    empty_table = pa.Table.from_pydict({}, schema=schema)
    buf = io.BytesIO()
    pq.write_table(empty_table, buf)
    empty_bytes = buf.getvalue()

    quarantine_files = {
        "quarantine/de/666666666666.parquet": empty_bytes,
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 1
    assert result["promoted_count"] == 0
    assert result["still_quarantined_count"] == 0
    assert result["read_errors"] == 0
    # Empty file should be deleted
    assert "quarantine/de/666666666666.parquet" not in quarantine_files
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_deletes_empty_quarantine_file -v`
Expected: PASS (Task 1's implementation already handles this)

- [ ] **Step 3: No implementation change needed**

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_deletes_empty_quarantine_file -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin empty-file cleanup"
```

---

## Task 5: Read error leaves the file untouched

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append:

```python
def test_read_error_leaves_file_untouched():
    cards_files = ["cards/OP01/OP01-001.webp"]
    quarantine_files = {
        "quarantine/de/777777777777.parquet": b"not a real parquet",
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 1
    assert result["promoted_count"] == 0
    assert result["still_quarantined_count"] == 0
    assert result["read_errors"] == 1
    # File remains in quarantine for next run to try again
    assert "quarantine/de/777777777777.parquet" in quarantine_files
    # remove_objects must NOT have been called
    assert minio._deleted == []
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_read_error_leaves_file_untouched -v`
Expected: PASS (Task 1's implementation already handles this — the `try/except` block around `get_object` and `pq.read_table` catches parquet parse failures and increments `read_errors`)

- [ ] **Step 3: No implementation change needed**

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_read_error_leaves_file_untouched -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin read-error tolerance"
```

---

## Task 6: Zero quarantined files is a clean no-op

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append:

```python
def test_handles_zero_quarantined_files():
    cards_files = ["cards/OP01/OP01-001.webp"]
    quarantine_files: dict = {}
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result == {
        "scanned": 0,
        "promoted_count": 0,
        "still_quarantined_count": 0,
        "read_errors": 0,
        "promoted": [],
    }
    assert minio._deleted == []
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_handles_zero_quarantined_files -v`
Expected: PASS

- [ ] **Step 3: No implementation change needed**

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_handles_zero_quarantined_files -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin zero-file no-op"
```

---

## Task 7: Collision suffix on quarantine file does not break the algorithm

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append:

```python
def test_handles_card_id_with_collision_suffix():
    """Quarantine file at quarantine/de/{event_id}_1.parquet (collision suffix
    from the writer) should still be processed correctly. The algorithm only
    reads the card_id column; the filename suffix is irrelevant."""
    cards_files = ["cards/OP11/OP11-001.webp"]
    quarantine_files = {
        # _1 suffix means: the same event_id was quarantined with a
        # different (sold_date, event_id, title) tuple already
        "quarantine/de/123456789012_1.parquet": _row_to_parquet_bytes("OP11-001"),
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 1
    assert result["promoted_count"] == 1
    assert result["promoted"][0]["path"] == "quarantine/de/123456789012_1.parquet"
    assert "quarantine/de/123456789012_1.parquet" not in quarantine_files
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_handles_card_id_with_collision_suffix -v`
Expected: PASS (Task 1's implementation already handles this — it iterates every path the `list_objects` returns, suffix or not, and only cares about the `card_id` column)

- [ ] **Step 3: No implementation change needed**

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_handles_card_id_with_collision_suffix -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin collision-suffix handling"
```

---

## Task 8: Reconciler uses the *current* cards folder, not a snapshot

**Files:**
- Modify: `tests/scraping/test_reconcile_quarantine.py`

- [ ] **Step 1: Add the failing test**

Append:

```python
def test_uses_cardset_at_run_time_not_at_quarantine_time():
    """Pin the central property: reconciler re-validates against the cards
    folder AS IT IS NOW, not against any snapshot from when the row was
    originally quarantined. We simulate this by running the reconciler twice
    against the same quarantine file: once before OP17-099 is in the cards
    folder (row stays quarantined), once after (row gets promoted)."""
    quarantine_files = {
        "quarantine/de/101010101010.parquet": _row_to_parquet_bytes("OP17-099"),
    }

    # Pass 1: OP17-099 not yet in cards folder
    cards_files_pass1: list[str] = []
    minio1 = _make_minio_with_files(cards_files_pass1, quarantine_files)
    result1 = _reconcile_region(minio1, "de")
    assert result1["promoted_count"] == 0
    assert result1["still_quarantined_count"] == 1
    assert "quarantine/de/101010101010.parquet" in quarantine_files

    # Pass 2: OP17-099 has been added to the cards folder (e.g. a later
    # sync_card_images_job run). The same quarantine file should now
    # be promoted.
    cards_files_pass2 = ["cards/OP17/OP17-099.webp"]
    minio2 = _make_minio_with_files(cards_files_pass2, quarantine_files)
    result2 = _reconcile_region(minio2, "de")
    assert result2["promoted_count"] == 1
    assert result2["still_quarantined_count"] == 0
    assert "quarantine/de/101010101010.parquet" not in quarantine_files
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_uses_cardset_at_run_time_not_at_quarantine_time -v`
Expected: PASS (Task 1's implementation already does this — `_build_card_id_set` is called fresh on every `_reconcile_region` invocation)

- [ ] **Step 3: No implementation change needed**

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scraping/test_reconcile_quarantine.py::test_uses_cardset_at_run_time_not_at_quarantine_time -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/scraping/test_reconcile_quarantine.py
git commit -m "test(reconcile_quarantine): pin run-time cardset re-validation"
```

---

## Task 9: Run full test suite to confirm no regressions

**Files:** None (verification step)

- [ ] **Step 1: Run all tests**

Run: `uv run pytest tests/ -v`
Expected: 110 existing tests + 8 new tests = 118 tests, all passing. (Existing baseline is 110 from the 2026-06-08 M8-T2 session log.)

- [ ] **Step 2: If any test fails**, fix the implementation. The most likely failure mode is if a test depends on the exact return-dict shape from `_reconcile_region`; if you added a key, update the test in Tasks 1-8 to match.

- [ ] **Step 3: Commit (only if any test was fixed)**

```bash
git add <fixed files>
git commit -m "fix: align reconcile_quarantine tests with full-suite run"
```

(If no fixes were needed, skip this commit.)

---

## Task 10: Add the Dagster assets for DE and UK

**Files:**
- Modify: `src/tcg_platform/defs/reconcile_quarantine.py`

- [ ] **Step 1: Add assets and jobs below the existing `_reconcile_region` helper**

Append to `src/tcg_platform/defs/reconcile_quarantine.py`:

```python
@dg.asset(required_resource_keys={"minio_client"})
def reconcile_quarantine_de(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Re-validate tcg-silver/quarantine/de/ rows against the current
    tcg-bronze/cards/ set. Deletes files whose card_id is now valid; the
    next silver_de_pipeline run will re-evaluate the corresponding bronze
    rows and write them to tcg-silver/data/de/."""
    minio_client: MinioClientResource = context.resources.minio_client
    result = _reconcile_region(minio_client, "de")
    context.log.info(
        f"DE reconcile: scanned={result['scanned']} "
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


@dg.asset(required_resource_keys={"minio_client"})
def reconcile_quarantine_uk(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Re-validate tcg-silver/quarantine/uk/ rows against the current
    tcg-bronze/cards/ set. See reconcile_quarantine_de for details."""
    minio_client: MinioClientResource = context.resources.minio_client
    result = _reconcile_region(minio_client, "uk")
    context.log.info(
        f"UK reconcile: scanned={result['scanned']} "
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


reconcile_quarantine_de_job = dg.define_asset_job(
    name="reconcile_quarantine_de_job",
    selection=["reconcile_quarantine_de"],
    description="Re-validate DE quarantined silver rows against the current card set.",
)

reconcile_quarantine_uk_job = dg.define_asset_job(
    name="reconcile_quarantine_uk_job",
    selection=["reconcile_quarantine_uk"],
    description="Re-validate UK quarantined silver rows against the current card set.",
)
```

- [ ] **Step 2: Verify Dagster definitions load cleanly**

Run: `uv run python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/tcg_platform/defs/reconcile_quarantine.py
git commit -m "feat(reconcile_quarantine): add DE/UK assets and jobs"
```

---

## Task 11: Register the new jobs in definitions.py

**Files:**
- Modify: `src/tcg_platform/definitions.py`

- [ ] **Step 1: Add imports**

In `src/tcg_platform/definitions.py`, add to the import block (after the `eu_pipeline_orchestrator` import on line 23-28):

```python
from tcg_platform.defs.eu_pipeline_orchestrator import (
    bronze_eu_orchestrator,
    backfill_de_asset,
    backfill_uk_asset,
    silver_eu_orchestrator,
)
from tcg_platform.defs.reconcile_quarantine import (
    reconcile_quarantine_de_job,
    reconcile_quarantine_uk_job,
)
```

- [ ] **Step 2: Add the new jobs to the jobs list**

In the `jobs=[` block (currently lines 87-98), add the two new jobs:

```python
        jobs=[
            ebay_de_job,
            ebay_uk_job,
            ebay_eu_job,
            backfill_de_job,
            backfill_uk_job,
            silver_de_job,
            silver_uk_job,
            silver_eu_job,
            complete_eu_pipeline,
            sync_card_images_job,
            reconcile_quarantine_de_job,
            reconcile_quarantine_uk_job,
        ],
```

- [ ] **Step 3: Verify Dagster definitions load cleanly**

Run: `uv run python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/definitions.py
git commit -m "feat(definitions): register reconcile_quarantine_de/uk jobs"
```

---

## Task 12: Wire the new jobs into silver_eu_orchestrator

**Files:**
- Modify: `src/tcg_platform/defs/eu_pipeline_orchestrator.py`

- [ ] **Step 1: Replace the `silver_eu_orchestrator` function**

In `src/tcg_platform/defs/eu_pipeline_orchestrator.py`, replace the existing `silver_eu_orchestrator` function (lines 50-72) with:

```python
@dg.asset(deps=[AssetKey("backfill_de_asset"), AssetKey("backfill_uk_asset")])
def silver_eu_orchestrator(context: dg.AssetExecutionContext):
    """Run reconcile_quarantine → silver for DE and UK sequentially.

    The reconciler runs FIRST so that any quarantined rows whose card_id
    is now in the cards folder get their quarantine parquets deleted. The
    subsequent silver runs then re-evaluate the corresponding bronze rows
    and write them to tcg-silver/data/.

    Waits for both backfills to succeed.
    """
    from tcg_platform.definitions import defs
    context.log.info("Starting silver_eu_orchestrator")

    resolved = defs.load_fn()

    # Reconcile DE quarantine first
    job_def_reconcile_de = resolved.resolve_job_def("reconcile_quarantine_de_job")
    context.log.info("Running reconcile_quarantine_de_job...")
    result_reconcile_de = job_def_reconcile_de.execute_in_process(instance=context.instance)
    context.log.info(f"reconcile_de complete, run_id={result_reconcile_de.run_id}")

    # Then UK
    job_def_reconcile_uk = resolved.resolve_job_def("reconcile_quarantine_uk_job")
    context.log.info("Running reconcile_quarantine_uk_job...")
    result_reconcile_uk = job_def_reconcile_uk.execute_in_process(instance=context.instance)
    context.log.info(f"reconcile_uk complete, run_id={result_reconcile_uk.run_id}")

    # Existing silver runs (DE then UK)
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

- [ ] **Step 2: Verify Dagster definitions load cleanly**

Run: `uv run python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest tests/ -v`
Expected: 118 tests pass (110 baseline + 8 new reconcile tests)

- [ ] **Step 4: Commit**

```bash
git add src/tcg_platform/defs/eu_pipeline_orchestrator.py
git commit -m "feat(orchestrator): wire reconcile_quarantine before silver_de/uk"
```

---

## Task 13: Final validation

**Files:** None (verification step)

- [ ] **Step 1: Run all tests one last time**

Run: `uv run pytest tests/ -v`
Expected: 118 tests, all passing

- [ ] **Step 2: Verify no untracked garbage**

Run: `git status --porcelain`
Expected: empty output (working tree clean)

- [ ] **Step 3: Show the full diff against main**

Run: `git log --oneline main..HEAD`
Expected: 9 commits (the spec commits from before this plan, plus the 7 implementation commits from Tasks 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12 — some tasks don't have commits, so count is between 9 and 13)

- [ ] **Step 4: Confirm Dagster definitions load cleanly**

Run: `uv run python -c "from tcg_platform.definitions import defs; print('OK')"`
Expected: `OK`

- [ ] **Step 5: No commit needed** (verification only)

---

## Verification Notes

After all tasks complete and the branch is pushed:

1. **Manual integration smoke test (per spec Section 5):**
   - Find a real quarantined file in `tcg-silver/quarantine/{de,uk}/` whose `card_id` corresponds to a card we *know* is in the cards folder (e.g. `OP01-001`).
   - Run `reconcile_quarantine_de_job` from Dagster UI → confirm the file is deleted.
   - Run `silver_de_pipeline` → confirm `data/de/{event_id}.parquet` is created.
   - Verify quarantine file is gone and data file exists with the expected content.

2. **Out of scope** (per spec): cleaning up historical quarantined rows with permanently-invalid card_ids, TTL on quarantined files, the 14 `failed_card_ids` from `sync_card_images`, the bronze `cardlist` parquet writer for Limitless, the silver `is_valid_card_id` path bug.

3. **No PR until** the user reviews and merges per the AGENTS.md git cycle.

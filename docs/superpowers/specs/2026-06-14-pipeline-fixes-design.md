# EU Pipeline Bronze Orchestrator Parallelization + MinIO Clock Watchdog

**Date:** 2026-06-14
**Status:** Draft
**Branch:** `2026-06-14-pipeline-fixes`

## Problem

Two independent symptoms, one user-reported session.

### Symptom 1 (today, 2026-06-14): complete_eu_pipeline fails with `RequestTimeTooSkewed`

Running `complete_eu_pipeline` (or any sub-job that initializes the MinIO resource) fails
immediately on the first `bucket_exists` call:

```
minio.error.S3Error: S3 operation failed; code: RequestTimeTooSkewed,
message: The difference between the request time and the server's time is too large.
```

Reproduction: `python -c "from tcg_platform.definitions import defs; defs.load_fn().resolve_job_def('ebay_de_raw_to_bronze').execute_in_process(raise_on_error=False)"`.

Root cause: MinIO server (podman container `minio`, image `docker.io/minio/minio:latest`,
running on this host) had a system clock ~18h 41m behind the host clock. S3 SDK rejects
requests where local time and server time differ by more than ~15 minutes.

The MinIO container's clock has since resynced to the host (verified 2026-06-14 19:45 CEST,
`podman exec minio date -u` == `date -u`). The error no longer reproduces. The
container clock can drift again if the host sleeps / resumes or the container is
paused, so the issue is recurring.

### Symptom 2 (yesterday, 2026-06-13): bronze_eu_orchestrator "stuck" for an hour

User reported that `complete_eu_pipeline` ran indefinitely with no new raw HTML being
written for an hour. Dagster UI showed `bronze_eu_orchestrator` as "in progress".

Likely root cause: the original 2026-06-02 orchestrator spec called for
"DE + UK eBay scrape + write to SQLite … in parallel", but the implementation
(`src/tcg_platform/defs/eu_pipeline_orchestrator.py:17-19`) calls
`execute_in_process` **sequentially**:

```python
result_de   = job_def_de.execute_in_process(instance=context.instance)
result_uk   = job_def_uk.execute_in_process(instance=context.instance)
result_rates = job_def_rates.execute_in_process(instance=context.instance)
```

The log message on line 16 says "in parallel" but the calls are sequential. If DE
sits in a slow Zyte API fetch loop (e.g., during rate-limit back-off or while
processing many already-seen items), UK does not start. The orchestrator asset
appears stuck in the UI even though the underlying `execute_in_process` is
working.

## Goals

1. Make `bronze_eu_orchestrator` actually run its 3 sub-jobs in parallel, as the
   2026-06-02 spec required.
2. Add a clock-skew watchdog for the MinIO container so the `RequestTimeTooSkewed`
   error is caught early, surfaced loudly, and documented.

## Non-goals

- No change to the silver orchestrator's structure (`silver_eu_orchestrator` has 4
  sub-jobs, currently sequential; same issue exists but the user did not report it
  and the spec at 2026-06-02 was less explicit about silver parallelism).
- No change to the MinIO client / resource code itself.
- No change to the Zyte retry / back-off logic.
- No general refactor of `eu_pipeline_orchestrator.py` beyond the parallelism fix.

## Design

### Change 1: Parallelize `bronze_eu_orchestrator`

**File:** `src/tcg_platform/defs/eu_pipeline_orchestrator.py`

Replace lines 17–19 (the three sequential `execute_in_process` calls) with a
`concurrent.futures.ThreadPoolExecutor(max_workers=3)` that submits all three
jobs and gathers the results. Fail-fast: if any sub-job raises, the executor
cancels pending submissions and re-raises. Log start + finish of each sub-job
by name with `run_id`.

Sketch:

```python
import concurrent.futures

with concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix="eu-bronze") as ex:
    futures = {
        ex.submit(job_def_de.execute_in_process, instance=context.instance): "de",
        ex.submit(job_def_uk.execute_in_process, instance=context.instance): "uk",
        ex.submit(job_def_rates.execute_in_process, instance=context.instance): "rates",
    }
    results: dict[str, ExecuteInProcessResult] = {}
    for fut in concurrent.futures.as_completed(futures):
        name = futures[fut]
        try:
            r = fut.result()
        except Exception as e:
            context.log.error(f"{name} sub-job failed: {e}")
            raise
        results[name] = r
        context.log.info(f"{name} sub-job complete, run_id={r.run_id}")
```

`context.instance` is a `DagsterInstance` object. It is documented as
thread-safe for `execute_in_process` (the in-process execution path is just
Python function calls into a local in-memory event log). SQLite-backed event
logs serialize writes internally, so concurrent submissions are safe. The
`minio_client` and `tcg_raw_client` resources are created **per-step** by
Dagster's executor (each `execute_in_process` creates its own step
subprocess), so they don't share state across sub-jobs.

**Surface area:** 1 file, ~15 lines changed, the asset body.

### Change 2: MinIO clock watchdog

**New file:** `scripts/check_minio_clock.sh` — bash script that:
1. `curl -sI http://localhost:9000/minio/health/live` (override URL via
   `MINIO_ENDPOINT` env var).
2. Read the `Date:` response header.
3. Compare to `date -u` (with a Python one-liner for portable date math).
4. Print one of:
   - `OK` if skew < 60s
   - `WARN  skew=Xs` if 60s ≤ skew < 300s
   - `FAIL  skew=Xs` if skew ≥ 300s
5. Exit codes:
   - `0` on `OK`
   - `0` on `WARN` (visible warning, not a hard fail — caught by the test as a `pytest.warn`)
   - `1` on `FAIL`
   - `0` on `SKIP` (MinIO unreachable; pytest will skip the test)
6. If MinIO is unreachable (curl non-200 or connection error), print
   `SKIP  minio_unreachable` and exit 0 (don't fail pytest in CI).

**New file:** `tests/test_minio_clock_skew.py` — pytest module that:
1. Has a session-level fixture that runs `scripts/check_minio_clock.sh`
   via `subprocess.run` and captures stdout.
2. `pytest.skip` if the script printed `SKIP` (MinIO not running).
3. `pytest.fail` if the script printed `FAIL` (or exited 1).
4.    `pytest.warn(UserWarning)` if the script printed `WARN` (does not fail).
   `pytest.skip` if the script printed `SKIP` (MinIO not running).

The test is in `tests/` so `pytest tests/` runs it; no new config.

**Docs:** add a one-paragraph note to `PROD.md` under the M9 section
(describing the clock-skew failure mode + how to run the check manually).

## File changes

| File | Change |
|------|--------|
| `src/tcg_platform/defs/eu_pipeline_orchestrator.py` | Replace 3 sequential `execute_in_process` calls with `ThreadPoolExecutor(max_workers=3)` |
| `scripts/check_minio_clock.sh` | New — MinIO clock skew check |
| `tests/test_minio_clock_skew.py` | New — pytest wrapper around the script |
| `PROD.md` | Add note about MinIO clock-skew failure mode + the check script |
| `log/SESSION_2026-06-14.md` | New — session log |

## Test plan

1. `pytest tests/defs/test_eu_pipeline_orchestrator.py` (new) — unit test for
   the parallel orchestrator. Use a stub `JobDefinition` whose
   `execute_in_process` sleeps 0.5s and returns a `MagicMock(run_id="...")`.
   Assert that:
   - All 3 sub-jobs are submitted.
   - Total wall time is < 1.0s (i.e., they ran in parallel, not 1.5s sequential).
   - Results are returned with the right run_ids.

2. `pytest tests/test_minio_clock_skew.py` — see above.

3. `pytest tests/` — full suite must still pass (156 → 158 tests).

## Verification (Rule 17)

Before pushing the branch:
- `pytest tests/ -v` — all green
- `python -c "from tcg_platform.definitions import defs; print('OK')"` — definitions load
- Run the script manually against the running MinIO: `./scripts/check_minio_clock.sh`
- `bash scripts/check_minio_clock.sh` — must print `OK`
- `git status --porcelain` — clean

## Success criteria

- `bronze_eu_orchestrator` actually runs its 3 sub-jobs concurrently
  (measurable: total wall time ≈ max(sub-job wall time), not the sum).
- `pytest tests/test_minio_clock_skew.py` passes against the running MinIO
  container.
- A 5-minute artificial clock skew injected into the MinIO container makes
  `tests/test_minio_clock_skew.py` fail.
- `complete_eu_pipeline` runs end-to-end against real MinIO + Zyte with
  no errors.
- All existing 156 tests still pass.

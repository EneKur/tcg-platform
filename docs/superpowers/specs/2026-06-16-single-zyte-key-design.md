# Single Zyte API Key + Per-Page Exception Tolerance

**Date:** 2026-06-16
**Status:** Approved
**Branch:** `2026-06-16-single-zyte-key`

## Problem

The operator has changed operational policy: **one Zyte API key, managed externally.** The current code in `src/tcg_platform/defs/zyte_resources.py` is built around multi-key rotation (keys 1..99 read from `ZYTE_API_KEY1`, `ZYTE_API_KEY2`, etc.) with dead-key marking and a "rotate to next key on timeout" retry loop. With a single key, all of that machinery is dead weight and, worse, actively misleads the operator.

### The "timed out" mislabel

When a Zyte call returns a 4xx (e.g. 403 from a dead-for-the-month key), the aiohttp 3.13 cross-loop bug fires `RuntimeError("Timeout context manager should be used inside a task")` *before* the response is processed. The current `_try_get` (lines 93-112) catches this `RuntimeError` and **mistakes it for a timeout**, then the rotation loop in `get()` (lines 144-150) **rotates the key and marks it dead**. With three keys, the user sees:

> `RuntimeError: All Zyte API keys timed out (KEY1, KEY2, KEY3) at hard_timeout=120.0s. Zyte may be unresponsive or the network is dropping packets.`

The real cause: KEY1 returned 403. The message prevents the operator from finding the dead key. With a single-key model, this mislabeling is the **only** error path the operator ever sees — so it must be correct.

### The per-call crash

The scraper (`src/tcg_platform/defs/scrape_raw.py:_scrape_region`) handles `statusCode != 200` from Zyte by logging `STOP` and breaking (good). But it does **not** catch Zyte SDK exceptions (timeouts, `RequestError`). When a Zyte call times out or returns a 4xx mid-scrape, the exception propagates and **crashes the whole scrape asset**. The 2026-06-14 and 2026-06-15 session logs show the user hitting this repeatedly: a single hung page kills the whole `complete_eu_pipeline`, with no bronze/silver work done.

## Goal

1. **One Zyte API key** (`ZYTE_API_KEY`, singular), no rotation, no dead-key machinery.
2. **Distinct, accurate error messages** for each failure mode: hard timeout vs. cross-loop timeout vs. Zyte 4xx vs. Zyte 5xx.
3. **Per-page / per-item exception tolerance** in the scraper: a single Zyte hiccup logs `STOP` or `FAIL` with the real error and continues — does not crash the run.
4. **Hard caps** on the scraper: `MAX_PAGES_PER_REGION=20`, `MAX_WALL_CLOCK_S=900` (15 min), so a runaway eBay category can't burn the API budget.
5. **Per-page heartbeat** in the asset's `MaterializeResult.metadata`, so the Dagster UI shows live progress.
6. **Lowered hard timeout** `ZYTE_API_TIMEOUT=60s` (was 120s) — verified against real Zyte, normal pages return in ~5s.

## Non-goals

- No change to the orchestrator's nested `execute_in_process` design. With a working key, it should now actually run cleanly; the user can decide later whether to refactor.
- No change to `silver_eu_orchestrator`'s 4 sequential calls.
- No change to bronze / silver / backfill / reconciliation logic.
- No change to M3/M4/M5/M6/M6.5/M7/M8 production code beyond the two files in this spec.

## Design

### Change 1: `ZyteSessionResource` becomes a single-key resource

**File:** `src/tcg_platform/defs/zyte_resources.py`

- Constructor: `__init__(self, api_key: str, n_conn: int = 2, max_retries: int = 3, api_timeout: float = 60.0)`. Drops `api_keys: list[str]`.
- Internal state: `self._client = ZyteAPI(api_key=api_key, n_conn=n_conn)`. Drops `_clients: list`, `_key_names`, `_dead_keys: set`.
- `_try_get(self, request)` (single key, no rotation): retries the same client up to `max_retries + 1` times on transient errors. The current retry logic for 5xx/429 stays. The cross-loop `RuntimeError` catch stays (still needed for the aiohttp bug), but it raises a **distinct** exception class.
- `get(self, request)` becomes a thin wrapper: call `_try_get`, on exception wrap with a clear message indicating the **real** failure mode. Drop the rotation loop, `tried_keys`, `last_was_timeout`, "all keys exhausted" / "all keys timed out" branches.
- Exception types: introduce a small set of named exception classes (or use plain `RuntimeError` with a distinctive message prefix) so tests can assert on the failure mode:
  - `ZyteTimeoutError(RuntimeError)` — call exceeded `api_timeout`.
  - `ZyteRequestError(RuntimeError)` — Zyte returned 4xx (carries `.status: int`, `.body: bytes`).
  - `ZyteServerError(RuntimeError)` — Zyte returned 5xx after retries (carries `.status`).
  - `ZyteCrossLoopError(RuntimeError)` — aiohttp cross-loop `RuntimeError` (carries `.original: Exception`).
- `close()` stays.
- `_read_api_keys()` renamed to `_read_api_key()`: returns a single string from `os.environ["ZYTE_API_KEY"]`. Raises `ValueError` if missing.

### Change 2: scraper per-page heartbeat + hard caps + exception tolerance

**File:** `src/tcg_platform/defs/scrape_raw.py`

- Add module-level constants: `MAX_PAGES_PER_REGION = 20`, `MAX_WALL_CLOCK_S = 900`.
- New helper `_format_log_line(level, **fields) -> str` to keep log format consistent.
- `_scrape_region(...)`:
  - Add `start_monotonic = time.monotonic()`.
  - **Page loop**: wrap the `zyte_client.get({...})` call in `try/except Exception as e`. On exception, log `STOP search_page={page} exc={type(e).__name__}: {str(e)[:200]}` and break. The exception does **not** propagate.
  - **Item loop**: wrap each `zyte_client.get({...})` call in `try/except Exception as e`. On exception, log `FAIL zyte_exc event_id={event_id} exc={type(e).__name__}: {str(e)[:200]}`, increment a counter, and `continue` (do not crash).
  - **Page heartbeat**: at the top of each page iteration, log a line: `HEARTBEAT search_page={page} elapsed_s={int(elapsed)} pages_fetched={pages_fetched} items_seen={items_seen}`. Emit it BEFORE the network call so the operator sees the page is in flight.
  - **Max pages check**: after `page += 1`, if `page > MAX_PAGES_PER_REGION`, log `STOP max_pages pages_fetched={pages_fetched}` and break.
  - **Max wall clock check**: at the top of the loop, if `time.monotonic() - start_monotonic > MAX_WALL_CLOCK_S`, log `STOP max_wall_clock elapsed_s={elapsed}` and break.
  - **New counters** in the returned counts dict: `pages_timeout`, `items_timeout`, `max_pages_stopped` (bool), `max_wall_clock_stopped` (bool), `wall_clock_seconds`. These flow into the asset's `MaterializeResult.metadata`.
- `scrape_ebay_de_raw` and `scrape_ebay_uk_raw`: pass `counts` (renamed) into `dg.MaterializeResult(metadata=counts)`.

### Change 3: env-var surface

**File:** `.env.example`

- Replace `ZYTE_API_KEY1`, `ZYTE_API_KEY2`, `ZYTE_API_KEY3` with `ZYTE_API_KEY`.
- Replace `ZYTE_API_TIMEOUT=120` with `ZYTE_API_TIMEOUT=60`.

**File:** `PROD.md` lines 191-202 (Environment Variables section) and lines 153 (operational note)

- Update env-var list.
- Update operational note text.

### Change 4: tests

**File:** `tests/defs/test_zyte_session_resource.py`

Delete (concepts gone):

- `test_key_rotation_on_exhausted_retries`
- `test_all_keys_exhausted_raises`
- `test_key_rotation_on_timeout`
- `test_all_keys_timed_out_surfaces_timeout_error`
- `test_read_api_keys_picks_up_zyte_api_key1`
- `test_key_402d_earlier_is_not_retried_again_this_run`

Update:

- `test_no_retry_on_4xx` — rename to `test_4xx_surfaces_as_zyte_request_error`. A 4xx from Zyte should now raise `ZyteRequestError(status=403)`, not return a `result` dict.
- `test_max_retries_respected` — change expected exception message from "All Zyte API keys exhausted" to "Zyte max retries (N) exhausted".
- `test_aiohttp_cross_loop_runtime_error_treated_as_timeout` — change expected exception class to `ZyteCrossLoopError`.
- `test_get_session_works_with_real_aiohttp_313` — change constructor to `api_key=...` (singular).
- `test_get_session_returns_same_session_across_calls` — same.
- `test_get_returns_within_hard_timeout_when_zyte_call_hangs` — same constructor; expected exception class `ZyteTimeoutError`.
- `test_get_retries_within_a_key_after_hard_timeout` — same constructor; keep the assertion (still meaningful — single-key retry on timeout).

Add:

- `test_read_api_key_singular` — `_read_api_key()` reads `ZYTE_API_KEY` from env, returns the string. Raises `ValueError` if missing.
- `test_4xx_status_403_surfaces_as_zyte_request_error_with_status` — assert `.status == 403`.
- `test_5xx_after_retries_surfaces_as_zyte_server_error` — 500 on every call raises `ZyteServerError(status=500)`.
- `test_cross_loop_runtime_error_surfaces_as_zyte_cross_loop_error` — the aiohttp cross-loop bug raises `ZyteCrossLoopError`, **not** a generic `TimeoutError`.
- `test_constructor_accepts_single_api_key` — `ZyteSessionResource(api_key="...")` works; `api_keys=[...]` raises `TypeError`.

**File:** `tests/scraping/test_scrape_raw.py`

Add (using existing fixture pattern):

- `test_scrape_region_continues_after_zyte_timeout_on_search_page` — a `RuntimeError` on the first search-page call logs `STOP ... exc=...` and breaks; the function returns `([], log)` with the `STOP` line in the log.
- `test_scrape_region_continues_after_zyte_timeout_on_item_page` — a `RuntimeError` on a per-item call logs `FAIL ... exc=...` and continues; other items are still fetched and written.
- `test_scrape_region_respects_max_pages` — with `MAX_PAGES_PER_REGION=3`, only 3 pages are fetched before `STOP max_pages`.
- `test_scrape_region_respects_max_wall_clock` — with `MAX_WALL_CLOCK_S=0.1`, the scraper stops after the first slow iteration with `STOP max_wall_clock`.
- `test_scrape_region_emits_heartbeat_per_page` — verify a `HEARTBEAT` log line per iteration with the right fields.

**File:** `tests/defs/test_definitions_load.py` — no change (still loads).

## Success criteria

- `pytest tests/ -v` — 100% pass.
- `python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"` — loads clean.
- `bash scripts/check_minio_clock.sh` — OK.
- **Live smoke**: run `ebay_de_raw_to_bronze` against real Zyte for 1 page + 1 item, verify:
  - The first call returns 200 in ~5s.
  - The asset's `MaterializeResult.metadata` includes `pages_fetched=1`, `items_seen=N`, `elapsed_s=...`, `written=1`.
  - The raw HTML file appears in `tcg-raw/ebay/DE/...`.
- `git status --porcelain` — clean.
- No work on `main`.

## Risks

- The cross-loop `RuntimeError` is being treated as a separate error class. If the aiohttp version changes (currently pinned via the venv), the exact `RuntimeError` message may change. The fix uses `"Timeout context manager" in str(e)` substring check (preserved from prior session), so it should remain robust.
- Lowering the timeout to 60s may fire on cold-cache eBay pages. Verified: real eBay search page returned in 5.3s. 60s gives 10x headroom.
- The orchestrator's nested `execute_in_process` is still there; if the inner sub-job raises an exception, the parent asset will fail. The new per-page tolerance means the inner job rarely raises now, but it can still raise on init errors (e.g. ZyteSessionResource init). That stays as-is.

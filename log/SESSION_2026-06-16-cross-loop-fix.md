# Session 2026-06-16 (cross-loop fix)

## Branch
`2026-06-16-zyte-cross-loop-fix`

## Goal

User reported that after PR #24 was merged, `complete_eu_pipeline`
"went green almost instantly" (~20s) but produced **no new items
in tcg-raw**. This was a regression — the pipeline was passing but
the scraper was a no-op.

## Done

### Diagnosis (systematic-debugging skill)

Inspected the 4 `tcg-raw/logs/2026-06-16-*.log` files written today.
All 4 showed the same pattern:

```
HEARTBEAT search_page=1 elapsed_s=0.0 pages_fetched=0 items_seen=0
FETCH search_page=1 url=https://www.ebay.de/sch/...&_pgn=1
STOP search_page=1 exc=ZyteCrossLoopError: Zyte call hit aiohttp
  cross-loop bug: Timeout context manager should be used inside a
  task. The Zyte SDK + aiohttp 3.13 session/loop mismatch; Zyte is
  not actually unresponsive.
END region=DE pages_fetched=0 ... written=0
```

The asset materialization **succeeded** but `pages_timeout=1, written=0`.
A real eBay page was never parsed, no items were written, and the
backfill/silver steps found nothing to process.

This was the **known follow-up from the prior session log** (2026-06-16,
PR #24) — explicitly flagged as a recommended next step:

> "The aiohttp 3.13 cross-loop bug fires on the first Zyte call in
> this environment because the resource pre-creates the
> `aiohttp.ClientSession` on one loop and uses it from another.
> **Recommended follow-up:** drop the pre-created session from the
> resource, let the SDK create its own per call, rely on the
> Python-level `future.result(timeout=api_timeout)` as the only
> hard timeout."

### Reproduction + verification (per user request)

Per the user's "first you test the zyte API functionality to fetch
html by actually fetching a test ebay-page, defined with all its
filters etc. like our scraper is intended to work", I fetched the
**exact** eBay URL the scraper uses (PSA 10, sold only, TCG
category, English-only, DE local, sort by newest) and tested 4
approaches:

| Approach | Result |
|---|---|
| Direct async call, NO pre-created session (Approach 1) | ✅ 200 OK in 5.6s (DE), 6.5s (UK), ~1.3-1.5MB HTML |
| Pre-created session in a persistent background loop (v5) | ❌ `RuntimeError: Timeout context manager should be used inside a task` immediately (no real Zyte call even attempted) |
| Real `ZyteSessionResource` (current production code, v3) | ❌ `ZyteCrossLoopError` after 3.0s, **3/3 calls fail** |
| Minimal `FixedResource` with NO pre-created session (v6) | ✅ 200 OK in 5-6s, **3/3 calls succeed** |

**The fix path is verified:** drop the pre-created session, let the
SDK create its own per call, rely on `future.result(timeout=api_timeout)`
as the only hard timeout.

### Spec & plan
- `docs/superpowers/specs/2026-06-16-zyte-cross-loop-fix-design.md`
- `docs/superpowers/plans/2026-06-16-zyte-cross-loop-fix.md`

### Implementation (TDD red → green)

#### Task 1: regression-guard test + obsolete test cleanup (TDD red)

**File:** `tests/defs/test_zyte_session_resource.py`

Added `test_get_does_not_create_aiohttp_session` that patches
`aiohttp.ClientSession` to raise on construction, then calls
`ZyteSessionResource(...).get(...)`. With the fix in place
(no pre-created session), the test passes. Without the fix
(i.e., the pre-fix production code), the test fails because the
constructor raises. **Verified TDD red on the pre-fix code.**

Also deleted 3 obsolete tests that pinned the pre-created-session
behavior:
- `test_get_session_works_with_real_aiohttp_313`
- `test_get_session_returns_same_session_across_calls`
- `test_session_has_configured_timeout`

Updated `test_handle_retries_false_passed_to_zyteapi` to drop
the `aiohttp.ClientSession` mock.

#### Task 2: refactor `zyte_resources.py` (TDD green)

**File:** `src/tcg_platform/defs/zyte_resources.py`

- Removed `_session`, `_get_session()`, `close()`.
- Removed `session=self._get_session()` argument from `_try_get`.
- Removed the `import aiohttp` (no longer needed).
- The cross-loop `RuntimeError` catch stays in `_try_get` for
  defense-in-depth: any future regression that re-introduces a
  pre-created session will surface as `ZyteCrossLoopError`
  (loud and clear) instead of a silent hang.
- Updated module docstring to explain the new timeout model.

**Re-ran pytest** — 18/18 tests in the resource file pass,
including the new regression-guard.

**Full suite:** 174/174 pass (was 176 before; -3 from deleted
obsolete tests, +1 from new regression-guard = net -2).

#### Task 3: PROD.md operational note

**File:** `PROD.md` line 153

Updated the operational note to reflect the new timeout model:
"lets the Zyte SDK create its own short-lived `aiohttp.ClientSession`
per call (no pre-created session — that triggered the aiohttp 3.13
cross-loop bug) AND applies a hard Python-level `future.result(timeout=ZYTE_API_TIMEOUT)`
on every Zyte call".

### End-to-end verification (Rule 17)

- `pytest tests/ -v` — **174/174 pass** in 9.40s
- `python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"` — **OK**
- `bash scripts/check_minio_clock.sh` — **OK skew=0s**
- `git status --porcelain` — clean (no work on main)

### Live smoke test — REAL eBay scrape

`dg.materialize(assets=[scrape_ebay_de_raw])` against real Zyte:

| Metric | Before fix (today) | After fix |
|---|---|---|
| `pages_timeout` | 1 (cross-loop) | 0 |
| `wall_clock_seconds` | 3.0 (cross-loop 1 call) | 63.0 (real Zyte calls) |
| `pages_fetched` | 0 | 7 |
| `items_seen` | 0 | 204 |
| `items_skipped_already_seen` | 0 | 201 |
| `items_fetched_zyte` | 0 | **3** (new items!) |
| `images_downloaded` | 0 | **3** |
| **`written`** | **0** | **3** ✅ |

`tcg-raw/ebay/DE/`: 138 files → **141 files** (+3 new):
- `168463872185.html` (1.6MB, sold 16 Jun 2026)
- `227388254436.html` (1.6MB, sold 16 Jun 2026)
- `327211403820.html` (1.6MB, sold 16 Jun 2026)

`tcg-raw/sold_images/DE/`: 74 files → **77 files** (+3 new):
- `168463872185.jpg`, `227388254436.jpg`, `327211403820.jpg`

**The pipeline now writes real items to tcg-raw.** The 3 new
items were the ones I identified as "new" via direct eBay fetch
during the verification step (sold 16 Jun 2026, sold within the
last 24 hours).

### AGENTS.md housekeeping

- `main` was in sync with `origin/main` at session start.
- Created session branch from main (Rule 15). No work on main.
- TDD red → green verified for the regression-guard test
  (Rule 9).
- Full suite green after the fix (Rule 17).
- 2 commits planned (spec+plan, code+tests+log), pushed to
  remote branch, not merged (Rule 19).

## Outstanding (post-this-PR)

- **First-call flakiness observed during the live smoke.** The
  first run of the scraper in `dg.materialize` showed
  `PARSED search_page=1 items=0` (then page 2 found 60 items).
  The second run (the one with `written=3`) parsed page 1
  correctly. The cause is unknown; the resource, parser, and
  URL are identical between runs. Possibly a transient Zyte
  response variation, or possibly a cold-start effect on
  the aiohttp session. Not blocking — the fix works end-to-end
  and writes real items.

- **The orchestrator's nested `execute_in_process`** is still
  load-bearing. With a working scraper, the inner sub-job now
  succeeds; init errors (e.g. ZyteSessionResource init) still
  propagate through the nested execute. Out of scope.

- **silver_eu_orchestrator** still has 4 sequential
  `execute_in_process` calls. Same factory pattern would apply
  for a follow-up. Not in scope.

- **M5-T2 (Dagster schedules)** still deferred.

## Blockers
None.

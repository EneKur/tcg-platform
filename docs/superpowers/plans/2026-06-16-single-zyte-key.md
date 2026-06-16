# Single Zyte API Key — Implementation Plan

**Spec:** `docs/superpowers/specs/2026-06-16-single-zyte-key-design.md`
**Branch:** `2026-06-16-single-zyte-key`
**Date:** 2026-06-16

## Tasks (TDD: red → green → refactor for each)

### Task 1 — Refactor `ZyteSessionResource` to single-key + distinct exception types

**Files:** `src/tcg_platform/defs/zyte_resources.py`

**Test first (red):** Update `tests/defs/test_zyte_session_resource.py`:
- Change all `api_keys=[...]` constructor calls to `api_key="..."`.
- Update the 6 tests that pin rotation behavior: assert they raise `ZyteRequestError` / `ZyteTimeoutError` / `ZyteCrossLoopError` instead of the old `RuntimeError("All Zyte API keys...")` strings.
- Delete: `test_key_rotation_on_exhausted_retries`, `test_all_keys_exhausted_raises`, `test_key_rotation_on_timeout`, `test_all_keys_timed_out_surfaces_timeout_error`, `test_read_api_keys_picks_up_zyte_api_key1`, `test_key_402d_earlier_is_not_retried_again_this_run`.
- Add: `test_constructor_accepts_single_api_key`, `test_4xx_surfaces_as_zyte_request_error_with_status`, `test_5xx_after_retries_surfaces_as_zyte_server_error`, `test_cross_loop_runtime_error_surfaces_as_zyte_cross_loop_error`, `test_read_api_key_singular`.

**Then run `pytest tests/defs/test_zyte_session_resource.py` — expect failures on the updated tests.**

**Then implement (green):** Refactor `src/tcg_platform/defs/zyte_resources.py`:
- Drop multi-key state, rotation loop, dead-key set.
- Add 4 named exception classes: `ZyteTimeoutError`, `ZyteRequestError`, `ZyteServerError`, `ZyteCrossLoopError`.
- `get(request)` raises the appropriate exception on each failure mode.
- `_read_api_key()` reads `os.environ["ZYTE_API_KEY"]` (singular).
- Factory `zyte_session_resource` reads `ZYTE_API_KEY` (singular) and `ZYTE_API_TIMEOUT=60` (lowered from 120).

**Re-run pytest** — expect all updated + new tests to pass.

### Task 2 — Scraper per-page heartbeat + hard caps + exception tolerance

**File:** `src/tcg_platform/defs/scrape_raw.py`

**Test first (red):** Add tests to `tests/scraping/test_scrape_raw.py`:
- `test_scrape_region_continues_after_zyte_timeout_on_search_page`
- `test_scrape_region_continues_after_zyte_timeout_on_item_page`
- `test_scrape_region_respects_max_pages`
- `test_scrape_region_respects_max_wall_clock`
- `test_scrape_region_emits_heartbeat_per_page`

**Then run `pytest tests/scraping/test_scrape_raw.py` — expect the 5 new tests to fail.**

**Then implement (green):** Refactor `_scrape_region`:
- Add `MAX_PAGES_PER_REGION = 20`, `MAX_WALL_CLOCK_S = 900`, `import time` at top.
- Add per-page `HEARTBEAT` log line.
- Wrap `zyte_client.get(...)` in `try/except` for both loops.
- Add max-pages and max-wall-clock checks.
- New counters: `pages_timeout`, `items_timeout`, `max_pages_stopped`, `max_wall_clock_stopped`, `wall_clock_seconds`.
- Pass `counts` dict into `MaterializeResult(metadata=counts)`.

**Re-run pytest** — expect all new tests to pass.

### Task 3 — Update env-var docs

**Files:** `.env.example`, `PROD.md`

- `.env.example`: rename `ZYTE_API_KEY1/2/3` → `ZYTE_API_KEY`, `ZYTE_API_TIMEOUT=120` → `ZYTE_API_TIMEOUT=60`.
- `PROD.md` lines 191-202 (env vars) and 153 (operational note): update accordingly.

No tests for this — it's doc-only.

### Task 4 — End-to-end verification

```bash
pytest tests/ -v                            # expect 100% pass
python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"  # expect OK
bash scripts/check_minio_clock.sh           # expect OK
git status --porcelain                      # expect clean
```

Then live smoke: 1 eBay DE page + 1 item page against real Zyte. Verify:
- Real 200 response in ~5s.
- `MaterializeResult.metadata` has all counters.
- Raw HTML file written to `tcg-raw/ebay/DE/...`.

### Task 5 — Session log + commit

- `log/SESSION_2026-06-16.md` — what was done, what was verified, what remains.
- `git add` + `git commit` (one commit per task, 2-3 commits total).
- `git push origin 2026-06-16-single-zyte-key`.
- **Do NOT merge to main.** Human-driven per AGENTS.md Rule 19.

## Dependencies

- No new packages.
- `tests/scraping/test_scrape_raw.py` already exists with helpers (mock `zyte_client`, real fixture HTML). The new tests reuse the existing pattern.

## Risks

- The cross-loop `RuntimeError` substring check ("Timeout context manager") may be brittle across aiohttp versions. Currently aiohttp 3.13.x is pinned via `pyproject.toml` / `uv.lock`. The substring check is preserved from the prior session's work.
- 60s timeout may be too short for first-of-day cold-cache eBay pages. Verified: 5.3s for a real page. 60s is 10x headroom.
- The orchestrator's nested `execute_in_process` may still surface confusing errors. Out of scope; tracked as future work.

## Done when

- All 4 tasks complete.
- pytest 100% pass.
- Live smoke test produces a valid raw HTML file in `tcg-raw/ebay/DE/`.
- Session log written.
- Branch pushed, not merged.

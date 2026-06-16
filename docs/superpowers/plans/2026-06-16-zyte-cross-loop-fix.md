# Zyte Session Loop Fix — Implementation Plan

**Spec:** `docs/superpowers/specs/2026-06-16-zyte-cross-loop-fix-design.md`
**Branch:** `2026-06-16-zyte-cross-loop-fix`
**Date:** 2026-06-16

## Tasks (TDD red → green for each)

### Task 1 — Add regression-guard test for the fix

**File:** `tests/defs/test_zyte_session_resource.py`

**Test (red):** Add `test_get_does_not_create_aiohttp_session` that
patches `aiohttp.ClientSession` to raise on construction, then
calls `ZyteSessionResource(...).get(...)`. With the fix in place
(no pre-created session), the test passes. Without the fix (i.e.,
if the resource pre-creates a session), the test fails because the
constructor raises.

Also DELETE the obsolete tests:

- `test_get_session_works_with_real_aiohttp_313`
- `test_get_session_returns_same_session_across_calls`
- `test_session_has_configured_timeout`

And UPDATE `test_handle_retries_false_passed_to_zyteapi` — the
call now has no `session=` arg.

**Run pytest** — expect the new test to fail (TDD red), and the
3 deleted tests to be removed cleanly.

### Task 2 — Refactor `ZyteSessionResource` to drop the pre-created session

**File:** `src/tcg_platform/defs/zyte_resources.py`

- Remove `_session`, `_get_session()`, `close()`.
- Remove `session=self._get_session()` argument from `_try_get`.
- The cross-loop `RuntimeError` catch stays for defense in depth.

**Run pytest** — expect all tests to pass (TDD green).

### Task 3 — Update PROD.md operational note

**File:** `PROD.md` line 153

Update the operational note to reflect the new timeout model (no
`aiohttp.ClientSession` injection; Python-level `future.result`
is the only hard timeout).

No tests for this.

### Task 4 — End-to-end verification

```bash
pytest tests/ -v                            # expect 100% pass
python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"
bash scripts/check_minio_clock.sh           # expect OK
git status --porcelain                      # expect clean
```

Then live smoke: `dg.materialize(assets=[scrape_ebay_de_raw])`
against real Zyte. Verify:
- First call returns 200 in ~5s.
- `MaterializeResult.metadata` includes `pages_fetched>=1, items_seen>=1, written>=1`.
- A raw HTML file is written to `tcg-raw/ebay/DE/`.

### Task 5 — Session log + commit + push + open PR

- `log/SESSION_2026-06-16-cross-loop-fix.md` — what was done, what
  was verified, what remains.
- `git add` + `git commit` (2-3 commits: spec+plan, code+tests,
  log).
- `git push origin 2026-06-16-zyte-cross-loop-fix`.
- `gh pr create` with the spec's success criteria as the PR body.
- **Do NOT merge to main.** Human-driven per AGENTS.md Rule 19.

## Dependencies

- No new packages.
- `aiohttp` stays as a dev dependency (for the existing aiohttp 3.13
  ClientSession test pattern in the deleted tests — but those tests
  are being removed, so the dep is no longer needed in tests; keep
  it for now in case other tests use it).

## Risks

- The Python-level `future.result(timeout=60)` is the only hard
  timeout. If a Zyte request hangs but doesn't raise, the
  ThreadPoolExecutor's `future.result` will return after 60s with a
  `TimeoutError`. Acceptable.
- The SDK's internal aiohttp session is short-lived; socket cleanup
  is the SDK's responsibility. No leak.
- The cross-loop catch stays in the code as defense-in-depth. Any
  future regression that re-introduces a pre-created session will
  surface as `ZyteCrossLoopError` (loud and clear) instead of a
  silent hang.

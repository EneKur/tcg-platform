# Zyte Session Loop Fix — Drop the Pre-Created aiohttp Session

**Date:** 2026-06-16
**Status:** Approved
**Branch:** `2026-06-16-zyte-cross-loop-fix`

## Problem

After PR #24 was merged and the operator ran `complete_eu_pipeline`,
**every scraper sub-job hit the aiohttp 3.13 cross-loop bug on the
first Zyte call** and wrote zero items. The pipeline "succeeded" in
~20 seconds (most sub-jobs are no-ops; the 2 scrapes take 3s each
because the cross-loop bug fires immediately and is then caught and
re-raised as `ZyteCrossLoopError`). Verified by reading the 4
`tcg-raw/logs/2026-06-16-*.log` files written today — every one shows:

```
HEARTBEAT search_page=1 elapsed_s=0.0 pages_fetched=0 items_seen=0
FETCH search_page=1 url=https://www.ebay.de/sch/...&_pgn=1
STOP search_page=1 exc=ZyteCrossLoopError: Zyte call hit aiohttp
  cross-loop bug: Timeout context manager should be used inside a
  task. The Zyte SDK + aiohttp 3.13 session/loop mismatch; Zyte is
  not actually unresponsive.
END region=DE pages_fetched=0 ... written=0
```

The previous session log explicitly flagged this as the known
follow-up after PR #24:

> "The aiohttp 3.13 cross-loop bug fires on the first Zyte call in
> this environment because the resource pre-creates the
> `aiohttp.ClientSession` on one loop and uses it from another.
> **Recommended follow-up:** drop the pre-created session from the
> resource, let the SDK create its own per call, rely on the
> Python-level `future.result(timeout=api_timeout)` as the only
> hard timeout."

## Root cause (verified today, 2026-06-16)

Tested the production code path with the EXACT eBay URLs the scraper
uses (PSA 10, sold only, TCG category, English-only, UK/DE local,
sort by newest):

| Test pattern | Result |
|---|---|
| Direct async call, **no pre-created session** (Approach 1) | ✅ 200 OK in 5.6s (DE), 6.5s (UK), ~1.3-1.5MB HTML |
| Direct sync call, **no pre-created session** (Approach 1 follow-up) | ✅ 200 OK in 6.1s |
| Direct sync call, **pre-created session in a persistent background loop** (v5) | ❌ `RuntimeError: Timeout context manager should be used inside a task` immediately (no real Zyte call) |
| Via real `ZyteSessionResource` (v3) | ❌ `ZyteCrossLoopError` after 3.0s, **3/3 calls** |
| Minimal `FixedResource` with no pre-created session (v6) | ✅ 200 OK in 5-6s, **3/3 calls** |

The bug is **deterministic** when the session is in a different
loop than the request. The aiohttp 3.13 `BaseTimerContext.__enter__`
(line 678 in `aiohttp/helpers.py`) raises
`RuntimeError("Timeout context manager should be used inside a task")`
when the session's loop is not the loop the request is running in:

```python
def __enter__(self) -> BaseTimerContext:
    task = asyncio.current_task(loop=self._loop)
    if task is None:
        raise RuntimeError("Timeout context manager should be used inside a task")
```

The resource's pattern: create the session in a loop via
`loop.run_until_complete(_make_session())`, then submit the call to
a `ThreadPoolExecutor` whose worker thread picks up a different loop.
The two loops never match.

## Goal

1. **Drop the pre-created `aiohttp.ClientSession`** from
   `ZyteSessionResource`. Let the Zyte SDK create its own session
   per call.
2. **Keep the Python-level `future.result(timeout=api_timeout)`** as
   the hard deadline — the SDK's session is short-lived (per call),
   so a hung request still gets aborted in 60s.
3. **The `ZyteCrossLoopError` exception class stays** in the public
   API for any future regression that reintroduces a cross-loop
   pattern. It just stops firing on the happy path.
4. **All existing tests pass.** The constructor, exception classes,
   retry logic, and the resource factory are unchanged in behavior.
5. **Live eBay pages parse and items land in `tcg-raw/ebay/DE/`** in
   the live smoke test.

## Non-goals

- No change to retry logic, exception class hierarchy, or the
  resource factory.
- No change to the scraper code.
- No change to other layers (bronze, silver, backfill, reconciliation).
- No change to the existing aiohttp version pin or test mocks for
  `aiohttp.ClientSession` (those tests still apply, even though the
  resource no longer uses it directly).

## Design

### Change 1: `ZyteSessionResource` no longer pre-creates a session

**File:** `src/tcg_platform/defs/zyte_resources.py`

Remove the `_session: aiohttp.ClientSession | None` attribute, the
`_get_session()` method, the `session=self._get_session()` argument
in `_try_get`, and the `close()` session-cleanup logic. The SDK
creates its own session per call (which is short-lived and disposed
of when the call returns).

`_try_get` simplifies to:

```python
def _try_get(self, request: dict) -> dict:
    last_error: BaseException | None = None
    for attempt in range(self._max_retries + 1):
        try:
            future = self._call_executor.submit(
                self._client.get,
                request,
                handle_retries=False,
            )
            # Hard Python-level deadline. This is the ONLY timeout
            # the resource enforces. The SDK creates a fresh
            # aiohttp.ClientSession per call, no cross-loop bug.
            response = future.result(timeout=self._api_timeout)
            status = response.get("statusCode", 0)
            if status >= 500 or status == 429:
                last_error = ZyteServerError(...)
                ...retry...
            if 400 <= status < 500:
                raise ZyteRequestError(status=..., body=..., message=...)
            return response
        except ZyteRequestError:
            raise
        except concurrent.futures.TimeoutError as e:
            last_error = ZyteTimeoutError(...)
            ...retry or raise...
        except RuntimeError as e:
            # The aiohttp cross-loop bug no longer fires (no
            # pre-created session), but keep the catch for defense
            # in depth — a future regression that re-introduces
            # the cross-loop pattern must still surface cleanly.
            if "Timeout context manager" in str(e):
                last_error = ZyteCrossLoopError(e)
                ...retry or raise...
            raise
        except TRANSIENT_ERRORS as e:
            last_error = ZyteServerError(...)
            ...retry or raise...
        except RequestError as e:
            raise ZyteRequestError(...) from e
```

Remove the `close()` method (no session to close). If callers
relied on `close()` for cleanup, they'd need to be updated; but the
prior session log shows it's a no-op anyway when the resource was
embedded inside Dagster's resource lifecycle.

### Change 2: regression-guard test

**File:** `tests/defs/test_zyte_session_resource.py`

Add a new test that pins the fix: when a real `ZyteSessionResource`
makes a real eBay-style call against a mock that returns a 200
response, the call succeeds **without** instantiating
`aiohttp.ClientSession`. The simplest test: patch
`aiohttp.ClientSession` to raise on construction. With the fix in
place, the resource never calls `aiohttp.ClientSession(...)` and the
test passes. Without the fix (i.e., if we re-introduce the
pre-created session), the test fails.

```python
def test_get_does_not_create_aiohttp_session(monkeypatch):
    """Regression guard: the resource MUST NOT pre-create an
    aiohttp.ClientSession. Doing so reintroduces the aiohttp 3.13
    cross-loop bug (Timeout context manager should be used inside
    a task) when the session's loop and the request's loop differ.
    """
    import aiohttp
    crash_on_construct = []

    class _CrashingSession:
        def __init__(self, *args, **kwargs):
            crash_on_construct.append((args, kwargs))
            raise AssertionError(
                "ZyteSessionResource must not pre-create aiohttp.ClientSession"
            )

    monkeypatch.setattr(aiohttp, "ClientSession", _CrashingSession)

    with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
        mock_client = MagicMock()
        mock_client.get = MagicMock(
            return_value={"statusCode": 200, "browserHtml": "<html/>"}
        )
        MockZyteAPI.return_value = mock_client

        resource = ZyteSessionResource(api_key="k1", api_timeout=10)
        result = resource.get({"url": "https://example.com", "browserHtml": True})

    assert result == {"statusCode": 200, "browserHtml": "<html/>"}
    assert crash_on_construct == [], (
        f"ZyteSessionResource must not pre-create aiohttp.ClientSession; "
        f"got {len(crash_on_construct)} construct attempts"
    )
```

### Change 3: remove the now-obsolete session-injection tests

**File:** `tests/defs/test_zyte_session_resource.py`

Delete or update tests that pin the old `aiohttp.ClientSession`
behavior:

- `test_get_session_works_with_real_aiohttp_313` — DELETE (no
  `_get_session` method anymore).
- `test_get_session_returns_same_session_across_calls` — DELETE
  (no session).
- `test_session_has_configured_timeout` — DELETE (no session, no
  timeout to configure).
- `test_handle_retries_false_passed_to_zyteapi` — UPDATE: still
  must pass `handle_retries=False`, but no session arg anymore.
  Update the assertion to check kwargs.
- `test_cross_loop_runtime_error_surfaces_as_zyte_cross_loop_error`
  — KEEP: defense-in-depth; the exception class still exists for
  any future regression. The test mocks the RuntimeError directly,
  so it still passes.

### Change 4: update `scrape_raw.py` docs to reflect the new timeout model

**File:** `src/tcg_platform/defs/scrape_raw.py`

The per-page `HEARTBEAT` log line is unchanged. No code change
needed.

### Change 5: PROD.md operational note

**File:** `PROD.md` line 153 (operational note)

Update from "creates a shared `aiohttp.ClientSession(timeout=...)`"
to "lets the Zyte SDK create its own per-call session, with a
hard Python-level `future.result(timeout=ZYTE_API_TIMEOUT)` as the
only hard timeout."

## Success criteria

- `pytest tests/ -v` — 100% pass.
- `python -c "from tcg_platform.definitions import defs; defs.load_fn(); print('OK')"` — loads.
- `bash scripts/check_minio_clock.sh` — OK.
- **Live smoke**: `dg.materialize(assets=[scrape_ebay_de_raw])`
  against real Zyte — first call returns 200 in ~5s, asset metadata
  shows `pages_fetched>=1, items_seen>=1, written>=1`. A real raw
  HTML file lands in `tcg-raw/ebay/DE/`.
- `git status --porcelain` — clean.
- No work on `main`.

## Risks

- **Removed `aiohttp.ClientTimeout`**: the aiohttp-level
  ClientTimeout is no longer injected. The hard timeout is now
  Python-level only (`future.result(timeout=api_timeout)`). For a
  hung Zyte request, the Python thread pool's `future.result`
  raises `TimeoutError` after 60s; the SDK's internal aiohttp
  socket may take a few extra seconds to close. **Net effect:** the
  call is still bounded to ~60s of wall time, which is what we
  want. A user-facing observation: the SDK's socket cleanup may
  log warnings on a hung connection. Acceptable.
- **Per-call session**: each call now creates a new
  `aiohttp.ClientSession`. The Zyte SDK's `ZyteAPI.get()` handles
  session lifecycle internally. Slight per-call overhead (~10-50ms
  for the session setup), which is negligible against the 5-6s
  per-call Zyte API time.
- **No aiohttp-level timeout** for in-flight requests. If Zyte
  sends a response slowly after returning headers, the Python-level
  timeout will fire. The SDK handles this by raising a RequestError.

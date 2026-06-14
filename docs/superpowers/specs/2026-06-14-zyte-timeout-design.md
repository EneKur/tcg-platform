# Zyte Per-Call Timeout (Hang → Key Rotation)

**Date:** 2026-06-14
**Status:** Draft
**Branch:** `2026-06-14-zyte-timeout`

## Problem

In the previous session (2026-06-14), an `ebay_uk_raw_to_bronze` sub-job hung for >50 minutes on a single Zyte API call. `lsof` showed an `ESTABLISHED` TCP connection to `69.41.180.81:443` (Zyte) that never received a response. No exception was raised, so the `_try_get` retry loop did not trigger and the key rotator never fired.

### Why the key rotator didn't save us

The Zyte key rotation in `src/tcg_platform/defs/zyte_resources.py:47-63` only fires on **exceptions**:

```python
for i, client in enumerate(self._clients):
    try:
        return self._try_get(client, request)
    except Exception:
        continue
```

And `_try_get` only retries inside `TRANSIENT_ERRORS` (line 9-12: `ConnectionError`, `TimeoutError`). If the Zyte SDK call hangs (no exception, just a long wait), neither retry nor rotation happens.

### Why the Zyte SDK doesn't have a built-in timeout

`zyte_api==0.10.0`'s `ZyteAPI.__init__` does not accept a `timeout` argument. It accepts `api_key`, `api_url`, `n_conn`, `retrying`, `user_agent`, `eth_key`, `trust_env`. The only knob for a per-call HTTP timeout is to **pass our own `aiohttp.ClientSession(timeout=...)`** to `ZyteAPI.get(session=...)`.

## Goal

1. Add a per-call HTTP timeout to Zyte requests so a hung connection raises `aiohttp.TimeoutError` (a subclass of `asyncio.TimeoutError`, which the SDK wraps and re-raises as `zyte_api.RequestError` — but the underlying `asyncio.TimeoutError` is also surfaced on the `requests`-style call path used by the current code).
2. When the timeout fires, the existing `_try_get` retry logic kicks in; when retries on key #1 are exhausted, the key rotator moves to key #2.
3. Make the timeout configurable via `ZYTE_API_TIMEOUT` env var, default 120s.

## Non-goals

- No change to the rotator logic — it already does the right thing on `TimeoutError` (in `TRANSIENT_ERRORS`).
- No change to the retry policy (`max_retries=3` default, 0.5/1.0/1.5s back-off) — proven to work in the existing `test_retry_on_transient_connection_error` test.
- No change to the `n_conn` connection-pool size (default 2 in our code, 15 in the SDK).
- No change to the scraper pagination logic — that's a separate concern (max-failed-pages cap, out of scope for this PR per the previous session log).

## Design

### Change 1: Per-call HTTP timeout via shared `aiohttp.ClientSession`

**File:** `src/tcg_platform/defs/zyte_resources.py`

The `ZyteSessionResource` class creates one `aiohttp.ClientSession` with a configurable `ClientTimeout(total=...)` and passes it to every `ZyteAPI.get(session=...)` call. When the timeout fires, `aiohttp` raises `asyncio.TimeoutError`, which the SDK's internal retry policy catches and re-raises as `zyte_api.RequestError`. We additionally catch the underlying `asyncio.TimeoutError` in `TRANSIENT_ERRORS` so it's recognized as transient (belt-and-suspenders; `asyncio.TimeoutError` is already a subclass of `Exception` and is in many `_try_get` exception lists via the SDK's `RequestError` wrapping — but we make it explicit).

Sketch:

```python
import asyncio
import aiohttp

TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,             # builtin; covers asyncio.TimeoutError in 3.11+
    asyncio.TimeoutError,     # explicit, in case builtin TimeoutError doesn't match
)


class ZyteSessionResource:
    def __init__(
        self,
        api_keys: list[str],
        n_conn: int = 2,
        max_retries: int = 3,
        api_timeout: float = 120.0,
    ):
        self._clients = [ZyteAPI(api_key=key, n_conn=n_conn) for key in api_keys]
        self._key_names = [f"KEY{i+1}" for i in range(len(api_keys))]
        self._n_conn = n_conn
        self._max_retries = max_retries
        self._api_timeout = api_timeout
        self._session: aiohttp.ClientSession | None = None
        self._retry_stats: dict[str, int] = {}

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._api_timeout)
            )
        return self._session

    def _try_get(self, client: ZyteAPI, request: dict) -> dict:
        last_error = None
        for attempt in range(self._max_retries + 1):
            try:
                response = client.get(request, session=self._get_session())
                status = response.get("statusCode", 0)
                if status >= 500 or status == 429:
                    raise ConnectionError(f"Transient status {status}")
                return response
            except TRANSIENT_ERRORS as e:
                last_error = e
                if attempt < self._max_retries:
                    self._retry_stats["retries_attempted"] = (
                        self._retry_stats.get("retries_attempted", 0) + 1
                    )
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
        raise last_error

    def close(self) -> None:
        if self._session is not None and not self._session.closed:
            # session.close() is async; run_until_complete is fine in this resource lifecycle
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Best-effort: schedule and let it close on GC
                    return
                loop.run_until_complete(self._session.close())
            except Exception:
                pass
            self._session = None

    # get() is unchanged
```

### Change 2: env var wiring

**File:** `src/tcg_platform/defs/zyte_resources.py` (the `zyte_session_resource` resource factory at the bottom)

```python
api_timeout_str = os.getenv("ZYTE_API_TIMEOUT", "120")
try:
    api_timeout = float(api_timeout_str)
except ValueError:
    api_timeout = 120.0

return ZyteSessionResource(
    api_keys=api_keys,
    n_conn=n_conn,
    max_retries=max_retries,
    api_timeout=api_timeout,
)
```

Also update `.env.example` to add the new var with a comment.

### Change 3: TDD test for the new behavior

**File:** `tests/defs/test_zyte_session_resource.py`

Add a new test that proves a `TimeoutError` (or `asyncio.TimeoutError`) on key #1 triggers retry, exhausts key #1, then rotates to key #2. This is the regression guard for tonight's exact failure mode.

```python
def test_key_rotation_on_timeout():
    """A hung Zyte key (TimeoutError) MUST rotate to key #2."""
    import asyncio
    with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
        mock_client_1 = MagicMock()
        mock_client_1.get = MagicMock(side_effect=asyncio.TimeoutError("hung"))

        mock_client_2 = MagicMock()
        mock_client_2.get = MagicMock(
            return_value={"statusCode": 200, "browserHtml": "<html/>"}
        )

        def side_effect(**kwargs):
            return mock_client_1 if kwargs.get("api_key") == "key1" else mock_client_2
        MockZyteAPI.side_effect = side_effect

        resource = ZyteSessionResource(
            api_keys=["key1", "key2"], max_retries=1, api_timeout=10.0
        )
        result = resource.get({"url": "https://example.com"})
        assert result == {"statusCode": 200, "browserHtml": "<html/>"}
        # key1 was tried 2 times (1 + 1 retry), then rotated to key2 once
        assert mock_client_1.get.call_count == 2
        assert mock_client_2.get.call_count == 1
        try:
            resource.close()
        except Exception:
            pass
```

Also add a test that the per-call timeout is actually configured on the `aiohttp.ClientSession` (we can patch `aiohttp.ClientSession` and assert the `timeout=...` kwarg was passed):

```python
def test_session_has_configured_timeout(monkeypatch):
    from unittest.mock import patch, MagicMock
    import aiohttp

    captured_kwargs = {}
    real_init = aiohttp.ClientSession.__init__

    def fake_init(self, *args, **kwargs):
        captured_kwargs.update(kwargs)
        # Don't actually open a session in the test
        raise RuntimeError("stop_init_for_test")

    monkeypatch.setattr(aiohttp.ClientSession, "__init__", fake_init)
    with patch("tcg_platform.defs.zyte_resources.ZyteAPI"):
        resource = ZyteSessionResource(
            api_keys=["key1"], api_timeout=42.0
        )
        try:
            resource._get_session()
        except RuntimeError:
            pass  # expected — we intercepted the init
    assert "timeout" in captured_kwargs
    assert captured_kwargs["timeout"].total == 42.0
```

### File changes

| File | Change |
|------|--------|
| `src/tcg_platform/defs/zyte_resources.py` | Add `api_timeout` param; create shared `aiohttp.ClientSession(timeout=...)`; pass `session=` to every `client.get()`; add `close()`; expand `TRANSIENT_ERRORS` with `asyncio.TimeoutError` |
| `tests/defs/test_zyte_session_resource.py` | Add 2 tests: key rotation on `TimeoutError`; session has configured timeout |
| `.env.example` | Add `ZYTE_API_TIMEOUT=120` with a comment |
| `PROD.md` | Add a one-paragraph note in the M9 operational section |
| `log/SESSION_2026-06-14-zyte-timeout.md` | New — session log |

## Test plan

1. `pytest tests/defs/test_zyte_session_resource.py -v` — existing 7 tests pass + 2 new tests pass.
2. `pytest tests/ -q` — full suite still 158+ (now 160).

## Verification (Rule 17)

- `pytest tests/ -v` — 160/160 pass
- `python -c "from tcg_platform.definitions import defs; print('OK')"` — definitions load
- `git status --porcelain` — clean

## Success criteria

- A `TimeoutError` on key #1's first attempt causes the rotator to move to key #2 (verified by the new test).
- The `aiohttp.ClientSession` is created with `timeout=aiohttp.ClientTimeout(total=ZYTE_API_TIMEOUT)` (verified by the new test).
- `ZYTE_API_TIMEOUT` env var overrides the default (verified by reading the resource factory).
- All existing 158 tests still pass.
- The `close()` method is safe to call (doesn't raise if session was never opened).

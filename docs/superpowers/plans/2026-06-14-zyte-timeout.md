# Zyte Per-Call Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-call HTTP timeout to Zyte API requests so a hung connection triggers the existing key-rotation logic instead of blocking the scraper indefinitely.

**Architecture:**
- `ZyteSessionResource` holds a single shared `aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ZYTE_API_TIMEOUT))` and passes it to every `ZyteAPI.get(session=...)` call.
- When the timeout fires, `aiohttp` raises `asyncio.TimeoutError` → `ZyteSessionResource._try_get` catches it (in `TRANSIENT_ERRORS`) and retries; on exhaustion, `get()` rotates to the next key.
- Default timeout 120s; configurable via `ZYTE_API_TIMEOUT` env var.

**Tech Stack:** Python 3.12, zyte-api 0.10.0, aiohttp 3.13.5, dagster 1.13.3, pytest.

**Spec:** `docs/superpowers/specs/2026-06-14-zyte-timeout-design.md`

**Branch:** `2026-06-14-zyte-timeout`

---

## File structure

| File | Status | Responsibility |
|------|--------|----------------|
| `src/tcg_platform/defs/zyte_resources.py` | Modify | Add `api_timeout` param; create shared `aiohttp.ClientSession` with `timeout=`; pass `session=` to `client.get()`; add `close()`; expand `TRANSIENT_ERRORS` |
| `tests/defs/test_zyte_session_resource.py` | Modify | Add 2 new tests: key rotation on `TimeoutError`; session has configured timeout |
| `.env.example` | Modify | Add `ZYTE_API_TIMEOUT=120` |
| `PROD.md` | Modify | Add a one-paragraph operational note in the M9 section |
| `log/SESSION_2026-06-14-zyte-timeout.md` | Create | Session log |

---

## Task 1: Failing tests for the new behavior

**Files:**
- Modify: `tests/defs/test_zyte_session_resource.py` (append 2 new tests)

- [ ] **Step 1: Append test `test_key_rotation_on_timeout` to the file**

At the bottom of `tests/defs/test_zyte_session_resource.py`, add:

```python
    def test_key_rotation_on_timeout(self):
        """A hung Zyte key (TimeoutError) MUST rotate to key #2.

        Regression test for the 2026-06-14 eBay UK hang: a single Zyte
        request that doesn't respond would previously block the scraper
        forever because no exception was raised. With a per-call timeout,
        the underlying aiohttp call raises asyncio.TimeoutError, which is
        in TRANSIENT_ERRORS and triggers _try_get's retry + key rotation.
        """
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

- [ ] **Step 2: Append test `test_session_has_configured_timeout`**

```python
    def test_session_has_configured_timeout(self, monkeypatch):
        """The shared aiohttp.ClientSession MUST be created with timeout=<configured>."""
        import aiohttp
        from tcg_platform.defs import zyte_resources

        captured_kwargs = {}

        real_init = aiohttp.ClientSession.__init__

        def fake_init(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            # We just want to capture the kwargs; don't actually open a session.
            # Stash a sentinel so the resource's "_session is None" check fails next time.
            self.closed = True

        monkeypatch.setattr(aiohttp.ClientSession, "__init__", fake_init)
        with patch.object(zyte_resources, "ZyteAPI"):
            resource = zyte_resources.ZyteSessionResource(
                api_keys=["key1"], api_timeout=42.0
            )
            session = resource._get_session()
        assert "timeout" in captured_kwargs, f"ClientSession was not constructed with timeout=; kwargs={captured_kwargs}"
        assert captured_kwargs["timeout"].total == 42.0
```

- [ ] **Step 3: Run the new tests to confirm they fail**

Run: `.venv/bin/python -m pytest tests/defs/test_zyte_session_resource.py -v -k "timeout"`

Expected: 2 failures. `test_key_rotation_on_timeout` fails because `ZyteSessionResource.__init__` doesn't accept `api_timeout`. `test_session_has_configured_timeout` fails because `_get_session()` doesn't exist.

- [ ] **Step 4: (deferred — implement in Task 2)**

Skip committing. Move directly to Task 2.

---

## Task 2: Implement the timeout + session sharing

**Files:**
- Modify: `src/tcg_platform/defs/zyte_resources.py`

- [ ] **Step 1: Read the current file**

Read `src/tcg_platform/defs/zyte_resources.py:1-99` to confirm imports and structure. Current imports: `os`, `time`, `dagster.resource`, `InitResourceContext`, `zyte_api.ZyteAPI`. Need to add: `asyncio`, `aiohttp`. The `TRANSIENT_ERRORS` tuple is at lines 9-12.

- [ ] **Step 2: Update imports**

At the top of the file (after the existing imports), add:

```python
import asyncio

import aiohttp
```

- [ ] **Step 3: Expand `TRANSIENT_ERRORS` to include `asyncio.TimeoutError`**

Replace lines 9-12:

```python
TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
)
```

- [ ] **Step 4: Update `ZyteSessionResource.__init__` to accept `api_timeout` and lazily hold a session**

Replace the class body (lines 15-66). Use the new version:

```python
class ZyteSessionResource:
    def __init__(
        self,
        api_keys: list[str],
        n_conn: int = 2,
        max_retries: int = 3,
        api_timeout: float = 120.0,
    ):
        self._clients = [
            ZyteAPI(api_key=key, n_conn=n_conn) for key in api_keys
        ]
        self._key_names = [f"KEY{i+1}" for i in range(len(api_keys))]
        self._n_conn = n_conn
        self._max_retries = max_retries
        self._api_timeout = api_timeout
        self._session: "aiohttp.ClientSession | None" = None
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

    def get(self, request: dict) -> dict:
        self._retry_stats = {"retries_attempted": 0}
        tried_keys: list[str] = []

        for i, client in enumerate(self._clients):
            key_name = self._key_names[i]
            tried_keys.append(key_name)
            try:
                return self._try_get(client, request)
            except Exception:
                continue

        tried = ", ".join(tried_keys)
        raise RuntimeError(
            f"All Zyte API keys exhausted ({tried}). "
            "Check rate limits or add more keys to .env as ZYTE_API_KEY3, etc."
        )

    def get_retry_stats(self) -> dict:
        return dict(self._retry_stats)

    def close(self) -> None:
        if self._session is None:
            return
        if self._session.closed:
            self._session = None
            return
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        try:
            if not loop.is_running():
                loop.run_until_complete(self._session.close())
        except Exception:
            pass
        finally:
            self._session = None
```

- [ ] **Step 5: Update the `zyte_session_resource` resource factory to read `ZYTE_API_TIMEOUT`**

At the bottom of the file (the `@resource def zyte_session_resource` function, currently lines 83-99), update it to read the env var and pass it through. Read the file first, then replace the return:

```python
@resource
def zyte_session_resource(init_context: InitResourceContext) -> ZyteSessionResource:
    api_keys = _read_api_keys()

    n_conn_str = os.getenv("ZYTE_N_CONN", "2")
    try:
        n_conn = int(n_conn_str)
    except ValueError:
        n_conn = 2

    max_retries_str = os.getenv("ZYTE_MAX_RETRIES", "3")
    try:
        max_retries = int(max_retries_str)
    except ValueError:
        max_retries = 3

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

- [ ] **Step 6: Run the new tests**

Run: `.venv/bin/python -m pytest tests/defs/test_zyte_session_resource.py -v -k "timeout"`

Expected: 2/2 new tests pass; all 7 existing tests still pass.

- [ ] **Step 7: Run the full test suite**

Run: `.venv/bin/python -m pytest tests/ -q`

Expected: 160/160 tests pass (was 158, +2 new).

- [ ] **Step 8: Verify definitions still load**

Run: `.venv/bin/python -c "from tcg_platform.definitions import defs; print('OK')"`

Expected: prints `OK`.

- [ ] **Step 9: Commit**

```bash
git add src/tcg_platform/defs/zyte_resources.py tests/defs/test_zyte_session_resource.py
git commit -m "fix(zyte): add per-call HTTP timeout so hung Zyte requests rotate keys

The 2026-06-14 eBay UK sub-job hung >50 min on a single Zyte API call.
lsof showed an ESTABLISHED TCP to 69.41.180.81:443 that never received a
response. The existing key rotator only fires on exceptions; a hung
connection raises nothing, so neither retries nor key rotation happened.

The Zyte SDK (zyte_api==0.10.0) has no per-call timeout parameter, but
its get() method accepts a caller-provided aiohttp.ClientSession. Pass
one with ClientTimeout(total=ZYTE_API_TIMEOUT). When aiohttp times out,
it raises asyncio.TimeoutError, which is added to TRANSIENT_ERRORS so
_try_get's existing retry logic kicks in, and on key-1 exhaustion the
rotator moves to key #2.

Default timeout: 120s. Override via ZYTE_API_TIMEOUT env var.

Tests: regression test for key rotation on TimeoutError + a test that
the shared session is created with the configured timeout."
```

---

## Task 3: Update `.env.example`

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Read `.env.example`**

Run: `cat .env.example`

- [ ] **Step 2: Add the new env var**

In the Zyte section (find lines mentioning `ZYTE_API_KEY` or `Z1TE_N_CONN`), add:

```
# Per-call HTTP timeout in seconds. A hung Zyte request will be aborted
# after this many seconds, which triggers retry + key rotation.
ZYTE_API_TIMEOUT=120
```

If `.env.example` is minimal (just `ZYTE_API_KEY=...`), add the new var after the existing Zyte line with a 2-line comment above it.

- [ ] **Step 3: Commit**

```bash
git add .env.example
git commit -m "docs(env): document ZYTE_API_TIMEOUT (default 120s)"
```

---

## Task 4: Update `PROD.md` operational note

**Files:**
- Modify: `PROD.md` (M9 section)

- [ ] **Step 1: Find the M9 section**

In `PROD.md`, find the "### Milestone 9" section (added in the previous session). The "Operational notes" subsection has the MinIO clock-skew bullet. Add a new bullet about the Zyte timeout.

- [ ] **Step 2: Append the new bullet**

After the existing "MinIO clock skew will break the pipeline..." bullet, add:

```
- **A hung Zyte request will block the scraper forever** (no per-call timeout in the Zyte SDK by default). Symptom: the `scrape_ebay_*_raw` step shows no log output for tens of minutes; `lsof -p <pid>` shows an `ESTABLISHED` TCP to `69.41.180.81:443`. Fixed in PR #20: `ZyteSessionResource` now creates a shared `aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ZYTE_API_TIMEOUT))` (default 120s, override via env). When the timeout fires, `asyncio.TimeoutError` triggers the existing retry + key-rotation logic, so a hung key #1 is followed by key #2.
```

- [ ] **Step 3: Commit**

```bash
git add PROD.md
git commit -m "docs(prod): add Zyte timeout operational note in M9 section"
```

---

## Task 5: Final verification

- [ ] **Step 1: Full test suite**

Run: `.venv/bin/python -m pytest tests/ -q`

Expected: 160/160 tests pass.

- [ ] **Step 2: Definitions load**

Run: `.venv/bin/python -c "from tcg_platform.definitions import defs; print('OK')"`

Expected: `OK`.

- [ ] **Step 3: Watchdog still OK (no regression)**

Run: `bash scripts/check_minio_clock.sh`

Expected: `OK    skew=Ns ...`.

- [ ] **Step 4: Working tree clean**

Run: `git status --porcelain`

Expected: empty.

- [ ] **Step 5: Push the branch**

Run: `git push origin 2026-06-14-zyte-timeout`

Expected: branch is created on origin.

- [ ] **Step 6: Write the session log**

Create `log/SESSION_2026-06-14-zyte-timeout.md` (do NOT overwrite the existing `log/SESSION_2026-06-14.md` from the previous session — this is a separate task) with:
- Branch name
- Goal (why)
- Done (4 commits: spec, impl, env example, prod doc)
- Verification (160/160, defs load, watchdog OK)
- Remains (no merge to main — human-driven per Rule 19; branch ready to PR)
- Blockers (none)

```bash
git add log/SESSION_2026-06-14-zyte-timeout.md
git commit -m "docs(log): session log for 2026-06-14-zyte-timeout"
```

---

## Self-review

- **Spec coverage:** Spec section "Change 1" → Tasks 1–2. Spec section "Change 2" → Task 2 Step 5. Spec section "Change 3" → Task 1. Spec section "File changes" (env, prod, log) → Tasks 3, 4, 5. ✓
- **Placeholder scan:** No "TBD", "TODO", "implement later". Every step has a concrete command or code. ✓
- **Type consistency:** `_session: "aiohttp.ClientSession | None" = None` matches `_get_session() -> aiohttp.ClientSession`. `close()` resets `self._session = None`. Test mocks `aiohttp.ClientSession.__init__` consistently. ✓
- **Scope check:** Single plan, one PR. ✓

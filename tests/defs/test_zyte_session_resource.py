"""Tests for ZyteSessionResource — single-key model (2026-06-16 redesign).

Pre-PR history: the resource used to rotate across ZYTE_API_KEY1..N, mark
dead keys after 402, and surface a single "All Zyte API keys exhausted"
or "All Zyte API keys timed out" message. The operator now provides a
single ZYTE_API_KEY managed externally; rotation is gone. Each failure
mode (4xx, 5xx, hard timeout, aiohttp cross-loop) is now a distinct
exception class so the operator can see the real cause.
"""
import asyncio
import time
from unittest.mock import patch, MagicMock

import pytest

from tcg_platform.defs.zyte_resources import (
    ZyteSessionResource,
    ZyteTimeoutError,
    ZyteRequestError,
    ZyteServerError,
    ZyteCrossLoopError,
    _read_api_key,
)


class TestZyteSessionResourceSingleKey:
    """The single-key contract."""

    def test_constructor_accepts_single_api_key(self):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            resource = ZyteSessionResource(api_key="test-key-123")
            assert resource is not None
            MockZyteAPI.assert_called_once()

    def test_constructor_rejects_api_keys_list(self):
        """The old plural `api_keys=[...]` argument is gone. A TypeError on
        the constructor surfaces the rename loudly at first use rather than
        silently being treated as a 1-element list."""
        with pytest.raises(TypeError):
            ZyteSessionResource(api_keys=["test-key-123"])

    def test_get_does_not_create_aiohttp_session(self, monkeypatch):
        """Regression guard: the resource MUST NOT pre-create an
        aiohttp.ClientSession. Doing so reintroduces the aiohttp 3.13
        cross-loop bug (`Timeout context manager should be used inside
        a task`) when the session's loop and the request's loop differ
        — see `docs/superpowers/specs/2026-06-16-zyte-cross-loop-fix-design.md`.

        The Zyte SDK creates its own short-lived session per call. The
        Python-level `future.result(timeout=api_timeout)` is the only
        hard deadline.
        """
        import aiohttp
        from tcg_platform.defs.zyte_resources import ZyteSessionResource

        crash_on_construct: list = []

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


class TestZyteSessionResourceSuccess:
    """Happy path: retry on transient errors, return the response."""

    def test_retry_on_transient_connection_error(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 3:
                    raise ConnectionError("transient")
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="test-key-123", n_conn=2)
            result = resource.get({"url": "https://example.com"})
            assert result == {"statusCode": 200, "browserHtml": "<html/>"}
            assert call_count[0] == 3

    def test_retry_stats_tracked(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 3:
                    raise ConnectionError("transient")
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="test-key-123")
            resource.get({"url": "https://example.com"})
            stats = resource.get_retry_stats()
            assert stats["retries_attempted"] == 2

    def test_5xx_triggers_retry_then_success(self, monkeypatch):
        """A 5xx from Zyte is transient — retry, then succeed."""
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 2:
                    return {"statusCode": 500, "browserHtml": ""}
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="test-key-123", max_retries=3)
            result = resource.get({"url": "https://example.com"})
            assert result == {"statusCode": 200, "browserHtml": "<html/>"}
            assert call_count[0] == 2

    def test_429_triggers_retry(self, monkeypatch):
        """429 (rate-limited) is transient — retry."""
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 2:
                    return {"statusCode": 429, "browserHtml": ""}
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="test-key-123", max_retries=3)
            result = resource.get({"url": "https://example.com"})
            assert result["statusCode"] == 200

    def test_handle_retries_false_passed_to_zyteapi(self, monkeypatch):
        """The Zyte SDK's internal retry policy wastes budget on dead keys.
        We pass handle_retries=False so our wrapper owns retry decisions.
        """
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            live_key = MagicMock()
            live_key.get = MagicMock(
                return_value={"statusCode": 200, "browserHtml": "<html/>"}
            )
            MockZyteAPI.return_value = live_key

            resource = ZyteSessionResource(api_key="live", max_retries=3)
            resource.get({"url": "https://example.com"})

            assert live_key.get.call_count == 1
            call_kwargs = live_key.get.call_args.kwargs
            assert call_kwargs.get("handle_retries") is False, (
                f"client.get() must be called with handle_retries=False; "
                f"got kwargs={call_kwargs}"
            )


class TestZyteSessionResourceFailureModes:
    """Each failure mode must surface as a distinct, named exception."""

    def test_4xx_surfaces_as_zyte_request_error_with_status(self, monkeypatch):
        """A 4xx from Zyte is NOT a timeout. It is a 4xx. Surface as
        ZyteRequestError with the real status code attached.
        """
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(
                return_value={"statusCode": 403, "browserHtml": ""}
            )
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=3)
            with pytest.raises(ZyteRequestError) as exc_info:
                resource.get({"url": "https://example.com"})
            assert exc_info.value.status == 403

    def test_4xx_no_retry(self, monkeypatch):
        """A 4xx is final — don't retry it, don't wait for timeouts."""
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(
                return_value={"statusCode": 401, "browserHtml": ""}
            )
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=3)
            with pytest.raises(ZyteRequestError):
                resource.get({"url": "https://example.com"})
            assert mock_client.get.call_count == 1

    def test_5xx_after_retries_surfaces_as_zyte_server_error(self, monkeypatch):
        """A 5xx that persists across all retries is reported as ZyteServerError."""
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(
                return_value={"statusCode": 503, "browserHtml": ""}
            )
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=1)
            with pytest.raises(ZyteServerError) as exc_info:
                resource.get({"url": "https://example.com"})
            assert exc_info.value.status == 503
            # max_retries=1 → 2 total attempts.
            assert mock_client.get.call_count == 2

    def test_max_retries_respected(self, monkeypatch):
        """A persistent transient error (e.g. ConnectionError) raises
        ZyteServerError after max_retries+1 attempts.
        """
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(side_effect=ConnectionError("always fails"))
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=1)
            with pytest.raises(ZyteServerError, match="max retries"):
                resource.get({"url": "https://example.com"})
            assert mock_client.get.call_count == 2

    def test_cross_loop_runtime_error_surfaces_as_zyte_cross_loop_error(self, monkeypatch):
        """aiohttp 3.13 raises RuntimeError('Timeout context manager should be
        used inside a task') when the session's timeout fires on a loop
        with no current task. This is a DIFFERENT failure mode from a true
        network hang. Surface it as ZyteCrossLoopError so the operator
        can tell them apart.
        """
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(
                side_effect=RuntimeError("Timeout context manager should be used inside a task")
            )
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=0, api_timeout=10.0)
            with pytest.raises(ZyteCrossLoopError) as exc_info:
                resource.get({"url": "https://example.com"})
            assert "cross-loop" in str(exc_info.value).lower() or "timeout" in str(exc_info.value).lower()

    def test_hard_timeout_surfaces_as_zyte_timeout_error(self, monkeypatch):
        """A Zyte call that hangs past api_timeout raises ZyteTimeoutError
        within api_timeout + small slack, not a generic RuntimeError.
        """
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            def hang(*args, **kwargs):
                time.sleep(5)  # longer than api_timeout=1 below
                return {"statusCode": 200}
            mock_client.get = hang
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=0, api_timeout=1.0)
            start = time.time()
            with pytest.raises(ZyteTimeoutError):
                resource.get({"url": "https://example.com", "browserHtml": True})
            elapsed = time.time() - start
            assert elapsed < 3.0, (
                f"hard timeout did not fire: call took {elapsed:.2f}s, expected < 3s"
            )

    def test_get_retries_within_a_key_after_hard_timeout(self, monkeypatch):
        """A hard timeout counts as a transient error: the same call should
        be retried up to max_retries before failing. This preserves the
        existing semantics where transient errors retry on the same key.
        """
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            call_count = [0]
            def slow_then_fast(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    time.sleep(2)  # first call: times out at 0.5s
                return {"statusCode": 200, "browserHtml": "<html/>"}
            mock_client.get = slow_then_fast
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_key="k1", max_retries=3, api_timeout=0.5)
            start = time.time()
            r = resource.get({"url": "https://example.com", "browserHtml": True})
            elapsed = time.time() - start
            assert r["statusCode"] == 200
            assert call_count[0] == 2
            assert elapsed < 1.5


class TestReadApiKey:
    """The single-env-var contract."""

    def test_read_api_key_singular(self, monkeypatch):
        monkeypatch.setenv("ZYTE_API_KEY", "k1")
        monkeypatch.delenv("ZYTE_API_KEY1", raising=False)
        monkeypatch.delenv("ZYTE_API_KEY2", raising=False)
        from tcg_platform.defs.zyte_resources import _read_api_key
        assert _read_api_key() == "k1"

    def test_read_api_key_missing_raises(self, monkeypatch):
        monkeypatch.delenv("ZYTE_API_KEY", raising=False)
        monkeypatch.delenv("ZYTE_API_KEY1", raising=False)
        from tcg_platform.defs.zyte_resources import _read_api_key
        with pytest.raises(ValueError, match="ZYTE_API_KEY"):
            _read_api_key()

    def test_read_api_key_ignores_old_plural_form(self, monkeypatch):
        """If the operator forgot to clean up the old ZYTE_API_KEY1, the
        resource must NOT silently fall back to it. Only ZYTE_API_KEY is
        valid in the single-key model.
        """
        monkeypatch.delenv("ZYTE_API_KEY", raising=False)
        monkeypatch.setenv("ZYTE_API_KEY1", "k1")
        from tcg_platform.defs.zyte_resources import _read_api_key
        with pytest.raises(ValueError, match="ZYTE_API_KEY"):
            _read_api_key()

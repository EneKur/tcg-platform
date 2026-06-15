import pytest
import os
from unittest.mock import patch, MagicMock

from tcg_platform.defs.zyte_resources import ZyteSessionResource


class TestZyteSessionResource:
    def test_retry_on_transient_connection_error(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 3:
                    raise ConnectionError("transient")
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"], n_conn=2)
            result = resource.get({"url": "https://example.com"})
            assert result == {"statusCode": 200, "browserHtml": "<html/>"}
            assert call_count[0] == 3

    def test_no_retry_on_4xx(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client = MagicMock()
            mock_client.get = MagicMock(return_value={"statusCode": 403, "browserHtml": ""})
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"])
            result = resource.get({"url": "https://example.com"})
            assert result["statusCode"] == 403
            assert mock_client.get.call_count == 1

    def test_no_retry_on_500(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 2:
                    raise ConnectionError("server error")
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"], max_retries=3)
            result = resource.get({"url": "https://example.com"})
            assert result == {"statusCode": 200, "browserHtml": "<html/>"}
            assert call_count[0] == 2

    def test_max_retries_respected(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client = MagicMock()
            mock_client.get = MagicMock(side_effect=ConnectionError("always fails"))
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"], max_retries=1)
            with pytest.raises(RuntimeError, match="All Zyte API keys exhausted"):
                resource.get({"url": "https://example.com"})
            assert mock_client.get.call_count == 2

    def test_retry_stats_tracked(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client = MagicMock()
            call_count = [0]

            def failing_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] < 3:
                    raise ConnectionError("transient")
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client.get = failing_get
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"])
            resource.get({"url": "https://example.com"})
            stats = resource.get_retry_stats()
            assert stats["retries_attempted"] == 2

    def test_key_rotation_on_exhausted_retries(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client_1 = MagicMock()
            mock_client_1.get = MagicMock(side_effect=ConnectionError("key1 exhausted"))

            mock_client_2 = MagicMock()
            call_count_2 = [0]

            def key2_get(*args, **kwargs):
                call_count_2[0] += 1
                if call_count_2[0] < 2:
                    raise ConnectionError("transient")
                return {"statusCode": 200, "browserHtml": "<html/>"}

            mock_client_2.get = key2_get

            def side_effect(**kwargs):
                key = kwargs.get("api_key")
                if key == "key1":
                    return mock_client_1
                return mock_client_2

            MockZyteAPI.side_effect = side_effect

            resource = ZyteSessionResource(
                api_keys=["key1", "key2"], max_retries=3
            )
            result = resource.get({"url": "https://example.com"})
            assert result == {"statusCode": 200, "browserHtml": "<html/>"}
            assert mock_client_1.get.call_count == 4
            assert call_count_2[0] == 2

    def test_all_keys_exhausted_raises(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            mock_client = MagicMock()
            mock_client.get = MagicMock(side_effect=ConnectionError("always fails"))
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(
                api_keys=["key1", "key2"], max_retries=1
            )
            with pytest.raises(RuntimeError, match="All Zyte API keys exhausted"):
                resource.get({"url": "https://example.com"})

    def test_key_rotation_on_timeout(self):
        """A hung Zyte key (TimeoutError) MUST rotate to key #2.

        Regression test for the 2026-06-14 eBay UK hang: a single Zyte
        request that doesn't respond would previously block the scraper
        forever because no exception was raised. With a per-call timeout,
        the underlying aiohttp call raises asyncio.TimeoutError, which is
        in TRANSIENT_ERRORS and triggers _try_get's retry + key rotation.
        """
        import asyncio
        import aiohttp
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

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
            assert mock_client_1.get.call_count == 2
            assert mock_client_2.get.call_count == 1
            try:
                resource.close()
            except Exception:
                pass

    def test_read_api_keys_picks_up_zyte_api_key1(self, monkeypatch):
        """Regression: ZYTE_API_KEY1 in .env was being silently ignored because
        the loop read ZYTE_API_KEY (no suffix) for i=1. With three Zyte keys
        configured, all three must be loaded — losing one means burning through
        two instead of three before 'all exhausted' is raised.
        """
        monkeypatch.setenv("ZYTE_API_KEY1", "k1")
        monkeypatch.delenv("ZYTE_API_KEY", raising=False)
        monkeypatch.setenv("ZYTE_API_KEY2", "k2")
        monkeypatch.setenv("ZYTE_API_KEY3", "k3")
        monkeypatch.delenv("ZYTE_API_KEY4", raising=False)

        from tcg_platform.defs.zyte_resources import _read_api_keys
        keys = _read_api_keys()
        assert keys == ["k1", "k2", "k3"]

    def test_402_passes_handle_retries_false_to_zyteapi(self):
        """A 402 'over-user-limit' from Zyte means the key is dead for the
        month. ZyteAPI's default retry policy retries 402 internally twice
        (x402_error_stop: stop_on_count(2)) before raising. On a dead-for-
        the-month key, those internal retries waste monthly quota budget
        and add latency for no benefit. We pass handle_retries=False to
        client.get() so the wrapper (not ZyteAPI) owns retry/rotation
        decisions.
        """
        from zyte_api import RequestError
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            live_key = MagicMock()
            live_key.get = MagicMock(
                return_value={"statusCode": 200, "browserHtml": "<html/>"}
            )
            MockZyteAPI.return_value = live_key

            resource = ZyteSessionResource(
                api_keys=["live"], max_retries=3
            )
            resource.get({"url": "https://example.com"})

            assert live_key.get.call_count == 1
            call_kwargs = live_key.get.call_args.kwargs
            assert call_kwargs.get("handle_retries") is False, (
                f"client.get() must be called with handle_retries=False to "
                f"prevent ZyteAPI from wasting internal retries on dead keys; "
                f"got kwargs={call_kwargs}"
            )

    def test_key_402d_earlier_is_not_retried_again_this_run(self):
        """Once a key returns 402 ('dead for the month'), subsequent calls in
        the same process must skip it entirely. Otherwise we waste monthly
        quota budget on a key that will never succeed this month.
        """
        from zyte_api import RequestError
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI, \
             patch("aiohttp.ClientSession") as MockSession:
            mock_session = MagicMock()
            mock_session.closed = False
            MockSession.return_value = mock_session

            dead_key = MagicMock()
            dead_key.get = MagicMock(
                side_effect=RequestError(
                    request_info=MagicMock(),
                    history=(),
                    status=402,
                    message="Payment Required",
                    headers={},
                    response_content=b'{"error":"limits/over-user-limit"}',
                    query={"url": "https://example.com"},
                )
            )

            live_key = MagicMock()
            live_key.get = MagicMock(
                return_value={"statusCode": 200, "browserHtml": "<html/>"}
            )

            def side_effect(**kwargs):
                return dead_key if kwargs.get("api_key") == "dead" else live_key
            MockZyteAPI.side_effect = side_effect

            resource = ZyteSessionResource(
                api_keys=["dead", "live"], max_retries=3
            )

            result1 = resource.get({"url": "https://example.com/a"})
            assert result1 == {"statusCode": 200, "browserHtml": "<html/>"}
            assert dead_key.get.call_count == 1

            result2 = resource.get({"url": "https://example.com/b"})
            assert result2 == {"statusCode": 200, "browserHtml": "<html/>"}
            assert dead_key.get.call_count == 1, (
                "Dead-for-the-month key must not be retried in same process; "
                f"was called {dead_key.get.call_count} times across two get() calls"
            )

    def test_get_session_works_with_real_aiohttp_313(self):
        """Regression: aiohttp 3.13's ClientSession constructor calls
        asyncio.get_running_loop() to capture the current event loop.
        Calling aiohttp.ClientSession(...) from sync code without a
        running loop raises RuntimeError: no running event loop. Every
        Zyte call would then appear as "all keys exhausted" because the
        session construction fails on every key. The session MUST be
        created inside a running event loop so aiohttp 3.13 can capture
        the loop reference.
        """
        from tcg_platform.defs import zyte_resources
        from tcg_platform.defs.zyte_resources import ZyteSessionResource

        with patch.object(zyte_resources, "ZyteAPI"):
            resource = ZyteSessionResource(api_keys=["k1"], api_timeout=42.0)
            # If _get_session synchronously calls aiohttp.ClientSession(...)
            # without a running loop, aiohttp 3.13 raises:
            #   RuntimeError: no running event loop
            # The test passes only if the session is created inside a loop.
            session = resource._get_session()
            assert session is not None
            assert session.closed is False
            # The configured timeout must be preserved.
            assert session.timeout.total == 42.0
            try:
                resource.close()
            except Exception:
                pass

    def test_get_session_returns_same_session_across_calls(self):
        """The session is shared across multiple get() calls (per-process
        connection reuse). _get_session must return the same instance
        while the session is open, and a fresh one after close.
        """
        from tcg_platform.defs import zyte_resources

        with patch.object(zyte_resources, "ZyteAPI"):
            resource = zyte_resources.ZyteSessionResource(api_keys=["k1"], api_timeout=30.0)
            s1 = resource._get_session()
            s2 = resource._get_session()
            assert s1 is s2, "session should be reused while open"
            try:
                resource.close()
            except Exception:
                pass

    def test_session_has_configured_timeout(self, monkeypatch):
        """The shared aiohttp.ClientSession MUST be created with timeout=<configured>."""
        import aiohttp
        from tcg_platform.defs import zyte_resources

        captured_kwargs: dict = {}

        class FakeSession:
            def __init__(self, *args, **kwargs):
                captured_kwargs.update(kwargs)
                self.closed = True

        monkeypatch.setattr(aiohttp, "ClientSession", FakeSession)
        with patch.object(zyte_resources, "ZyteAPI"):
            resource = zyte_resources.ZyteSessionResource(
                api_keys=["key1"], api_timeout=42.0
            )
            resource._get_session()
        assert "timeout" in captured_kwargs, f"ClientSession kwargs={captured_kwargs}"
        assert captured_kwargs["timeout"].total == 42.0

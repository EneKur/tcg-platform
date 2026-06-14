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

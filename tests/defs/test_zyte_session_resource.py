import pytest
import os
from unittest.mock import patch, MagicMock

from tcg_platform.defs.zyte_resources import ZyteSessionResource


class TestZyteSessionResource:
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

            resource = ZyteSessionResource(api_keys=["test-key-123"], n_conn=2)
            result = resource.get({"url": "https://example.com"})
            assert result == {"statusCode": 200, "browserHtml": "<html/>"}
            assert call_count[0] == 3

    def test_no_retry_on_4xx(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(return_value={"statusCode": 403, "browserHtml": ""})
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"])
            result = resource.get({"url": "https://example.com"})
            assert result["statusCode"] == 403
            assert mock_client.get.call_count == 1

    def test_no_retry_on_500(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
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
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(side_effect=ConnectionError("always fails"))
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(api_keys=["test-key-123"], max_retries=1)
            with pytest.raises(RuntimeError, match="All Zyte API keys exhausted"):
                resource.get({"url": "https://example.com"})
            assert mock_client.get.call_count == 2

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

            resource = ZyteSessionResource(api_keys=["test-key-123"])
            resource.get({"url": "https://example.com"})
            stats = resource.get_retry_stats()
            assert stats["retries_attempted"] == 2

    def test_key_rotation_on_exhausted_retries(self, monkeypatch):
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
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
        with patch("tcg_platform.defs.zyte_resources.ZyteAPI") as MockZyteAPI:
            mock_client = MagicMock()
            mock_client.get = MagicMock(side_effect=ConnectionError("always fails"))
            MockZyteAPI.return_value = mock_client

            resource = ZyteSessionResource(
                api_keys=["key1", "key2"], max_retries=1
            )
            with pytest.raises(RuntimeError, match="All Zyte API keys exhausted"):
                resource.get({"url": "https://example.com"})

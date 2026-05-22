import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import MagicMock, patch
from dagster import build_op_context
from tcg_platform.resources.steel_session import SteelSessionResource


def test_steel_session_resource_creation(monkeypatch):
    monkeypatch.setenv("STEEL_API_KEY", "test-key")
    resource = SteelSessionResource(site_name="ebay", auth_dir="auth")
    assert resource.site_name == "ebay"
    assert resource.auth_dir == "auth"
    assert resource._session_id is None


def test_steel_session_requires_api_key(monkeypatch):
    monkeypatch.delenv("STEEL_API_KEY", raising=False)
    with pytest.raises(ValueError, match="STEEL_API_KEY"):
        SteelSessionResource(site_name="ebay")


def test_inject_auth_profile(monkeypatch, tmp_path):
    monkeypatch.setenv("STEEL_API_KEY", "test-key")
    profile_data = {"cookies": [{"name": "session", "value": "abc123"}], "localStorage": {}}
    profile_path = tmp_path / "profile_ebay.json"
    profile_path.write_text(json.dumps(profile_data))

    resource = SteelSessionResource(site_name="ebay", auth_dir=str(tmp_path))

    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_session.id = "session-123"
    mock_client.sessions.create.return_value = mock_session

    resource._client = mock_client
    resource._session_id = "session-123"
    resource._auth_profile = profile_data

    resource._inject_auth_profile()

    mock_client.sessions.add_cookie.assert_called_once_with(
        "session-123", {"name": "session", "value": "abc123"}
    )


def test_save_auth_profile_persists(monkeypatch, tmp_path):
    monkeypatch.setenv("STEEL_API_KEY", "test-key")
    profile_path = tmp_path / "profile_ebay.json"

    resource = SteelSessionResource(site_name="ebay", auth_dir=str(tmp_path))

    mock_client = MagicMock()
    mock_client.sessions.get_cookies.return_value = [
        {"name": "auth", "value": "xyz789"}
    ]
    resource._client = mock_client
    resource._session_id = "session-456"

    resource.save_auth_profile()

    assert profile_path.exists()
    saved = json.loads(profile_path.read_text())
    assert saved["cookies"] == [{"name": "auth", "value": "xyz789"}]
    assert saved["localStorage"] == {}
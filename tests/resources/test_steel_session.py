import pytest
from dagster import build_op_context
from tcg_platform.resources.steel_session import SteelSessionResource

def test_steel_session_resource_creation():
    resource = SteelSessionResource(site_name="ebay")
    assert resource.site_name == "ebay"
    assert resource._session_id is None

def test_steel_session_requires_api_key(monkeypatch):
    monkeypatch.delenv("STEEL_API_KEY", raising=False)
    with pytest.raises(ValueError, match="STEEL_API_KEY"):
        SteelSessionResource(site_name="ebay")
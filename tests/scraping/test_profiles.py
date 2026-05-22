import pytest
import json
import tempfile
from pathlib import Path
from tcg_platform.scraping.profiles import ProfileManager, load_profile, save_profile

def test_load_profile_returns_dict(tmp_path):
    profile_data = {"cookies": [{"name": "session", "value": "abc"}], "localStorage": {}}
    profile_file = tmp_path / "profile_test.json"
    profile_file.write_text(json.dumps(profile_data))

    result = load_profile("test", tmp_path)
    assert result == profile_data
    assert result["cookies"][0]["name"] == "session"

def test_load_profile_returns_empty_if_missing(tmp_path):
    result = load_profile("nonexistent", tmp_path)
    assert result == {"cookies": [], "localStorage": {}}

def test_save_profile_writes_file(tmp_path):
    profile_data = {"cookies": [], "localStorage": {}}
    save_profile("test", tmp_path, profile_data)
    profile_file = tmp_path / "profile_test.json"
    assert profile_file.exists()
    assert json.loads(profile_file.read_text()) == profile_data

def test_profile_manager_get(tmp_path):
    pm = ProfileManager("test", tmp_path)
    pm.save({"cookies": [{"name": "auth", "value": "xyz"}], "localStorage": {}})
    profile = pm.get()
    assert profile["cookies"][0]["value"] == "xyz"

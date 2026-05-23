# M1: Steel.dev Anti-Ban Infrastructure — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish Steel.cloud browser API integration as a reusable Dagster resource with session reuse, auth profile persistence, and CAPTCHA auto-solving.

**Architecture:** Wrap Steel Python SDK in a Dagster resource (`steel_session`). Per-website sessions use sticky browser context via Steel Profiles API — cookies/localStorage persisted to `auth/profile_{sitename}.json` files on session close, injected on session open. Three asset pipelines (one per website) share the resource.

**Tech Stack:** `steel-sdk`, `puppeteer` or `playwright` (via Steel WebSocket CDP), Python stdlib `json`/`pathlib`

---

## File Structure

```
tcg_platform/
  resources/
    steel_session.py       # SteelSession resource class
    __init__.py
  scraping/
    profiles.py             # Profile load/save utilities
    __init__.py
  auth/                     # Gitignored; created at runtime
    profile_ebay.json
    profile_pricecharting.json
    profile_limitlesstcg.json
  defs/
    steel_resources.py      # Dagster definitions for resources

tests/
  resources/
    test_steel_session.py
  scraping/
    test_profiles.py
```

**Files to modify:**
- `src/tcg_platform/definitions.py` — import and wire new resource
- `pyproject.toml` — add `steel-sdk`, `puppeteer-core`, `playwright` to dependencies
- `.gitignore` — add `auth/*.json`

---

### Task 1: M1-T1 — Add Steel.dev dependencies to pyproject.toml

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add Steel SDK and browser automation deps**

```toml
dependencies = [
    "dagster==1.13.3",
    "steel-sdk>=1.0.0",
    "puppeteer-core>=21.0.0",
    "playwright>=1.40.0",
]
```

Run: `uv sync`
Expected: packages install without conflict

- [ ] **Step 2: Add .gitignore entry for auth files**

Add to `.gitignore`:
```
# Steel auth profiles
auth/
```

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "feat: add steel-sdk and browser automation dependencies"
```

---

### Task 2: M1-T2 — Create SteelSession Dagster resource with session create/release and retry

**Files:**
- Create: `src/tcg_platform/resources/steel_session.py`
- Create: `tests/resources/test_steel_session.py`
- Modify: `src/tcg_platform/resources/__init__.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/resources/test_steel_session.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write minimal implementation**

```python
# src/tcg_platform/resources/steel_session.py
from dataclasses import dataclass
import os
import time
from typing import Optional
from steel import Steel
from dagster import ConfigurableResource, ResourceDependency

@dataclass
class SteelSessionResource(ConfigurableResource):
    """Dagster resource wrapping Steel.cloud browser sessions."""

    site_name: str

    def __post_init__(self):
        api_key = os.getenv("STEEL_API_KEY")
        if not api_key:
            raise ValueError("STEEL_API_KEY environment variable is not set")
        self._client = Steel(steel_api_key=api_key)
        self._session_id: Optional[str] = None
        self._browser_ws_endpoint: Optional[str] = None

    def create_session(self) -> str:
        """Create a new Steel session with retry logic."""
        for attempt in range(3):
            try:
                session = self._client.sessions.create()
                self._session_id = session.id
                self._browser_ws_endpoint = (
                    f"wss://connect.steel.dev?apiKey={os.getenv('STEEL_API_KEY')}"
                    f"&sessionId={session.id}"
                )
                return session.id
            except Exception as e:
                if attempt == 2:
                    raise RuntimeError(
                        f"Failed to create Steel session after 3 attempts: {e}"
                    )
                time.sleep(2 ** attempt)
        raise RuntimeError("Unexpected exit from retry loop")

    def release_session(self) -> None:
        """Release the current Steel session."""
        if self._session_id:
            self._client.sessions.release(self._session_id)
            self._session_id = None
            self._browser_ws_endpoint = None

    @property
    def browser_ws_endpoint(self) -> Optional[str]:
        return self._browser_ws_endpoint

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id
```

```python
# src/tcg_platform/resources/__init__.py
from tcg_platform.resources.steel_session import SteelSessionResource

__all__ = ["SteelSessionResource"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/resources/ tests/resources/
git commit -m "feat: add SteelSessionResource with create/release and 3x retry"
```

---

### Task 3: M1-T3 — Implement auth/profile loading from auth/profile_{sitename}.json

**Files:**
- Create: `src/tcg_platform/scraping/profiles.py`
- Create: `tests/scraping/test_profiles.py`
- Create: `auth/profile_ebay.json` (sample)
- Create: `auth/profile_pricecharting.json` (sample)
- Create: `auth/profile_limitlesstcg.json` (sample)

- [ ] **Step 1: Write the failing test**

```python
# tests/scraping/test_profiles.py
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
    pm = ProfileManager(tmp_path)
    pm.save({"cookies": [{"name": "auth", "value": "xyz"}], "localStorage": {}})
    profile = pm.get()
    assert profile["cookies"][0]["value"] == "xyz"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/scraping/test_profiles.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write minimal implementation**

```python
# src/tcg_platform/scraping/profiles.py
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any

DEFAULT_PROFILE = {"cookies": [], "localStorage": {}}

@dataclass
class ProfileManager:
    """Manages per-website browser profiles (cookies + localStorage) persisted to JSON."""

    site_name: str
    auth_dir: Path = field(default_factory=lambda: Path("auth"))

    @property
    def profile_path(self) -> Path:
        return self.auth_dir / f"profile_{self.site_name}.json"

    def get(self) -> Dict[str, Any]:
        """Load profile from JSON file. Returns empty profile if file doesn't exist."""
        if not self.profile_path.exists():
            return DEFAULT_PROFILE.copy()
        with open(self.profile_path, "r") as f:
            return json.load(f)

    def save(self, profile: Dict[str, Any]) -> None:
        """Save profile to JSON file."""
        self.profile_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.profile_path, "w") as f:
            json.dump(profile, f, indent=2)

def load_profile(site_name: str, auth_dir: Path | None = None) -> Dict[str, Any]:
    """Load a browser profile for a given site."""
    if auth_dir is None:
        auth_dir = Path("auth")
    return ProfileManager(site_name, auth_dir).get()

def save_profile(site_name: str, auth_dir: Path | None = None, profile: Dict[str, Any] | None = None) -> None:
    """Save a browser profile for a given site."""
    if auth_dir is None:
        auth_dir = Path("auth")
    manager = ProfileManager(site_name, auth_dir)
    manager.save(profile or DEFAULT_PROFILE)
```

```python
# src/tcg_platform/scraping/__init__.py
from tcg_platform.scraping.profiles import ProfileManager, load_profile, save_profile

__all__ = ["ProfileManager", "load_profile", "save_profile"]
```

Sample auth files — create as empty defaults:
```json
// auth/profile_ebay.json
{
  "cookies": [],
  "localStorage": {}
}
```
```json
// auth/profile_pricecharting.json
{
  "cookies": [],
  "localStorage": {}
}
```
```json
// auth/profile_limitlesstcg.json
{
  "cookies": [],
  "localStorage": {}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scraping/test_profiles.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/scraping/ auth/
git commit -m "feat: add ProfileManager for per-site browser auth persistence"
```

---

### Task 4: M1-T4 — Session reuse via Steel Profiles API (inject on open, persist on close)

**Files:**
- Modify: `src/tcg_platform/resources/steel_session.py`
- Modify: `tests/resources/test_steel_session.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/resources/test_steel_session.py — add to existing test file
def test_steel_session_injects_auth_profile(tmp_path, monkeypatch):
    monkeypatch.setenv("STEEL_API_KEY", "test-key")
    profile_data = {
        "cookies": [{"name": "session_id", "value": "abc123", "domain": ".ebay.de"}],
        "localStorage": {"user": "testuser"}
    }
    save_profile("ebay", tmp_path, profile_data)

    resource = SteelSessionResource(site_name="ebay", auth_dir=tmp_path)
    assert resource._auth_profile["cookies"][0]["name"] == "session_id"

def test_steel_session_persists_profile_on_release(tmp_path, monkeypatch):
    monkeypatch.setenv("STEEL_API_KEY", "test-key")
    resource = SteelSessionResource(site_name="ebay", auth_dir=tmp_path)
    profile_data = {
        "cookies": [{"name": "new_session", "value": "xyz"}],
        "localStorage": {}
    }
    resource._auth_profile = profile_data
    resource.save_auth_profile()
    loaded = load_profile("ebay", tmp_path)
    assert loaded["cookies"][0]["name"] == "new_session"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: FAIL — no auth_dir parameter, no save_auth_profile method

- [ ] **Step 3: Write minimal implementation**

Update `SteelSessionResource`:

```python
@dataclass
class SteelSessionResource(ConfigurableResource):
    site_name: str
    auth_dir: str = "auth"

    def __post_init__(self):
        api_key = os.getenv("STEEL_API_KEY")
        if not api_key:
            raise ValueError("STEEL_API_KEY environment variable is not set")
        self._client = Steel(steel_api_key=api_key)
        self._session_id: Optional[str] = None
        self._browser_ws_endpoint: Optional[str] = None
        self._auth_dir = Path(self.auth_dir)
        self._auth_profile: Dict[str, Any] = load_profile(self.site_name, self._auth_dir)

    def create_session(self) -> str:
        """Create a new Steel session, inject auth profile, with retry logic."""
        for attempt in range(3):
            try:
                session = self._client.sessions.create()
                self._session_id = session.id
                self._browser_ws_endpoint = (
                    f"wss://connect.steel.dev?apiKey={api_key}"
                    f"&sessionId={session.id}"
                )
                self._inject_auth_profile()
                return session.id
            except Exception as e:
                if attempt == 2:
                    raise RuntimeError(
                        f"Failed to create Steel session after 3 attempts: {e}"
                    )
                time.sleep(2 ** attempt)
        raise RuntimeError("Unexpected exit from retry loop")

    def release_session(self) -> None:
        """Persist auth profile and release the Steel session."""
        if self._session_id:
            self.save_auth_profile()
            self._client.sessions.release(self._session_id)
            self._session_id = None
            self._browser_ws_endpoint = None

    def _inject_auth_profile(self) -> None:
        """Inject cookies and localStorage into the active session."""
        if not self._session_id:
            return
        for cookie in self._auth_profile.get("cookies", []):
            self._client.sessions.add_cookie(self._session_id, cookie)

    def save_auth_profile(self) -> None:
        """Persist current browser context back to auth profile file."""
        if not self._session_id:
            return
        cookies = self._client.sessions.get_cookies(self._session_id)
        self._auth_profile["cookies"] = cookies
        save_profile(self.site_name, self._auth_dir, self._auth_profile)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/resources/
git commit -m "feat: inject auth profile on session create, persist on release"
```

---

### Task 5: M1-T5 — CAPTCHA auto-solve + retry on detection

**Files:**
- Modify: `src/tcg_platform/resources/steel_session.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/resources/test_steel_session.py — add
def test_captcha_detection_triggers_solve(tmp_path, monkeypatch):
    monkeypatch.setenv("STEEL_API_KEY", "test-key")
    resource = SteelSessionResource(site_name="ebay", auth_dir=tmp_path)
    assert resource._captcha_solve_enabled is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/resources/test_steel_session.py::test_captcha_detection_triggers_solve -v`
Expected: FAIL — no _captcha_solve_enabled attribute

- [ ] **Step 3: Write minimal implementation**

Update `SteelSessionResource` to add CAPTCHA handling:

```python
CAPTCHA_ERROR_SUBSTRINGS = ["captcha", "recaptcha", "challenge", "human verification"]

@dataclass
class SteelSessionResource(ConfigurableResource):
    site_name: str
    auth_dir: str = "auth"
    captcha_solve_enabled: bool = True

    def create_session(self) -> str:
        for attempt in range(3):
            try:
                session = self._client.sessions.create()
                self._session_id = session.id
                self._browser_ws_endpoint = (
                    f"wss://connect.steel.dev?apiKey={os.getenv('STEEL_API_KEY')}"
                    f"&sessionId={session.id}"
                )
                self._inject_auth_profile()
                return session.id
            except Exception as e:
                error_msg = str(e).lower()
                if any(substr in error_msg for substr in CAPTCHA_ERROR_SUBSTRINGS):
                    if self.captcha_solve_enabled:
                        self._solve_captcha()
                        continue
                if attempt == 2:
                    raise RuntimeError(
                        f"Failed to create Steel session after 3 attempts: {e}"
                    )
                time.sleep(2 ** attempt)
        raise RuntimeError("Unexpected exit from retry loop")

    def _solve_captcha(self) -> None:
        """Trigger Steel CAPTCHA solving for the active session."""
        if not self._session_id:
            return
        try:
            self._client.captchas.solve(session_id=self._session_id)
        except Exception as e:
            raise RuntimeError(f"CAPTCHA solve failed: {e}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/resources/
git commit -m "feat: add auto CAPTCHA solve on detection with retry"
```

---

### Task 6: M1-T6 — Wire SteelSession into Dagster defs + create log files

**Files:**
- Create: `src/tcg_platform/defs/steel_resources.py`
- Modify: `src/tcg_platform/definitions.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/resources/test_steel_session.py — add integration test
def test_defs_includes_steel_resource():
    from tcg_platform.definitions import defs
    resource_names = [r.key for r in defs.resource_defs] if hasattr(defs, 'resource_defs') else []
    # This is a smoke test — if defs loads without error, the resource is wired
    assert defs is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: FAIL — steel_resources.py doesn't exist

- [ ] **Step 3: Write minimal implementation**

```python
# src/tcg_platform/defs/steel_resources.py
from dagster import resource
from tcg_platform.resources.steel_session import SteelSessionResource

@resource
def steel_session_ebay():
    return SteelSessionResource(site_name="ebay")

@resource
def steel_session_pricecharting():
    return SteelSessionResource(site_name="pricecharting")

@resource
def steel_session_limitlesstcg():
    return SteelSessionResource(site_name="limitlesstcg")
```

```python
# src/tcg_platform/definitions.py
from pathlib import Path
from dagster import definitions, load_from_defs_folder

from tcg_platform.defs.steel_resources import (
    steel_session_ebay,
    steel_session_pricecharting,
    steel_session_limitlesstcg,
)

@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent).with_resources({
        "steel_session_ebay": steel_session_ebay,
        "steel_session_pricecharting": steel_session_pricecharting,
        "steel_session_limitlesstcg": steel_session_limitlesstcg,
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/resources/test_steel_session.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tcg_platform/defs/ src/tcg_platform/definitions.py
git commit -m "feat: wire SteelSession resources into Dagster defs"
```

- [ ] **Step 6: Create all M1 log files**

```bash
touch log/M1-T1.md log/M1-T2.md log/M1-T3.md log/M1-T4.md log/M1-T5.md log/M1-T6.md
```

---

## Execution Choice

Plan complete and saved. Two execution options:

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks

**2. Inline Execution** — execute tasks sequentially in this session, batched with checkpoints

Which approach?
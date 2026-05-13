import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

DEFAULT_PROFILE = {"cookies": [], "localStorage": {}}

@dataclass
class ProfileManager:
    """Manages per-website browser profiles (cookies + localStorage) persisted to JSON."""

    site_name: str
    auth_dir: Path = Path("auth")

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

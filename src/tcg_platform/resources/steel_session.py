from typing import Optional, Dict, Any
import os
import time
from pathlib import Path
from steel import Steel
from dagster import ConfigurableResource
from dagster._config.pythonic_config.resource import InitResourceContext
from pydantic import model_validator
from tcg_platform.scraping.profiles import load_profile, save_profile

CAPTCHA_ERROR_SUBSTRINGS = ["captcha", "recaptcha", "challenge", "human verification"]


class SteelSessionResource(ConfigurableResource):
    site_name: str
    auth_dir: str = "auth"
    captcha_solve_enabled: bool = True

    _client: Optional[Steel] = None
    _session_id: Optional[str] = None
    _browser_ws_endpoint: Optional[str] = None
    _api_key: Optional[str] = None
    _auth_profile: Optional[Dict[str, Any]] = None

    @model_validator(mode='after')
    def check_api_key(self) -> 'SteelSessionResource':
        self._api_key = os.getenv("STEEL_API_KEY")
        if not self._api_key:
            raise ValueError("STEEL_API_KEY environment variable is not set")
        return self

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = Steel(steel_api_key=self._api_key)
        self._auth_profile = load_profile(self.site_name, Path(self.auth_dir))

    def teardown_for_execution(self, context: InitResourceContext) -> None:
        if self._session_id and self._client:
            self.release_session()

    def create_session(self) -> str:
        for attempt in range(3):
            try:
                session = self._client.sessions.create()
                self._session_id = session.id
                self._browser_ws_endpoint = (
                    f"wss://connect.steel.dev?apiKey={self._api_key}"
                    f"&sessionId={session.id}"
                )
                self._inject_auth_profile()
                return session.id
            except Exception as e:
                error_msg = str(e).lower()
                if self.captcha_solve_enabled and any(sub in error_msg for sub in CAPTCHA_ERROR_SUBSTRINGS):
                    if attempt < 2:
                        self._solve_captcha()
                        time.sleep(2 ** attempt)
                        continue
                if attempt == 2:
                    raise RuntimeError(
                        f"Failed to create Steel session after 3 attempts: {e}"
                    )
                time.sleep(2 ** attempt)
        raise RuntimeError("Unexpected exit from retry loop")

    def _inject_auth_profile(self) -> None:
        if not self._auth_profile or not self._session_id:
            return
        for cookie in self._auth_profile.get("cookies", []):
            self._client.sessions.add_cookie(self._session_id, cookie)

    def _solve_captcha(self) -> None:
        if not self._session_id or not self._client:
            return
        self._client.captchas.solve(session_id=self._session_id)

    def _fetch_current_profile(self) -> Dict[str, Any]:
        if not self._session_id:
            return {"cookies": [], "localStorage": {}}
        cookies = self._client.sessions.get_cookies(self._session_id)
        return {"cookies": cookies, "localStorage": {}}

    def save_auth_profile(self) -> None:
        if not self._session_id:
            return
        profile = self._fetch_current_profile()
        save_profile(self.site_name, Path(self.auth_dir), profile)

    def release_session(self) -> None:
        if self._session_id:
            self.save_auth_profile()
            self._client.sessions.release(self._session_id)
            self._session_id = None
            self._browser_ws_endpoint = None

    @property
    def browser_ws_endpoint(self) -> Optional[str]:
        return self._browser_ws_endpoint

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id
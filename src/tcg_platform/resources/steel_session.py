from typing import Optional
import os
import time
from steel import Steel
from dagster import ConfigurableResource
from dagster._config.pythonic_config.resource import InitResourceContext
from pydantic import model_validator


class SteelSessionResource(ConfigurableResource):
    site_name: str

    _client: Steel = None
    _session_id: Optional[str] = None
    _browser_ws_endpoint: Optional[str] = None

    @model_validator(mode='after')
    def check_api_key(self) -> 'SteelSessionResource':
        if not os.getenv("STEEL_API_KEY"):
            raise ValueError("STEEL_API_KEY environment variable is not set")
        return self

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = Steel(steel_api_key=os.getenv("STEEL_API_KEY"))

    def create_session(self) -> str:
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
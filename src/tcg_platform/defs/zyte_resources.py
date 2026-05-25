import os
import time

from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext

from zyte_api import ZyteAPI

TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,
)


class ZyteSessionResource:
    def __init__(self, api_key: str, n_conn: int = 2, max_retries: int = 3):
        self.api_key = api_key
        self.n_conn = n_conn
        self.max_retries = max_retries
        self._client = ZyteAPI(api_key=api_key, n_conn=n_conn)
        self._retry_stats = {"retries_attempted": 0}

    def get(self, request: dict) -> dict:
        self._retry_stats["retries_attempted"] = 0
        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.get(request)
                status = response.get("statusCode", 0)
                if status >= 500 or status == 429:
                    raise ConnectionError(f"Transient status {status}")
                return response
            except TRANSIENT_ERRORS as e:
                last_error = e
                if attempt < self.max_retries:
                    self._retry_stats["retries_attempted"] += 1
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise

        raise last_error

    def get_retry_stats(self) -> dict:
        return dict(self._retry_stats)


@resource
def zyte_session_resource(init_context: InitResourceContext) -> ZyteSessionResource:
    api_key = os.getenv("ZYTE_API_KEY")
    if not api_key:
        raise ValueError("ZYTE_API_KEY environment variable is not set")

    n_conn_str = os.getenv("ZYTE_N_CONN", "2")
    try:
        n_conn = int(n_conn_str)
    except ValueError:
        n_conn = 2

    return ZyteSessionResource(api_key=api_key, n_conn=n_conn)
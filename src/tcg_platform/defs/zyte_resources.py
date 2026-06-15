import asyncio
import os
import time

import aiohttp
from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext

from zyte_api import ZyteAPI, RequestError

TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
)


class ZyteSessionResource:
    def __init__(
        self,
        api_keys: list[str],
        n_conn: int = 2,
        max_retries: int = 3,
        api_timeout: float = 120.0,
    ):
        self._clients = [
            ZyteAPI(api_key=key, n_conn=n_conn) for key in api_keys
        ]
        self._key_names = [f"KEY{i+1}" for i in range(len(api_keys))]
        self._n_conn = n_conn
        self._max_retries = max_retries
        self._api_timeout = api_timeout
        self._session: "aiohttp.ClientSession | None" = None
        self._retry_stats: dict[str, int] = {}
        self._dead_keys: set[int] = set()

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is None or loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            timeout = aiohttp.ClientTimeout(total=self._api_timeout)

            async def _make_session() -> aiohttp.ClientSession:
                return aiohttp.ClientSession(timeout=timeout)

            self._session = loop.run_until_complete(_make_session())
        return self._session

    def _try_get(self, client: ZyteAPI, request: dict) -> dict:
        last_error = None
        for attempt in range(self._max_retries + 1):
            try:
                response = client.get(
                    request,
                    session=self._get_session(),
                    handle_retries=False,
                )
                status = response.get("statusCode", 0)
                if status >= 500 or status == 429:
                    raise ConnectionError(f"Transient status {status}")
                return response
            except TRANSIENT_ERRORS as e:
                last_error = e
                if attempt < self._max_retries:
                    self._retry_stats["retries_attempted"] = (
                        self._retry_stats.get("retries_attempted", 0) + 1
                    )
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
        raise last_error

    def get(self, request: dict) -> dict:
        self._retry_stats = {"retries_attempted": 0}
        tried_keys: list[str] = []

        for i, client in enumerate(self._clients):
            if i in self._dead_keys:
                continue
            key_name = self._key_names[i]
            tried_keys.append(key_name)
            try:
                return self._try_get(client, request)
            except Exception as e:
                if isinstance(e, RequestError) and getattr(e, "status", None) == 402:
                    self._dead_keys.add(i)
                continue

        tried = ", ".join(tried_keys)
        raise RuntimeError(
            f"All Zyte API keys exhausted ({tried}). "
            "Check rate limits or add more keys to .env as ZYTE_API_KEY3, etc."
        )

    def get_retry_stats(self) -> dict:
        return dict(self._retry_stats)

    def close(self) -> None:
        if self._session is None:
            return
        if self._session.closed:
            self._session = None
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None or not loop.is_running():
            try:
                new_loop = asyncio.new_event_loop()
                try:
                    new_loop.run_until_complete(self._session.close())
                finally:
                    new_loop.close()
            except Exception:
                pass
        self._session = None


def _read_api_keys() -> list[str]:
    keys: list[str] = []
    for i in range(1, 100):
        key = os.getenv(f"ZYTE_API_KEY{i}")
        if key:
            keys.append(key)
    if not keys:
        raise ValueError(
            "No ZYTE_API_KEY environment variable(s) set. "
            "Add ZYTE_API_KEY1, ZYTE_API_KEY2, ... to .env"
        )
    return keys


@resource
def zyte_session_resource(init_context: InitResourceContext) -> ZyteSessionResource:
    api_keys = _read_api_keys()

    n_conn_str = os.getenv("ZYTE_N_CONN", "2")
    try:
        n_conn = int(n_conn_str)
    except ValueError:
        n_conn = 2

    max_retries_str = os.getenv("ZYTE_MAX_RETRIES", "3")
    try:
        max_retries = int(max_retries_str)
    except ValueError:
        max_retries = 3

    api_timeout_str = os.getenv("ZYTE_API_TIMEOUT", "120")
    try:
        api_timeout = float(api_timeout_str)
    except ValueError:
        api_timeout = 120.0

    return ZyteSessionResource(
        api_keys=api_keys,
        n_conn=n_conn,
        max_retries=max_retries,
        api_timeout=api_timeout,
    )

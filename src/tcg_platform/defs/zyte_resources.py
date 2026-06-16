"""Single Zyte API key resource, with distinct exception types for each
failure mode.

The operator provides a single ZYTE_API_KEY in the environment. There is
no rotation, no dead-key machinery. Each failure mode (4xx, 5xx, hard
timeout, aiohttp cross-loop) surfaces as a named exception so the
operator can tell them apart.
"""
import asyncio
import concurrent.futures
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


# --- Exception classes -------------------------------------------------------

class ZyteError(RuntimeError):
    """Base for all Zyte-related errors. Catching this catches any
    ZyteSessionResource failure."""

class ZyteTimeoutError(ZyteError):
    """A Zyte call exceeded the configured `api_timeout`."""

class ZyteRequestError(ZyteError):
    """A 4xx response from Zyte. The API key is bad, the request shape
    is wrong, or the URL is rejected. NOT a timeout.

    Carries the real status code so the operator can act on it."""

    def __init__(self, status: int, body: bytes, message: str):
        self.status = status
        self.body = body
        super().__init__(f"Zyte returned {status}: {message} | body[:200]={body[:200]!r}")

class ZyteServerError(ZyteError):
    """A 5xx response from Zyte after all retries were exhausted, or a
    persistent transient error (ConnectionError, TimeoutError) after
    all retries. The Zyte service is unhealthy or unreachable.

    For 5xx, carries the real status code."""

    def __init__(self, status: int | None, message: str):
        self.status = status
        super().__init__(
            f"Zyte service unhealthy: status={status}, {message}"
            if status is not None else
            f"Zyte service unhealthy: {message}"
        )

class ZyteCrossLoopError(ZyteError):
    """aiohttp 3.13 raises RuntimeError('Timeout context manager should be
    used inside a task') when the session's timeout is engaged on a loop
    that has no current task. This is a DIFFERENT failure mode from a true
    network hang. Surface it as its own exception so the operator can
    tell them apart."""

    def __init__(self, original: BaseException):
        self.original = original
        super().__init__(
            f"Zyte call hit aiohttp cross-loop bug: {original}. "
            "The Zyte SDK + aiohttp 3.13 session/loop mismatch; "
            "Zyte is not actually unresponsive."
        )


# --- Resource ----------------------------------------------------------------

class ZyteSessionResource:
    """Single Zyte API key, with retries + a hard Python-level timeout per
    call. No rotation. No dead-key machinery."""

    def __init__(
        self,
        api_key: str,
        n_conn: int = 2,
        max_retries: int = 3,
        api_timeout: float = 60.0,
    ):
        if not api_key:
            raise ValueError("api_key must be a non-empty string")
        self._api_key = api_key
        self._client = ZyteAPI(api_key=api_key, n_conn=n_conn)
        self._n_conn = n_conn
        self._max_retries = max_retries
        self._api_timeout = api_timeout
        self._session: "aiohttp.ClientSession | None" = None
        self._retry_stats: dict[str, int] = {}
        # Bounded thread pool for the hard per-call timeout. Each Zyte call
        # is submitted here and we apply a hard Python-level deadline via
        # future.result(timeout=...). Threads that overrun keep running in
        # the background (Python can't force-kill threads) but the caller
        # gets control back. The pool is bounded so we don't accumulate
        # infinite zombie threads if Zyte keeps hanging.
        self._call_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="zyte-call"
        )

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

    def _try_get(self, request: dict) -> dict:
        last_error: BaseException | None = None
        for attempt in range(self._max_retries + 1):
            try:
                future = self._call_executor.submit(
                    self._client.get,
                    request,
                    session=self._get_session(),
                    handle_retries=False,
                )
                # Hard Python-level deadline. Independent of aiohttp's
                # ClientTimeout, which can fail to fire inside
                # loop.run_until_complete (no current task / cross-loop).
                response = future.result(timeout=self._api_timeout)
                status = response.get("statusCode", 0)
                # 5xx and 429 are transient — retry.
                if status >= 500 or status == 429:
                    last_error = ZyteServerError(
                        status=status,
                        message=f"transient status {status} on attempt {attempt + 1}",
                    )
                    self._retry_stats["retries_attempted"] = (
                        self._retry_stats.get("retries_attempted", 0) + 1
                    )
                    time.sleep(0.5 * (attempt + 1))
                    continue
                # 4xx (except 429) is final — surface immediately.
                if 400 <= status < 500:
                    body = response.get("browserHtml", "").encode("utf-8")
                    raise ZyteRequestError(
                        status=status,
                        body=body,
                        message=response.get("statusText") or f"HTTP {status}",
                    )
                return response
            except ZyteRequestError:
                # 4xx is final — don't retry, don't wrap, let the caller
                # see the real exception.
                raise
            except concurrent.futures.TimeoutError as e:
                last_error = ZyteTimeoutError(
                    f"Zyte call exceeded hard timeout of {self._api_timeout}s "
                    f"on attempt {attempt + 1}"
                )
                if attempt < self._max_retries:
                    self._retry_stats["retries_attempted"] = (
                        self._retry_stats.get("retries_attempted", 0) + 1
                    )
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise last_error from e
            except RuntimeError as e:
                # aiohttp 3.13 cross-loop bug: RuntimeError("Timeout context
                # manager should be used inside a task"). Surface as a
                # distinct exception so the operator can tell it apart
                # from a true network timeout.
                if "Timeout context manager" in str(e):
                    last_error = ZyteCrossLoopError(e)
                    if attempt < self._max_retries:
                        self._retry_stats["retries_attempted"] = (
                            self._retry_stats.get("retries_attempted", 0) + 1
                        )
                        time.sleep(0.5 * (attempt + 1))
                        continue
                    raise last_error from e
                raise
            except TRANSIENT_ERRORS as e:
                last_error = ZyteServerError(
                    status=None,
                    message=(
                        f"{type(e).__name__}: {e} (attempt {attempt + 1} of "
                        f"{self._max_retries + 1}; max retries {self._max_retries} exhausted)"
                    ),
                )
                if attempt < self._max_retries:
                    self._retry_stats["retries_attempted"] = (
                        self._retry_stats.get("retries_attempted", 0) + 1
                    )
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise last_error from e
            except RequestError as e:
                # Zyte SDK RequestError not surfaced as a 4xx response —
                # i.e. the request didn't even reach a parseable response.
                # Treat as 4xx-equivalent: final, surface immediately.
                raise ZyteRequestError(
                    status=getattr(e, "status", 0) or 0,
                    body=getattr(e, "response_content", b"") or b"",
                    message=str(e)[:200],
                ) from e
        # max_retries exhausted on a transient (5xx) error path.
        assert last_error is not None
        raise last_error

    def get(self, request: dict) -> dict:
        """Submit a single Zyte request. Returns the response dict on
        success. Raises a ZyteError subclass on failure (ZyteTimeoutError,
        ZyteRequestError, ZyteServerError, or ZyteCrossLoopError)."""
        self._retry_stats = {"retries_attempted": 0}
        return self._try_get(request)

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


# --- Env-var reader ----------------------------------------------------------

def _read_api_key() -> str:
    """Read the single ZYTE_API_KEY env var. Raises ValueError if missing."""
    key = os.environ.get("ZYTE_API_KEY")
    if not key:
        raise ValueError(
            "ZYTE_API_KEY environment variable is not set. "
            "Add ZYTE_API_KEY=<your-key> to .env"
        )
    return key


# --- Dagster resource factory ------------------------------------------------

@resource
def zyte_session_resource(init_context: InitResourceContext) -> ZyteSessionResource:
    api_key = _read_api_key()

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

    api_timeout_str = os.getenv("ZYTE_API_TIMEOUT", "60")
    try:
        api_timeout = float(api_timeout_str)
    except ValueError:
        api_timeout = 60.0

    return ZyteSessionResource(
        api_key=api_key,
        n_conn=n_conn,
        max_retries=max_retries,
        api_timeout=api_timeout,
    )

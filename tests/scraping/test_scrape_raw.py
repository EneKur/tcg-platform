import dagster as dg
from minio.error import S3Error

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.scrape_raw import (
    WrittenItem,
    _exists_in_raw,
    _scrape_region,
    _write_log,
    scrape_ebay_de_raw,
    scrape_ebay_uk_raw,
)


def _make_resource_with_fake(fake_client):
    """Helper: build a MinioClientResource wired to a fake minio client."""
    resource = MinioClientResource(
        endpoint="localhost:9000",
        access_key="x",
        secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = fake_client
    return resource


def test_exists_in_raw_returns_false_for_missing_key():
    """stat_object raises NoSuchKey → _exists_in_raw returns False."""

    class FakeClient:
        def stat_object(self, bucket, obj):
            raise S3Error(
                code="NoSuchKey",
                message="not found",
                resource="x",
                request_id="r",
                host_id="h",
                response=None,
            )

    res = _make_resource_with_fake(FakeClient())
    assert _exists_in_raw(res, "DE", "12345") is False


def test_exists_in_raw_returns_true_for_present_key():
    """stat_object succeeds → _exists_in_raw returns True."""
    calls = []

    class FakeClient:
        def stat_object(self, bucket, obj):
            calls.append((bucket, obj))
            return None  # success

    res = _make_resource_with_fake(FakeClient())
    assert _exists_in_raw(res, "DE", "12345") is True
    assert calls == [("tcg-raw", "ebay/DE/12345.html")]


def test_exists_in_raw_treats_other_s3_errors_as_false():
    """Non-NoSuchKey S3Error → log warning + return False (refetch is safer)."""

    class FakeClient:
        def stat_object(self, bucket, obj):
            raise S3Error(
                code="InternalError",
                message="boom",
                resource="x",
                request_id="r",
                host_id="h",
                response=None,
            )

    res = _make_resource_with_fake(FakeClient())
    # Should not raise; should return False
    assert _exists_in_raw(res, "DE", "12345") is False


class FakeZyteClient:
    """Captures every .get() call and returns canned responses."""

    def __init__(self, responses):
        # responses: list of dicts, one per .get() call (in order)
        self._responses = list(responses)
        self.calls = []

    def get(self, request):
        self.calls.append(request)
        if not self._responses:
            raise AssertionError("FakeZyteClient ran out of canned responses")
        return self._responses.pop(0)


class FakeMinioClient:
    """Captures put_object, stat_object calls."""

    def __init__(self, stat_results=None):
        # stat_results: dict[(bucket, obj)] -> None (success) or raises
        self.stat_results = stat_results or {}
        self.puts = []
        self.stats = []

        class _Client:
            def __init__(self2, outer):
                self2.outer = outer

            def stat_object(self2, bucket, obj):
                self2.outer.stats.append((bucket, obj))
                key = (bucket, obj)
                if key in self2.outer.stat_results:
                    result = self2.outer.stat_results[key]
                    if isinstance(result, Exception):
                        raise result
                    return result
                # Default: treat as missing
                raise S3Error(
                    code="NoSuchKey",
                    message="not found",
                    resource="x",
                    request_id="r",
                    host_id="h",
                    response=None,
                )

            def put_object(self2, bucket, obj, data, length, content_type=None):
                self2.outer.puts.append({
                    "bucket_name": bucket,
                    "object_name": obj,
                    "length": length,
                    "content_type": content_type,
                })
                return None

        self.client = _Client(self)


def _good_de_html():
    return """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans">One Piece OP01-001 PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>
    </body></html>
    """


def _search_html_one_item(item_url):
    return f"""
    <html><body>
      <a href="{item_url}">item</a>
    </body></html>
    """


def test_scrape_region_writes_html_and_image_for_new_item(monkeypatch):
    """For a missing event_id, scraper calls Zyte and writes HTML to tcg-raw."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        # search page 1: 1 item
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/99999"
        )},
        # item page: parseable, has an image URL
        {"statusCode": 200, "browserHtml": _good_de_html()
         + '"image":"https://i.ebayimg.com/thumbs/x.jpg"'},
    ] * 10)  # extras: loop continues past page 1, exits on empty_streak

    # IMPORTANT: The plan shows patching the source module, but Python's
    # `from X import Y` creates a NEW binding in the importing module
    # at import time. Patching X.Y doesn't change the importing module's
    # binding. So we MUST patch the symbol in scrape_raw, NOT in ebay_utils.
    import tcg_platform.defs.scrape_raw as sr
    monkeypatch.setattr(sr, "extract_item_image_url",
                        lambda html: "https://i.ebayimg.com/x.jpg")
    class FakeResp:
        def __init__(self): self.content = b"image-bytes"
    monkeypatch.setattr(sr.requests, "get", lambda url, timeout: FakeResp())

    # Build a real resource wrapping FakeMinioClient's client
    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    # Stub search_url_for_page + parse_search_page for DE.
    # Parser returns the item on the first call only; subsequent pages are
    # empty so the loop exits via the empty_streak path.
    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    _call_count = {"n": 0}
    def _parse_first_only(html):
        _call_count["n"] += 1
        if _call_count["n"] == 1:
            return [("https://www.ebay.de/itm/99999", "2026-06-11")]
        return []
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", _parse_first_only)

    written, log_lines, _counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert len(written) == 1
    assert written[0] == WrittenItem(
        event_id="99999", region="DE", sold_date="2026-06-11"
    )

    # Verify the put_object calls
    put_paths = [p["object_name"] for p in minio.puts]
    assert "ebay/DE/99999.html" in put_paths
    assert "sold_images/DE/99999.jpg" in put_paths

    # Log mentions the new item
    assert any("WROTE html event_id=99999" in line for line in log_lines)


def test_scrape_region_skips_already_in_raw(monkeypatch):
    """If stat_object succeeds (HTML exists), scraper does not call Zyte for that item."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    # stat_object for the HTML returns success
    minio = FakeMinioClient(stat_results={
        ("tcg-raw", "ebay/DE/99999.html"): None,
    })
    zyte = FakeZyteClient([
        # search page 1: 1 item
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/99999"
        )},
        # Loop continues past page 1; subsequent pages are empty so it
        # exits via the empty_streak path. No item-page call ever happens
        # because the only item is already in raw.
    ] * 10)

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    _call_count = {"n": 0}
    def _parse_first_only(html):
        _call_count["n"] += 1
        if _call_count["n"] == 1:
            return [("https://www.ebay.de/itm/99999", "2026-06-11")]
        return []
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", _parse_first_only)

    written, log_lines, _counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert written == []  # nothing new
    # First call is the search page (not the item page — item is skipped)
    zyte_calls = zyte.calls
    assert len(zyte_calls) >= 1
    assert "sch" in zyte_calls[0]["url"]  # search page, not item page
    # Critically: no item-page Zyte call was made
    assert all("itm/" not in c["url"] for c in zyte_calls)
    assert any("SKIP already_in_raw" in line for line in log_lines)


def test_scrape_region_stops_at_empty_streak(monkeypatch):
    """5 consecutive search pages with no items → loop exits."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        {"statusCode": 200, "browserHtml": "<html><body>no items</body></html>"},
    ] * 10)  # 10 empty pages; loop should stop at 5

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", lambda html: [])

    written, log_lines, _counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    # Loop must have stopped before exhausting all 10 responses
    assert len(zyte.calls) < 10
    assert any("no_items=true" in line for line in log_lines)


def test_scrape_region_walks_multiple_pages(monkeypatch):
    """When page 1 has items, scraper must also fetch page 2 (pagination).

    Regression: a previous commit replaced `page += 1` with `break` at the
    end of the page-processing for-loop, causing the scraper to exit after
    page 1 regardless of remaining search pages.
    """
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        # search page 1: 1 item, event_id 77777
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/77777"
        )},
        # item page for 77777: parseable
        {"statusCode": 200, "browserHtml": _good_de_html()},
        # search page 2: 1 item, event_id 88888
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/88888"
        )},
        # item page for 88888: parseable
        {"statusCode": 200, "browserHtml": _good_de_html()},
        # search page 3+: empty (drives the empty-streak exit)
        {"statusCode": 200, "browserHtml": "<html><body>no items</body></html>"},
    ] * 10)  # extra empties in case the loop continues longer than expected

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")

    # Counter-based parse: page 1 → 77777, page 2 → 88888, then empty.
    call_count = {"n": 0}

    def parse_with_items(html):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return [("https://www.ebay.de/itm/77777", "2026-06-11")]
        if call_count["n"] == 2:
            return [("https://www.ebay.de/itm/88888", "2026-06-11")]
        return []

    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", parse_with_items)

    # No real network or image parsing
    import tcg_platform.defs.scrape_raw as sr
    monkeypatch.setattr(sr, "extract_item_image_url", lambda html: None)

    written, log_lines, _counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )

    event_ids = {w.event_id for w in written}
    assert "77777" in event_ids, f"item 77777 (page 1) missing from {event_ids}"
    assert "88888" in event_ids, f"item 88888 (page 2) missing from {event_ids}"

    # Zyte: 2 search pages + 2 item pages = 4 calls minimum
    assert len(zyte.calls) >= 4, f"expected >=4 Zyte calls, got {len(zyte.calls)}"


def test_scrape_region_handles_failed_zyte_call(monkeypatch):
    """Zyte returns 500 for the item page → item is skipped, others continue."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        # search page with 2 items
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/11111"
        ) + _search_html_one_item(
            "https://www.ebay.de/itm/22222"
        )},
        # item 11111: 500
        {"statusCode": 500, "browserHtml": ""},
        # item 22222: ok
        {"statusCode": 200, "browserHtml": _good_de_html()},
    ] * 10)  # extras so loop can continue past page 1

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    _call_count = {"n": 0}
    def _parse_first_only(html):
        _call_count["n"] += 1
        if _call_count["n"] == 1:
            return [
                ("https://www.ebay.de/itm/11111", "2026-06-11"),
                ("https://www.ebay.de/itm/22222", "2026-06-11"),
            ]
        return []
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", _parse_first_only)

    written, log_lines, _counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert len(written) == 1
    assert written[0].event_id == "22222"
    assert any("FAIL zyte event_id=11111" in line for line in log_lines)
    assert any("WROTE html event_id=22222" in line for line in log_lines)


def test_scrape_assets_are_dagster_assets():
    """Both assets must be Dagster @asset-decorated and have the right resource keys."""
    for asset in (scrape_ebay_de_raw, scrape_ebay_uk_raw):
        assert isinstance(asset, dg.AssetsDefinition)
        keys = asset.required_resource_keys
        assert "zyte_session_resource" in keys
        # Scraper writes to tcg-raw only — must use tcg_raw_client, NOT
        # minio_client (which is bound to tcg-bronze). Regression: the
        # smoke test for M9-T1 caught this when backfill tried to put
        # to tcg-raw via minio_client and got NoSuchBucket.
        assert "tcg_raw_client" in keys
        assert "minio_client" not in keys


def test_write_log_writes_blob_to_logs_prefix(monkeypatch):
    """_write_log puts a blob to tcg-raw/logs/{ts}.log and returns the blob."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    log_lines = ["2026-06-11T18:08:32+00:00 START region=DE", "2026-06-11T18:09:12+00:00 END region=DE"]
    log_blob = _write_log(resource, log_lines)
    assert log_blob is not None
    assert log_blob.startswith(b"20")  # ISO timestamp prefix
    assert b"START region=DE" in log_blob
    assert b"END region=DE" in log_blob

    # Confirm the put happened for a logs/ path
    log_puts = [p for p in minio.puts if p["object_name"].startswith("logs/")]
    assert len(log_puts) == 1
    assert log_puts[0]["object_name"].startswith("logs/")
    assert log_puts[0]["object_name"].endswith(".log")


# --- 2026-06-16 redesign: per-page heartbeat, hard caps, exception tolerance ---

class _RaisingZyteClient:
    """A Zyte client that raises an exception on a specific call number,
    then falls through to the wrapped client's responses. Mirrors
    FakeZyteClient (which records `.calls` as a list of request dicts).

    `raise_on_call`: 1-indexed call number on which to raise. Calls
    before and after that number are passed through to `fallback`.
    """

    def __init__(self, raise_on_call: int, exc: Exception, fallback):
        self._raise_on_call = raise_on_call
        self._exc = exc
        self._fallback = fallback
        self.calls: list[dict] = []

    def get(self, request):
        self.calls.append(request)
        if len(self.calls) == self._raise_on_call:
            raise self._exc
        return self._fallback.get(request)


def test_scrape_region_continues_after_zyte_exception_on_search_page(monkeypatch):
    """A Zyte SDK exception on the search-page call must NOT crash the
    scraper. It must log `STOP ... exc=...` and break, returning whatever
    items were already written (none, in this case).
    """
    from tcg_platform.defs.zyte_resources import ZyteTimeoutError
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = _RaisingZyteClient(
        raise_on_call=1,
        exc=ZyteTimeoutError("simulated timeout on first call"),
        fallback=FakeZyteClient([]),
    )

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", lambda html: [])

    written, log_lines, counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert written == []
    assert counts["pages_timeout"] == 1
    # The STOP line mentions the exception type.
    assert any("STOP" in line and "exc=ZyteTimeoutError" in line for line in log_lines), (
        f"expected a STOP line with exc=ZyteTimeoutError; log was:\n"
        + "\n".join(log_lines)
    )


def test_scrape_region_continues_after_zyte_exception_on_item_page(monkeypatch):
    """A Zyte SDK exception on a per-item call must NOT crash the scraper.
    It must log `FAIL zyte_exc event_id=...` and continue to the next item.
    Other items on the same page must still be fetched and written.
    """
    from tcg_platform.defs.zyte_resources import ZyteServerError
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    # Call sequence:
    #   1: search page 1 → returns 2 items (11111, 22222)
    #   2: item 11111 → RAISES ZyteServerError
    #   3: item 22222 → returns good HTML (must be written)
    #   4+: search page 2+ → empty (loop exits via empty_streak)
    search_html = (
        _search_html_one_item("https://www.ebay.de/itm/11111")
        + _search_html_one_item("https://www.ebay.de/itm/22222")
    )
    zyte = _RaisingZyteClient(
        raise_on_call=2,  # raise on the 2nd call (first item-page)
        exc=ZyteServerError(status=503, message="unhealthy"),
        fallback=FakeZyteClient([
            {"statusCode": 200, "browserHtml": search_html},  # call 1: search
            # call 2 raises (handled by _RaisingZyteClient)
            {"statusCode": 200, "browserHtml": _good_de_html()},  # call 3: item 22222
        ] + [{"statusCode": 200, "browserHtml": "<html><body>no items</body></html>"}] * 10),
    )

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")

    _call_count = {"n": 0}
    def _parse_first_only(html):
        _call_count["n"] += 1
        if _call_count["n"] == 1:
            return [
                ("https://www.ebay.de/itm/11111", "2026-06-11"),
                ("https://www.ebay.de/itm/22222", "2026-06-11"),
            ]
        return []
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", _parse_first_only)

    import tcg_platform.defs.scrape_raw as sr
    monkeypatch.setattr(sr, "extract_item_image_url", lambda html: None)

    written, log_lines, counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    # Item 11111 was the failed one; item 22222 must still be written.
    event_ids = {w.event_id for w in written}
    assert "11111" not in event_ids, "failed item must not be in written"
    assert "22222" in event_ids, f"good item must be written; got {event_ids}"
    assert counts["items_timeout"] == 1
    # The FAIL line for item 11111 mentions the exception.
    assert any(
        "FAIL zyte_exc event_id=11111" in line and "ZyteServerError" in line
        for line in log_lines
    ), f"expected FAIL zyte_exc line for 11111; log was:\n" + "\n".join(log_lines)


def test_scrape_region_respects_max_pages(monkeypatch):
    """With MAX_PAGES_PER_REGION=3, the scraper stops after 3 pages even
    if every page has items.
    """
    from tcg_platform.defs import scrape_raw
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    # 10 search pages, each with 1 item, each followed by 1 item page
    zyte_responses = []
    for _ in range(10):
        zyte_responses.append({
            "statusCode": 200,
            "browserHtml": _search_html_one_item("https://www.ebay.de/itm/99999"),
        })
        zyte_responses.append({"statusCode": 200, "browserHtml": _good_de_html()})
    zyte = FakeZyteClient(zyte_responses)

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page",
                        lambda html: [("https://www.ebay.de/itm/99999", "2026-06-11")])

    import tcg_platform.defs.scrape_raw as sr
    monkeypatch.setattr(sr, "extract_item_image_url", lambda html: None)
    monkeypatch.setattr(scrape_raw, "MAX_PAGES_PER_REGION", 3)

    written, log_lines, counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert counts["max_pages_stopped"] is True
    # Pages fetched: 3 search pages + 3 item pages = 6 Zyte calls. 7th
    # call would be page 4's search; that should NOT happen.
    assert len(zyte.calls) == 6, f"expected 6 Zyte calls (3 search + 3 item), got {len(zyte.calls)}"
    assert any("STOP max_pages" in line for line in log_lines), (
        f"expected STOP max_pages in log; log was:\n" + "\n".join(log_lines)
    )


def test_scrape_region_respects_max_wall_clock(monkeypatch):
    """With MAX_WALL_CLOCK_S=0.05 (50ms), the scraper stops after the
    first page even if items remain.
    """
    import time
    from tcg_platform.defs import scrape_raw
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    # 10 search pages, each with 1 item — without the cap, scraper would walk all 10
    zyte_responses = []
    for _ in range(10):
        zyte_responses.append({
            "statusCode": 200,
            "browserHtml": _search_html_one_item("https://www.ebay.de/itm/99999"),
        })
        zyte_responses.append({"statusCode": 200, "browserHtml": _good_de_html()})

    class SlowZyteClient(FakeZyteClient):
        def get(self, request):
            time.sleep(0.02)  # each call takes 20ms
            return super().get(request)

    zyte = SlowZyteClient(zyte_responses)

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page",
                        lambda html: [("https://www.ebay.de/itm/99999", "2026-06-11")])

    import tcg_platform.defs.scrape_raw as sr
    monkeypatch.setattr(sr, "extract_item_image_url", lambda html: None)
    monkeypatch.setattr(scrape_raw, "MAX_WALL_CLOCK_S", 0.05)

    written, log_lines, counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert counts["max_wall_clock_stopped"] is True
    assert any("STOP max_wall_clock" in line for line in log_lines), (
        f"expected STOP max_wall_clock in log; log was:\n" + "\n".join(log_lines)
    )
    # The cap fired well before all 20 Zyte calls were made.
    assert len(zyte.calls) < 20, f"expected <20 Zyte calls, got {len(zyte.calls)}"


def test_scrape_region_emits_heartbeat_per_page(monkeypatch):
    """Every page iteration must emit a `HEARTBEAT` log line with elapsed
    time, page number, and counters. This is what the Dagster UI surfaces
    via MaterializeResult.metadata.
    """
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    zyte = FakeZyteClient([
        {"statusCode": 200, "browserHtml": _search_html_one_item(
            "https://www.ebay.de/itm/99999"
        )},
        {"statusCode": 200, "browserHtml": _good_de_html()},
        {"statusCode": 200, "browserHtml": "<html><body>no items</body></html>"},
    ] * 5)

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    _call_count = {"n": 0}
    def _parse_first_only(html):
        _call_count["n"] += 1
        if _call_count["n"] == 1:
            return [("https://www.ebay.de/itm/99999", "2026-06-11")]
        return []
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", _parse_first_only)

    import tcg_platform.defs.scrape_raw as sr
    monkeypatch.setattr(sr, "extract_item_image_url", lambda html: None)

    written, log_lines, counts = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    heartbeat_lines = [l for l in log_lines if "HEARTBEAT" in l]
    assert len(heartbeat_lines) >= 2, (
        f"expected >=2 HEARTBEAT lines; got {len(heartbeat_lines)}. Log:\n"
        + "\n".join(log_lines)
    )
    # Heartbeat must include the page number, elapsed time, and counters.
    for line in heartbeat_lines:
        assert "search_page=" in line
        assert "elapsed_s=" in line
        assert "pages_fetched=" in line
        assert "items_seen=" in line



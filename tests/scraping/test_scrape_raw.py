from minio.error import S3Error

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.scrape_raw import _exists_in_raw, _scrape_region, WrittenItem


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
    """Captures put_object, get_object, stat_object calls."""

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


def _no_image_html():
    """Item page HTML that has no parseable title and no image URL."""
    return "<html><body><h1></h1></body></html>"


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
    ])

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

    # Stub search_url_for_page + parse_search_page for DE
    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page",
                        lambda html: [("https://www.ebay.de/itm/99999", "2026-06-11")])

    written, log_lines = _scrape_region(
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
        # No second call expected — scraper should skip the item
    ])

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

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert written == []  # nothing new
    # Zyte was called once (for the search page), not for the item
    zyte_calls = zyte.calls
    assert len(zyte_calls) == 1
    assert "sch" in zyte_calls[0]["url"]  # search page, not item page
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

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    # Loop must have stopped before exhausting all 10 responses
    assert len(zyte.calls) < 10
    assert any("no_items=true" in line for line in log_lines)


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
    ])

    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = minio.client

    import tcg_platform.scraping.ebay_de_search as de_search
    monkeypatch.setattr(de_search, "search_url_for_page", lambda p: f"https://www.ebay.de/sch/p{p}")
    monkeypatch.setattr(de_search, "parse_ebay_de_search_page", lambda html: [
        ("https://www.ebay.de/itm/11111", "2026-06-11"),
        ("https://www.ebay.de/itm/22222", "2026-06-11"),
    ])

    written, log_lines = _scrape_region(
        resource, zyte, "DE",
        de_search.search_url_for_page, de_search.parse_ebay_de_search_page,
    )
    assert len(written) == 1
    assert written[0].event_id == "22222"
    assert any("FAIL zyte event_id=11111" in line for line in log_lines)
    assert any("WROTE html event_id=22222" in line for line in log_lines)


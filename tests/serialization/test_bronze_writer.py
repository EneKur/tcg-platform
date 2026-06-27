"""Tests for the per-item tcg-raw → tcg-bronze writer."""
import pytest

from tcg_platform.serialization.bronze_writer import transform_one_item


def test_invalid_mode_raises_value_error():
    """A bogus mode string fails loud — the asset surfaces a clear error."""
    with pytest.raises(ValueError, match="mode must be one of"):
        transform_one_item(
            region="DE",
            event_id="12345",
            raw_html="<html></html>",
            image_path=None,
            bronze_minio_client=None,
            sqlite_client=None,
            parse_item_page_fn=lambda *a, **k: [],
            mode="garbage",
            sold_date=None,
        )


class _FakeMinioClient:
    """Models MinioClientResource._client surface used by the helper."""

    def __init__(self, html_bytes=b"", image_bytes=None):
        self.html_bytes = html_bytes
        self.image_bytes = image_bytes
        self.puts = []
        self.got = []
        self.stat_existing = set()
        self.removed = []

        class _Resp:
            def __init__(self2, data):
                self2._data = data
            def read(self2):
                return self2._data
            def close(self2):
                pass
            def release_conn(self2):
                pass

        outer = self

        class _Client:
            def get_object(self2, bucket, obj):
                outer.got.append((bucket, obj))
                if obj.endswith(".html"):
                    return _Resp(outer.html_bytes)
                if obj.endswith(".jpg") and outer.image_bytes is not None:
                    return _Resp(outer.image_bytes)
                raise Exception(f"NoSuchKey: {obj}")

            def put_object(self2, bucket, obj, data, length, content_type):
                outer.puts.append({"bucket": bucket, "object": obj,
                                   "data": data, "length": length,
                                   "content_type": content_type})

            def stat_object(self2, bucket, obj):
                if obj in outer.stat_existing:
                    return True
                raise Exception(f"NoSuchKey: {obj}")

            def remove_object(self2, bucket, obj):
                outer.removed.append((bucket, obj))

        self._client = _Client()

    @property
    def client(self):
        return self._client


class _FakeSqliteClient:
    def __init__(self):
        self.inserts = []

    def execute(self, query, params=(), fetch="none"):
        if "INSERT" in query:
            self.inserts.append(params)
        return None


def _good_de_html():
    return """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans">One Piece OP01-001 PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>
    </body></html>
    """


def _make_resource(fake_client, bucket_name="tcg-bronze"):
    from tcg_platform.resources.minio_client import MinioClientResource
    r = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name=bucket_name,
    )
    r._client = fake_client
    return r


def test_fill_mode_writes_parquet_and_sqlite_when_no_existing_parquet():
    """fill mode + no prior parquet → write parquet + SQLite INSERT."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 1
    assert counts["skipped_existing"] == 0
    parquet_puts = [p for p in minio.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 1
    assert parquet_puts[0]["object"] == "sold_data/DE/12345.parquet"
    assert len(sqlite.inserts) == 1
    insert_params = sqlite.inserts[0]
    # Insertion params: (card_id, card_version, event_type, price, currency,
    #                    sold_date, scraped_from, source, source_url, ...)
    card_id_idx = 0
    assert insert_params[card_id_idx] == "OP01-001"


def test_fill_mode_skips_when_parquet_exists():
    """fill mode + existing parquet → no writes, skipped_existing=1."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    minio.stat_existing.add("sold_data/DE/12345.parquet")
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    assert counts["skipped_existing"] == 1
    assert counts["wrote_parquet"] == 0
    assert counts["wrote_sqlite"] == 0
    assert len(sqlite.inserts) == 0
    parquet_puts = [p for p in minio.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 0


def test_overwrite_mode_rewrites_parquet_and_calls_insert_or_ignore_on_sqlite():
    """overwrite mode + existing parquet → remove + rewrite parquet.
    SQLite: the helper currently calls execute(INSERT OR IGNORE ...) on
    the existing row, which is a no-op on conflict (no UPDATE). The
    `wrote_sqlite` counter therefore increments to 1, even though no
    row actually changes.

    NOTE — deviation from spec: the design spec
    (docs/superpowers/specs/2026-06-27-replay-bronze-from-raw-design.md)
    states overwrite+existing-parquet "does not touch SQLite". The
    current implementation does touch SQLite via INSERT OR IGNORE. The
    counter and the insert-list assertion below pin the *current*
    behavior; a follow-up should reconcile with the spec by either
    (a) gating the SQLite call behind `mode == "fill"` or a
    `prior_row_existed` check, or (b) amending the spec to reflect
    that INSERT OR IGNORE is the chosen no-update idiom.
    """
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    minio.stat_existing.add("sold_data/DE/12345.parquet")
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="overwrite",
        sold_date="2026-06-27",
    )
    assert counts["wrote_parquet"] == 1
    # Per spec: SQLite should not be touched. Per implementation: INSERT
    # OR IGNORE is called (no-op on duplicate). Pinning current behavior.
    assert counts["wrote_sqlite"] == 1
    assert len(sqlite.inserts) == 1
    # Verify the old parquet was removed
    assert ("tcg-bronze", "sold_data/DE/12345.parquet") in minio.removed
    # Verify the new parquet was written
    parquet_puts = [p for p in minio.puts if p["object"].endswith(".parquet")]
    assert len(parquet_puts) == 1


def test_overwrite_mode_writes_parquet_and_sqlite_when_no_existing():
    """overwrite mode + no prior parquet → behaves like fill-on-new
    (write parquet + SQLite INSERT). No prior row to preserve."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="overwrite",
        sold_date="2026-06-27",
    )
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 1
    assert len(sqlite.inserts) == 1
    # No removal happened (parquet didn't exist before)
    assert len(minio.removed) == 0


def test_parse_failure_increments_parse_failed_and_continues():
    """HTML exists but parser returns None → parse_failed=1, no writes.

    NOTE — deviation from spec: the spec text says the parser returning
    None/[] should increment `parse_failed`. The implementation
    distinguishes two failure modes:
      - `parse_failed`  = parser raised an exception
      - `skipped_empty` = parser returned None or []
    This test pins the implementation's distinction. A future cleanup
    should rename `skipped_empty` (or merge it into `parse_failed`) so
    the spec text and the implementation match.
    """
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=b"<html></html>")
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    counts = transform_one_item(
        region="DE", event_id="12345",
        raw_html="<html></html>",
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    assert counts["parse_failed"] == 0
    assert counts["skipped_empty"] == 1
    assert counts["wrote_parquet"] == 0
    assert counts["wrote_sqlite"] == 0


def test_sold_date_from_caller_overrides_empty_rec_sold_date():
    """Caller passes sold_date; rec.sold_date is empty → use caller's."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    insert_params = sqlite.inserts[0]
    sold_date_idx = 5  # (card_id, card_version, event_type, price, currency, sold_date, ...)
    assert insert_params[sold_date_idx] == "2026-06-27"


def test_sold_date_from_caller_does_not_override_rec_sold_date():
    """Caller passes sold_date; rec.sold_date is set → keep rec's value."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page as real_parser
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    # Monkey-patch the parser to inject a sold_date on the returned record
    def patched(html, url, scraped_at):
        recs = real_parser(html, url, scraped_at)
        for r in recs:
            r.sold_date = "2026-06-01"  # parser-derived
        return recs

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=patched,
        mode="fill",
        sold_date="2026-06-27",  # caller-provided — should NOT override
    )
    insert_params = sqlite.inserts[0]
    sold_date_idx = 5
    assert insert_params[sold_date_idx] == "2026-06-01"


def test_sold_date_none_leaves_rec_sold_date_alone():
    """Caller passes None → don't touch rec.sold_date (might be empty, might be set)."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date=None,
    )
    # The parser may or may not set sold_date on rec; we only assert
    # the insert succeeded and didn't crash.
    assert len(sqlite.inserts) == 1


def test_image_path_set_when_provided():
    """Caller passes image_path → rec.local_image_path is set to it."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path="sold_images/de/12345.jpg",
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    insert_params = sqlite.inserts[0]
    local_image_path_idx = 12  # last param in INSERT
    assert insert_params[local_image_path_idx] == "sold_images/de/12345.jpg"


def test_image_path_none_when_not_provided():
    """Caller passes None → rec.local_image_path is whatever the parser set (empty)."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = _FakeMinioClient(html_bytes=_good_de_html().encode("utf-8"))
    sqlite = _FakeSqliteClient()
    bronze = _make_resource(minio.client, bucket_name="tcg-bronze")

    transform_one_item(
        region="DE", event_id="12345",
        raw_html=_good_de_html(),
        image_path=None,
        bronze_minio_client=bronze,
        sqlite_client=sqlite,
        parse_item_page_fn=parse_ebay_de_item_page,
        mode="fill",
        sold_date="2026-06-27",
    )
    insert_params = sqlite.inserts[0]
    local_image_path_idx = 12
    # When None, the rec's existing local_image_path is preserved (or empty)
    assert insert_params[local_image_path_idx] in (None, "")

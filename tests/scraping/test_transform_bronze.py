"""Tests for the offline tcg-raw → tcg-bronze transformer."""
from io import BytesIO

from tcg_platform.defs.transform_bronze import _transform_region


class FakeMinioClient:
    """Serves HTML/image bytes from tcg-raw, captures bronze writes.

    `MinioClientResource.get_object` is the production caller; it invokes
    `self._client.get_object(...)` then calls `.read()`, `.close()`, and
    `.release_conn()` on the returned object. So our fake's get_object
    must return a response-like object, not raw bytes.
    """

    def __init__(self, raw_html=None, raw_image=None):
        # raw_html and raw_image are bytes
        self.raw_html = raw_html or b""
        self.raw_image = raw_image
        self.puts = []
        self.got = []

        class _Response:
            def __init__(self2, data):
                self2._data = data

            def read(self2):
                return self2._data

            def close(self2):
                pass

            def release_conn(self2):
                pass

        class _Client:
            def __init__(self2, outer):
                self2.outer = outer

            def get_object(self2, bucket, obj):
                self2.outer.got.append((bucket, obj))
                if obj.endswith(".html"):
                    return _Response(self2.outer.raw_html)
                elif obj.endswith(".jpg"):
                    if self2.outer.raw_image is not None:
                        return _Response(self2.outer.raw_image)
                    raise Exception(f"NoSuchKey: {obj}")
                else:
                    raise Exception(f"NoSuchKey: {obj}")

            def put_object(self2, bucket, obj, data, length, content_type):
                self2.outer.puts.append({
                    "bucket": bucket,
                    "object_name": obj,
                    "length": length,
                    "content_type": content_type,
                })

        self.client = _Client(self)


class FakeSqliteClient:
    def __init__(self):
        self.inserts = []

    def execute(self, query, params=(), fetch="none"):
        if "INSERT" in query:
            self.inserts.append(params)
            return None
        return []


def _good_de_html():
    return """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans">One Piece OP01-001 PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 50,00</span></div>
    </body></html>
    """


def _bad_html_no_title():
    return '<html><body><div data-testid="x-price-primary"><span class="ux-textspans">EUR 5,00</span></div></body></html>'


def _make_resource(fake_client):
    from tcg_platform.resources.minio_client import MinioClientResource
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y",
        bucket_name="tcg-bronze",
    )
    resource._client = fake_client
    return resource


def test_transform_reads_raw_writes_bronze(monkeypatch):
    """A known-good raw HTML results in a bronze parquet + SQLite row."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_good_de_html().encode("utf-8"))
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    written_items = [{"event_id": "99999", "region": "DE", "sold_date": "2026-06-11"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)

    assert counts["read_html"] == 1
    assert counts["wrote_parquet"] == 1
    assert counts["wrote_sqlite"] == 1
    # Verify a bronze parquet was written
    parquet_puts = [p for p in minio.puts if p["object_name"].endswith(".parquet")]
    assert len(parquet_puts) == 1
    assert parquet_puts[0]["object_name"] == "sold_data/DE/99999.parquet"
    # Verify SQLite got a row with the carried-through sold_date
    assert len(sqlite.inserts) == 1
    insert_params = sqlite.inserts[0]
    # Insertion params: (card_id, card_version, event_type, price, currency,
    #                    sold_date, scraped_from, source, source_url, ...)
    sold_date_idx = 5
    assert insert_params[sold_date_idx] == "2026-06-11"


def test_transform_handles_missing_image_gracefully(monkeypatch):
    """Raw HTML exists, no image → local_image_path is None, transform succeeds."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_good_de_html().encode("utf-8"), raw_image=None)
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    written_items = [{"event_id": "99999", "region": "DE"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)
    assert counts["wrote_parquet"] == 1
    assert counts["image_missing"] == 1


def test_transform_handles_parse_failure(monkeypatch):
    """HTML exists but has no title → skipped_empty=1, no bronze writes."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_bad_html_no_title().encode("utf-8"))
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    written_items = [{"event_id": "99999", "region": "DE"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)
    assert counts["skipped_empty"] == 1
    assert counts["wrote_parquet"] == 0
    assert len(sqlite.inserts) == 0


def test_transform_filters_wrong_region_in_input(monkeypatch):
    """If the scraper returns a UK event_id, the DE transformer ignores it."""
    from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page
    minio = FakeMinioClient(raw_html=_good_de_html().encode("utf-8"))
    sqlite = FakeSqliteClient()
    resource = _make_resource(minio.client)

    # Pass UK item to DE transformer
    written_items = [{"event_id": "99999", "region": "UK"}]
    counts = _transform_region(resource, sqlite, "DE", written_items, parse_ebay_de_item_page)
    assert counts["read_html"] == 0  # filtered out
    assert counts["wrote_parquet"] == 0

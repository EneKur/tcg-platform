# tests/scraping/test_backfill_raw_html.py
from minio.error import S3Error
from tcg_platform.defs.backfill_raw_html import _backfill_region
from tcg_platform.resources.minio_client import MinioClientResource


def _make_resource_with_fake(fake_client):
    """Build a MinioClientResource wired to a fake minio client."""
    resource = MinioClientResource(
        endpoint="localhost:9000",
        access_key="x",
        secret_key="y",
        bucket_name="tcg-raw",
    )
    resource._client = fake_client
    return resource


class FakeMinioClient:
    def __init__(self, existing_event_ids=()):
        self.existing = set(existing_event_ids)
        self.puts = []
        self.stats = []

        class _Client:
            def __init__(self2, outer):
                self2.outer = outer

            def stat_object(self2, bucket, obj):
                self2.outer.stats.append((bucket, obj))
                # If the HTML path is in self.outer.existing, return success
                if obj.endswith(".html"):
                    event_id = obj.split("/")[-1].replace(".html", "")
                    if event_id in self2.outer.existing:
                        return None
                raise S3Error(
                    code="NoSuchKey", message="not found",
                    resource="x", request_id="r", host_id="h", response=None,
                )

            def put_object(self2, bucket, obj, data, length, content_type):
                self2.outer.puts.append((bucket, obj, data, length, content_type))

        self.client = _Client(self)


class FakeSqliteClient:
    def __init__(self, urls):
        # urls: list of URL strings
        self._rows = [{"source_url": u} for u in urls]

    def execute(self, query, params=(), fetch="none"):
        if "SELECT source_url" in query:
            # Caller-side filter is a no-op for tests; the tests only put
            # region-matching URLs into the fake.
            return list(self._rows)
        return []


class FakeZyteClient:
    def __init__(self):
        self.calls = []

    def get(self, request):
        self.calls.append(request)
        return {
            "statusCode": 200,
            "browserHtml": "<html><body>test</body></html>",
        }


def test_backfill_skips_event_ids_already_in_raw(monkeypatch):
    """If raw HTML already exists for an event_id, no Zyte call happens for it."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient(existing_event_ids=["11111"])
    sqlite = FakeSqliteClient(["https://www.ebay.de/itm/11111"])
    zyte = FakeZyteClient()

    resource = _make_resource_with_fake(minio.client)
    counts = _backfill_region(resource, zyte, sqlite, "DE")
    assert counts["checked"] == 1
    assert counts["already_have"] == 1
    assert counts["fetched"] == 0
    assert len(zyte.calls) == 0  # no Zyte call for the already-present event


def test_backfill_fetches_and_writes_missing_event_ids(monkeypatch):
    """Missing event_ids trigger a Zyte call + raw HTML put."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient(existing_event_ids=[])
    sqlite = FakeSqliteClient(["https://www.ebay.de/itm/22222"])
    zyte = FakeZyteClient()

    resource = _make_resource_with_fake(minio.client)
    counts = _backfill_region(resource, zyte, sqlite, "DE")
    assert counts["checked"] == 1
    assert counts["fetched"] == 1
    assert len(zyte.calls) == 1
    # Verify a put to tcg-raw/ebay/DE/22222.html
    html_puts = [p for p in minio.puts if p[1] == "ebay/DE/22222.html"]
    assert len(html_puts) == 1


def test_backfill_counts_shape(monkeypatch):
    """Return value must have the documented keys."""
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "y")
    minio = FakeMinioClient()
    sqlite = FakeSqliteClient([])
    zyte = FakeZyteClient()

    resource = _make_resource_with_fake(minio.client)
    counts = _backfill_region(resource, zyte, sqlite, "DE")
    assert set(counts.keys()) == {"checked", "already_have", "fetched", "failed"}


def test_backfill_assets_use_tcg_raw_client_resource():
    """Backfill writes to tcg-raw only — must use tcg_raw_client, NOT minio_client.

    Regression: the M9-T1 smoke test caught this when the backfill tried
    to put to tcg-raw via minio_client and got NoSuchBucket. The asset
    would silently fail to persist.
    """
    import dagster as dg
    from tcg_platform.defs.backfill_raw_html import (
        backfill_raw_html_de,
        backfill_raw_html_uk,
    )
    for asset in (backfill_raw_html_de, backfill_raw_html_uk):
        assert isinstance(asset, dg.AssetsDefinition)
        keys = asset.required_resource_keys
        assert "tcg_raw_client" in keys
        assert "minio_client" not in keys

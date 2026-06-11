from minio.error import S3Error

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.scrape_raw import _exists_in_raw


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

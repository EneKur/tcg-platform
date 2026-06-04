from minio.deleteobjects import DeleteObject
from tcg_platform.resources.minio_client import MinioClientResource


def test_remove_objects_calls_batch_delete(monkeypatch):
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    resource = MinioClientResource(
        endpoint="localhost:9000", access_key="x", secret_key="y", bucket_name="b"
    )

    called_with = {}

    class FakeClient:
        def remove_objects(self, bucket, delete_list):
            called_with["bucket"] = bucket
            called_with["items"] = [d.name for d in delete_list]
            return iter([])  # no errors

    resource._client = FakeClient()

    items = [DeleteObject("a.parquet"), DeleteObject("b.parquet")]
    resource.remove_objects("b", items)

    assert called_with["bucket"] == "b"
    assert called_with["items"] == ["a.parquet", "b.parquet"]

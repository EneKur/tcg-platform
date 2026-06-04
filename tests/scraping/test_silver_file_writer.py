from unittest.mock import MagicMock
from tcg_platform.defs.silver_transform import _cleanup_legacy_aggregated_files


def test_cleanup_deletes_legacy_aggregated_files():
    minio_client = MagicMock()

    # list_objects returns names matching the requested full-path prefix
    def fake_list(bucket, prefix=""):
        if prefix == "data/de/data.parquet":
            return ["data/de/data.parquet"]
        if prefix == "quarantine/de/data.parquet":
            return ["quarantine/de/data.parquet"]
        return []

    minio_client.list_objects = fake_list
    minio_client.remove_objects = MagicMock()

    _cleanup_legacy_aggregated_files(minio_client, "DE")

    # Both DE legacy files should have been removed (one remove_objects call each)
    assert minio_client.remove_objects.call_count == 2
    removed_paths = [obj.name for _, objs in minio_client.remove_objects.call_args_list for obj in objs[1]]
    assert "data/de/data.parquet" in removed_paths
    assert "quarantine/de/data.parquet" in removed_paths

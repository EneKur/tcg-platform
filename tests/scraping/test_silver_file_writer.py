from unittest.mock import MagicMock
from tcg_platform.defs.silver_transform import _cleanup_legacy_aggregated_files


def test_cleanup_deletes_legacy_aggregated_files():
    minio_client = MagicMock()
    minio_client.bucket_name = "tcg-bronze"  # unused, kept for interface

    # list_objects returns names matching the requested prefix
    def fake_list(bucket, prefix=""):
        if prefix == "data/de/":
            return ["data/de/data.parquet"]
        if prefix == "quarantine/de/":
            return ["quarantine/de/data.parquet"]
        return []

    minio_client.list_objects = fake_list
    minio_client.remove_objects = MagicMock()

    _cleanup_legacy_aggregated_files(minio_client, "DE")

    # Both DE legacy files should have been removed (one remove_objects call each)
    assert minio_client.remove_objects.call_count == 2
    removed_paths = []
    for call in minio_client.remove_objects.call_args_list:
        args, _ = call
        # remove_objects(bucket_name, [DeleteObject, ...])
        for obj in args[1]:
            # DeleteObject has .name, not .object_name
            name = getattr(obj, "name", None) or getattr(obj, "object_name", None) or str(obj)
            removed_paths.append(name)
    assert "data/de/data.parquet" in removed_paths
    assert "quarantine/de/data.parquet" in removed_paths

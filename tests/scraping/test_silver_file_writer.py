import io
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from tcg_platform.defs.silver_transform import (
    _cleanup_legacy_aggregated_files,
    _write_silver_parquet,
)


def _make_minio_with_existing_files(file_map: dict) -> MagicMock:
    """Build a mock MinioClientResource pre-populated with given files.

    file_map: {object_name: parquet_bytes}
    """
    minio = MagicMock()

    def list_objects(bucket, prefix=""):
        return [name for name in file_map if name.startswith(prefix)]

    def get_object(bucket, name):
        return file_map[name]

    def put_object(bucket_name, object_name, data, length, content_type="application/octet-stream"):
        if hasattr(data, "read"):
            file_map[object_name] = data.read()
        else:
            file_map[object_name] = data

    minio.list_objects = list_objects
    minio.get_object = get_object
    minio.put_object = put_object
    return minio


def _row_dict(**overrides) -> dict:
    base = {
        "event_id": "",
        "card_id": "OP01-001",
        "card_version": None,
        "event_type": "sale",
        "price": 100.0,
        "currency": "EUR",
        "sold_date": "2026-06-04",
        "scraped_from": "ebay",
        "source": "DE",
        "source_url": "https://www.ebay.de/itm/127860244828",
        "language": "EN",
        "scraped_at": "2026-06-04T09:00:00+00:00",
        "image_url": "https://i.ebayimg.com/x.jpg",
        "title": "OP01-001 Luffy",
    }
    base.update(overrides)
    return base


def test_writes_per_item_id_file_with_event_id_populated():
    file_map = {}
    minio = _make_minio_with_existing_files(file_map)

    _write_silver_parquet(minio, "DE", "data", _row_dict())

    assert "data/de/127860244828.parquet" in file_map
    table = pq.read_table(io.BytesIO(file_map["data/de/127860244828.parquet"]))
    assert table.column("event_id").to_pylist() == ["127860244828"]


def test_writes_to_quarantine_prefix_for_invalid_card_id():
    file_map = {}
    minio = _make_minio_with_existing_files(file_map)

    _write_silver_parquet(minio, "DE", "quarantine", _row_dict(card_id="MALFORMED_TITLE"))

    assert "quarantine/de/127860244828.parquet" in file_map


def test_overwrites_in_place_when_tuple_matches():
    file_map = {}
    existing_table = pa.Table.from_pydict({
        "event_id": ["127860244828"],
        "card_id": ["OP01-001"],
        "card_version": [None],
        "event_type": ["sale"],
        "price": [100.0],
        "currency": ["EUR"],
        "sold_date": ["2026-06-04"],
        "scraped_from": ["ebay"],
        "source": ["DE"],
        "source_url": ["https://www.ebay.de/itm/127860244828"],
        "language": ["EN"],
        "scraped_at": ["2026-06-04T09:00:00+00:00"],
        "image_url": ["https://i.ebayimg.com/x.jpg"],
        "title": ["OP01-001 Luffy"],
    })
    buf = io.BytesIO()
    pq.write_table(existing_table, buf)
    file_map["data/de/127860244828.parquet"] = buf.getvalue()

    minio = _make_minio_with_existing_files(file_map)
    _write_silver_parquet(minio, "DE", "data", _row_dict())

    assert "data/de/127860244828_1.parquet" not in file_map
    assert "data/de/127860244828.parquet" in file_map


def test_adds_suffix_when_sold_date_differs():
    file_map = {}
    existing_table = pa.Table.from_pydict({
        "event_id": ["127860244828"],
        "card_id": ["OP01-001"],
        "card_version": [None],
        "event_type": ["sale"],
        "price": [100.0],
        "currency": ["EUR"],
        "sold_date": ["2026-06-01"],
        "scraped_from": ["ebay"],
        "source": ["DE"],
        "source_url": ["https://www.ebay.de/itm/127860244828"],
        "language": ["EN"],
        "scraped_at": ["2026-06-04T09:00:00+00:00"],
        "image_url": ["https://i.ebayimg.com/x.jpg"],
        "title": ["OP01-001 Luffy"],
    })
    buf = io.BytesIO()
    pq.write_table(existing_table, buf)
    file_map["data/de/127860244828.parquet"] = buf.getvalue()

    minio = _make_minio_with_existing_files(file_map)
    _write_silver_parquet(minio, "DE", "data", _row_dict(sold_date="2026-06-04"))

    assert "data/de/127860244828_1.parquet" in file_map
    assert "data/de/127860244828.parquet" in file_map


def test_adds_suffix_when_title_differs():
    file_map = {}
    existing_table = pa.Table.from_pydict({
        "event_id": ["127860244828"],
        "card_id": ["OP01-001"],
        "card_version": [None],
        "event_type": ["sale"],
        "price": [100.0],
        "currency": ["EUR"],
        "sold_date": ["2026-06-04"],
        "scraped_from": ["ebay"],
        "source": ["DE"],
        "source_url": ["https://www.ebay.de/itm/127860244828"],
        "language": ["EN"],
        "scraped_at": ["2026-06-04T09:00:00+00:00"],
        "image_url": ["https://i.ebayimg.com/x.jpg"],
        "title": ["OP01-001 Luffy (alt)"],
    })
    buf = io.BytesIO()
    pq.write_table(existing_table, buf)
    file_map["data/de/127860244828.parquet"] = buf.getvalue()

    minio = _make_minio_with_existing_files(file_map)
    _write_silver_parquet(minio, "DE", "data", _row_dict(title="OP01-001 Luffy"))

    assert "data/de/127860244828_1.parquet" in file_map


def test_increments_suffix_until_free():
    file_map = {}
    for suffix, date in [("", "2026-06-01"), ("_1", "2026-06-02"), ("_2", "2026-06-03")]:
        existing_table = pa.Table.from_pydict({
            "event_id": ["127860244828"],
            "card_id": ["OP01-001"],
            "card_version": [None],
            "event_type": ["sale"],
            "price": [100.0],
            "currency": ["EUR"],
            "sold_date": [date],
            "scraped_from": ["ebay"],
            "source": ["DE"],
            "source_url": ["https://www.ebay.de/itm/127860244828"],
            "language": ["EN"],
            "scraped_at": ["2026-06-04T09:00:00+00:00"],
            "image_url": ["https://i.ebayimg.com/x.jpg"],
            "title": ["OP01-001 Luffy"],
        })
        buf = io.BytesIO()
        pq.write_table(existing_table, buf)
        file_map[f"data/de/127860244828{suffix}.parquet"] = buf.getvalue()

    minio = _make_minio_with_existing_files(file_map)
    _write_silver_parquet(minio, "DE", "data", _row_dict(sold_date="2026-06-04"))

    assert "data/de/127860244828_3.parquet" in file_map


def test_skips_row_when_source_url_has_no_item_id():
    file_map = {}
    minio = _make_minio_with_existing_files(file_map)

    _write_silver_parquet(minio, "DE", "data", _row_dict(source_url="https://example.com/no-item"))

    assert len(file_map) == 0


def test_cleanup_deletes_legacy_aggregated_files():
    minio_client = MagicMock()

    def fake_list(bucket, prefix=""):
        if prefix == "data/de/data.parquet":
            return ["data/de/data.parquet"]
        if prefix == "quarantine/de/data.parquet":
            return ["quarantine/de/data.parquet"]
        return []

    minio_client.list_objects = fake_list
    minio_client.remove_objects = MagicMock()

    _cleanup_legacy_aggregated_files(minio_client, "DE")

    assert minio_client.remove_objects.call_count == 2
    removed_paths = []
    for call in minio_client.remove_objects.call_args_list:
        args, _ = call
        for obj in args[1]:
            removed_paths.append(obj.name)
    assert "data/de/data.parquet" in removed_paths
    assert "quarantine/de/data.parquet" in removed_paths


def test_base_read_failure_falls_through_to_suffix():
    # Base file exists but is corrupt (not a valid parquet). Collision check
    # should log a warning and fall through to _1.parquet (don't overwrite
    # the corrupt base).
    file_map = {"data/de/127860244828.parquet": b"not a parquet file"}
    minio = _make_minio_with_existing_files(file_map)
    _write_silver_parquet(minio, "DE", "data", _row_dict())
    assert "data/de/127860244828_1.parquet" in file_map
    # Base should NOT be overwritten (corrupt file preserved)
    assert file_map["data/de/127860244828.parquet"] == b"not a parquet file"


def test_suffixed_read_failure_overwrites_corrupt_candidate():
    # _1.parquet exists but is corrupt. Writer should overwrite it (better
    # than skipping; at least the result is a valid file).
    file_map = {"data/de/127860244828_1.parquet": b"not a parquet file"}
    minio = _make_minio_with_existing_files(file_map)
    _write_silver_parquet(minio, "DE", "data", _row_dict())
    # _1 should now be overwritten with valid parquet
    table = pq.read_table(io.BytesIO(file_map["data/de/127860244828_1.parquet"]))
    assert table.column("event_id").to_pylist() == ["127860244828"]


def test_missing_title_not_stored_as_float():
    # Regression: when the row has title=None, an all-None pandas column
    # used to be inferred as float64 and filled with 0.0, which broke
    # the collision-check tuple. Now None is replaced with "" before the
    # DataFrame is built, so the column stays as object (or is dropped
    # entirely if absent from the row dict — matching the bronze schema
    # which has no title column).
    file_map = {}
    minio = _make_minio_with_existing_files(file_map)
    row = _row_dict()
    row["title"] = None
    _write_silver_parquet(minio, "DE", "data", row)
    table = pq.read_table(io.BytesIO(file_map["data/de/127860244828.parquet"]))
    title_col = table.column("title")
    assert str(title_col.type) in ("large_string", "string"), \
        f"title was stored as {title_col.type}, not string"


def test_re_run_overwrites_in_place_when_title_missing():
    # End-to-end idempotency: write a row with no title, then write the
    # same row again. The second call should overwrite the base file,
    # not create a _1.
    file_map = {}
    minio = _make_minio_with_existing_files(file_map)
    row = _row_dict()
    del row["title"]
    _write_silver_parquet(minio, "DE", "data", row)
    _write_silver_parquet(minio, "DE", "data", row)
    assert "data/de/127860244828.parquet" in file_map
    assert "data/de/127860244828_1.parquet" not in file_map


def test_legacy_title_as_float_0_does_not_create_suffix():
    # Regression: pre-fix silver files stored title=0.0 (pandas filled
    # an all-None column with 0.0). The new writer stores title="".
    # A re-run that sees the legacy file must treat 0.0 as equivalent
    # to "" and overwrite in place, not create a _1.
    file_map = {}
    legacy_table = pa.Table.from_pydict({
        "event_id": ["127860244828"],
        "card_id": ["OP01-001"],
        "sold_date": ["2026-06-04"],
        "title": [0.0],
    })
    buf = io.BytesIO()
    pq.write_table(legacy_table, buf)
    file_map["data/de/127860244828.parquet"] = buf.getvalue()

    minio = _make_minio_with_existing_files(file_map)
    # New write — row has no title (matches what we'd write today)
    row = _row_dict()
    del row["title"]
    _write_silver_parquet(minio, "DE", "data", row)

    assert "data/de/127860244828.parquet" in file_map
    assert "data/de/127860244828_1.parquet" not in file_map


def test_nan_title_does_not_create_suffix():
    # Regression: Spark's toPandas converts None to NaN (a float) for
    # str-dtype columns. The collision-check tuple must not see a NaN
    # on the new-write side either, or a re-run will mismatch the
    # existing file's "" and create a spurious _1.
    import math
    file_map = {}
    existing_table = pa.Table.from_pydict({
        "event_id": ["127860244828"],
        "card_id": ["OP01-001"],
        "sold_date": ["2026-06-04"],
        "title": [""],
    })
    buf = io.BytesIO()
    pq.write_table(existing_table, buf)
    file_map["data/de/127860244828.parquet"] = buf.getvalue()

    minio = _make_minio_with_existing_files(file_map)
    row = _row_dict()
    row["title"] = math.nan  # what Spark's toPandas actually produces
    _write_silver_parquet(minio, "DE", "data", row)

    assert "data/de/127860244828.parquet" in file_map
    assert "data/de/127860244828_1.parquet" not in file_map

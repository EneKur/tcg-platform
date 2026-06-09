import io
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from tcg_platform.defs.reconcile_quarantine import _reconcile_region


def _row_to_parquet_bytes(card_id: str) -> bytes:
    """Build a single-row quarantine parquet for the given card_id."""
    table = pa.Table.from_pydict({
        "event_id": ["123456789012"],
        "card_id": [card_id],
        "card_version": [None],
        "event_type": ["sale"],
        "price": [100.0],
        "currency": ["EUR"],
        "sold_date": ["2026-06-04"],
        "scraped_from": ["ebay"],
        "source": ["DE"],
        "source_url": ["https://www.ebay.de/itm/123456789012"],
        "language": ["EN"],
        "scraped_at": ["2026-06-04T09:00:00+00:00"],
        "image_url": [""],
        "title": [""],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_minio_with_files(
    cards_files: list[str],
    quarantine_files: dict,
) -> MagicMock:
    """Build a mock MinioClientResource.

    cards_files: object names in tcg-bronze/cards/ (the card set)
    quarantine_files: {object_name: parquet_bytes} in tcg-silver/quarantine/{region}/
    """
    minio = MagicMock()

    def list_objects(bucket, prefix=""):
        if bucket == "tcg-bronze":
            return list(cards_files)
        if bucket == "tcg-silver":
            return [name for name in quarantine_files if name.startswith(prefix)]
        return []

    def get_object(bucket, name):
        if bucket == "tcg-silver":
            return quarantine_files[name]
        raise RuntimeError(f"unexpected get_object({bucket}, {name})")

    deleted: list[list] = []

    def remove_objects(bucket, delete_list):
        deleted.append([d.name for d in delete_list])
        for d in delete_list:
            quarantine_files.pop(d.name, None)

    minio.list_objects = list_objects
    minio.get_object = get_object
    minio.remove_objects = remove_objects
    minio._deleted = deleted
    return minio


def test_promotes_row_whose_card_id_now_passes():
    cards_files = ["cards/OP16/OP16-005.webp"]
    quarantine_files = {
        "quarantine/de/999999999999.parquet": _row_to_parquet_bytes("OP16-005"),
    }
    minio = _make_minio_with_files(cards_files, quarantine_files)

    result = _reconcile_region(minio, "de")

    assert result["scanned"] == 1
    assert result["promoted_count"] == 1
    assert result["still_quarantined_count"] == 0
    assert result["read_errors"] == 0
    assert result["promoted"] == [
        {"path": "quarantine/de/999999999999.parquet", "card_id": "OP16-005"}
    ]
    # File must actually be gone from the mock's state
    assert "quarantine/de/999999999999.parquet" not in quarantine_files
    # And remove_objects must have been called for it
    assert minio._deleted == [["quarantine/de/999999999999.parquet"]]

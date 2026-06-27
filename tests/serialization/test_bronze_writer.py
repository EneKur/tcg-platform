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

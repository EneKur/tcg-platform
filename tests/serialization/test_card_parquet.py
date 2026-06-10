# tests/serialization/test_card_parquet.py
"""Pin the output schema of card_records_to_parquet and price_records_to_parquet.

These two helpers serialize the Pydantic models from
src/tcg_platform/scraping/models.py into parquet bytes for the dormant
bronze_cardlist_parquet and bronze_fact_events_parquet assets. They
have no test coverage; this file pins their current behavior so any
future schema change is a deliberate, test-failing event.

The known-stale behaviors below (event_id always "", image_url /
local_image_path dropped, partition_date argument ignored) are
pinned intentionally — see docs/superpowers/specs/2026-06-10-m8-t5-card-parquet-tests-design.md.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import BufferReader

from tcg_platform.scraping.models import CardRecord
from tcg_platform.serialization.card_parquet import card_records_to_parquet


def _make_card(**overrides) -> CardRecord:
    """Build a fully-populated CardRecord; override any field by name."""
    base = dict(
        card_id="OP01-001",
        card_version="v1",
        card_name="Monkey D. Luffy",
        set_code="OP01",
        rarity="L",
        card_type="Character",
        attribute="STR",
        power=5000,
        cost=4,
        color="Red",
        source_url="https://onepiece.limitlesstcg.com/cards/OP01-001",
        scraped_at=datetime(2026, 6, 10, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return CardRecord(**base)


def test_cards_empty_input_returns_zero_row_parquet():
    bytes_out, count = card_records_to_parquet([], "2026-06-10")
    assert count == 0
    table = pq.read_table(BufferReader(bytes_out))
    assert table.num_rows == 0


def test_cards_single_card_writes_all_required_columns():
    card = _make_card()
    bytes_out, count = card_records_to_parquet([card], "2026-06-10")
    assert count == 1
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column_names == [
        "card_id",
        "card_version",
        "card_name",
        "set_code",
        "rarity",
        "card_type",
        "attribute",
        "power",
        "cost",
        "color",
        "source_url",
        "scraped_at",
    ]


def test_cards_optional_fields_default_to_empty_string_or_zero():
    card = _make_card(
        card_version=None,
        rarity="",
        attribute=None,
        power=None,
        cost=None,
        color=None,
    )
    bytes_out, _ = card_records_to_parquet([card], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    row = table.to_pylist()[0]
    assert row["card_version"] == ""
    assert row["rarity"] == ""
    assert row["attribute"] == ""
    assert row["power"] == 0
    assert row["cost"] == 0
    assert row["color"] == ""

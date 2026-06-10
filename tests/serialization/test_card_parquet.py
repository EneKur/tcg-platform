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


def test_cards_scraped_at_stamped_at_call_time():
    before = datetime.now(timezone.utc)
    bytes_out, _ = card_records_to_parquet([_make_card()], "2026-06-10")
    after = datetime.now(timezone.utc)

    table = pq.read_table(BufferReader(bytes_out))
    stamped = datetime.fromisoformat(table.to_pylist()[0]["scraped_at"])
    slack = __import__("datetime").timedelta(seconds=1)
    assert before - slack <= stamped <= after + slack


def test_cards_partition_date_argument_is_ignored():
    card = _make_card()
    bytes_a, count_a = card_records_to_parquet([card], "2026-06-10")
    bytes_b, count_b = card_records_to_parquet([card], "2099-12-31")
    assert count_a == count_b == 1

    table_a = pq.read_table(BufferReader(bytes_a))
    table_b = pq.read_table(BufferReader(bytes_b))
    assert table_a.column_names == table_b.column_names
    row_a = table_a.to_pylist()[0]
    row_b = table_b.to_pylist()[0]
    for col in table_a.column_names:
        if col == "scraped_at":
            continue
        assert row_a[col] == row_b[col], f"{col} differs"


def test_cards_returned_row_count_matches_input():
    cards = [_make_card(card_id=f"OP01-{i:03d}") for i in range(1, 6)]
    _, count = card_records_to_parquet(cards, "2026-06-10")
    assert count == 5

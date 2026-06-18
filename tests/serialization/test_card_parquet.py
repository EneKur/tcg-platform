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

from tcg_platform.scraping.models import CardRecord, PriceRecord
from tcg_platform.serialization.card_parquet import (
    card_records_to_parquet,
    price_records_to_parquet,
)


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


def _make_price(**overrides) -> PriceRecord:
    """Build a fully-populated PriceRecord; override any field by name."""
    base = dict(
        card_id="OP01-001",
        card_version="v1",
        event_type="price_update",
        price=12.50,
        currency="USD",
        sold_date="2026-06-09",
        scraped_from="limitlesstcg",
        source="US",
        source_url="https://onepiece.limitlesstcg.com/cards/OP01-001",
        language="EN",
        scraped_at=datetime(2026, 6, 10, tzinfo=timezone.utc),
        image_url="https://limitlesstcg.nyc3.digitaloceanspaces.com/OP01/OP01-001.webp",
        local_image_path="cards/OP01/OP01-001.webp",
        title="Monkey D. Luffy",
    )
    base.update(overrides)
    return PriceRecord(**base)


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
        "partition_date",
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


def test_cards_scraped_at_derived_from_partition_date():
    bytes_out, _ = card_records_to_parquet([_make_card()], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("scraped_at").to_pylist() == ["2026-06-10T00:00:00+00:00"]


def test_cards_scraped_at_is_pure_across_calls():
    """Two calls with the same partition_date must produce the same scraped_at."""
    bytes_a, _ = card_records_to_parquet([_make_card()], "2026-06-10")
    bytes_b, _ = card_records_to_parquet([_make_card()], "2026-06-10")
    table_a = pq.read_table(BufferReader(bytes_a))
    table_b = pq.read_table(BufferReader(bytes_b))
    assert (
        table_a.column("scraped_at").to_pylist()
        == table_b.column("scraped_at").to_pylist()
    )


def test_cards_partition_date_column_reflects_arg():
    bytes_out, _ = card_records_to_parquet([_make_card()], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert "partition_date" in table.column_names
    assert table.column("partition_date").to_pylist() == ["2026-06-10"]


def test_cards_missing_partition_date_raises():
    import pytest
    with pytest.raises(ValueError, match="partition_date is required"):
        card_records_to_parquet([_make_card()], "")


def test_cards_returned_row_count_matches_input():
    cards = [_make_card(card_id=f"OP01-{i:03d}") for i in range(1, 6)]
    _, count = card_records_to_parquet(cards, "2026-06-10")
    assert count == 5


def test_prices_empty_input_returns_zero_row_parquet():
    bytes_out, count = price_records_to_parquet([], "2026-06-10")
    assert count == 0
    table = pq.read_table(BufferReader(bytes_out))
    assert table.num_rows == 0


def test_prices_single_price_writes_all_required_columns():
    bytes_out, _ = price_records_to_parquet([_make_price()], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column_names == [
        "event_id",
        "card_id",
        "card_version",
        "event_type",
        "price",
        "currency",
        "sold_date",
        "scraped_from",
        "source",
        "source_url",
        "language",
        "scraped_at",
        "image_url",
        "local_image_path",
        "title",
        "partition_date",
    ]


def test_prices_event_id_derived_from_limitless_url():
    bytes_out, _ = price_records_to_parquet([_make_price()], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("event_id").to_pylist() == ["limitless-OP01-001"]


def test_prices_event_id_derived_from_ebay_url():
    price = _make_price(
        source_url="https://www.ebay.de/itm/123456789",
    )
    bytes_out, _ = price_records_to_parquet([price], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("event_id").to_pylist() == ["123456789"]


def test_prices_image_url_passes_through():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(image_url="https://cdn.example.com/x.webp")],
        "2026-06-10",
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert "image_url" in table.column_names
    assert table.column("image_url").to_pylist() == ["https://cdn.example.com/x.webp"]


def test_prices_local_image_path_passes_through():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(local_image_path="cards/OP01/x.webp")],
        "2026-06-10",
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert "local_image_path" in table.column_names
    assert table.column("local_image_path").to_pylist() == ["cards/OP01/x.webp"]


def test_prices_local_image_path_backfilled_from_map_when_record_empty():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(local_image_path="")],
        "2026-06-10",
        local_image_path_map={"OP01-001": "cards/OP01/OP01-001.webp"},
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("local_image_path").to_pylist() == [
        "cards/OP01/OP01-001.webp"
    ]


def test_prices_local_image_path_record_value_beats_map():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(local_image_path="cards/explicit/explicit.webp")],
        "2026-06-10",
        local_image_path_map={"OP01-001": "cards/OP01/OP01-001.webp"},
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("local_image_path").to_pylist() == [
        "cards/explicit/explicit.webp"
    ]


def test_prices_local_image_path_empty_when_card_id_not_in_map():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(card_id="OP99-999", local_image_path="")],
        "2026-06-10",
        local_image_path_map={"OP01-001": "cards/OP01/OP01-001.webp"},
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("local_image_path").to_pylist() == [""]


def test_prices_partition_date_column_reflects_arg():
    bytes_out, _ = price_records_to_parquet([_make_price()], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert "partition_date" in table.column_names
    assert table.column("partition_date").to_pylist() == ["2026-06-10"]


def test_prices_title_defaults_to_empty_string():
    price = _make_price()
    object.__delattr__(price, "title")
    bytes_out, _ = price_records_to_parquet([price], "2026-06-10")
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("title").to_pylist() == [""]


def test_prices_title_passed_through_when_set():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(title="Monkey D. Luffy (Alt Art)")],
        "2026-06-10",
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("title").to_pylist() == ["Monkey D. Luffy (Alt Art)"]


def test_prices_card_version_none_becomes_empty_string():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(card_version=None)],
        "2026-06-10",
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("card_version").to_pylist() == [""]


def test_prices_sold_date_none_becomes_empty_string():
    bytes_out, _ = price_records_to_parquet(
        [_make_price(sold_date=None)],
        "2026-06-10",
    )
    table = pq.read_table(BufferReader(bytes_out))
    assert table.column("sold_date").to_pylist() == [""]


def test_prices_returned_row_count_matches_input():
    prices = [_make_price(card_id=f"OP01-{i:03d}") for i in range(1, 4)]
    _, count = price_records_to_parquet(prices, "2026-06-10")
    assert count == 3


def test_derive_event_id_for_ebay_de_url():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert derive_event_id("https://www.ebay.de/itm/123456789") == "123456789"


def test_derive_event_id_for_ebay_uk_url():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert derive_event_id("https://www.ebay.co.uk/itm/987654321") == "987654321"


def test_derive_event_id_for_limitless_url():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert (
        derive_event_id("https://onepiece.limitlesstcg.com/cards/OP01-001")
        == "limitless-OP01-001"
    )


def test_derive_event_id_for_unknown_url_is_deterministic():
    from tcg_platform.serialization.card_parquet import derive_event_id
    url = "https://example.com/some/odd/path"
    first = derive_event_id(url)
    second = derive_event_id(url)
    assert first == second
    assert first.startswith("unknown-")
    assert len(first) == len("unknown-") + 8


def test_derive_event_id_for_empty_string_returns_unknown_zero():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert derive_event_id("") == "unknown-0"


def test_derive_event_id_uses_md5_not_python_hash():
    """Python's hash() is randomized per process; unknown URLs must use
    hashlib.md5 for cross-run stability."""
    from tcg_platform.serialization.card_parquet import derive_event_id
    url = "https://example.com/some/odd/path"
    expected = (
        "unknown-"
        + __import__("hashlib").md5(url.encode()).hexdigest()[:8]
    )
    assert derive_event_id(url) == expected


def test_derive_event_id_strips_query_string_from_limitless_url():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert (
        derive_event_id("https://onepiece.limitlesstcg.com/cards/OP01-001?v=1")
        == "limitless-OP01-001"
    )


def test_derive_event_id_strips_trailing_slash_and_query():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert (
        derive_event_id("https://onepiece.limitlesstcg.com/cards/OP01-001/?v=1")
        == "limitless-OP01-001"
    )


def test_derive_event_id_handles_none_input():
    from tcg_platform.serialization.card_parquet import derive_event_id
    assert derive_event_id(None) == "unknown-0"


def test_derive_event_id_malformed_ebay_falls_through_to_unknown():
    from tcg_platform.serialization.card_parquet import derive_event_id
    result = derive_event_id("https://www.ebay.de/itm/not-a-number")
    assert result.startswith("unknown-")
    assert result != "unknown-0"

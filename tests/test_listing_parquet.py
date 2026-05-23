import io
import pyarrow as pa

def test_listing_parquet_schema():
    from tcg_platform.serialization.listing_parquet import LISTING_SCHEMA, row_to_arrow_table

    row = {
        "item_id": "406939215710",
        "source_url": "https://www.ebay.com/itm/406939215710",
        "scraped_at": "2026-05-23T10:00:00Z",
        "region": "US",
        "card_id": "OP15-001",
        "card_version": "_Alternative_Art",
        "title": "One Piece TCG OP15-001 Alternative Art",
        "price": 12.99,
        "currency": "USD",
        "sold_date": "2026-04-15",
        "language": "EN",
        "html_payload": b"<html></html>",
        "thumbnail_url": "https://i.ebayimg.com/1234.jpg",
        "image_path": "bronze/images/OP15-001_Alternative_Art_406939215710.jpg",
    }

    table = row_to_arrow_table([row])
    assert table.schema.equals(LISTING_SCHEMA)
    assert table.num_rows == 1
    assert table.column("item_id")[0].as_py() == "406939215710"
    assert table.column("image_path")[0].as_py() == "bronze/images/OP15-001_Alternative_Art_406939215710.jpg"


def test_listing_parquet_roundtrip():
    from tcg_platform.serialization.listing_parquet import write_parquet_bytes, read_parquet_bytes

    row = {
        "item_id": "999",
        "source_url": "https://www.ebay.com/itm/999",
        "scraped_at": "2026-05-23T10:00:00Z",
        "region": "US",
        "card_id": "ST01",
        "card_version": "",
        "title": "One Piece TCG Starter Deck",
        "price": 5.99,
        "currency": "USD",
        "sold_date": "",
        "language": "EN",
        "html_payload": b"<html>test</html>",
        "thumbnail_url": "https://i.ebayimg.com/test.jpg",
        "image_path": "",
    }

    data = write_parquet_bytes([row])
    table = read_parquet_bytes(data)
    assert table.num_rows == 1
    assert table.column("card_id")[0].as_py() == "ST01"
    assert table.column("html_payload")[0].as_py() == b"<html>test</html>"
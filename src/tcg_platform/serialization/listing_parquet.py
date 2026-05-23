import io
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq


LISTING_SCHEMA = pa.schema([
    pa.field("item_id", pa.string()),
    pa.field("source_url", pa.string()),
    pa.field("scraped_at", pa.string()),
    pa.field("region", pa.string()),
    pa.field("card_id", pa.string()),
    pa.field("card_version", pa.string()),
    pa.field("title", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("sold_date", pa.string()),
    pa.field("language", pa.string()),
    pa.field("html_payload", pa.binary()),
    pa.field("thumbnail_url", pa.string()),
    pa.field("image_path", pa.string()),
])


def row_to_arrow_table(rows: list[dict]) -> pa.Table:
    now = datetime.now(timezone.utc)
    processed = []
    for row in rows:
        processed.append({
            "item_id": str(row.get("item_id", "")),
            "source_url": str(row.get("source_url", "")),
            "scraped_at": row.get("scraped_at") or now.isoformat(),
            "region": str(row.get("region", "US")),
            "card_id": str(row.get("card_id", "")),
            "card_version": str(row.get("card_version", "")),
            "title": str(row.get("title", "")),
            "price": float(row.get("price") or 0.0),
            "currency": str(row.get("currency", "USD")),
            "sold_date": str(row.get("sold_date", "")),
            "language": str(row.get("language", "EN")),
            "html_payload": row.get("html_payload") or b"",
            "thumbnail_url": str(row.get("thumbnail_url", "")),
            "image_path": str(row.get("image_path", "")),
        })
    table = pa.Table.from_pylist(processed, schema=LISTING_SCHEMA)
    return table


def write_parquet_bytes(rows: list[dict]) -> bytes:
    table = row_to_arrow_table(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def read_parquet_bytes(data: bytes) -> pa.Table:
    buf = io.BytesIO(data)
    return pq.read_table(buf)
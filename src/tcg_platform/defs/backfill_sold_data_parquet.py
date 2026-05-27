import dagster as dg
import re
from datetime import datetime

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.card_parquet import price_records_to_parquet


_ITEM_ID_RE = re.compile(r"/itm/(\d+)")


def _extract_item_id(url: str) -> str:
    match = _ITEM_ID_RE.search(url)
    return match.group(1) if match else url


def _row_to_record(row) -> dict:
    sold_date = row["sold_date"]
    if sold_date:
        try:
            sold_date_fmt = datetime.strptime(sold_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            sold_date_fmt = sold_date
    else:
        sold_date_fmt = ""

    scraped_at_raw = row["scraped_at"]
    if isinstance(scraped_at_raw, str):
        scraped_at_dt = datetime.fromisoformat(scraped_at_raw)
    else:
        scraped_at_dt = scraped_at_raw

    return {
        "event_id": "",
        "card_id": row["card_id"] or "",
        "card_version": row["card_version"] or "",
        "event_type": row["event_type"] or "sale",
        "price": row["price"] or 0.0,
        "currency": row["currency"] or "",
        "sold_date": sold_date_fmt,
        "scraped_from": row["scraped_from"] or "ebay",
        "source": row["source"] or "",
        "source_url": row["source_url"] or "",
        "language": row["language"] or "EN",
        "scraped_at": scraped_at_dt.isoformat(),
        "image_url": row["image_url"] or "",
    }


def _backfill_sold_data(
    context: dg.AssetExecutionContext,
    sqlite_client,
    minio_client: MinioClientResource,
    region: str,
) -> int:
    rows = sqlite_client.get_unparqueted_fact_events(region)
    if not rows:
        context.log.info(f"No unparqueted {region} records found")
        return 0

    context.log.info(f"Found {len(rows)} unparqueted {region} records to backfill")

    for row in rows:
        item_id = _extract_item_id(row["source_url"])
        object_name = f"sold_data/{region}/{item_id}.parquet"
        partition_date = row["scraped_at"]
        if isinstance(partition_date, str):
            partition_date = datetime.fromisoformat(partition_date).strftime("%Y-%m-%d")
        else:
            partition_date = partition_date.strftime("%Y-%m-%d")

        record_dict = _row_to_record(row)
        table_data = [{
            "event_id": record_dict["event_id"],
            "card_id": record_dict["card_id"],
            "card_version": record_dict["card_version"],
            "event_type": record_dict["event_type"],
            "price": record_dict["price"],
            "currency": record_dict["currency"],
            "sold_date": record_dict["sold_date"],
            "scraped_from": record_dict["scraped_from"],
            "source": record_dict["source"],
            "source_url": record_dict["source_url"],
            "language": record_dict["language"],
            "scraped_at": record_dict["scraped_at"],
            "image_url": record_dict["image_url"],
        }]

        import pyarrow as pa
        import pyarrow.parquet as pq
        import io
        table = pa.Table.from_pylist(table_data)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        parquet_bytes = buffer.getvalue()

        minio_client.put_object(
            bucket_name=minio_client.bucket_name,
            object_name=object_name,
            data=parquet_bytes,
            length=len(parquet_bytes),
            content_type="application/parquet",
        )

    marked = sqlite_client.mark_fact_events_parqueted(region)
    context.log.info(f"Backfilled {len(rows)} {region} records, marked {marked} as parqueted")
    return len(rows)


@dg.asset(required_resource_keys={"sqlite_client_de", "minio_client"})
def backfill_de_sold_data_parquet(
    context: dg.AssetExecutionContext,
    bronze_ebay_de_sqlite_writer: dg.AssetOut,
) -> dg.MaterializeResult:
    sqlite_client_de = context.resources.sqlite_client_de
    minio_client = context.resources.minio_client
    n = _backfill_sold_data(context, sqlite_client_de, minio_client, "DE")
    return dg.MaterializeResult(metadata={"records_backfilled": n})


@dg.asset(required_resource_keys={"sqlite_client_uk", "minio_client"})
def backfill_uk_sold_data_parquet(
    context: dg.AssetExecutionContext,
    bronze_ebay_uk_sqlite_writer: dg.AssetOut,
) -> dg.MaterializeResult:
    sqlite_client_uk = context.resources.sqlite_client_uk
    minio_client = context.resources.minio_client
    n = _backfill_sold_data(context, sqlite_client_uk, minio_client, "UK")
    return dg.MaterializeResult(metadata={"records_backfilled": n})
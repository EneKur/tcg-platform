import dagster as dg
import re

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.card_parquet import price_records_to_parquet


_ITEM_ID_RE = re.compile(r"/itm/(\d+)")


def _extract_item_id(url: str) -> str:
    match = _ITEM_ID_RE.search(url)
    return match.group(1) if match else url


def _write_sold_data_parquet(
    records: list,
    region: str,
    minio_client: MinioClientResource,
) -> int:
    written = 0
    for record in records:
        item_id = _extract_item_id(record.source_url)
        object_name = f"sold_data/{region}/{item_id}.parquet"
        parquet_bytes, _ = price_records_to_parquet([record], record.scraped_at.strftime("%Y-%m-%d"))
        minio_client.put_object(
            bucket_name=minio_client.bucket_name,
            object_name=object_name,
            data=parquet_bytes,
            length=len(parquet_bytes),
            content_type="application/parquet",
        )
        written += 1
    return written


@dg.asset
def bronze_de_sold_data_parquet(
    context: dg.AssetExecutionContext,
    ebay_de_sold_listings: list,
    minio_client: MinioClientResource,
) -> dg.MaterializeResult:
    if not ebay_de_sold_listings:
        context.log.info("No DE records to write")
        return dg.MaterializeResult(metadata={"num_files": 0})

    n = _write_sold_data_parquet(ebay_de_sold_listings, "DE", minio_client)
    context.log.info(f"Wrote {n} DE sold data parquet files to MinIO")
    return dg.MaterializeResult(metadata={"num_files": n})


@dg.asset
def bronze_uk_sold_data_parquet(
    context: dg.AssetExecutionContext,
    ebay_uk_sold_listings: list,
    minio_client: MinioClientResource,
) -> dg.MaterializeResult:
    if not ebay_uk_sold_listings:
        context.log.info("No UK records to write")
        return dg.MaterializeResult(metadata={"num_files": 0})

    n = _write_sold_data_parquet(ebay_uk_sold_listings, "UK", minio_client)
    context.log.info(f"Wrote {n} UK sold data parquet files to MinIO")
    return dg.MaterializeResult(metadata={"num_files": n})
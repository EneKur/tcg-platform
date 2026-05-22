import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.card_parquet import price_records_to_parquet


FACT_EVENTS_PATH = "bronze/fact_events/partition_date={date}/prices.parquet"


@dg.asset
def bronze_fact_events_parquet(
    context: dg.AssetExecutionContext,
    limitless_op_prices: dg.AssetOut,
    minio_client: MinioClientResource,
) -> dg.MaterializeResult:
    """Serialize price records to parquet and upload to MinIO."""
    from datetime import datetime, timezone

    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    price_bytes, num_prices = price_records_to_parquet(limitless_op_prices, partition_date)

    object_name = FACT_EVENTS_PATH.format(date=partition_date)
    minio_client.put_object(
        bucket_name=minio_client.bucket_name,
        object_name=object_name,
        data=price_bytes,
        length=len(price_bytes),
        content_type="application/parquet",
    )

    context.log.info(f"Wrote {num_prices} prices to {object_name}")
    return dg.MaterializeResult(metadata={"num_prices": num_prices, "partition_date": partition_date})
import dagster as dg

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.serialization.card_parquet import card_records_to_parquet


CARDLIST_PATH = "bronze/cardlist/partition_date={date}/cards.parquet"


@dg.asset
def bronze_cardlist_parquet(
    context: dg.AssetExecutionContext,
    limitless_op_cards: dg.AssetOut,
    minio_client: MinioClientResource,
) -> dg.MaterializeResult:
    """Serialize card records to parquet and upload to MinIO."""
    from datetime import datetime, timezone

    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    card_bytes, num_cards = card_records_to_parquet(limitless_op_cards, partition_date)

    object_name = CARDLIST_PATH.format(date=partition_date)
    minio_client.put_object(
        bucket_name=minio_client.bucket_name,
        object_name=object_name,
        data=card_bytes,
        length=len(card_bytes),
        content_type="application/parquet",
    )

    context.log.info(f"Wrote {num_cards} cards to {object_name}")
    return dg.MaterializeResult(metadata={"num_cards": num_cards, "partition_date": partition_date})
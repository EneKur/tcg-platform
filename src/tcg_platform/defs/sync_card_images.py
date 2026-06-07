import time
import dagster as dg
import requests
from dagster import AssetIn

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.scraping.limitless_sync import (
    build_cdn_url,
    build_card_image_diff,
)


@dg.asset(
    required_resource_keys={"minio_client"},
    ins={"discover_limitless_catalog": AssetIn()},
)
def sync_card_images(
    context: dg.AssetExecutionContext,
    discover_limitless_catalog: list[tuple[str, str, int | None]],
) -> dg.MaterializeResult:
    """Diff the discovered Limitless catalog against tcg-bronze/cards/, download missing."""
    minio_client: MinioClientResource = context.resources.minio_client
    bucket = minio_client.bucket_name

    started = time.time()
    existing_keys = set(minio_client.list_objects(bucket, "cards/"))
    context.log.info(f"MinIO has {len(existing_keys)} existing objects under cards/")

    diff = build_card_image_diff(discover_limitless_catalog, existing_keys)
    context.log.info(f"Diff: {len(diff)} new images to download (catalog size: {len(discover_limitless_catalog)})")

    new_card_ids: list[str] = []
    failed_card_ids: list[str] = []

    for set_code, card_id, variant, key in diff:
        url = build_cdn_url(set_code, card_id, variant)
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.content
        except Exception as e:
            context.log.warning(f"CDN fetch failed for {key} ({url}): {e}")
            failed_card_ids.append(card_id)
            continue

        try:
            minio_client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=data,
                length=len(data),
                content_type="image/webp",
            )
            new_card_ids.append(card_id)
        except Exception as e:
            context.log.warning(f"put_object failed for {key}: {e}")
            failed_card_ids.append(card_id)

    duration = round(time.time() - started, 2)
    context.log.info(
        f"Sync complete in {duration}s: "
        f"discovered={len(discover_limitless_catalog)} existing={len(existing_keys)} "
        f"new={len(new_card_ids)} failed={len(failed_card_ids)}"
    )

    return dg.MaterializeResult(
        metadata={
            "discovered_count": len(discover_limitless_catalog),
            "existing_count": len(existing_keys),
            "new_count": len(new_card_ids),
            "failed_count": len(failed_card_ids),
            "new_card_ids": dg.MetadataValue.json(new_card_ids),
            "failed_card_ids": dg.MetadataValue.json(failed_card_ids),
            "duration_seconds": duration,
        }
    )

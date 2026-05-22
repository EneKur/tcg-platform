import dagster as dg
import requests

from tcg_platform.resources import MinioClientResource
from tcg_platform.scraping.image_downloader import (
    download_card_image,
    get_all_cards_with_variants,
    is_image_in_minio,
    upload_image_to_minio,
)


@dg.asset
def limitless_op_card_images(
    context: dg.AssetExecutionContext,
    minio_client: MinioClientResource,
) -> dg.MaterializeResult:
    """Download card images from Limitless TCG CDN and upload to MinIO.

    Idempotent: skips cards already in MinIO. Handles base + variant images
    (e.g., ?v=1 -> _v1.webp, ?v=2 -> _v2.webp).
    """
    context.log.info("Starting card image download from Limitless TCG CDN")

    session = requests.Session()
    total = 0
    uploaded = 0
    skipped = 0
    failed = 0

    card_entries = get_all_cards_with_variants()

    unique_cards = {}
    for set_code, card_id, variant in card_entries:
        key = (set_code, card_id, variant)
        if key not in unique_cards:
            unique_cards[key] = True

    context.log.info(f"Found {len(unique_cards)} unique cards with variants")

    bucket = minio_client.bucket_name

    for (set_code, card_id, variant) in unique_cards:
        total += 1

        try:
            if is_image_in_minio(minio_client, bucket, set_code, card_id, variant):
                skipped += 1
                continue

            image_data, source_url = download_card_image(
                set_code, card_id, variant, session
            )
            if image_data:
                upload_image_to_minio(
                    minio_client,
                    bucket,
                    set_code,
                    card_id,
                    image_data,
                    variant=variant,
                )
                uploaded += 1
            else:
                failed += 1
                context.log.warning(
                    f"Failed to download image for {card_id}"
                    + (f" v{variant}" if variant else "")
                )

            if total % 100 == 0:
                context.log.info(
                    f"Progress: {total}/{len(unique_cards)} — "
                    f"uploaded: {uploaded}, skipped: {skipped}, failed: {failed}"
                )

        except Exception as e:
            failed += 1
            context.log.warning(f"Error processing {card_id}: {e}")

    context.log.info(
        f"Image download complete: {uploaded} uploaded, {skipped} skipped, {failed} failed"
    )

    return dg.MaterializeResult(
        metadata={
            "total_processed": total,
            "uploaded": uploaded,
            "skipped": skipped,
            "failed": failed,
        }
    )
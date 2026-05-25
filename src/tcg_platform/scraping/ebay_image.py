import io
import requests
from minio.error import S3Error

from tcg_platform.scraping.ebay import extract_item_image_url


def download_and_save_image(
    item_id: str,
    region: str,
    html: str,
    minio_client,
) -> str | None:
    img_url = extract_item_image_url(html)
    if not img_url:
        return None

    try:
        img_data = requests.get(img_url, timeout=30).content
    except Exception:
        return None

    object_path = f"sold_images/{region}/{item_id}.jpg"

    try:
        minio_client.put_object(
            bucket_name=minio_client.bucket_name,
            object_name=object_path,
            data=img_data,
            length=len(img_data),
            content_type="image/jpeg",
        )
    except S3Error:
        return None

    return object_path


def image_exists_in_minio(minio_client, item_id: str, region: str) -> bool:
    object_path = f"sold_images/{region}/{item_id}.jpg"
    try:
        minio_client.client.stat_object(minio_client.bucket_name, object_path)
        return True
    except Exception:
        return False
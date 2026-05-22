import io
import re
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from minio.error import S3Error
from playwright.sync_api import sync_playwright

LIMITLESS_OP_BASE = "https://onepiece.limitlesstcg.com"
IMAGE_CDN_BASE = "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com"


def _build_cdn_url(set_code: str, card_id: str, variant: Optional[int] = None) -> str:
    card_slug = card_id.replace(" ", "-")
    if variant and variant >= 1:
        filename = f"{card_slug}_p{variant}_EN.webp"
    else:
        filename = f"{card_slug}_EN.webp"
    return f"{IMAGE_CDN_BASE}/one-piece/{set_code}/{filename}"


def _get_card_info_from_page(url: str) -> tuple[Optional[str], Optional[str], Optional[int]]:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        title_tag = soup.find("meta", property="og:title")
        if not title_tag:
            return None, None, None

        full_title = title_tag.get("content", "")
        card_name_match = re.match(r"(.+?)\s*\(([^)]+)\)", full_title)
        if card_name_match:
            card_name = card_name_match.group(1).strip()
            card_id = card_name_match.group(2).strip()
        else:
            return None, None, None

        variant = None
        if "?v=" in url:
            variant_str = url.split("?v=")[-1]
            try:
                variant = int(variant_str)
            except ValueError:
                variant = None

        return card_name, card_id, variant
    except Exception:
        return None, None, None


def download_card_image(
    set_code: str,
    card_id: str,
    variant: Optional[int] = None,
    session: Optional[requests.Session] = None,
) -> tuple[Optional[bytes], Optional[str]]:
    if session is None:
        session = requests.Session()

    image_url = _build_cdn_url(set_code, card_id, variant)
    try:
        resp = session.get(image_url, timeout=30)
        if resp.status_code == 200:
            return resp.content, image_url
    except Exception:
        pass

    return None, None


def _parse_variant(card_path: str) -> tuple[str, Optional[int]]:
    if "?v=" in card_path:
        card_id_part = card_path.split("?")[0]
        variant_str = card_path.split("?v=")[-1]
        try:
            variant = int(variant_str)
        except ValueError:
            variant = None
    else:
        card_id_part = card_path
        variant = None
    return card_id_part, variant


def _derive_set_from_card_id(card_id: str) -> str:
    parts = card_id.split("-")
    if len(parts) >= 2:
        return parts[0]
    return "UNKNOWN"


def get_all_cards_with_variants() -> list[tuple[str, str, Optional[int]]]:
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{LIMITLESS_OP_BASE}/cards/", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=30000)
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return results

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")

    set_entries = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        set_code = cells[0].get_text(strip=True)
        link = cells[1].find("a")
        href = link.get("href") if link else None
        if href and "/cards/" in href:
            set_entries.append((set_code, href))

    for set_code, set_href in set_entries:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f"{LIMITLESS_OP_BASE}{set_href}", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=30000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "lxml")

        seen = set()
        for a in soup.find_all("a"):
            href = a.get("href", "")
            if "/cards/" not in href:
                continue
            card_path = href.split("/")[-1]
            card_id_part, variant = _parse_variant(card_path)
            actual_set = _derive_set_from_card_id(card_id_part)
            key = (card_id_part, variant)
            if key in seen:
                continue
            seen.add(key)
            results.append((actual_set, card_id_part, variant))

    return results


def upload_image_to_minio(
    minio_client,
    bucket: str,
    set_code: str,
    card_id: str,
    image_data: bytes,
    variant: Optional[int] = None,
    content_type: str = "image/webp",
) -> str:
    card_slug = card_id.replace(" ", "-")
    if variant and variant >= 1:
        object_name = f"cards/{set_code}/{card_slug}_v{variant}.webp"
    else:
        object_name = f"cards/{set_code}/{card_slug}.webp"

    try:
        minio_client.put_object(
            bucket,
            object_name,
            image_data,
            len(image_data),
            content_type=content_type,
        )
    except S3Error as e:
        raise RuntimeError(f"Failed to upload image {object_name}: {e}")
    return object_name


def is_image_in_minio(
    minio_client, bucket: str, set_code: str, card_id: str, variant: Optional[int] = None
) -> bool:
    card_slug = card_id.replace(" ", "-")
    if variant and variant >= 1:
        object_name = f"cards/{set_code}/{card_slug}_v{variant}.webp"
    else:
        object_name = f"cards/{set_code}/{card_slug}.webp"

    try:
        minio_client.client.stat_object(bucket, object_name)
        return True
    except Exception:
        return False



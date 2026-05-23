#!/usr/bin/env uv run python3
"""
Scrape eBay US listings → MinIO bronze layer.

Resume-safe: tracks state in data/scrape_state_us.json
Saves per-item parquet to bronze/listings/us/{item_id}.parquet
Saves card images to bronze/images/{card_id}_{card_version}_{item_id}.{ext}
"""
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from minio import Minio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from tcg_platform.scraping.ebay_bronze import (
    extract_thumbnail_url,
    extract_card_id,
    extract_card_version,
    normalize_title,
    parse_listing_page,
)
from tcg_platform.serialization.listing_parquet import write_parquet_bytes

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "tcg-bronze")

BASE_URL = (
    "https://www.ebay.com/sch/i.html"
    "?_nkw=One+Piece+TCG+&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
)
LISTING_LINK_RE = re.compile(r'href="(https://www\.ebay\.com/itm/\d+[^"]*)"')
ITEM_ID_RE = re.compile(r"/itm/(\d+)")
STATE_FILE = Path("data/scrape_state_us.json")
IMG_BUCKET = MINIO_BUCKET
IMG_PREFIX = "bronze/images/"
LIST_PREFIX = "bronze/listings/us/"
PAGES_BEFORE_STOP = 3
RATE_LIMIT = 1.0  # seconds between requests


def get_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_page": 0, "seen_ids": [], "page_errors": 0}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def item_exists_in_minio(client: Minio, item_id: str) -> bool:
    key = f"{LIST_PREFIX}{item_id}.parquet"
    try:
        client.stat_object(MINIO_BUCKET, key)
        return True
    except Exception:
        return False


def upload_image(client: Minio, item_id: str, card_id: str, card_version: str, url: str) -> str:
    """Download image and upload to MinIO. Returns MinIO key or empty string on failure."""
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg").split(";")[0]
        ext = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}.get(content_type, "jpg")
    except Exception:
        return ""

    img_key = f"{IMG_PREFIX}{card_id}{card_version}_{item_id}.{ext}"
    try:
        client.put_object(
            IMG_BUCKET,
            img_key,
            io.BytesIO(resp.content),
            length=len(resp.content),
            content_type=content_type,
        )
        return img_key
    except Exception:
        return ""


def get_page_urls(session: requests.Session, url: str) -> list[str]:
    """Fetch a search results page and return deduplicated item URLs."""
    resp = session.get(url, timeout=30)
    if resp.status_code != 200:
        return []
    html = resp.text
    html = html.replace("&amp;", "&")
    urls = []
    for m in LISTING_LINK_RE.finditer(html):
        raw_url = m.group(1).split("?")[0]
        item_id_m = ITEM_ID_RE.search(raw_url)
        if item_id_m:
            urls.append((item_id_m.group(1), raw_url))
    seen = set()
    deduped = []
    for item_id, raw_url in urls:
        if item_id not in seen:
            seen.add(item_id)
            deduped.append(raw_url)
    return deduped


def main():
    print(f"Starting eBay US bronze scraper")
    print(f"MinIO: {MINIO_ENDPOINT}/{MINIO_BUCKET}")

    state = load_state()
    last_page = state.get("last_page", 0)
    seen_ids = set(state.get("seen_ids", []))
    page_errors = state.get("page_errors", 0)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })

    client = get_minio_client()
    scraped_at = datetime.now(timezone.utc)
    new_scraped = 0
    new_images = 0

    page = last_page + 1
    empty_pages = 0

    while page < 9999:
        url = BASE_URL if page == 1 else f"{BASE_URL}&_pgn={page}"
        print(f"\nPage {page}: fetching...", flush=True)

        try:
            resp = session.get(url, timeout=30)
            status = resp.status_code
        except Exception as e:
            print(f"  HTTP error: {e}")
            page_errors += 1
            if page_errors >= 3:
                print("Too many page errors — stopping")
                break
            page += 1
            continue

        if status != 200:
            print(f"  HTTP {status} — stopping")
            break

        item_urls = get_page_urls(session, url)

        if not item_urls:
            empty_pages += 1
            print(f"  0 URLs (empty_pages={empty_pages})", flush=True)
            if empty_pages >= PAGES_BEFORE_STOP:
                print("Exhausted — stopping")
                break
            page += 1
            continue

        empty_pages = 0
        print(f"  {len(item_urls)} item URLs found, {len(seen_ids)} already known")

        for item_url in item_urls:
            item_id_m = ITEM_ID_RE.search(item_url)
            item_id = item_id_m.group(1) if item_id_m else ""

            if item_id in seen_ids:
                continue

            if item_exists_in_minio(client, item_id):
                seen_ids.add(item_id)
                continue

            print(f"  Scraping item {item_id}...", end="", flush=True)

            try:
                item_resp = session.get(item_url, timeout=30)
                if item_resp.status_code != 200:
                    print(f" HTTP {item_resp.status_code}")
                    time.sleep(RATE_LIMIT)
                    continue
                item_html = item_resp.text
            except Exception as e:
                print(f" err: {e}")
                time.sleep(RATE_LIMIT)
                continue

            row = parse_listing_page(item_html, item_url, scraped_at)

            if not row["card_id"]:
                print(f" no_card_id", end="")
                seen_ids.add(item_id)
                time.sleep(RATE_LIMIT)
                continue

            thumbnail_url = row["thumbnail_url"]
            if thumbnail_url:
                card_id = row["card_id"]
                card_version = row["card_version"]
                img_path = upload_image(client, item_id, card_id, card_version, thumbnail_url)
                row["image_path"] = img_path
                if img_path:
                    print(f" img={img_path.split('/')[-1][:30]}", end="")
                    new_images += 1

            parquet_data = write_parquet_bytes([row])
            parquet_key = f"{LIST_PREFIX}{item_id}.parquet"
            try:
                client.put_object(
                    MINIO_BUCKET,
                    parquet_key,
                    io.BytesIO(parquet_data),
                    length=len(parquet_data),
                    content_type="application/parquet",
                )
                print(f" row", end="")
                new_scraped += 1
            except Exception as e:
                print(f" parquet err: {e}", end="")

            seen_ids.add(item_id)
            time.sleep(RATE_LIMIT)

        state["last_page"] = page
        state["seen_ids"] = list(seen_ids)
        state["page_errors"] = page_errors
        save_state(state)

        page += 1

    print(f"\nDone. New rows: {new_scraped}, new images: {new_images}")
    print(f"State saved: page={page}, seen={len(seen_ids)}")


if __name__ == "__main__":
    main()
#!/usr/bin/env uv run python3
"""Run UK then US eBay scrapes, writing to tcg_uk.db and tcg_us.db."""
import os
import sys
import time
import sqlite3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from zyte_api import ZyteAPI
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from tcg_platform.scraping.ebay import (
    EBAY_REGION_CONFIGS,
    parse_ebay_item_page,
    scrape_ebay_listings,
)

API_KEY = os.getenv("ZYTE_API_KEY")
if not API_KEY:
    print("ZYTE_API_KEY not set")
    sys.exit(1)

SCRAPE_DELAY = 1.0


def create_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_events (
            card_id TEXT NOT NULL,
            card_version TEXT,
            event_type TEXT NOT NULL,
            price REAL,
            currency TEXT,
            sold_date TEXT,
            scraped_from TEXT NOT NULL,
            source TEXT,
            source_url TEXT NOT NULL,
            language TEXT NOT NULL DEFAULT 'EN',
            scraped_at TIMESTAMP NOT NULL,
            PRIMARY KEY (card_id, source_url)
        );
        CREATE INDEX IF NOT EXISTS idx_fe_url ON fact_events(source_url);
    """)
    conn.close()


def get_seen_ids(db_path: str) -> set[str]:
    import re
    ITEM_ID_RE = re.compile(r"/itm/(\d+)")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT source_url FROM fact_events WHERE scraped_from = 'ebay'")
    ids = set()
    for row in cur.fetchall():
        match = ITEM_ID_RE.search(row[0] or "")
        if match:
            ids.add(match.group(1))
    conn.close()
    return ids


def write_records(db_path: str, records: list, region: str) -> int:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = [
        (
            r.card_id,
            r.card_version or "",
            r.event_type,
            r.price,
            r.currency,
            r.sold_date or "",
            r.scraped_from,
            r.source,
            r.source_url,
            r.language,
            r.scraped_at.isoformat() if hasattr(r.scraped_at, "isoformat") else str(r.scraped_at),
        )
        for r in records
    ]
    if rows:
        cur.executemany(
            """INSERT OR REPLACE INTO fact_events
               (card_id, card_version, event_type, price, currency, sold_date,
                scraped_from, source, source_url, language, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
    conn.commit()
    conn.close()
    return len(rows)


def scrape_region(region: str, db_path: str) -> None:
    cfg = EBAY_REGION_CONFIGS[region]
    print(f"\n=== Starting {region} scrape ({cfg['currency']}) ===")
    print(f"URL: {cfg['base_url'][:60]}...")

    create_db(db_path)
    seen = get_seen_ids(db_path)
    print(f"Known item IDs in DB: {len(seen)}")

    client = ZyteAPI(api_key=API_KEY)
    scraped_at = datetime.now(timezone.utc)

    all_urls = list(scrape_ebay_listings(client, region, seen))
    print(f"New URLs to scrape: {len(all_urls)}")

    if not all_urls:
        print("No new listings found")
        return

    written = 0
    for i, url in enumerate(all_urls, 1):
        item_id_m = re.search(r"/itm/(\d+)", url)
        item_id = item_id_m.group(1) if item_id_m else url

        try:
            resp = client.get({"url": url, "browserHtml": True})
            if resp.get("statusCode") != 200:
                print(f"  [{i}/{len(all_urls)}] {item_id} HTTP {resp.get('statusCode')}")
                continue
            html = resp.get("browserHtml", "") or ""
            if len(html) < 1000:
                print(f"  [{i}/{len(all_urls)}] {item_id} thin HTML ({len(html)}b)")
                continue

            records = parse_ebay_item_page(html, url, scraped_at, region)
            if records:
                n = write_records(db_path, records, region)
                written += n
                rec = records[0]
                print(f"  [{i}/{len(all_urls)}] {item_id} OK {cfg['currency']} {rec.price} lang={rec.language}")
            else:
                print(f"  [{i}/{len(all_urls)}] {item_id} no records")

        except Exception as e:
            print(f"  [{i}/{len(all_urls)}] {item_id} ERROR: {e}")

        if i % 20 == 0:
            print(f"  ... progress: {i}/{len(all_urls)}")

        time.sleep(SCRAPE_DELAY)

    final_count = sqlite3.connect(db_path).execute(
        f"SELECT COUNT(*) FROM fact_events WHERE scraped_from = 'ebay'"
    ).fetchone()[0]
    lang_counts = {}
    for row in sqlite3.connect(db_path).execute(
        f"SELECT language, COUNT(*) FROM fact_events WHERE scraped_from = 'ebay' GROUP BY language"
    ).fetchall():
        lang_counts[row[0]] = row[1]

    print(f"\n=== {region} scrape complete ===")
    print(f"Total rows in {db_path}: {final_count}")
    print(f"Languages: {lang_counts}")


if __name__ == "__main__":
    import re

    UK_DB = "data/tcg_uk.db"
    US_DB = "data/tcg_us.db"

    scrape_region("UK", UK_DB)
    print("\n" + "="*50)
    scrape_region("US", US_DB)

    print("\n=== DONE ===")
    for path, region in [(UK_DB, "UK"), (US_DB, "US")]:
        count = sqlite3.connect(path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
        langs = {}
        for row in sqlite3.connect(path).execute(
            "SELECT language, COUNT(*) FROM fact_events GROUP BY language"
        ).fetchall():
            langs[row[0]] = row[1]
        print(f"{region} ({path}): {count} rows, {langs}")
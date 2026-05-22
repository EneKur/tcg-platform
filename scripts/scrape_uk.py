#!/usr/bin/env uv run python3
"""Scrape UK eBay sold listings to tcg_uk.db"""
import os, sys, re, time, sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))
from zyte_api import ZyteAPI
from datetime import datetime, timezone
from tcg_platform.scraping.ebay import parse_ebay_item_page, scrape_ebay_listings, EBAY_REGION_CONFIGS

API_KEY = os.getenv("ZYTE_API_KEY")
client = ZyteAPI(api_key=API_KEY)
DELAY = 0.5
REGION = "UK"
DB_PATH = "data/tcg_uk.db"

conn = sqlite3.connect(DB_PATH)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS fact_events (
        card_id TEXT NOT NULL, card_version TEXT, event_type TEXT NOT NULL,
        price REAL, currency TEXT, sold_date TEXT, scraped_from TEXT NOT NULL,
        source TEXT, source_url TEXT NOT NULL, language TEXT NOT NULL DEFAULT 'EN',
        scraped_at TIMESTAMP NOT NULL, PRIMARY KEY (card_id, source_url));
    CREATE INDEX IF NOT EXISTS idx_fe_url ON fact_events(source_url);
""")
conn.close()

seen = {re.search(r"/itm/(\d+)", r[0]).group(1) for r in
        sqlite3.connect(DB_PATH).execute("SELECT source_url FROM fact_events").fetchall()
        if re.search(r"/itm/(\d+)", r[0] or "")}
print(f"UK: {len(seen)} known IDs in DB")

scraped_at = datetime.now(timezone.utc)
urls = list(scrape_ebay_listings(client, REGION, seen))
print(f"UK: {len(urls)} new URLs to scrape")

for i, url in enumerate(urls, 1):
    item_id = re.search(r"/itm/(\d+)", url).group(1) if re.search(r"/itm/(\d+)", url) else url
    try:
        resp = client.get({"url": url, "browserHtml": True})
        if resp.get("statusCode") != 200:
            print(f"  [{i}/{len(urls)}] {item_id} HTTP {resp.get('statusCode')}")
            time.sleep(DELAY); continue
        html = resp.get("browserHtml", "") or ""
        if len(html) < 1000:
            time.sleep(DELAY); continue
        recs = parse_ebay_item_page(html, url, scraped_at, REGION)
        if recs:
            conn = sqlite3.connect(DB_PATH)
            rows = [(r.card_id, r.card_version or "", r.event_type, r.price, r.currency,
                     r.sold_date or "", r.scraped_from, r.source, r.source_url, r.language,
                     r.scraped_at.isoformat()) for r in recs]
            conn.executemany("INSERT OR REPLACE INTO fact_events VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit(); conn.close()
            print(f"  [{i}/{len(urls)}] {item_id} OK {recs[0].price} GBP lang={recs[0].language}")
    except Exception as e:
        print(f"  [{i}/{len(urls)}] {item_id} ERR: {e}")
    time.sleep(DELAY)

count = sqlite3.connect(DB_PATH).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
print(f"UK DONE: {count} rows in {DB_PATH}")
#!/usr/bin/env uv run python3
"""Scrape UK and US eBay sold listings in parallel, writing to tcg_uk.db and tcg_us.db."""
import os, sys, re, time, sqlite3, threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))
from zyte_api import ZyteAPI
from datetime import datetime, timezone
from tcg_platform.scraping.ebay import parse_ebay_item_page, scrape_ebay_listings, EBAY_REGION_CONFIGS

API_KEY = os.getenv("ZYTE_API_KEY")
MAX_WORKERS = 8
DELAY = 0.3
_lock = threading.Lock()

NZYTE = 2  # two parallel ZyteAPI sessions to avoid connection contention


def create_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_events (
            card_id TEXT NOT NULL, card_version TEXT, event_type TEXT NOT NULL,
            price REAL, currency TEXT, sold_date TEXT, scraped_from TEXT NOT NULL,
            source TEXT, source_url TEXT NOT NULL, language TEXT NOT NULL DEFAULT 'EN',
            scraped_at TIMESTAMP NOT NULL, PRIMARY KEY (card_id, source_url));
        CREATE INDEX IF NOT EXISTS idx_fe_url ON fact_events(source_url);
    """)
    conn.close()


def get_seen_ids(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT source_url FROM fact_events WHERE scraped_from = 'ebay'")
    ids = set()
    for row in cur.fetchall():
        m = re.search(r"/itm/(\d+)", row[0] or "")
        if m: ids.add(m.group(1))
    conn.close()
    return ids


def write_records(db_path, records):
    if not records: return 0
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = [(r.card_id, r.card_version or "", r.event_type, r.price,
              r.currency, r.sold_date or "", r.scraped_from, r.source,
              r.source_url, r.language,
              r.scraped_at.isoformat() if hasattr(r.scraped_at, "isoformat") else str(r.scraped_at))
             for r in records]
    cur.executemany("""INSERT OR REPLACE INTO fact_events
        (card_id, card_version, event_type, price, currency, sold_date,
         scraped_from, source, source_url, language, scraped_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    conn.close()
    return len(rows)


def scrape_one(client, region, db_path, url, scraped_at):
    item_id_m = re.search(r"/itm/(\d+)", url)
    item_id = item_id_m.group(1) if item_id_m else url
    cfg = EBAY_REGION_CONFIGS[region]

    try:
        resp = client.get({"url": url, "browserHtml": True})
        if resp.get("statusCode") != 200:
            return item_id, "http", None
        html = resp.get("browserHtml", "") or ""
        if len(html) < 1000:
            return item_id, "thin", None
        recs = parse_ebay_item_page(html, url, scraped_at, region)
        if not recs:
            return item_id, "no_rec", None
        n = write_records(db_path, recs)
        return item_id, "ok", (recs[0].price, cfg["currency"], recs[0].language)
    except Exception as e:
        return item_id, "err", str(e)


def scrape_region(region, db_path):
    print(f"\n=== {region} starting ===")
    create_db(db_path)
    seen = get_seen_ids(db_path)
    print(f"{region}: {len(seen)} known IDs in DB")

    # Use dedicated ZyteAPI instance per region
    client = ZyteAPI(api_key=API_KEY, n_conn=MAX_WORKERS)
    scraped_at = datetime.now(timezone.utc)

    all_urls = list(scrape_ebay_listings(client, region, seen))
    print(f"{region}: {len(all_urls)} new URLs")

    if not all_urls:
        count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
        print(f"{region} DONE (no new): {count} rows")
        return

    results = {"ok": 0, "http": 0, "thin": 0, "no_rec": 0, "err": 0}
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_one, client, region, db_path, url, scraped_at): url
            for url in all_urls
        }
        for future in as_completed(futures):
            item_id, status, data = future.result()
            results[status] = results.get(status, 0) + 1
            completed += 1

            if status == "ok":
                price, currency, lang = data
                print(f"  [{completed}/{len(all_urls)}] {item_id} OK {price} {currency} lang={lang}")
            elif status == "http":
                print(f"  [{completed}/{len(all_urls)}] {item_id} HTTP {data}")
            elif status == "thin":
                print(f"  [{completed}/{len(all_urls)}] {item_id} thin HTML")
            elif status == "no_rec":
                print(f"  [{completed}/{len(all_urls)}] {item_id} no records")
            else:
                print(f"  [{completed}/{len(all_urls)}] {item_id} ERR: {data}")

            time.sleep(DELAY)

    count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    print(f"\n{region} SUMMARY: {results} | {count} rows in {db_path}")


def main():
    print(f"Starting parallel UK + US scrape")
    print(f"API: {API_KEY[:8]}...")
    print(f"Max workers per region: {MAX_WORKERS}, delay: {DELAY}s")

    import multiprocessing
    t1 = threading.Thread(target=lambda: scrape_region("UK", "data/tcg_uk.db"))
    t2 = threading.Thread(target=lambda: scrape_region("US", "data/tcg_us.db"))

    t1.start()
    time.sleep(1)  # slight stagger to avoid thundering herd
    t2.start()

    t1.join()
    t2.join()

    print("\n=== ALL DONE ===")
    for path, region in [("data/tcg_uk.db", "UK"), ("data/tcg_us.db", "US")]:
        conn = sqlite3.connect(path)
        count = conn.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
        langs = {r[0]: r[1] for r in conn.execute("SELECT language, COUNT(*) FROM fact_events GROUP BY language").fetchall()}
        curs = {r[0]: r[1] for r in conn.execute("SELECT currency, COUNT(*) FROM fact_events GROUP BY currency").fetchall()}
        print(f"  {region}: {count} rows | langs: {langs} | currencies: {curs}")


if __name__ == "__main__":
    main()
import os
import sqlite3
import re
import json
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from zyte_api import ZyteAPI
from dotenv import load_dotenv

load_dotenv()

from tcg_platform.scraping.ebay import parse_ebay_item_page

SOURCE_DB = "data/tcg.db"
VERIFY_DB = "data/tcg_verify.db"
MAX_WORKERS = 8
SCRAPE_DELAY = 0.5


_lock = threading.Lock()


def load_urls(db_path: str) -> list[tuple[str, str, str]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT card_id, source_url, language AS current_lang
        FROM fact_events
        WHERE scraped_from = 'ebay'
        ORDER BY rowid
    """)
    rows = cur.fetchall()
    conn.close()
    return [(str(r["card_id"]), r["source_url"], r["current_lang"]) for r in rows]


def init_verify_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(f"""
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


def write_record(db_path: str, rec: dict) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO fact_events
        (card_id, card_version, event_type, price, currency, sold_date,
         scraped_from, source, source_url, language, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        rec["card_id"], rec.get("card_version"), rec["event_type"], rec["price"],
        rec["currency"], rec.get("sold_date"), rec["scraped_from"], rec.get("source"),
        rec["source_url"], rec["language"], rec["scraped_at"]
    ))
    conn.commit()
    conn.close()


def scrape_one(client, card_id: str, url: str, current_lang: str) -> dict:
    item_id_m = re.search(r"/itm/(\d+)", url)
    item_id = item_id_m.group(1) if item_id_m else url

    result = {
        "item_id": item_id,
        "url": url,
        "old_card_id": card_id,
        "new_card_id": None,
        "old_lang": current_lang,
        "new_lang": None,
        "is_proxy": False,
        "price": None,
        "status": "error",
        "error": None,
        "html_size": 0,
    }

    try:
        resp = client.get({"url": url, "browserHtml": True})
        status = resp.get("statusCode", 0)
        if status != 200:
            result["status"] = f"http_{status}"
            return result

        html = resp.get("browserHtml", "") or ""
        result["html_size"] = len(html)

        if len(html) < 1000:
            result["status"] = "thin_html"
            return result

        records = parse_ebay_item_page(html, url, datetime.now())
        if not records:
            result["status"] = "no_records"
            return result

        rec = records[0]
        result["new_card_id"] = rec.card_id
        result["new_lang"] = rec.language
        result["is_proxy"] = rec.card_id.startswith("proxy_")
        result["price"] = rec.price

        mismatch = result["new_lang"] != current_lang or result["is_proxy"]
        result["status"] = "mismatch" if mismatch else "ok"

        with _lock:
            write_record(VERIFY_DB, rec.model_dump())

        return result

    except Exception as e:
        result["status"] = "exception"
        result["error"] = str(e)
        return result


def main():
    init_verify_db(VERIFY_DB)
    urls = load_urls(SOURCE_DB)
    print(f"Loaded {len(urls)} URLs from {SOURCE_DB}")

    api_key = os.getenv("ZYTE_API_KEY")
    if not api_key:
        print("ZYTE_API_KEY not set")
        return

    client = ZyteAPI(api_key=api_key, n_conn=MAX_WORKERS)

    mismatches = []
    results_list = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_one, client, card_id, url, current_lang): (card_id, url, current_lang)
            for card_id, url, current_lang in urls
        }

        for future in as_completed(futures):
            res = future.result()
            results_list.append(res)
            completed += 1

            if res["status"] == "mismatch":
                with _lock:
                    mismatches.append(res)
                    print(f"[{completed}/{len(urls)}] MISMATCH {res['item_id']}: "
                          f"{res['old_lang']}→{res['new_lang']} proxy={res['is_proxy']}")
                    print(f"    card: {res['old_card_id'][:40]} → {res['new_card_id'][:40]}")
            elif res["status"] == "ok":
                print(f"[{completed}/{len(urls)}] OK {res['item_id']} lang={res['new_lang']} price={res['price']}")
            else:
                print(f"[{completed}/{len(urls)}] {res['status']} {res['item_id']} ({res.get('error','')}")

            time.sleep(SCRAPE_DELAY)

    print(f"\n=== SUMMARY ===")
    status_counts = {}
    for r in results_list:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    for k, v in sorted(status_counts.items()):
        print(f"  {k}: {v}")

    with open("data/verify_mismatches.json", "w") as f:
        json.dump(mismatches, f, indent=2, default=str)

    verify_count = sqlite3.connect(VERIFY_DB).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    print(f"\nVerify DB rows: {verify_count}")
    print(f"Mismatches: {len(mismatches)}")
    print(f"Full mismatch list → data/verify_mismatches.json")


if __name__ == "__main__":
    main()
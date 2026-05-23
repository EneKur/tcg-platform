#!/usr/bin/env uv run python3
"""Exhaustively scrape US eBay sold listings with incremental flush."""
import os, sys, re, time, sqlite3, html as _html_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.getcwd(), '.env'))
from zyte_api import ZyteAPI

API_KEY = os.getenv("ZYTE_API_KEY")
MAX_WORKERS = 8
DELAY = 0.3
PAGES_BEFORE_ASSERT = 3
FLUSH_EVERY = 100

EBAY_CFG = {
    "base_url": "https://www.ebay.com/sch/i.html?_nkw=One+Piece+TCG+&_sacat=0&_from=R40&_sop=13&LH_Sold=1",
    "currency": "USD",
    "listing_pattern": r'href="(https://www\.ebay\.com/itm/\d+[^"]*)"',
}
SET_CODE_RE = re.compile(r"(OP\d+|EB\d+|ST\d+|PRB\d+|P\d+)")
PRICE_RE = re.compile(r"data-testid=\"x-price-primary\".*?<span[^>]*>([^<]+)</span>", re.DOTALL)
TITLE_RE = re.compile(r"<h1[^>]*>.*?<span[^>]*>(.*?)</span>", re.DOTALL)
ITEM_ID_RE = re.compile(r"/itm/(\d+)")
MONTHS_EN = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
             "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12}


def _parse_date_en(html):
    month_names = "|".join(MONTHS_EN.keys())
    m = re.search(rf"(\d{{1,2}})\s+({month_names})\s+(\d{{4}})", html, re.IGNORECASE)
    if m:
        d, mn, y = m.groups()
        return f"{y}-{MONTHS_EN[mn.lower()]:02d}-{int(d):02d}"
    return None


def _normalize(t):
    t = re.sub(r"\s*\(.*?\)", "", t)
    t = re.sub(r"\s*\[.*?\]", "", t)
    t = re.sub(r"[^a-zA-Z0-9\s]", "", t)
    return t.strip().replace(" ", "_")[:50]


def _split(t):
    m = SET_CODE_RE.search(t)
    return (t[:m.end()], t[m.end():].strip("_")) if m else (t, "")


def _lang(t):
    t = t.lower()
    return "JP" if any(i in t for i in ["japan", "jap", "jp_", " japanese", "jp-", " japan", "japan import", " japan", "japanese version"]) else "EN"


def _proxy(t):
    return any(i in t.lower() for i in ["proxy", "dummy", "fake card", "replica"])


def create_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript("""CREATE TABLE IF NOT EXISTS fact_events (
        card_id TEXT NOT NULL, card_version TEXT, event_type TEXT NOT NULL,
        price REAL, currency TEXT, sold_date TEXT, scraped_from TEXT NOT NULL,
        source TEXT, source_url TEXT NOT NULL, language TEXT NOT NULL DEFAULT 'EN',
        scraped_at TIMESTAMP NOT NULL, PRIMARY KEY (card_id, source_url));
        CREATE INDEX IF NOT EXISTS idx_fe_url ON fact_events(source_url);""")
    conn.close()


def get_seen_ids(db_path):
    conn = sqlite3.connect(db_path)
    ids = set()
    for row in conn.execute('SELECT source_url FROM fact_events WHERE scraped_from = "ebay"'):
        m = ITEM_ID_RE.search(row[0] or "")
        if m:
            ids.add(m.group(1))
    conn.close()
    return ids


def write_records(db_path, records):
    if not records:
        return 0
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = []
    for r in records:
        if isinstance(r, dict):
            card_id = r["card_id"]
            card_version = r.get("card_version") or ""
            event_type = r["event_type"]
            price = r["price"]
            currency = r["currency"]
            sold_date = r.get("sold_date") or ""
            scraped_from = r["scraped_from"]
            source = r["source"]
            source_url = r["source_url"]
            language = r["language"]
            scraped_at = r["scraped_at"]
        else:
            card_id = r.card_id
            card_version = getattr(r, "card_version", "") or ""
            event_type = r.event_type
            price = r.price
            currency = r.currency
            sold_date = getattr(r, "sold_date", "") or ""
            scraped_from = r.scraped_from
            source = r.source
            source_url = r.source_url
            language = r.language
            scraped_at = r.scraped_at
        sa = scraped_at.isoformat() if hasattr(scraped_at, "isoformat") else str(scraped_at)
        rows.append((card_id, card_version, event_type, price, currency, sold_date,
                     scraped_from, source, source_url, language, sa))
    cur.executemany("""INSERT OR REPLACE INTO fact_events
        (card_id, card_version, event_type, price, currency, sold_date,
         scraped_from, source, source_url, language, scraped_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    conn.close()
    return len(rows)


def parse_item(html, item_url, scraped_at, region):
    cfg = EBAY_CFG
    pm = PRICE_RE.search(html)
    pt = pm.group(1).strip() if pm else ""
    pv = re.sub(r"[^\d.]", "", pt)
    try:
        price = float(pv) if pt else None
    except:
        price = None
    tm = TITLE_RE.search(html)
    title = tm.group(1).strip() if tm else ""
    raw = _normalize(title)
    card_id, cv = _split(raw)
    sd = _parse_date_en(html)
    lang = _lang(title)
    if _proxy(title):
        return []
    if price is not None:
        return [{"card_id": card_id, "card_version": cv or None, "event_type": "sale",
                 "price": price, "currency": cfg["currency"], "sold_date": sd,
                 "scraped_from": "ebay", "source": region, "source_url": item_url,
                 "language": lang, "scraped_at": scraped_at}]
    return []


class Rec:
    def __init__(self, d):
        self.__dict__.update(d)


def scrape_one(client, region, db_path, url, scraped_at):
    item_id_m = ITEM_ID_RE.search(url)
    item_id = item_id_m.group(1) if item_id_m else url
    try:
        resp = client.get({"url": url, "browserHtml": True})
        if resp.get("statusCode") != 200:
            return item_id, "http", None
        html = resp.get("browserHtml", "") or ""
        if len(html) < 1000:
            return item_id, "thin", None
        recs = parse_item(html, url, scraped_at, region)
        if not recs:
            return item_id, "no_rec", None
        write_records(db_path, [Rec(r) for r in recs])
        return item_id, "ok", (recs[0].price, region)
    except Exception as e:
        return item_id, "err", str(e)


def get_page_urls(html, region):
    decoded = _html_module.unescape(html)
    urls = []
    for match in re.finditer(EBAY_CFG["listing_pattern"], decoded):
        raw_url = match.group(1).replace("&amp;", "&")
        clean_url = re.sub(r"\?.*", "", raw_url)
        if clean_url and clean_url not in [u for u, _ in urls]:
            urls.append((clean_url, clean_url))
    return urls


def main():
    region = "US"
    db_path = "data/tcg_us.db"
    print("Starting US exhaustive scrape (incremental flush)")
    print(f"API: {API_KEY[:8]}...")

    create_db(db_path)
    seen = get_seen_ids(db_path)
    prev_count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    print(f"US: {prev_count} existing rows, {len(seen)} known IDs")

    client = ZyteAPI(api_key=API_KEY, n_conn=MAX_WORKERS)
    scraped_at = datetime.now(timezone.utc)

    all_new = []
    empty_pages = 0
    page = 1

    while page < 9999:
        url = EBAY_CFG["base_url"] if page == 1 else f"{EBAY_CFG['base_url']}&_pgn={page}"
        print(f"  [US] page {page}: fetching...", flush=True)
        resp = client.get({"url": url, "browserHtml": True})
        status = resp.get("statusCode")
        if status != 200:
            print(f"  [US] page {page}: HTTP {status} — stopping")
            break
        html = resp.get("browserHtml", "") or ""
        if not html or len(html) < 500:
            print(f"  [US] page {page}: thin HTML — stopping")
            break
        page_urls = get_page_urls(html, region)
        if not page_urls:
            empty_pages += 1
            print(f"  [US] page {page}: 0 URLs (empty_pages={empty_pages})", flush=True)
            if empty_pages >= PAGES_BEFORE_ASSERT:
                print(f"  [US] exhausted")
                break
            page += 1
            continue
        empty_pages = 0
        new_urls = []
        for raw_url, _ in page_urls:
            item_id = (ITEM_ID_RE.search(raw_url) or re.match(r"(\d+)", raw_url)).group(1)
            if item_id not in seen:
                new_urls.append(raw_url)
                seen.add(item_id)
        print(f"  [US] page {page}: total={len(page_urls)} new={len(new_urls)} cumulative={len(all_new) + len(new_urls)}", flush=True)
        all_new.extend(new_urls)
        if len(page_urls) < 10:
            print(f"  [US] page {page}: history end")
            break

        page += 1

        if len(all_new) >= FLUSH_EVERY:
            print(f"\n  === FLUSH: scraping {len(all_new)} URLs ===")
            results = {"ok": 0, "http": 0, "thin": 0, "no_rec": 0, "err": 0}
            batch = all_new[:FLUSH_EVERY]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(scrape_one, client, region, db_path, url, scraped_at): url for url in batch}
                for future in as_completed(futures):
                    item_id, status, data = future.result()
                    results[status] = results.get(status, 0) + 1
                    if status == "ok":
                        print(f"  [ok] {item_id} {data[0]} USD")
                    else:
                        print(f"  [{status}] {item_id} {data}")
                    time.sleep(DELAY)
            count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
            print(f"  Flush done: {results} | total={count}")
            all_new = all_new[FLUSH_EVERY:]
            scraped_at = datetime.now(timezone.utc)

        time.sleep(0.5)

    if all_new:
        print(f"\n  === FINAL FLUSH: scraping {len(all_new)} URLs ===")
        results = {"ok": 0, "http": 0, "thin": 0, "no_rec": 0, "err": 0}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_one, client, region, db_path, url, scraped_at): url for url in all_new}
            for future in as_completed(futures):
                item_id, status, data = future.result()
                results[status] = results.get(status, 0) + 1
                if status == "ok":
                    print(f"  [ok] {item_id} {data[0]} USD")
                else:
                    print(f"  [{status}] {item_id} {data}")
                time.sleep(DELAY)
        count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
        print(f"  Final flush done: {results} | total={count}")

    count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    langs = dict(sqlite3.connect(db_path).execute("SELECT language, COUNT(*) FROM fact_events GROUP BY language").fetchall())
    print(f"\nUS DONE: {count} rows (was {prev_count}) | langs: {langs}")


if __name__ == "__main__":
    main()
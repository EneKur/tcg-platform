#!/usr/bin/env uv run python3
"""Exhaustively scrape UK and US eBay sold listings until no more pages remain."""
import os, sys, re, time, sqlite3, threading, html as _html_module
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

EBAY_REGION_CONFIGS = {
    "UK": {
        "base_url": (
            "https://www.ebay.co.uk/sch/i.html"
            "?_nkw=One+Piece+TCG+&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
        ),
        "currency": "GBP",
        "listing_pattern": r'href="(https://www\.ebay\.co\.uk/itm/\d+[^"]*)"',
    },
    "US": {
        "base_url": (
            "https://www.ebay.com/sch/i.html"
            "?_nkw=One+Piece+TCG+&_sacat=0&_from=R40&_sop=13&LH_Sold=1"
        ),
        "currency": "USD",
        "listing_pattern": r'href="(https://www\.ebay\.com/itm/\d+[^"]*)"',
    },
}

SET_CODE_RE = re.compile(r"(OP\d+|EB\d+|ST\d+|PRB\d+|P\d+)")
DATE_RE_ENGLISH = re.compile(
    r"Sold\s+\w+\s+\d{1,2}\s+\w+\s+\d{4}|"
    r"Sold\s+\w+\s+\d{1,2},\s+\d{4}|"
    r"\d{1,2}\s+\w+\s+\d{4}"
)
PRICE_RE = re.compile(r"data-testid=\"x-price-primary\".*?<span[^>]*>([^<]+)</span>", re.DOTALL)
TITLE_RE = re.compile(r"<h1[^>]*>.*?<span[^>]*>(.*?)</span>", re.DOTALL)
ITEM_ID_RE = re.compile(r"/itm/(\d+)")
MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_date_en(html: str):
    month_names = "|".join(MONTHS_EN.keys())
    m = re.search(rf"(\d{{1,2}})\s+({month_names})\s+(\d{{4}})", html, re.IGNORECASE)
    if m:
        day, month_name, year = m.groups()
        return f"{year}-{MONTHS_EN[month_name.lower()]:02d}-{int(day):02d}"
    return None


def _normalize_card_id(title: str) -> str:
    title = re.sub(r"\s*\(.*?\)", "", title)
    title = re.sub(r"\s*\[.*?\]", "", title)
    title = re.sub(r"[^a-zA-Z0-9\s]", "", title)
    return title.strip().replace(" ", "_")[:50]


def _split_base_version(card_id: str):
    set_m = SET_CODE_RE.search(card_id)
    if not set_m:
        return card_id, ""
    return card_id[: set_m.end()], card_id[set_m.end() :].strip("_")


def _detect_language(title: str) -> str:
    title_lower = title.lower()
    jp_indicators = ["japan", "jap", "jp_", " japanese", "jp-", " japan", "japan import", " japan", "japanese version"]
    return "JP" if any(indic in title_lower for indic in jp_indicators) else "EN"


def _is_proxy(title: str) -> bool:
    title_lower = title.lower()
    proxy_indicators = ["proxy", "dummy", "fake card", "replica"]
    return any(ind in title_lower for ind in proxy_indicators)


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


def parse_item(html, item_url, scraped_at, region):
    cfg = EBAY_REGION_CONFIGS[region]
    price_match = PRICE_RE.search(html)
    price_text = price_match.group(1).strip() if price_match else ""
    price_value = re.sub(r"[^\d.]", "", price_text)
    try:
        price = float(price_value) if price_text else None
    except ValueError:
        price = None
    title_match = TITLE_RE.search(html)
    title = title_match.group(1).strip() if title_match else ""
    raw_card_id = _normalize_card_id(title)
    card_id, card_version = _split_base_version(raw_card_id)
    sold_date = _parse_date_en(html)
    language = _detect_language(title)
    if _is_proxy(title):
        return []
    if price is not None:
        return [{"card_id": card_id, "card_version": card_version or None,
                 "event_type": "sale", "price": price, "currency": cfg["currency"],
                 "sold_date": sold_date, "scraped_from": "ebay", "source": region,
                 "source_url": item_url, "language": language, "scraped_at": scraped_at}]
    return []


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
        write_records(db_path, [type('Rec', (), r) for r in recs])
        return item_id, "ok", (recs[0]["price"], region)
    except Exception as e:
        return item_id, "err", str(e)


def get_page_urls(html, region):
    cfg = EBAY_REGION_CONFIGS[region]
    decoded = _html_module.unescape(html)
    urls = []
    for match in re.finditer(cfg["listing_pattern"], decoded):
        raw_url = match.group(1).replace("&amp;", "&")
        clean_url = re.sub(r"\?.*", "", raw_url)
        if clean_url and clean_url not in [u for u, _ in urls]:
            urls.append((clean_url, clean_url))
    return urls


def paginate_and_collect(client, region, seen_ids):
    """Paginate until we hit empty pages, collecting all new item URLs."""
    cfg = EBAY_REGION_CONFIGS[region]
    base_url = cfg["base_url"]
    all_new = []
    empty_pages = 0

    for page in range(1, 9999):
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}&_pgn={page}"

        print(f"  [{region}] page {page}: fetching...")
        resp = client.get({"url": url, "browserHtml": True})
        if resp.get("statusCode") != 200:
            print(f"  [{region}] page {page}: HTTP {resp.get('statusCode')} — stopping")
            break
        html = resp.get("browserHtml", "") or ""
        if not html or len(html) < 500:
            print(f"  [{region}] page {page}: empty/thin HTML — stopping")
            break

        page_urls = get_page_urls(html, region)
        if not page_urls:
            empty_pages += 1
            print(f"  [{region}] page {page}: 0 URLs (empty_pages={empty_pages})")
            if empty_pages >= PAGES_BEFORE_ASSERT:
                print(f"  [{region}] {empty_pages} consecutive empty pages — eBay exhausted, stopping")
                break
            continue

        empty_pages = 0
        new_urls = []
        for raw_url, _ in page_urls:
            item_id = (ITEM_ID_RE.search(raw_url) or re.match(r"(\d+)", raw_url)).group(1)
            if item_id not in seen_ids:
                new_urls.append(raw_url)
                seen_ids.add(item_id)

        print(f"  [{region}] page {page}: {len(page_urls)} total URLs, {len(new_urls)} new (cumulative new: {len(all_new) + len(new_urls)})")
        all_new.extend(new_urls)

        if len(page_urls) < 10:
            print(f"  [{region}] page {page}: only {len(page_urls)} URLs — eBay history end")
            break

    return all_new


def scrape_region(region, db_path):
    print(f"\n=== {region} ===")
    create_db(db_path)
    seen = get_seen_ids(db_path)
    prev_count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    print(f"{region}: {prev_count} existing rows, {len(seen)} known IDs")

    client = ZyteAPI(api_key=API_KEY, n_conn=MAX_WORKERS)
    scraped_at = datetime.now(timezone.utc)

    new_urls = paginate_and_collect(client, region, seen)
    print(f"{region}: collected {len(new_urls)} new URLs to scrape")

    if not new_urls:
        print(f"{region} DONE (no new): {prev_count} rows")
        return

    results = {"ok": 0, "http": 0, "thin": 0, "no_rec": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_one, client, region, db_path, url, scraped_at): url for url in new_urls}
        for future in as_completed(futures):
            item_id, status, data = future.result()
            results[status] = results.get(status, 0) + 1
            if status == "ok":
                print(f"  [{status}] {item_id} OK {data[0]} {data[1]}")
            else:
                print(f"  [{status}] {item_id} {data}")
            time.sleep(DELAY)

    count = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    print(f"\n{region} SUMMARY: {results} | {count} rows (was {prev_count})")


def main():
    print(f"Exhaustive UK + US scrape")
    print(f"API: {API_KEY[:8]}...")
    print(f"Max workers: {MAX_WORKERS}, delay: {DELAY}s")

    t1 = threading.Thread(target=lambda: scrape_region("UK", "data/tcg_uk.db"))
    t2 = threading.Thread(target=lambda: scrape_region("US", "data/tcg_us.db"))
    t1.start()
    time.sleep(1)
    t2.start()
    t1.join()
    t2.join()

    print("\n=== ALL DONE ===")
    for path, region in [("data/tcg_uk.db", "UK"), ("data/tcg_us.db", "US")]:
        conn = sqlite3.connect(path)
        count = conn.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
        langs = dict(conn.execute("SELECT language, COUNT(*) FROM fact_events GROUP BY language").fetchall())
        curs = dict(conn.execute("SELECT currency, COUNT(*) FROM fact_events GROUP BY currency").fetchall())
        print(f"  {region}: {count} rows | langs: {langs} | currencies: {curs}")


if __name__ == "__main__":
    main()
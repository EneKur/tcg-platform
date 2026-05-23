# eBay Listings Bronze Layer — Scraping + MinIO Storage

## Context

The Zyte API free tier is exhausted. We need to scrape eBay US listings using normal Python HTTP packages (requests/httpx/BeautifulSoup). The goal is to build a Bronze layer in MinIO where:

- Each item_id = one parquet file (static, no paged data)
- Parquet contains: card_id, card_version, price, currency, sold_date, language, raw_html_payload, source_url, scraped_at
- No filtering at Bronze level — all listings saved as-is
- Later (Silver layer): card_id validated against MinIO cardlist, currency normalized, bad listings dropped

## Architecture

```
eBay US → Python Scraper → Per-item parquet → bronze/listings/us/{item_id}.parquet
                                    ↓
                            Cardlist from MinIO
                            (cards/*/)
                                    ↓
                            Silver: validate, normalize, dedup
                                    ↓
                            Gold: business aggregates
```

## Scraping Pipeline

### Input
- Seed URLs: `https://www.ebay.com/sch/i.html?_nkw=One+Piece+TCG+&_sacat=0&_from=R40&_sop=13&LH_Sold=1` with pagination `&_pgn={n}`
- Resume from existing state (track last scraped page + known item_ids in MinIO)

### Per-item parquet schema

```
item_id:          string   (e.g. "406939215710")
source_url:       string   (eBay listing URL)
scraped_at:       string   (ISO timestamp)
region:           string   ("US")

card_id:          string   (e.g. "OP15", "OP09119", "EB01-001" — extracted from title)
card_version:     string   (e.g. "_Alternative_Art" or "" if none)
title:            string   (raw listing title)
price:            float
currency:          string   ("USD", "AUD", "CAD", "GBP", "EUR" — detected from page or URL)
sold_date:        string   (YYYY-MM-DD or "" if unavailable)
language:         string   ("EN" or "JP")
html_payload:     bytes    (full HTML of the listing page, compressed)
thumbnail_url:    string   (listing image URL from HTML `icImg` id — smaller version)
image_path:       string   (MinIO key for downloaded image, e.g. "bronze/images/OP15-001_Alternative_Art_406939215710.jpg")
```

### Currency detection
- eBay page DOM: check `<span data-testid="x-price-primary">` or similar
- eBay URL pattern: `ebay.com` = USD default, `ebay.com.au` = AUD, `ebay.ca` = CAD
- If price contains `A$` or `AUD` → AUD
- If price contains `C$` or `CAD` → CAD
- If no indicator and `ebay.com` domain → USD

### Card ID extraction from title
Regex patterns to try in order:
1. `\b(OP\d+-\d+)\b` → e.g. "OP15-001"
2. `\b(EB\d+-\d+)\b` → e.g. "EB01-001"
3. `\b(OP\d+)\b` → e.g. "OP15" (set-only for ST/PR cards)
4. `\b(ST\d+|PRB\d+)\b` → starter/promo sets

Strip everything else. Title normalization:
- Remove `(...)` and `[...]` noise
- Replace spaces with `_`
- Keep only alphanumeric + underscore

### sold_date parsing (English)
eBay US format: "Sold Mon, Jan 1, 2024" or "Sold Monday, January 1, 2024"
```
Sold\s+\w+,\s+(\w+)\s+(\d{1,2}),\s+(\d{4})
→ parse month from name → YYYY-MM-DD
```
Also handle: "Sold Jan 1, 2024" (no weekday), "Jan 1, 2024"

### Storage path
`bronze/listings/us/{item_id}.parquet`
`bronze/images/{card_id}_{card_version}_{item_id}.{ext}`

Each file is self-contained. Check by item_id if exists before re-scraping.

### Image download
- Extract thumbnail URL from HTML: `<img id="icImg"` → `data-old-hires` or `src` attribute
- Download image bytes (requests, timeout 10s)
- Detect format from Content-Type header (jpeg/png/webp)
- Upload to `bronze/images/{card_id}_{card_version}_{item_id}.{ext}`
- Write `image_path` column into the parquet entry
- If download fails: set `image_path=""` and continue

### Pagination strategy
- Page 1 = base URL
- Page N = `base_url&_pgn={N}`
- Stop when: page returns HTTP error, or <10 results, or 3 consecutive empty pages
- Track scraped pages in a state file: `data/scrape_state_us.json` (last page, seen item_ids)

## Out of Scope (Silver layer, not Bronze)
- Card_id validation against cardlist
- Currency normalization
- Deduplication across regions
- Pack/bundle filtering

## Implementation Notes
- Use `requests` or `httpx` + BeautifulSoup for scraping
- Rate limit: 1 request per second to avoid ban
- Retry on HTTP error: 3 attempts with backoff
- Store raw HTML as compressed bytes in parquet (snappy compression)
- Thumbnail extraction: `<img id="icImg"` or `data-old-hires` attribute
- MinIO client already in `tcg_platform.resources.minio_client.MinioClientResource`
- For each item: check if `bronze/listings/us/{item_id}.parquet` exists before scraping
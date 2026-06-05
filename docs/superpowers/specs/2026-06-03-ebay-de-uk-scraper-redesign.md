# eBay DE + UK Scraper Redesign — Design Spec

**Date:** 2026-06-03
**Status:** Draft (awaiting user review)
**Author:** opencode + user
**Supersedes:** M6-T2 (current shared `ebay.py` parser)

## Problem

The current `src/tcg_platform/scraping/ebay.py` uses a single region-parameterized parser
for both DE and UK. The assumption that one parser works for both regions is wrong:

1. **Search-page date format differs** — UK: `Sold  D Mon YYYY`, DE: `Verkauft  D. Mon YYYY`. The DE search page also has only URL-param `LH_Sold=1` text, no human-readable date per card.
2. **Item-page date format differs** — DE: `verkauft am D. Monat YYYY um HH:MM`, UK: no human-readable sold text (only JSON `endDate`).
3. **Price/currency parsing differs** — DE: comma decimal (1.234,56 EUR), UK: period decimal (1,234.56 GBP), different currency symbols.

**Outcome:** UK `sold_date` is 99% null because the parser expects a format that doesn't exist on UK item pages. The DE parser works but uses a fragile regex on text that may be removed in a UI update.

## Goals

- **Goal 1:** Extract `sold_date` from the search results page where it appears in the green "Sold D Mon YYYY" (UK) / "Verkauft D. Mon YYYY" (DE) span.
- **Goal 2:** One Zyte call per search page; one additional call per item only for price/currency/image.
- **Goal 3:** Each region's parser is independent — no shared abstraction between DE and UK.
- **Goal 4:** Idempotency preserved (re-running skips seen item_ids; one parquet per item_id).
- **Goal 5:** All existing tests still pass (Zyte retry tests, etc.).

## Non-Goals

- Currency normalization to EUR (silver-layer concern, deferred).
- Cardlist join (silver-layer concern, deferred).
- Scheduling/daily runs (M5-T2, deferred).
- Fixing the 2 pre-existing failing tests in `test_exchange_rate.py` (separate task).
- Scraping US, AU, or any other region.

## New Search URLs (per user spec)

**UK:**
```
https://www.ebay.co.uk/sch/i.html?_nkw=One+Piece+TCG+PSA+10&_sacat=0&_from=R40&LH_PrefLoc=1&Language=English&_dcat=183454&rt=nc&LH_Sold=1
```

**DE:**
```
https://www.ebay.de/sch/i.html?_nkw=One+Piece+TCG+PSA+10&_sacat=0&_from=R40&Sprache=Englisch&_dcat=183454&LH_PrefLoc=1&rt=nc&LH_Sold=1
```

Filter rationale: `_dcat=183454` (TCG category), `rt=nc` (sort newest), `LH_Sold=1` (sold only), `LH_PrefLoc=1` + language filter to ensure UK/DE local sellers.

## Architecture

```
ebay_de_sold_listings (asset)
   │
   ├── 1. fetch search page (1 Zyte call) ─── ebay_de_search.parse()
   │                                            returns: list[tuple[item_url, sold_date]]
   │
   ├── 2. for each new item_url:
   │       ├── fetch item page (1 Zyte call) ── ebay_de_item.parse()
   │       │                                     returns: list[PriceRecord]
   │       └── attach sold_date from step 1 to each PriceRecord
   │
   └── 3. inline write parquet + image to MinIO
       │
       └── returns list[PriceRecord] (with sold_date set)

   ↓
bronze_ebay_de_sqlite_writer (unchanged)
```

Same shape for `ebay_uk_sold_listings` with `ebay_uk_search` + `ebay_uk_item`.

## Components

### `src/tcg_platform/scraping/ebay_de_search.py` (NEW)

Region-specific search-page parser. No UK code, no shared logic.

**Exports:**
- `DE_SEARCH_URL` — the constant URL from above
- `parse_ebay_de_search_page(html: str, base_url: str = DE_SEARCH_URL) -> list[tuple[str, str]]`
  - Returns list of `(item_url, sold_date)` pairs
  - `sold_date` format: `YYYY-MM-DD` (or `""` if unparseable)
  - Dedupes by item_id within the page
  - Skips listings with no parseable item_id

**Parsing logic:**
- Item URL: `re.findall(r'href="(https://www\.ebay\.de/itm/\d+[^"]*)"', html)`
  - Strip `?*` query params: `re.sub(r"\?.*", "", url)`
  - Dedup by `re.search(r"/itm/(\d+)", url).group(1)`
- Sold date: extract from `<span aria-label="Verkauft D. Mon YYYY" class="su-styled-text positive default">Verkauft  D. Mon YYYY</span>` spans.
  - Each span sits inside a card; the card's item URL is in a sibling `<a class="s-card__link">`.
  - Pattern: find the parent card container (the closest ancestor `<li class="s-card">` or `<div class="s-card">`), extract the URL from `<a class="s-card__link">` and the date from the `Verkauft` span.
  - Regex on the date text: `r"Verkauft\s+(\d{1,2})\.\s+(\w+)\s+(\d{4})"`
  - Convert to `YYYY-MM-DD`
- Relative dates: handle `Heute` (today) and `Gestern` (yesterday) using `datetime.now().date()` and `.date() - timedelta(days=1)`.

### `src/tcg_platform/scraping/ebay_uk_search.py` (NEW)

Same shape, UK-specific:
- `UK_SEARCH_URL` constant
- `parse_ebay_uk_search_page(html, base_url=UK_SEARCH_URL) -> list[tuple[str, str]]`
- Item URL regex: `https://www\.ebay\.co\.uk/itm/\d+`
- Date regex: `r"Sold\s+(\d{1,2})\s+(\w+)\s+(\d{4})"` (no period after day)
- Relative dates: `Today` → today, `Yesterday` → yesterday

### `src/tcg_platform/scraping/ebay_de_item.py` (NEW)

Parses a single DE item page HTML into `PriceRecord` list.

**Exports:**
- `parse_ebay_de_item_page(html: str, item_url: str, scraped_at: datetime) -> list[PriceRecord]`

**Logic:**
- Price: `data-testid="x-price-primary"` span, then strip `[^\d,]` and replace `,` with `.`
- Currency: hardcoded `"EUR"` (URL is `.de`)
- Title: `<h1>...</h1>` regex; strip parentheses; normalize
- Card_id: same as before — extract from title using `(OP\d+|EB\d+|ST\d+|PRB\d+|P\d+)` regex
- Card_version: split on the set code; suffix becomes version
- Language: `JP` if "japan"/"japanese" in title; else `EN` (filter is `Sprache=Englisch` so most rows are EN)
- Proxy filter: skip titles containing `proxy`, `dummy`, `fake card`, `replica`
- Returns empty list if price is missing or proxy detected
- `sold_date` field is left empty in the returned record — the asset attaches it from search page

### `src/tcg_platform/scraping/ebay_uk_item.py` (NEW)

Same shape, UK-specific:
- Price regex: `£` strip instead of `€`; comma thousands + period decimal
- Currency: hardcoded `"GBP"`
- Title parsing: same logic, no special German handling

### `src/tcg_platform/defs/ebay_de_sold_listings.py` (REWRITTEN)

```python
@dg.asset(required_resource_keys={"zyte_session_resource", "sqlite_client_de", "minio_client"})
def ebay_de_sold_listings(context):
    zyte = context.resources.zyte_session_resource
    sqlite = context.resources.sqlite_client_de
    minio = context.resources.minio_client

    seen = sqlite.get_seen_ebay_item_ids()
    # 1. Fetch search page
    resp = zyte.get({"url": DE_SEARCH_URL, "browserHtml": True})
    if resp.get("statusCode") != 200: return []
    # 2. Parse URLs + dates from search
    pairs = parse_ebay_de_search_page(resp["browserHtml"])
    # 3. For each new item: fetch item page, parse, attach date
    records = []
    for item_url, sold_date in pairs:
        item_id = _extract_item_id(item_url)
        if item_id in seen: continue
        try:
            item_resp = zyte.get({"url": item_url, "browserHtml": True})
            if item_resp.get("statusCode") != 200: continue
            parsed = parse_ebay_de_item_page(
                item_resp["browserHtml"], item_url, datetime.now(timezone.utc)
            )
            # Image + MinIO write as before
            ...
            # Attach the search-page sold_date
            for rec in parsed:
                rec.sold_date = sold_date or None
            records.extend(parsed)
        except Exception as e:
            context.log.warning(f"Failed {item_url}: {e}")
    return records
```

### `src/tcg_platform/defs/ebay_uk_sold_listings.py` (REWRITTEN)

Mirror of DE with UK parsers.

### `src/tcg_platform/scraping/ebay.py` (DELETED)

The user explicitly said "no abstraction." After DE and UK assets are migrated, the entire file is removed. This includes:
- `EBAY_REGION_CONFIGS` (no longer used)
- `scrape_ebay_listings()` (no longer used)
- `parse_ebay_item_page()` (replaced by per-region parsers)
- `parse_ebay_listings_page()` (replaced by per-region search parsers)
- `_parse_date()`, `_DATE_RE_GERMAN`, `_DATE_RE_ENGLISH` (replaced)
- `_extract_item_id()` (helper, moved to a shared util if needed)

`_ITEM_ID_RE` is used in two places (`ebay_de_sold_listings.py`, `ebay_uk_sold_listings.py`). Move it to a tiny shared util `src/tcg_platform/scraping/ebay_item_id.py` so the two new files don't duplicate the regex.

## Data flow

```
ebay_de_sold_listings
  │
  ├── zyte.get(DE_SEARCH_URL) → HTML_de
  ├── parse_ebay_de_search_page(HTML_de) → [(url1, "2026-06-03"), (url2, "2026-06-02"), ...]
  │
  ├── for (url, date) where item_id not in seen:
  │     ├── zyte.get(url) → HTML_item
  │     ├── parse_ebay_de_item_page(HTML_item, url, now) → [PriceRecord]
  │     ├── (image download + MinIO put, unchanged)
  │     ├── rec.sold_date = date
  │     └── records.append(rec)
  │
  └── return records
       ↓
bronze_ebay_de_sqlite_writer (unchanged): INSERT OR IGNORE into fact_events
```

## Error handling

- Search page 4xx/5xx: log warning, return `[]` (asset completes empty)
- Item page 4xx/5xx: skip that item, log warning, continue
- No item URLs in search: log "no items found", return `[]`
- `sold_date` unparseable: store as `None` in record (the search-page parser falls back to `""` if the regex misses)
- Proxy/fake card detected: filter in item-page parser, return `[]`
- Zyte transient error: existing `ZyteSessionResource` retry (3×) covers this

## Testing strategy

TDD throughout. Test files under `tests/scraping/`:

1. `tests/scraping/test_ebay_de_search.py`:
   - `test_parse_extracts_urls_and_dates` — 5-item fixture, expect 5 pairs
   - `test_handles_today_yesterday` — German relative dates
   - `test_dedupes_same_item_id`
   - `test_skips_urls_without_item_id`

2. `tests/scraping/test_ebay_uk_search.py`:
   - Mirror of DE tests with English fixture

3. `tests/scraping/test_ebay_de_item.py`:
   - `test_parse_extracts_price_eur` — comma decimal
   - `test_parse_handles_proxy_title` — returns `[]`
   - `test_parse_extracts_card_id_from_title`
   - `test_parse_returns_empty_on_no_price`

4. `tests/scraping/test_ebay_uk_item.py`:
   - Mirror with £/GBP

5. Update `tests/defs/test_eu_pipeline_orchestrator.py`:
   - The `ebay_de_job` and `ebay_uk_job` job definitions stay; tests don't change.
   - Add: `test_ebay_de_pipeline_still_runs` — smoke test that the defs load.

The existing `tests/defs/test_zyte_session_resource.py` (5 tests) and `tests/scraping/test_extract_item_image.py` (1 test) stay unchanged.

## Files

**Create:**
- `src/tcg_platform/scraping/ebay_de_search.py`
- `src/tcg_platform/scraping/ebay_uk_search.py`
- `src/tcg_platform/scraping/ebay_de_item.py`
- `src/tcg_platform/scraping/ebay_uk_item.py`
- `src/tcg_platform/scraping/ebay_item_id.py` (shared `_ITEM_ID_RE` only)
- `tests/scraping/test_ebay_de_search.py`
- `tests/scraping/test_ebay_uk_search.py`
- `tests/scraping/test_ebay_de_item.py`
- `tests/scraping/test_ebay_uk_item.py`

**Modify:**
- `src/tcg_platform/defs/ebay_de_sold_listings.py` (full rewrite)
- `src/tcg_platform/defs/ebay_uk_sold_listings.py` (full rewrite)
- `PROD.md` (mark M6-T2 superseded, add M6.5-T1 for this redesign)

**Delete:**
- `src/tcg_platform/scraping/ebay.py` (entire file, including `EBAY_REGION_CONFIGS` and the `_parse_english_date` shim)

## Success criteria

1. `python -c "from tcg_platform.definitions import defs; print('OK')"` passes
2. `pytest tests/scraping/test_ebay_de_search.py tests/scraping/test_ebay_uk_search.py tests/scraping/test_ebay_de_item.py tests/scraping/test_ebay_uk_item.py` — all pass
3. `pytest tests/` — all 14 currently-passing tests still pass + 4 new test files (≥10 new tests) pass
4. After running `ebay_de_sold_listings` and `ebay_uk_sold_listings` via `dg dev`, sample 10 random rows from each SQLite DB and verify:
   - `sold_date` is non-null in ≥80% of UK rows (was 1%)
   - `sold_date` is non-null in ≥80% of DE rows (was 46%)
   - `currency` matches region (EUR for DE, GBP for UK)
5. New `ebay.py` file does not exist; `EBAY_REGION_CONFIGS` is gone.
6. Two existing failing tests in `test_exchange_rate.py` are out of scope — not part of this redesign.

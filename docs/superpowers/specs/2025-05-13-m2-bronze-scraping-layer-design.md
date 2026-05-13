# M2: Bronze Scraping Layer — Design Spec

**Date:** 2025-05-13
**Status:** Approved by user

## Scope

Build three bronze scraping pipelines (Dagster assets) that scrape raw data from PriceCharting and Limitless TCG using Steel.cloud `scrape()` API + BeautifulSoup. eBay deferred to M6 (requires browser session).

## Data Sources

| Website | URL | Method | Bronze Output |
|---------|-----|--------|---------------|
| PriceCharting | `https://www.pricecharting.com/category/one-piece-cards` | `scrape()` + BS4 | `fact_events` (price_update) |
| Limitless TCG | `https://onepiece.limitlesstcg.com/cards` | `scrape()` + BS4 | `cardlist_dimension` + images |
| eBay DE | (deferred) | — | — |

## Adaptive Strategy

- **Static HTML** (PriceCharting, Limitless TCG) → Steel `scrape()` → BeautifulSoup → structured data
- **Dynamic JS** (eBay) → Steel browser session via WebSocket → deferred to M6

## Limitless TCG Pipeline (M2-T2)

**Output:** `cardlist_dimension` entries + card images to MinIO

**Card fields extracted:**
- `card_id` — from card ID/URL (e.g., `OP01-001`)
- `card_version` — version variant (e.g., `BaseArt`, `AltArt`, `v1`, `v2`). NULL if unversioned.
- `card_name` — display name
- `set_code` — set abbreviation (e.g., `OP01`)
- `rarity` — e.g., `Leader`, `Character`, `Event`, `Stage`
- `card_type` — type field
- `attribute` — element/attribute
- `power` — power stat (INT, nullable)
- `cost` — cost to play (INT, nullable)
- `color` — card color
- `source_url` — `https://onepiece.limitlesstcg.com/cards`
- `scraped_at` — timestamp

**Image download:**
- Source: card image URL from page
- MinIO path: `cards/{set_code}/{card_id}_{card_version}.jpg` (e.g., `cards/OP01/OP01-001_BaseArt.jpg`)
- Version included in path to distinguish AltArt vs BaseArt variants
- On duplicate card_id+version: don't re-download (check MinIO object exists first)

## PriceCharting Pipeline (M2-T1)

**Output:** `fact_events` entries

**Fields extracted:**
- `card_id` — matched to Limitless TCG catalog
- `card_version` — version variant (e.g., `BaseArt`, `AltArt`, `v1`, `v2`). NULL if unversioned.
- `event_type` — `'price_update'`
- `price` — price value (numeric)
- `currency` — `'USD'` or `'EUR'` (Germany source)
- `sold_date` — `NULL` (price update, not a sale)
- `scraped_from` — `'pricecharting'`
- `source` — `'US'` or `'Germany'` (from which region the price is sourced)
- `source_url` — `https://www.pricecharting.com/category/one-piece-cards`
- `scraped_at` — timestamp

**Card matching:** PriceCharting card names must be matched to Limitless TCG `card_id`. Strategy: fuzzy string match on `card_name` after normalizing (lowercase, remove special chars).

## Idempotency

- Check `(card_id, source_url, scraped_at_date)` before upsert
- Images: check MinIO object exists before downloading
- No duplicate inserts on re-run

## Dependencies

- Steel `scrape()` for HTTP fetch
- BeautifulSoup + lxml for HTML parsing
- `python-dotenv` for `.env` loading
- `requests` for image downloads (or MinIO `fput_object`)

## M2 Task Mapping

| Task | Asset | Output |
|------|-------|--------|
| M2-T1 | `scrape_pricecharting` | `fact_events` records |
| M2-T2 | `scrape_limitlesstcg` | `cardlist_dimension` records |
| M2-T3 | `download_limitlesstcg_images` | MinIO card images |

## Open Issues

- **WebSocket (M6):** `wss://connect.steel.dev` returns 502. eBay deferred until resolved.
- **Card matching:** PriceCharting → Limitless TCG name mapping needs fuzzy match fallback.
- **MinIO/SQLite (M3/M4):** Must be built before M2 assets can write data. Order: M2 → M3 → M4.
import dagster as dg

from datetime import datetime, timezone

from tcg_platform.scraping.ebay import (
    parse_ebay_item_page,
    scrape_ebay_listings,
)


@dg.asset(
    required_resource_keys={"zyte_client", "sqlite_client_uk"},
)
def ebay_uk_sold_listings(context: dg.AssetExecutionContext) -> list:
    """Scrape NEW sold One Piece TCG listings from eBay UK since last run."""
    zyte_client = context.resources.zyte_client
    sqlite_client = context.resources.sqlite_client_uk

    already_seen = sqlite_client.get_seen_ebay_item_ids()
    context.log.info(f"Known item IDs in UK DB: {len(already_seen)}")

    new_item_urls = []
    for item_url in scrape_ebay_listings(zyte_client, "UK", already_seen):
        new_item_urls.append(item_url)

    context.log.info(f"New UK items to scrape: {len(new_item_urls)}")

    if not new_item_urls:
        context.log.info("No new UK items found")
        return []

    records = []
    scraped_at = datetime.now(timezone.utc)

    for item_url in new_item_urls:
        try:
            resp = zyte_client.get({"url": item_url, "browserHtml": True})
            if resp.get("statusCode") != 200:
                continue
            html = resp.get("browserHtml", "")
            if not html:
                continue
            parsed = parse_ebay_item_page(html, item_url, scraped_at, "UK")
            records.extend(parsed)
        except Exception as e:
            context.log.warning(f"Failed to scrape {item_url}: {e}")
            continue

    context.log.info(f"Scraped {len(records)} new UK sold listing records")
    return records
import dagster as dg

from tcg_platform.scraping.limitlesstcg import (
    LIMITLESS_OP_BASE,
    _get_all_sets,
    extract_card_links_from_set_page,
)
from playwright.sync_api import sync_playwright


@dg.asset
def discover_limitless_catalog(
    context: dg.AssetExecutionContext,
) -> list[tuple[str, str, int | None]]:
    """Scrape Limitless TCG and return a list of (set_code, card_id, variant).

    Discovers all sets dynamically from /cards/ (not hardcoded). For each set,
    scrapes the set page and extracts base cards + ?v=N variant links. Output
    is a flat list of tuples; no MinIO writes.
    """
    catalog: list[tuple[str, str, int | None]] = []
    seen: set[tuple[str, str, int | None]] = set()

    sets = _get_all_sets()
    context.log.info(f"Discovered {len(sets)} sets on Limitless")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for set_code, set_path in sets:
            try:
                page = browser.new_page()
                page.goto(f"{LIMITLESS_OP_BASE}{set_path}", timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
                html = page.content()
                page.close()
            except Exception as e:
                context.log.warning(f"Failed to scrape set {set_code} ({set_path}): {e}; skipping")
                continue

            for card_id, variant in extract_card_links_from_set_page(html):
                key = (set_code, card_id, variant)
                if key not in seen:
                    seen.add(key)
                    catalog.append(key)

        browser.close()

    if not catalog:
        raise RuntimeError(
            "Discovery returned 0 cards. Limitless HTML may have changed; "
            "check extract_card_links_from_set_page and _get_all_sets in "
            "src/tcg_platform/scraping/limitlesstcg.py"
        )

    context.log.info(f"Discovery complete: {len(catalog)} (set, card, variant) tuples across {len(sets)} sets")
    return catalog

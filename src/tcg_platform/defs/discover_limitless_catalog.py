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
) -> list[tuple[str, int | None]]:
    """Scrape Limitless TCG and return a list of (card_id, variant).

    Discovers all sets dynamically from /cards/ (not hardcoded). For each set,
    scrapes the set page and extracts base cards + ?v=N variant links. The
    set_code is NOT included in the tuple — the home set is encoded in the
    card_id prefix (e.g. OP05-001 -> OP05), and the sync helpers derive the
    set_code from the card_id. Including a separate set_code caused
    cross-set reprints (e.g. ST15-005_p4 listed on the OP16 set page) to be
    fetched from the wrong CDN path and 403.

    Phantom card_ids (e.g. OP16-THE-TIME-OF-BATTLE, set-name slugs that
    Limitless uses as set-page aliases) are filtered out at the
    extract_card_links_from_set_page layer.
    """
    catalog: list[tuple[str, int | None]] = []
    seen: set[tuple[str, int | None]] = set()

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
                key = (card_id, variant)
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

    context.log.info(f"Discovery complete: {len(catalog)} (card, variant) tuples across {len(sets)} sets")
    return catalog

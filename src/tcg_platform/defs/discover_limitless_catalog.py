import dagster as dg
import requests

from tcg_platform.scraping.limitlesstcg import (
    LIMITLESS_OP_BASE,
    _get_all_sets,
    extract_card_links_from_set_page,
    extract_promo_subpages,
)
from playwright.sync_api import sync_playwright


def _fetch_promo_subpage_html(slug: str, timeout: int = 30) -> str | None:
    """Fetch a promo sub-page via plain HTTP. The sub-pages are server-rendered
    (no JS required), so requests is faster and more reliable than Playwright
    for the bulk walk of ~77 pages."""
    try:
        r = requests.get(
            f"{LIMITLESS_OP_BASE}/cards/{slug}",
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        return r.text
    except Exception:
        return None


@dg.asset
def discover_limitless_catalog(
    context: dg.AssetExecutionContext,
) -> list[tuple[str, int | None]]:
    """Scrape Limitless TCG and return a list of (card_id, variant).

    Discovers cards from two sources:

    1. **Standard set list** — walks every set on /cards/ (OP01–OP16, EB01–04,
       ST01–30, PRB01–02, P) via Playwright (the /cards/ index uses JS) and
       scrapes each set page for base cards + ?v=N variant links.
    2. **Promo sub-pages** — walks /cards/promos (an index of ~77
       tournament/event/regional/championship/dash/gift/misc sub-pages)
       and scrapes each sub-page via plain HTTP. Each sub-page lists
       cards from multiple sets mixed together (e.g. OP05, P, OP14 all on
       one page) with ?v=N printings. These are net-new to MinIO — they
       don't appear on the standard set pages.

    The set_code is NOT included in the tuple — the home set is encoded in
    the card_id prefix (e.g. OP05-001 -> OP05), and the sync helpers
    derive the set_code from the card_id. Including a separate set_code
    caused cross-set reprints (e.g. ST15-005_p4 listed on the OP16 set
    page) to be fetched from the wrong CDN path and 403.

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

        # Fetch the /cards/promos index with Playwright (the index page is
        # JS-rendered) just to enumerate the sub-page slugs.
        try:
            page = browser.new_page()
            page.goto(f"{LIMITLESS_OP_BASE}/cards/promos", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=30000)
            promos_html = page.content()
            page.close()
        except Exception as e:
            context.log.warning(f"Failed to scrape /cards/promos: {e}; skipping promos")
            promos_html = ""

        promo_subpages = extract_promo_subpages(promos_html)
        context.log.info(f"Discovered {len(promo_subpages)} promo sub-pages")

        browser.close()

    # Walk each promo sub-page via plain HTTP (server-rendered, faster than
    # Playwright for the bulk walk). The HTTP path here is a different
    # process concern from the per-card CDN fetches in sync_card_images.
    promo_added = 0
    for slug in promo_subpages:
        sub_html = _fetch_promo_subpage_html(slug)
        if sub_html is None:
            context.log.warning(f"Failed to scrape promo sub-page {slug}; skipping")
            continue
        for card_id, variant in extract_card_links_from_set_page(sub_html):
            key = (card_id, variant)
            if key not in seen:
                seen.add(key)
                catalog.append(key)
                promo_added += 1

    if not catalog:
        raise RuntimeError(
            "Discovery returned 0 cards. Limitless HTML may have changed; "
            "check extract_card_links_from_set_page and _get_all_sets in "
            "src/tcg_platform/scraping/limitlesstcg.py"
        )

    context.log.info(
        f"Discovery complete: {len(catalog)} (card, variant) tuples "
        f"({promo_added} new from {len(promo_subpages)} promo sub-pages, "
        f"rest from {len(sets)} standard sets)"
    )
    return catalog

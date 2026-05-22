import dagster as dg
from dagster import AssetOut

from datetime import datetime, timezone

from tcg_platform.scraping.limitlesstcg import scrape_limitless_op


@dg.multi_asset(
    outs={
        "limitless_op_cards": AssetOut(),
        "limitless_op_prices": AssetOut(),
    },
)
def limitless_op_cards(context: dg.AssetExecutionContext):
    """Scrape One Piece TCG card catalog from Limitless TCG."""
    context.log.info("Starting Limitless OP card scrape")

    cards, prices = scrape_limitless_op()

    context.log.info(f"Scraped {len(cards)} cards and {len(prices)} price records")
    return {"limitless_op_cards": cards, "limitless_op_prices": prices}
"""Network-only eBay scraper. Writes raw HTML + images + logs to tcg-raw.

The transformer (`transform_bronze.py`) reads from tcg-raw and writes
the structured bronze layer. The scraper does not know what a
card_id is; it only deals with event_id (the eBay item id from the URL).
"""
import logging
from datetime import datetime, timezone
from typing import NamedTuple

import dagster as dg
import requests
from minio.error import S3Error

from tcg_platform.scraping.ebay_de_search import (
    parse_ebay_de_search_page,
    search_url_for_page as de_search_url_for_page,
)
from tcg_platform.scraping.ebay_uk_search import (
    parse_ebay_uk_search_page,
    search_url_for_page as uk_search_url_for_page,
)
from tcg_platform.scraping.ebay_utils import (
    extract_item_id,
    extract_item_image_url,
)

_LOG = logging.getLogger(__name__)

RAW_BUCKET = "tcg-raw"
EMPTY_STREAK_THRESHOLD = 5


class WrittenItem(NamedTuple):
    event_id: str
    region: str  # "DE" or "UK"
    sold_date: str | None  # YYYY-MM-DD, or None if the search page didn't show it


def _exists_in_raw(minio_client, region: str, event_id: str) -> bool:
    """Atomic existence check against tcg-raw/ebay/{region}/{event_id}.html.

    Returns True if the object is present, False otherwise (including
    on unexpected errors — refetching is safer than silently skipping
    on a transient failure).
    """
    try:
        minio_client.client.stat_object(
            RAW_BUCKET, f"ebay/{region}/{event_id}.html"
        )
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject"):
            return False
        _LOG.warning(
            f"stat_object unexpected error for {region}/{event_id}: {e}"
        )
        return False
    except Exception as e:
        _LOG.warning(f"stat_object error for {region}/{event_id}: {e}")
        return False

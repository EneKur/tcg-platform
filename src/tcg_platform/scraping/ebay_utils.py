"""Shared eBay utilities used by both DE and UK scrapers."""
import re

ITEM_ID_RE = re.compile(r"/itm/(\d+)")

# eBay item pages embed the main image URL as a JSON-style field.
IMAGE_RE = re.compile(r'"image":"(https://i\.ebayimg\.com/[^"]+)"')


def extract_item_id(url: str) -> str:
    """Extract the eBay item_id from an item URL.

    Returns the matched digits, or the original URL unchanged if no match.
    """
    m = ITEM_ID_RE.search(url)
    return m.group(1) if m else url


def extract_item_image_url(html: str) -> str | None:
    """Extract the main image URL from an eBay item page HTML.

    Returns the URL, or None if not found.
    """
    m = IMAGE_RE.search(html)
    return m.group(1) if m else None

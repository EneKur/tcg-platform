from tcg_platform.scraping.ebay_utils import (
    extract_item_id,
    extract_item_image_url,
)


def test_extract_item_id_de():
    assert extract_item_id("https://www.ebay.de/itm/123456789") == "123456789"


def test_extract_item_id_uk_with_query():
    assert extract_item_id(
        "https://www.ebay.co.uk/itm/987654321?_skw=foo&itmmeta=01ABC"
    ) == "987654321"


def test_extract_item_id_returns_url_when_no_match():
    # No /itm/ in URL — return the URL unchanged (caller's signal to skip)
    assert extract_item_id("https://example.com/foo") == "https://example.com/foo"


def test_extract_item_image_url_found():
    html = '{"image":"https://i.ebayimg.com/images/something.jpg"}'
    assert extract_item_image_url(html) == "https://i.ebayimg.com/images/something.jpg"


def test_extract_item_image_url_missing():
    assert extract_item_image_url("no image here") is None

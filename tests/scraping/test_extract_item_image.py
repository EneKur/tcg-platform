import pytest
from tcg_platform.scraping.ebay_utils import extract_item_image_url


def test_extract_item_image_url():
    html = '{"image":"https://i.ebayimg.com/images/something.jpg"}'
    url = extract_item_image_url(html)
    assert url == "https://i.ebayimg.com/images/something.jpg"

    empty_html = '{"other":"data"}'
    url = extract_item_image_url(empty_html)
    assert url is None
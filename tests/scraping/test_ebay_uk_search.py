from datetime import datetime, timedelta
from pathlib import Path

from tcg_platform.scraping.ebay_uk_search import parse_ebay_uk_search_page


FIXTURE = Path(__file__).parent.parent / "fixtures" / "ebay_uk_search_sample.html"


def test_returns_list_of_url_date_pairs():
    html = FIXTURE.read_text(encoding="utf-8")
    pairs = parse_ebay_uk_search_page(html)
    assert isinstance(pairs, list)
    assert len(pairs) >= 1
    for url, date in pairs:
        assert url.startswith("https://www.ebay.co.uk/itm/")
        assert date == "" or (len(date) == 10 and date[4] == "-" and date[7] == "-")


def test_extracts_sold_date_with_english_format():
    # Hand-crafted fixture: one card with "Sold 3 Jun 2026" (no period after day)
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Sold item">Sold  3 Jun 2026</span>
      <a class="s-card__link" href="https://www.ebay.co.uk/itm/111111111?hash=item1"></a>
    </li>
    """
    pairs = parse_ebay_uk_search_page(html)
    assert pairs == [("https://www.ebay.co.uk/itm/111111111", "2026-06-03")]


def test_handles_today():
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Sold item">Sold  Today</span>
      <a class="s-card__link" href="https://www.ebay.co.uk/itm/222222222?hash=item2"></a>
    </li>
    """
    pairs = parse_ebay_uk_search_page(html)
    assert len(pairs) == 1
    expected_today = datetime.now().date().strftime("%Y-%m-%d")
    assert pairs[0][1] == expected_today


def test_handles_yesterday():
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Sold item">Sold  Yesterday</span>
      <a class="s-card__link" href="https://www.ebay.co.uk/itm/333333333?hash=item3"></a>
    </li>
    """
    pairs = parse_ebay_uk_search_page(html)
    expected_yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    assert pairs[0][1] == expected_yesterday


def test_dedupes_same_item_id():
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Sold item">Sold  1 Jun 2026</span>
      <a class="s-card__link" href="https://www.ebay.co.uk/itm/444444444?hash=item4a"></a>
    </li>
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Sold item">Sold  1 Jun 2026</span>
      <a class="s-card__link" href="https://www.ebay.co.uk/itm/444444444?hash=item4b"></a>
    </li>
    """
    pairs = parse_ebay_uk_search_page(html)
    assert len(pairs) == 1


def test_empty_html_returns_empty_list():
    assert parse_ebay_uk_search_page("") == []
    assert parse_ebay_uk_search_page("<html><body></body></html>") == []

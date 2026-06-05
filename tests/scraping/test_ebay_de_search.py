from datetime import datetime, timedelta
from pathlib import Path

from tcg_platform.scraping.ebay_de_search import parse_ebay_de_search_page


FIXTURE = Path(__file__).parent.parent / "fixtures" / "ebay_de_search_sample.html"


def test_returns_list_of_url_date_pairs():
    html = FIXTURE.read_text(encoding="utf-8")
    pairs = parse_ebay_de_search_page(html)
    assert isinstance(pairs, list)
    assert len(pairs) >= 1
    for item in pairs:
        assert len(item) == 2
        url, date = item
        assert url.startswith("https://www.ebay.de/itm/")
        # date is YYYY-MM-DD or "" (unparseable)
        assert date == "" or (len(date) == 10 and date[4] == "-" and date[7] == "-")


def test_extracts_sold_date_with_german_format():
    # Hand-crafted minimal fixture: one card with "Verkauft 3. Jun 2026"
    # URL comes after the date span (real DOM order: image-link, then content
    # block containing the date span and the title-link).
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Verkaufter Artikel">Verkauft  3. Jun 2026</span>
      <a class="s-card__link" href="https://www.ebay.de/itm/111111111?hash=item1"></a>
    </li>
    """
    pairs = parse_ebay_de_search_page(html)
    assert pairs == [("https://www.ebay.de/itm/111111111", "2026-06-03")]


def test_handles_heute_as_today():
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Verkaufter Artikel">Verkauft  Heute</span>
      <a class="s-card__link" href="https://www.ebay.de/itm/222222222?hash=item2"></a>
    </li>
    """
    pairs = parse_ebay_de_search_page(html)
    assert len(pairs) == 1
    assert pairs[0][0] == "https://www.ebay.de/itm/222222222"
    expected_today = datetime.now().date().strftime("%Y-%m-%d")
    assert pairs[0][1] == expected_today


def test_handles_gestern_as_yesterday():
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Verkaufter Artikel">Verkauft  Gestern</span>
      <a class="s-card__link" href="https://www.ebay.de/itm/333333333?hash=item3"></a>
    </li>
    """
    pairs = parse_ebay_de_search_page(html)
    assert len(pairs) == 1
    expected_yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    assert pairs[0][1] == expected_yesterday


def test_dedupes_same_item_id():
    html = """
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Verkaufter Artikel">Verkauft  1. Jun 2026</span>
      <a class="s-card__link" href="https://www.ebay.de/itm/444444444?hash=item4a"></a>
    </li>
    <li class="s-card">
      <span class="su-styled-text positive default" aria-label="Verkaufter Artikel">Verkauft  1. Jun 2026</span>
      <a class="s-card__link" href="https://www.ebay.de/itm/444444444?hash=item4b"></a>
    </li>
    """
    pairs = parse_ebay_de_search_page(html)
    # Same item_id 444444444 appears twice in the page — should be deduped
    assert len(pairs) == 1
    assert pairs[0][0] == "https://www.ebay.de/itm/444444444"


def test_empty_html_returns_empty_list():
    assert parse_ebay_de_search_page("") == []
    assert parse_ebay_de_search_page("<html><body></body></html>") == []

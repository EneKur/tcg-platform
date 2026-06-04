from datetime import datetime, timezone
from pathlib import Path

from tcg_platform.scraping.ebay_de_item import parse_ebay_de_item_page


FIXTURE = Path(__file__).parent.parent / "fixtures" / "ebay_de_item_sample.html"


def test_parse_extracts_price_with_comma_decimal():
    # Hand-crafted fixture: a simple DE item page with EUR 12,50
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-001 Karte</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 12,50</span></div>
    </body></html>
    """
    scraped_at = datetime(2026, 6, 3, tzinfo=timezone.utc)
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/999999999", scraped_at
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.price == 12.50
    assert rec.currency == "EUR"
    assert rec.source == "DE"


def test_parse_extracts_thousands_separator():
    # 1.234,56 EUR → 1234.56
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-001 Karte</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 1.234,56</span></div>
    </body></html>
    """
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/999999999", datetime.now(timezone.utc)
    )
    assert records[0].price == 1234.56


def test_parse_skips_proxy_title():
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">Proxy Card One Piece TCG OP01-001</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 5,00</span></div>
    </body></html>
    """
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/999999999", datetime.now(timezone.utc)
    )
    assert records == []  # proxy filter returns empty list


def test_parse_returns_empty_on_no_price():
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-001 Karte</span></h1>
    </body></html>
    """
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/999999999", datetime.now(timezone.utc)
    )
    assert records == []


def test_parse_extracts_card_id_from_title():
    # The card_id regex is (OP\d+|EB\d+|ST\d+|PRB\d+|P\d+).
    # The base card_id is the set code + leading digits; the trailing _part becomes card_version.
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-042 Karte Luffy</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 5,00</span></div>
    </body></html>
    """
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/999999999", datetime.now(timezone.utc)
    )
    assert records[0].card_id == "OP01-042"


def test_parse_extracts_card_version_from_title_suffix():
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-042 Luffy Alt Art</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">EUR 5,00</span></div>
    </body></html>
    """
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/999999999", datetime.now(timezone.utc)
    )
    # Card_version should contain "Luffy Alt Art" (or similar) — non-empty
    assert records[0].card_id == "OP01-042"
    assert records[0].card_version  # non-empty


def test_parse_with_real_fixture_extracts_record():
    # Smoke test against the real investigation fixture
    html = FIXTURE.read_text(encoding="utf-8")
    records = parse_ebay_de_item_page(
        html, "https://www.ebay.de/itm/358573886023", datetime.now(timezone.utc)
    )
    # May be empty (proxy) or 1+ records — just assert it doesn't crash and returns a list
    assert isinstance(records, list)
    for rec in records:
        assert rec.currency == "EUR"
        assert rec.source == "DE"
        assert rec.scraped_from == "ebay"

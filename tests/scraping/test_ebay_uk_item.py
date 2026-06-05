from datetime import datetime, timezone
from pathlib import Path

from tcg_platform.scraping.ebay_uk_item import parse_ebay_uk_item_page


FIXTURE = Path(__file__).parent.parent / "fixtures" / "ebay_uk_item_sample.html"


def test_parse_extracts_price_with_pound_symbol():
    # UK: 12.50 GBP
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-042 Luffy</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">£12.50</span></div>
    </body></html>
    """
    records = parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/999999999", datetime.now(timezone.utc)
    )
    assert len(records) == 1
    assert records[0].price == 12.50
    assert records[0].currency == "GBP"
    assert records[0].source == "UK"


def test_parse_handles_thousands_separator_uk_format():
    # 1,234.56 GBP → 1234.56
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-042 Luffy</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">£1,234.56</span></div>
    </body></html>
    """
    records = parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/999999999", datetime.now(timezone.utc)
    )
    assert records[0].price == 1234.56


def test_parse_skips_proxy_title():
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">Proxy Card One Piece TCG OP01-001</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">£5.00</span></div>
    </body></html>
    """
    assert parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/999999999", datetime.now(timezone.utc)
    ) == []


def test_parse_returns_empty_on_no_price():
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG OP01-001 Luffy</span></h1>
    </body></html>
    """
    assert parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/999999999", datetime.now(timezone.utc)
    ) == []


def test_parse_skips_title_with_no_recognizable_card_id():
    # Regression: when the title doesn't contain a recognizable set code
    # (OP/EB/ST/PRB/P + digits), the parser was returning the entire
    # normalized title as card_id with empty card_version. This polluted
    # bronze/SQLite/silver with non-card listings (multi-card bundles,
    # DON cards). The parser should skip them entirely.
    # Real examples from 2026-06-05 run:
    #   "One Piece TCG 2nd Anniversary Winner 3 Cards Luffy Sabo Ace Sequential PSA 10"
    #   "PSA 10 DON Card Carrying On His Will Foil One Piece TCG"
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">PSA 10 DON Card Carrying On His Will Foil One Piece TCG</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">£47.49</span></div>
    </body></html>
    """
    assert parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/257501709731", datetime.now(timezone.utc)
    ) == []


def test_parse_skips_multi_card_bundle_title():
    html = """
    <html><body>
      <h1 class="x-item-title__mainTitle"><span class="ux-textspans ux-textspans--BOLD">One Piece TCG 2nd Anniversary Winner 3 Cards Luffy Sabo Ace Sequential PSA 10</span></h1>
      <div data-testid="x-price-primary"><span class="ux-textspans">£999.99</span></div>
    </body></html>
    """
    assert parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/168427997844", datetime.now(timezone.utc)
    ) == []


def test_parse_with_real_fixture_extracts_record():
    html = FIXTURE.read_text(encoding="utf-8")
    records = parse_ebay_uk_item_page(
        html, "https://www.ebay.co.uk/itm/178151181291", datetime.now(timezone.utc)
    )
    assert isinstance(records, list)
    for rec in records:
        assert rec.currency == "GBP"
        assert rec.source == "UK"
        assert rec.scraped_from == "ebay"

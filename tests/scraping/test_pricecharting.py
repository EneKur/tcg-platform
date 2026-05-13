import pytest

from tcg_platform.scraping.pricecharting import parse_pricecharting_html, _normalize_card_id


def test_parse_extracts_rows():
    html = """
    <tr><td></td><td>DON!! Card</td><td>One Piece Japanese Promo</td><td>$13.40</td><td></td><td></td></tr>
    <tr><td></td><td>One CH020</td><td>One Piece The Quest Begins</td><td>$4.85</td><td></td><td></td></tr>
    """
    records = parse_pricecharting_html(html)
    assert len(records) == 2
    assert records[0].price == 13.40
    assert records[0].currency == "USD"
    assert records[0].event_type == "price_update"


def test_parse_handles_empty_html():
    records = parse_pricecharting_html("<html><body></body></html>")
    assert records == []


def test_normalize_card_id():
    assert _normalize_card_id("DON!! Card") == "DON_Card"
    assert _normalize_card_id("One CH020") == "One_CH020"


def test_parse_high_price():
    html = """
    <tr><td></td><td>Test Card</td><td>OP01</td><td>$5.00</td><td></td><td>$15.50</td></tr>
    """
    records = parse_pricecharting_html(html)
    assert len(records) == 2
    prices = [r.price for r in records]
    assert 5.00 in prices
    assert 15.50 in prices
import re

def test_card_id_extraction():
    from tcg_platform.scraping.ebay_bronze import extract_card_id

    cases = [
        ("One Piece TCG OP15-001 Alternative Art", "OP15-001"),
        ("One Piece OP09119 Luffy", "OP09119"),
        ("EB01-001 Black Card", "EB01-001"),
        ("ST03013 Starter Deck", "ST03013"),
        ("14x One Piece TCG OP15", "OP15"),
        ("One Piece TCG PRB01 Promo", "PRB01"),
        ("Random Bundle Not a Card", ""),
        ("OPCG Charlotte Linlin", ""),
        ("Borsalino OP15", "OP15"),
        ("OP15-001 (Alternative Art)", "OP15-001"),
    ]

    for title, expected in cases:
        result = extract_card_id(title)
        assert result == expected, f"Title: {title!r} → got {result!r}, expected {expected!r}"


def test_currency_detection():
    from tcg_platform.scraping.ebay_bronze import detect_currency

    cases = [
        ("$12.99", "https://www.ebay.com/itm/123", "USD"),
        ("A$45.00", "https://www.ebay.com/itm/123", "AUD"),
        ("£9.99", "https://www.ebay.co.uk/itm/123", "GBP"),
        ("€15.00", "https://www.ebay.de/itm/123", "EUR"),
        ("C$25.00", "https://www.ebay.ca/itm/123", "CAD"),
        ("$100.00", "https://www.ebay.com.au/itm/123", "AUD"),
        ("12,345.00", "https://www.ebay.com/itm/123", "USD"),
        ("AU$59.99", "https://www.ebay.com/itm/123", "AUD"),
    ]

    for price_text, url, expected in cases:
        result = detect_currency(price_text, url)
        assert result == expected, f"price={price_text!r}, url={url!r} → {result!r}, expected {expected!r}"


def test_sold_date_parsing():
    from tcg_platform.scraping.ebay_bronze import parse_sold_date

    cases = [
        ('Sold Mon, Jan 1, 2024', "2024-01-01"),
        ('Sold Tuesday, January 14, 2025', "2025-01-14"),
        ('Sold Jan 15, 2024', "2024-01-15"),
        ('Sold Dec 25, 2023', "2023-12-25"),
        ('Jan 1, 2024', "2024-01-01"),
        ('February 3, 2024', "2024-02-03"),
        ('Sold Fri, Mar 7, 2026', "2026-03-07"),
        ('No date here', ""),
        ('verkauft am Montag, 1. Januar 2024', ""),
    ]

    for html, expected in cases:
        result = parse_sold_date(html)
        assert result == expected, f"html={html!r} → {result!r}, expected {expected!r}"


def test_card_version_extraction():
    from tcg_platform.scraping.ebay_bronze import extract_card_version

    cases = [
        ("One Piece TCG OP15-001 Alternative Art", "OP15-001", "_Alternative_Art"),
        ("OP15-001", "OP15-001", ""),
        ("EB01-001 Black", "EB01-001", "_Black"),
        ("OP15 Alternative Art", "OP15", "_Alternative_Art"),
        ("ST03013", "ST03013", ""),
("", "", ""),
]

    for title, card_id, expected in cases:
        result = extract_card_version(title, card_id)
        assert result == expected, f"title={title!r}, card_id={card_id!r} → {result!r}, expected {expected!r}"
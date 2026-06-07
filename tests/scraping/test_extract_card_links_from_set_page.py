from tcg_platform.scraping.limitlesstcg import extract_card_links_from_set_page


SET_PAGE_HTML = """
<html><body>
  <a href="/cards/op01-001">Luffy</a>
  <a href="/cards/op01-002">Zoro</a>
  <a href="/cards/op01-001?v=1">Luffy alt</a>
  <a href="/cards/op01-001?v=2">Luffy manga</a>
  <a href="/cards/p-001">Promo Luffy</a>
  <a href="/cards/leader-some-event">Ignore me</a>  <!-- not a card link -->
  <a href="/something/else">Ignore</a>
  <a href="/cards/op01-001">Luffy</a>  <!-- duplicate, dedupe -->
</body></html>
"""


def test_parses_base_cards():
    result = extract_card_links_from_set_page(SET_PAGE_HTML)
    base_cards = [c for (c, v) in result if v is None]
    assert "OP01-001" in base_cards
    assert "OP01-002" in base_cards
    assert "P-001" in base_cards  # P prefix is included (fixes existing bug)


def test_parses_variants():
    result = extract_card_links_from_set_page(SET_PAGE_HTML)
    variants = {c: v for (c, v) in result if v is not None}
    assert variants == {"OP01-001": 1} or "OP01-001" in variants
    # OP01-001 has v=1 and v=2 → both should appear
    op001_variants = sorted([v for (c, v) in result if c == "OP01-001" and v is not None])
    assert op001_variants == [1, 2]


def test_dedupes_duplicate_links():
    result = extract_card_links_from_set_page(SET_PAGE_HTML)
    # OP01-001 base appears twice in HTML; should appear once in result
    base_op001 = [1 for (c, v) in result if c == "OP01-001" and v is None]
    assert sum(base_op001) == 1


def test_empty_html():
    assert extract_card_links_from_set_page("<html><body></body></html>") == []


def test_no_variants_returns_none_variant():
    html = """<html><body><a href="/cards/op01-001">x</a><a href="/cards/op01-002">y</a></body></html>"""
    result = extract_card_links_from_set_page(html)
    assert result == [("OP01-001", None), ("OP01-002", None)]


def test_card_id_uppercased():
    """Limitless URLs are lowercase; output card_ids must be uppercase."""
    html = '<html><body><a href="/cards/op01-001">x</a></body></html>'
    result = extract_card_links_from_set_page(html)
    assert result == [("OP01-001", None)]

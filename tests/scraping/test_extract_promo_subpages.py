from tcg_platform.scraping.limitlesstcg import extract_promo_subpages


PROMOS_INDEX_HTML = """
<html><body>
  <a href="/cards/promos">Promos index self-link</a>
  <a href="/cards/op16-the-time-of-battle">OP16 alias (should be excluded)</a>
  <a href="/cards/st30-ex-luffy-ace">ST30 alias (should be excluded)</a>
  <a href="/cards/tournament-pack-14">Tournament pack</a>
  <a href="/cards/event-pack-09">Event pack</a>
  <a href="/cards/regional-participation-pack-2026-1">Regional</a>
  <a href="/cards/misc-promos">Misc promos</a>
  <a href="/cards/championship-2024-event-pack">Championship</a>
  <a href="/cards/dash-pack-op14">Dash pack</a>
  <a href="/cards/gift-collection-01">Gift collection</a>
  <a href="/cards/tournament-pack-14">Tournament pack duplicate</a>
  <a href="/something/else">Non-card link</a>
  <a href="/cards/3rd-anniversary-treasure-campaign-pack">3rd anniversary</a>
</body></html>
"""


def test_extracts_promo_subpages():
    result = extract_promo_subpages(PROMOS_INDEX_HTML)
    expected = {
        "tournament-pack-14",
        "event-pack-09",
        "regional-participation-pack-2026-1",
        "misc-promos",
        "championship-2024-event-pack",
        "dash-pack-op14",
        "gift-collection-01",
        "3rd-anniversary-treasure-campaign-pack",
    }
    assert set(result) == expected


def test_excludes_self_link():
    """The /cards/promos self-link must not be treated as a sub-page."""
    result = extract_promo_subpages(PROMOS_INDEX_HTML)
    assert "promos" not in result


def test_excludes_set_name_aliases():
    """The OP16/ST30 set-name alias slugs (set-page aliases, not real sub-pages)."""
    result = extract_promo_subpages(PROMOS_INDEX_HTML)
    assert "op16-the-time-of-battle" not in result
    assert "st30-ex-luffy-ace" not in result


def test_dedupes_duplicates():
    """Duplicate sub-page links in HTML should appear once in output."""
    result = extract_promo_subpages(PROMOS_INDEX_HTML)
    assert result.count("tournament-pack-14") == 1


def test_preserves_discovery_order():
    """Sub-page order is stable for predictable logging."""
    result = extract_promo_subpages(PROMOS_INDEX_HTML)
    # In HTML order, with duplicates removed
    seen = set()
    expected = []
    for sub in [
        "tournament-pack-14", "event-pack-09", "regional-participation-pack-2026-1",
        "misc-promos", "championship-2024-event-pack", "dash-pack-op14",
        "gift-collection-01", "3rd-anniversary-treasure-campaign-pack",
    ]:
        if sub not in seen:
            seen.add(sub)
            expected.append(sub)
    assert result == expected


def test_empty_index_page():
    assert extract_promo_subpages("<html><body></body></html>") == []


def test_no_subpages_only_aliases():
    """If the index only links to set-name aliases, return empty."""
    html = """<html><body>
      <a href="/cards/op16-the-time-of-battle">x</a>
      <a href="/cards/st30-ex-luffy-ace">y</a>
    </body></html>"""
    assert extract_promo_subpages(html) == []


def test_lowercase_slugs():
    """Limitless URLs are lowercase; output slugs are lowercase too."""
    html = '<html><body><a href="/cards/tournament-pack-14">x</a></body></html>'
    result = extract_promo_subpages(html)
    assert result == ["tournament-pack-14"]

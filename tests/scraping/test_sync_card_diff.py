import pytest

from tcg_platform.scraping.limitless_sync import (
    build_cdn_url,
    build_minio_key,
    build_card_image_diff,
    home_set_from_card_id,
    is_real_card_id,
)


def test_build_cdn_url_base():
    url = build_cdn_url("OP01-001", None)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/OP01/OP01-001_EN.webp"


def test_build_cdn_url_variant():
    url = build_cdn_url("OP01-001", 1)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/OP01/OP01-001_p1_EN.webp"


def test_build_cdn_url_uses_card_id_prefix_not_container_set():
    """Cross-set reprints (e.g., EB02-001 is a reissue of OP05-001) have their
    CDN image at the card_id's home prefix path, not the container set path."""
    url = build_cdn_url("EB02-001", 2)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/EB02/EB02-001_p2_EN.webp"


def test_build_cdn_url_starter_deck_card():
    url = build_cdn_url("ST15-005", 4)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/ST15/ST15-005_p4_EN.webp"


def test_build_cdn_url_premium_booster_card():
    url = build_cdn_url("PRB02-014", 2)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/PRB02/PRB02-014_p2_EN.webp"


def test_build_cdn_url_promo_card():
    url = build_cdn_url("P-105", None)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/P/P-105_EN.webp"


def test_build_minio_key_base():
    assert build_minio_key("OP01-001", None) == "cards/OP01/OP01-001.webp"


def test_build_minio_key_variant():
    assert build_minio_key("OP01-001", 1) == "cards/OP01/OP01-001_v1.webp"
    assert build_minio_key("OP01-001", 2) == "cards/OP01/OP01-001_v2.webp"


def test_build_minio_key_uses_card_id_prefix():
    assert build_minio_key("EB02-001", 2) == "cards/EB02/EB02-001_v2.webp"
    assert build_minio_key("ST15-005", 4) == "cards/ST15/ST15-005_v4.webp"


def test_home_set_from_card_id_op():
    assert home_set_from_card_id("OP01-001") == "OP01"
    assert home_set_from_card_id("OP16-119") == "OP16"


def test_home_set_from_card_id_eb():
    assert home_set_from_card_id("EB01-001") == "EB01"
    assert home_set_from_card_id("EB04-054") == "EB04"


def test_home_set_from_card_id_st():
    assert home_set_from_card_id("ST01-001") == "ST01"
    assert home_set_from_card_id("ST30-001") == "ST30"


def test_home_set_from_card_id_prb():
    assert home_set_from_card_id("PRB01-001") == "PRB01"
    assert home_set_from_card_id("PRB02-014") == "PRB02"


def test_home_set_from_card_id_promo():
    assert home_set_from_card_id("P-105") == "P"


def test_home_set_from_card_id_invalid_raises():
    with pytest.raises(ValueError):
        home_set_from_card_id("OP16-THE-TIME-OF-BATTLE")
    with pytest.raises(ValueError):
        home_set_from_card_id("ST30-EX-LUFFY-ACE")
    with pytest.raises(ValueError):
        home_set_from_card_id("not-a-card")
    with pytest.raises(ValueError):
        home_set_from_card_id("")


def test_is_real_card_id_accepts_standard_format():
    """Real card_ids have a 2-digit numeric prefix and a short suffix."""
    assert is_real_card_id("OP01-001") is True
    assert is_real_card_id("OP16-119") is True
    assert is_real_card_id("EB01-001") is True
    assert is_real_card_id("EB04-054") is True
    assert is_real_card_id("ST01-001") is True
    assert is_real_card_id("ST30-001") is True
    assert is_real_card_id("PRB01-001") is True
    assert is_real_card_id("PRB02-014") is True
    assert is_real_card_id("P-105") is True


def test_is_real_card_id_rejects_set_name_aliases():
    """Limitless set pages contain links like /cards/OP16-THE-TIME-OF-BATTLE
    that are set-name slugs (page aliases for the set itself), not real card
    IDs. They match the prefix regex but have no CDN image."""
    assert is_real_card_id("OP16-THE-TIME-OF-BATTLE") is False
    assert is_real_card_id("ST30-EX-LUFFY-ACE") is False
    assert is_real_card_id("OP15-AWAKENING-OF-A-NEW-ERA") is False


def test_is_real_card_id_rejects_non_card_strings():
    assert is_real_card_id("") is False
    assert is_real_card_id("leader-some-event") is False
    assert is_real_card_id("/cards/") is False


def test_diff_no_new_cards():
    discovered = [
        ("OP01-001", None),
        ("OP01-002", None),
    ]
    existing = {"cards/OP01/OP01-001.webp", "cards/OP01/OP01-002.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == []


def test_diff_with_new_cards():
    discovered = [
        ("OP01-001", None),
        ("OP01-002", None),
        ("OP01-003", None),
    ]
    existing = {"cards/OP01/OP01-001.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == [
        ("OP01-002", None, "cards/OP01/OP01-002.webp"),
        ("OP01-003", None, "cards/OP01/OP01-003.webp"),
    ]


def test_diff_includes_variants():
    discovered = [
        ("OP01-001", None),
        ("OP01-001", 1),
        ("OP01-001", 2),
    ]
    existing = {"cards/OP01/OP01-001.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == [
        ("OP01-001", 1, "cards/OP01/OP01-001_v1.webp"),
        ("OP01-001", 2, "cards/OP01/OP01-001_v2.webp"),
    ]


def test_diff_ignores_legacy_files_in_other_prefixes():
    """Existing files like sold_data/ should not interfere with the cards/ diff."""
    discovered = [("OP01-001", None)]
    existing = {"cards/OP01/OP01-001.webp", "sold_data/DE/123.parquet"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == []

from tcg_platform.scraping.limitless_sync import (
    build_cdn_url,
    build_minio_key,
    build_card_image_diff,
)


def test_build_cdn_url_base():
    url = build_cdn_url("OP01", "OP01-001", None)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/OP01/OP01-001_EN.webp"


def test_build_cdn_url_variant():
    url = build_cdn_url("OP01", "OP01-001", 1)
    assert url == "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/OP01/OP01-001_p1_EN.webp"


def test_build_minio_key_base():
    assert build_minio_key("OP01", "OP01-001", None) == "cards/OP01/OP01-001.webp"


def test_build_minio_key_variant():
    assert build_minio_key("OP01", "OP01-001", 1) == "cards/OP01/OP01-001_v1.webp"
    assert build_minio_key("OP01", "OP01-001", 2) == "cards/OP01/OP01-001_v2.webp"


def test_diff_no_new_cards():
    discovered = [
        ("OP01", "OP01-001", None),
        ("OP01", "OP01-002", None),
    ]
    existing = {"cards/OP01/OP01-001.webp", "cards/OP01/OP01-002.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == []


def test_diff_with_new_cards():
    discovered = [
        ("OP01", "OP01-001", None),
        ("OP01", "OP01-002", None),
        ("OP01", "OP01-003", None),
    ]
    existing = {"cards/OP01/OP01-001.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == [
        ("OP01", "OP01-002", None, "cards/OP01/OP01-002.webp"),
        ("OP01", "OP01-003", None, "cards/OP01/OP01-003.webp"),
    ]


def test_diff_includes_variants():
    discovered = [
        ("OP01", "OP01-001", None),
        ("OP01", "OP01-001", 1),
        ("OP01", "OP01-001", 2),
    ]
    existing = {"cards/OP01/OP01-001.webp"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == [
        ("OP01", "OP01-001", 1, "cards/OP01/OP01-001_v1.webp"),
        ("OP01", "OP01-001", 2, "cards/OP01/OP01-001_v2.webp"),
    ]


def test_diff_ignores_legacy_files_in_other_prefixes():
    """Existing files like sold_data/ should not interfere with the cards/ diff."""
    discovered = [("OP01", "OP01-001", None)]
    existing = {"cards/OP01/OP01-001.webp", "sold_data/DE/123.parquet"}
    diff = build_card_image_diff(discovered, existing)
    assert diff == []

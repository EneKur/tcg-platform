"""Pure helpers for syncing Limitless TCG card images to MinIO.

Lives in scraping/ (not defs/) because it has no Dagster dependency and can
be unit-tested in isolation. The Dagster asset wraps these functions.
"""

CDN_BASE = "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece"


def build_cdn_url(set_code: str, card_id: str, variant: int | None) -> str:
    """Build the Limitless CDN URL for a card image.

    Base cards end in _EN.webp; variant N ends in _pN_EN.webp.
    """
    suffix = f"_p{variant}_EN.webp" if variant is not None else "_EN.webp"
    return f"{CDN_BASE}/{set_code}/{card_id}{suffix}"


def build_minio_key(set_code: str, card_id: str, variant: int | None) -> str:
    """Build the MinIO object key for a card image.

    Base cards: cards/{set_code}/{card_id}.webp
    Variant N:  cards/{set_code}/{card_id}_vN.webp
    """
    suffix = f"_v{variant}" if variant is not None else ""
    return f"cards/{set_code}/{card_id}{suffix}.webp"


def build_card_image_diff(
    discovered: list[tuple[str, str, int | None]],
    existing_keys: set[str],
) -> list[tuple[str, str, int | None, str]]:
    """Compute missing images: each entry is (set_code, card_id, variant, minio_key).

    Order of output preserves order of input `discovered` for stable logging.
    """
    out: list[tuple[str, str, int | None, str]] = []
    for set_code, card_id, variant in discovered:
        key = build_minio_key(set_code, card_id, variant)
        if key not in existing_keys:
            out.append((set_code, card_id, variant, key))
    return out

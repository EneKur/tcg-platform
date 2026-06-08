"""Pure helpers for syncing Limitless TCG card images to MinIO.

Lives in scraping/ (not defs/) because it has no Dagster dependency and can
be unit-tested in isolation. The Dagster asset wraps these functions.
"""

import re

CDN_BASE = "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece"


# Matches the set prefix of a real card_id. The numeric part after the prefix
# may be either 3 digits (e.g. OP01-001, EB04-054, ST15-005, PRB02-014) or
# a short alphanumeric token for promo cards (e.g. P-105). The suffix is
# capped at 8 chars so that set-name alias slugs (e.g. OP16-THE-TIME-OF-BATTLE,
# ST30-EX-LUFFY-ACE) are rejected.
_HOME_SET_RE = re.compile(r"^(OP|EB|ST|PRB|P)(\d{2}|[A-Z0-9]+)?-([A-Z0-9]{1,8})$")
_PREFIX_RE = re.compile(r"^(OP|EB|ST|PRB|P)")


def home_set_from_card_id(card_id: str) -> str:
    """Extract the home set code from a card_id (e.g. 'OP05-001' -> 'OP05').

    The home set is the set whose prefix matches the card_id. Real card_ids
    always have a 2-digit numeric suffix after the prefix, optionally
    followed by a dash and a short numeric/alphanumeric suffix.

    Raises ValueError if the card_id is malformed (e.g. an OP16-THE-TIME-OF-BATTLE
    set-page alias slug).
    """
    m = _HOME_SET_RE.match(card_id)
    if not m:
        raise ValueError(f"Invalid card_id (cannot extract home set): {card_id!r}")
    prefix = m.group(1)
    digits = m.group(2) or ""
    return f"{prefix}{digits}"


def is_real_card_id(card_id: str) -> bool:
    """True iff card_id has the shape of a real Limitless card ID.

    Real card IDs have a set prefix (OP/EB/ST/PRB/P), optional 2-digit set
    number, a dash, and a short (≤8 char) alphanumeric suffix. Set-page
    alias slugs (e.g. OP16-THE-TIME-OF-BATTLE) match the prefix regex used
    by extract_card_links_from_set_page but have no CDN image and must be
    filtered out.
    """
    return _HOME_SET_RE.match(card_id) is not None


def build_cdn_url(card_id: str, variant: int | None) -> str:
    """Build the Limitless CDN URL for a card image.

    Base cards end in _EN.webp; variant N ends in _pN_EN.webp. The set
    directory in the URL is derived from the card_id's home-set prefix
    (e.g. ST15-005 -> ST15/ST15-005_p4_EN.webp), NOT the container set
    where the card was scraped from. This matters for cross-set reprints
    like EB02-001_p2 whose image lives at EB02/, not at the OP16 set page
    that linked it.
    """
    suffix = f"_p{variant}_EN.webp" if variant is not None else "_EN.webp"
    return f"{CDN_BASE}/{home_set_from_card_id(card_id)}/{card_id}{suffix}"


def build_minio_key(card_id: str, variant: int | None) -> str:
    """Build the MinIO object key for a card image.

    Base cards: cards/{home_set}/{card_id}.webp
    Variant N:  cards/{home_set}/{card_id}_vN.webp

    The set directory in the key matches the CDN layout (home set of the
    card_id, not the container set on the source page).
    """
    suffix = f"_v{variant}" if variant is not None else ""
    return f"cards/{home_set_from_card_id(card_id)}/{card_id}{suffix}.webp"


def build_card_image_diff(
    discovered: list[tuple[str, int | None]],
    existing_keys: set[str],
) -> list[tuple[str, int | None, str]]:
    """Compute missing images: each entry is (card_id, variant, minio_key).

    Order of output preserves order of input `discovered` for stable logging.
    """
    out: list[tuple[str, int | None, str]] = []
    for card_id, variant in discovered:
        key = build_minio_key(card_id, variant)
        if key not in existing_keys:
            out.append((card_id, variant, key))
    return out

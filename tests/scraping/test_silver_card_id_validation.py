from tcg_platform.defs.silver_transform import is_valid_card_id


CARDLIST = {
    "OP01-001", "OP11-080", "ST13-003", "OP04-118", "OP07-029",
    "PRB02-001", "EB01-001", "P-014",
}


def test_already_normalized_card_id_in_cardlist():
    # The M6.5-T1 scraper emits card_ids in the final format (e.g. OP11-080).
    # The silver transform must accept these as valid without trying to
    # re-normalize them — the regex would otherwise match just the set
    # code and miss the cardlist entry.
    assert is_valid_card_id("OP11-080", CARDLIST) is True


def test_already_normalized_set_with_dash_in_cardlist():
    assert is_valid_card_id("ST13-003", CARDLIST) is True


def test_unnormalized_5_digit_format():
    # Legacy format from the old scraper: ST13003 (no dash) → ST13-003.
    assert is_valid_card_id("ST13003", CARDLIST) is True


def test_unnormalized_4_digit_format():
    # OP07029 → OP07-029 (4-digit number)
    assert is_valid_card_id("OP07029", CARDLIST) is True


def test_unnormalized_p_card():
    # P-014 already in cardlist
    assert is_valid_card_id("P014", CARDLIST) is True
    assert is_valid_card_id("P-014", CARDLIST) is True


def test_set_code_only_is_invalid():
    # PRB02 (no number) — set code only, not a complete card.
    assert is_valid_card_id("PRB02", CARDLIST) is False


def test_garbage_input_is_invalid():
    assert is_valid_card_id("", CARDLIST) is False
    assert is_valid_card_id("MALFORMED_TITLE", CARDLIST) is False
    assert is_valid_card_id(None, CARDLIST) is False


def test_unnormalized_with_garbage_suffix_is_invalid():
    # The bronze card_id field is a single column. If the scraper ever
    # concatenates card_id + version text in this field, the prefix may
    # look like a valid card_id. We only accept exact matches against
    # the cardlist — concatenations are rejected.
    assert is_valid_card_id("OP11-080GearTwoV2MonkeyDLuffyPSA10", CARDLIST) is False


def test_p_set_code_only_is_invalid():
    # P alone (no number) is a set code, not a card.
    assert is_valid_card_id("P", CARDLIST) is False

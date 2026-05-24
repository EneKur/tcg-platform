import pytest
import tempfile

from tcg_platform.resources.currency_rates import CurrencyRatesDB


def test_insert_and_retrieve_rates(tmp_path):
    db_path = str(tmp_path / "rates.db")
    db = CurrencyRatesDB(db_path=db_path)
    db.setup()

    db.insert_rate("EUR", "GBP", 0.864, "2025-05-24 00:00:00")
    db.insert_rate("EUR", "GBP", 0.863, "2025-05-23 00:00:00")

    last = db.get_last_timestamp()
    assert last is not None
    assert last.strftime("%Y-%m-%d") == "2025-05-24"

    rates = db.get_all_rates()
    assert len(rates) == 2

    db.insert_rate("EUR", "GBP", 0.864, "2025-05-24 00:00:00")
    rates = db.get_all_rates()
    assert len(rates) == 2

    db.close()


def test_get_last_timestamp_empty():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = CurrencyRatesDB(db_path=f.name)
        db.setup()
        last = db.get_last_timestamp()
        assert last is None
        db.close()
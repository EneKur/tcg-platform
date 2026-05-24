import os

from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext
from dotenv import load_dotenv

from tcg_platform.resources.currency_rates import CurrencyRatesDB

load_dotenv()


@resource
def currency_rates_db(init_context: InitResourceContext):
    db_path = os.getenv("CURRENCY_RATES_DB")
    if not db_path:
        raise ValueError(
            "CURRENCY_RATES_DB environment variable is not set. "
            "Set it to a path like ./data/currency_rates.db"
        )
    return CurrencyRatesDB(db_path=db_path)
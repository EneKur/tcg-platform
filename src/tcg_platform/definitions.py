from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder, define_asset_job

from tcg_platform.defs.currency_rates_resource import (
    currency_rates_db,
)
from tcg_platform.defs.minio_resources import (
    minio_client,
)
from tcg_platform.resources.sqlite_client import (
    SqliteClientResource,
)
from tcg_platform.defs.zyte_resources import (
    zyte_session_resource,
)


ebay_de_job = define_asset_job(
    name="ebay_de_pipeline",
    selection=["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer"],
    description="Scrape DE eBay PSA sold listings and persist to SQLite",
)

ebay_uk_job = define_asset_job(
    name="ebay_uk_pipeline",
    selection=["ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"],
    description="Scrape UK eBay PSA sold listings and persist to SQLite",
)

ebay_eu_job = define_asset_job(
    name="ebay_eu_pipeline",
    selection=["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer",
               "ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"],
    description="Scrape DE+UK eBay PSA sold listings and persist to SQLite",
)


@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions(
        assets=base.assets,
        asset_checks=base.asset_checks,
        jobs=[ebay_de_job, ebay_uk_job, ebay_eu_job],
        resources={
            "currency_rates_db": currency_rates_db,
            "minio_client": minio_client,
            "sqlite_client_de": SqliteClientResource(db_path="./data/tcg_de.db"),
            "sqlite_client_uk": SqliteClientResource(db_path="./data/tcg_uk.db"),
            "zyte_session_resource": zyte_session_resource,
        },
    )
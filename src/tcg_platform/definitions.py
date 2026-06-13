from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder, define_asset_job

from tcg_platform.defs.backfill_sold_data_parquet import (
    backfill_de_job,
    backfill_uk_job,
    backfill_de_sensor,
    backfill_uk_sensor,
)
from tcg_platform.defs.currency_rates_resource import (
    currency_rates_db,
)
from tcg_platform.defs.exchange_rates_asset import (
    exchange_rates,
)
from tcg_platform.defs.minio_resources import (
    minio_client,
    tcg_raw_client,
)
from tcg_platform.resources.sqlite_client import (
    SqliteClientResource,
)
from tcg_platform.defs.zyte_resources import (
    zyte_session_resource,
)
from tcg_platform.defs.eu_pipeline_orchestrator import (
    bronze_eu_orchestrator,
    backfill_de_asset,
    backfill_uk_asset,
    silver_eu_orchestrator,
)
from tcg_platform.defs.reconcile_quarantine import (
    reconcile_quarantine_de_job,
    reconcile_quarantine_uk_job,
)


exchange_rates_job = define_asset_job(
    name="exchange_rates_job",
    selection=["exchange_rates"],
    description="Backfill EUR→GBP exchange rates from Frankfurter.app",
)


silver_de_job = define_asset_job(
    name="silver_de_pipeline",
    selection=["silver_de_transform"],
    description="Transform DE bronze parquets to silver layer",
)

silver_uk_job = define_asset_job(
    name="silver_uk_pipeline",
    selection=["silver_uk_transform"],
    description="Transform UK bronze parquets to silver layer",
)

silver_eu_job = define_asset_job(
    name="silver_eu_pipeline",
    selection=["silver_de_transform", "silver_uk_transform"],
    description="Transform DE+UK bronze parquets to silver layer",
)

complete_eu_pipeline = define_asset_job(
    name="complete_eu_pipeline",
    selection=["bronze_eu_orchestrator", "backfill_de_asset", "backfill_uk_asset", "silver_eu_orchestrator"],
    description="Full EU pipeline: bronze → backfill (DE+UK parallel) → silver",
)

sync_card_images_job = define_asset_job(
    name="sync_card_images_job",
    selection=["discover_limitless_catalog", "sync_card_images"],
    description="Diff Limitless catalog against tcg-bronze/cards/, download missing images.",
)

ebay_de_raw_to_bronze_job = define_asset_job(
    name="ebay_de_raw_to_bronze",
    selection=["scrape_ebay_de_raw", "transform_ebay_de_to_bronze"],
    description="DE: scrape tcg-raw + transform to tcg-bronze",
)

ebay_uk_raw_to_bronze_job = define_asset_job(
    name="ebay_uk_raw_to_bronze",
    selection=["scrape_ebay_uk_raw", "transform_ebay_uk_to_bronze"],
    description="UK: scrape tcg-raw + transform to tcg-bronze",
)

backfill_raw_html_de_job = define_asset_job(
    name="backfill_raw_html_de_job",
    selection=["backfill_raw_html_de"],
    description="One-time: fetch raw HTML for existing DE fact_events rows.",
)

backfill_raw_html_uk_job = define_asset_job(
    name="backfill_raw_html_uk_job",
    selection=["backfill_raw_html_uk"],
    description="One-time: fetch raw HTML for existing UK fact_events rows.",
)


@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions(
        assets=base.assets,
        asset_checks=base.asset_checks,
        jobs=[
            backfill_de_job,
            backfill_uk_job,
            silver_de_job,
            silver_uk_job,
            silver_eu_job,
            complete_eu_pipeline,
            sync_card_images_job,
            reconcile_quarantine_de_job,
            reconcile_quarantine_uk_job,
            # NEW for M9-T1
            ebay_de_raw_to_bronze_job,
            ebay_uk_raw_to_bronze_job,
            backfill_raw_html_de_job,
            backfill_raw_html_uk_job,
            exchange_rates_job,
        ],
        sensors=[backfill_de_sensor, backfill_uk_sensor],
        resources={
            "currency_rates_db": currency_rates_db,
            "minio_client": minio_client,
            "tcg_raw_client": tcg_raw_client,
            "sqlite_client_de": SqliteClientResource(db_path="./data/tcg_de.db"),
            "sqlite_client_uk": SqliteClientResource(db_path="./data/tcg_uk.db"),
            "zyte_session_resource": zyte_session_resource,
        },
    )
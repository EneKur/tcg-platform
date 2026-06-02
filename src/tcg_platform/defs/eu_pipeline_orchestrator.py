import dagster as dg
from dagster import AssetKey


@dg.asset
def bronze_eu_orchestrator(context: dg.AssetExecutionContext):
    """Triggers ebay_eu_pipeline — runs DE + UK scrape in sequence."""
    from tcg_platform.definitions import ebay_eu_job
    context.log.info("Starting bronze_eu_orchestrator")
    run_config = {}
    result = context.instance.create_run(
        job_name=ebay_eu_job.name,
        run_config=run_config,
    )
    result = context.instance.submit_run(result.run_id)
    context.log.info(f"bronze_eu_orchestrator complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("bronze_eu_orchestrator")])
def backfill_de_asset(context: dg.AssetExecutionContext):
    """Backfill DE sold data from SQLite to MinIO. Runs after bronze."""
    context.log.info("Starting backfill_de_asset")
    from tcg_platform.defs.backfill_sold_data_parquet import (
        backfill_de_sold_data_parquet,
    )
    job = backfill_de_sold_data_parquet.backfill_de_job
    run = context.instance.create_run(
        job_name=job.name,
        run_config={},
    )
    result = context.instance.submit_run(run.run_id)
    context.log.info(f"backfill_de_asset complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("bronze_eu_orchestrator")])
def backfill_uk_asset(context: dg.AssetExecutionContext):
    """Backfill UK sold data from SQLite to MinIO. Runs after bronze, in parallel with DE."""
    context.log.info("Starting backfill_uk_asset")
    from tcg_platform.defs.backfill_sold_data_parquet import (
        backfill_uk_sold_data_parquet,
    )
    job = backfill_uk_sold_data_parquet.backfill_uk_job
    run = context.instance.create_run(
        job_name=job.name,
        run_config={},
    )
    result = context.instance.submit_run(run.run_id)
    context.log.info(f"backfill_uk_asset complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("backfill_de_asset"), AssetKey("backfill_uk_asset")])
def silver_eu_orchestrator(context: dg.AssetExecutionContext):
    """Run silver DE + UK transforms. Waits for both backfills to succeed."""
    context.log.info("Starting silver_eu_orchestrator")
    from tcg_platform.definitions import silver_eu_job
    run = context.instance.create_run(
        job_name=silver_eu_job.name,
        run_config={},
        resolved_top_level_resources=True,
    )
    result = context.instance.submit_run(run.run_id)
    context.log.info(f"silver_eu_orchestrator complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


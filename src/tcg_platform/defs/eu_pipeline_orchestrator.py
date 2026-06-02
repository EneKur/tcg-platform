import dagster as dg
from dagster import AssetKey


@dg.asset
def bronze_eu_orchestrator(context: dg.AssetExecutionContext):
    """Triggers ebay_eu_pipeline — runs DE + UK scrape in sequence."""
    from tcg_platform.definitions import defs
    context.log.info("Starting bronze_eu_orchestrator")
    resolved = defs.load_fn()
    job_def = resolved.resolve_job_def("ebay_eu_pipeline")
    result = job_def.execute_in_process(instance=context.instance)
    context.log.info(f"bronze_eu_orchestrator complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("bronze_eu_orchestrator")])
def backfill_de_asset(context: dg.AssetExecutionContext):
    """Backfill DE sold data from SQLite to MinIO. Runs after bronze."""
    context.log.info("Starting backfill_de_asset")
    from tcg_platform.definitions import defs
    resolved = defs.load_fn()
    job_def = resolved.resolve_job_def("backfill_de_sold_data_job")
    result = job_def.execute_in_process(instance=context.instance)
    context.log.info(f"backfill_de_asset complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("bronze_eu_orchestrator")])
def backfill_uk_asset(context: dg.AssetExecutionContext):
    """Backfill UK sold data from SQLite to MinIO. Runs after bronze, in parallel with DE."""
    context.log.info("Starting backfill_uk_asset")
    from tcg_platform.definitions import defs
    resolved = defs.load_fn()
    job_def = resolved.resolve_job_def("backfill_uk_sold_data_job")
    result = job_def.execute_in_process(instance=context.instance)
    context.log.info(f"backfill_uk_asset complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


@dg.asset(deps=[AssetKey("backfill_de_asset"), AssetKey("backfill_uk_asset")])
def silver_eu_orchestrator(context: dg.AssetExecutionContext):
    """Run silver DE + UK transforms. Waits for both backfills to succeed."""
    context.log.info("Starting silver_eu_orchestrator")
    from tcg_platform.definitions import defs
    resolved = defs.load_fn()
    job_def = resolved.resolve_job_def("silver_eu_pipeline")
    result = job_def.execute_in_process(instance=context.instance)
    context.log.info(f"silver_eu_orchestrator complete, run_id={result.run_id}")
    return dg.MaterializeResult(metadata={"run_id": result.run_id})


import dagster as dg
from dagster import AssetKey


@dg.asset
def bronze_eu_orchestrator(context: dg.AssetExecutionContext):
    """Triggers ebay_de and ebay_uk scrapes in parallel."""
    from tcg_platform.definitions import defs
    context.log.info("Starting bronze_eu_orchestrator")
    resolved = defs.load_fn()

    job_def_de = resolved.resolve_job_def("ebay_de_pipeline")
    job_def_uk = resolved.resolve_job_def("ebay_uk_pipeline")

    context.log.info("Running ebay_de_pipeline and ebay_uk_pipeline in parallel...")
    result_de = job_def_de.execute_in_process(instance=context.instance)
    result_uk = job_def_uk.execute_in_process(instance=context.instance)

    context.log.info(f"bronze complete, de_run_id={result_de.run_id}, uk_run_id={result_uk.run_id}")
    return dg.MaterializeResult(metadata={
        "de_run_id": result_de.run_id,
        "uk_run_id": result_uk.run_id,
    })


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
    """Run silver DE then UK transforms sequentially. Waits for both backfills to succeed."""
    from tcg_platform.definitions import defs
    context.log.info("Starting silver_eu_orchestrator")

    # Run DE first
    resolved = defs.load_fn()
    job_def_de = resolved.resolve_job_def("silver_de_pipeline")
    context.log.info("Running silver_de_pipeline...")
    result_de = job_def_de.execute_in_process(instance=context.instance)
    context.log.info(f"silver_de complete, run_id={result_de.run_id}")

    # Then UK
    job_def_uk = resolved.resolve_job_def("silver_uk_pipeline")
    context.log.info("Running silver_uk_pipeline...")
    result_uk = job_def_uk.execute_in_process(instance=context.instance)
    context.log.info(f"silver_uk complete, run_id={result_uk.run_id}")

    return dg.MaterializeResult(metadata={
        "de_run_id": result_de.run_id,
        "uk_run_id": result_uk.run_id,
    })


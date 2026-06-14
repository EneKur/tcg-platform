import dagster as dg
from dagster import AssetKey


@dg.asset
def bronze_eu_orchestrator(context: dg.AssetExecutionContext):
    """Triggers ebay_de, ebay_uk scrapes and exchange_rates backfill in parallel."""
    import concurrent.futures
    from typing import Any

    from tcg_platform.definitions import defs

    context.log.info("Starting bronze_eu_orchestrator")
    resolved = defs.load_fn()

    job_def_de    = resolved.resolve_job_def("ebay_de_raw_to_bronze")
    job_def_uk    = resolved.resolve_job_def("ebay_uk_raw_to_bronze")
    job_def_rates = resolved.resolve_job_def("exchange_rates_job")

    context.log.info("Running ebay_de_raw_to_bronze, ebay_uk_raw_to_bronze, exchange_rates_job in parallel...")

    sub_jobs = {
        "de":    job_def_de.execute_in_process,
        "uk":    job_def_uk.execute_in_process,
        "rates": job_def_rates.execute_in_process,
    }

    results: dict[str, Any] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix="eu-bronze") as ex:
        futures = {ex.submit(fn, instance=context.instance): name for name, fn in sub_jobs.items()}
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                context.log.error(f"{name} sub-job failed: {e}")
                raise
            results[name] = r
            context.log.info(f"{name} sub-job complete, run_id={r.run_id}")

    return dg.MaterializeResult(metadata={
        "de_run_id":    results["de"].run_id,
        "uk_run_id":    results["uk"].run_id,
        "rates_run_id": results["rates"].run_id,
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
    """Run reconcile_quarantine → silver for DE and UK sequentially.

    The reconciler runs FIRST so that any quarantined rows whose card_id
    is now in the cards folder get their quarantine parquets deleted. The
    subsequent silver runs then re-evaluate the corresponding bronze rows
    and write them to tcg-silver/data/.

    Waits for both backfills to succeed.
    """
    from tcg_platform.definitions import defs
    context.log.info("Starting silver_eu_orchestrator")

    resolved = defs.load_fn()

    # Reconcile DE quarantine first
    job_def_reconcile_de = resolved.resolve_job_def("reconcile_quarantine_de_job")
    context.log.info("Running reconcile_quarantine_de_job...")
    result_reconcile_de = job_def_reconcile_de.execute_in_process(instance=context.instance)
    context.log.info(f"reconcile_de complete, run_id={result_reconcile_de.run_id}")

    # Then UK
    job_def_reconcile_uk = resolved.resolve_job_def("reconcile_quarantine_uk_job")
    context.log.info("Running reconcile_quarantine_uk_job...")
    result_reconcile_uk = job_def_reconcile_uk.execute_in_process(instance=context.instance)
    context.log.info(f"reconcile_uk complete, run_id={result_reconcile_uk.run_id}")

    # Existing silver runs (DE then UK)
    job_def_de = resolved.resolve_job_def("silver_de_pipeline")
    context.log.info("Running silver_de_pipeline...")
    result_de = job_def_de.execute_in_process(instance=context.instance)
    context.log.info(f"silver_de complete, run_id={result_de.run_id}")

    job_def_uk = resolved.resolve_job_def("silver_uk_pipeline")
    context.log.info("Running silver_uk_pipeline...")
    result_uk = job_def_uk.execute_in_process(instance=context.instance)
    context.log.info(f"silver_uk complete, run_id={result_uk.run_id}")

    return dg.MaterializeResult(metadata={
        "reconcile_de_run_id": result_reconcile_de.run_id,
        "reconcile_uk_run_id": result_reconcile_uk.run_id,
        "de_run_id": result_de.run_id,
        "uk_run_id": result_uk.run_id,
    })


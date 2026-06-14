import time
from unittest.mock import MagicMock, patch

from dagster import AssetKey


def test_bronze_eu_orchestrator_asset_exists():
    from tcg_platform.defs.eu_pipeline_orchestrator import bronze_eu_orchestrator
    assert bronze_eu_orchestrator is not None


def test_backfill_de_asset_depends_on_bronze():
    from dagster import AssetKey
    from tcg_platform.defs.eu_pipeline_orchestrator import backfill_de_asset

    deps = backfill_de_asset.dependency_keys
    assert AssetKey("bronze_eu_orchestrator") in deps


def test_backfill_uk_asset_depends_on_bronze():
    from dagster import AssetKey
    from tcg_platform.defs.eu_pipeline_orchestrator import backfill_uk_asset

    deps = backfill_uk_asset.dependency_keys
    assert AssetKey("bronze_eu_orchestrator") in deps


def test_silver_eu_orchestrator_depends_on_both_backfills():
    from dagster import AssetKey
    from tcg_platform.defs.eu_pipeline_orchestrator import silver_eu_orchestrator

    deps = silver_eu_orchestrator.dependency_keys
    assert AssetKey("backfill_de_asset") in deps
    assert AssetKey("backfill_uk_asset") in deps


def test_bronze_eu_orchestrator_runs_sub_jobs_in_parallel():
    """The 3 sub-jobs (ebay_de, ebay_uk, exchange_rates) MUST run concurrently.

    Pinning both behaviors:
    1. All 3 sub-jobs are submitted (not skipped).
    2. Total wall time < sum-of-sleeps (proves they ran in parallel, not sequential).

    The threshold: with 0.25s sleep per job and 3 jobs, sequential = 0.75s,
    parallel ~= 0.25s. Use 0.55s as the fail threshold to leave a margin
    for thread-scheduling jitter on a busy host.
    """
    import tcg_platform.definitions
    from tcg_platform.defs import eu_pipeline_orchestrator as mod

    sleep_per_job = 0.25
    fake_results = {
        "de":    MagicMock(run_id="r-de"),
        "uk":    MagicMock(run_id="r-uk"),
        "rates": MagicMock(run_id="r-rates"),
    }

    def _make_fake_executor(name):
        def _exec(instance=None):
            time.sleep(sleep_per_job)
            return fake_results[name]
        return _exec

    fake_jobs = {
        "ebay_de_raw_to_bronze": MagicMock(execute_in_process=_make_fake_executor("de")),
        "ebay_uk_raw_to_bronze": MagicMock(execute_in_process=_make_fake_executor("uk")),
        "exchange_rates_job":    MagicMock(execute_in_process=_make_fake_executor("rates")),
    }
    fake_resolved = MagicMock()
    fake_resolved.resolve_job_def.side_effect = lambda n: fake_jobs[n]

    fake_context = MagicMock()
    fake_context.instance = MagicMock()
    fake_context.log = MagicMock()

    with patch.object(tcg_platform.definitions, "defs") as fake_defs:
        fake_defs.load_fn.return_value = fake_resolved
        op = mod.bronze_eu_orchestrator.op
        start = time.monotonic()
        op.compute_fn.decorated_fn(fake_context)
        elapsed = time.monotonic() - start

    # 1. All 3 sub-jobs were dispatched
    assert {c.args[0] for c in fake_resolved.resolve_job_def.call_args_list} == {
        "ebay_de_raw_to_bronze", "ebay_uk_raw_to_bronze", "exchange_rates_job",
    }
    # 2. Wall time < sum-of-sleeps (i.e., not sequential)
    assert elapsed < 3 * sleep_per_job - 0.10, (
        f"sub-jobs ran sequentially: elapsed={elapsed:.3f}s, "
        f"3*sleep={3*sleep_per_job:.3f}s"
    )
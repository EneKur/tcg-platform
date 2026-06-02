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
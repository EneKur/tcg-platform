def test_definitions_load_cleanly():
    """Dagster definitions must load without errors (catches wiring mistakes)."""
    from tcg_platform.definitions import defs
    # The @definitions decorator defers loading; calling load_fn() forces it
    resolved = defs.load_fn()
    assert resolved is not None


def test_new_jobs_importable():
    """The 4 new jobs must be importable from definitions."""
    from tcg_platform.definitions import (
        ebay_de_raw_to_bronze_job,
        ebay_uk_raw_to_bronze_job,
        backfill_raw_html_de_job,
        backfill_raw_html_uk_job,
    )
    for j in (ebay_de_raw_to_bronze_job, ebay_uk_raw_to_bronze_job,
              backfill_raw_html_de_job, backfill_raw_html_uk_job):
        assert j is not None

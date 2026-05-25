#!/usr/bin/env python3
"""Run DE and UK eBay PSA sold listing pipelines for up to N records each."""
import sys, os, time
sys.path.insert(0, "src")
from dotenv import load_dotenv
load_dotenv()

from dagster import DagsterInstance
from tcg_platform.definitions import defs

TARGET_RECORDS = 1000

resolved = defs.load_fn()
instance = DagsterInstance.ephemeral()
job = resolved.get_implicit_global_asset_job_def()

for region, assets in [
    ("DE", ["ebay_de_sold_listings", "bronze_ebay_de_sqlite_writer"]),
    ("UK", ["ebay_uk_sold_listings", "bronze_ebay_uk_sqlite_writer"]),
]:
    print(f"\n{'='*60}")
    print(f"Running {region} — target: {TARGET_RECORDS} records")
    print(f"MinIO: {os.getenv('MINIO_ENDPOINT')} / bucket: {os.getenv('MINIO_BUCKET')}")
    print(f"{'='*60}")

    run_result = job.execute_in_process(
        op_selection=assets,
        instance=instance,
        raise_on_error=False,
    )

    events = list(run_result.all_events())
    for e in events:
        if e.is_asset_materialization:
            print(f"  ✅ {e.asset_key}: {dict(e.metadata)}")
        elif e.is_step_failure:
            print(f"  ❌ {e.step_key}")

    if not run_result.success:
        print(f"❌ {region} pipeline FAILED")
    else:
        print(f"✅ {region} pipeline succeeded")

print(f"\n{'='*60}")
print("Done — check SQLite DBs at:")
print(f"  DE: {os.getenv('SQLITE_PATH_DE')}")
print(f"  UK: {os.getenv('SQLITE_PATH_UK')}")
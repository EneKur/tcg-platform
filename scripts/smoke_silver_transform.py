"""Smoke test: materialize silver_de_transform + silver_uk_transform via Dagster.

Per M7-T2 update Task 4. Uses Dagster's `materialize` API instead of `dg dev` UI.
"""
import sys
import time
from pathlib import Path

# Make project root importable
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dagster import materialize, with_resources
from tcg_platform.definitions import defs
from tcg_platform.defs.silver_transform import silver_de_transform, silver_uk_transform


def main():
    # LazyDefinitions is a callable. Calling it returns a Definitions
    # object with concrete .resources, .assets, etc.
    resolved = defs()
    resources = resolved.resources
    # Restrict to only the resources our two assets need
    needed = {"minio_client": resources["minio_client"]}

    print("Materializing silver_de_transform...")
    t0 = time.time()
    result = materialize(
        assets=[silver_de_transform],
        resources=needed,
    )
    print(f"  done in {time.time() - t0:.1f}s, success={result.success}")
    if not result.success:
        for ev in result.get_asset_materialization_events():
            print(f"  {ev.asset_key}: {ev}")
        sys.exit(1)

    print("\nMaterializing silver_uk_transform...")
    t0 = time.time()
    result = materialize(
        assets=[silver_uk_transform],
        resources=needed,
    )
    print(f"  done in {time.time() - t0:.1f}s, success={result.success}")
    if not result.success:
        sys.exit(1)

    print("\nSmoke test complete.")


if __name__ == "__main__":
    main()

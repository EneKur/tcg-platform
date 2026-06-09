import io
import logging

import dagster as dg
import pyarrow.parquet as pq

from tcg_platform.resources.minio_client import MinioClientResource
from tcg_platform.defs.silver_transform import (
    _build_card_id_set,
    is_valid_card_id,
)

_LOG = logging.getLogger(__name__)

SILVER_BUCKET = "tcg-silver"
BRONZE_BUCKET = "tcg-bronze"


def _reconcile_region(minio_client: MinioClientResource, region: str) -> dict:
    """Re-validate each parquet in tcg-silver/quarantine/{region}/.

    For each, check the row's card_id against the current tcg-bronze/cards/
    set. If valid, batch-delete the parquet. Otherwise leave it.

    Returns a dict with keys: scanned, promoted_count, still_quarantined_count,
    read_errors, promoted (list of {path, card_id}).
    """
    valid_card_ids = _build_card_id_set(minio_client, BRONZE_BUCKET)
    quarantine_prefix = f"quarantine/{region}/"
    quarantined_paths = list(
        minio_client.list_objects(SILVER_BUCKET, prefix=quarantine_prefix)
    )

    promoted: list[dict] = []
    still_quarantined = 0
    read_errors = 0
    to_delete: list[str] = []

    for path in quarantined_paths:
        try:
            data = minio_client.get_object(SILVER_BUCKET, path)
            table = pq.read_table(io.BytesIO(data))
        except Exception as e:
            _LOG.warning(f"Reconcile: failed to read {path}: {e}")
            read_errors += 1
            continue

        if table.num_rows == 0:
            # Empty file: cleanup, no re-validation needed.
            to_delete.append(path)
            continue

        card_id = table.column("card_id").to_pylist()[0]
        if is_valid_card_id(card_id, valid_card_ids):
            to_delete.append(path)
            promoted.append({"path": path, "card_id": card_id})
            _LOG.info(f"Reconcile: promoted {card_id} ({path})")
        else:
            still_quarantined += 1

    if to_delete:
        from minio.deleteobjects import DeleteObject
        minio_client.remove_objects(
            SILVER_BUCKET, [DeleteObject(name=p) for p in to_delete]
        )

    return {
        "scanned": len(quarantined_paths),
        "promoted_count": len(promoted),
        "still_quarantined_count": still_quarantined,
        "read_errors": read_errors,
        "promoted": promoted,
    }


@dg.asset(required_resource_keys={"minio_client"})
def reconcile_quarantine_de(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Re-validate tcg-silver/quarantine/de/ rows against the current
    tcg-bronze/cards/ set. Deletes files whose card_id is now valid; the
    next silver_de_pipeline run will re-evaluate the corresponding bronze
    rows and write them to tcg-silver/data/de/."""
    minio_client: MinioClientResource = context.resources.minio_client
    result = _reconcile_region(minio_client, "de")
    context.log.info(
        f"DE reconcile: scanned={result['scanned']} "
        f"promoted={result['promoted_count']} "
        f"still_quarantined={result['still_quarantined_count']} "
        f"read_errors={result['read_errors']}"
    )
    return dg.MaterializeResult(
        metadata={
            "scanned": result["scanned"],
            "promoted_count": result["promoted_count"],
            "still_quarantined_count": result["still_quarantined_count"],
            "read_errors": result["read_errors"],
            "promoted_card_ids": dg.MetadataValue.json(
                [p["card_id"] for p in result["promoted"]]
            ),
        }
    )


@dg.asset(required_resource_keys={"minio_client"})
def reconcile_quarantine_uk(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Re-validate tcg-silver/quarantine/uk/ rows against the current
    tcg-bronze/cards/ set. See reconcile_quarantine_de for details."""
    minio_client: MinioClientResource = context.resources.minio_client
    result = _reconcile_region(minio_client, "uk")
    context.log.info(
        f"UK reconcile: scanned={result['scanned']} "
        f"promoted={result['promoted_count']} "
        f"still_quarantined={result['still_quarantined_count']} "
        f"read_errors={result['read_errors']}"
    )
    return dg.MaterializeResult(
        metadata={
            "scanned": result["scanned"],
            "promoted_count": result["promoted_count"],
            "still_quarantined_count": result["still_quarantined_count"],
            "read_errors": result["read_errors"],
            "promoted_card_ids": dg.MetadataValue.json(
                [p["card_id"] for p in result["promoted"]]
            ),
        }
    )


reconcile_quarantine_de_job = dg.define_asset_job(
    name="reconcile_quarantine_de_job",
    selection=["reconcile_quarantine_de"],
    description="Re-validate DE quarantined silver rows against the current card set.",
)

reconcile_quarantine_uk_job = dg.define_asset_job(
    name="reconcile_quarantine_uk_job",
    selection=["reconcile_quarantine_uk"],
    description="Re-validate UK quarantined silver rows against the current card set.",
)

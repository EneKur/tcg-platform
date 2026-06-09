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

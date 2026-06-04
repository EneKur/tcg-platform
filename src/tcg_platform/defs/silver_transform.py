import io
import re
import logging

import dagster as dg
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StructType, StructField, StringType, DoubleType


_LOG = logging.getLogger(__name__)

_BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("card_version", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("sold_date", StringType(), True),
    StructField("scraped_from", StringType(), True),
    StructField("source", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("language", StringType(), True),
    StructField("scraped_at", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("title", StringType(), True),
])

_VALID_SET_RE = re.compile(r"(OP\d+|EB\d+|ST\d+|PRB\d+|P\d+)", re.IGNORECASE)


def _normalize_extracted_id(extracted: str) -> str | None:
    """Normalize extracted card_id: add hyphen between set and number.

    ST13003 -> ST13-003 (5 digits: first 2 = set number, last 3 = card number)
    OP07029 -> OP07-029
    ST08005 -> ST08-005
    OP02030 -> OP02-030
    ST10 -> ST10 (pure set code, no number)
    OP01 -> OP01
    EB01-001 -> EB01-001 (already normalized)
    P014 -> P-014
    P-014 -> P-014 (already normalized)
    """
    if not extracted:
        return None
    upper = extracted.upper()
    if upper.startswith("P") and len(upper) >= 2:
        set_code = "P"
        number = upper[1:]
        if number.isdigit() and len(number) == 3:
            return f"P-{number}"
        return upper
    if upper[:3] == "PRB":
        set_code = "PRB"
        number = upper[3:]
    else:
        set_code = upper[:2]
        number = upper[2:]
    if not number.isdigit():
        return upper
    if len(number) == 3:
        return f"{set_code}-{number}"
    if len(number) == 4:
        return f"{set_code[:2]}-{number}"
    if len(number) == 5:
        return f"{set_code[:2]}{number[:2]}-{number[2:]}"
    return upper


def _is_complete_card_id(extracted: str) -> bool:
    """Check if extracted card_id has a number component (not just a set code)."""
    if not extracted:
        return False
    upper = extracted.upper()
    if upper[:3] == "PRB":
        return len(upper) > 3 and upper[3:].isdigit()
    if upper.startswith("P"):
        return len(upper) > 1 and upper[1:].isdigit()
    return len(upper) > 2 and upper[2:].isdigit()


def _build_card_id_set(minio_client, bucket: str) -> set[str]:
    """Read valid card_ids from MinIO cards/ directory structure.

    Files are at cards/{set_code}/{card_id}.webp
    e.g. cards/OP01/OP01-001.webp -> card_id = OP01-001

    Handles both MinioClientResource (returns strings) and raw Minio client (returns Object).
    """
    raw_list = minio_client.list_objects(bucket, prefix="cards/")
    cards = list(raw_list)
    card_ids: set[str] = set()
    for obj in cards:
        if isinstance(obj, str):
            object_name = obj
        else:
            object_name = getattr(obj, "object_name", None)
            if object_name is None:
                continue
        parts = object_name.split("/")
        if len(parts) == 3:
            filename = parts[2]
            for variant_suffix in ["_v1", "_v2", "_v3", "_v4"]:
                if filename.endswith(variant_suffix + ".webp"):
                    filename = filename[: -(len(variant_suffix) + 5)]
                    break
            else:
                if filename.endswith(".webp"):
                    filename = filename[:-5]
            card_ids.add(filename.upper())
    _LOG.info(f"Loaded {len(card_ids)} valid card_ids from MinIO")
    return card_ids


def _get_object_names(minio_client, bucket: str, prefix: str) -> list[str]:
    """Get object names from MinIO, handling both string and Object types."""
    raw_list = minio_client.list_objects(bucket, prefix=prefix)
    names = []
    for obj in list(raw_list):
        if isinstance(obj, str):
            names.append(obj)
        else:
            name = getattr(obj, "object_name", None)
            if name:
                names.append(name)
    return names


def _cleanup_legacy_aggregated_files(minio_client, region: str) -> None:
    """Delete the old aggregated data.parquet files (one-time cleanup).

    On the first run after this change, the silver bucket still contains
    the legacy aggregated files:
        tcg-silver/data/{region}/data.parquet
        tcg-silver/quarantine/{region}/data.parquet

    Delete them so only per-item-id files remain. Subsequent runs are
    no-ops because the files are already gone.
    """
    from minio.deleteobjects import DeleteObject

    legacy_paths = [
        f"data/{region.lower()}/data.parquet",
        f"quarantine/{region.lower()}/data.parquet",
    ]
    for path in legacy_paths:
        try:
            existing = minio_client.list_objects("tcg-silver", prefix=path)
        except Exception as e:
            _LOG.warning(f"Cleanup list failed for {path}: {e}")
            continue
        if not existing:
            continue
        to_delete = [DeleteObject(name) for name in existing]
        try:
            minio_client.remove_objects("tcg-silver", to_delete)
            _LOG.info(f"Deleted legacy {len(to_delete)} file(s) at {path}")
        except Exception as e:
            _LOG.warning(f"Cleanup remove failed for {path}: {e}")


def _write_parquet(minio_client, table: pa.Table, dest_prefix: str) -> int:
    """Write a PyArrow Table as a single parquet file to MinIO tcg-silver bucket."""
    if table.num_rows == 0:
        return 0

    buf = io.BytesIO()
    pq.write_table(table, buf, use_dictionary=False)
    data = buf.getvalue()

    object_name = f"{dest_prefix}/data.parquet"
    minio_client.put_object(
        bucket_name="tcg-silver",
        object_name=object_name,
        data=data,
        length=len(data),
        content_type="application/parquet",
    )
    return table.num_rows


def _run_silver_transform(spark, minio_client, region: str) -> dict:
    """Read bronze parquets, validate card_ids via Spark, write to silver bucket."""
    valid_card_ids = _build_card_id_set(minio_client, "tcg-bronze")

    def make_is_valid(card_ids_set):
        def is_valid(cid: str) -> bool:
            if not cid:
                return False
            m = _VALID_SET_RE.search(cid)
            if not m:
                return False
            extracted = m.group(0).upper()
            if not _is_complete_card_id(extracted):
                return False
            normalized = _normalize_extracted_id(extracted)
            if normalized and normalized in card_ids_set:
                return True
            if extracted in card_ids_set:
                return True
            return False
        return is_valid

    is_valid_fn = make_is_valid(valid_card_ids)
    spark.udf.register("is_valid_card_id", is_valid_fn, BooleanType())

    def make_extract(cid):
        if not cid: return None
        m = _VALID_SET_RE.search(cid)
        return m.group(0).upper() if m else None

    extract_fn = make_extract
    spark.udf.register("extract_card_id", extract_fn, StringType())

    prefix = f"sold_data/{region}/"
    object_names = _get_object_names(minio_client, "tcg-bronze", prefix)
    if not object_names:
        _LOG.info(f"No bronze parquets found for region {region}")
        return {"valid": 0, "quarantine": 0}

    _LOG.info(f"Reading {len(object_names)} bronze parquets for {region}")

    all_rows = []
    for obj_name in object_names:
        try:
            data = minio_client.get_object("tcg-bronze", obj_name)
            table = pq.read_table(io.BytesIO(data))
            d = {k: list(v) for k, v in table.to_pydict().items()}
            for i in range(table.num_rows):
                row = tuple(d.get(col, [None])[i] if d.get(col) else None for col in _BRONZE_SCHEMA.names)
                all_rows.append(row)
        except Exception as e:
            _LOG.warning(f"Failed to read {obj_name}: {e}")
            continue

    if not all_rows:
        return {"valid": 0, "quarantine": 0}

    bronze_df = spark.createDataFrame(all_rows, schema=_BRONZE_SCHEMA)
    bronze_df.createOrReplaceTempView("bronze_raw")

    valid_df = spark.sql("""
        SELECT * FROM bronze_raw
        WHERE is_valid_card_id(card_id)
    """)
    quarantine_df = spark.sql("""
        SELECT * FROM bronze_raw
        WHERE NOT is_valid_card_id(card_id)
    """)

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    _LOG.info(f"[{region}] Valid: {valid_count}, Quarantine: {quarantine_count}")

    sample_rows = []
    if valid_count > 0:
        valid_pdf = valid_df.toPandas()
        _LOG.info(f"Valid PDF dtypes: {dict(valid_pdf.dtypes)}")
        for col in valid_pdf.columns:
            if valid_pdf[col].dtype == object:
                valid_pdf[col] = valid_pdf[col].fillna("")
            elif valid_pdf[col].dtype == "float64" or valid_pdf[col].dtype.name.startswith("float"):
                valid_pdf[col] = valid_pdf[col].fillna(0.0)
        valid_pa = pa.Table.from_pandas(valid_pdf, preserve_index=False)
        _write_parquet(minio_client, valid_pa, f"data/{region.lower()}")
        sample_rows = valid_pdf.head(5).to_dict(orient="records")

    if quarantine_count > 0:
        quarantine_pdf = quarantine_df.toPandas()
        for col in quarantine_pdf.columns:
            if quarantine_pdf[col].dtype == object:
                quarantine_pdf[col] = quarantine_pdf[col].fillna("")
            elif quarantine_pdf[col].dtype == "float64" or quarantine_pdf[col].dtype.name.startswith("float"):
                quarantine_pdf[col] = quarantine_pdf[col].fillna(0.0)
        quarantine_pa = pa.Table.from_pandas(quarantine_pdf, preserve_index=False)
        _write_parquet(minio_client, quarantine_pa, f"quarantine/{region.lower()}")

    return {"valid": valid_count, "quarantine": quarantine_count, "sample": sample_rows}


@dg.asset(required_resource_keys={"minio_client"})
def silver_de_transform(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Transform DE bronze parquets into silver layer.

    Valid card_ids (found in tcg-bronze/cards/) -> tcg-silver/data/de/
    Invalid card_ids -> tcg-silver/quarantine/de/
    """
    minio_client = context.resources.minio_client

    try:
        active = SparkSession.getActiveSession()
        if active:
            active.stop()
    except Exception:
        pass

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    addr, port = server.listening_address
    context.log.info(f"SparkConnectServer started at sc://localhost:{port}")

    spark = None
    try:
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").appName("silver_de_transform").getOrCreate()
        context.log.info("Connected to Spark")
        result = _run_silver_transform(spark, minio_client, "DE")
        context.log.info(f"DE transform done: {result}")
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
        server.stop()

    sample_meta = {}
    if result.get("sample"):
        for i, row in enumerate(result["sample"][:3]):
            for col, val in row.items():
                sample_meta[f"sample_{i+1}.{col}"] = str(val) if val else ""

    return dg.MaterializeResult(
        metadata={
            "valid_records": result["valid"],
            "quarantined_records": result["quarantine"],
            **sample_meta,
        }
    )


@dg.asset(required_resource_keys={"minio_client"})
def silver_uk_transform(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Transform UK bronze parquets into silver layer.

    Valid card_ids (found in tcg-bronze/cards/) -> tcg-silver/data/uk/
    Invalid card_ids -> tcg-silver/quarantine/uk/
    """
    minio_client = context.resources.minio_client

    try:
        active = SparkSession.getActiveSession()
        if active:
            active.stop()
    except Exception:
        pass

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    addr, port = server.listening_address
    context.log.info(f"SparkConnectServer started at sc://localhost:{port}")

    spark = None
    try:
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").appName("silver_uk_transform").getOrCreate()
        context.log.info("Connected to Spark")
        result = _run_silver_transform(spark, minio_client, "UK")
        context.log.info(f"UK transform done: {result}")
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
        server.stop()

    sample_meta = {}
    if result.get("sample"):
        for i, row in enumerate(result["sample"][:3]):
            for col, val in row.items():
                sample_meta[f"sample_{i+1}.{col}"] = str(val) if val else ""

    return dg.MaterializeResult(
        metadata={
            "valid_records": result["valid"],
            "quarantined_records": result["quarantine"],
            **sample_meta,
        }
    )
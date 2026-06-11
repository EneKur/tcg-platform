import os

from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext
from dotenv import load_dotenv
from pydantic import model_validator

from tcg_platform.resources.minio_client import MinioClientResource

load_dotenv()


def _get_minio_config(prefix: str = "MINIO") -> dict:
    return {
        "endpoint": os.getenv(f"{prefix}_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv(f"{prefix}_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv(f"{prefix}_SECRET_KEY", "minioadmin"),
        "bucket_name": os.getenv(f"{prefix}_BUCKET", "tcg-bronze"),
        "secure": False,
    }


def _get_raw_config() -> dict:
    """Read RAW_* env vars with 'tcg-raw' as the default bucket name.

    Mirrors _get_minio_config but does not reuse it, because the helper
    hardcodes 'tcg-bronze' as its default bucket. This is the entry
    point for the tcg_raw_client resource.
    """
    return {
        "endpoint": os.getenv("RAW_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv("RAW_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("RAW_SECRET_KEY", "minioadmin"),
        "bucket_name": os.getenv("RAW_BUCKET", "tcg-raw"),
        "secure": False,
    }


@resource
def minio_client(init_context: InitResourceContext):
    config = _get_minio_config()
    client = MinioClientResource(**config)
    return client.create_resource(init_context)


@resource
def minio_client_zyte(init_context: InitResourceContext):
    config = _get_minio_config(prefix="ZYTE_MINIO")
    client = MinioClientResource(**config)
    return client.create_resource(init_context)


@resource
def tcg_raw_client(init_context: InitResourceContext):
    config = _get_raw_config()
    client = MinioClientResource(**config)
    return client.create_resource(init_context)
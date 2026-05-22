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


@resource
def minio_client(init_context: InitResourceContext):
    config = _get_minio_config()
    return MinioClientResource(**config)


@resource
def minio_client_zyte(init_context: InitResourceContext):
    config = _get_minio_config(prefix="ZYTE_MINIO")
    return MinioClientResource(**config)
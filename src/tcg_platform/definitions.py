from pathlib import Path

from dagster import definitions, load_from_defs_folder

from tcg_platform.defs.minio_resources import (
    minio_client,
)
from tcg_platform.defs.sqlite_resources import (
    sqlite_client_de,
    sqlite_client_uk,
    sqlite_client_us,
)
from tcg_platform.defs.zyte_resources import (
    zyte_client,
)


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent).with_resources(
        {
            "minio_client": minio_client,
            "sqlite_client_de": sqlite_client_de,
            "sqlite_client_uk": sqlite_client_uk,
            "sqlite_client_us": sqlite_client_us,
            "zyte_client": zyte_client,
        }
    )
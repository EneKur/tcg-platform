from pathlib import Path

from dagster import definitions, load_from_defs_folder

from tcg_platform.defs.steel_resources import (
    steel_session_ebay,
    steel_session_pricecharting,
    steel_session_limitlesstcg,
)


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent).with_resources(
        {
            "steel_session_ebay": steel_session_ebay,
            "steel_session_pricecharting": steel_session_pricecharting,
            "steel_session_limitlesstcg": steel_session_limitlesstcg,
        }
    )
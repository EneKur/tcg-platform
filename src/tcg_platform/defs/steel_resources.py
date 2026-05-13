from dagster import resource
from tcg_platform.resources.steel_session import SteelSessionResource


@resource
def steel_session_ebay(init_context):
    return SteelSessionResource(site_name="ebay")


@resource
def steel_session_pricecharting(init_context):
    return SteelSessionResource(site_name="pricecharting")


@resource
def steel_session_limitlesstcg(init_context):
    return SteelSessionResource(site_name="limitlesstcg")
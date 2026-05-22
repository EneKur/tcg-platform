from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext
from dotenv import load_dotenv
from zyte_api import ZyteAPI

load_dotenv()


@resource
def zyte_client(init_context: InitResourceContext):
    import os
    api_key = os.getenv("ZYTE_API_KEY")
    if not api_key:
        raise ValueError("ZYTE_API_KEY environment variable is not set")
    return ZyteAPI(api_key=api_key)
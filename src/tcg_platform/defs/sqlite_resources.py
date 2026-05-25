import os

from dagster import resource
from dagster._config.pythonic_config.resource import InitResourceContext
from dotenv import load_dotenv

from tcg_platform.resources.sqlite_client import SqliteClientResource

load_dotenv()


@resource
def sqlite_client_de(init_context: InitResourceContext):
    db_path = os.getenv("SQLITE_PATH_DE", "./data/tcg_de.db")
    client = SqliteClientResource(db_path=db_path)
    return client.create_resource(init_context)


@resource
def sqlite_client_uk(init_context: InitResourceContext):
    db_path = os.getenv("SQLITE_PATH_UK", "./data/tcg_uk.db")
    client = SqliteClientResource(db_path=db_path)
    return client.create_resource(init_context)
"""Module for entry points."""
import os
from typing import Any, Optional

import sqlalchemy as sa
import typer

from cads_broker import config, database, dispatcher, object_storage

app = typer.Typer()


@app.command()
def info(connection_string: Optional[str] = None) -> None:
    """Test connection to the database located at URI `connection_string`.

    Parameters
    ----------
    connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    connection.close()
    print("successfully connected to the broker database.")


@app.command()
def init_db(connection_string: Optional[str] = None, force: bool = False) -> None:
    """Create/update the database structure and the cache area in the object storage.

    Parameters
    ----------
    connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    force: if True, drop the database structure and build again from scratch
    """
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    database.init_database(connection_string, force=force)
    print("successfully created/updated the broker database structure.")

    # get storage parameters from environment
    for key in ("OBJECT_STORAGE_URL", "STORAGE_ADMIN", "STORAGE_PASSWORD"):
        if key not in os.environ:
            msg = (
                "key %r must be defined in the environment in order to use the object storage"
                % key
            )
            raise KeyError(msg)
    object_storage_url = os.environ["OBJECT_STORAGE_URL"]
    storage_kws: dict[str, Any] = {
        "aws_access_key_id": os.environ["STORAGE_ADMIN"],
        "aws_secret_access_key": os.environ["STORAGE_PASSWORD"],
    }
    object_storage.create_download_bucket(
        os.environ.get("CACHE_BUCKET", "cache"), object_storage_url, **storage_kws
    )
    print("successfully created the cache area in the object storage.")


@app.command()
def run(
    address: str = "scheduler:8786",
) -> None:
    """Start the broker.

    Parameters
    ----------
    max_running_requests: maximum number of requests to run in parallel
    scheduler_address: address of the scheduler
    """
    broker = dispatcher.Broker.from_address(address=address)
    broker.run()


def main() -> None:
    """Run main broker entry points."""
    app()

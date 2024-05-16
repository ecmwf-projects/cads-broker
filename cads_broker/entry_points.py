"""Module for entry points."""

import datetime
import os
import random
import sqlite3
import uuid
from enum import Enum
from typing import Any, Optional

import sqlalchemy as sa
import typer
from typing_extensions import Annotated

from cads_broker import config, database, dispatcher, object_storage

app = typer.Typer()


@app.command()
def add_dummy_requests(
    number_of_requests: int, requests_db: str, number_of_users: int = 1000
) -> None:
    connection = sqlite3.connect(requests_db)
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(f"SELECT * FROM broker limit {number_of_requests}")
    user_uids = [str(uuid.uuid4()) for _ in range(number_of_users)]
    with database.ensure_session_obj(None)() as session:
        for i, row in enumerate(cursor):
            if row["elapsed"] != "null":
                database.ensure_adaptor_properties(
                    hash="test",
                    config={},
                    form={},
                    session=session,
                )
                request = database.SystemRequest(
                    request_uid=str(uuid.uuid4()),
                    process_id="test-adaptor-dummy",
                    user_uid=random.choice(user_uids),
                    status="accepted",
                    request_body={
                        "setup_code": None,
                        "request": {
                            "elapsed": str(datetime.timedelta(seconds=row["elapsed"])),
                            "timestamp": str(datetime.datetime.now()),
                        },
                    },
                    request_metadata={},
                    origin="api",
                    portal="c3s",
                    adaptor_properties_hash="test",
                    entry_point="cads_adaptors:DummyAdaptor",
                )
                session.add(request)
            if i % 100 == 0:
                session.commit()
        session.commit()


@app.command()
def requests_cleaner(
    connection_string: Optional[str] = None, older_than_days: Optional[int] = 365
) -> None:
    """Remove records from the system_requests table older than `older_than_days`.

    Parameters
    ----------
    connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    older_than_days: minimum age (in days) to consider a record to be removed
    """
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    engine = sa.create_engine(connection_string)
    time_delta = datetime.datetime.now() - datetime.timedelta(days=older_than_days)
    # clean system requests and (via cascading delete) events
    with engine.begin() as conn:
        database.logger.info("deleting old system_requests and events...")
        stmt = sa.delete(database.SystemRequest).where(
            database.SystemRequest.created_at <= time_delta
        )
        result = conn.execute(stmt)
        conn.commit()
        num_requests_deleted = result.rowcount
        database.logger.info(
            f"{num_requests_deleted} old system requests "
            f"successfully removed from the broker database."
        )
    # clean adaptor_properties
    with engine.begin() as conn:
        try:
            database.logger.info("deleting old adaptor_properties...")
            stmt_ap_delete = sa.delete(database.AdaptorProperties).where(
                database.AdaptorProperties.timestamp <= time_delta
            )
            result = conn.execute(stmt_ap_delete)
            conn.commit()
            num_ap_deleted = result.rowcount
            database.logger.info(
                f"{num_ap_deleted} old adaptor properties "
                f"successfully removed from the broker database."
            )
            return
        except sa.exc.IntegrityError:
            database.logger.error(
                "cannot remove some old records from table adaptor_properties."
            )
            raise


class RequestStatus(str, Enum):
    """Enum for request status."""

    running = "running"
    accepted = "accepted"


@app.command()
def delete_requests(
    status: RequestStatus = RequestStatus.running,
    connection_string: Optional[str] = None,
    minutes: float = typer.Option(0.0),
    seconds: float = typer.Option(0.0),
    hours: float = typer.Option(0.0),
    days: float = typer.Option(0.0),
    skip_confirmation: Annotated[bool, typer.Option("--yes", "-y")] = False,
) -> None:
    """Remove records from the system_requests table that are in the specified status.

    Parameters
    ----------
    connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    timestamp = datetime.datetime.now() - datetime.timedelta(
        minutes=minutes, seconds=seconds, hours=hours, days=days
    )
    with database.ensure_session_obj(None)() as session:
        database.logger.info(f"deleting {status} system_requests before {timestamp}.")
        statement = (
            sa.delete(database.SystemRequest)
            .where(database.SystemRequest.status == status)
            .where(database.SystemRequest.created_at < timestamp)
        )
        number_of_requests = session.execute(statement).rowcount
        if not skip_confirmation:
            if not typer.confirm(
                f"Deleting {number_of_requests} {status} requests. Do you want to continue?",
                abort=True,
                default=True,
            ):
                typer.echo("Operation cancelled.")
                return
        session.commit()
        typer.echo(f"{number_of_requests} requests successfully removed from the broker database.")


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

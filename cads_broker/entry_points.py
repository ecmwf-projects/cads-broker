"""Module for entry points."""

import datetime
import enum
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional

import prettytable
import sqlalchemy as sa
import typer
from typing_extensions import Annotated

from cads_broker import config, database, dispatcher, object_storage

app = typer.Typer()


@app.command()
def add_dummy_requests(
    number_of_requests: int = 1000, number_of_users: int = 100, max_length: int = 60
) -> None:
    user_uids = [str(uuid.uuid4()) for _ in range(number_of_users)]
    with database.ensure_session_obj(None)() as session:
        database.ensure_adaptor_properties(
            hash="test",
            config={},
            form={},
            session=session,
        )
        for i in range(number_of_requests):
            request = database.SystemRequest(
                request_uid=str(uuid.uuid4()),
                process_id="test-adaptor-dummy",
                user_uid=random.choice(user_uids),
                # avoid using the same timestamp for all requests
                created_at=datetime.datetime.now()
                - datetime.timedelta(seconds=number_of_requests - i),
                status="accepted",
                request_body={
                    "setup_code": None,
                    "request": {
                        "elapsed": str(
                            datetime.timedelta(seconds=random.randint(0, max_length))
                        ),
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

            print(f"Added request {i} with request_uid {request.request_uid}")

        # Commit all at the end
        session.commit()
        print(f"Committed {number_of_requests} requests")


@app.command()
def requests_cleaner(
    connection_string: Optional[str] = None,
    older_than_days: Optional[int] = 365,
    delete_bulk_size: Optional[int] = None,
    delete_sleep_time: Optional[int] = None,
) -> None:
    """Remove records from the system_requests table older than `older_than_days`."""
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    engine = sa.create_engine(connection_string)
    time_delta = datetime.datetime.now() - datetime.timedelta(days=older_than_days)
    # clean system requests and (via cascading delete) events
    database.logger.info("deleting old system_requests and events...")
    curr_deleted = 1
    subquery = sa.select(database.SystemRequest.request_uid).where(
        database.SystemRequest.created_at <= time_delta
    )
    with engine.connect() as conn:
        if delete_bulk_size is not None:
            # delete in sized bulks to give time to db replicas for synch
            subquery = subquery.limit(delete_bulk_size)
        stmt = sa.delete(database.SystemRequest).where(
            database.SystemRequest.request_uid.in_(subquery)
        )
        while curr_deleted:
            with conn.begin():
                result = conn.execute(stmt)
                conn.commit()
                curr_deleted = result.rowcount
            database.logger.info(
                f"{curr_deleted} old system requests "
                f"successfully removed from the broker database."
            )
            if delete_sleep_time is not None:
                time.sleep(delete_sleep_time)
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


@app.command()
def list_request_uids(
    query: str,
    output_file: Annotated[Path, typer.Argument(file_okay=True, dir_okay=False)]
    | None = Path("request_uids.txt"),
) -> None:
    """List request_uids from the system_requests table."""
    with database.ensure_session_obj(None)() as session:
        result = session.execute(
            sa.text(f"select request_uid from system_requests where {query}")
        )
        with output_file.open("w") as f:
            for row in result:
                f.write(str(row[0]) + "\n")
    print(f"successfully wrote {result.rowcount} request_uids to {output_file}")


class RequestStatus(str, enum.Enum):
    """Enum for request status."""

    running = "running"
    accepted = "accepted"


@app.command()
def get_dynamic_priority(
    request_uid: Optional[str] = None,
    request_uids_file: Annotated[
        Path, typer.Argument(exists=True, file_okay=True, dir_okay=False)
    ]
    | None = None,
    interval: float = 24 * 60 * 60,
    origin: Optional[str] = None,
    resource_mul: float = -1.0,
    last_completed_mul: float = 0.8,
):
    with database.ensure_session_obj(None)() as session:
        users_resources = database.get_users_queue_from_processing_time(
            session=session,
            interval_stop=datetime.datetime.now(),
            interval=datetime.timedelta(hours=interval / 60 / 60),
            origin=origin,
        )
        if request_uid:
            request_uids = [request_uid]
        elif request_uids_file:
            request_uids = request_uids_file.open().read().splitlines()
        table = prettytable.PrettyTable(
            [
                "user_uid",
                "request_uid",
                "process_id",
                "entry_point",
                "user_resources_used",
                "user_last_completed_request",
                "priority",
            ]
        )
        for request_uid in request_uids:
            request = database.get_request(request_uid, session)
            resources = users_resources[request.user_uid]
            last_completed_request = database.user_last_completed_request(
                session, request.user_uid, interval
            )
            table.add_row(
                [
                    request.user_uid,
                    request_uid,
                    request.process_id,
                    request.entry_point,
                    resources,
                    last_completed_request,
                    resource_mul * resources
                    + last_completed_mul * last_completed_request,
                ]
            )
        typer.echo(table)


@app.command()
def delete_requests(
    status: RequestStatus = RequestStatus.running,
    user_uid: Optional[str] = None,
    request_uid: Optional[str] = None,
    request_uids_file: Annotated[
        Path, typer.Argument(exists=True, file_okay=True, dir_okay=False)
    ]
    | None = None,
    connection_string: Optional[str] = None,
    minutes: float = 0,
    seconds: float = 0,
    hours: float = 0,
    days: float = 0,
    message: Optional[str] = "The request has been dismissed by the administrator.",
    skip_confirmation: Annotated[bool, typer.Option("--yes", "-y")] = False,
) -> None:
    """Set the status of records in the system_requests table to 'dismissed'."""
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    timestamp = datetime.datetime.now() - datetime.timedelta(
        minutes=minutes, seconds=seconds, hours=hours, days=days
    )
    with database.ensure_session_obj(None)() as session:
        if request_uids_file:
            with request_uids_file.open() as f:
                request_uids = f.read().splitlines()
            statement = sa.update(database.SystemRequest).where(
                database.SystemRequest.request_uid.in_(request_uids)
            )
        else:
            statement = (
                sa.update(database.SystemRequest)
                .where(database.SystemRequest.status == status)
                .where(database.SystemRequest.created_at < timestamp)
            )
        if user_uid:
            statement = statement.where(database.SystemRequest.user_uid == user_uid)
        if request_uid:
            statement = statement.where(
                database.SystemRequest.request_uid == request_uid
            )
        statement = statement.values(
            status="dismissed",
            request_metadata={
                "dismission": {
                    "reason": "PermissionError",
                    "message": message,
                    "previous_status": status,
                }
            },
        )
        number_of_requests = session.execute(statement).rowcount
        if not skip_confirmation:
            if not typer.confirm(
                f"Setting status to 'dismissed' for {number_of_requests} requests. "
                "Do you want to continue?",
                abort=True,
                default=True,
            ):
                typer.echo("Operation cancelled.")
                return
        session.commit()
        typer.echo(
            f"Status set to 'dismissed' for {number_of_requests} requests in the broker database."
        )


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
    database.logger.info("starting creation/updating of broker db structure and storage cache area.")
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    database.init_database(connection_string, force=force)
    database.logger.info("successfully created/updated the broker database structure.")

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
    download_buckets: List[str] = object_storage.parse_data_volumes_config()
    for download_bucket in download_buckets:
        if download_bucket.startswith("s3://"):
            object_storage.create_download_bucket(
                download_bucket, object_storage_url, **storage_kws
            )
    database.logger.info("successfully created the cache areas in the object storage.")
    database.logger.info("end of creation/updating of broker db structure and storage cache area.")


@app.command()
def run(
    scheduler_url: List[str] = ["scheduler:8786"],
) -> None:
    """Start the broker.

    Parameters
    ----------
    scheduler_url: schedulers' urls
    """
    typer.echo(
        f"Starting the broker with {len(scheduler_url)} scheduler(s) at {scheduler_url}"
    )
    broker = dispatcher.Broker.from_urls(scheduler_url=scheduler_url)
    broker.run()


def main() -> None:
    """Run main broker entry points."""
    app()

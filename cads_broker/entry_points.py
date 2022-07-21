"""Module for entry points."""
import json
import pathlib

import sqlalchemy as sa
import typer

from cads_broker import database, dispatcher

app = typer.Typer()


@app.command()
def info(connection_string: str) -> None:
    """
    Test connection to the database located at URI `connection_string`.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    connection.close()
    print("successfully connected to the broker database.")


@app.command()
def init_db(
    connection_string: str = typer.Argument(
        None, help="Connection string to the broker database"
    )
) -> None:
    """
    Create the database structure.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    if connection_string is None:
        connection_string = database.dbsettings.connection_string
    database.init_database(connection_string)
    print("successfully created the broker database structure.")


@app.command()
def run(
    max_running_requests: int = 4,
    address: str = "scheduler:8786",
) -> None:
    """
    Start the broker.

    :param max_running_requests: maximum number of requests to run in parallel
    :param scheduler_address: address of the scheduler
    """
    broker = dispatcher.Broker.from_address(
        address=address, max_running_requests=max_running_requests
    )
    broker.run()


@app.command()
def add_system_request(
    file_path: pathlib.Path = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        # resolve_path=True,
        help="Path to the file containing the JSON request to the Broker",
    ),
    context: str = typer.Option(None, help="Context of the request"),
    callable_call: str = typer.Option(None, help="Call to the callable"),
    connection_string: str = typer.Option(
        None, help="Connection string to the broker database"
    ),
) -> None:
    """
    Add a system request to the database.

    :param seconds: number of seconds to sleep
    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    if connection_string is None:
        connection_string = database.dbsettings.connection_string
    engine = sa.create_engine(connection_string)
    session_obj = sa.orm.sessionmaker(engine)
    if file_path is not None:
        print(file_path)
        database.create_request(
            process_id="submit-workflow",
            session_obj=session_obj,
            **json.loads(file_path.read_text())
        )
    elif context is not None and callable_call is not None:
        database.create_request(
            process_id="submit-workflow",
            context=context,
            callable_call=callable_call,
            session_obj=session_obj,
        )
    else:
        raise ValueError(
            "either file_path or context and callable_call must be provided"
        )


def main() -> None:
    """Run main broker entry points."""
    app()

"""module for entry points."""
import sqlalchemy as sa
import typer

from cads_broker import config, database, dispatcher

app = typer.Typer()


@app.command()
def info(connection_string: str | None = None) -> None:
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
def init_db(connection_string: str | None = None) -> None:
    """Create the database structure.

    Parameters
    ----------
    connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    if not connection_string:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    database.init_database(connection_string)
    print("successfully created the broker database structure.")


@app.command()
def run(
    max_running_requests: int = 4,
    scheduler_address: str = "scheduler:8786",
) -> None:
    """Start the broker.

    Parameters
    ----------
    max_running_requests: maximum number of requests to run in parallel
    scheduler_address: address of the scheduler
    """
    broker = dispatcher.Broker(  # type: ignore
        max_running_requests=max_running_requests, scheduler_address=scheduler_address
    )
    broker.run()


@app.command()
def add_system_request(
    seconds: int,
    connection_string: str | None = None,
) -> None:
    """Add a system request to the database.

    Parameters
    ----------
    seconds: number of seconds to sleep
    connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    if connection_string is None:
        dbsettings = config.ensure_settings(config.dbsettings)
        connection_string = dbsettings.connection_string
    engine = sa.create_engine(connection_string)
    session_obj = sa.orm.sessionmaker(engine)
    database.create_request(seconds, session_obj)


def main() -> None:
    """Run main broker entry points."""
    app()

"""module for entry points"""
import sqlalchemy as sa
import typer

from cads_broker import database

app = typer.Typer()


@app.command()
def info(connection_string: str) -> None:
    """
    Test connection to the database located at URI `connection_string`

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    connection.close()
    print("successfully connected to the broker database.")


@app.command()
def init_db(connection_string: str) -> None:
    """
    Create the database structure

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    database.init_database(connection_string)
    print("successfully created the broker database structure.")


def main() -> None:
    """run main broker entry points"""
    app()

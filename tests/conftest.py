import os
from typing import Any

import pytest
from psycopg import Connection
from pytest_postgresql import factories
from sqlalchemy.orm import sessionmaker

from cads_broker import database

postgresql_proc2 = factories.postgresql_proc(password="my@strange@password")
postgresql2 = factories.postgresql("postgresql_proc2")


@pytest.fixture()
def session_obj(postgresql: Connection[str]) -> sessionmaker:
    """Init the test database and return a connection object."""
    connection_string = (
        f"postgresql+psycopg2://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )
    engine = database.init_database(connection_string, force=True)
    session_obj = sessionmaker(engine)
    return session_obj


@pytest.fixture()
def temp_environ() -> Any:
    """Create a modifiable environment that affect only the test scope."""
    old_environ = dict(os.environ)

    yield os.environ

    os.environ.clear()
    os.environ.update(old_environ)

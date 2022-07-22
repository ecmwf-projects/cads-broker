import os
from typing import Any

import pytest
from psycopg import Connection
from sqlalchemy.orm import sessionmaker

from cads_broker import database


@pytest.fixture()
def session_obj(postgresql: Connection[str]) -> sessionmaker:
    """Init the test database and return a connection object"""
    connection_string = (
        f"postgresql+psycopg2://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )
    engine = database.init_database(connection_string)
    session_obj = sessionmaker(engine)
    return session_obj


@pytest.fixture()
def temp_environ() -> Any:
    """Create a modifiable environment that affect only the test scope"""
    old_environ = dict(os.environ)

    yield os.environ

    os.environ.clear()
    os.environ.update(old_environ)

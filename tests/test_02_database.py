from typing import Any

import pytest
import sqlalchemy as sa
from psycopg import Connection
from sqlalchemy.orm import sessionmaker

from cads_broker import database


def test_init_database(postgresql: Connection[str]) -> None:
    connection_string = (
        f"postgresql+psycopg2://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )
    engine = sa.create_engine(connection_string)
    conn = engine.connect()
    query = (
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    )
    expected_tables_at_beginning: set[str] = set()
    expected_tables_complete = set(database.metadata.tables)
    assert set(conn.execute(query).scalars()) == expected_tables_at_beginning  # type: ignore

    database.init_database(connection_string)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore


def test_ensure_session_obj(
    postgresql: Connection[str], session_obj: sessionmaker, temp_environ: Any
) -> None:
    # case of session is already set
    ret_value = database.ensure_session_obj(session_obj)
    assert ret_value is session_obj

    # case of session not set
    with pytest.raises(ValueError) as excinfo:
        ret_value = database.ensure_session_obj(None)
        assert "postgres_password" in str(excinfo.value)
    temp_environ["postgres_password"] = postgresql.info.password
    ret_value = database.ensure_session_obj(None)
    assert isinstance(ret_value, sessionmaker)

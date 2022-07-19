from typing import Any

import sqlalchemy as sa
from psycopg import Connection

from cads_broker import config, database


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


def test_ensure_settings(session_obj: sa.orm.sessionmaker, temp_environ: Any) -> None:
    temp_environ["postgres_password"] = "apassword"

    # initially global settings is importable, but it is None
    assert database.dbsettings is None

    # at first run returns right connection and set global setting
    effective_settings = database.ensure_settings()
    assert (
        effective_settings.connection_string
        == "postgresql://broker:apassword@compute-db/broker"
    )
    assert database.dbsettings == effective_settings

    # setting a custom configuration works as well
    my_settings_dict = {
        "postgres_user": "monica",
        "postgres_password": "secret1",
        "postgres_host": "myhost",
        "postgres_dbname": "mybroker",
    }
    my_settings_connection_string = (
        "postgresql://%(postgres_user)s:%(postgres_password)s"
        "@%(postgres_host)s/%(postgres_dbname)s" % my_settings_dict
    )
    mysettings = config.SqlalchemySettings(**my_settings_dict)
    effective_settings = database.ensure_settings(mysettings)

    assert database.dbsettings == effective_settings
    assert effective_settings == mysettings
    assert effective_settings.connection_string == my_settings_connection_string

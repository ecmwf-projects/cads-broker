import sqlalchemy as sa
from psycopg import Connection

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

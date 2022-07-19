import os.path

import sqlalchemy as sa
from psycopg import Connection
from typer.testing import CliRunner

from cads_broker import database, entry_points

THIS_PATH = os.path.abspath(os.path.dirname(__file__))
TESTDATA_PATH = os.path.join(THIS_PATH, "data")
runner = CliRunner()


def test_init_db(postgresql: Connection[str]) -> None:
    connection_string = (
        f"postgresql://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )
    engine = sa.create_engine(connection_string)
    conn = engine.connect()
    query = (
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    )

    result = runner.invoke(
        entry_points.app, ["init-db", "--connection-string", connection_string]
    )

    assert result.exit_code == 0
    assert set(conn.execute(query).scalars()) == set(database.metadata.tables)  # type: ignore

    # uncomment to update testdb.sql
    # dump_path = os.path.join(TESTDATA_PATH, "testdb.sql")
    # with open(dump_path, "w") as dumped_file:
    #     ret = subprocess.call(["pg_dump", connection_string], stdout=dumped_file)
    # assert ret == 0
    # assert os.path.exists(dump_path)

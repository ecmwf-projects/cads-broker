from typing import Any

import cacholote
import sqlalchemy as sa
from psycopg import Connection
from typer.testing import CliRunner

from cads_broker import database, entry_points, object_storage

runner = CliRunner()


def test_init_db(postgresql: Connection[str], mocker) -> None:
    patch_storage = mocker.patch.object(object_storage, "create_download_bucket")
    connection_string = (
        f"postgresql://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )
    engine = sa.create_engine(connection_string)
    conn = engine.connect()
    query = sa.text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    )
    object_storage_url = "http://myobject-storage:myport/"
    object_storage_kws: dict[str, Any] = {
        "aws_access_key_id": "storage_user",
        "aws_secret_access_key": "storage_password",
    }
    result = runner.invoke(
        entry_points.app,
        ["init-db", "--connection-string", connection_string, "--force"],
        env={
            "OBJECT_STORAGE_URL": object_storage_url,
            "STORAGE_ADMIN": object_storage_kws["aws_access_key_id"],
            "STORAGE_PASSWORD": object_storage_kws["aws_secret_access_key"],
        },
    )
    assert result.exit_code == 0
    patch_storage.assert_called_once_with(
        "cache", object_storage_url, **object_storage_kws
    )
    assert set(conn.execute(query).scalars()) == set(
        database.BaseModel.metadata.tables
    ).union({"alembic_version"}).union(set(cacholote.database.Base.metadata.tables))
    conn.close()

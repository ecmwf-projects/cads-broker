import datetime
import uuid
from typing import Any

import cacholote
import sqlalchemy as sa
from psycopg import Connection
from typer.testing import CliRunner

from cads_broker import database, entry_points, object_storage

runner = CliRunner()


def mock_system_request(
    status: str | None = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
    process_id: str | None = "process_id",
    user_uid: str | None = None,
    cache_id: int | None = None,
    request_body: dict | None = None,
    request_metadata: dict | None = None,
    adaptor_properties_hash: str = "",
    entry_point: str = "entry_point",
    finished_at: datetime.datetime | None = None,
) -> database.SystemRequest:
    system_request = database.SystemRequest(
        request_uid=request_uid or str(uuid.uuid4()),
        process_id=process_id,
        status=status,
        user_uid=user_uid,
        created_at=created_at,
        started_at=None,
        cache_id=cache_id,
        request_body=request_body or {"request_type": "test"},
        request_metadata=request_metadata or {},
        adaptor_properties_hash=adaptor_properties_hash,
        entry_point=entry_point,
        finished_at=finished_at,
    )
    return system_request


def mock_config(hash: str = "", config: dict[str, Any] = {}, form: dict[str, Any] = {}):
    adaptor_properties = database.AdaptorProperties(
        hash=hash,
        config=config,
        form=form,
    )
    return adaptor_properties


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


def test_remove_old_records(session_obj: sa.orm.sessionmaker):
    now = sa.func.now()
    old_requests = []
    unfinished_requests = []
    recent_requests = []
    with session_obj() as session:
        adaptor_properties = mock_config()
        session.add(adaptor_properties)
        # create 5 "old" requests
        finished_at = now - datetime.timedelta(days=360)
        for request in range(5):
            request = mock_system_request(finished_at=finished_at)
            session.add(request)
            old_requests.append(request.request_uid)
        # create 5 unfinished requests
        finished_at = None
        for request in range(5):
            request = mock_system_request(finished_at=finished_at)
            session.add(request)
            unfinished_requests.append(request.request_uid)
        # create 5 "recent" requests
        finished_at = now - datetime.timedelta(days=3)
        for request in range(5):
            request = mock_system_request(finished_at=finished_at)
            session.add(request)
            recent_requests.append(request.request_uid)
        session.commit()
    connection_string = session_obj.kw["bind"].url
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        assert len(all_requests) == 15

    # remove nothing, older_than_days=365 by default, and oldest is 360
    result = runner.invoke(
        entry_points.app,
        ["remove-old-requests", "--connection-string", connection_string],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        assert len(all_requests) == 15

    # remove 360 day old requests
    result = runner.invoke(
        entry_points.app,
        [
            "remove-old-requests",
            "--connection-string",
            connection_string,
            "--older-than-days",
            "360",
        ],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        assert len(all_requests) == 10
        assert set([r.request_uid for r in all_requests]) == set(recent_requests) | set(
            unfinished_requests
        )

    # remove all but unfinished
    result = runner.invoke(
        entry_points.app,
        [
            "remove-old-requests",
            "--connection-string",
            connection_string,
            "--older-than-days",
            "2",
        ],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        assert len(all_requests) == 5
        assert set([r.request_uid for r in all_requests]) == set(unfinished_requests)

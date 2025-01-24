import datetime
import json
import logging
import uuid
from typing import Any

import cacholote
import pytest
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


def mock_config(
    hash: str = "",
    config: dict[str, Any] = {},
    form: dict[str, Any] = {},
    timestamp: datetime.datetime | None = None,
):
    adaptor_properties = database.AdaptorProperties(
        hash=hash,
        config=config,
        form=form,
        timestamp=timestamp,
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
    ).union(
        {
            "alembic_version",
            "alembic_version_cacholote",
        }
    ).union(set(cacholote.database.Base.metadata.tables))
    conn.close()


def prepare_db(
    session_obj,
    num_old_props_used_by_old,
    num_recent_props_used_by_recent,
    recent_days=3,
    old_days=370,
):
    now = sa.func.now()
    old_date = now - datetime.timedelta(days=old_days)
    recent_date = now - datetime.timedelta(days=recent_days)

    with session_obj() as session:
        # initialize
        session.query(database.SystemRequest).delete()
        session.query(database.AdaptorProperties).delete()
        session.commit()
        # add old properties with old requests
        for x in range(num_old_props_used_by_old):
            old_hash = f"old_hash_{x}"
            adaptor_properties = mock_config(hash=old_hash, timestamp=old_date)
            session.add(adaptor_properties)
            request = mock_system_request(
                adaptor_properties_hash=old_hash, created_at=old_date
            )
            session.add(request)
            event = database.Events(request_uid=request.request_uid)
            session.add(event)
            session.commit()
        # add recent properties with recent requests
        for x in range(num_recent_props_used_by_recent):
            new_hash = f"new_hash_{x}"
            adaptor_properties = mock_config(hash=new_hash, timestamp=recent_date)
            session.add(adaptor_properties)
            request = mock_system_request(
                adaptor_properties_hash=new_hash, created_at=recent_date
            )
            session.add(request)
            event = database.Events(request_uid=request.request_uid)
            session.add(event)
            session.commit()
        # not existing case of recent properties with old requests


def test_requests_cleaner(
    session_obj: sa.orm.sessionmaker, caplog: pytest.LogCaptureFixture
):
    connection_string = session_obj.kw["bind"].url

    # test remove nothing, older_than_days=365 by default
    prepare_db(
        session_obj,
        num_old_props_used_by_old=0,
        num_recent_props_used_by_recent=5,
    )
    result = runner.invoke(
        entry_points.app,
        ["requests-cleaner", "--connection-string", connection_string],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        all_events = session.query(database.Events).all()
        all_props = session.query(database.AdaptorProperties).all()
        assert len(all_requests) == 5
        assert len(all_events) == 5
        assert len(all_props) == 5

    # test remove all (most recent is 3 days old)
    result = runner.invoke(
        entry_points.app,
        [
            "requests-cleaner",
            "--connection-string",
            connection_string,
            "--older-than-days",
            "1",
        ],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        all_events = session.query(database.Events).all()
        all_props = session.query(database.AdaptorProperties).all()
        assert len(all_requests) == 0
        assert len(all_events) == 0
        assert len(all_props) == 0

    # test remove only some requests (all old props have old requests)
    prepare_db(
        session_obj,
        num_old_props_used_by_old=10,
        num_recent_props_used_by_recent=5,
    )
    result = runner.invoke(
        entry_points.app,
        ["requests-cleaner", "--connection-string", connection_string],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        all_events = session.query(database.Events).all()
        all_props = session.query(database.AdaptorProperties).all()
        assert len(all_requests) == 5
        assert len(all_events) == 5
        assert len(all_props) == 5

    # test remove 10 requests in bulk size of 3 (all old props have old requests)
    prepare_db(
        session_obj,
        num_old_props_used_by_old=10,
        num_recent_props_used_by_recent=5,
    )
    caplog.clear()
    caplog.set_level(logging.INFO)
    result = runner.invoke(
        entry_points.app,
        [
            "requests-cleaner",
            "--connection-string",
            connection_string,
            "--delete-bulk-size",
            3,
            "--delete-sleep-time",
            1,
        ],
    )
    assert result.exit_code == 0
    with session_obj() as session:
        all_requests = session.query(database.SystemRequest).all()
        all_events = session.query(database.Events).all()
        all_props = session.query(database.AdaptorProperties).all()
        assert len(all_requests) == 5
        assert len(all_events) == 5
        assert len(all_props) == 5
    with caplog.at_level(logging.ERROR):
        log_msgs = [json.loads(r.msg)["event"] for r in caplog.records]
    assert log_msgs == [
        "deleting old system_requests and events...",
        "3 old system requests successfully removed from the broker database.",
        "3 old system requests successfully removed from the broker database.",
        "3 old system requests successfully removed from the broker database.",
        "1 old system requests successfully removed from the broker database.",
        "0 old system requests successfully removed from the broker database.",
        "deleting old adaptor_properties...",
        "10 old adaptor properties successfully removed from the broker database.",
    ]
    caplog.clear()

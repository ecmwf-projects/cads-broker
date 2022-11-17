import datetime
import random
import uuid
from typing import Any

import cacholote
import pytest
import sqlalchemy as sa
import sqlalchemy.orm.exc
from psycopg import Connection
from sqlalchemy.orm import sessionmaker

from cads_broker import config
from cads_broker import database as db


def mock_system_request(
    status: str = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
    process_id: str | None = None,
    cache_key: str | None = None,
    cache_expiration: datetime.datetime | None = None,
    request_body: dict | None = None,
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_id=random.randrange(1, 100),
        request_uid=request_uid or str(uuid.uuid4()),
        process_id=process_id,
        status=status,
        created_at=created_at,
        started_at=None,
        cache_key=cache_key,
        cache_expiration=cache_expiration,
        request_body=request_body or {"request_type": "test"},
    )
    return system_request


def mock_cache_entry() -> db.SystemRequest:
    cache_entry = cacholote.config.CacheEntry(
        key=cacholote.utils.hexdigestify("test"),
        result={"href": "", "args": [1, 2]},
        expiration=datetime.datetime.today(),
    )
    return cache_entry


def test_get_accepted_requests(session_obj: sa.orm.sessionmaker) -> None:
    successful_request = mock_system_request(status="running")
    accepted_request = mock_system_request(status="accepted")
    accepted_request_uid = accepted_request.request_uid
    with session_obj() as session:
        session.add(successful_request)
        session.add(accepted_request)
        session.commit()
    requests = db.get_accepted_requests(session_obj)
    assert len(requests) == 1
    assert requests[0].request_uid == accepted_request_uid


def test_count_accepted_requests(session_obj: sa.orm.sessionmaker) -> None:
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(status="accepted", process_id=process_id)
    request2 = mock_system_request(status="accepted")

    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.commit()

    assert 2 == db.count_accepted_requests(session_obj)
    assert 1 == db.count_accepted_requests(session_obj, process_id=process_id)


def test_set_request_status(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid

    # running status
    with session_obj() as session:
        session.add(request)
        session.commit()

    db.set_request_status(
        request_uid,
        status="running",
        session_obj=session_obj,
    )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        running_request = session.scalars(statement).one()

    assert running_request.status == "running"

    # successful status
    with session_obj() as session:
        cache_entry = mock_cache_entry()
        cache_key = cache_entry.key
        session.add(cache_entry)
        session.commit()

    db.set_request_status(
        request_uid,
        status="successful",
        cache_key=cache_key,
        session_obj=session_obj,
    )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        successful_request = session.scalars(statement).one()

    assert successful_request.status == "successful"
    assert successful_request.cache_key == cache_key
    assert successful_request.response_traceback is None
    assert successful_request.finished_at is not None

    # failed status
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid

    with session_obj() as session:
        session.add(request)
        session.commit()

    traceback = "traceback"
    db.set_request_status(
        request_uid,
        status="failed",
        traceback=traceback,
        session_obj=session_obj,
    )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        failed_request = session.scalars(statement).one()

    assert failed_request.status == "failed"
    assert failed_request.response_traceback == traceback
    assert failed_request.cache_key is None
    assert failed_request.finished_at is not None


def test_create_request(session_obj: sa.orm.sessionmaker) -> None:
    request_dict = db.create_request(
        user_id=1,
        setup_code="",
        entry_point="sum",
        kwargs={},
        metadata={},
        process_id="submit-workflow",
        session_obj=session_obj,
    )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_dict["request_uid"]
        )
        request = session.scalars(statement).one()
    assert request.request_uid == request_dict["request_uid"]
    assert (
        request.request_metadata["user_id"]
        == request_dict["request_metadata"]["user_id"]
    )


def test_get_request_in_session(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()
    with session_obj() as session:
        request = db.get_request_in_session(request_uid, session)
    assert request.request_uid == request_uid


def test_get_request(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()
    request = db.get_request(request_uid, session_obj)
    assert request.request_uid == request_uid


def test_get_request_result(session_obj: sa.orm.sessionmaker) -> None:
    cache_entry = mock_cache_entry()
    request = mock_system_request(
        status="successful",
        cache_key=cache_entry.key,
        cache_expiration=cache_entry.expiration,
    )
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(cache_entry)
        session.add(request)
        session.commit()
    result = db.get_request_result(request_uid, session_obj)
    assert len(result) == 2


def test_delete_request(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()
    request = db.delete_request(request_uid, session_obj)
    assert request.request_uid == request_uid
    assert request.status == "dismissed"
    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        request = db.get_request(request_uid, session_obj)


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
    expected_tables_complete = set(db.BaseModel.metadata.tables)
    assert set(conn.execute(query).scalars()) == expected_tables_at_beginning  # type: ignore

    db.init_database(connection_string)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore

    request = mock_system_request()
    session_obj = sa.orm.sessionmaker(engine)
    with session_obj() as session:
        session.add(request)
        session.commit()

    db.init_database(connection_string)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore
    requests = db.get_accepted_requests(session_obj=session_obj)
    assert len(requests) == 1

    db.init_database(connection_string, force=True)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore
    requests = db.get_accepted_requests(session_obj=session_obj)
    assert len(requests) == 0


def test_ensure_session_obj(
    postgresql: Connection[str], session_obj: sessionmaker, temp_environ: Any
) -> None:
    # case of session is already set
    ret_value = db.ensure_session_obj(session_obj)
    assert ret_value is session_obj
    config.dbsettings = None

    # case of session not set
    temp_environ["compute_db_password"] = postgresql.info.password
    ret_value = db.ensure_session_obj(None)
    assert isinstance(ret_value, sessionmaker)
    config.dbsettings = None

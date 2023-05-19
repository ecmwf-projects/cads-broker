import datetime
import uuid
from collections import defaultdict
from typing import Any

import cacholote
import pytest
import sqlalchemy as sa
from psycopg import Connection
from sqlalchemy.orm import sessionmaker

from cads_broker import config
from cads_broker import database as db


def mock_system_request(
    status: str | None = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
    process_id: str | None = None,
    user_uid: str | None = None,
    cache_id: int | None = None,
    request_body: dict | None = None,
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_uid=request_uid or str(uuid.uuid4()),
        process_id=process_id,
        status=status,
        user_uid=user_uid,
        created_at=created_at,
        started_at=None,
        cache_id=cache_id,
        request_body=request_body or {"request_type": "test"},
    )
    return system_request


def mock_cache_entry() -> db.SystemRequest:
    cache_entry = cacholote.database.CacheEntry(
        result={"href": "", "args": [1, 2]},
    )
    return cache_entry


def response_as_dict(response: list[tuple]) -> dict:
    result = defaultdict(dict)
    for process_id, status, count in response:
        result[process_id][status] = count
    return result


def test_get_accepted_requests(session_obj: sa.orm.sessionmaker) -> None:
    successful_request = mock_system_request(status="running")
    accepted_request = mock_system_request(status="accepted")
    accepted_request_uid = accepted_request.request_uid
    with session_obj() as session:
        session.add(successful_request)
        session.add(accepted_request)
        session.commit()
        requests = db.get_accepted_requests(session=session)
    assert len(requests) == 1
    assert requests[0].request_uid == accepted_request_uid


def test_count_finished_requests_per_user(session_obj: sa.orm.sessionmaker) -> None:
    request1 = mock_system_request(status="successful")
    request1.finished_at = datetime.datetime.now()
    request2 = mock_system_request(status="failed")
    request2.finished_at = datetime.datetime.now()

    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.commit()
        assert 2 == db.count_finished_requests_per_user_in_session(
            user_uid=request1.user_uid, last_hours=1, session=session
        )


def test_count_requests(session_obj: sa.orm.sessionmaker) -> None:
    process_id1 = "reanalysis-era5-pressure-levels"
    process_id2 = "reanalysis-era5-single-levels"
    user_uid1 = str(uuid.uuid4())
    user_uid2 = str(uuid.uuid4())
    request1 = mock_system_request(process_id=process_id1, user_uid=user_uid1)
    request2 = mock_system_request(process_id=process_id2, user_uid=user_uid2)
    request3 = mock_system_request(
        status="running", process_id=process_id2, user_uid=user_uid2
    )

    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.commit()
        assert 3 == db.count_requests(session=session)
        assert 1 == db.count_requests(session=session, process_id=process_id1)
        assert 2 == db.count_requests(session=session, status="accepted")
        assert 2 == db.count_requests(session=session, user_uid=user_uid2)
        assert 1 == db.count_requests(
            session=session, status="accepted", user_uid=user_uid2
        )


def test_count_requests_per_dataset_status(session_obj: sa.orm.sessionmaker) -> None:
    process_id_era5 = "reanalysis-era5-pressure-levels"
    process_id_dummy = "dummy-dataset"
    request1 = mock_system_request(status="accepted", process_id=process_id_era5)
    request2 = mock_system_request(status="accepted", process_id=process_id_era5)
    request3 = mock_system_request(status="accepted", process_id=process_id_dummy)
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.commit()
        response = db.count_requests_per_dataset_status(session=session)
        assert 2 == len(response)
        if response[0][0] == process_id_era5:
            assert response[0][2] == 2
        elif response[0][0] == process_id_dummy:
            assert response[0][2] == 1
        if response[1][0] == process_id_era5:
            assert response[1][2] == 2
        elif response[1][0] == process_id_dummy:
            assert response[1][2] == 1


def test_count_last_day_requests_per_dataset_status(
    session_obj: sa.orm.sessionmaker,
) -> None:
    process_id_era5 = "reanalysis-era5-pressure-levels"
    process_id_dummy = "dummy-dataset"
    request1 = mock_system_request(status="failed", process_id=process_id_era5)
    request2 = mock_system_request(status="failed", process_id=process_id_era5)
    request3 = mock_system_request(status="successful", process_id=process_id_dummy)
    request4 = mock_system_request(status="successful", process_id=process_id_dummy)
    request4.created_at = datetime.datetime(2020, 1, 1)
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.commit()
        response = db.count_last_day_requests_per_dataset_status(session=session)
    assert 2 == len(response)
    result = response_as_dict(response)
    assert result[process_id_era5]["failed"] == 2
    assert result[process_id_dummy]["successful"] == 1


def test_total_request_time_per_dataset_status(
    session_obj: sa.orm.sessionmaker,
) -> None:
    process_id_era5 = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(status="successful", process_id=process_id_era5)
    # entry not counted (created before yesterday)
    request1.created_at = datetime.datetime(2020, 1, 1)
    request1.started_at = datetime.datetime(2023, 1, 1)
    request1.finished_at = datetime.datetime(2023, 1, 2)
    request2 = mock_system_request(status="successful", process_id=process_id_era5)
    request2.started_at = datetime.datetime(2023, 1, 1)
    request2.finished_at = datetime.datetime(2023, 1, 2)

    process_id_dummy = "dummy-dataset"
    request3 = mock_system_request(status="successful", process_id=process_id_dummy)
    request3.started_at = datetime.datetime(2023, 1, 1)
    request3.finished_at = datetime.datetime(2023, 1, 2)
    request4 = mock_system_request(status="successful", process_id=process_id_dummy)
    request4.started_at = datetime.datetime(2023, 2, 1)
    request4.finished_at = datetime.datetime(2023, 2, 2)
    request5 = mock_system_request(status="failed", process_id=process_id_dummy)
    request5.started_at = datetime.datetime(2023, 1, 1)
    request5.finished_at = datetime.datetime(2023, 1, 2)

    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.add(request5)
        session.commit()
        response = db.total_request_time_per_dataset_status(session=session)
    assert 3 == len(response)
    result = response_as_dict(response)
    assert result[process_id_era5]["successful"] == datetime.timedelta(days=1)
    assert result[process_id_dummy]["successful"] == datetime.timedelta(days=2)
    assert result[process_id_dummy]["failed"] == datetime.timedelta(days=1)


def test_count_active_users(session_obj: sa.orm.sessionmaker) -> None:
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(status="accepted", process_id=process_id)
    request1.user_uid = "aaa"
    request2 = mock_system_request(status="successful", process_id=process_id)
    request2.user_uid = "aaa"
    request3 = mock_system_request(status="running", process_id=process_id)
    request3.user_uid = "bbb"
    request4 = mock_system_request(status="failed", process_id=process_id)
    # third user is inactive
    request4.user_uid = "ccc"
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.commit()
        response = db.count_active_users(session=session)
    assert 1 == len(response)
    assert 2 == response[0][1]


def test_count_queued_users(session_obj: sa.orm.sessionmaker) -> None:
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(status="accepted", process_id=process_id)
    request1.user_uid = "aaa"
    request2 = mock_system_request(status="running", process_id=process_id)
    request2.user_uid = "bbb"
    request3 = mock_system_request(status="accepted", process_id=process_id)
    request3.user_uid = "ccc"
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.commit()
        response = db.count_queued_users(session=session)
        assert 1 == len(response)
        assert 2 == response[0][1]


def test_count_waiting_users_behind_themselves(
    session_obj: sa.orm.sessionmaker,
) -> None:
    process_id = "reanalysis-era5-pressure-levels"
    process_id_dummy = "process_id_dummy"
    request1 = mock_system_request(status="accepted", process_id=process_id)
    request1.user_uid = "aaa"
    request2 = mock_system_request(status="running", process_id=process_id)
    request2.user_uid = "aaa"
    request3 = mock_system_request(status="accepted", process_id=process_id)
    request3.user_uid = "bbb"
    request4 = mock_system_request(status="accepted", process_id=process_id_dummy)
    request4.user_uid = "bbb"
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.commit()
        response = db.count_waiting_users_queued_behind_themselves(session=session)
        assert 1 == len(response)
        # user bbb is not queued behind himself
        assert 1 == response[0][1]


def test_count_waiting_users_queued(session_obj: sa.orm.sessionmaker) -> None:
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(status="accepted", process_id=process_id)
    request1.user_uid = "aaa"
    request2 = mock_system_request(status="successful", process_id=process_id)
    request2.user_uid = "aaa"
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.commit()
        response = db.count_waiting_users_queued(session=session)
        assert 1 == len(response)
        assert 1 == response[0][1]


def test_count_running_users(session_obj: sa.orm.sessionmaker) -> None:
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(status="running", process_id=process_id)
    request1.user_uid = "aaa"
    request2 = mock_system_request(status="running", process_id=process_id)
    request2.user_uid = "bbb"
    request3 = mock_system_request(status="accepted", process_id=process_id)
    request3.user_uid = "ccc"
    with session_obj() as session:
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.commit()
        response = db.count_running_users(session=session)
        assert 1 == len(response)
        assert 2 == response[0][1]


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
            session=session,
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
        cache_id = cache_entry.id
        session.add(cache_entry)
        session.commit()

        db.set_request_status(
            request_uid,
            status="successful",
            cache_id=cache_id,
            session=session,
        )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        successful_request = session.scalars(statement).one()

    assert successful_request.status == "successful"
    assert successful_request.cache_id == cache_id
    assert successful_request.response_error == {}
    assert successful_request.finished_at is not None

    # failed status
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid

    with session_obj() as session:
        session.add(request)
        session.commit()

        error_message = "error_message"
        error_reason = "error_reason"
        db.set_request_status(
            request_uid,
            status="failed",
            error_message=error_message,
            error_reason=error_reason,
            session=session,
        )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        failed_request = session.scalars(statement).one()

    assert failed_request.status == "failed"
    assert failed_request.response_error["reason"] == error_reason
    assert failed_request.response_error["message"] == error_message
    assert failed_request.cache_id is None
    assert failed_request.finished_at is not None


def test_create_request(session_obj: sa.orm.sessionmaker) -> None:
    with session_obj() as session:
        request_dict = db.create_request(
            user_uid="abc123",
            setup_code="",
            entry_point="sum",
            kwargs={},
            metadata={},
            process_id="submit-workflow",
            session=session,
        )
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_dict["request_uid"]
        )
        request = session.scalars(statement).one()
    assert request.request_uid == request_dict["request_uid"]
    assert request.user_uid == request_dict["user_uid"]


def test_get_request(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()
    with session_obj() as session:
        request = db.get_request(request_uid, session)
        with pytest.raises(db.NoResultFound):
            request = db.get_request(str(uuid.uuid4()), session)
    assert request.request_uid == request_uid


def test_get_request_result(session_obj: sa.orm.sessionmaker) -> None:
    cache_entry = mock_cache_entry()
    with session_obj() as session:
        session.add(cache_entry)
        session.flush()
        request = mock_system_request(
            status="successful",
            cache_id=cache_entry.id,
        )
        request_uid = request.request_uid
        session.add(request)
        session.commit()
        result = db.get_request_result(request_uid, session=session)
    assert len(result) == 2


def test_delete_request(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()
        request = db.delete_request(request_uid, session=session)
    assert request.request_uid == request_uid
    assert request.status == "dismissed"
    with pytest.raises(db.NoResultFound):
        with session_obj() as session:
            request = db.get_request(request_uid, session=session)


def test_init_database(postgresql: Connection[str]) -> None:
    connection_string = (
        f"postgresql+psycopg2://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )
    engine = sa.create_engine(connection_string)
    conn = engine.connect()
    query = sa.text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    )
    # start with an empty db structure
    expected_tables_at_beginning: set[str] = set()
    assert set(conn.execute(query).scalars()) == expected_tables_at_beginning  # type: ignore

    # verify create structure
    db.init_database(connection_string, force=True)
    expected_tables_complete = set(db.BaseModel.metadata.tables).union(
        {"alembic_version"}
    )
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore

    request = mock_system_request()
    session_obj = sa.orm.sessionmaker(engine)
    with session_obj() as session:
        session.add(request)
        session.commit()

    db.init_database(connection_string)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore
    with session_obj() as session:
        requests = db.get_accepted_requests(session=session)
    assert len(requests) == 1

    db.init_database(connection_string, force=True)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore
    with session_obj() as session:
        requests = db.get_accepted_requests(session=session)
    assert len(requests) == 0
    conn.close()


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

import datetime
import random
import uuid

import sqlalchemy as sa
from psycopg import Connection

from cads_broker import database as db


def mock_system_request(
    status: str = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_id=random.randrange(1, 100),
        request_uid=request_uid or uuid.uuid4().hex,
        status=status,
        created_at=created_at,
        started_at=None,
        request_body={"request_type": "test"},
    )
    return system_request


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


def test_set_request_status(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
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

    result = {"content_type": "application/json"}
    traceback = None
    db.set_request_status(
        request_uid,
        status="successful",
        result=result,
        traceback=traceback,
        session_obj=session_obj,
    )
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        running_request = session.scalars(statement).one()

    assert running_request.status == "successful"
    assert running_request.response_body["result"] == result
    assert running_request.response_body["traceback"] == traceback
    assert running_request.finished_at is not None


def test_create_request(session_obj: sa.orm.sessionmaker) -> None:
    request_dict = db.create_request(session_obj=session_obj)
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_dict["request_uid"]
        )
        request = session.scalars(statement).one()
    assert request.request_uid == request_dict["request_uid"]


def test_get_request(session_obj: sa.orm.sessionmaker) -> None:
    request = mock_system_request(status="accepted")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()
    request = db.get_request(request_uid, session_obj)
    assert request.request_uid == request_uid


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
    expected_tables_complete = set(db.metadata.tables)
    assert set(conn.execute(query).scalars()) == expected_tables_at_beginning  # type: ignore

    db.init_database(connection_string)
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore

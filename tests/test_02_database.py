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


class MockRule:
    def __init__(self, name, uid, conclusion, info, condition):
        self.name = name
        self.uid = uid
        self.conclusion = conclusion
        self.info = info
        self.condition = condition

    def get_uid(self, request):
        return self.uid

    def evaluate(self, request):
        return self.uid


def mock_config(hash: str = "", config: dict[str, Any] = {}, form: dict[str, Any] = {}):
    adaptor_properties = db.AdaptorProperties(
        hash=hash,
        config=config,
        form=form,
    )
    return adaptor_properties


def mock_system_request(
    status: str | None = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
    process_id: str | None = "process_id",
    user_uid: str | None = None,
    cache_id: int | None = None,
    request_body: dict | None = None,
    request_metadata: dict | None = None,
    adaptor_properties_hash: str = "adaptor_properties_hash",
    entry_point: str = "entry_point",
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
        request_metadata=request_metadata or {},
        adaptor_properties_hash=adaptor_properties_hash,
        entry_point=entry_point,
    )
    return system_request


def mock_cache_entry() -> db.SystemRequest:
    cache_entry = cacholote.database.CacheEntry(
        result={"href": "", "args": [1, 2]},
    )
    return cache_entry


def mock_event(
    request_uid: str,
    event_type: str = "user_visible_log",
    message: str = "message",
    timestamp: datetime.datetime = datetime.datetime.now(),
) -> db.Events:
    event = db.Events(
        request_uid=request_uid,
        event_type=event_type,
        message=message,
        timestamp=timestamp,
    )
    return event


def response_as_dict(response: list[tuple]) -> dict:
    result = defaultdict(dict)
    for process_id, status, count in response:
        result[process_id][status] = count
    return result


def test_get_accepted_requests(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    successful_request = mock_system_request(
        status="running", adaptor_properties_hash=adaptor_properties.hash
    )
    accepted_request = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    accepted_request_uid = accepted_request.request_uid
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(successful_request)
        session.add(accepted_request)
        session.commit()
        requests = db.get_accepted_requests(session=session)
    assert len(requests) == 1
    assert requests[0].request_uid == accepted_request_uid


def test_count_finished_requests_per_user(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request1 = mock_system_request(
        status="successful", adaptor_properties_hash=adaptor_properties.hash
    )
    request1.finished_at = datetime.datetime.now()
    request2 = mock_system_request(
        status="failed", adaptor_properties_hash=adaptor_properties.hash
    )
    request2.finished_at = datetime.datetime.now()

    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request1)
        session.add(request2)
        session.commit()
        assert 2 == db.count_finished_requests_per_user_in_session(
            user_uid=request1.user_uid, seconds=60, session=session
        )


def test_count_requests(session_obj: sa.orm.sessionmaker) -> None:
    entry_point_1 = "entry_point_1"
    entry_point_2 = "entry_point_2"
    process_id_1 = "process_id_1"
    process_id_2 = "process_id_2"
    user_uid_1 = str(uuid.uuid4())
    user_uid_2 = str(uuid.uuid4())
    adaptor_properties = mock_config()
    request1 = mock_system_request(
        status="accepted",
        entry_point=entry_point_1,
        process_id=process_id_1,
        user_uid=user_uid_1,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2 = mock_system_request(
        status="accepted",
        entry_point=entry_point_2,
        process_id=process_id_2,
        user_uid=user_uid_2,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3 = mock_system_request(
        status="running",
        entry_point=entry_point_2,
        process_id=process_id_2,
        user_uid=user_uid_2,
        adaptor_properties_hash=adaptor_properties.hash,
    )

    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.commit()
        assert 3 == db.count_requests(session=session)
        assert 1 == db.count_requests(session=session, entry_point=entry_point_1)
        assert 2 == db.count_requests(session=session, status="accepted")
        assert 2 == db.count_requests(session=session, user_uid=user_uid_2)
        assert 2 == db.count_requests(session=session, process_id=process_id_2)
        assert 1 == db.count_requests(
            session=session, status="accepted", user_uid=user_uid_2
        )
        assert 3 == db.count_requests(session=session, status=["accepted", "running"])


def test_count_requests_per_dataset_status(session_obj: sa.orm.sessionmaker) -> None:
    process_id_era5 = "reanalysis-era5-pressure-levels"
    process_id_dummy = "dummy-dataset"
    adaptor_properties = mock_config()
    request1 = mock_system_request(
        status="accepted",
        process_id=process_id_era5,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2 = mock_system_request(
        status="accepted",
        process_id=process_id_era5,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3 = mock_system_request(
        status="accepted",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    with session_obj() as session:
        session.add(adaptor_properties)
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
    adaptor_properties = mock_config()
    process_id_dummy = "dummy-dataset"
    request1 = mock_system_request(
        status="failed",
        process_id=process_id_era5,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2 = mock_system_request(
        status="failed",
        process_id=process_id_era5,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3 = mock_system_request(
        status="successful",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request4 = mock_system_request(
        status="successful",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request4.created_at = datetime.datetime(2020, 1, 1)
    with session_obj() as session:
        session.add(adaptor_properties)
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
    adaptor_properties = mock_config()
    process_id_era5 = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(
        status="successful",
        process_id=process_id_era5,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    # entry not counted (created before yesterday)
    request1.created_at = datetime.datetime(2020, 1, 1)
    request1.started_at = datetime.datetime(2023, 1, 1)
    request1.finished_at = datetime.datetime(2023, 1, 2)
    request2 = mock_system_request(
        status="successful",
        process_id=process_id_era5,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2.started_at = datetime.datetime(2023, 1, 1)
    request2.finished_at = datetime.datetime(2023, 1, 2)

    process_id_dummy = "dummy-dataset"
    request3 = mock_system_request(
        status="successful",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3.started_at = datetime.datetime(2023, 1, 1)
    request3.finished_at = datetime.datetime(2023, 1, 2)
    request4 = mock_system_request(
        status="successful",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request4.started_at = datetime.datetime(2023, 2, 1)
    request4.finished_at = datetime.datetime(2023, 2, 2)
    request5 = mock_system_request(
        status="failed",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request5.started_at = datetime.datetime(2023, 1, 1)
    request5.finished_at = datetime.datetime(2023, 1, 2)

    with session_obj() as session:
        session.add(adaptor_properties)
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
    adaptor_properties = mock_config()
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(
        status="accepted",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request1.user_uid = "aaa"
    request2 = mock_system_request(
        status="successful",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2.user_uid = "aaa"
    request3 = mock_system_request(
        status="running",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3.user_uid = "bbb"
    request4 = mock_system_request(
        status="failed",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    # third user is inactive
    request4.user_uid = "ccc"
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.commit()
        response = db.count_active_users(session=session)
    assert 1 == len(response)
    assert 2 == response[0][1]


def test_count_queued_users(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(
        status="accepted",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request1.user_uid = "aaa"
    request2 = mock_system_request(
        status="running",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2.user_uid = "bbb"
    request3 = mock_system_request(
        status="accepted",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3.user_uid = "ccc"
    with session_obj() as session:
        session.add(adaptor_properties)
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
    adaptor_properties = mock_config()
    process_id_dummy = "process_id_dummy"
    request1 = mock_system_request(
        status="accepted",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request1.user_uid = "aaa"
    request2 = mock_system_request(
        status="running",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2.user_uid = "aaa"
    request3 = mock_system_request(
        status="accepted",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request3.user_uid = "bbb"
    request4 = mock_system_request(
        status="accepted",
        process_id=process_id_dummy,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request4.user_uid = "bbb"
    with session_obj() as session:
        session.add(adaptor_properties)
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
    adaptor_properties = mock_config()
    process_id = "reanalysis-era5-pressure-levels"
    request1 = mock_system_request(
        status="accepted",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request1.user_uid = "aaa"
    request2 = mock_system_request(
        status="successful",
        process_id=process_id,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2.user_uid = "aaa"
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request1)
        session.add(request2)
        session.commit()
        response = db.count_waiting_users_queued(session=session)
        assert 1 == len(response)
        assert 1 == response[0][1]


def test_count_running_users(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request0 = mock_system_request(
        status="running",
        entry_point="foobar",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="bob",
    )
    request1 = mock_system_request(
        status="running",
        entry_point="bar",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="bob",
    )
    request2 = mock_system_request(
        status="running",
        entry_point="bar",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="bob",
    )
    request3 = mock_system_request(
        status="accepted",
        entry_point="bar",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="carl",
    )
    request4 = mock_system_request(
        status="accepted",
        entry_point="bar",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="bob",
    )
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request0)
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.commit()
        assert 2 == db.count_users(
            session=session, entry_point="bar", status="accepted"
        )
        assert 1 == db.count_users(session=session, entry_point="bar", status="running")
        assert 0 == db.count_users(
            session=session, entry_point="foobar", status="accepted"
        )
        assert 1 == db.count_users(
            session=session, entry_point="foobar", status="running"
        )


def test_set_request_qos_rule(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_uid = request.request_uid
    rule_name = "limit"
    limit_1 = MockRule(
        name=rule_name,
        uid="1",
        condition="condition",
        conclusion="condition",
        info="info",
    )
    limit_2 = MockRule(
        name=rule_name,
        uid="2",
        condition="condition",
        conclusion="condition",
        info="info",
    )
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request)
        session.commit()
        db.set_request_qos_rule(request=request, rule=limit_1, session=session)
        db.set_request_qos_rule(request=request, rule=limit_2, session=session)
        session.commit()
    with session_obj() as session:
        request = db.get_request(request_uid=request_uid, session=session)
        assert "1" in request.qos_status.get(rule_name, [])
        assert "2" in request.qos_status.get(rule_name, [])


def test_get_events_from_request(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request = mock_system_request(adaptor_properties_hash=adaptor_properties.hash)
    request_uid = request.request_uid
    test_events = [
        {
            "event_type": "user_visible_log",
            "message": "log message 1",
            "timestamp": datetime.datetime(2024, 1, 1, 8, 00, 00, 000000),
        },
        {
            "event_type": "user_visible_log",
            "message": "log message 2",
            "timestamp": datetime.datetime(2024, 1, 2, 8, 00, 00, 000000),
        },
        {
            "event_type": "user_visible_error",
            "message": "error message 1",
            "timestamp": datetime.datetime(2024, 1, 1, 16, 00, 00, 000000),
        },
        {
            "event_type": "user_visible_error",
            "message": "error message 2",
            "timestamp": datetime.datetime(2024, 1, 2, 16, 00, 00, 000000),
        },
    ]
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request)
        for test_event in test_events:
            event = mock_event(
                request_uid=request_uid,
                event_type=test_event["event_type"],
                message=test_event["message"],
                timestamp=test_event["timestamp"],
            )
            session.add(event)
        session.commit()
    with session_obj() as session:
        events = db.get_events_from_request(request_uid=request_uid, session=session)
    assert len(events) == 4
    for i, event in enumerate(events):
        assert event.event_type == test_events[i]["event_type"]
        assert event.message == test_events[i]["message"]
        assert event.timestamp == test_events[i]["timestamp"]
    with session_obj() as session:
        events = db.get_events_from_request(
            request_uid=request_uid,
            event_type="user_visible_log",
            session=session,
        )
    assert len(events) == 2
    assert events[0].event_type == "user_visible_log"
    assert events[1].event_type == "user_visible_log"
    with session_obj() as session:
        events = db.get_events_from_request(
            request_uid=request_uid,
            event_type="user_visible_error",
            session=session,
        )
    assert len(events) == 2
    assert events[0].event_type == "user_visible_error"
    assert events[1].event_type == "user_visible_error"
    with session_obj() as session:
        events = db.get_events_from_request(
            request_uid=request_uid,
            start_time=datetime.datetime(2024, 1, 1, 15, 00, 00, 000000),
            stop_time=datetime.datetime(2024, 1, 2, 9, 00, 00, 000000),
            session=session,
        )
    assert len(events) == 2


def test_get_qos_status_from_request() -> None:
    test_qos_status = {
        "qos_status": {
            "rule_name_1": {
                "rule_key_1_1": {
                    "condition": "condition_1_1",
                    "info": "info_1_1",
                    "conclusion": "conclusion_1_1",
                },
                "rule_key_1_2": {
                    "condition": "condition_1_2",
                    "info": "info_1_2",
                    "conclusion": "conclusion_1_2",
                },
            },
            "rule_name_2": {"rule_key_2_1": {}},
        }
    }
    test_request = db.SystemRequest(**test_qos_status)
    exp_qos_status = {
        "rule_name_1": [("info_1_1", "conclusion_1_1"), ("info_1_2", "conclusion_1_2")],
        "rule_name_2": [("", "")],
    }
    res_qos_staus = db.get_qos_status_from_request(test_request)
    assert exp_qos_status == res_qos_staus


def test_set_request_status(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_uid = request.request_uid

    # running status
    with session_obj() as session:
        session.add(adaptor_properties)
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
    adaptor_properties = mock_config(hash="test")
    request = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_uid = request.request_uid

    with session_obj() as session:
        session.add(adaptor_properties)
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


def test_add_event(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request_1 = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_2 = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_uid_1 = request_1.request_uid
    request_uid_2 = request_2.request_uid

    # running status
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request_1)
        session.add(request_2)
        session.commit()

        db.add_event("event_1", request_uid_1, "message_1", session=session)
        db.add_event("event_1", request_uid_1, "message_11", session=session)
        db.add_event("event_2", request_uid_2, "message_2", session=session)

    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid_1
        )
        request = session.scalars(statement).one()

        assert request.events[0].event_type == "event_1"
        assert request.events[0].message == "message_1"
        assert request.events[1].message == "message_11"
        with pytest.raises(IndexError):
            request.events[2].message


def test_create_request(session_obj: sa.orm.sessionmaker) -> None:
    with session_obj() as session:
        request_dict = db.create_request(
            user_uid="abc123",
            setup_code="",
            entry_point="sum",
            request={},
            metadata={},
            process_id="submit-workflow",
            session=session,
            portal="c3s",
            adaptor_config={"dummy_config": {"foo": "bar"}},
            adaptor_form={},
            adaptor_properties_hash="adaptor_properties_hash",
        )
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_dict["request_uid"]
        )
        request = session.scalars(statement).one()
        adaptor_properties = request.adaptor_properties
        assert adaptor_properties.hash == "adaptor_properties_hash"
        initial_timestamp = adaptor_properties.timestamp
    assert request.request_uid == request_dict["request_uid"]
    assert request.user_uid == request_dict["user_uid"]
    # create again a new request using the same adaptor properties: timestamp updated
    with session_obj() as session:
        request_dict = db.create_request(
            user_uid="abc456",
            setup_code="",
            entry_point="sum",
            request={},
            metadata={},
            process_id="submit-workflow",
            session=session,
            portal="c3s",
            adaptor_config={"dummy_config": {"foo": "bar"}},
            adaptor_form={},
            adaptor_properties_hash="adaptor_properties_hash",
        )
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_dict["request_uid"]
        )
        request = session.scalars(statement).one()
        adaptor_properties = request.adaptor_properties
        assert adaptor_properties.hash == "adaptor_properties_hash"
        adaptor_properties.timestamp > initial_timestamp


def test_get_request(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(adaptor_properties)
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
        adaptor_properties = mock_config()
        request = mock_system_request(
            status="successful",
            cache_id=cache_entry.id,
            adaptor_properties_hash=adaptor_properties.hash,
        )
        request_uid = request.request_uid
        session.add(adaptor_properties)
        session.add(request)
        session.commit()
        result = db.get_request_result(request_uid, session=session)
    assert len(result) == 2


def test_delete_request(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request)
        session.commit()
        db.delete_request(request, session=session)
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
    expected_tables_complete = (
        set(db.BaseModel.metadata.tables)
        .union({"alembic_version"})
        .union(set(cacholote.database.Base.metadata.tables))
    )
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore

    adaptor_properties = mock_config()
    request = mock_system_request(adaptor_properties_hash=adaptor_properties.hash)
    session_obj = sa.orm.sessionmaker(engine)
    with session_obj() as session:
        session.add(adaptor_properties)
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


def test_init_database_with_password(postgresql2: Connection[str]) -> None:
    connection_url = sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=postgresql2.info.user,
        password=postgresql2.info.password,
        host=postgresql2.info.host,
        port=postgresql2.info.port,
        database=postgresql2.info.dbname,
    )
    connection_string = connection_url.render_as_string(False)
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
    expected_tables_complete = (
        set(db.BaseModel.metadata.tables)
        .union({"alembic_version"})
        .union(set(cacholote.database.Base.metadata.tables))
    )
    assert set(conn.execute(query).scalars()) == expected_tables_complete  # type: ignore

    adaptor_properties = mock_config()
    request = mock_system_request(adaptor_properties_hash=adaptor_properties.hash)
    session_obj = sa.orm.sessionmaker(engine)
    with session_obj() as session:
        session.add(adaptor_properties)
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
    temp_environ.update(
        dict(
            compute_db_password=postgresql.info.password,
            compute_db_host=postgresql.info.host,
            compute_db_host_read=postgresql.info.host,
            compute_db_name=postgresql.info.dbname,
            compute_db_user=postgresql.info.user,
        )
    )
    ret_value = db.ensure_session_obj(None)
    assert isinstance(ret_value, sessionmaker)
    config.dbsettings = None

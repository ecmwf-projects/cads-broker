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
    def __init__(self, name, conclusion, info, condition, queued=[], running=0):
        self.name = name
        self.conclusion = conclusion
        self.info = info
        self.condition = condition
        self.queued = queued
        self.value = running

    def evaluate(self, request):
        return 10


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
    started_at: datetime.datetime | None = None,
    finished_at: datetime.datetime | None = None,
    request_uid: str | None = None,
    process_id: str | None = "process_id",
    user_uid: str | None = None,
    cache_id: int | None = None,
    request_body: dict | None = None,
    request_metadata: dict | None = None,
    adaptor_properties_hash: str = "adaptor_properties_hash",
    entry_point: str = "entry_point",
    origin: str = "api",
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_uid=request_uid or str(uuid.uuid4()),
        process_id=process_id,
        status=status,
        user_uid=user_uid,
        created_at=created_at,
        started_at=started_at,
        finished_at=finished_at,
        cache_id=cache_id,
        request_body=request_body or {"request_type": "test"},
        request_metadata=request_metadata or {},
        adaptor_properties_hash=adaptor_properties_hash,
        entry_point=entry_point,
        origin=origin,
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
    accepted_request1 = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    accepted_request2 = mock_system_request(
        status="accepted", adaptor_properties_hash=adaptor_properties.hash
    )
    accepted_request_uid = accepted_request1.request_uid
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(successful_request)
        session.add(accepted_request1)
        session.add(accepted_request2)
        session.commit()
        all_requests = db.get_accepted_requests(session=session)
        one_request = db.get_accepted_requests(session=session, limit=1)
    assert len(all_requests) == 2
    assert len(one_request) == 1
    assert min(all_requests, key=lambda x: x.created_at) == one_request[0]
    assert all_requests[0].request_uid == accepted_request_uid


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


def test_cache_count_requests(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    user_uid = str(uuid.uuid4())
    request1 = mock_system_request(
        status="accepted",
        user_uid=user_uid,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2 = mock_system_request(
        status="accepted",
        user_uid=user_uid,
        adaptor_properties_hash=adaptor_properties.hash,
    )
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request1)
        session.commit()
        assert (
            db.cache_count_requests(
                session=session,
                user_uid=user_uid,
                request_uid=request1.request_uid,
                status="accepted",
            )
            == 1
        )
        session.add(request2)
        # cache the previous result
        assert (
            db.cache_count_requests(
                session=session,
                user_uid=user_uid,
                request_uid=request1.request_uid,
                status="accepted",
            )
            == 1
        )
        assert (
            db.cache_count_requests(
                session=session,
                user_uid=user_uid,
                request_uid=request2.request_uid,
                status="accepted",
            )
            == 2
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


def test_get_stuck_running_requests(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request1 = mock_system_request(
        status="accepted",
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request2 = mock_system_request(
        status="running",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=2),
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request_uid2 = request2.request_uid
    request3 = mock_system_request(
        status="running",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=1, minutes=10),
        adaptor_properties_hash=adaptor_properties.hash,
    )
    request_uid3 = request3.request_uid
    request4 = mock_system_request(
        status="running",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=5, minutes=10),
        adaptor_properties_hash=adaptor_properties.hash,
    )
    event4 = mock_event(
        request_uid=request4.request_uid,
        event_type="worker-name",
        message="worker-0",
        timestamp=datetime.datetime.now() - datetime.timedelta(minutes=5),
    )

    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request1)
        session.add(request2)
        session.add(request3)
        session.add(request4)
        session.add(event4)
        session.commit()
        requests = db.get_stuck_requests(session=session)

    assert len(requests) == 2
    assert request_uid2 in requests
    assert request_uid3 in requests


def test_add_qos_rule(session_obj: sa.orm.sessionmaker) -> None:
    rule = MockRule("rule_name", "conclusion", "info", "condition")
    with session_obj() as session:
        db.add_qos_rule(rule, session=session)
    with session_obj() as session:
        assert db.get_qos_rule(str(rule.__hash__()), session=session).name == rule.name


def test_add_request_qos_status(session_obj: sa.orm.sessionmaker) -> None:
    rule1 = MockRule(
        "name1", "conclusion1", "info1", "condition1", queued=list(range(5))
    )
    rule2 = MockRule(
        "name2", "conclusion2", "info2", "condition2", queued=list(range(1))
    )
    adaptor_properties = mock_config()
    request = mock_system_request(adaptor_properties_hash=adaptor_properties.hash)
    request_uid = request.request_uid
    with session_obj() as session:
        qos_rule = db.add_qos_rule(rule1, queued=5, session=session)
        rules_in_db = {qos_rule.uid: qos_rule}
        session.add(adaptor_properties)
        session.add(request)
        session.commit()
        db.add_request_qos_status(
            request, [rule1, rule2], rules_in_db=rules_in_db, session=session
        )
        session.commit()
    with session_obj() as session:
        request = db.get_request(request_uid, session=session)
        assert db.get_qos_status_from_request(request) == {
            "name1": [{"info": "info1", "queued": 5, "running": 0, "conclusion": "10"}],
            "name2": [{"info": "info2", "queued": 1, "running": 0, "conclusion": "10"}],
        }


def test_delete_request_qos_status(session_obj: sa.orm.sessionmaker) -> None:
    rule1 = MockRule(
        "name1", "conclusion1", "info1", "condition1", queued=list(range(5)), running=2
    )
    rule2 = MockRule(
        "name2", "conclusion2", "info2", "condition2", queued=list(range(3)), running=2
    )
    adaptor_properties = mock_config()
    request = mock_system_request(adaptor_properties_hash=adaptor_properties.hash)
    request_uid = request.request_uid
    rule1_queued = 5
    rule1_running = 2
    rule2_queued = 3
    rule2_running = 4
    with session_obj() as session:
        rule1_db = db.add_qos_rule(
            rule1, queued=rule1_queued, running=rule1_running, session=session
        )
        rule2_db = db.add_qos_rule(
            rule2, queued=rule2_queued, running=rule2_running, session=session
        )
        rules_in_db = {rule1_db.uid: rule1_db, rule2_db.uid: rule2_db}
        session.add(adaptor_properties)
        session.add(request)
        session.commit()
        db.add_request_qos_status(
            request, [rule1, rule2], rules_in_db=rules_in_db, session=session
        )
        session.commit()
    with session_obj() as session:
        db.delete_request_qos_status(request_uid, [rule1, rule2], session=session)
        session.commit()
    with session_obj() as session:
        rule1 = db.get_qos_rule(str(rule1.__hash__()), session=session)
        rule2 = db.get_qos_rule(str(rule2.__hash__()), session=session)
        assert rule1.queued == rule1_queued
        assert rule2.queued == rule2_queued


def test_decrement_qos_rule_running(session_obj: sa.orm.sessionmaker) -> None:
    rule1 = MockRule(
        "name1", "conclusion1", "info1", "condition1", queued=list(range(5)), running=2
    )
    rule2 = MockRule(
        "name2", "conclusion2", "info2", "condition2", queued=list(range(3)), running=4
    )
    rule1_queued = 5
    rule1_running = 2
    rule2_queued = 3
    rule2_running = 4
    with session_obj() as session:
        rule1_db = db.add_qos_rule(
            rule1, queued=rule1_queued, running=rule1_running, session=session
        )
        rule2_db = db.add_qos_rule(
            rule2, queued=rule2_queued, running=rule2_running, session=session
        )
        rules_in_db = {rule1_db.uid: rule1_db, rule2_db.uid: rule2_db}
        session.commit()
        db.decrement_qos_rule_running(
            [rule1, rule2], rules_in_db=rules_in_db, session=session
        )
        session.commit()
    with session_obj() as session:
        assert (
            db.get_qos_rule(str(rule1.__hash__()), session=session).running
            == rule1_running
        )
        assert (
            db.get_qos_rule(str(rule2.__hash__()), session=session).running
            == rule2_running
        )


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
    test_qos_rules = {
        "qos_rules": [
            db.QoSRule(
                name="rule_name_1",
                info="info_1_1",
                queued="queued_1_1",
                running="running_1_1",
            ),
            db.QoSRule(
                name="rule_name_1",
                info="info_1_2",
                queued="queued_1_2",
                running="running_1_2",
            ),
            db.QoSRule(
                name="rule_name_2",
                info="info_2_1",
                queued="queued_2_1",
                running="running_2_1",
            ),
        ]
    }
    test_request = db.SystemRequest(**test_qos_rules)
    exp_qos_status = {
        "rule_name_1": [
            {
                "info": "info_1_1",
                "queued": "queued_1_1",
                "running": "running_1_1",
                "conclusion": None,
            },
            {
                "info": "info_1_2",
                "queued": "queued_1_2",
                "running": "running_1_2",
                "conclusion": None,
            },
        ],
        "rule_name_2": [
            {
                "info": "info_2_1",
                "queued": "queued_2_1",
                "running": "running_2_1",
                "conclusion": None,
            },
        ],
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


def test_get_users_queue_from_processing_time(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request_1 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        origin="api",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=10),
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=5),
    )
    request_2 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        origin="ui",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=20),
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=10),
    )
    request_3 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user2",
        origin="api",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=20),
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=10),
    )
    request_4 = mock_system_request(
        status="running",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user2",
        origin="api",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=20),
    )
    request_5 = mock_system_request(
        status="accepted",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user2",
        origin="api",
    )
    request_6 = mock_system_request(
        status="accepted",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user3",
        origin="api",
    )
    request_7 = mock_system_request(
        status="failed",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user3",
        origin="api",
        started_at=None,
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=10),
    )
    request_8 = mock_system_request(
        status="deleted",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user2",
        origin="api",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=15),
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=10),
    )
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request_1)
        session.add(request_2)
        session.add(request_3)
        session.add(request_4)
        session.add(request_5)
        session.add(request_6)
        session.add(request_7)
        session.add(request_8)
        session.commit()
    with session_obj() as session:
        users_cost = db.get_users_queue_from_processing_time(
            session, interval_stop=datetime.datetime.now()
        )
        users_cost_api = db.get_users_queue_from_processing_time(
            session, interval_stop=datetime.datetime.now(), origin="api"
        )
        users_cost_ui = db.get_users_queue_from_processing_time(
            session, interval_stop=datetime.datetime.now(), origin="ui"
        )
    assert users_cost["user3"] == users_cost_api["user3"] == users_cost_ui["user3"] == 0
    assert users_cost["user1"] == (5 + 10) * 60 * 60
    assert users_cost["user2"] == users_cost_api["user2"] == (10 + 20 + 5) * 60 * 60
    assert users_cost_api["user1"] == 5 * 60 * 60
    assert users_cost_ui["user1"] == 10 * 60 * 60


def test_users_last_finished_at(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    now = datetime.datetime.now()
    finished_at = now - datetime.timedelta(hours=5)
    finished_at_old = now - datetime.timedelta(hours=30)
    request_1 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        started_at=now - datetime.timedelta(hours=10),
        finished_at=finished_at,
    )
    request_2 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        started_at=now - datetime.timedelta(hours=20),
        finished_at=finished_at - datetime.timedelta(hours=5),
    )
    request_3 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user2",
        started_at=now - datetime.timedelta(hours=40),
        finished_at=finished_at_old,
    )

    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request_1)
        session.add(request_2)
        session.add(request_3)
        session.commit()
        users_last_finished_at = db.users_last_finished_at(
            session=session, after=now - datetime.timedelta(hours=24)
        )
        assert finished_at == users_last_finished_at["user1"]
        assert "user2" not in users_last_finished_at


def test_user_last_completed_request(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    now = datetime.datetime.now()
    finished_at = now - datetime.timedelta(hours=5)
    finished_at_old = now - datetime.timedelta(hours=30)
    request_1 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        started_at=now - datetime.timedelta(hours=10),
        finished_at=finished_at,
    )
    request_2 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        started_at=now - datetime.timedelta(hours=20),
        finished_at=finished_at + datetime.timedelta(hours=5),
    )
    request_3 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user2",
        started_at=now - datetime.timedelta(hours=40),
        finished_at=finished_at_old,
    )
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request_1)
        session.add(request_3)
        session.commit()
        assert (now - finished_at).seconds == db.user_last_completed_request(
            session=session, user_uid="user1", interval=60 * 60 * 24
        )
        assert 60 * 60 * 24 == db.user_last_completed_request(
            session=session, user_uid="user2", interval=60 * 60 * 24
        )
        session.add(request_2)
        session.commit()
        assert (now - finished_at).seconds == db.user_last_completed_request(
            session=session, user_uid="user1", interval=60 * 60 * 24
        )
        # invalidate cache
        db.QOS_FUNCTIONS_CACHE = {}
        assert 0 == db.user_last_completed_request(
            session=session, user_uid="user1", interval=60 * 60 * 24
        )


def test_user_resource_used(session_obj: sa.orm.sessionmaker) -> None:
    adaptor_properties = mock_config()
    request_1 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=10),
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=5),
    )
    request_2 = mock_system_request(
        status="successful",
        adaptor_properties_hash=adaptor_properties.hash,
        user_uid="user1",
        started_at=datetime.datetime.now() - datetime.timedelta(hours=20),
        finished_at=datetime.datetime.now() - datetime.timedelta(hours=10),
    )
    with session_obj() as session:
        session.add(adaptor_properties)
        session.add(request_1)
        session.commit()
        assert 5 * 60 * 60 == db.user_resource_used(
            session=session, user_uid="user1", interval=60 * 60 * 24
        )
        session.add(request_2)
        session.commit()
        # cache hit
        assert 5 * 60 * 60 == db.user_resource_used(
            session=session, user_uid="user1", interval=60 * 60 * 24
        )
        # invalidate cache
        db.QOS_FUNCTIONS_CACHE = {}
        assert (5 + 10) * 60 * 60 == db.user_resource_used(
            session=session, user_uid="user1", interval=60 * 60 * 24
        )


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
    conn.close()
    # verify create structure
    db.init_database(connection_string, force=True)
    conn = engine.connect()
    expected_tables_complete = (
        set(db.BaseModel.metadata.tables)
        .union({"alembic_version"})
        .union({"alembic_version_cacholote"})
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
        .union({"alembic_version_cacholote"})
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

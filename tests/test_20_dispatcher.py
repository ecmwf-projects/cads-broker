import datetime
import uuid

import distributed
import pytest_mock
import sqlalchemy as sa

from cads_broker import Environment, dispatcher
from cads_broker import database as db
from cads_broker.qos import QoS, Rule

# create client object and connect to local cluster
CLIENT = distributed.Client(distributed.LocalCluster())


def unique_id_generator():
    """Make a generator of a sequence of integers."""
    current_id = 1
    while True:
        yield current_id
        current_id += 1


request_ids = unique_id_generator()


def mock_system_request(
    status: str = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_id=next(request_ids),
        request_uid=request_uid or str(uuid.uuid4()),
        status=status,
        created_at=created_at,
        started_at=None,
        request_body={"request_type": "test"},
        request_metadata={},
    )
    return system_request


def test_broker_update_database(
    mocker: pytest_mock.plugin.MockerFixture, session_obj: sa.orm.sessionmaker
) -> None:
    environment = Environment.Environment()
    qos = QoS.QoS(rules=Rule.RuleSet(), environment=environment, rules_hash="")
    broker = dispatcher.Broker(
        client=CLIENT,
        environment=environment,
        qos=qos,
        session_maker=session_obj,
    )

    successful_uid = str(uuid.uuid4())
    queued_in_dask_uid = str(uuid.uuid4())
    successful_request = mock_system_request(
        request_uid=successful_uid, status="running"
    )
    queued_in_dask_request = mock_system_request(
        request_uid=queued_in_dask_uid, status="running"
    )
    with session_obj() as session:
        session.add(successful_request)
        session.add(queued_in_dask_request)
        session.commit()

    def mock_fetch_dask_task_status(_, uid: str) -> tuple[int, str]:
        if uid == successful_uid:
            return 0, "successful"
        if uid == queued_in_dask_uid:
            return 0, "running"
        else:
            return 0, "failed"

    mocker.patch(
        "cads_broker.dispatcher.Broker.fetch_dask_task_status",
        new=mock_fetch_dask_task_status,
    )

    with session_obj() as session:
        broker.update_database(session=session)

        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == successful_uid
        )
        assert session.scalars(statement).first().status == "running"

        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == queued_in_dask_uid
        )
        assert session.scalars(statement).first().status == "running"


def test_broker_fetch_dask_task_status(
    mocker: pytest_mock.plugin.MockerFixture, session_obj: sa.orm.sessionmaker
) -> None:
    environment = Environment.Environment()
    qos = QoS.QoS(rules=Rule.RuleSet(), environment=environment, rules_hash="")
    broker = dispatcher.Broker(
        client=CLIENT, environment=environment, qos=qos, session_maker=session_obj
    )

    def mock_get_tasks() -> dict[str, str]:
        return {"dask-scheduler": "successful"}

    mocker.patch(
        "cads_broker.dispatcher.get_tasks",
        return_value=mock_get_tasks(),
    )

    # add a pending future to the broker
    broker.futures = {"future": distributed.Future("future", CLIENT)}

    assert broker.fetch_dask_task_status("future") == (
        0,
        dispatcher.DASK_STATUS_TO_STATUS["pending"],
    )
    assert broker.fetch_dask_task_status("dask-scheduler") == (0, "successful")
    assert broker.fetch_dask_task_status("unknown") == (1, "accepted")

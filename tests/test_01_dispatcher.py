import random
import uuid

import distributed
import pytest_mock
import sqlalchemy as sa

from cads_broker import database as db
from cads_broker import dispatcher

# create client object and connect to local cluster
CLIENT = distributed.Client(distributed.LocalCluster())


def mock_system_request(
    status: str = "queued", created_at: float = 0
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_id=random.randrange(1, 100),
        request_uid=uuid.uuid4().hex,
        status=status,
        request_metadata={"created_at": created_at},
        request_body={"request_type": "test"},
    )
    return system_request


def test_broker_update_queue(session_obj: sa.orm.sessionmaker) -> None:
    broker = dispatcher.Broker(client=CLIENT)

    assert broker.update_queue(session_obj) == []

    # add a queued request to the database
    request = mock_system_request(status="queued")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()

    assert broker.update_queue(session_obj)[0].request_uid == request_uid


def test_broker_choose_request(mocker: pytest_mock.plugin.MockerFixture) -> None:
    broker = dispatcher.Broker(client=CLIENT)
    number_of_requests = 5

    def mock_get_queued_system_requests() -> list[db.SystemRequest]:
        return [mock_system_request(created_at=i) for i in range(number_of_requests)]

    mocker.patch(
        "cads_broker.dispatcher.Broker.update_queue",
        return_value=mock_get_queued_system_requests(),
    )
    print(type(mocker))
    request = broker.choose_request()
    if isinstance(request.request_metadata, dict):
        assert request.request_metadata["created_at"] == number_of_requests - 1


def test_broker_priority() -> None:
    broker = dispatcher.Broker(client=CLIENT)

    assert broker.priority(mock_system_request(created_at=9)) == 9
    assert broker.priority(mock_system_request(created_at=0)) == 0
    request = mock_system_request(created_at=0)
    request.request_metadata = None
    assert broker.priority(request) == 0


def test_broker_fetch_dask_task_status(
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    broker = dispatcher.Broker(client=CLIENT)

    def mock_get_tasks() -> dict[str, str]:
        return {"dask-scheduler": "completed"}

    mocker.patch(
        "cads_broker.dispatcher.get_tasks",
        return_value=mock_get_tasks(),
    )

    # add a pending future to the broker
    broker.futures = {"future": distributed.Future("future", CLIENT)}

    assert (
        broker.fetch_dask_task_status("future")
        == dispatcher.DASK_STATUS_TO_STATUS["pending"]
    )
    assert broker.fetch_dask_task_status("dask-scheduler") == "completed"
    assert broker.fetch_dask_task_status("unknown") == "queued"


def test_broker_update_database(session_obj: sa.orm.sessionmaker) -> None:
    broker = dispatcher.Broker(client=CLIENT)

    # add a running request to the database
    request = mock_system_request(status="running")
    request_uid = request.request_uid
    with session_obj() as session:
        session.add(request)
        session.commit()

    # add a running request to the dask scheduler
    broker.futures = {request_uid: distributed.Future(request_uid, CLIENT)}
    # update the database
    broker.update_database(session_obj)

    # check that the request is still running
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        updated_request = session.scalars(statement).one()
    assert updated_request.status == "running"

    # change state of the request to finished
    broker.futures = {request_uid: broker.client.submit(lambda x: x, request_uid)}
    broker.futures[request_uid].result()
    # update the database
    broker.update_database(session_obj)

    # check that the request is now completed
    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == request_uid
        )
        updated_request = session.scalars(statement).one()
    assert updated_request.status == "completed"

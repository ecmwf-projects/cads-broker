import datetime
import random
import uuid

import distributed
import pytest
import pytest_mock
import sqlalchemy as sa

from cads_broker import database as db
from cads_broker import dispatcher

# create client object and connect to local cluster
CLIENT = distributed.Client(distributed.LocalCluster())


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


def test_broker_update_database(
    mocker: pytest_mock.plugin.MockerFixture, session_obj: sa.orm.sessionmaker
) -> None:

    broker = dispatcher.Broker(client=CLIENT, max_running_requests=1)

    successful_request = mock_system_request(request_uid="successful", status="running")
    queued_in_dask_request = mock_system_request(
        request_uid="queued_in_dask", status="running"
    )
    with session_obj() as session:
        session.add(successful_request)
        session.add(queued_in_dask_request)
        session.commit()

    mocker.patch(
        "cads_broker.dispatcher.Broker.fetch_dask_task_status",
        new=lambda x, y: y,
    )

    broker.update_database(session_obj)

    with session_obj() as session:
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == "successful"
        )
        assert session.scalars(statement).first().status == "successful"

        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == "queued_in_dask"
        )
        assert session.scalars(statement).first().status == "running"

    unknown_request = mock_system_request(request_uid="unknown", status="running")
    with session_obj() as session:
        session.add(unknown_request)
        session.commit()

    with pytest.raises(ValueError):
        broker.update_database(session_obj)


def test_broker_choose_request(mocker: pytest_mock.plugin.MockerFixture) -> None:
    broker = dispatcher.Broker(client=CLIENT, max_running_requests=1)
    number_of_requests = 5

    def get_accepted_requests() -> list[db.SystemRequest]:
        return [
            mock_system_request(created_at=datetime.datetime(day=i, month=1, year=2020))
            for i in range(1, number_of_requests + 1)
        ]

    mocker.patch(
        "cads_broker.database.get_accepted_requests",
        return_value=get_accepted_requests(),
    )
    request = broker.choose_request()
    assert request.created_at.day == 1


def test_broker_priority() -> None:
    broker = dispatcher.Broker(client=CLIENT, max_running_requests=1)

    created_at = datetime.datetime(day=1, month=1, year=2022, hour=1)
    assert (
        broker.priority(mock_system_request(created_at=created_at))
        == created_at.timestamp()
    )


def test_broker_fetch_dask_task_status(
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    broker = dispatcher.Broker(client=CLIENT, max_running_requests=1)

    def mock_get_tasks() -> dict[str, str]:
        return {"dask-scheduler": "successful"}

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
    assert broker.fetch_dask_task_status("dask-scheduler") == "successful"
    assert broker.fetch_dask_task_status("unknown") == "accepted"
import datetime
import random
import uuid

import distributed
import pytest_mock
import sqlalchemy as sa

from cads_broker import Environment, dispatcher
from cads_broker import database as db
from cads_broker.qos import QoS, Rule

# create client object and connect to local cluster
CLIENT = distributed.Client(distributed.LocalCluster())


def mock_system_request(
    status: str = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_id=random.randrange(1, 100),
        request_uid=request_uid or str(uuid.uuid4()),
        status=status,
        created_at=created_at,
        started_at=None,
        request_body={"request_type": "test"},
        request_metadata={},
    )
    return system_request


def test_broker_sync_database(
    mocker: pytest_mock.plugin.MockerFixture, session_obj: sa.orm.sessionmaker
) -> None:
    environment = Environment.Environment()
    qos = QoS.QoS(rules=Rule.RuleSet(), environment=environment, rules_hash="")
    broker = dispatcher.Broker(
        client=CLIENT,
        environment=environment,
        qos=qos,
        address="scheduler-address",
        session_maker=session_obj,
    )

    in_futures_request_uid = str(uuid.uuid4())
    in_dask_request_uid = str(uuid.uuid4())
    lost_request_uid = str(uuid.uuid4())
    in_futures_request = mock_system_request(
        request_uid=in_futures_request_uid, status="running"
    )
    in_dask_request = mock_system_request(
        request_uid=in_dask_request_uid, status="running"
    )
    lost_request = mock_system_request(request_uid=lost_request_uid, status="running")
    with session_obj() as session:
        session.add(in_futures_request)
        session.add(in_dask_request)
        session.add(lost_request)
        session.commit()

    def mock_get_tasks() -> dict[str, str]:
        return {in_dask_request_uid: "..."}

    mocker.patch(
        "cads_broker.dispatcher.get_tasks",
        return_value=mock_get_tasks(),
    )
    broker.futures = {in_futures_request_uid: "..."}

    with session_obj() as session:
        broker.sync_database(session=session)

        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == in_futures_request_uid
        )
        assert session.scalars(statement).first().status == "running"

        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == in_dask_request_uid
        )
        assert session.scalars(statement).first().status == "running"

        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.request_uid == lost_request_uid
        )
        output_request = session.scalars(statement).first()
        assert output_request.status == "accepted"
        assert output_request.request_metadata.get("resubmit_number") == 1

import datetime
import logging
import uuid
from typing import Any

import distributed
import pytest_mock
import sqlalchemy as sa

from cads_broker import Environment, dispatcher
from cads_broker import database as db
from cads_broker.qos import QoS, Rule

# create client object and connect to local cluster
CLIENT = distributed.Client(distributed.LocalCluster())

logger = logging.getLogger("test")


def mock_config(hash: str = "", config: dict[str, Any] = {}, form: dict[str, Any] = {}):
    adaptor_metadata = db.AdaptorProperties(
        hash=hash,
        config=config,
        form=form,
    )
    return adaptor_metadata


def mock_system_request(
    status: str = "accepted",
    created_at: datetime.datetime = datetime.datetime.now(),
    request_uid: str | None = None,
    user_uid: str = "user_uid",
    adaptor_properties_hash: str = "adaptor_properties_hash",
) -> db.SystemRequest:
    system_request = db.SystemRequest(
        request_uid=request_uid or str(uuid.uuid4()),
        status=status,
        user_uid=user_uid,
        created_at=created_at,
        started_at=None,
        request_body={"request_type": "test"},
        request_metadata={},
        adaptor_properties_hash=adaptor_properties_hash,
    )
    return system_request


def test_broker_sync_database(
    mocker: pytest_mock.plugin.MockerFixture, session_obj: sa.orm.sessionmaker
) -> None:
    environment = Environment.Environment()
    qos = QoS.QoS(
        rules=Rule.RuleSet(), environment=environment, rules_hash="", logger=logger
    )
    broker = dispatcher.Broker(
        client=CLIENT,
        environment=environment,
        qos=qos,
        address="scheduler-address",
        session_maker_read=session_obj,
        session_maker_write=session_obj,
    )

    in_futures_request_uid = str(uuid.uuid4())
    in_dask_request_uid = str(uuid.uuid4())
    lost_request_uid = str(uuid.uuid4())
    # dismissed_request_uid = str(uuid.uuid4())
    adaptor_metadata = mock_config()
    in_futures_request = mock_system_request(
        request_uid=in_futures_request_uid,
        status="running",
        adaptor_properties_hash=adaptor_metadata.hash,
    )
    in_dask_request = mock_system_request(
        request_uid=in_dask_request_uid,
        status="running",
        adaptor_properties_hash=adaptor_metadata.hash,
    )
    lost_request = mock_system_request(
        request_uid=lost_request_uid,
        status="running",
        adaptor_properties_hash=adaptor_metadata.hash,
    )
    # dismissed_request = mock_system_request(
    #     request_uid=dismissed_request_uid,
    #     status="dismissed",
    #     adaptor_properties_hash=adaptor_metadata.hash,
    # )
    with session_obj() as session:
        session.add(adaptor_metadata)
        session.add(in_futures_request)
        session.add(in_dask_request)
        session.add(lost_request)
        # session.add(dismissed_request)
        session.commit()

    def mock_get_tasks() -> dict[str, str]:
        return {in_dask_request_uid: {"state": "...", "exception": None}}

    mocker.patch(
        "cads_broker.dispatcher.get_tasks_from_scheduler",
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

        # with pytest.raises(db.NoResultFound):
        #     with session_obj() as session:
        #         db.get_request(dismissed_request_uid, session=session)

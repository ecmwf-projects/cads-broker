from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from cads_broker import SystemRequest
from cads_broker.main import app
from tests.test_02_database import mock_system_request

client = TestClient(app)


@pytest.mark.skip()
@pytest.mark.parametrize(
    "entry_num",
    [1, 100, 1000, 10000]
)
def test_broker_queue(entry_num, session_obj):
    """ Test custom metric and check performance time (count accepted requests)"""
    print(f"Num entries {entry_num}")
    with session_obj() as session:
        # clean table
        session.query(SystemRequest).delete()
        session.commit()
        for i in range(entry_num):
            accepted_request = mock_system_request(status="accepted")
            accepted_request.request_id = i
            session.add(accepted_request)
            session.commit()
    start = datetime.now()
    response = client.get("/metrics")
    assert "broker_queue" in response.text
    print(datetime.now() - start)

"""Unit tests for the dask_utils module."""

import threading
from unittest.mock import Mock, patch

import pytest
from distributed import Client, comm

from cads_broker import dask_utils


def test_add_client() -> None:
    """Test adding a client to the schedulers dict."""
    schedulers = dask_utils.Schedulers()
    mock_client = Mock(spec=Client)

    schedulers.add_client("client_1", mock_client)

    retrieved_client = schedulers.get_client("client_1")
    assert retrieved_client is mock_client

def test_pop_client() -> None:
    """Test popping a client from the schedulers dict."""
    schedulers = dask_utils.Schedulers()
    mock_client = Mock(spec=Client)
    schedulers.add_client("client_1", mock_client)

    popped_client = schedulers.pop_client("client_1")

    assert popped_client is mock_client
    assert schedulers.get_client("client_1") is None

def test_pop_client_not_found() -> None:
    """Test popping a non-existent client returns None."""
    schedulers = dask_utils.Schedulers()

    popped_client = schedulers.pop_client("non_existent")

    assert popped_client is None

def test_get_client() -> None:
    """Test getting a client from the schedulers dict."""
    schedulers = dask_utils.Schedulers()
    mock_client = Mock(spec=Client)
    schedulers.add_client("client_1", mock_client)

    retrieved_client = schedulers.get_client("client_1")

    assert retrieved_client is mock_client

def test_get_client_not_found() -> None:
    """Test getting a non-existent client returns None."""
    schedulers = dask_utils.Schedulers()

    retrieved_client = schedulers.get_client("non_existent")

    assert retrieved_client is None

def test_get_clients_list() -> None:
    """Test getting all clients as a list."""
    schedulers = dask_utils.Schedulers()
    mock_client_1 = Mock(spec=Client)
    mock_client_2 = Mock(spec=Client)
    schedulers.add_client("client_1", mock_client_1)
    schedulers.add_client("client_2", mock_client_2)

    clients = list(schedulers.get_clients_list())

    assert len(clients) == 2
    assert mock_client_1 in clients
    assert mock_client_2 in clients

def test_get_clients_addresses() -> None:
    """Test getting all client addresses (keys)."""
    schedulers = dask_utils.Schedulers()
    mock_client_1 = Mock(spec=Client)
    mock_client_2 = Mock(spec=Client)
    schedulers.add_client("client_1", mock_client_1)
    schedulers.add_client("client_2", mock_client_2)

    addresses = list(schedulers.get_clients_addresses())

    assert len(addresses) == 2
    assert "client_1" in addresses
    assert "client_2" in addresses

def test_get_clients_items() -> None:
    """Test getting all client items."""
    schedulers = dask_utils.Schedulers()
    mock_client_1 = Mock(spec=Client)
    mock_client_2 = Mock(spec=Client)
    schedulers.add_client("client_1", mock_client_1)
    schedulers.add_client("client_2", mock_client_2)

    items = list(schedulers.get_clients_items())

    assert len(items) == 2
    assert ("client_1", mock_client_1) in items
    assert ("client_2", mock_client_2) in items

def test_thread_safety() -> None:
    """Test thread safety of Schedulers class."""
    schedulers = dask_utils.Schedulers()
    mock_clients = [Mock(spec=Client) for _ in range(10)]
    results = []

    def add_clients():
        for i, client in enumerate(mock_clients):
            schedulers.add_client(f"client_{i}", client)

    def get_clients():
        for i in range(len(mock_clients)):
            client = schedulers.get_client(f"client_{i}")
            if client:
                results.append(client)

    threads = [
        threading.Thread(target=add_clients),
        threading.Thread(target=get_clients),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(schedulers.get_clients_list()) == 10


def test_get_number_of_workers_all_running() -> None:
    """Test getting number of running workers."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {
        "workers": {
            "worker_1": {"status": "running"},
            "worker_2": {"status": "running"},
            "worker_3": {"status": "running"},
        }
    }

    # Clear cache before test
    dask_utils.get_number_of_workers.cache_clear()

    result = dask_utils.get_number_of_workers(mock_client)

    assert result == 3

def test_get_number_of_workers_mixed_statuses() -> None:
    """Test getting number of workers with mixed statuses."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {
        "workers": {
            "worker_1": {"status": "running"},
            "worker_2": {"status": "idle"},
            "worker_3": {"status": "running"},
            "worker_4": {"status": "closed"},
        }
    }

    # Clear cache before test
    dask_utils.get_number_of_workers.cache_clear()

    result = dask_utils.get_number_of_workers(mock_client)

    assert result == 2

def test_get_number_of_workers_no_workers() -> None:
    """Test getting number of workers when no workers are available."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {"workers": {}}

    # Clear cache before test
    dask_utils.get_number_of_workers.cache_clear()

    result = dask_utils.get_number_of_workers(mock_client)

    assert result == 0

def test_get_number_of_workers_no_status() -> None:
    """Test getting number of workers when status is missing."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {
        "workers": {
            "worker_1": {},
            "worker_2": {"status": "running"},
        }
    }

    # Clear cache before test
    dask_utils.get_number_of_workers.cache_clear()

    result = dask_utils.get_number_of_workers(mock_client)

    assert result == 1


def test_get_total_number_of_workers() -> None:
    """Test getting total number of workers across multiple clients."""
    mock_client_1 = Mock(spec=Client)
    mock_client_1.scheduler_info.return_value = {
        "workers": {
            "worker_1": {"status": "running"},
            "worker_2": {"status": "running"},
        }
    }

    mock_client_2 = Mock(spec=Client)
    mock_client_2.scheduler_info.return_value = {
        "workers": {
            "worker_3": {"status": "running"},
            "worker_4": {"status": "idle"},
        }
    }

    # Clear cache before test
    dask_utils.get_number_of_workers.cache_clear()

    result = dask_utils.get_total_number_of_workers([mock_client_1, mock_client_2])

    assert result == 3

def test_get_total_number_of_workers_empty_list() -> None:
    """Test getting total number of workers with no clients."""
    # Clear cache before test
    dask_utils.get_number_of_workers.cache_clear()

    result = dask_utils.get_total_number_of_workers([])

    assert result == 0


def test_create_dask_client_success() -> None:
    """Test successful creation of a dask client."""
    with patch("distributed.Client") as mock_client_class:
        mock_client_instance = Mock(spec=Client)
        mock_client_class.return_value = mock_client_instance

        result = dask_utils.create_dask_client("tcp://scheduler:8786")

        mock_client_class.assert_called_once_with(
            "tcp://scheduler:8786", heartbeat_interval=1000
        )
        assert result is mock_client_instance

def test_create_dask_client_oserror(mocker) -> None:
    """Test create_dask_client logs error on OSError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")

    with patch("distributed.Client") as mock_client_class:
        mock_client_class.side_effect = OSError("Connection refused")

        result = dask_utils.create_dask_client("tcp://invalid:8786")

        assert result is None
        mock_logger.error.assert_called_once()
        call_kwargs = mock_logger.error.call_args[1]
        assert "scheduler_url" in call_kwargs
        assert "tcp://invalid:8786" in call_kwargs.values()


def test_get_workers_resources() -> None:
    """Test getting workers resources."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {
        "workers": {
            "worker_1": {"resources": {"gpu": 1}},
            "worker_2": {"resources": {"cpu": 4, "memory": 8}},
            "worker_3": {"resources": {}},
        }
    }

    # Clear cache before test
    dask_utils.get_workers_resources.cache_clear()

    result = dask_utils.get_workers_resources(mock_client)

    assert "gpu" in result
    assert "cpu" in result
    assert "memory" in result
    assert len(result) == 3

def test_get_workers_resources_no_workers() -> None:
    """Test getting workers resources when no workers are available."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {"workers": {}}

    # Clear cache before test
    dask_utils.get_workers_resources.cache_clear()

    result = dask_utils.get_workers_resources(mock_client)

    assert result == []

def test_get_workers_resources_no_resources() -> None:
    """Test getting workers resources when workers have no resources."""
    mock_client = Mock(spec=Client)
    mock_client.scheduler_info.return_value = {
        "workers": {
            "worker_1": {},
            "worker_2": {"resources": {"gpu": 1}},
        }
    }

    # Clear cache before test
    dask_utils.get_workers_resources.cache_clear()

    result = dask_utils.get_workers_resources(mock_client)

    assert "gpu" in result
    assert len(result) == 1


def test_get_tasks_from_scheduler_success() -> None:
    """Test successfully getting tasks from scheduler."""
    mock_client = Mock(spec=Client)
    expected_tasks = {
        "task_1": {"state": "running", "exception": None},
        "task_2": {"state": "pending", "exception": None},
    }
    mock_client.run_on_scheduler.return_value = expected_tasks

    # Clear cache before test
    dask_utils.get_tasks_from_scheduler.cache_clear()

    result = dask_utils.get_tasks_from_scheduler(mock_client)

    assert result == expected_tasks

def test_get_tasks_from_scheduler_commclosed_error(mocker) -> None:
    """Test get_tasks_from_scheduler handles CommClosedError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_client = Mock(spec=Client)
    mock_client.scheduler = Mock()
    mock_client.scheduler.address = "tcp://scheduler:8786"
    mock_client.run_on_scheduler.side_effect = comm.core.CommClosedError(
        "Connection closed"
    )

    # Clear cache before test
    dask_utils.get_tasks_from_scheduler.cache_clear()

    result = dask_utils.get_tasks_from_scheduler(mock_client)

    assert result == {}
    mock_logger.error.assert_called_once()

def test_get_tasks_from_scheduler_oserror(mocker) -> None:
    """Test get_tasks_from_scheduler handles OSError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_client = Mock(spec=Client)
    mock_client.scheduler = Mock()
    mock_client.scheduler.address = "tcp://scheduler:8786"
    mock_client.run_on_scheduler.side_effect = OSError("Connection refused")

    # Clear cache before test
    dask_utils.get_tasks_from_scheduler.cache_clear()

    result = dask_utils.get_tasks_from_scheduler(mock_client)

    assert result == {}
    mock_logger.error.assert_called_once()


def test_kill_job_on_worker_no_client(mocker) -> None:
    """Test kill_job_on_worker when client is None."""
    mock_session = Mock()

    # Should not raise any error
    dask_utils.kill_job_on_worker(None, "request_123", mock_session)

def test_kill_job_on_worker_success(mocker) -> None:
    """Test successfully killing a job on worker."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_session = Mock()
    mock_database_get_worker_pid = mocker.patch(
        "cads_broker.database.get_worker_pid"
    )
    mock_client = Mock(spec=Client)

    mock_database_get_worker_pid.return_value = [
        {"pid": 12345, "worker": "worker_1"},
        {"pid": 12346, "worker": "worker_2"},
    ]

    dask_utils.kill_job_on_worker(mock_client, "request_123", mock_session)

    assert mock_client.run.call_count == 2
    mock_logger.info.assert_called()

def test_kill_job_on_worker_keyerror(mocker) -> None:
    """Test kill_job_on_worker handles KeyError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_session = Mock()
    mock_database_get_worker_pid = mocker.patch(
        "cads_broker.database.get_worker_pid"
    )
    mock_client = Mock(spec=Client)

    mock_database_get_worker_pid.return_value = [
        {"pid": 12345, "worker": "worker_1"},
    ]
    mock_client.run.side_effect = KeyError("Worker not found")

    dask_utils.kill_job_on_worker(mock_client, "request_123", mock_session)

    mock_logger.warning.assert_called()

def test_kill_job_on_worker_process_lookup_error(mocker) -> None:
    """Test kill_job_on_worker handles ProcessLookupError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_session = Mock()
    mock_database_get_worker_pid = mocker.patch(
        "cads_broker.database.get_worker_pid"
    )
    mock_client = Mock(spec=Client)

    mock_database_get_worker_pid.return_value = [
        {"pid": 12345, "worker": "worker_1"},
    ]
    mock_client.run.side_effect = ProcessLookupError("Process not found")

    dask_utils.kill_job_on_worker(mock_client, "request_123", mock_session)

    mock_logger.warning.assert_called()


def test_cancel_jobs_on_scheduler_success() -> None:
    """Test successfully canceling jobs on scheduler."""
    mock_client = Mock(spec=Client)
    job_ids = ["job_1", "job_2"]

    dask_utils.cancel_jobs_on_scheduler(mock_client, job_ids)

    mock_client.run_on_scheduler.assert_called_once()
    call_kwargs = mock_client.run_on_scheduler.call_args[1]
    assert call_kwargs["job_ids"] == job_ids

def test_cancel_jobs_on_scheduler_commclosed_error(mocker) -> None:
    """Test cancel_jobs_on_scheduler handles CommClosedError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_client = Mock(spec=Client)
    job_ids = ["job_1"]

    mock_client.run_on_scheduler.side_effect = comm.core.CommClosedError(
        "Connection closed"
    )

    dask_utils.cancel_jobs_on_scheduler(mock_client, job_ids)

    mock_logger.error.assert_called_once()

def test_cancel_jobs_on_scheduler_oserror(mocker) -> None:
    """Test cancel_jobs_on_scheduler handles OSError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_client = Mock(spec=Client)
    job_ids = ["job_1"]

    mock_client.run_on_scheduler.side_effect = OSError("Connection refused")

    dask_utils.cancel_jobs_on_scheduler(mock_client, job_ids)

    mock_logger.error.assert_called_once()

def test_cancel_jobs_on_scheduler_attribute_error(mocker) -> None:
    """Test cancel_jobs_on_scheduler handles AttributeError."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_client = Mock(spec=Client)
    job_ids = ["job_1"]

    mock_client.run_on_scheduler.side_effect = AttributeError("Attribute not found")

    dask_utils.cancel_jobs_on_scheduler(mock_client, job_ids)

    mock_logger.error.assert_called_once()


def test_clean_scheduler_memory() -> None:
    """Test cleaning scheduler memory."""
    mock_client = Mock(spec=Client)

    dask_utils.clean_scheduler_memory(mock_client)

    mock_client.run_on_scheduler.assert_called_once()


def test_clean_scheduler_memory_for_all_clients(mocker) -> None:
    """Test cleaning scheduler memory for all clients."""
    mocker.patch("cads_broker.dask_utils.logger")
    mock_sleep = mocker.patch("time.sleep")
    mock_clean = mocker.patch("cads_broker.dask_utils.clean_scheduler_memory")

    mock_client_1 = Mock(spec=Client)
    mock_client_2 = Mock(spec=Client)

    schedulers = dask_utils.Schedulers()
    schedulers.add_client("client_1", mock_client_1)
    schedulers.add_client("client_2", mock_client_2)

    # Simulate the function running and then stopping after the first iteration
    mock_sleep.side_effect = [None, KeyboardInterrupt()]

    with pytest.raises(KeyboardInterrupt):
        dask_utils.clean_scheduler_memory_for_all_clients(schedulers, timeout_seconds=60)

    # Check that sleep was called with the correct timeout
    mock_sleep.assert_called_with(60)
    # Check that clean_scheduler_memory was called for each client
    assert mock_clean.call_count == 2

def test_clean_scheduler_memory_for_all_clients_with_error(mocker) -> None:
    """Test cleaning scheduler memory handles errors."""
    mock_logger = mocker.patch("cads_broker.dask_utils.logger")
    mock_sleep = mocker.patch("time.sleep")
    mock_clean = mocker.patch("cads_broker.dask_utils.clean_scheduler_memory")

    mock_client = Mock(spec=Client)
    mock_clean.side_effect = Exception("Cleaning error")

    schedulers = dask_utils.Schedulers()
    schedulers.add_client("client_1", mock_client)

    # Simulate the function running and then stopping after the first iteration
    mock_sleep.side_effect = [None, KeyboardInterrupt()]

    with pytest.raises(KeyboardInterrupt):
        dask_utils.clean_scheduler_memory_for_all_clients(schedulers, timeout_seconds=60)

    # Check that error logging was called
    mock_logger.error.assert_called()

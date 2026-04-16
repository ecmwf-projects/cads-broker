import os
import signal
import threading
import time
from typing import Any, Iterable

import cachetools
import distributed
import structlog

from cads_broker import config, database

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

CONFIG = config.BrokerConfig()


class Schedulers:
    def __init__(self):
        self.clients = {}
        self.lock = threading.Lock()

    def add_client(self, client_id: str, client: distributed.Client) -> None:
        with self.lock:
            self.clients[client_id] = client

    def pop_client(self, client_id: str) -> distributed.Client | None:
        with self.lock:
            return self.clients.pop(client_id, None)

    def get_client(self, client_id: str) -> distributed.Client | None:
        with self.lock:
            return self.clients.get(client_id)

    def get_clients_list(self) -> Iterable[distributed.Client]:
        with self.lock:
            return self.clients.values()

    def get_clients_addresses(self) -> Iterable[str]:
        with self.lock:
            return self.clients.keys()

    def get_clients_items(self):
        with self.lock:
            return self.clients.items()


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=CONFIG.broker_get_number_of_workers_cache_time
    ),
    info=True,
)
def get_number_of_workers(client: distributed.Client) -> int:
    workers = client.scheduler_info().get("workers", {})
    number_of_workers = len(
        [w for w in workers.values() if w.get("status", None) == "running"]
    )
    return number_of_workers


def get_total_number_of_workers(clients: Iterable[distributed.Client]) -> int:
    number_of_workers = 0
    for client in clients:
        number_of_workers += get_number_of_workers(client)
    return number_of_workers


def create_dask_client(scheduler_url):
    try:
        client = distributed.Client(scheduler_url, heartbeat_interval=1000)
        return client
    except OSError as e:
        logger.error(
            "Cannot connect to scheduler",
            function="create_dask_client",
            scheduler_url=scheduler_url,
            error=str(e),
        )
        return


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=CONFIG.broker_get_workers_resources_cache_time
    ),
    info=True,
)
def get_workers_resources(client: distributed.Client) -> list[str]:
    workers_resources = []
    for worker in client.scheduler_info().get("workers", {}).values():
        workers_resources.extend(list(worker.get("resources", {}).keys()))
    return workers_resources


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=CONFIG.broker_get_tasks_from_scheduler_cache_time
    ),
    info=True,
)
def get_tasks_from_scheduler(client: distributed.Client) -> Any:
    """Get the tasks from the scheduler.

    This function is executed on the scheduler pod.
    """

    def get_tasks_on_scheduler(dask_scheduler: distributed.Scheduler) -> dict[str, Any]:
        tasks = {}
        for task_id, task in dask_scheduler.tasks.items():
            tasks[task_id] = {
                "state": task.state,
                "exception": task.exception,
            }
        return tasks

    try:
        return client.run_on_scheduler(get_tasks_on_scheduler)
    except (distributed.comm.core.CommClosedError, OSError) as e:
        logger.error(
            "Cannot connect to scheduler",
            scheduler_url=client.scheduler.address,
            error=str(e),
        )
        return {}


def kill_job_on_worker(
    client: distributed.Client | None,
    request_uid: str,
    session: database.sa.orm.Session,
) -> None:
    """Kill the job on the worker."""
    # loop on all the processes related to the request_uid
    if client is None:
        return
    for worker_pid_event in database.get_worker_pid(request_uid, session=session):
        pid = worker_pid_event["pid"]
        worker_ip = worker_pid_event["worker"]
        try:
            client.run(
                os.kill,
                pid,
                signal.SIGTERM,
                workers=[worker_ip],
                nanny=True,
                on_error="ignore",
            )
            logger.info(
                "killed job on worker", job_id=request_uid, pid=pid, worker_ip=worker_ip
            )
        except (KeyError, NameError):
            logger.warning(
                "worker not found", job_id=request_uid, pid=pid, worker_ip=worker_ip
            )
        except ProcessLookupError:
            logger.warning(
                "process not found", job_id=request_uid, pid=pid, worker_ip=worker_ip
            )


def cancel_jobs_on_scheduler(client: distributed.Client, job_ids: list[str]) -> None:
    """Cancel jobs on the dask scheduler.

    This function is executed on the scheduler pod. This just cancel the jobs on the scheduler.
    See https://stackoverflow.com/questions/49203128/how-do-i-stop-a-running-task-in-dask.
    """

    def cancel_jobs(dask_scheduler: distributed.Scheduler, job_ids: list[str]) -> None:
        for job_id in job_ids:
            if job_id in dask_scheduler.tasks:
                dask_scheduler.transitions(
                    {job_id: "cancelled"}, stimulus_id="manual-cancel"
                )

    try:
        return client.run_on_scheduler(cancel_jobs, job_ids=job_ids)
    except (distributed.comm.core.CommClosedError, OSError, AttributeError) as e:
        logger.error(
            "Cannot connect to scheduler",
            function="cancel_jobs_on_scheduler",
            job_ids=job_ids,
            error=str(e),
        )


def clean_scheduler_memory(client: distributed.Client):
    """
    Safely flushes Dask's internal BatchedSend network logs.

    This prevents dask scheduler memory leaks.
    """

    def flush_network_logs(dask_scheduler):
        import gc

        from distributed.batched import BatchedSend

        # Find all active and dead network buffers
        for obj in gc.get_objects():
            if isinstance(obj, BatchedSend) and hasattr(obj, "recent_message_log"):
                # Empty the log to sever the task references
                obj.recent_message_log.clear()

    client.run_on_scheduler(flush_network_logs)


def clean_scheduler_memory_for_all_clients(
    schedulers: Schedulers, timeout_seconds: int = 300
):
    logger.info(
        "Starting periodic scheduler memory cleanup thread",
        timeout_seconds=timeout_seconds,
    )
    while True:
        time.sleep(timeout_seconds)
        for client_address, client in schedulers.get_clients_items():
            logger.info(
                "Cleaning scheduler memory for client",
                client_address=client_address,
            )
            try:
                clean_scheduler_memory(client)
            except Exception as e:
                logger.error(
                    "Error while cleaning scheduler memory for client",
                    client_address=client_address,
                    error=str(e),
                )
            logger.info(
                "Finished cleaning scheduler memory for client",
                client_address=client_address,
            )

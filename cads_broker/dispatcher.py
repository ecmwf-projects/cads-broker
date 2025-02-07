import datetime
import hashlib
import io
import os
import pickle
import signal
import threading
import time
import traceback
from typing import Any

import attrs
import cachetools
import distributed
import sqlalchemy as sa
import structlog
from typing_extensions import Iterable

try:
    from cads_worker import worker
except ModuleNotFoundError:
    pass

from cads_broker import Environment, config, factory
from cads_broker import database as db
from cads_broker.qos import QoS

config.configure_logger()
logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


DASK_STATUS_TO_STATUS = {
    "pending": "running",  # Pending status in dask is the same as running status in broker
    "processing": "running",
    "error": "failed",
    "finished": "successful",
}

ONE_SECOND = datetime.timedelta(seconds=1)
ONE_MINUTE = ONE_SECOND * 60
ONE_HOUR = ONE_MINUTE * 60
CONFIG = config.BrokerConfig()


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


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(maxsize=1024, ttl=CONFIG.broker_qos_rules_cache_time),
    info=True,
)
def get_rules_hash(rules_path: str):
    if rules_path is None or not os.path.exists(rules_path):
        rules = ""
    else:
        with open(rules_path) as f:
            rules = f.read()
    return hashlib.md5(rules.encode()).hexdigest()


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

    return client.run_on_scheduler(get_tasks_on_scheduler)


def kill_job_on_worker(client: distributed.Client, request_uid: str) -> None:
    """Kill the job on the worker."""
    # loop on all the processes related to the request_uid
    for worker_pid_event in client.get_events(request_uid):
        _, worker_pid_event = worker_pid_event
        pid = worker_pid_event["pid"]
        worker_ip = worker_pid_event["worker"]
        try:
            client.run(
                os.kill,
                pid,
                signal.SIGTERM,
                workers=[worker_ip],
                nanny=True,
            )
            logger.info(
                "killed job on worker", job_id=request_uid, pid=pid, worker_ip=worker_ip
            )
        except (KeyError, NameError):
            logger.warning(
                "worker not found", job_id=request_uid, pid=pid, worker_ip=worker_ip
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

    return client.run_on_scheduler(cancel_jobs, job_ids=job_ids)


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=CONFIG.broker_cancel_stuck_requests_cache_ttl
    ),
    info=True,
)
def cancel_stuck_requests(client: distributed.Client, session: sa.orm.Session) -> None:
    """Get the stuck requests from the database and cancel them on the dask scheduler."""
    stuck_requests = db.get_stuck_requests(
        session=session, minutes=CONFIG.broker_stuck_requests_limit_minutes
    )
    if stuck_requests:
        logger.info(
            f"canceling stuck requests for more than {CONFIG.broker_stuck_requests_limit_minutes} minutes",
            stuck_requests=stuck_requests,
        )
        cancel_jobs_on_scheduler(client, job_ids=stuck_requests)


class Scheduler:
    """A simple scheduler to store the tasks to update the qos_rules in the database.

    It ensures that the scheduler is thread-safe.
    """

    def __init__(self) -> None:
        self.queue: list = list()
        self.index: dict[str, set] = dict()
        self._lock = threading.RLock()

    def append(self, item: Any) -> None:
        if item["kwargs"]["request_uid"] not in self.index.get(
            item["function"].__name__, set()
        ):
            with self._lock:
                self.queue.append(item)
                self.index.setdefault(item["function"].__name__, set()).add(
                    item["kwargs"]["request_uid"]
                )

    def remove(self, item: Any) -> None:
        with self._lock:
            self.queue.remove(item)
            self.index[item["function"].__name__].remove(item["kwargs"]["request_uid"])

    def refresh(self) -> None:
        with self._lock:
            self.queue = list()
            self.index = dict()


def perf_logger(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        stop = time.perf_counter()
        if (elapsed := stop - start) > 1:
            logger.info("performance", function=func.__name__, elapsed=elapsed)
        return result

    return wrapper


def instantiate_qos(session_read: sa.orm.Session, number_of_workers: int) -> QoS.QoS:
    qos_config = QoSRules(number_of_workers=number_of_workers)
    factory.register_functions()
    rules_hash = get_rules_hash(qos_config.rules_path)
    qos = QoS.QoS(
        qos_config.rules,
        qos_config.environment,
        rules_hash=rules_hash,
        logger=logger,
    )
    qos.environment.set_session(session_read)
    return qos


def reload_qos_rules(session: sa.orm.sessionmaker, qos: QoS.QoS) -> None:
    perf_logger(qos.reload_rules)(session=session)
    perf_logger(db.reset_qos_rules)(session, qos)


class Queue:
    """A simple queue to store the requests that have been accepted by the broker.

    - It ensures that the queue is thread-safe.
    - It stores the last created_at datetime of the requests that have been added to the queue.
    """

    def __init__(self) -> None:
        self.queue_dict: dict = dict()
        self._lock = threading.RLock()
        # default value is before the release
        self.last_created_at: datetime.datetime
        self.set_default_last_created_at()

    def set_default_last_created_at(self) -> None:
        self.last_created_at = datetime.datetime(2024, 1, 1)

    def get(self, key: str, default=None) -> Any:
        with self._lock:
            return self.queue_dict.get(key, default)

    def add(self, key: str, item: Any) -> None:
        with self._lock:
            self.queue_dict[key] = item

    @perf_logger
    def add_accepted_requests(self, accepted_requests: dict) -> None:
        with self._lock:
            for request in accepted_requests:
                self.queue_dict[request.request_uid] = request
        if accepted_requests:
            self.last_created_at = max(
                accepted_requests[-1].created_at, self.last_created_at
            )

    def values(self) -> Iterable[Any]:
        with self._lock:
            return self.queue_dict.values()

    def pop(self, key: str, default=None) -> Any:
        with self._lock:
            return self.queue_dict.pop(key, default)

    def len(self) -> int:
        with self._lock:
            return len(self.queue_dict)

    @cachetools.cachedmethod(lambda self: cachetools.TTLCache(maxsize=1024, ttl=60))
    def reset(self) -> None:
        with self._lock:
            self.queue_dict = dict()
            self.set_default_last_created_at()


class QoSRules:
    def __init__(self, number_of_workers) -> None:
        self.environment = Environment.Environment(number_of_workers=number_of_workers)
        self.rules_path = CONFIG.broker_rules_path
        if os.path.exists(self.rules_path):
            self.rules = self.rules_path
        else:
            logger.info("rules file not found", rules_path=self.rules_path)
            parser = QoS.RulesParser(io.StringIO(""), logger=logger)
            self.rules = QoS.RuleSet()
            parser.parse_rules(self.rules, self.environment, raise_exception=False)


def set_running_request(
    request: db.SystemRequest,
    priority: int | None,
    qos: QoS.QoS,
    queue: Queue,
    internal_scheduler: Scheduler,
    session: sa.orm.Session,
) -> db.SystemRequest:
    """Set the status of the request to running and notify the qos rules."""
    request = db.set_request_status(
        request_uid=request.request_uid,
        status="running",
        priority=priority,
        session=session,
    )
    qos.notify_start_of_request(request, scheduler=internal_scheduler)
    queue.pop(request.request_uid)
    return request


def set_successful_request(
    request: db.SystemRequest,
    qos: QoS.QoS,
    internal_scheduler: Scheduler,
    session: sa.orm.Session,
) -> db.SystemRequest:
    """Set the status of the request to successful and notify the qos rules."""
    if request.status == "successful":
        return request
    request = db.set_successful_request(
        request_uid=request.request_uid,
        session=session,
    )
    qos.notify_end_of_request(request, scheduler=internal_scheduler)
    logger.info(
        "job has finished",
        **db.logger_kwargs(request=request),
    )
    return request


def set_failed_request(
    request: db.SystemRequest,
    error_message: str,
    error_reason: str,
    qos: QoS.QoS,
    internal_scheduler: Scheduler,
    session: sa.orm.Session,
) -> db.SystemRequest:
    """Set the status of the request to failed and notify the qos rules."""
    request = db.set_request_status(
        request_uid=request.request_uid,
        status="failed",
        error_message=error_message,
        error_reason=error_reason,
        session=session,
    )
    qos.notify_end_of_request(request, scheduler=internal_scheduler)
    logger.info(
        "job has finished",
        **db.logger_kwargs(request=request),
    )
    return request


def requeue_request(
    request: db.SystemRequest,
    qos: QoS.QoS,
    queue: Queue,
    internal_scheduler: Scheduler,
    session: sa.orm.Session,
) -> db.SystemRequest:
    """Re-queue the request and notify the qos rules."""
    if request.status == "running":
        queued_request = db.requeue_request(request=request, session=session)
        qos.notify_end_of_request(queued_request, scheduler=internal_scheduler)
        queue.add(queued_request.request_uid, request)
        return queued_request
    return request


@attrs.define
class Broker:
    client: distributed.Client
    environment: Environment.Environment
    qos: QoS.QoS
    address: str
    session_maker_read: sa.orm.sessionmaker
    session_maker_write: sa.orm.sessionmaker
    wait_time: float = CONFIG.broker_wait_time
    ttl_cache = cachetools.TTLCache(
        maxsize=1024, ttl=CONFIG.broker_sync_database_cache_time
    )

    futures: dict[str, distributed.Future] = attrs.field(
        factory=dict,
        repr=lambda futures: " ".join(futures.keys()),
    )
    running_requests: int = 0
    internal_scheduler: Scheduler = Scheduler()
    queue: Queue = Queue()

    @classmethod
    def from_address(
        cls,
        address="scheduler:8786",
        session_maker_read: sa.orm.sessionmaker | None = None,
        session_maker_write: sa.orm.sessionmaker | None = None,
    ):
        client = distributed.Client(address)
        session_maker_read = db.ensure_session_obj(session_maker_read, mode="r")
        session_maker_write = db.ensure_session_obj(session_maker_write, mode="w")
        with session_maker_read() as session_read:
            qos = instantiate_qos(session_read, get_number_of_workers(client))
        with session_maker_write() as session:
            reload_qos_rules(session, qos)
        self = cls(
            client=client,
            session_maker_read=session_maker_read,
            session_maker_write=session_maker_write,
            environment=qos.environment,
            qos=qos,
            address=address,
        )
        return self

    def set_number_of_workers(self):
        if self.client.scheduler is None:
            logger.info("Reconnecting to dask scheduler...")
            self.client = distributed.Client(self.address)
        number_of_workers = get_number_of_workers(client=self.client)
        self.environment.number_of_workers = number_of_workers
        return number_of_workers

    def update_number_of_workers(self, session_write):
        """Reload qos rules if number of workers has changed by a number greater than BROKER_WORKERS_GAP."""
        if (
            abs(self.environment.number_of_workers - get_number_of_workers(self.client))
            > CONFIG.broker_workers_gap
        ):
            self.set_number_of_workers()
            reload_qos_rules(session_write, self.qos)
            self.internal_scheduler.refresh()

    def set_request_error_status(
        self, exception, request_uid, session
    ) -> db.SystemRequest:
        """Set the status of the request to failed and write the error message and reason.

        If the error reason is "KilledWorker":
            - if the worker has been killed by the Nanny for memory usage, it add the event for the user
            - if the worker is killed for unknown reasons, it re-queues the request
              if the requeue limit is not reached. This is configurable with the environment variable
        """
        error_message = "".join(traceback.format_exception(exception))
        error_reason = exception.__class__.__name__
        request = db.get_request(request_uid, session=session)
        if request.status != "running":
            return request
        requeue = CONFIG.broker_requeue_on_killed_worker_requests
        if error_reason == "KilledWorker":
            worker_restart_events = self.client.get_events("worker-restart-memory")
            # get info on worker and pid of the killed request
            try:
                worker_pid_event = self.client.get_events(request_uid)[0][1]
            except IndexError:
                worker_restart_events = False
                requeue = True
            if worker_restart_events:
                for event in worker_restart_events:
                    _, job = event
                    if (
                        job["worker"] == worker_pid_event["worker"]
                        and job["pid"] == worker_pid_event["pid"]
                    ):
                        db.add_event(
                            event_type="killed_worker",
                            request_uid=request_uid,
                            message="Worker has been killed by the Nanny due to memory usage."
                            f"{job['worker']=}, {job['pid']=}, {job['rss']=}",
                            session=session,
                        )
                        db.add_event(
                            event_type="user_visible_error",
                            request_uid=request_uid,
                            message=CONFIG.broker_memory_error_user_visible_log,
                            session=session,
                        )
                        request = set_failed_request(
                            request=request,
                            error_message=error_message,
                            error_reason=error_reason,
                            qos=self.qos,
                            internal_scheduler=self.internal_scheduler,
                            session=session,
                        )
                        requeue = False
            if (
                requeue
                and request.request_metadata.get("resubmit_number", 0)
                < CONFIG.broker_requeue_limit
            ):
                logger.info("worker killed: re-queueing", job_id=request_uid)
                request = requeue_request(
                    request=request,
                    qos=self.qos,
                    queue=self.queue,
                    internal_scheduler=self.internal_scheduler,
                    session=session,
                )
        else:
            request = set_failed_request(
                request=request,
                error_message=error_message,
                error_reason=error_reason,
                qos=self.qos,
                internal_scheduler=self.internal_scheduler,
                session=session,
            )
        return request

    def manage_dismissed_request(self, request, session):
        dismission_metadata = request.request_metadata.get("dismission", {})
        db.add_event(
            event_type="user_visible_error",
            request_uid=request.request_uid,
            message=dismission_metadata.get("message", ""),
            session=session,
            commit=False,
        )
        previous_status = dismission_metadata.get("previous_status", "accepted")
        if dismission_metadata.get("reason", "DismissedRequest") == "PermissionError":
            request.status = "failed"
        else:
            request.status = "deleted"
        if previous_status == "running":
            self.qos.notify_end_of_request(request, scheduler=self.internal_scheduler)
        elif previous_status == "accepted":
            self.queue.pop(request.request_uid, None)
            self.qos.notify_dismission_of_request(
                request, scheduler=self.internal_scheduler
            )
        # set finished_at if it is not set
        if request.finished_at is None:
            request.finished_at = datetime.datetime.now()
        logger.info("job has finished", **db.logger_kwargs(request=request))
        return session

    @cachetools.cachedmethod(lambda self: self.ttl_cache)
    @perf_logger
    def sync_database(self, session: sa.orm.Session) -> None:
        """Sync the database with the current status of the dask tasks.

        - If the task is in the futures list it does nothing.
        - If the task is not in the futures list but it is in the scheduler:
            - If the task is in memory (it is successful but it has been lost by the broker),
              it is set to successful.
            - If the task is in error, it is set to failed.
        - If the task is not in the dask scheduler, it is re-queued.
          This behaviour can be changed with an environment variable.
        """
        # the retrieve API sets the status to "dismissed",
        # here the broker fixes the QoS and queue status accordingly
        dismissed_requests = db.get_dismissed_requests(
            session, limit=CONFIG.broker_max_dismissed_requests
        )
        for request in dismissed_requests:
            if future := self.futures.pop(request.request_uid, None):
                future.cancel()
            else:
                # if the request is not in the futures, it means that the request has been lost by the broker
                # try to cancel the job directly on the scheduler
                cancel_jobs_on_scheduler(self.client, job_ids=[request.request_uid])
            kill_job_on_worker(self.client, request.request_uid)
            session = self.manage_dismissed_request(request, session)
        session.commit()

        scheduler_tasks = get_tasks_from_scheduler(self.client)
        requests = db.get_running_requests(session=session)
        if len(scheduler_tasks) == 0 and len(self.futures):
            logger.info(
                f"Scheduler is empty, but futures are {len(self.futures)}. Resetting futures."
            )
            self.futures = {}
        for request in requests:
            # if request is in futures, go on
            if request.request_uid in self.futures:
                # notify start of request if it is not already notified
                self.qos.notify_start_of_request(
                    request, scheduler=self.internal_scheduler
                )
            elif task := scheduler_tasks.get(request.request_uid, None):
                if (state := task["state"]) == "memory":
                    # if the task is in memory and it is not in the futures
                    # it means that the task has been lost by the broker (broker has been restarted)
                    # the task is successful. If the "set_successful_request" function returns None
                    # it means that the request has already been set to successful
                    set_successful_request(
                        request=request,
                        qos=self.qos,
                        internal_scheduler=self.internal_scheduler,
                        session=session,
                    )
                elif state == "erred":
                    exception = pickle.loads(task["exception"])
                    self.set_request_error_status(
                        exception=exception,
                        request_uid=request.request_uid,
                        session=session,
                    )
                # if the task is in processing, it means that the task is still running
                elif state == "processing":
                    # notify start of request if it is not already notified
                    self.qos.notify_start_of_request(
                        request, scheduler=self.internal_scheduler
                    )
                elif state == "released":
                    # notify start of request if it is not already notified
                    requeue_request(
                        request=request,
                        qos=self.qos,
                        queue=self.queue,
                        internal_scheduler=self.internal_scheduler,
                        session=session,
                    )
            # if it doesn't find the request: re-queue it
            else:
                request = db.get_request(request.request_uid, session=session)
                # if the broker finds the cache_id it means that the job has finished
                if request.cache_id:
                    set_successful_request(
                        request=request,
                        qos=self.qos,
                        internal_scheduler=self.internal_scheduler,
                        session=session,
                    )
                # check how many times the request has been re-queued
                elif (
                    CONFIG.broker_requeue_on_lost_requests
                    and request.request_metadata.get("resubmit_number", 0)
                    < CONFIG.broker_requeue_limit
                ):
                    logger.info(
                        "request not found: re-queueing", job_id={request.request_uid}
                    )
                    requeue_request(
                        request=request,
                        qos=self.qos,
                        queue=self.queue,
                        internal_scheduler=self.internal_scheduler,
                        session=session,
                    )
                else:
                    set_failed_request(
                        request=request,
                        error_message="Request not found in dask scheduler",
                        error_reason="not_found",
                        qos=self.qos,
                        internal_scheduler=self.internal_scheduler,
                        session=session,
                    )

    @perf_logger
    def sync_qos_rules(self, session_write) -> None:
        """Sync the qos rules status with the database.

        The update tasks to the qos_rules table are piled up in the internal_scheduler.
        The internal_scheduler is used to minimize the number of updates to the database using:
            - the same session
            - the same qos_rules that are read from the database once and then updated at each step if needed
            - the requests from the self.queue.
              If a request is updated the relative self.queue entry is updated too
        """
        qos_rules = perf_logger(db.get_qos_rules)(session=session_write)
        if tasks_number := len(self.internal_scheduler.queue):
            logger.info("performance", tasks_number=tasks_number)
        for task in list(self.internal_scheduler.queue)[
            : CONFIG.broker_max_internal_scheduler_tasks
        ]:
            # the internal scheduler is used to asynchronously add qos rules to database
            # it returns a new qos rule if a new qos rule is added to database
            request, new_qos_rules = perf_logger(task["function"])(
                session=session_write,
                request=self.queue.get(task["kwargs"].get("request_uid")),
                rules_in_db=qos_rules,
                **task["kwargs"],
            )
            self.internal_scheduler.remove(task)
            # if a new qos rule is added, the new qos rule is added to the list of qos rules
            if request:
                self.queue.add(task["kwargs"].get("request_uid"), request)
            if new_qos_rules:
                qos_rules.update(new_qos_rules)

    @perf_logger
    def sync_futures(self) -> None:
        """Check if the futures are finished, error or cancelled and update the database accordingly.

        In a previous version of the broker used to call the client.add_done_callback method but
        it appears to be unreliable. The futures are now checked in a loop and the status is updated.
        The futures are removed from the list of futures if they are finished in a different for loop to avoid
        "RuntimeError: dictionary changed size during iteration."
        """
        finished_futures = []
        for future in self.futures.values():
            if future.status in ("finished", "error", "cancelled"):
                finished_futures.append(self.on_future_done(future))
        for key in finished_futures:
            self.futures.pop(key, None)

    def on_future_done(self, future: distributed.Future) -> str | None:
        """Update the database status of the request according to the status of the future.

        If the status of the request in the database is not "running", it does nothing and returns None.
        """
        with self.session_maker_write() as session:
            try:
                request = db.get_request(future.key, session=session)
            except db.NoResultFound:
                logger.warning(
                    "request not found",
                    job_id=future.key,
                    dask_status=future.status,
                )
                return future.key
            if request.status != "running":
                return None
            if future.status == "finished":
                # the result is updated in the database by the worker
                set_successful_request(
                    request=request,
                    qos=self.qos,
                    internal_scheduler=self.internal_scheduler,
                    session=session,
                )
            elif future.status == "error":
                exception = future.exception()
                self.set_request_error_status(
                    exception=exception, request_uid=future.key, session=session
                )
            elif future.status != "cancelled":
                # if the dask status is unknown, re-queue it
                requeue_request(
                    request=request,
                    qos=self.qos,
                    queue=self.queue,
                    internal_scheduler=self.internal_scheduler,
                    session=session,
                )
            else:
                # if the dask status is cancelled, the qos has already been reset by sync_database
                return None
            future.release()
        return future.key

    @perf_logger
    def cache_requests_qos_properties(self, requests, session: sa.orm.Session) -> None:
        """Cache the qos properties of the requests."""
        # copy list of requests to avoid RuntimeError: dictionary changed size during iteration
        for request in list(requests):
            try:
                self.qos._properties(request, check_permissions=True)
            except PermissionError as exception:
                db.add_event(
                    event_type="user_visible_error",
                    request_uid=request.request_uid,
                    message=exception.args[0],
                    session=session,
                )
                request = db.get_request(request.request_uid, session=session)
                request.status = "failed"
                request.finished_at = datetime.datetime.now()
                request.response_error = {
                    "reason": "PermissionError",
                    "message": exception.args[0],
                }
                self.queue.pop(request.request_uid, None)
                self.qos.notify_dismission_of_request(
                    request, scheduler=self.internal_scheduler
                )
                logger.info("job has finished", **db.logger_kwargs(request=request))
        session.commit()

    @perf_logger
    def submit_requests(
        self,
        session_write: sa.orm.Session,
        number_of_requests: int,
        candidates: Iterable[db.SystemRequest],
    ) -> None:
        """Check the qos rules and submit the requests to the dask scheduler."""
        queue = sorted(
            candidates,
            key=lambda candidate: self.qos.priority(candidate),
            reverse=True,
        )
        requests_counter = 0
        for request in queue:
            if self.qos.can_run(request, scheduler=self.internal_scheduler):
                if requests_counter <= int(number_of_requests):
                    self.submit_request(
                        request,
                        session=session_write,
                        priority=self.qos.priority(request),
                    )
                requests_counter += 1

    def submit_request(
        self,
        request: db.SystemRequest,
        session: sa.orm.Session,
        priority: int | None = None,
    ) -> None:
        """Submit the request to the dask scheduler and update the qos rules accordingly."""
        request = set_running_request(
            request=request,
            priority=priority,
            queue=self.queue,
            qos=self.qos,
            internal_scheduler=self.internal_scheduler,
            session=session,
        )
        future = self.client.submit(
            worker.submit_workflow,
            key=request.request_uid,
            setup_code=request.request_body.get("setup_code", ""),
            entry_point=request.entry_point,
            config=dict(
                request_uid=request.request_uid,
                user_uid=request.user_uid,
                hostname=os.getenv("CDS_PROJECT_URL"),
            ),
            resources=request.request_metadata.get("resources", {}),
            metadata=request.request_metadata,
        )
        distributed.fire_and_forget(future)
        self.futures[request.request_uid] = future
        logger.info(
            "submitted job to scheduler",
            priority=priority,
            **db.logger_kwargs(request=request),
        )

    def run(self) -> None:
        """Run the broker loop."""
        while True:
            start_loop = time.perf_counter()
            # reset the cache of the qos functions
            db.QOS_FUNCTIONS_CACHE.clear()
            with self.session_maker_read() as session_read:
                if get_rules_hash(self.qos.path) != self.qos.rules_hash:
                    logger.info("reloading qos rules")
                    self.qos = instantiate_qos(
                        session_read, self.environment.number_of_workers
                    )
                    with self.session_maker_write() as session_write:
                        reload_qos_rules(session_write, self.qos)
                        self.internal_scheduler.refresh()
                self.qos.environment.set_session(session_read)
                # expire_on_commit=False is used to detach the accepted requests without an error
                # this is not a problem because accepted requests cannot be modified in this loop
                with self.session_maker_write(expire_on_commit=False) as session_write:
                    # reload qos rules if the number of workers has changed
                    self.update_number_of_workers(session_write)
                    self.sync_qos_rules(session_write)
                    self.sync_futures()
                    self.sync_database(session=session_write)
                    session_write.commit()
                    if (queue_length := self.queue.len()) != (
                        db_queue := db.count_accepted_requests_before(
                            session=session_write,
                            last_created_at=self.queue.last_created_at,
                        )
                    ):
                        # if the internal queue is not in sync with the database, re-sync it
                        logger.info(
                            "re-syncing internal queue",
                            internal_queue=queue_length,
                            db_queue=db_queue,
                        )
                        self.queue.reset()
                    self.cache_requests_qos_properties(
                        self.queue.values(), session_write
                    )

                cancel_stuck_requests(client=self.client, session=session_read)
                running_requests = len(db.get_running_requests(session=session_read))
                queue_length = self.queue.len()
                available_workers = (
                    self.environment.number_of_workers - running_requests
                )
                if queue_length > 0:
                    logger.info(
                        "broker info",
                        available_workers=available_workers,
                        running_requests=running_requests,
                        number_of_workers=self.environment.number_of_workers,
                        futures=len(self.futures),
                    )
                    if available_workers > 0:
                        logger.info("broker info", queued_jobs=queue_length)
                        with self.session_maker_write() as session_write:
                            self.submit_requests(
                                session_write=session_write,
                                number_of_requests=available_workers,
                                candidates=self.queue.values(),
                            )
            time.sleep(max(0, self.wait_time - (time.perf_counter() - start_loop)))

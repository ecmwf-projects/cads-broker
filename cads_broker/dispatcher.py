import datetime
import hashlib
import io
import os
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

WORKERS_MULTIPLIER = float(os.getenv("WORKERS_MULTIPLIER", 1))
ONE_SECOND = datetime.timedelta(seconds=1)


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=float(os.getenv("GET_NUMBER_OF_WORKERS_CACHE_TIME", 10))
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
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=int(os.getenv("QOS_RULES_CACHE_TIME", 10))
    ),
    info=True,
)
def get_rules_hash(rules_path: str):
    if rules_path is None or not os.path.exists(rules_path):
        rules = os.getenv("DEFAULT_RULES", "")
    else:
        with open(rules_path) as f:
            rules = f.read()
    return hashlib.md5(rules.encode()).hexdigest()


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=int(os.getenv("GET_TASKS_FROM_SCHEDULER_CACHE_TIME", 1))
    ),
    info=True,
)
def get_tasks(client: distributed.Client) -> Any:
    def get_tasks_on_scheduler(dask_scheduler: distributed.Scheduler) -> dict[str, str]:
        scheduler_state_to_status = {
            "waiting": "running",  # Waiting status in dask is the same as running status in broker
            "processing": "running",
            "erred": "failed",
            "finished": "successful",
            "no-worker": "accepted",  # if the job is no-worker should be re-submitted
            "memory": "memory",  # the scheduler didn't submit on_future_done
        }
        tasks = {}
        for task_id, task in dask_scheduler.tasks.items():
            tasks[task_id] = scheduler_state_to_status.get(task.state, "accepted")
        return tasks

    return client.run_on_scheduler(get_tasks_on_scheduler)


class Scheduler:
    def __init__(self) -> None:
        self.queue: list = list()
        self._lock = threading.RLock()

    def append(self, item: Any) -> None:
        with self._lock:
            self.queue.append(item)

    def pop(self, index=-1) -> Any:
        with self._lock:
            return self.queue.pop(index)

    def remove(self, item: Any) -> None:
        with self._lock:
            self.queue.remove(item)


def perf_logger(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        stop = time.perf_counter()
        logger.info("performance", function=func.__name__, elapsed=stop - start)
        return result

    return wrapper


class Queue:
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

    def pop(self, key: str) -> Any:
        with self._lock:
            return self.queue_dict.pop(key, None)

    def len(self) -> int:
        with self._lock:
            return len(self.queue_dict)

    def reset(self) -> None:
        with self._lock:
            self.queue_dict = dict()
            self.set_default_last_created_at()


class QoSRules:
    def __init__(self) -> None:
        self.environment = Environment.Environment()
        self.rules_path = os.getenv("RULES_PATH", "/src/rules.qos")
        if os.path.exists(self.rules_path):
            self.rules = self.rules_path
        else:
            parser = QoS.RulesParser(io.StringIO(os.getenv("DEFAULT_RULES", "")))
            self.rules = QoS.RuleSet()
            parser.parse_rules(self.rules, self.environment)


@attrs.define
class Broker:
    client: distributed.Client
    environment: Environment.Environment
    qos: QoS.QoS
    address: str
    session_maker_read: sa.orm.sessionmaker
    session_maker_write: sa.orm.sessionmaker
    wait_time: float = float(os.getenv("BROKER_WAIT_TIME", 2))
    ttl_cache = cachetools.TTLCache(
        maxsize=1024, ttl=int(os.getenv("SYNC_DATABASE_CACHE_TIME", 10))
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
        qos_config = QoSRules()
        factory.register_functions()
        session_maker_read = db.ensure_session_obj(session_maker_read, mode="r")
        session_maker_write = db.ensure_session_obj(session_maker_write, mode="w")
        rules_hash = get_rules_hash(qos_config.rules_path)
        qos = QoS.QoS(
            qos_config.rules,
            qos_config.environment,
            rules_hash=rules_hash,
        )
        with session_maker_write() as session:
            db.reset_qos_rules(session, qos)
        self = cls(
            client=client,
            session_maker_read=session_maker_read,
            session_maker_write=session_maker_write,
            environment=qos_config.environment,
            qos=qos,
            address=address,
        )
        return self

    @property
    def number_of_workers(self):
        if self.client.scheduler is None:
            self.client = distributed.Client(self.address)
        number_of_workers = get_number_of_workers(client=self.client)
        self.environment.number_of_workers = number_of_workers
        return number_of_workers

    @cachetools.cachedmethod(lambda self: self.ttl_cache)
    @perf_logger
    def sync_database(self, session: sa.orm.Session) -> None:
        """Sync the database with the current status of the dask tasks.

        If the task is not in the dask scheduler, it is re-queued.
        """
        # the retrieve API sets the status to "dismissed", here the broker deletes the request
        # this is to better control the status of the QoS
        dismissed_uids = db.update_dismissed_requests(session)
        for uid in dismissed_uids:
            if future := self.futures.pop(uid, None):
                future.cancel()
        if dismissed_uids:
            self.queue.reset()
            self.qos.reload_rules(session)
            db.reset_qos_rules(session, self.qos)
        session.commit()

        statement = sa.select(db.SystemRequest).where(db.SystemRequest.status == "running")
        dask_tasks = get_tasks(self.client)
        for request in session.scalars(statement):
            # if request is in futures, go on
            if request.request_uid in self.futures:
                continue
            # if request is in the scheduler, go on
            elif request.request_uid in dask_tasks:
                continue
            # if it doesn't find the request: re-queue it
            else:
                # FIXME: check if request status has changed
                if os.getenv(
                    "BROKER_REQUEUE_ON_LOST_REQUESTS", True
                ) and request.request_metadata.get("resubmit", 0) < os.getenv(
                    "BROKER_REQUEUE_LIMIT", 3
                ):
                    logger.info(
                        "request not found: re-queueing", job_id={request.request_uid}
                    )
                    db.requeue_request(request_uid=request.request_uid, session=session)
                    self.queue.add(request.request_uid, request)
                    self.qos.notify_end_of_request(
                        request, session, scheduler=self.internal_scheduler
                    )
                else:
                    db.set_request_status(
                        request_uid=request.request_uid,
                        status="failed",
                        error_message="Request not found in dask scheduler",
                        error_reason="not_found",
                        session=session,
                    )
                    self.qos.notify_end_of_request(
                        request, session, scheduler=self.internal_scheduler
                    )

    @perf_logger
    def sync_qos_rules(self, session_write) -> None:
        qos_rules = db.get_qos_rules(session=session_write)
        logger.info("performance", tasks_number=len(self.internal_scheduler.queue))
        for task in list(self.internal_scheduler.queue):
            # the internal scheduler is used to asynchronously add qos rules to database
            # it returns a new qos rule if a new qos rule is added to database
            new_qos_rules = task["function"](
                session=session_write,
                request_uid=self.queue.get(task["kwargs"].get("request_uid")),
                rules_in_db=qos_rules,
                **task["kwargs"],
            )
            self.internal_scheduler.remove(task)
            # if a new qos rule is added, the new qos rule is added to the list of qos rules
            if new_qos_rules:
                qos_rules.update(new_qos_rules)

    def on_future_done(self, future: distributed.Future) -> None:
        job_status = DASK_STATUS_TO_STATUS.get(future.status, "accepted")
        logger_kwargs: dict[str, Any] = {}
        with self.session_maker_write() as session:
            if future.status == "finished":
                result = future.result()
                request = db.set_request_status(
                    future.key,
                    job_status,
                    cache_id=result,
                    session=session,
                )
            elif future.status == "error":
                exception = future.exception()
                error_message = "".join(traceback.format_exception(exception))
                error_reason = exception.__class__.__name__
                request = db.get_request(future.key, session=session)
                requeue = os.getenv("BROKER_REQUEUE_ON_KILLED_WORKER_REQUESTS", False)
                if error_reason == "KilledWorker":
                    worker_restart_events = self.client.get_events(
                        "worker-restart-memory"
                    )
                    # get info on worker and pid of the killed request
                    _, worker_pid_event = self.client.get_events(future.key)[0]
                    if worker_restart_events:
                        for event in worker_restart_events:
                            _, job = event
                            if (
                                job["worker"] == worker_pid_event["worker"]
                                and job["pid"] == worker_pid_event["pid"]
                            ):
                                db.add_event(
                                    event_type="killed_worker",
                                    request_uid=future.key,
                                    message="Worker has been killed by the Nanny due to memory usage. "
                                    f"{job['worker']=}, {job['pid']=}, {job['rss']=}",
                                    session=session,
                                )
                                request = db.set_request_status(
                                    future.key,
                                    "failed",
                                    error_message=error_message,
                                    error_reason=error_reason,
                                    session=session,
                                )
                                requeue = False
                    if requeue and request.request_metadata.get(
                        "resubmit", 0
                    ) < os.getenv("BROKER_REQUEUE_LIMIT", 3):
                        logger.info("worker killed: re-queueing", job_id=future.key)
                        db.requeue_request(request_uid=future.key, session=session)
                        self.queue.add(future.key, request)
                else:
                    request = db.set_request_status(
                        future.key,
                        job_status,
                        error_message=error_message,
                        error_reason=error_reason,
                        session=session,
                    )
            elif future.status != "cancelled":
                # if the dask status is unknown, re-queue it
                request = db.set_request_status(
                    future.key,
                    job_status,
                    session=session,
                    resubmit=True,
                )
                self.queue.add(future.key, request)
                logger.warning(
                    "unknown dask status, re-queing",
                    job_status={future.status},
                    job_id=request.request_uid,
                )
            else:
                # if the dask status is cancelled, the qos has already been reset by sync_database
                return
            self.futures.pop(future.key, None)
            self.qos.notify_end_of_request(
                request, session, scheduler=self.internal_scheduler
            )
            logger.info(
                "job has finished",
                dask_status=future.status,
                **db.logger_kwargs(request=request),
                **logger_kwargs,
            )

    def submit_requests(
        self,
        session_write: sa.orm.Session,
        number_of_requests: int,
        candidates: Iterable[db.SystemRequest],
    ) -> None:
        queue = sorted(
            candidates,
            key=lambda candidate: self.qos.priority(candidate, session_write),
            reverse=True,
        )
        requests_counter = 0
        for request in queue:
            if self.qos.can_run(
                request, session=session_write, scheduler=self.internal_scheduler
            ):
                if requests_counter <= int(number_of_requests * WORKERS_MULTIPLIER):
                    self.submit_request(request, session=session_write)
                requests_counter += 1

    def submit_request(
        self, request: db.SystemRequest, session: sa.orm.Session
    ) -> None:
        request = db.set_request_status(
            request_uid=request.request_uid, status="running", session=session
        )
        self.qos.notify_start_of_request(
            request, session, scheduler=self.internal_scheduler
        )
        self.queue.pop(request.request_uid)
        future = self.client.submit(
            worker.submit_workflow,
            key=request.request_uid,
            setup_code=request.request_body.get("setup_code", ""),
            entry_point="cads_adaptors:DummyAdaptor",
            config=dict(
                request_uid=request.request_uid,
                user_uid=request.user_uid,
                hostname=os.getenv("CDS_PROJECT_URL"),
                **request.adaptor_properties.config,
            ),
            form=request.adaptor_properties.form,
            request=request.request_body.get("request", {}),
            resources=request.request_metadata.get("resources", {}),
            metadata=request.request_metadata,
        )
        self.futures[request.request_uid] = future
        future.add_done_callback(self.on_future_done)
        logger.info(
            "submitted job to scheduler",
            **db.logger_kwargs(request=request),
        )

    def run(self) -> None:
        while True:
            start_loop = time.perf_counter()
            with self.session_maker_read() as session_read:
                if (rules_hash := get_rules_hash(self.qos.path)) != self.qos.rules_hash:
                    logger.info("reloading qos rules")
                    self.qos.reload_rules(session=session_read)
                    self.qos.rules_hash = rules_hash
                self.qos.environment.set_session(session_read)
                # expire_on_commit=False is used to detach the accepted requests without an error
                # this is not a problem because accepted requests cannot be modified in this loop
                with self.session_maker_write(expire_on_commit=False) as session_write:
                    self.queue.add_accepted_requests(
                        db.get_accepted_requests(
                            session=session_write,
                            last_created_at=self.queue.last_created_at,
                        )
                    )
                    self.sync_database(session=session_write)
                    self.sync_qos_rules(session_write)
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
                            internal_queue={queue_length},
                            db_queue={db_queue},
                        )
                        self.queue.reset()

                self.running_requests = len(
                    [
                        future
                        for future in self.futures.values()
                        if DASK_STATUS_TO_STATUS.get(future.status)
                        not in ("successful", "failed")
                    ]
                )
                queue_length = self.queue.len()
                available_workers = self.number_of_workers - self.running_requests
                if queue_length > 0:
                    logger.info(
                        "broker info",
                        available_workers=available_workers,
                        running_requests=self.running_requests,
                        number_of_workers=self.number_of_workers,
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

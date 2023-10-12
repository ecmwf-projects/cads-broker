import hashlib
import io
import os
import time
import traceback
from typing import Any

import attrs
import cachetools
import distributed
import sqlalchemy as sa
import structlog

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

WORKERS_MULTIPLIER = float(os.getenv("WORKERS_MULTIPLIER", 1.5))

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
        }
        tasks = {}
        for task_id, task in dask_scheduler.tasks.items():
            tasks[task_id] = scheduler_state_to_status.get(task.state, "accepted")
        return tasks

    return client.run_on_scheduler(get_tasks_on_scheduler)


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
    wait_time: float = float(os.getenv("BROKER_WAIT_TIME", 2))

    futures: dict[str, distributed.Future] = attrs.field(
        factory=dict,
        repr=lambda futures: " ".join(futures.keys()),
    )
    running_requests: int = 0
    session_maker: sa.orm.sessionmaker | None = None

    @classmethod
    def from_address(
        cls,
        address="scheduler:8786",
        session_maker: sa.orm.sessionmaker = None,
    ):
        client = distributed.Client(address)
        qos_config = QoSRules()
        factory.register_functions()
        session_maker = db.ensure_session_obj(session_maker)
        rules_hash = get_rules_hash(qos_config.rules_path)
        self = cls(
            client=client,
            session_maker=session_maker,
            environment=qos_config.environment,
            qos=QoS.QoS(
                qos_config.rules,
                qos_config.environment,
                rules_hash=rules_hash,
            ),
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

    def sync_database(self, session: sa.orm.Session) -> None:
        """Sync the database with the current status of the dask tasks.

        If the task is not in the dask scheduler, it is re-queued.
        """
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.status.in_(("running", "dismissed"))
        )
        for request in session.scalars(statement):
            # the retrieve API set the status to "dismissed", here the broker deletes the request
            # this is to better control the status of the QoS
            if request.status == "dismissed":
                db.delete_request(request=request, session=session)
                self.qos.notify_end_of_request(request, session)
                continue
            # if request is in futures, go on
            if request.request_uid in self.futures:
                continue
            # if request is in the scheduler, go on
            elif request.request_uid in get_tasks(self.client):
                continue
            # if it doesn't find the request: re-queue it
            else:
                # FIXME: check if request status has changed
                logger.info(f"--------> request not found {request.request_uid}")
                db.requeue_request(request_uid=request.request_uid, session=session)

    def on_future_done(self, future: distributed.Future) -> None:
        job_status = DASK_STATUS_TO_STATUS.get(future.status, "accepted")
        logger_kwargs: dict[str, Any] = {}
        log = list(self.client.get_events(f"{future.key}/log"))
        user_visible_log = list(
            self.client.get_events(f"{future.key}/user_visible_log")
        )
        with self.session_maker() as session:
            if future.status == "finished":
                result = future.result()
                request = db.set_request_status(
                    future.key,
                    job_status,
                    cache_id=result,
                    session=session,
                    log=log,
                    user_visible_log=user_visible_log,
                )
            elif future.status == "error":
                exception = future.exception()
                request = db.set_request_status(
                    future.key,
                    job_status,
                    error_message="".join(traceback.format_exception(exception)),
                    error_reason=traceback.format_exception_only(exception)[0],
                    log=log,
                    user_visible_log=user_visible_log,
                    session=session,
                )
            else:
                # if the dask status is unknown, re-queue it
                request = db.set_request_status(
                    future.key,
                    job_status,
                    session=session,
                    resubmit=True,
                    log=log,
                    user_visible_log=user_visible_log,
                )
                logger.warning(
                    "unknown dask status, re-queing",
                    job_status={future.status},
                    job_id=request.request_uid,
                )
                return
            self.futures.pop(future.key)
            self.qos.notify_end_of_request(request, session)
            logger.info(
                "job has finished",
                dask_status=future.status,
                **db.logger_kwargs(request=request),
                **logger_kwargs,
            )

    def submit_requests(self, session: sa.orm.Session, number_of_requests: int) -> None:
        candidates = db.get_accepted_requests(session=session)
        sort_start = time.time()
        queue = sorted(
            candidates,
            key=lambda candidate: self.qos.priority(candidate, session),
            reverse=True,
        )
        logger.info(f"------------- SORT {time.time() - sort_start}")
        requests_counter = 0
        can_run_start = time.time()
        for request in queue:
            if self.qos.can_run(request, session=session):
                self.submit_request(request, session=session)
                requests_counter += 1
                if requests_counter == int(number_of_requests * WORKERS_MULTIPLIER):
                    logger.info(f"------------- CAN RUN {time.time() - can_run_start}")
                    break

    def submit_request(
        self, request: db.SystemRequest, session: sa.orm.Session
    ) -> None:
        future = self.client.submit(
            worker.submit_workflow,
            key=request.request_uid,
            setup_code=request.request_body.get("setup_code", ""),
            entry_point=request.entry_point,
            config=request.adaptor_properties.config,
            form=request.adaptor_properties.form,
            request=request.request_body.get("request", {}),
            resources=request.request_metadata.get("resources", {}),
            metadata=request.request_metadata,
        )
        request = db.set_request_status(
            request_uid=request.request_uid, status="running", session=session
        )
        self.qos.notify_start_of_request(request, session)
        self.futures[request.request_uid] = future
        future.add_done_callback(self.on_future_done)
        logger.info(
            "submitted job to scheduler",
            **db.logger_kwargs(request=request),
        )

    def run(self) -> None:
        while True:
            start_time = time.time()
            with self.session_maker() as session:
                if (rules_hash := get_rules_hash(self.qos.path)) != self.qos.rules_hash:
                    logger.info("reloading qos rules")
                    self.qos.reload_rules(session=session)
                    self.qos.rules_hash = rules_hash
                sync_start = time.time()
                self.sync_database(session=session)
                logger.info(f"------> sync {time.time() - sync_start}")
                running_start = time.time()
                self.running_requests = len(
                    [
                        future
                        for future in self.futures.values()
                        if DASK_STATUS_TO_STATUS.get(future.status)
                        not in ("successful", "failed")
                    ]
                )
                logger.info(f"------> running {time.time() - running_start}")
                count_start = time.time()
                number_accepted_requests = db.count_requests(
                    session=session, status="accepted"
                )
                logger.info(f"------> count {time.time() - count_start}")
                available_workers = self.number_of_workers - self.running_requests
                if number_accepted_requests > 0:
                    if available_workers > 0:
                        logger.info("broker info", queued_jobs=number_accepted_requests)
                        logger.info(
                            "broker info",
                            available_workers=available_workers,
                            number_of_workers=self.number_of_workers,
                        )
                        self.submit_requests(
                            session=session, number_of_requests=available_workers
                        )
                    elif available_workers == 0:
                        logger.info(
                            "broker info",
                            available_workers=available_workers,
                            number_of_workers=self.number_of_workers,
                        )
            logger.info(f"------------ cycle time = {time.time() - start_time}")
            time.sleep(self.wait_time)

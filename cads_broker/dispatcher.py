import hashlib
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

from cads_broker import Environment, config, expressions, metrics
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


@cachetools.cached(  # type: ignore
    cache=cachetools.TTLCache(
        maxsize=1024, ttl=int(os.getenv("GET_NUMBER_OF_WORKERS_CACHE_TIME", 10))
    ),
    info=True,
)
def get_number_of_workers(client: distributed.Client) -> int:
    workers = client.scheduler_info()["workers"]
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
        self.qos_rules: str = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "qos.rules"
        )

    def register_functions(self):
        expressions.FunctionFactory.FunctionFactory.register_function(
            "dataset",
            lambda context, *args: context.request.process_id,
        )
        expressions.FunctionFactory.FunctionFactory.register_function(
            "adaptor",
            lambda context, *args: context.request.request_body.get("entry_point", ""),
        )
        expressions.FunctionFactory.FunctionFactory.register_function(
            "finished_requests",
            lambda context, *args: db.count_finished_requests_per_user(
                user_uid=context.request.user_uid,
                last_hours=args[0],
            ),
        )


@attrs.define
class Broker:
    client: distributed.Client
    environment: Environment.Environment
    qos: QoS.QoS
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
        environment = Environment.Environment()
        qos_config = QoSRules()
        qos_config.register_functions()
        session_maker = db.ensure_session_obj(session_maker)
        rules_hash = get_rules_hash(qos_config.qos_rules)
        self = cls(
            client=client,
            session_maker=session_maker,
            environment=environment,
            qos=QoS.QoS(
                qos_config.qos_rules,
                environment,
                rules_hash=rules_hash,
            ),
        )
        return self

    @property
    def number_of_workers(self):
        number_of_workers = get_number_of_workers(client=self.client)
        self.environment.number_of_workers = number_of_workers
        return number_of_workers

    def fetch_dask_task_status(self, request_uid: str) -> str | Any:
        # check if the task is in the future object
        if request_uid in self.futures:
            return DASK_STATUS_TO_STATUS[self.futures[request_uid].status]
        # check if the task is in the scheduler
        elif request_uid in (tasks := get_tasks(self.client)):
            return tasks.get(request_uid)
        # if request is not in the dask scheduler, re-queue it
        else:
            return "accepted"

    def update_database(self, session: sa.orm.Session) -> None:
        """Update the database with the current status of the dask tasks.

        If the task is not in the dask scheduler, it is re-queued.
        """
        statement = sa.select(db.SystemRequest).where(
            db.SystemRequest.status == "running"
        )
        for request in session.scalars(statement):
            status = self.fetch_dask_task_status(request.request_uid)
            if status in ("successful", "failed"):
                # status successful or failed must be set by on_future_done method
                request.status = "running"
            else:
                request.status = status
        session.commit()

    def on_future_done(self, future: distributed.Future) -> None:
        job_status = DASK_STATUS_TO_STATUS.get(future.status, "accepted")
        logger_kwargs = {}
        with self.session_maker() as session:
            if future.status == "finished":
                result = future.result()
                request = db.set_request_status(
                    future.key,
                    job_status,
                    cache_id=result,
                    session=session,
                )
                logger_kwargs["result"] = request.cache_entry.result
                metrics.increase_bytes_counter(result=request.cache_entry.result)
            elif future.status == "error":
                request = db.set_request_status(
                    future.key,
                    job_status,
                    traceback="".join(traceback.format_exception(future.exception())),
                    session=session,
                )
                logger_kwargs["traceback"] = request.response_traceback
            else:
                request = db.set_request_status(
                    future.key,
                    job_status,
                    session=session,
                )
                logger.warning(
                    "unknown dask status",
                    job_status={future.status},
                    job_id=request.request_uid,
                )
            self.futures.pop(future.key)
            self.qos.notify_end_of_request(request, session)
            logger.info(
                "job has finished",
                dask_status=future.status,
                **db.logger_kwargs(request=request),
                **logger_kwargs,
            )

    def submit_requests(self, session: sa.orm.Session, number_of_requests: int) -> None:
        queue = db.get_accepted_requests(session=session)
        for _ in range(number_of_requests):
            request = self.qos.pick(queue, session=session)
            if not request:
                return
            self.submit_request(request, session=session)

    def submit_request(
        self, request: db.SystemRequest, session: sa.orm.Session
    ) -> None:
        future = self.client.submit(
            worker.submit_workflow,
            key=request.request_uid,
            setup_code=request.request_body.get("setup_code", ""),
            entry_point=request.request_body.get("entry_point", ""),
            kwargs=request.request_body.get("kwargs", {}),
            resources=request.request_metadata.get("resources", {}),
            metadata=request.request_metadata,
        )
        future.add_done_callback(self.on_future_done)
        request = db.set_request_status(
            request_uid=request.request_uid, status="running", session=session
        )
        self.qos.notify_start_of_request(request, session)
        self.futures[request.request_uid] = future
        logger.info(
            "submitted job to scheduler",
            **db.logger_kwargs(request=request),
        )

    def run(self) -> None:
        while True:
            with self.session_maker() as session:
                if (rules_hash := get_rules_hash(self.qos.path)) != self.qos.rules_hash:
                    logger.info("reloading qos rules")
                    self.qos.reload_rules(session=session)
                    self.qos.rules_hash = rules_hash
                self.update_database(session=session)
                self.running_requests = len(
                    [
                        future
                        for future in self.futures.values()
                        if DASK_STATUS_TO_STATUS.get(future.status)
                        not in ("successful", "failed")
                    ]
                )
                number_accepted_requests = db.count_requests(
                    session=session, status="accepted"
                )
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
            time.sleep(self.wait_time)

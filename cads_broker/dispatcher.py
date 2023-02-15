import os
import time
import traceback
from typing import Any

import attrs
import distributed
import sqlalchemy as sa
import structlog

try:
    from cads_worker import worker
except ModuleNotFoundError:
    pass

from cads_broker import config
from cads_broker import database as db

config.configure_logger()
logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


DASK_STATUS_TO_STATUS = {
    "pending": "running",  # Pending status in dask is the same as running status in broker
    "processing": "running",
    "error": "failed",
    "finished": "successful",
}


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


@attrs.define
class Broker:

    client: distributed.Client
    max_running_requests: int
    wait_time: float = float(os.getenv("BROKER_WAIT_TIME", 2))

    futures: dict[str, distributed.Future] = attrs.field(factory=dict)
    running_requests: int = 0
    session_maker: sa.orm.sessionmaker | None = None

    def __attrs_post_init__(self):
        self.session_maker = db.ensure_session_obj(self.session_maker)

    @classmethod
    def from_address(
        cls,
        address="scheduler:8786",
        max_running_requests=int(os.getenv("MAX_RUNNING_REQUESTS", 1)),
        session_maker: sa.orm.sessionmaker = None,
    ):
        client = distributed.Client(address)
        return cls(
            client=client,
            max_running_requests=max_running_requests,
            session_maker=session_maker,
        )

    def choose_request(self, session: sa.orm.Session) -> db.SystemRequest | None:
        queue = db.get_accepted_requests_in_session(session=session)
        candidates = sorted(
            queue,
            key=lambda r: self.priority(r),
        )
        return candidates[0] if candidates else None

    def priority(self, request: db.SystemRequest) -> float:
        return request.created_at.timestamp()

    def fetch_dask_task_status(self, request_uid: str) -> str | Any:
        # check if the task is in the future object
        if request_uid in self.futures:
            return DASK_STATUS_TO_STATUS[self.futures[request_uid].status]
        # check if the task is in the scheduler
        elif request_uid in get_tasks(self.client):
            return get_tasks(self.client).get(request_uid, "unknown")
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
            request.status = self.fetch_dask_task_status(request.request_uid)
        session.commit()

    def on_future_done(self, future: distributed.Future) -> None:
        job_status = DASK_STATUS_TO_STATUS.get(future.status, "accepted")
        with self.session_maker() as session:
            if future.status in "finished":
                result = future.result()
                request = db.set_request_status_in_session(
                    future.key,
                    job_status,
                    cache_key=result["key"],
                    cache_expiration=result["expiration"],
                    session=session,
                )
            elif future.status in "error":
                request = db.set_request_status_in_session(
                    future.key,
                    job_status,
                    traceback="".join(traceback.format_exception(future.exception())),
                    session=session,
                )
            else:
                logger.warning(
                    f"Unknown future status {future.status}", job_id=future.key
                )
                request = db.set_request_status_in_session(
                    future.key,
                    job_status,
                    session=session,
                )
            self.futures.pop(future.key)
            logger.info(
                "job has finished",
                job_id=future.key,
                job_status=DASK_STATUS_TO_STATUS.get(future.status, "accepted"),
                dask_status=future.status,
                created_at=request.created_at,
                started_at=request.started_at,
                finished_at=request.finished_at,
                updated_at=request.updated_at,
            )

    def submit_request(self, session: sa.orm.Session) -> None:
        request = self.choose_request(session=session)
        if not request:
            return

        future = self.client.submit(
            worker.submit_workflow,
            key=request.request_uid,
            setup_code=request.request_body.get("setup_code", ""),
            entry_point=request.request_body.get("entry_point", ""),
            kwargs=request.request_body.get("kwargs", {}),
            metadata=request.request_metadata,
        )
        future.add_done_callback(self.on_future_done)
        request = db.set_request_status_in_session(
            request_uid=request.request_uid, status="running", session=session
        )
        self.futures[request.request_uid] = future
        logger.info(
            "submitted job to scheduler",
            job_id=future.key,
            created_at=request.created_at,
            started_at=request.started_at,
            updated_at=request.updated_at,
        )

    def run(self) -> None:
        while True:
            with self.session_maker() as session:
                self.update_database(session=session)
                self.running_requests = len(
                    [
                        future
                        for future in self.futures.values()
                        if DASK_STATUS_TO_STATUS.get(future.status)
                        not in ("successful", "failed")
                    ]
                )
                number_accepted_requests = db.count_accepted_requests_in_session(
                    session=session
                )
                available_workers = self.max_running_requests - self.running_requests
                if number_accepted_requests > 0:
                    if available_workers > 0:
                        logger.info(f"Queued jobs: {number_accepted_requests}")
                        logger.info(f"Available workers: {available_workers}")
                        [
                            self.submit_request(session=session)
                            for _ in range(available_workers)
                        ]
                    elif available_workers == 0:
                        logger.info(f"Available workers: {available_workers}")
            time.sleep(self.wait_time)

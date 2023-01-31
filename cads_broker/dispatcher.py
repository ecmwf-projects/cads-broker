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

from cads_broker import database as db

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
            tasks[task_id] = scheduler_state_to_status.get(task.state, "unknown")
        return tasks

    return client.run_on_scheduler(get_tasks_on_scheduler)


@attrs.define
class Broker:

    client: distributed.Client
    max_running_requests: int
    wait_time: float = float(os.getenv("BROKER_WAIT_TIME", 2))

    futures: dict[str, distributed.Future] = attrs.field(factory=dict)
    queue: list[db.SystemRequest] = attrs.field(factory=list)
    running_requests: int = 0
    session_maker: sa.orm.sessionmaker = db.ensure_session_obj(None)

    @classmethod
    def from_address(
        cls,
        address="scheduler:8786",
        max_running_requests=int(os.getenv("MAX_RUNNING_REQUESTS", 1)),
    ):
        client = distributed.Client(address)
        return cls(client=client, max_running_requests=max_running_requests)

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
        logger.info(f"Future {future.key} is {future.status}", job_id=future.key)
        with self.session_maker() as session:
            if future.status in "finished":
                result = future.result()
                db.set_request_status_in_session(
                    future.key,
                    DASK_STATUS_TO_STATUS[future.status],
                    cache_key=result["key"],
                    cache_expiration=result["expiration"],
                    session=session,
                )
            elif future.status in "error":
                db.set_request_status_in_session(
                    future.key,
                    DASK_STATUS_TO_STATUS[future.status],
                    traceback="".join(traceback.format_exception(future.exception())),
                    session=session,
                )
            else:
                logger.warning(f"Unknown future status {future.status}", job_id=future.key)
                db.set_request_status_in_session(
                    future.key, DASK_STATUS_TO_STATUS.get(future.status, "unknown"),
                    session=session,
                )
            self.futures.pop(future.key)

    def submit_request(self, session: sa.orm.Session) -> None:
        request = self.choose_request(session=session)
        if not request:
            return
        logger.info(
            f"Submitting {request.request_uid}",
            job_id=request.request_uid,
        )

        future = self.client.submit(
            worker.submit_workflow,
            key=request.request_uid,
            setup_code=request.request_body.get("setup_code", ""),
            entry_point=request.request_body.get("entry_point", ""),
            kwargs=request.request_body.get("kwargs", {}),
            metadata=request.request_metadata,
        )
        future.add_done_callback(self.on_future_done)
        db.set_request_status_in_session(
            request_uid=request.request_uid, status="running", session=session
        )
        self.futures[request.request_uid] = future
        logger.info(f"Submitted {request.request_uid}", job_id=future.key)

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
                queue = db.get_accepted_requests_in_session(session=session)
                available_workers = self.max_running_requests - self.running_requests
                if queue and available_workers > 0:
                    logger.info(f"Queue length: {len(queue)}")
                    logger.info(f"Available workers are: {available_workers}")
                    [
                        self.submit_request(session=session)
                        for _ in range(available_workers)
                    ]
            time.sleep(self.wait_time)

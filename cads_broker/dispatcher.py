import logging
import time
from typing import Any

import attrs
import distributed
import sqlalchemy as sa

from cads_broker import database as db

logging.getLogger().setLevel(logging.INFO)


DASK_STATUS_TO_STATUS = {
    "pending": "queued_in_dask",
    "processing": "running",
    "erred": "failed",
    "finished": "completed",
}


def get_tasks(client: distributed.Client) -> Any:
    def get_tasks_on_scheduler(dask_scheduler: distributed.Scheduler) -> dict[str, str]:
        scheduler_state_to_status = {
            "waiting": "queued",
            "processing": "running",
            "erred": "failed",
            "finished": "completed",
        }
        tasks = {}
        for task_id, task in dask_scheduler.tasks.items():
            tasks[task_id] = scheduler_state_to_status.get(task.state, "unknown")
        return tasks

    return client.run_on_scheduler(get_tasks_on_scheduler)


@attrs.define
class Broker:
    scheduler_address: str | distributed.LocalCluster = attrs.field(
        default="scheduler:8786"
    )
    queue: list[db.SystemRequest] = attrs.field(factory=list)
    max_running_requests: int = attrs.field(default=1)
    running_requests: int = 0
    futures: dict[str, distributed.Future] = attrs.field(factory=dict)
    client: distributed.Client = attrs.field(factory=distributed.Client)

    def __attrs_post_init__(self) -> None:
        if not self.client:
            self.client = distributed.Client(self.scheduler_address)

    def update_queue(
        self, session_obj: sa.orm.sessionmaker = db.SESSION_OBJ
    ) -> list[db.SystemRequest] | Any:
        with session_obj() as session:
            stmt = sa.select(db.SystemRequest).where(
                db.SystemRequest.status == "queued"
            )
            return session.scalars(stmt).all()

    def choose_request(self) -> db.SystemRequest | Any:
        queue = self.update_queue()
        candidates = sorted(
            queue,
            key=lambda r: self.priority(r),
            reverse=True,
        )
        return candidates[0]

    def priority(self, request: db.SystemRequest) -> float:
        if request.request_metadata is not None and isinstance(
            request.request_metadata, dict
        ):
            return request.request_metadata.get("created_at", 0.0)
        else:
            return 0.0

    def fetch_dask_task_status(self, request_uid: str) -> str | Any:
        # check if the task is in the future object
        if request_uid in self.futures:
            return DASK_STATUS_TO_STATUS[self.futures[request_uid].status]
        # check if the task is in the scheduler
        elif request_uid in get_tasks(self.client):
            return get_tasks(self.client).get(request_uid, "unknown")
        # if request is not in the dask scheduler, re-queue it
        else:
            return "queued"

    def update_database(
        self, session_obj: sa.orm.sessionmaker = db.SESSION_OBJ
    ) -> None:
        """Update the database with the current status of the dask tasks.
        If the task is not in the dask scheduler, it is re-queued.
        """
        with session_obj() as session:
            statement = sa.select(db.SystemRequest).where(
                db.SystemRequest.status == "running"
            )
            for request in session.scalars(statement):
                dask_task_status = self.fetch_dask_task_status(request.request_uid)
                if dask_task_status in ("completed", "failed", "queued"):
                    request.status = dask_task_status
                else:
                    request.status = "running"
            session.commit()

    def on_future_done(self, future: distributed.Future) -> None:
        logging.info(f"Future {future.key} is done")
        self.futures.pop(future.key)
        db.set_request_status(
            future.key, DASK_STATUS_TO_STATUS.get(future.status, "unknown")
        )

    def submit_request(self) -> None:
        request = self.choose_request()

        logging.info(
            f"Submitting {request.request_uid}",
        )

        import time

        request_body = request.request_body
        future = self.client.submit(
            time.sleep,
            request_body["seconds"] if isinstance(request_body, dict) else 0,
            key=request.request_uid,
        )
        future.add_done_callback(self.on_future_done)
        db.set_request_status(request.request_uid, "running")
        self.futures[request.request_uid] = future
        logging.info(f"Submitted {request.request_uid}")

    def run(self) -> None:
        while True:
            self.update_database()
            self.running_requests = len(
                [
                    future_status
                    for future_status in self.futures.values()
                    if DASK_STATUS_TO_STATUS.get(future_status)
                    not in ("completed", "failed")
                ]
            )
            logging.info(f"running: {self.running_requests}")
            logging.info(f"futures: {self.futures}")
            queue = self.update_queue()
            available_slots = self.max_running_requests - self.running_requests
            if queue and available_slots > 0:
                logging.info(f"queued: {queue}")
                logging.info(f"available_slots: {available_slots}")
                self.submit_request()
            time.sleep(2)

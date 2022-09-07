import logging
import time
import traceback
from typing import Any

import attrs
import distributed
import sqlalchemy as sa
from cads_worker import worker

from cads_broker import database as db

logging.getLogger().setLevel(logging.INFO)


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

    queue: list[db.SystemRequest] = attrs.field(factory=list)
    running_requests: int = 0
    futures: dict[str, distributed.Future] = attrs.field(factory=dict)

    @classmethod
    def from_address(cls, address="scheduler:8786", max_running_requests=1):
        client = distributed.Client(address)
        return cls(client=client, max_running_requests=max_running_requests)

    def choose_request(self) -> db.SystemRequest:
        queue = db.get_accepted_requests()
        candidates = sorted(
            queue,
            key=lambda r: self.priority(r),
        )
        return candidates[0]

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

    def update_database(self, session_obj: sa.orm.sessionmaker | None = None) -> None:
        """Update the database with the current status of the dask tasks.

        If the task is not in the dask scheduler, it is re-queued.
        """
        session_obj = db.ensure_session_obj(session_obj)
        with session_obj() as session:
            statement = sa.select(db.SystemRequest).where(
                db.SystemRequest.status == "running"
            )
            for request in session.scalars(statement):
                request.status = self.fetch_dask_task_status(request.request_uid)
            session.commit()

    def on_future_done(self, future: distributed.Future) -> None:
        logging.info(f"Future {future.key} is {future.status}")
        if future.status == "finished":
            db.set_request_status(future.key, "successful", result=future.result())
        elif future.status == "error":
            db.set_request_status(
                future.key,
                "failed",
                traceback="".join(traceback.format_exception(future.exception())),
            )
        else:
            logging.warning(f"Unknown future status {future.status}")
            db.set_request_status(
                future.key, DASK_STATUS_TO_STATUS.get(future.status, "unknown")
            )
        self.futures.pop(future.key)

    def submit_request(self) -> None:
        request = self.choose_request()

        logging.info(
            f"Submitting {request.request_uid}",
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
        db.set_request_status(request.request_uid, "running")
        self.futures[request.request_uid] = future
        logging.info(f"Submitted {request.request_uid}")

    def run(self) -> None:
        while True:
            self.update_database()
            self.running_requests = len(
                [
                    future
                    for future in self.futures.values()
                    if DASK_STATUS_TO_STATUS.get(future.status)
                    not in ("successful", "failed")
                ]
            )
            queue = db.get_accepted_requests()
            available_slots = self.max_running_requests - self.running_requests
            if queue and available_slots > 0:
                logging.info(f"queued: {queue}")
                logging.info(f"available_slots: {available_slots}")
                self.submit_request()
            time.sleep(2)

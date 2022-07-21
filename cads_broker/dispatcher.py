import logging
import time
import traceback
from typing import Any

import attrs
import distributed
import sqlalchemy as sa

from cads_broker import database as db

logging.getLogger().setLevel(logging.INFO)


DASK_STATUS_TO_STATUS = {
    "pending": "queued_in_dask",
    "processing": "running",
    "error": "failed",
    "finished": "successful",
}


def get_tasks(client: distributed.Client) -> Any:
    def get_tasks_on_scheduler(dask_scheduler: distributed.Scheduler) -> dict[str, str]:
        scheduler_state_to_status = {
            "waiting": "accepted",
            "processing": "running",
            "erred": "failed",
            "finished": "successful",
        }
        tasks = {}
        for task_id, task in dask_scheduler.tasks.items():
            tasks[task_id] = scheduler_state_to_status.get(task.state, "unknown")
        return tasks

    return client.run_on_scheduler(get_tasks_on_scheduler)


def submit_to_client(
    client: distributed.Client,
    key: str,
    context: str = "",
    callable_call: str = "",
    args=[],
    kwargs={},
) -> distributed.Future:
    def submit_workflow(
        context: str = "",
        callable_call: str = "",
        args=[],
        kwargs={},
    ) -> dict[str, Any] | list[dict[str, Any]]:
        import hashlib

        import xarray as xr

        # temporary implementation of dump
        def dump_results(
            results: Any, request_hash: str
        ) -> dict[str, Any] | list[dict[str, Any]]:
            if isinstance(results, (list, tuple)):
                return [dump_results(r, request_hash) for r in results]
            elif isinstance(results, dict):
                return {k: dump_results(v, request_hash) for k, v in results.items()}
            elif isinstance(results, (xr.Dataset, xr.DataArray)):
                path = f"/tmp/{request_hash}.nc"
                results.to_netcdf(path=path)
                return {"path": path, "content_type": "application/x-netcdf"}
            return results

        exec(context)
        if callable_call == "":
            callable_call = "execute(*{args}, **{kwargs})"
        results = eval(callable_call.format(args=args, kwargs=kwargs))
        request_hash = hashlib.md5(
            (context + callable_call + str(time.time())).encode()
        ).hexdigest()
        return dump_results(results, request_hash=request_hash)

    return client.submit(submit_workflow, context, callable_call, args, kwargs, key=key)


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
                dask_task_status = self.fetch_dask_task_status(request.request_uid)
                if dask_task_status in ("successful", "failed", "accepted"):
                    request.status = dask_task_status
                # if the dask status is pending, the request is running for the broker
                elif dask_task_status == "queued_in_dask":
                    request.status = "running"
                    if request.started_at is None:
                        request.started_at = sa.func.now()
                else:
                    raise ValueError(f"Unknown dask status: {dask_task_status}")
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

    def submit_request(self, session_obj: sa.orm.sessionmaker | None = None) -> None:
        request = self.choose_request()

        logging.info(
            f"Submitting {request.request_uid}",
        )

        future = submit_to_client(
            self.client,
            key=request.request_uid,
            context=request.request_body.get("context", ""),
            callable_call=request.request_body.get("callable_call", ""),
            args=request.request_body.get("args", ""),
            kwargs=request.request_body.get("kwargs", ""),
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
            logging.info(f"running: {self.running_requests}")
            logging.info(f"futures: {self.futures}")
            queue = db.get_accepted_requests()
            available_slots = self.max_running_requests - self.running_requests
            if queue and available_slots > 0:
                logging.info(f"queued: {queue}")
                logging.info(f"available_slots: {available_slots}")
                self.submit_request()
            time.sleep(2)

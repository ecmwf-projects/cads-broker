# type: ignore
# Copyright 2022, European Union.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
import json
from typing import Any

import attrs
import fastapi
from ogc_api_processes_fastapi import clients, exceptions, main, models
from starlette_exporter import PrometheusMiddleware, handle_metrics

from cads_broker import database
from cads_broker.metrics import add_metrics_middleware


@attrs.define
class NotValidJobId(Exception):
    detail: str | None = None


def not_valid_job_id_exception_handler(
    request: fastapi.Request, exc: Exception
) -> fastapi.responses.JSONResponse:
    return fastapi.responses.JSONResponse(
        status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=models.Exception(
            type="not-valid-job-id",
            title="not valid job id",
            detail=exc.detail,
            instance=str(request.url),
        ).dict(exclude_unset=True),
    )


@attrs.define
class ComputeClient(clients.BaseClient):
    def get_processes(self, limit: int, offset: int) -> list[models.ProcessSummary]:
        available_processes = [
            models.ProcessSummary(
                title="Submit a workflow",
                id="submit-workflow",
                version="1.0.0",
                jobControlOptions=[
                    "async-execute",
                ],
                outputTransmission=[
                    "reference",
                ],
            )
        ]
        return available_processes

    def get_process(self, process_id: str) -> models.ProcessDescription:
        if process_id == "submit-workflow":
            process_description = models.ProcessDescription(
                id=process_id,
                version="1.0.0",
                inputs={
                    "setup_code": models.InputDescription(
                        schema_=models.SchemaItem(type="string")
                    ),
                    "entry_point": models.InputDescription(
                        schema_=models.SchemaItem(type="string")
                    ),
                    "kwargs": models.InputDescription(
                        schema_=models.SchemaItem(type="object")
                    ),
                    "metadata": models.InputDescription(
                        schema_=models.SchemaItem(type="object")
                    ),
                },
                outputs=[],
            )
        else:
            raise exceptions.NoSuchProcess(f"{process_id} is not supported")
        return process_description

    def post_process_execute(
        self,
        process_id: str,
        request: fastapi.Request,
        execution_content: models.Execute,
    ) -> models.StatusInfo:
        if process_id != "submit-workflow":
            raise exceptions.NoSuchProcess(f"{process_id} is not supported")
        job_id = request.headers["X-Forward-Job-ID"]
        orig_process_id = request.headers["X-Forward-Process-ID"]
        inputs = execution_content.dict()["inputs"]
        # workaround for acceping key-value objects as input
        inputs["kwargs"] = inputs["kwargs"]["value"]
        try:
            job = database.create_request(
                process_id=process_id,
                request_uid=job_id,
                metadata={"process_id": orig_process_id},
                **inputs,
            )
        except database.sa.exc.IntegrityError:
            raise NotValidJobId(detail=f"Job ID {job_id} already exists.")
        except database.sa.exc.StatementError:
            raise NotValidJobId(detail=f"Job ID {job_id} is not valid.")

        status_info = models.StatusInfo(
            processID=job["process_id"],
            type=models.JobType("process"),
            jobID=job["request_uid"],
            status=models.StatusCode(job["status"]),
            created=job["created_at"],
            started=job["started_at"],
            finished=job["finished_at"],
            updated=job["updated_at"],
            metadata={"apiProcessID": orig_process_id},
        )
        return status_info

    def get_jobs(self) -> list[models.StatusInfo]:
        session_obj = database.ensure_session_obj(None)
        with session_obj() as session:
            statement = database.sa.select(database.SystemRequest)
            jobs = session.scalars(statement).all()

        return [
            models.StatusInfo(
                type=models.JobType("process"),
                jobID=job.request_uid,
                processID=job.process_id,
                status=models.StatusCode(job.status),
                created=job.created_at,
                started=job.started_at,
                finished=job.finished_at,
                updated=job.updated_at,
                metadata={"apiProcessID": job.request_metadata.get("process_id")},
            )
            for job in jobs
        ]

    def get_job(self, job_id: str) -> models.StatusInfo:
        try:
            job = database.get_request(request_uid=job_id)
        except (database.sa.exc.StatementError, database.sa.exc.NoResultFound):
            raise exceptions.NoSuchJob(f"Can't find the job {job_id}.")

        status_info = models.StatusInfo(
            processID=job.process_id,
            type=models.JobType("process"),
            jobID=job.request_uid,
            status=models.StatusCode(job.status),
            created=job.created_at,
            started=job.started_at,
            finished=job.finished_at,
            updated=job.updated_at,
            metadata={"apiProcessID": job.request_metadata.get("process_id")},
        )
        return status_info

    def get_job_results(self, job_id: str) -> dict[str, Any]:
        try:
            job = database.get_request(request_uid=job_id)
        except (database.sa.exc.StatementError, database.sa.exc.NoResultFound):
            raise exceptions.NoSuchJob(f"Can't find the job {job_id}.")
        if job.status == "successful":
            return {"asset": {"value": json.loads(job.response_body.get("result"))}}
        elif job.status == "failed":
            raise exceptions.JobResultsFailed(
                type="RuntimeError", detail=job.response_body.get("traceback")
            )
        elif job.status in ("accepted", "running"):
            raise exceptions.ResultsNotReady(f"Status of {job_id} is {job.status}.")
        else:
            raise exceptions.NoSuchJob(f"Can't find the job {job_id}.")


app = fastapi.FastAPI()
app.add_middleware(PrometheusMiddleware)
add_metrics_middleware(app)
app = main.include_routers(app=app, client=ComputeClient())
app = main.include_exception_handlers(app=app)
app.add_exception_handler(NotValidJobId, not_valid_job_id_exception_handler)
app.add_route("/metrics", handle_metrics)

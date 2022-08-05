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
import attrs
import fastapi
from ogc_api_processes_fastapi import clients, exceptions, main, models

from cads_broker import database


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
        self, process_id: str, execution_content: models.Execute
    ) -> models.StatusInfo:
        inputs = execution_content.dict()["inputs"]
        # workaround for acceping key-value objects as input
        inputs["kwargs"] = inputs["kwargs"]["value"]
        inputs["metadata"] = inputs["metadata"]["value"]

        request = database.create_request(
            process_id=process_id,
            request_uid=inputs["metadata"]["jobID"],
            **inputs,
        )

        status_info = models.StatusInfo(
            processID=request["process_id"],
            type=models.JobType("process"),
            jobID=request["request_uid"],
            status=models.StatusCode(request["status"]),
            created=request["created_at"],
            started=request["started_at"],
            finished=request["finished_at"],
            updated=request["updated_at"],
        )
        return status_info

    def get_jobs(self) -> list[models.StatusInfo]:
        session_obj = database.ensure_session_obj(None)
        with session_obj() as session:
            statement = database.sa.select(database.SystemRequest)
            requests = session.scalars(statement).all()

        return [
            models.StatusInfo(
                type=models.JobType("process"),
                jobID=request.request_uid,
                processID=request.process_id,
                status=models.StatusCode(request.status),
                created=request.created_at,
                started=request.started_at,
                finished=request.finished_at,
                updated=request.updated_at,
            )
            for request in requests
        ]

    def get_job(self, job_id: str) -> models.StatusInfo:
        request = database.get_request(request_uid=job_id)
        status_info = models.StatusInfo(
            processID=request.process_id,
            type=models.JobType("process"),
            jobID=request.request_uid,
            status=models.StatusCode(request.status),
            created=request.created_at,
            started=request.started_at,
            finished=request.finished_at,
            updated=request.updated_at,
        )
        return status_info

    def get_job_results(self, job_id: str) -> models.Link:
        request = database.get_request(request_uid=job_id)
        return models.Link(
            href=request.response_body.get("result"),
        )


app = fastapi.FastAPI()
app = main.include_routers(app=app, client=ComputeClient())

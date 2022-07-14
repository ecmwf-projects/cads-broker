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


from typing import Any

import attrs
import fastapi
from ogc_api_processes_fastapi import clients, main, models


@attrs.define
class ComputeClient(clients.BaseClient):
    def get_processes_list(
        self, limit: int, offset: int
    ) -> list[models.ProcessSummary]:
        available_processes = [
            models.ProcessSummary(
                title="Retrieve from interal MARS archive",
                id="retrieve-internal-mars",
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

    def get_process_description(self, process_id: str) -> models.ProcessDescription:
        process_description = models.ProcessDescription(
            inputs=[],
            outputs=[],
        )
        return process_description

    def post_process_execute(
        self, process_id: str, execution_content: models.Execute
    ) -> Any:
        return None

    def get_job_status(self, job_id: str) -> models.StatusInfo:
        return None


app = fastapi.FastAPI()
app = main.include_routers(app=app, client=ComputeClient())

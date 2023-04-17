"""configuration utilities."""
# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import cads_common.logging
import pydantic

from cads_broker import expressions

dbsettings = None


class QoSRules(pydantic.BaseSettings):
    qos_rules: str = os.path.join(
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


class SqlalchemySettings(pydantic.BaseSettings):
    """Postgres-specific API settings.

    - ``compute_db_user``: postgres username.
    - ``compute_db_password``: postgres password.
    - ``compute_db_host``: hostname for the connection.
    - ``compute_db_name``: database name.
    """

    compute_db_user: str = "broker"
    compute_db_password: str | None = None
    compute_db_host: str = "compute-db"
    compute_db_name: str = "broker"
    pool_timeout: float = 1.0
    pool_recycle: int = 60

    @pydantic.validator("compute_db_password")
    def password_must_be_set(cls: pydantic.BaseSettings, v: str | None) -> str | None:
        """Check that password is explicitly set."""
        if v is None:
            raise ValueError("compute_db_password must be set")
        return v

    @property
    def connection_string(self) -> str:
        """Create reader psql connection string."""
        return (
            f"postgresql://{self.compute_db_user}"
            f":{self.compute_db_password}@{self.compute_db_host}"
            f"/{self.compute_db_name}"
        )


def ensure_settings(settings: SqlalchemySettings | None = None) -> SqlalchemySettings:
    """If `settings` is None, create a new SqlalchemySettings object.

    Parameters
    ----------
    settings: an optional config.SqlalchemySettings to be set

    Returns
    -------
    sqlalchemysettings:
        a SqlalchemySettings object
    """
    global dbsettings
    if settings and isinstance(settings, SqlalchemySettings):
        dbsettings = settings
    else:
        dbsettings = SqlalchemySettings()
    return dbsettings


def configure_logger() -> None:
    """
    Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    cads_common.logging.structlog_configure()
    cads_common.logging.logging_configure()

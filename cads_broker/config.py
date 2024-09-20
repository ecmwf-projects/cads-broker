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

import logging
import sys

import pydantic
import pydantic_core
import pydantic_settings
import sqlalchemy as sa
import structlog

dbsettings = None


class BrokerConfig(pydantic_settings.BaseSettings):

    high_priority_user_uid: str = "8d8ee054-6a09-4da8-a5be-d5dff52bbc5f"
    broker_priority_algorithm: str = "legacy"
    broker_priority_interval_hours: int = 24
    get_number_of_workers_cache_time: int = 10
    qos_rules_cache_time: int = 10
    get_tasks_from_scheduler_cache_time: int = 1
    rules_path: str = "/src/rules.qos"
    wait_time: float = 2.
    sync_database_cache_time: int = 10
    broker_requeue_on_killed_worker_requests: bool = False
    broker_requeue_on_lost_requests: bool = True
    broker_requeue_limit: int = 3
    broker_max_internal_scheduler_tasks: int = 500


class SqlalchemySettings(pydantic_settings.BaseSettings):
    """Postgres-specific API settings.

    - ``compute_db_user``: postgres username.
    - ``compute_db_password``: postgres password.
    - ``compute_db_host``: hostname for the connection.
    - ``compute_db_name``: database name.
    """

    compute_db_user: str | None = None
    compute_db_password: str | None = None
    compute_db_host: str | None = None
    compute_db_host_read: str | None = None
    compute_db_name: str | None = None
    pool_timeout: float = 1.0
    pool_recycle: int = 60
    pool_size: int = 5
    max_overflow: int = 15

    @pydantic.field_validator(
        "compute_db_user",
        "compute_db_password",
        "compute_db_host",
        "compute_db_host_read",
        "compute_db_name",
    )
    def db_connection_env_vars_must_be_set(
        cls: pydantic_settings.BaseSettings,
        v: str | None,
        info: pydantic_core.core_schema.ValidationInfo,
    ) -> str | None:
        """Check that database connection environment variables are explicitly set."""
        if v is None:
            raise ValueError(f"{info.field_name} must be set")
        return v

    @property
    def connection_string(self) -> str:
        """Create reader psql connection string."""
        url = sa.engine.URL.create(
            drivername="postgresql",
            username=self.compute_db_user,
            password=self.compute_db_password,
            host=self.compute_db_host,
            database=self.compute_db_name,
        )
        ret_value = url.render_as_string(False)
        return ret_value

    @property
    def connection_string_read(self) -> str:
        """Create reader psql connection string."""
        url = sa.engine.URL.create(
            drivername="postgresql",
            username=self.compute_db_user,
            password=self.compute_db_password,
            host=self.compute_db_host_read,
            database=self.compute_db_name,
        )
        ret_value = url.render_as_string(False)
        return ret_value


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

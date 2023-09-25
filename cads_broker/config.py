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
import structlog

dbsettings = None


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
    compute_db_name: str | None = None

    read_db_user: str | None = None
    read_db_password: str | None = None
    write_db_user: str | None = None
    write_db_password: str | None = None
    db_host: str | None = None

    pool_timeout: float = 1.0
    pool_recycle: int = 60

    @pydantic.field_validator("*", mode="before")
    def assign_defaults_for_ro_fields(
        cls: pydantic_settings.BaseSettings,
        v: str | None,
        info: pydantic_core.core_schema.FieldValidationInfo,
    ) -> str | None:
        """Set defaults for read-only db connection fields."""
        default_fields_map = {
            "read_db_user": "compute_db_user",
            "write_db_user": "compute_db_user",
            "read_db_password": "compute_db_password",
            "write_db_password": "compute_db_password",
            "db_host": "compute_db_host",
        }
        if info.field_name in default_fields_map and v is None:
            return info.data.get(default_fields_map[info.field_name])
        return v

    @pydantic.field_validator("read_db_password", "write_db_password")
    def password_must_be_set(
        cls: pydantic_settings.BaseSettings,
        v: str | None,
        info: pydantic_core.core_schema.FieldValidationInfo,
    ) -> str | None:
        """Check that password is explicitly set."""
        if v is None:
            raise ValueError(f"{info.field_name} must be set")
        return v

    @property
    def connection_string(self) -> str:
        """Create reader psql connection string."""
        url = f"postgresql://{self.write_db_user}:{self.write_db_password}@{self.db_host}/{self.compute_db_name}"
        return url

    @property
    def connection_string_ro(self) -> str:
        """Create reader psql connection string in read-only mode."""
        url = f"postgresql://{self.read_db_user}:{self.read_db_password}@{self.db_host}/{self.compute_db_name}"
        return url


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

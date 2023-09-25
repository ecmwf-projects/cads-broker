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

    compute_db_user: str = "broker"
    compute_db_password: str | None = None
    compute_db_host: str = "compute-db"
    compute_db_name: str = "broker"
    # do not prepend ro_* fields: ordering is important
    ro_compute_db_user: str | None = None
    ro_compute_db_password: str | None = None
    ro_compute_db_host: str | None = None
    ro_compute_db_name: str | None = None

    pool_timeout: float = 1.0
    pool_recycle: int = 60

    @pydantic.field_validator("*", mode="before")
    def assign_defaults_for_ro_fields(
        cls: pydantic_settings.BaseSettings,
        v: str | None,
        info: pydantic_core.core_schema.FieldValidationInfo,
    ) -> str | None:
        """Set defaults for read-only db connection fields."""
        default_fields_map = [
            ("ro_compute_db_user", "compute_db_user"),
            ("ro_compute_db_password", "compute_db_password"),
            ("ro_compute_db_host", "compute_db_host"),
            ("ro_compute_db_name", "compute_db_name"),
        ]
        for ro_field, rw_field in default_fields_map:
            if info.field_name == ro_field and not v:
                return info.data.get(rw_field)
        return v

    @pydantic.field_validator("compute_db_password")
    def password_must_be_set(
        cls: pydantic_settings.BaseSettings, v: str | None
    ) -> str | None:
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

    @property
    def connection_string_ro(self) -> str:
        """Create reader psql connection string in read-only mode."""
        return (
            f"postgresql://{self.ro_compute_db_user}"
            f":{self.ro_compute_db_password}@{self.ro_compute_db_host}"
            f"/{self.ro_compute_db_name}"
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

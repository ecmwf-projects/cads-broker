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

import dataclasses
import logging
import os
import sys

import structlog

dbsettings = None


@dataclasses.dataclass
class SqlalchemySettings:
    """Postgres-specific API settings.

    - ``compute_db_user``: postgres username.
    - ``compute_db_password``: postgres password.
    - ``compute_db_host``: hostname for the connection.
    - ``compute_db_name``: database name.
    """

    compute_db_password: str = dataclasses.field(repr=False)
    compute_db_user: str = "broker"
    compute_db_host: str = "compute-db"
    compute_db_name: str = "broker"
    pool_timeout: float = 1.0
    pool_recycle: int = 60

    def __init__(self, **kwargs):
        self.match_args = kwargs
        for field in dataclasses.fields(self):
            if field.name in kwargs:
                setattr(self, field.name, kwargs[field.name])
            else:
                setattr(self, field.name, field.default)
        self.__post_init__()

    def __post_init__(self):
        # overwrite instance getting attributes from the environment
        environ = os.environ.copy()
        environ_lower = {k.lower(): v for k, v in environ.items()}
        for field in dataclasses.fields(self):
            if field.name in self.match_args:
                # do not overwrite if passed to __init__
                continue
            if field.name in environ:
                setattr(self, field.name, environ[field.name])
            elif field.name in environ_lower:
                setattr(self, field.name, environ_lower[field.name])

        # automatic casting
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value != dataclasses.MISSING and not isinstance(value, field.type):
                try:
                    setattr(self, field.name, field.type(value))
                except:  # noqa
                    raise ValueError(
                        f"{field.name} '{value}' has not type {repr(field.type)}"
                    )

        # validations
        # defined fields without a default must have a value
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.default == dataclasses.MISSING and value == dataclasses.MISSING:
                raise ValueError(f"{field.name} must be set")
        # compute_db_password must be set
        if self.compute_db_password is None:
            raise ValueError("compute_db_password must be set")

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

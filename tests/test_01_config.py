from typing import Any

import pytest
import sqlalchemy as sa

from cads_broker import config


def test_sqlalchemysettings(temp_environ: Any) -> None:
    # check settings must have a password set (no default)
    temp_environ.pop("postgres_password", default=None)
    with pytest.raises(ValueError) as excinfo:
        config.SqlalchemySettings()
    assert "postgres_password" in str(excinfo.value)

    # also an empty password can be set
    settings = config.SqlalchemySettings(postgres_password="")
    assert settings.postgres_password == ""

    # also a not empty password can be set
    temp_environ["postgres_password"] = "a password"
    settings = config.SqlalchemySettings()
    assert settings.postgres_password == "a password"


def test_ensure_settings(session_obj: sa.orm.sessionmaker, temp_environ: Any) -> None:
    temp_environ["postgres_password"] = "apassword"

    # initially global settings is importable, but it is None
    assert config.dbsettings is None

    # at first run returns right connection and set global setting
    effective_settings = config.ensure_settings()
    assert (
        effective_settings.connection_string
        == "postgresql://broker:apassword@compute-db/broker"
    )
    assert config.dbsettings == effective_settings

    # setting a custom configuration works as well
    my_settings_dict = {
        "postgres_user": "monica",
        "postgres_password": "secret1",
        "postgres_host": "myhost",
        "postgres_dbname": "mybroker",
    }
    my_settings_connection_string = (
        "postgresql://%(postgres_user)s:%(postgres_password)s"
        "@%(postgres_host)s/%(postgres_dbname)s" % my_settings_dict
    )
    mysettings = config.SqlalchemySettings(**my_settings_dict)
    effective_settings = config.ensure_settings(mysettings)

    assert config.dbsettings == effective_settings
    assert effective_settings == mysettings
    assert effective_settings.connection_string == my_settings_connection_string

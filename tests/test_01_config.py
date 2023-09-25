from typing import Any

import pytest
import sqlalchemy as sa

from cads_broker import config


def test_sqlalchemysettings(temp_environ: Any) -> None:
    # check settings must have a password set (no default)
    temp_environ.pop("compute_db_password", default=None)
    with pytest.raises(ValueError) as excinfo:
        config.SqlalchemySettings()
    assert "read_db_user" in str(excinfo.value)

    # also an empty password can be set
    temp_environ["compute_db_user"] = "user1"
    temp_environ["compute_db_host"] = "host1"
    temp_environ["compute_db_name"] = "dbname1"
    settings = config.SqlalchemySettings(compute_db_password="")
    assert settings.compute_db_password == ""

    # check backward compatibility defaults
    assert dict(settings) == {
        "compute_db_user": "user1",
        "compute_db_password": "",
        "compute_db_host": "host1",
        "compute_db_name": "dbname1",
        "read_db_user": "user1",
        "read_db_password": "",
        "write_db_user": "user1",
        "write_db_password": "",
        "db_host": "host1",
        "pool_timeout": 1.0,
        "pool_recycle": 60,
    }

    # use not backward compatibility variables
    temp_environ["read_db_user"] = "ro_user"
    temp_environ["read_db_password"] = "ro_password"
    temp_environ["write_db_user"] = "rw_user"
    temp_environ["write_db_password"] = "rw_password"
    temp_environ["db_host"] = "new_db_host"
    settings = config.SqlalchemySettings()
    assert dict(settings) == {
        "compute_db_user": "user1",
        "compute_db_password": None,
        "compute_db_host": "host1",
        "compute_db_name": "dbname1",
        "read_db_user": "ro_user",
        "read_db_password": "ro_password",
        "write_db_user": "rw_user",
        "write_db_password": "rw_password",
        "db_host": "new_db_host",
        "pool_timeout": 1.0,
        "pool_recycle": 60,
    }
    assert (
        settings.connection_string
        == "postgresql://rw_user:rw_password@new_db_host/dbname1"
    )
    assert (
        settings.connection_string_ro
        == "postgresql://ro_user:ro_password@new_db_host/dbname1"
    )


def test_ensure_settings(session_obj: sa.orm.sessionmaker, temp_environ: Any) -> None:
    temp_environ["compute_db_user"] = "auser"
    temp_environ["compute_db_password"] = "apassword"
    temp_environ["compute_db_host"] = "ahost"
    temp_environ["compute_db_name"] = "aname"
    # initially global settings is importable, but it is None
    assert config.dbsettings is None

    # at first run returns right connection and set global setting
    effective_settings = config.ensure_settings()
    assert (
        effective_settings.connection_string
        == "postgresql://auser:apassword@ahost/aname"
    )
    assert (
        effective_settings.connection_string_ro
        == "postgresql://auser:apassword@ahost/aname"
    )
    assert config.dbsettings == effective_settings
    config.dbsettings = None

    # setting a custom configuration works as well
    my_settings_dict = {
        "compute_db_user": "monica",
        "compute_db_password": "secret1",
        "compute_db_host": "myhost",
        "compute_db_name": "mybroker",
        "read_db_user": "ro_user",
        "read_db_password": "ro_password",
        "write_db_user": "rw_user",
        "write_db_password": "rw_password",
        "db_host": "new_db_host",
    }
    my_settings_connection_string = (
        "postgresql://%(write_db_user)s:%(write_db_password)s"
        "@%(db_host)s/%(compute_db_name)s" % my_settings_dict
    )
    my_settings_connection_string_ro = (
        "postgresql://%(read_db_user)s:%(read_db_password)s"
        "@%(db_host)s/%(compute_db_name)s" % my_settings_dict
    )
    mysettings = config.SqlalchemySettings(**my_settings_dict)
    effective_settings = config.ensure_settings(mysettings)

    assert config.dbsettings == effective_settings
    assert effective_settings == mysettings
    assert effective_settings.connection_string == my_settings_connection_string
    assert effective_settings.connection_string_ro == my_settings_connection_string_ro
    config.dbsettings = None

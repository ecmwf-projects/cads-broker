from typing import Any

import pytest
import sqlalchemy as sa

from cads_broker import config


def test_sqlalchemysettings(temp_environ: Any) -> None:
    # check settings must have a password set (no default)
    temp_environ.pop("compute_db_password", default=None)
    with pytest.raises(ValueError):
        config.SqlalchemySettings()
    config.dbsettings = None

    # also an empty password can be set
    settings = config.SqlalchemySettings(
        compute_db_password="",
        compute_db_host="broker",
        compute_db_host_read="broker",
        compute_db_name="broker",
        compute_db_user="broker",
    )
    assert settings.compute_db_password == ""
    config.dbsettings = None

    # also a not empty password can be set
    temp_environ.update(
        dict(
            compute_db_password="a password",
            compute_db_host="broker",
            compute_db_host_read="broker",
            compute_db_name="broker",
            compute_db_user="broker",
        )
    )
    settings = config.SqlalchemySettings()
    assert settings.compute_db_password == "a password"
    config.dbsettings = None


def test_ensure_settings(session_obj: sa.orm.sessionmaker, temp_environ: Any) -> None:
    temp_environ.update(
        dict(
            compute_db_password="apassword",
            compute_db_host="compute-db",
            compute_db_host_read="compute-db",
            compute_db_name="broker",
            compute_db_user="broker",
        )
    )

    # initially global settings is importable, but it is None
    assert config.dbsettings is None

    # at first run returns right connection and set global setting
    effective_settings = config.ensure_settings()
    assert (
        effective_settings.connection_string
        == "postgresql://{compute_db_user}:{compute_db_password}@{compute_db_host}/{compute_db_name}".format(
            **temp_environ
        )
    )
    assert config.dbsettings == effective_settings
    config.dbsettings = None

    # setting a custom configuration works as well
    my_settings_dict = {
        "compute_db_user": "monica",
        "compute_db_password": "secret1",
        "compute_db_host": "myhost",
        "compute_db_name": "mybroker",
    }
    my_settings_connection_string = (
        "postgresql://%(compute_db_user)s:%(compute_db_password)s"
        "@%(compute_db_host)s/%(compute_db_name)s" % my_settings_dict
    )
    mysettings = config.SqlalchemySettings(**my_settings_dict)
    effective_settings = config.ensure_settings(mysettings)

    assert config.dbsettings == effective_settings
    assert effective_settings == mysettings
    assert effective_settings.connection_string == my_settings_connection_string
    config.dbsettings = None

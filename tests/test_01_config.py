from typing import Any

import pytest

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

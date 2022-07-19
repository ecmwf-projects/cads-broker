import pydantic


class SqlalchemySettings(pydantic.BaseSettings):
    """Postgres-specific API settings.

    - ``postgres_user``: postgres username.
    - ``postgres_password``: postgres password.
    - ``postgres_host``: hostname for the connection.
    - ``postgres_dbname``: database name.
    """

    postgres_user: str = "broker"
    postgres_password: str | None = None
    postgres_host: str = "compute-db"
    postgres_dbname: str = "broker"

    @pydantic.validator("postgres_password")
    def password_must_be_set(cls: pydantic.BaseSettings, v: str | None) -> str | None:
        """Check that password is explicitly set."""
        if v is None:
            raise ValueError("postgres_password must be set")
        return v

    @property
    def connection_string(self) -> str:
        """Create reader psql connection string."""
        return (
            f"postgresql://{self.postgres_user}"
            f":{self.postgres_password}@{self.postgres_host}"
            f"/{self.postgres_dbname}"
        )

"""SQLAlchemy ORM model."""
import uuid
from typing import Any

import cacholote
import sqlalchemy as sa
import sqlalchemy_utils
from sqlalchemy.dialects.postgresql import JSONB

from cads_broker import config

BaseModel = cacholote.config.Base


status_enum = sa.Enum("accepted", "running", "failed", "successful", name="status")


class SystemRequest(BaseModel):
    """Resource ORM model."""

    __tablename__ = "system_requests"

    request_id = sa.Column(sa.Integer, primary_key=True)
    request_uid = sa.Column(
        sa.dialects.postgresql.UUID(),
        index=True,
        unique=True,
    )
    process_id = sa.Column(sa.VARCHAR(1024))
    status = sa.Column(status_enum)
    cache_key = sa.Column(sa.String(56))
    cache_expiration = sa.Column(sa.DateTime)
    request_body = sa.Column(JSONB, nullable=False)
    request_metadata = sa.Column(JSONB)
    response_traceback = sa.Column(JSONB)
    response_metadata = sa.Column(JSONB)
    created_at = sa.Column(sa.TIMESTAMP, default=sa.func.now())
    started_at = sa.Column(sa.TIMESTAMP)
    finished_at = sa.Column(sa.TIMESTAMP)
    updated_at = sa.Column(sa.TIMESTAMP, default=sa.func.now(), onupdate=sa.func.now())

    __table_args__: tuple[sa.ForeignKeyConstraint, dict[None, None]] = (
        sa.ForeignKeyConstraint(
            [cache_key, cache_expiration],
            [cacholote.config.CacheEntry.key, cacholote.config.CacheEntry.expiration],
            ondelete="set null",
        ),
        {},
    )

    cache_entry = sa.orm.relationship(cacholote.config.CacheEntry)


def ensure_session_obj(session_obj: sa.orm.sessionmaker | None) -> sa.orm.sessionmaker:
    """If `session_obj` is None, create a new session object.

    Parameters
    ----------
    session_obj: sqlalchemy Session object

    Returns
    -------
    session_obj:
        a SQLAlchemy Session object
    """
    if session_obj:
        return session_obj
    settings = config.ensure_settings(config.dbsettings)
    session_obj = sa.orm.sessionmaker(sa.create_engine(settings.connection_string))
    return session_obj


def get_accepted_requests(
    session_obj: sa.orm.sessionmaker | None = None,
) -> list[SystemRequest]:
    """Get all accepted requests."""
    session_obj = ensure_session_obj(session_obj)
    with session_obj() as session:
        statement = sa.select(SystemRequest).where(SystemRequest.status == "accepted")
        return session.scalars(statement).all()


def count_accepted_requests(
    session_obj: sa.orm.sessionmaker | None = None,
) -> int:
    """Count all accepted requests."""
    session_obj = ensure_session_obj(session_obj)
    with session_obj() as session:
        return (
            session.query(SystemRequest)
            .filter(SystemRequest.status == "accepted")
            .count()
        )


def set_request_status(
    request_uid: str,
    status: str,
    cache_key: str | None = None,
    cache_expiration: sa.DateTime | None = None,
    traceback: str | None = None,
    session_obj: sa.orm.sessionmaker | None = None,
) -> None:
    """Set the status of a request."""
    session_obj = ensure_session_obj(session_obj)
    with session_obj() as session:
        statement = sa.select(SystemRequest).where(
            SystemRequest.request_uid == request_uid
        )
        request = session.scalars(statement).one()
        if status == "successful":
            request.finished_at = sa.func.now()
            request.cache_key = cache_key
            request.cache_expiration = cache_expiration
        elif status == "failed":
            request.finished_at = sa.func.now()
            request.response_traceback = traceback
        elif status == "running":
            request.started_at = sa.func.now()
        request.status = status
        session.commit()


def create_request(
    setup_code: str,
    entry_point: str,
    kwargs: dict,
    process_id: str,
    metadata: dict = {},
    request_uid: str = None,
    session_obj: sa.orm.sessionmaker | None = None,
) -> dict[str, Any]:
    """Temporary function to create a request."""
    session_obj = ensure_session_obj(session_obj)

    with session_obj() as session:
        request = SystemRequest(
            request_uid=request_uid or str(uuid.uuid4()),
            process_id=process_id,
            status="accepted",
            request_body={
                "setup_code": setup_code,
                "entry_point": entry_point,
                "kwargs": kwargs,
            },
            request_metadata=metadata,
        )
        session.add(request)
        session.commit()
        ret_value = {
            column.key: getattr(request, column.key)
            for column in sa.inspect(request).mapper.column_attrs
        }
    return ret_value


def get_request(
    request_uid: str, session_obj: sa.orm.sessionmaker | None = None
) -> SystemRequest:
    """Get a request by its UID."""
    session_obj = ensure_session_obj(session_obj)
    with session_obj() as session:
        statement = sa.select(SystemRequest).where(
            SystemRequest.request_uid == request_uid
        )
        return session.scalars(statement).one()


def get_request_result(
    request_uid: str, session_obj: sa.orm.sessionmaker | None = None
) -> dict[str, Any]:
    session_obj = ensure_session_obj(session_obj)
    request = get_request(request_uid, session_obj)
    with session_obj() as session:
        statement = sa.select(cacholote.config.CacheEntry.result).where(
            cacholote.config.CacheEntry.key == request.cache_key,
            cacholote.config.CacheEntry.expiration == request.cache_expiration,
        )
        return session.scalars(statement).one()


def init_database(connection_string: str) -> sa.engine.Engine:
    """
    Initialize the database located at URI `connection_string` and return the engine object.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    engine = sa.create_engine(connection_string)
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)
    # cleanup and create the schema
    BaseModel.metadata.drop_all(engine)
    BaseModel.metadata.create_all(engine)
    return engine

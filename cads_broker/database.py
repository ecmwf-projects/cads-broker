"""SQLAlchemy ORM model."""
import uuid
from typing import Any

import cacholote
import sqlalchemy as sa
import sqlalchemy.orm.exc
import sqlalchemy_utils
import structlog
from sqlalchemy.dialects.postgresql import JSONB

from cads_broker import config

BaseModel = cacholote.database.Base

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


status_enum = sa.Enum(
    "accepted", "running", "failed", "successful", "dismissed", name="status"
)


class NoResultFound(Exception):
    pass


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
            [
                cacholote.database.CacheEntry.key,
                cacholote.database.CacheEntry.expiration,
            ],
            ondelete="set null",
        ),
        {},
    )

    cache_entry = sa.orm.relationship(cacholote.database.CacheEntry)


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
    session_obj = sa.orm.sessionmaker(
        sa.create_engine(settings.connection_string, pool_recycle=settings.pool_recycle)
    )
    return session_obj


def get_accepted_requests(
    session: sa.orm.Session = None,
):
    """Get all accepted requests."""
    statement = sa.select(SystemRequest).where(SystemRequest.status == "accepted")
    return session.scalars(statement).all()


def count_accepted_requests(
    session: sa.orm.Session,
    process_id: str | None = None,
) -> int:
    """Count all accepted requests."""
    statement = session.query(SystemRequest).filter(SystemRequest.status == "accepted")
    if process_id is not None:
        statement = statement.filter(SystemRequest.process_id == process_id)
    return statement.count()


def set_request_status(
    request_uid: str,
    status: str,
    session: sa.orm.Session,
    cache_key: str | None = None,
    cache_expiration: sa.DateTime | None = None,
    traceback: str | None = None,
) -> SystemRequest:
    """Set the status of a request."""
    statement = sa.select(SystemRequest).where(SystemRequest.request_uid == request_uid)
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
    return request


def create_request(
    session: sa.orm.Session,
    user_uid: str,
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any],
    process_id: str,
    metadata: dict[str, Any] = {},
    request_uid: str | None = None,
) -> dict[str, Any]:
    """Temporary function to create a request."""
    metadata["user_uid"] = user_uid
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
    logger.info(
        "accepted job",
        job_id=request.request_uid,
        created_at=request.created_at.strftime(config.timestamp_format),
        updated_at=request.updated_at.strftime(config.timestamp_format),
        request=kwargs.get("request", {}),
        process_id=process_id,
    )
    ret_value = {
        column.key: getattr(request, column.key)
        for column in sa.inspect(request).mapper.column_attrs
    }
    return ret_value


def get_request(
    request_uid: str,
    session: sa.orm.Session,
) -> SystemRequest:
    try:
        statement = sa.select(SystemRequest).where(
            SystemRequest.request_uid == request_uid
        )
        return session.scalars(statement).one()
    except (sqlalchemy.orm.exc.NoResultFound, sqlalchemy.exc.StatementError):
        raise NoResultFound(f"No request found with request_uid {request_uid}")


def get_request_result(
    request_uid: str,
    session: sa.orm.Session,
) -> SystemRequest:
    request = get_request(request_uid, session)
    statement = sa.select(cacholote.database.CacheEntry.result).where(
        cacholote.database.CacheEntry.key == request.cache_key,
        cacholote.database.CacheEntry.expiration == request.cache_expiration,
    )
    return session.scalars(statement).one()


def delete_request(
    request_uid: str,
    session: sa.orm.Session,
) -> SystemRequest:
    set_request_status(request_uid=request_uid, status="dismissed", session=session)
    request = get_request(request_uid, session)
    session.delete(request)
    session.commit()
    return request


def init_database(connection_string: str, force: bool = False) -> sa.engine.Engine:
    """
    Initialize the database located at URI `connection_string` and return the engine object.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    structure_exists = True
    engine = sa.create_engine(connection_string)
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)
        structure_exists = False
    else:
        conn = engine.connect()
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        if set(conn.execute(query).scalars()) != set(BaseModel.metadata.tables):  # type: ignore
            structure_exists = False
    if not structure_exists or force:
        # cleanup and create the schema
        BaseModel.metadata.drop_all(engine)
        BaseModel.metadata.create_all(engine)
    return engine

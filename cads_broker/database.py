"""SQLAlchemy ORM model."""
import hashlib
import uuid
from typing import Any

import sqlalchemy as sa
import sqlalchemy_utils
from sqlalchemy.dialects.postgresql import JSONB

from cads_broker import config

metadata = sa.MetaData()
BaseModel = sa.ext.declarative.declarative_base(metadata=metadata)


status_enum = sa.Enum("accepted", "running", "failed", "successful", name="status")


class SystemRequest(BaseModel):
    """Resource ORM model."""

    __tablename__ = "system_requests"

    request_id = sa.Column(sa.Integer, primary_key=True)
    request_uid = sa.Column(sa.VARCHAR(1024), index=True)
    request_hash = sa.Column(sa.VARCHAR(1024))
    process_id = sa.Column(sa.VARCHAR(1024))
    status = sa.Column(status_enum)
    request_body = sa.Column(JSONB, nullable=False)
    request_metadata = sa.Column(JSONB)
    response_body = sa.Column(JSONB)
    response_metadata = sa.Column(JSONB)
    created_at = sa.Column(sa.TIMESTAMP, default=sa.func.now())
    started_at = sa.Column(sa.TIMESTAMP)
    finished_at = sa.Column(sa.TIMESTAMP)
    updated_at = sa.Column(sa.TIMESTAMP, default=sa.func.now(), onupdate=sa.func.now())
    expire = sa.Column(sa.DateTime)


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


def set_request_status(
    request_uid: str,
    status: str,
    result: str | dict[str, str] | None = None,
    traceback: str | None = None,
    session_obj: sa.orm.sessionmaker | None = None,
) -> None:
    """Set the status of a request."""
    session_obj = ensure_session_obj(session_obj)
    response_body = {"result": result, "traceback": traceback}
    with session_obj() as session:
        statement = sa.select(SystemRequest).where(
            SystemRequest.request_uid == request_uid
        )
        request = session.scalars(statement).one()
        if status in ("successful", "failed"):
            request.finished_at = sa.func.now()
        request.status = status
        request.response_body = response_body
        session.commit()


# temporary implementation of cache
def get_cached_result(
    request: SystemRequest,
    session_obj: sa.orm.sessionmaker | None = None,
):
    session_obj = ensure_session_obj(session_obj)
    with session_obj() as session:
        statement = (
            sa.select(SystemRequest)
            .where(SystemRequest.request_hash == request.request_hash)
            .where(SystemRequest.status == "successful")
        )
        return session.scalars(statement).first()


def create_request(
    request_uid: str = None,
    context: str = "",
    callable_call: str = "",
    process_id: str = "",
    session_obj: sa.orm.sessionmaker | None = None,
    **kwargs
) -> dict[str, Any]:
    """Temporary function to create a request."""
    session_obj = ensure_session_obj(session_obj)

    request_hash = hashlib.md5((context + callable_call).encode()).hexdigest()
    with session_obj() as session:
        request = SystemRequest(
            request_uid=request_uid or str(uuid.uuid4()),
            request_hash=request_hash,
            process_id=process_id,
            status="accepted",
            request_body={"context": context, "callable_call": callable_call},
            request_metadata=kwargs,
        )
        # temporary implementation of cache
        cached_request = get_cached_result(request, session_obj)
        if cached_request is not None:
            request.status = cached_request.status
            request.response_body["result"] = cached_request.response_body.get("result")

        session.add(request)
        session.commit()
        ret_value = {
            c.key: getattr(request, c.key)
            for c in sa.inspect(request).mapper.column_attrs
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


def init_database(connection_string: str) -> sa.engine.Engine:
    """
    Initialize the database located at URI `connection_string` and return the engine object.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    engine = sa.create_engine(connection_string)
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)
    # cleanup and create the schema
    metadata.drop_all(engine)
    metadata.create_all(engine)
    return engine

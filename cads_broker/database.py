"""SQLAlchemy ORM model."""
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

from cads_broker import config

metadata = sa.MetaData()
BaseModel = declarative_base(metadata=metadata)

dbsettings = config.SqlalchemySettings()

status_enum = sa.Enum("queued", "running", "failed", "completed", name="status")


class SystemRequest(BaseModel):
    """Resource ORM model."""

    __tablename__ = "system_requests"

    request_id = sa.Column(sa.Integer, primary_key=True)
    request_uid = sa.Column(sa.VARCHAR(1024), index=True)
    status = sa.Column(status_enum)
    request_body = sa.Column(JSONB, nullable=False)
    request_metadata = sa.Column(JSONB)
    response_body = sa.Column(JSONB)
    response_metadata = sa.Column(JSONB)
    expire = sa.Column(sa.DateTime)


def ensure_session_obj(session_obj: sa.orm.sessionmaker | None) -> sa.orm.sessionmaker:
    """
    If `session_obj` is None, create a new session object.

    :param session_obj:
    """
    return session_obj or sa.orm.sessionmaker(
        sa.create_engine(dbsettings.connection_string)
    )


def set_request_status(
    request_uid: str, status: str, session_obj: sa.orm.sessionmaker | None = None
) -> None:
    """Set the status of a request."""
    session_obj = ensure_session_obj(session_obj)
    with session_obj() as session:
        statement = sa.select(SystemRequest).where(
            SystemRequest.request_uid == request_uid
        )
        request = session.scalars(statement).one()
        request.status = status
        session.commit()


def create_request(
    seconds: int, session_obj: sa.orm.sessionmaker | None = None
) -> SystemRequest:
    """Temporary function to create a request."""
    session_obj = ensure_session_obj(session_obj)
    import time
    import uuid

    with session_obj() as session:
        request = SystemRequest(
            request_uid=uuid.uuid4().hex,
            status="queued",
            request_body={"seconds": seconds},
            request_metadata={"created_at": time.time()},
        )
        session.add(request)
        session.commit()
    return request


def init_database(
    connection_string: str = dbsettings.connection_string,
) -> sa.engine.Engine:
    """
    Initialize the database located at URI `connection_string` and return the engine object.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    engine = sa.create_engine(connection_string)

    # cleanup and create the schema
    metadata.drop_all(engine)
    metadata.create_all(engine)
    return engine

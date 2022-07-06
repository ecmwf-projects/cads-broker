"""SQLAlchemy ORM model"""
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

metadata = sa.MetaData()
BaseModel = declarative_base(metadata=metadata)


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


def init_database(connection_string: str) -> sa.engine.Engine:
    """
    Initialize the database located at URI `connection_string` and return the engine object.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    """
    engine = sa.create_engine(connection_string)

    # cleanup and create the schema
    metadata.drop_all(engine)
    metadata.create_all(engine)
    return engine

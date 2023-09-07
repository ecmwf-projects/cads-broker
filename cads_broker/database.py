"""SQLAlchemy ORM model."""
import datetime
import hashlib
import json
import os
import uuid
from typing import Any

import cacholote
import sqlalchemy as sa
import sqlalchemy.orm.exc
import sqlalchemy_utils
import structlog
from sqlalchemy.dialects.postgresql import JSONB

import alembic.command
import alembic.config
from cads_broker import config

BaseModel = cacholote.database.Base

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


status_enum = sa.Enum(
    "accepted", "running", "failed", "successful", "dismissed", name="status"
)


class NoResultFound(Exception):
    pass


class AdaptorProperties(BaseModel):
    """Adaptor Metadata ORM model."""

    __tablename__ = "adaptor_properties"

    hash = sa.Column(sa.Text, primary_key=True)
    config = sa.Column(JSONB)
    form = sa.Column(JSONB)


class SystemRequest(BaseModel):
    """System Request ORM model."""

    __tablename__ = "system_requests"

    request_id = sa.Column(sa.Integer, primary_key=True)
    request_uid = sa.Column(
        sa.dialects.postgresql.UUID(False),
        index=True,
        unique=True,
    )
    process_id = sa.Column(sa.Text)
    user_uid = sa.Column(sa.Text)
    status = sa.Column(status_enum)
    cache_id = sa.Column(sa.Integer)
    request_body = sa.Column(JSONB, nullable=False)
    request_metadata = sa.Column(JSONB)
    response_error = sa.Column(JSONB, default={})
    response_log = sa.Column(JSONB, default="[]")
    response_user_visible_log = sa.Column(JSONB, default="[]")
    response_metadata = sa.Column(JSONB)
    created_at = sa.Column(sa.TIMESTAMP, default=sa.func.now())
    started_at = sa.Column(sa.TIMESTAMP)
    finished_at = sa.Column(sa.TIMESTAMP)
    updated_at = sa.Column(sa.TIMESTAMP, default=sa.func.now(), onupdate=sa.func.now())
    origin = sa.Column(sa.Text, default="ui")
    portal = sa.Column(sa.Text)
    adaptor_properties_hash = sa.Column(
        sa.Text, sa.ForeignKey("adaptor_properties.hash"), nullable=False
    )
    entry_point = sa.Column(sa.Text)

    __table_args__: tuple[sa.ForeignKeyConstraint, dict[None, None]] = (
        sa.ForeignKeyConstraint(
            [cache_id], [cacholote.database.CacheEntry.id], ondelete="set null"
        ),
        {},
    )

    # joined is temporary
    cache_entry = sa.orm.relationship(cacholote.database.CacheEntry, lazy="joined")
    adaptor_properties = sa.orm.relationship(AdaptorProperties, lazy="joined")

    @property
    def age(self):
        """Returns the age of the request in seconds".

        Returns
        -------
            float: Age in seconds.
        """
        return (datetime.datetime.now() - self.created_at).seconds

    @property
    def cost(self):
        return (0, 0)


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


def get_running_requests(
    session: sa.orm.Session,
):
    """Get all running requests."""
    statement = sa.select(SystemRequest).where(SystemRequest.status == "running")
    return session.scalars(statement).all()


def get_accepted_requests(
    session: sa.orm.Session,
):
    """Get all accepted requests."""
    statement = sa.select(SystemRequest).where(SystemRequest.status == "accepted")
    return session.scalars(statement).all()


def count_finished_requests_per_user_in_session(
    user_uid: str,
    session: sa.orm.Session,
    seconds: int | None = None,
) -> int:
    """Count running requests for user_uid."""
    statement = (
        session.query(SystemRequest)
        .where(SystemRequest.user_uid == user_uid)
        .where(SystemRequest.status.in_(("successful", "failed")))
    )
    if seconds is not None:
        finished_at = datetime.datetime.now() - datetime.timedelta(seconds=seconds)
        statement = statement.where(SystemRequest.finished_at >= finished_at)
    return statement.count()


def count_finished_requests_per_user(
    user_uid: str,
    seconds: int | None = None,
    session_maker: sa.orm.Session = None,
) -> int:
    """Count running requests for user_uid."""
    session_maker = ensure_session_obj(session_maker)
    with session_maker() as session:
        ret_value = count_finished_requests_per_user_in_session(
            user_uid=user_uid, seconds=seconds, session=session
        )
        return ret_value


def count_requests(
    session: sa.orm.Session,
    status: str | None = None,
    entry_point: str | None = None,
    user_uid: str | None = None,
) -> int:
    """Count requests."""
    statement = session.query(SystemRequest)
    if status is not None:
        statement = statement.filter(SystemRequest.status == status)
    if entry_point is not None:
        statement = statement.filter(SystemRequest.entry_point == entry_point)
    if user_uid is not None:
        statement = statement.filter(SystemRequest.user_uid == user_uid)
    return statement.count()


def count_requests_per_dataset_status(
    session: sa.orm.Session,
) -> list:
    """Count request by dataset and status (status that change over time)."""
    return session.execute(
        sa.select(SystemRequest.process_id, SystemRequest.status, sa.func.count())
        .where(SystemRequest.status.in_(("accepted", "running")))
        .group_by(SystemRequest.status, SystemRequest.process_id)
    ).all()


def count_last_day_requests_per_dataset_status(session: sa.orm.Session) -> list:
    """Count last day requests by dataset and status (permanent status)."""
    return session.execute(
        sa.select(SystemRequest.process_id, SystemRequest.status, sa.func.count())
        .where(
            SystemRequest.created_at
            > (datetime.datetime.now() - datetime.timedelta(days=1))
        )
        .where(SystemRequest.status.in_(("failed", "successful", "dismissed")))
        .group_by(SystemRequest.status, SystemRequest.process_id)
    ).all()


def total_request_time_per_dataset_status(
    session: sa.orm.Session,
) -> list:
    return session.execute(
        sa.select(
            SystemRequest.process_id,
            SystemRequest.status,
            sa.func.sum(SystemRequest.finished_at - SystemRequest.started_at),
        )
        .filter(
            SystemRequest.created_at
            > (datetime.datetime.now() - datetime.timedelta(days=1)),
            SystemRequest.started_at.isnot(None),
            SystemRequest.finished_at.isnot(None),
        )
        .group_by(SystemRequest.process_id, SystemRequest.status)
    ).all()


def count_active_users(session: sa.orm.Session) -> list:
    """Users which have requests with status running or accepted, per dataset."""
    return session.execute(
        sa.select(
            SystemRequest.process_id, sa.func.count(sa.distinct(SystemRequest.user_uid))
        )
        .filter(SystemRequest.status.in_(("running", "accepted")))
        .group_by(SystemRequest.process_id)
    ).all()


def count_queued_users(session: sa.orm.Session) -> list:
    """Users that have requests with status accepted, per dataset."""
    return session.execute(
        sa.select(
            SystemRequest.process_id, sa.func.count(sa.distinct(SystemRequest.user_uid))
        )
        .filter(SystemRequest.status == "accepted")
        .group_by(SystemRequest.process_id)
    ).all()


def count_waiting_users_queued_behind_themselves(session: sa.orm.Session) -> list:
    """Users that have at least an accepted and a running request, per dataset."""
    sr1 = sa.orm.aliased(SystemRequest)
    sr2 = sa.orm.aliased(SystemRequest)

    subq = (
        sa.select(sr1.process_id, sr1.user_uid)
        .join(sr2, (sr1.user_uid == sr2.user_uid) & (sr1.process_id == sr2.process_id))
        .filter(sr1.status == "accepted", sr2.status == "running")
        .group_by(sr1.process_id, sr1.user_uid)
        .subquery()
    )

    # count the number of user_uid values from the subquery and per dataset
    return session.execute(
        sa.select(subq.c.process_id, sa.func.count(subq.c.user_uid)).group_by(
            subq.c.process_id
        )
    ).all()


def count_waiting_users_queued(session: sa.orm.Session):
    """Users that only have accepted requests (not running requests), per dataset."""
    subquery = (
        sa.select(
            SystemRequest.process_id,
            SystemRequest.user_uid,
            sa.func.array_agg(sa.distinct(SystemRequest.status)).label("in_status"),
        )
        .group_by(SystemRequest.process_id, SystemRequest.user_uid)
        .subquery()
    )

    return session.execute(
        sa.select(subquery.c.process_id, sa.func.count().label("count"))
        .where(sa.all_(subquery.c.in_status) != "running")
        .where(sa.any_(subquery.c.in_status) == "accepted")
        .group_by(subquery.c.process_id)
    ).all()


def count_users(status: str, entry_point: str, session: sa.orm.Session) -> int:
    """Users that have running requests, per dataset."""
    return (
        session.query(SystemRequest.user_uid)
        .filter(
            SystemRequest.status == status, SystemRequest.entry_point == entry_point
        )
        .distinct()
        .count()
    )


def set_request_status(
    request_uid: str,
    status: str,
    session: sa.orm.Session,
    cache_id: int | None = None,
    error_message: str | None = None,
    error_reason: str | None = None,
    log: list[tuple[int, str]] = [],
    user_visible_log: list[tuple[int, str]] = [],
    resubmit: bool | None = None,
) -> SystemRequest:
    """Set the status of a request."""
    statement = sa.select(SystemRequest).where(SystemRequest.request_uid == request_uid)
    request = session.scalars(statement).one()
    if resubmit:
        # ugly implementation because sqlalchemy doesn't allow to directly update JSONB
        # FIXME: use a specific column for resubmit_number
        metadata = dict(request.request_metadata)
        metadata.update(
            {"resubmit_number": request.request_metadata.get("resubmit_number", 0) + 1}
        )
        request.request_metadata = metadata
    if status == "successful":
        request.finished_at = sa.func.now()
        request.cache_id = cache_id
    elif status == "failed":
        request.finished_at = sa.func.now()
        request.response_error = {"message": error_message, "reason": error_reason}
    elif status == "running":
        request.started_at = sa.func.now()
    # FIXME: logs can't be live updated
    request.response_log = json.dumps(log)
    request.response_user_visible_log = json.dumps(user_visible_log)
    request.status = status
    session.commit()
    return request


def logger_kwargs(request: SystemRequest) -> dict[str, str]:
    kwargs = {
        "event_type": "DATASET_REQUEST",
        "job_id": request.request_uid,
        "user_uid": request.user_uid,
        "status": request.status,
        "result": request.cache_entry.result if request.cache_entry else None,
        "created_at": request.created_at.isoformat(),
        "updated_at": request.updated_at.isoformat(),
        "started_at": request.started_at.isoformat()
        if request.started_at is not None
        else None,
        "finished_at": request.finished_at.isoformat()
        if request.finished_at is not None
        else None,
        "request_kwargs": request.request_body.get("request", {}),
        "user_request": True,
        "process_id": request.process_id,
        "resubmit_number": request.request_metadata.get("resubmit_number", 0),
        "origin": request.origin,
        "entry_point": request.entry_point,
        **request.response_error,
    }
    return kwargs


def generate_adaptor_properties_hash(
    config: dict[str, Any], form: dict[str, Any]
) -> str:
    config_form = {"config": config, "form": form}
    return hashlib.md5(
        json.dumps(config_form, sort_keys=True).encode("utf-8")
    ).hexdigest()


def get_adaptor_properties(
    adaptor_properties_hash: str,
    session: sa.orm.Session,
) -> AdaptorProperties | None:
    try:
        statement = sa.select(AdaptorProperties).where(
            AdaptorProperties.hash == adaptor_properties_hash
        )
        return session.scalars(statement).one()
    except sqlalchemy.orm.exc.NoResultFound:
        return None


def add_adaptor_properties(
    hash: str,
    config: dict[str, Any],
    form: dict[str, Any],
    session: sa.orm.Session,
):
    adaptor_properties = AdaptorProperties(hash=hash, config=config, form=form)
    session.add(adaptor_properties)


def create_request(
    session: sa.orm.Session,
    user_uid: str,
    setup_code: str,
    entry_point: str,
    request: dict[str, Any],
    process_id: str,
    portal: str,
    adaptor_config: dict[str, Any],
    adaptor_form: dict[str, Any],
    adaptor_properties_hash: str,
    metadata: dict[str, Any] = {},
    resources: dict[str, Any] = {},
    qos_tags: list[str] = [],
    origin: str = "ui",
    request_uid: str | None = None,
) -> dict[str, Any]:
    """Create a request."""
    if (
        get_adaptor_properties(
            adaptor_properties_hash=adaptor_properties_hash, session=session
        )
        is None
    ):
        add_adaptor_properties(
            hash=adaptor_properties_hash,
            config=adaptor_config,
            form=adaptor_form,
            session=session,
        )
    metadata["resources"] = resources
    metadata["qos_tags"] = qos_tags
    request = SystemRequest(
        request_uid=request_uid or str(uuid.uuid4()),
        process_id=process_id,
        user_uid=user_uid,
        status="accepted",
        request_body={
            "setup_code": setup_code,
            "request": request,
        },
        request_metadata=metadata,
        origin=origin,
        portal=portal,
        adaptor_properties_hash=adaptor_properties_hash,
        entry_point=entry_point,
    )
    session.add(request)
    session.commit()
    logger.info("accepted job", **logger_kwargs(request=request))
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
    except sqlalchemy.orm.exc.NoResultFound:
        logger.exception("get_request failed")
        raise NoResultFound(f"No request found with request_uid {request_uid}")


def get_request_result(
    request_uid: str,
    session: sa.orm.Session,
) -> SystemRequest:
    request = get_request(request_uid, session)
    logger.info(
        "result accessed",
        user_uid=request.user_uid,
        job_id=request.request_uid,
        process_id=request.process_id,
        status=request.status,
    )
    return request.cache_entry.result


def delete_request(
    request: SystemRequest,
    session: sa.orm.Session,
) -> None:
    session.delete(request)
    session.commit()


def init_database(connection_string: str, force: bool = False) -> sa.engine.Engine:
    """
    Make sure the db located at URI `connection_string` exists updated and return the engine object.

    :param connection_string: something like 'postgresql://user:password@netloc:port/dbname'
    :param force: if True, drop the database structure and build again from scratch
    """
    engine = sa.create_engine(connection_string)
    migration_directory = os.path.abspath(os.path.join(__file__, "..", ".."))
    os.chdir(migration_directory)
    alembic_config_path = os.path.join(migration_directory, "alembic.ini")
    alembic_cfg = alembic.config.Config(alembic_config_path)
    alembic_cfg.set_main_option("sqlalchemy.url", connection_string)
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)
        # cleanup and create the schema
        BaseModel.metadata.drop_all(engine)
        BaseModel.metadata.create_all(engine)
        alembic.command.stamp(alembic_cfg, "head")
    else:
        # check the structure is empty or incomplete
        query = sa.text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        with engine.connect() as conn:
            if "system_requests" not in conn.execute(query).scalars().all():
                force = True
    if force:
        # cleanup and create the schema
        BaseModel.metadata.drop_all(engine)
        BaseModel.metadata.create_all(engine)
        alembic.command.stamp(alembic_cfg, "head")
    else:
        # update db structure
        alembic.command.upgrade(alembic_cfg, "head")
    return engine

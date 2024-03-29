"""SQLAlchemy ORM model."""

import datetime
import hashlib
import json
import os
import uuid
from typing import Any

import cacholote
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.orm.exc
import sqlalchemy_utils
import structlog
from sqlalchemy.dialects.postgresql import JSONB

import alembic.command
import alembic.config
from cads_broker import config

BaseModel = sa.orm.declarative_base()

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


status_enum = sa.Enum(
    "accepted", "running", "failed", "successful", "dismissed", name="status"
)


class NoResultFound(Exception):
    pass


class InvalidRequestID(Exception):
    pass


class Events(BaseModel):
    """Events ORM model."""

    __tablename__ = "events"

    event_id = sa.Column(sa.Integer, primary_key=True)
    event_type = sa.Column(sa.Text, index=True)
    request_uid = sa.Column(
        sa.dialects.postgresql.UUID(False),
        sa.ForeignKey("system_requests.request_uid"),
        index=True,
    )
    message = sa.Column(sa.Text)
    timestamp = sa.Column(sa.TIMESTAMP, default=sa.func.now())


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
    process_id = sa.Column(sa.Text, index=True)
    user_uid = sa.Column(sa.Text, index=True)
    status = sa.Column(status_enum)
    cache_id = sa.Column(sa.Integer, index=True)
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
    qos_status = sa.Column(JSONB, default=dict)

    __table_args__: tuple[sa.ForeignKeyConstraint, dict[None, None]] = (
        sa.ForeignKeyConstraint(
            [cache_id], [cacholote.database.CacheEntry.id], ondelete="set null"
        ),
        {},
    )

    # joined is temporary
    cache_entry = sa.orm.relationship(cacholote.database.CacheEntry, lazy="joined")
    adaptor_properties = sa.orm.relationship(AdaptorProperties, lazy="select")
    events = sa.orm.relationship(Events, lazy="select")

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
    if settings.pool_size == -1:
        engine = sa.create_engine(settings.connection_string, poolclass=sa.pool.NullPool)
    else:
        engine = sa.create_engine(
            settings.connection_string,
            pool_recycle=settings.pool_recycle,
            pool_size=settings.pool_size,
            pool_timeout=settings.pool_timeout,
            max_overflow=settings.max_overflow,
        )
    session_obj = sa.orm.sessionmaker(engine)
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
    statement = (
        sa.select(SystemRequest)
        .where(SystemRequest.status == "accepted")
        .order_by(SystemRequest.created_at)
    )
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
    process_id: str | list[str] | None = None,
    status: str | list[str] | None = None,
    entry_point: str | list[str] | None = None,
    user_uid: str | list[str] | None = None,
    portal: str | list[str] | None = None,
) -> int:
    """Count requests."""
    statement = session.query(SystemRequest)
    if process_id is not None:
        if isinstance(process_id, str):
            process_id = [process_id]
        statement = statement.filter(SystemRequest.process_id.in_(process_id))
    if status is not None:
        if isinstance(status, str):
            status = [status]
        statement = statement.filter(SystemRequest.status.in_(status))
    if entry_point is not None:
        if isinstance(entry_point, str):
            entry_point = [entry_point]
        statement = statement.filter(SystemRequest.entry_point.in_(entry_point))
    if user_uid is not None:
        if isinstance(user_uid, str):
            user_uid = [user_uid]
        statement = statement.filter(SystemRequest.user_uid.in_(user_uid))
    if portal is not None:
        if isinstance(portal, str):
            portal = [portal]
        statement = statement.filter(SystemRequest.portal.in_(portal))
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


def get_events_from_request(
    request_uid: str,
    session: sa.orm.Session,
    event_type: str | None = None,
    start_time: datetime.datetime | None = None,
    stop_time: datetime.datetime | None = None,
) -> list[Events]:
    statement = sa.select(Events).filter(Events.request_uid == request_uid)
    if event_type is not None:
        statement = statement.filter(Events.event_type == event_type)
    if start_time is not None:
        statement = statement.filter(Events.timestamp > start_time)
    if stop_time is not None:
        statement = statement.filter(Events.timestamp <= stop_time)
    events = session.scalars(statement).all()
    return events


def get_qos_status_from_request(
    request: SystemRequest,
) -> dict[str, list[tuple[str, str]]]:
    ret_value: dict[str, list[str]] = {}
    for rule_name, rules in request.qos_status.items():
        ret_value[rule_name] = []
        for rule in rules.values():
            ret_value[rule_name].append(
                (rule.get("info", ""), rule.get("conclusion", ""))
            )
    return ret_value


def set_request_qos_rule(
    request: SystemRequest,
    rule,
    session: sa.orm.Session,
):
    qos_status = request.qos_status
    old_rules = qos_status.get(rule.name, {})
    rule_uid = rule.get_uid(request)
    if rule_uid in old_rules:
        return
    old_rules[rule_uid] = {
        "conclusion": str(rule.evaluate(request)),
        "info": str(rule.info).replace('"', ""),
        "condition": str(rule.condition),
    }
    qos_status[rule.name] = old_rules
    session.execute(
        sa.update(SystemRequest)
        .filter_by(request_uid=request.request_uid)
        .values(qos_status=qos_status)
    )


def requeue_request(
    request_uid: str,
    session: sa.orm.Session,
):
    statement = sa.select(SystemRequest).where(SystemRequest.request_uid == request_uid)
    request = session.scalars(statement).one()
    if request.status == "running":
        # ugly implementation because sqlalchemy doesn't allow to directly update JSONB
        # FIXME: use a specific column for resubmit_number
        metadata = dict(request.request_metadata)
        metadata.update(
            {"resubmit_number": request.request_metadata.get("resubmit_number", 0) + 1}
        )
        request.request_metadata = metadata
        request.status = "accepted"
        session.commit()
        logger.info("--------- requeueing request", **logger_kwargs(request=request))
        return request
    else:
        return


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
        request.qos_status = {}
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
        "started_at": (
            request.started_at.isoformat() if request.started_at is not None else None
        ),
        "finished_at": (
            request.finished_at.isoformat() if request.finished_at is not None else None
        ),
        "request_kwargs": request.request_body.get("request", {}),
        "user_request": True,
        "process_id": request.process_id,
        "resubmit_number": request.request_metadata.get("resubmit_number", 0),
        "worker_name": [
            event.message
            for event in request.events
            if event.event_type == "worker_name"
        ],
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
        statement = sa.select(AdaptorProperties.hash).where(
            AdaptorProperties.hash == adaptor_properties_hash
        )
        return session.execute(statement).one()
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


def add_event(
    event_type: str,
    request_uid: str,
    message: str,
    session: sa.orm.Session,
):
    event = Events(event_type=event_type, request_uid=request_uid, message=message)
    session.add(event)
    session.commit()


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
    except sqlalchemy.exc.DataError:
        raise InvalidRequestID(f"Invalid request_uid {request_uid}")
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
    for option in ["drivername", "username", "password", "host", "port", "database"]:
        value = getattr(engine.url, option)
        if value is None:
            value = ""
        alembic_cfg.set_main_option(option, str(value))
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)
        # cleanup and create the schema
        BaseModel.metadata.drop_all(engine)
        cacholote.database.Base.metadata.drop_all(engine)
        cacholote.database.Base.metadata.create_all(engine)
        BaseModel.metadata.create_all(engine)
        alembic.command.stamp(alembic_cfg, "head")
    else:
        # check the structure is empty or incomplete
        query = sa.text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        conn = engine.connect()
        if "system_requests" not in conn.execute(query).scalars().all():
            force = True
    if force:
        # cleanup and create the schema
        BaseModel.metadata.drop_all(engine)
        cacholote.database.Base.metadata.drop_all(engine)
        cacholote.database.Base.metadata.create_all(engine)
        BaseModel.metadata.create_all(engine)
        alembic.command.stamp(alembic_cfg, "head")
    else:
        # update db structure
        alembic.command.upgrade(alembic_cfg, "head")
    return engine

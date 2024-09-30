"""SQLAlchemy ORM model."""

import datetime
import hashlib
import json
import os
import uuid
from typing import Any

import cachetools
import cacholote
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.orm.exc
import sqlalchemy_utils
import structlog
from sqlalchemy.dialects.postgresql import JSONB
from typing_extensions import Iterable

import alembic.command
import alembic.config
from cads_broker import config

BaseModel = sa.orm.declarative_base()

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

CONFIG = config.BrokerConfig()

status_enum = sa.Enum(
    "accepted", "running", "failed", "successful", "dismissed", "deleted", name="status"
)
DISMISSED_MESSAGE = os.getenv(
    "DISMISSED_MESSAGE", "The request has been dismissed by the system."
)
COUNT_REQUESTS_CALLS = 0
COUNT_REQUEST_CACHE = cachetools.LRUCache(maxsize=1000)


class NoResultFound(Exception):
    pass


class InvalidRequestID(Exception):
    pass


class QoSRule(BaseModel):
    """QoS Rule ORM model."""

    __tablename__ = "qos_rules"

    uid = sa.Column(sa.Text, primary_key=True)
    name = sa.Column(sa.Text)
    info = sa.Column(sa.Text)
    condition = sa.Column(sa.Text)
    conclusion = sa.Column(sa.Text)
    conclusion_value = sa.Column(sa.Text)
    queued = sa.Column(sa.Integer)
    running = sa.Column(sa.Integer)

    system_requests: sa.orm.Mapped[list["SystemRequest"]] = sa.orm.relationship(
        "SystemRequest",
        secondary="system_request_qos_rule",
        back_populates="qos_rules",
        uselist=True,
    )


class SystemRequestQoSRule(BaseModel):
    """Association table for SystemRequest and QoSRule."""

    __tablename__ = "system_request_qos_rule"

    request_uid = sa.Column(
        sa.dialects.postgresql.UUID(False),
        sa.ForeignKey("system_requests.request_uid", ondelete="CASCADE"),
        primary_key=True,
    )
    rule_uid = sa.Column(
        sa.Text, sa.ForeignKey("qos_rules.uid", ondelete="CASCADE"), primary_key=True
    )


class Events(BaseModel):
    """Events ORM model."""

    __tablename__ = "events"

    event_id = sa.Column(sa.Integer, primary_key=True)
    event_type = sa.Column(sa.Text, index=True)
    request_uid = sa.Column(
        sa.dialects.postgresql.UUID(False),
        sa.ForeignKey("system_requests.request_uid", ondelete="CASCADE"),
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
    timestamp = sa.Column(sa.TIMESTAMP, default=sa.func.now())


class SystemRequest(BaseModel):
    """System Request ORM model."""

    __tablename__ = "system_requests"

    request_uid = sa.Column(sa.dialects.postgresql.UUID(False), primary_key=True)
    process_id = sa.Column(sa.Text, index=True)
    user_uid = sa.Column(sa.Text, index=True)
    status = sa.Column(status_enum, index=True)
    cache_id = sa.Column(sa.Integer, index=True)
    request_body = sa.Column(JSONB, nullable=False)
    request_metadata = sa.Column(JSONB)
    response_error = sa.Column(JSONB, default={})
    response_log = sa.Column(JSONB, default="[]")
    response_user_visible_log = sa.Column(JSONB, default="[]")
    response_metadata = sa.Column(JSONB)
    created_at = sa.Column(sa.TIMESTAMP, default=sa.func.now(), index=True)
    started_at = sa.Column(sa.TIMESTAMP)
    finished_at = sa.Column(sa.TIMESTAMP, index=True)
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
    # https://github.com/sqlalchemy/sqlalchemy/issues/11063#issuecomment-2008101926
    __mapper_args__ = {"eager_defaults": False}

    # joined is temporary
    cache_entry: sa.orm.Mapped["cacholote.database.CacheEntry"] = sa.orm.relationship(
        cacholote.database.CacheEntry, lazy="joined"
    )
    adaptor_properties: sa.orm.Mapped["AdaptorProperties"] = sa.orm.relationship(
        AdaptorProperties, lazy="select"
    )
    events: sa.orm.Mapped[list["Events"]] = sa.orm.relationship(
        Events, lazy="select", passive_deletes=True
    )
    qos_rules: sa.orm.Mapped[list["QoSRule"]] = sa.orm.relationship(
        "QoSRule",
        secondary="system_request_qos_rule",
        back_populates="system_requests",
        uselist=True,
        lazy="subquery",
    )

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


def ensure_session_obj(
    session_obj: sa.orm.sessionmaker | None, mode="w"
) -> sa.orm.sessionmaker:
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
    if mode == "r":
        connection_string = settings.connection_string_read
    elif mode == "w":
        connection_string = settings.connection_string
    if settings.pool_size == -1:
        engine = sa.create_engine(connection_string, poolclass=sa.pool.NullPool)
    else:
        engine = sa.create_engine(
            connection_string,
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
    last_created_at: datetime.datetime | None = None,
):
    """Get all accepted requests."""
    statement = sa.select(SystemRequest)
    if last_created_at:
        statement = statement.where(SystemRequest.created_at >= last_created_at)
    statement = statement.where(SystemRequest.status == "accepted").order_by(
        SystemRequest.created_at
    )
    return session.scalars(statement).all()


def count_accepted_requests_before(
    session: sa.orm.Session,
    last_created_at: datetime.datetime,
) -> int:
    """Count running requests for user_uid."""
    statement = (
        session.query(SystemRequest)
        .where(SystemRequest.status == "accepted")
        .where(SystemRequest.created_at <= last_created_at)
    )
    return statement.count()


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


def cached_count_requests(*args, **kwargs):
    global COUNT_REQUESTS_CALLS

    COUNT_REQUESTS_CALLS += 1

    if COUNT_REQUESTS_CALLS >= CONFIG.broker_count_requests_cache_size:
        COUNT_REQUEST_CACHE.clear()
        COUNT_REQUESTS_CALLS = 0

    key = cachetools.keys.hashkey(*args, **kwargs)
    if key in COUNT_REQUEST_CACHE:
        return COUNT_REQUEST_CACHE[key]

    result = count_requests(*args, **kwargs)
    COUNT_REQUEST_CACHE[key] = result

    return result


def get_dismissed_requests(session: sa.orm.Session) -> Iterable[SystemRequest]:
    stmt_dismissed = sa.select(SystemRequest).where(SystemRequest.status == "dismissed")
    return session.scalars(stmt_dismissed).fetchall()


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


def reset_qos_rules(session: sa.orm.Session, qos):
    """Delete all QoS rules."""
    session.execute(sa.text("truncate qos_rules cascade"))
    # for rule in session.scalars(sa.select(QoSRule)):
    #     # rule.system_requests = []
    #     session.delete(rule)

    cached_rules: dict[str, Any] = {}
    for request in get_running_requests(session):
        # Recompute the limits
        # It just updates the database. Internal qos is already updated.
        limits = qos.limits_for(request, session)
        _, rules = delete_request_qos_status(
            request_uid=request.request_uid,
            rules=limits,
            session=session,
            rules_in_db=cached_rules,
        )
        cached_rules.update(rules)
    session.commit()


def count_system_request_qos_rule(session: sa.orm.Session) -> int:
    """Count the number of rows in system_request_qos_rule."""
    return session.query(SystemRequestQoSRule).count()


def get_qos_rule(uid: str, session: sa.orm.Session):
    """Get a QoS rule."""
    statement = sa.select(QoSRule).where(QoSRule.uid == uid)
    return session.scalars(statement).one()


def get_qos_rules(session: sa.orm.Session):
    """Get all QoS rules."""
    statement = sa.select(QoSRule)
    return {rule.uid: rule for rule in session.scalars(statement).all()}


def add_qos_rule(
    rule,
    session: sa.orm.Session,
    queued: int = 0,
    running: int = 0,
):
    """Add a QoS rule."""
    conclusion_value = str(rule.evaluate(request=None))
    qos_rule = QoSRule(
        uid=str(rule.__hash__()),
        name=str(rule.name),
        info=str(rule.info).replace("$value", conclusion_value),
        condition=str(rule.condition),
        conclusion=str(rule.conclusion),
        # conclusion_value may change over time, this case is not handled
        conclusion_value=conclusion_value,
        queued=queued,
        running=running,
    )
    session.add(qos_rule)
    session.commit()
    return qos_rule


def decrement_qos_rule_running(
    rules: list, session: sa.orm.Session, rules_in_db: dict[str, QoSRule] = {}, **kwargs
):
    """Decrement the running counter of a QoS rule."""
    for rule in rules:
        if (rule_uid := str(rule.__hash__())) in rules_in_db:
            qos_rule = rules_in_db[rule_uid]
        else:
            try:
                qos_rule = get_qos_rule(rule_uid, session)
            except sqlalchemy.orm.exc.NoResultFound:
                # this happend when a request is finished after a broker restart.
                # the rule is not in the database anymore because it has been reset.
                continue
        qos_rule.running = rule.value
    return None, None


def get_users_queue_from_processing_time(
    session: sa.orm.Session,
    interval_stop: datetime.datetime,
    interval: datetime.timedelta = datetime.timedelta(hours=24),
) -> dict[str, int]:
    """Build the queue of the users from the processing time."""
    interval_start = interval_stop - interval
    request_processing_time = sa.sql.func.least(
        SystemRequest.finished_at, interval_stop
    ) - sa.sql.func.greatest(SystemRequest.started_at, interval_start)
    user_cumulative_processing_time = sa.sql.func.sum(request_processing_time)
    user_cost = (
        sa.sql.func.extract("epoch", user_cumulative_processing_time)
        .cast(sa.Integer)
        .label("user_cost")
    )
    interval_clause = sa.sql.and_(
        SystemRequest.finished_at >= interval_start,
        SystemRequest.finished_at < interval_stop,
        SystemRequest.status != "deleted",
    )
    where_clause = sa.sql.or_(interval_clause, SystemRequest.status == "running")

    statement = (
        sa.sql.select(SystemRequest.user_uid, user_cost)
        .where(where_clause)
        .group_by(SystemRequest.user_uid)
        .order_by("user_cost")
    )

    running_user_costs = dict(session.execute(statement).all())

    queue_users = session.execute(
        sa.select(SystemRequest.user_uid)
        .where(SystemRequest.status == "accepted")
        .distinct()
    ).all()

    queueing_user_costs = {u: 0 for (u,) in queue_users if u not in running_user_costs}

    return queueing_user_costs | running_user_costs


def delete_request_qos_status(
    request_uid: str,
    rules: list,
    session: sa.orm.Session,
    rules_in_db: dict[str, QoSRule] = {},
    **kwargs,
):
    """Delete all QoS rules from a request."""
    created_rules: dict = {}
    request = get_request(request_uid, session)
    for rule in rules:
        if (rule_uid := str(rule.__hash__())) in rules_in_db:
            qos_rule = rules_in_db[rule_uid]
        else:
            try:
                qos_rule = get_qos_rule(rule_uid, session)
            except sqlalchemy.orm.exc.NoResultFound:
                qos_rule = add_qos_rule(rule=rule, session=session)
                created_rules[qos_rule.uid] = qos_rule
        qos_rule.queued = len(rule.queued)
        qos_rule.running = rule.value
    request.qos_rules = []
    return None, created_rules


def add_request_qos_status(
    request: SystemRequest,
    rules: list,
    session: sa.orm.Session,
    rules_in_db: dict[str, QoSRule] = {},
    **kwargs,
) -> tuple[SystemRequest | None, dict[str, QoSRule]]:
    created_rules: dict = {}
    new_request = None
    if request is None:
        return new_request, {}
    for rule in rules:
        if (rule_uid := str(rule.__hash__())) in rules_in_db:
            qos_rule = rules_in_db[rule_uid]
        else:
            qos_rule = add_qos_rule(rule=rule, session=session)
            created_rules[qos_rule.uid] = qos_rule
        if qos_rule.uid not in [r.uid for r in request.qos_rules]:
            qos_rule.queued = len(rule.queued)
            new_request = get_request(request.request_uid, session)
            new_request.qos_rules.append(qos_rule)
    return new_request, created_rules


def get_qos_status_from_request(
    request: SystemRequest,
) -> dict[str, list[dict[str, str]]]:
    ret_value: dict[str, list[dict[str, str]]] = {}
    rules = request.qos_rules
    for rule in rules:
        rule_name = rule.name
        rule_summary = {
            "info": rule.info,
            "queued": rule.queued,
            "running": rule.running,
            "conclusion": rule.conclusion_value,
        }
        if rule_name not in ret_value:
            ret_value[rule_name] = [rule_summary]
        else:
            ret_value[rule_name].append(rule_summary)
    return ret_value


def requeue_request(
    request: SystemRequest,
    session: sa.orm.Session,
) -> SystemRequest | None:
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
        logger.info("requeueing request", **logger_kwargs(request=request))
        return request
    else:
        return None


def set_request_cache_id(request_uid: str, cache_id: int, session: sa.orm.Session):
    statement = sa.select(SystemRequest).where(SystemRequest.request_uid == request_uid)
    request = session.scalars(statement).one()
    request.cache_id = cache_id
    session.commit()
    return request


def set_successful_request(
    request_uid: str,
    session: sa.orm.Session,
) -> SystemRequest | None:
    statement = sa.select(SystemRequest).where(SystemRequest.request_uid == request_uid)
    request = session.scalars(statement).one()
    if request.status == "successful":
        return None
    request.status = "successful"
    request.finished_at = sa.func.now()
    session.commit()
    return request


def set_dismissed_request(
    request_uid: str,
    session: sa.orm.Session,
    message: str = "Dismissed by the user.",
    reason: str = "DismissedRequest",
) -> SystemRequest:
    request = get_request(request_uid=request_uid, session=session)
    metadata = dict(request.request_metadata)
    metadata.update(
        {
            "dismission": {
                "previous_status": request.status,
                "message": message,
                "reason": reason,
            }
        }
    )
    request.request_metadata = metadata
    request.status = "dismissed"
    session.commit()
    logger.info("dismissed job by the user.", **logger_kwargs(request=request))
    return request


def set_request_status(
    request_uid: str,
    status: str,
    session: sa.orm.Session,
    cache_id: int | None = None,
    error_message: str | None = None,
    error_reason: str | None = None,
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
    elif status == "failed":
        request.finished_at = sa.func.now()
        request.response_error = {"message": error_message, "reason": error_reason}
    elif status == "running":
        request.started_at = sa.func.now()
        request.qos_status = {}
    if cache_id is not None:
        request.cache_id = cache_id
    # FIXME: logs can't be live updated
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
        "portal": request.portal,
        "entry_point": request.entry_point,
        "request_metadata": request.request_metadata,
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


def ensure_adaptor_properties(
    hash: str,
    config: dict[str, Any],
    form: dict[str, Any],
    session: sa.orm.Session,
) -> None:
    """Create adaptor properties (if not exists) or update its timestamp."""
    try:
        adaptor_properties = AdaptorProperties(hash=hash, config=config, form=form)
        session.add(adaptor_properties)
        session.commit()
    except sa.exc.IntegrityError:  # hash already present
        session.rollback()
        statement = (
            AdaptorProperties.__table__.update()
            .where(AdaptorProperties.__table__.c.hash == hash)
            .values(timestamp=datetime.datetime.now())
        )
        session.execute(statement)
        session.commit()


def add_event(
    event_type: str,
    request_uid: str,
    message: str,
    session: sa.orm.Session,
):
    event = Events(event_type=event_type, request_uid=request_uid, message=message)
    session.add(event)
    session.commit()


def dictify_request(request: SystemRequest) -> dict[str, Any]:
    ret_value = {
        column.key: getattr(request, column.key)
        for column in sa.inspect(request).mapper.column_attrs
    }
    ret_value["qos_rules"] = [rule.uid for rule in request.qos_rules]
    return ret_value


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
    ensure_adaptor_properties(
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
    return dictify_request(request)


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
        alembic.command.stamp(alembic_cfg, "head")
        cacholote.init_database(connection_string, force)
        BaseModel.metadata.create_all(engine)
    else:
        # check the structure is empty or incomplete
        query = sa.text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        conn = engine.connect()
        if "system_requests" not in conn.execute(query).scalars().all():
            force = True
        conn.close()
    if force:
        # cleanup and create the schema
        BaseModel.metadata.drop_all(engine)
        alembic.command.stamp(alembic_cfg, "head")
        cacholote.init_database(connection_string, force)
        BaseModel.metadata.create_all(engine)
    else:
        # update db structure
        alembic.command.upgrade(alembic_cfg, "head")
        cacholote.init_database(connection_string, force)
    return engine

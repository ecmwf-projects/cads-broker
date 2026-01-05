# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import io
from typing import Optional

from pydantic import model_validator
from redis_om import Field, JsonModel, Migrator, get_redis_connection

from cads_broker import config
from cads_broker.expressions import RulesParser

CONFIG = config.BrokerConfig()

REDIS_DB = get_redis_connection(decode_responses=True)

class Context:
    def __init__(self, request, environment):
        self.request = request
        self.environment = environment


def get_uid(*args):
    return str(hash(" ".join(map(str, args))))


class QoSRule(JsonModel):
    """
    It represents an QoS rule.

    Rules have two parts:
    the 'condition' and the 'conclusion', which are both Expressions. The
    'condition' is used to match request, while the 'conclusion' is used
    to perform an action with the matching request. For example, in the
    case of a 'Limit', the conclusion represents the maximum value
    (self,capacity) of that limit. For 'Permission', the conclusion must
    evaluate to a  that states if the request is accepted or
    denied. For 'Priority', the conclusion must evaluate to a starting
    priority for the request.

    The 'info' is currently simply a string that explains the rule. It is
    an expression, so it can be evaluated dynamically later.
    """

    info: str = Field(index=False)
    name: str = Field(index=True)
    conclusion_str: str = Field(index=False)
    condition_str: str = Field(index=False)
    uid: Optional[str] = Field(default=None, primary_key=True)

    def __post_init__(self, redis_client):
        self.redis_client = redis_client

    @model_validator(mode='after')
    def compute_uid(self) -> 'QoSRule':
        if self.uid is None:
            self.uid = get_uid(
                self.info,
                self.name,
                self.condition_str,
                self.conclusion_str,
            )
        return self

    @property
    def condition(self):
        c = io.StringIO(self.condition_str)
        parser = RulesParser.RulesParser(c, logger=None)
        return parser.parse_expression()

    @property
    def conclusion(self):
        c = io.StringIO(self.conclusion_str)
        parser = RulesParser.RulesParser(c, logger=None)
        return parser.parse_expression()

    def evaluate(self, request):
        return self.conclusion.evaluate(Context(request))

    def match(self, request):
        try:
            ret_value = self.condition.evaluate(Context(request))
        except Exception as e:
            print(
                f"Error evaluating condition {self.condition} for request {request.request_uid}"
            )
            print(e)
            return False
        return ret_value

    def dump(self, out):
        out(self)

    def __repr__(self):
        return f"{self.name} {self.info} {self.condition} : {self.conclusion}"


class Priority(QoSRule):
    """
    It represents a priority rule.

    The 'conclusion' part must evaluate as a number,
    which is then used to compute the starting
    priority of a request. All rules matching a request contributes to
    the starting priority of the request. The priority is a number that
    represents a number of seconds. For example, a request with a
    starting priority of 120 will overtake all requests of priority zero
    added to the queue in the last 2 minutes. Priorities are only used to
    decide which request to run next. It does not affect already running
    requests.

    The request priority is equal to its starting priority plus its time
    in the queue. So if a request as a starting priority of zero and
    stays in the queue for 1 hour, its priority will be 3600. Another way
    of understanding this algorithm is to say that the priority of each
    queued request is increased by one every second. This will ensure
    that requests starting with a very low priority will eventually be
    scheduled, and will not always be overtaken by higher priority
    requests.
    """

    name: str = "priority"

    class Meta:
        database = REDIS_DB
        model_key_prefix = "priority"
        global_key_prefix = "broker"


class DynamicPriority(QoSRule):
    """
    It represents a priority rule.

    The 'conclusion' part must evaluate as a number,
    which is then used to compute the dynamic priority
    of a request. All rules matching a request contributes to
    the dynamic priority of the request. The priority is a number that
    represents a number of seconds. Priorities are only used to
    decide which request to run next. It does not affect already running
    requests.
    """

    name: str = "dynamic_priority"

    class Meta:
        database = REDIS_DB
        model_key_prefix = "dynamic_priority"
        global_key_prefix = "broker"


class Permission(QoSRule):
    """
    It implements the permission rule.

    Its 'conclusion' must evaluate to a boolean.
    If the evaluation returns True, the matching
    requests are granted execution, otherwise they are denied execution
    and are immediately set to aborted.
    """

    name: str = "permission"

    class Meta:
        database = REDIS_DB
        model_key_prefix = "permission"
        global_key_prefix = "broker"


class Limit(QoSRule):
    """
    It implements a limit in the QoS system.

    Its 'conclusion' must evaluate to a positive integer,
    which represent the 'capacity' of the limit, i.e. the number
    of requests that this rule will allow to execute simultaneously.

    A limit holds a counter that is incremented each time a request
    matching the 'condition' part of the rule is started, and decremented
    when the request finishes. If the counter value reaches the maximum
    capacity of the limit, no requests matching that limit can run.
    """

    queued: set[str] = Field(default_factory=set, index=True)
    running: set[str] = Field(default_factory=set)

    def increment(self, request_uid):
        self.remove_from_queue(request_uid)
        self.running.add(request_uid)
        self.save()

    def remove_from_queue(self, request_uid):
        if request_uid in self.queued:
            self.queued.remove(request_uid)
        self.save()

    def decrement(self, request_uid):
        if request_uid in self.running:
            self.running.remove(request_uid)
        self.save()

    def queue(self, request_uid):
        self.queued.add(request_uid)
        self.save()

    def capacity(self, request):
        return self.evaluate(request)

    @property
    def value(self):
        return len(self.running)

    def full(self, request):
        # NOTE: the self.value can be greater than the limit capacity after a
        # reconfiguration of the QoS
        return self.value >= self.capacity(request)


class GlobalLimit(Limit):
    """
    It is a subclass of 'Limit' and exists for the sake of strong typing.

    Global limits are shared by all users.
    """

    name: str = "limit"

    class Meta:
        database = REDIS_DB
        model_key_prefix = "limit"
        global_key_prefix = "broker"


class UserLimit(Limit):
    name: str = "user"

    def clone(self):
        return UserLimit(
            info=self.info,
            condition_str=self.condition_str,
            conclusion_str=self.conclusion_str,
        )

    class Meta:
        database = REDIS_DB
        model_key_prefix = "user"
        global_key_prefix = "broker"


class RuleSet:
    """It is used to store all rules in a single neat an tidy location."""

    def __init__(self, redis_client=REDIS_DB):
        self.redis_client = redis_client

    def migrate(self):
        """Create Redis Search indexes for all QoS models."""
        Migrator().run()

    def reset(self):
        Priority.find().delete()
        DynamicPriority.find().delete()
        GlobalLimit.find().delete()
        Permission.find().delete()
        UserLimit.find().delete()

    @property
    def priorities(self):
        return Priority.find().all()

    @property
    def dynamic_priorities(self):
        return DynamicPriority.find().all()

    @property
    def global_limits(self):
        return GlobalLimit.find().all()

    @property
    def permissions(self):
        try:
            return Permission.find().all()
        except Exception:  # FIXME: specify exception ResponseError --- IGNORE ---
            return []

    @property
    def user_limits(self):
        return UserLimit.find().all()

    def add_priority(self, environment, info, condition, conclusion):
        Priority(
            info=str(info),
            condition_str=str(condition),
            conclusion_str=str(conclusion),
        ).save()

    def add_dynamic_priority(self, environment, info, condition, conclusion):
        DynamicPriority(
            info=str(info),
            condition_str=str(condition),
            conclusion_str=str(conclusion),
        ).save()

    def add_permission(self, environment, info, condition, conclusion):
        Permission(
            info=str(info),
            condition_str=str(condition),
            conclusion_str=str(conclusion),
        ).save()

    def add_user_limit(self, environment, info, condition, conclusion):
        UserLimit(
            info=str(info),
            condition_str=str(condition),
            conclusion_str=str(conclusion),
            queued=set(),
            running=set(),
        ).save()

    def add_global_limit(self, environment, info, condition, conclusion):
        limit = GlobalLimit(
            info=str(info),
            condition_str=str(condition),
            conclusion_str=str(conclusion),
            queued=set(),
            running=set(),
        )
        print("Adding global limit:", limit)
        limit.save()

    def dump(self, out=print):
        out()
        out("# Permissions:")
        out()
        for p in self.permissions:
            p.dump(out)

        out()
        out("# Global limits:")
        out()
        for p in self.global_limits:
            p.dump(out)

        out()
        out("# Per user limits:")
        out(self)
        for p in self.user_limits:
            p.dump(out)

        out()
        out("# Priorities:")
        out()
        for p in self.priorities:
            p.dump(out)

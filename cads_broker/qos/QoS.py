# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import threading
from functools import wraps

from .. import database

from ..expressions.RulesParser import RulesParser
from .Properties import Properties
from .Rule import Context, RuleSet


def locked(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)

    return wrapped


def mul(data):
    result = 1
    for i in data:
        result *= i
    return result


class QoS:
    def __init__(self, rules, environment):
        self.lock = threading.RLock()

        self.environment = environment
        # The list of active requests

        # Cache associating Request and their Properties
        self.requests_properties_cache = dict()

        # Mapping between user names and corresponding per-user limit
        self.per_user_limits = dict()

        if isinstance(rules, RuleSet):
            self.path = None
            self.rules = rules
        else:
            self.path = rules
            self.rules = None
            # Read the files from the rules file
            self.read_rules()

    @locked
    def read_rules(self):
        """Reads the rule files and populate the rule_set"""

        # Create a parser to parse the rules file
        parser = RulesParser(self.path)

        # The rules will be stored in self.rules
        self.rules = RuleSet()

        # Parse the rules
        parser.parse_rules(self.rules, self.environment)

        # Print the rules
        self.rules.dump()

    @locked
    def reload_rules(self, session):
        """This methods allow a 'hot' reloading of the rules. For example, a thread
        could be monitoring the time stamp of the rules file and call this method
        """
        self.read_rules()
        self.reconfigure(session=session)

    @locked
    def reconfigure(self, session):
        """Reset the status of the QoS. This method must be called if the rule_set is
        changed.
        """

        # Reset per-user limits
        self.per_user_limits.clear()

        # Invalidate all caches, so the  rules will be applied
        self.requests_properties_cache.clear()

        # Re-register the active tasks
        for request in database.get_running_requests(session=session):
            # Recompute the limits
            for limit in self.limits_for(request, session):
                limit.increment()

    @locked
    def can_run(self, request, session):
        """Checks if a request can run"""
        properties = self._properties(request=request, session=session)
        limits_constraint = not any(limit.full(request) for limit in properties.limits)
        permissions_contraint = mul([
            permission.evaluate(request) for permission in properties.permissions
        ])
        return limits_constraint and permissions_contraint

    @locked
    def _properties(self, request, session):
        """Returns the Properties object associated with a request. If it does not
        exists it is created. The property object caches the rules matching the
        request. The method also checks permission and establish starting
        priority.
        """
        properties = self.requests_properties_cache.get(request.request_uid)
        if properties is not None:
            return properties

        properties = Properties()

        # First check permissions
        for rule in self.rules.permissions:
            if rule.match(request):
                properties.permissions.append(rule)
                if not rule.evaluate(request):
                    database.set_request_status(
                        request_uid=request.request_uid,
                        status="failed",
                        session=session,
                        traceback=rule.info.evaluate(
                            Context(request, self.environment)
                        ),
                    )
                    break

        # Add general limits
        for rule in self.rules.global_limits:
            if rule.match(request):
                properties.limits.append(rule)

        # Add per-user limits
        limit = self.user_limit(request)
        if limit is not None:
            properties.limits.append(limit)

        # Add priorities and compute starting priority
        priority = 0
        for rule in self.rules.priorities:
            if rule.match(request):
                properties.priorities.append(rule)
                priority += rule.evaluate(request)

        # Set starting priority
        properties.starting_priority = priority

        # Store in cache
        self.requests_properties_cache[request.request_uid] = properties

        return properties

    @locked
    def priority(self, request, session):
        """Computes the priority of a request"""
        # The priority of a request increases with time
        return self._properties(request, session).starting_priority + request.age

    def dump(self, out=print):
        self.rules.dump(out)

    @locked
    def status(self, requests, session, out=print):
        out()
        out("===================================================================")
        out("REQUESTS")
        out("===================================================================")

        for request in requests:
            self._status(request, session, out)

        out()
        out("===================================================================")

    def _status(self, request, session, out):

        out()
        out("===================================================================")
        out("QoS info for:")
        out(request, request.status)
        out("Priority: {}".format(self.priority(request, session)))
        out("Limits rules:")
        for limit in self.limits_for(request, session):
            out(
                "    {} ({}/{}) {}".format(
                    limit,
                    limit.value,
                    limit.capacity(request),
                    "** FULL **" if limit.full(request) else "-",
                )
            )

        out("Priorities rules:")
        for priority in self.priorities_for(request, session):
            out("    {}".format(priority))

        out("Permissions rules:")
        for permission in self.permissions_for(request, session):
            out("    {}".format(permission))

    @locked
    def limits_for(self, request, session):
        """Returns the limit rules that applies to a request. Ensure that the
        properties cache is created if needed."""
        return self._properties(request, session).limits

    @locked
    def permissions_for(self, request, session):
        """Returns the permission rules that applies to a request. Ensure that the
        properties cache is created if needed."""
        return self._properties(request, session).permissions

    @locked
    def priorities_for(self, request, session):
        """Returns the priority rules that applies to a request. Ensure that the
        properties cache is created if needed."""
        return self._properties(request, session).priorities

    @locked
    def user_limit(self, request):
        """Returns the per-user limit for the user associated with the request"""
        user = request.user_uid

        limit = self.per_user_limits.get(user)
        if limit is not None:
            print(user, limit)
            return limit

        for limit in self.rules.user_limits:
            if limit.match(request):
                """
                We clone the rule because we need one instance per different
                user otherwise all users will share that limit
                """
                limit = limit.clone()
                self.per_user_limits[user] = limit
                return limit
        return None
        # raise Exception(f"Not rules matching user '{user}'")

    @locked
    def pick(self, queue, session):

        # Create the list of requests than can run
        candidates = [r for r in queue if self.can_run(r, session)]

        # If no request can run, return 'None'
        if len(candidates) == 0:
            return None

        # Sort according to priorities, highest first
        candidates = sorted(
            candidates,
            key=lambda r: self.priority(r, session),
            reverse=True,
        )

        # Select the request with the highest priority
        request = candidates[0]

        return request

    @locked
    def notify_start_of_request(self, request, session):
        """Increments the limits matching that request so that other request
        sharing the same limits may be kept in the queue if a limit reaches
        its capacity
        """
        for limit in self.limits_for(request, session):
            limit.increment()

        # Keep track of the running request. This is needed by reconfigure(self)

    @locked
    def notify_end_of_request(self, request, session):
        """Decrements the limits matching that request so that other request
        sharing the same limits can run
        """
        for limit in self.limits_for(request, session):
            limit.decrement()

        # Remove requests all collections
        self.requests_properties_cache.pop(request.request_uid)

from cads_broker import database, expressions


def tagged(context, value):
    if value in context.request.request_metadata.get("qos_tags", []):
        return True


def contains_all(request_values, values):
    if not isinstance(request_values, (list, tuple)):
        request_values = [request_values]
    s1 = set(request_values)
    s2 = set(values)
    return len(s1 & s2) == len(s2)


def contains_any(request_values, values):
    if not isinstance(request_values, (list, tuple)):
        request_values = [request_values]
    s1 = set(request_values)
    s2 = set(values)
    return len(s1 & s2) > 0


def request_contains_all(context, key, values):
    request_values = context.request.request_body.get("request").get(key)
    return contains_all(request_values, values)


def request_contains_any(context, column, key, values):
    request_values = context.request.request_body.get("request").get(key)
    return contains_any(request_values, values)


def metadata_contains_all(context, key, values):
    metadata_values = context.request.request_metadata.get(key)
    return contains_all(metadata_values, values)


def metadata_contains_any(context, key, values):
    metadata_values = context.request.request_metadata.get(key)
    return contains_any(metadata_values, values)


def register_functions():
    expressions.FunctionFactory.FunctionFactory.register_function(
        "dataset",
        lambda context, *args: context.request.process_id,
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "adaptor",
        lambda context, *args: context.request.entry_point,
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "origin",
        lambda context, *args: context.request.origin,
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "portal",
        lambda context, *args: context.request.portal,
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "priority_from_cost",
        lambda context,
        cost_key,
        scale_factor=1,
        *args: context.request.request_metadata.get("costs", {}).get(cost_key, 0)
        * scale_factor,
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "user_finished_request_count",
        lambda context, seconds: database.count_finished_requests_per_user(
            user_uid=context.request.user_uid,
            seconds=seconds,
            session=context.environment.session,
        ),
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "user_request_count",
        lambda context, status: database.cache_count_requests(
            user_uid=context.request.user_uid,
            status=status,
            request_uid=context.request.request_uid,
            session=context.environment.session,
        ),
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "user_resource_used",
        lambda context, interval=24 * 60 * 60, origin=None: database.user_resource_used(
            user_uid=context.request.user_uid,
            interval=interval,
            origin=origin,
            session=context.environment.session,
        ),
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "user_last_completed_request",
        lambda context, interval=24 * 60 * 60: database.user_last_completed_request(
            user_uid=context.request.user_uid,
            interval=interval,
            session=context.environment.session,
        ),
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_age", lambda context: context.request.age
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "user_data",
        lambda context: context.request.request_metadata.get("user_data", {}),
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "get", lambda context, object, key, default=None: object.get(key, default)
    )

    expressions.FunctionFactory.FunctionFactory.register_function("tagged", tagged)
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_contains_all", request_contains_all
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_contains_any", request_contains_any
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "metadata_contains_all", metadata_contains_all
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "metadata_contains_any", metadata_contains_any
    )

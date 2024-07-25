from cads_broker import database, expressions


def tagged(context, value):
    if value in context.request.request_metadata.get("qos_tags"):
        return True


def request_contains_all(context, key, values):
    request_values = context.request.request_body.get("request").get(key)
    if not isinstance(request_values, (list, tuple)):
        request_values = [request_values]
    s1 = set(request_values)
    s2 = set(values)
    return len(s1 & s2) == len(s2)


def request_contains_any(context, key, values):
    request_values = context.request.request_body.get("request").get(key)
    if not isinstance(request_values, (list, tuple)):
        request_values = [request_values]
    s1 = set(request_values)
    s2 = set(values)
    return len(s1 & s2) > 0


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
        lambda context,
        status,
        portal=None,
        process_id=None,
        entry_point=None: database.count_requests(
            user_uid=context.request.user_uid,
            status=status,
            process_id=process_id,
            entry_point=entry_point,
            portal=portal,
            session=context.environment.session,
        ),
    )
    expressions.FunctionFactory.FunctionFactory.register_function("tagged", tagged)
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_contains_all", request_contains_all
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_contains_any", request_contains_any
    )

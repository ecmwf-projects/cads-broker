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
        "user_request_count",
        lambda context, seconds: database.count_finished_requests_per_user(
            user_uid=context.request.user_uid,
            seconds=seconds,
        ),
    )
    expressions.FunctionFactory.FunctionFactory.register_function("tagged", tagged)
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_contains_all", request_contains_all
    )
    expressions.FunctionFactory.FunctionFactory.register_function(
        "request_contains_any", request_contains_any
    )

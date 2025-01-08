import io
import logging

from cads_broker import Environment, database, factory
from cads_broker.expressions import FunctionFactory
from cads_broker.expressions.RulesParser import RulesParser
from cads_broker.qos.Rule import RuleSet

FunctionFactory.FunctionFactory.register_function(
    "dataset",
    lambda context, *args: context.request.dataset,
)
FunctionFactory.FunctionFactory.register_function(
    "adaptor",
    lambda context, *args: context.request.adaptor,
)
factory.register_functions()
logger = logging.getLogger("test")


def mock_system_request(
    user_uid: str, request_metadata: dict = {}, process_id: str = "era5"
) -> database.SystemRequest:
    system_request = database.SystemRequest(
        user_uid=user_uid, request_metadata=request_metadata, process_id=process_id
    )
    return system_request


class TestRequest:
    user_uid = "david"
    dataset = "dataset-1"
    adaptor = "adaptor1"
    cost = (1024 * 1024, 60 * 60 * 24)


request = TestRequest()


environment = Environment.Environment()
environment.disable_resource("adaptor2")


def compile(text):
    parser = RulesParser(io.StringIO(text), logger=logger)
    rules = RuleSet()
    parser.parse_rules(rules, environment)
    return rules


def test_rules():
    rules = compile(
        """
    user "Limit for david"       (user == "david") : 5
    """
    )
    assert len(rules.user_limits) == 1
    assert rules.user_limits[0].match(request)


def test_definition():
    rules = compile(
        """
    define user = "david"

    limit "Limit for user" user == user: 5
    """
    )
    assert len(rules.global_limits) == 1
    request = mock_system_request(user_uid="david")
    assert rules.global_limits[0].capacity(request) == 5
    assert rules.global_limits[0].match(request)


def test_contains():
    rules = compile(
        """
    define users = ["alice", "bob"]

    limit "Limit for Alice and Bob" contains(users, user): 5
    limit "Limit for era5" (dataset() == "era5") && !(user in users): 10
    limit "Limit for alice" "lice" in user: 1
    """
    )
    assert len(rules.global_limits) == 3
    request_david = mock_system_request(user_uid="david", process_id="era5")
    request_bob = mock_system_request(user_uid="bob", process_id="era5")
    request_alice = mock_system_request(user_uid="alice", process_id="era5")
    assert not rules.global_limits[0].match(request_david)
    assert rules.global_limits[0].match(request_bob)
    assert rules.global_limits[0].match(request_alice)

    assert not rules.global_limits[1].match(request_alice)
    assert not rules.global_limits[1].match(request_bob)
    assert rules.global_limits[1].match(request_david)

    assert rules.global_limits[2].match(request_alice)
    assert not rules.global_limits[2].match(request_bob)
    assert not rules.global_limits[2].match(request_david)


def test_dynamic_prorities():
    request_alice = mock_system_request(user_uid="alice")
    request_bob = mock_system_request(user_uid="bob")
    request_david = mock_system_request(user_uid="david")

    rules = compile(
        """
    dynamic_priority "Priority for Alice" user == "alice": 5
    """
    )
    assert len(rules.dynamic_priorities) == 1
    assert rules.dynamic_priorities[0].match(request_alice)
    assert rules.dynamic_priorities[0].evaluate(request_alice) == 5
    assert not rules.dynamic_priorities[0].match(request_bob)

    rules = compile(
        """
    define users = ["alice", "bob"]

    dynamic_priority "Priority for Alice and Bob" user in users: 5
    dynamic_priority "Priority for David" user == "david": 10
    """
    )
    assert len(rules.dynamic_priorities) == 2
    assert rules.dynamic_priorities[0].match(request_alice)
    assert rules.dynamic_priorities[0].match(request_bob)

    assert not rules.dynamic_priorities[1].match(request_alice)
    assert not rules.dynamic_priorities[1].match(request_bob)

    assert rules.dynamic_priorities[0].evaluate(request_alice) == 5
    assert rules.dynamic_priorities[0].evaluate(request_bob) == 5

    assert not rules.dynamic_priorities[0].match(request_david)
    assert rules.dynamic_priorities[1].match(request_david)
    assert rules.dynamic_priorities[1].evaluate(request_david) == 10


def test_user_data():
    request_alice = mock_system_request(
        user_uid="alice", request_metadata={"user_data": {"email": "alice@ecmwf.int"}}
    )

    rules = compile(
        """
    user "Limit for Alice" ("alice" in get(user_data, "email")): 0
    """
    )
    assert len(rules.user_limits) == 1
    assert rules.user_limits[0].match(request_alice)

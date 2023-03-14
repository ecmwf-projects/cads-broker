import io

from cads_broker import Environment
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


class TestRequest:

    user_uid = "david"
    dataset = "dataset-1"
    adaptor = "adaptor1"
    cost = (1024 * 1024, 60 * 60 * 24)


request = TestRequest()


environment = Environment.Environment(number_of_workers=1)
environment.disable_resource("adaptor2")


def compile(text):
    parser = RulesParser(io.StringIO(text))
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

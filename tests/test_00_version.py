import cads_broker


def test_version() -> None:
    assert cads_broker.__version__ != "999"

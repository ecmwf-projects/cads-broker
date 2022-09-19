from typing import Callable

from prometheus_client import Gauge, Info

from cads_broker import database


def get_broker_queue() -> Callable[[Info], None]:
    GAUGE = Gauge("broker_queue", "Number of accepted requests", labelnames=("queue",))

    def instrumentation(info: Info) -> None:
        GAUGE.labels("queue").set(len(database.get_accepted_requests()))

    return instrumentation

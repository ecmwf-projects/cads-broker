import os
from urllib.error import URLError

import structlog
from prometheus_client import REGISTRY, Counter, push_to_gateway

from cads_broker import config

config.configure_logger()
logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


GENERATED_BYTES_COUNTER = Counter(
    "generated_bytes_counter", "Total bytes generated in successful requests"
)


def push_to_prometheus():
    exporter_host = os.environ.get("EXPORTER_HOST")
    if exporter_host is not None:
        push_to_gateway(
            exporter_host, job="cads-broker-metrics-job", registry=REGISTRY
        )
    else:
        print("asd")


def increase_bytes_counter(file_size):
    GENERATED_BYTES_COUNTER.inc(file_size)
    push_to_prometheus()

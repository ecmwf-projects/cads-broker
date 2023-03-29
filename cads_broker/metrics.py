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

exporter_host = os.environ.get("EXPORTER_HOST")


def push_to_prometheus():
    exporter_host = os.environ.get("EXPORTER_HOST")
    if exporter_host is not None:
        try:
            push_to_gateway(
                exporter_host, job="cads-broker-metrics-job", registry=REGISTRY
            )
        except URLError:
            logger.warning("Connection to Prometheus host refused.")
        except Exception:
            logger.error("Unexpected error")


def increase_bytes_counter(result):
    file_size = result["args"][0]["file:size"]
    GENERATED_BYTES_COUNTER.inc(file_size)
    push_to_prometheus()

import threading
import time
from typing import Iterable

import distributed
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class Schedulers:
    def __init__(self):
        self.clients = {}
        self.lock = threading.Lock()

    def add_client(self, client_id: str, client: distributed.Client) -> None:
        with self.lock:
            self.clients[client_id] = client

    def pop_client(self, client_id: str) -> distributed.Client | None:
        with self.lock:
            return self.clients.pop(client_id, None)

    def get_client(self, client_id: str) -> distributed.Client | None:
        with self.lock:
            return self.clients.get(client_id)

    def get_clients_list(self) -> Iterable[distributed.Client]:
        with self.lock:
            return self.clients.values()

    def get_clients_addresses(self) -> Iterable[str]:
        with self.lock:
            return self.clients.keys()

    def get_clients_items(self):
        with self.lock:
            return self.clients.items()


def clean_scheduler_memory(client: distributed.Client):
    """
    Safely flushes Dask's internal BatchedSend network logs.

    This prevents memory leaks from massive task payloads during high-throughput load tests.
    """

    def flush_network_logs(dask_scheduler):
        import gc

        from distributed.batched import BatchedSend

        # Find all active and dead network buffers
        for obj in gc.get_objects():
            if isinstance(obj, BatchedSend) and hasattr(obj, "recent_message_log"):
                # Empty the log to sever the task references
                obj.recent_message_log.clear()

    client.run_on_scheduler(flush_network_logs)


def clean_scheduler_memory_for_all_clients(
    schedulers: Schedulers, timeout_seconds: int = 300
):
    logger.info(
        "Starting periodic scheduler memory cleanup thread",
        timeout_seconds=timeout_seconds,
    )
    while True:
        time.sleep(timeout_seconds)
        for client_address, client in schedulers.get_clients_items():
            logger.info(
                "Cleaning scheduler memory for client",
                client_address=client_address,
            )
            clean_scheduler_memory(client)
            logger.info(
                "Finished cleaning scheduler memory for client",
                client_address=client_address,
            )

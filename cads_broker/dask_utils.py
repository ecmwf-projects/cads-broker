import threading
from typing import Iterable

import distributed


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

    def get_client_addresses(self) -> Iterable[str]:
        with self.lock:
            return self.clients.keys()


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


def clean_scheduler_memory_for_all_clients(schedulers: Schedulers):
    for client in schedulers.get_clients_list():
        clean_scheduler_memory(client)

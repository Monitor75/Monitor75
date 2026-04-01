import etcd3
import logging
import random
from typing import List, Tuple

logger = logging.getLogger(__name__)

# All three etcd cluster endpoints. Python's etcd3 library connects to one
# at a time; we store the list and reconnect to the next if one fails.
ETCD_ENDPOINTS: List[Tuple[str, int]] = [
    ("127.0.0.1", 2379),
    ("127.0.0.1", 2381),
    ("127.0.0.1", 2383),
]


class EtcdDiscovery:
    def __init__(self, endpoints: List[Tuple[str, int]] = None):
        self.endpoints = endpoints or ETCD_ENDPOINTS
        self.client = self._connect()

    def _connect(self) -> etcd3.Etcd3Client:
        """Try each endpoint in order, return first that responds."""
        for host, port in self.endpoints:
            try:
                c = etcd3.client(host=host, port=port)
                c.status()
                logger.debug("Connected to etcd at %s:%d", host, port)
                return c
            except Exception:
                logger.debug("etcd %s:%d unreachable, trying next", host, port)
        # Fall back to first endpoint without health check
        host, port = self.endpoints[0]
        logger.warning("No etcd endpoint healthy; falling back to %s:%d", host, port)
        return etcd3.client(host=host, port=port)

    def _exec(self, fn):
        """Run fn(); on failure reconnect and retry once."""
        try:
            return fn()
        except Exception:
            self.client = self._connect()
            return fn()

    def get_metadata_server(self) -> str:
        """Return the address of a random healthy metadata server."""
        servers = self._exec(
            lambda: [v.decode() for v, _ in self.client.get_prefix("/services/metadata/")]
        )
        if not servers:
            raise RuntimeError("No metadata servers found in etcd")
        return random.choice(servers)

    async def get_metadata_servers(self) -> List[str]:
        """Return all active metadata server addresses (async-friendly)."""
        results = self._exec(
            lambda: list(self.client.get_prefix("/services/metadata/"))
        )
        return [val.decode() for val, _ in results]

    def get_storage_nodes(self) -> List[str]:
        """Return all registered storage node addresses."""
        nodes = self._exec(
            lambda: list(self.client.get_prefix("/services/storage/"))
        )
        return [v.decode() for v, _ in nodes]

    def watch_storage_nodes(self, on_add, on_remove):
        """Watch for storage node join/leave events."""
        events_iterator, cancel = self.client.watch_prefix("/services/storage/")
        for event in events_iterator:
            addr = event.value.decode() if hasattr(event, "value") else ""
            if isinstance(event, etcd3.events.PutEvent):
                on_add(addr)
            elif isinstance(event, etcd3.events.DeleteEvent):
                on_remove(addr)

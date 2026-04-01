"""
gRPC connection pool.

Maintains one persistent gRPC channel per backend address and
round-robins across them so no single connection becomes a bottleneck.
All channels use keepalive pings so idle connections stay warm and TCP
re-establishment latency never hits a request.
"""
import grpc
import threading
from typing import Dict, List


_KEEPALIVE_OPTIONS = [
    # Send a keepalive ping every 5 minutes on idle channels.
    # grpc-java refuses pings more frequent than its server-side minimum
    # (default 5 min) and responds with ENHANCE_YOUR_CALM / GOAWAY when
    # that minimum is violated.  300 s matches the Java default exactly,
    # eliminating the "too_many_pings" warnings in the application log.
    ("grpc.keepalive_time_ms",              300_000),
    ("grpc.keepalive_timeout_ms",            10_000),
    ("grpc.keepalive_permit_without_calls",       1),
    ("grpc.http2.min_time_between_pings_ms", 300_000),
    ("grpc.http2.max_pings_without_data",         0),
    ("grpc.max_reconnect_backoff_ms",         5_000),
    # Remove the default 4 MB gRPC message cap so large files go through
    ("grpc.max_send_message_length",             -1),
    ("grpc.max_receive_message_length",          -1),
]


class GrpcChannelPool:
    """Thread-safe pool of persistent gRPC channels with round-robin dispatch."""

    def __init__(self, addresses: List[str], extra_options=None):
        self._options = _KEEPALIVE_OPTIONS + (extra_options or [])
        self._channels: Dict[str, grpc.Channel] = {}
        self._lock = threading.Lock()
        self._addresses = list(addresses)
        self._idx = 0
        for addr in self._addresses:
            self._ensure(addr)

    def _ensure(self, addr: str) -> grpc.Channel:
        if addr not in self._channels:
            self._channels[addr] = grpc.insecure_channel(addr, options=self._options)
        return self._channels[addr]

    def next_channel(self) -> grpc.Channel:
        """Return the next channel in round-robin order."""
        with self._lock:
            addr = self._addresses[self._idx % len(self._addresses)]
            self._idx += 1
            return self._ensure(addr)

    def channel_for(self, addr: str) -> grpc.Channel:
        """Return the persistent channel for a specific address."""
        with self._lock:
            return self._ensure(addr)

    def close(self):
        with self._lock:
            for ch in self._channels.values():
                ch.close()
            self._channels.clear()

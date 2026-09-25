"""Consistent hash ring with virtual nodes.

The original (2011) version was adapted from http://amix.dk/blog/post/19367.
This one keeps the same idea but uses ``bisect`` for lookups and always
returns the right number of distinct replica nodes.
"""

from __future__ import annotations

import bisect
import hashlib
import threading
from typing import Iterable


def ring_position(key: str) -> int:
    """Place ``key`` on the ring (128-bit MD5, not used for security)."""
    return int.from_bytes(
        hashlib.md5(key.encode(), usedforsecurity=False).digest(), "big"
    )


class HashRing:
    """Maps keys to an ordered list of nodes.

    ``replicas`` is how many distinct nodes hold each key.
    ``vnodes`` is how many points each node gets on the ring; more points
    means a more even spread of keys.
    """

    def __init__(self, nodes: Iterable[str] = (), replicas: int = 3, vnodes: int = 64):
        if replicas < 1:
            raise ValueError("replicas must be at least 1")
        self.replicas = replicas
        self.vnodes = vnodes
        self._lock = threading.RLock()
        self._points: list[int] = []
        self._owners: dict[int, str] = {}
        self._nodes: set[str] = set()
        for node in nodes:
            self.add_node(node)

    def __contains__(self, node: str) -> bool:
        return node in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    def add_node(self, node: str) -> None:
        with self._lock:
            if node in self._nodes:
                return
            self._nodes.add(node)
            for i in range(self.vnodes):
                point = ring_position(f"{node}#{i}")
                self._owners[point] = node
                bisect.insort(self._points, point)

    def remove_node(self, node: str) -> None:
        with self._lock:
            if node not in self._nodes:
                return
            self._nodes.discard(node)
            self._points = [p for p in self._points if self._owners[p] != node]
            self._owners = {p: self._owners[p] for p in self._points}

    def nodes(self) -> list[str]:
        with self._lock:
            return sorted(self._nodes)

    def replicas_for(self, key: str) -> list[str]:
        """The distinct nodes responsible for ``key``, primary first."""
        with self._lock:
            if not self._points:
                return []
            wanted = min(self.replicas, len(self._nodes))
            start = bisect.bisect_left(self._points, ring_position(key))
            result: list[str] = []
            for i in range(len(self._points)):
                node = self._owners[self._points[(start + i) % len(self._points)]]
                if node not in result:
                    result.append(node)
                    if len(result) == wanted:
                        break
            return result

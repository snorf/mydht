"""Talking to other nodes over plain HTTP.

Nodes are addressed as ``"host:port"`` strings. Every call returns ``None``
when the peer cannot be reached instead of raising, since an unreachable
peer is an expected state in a DHT.
"""

from __future__ import annotations

import http.client
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, TypeVar
from urllib.parse import quote

from .store import Entry

log = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

TIMESTAMP = "X-DHT-Timestamp"
DELETED = "X-DHT-Deleted"


@dataclass
class Response:
    status: int
    headers: http.client.HTTPMessage
    body: bytes

    def json(self):
        return json.loads(self.body)


def key_path(key: str) -> str:
    return quote(key, safe="")


def request(
    node: str,
    method: str,
    path: str,
    body: bytes = b"",
    headers: Optional[dict] = None,
    timeout: float = 5.0,
) -> Optional[Response]:
    host, _, port = node.rpartition(":")
    conn = http.client.HTTPConnection(host, int(port), timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        return Response(resp.status, resp.headers, resp.read())
    except OSError as e:
        log.warning("%s %s%s failed: %s", method, node, path, e)
        return None
    finally:
        conn.close()


def parallel(fn: Callable[[T], R], items: Iterable[T]) -> list[tuple[T, R]]:
    """Run ``fn`` on every item concurrently and return ``(item, result)`` pairs."""
    items = list(items)
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=len(items)) as pool:
        return list(zip(items, pool.map(fn, items)))


def entry_headers(entry: Entry) -> dict:
    headers = {TIMESTAMP: str(entry.timestamp)}
    if entry.deleted:
        headers[DELETED] = "1"
    elif entry.content_type:
        headers["Content-Type"] = entry.content_type
    return headers


def entry_from_response(resp: Response) -> Entry:
    deleted = resp.headers.get(DELETED) == "1"
    return Entry(
        timestamp=int(resp.headers[TIMESTAMP]),
        value=None if deleted else resp.body,
        content_type=None if deleted else resp.headers.get("Content-Type"),
    )


def entry_from_digest(item: list) -> Entry:
    """A value-less stand-in for comparing versions from a digest."""
    timestamp, deleted = item
    return Entry(timestamp=timestamp, value=None if deleted else b"")

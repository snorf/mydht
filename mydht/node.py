"""A MyDHT node: an HTTP server that owns part of the key space.

Public API (curl-friendly):

    GET    /                 HTML status page
    GET    /ring             ring membership as JSON
    GET    /keys             entries stored on this node as JSON
    PUT    /keys/{key}       store the request body under key
    GET    /keys/{key}       fetch a value (HEAD works too)
    DELETE /keys/{key}       delete a key
    GET    /whereis/{key}    which nodes should hold key
    POST   /balance          run anti-entropy on every node
    POST   /purge            drop keys this node no longer owns
    DELETE /ring/{node}      remove a crashed node from the ring

Any node can take any request; it coordinates with the replicas for the key.
Nodes talk to each other through the /internal/ endpoints.
"""

from __future__ import annotations

import html
import json
import logging
import mimetypes
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterable, Optional, Union
from urllib.parse import unquote, urlsplit

from . import peers
from .hashring import HashRing
from .store import Entry, now, open_store

log = logging.getLogger(__name__)


def quorum(n: int) -> int:
    """Majority of ``n`` replicas."""
    return n // 2 + 1


class Unavailable(Exception):
    """Not enough replicas answered."""

    def __init__(self, detail: dict):
        super().__init__(detail)
        self.detail = detail


class Node:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50140,
        replicas: int = 3,
        bind: Optional[str] = None,
        timeout: float = 5.0,
        data_dir: Optional[str] = None,
        rejoin_interval: float = 10.0,
    ):
        self.httpd = ThreadingHTTPServer((bind or host, port), _make_handler(self))
        self.httpd.daemon_threads = True
        self.name = f"{host}:{self.httpd.server_address[1]}"
        self.ring = HashRing([self.name], replicas=replicas)
        self.store = open_store(data_dir, self.name)
        self.timeout = timeout
        self.rejoin_interval = rejoin_interval
        self._thread: Optional[threading.Thread] = None
        self._stopping = threading.Event()

    # -- lifecycle ---------------------------------------------------------

    def start(self, join: Union[str, Iterable[str], None] = None) -> "Node":
        """Join an existing ring and start serving.

        ``join`` is one seed node or several (a list, or a comma-separated
        string); the first one that answers is used. If none answers, the
        node starts on its own and keeps retrying in the background, so the
        nodes of a cluster can be started in any order.
        """
        seeds = self._seeds(join)
        joined = self._join_any(seeds)
        self._thread = threading.Thread(
            target=self.httpd.serve_forever, name=f"mydht-{self.name}", daemon=True
        )
        self._thread.start()
        log.info("%s serving, ring: %s", self.name, " ".join(self.ring.nodes()))
        if joined:
            # Exchange data with the rest of the ring: receive the keys this
            # node now owns, and hand back anything it kept on disk.
            self._background(self.balance_cluster)
        elif seeds:
            log.warning("%s could not reach %s, running alone and retrying",
                        self.name, ", ".join(seeds))
            self._background(self._keep_trying_to_join, seeds)
        return self

    def stop(self, leave: bool = True) -> None:
        """Stop serving. With ``leave`` the node first hands its data over."""
        self._stopping.set()
        if leave:
            self.leave()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.store.close()

    def _seeds(self, join) -> list[str]:
        if not join:
            return []
        if isinstance(join, str):
            join = join.split(",")
        return [s.strip() for s in join if s.strip() and s.strip() != self.name]

    def _join_any(self, seeds: list[str]) -> bool:
        for seed in seeds:
            resp = self._call(seed, "POST", "/internal/join", self.name.encode())
            if resp is not None and resp.status == 200:
                info = resp.json()
                ring = HashRing(info["nodes"], replicas=info["replicas"])
                ring.add_node(self.name)
                self.ring = ring
                return True
        return False

    def _keep_trying_to_join(self, seeds: list[str]) -> None:
        # Stop once anyone has joined us or we have joined someone.
        while not self._stopping.wait(self.rejoin_interval) and len(self.ring) == 1:
            if self._join_any(seeds):
                log.info("%s joined ring: %s", self.name, " ".join(self.ring.nodes()))
                self.balance_cluster()

    def leave(self) -> None:
        """Leave the ring and push every key to its new replicas."""
        self.ring.remove_node(self.name)
        if not len(self.ring):
            if self.store.persistent:
                log.info("%s was the last node; data is kept on disk", self.name)
            else:
                log.warning("%s was the last node; its data is lost", self.name)
            return
        self._broadcast("DELETE", f"/internal/ring/{self.name}")
        log.info("%s leaving, handing over data: %s", self.name, self.balance())

    # -- helpers -----------------------------------------------------------

    def _call(self, node: str, method: str, path: str, body: bytes = b"", headers=None):
        return peers.request(node, method, path, body, headers, self.timeout)

    def _peers(self) -> list[str]:
        return [n for n in self.ring.nodes() if n != self.name]

    def _broadcast(self, method: str, path: str, body: bytes = b"", exclude=()) -> dict:
        targets = [n for n in self._peers() if n not in exclude]
        results = peers.parallel(lambda n: self._call(n, method, path, body), targets)
        return {n: (r.status if r else None) for n, r in results}

    @staticmethod
    def _background(fn, *args) -> None:
        threading.Thread(target=fn, args=args, daemon=True).start()

    # -- replica access (local or over HTTP) --------------------------------

    def _read_replica(self, node: str, key: str, with_value: bool):
        """Returns ``(reachable, entry)``; ``entry`` is None if the node has no version."""
        if node == self.name:
            return True, self.store.get(key)
        method = "GET" if with_value else "HEAD"
        resp = self._call(node, method, f"/internal/keys/{peers.key_path(key)}")
        if resp is None or resp.status not in (200, 404):
            return False, None
        if resp.status == 404:
            return True, None
        return True, peers.entry_from_response(resp)

    def _write_replica(self, node: str, key: str, entry: Entry) -> bool:
        if node == self.name:
            self.store.apply(key, entry)
            return True
        resp = self._call(
            node,
            "PUT",
            f"/internal/keys/{peers.key_path(key)}",
            entry.value or b"",
            peers.entry_headers(entry),
        )
        return resp is not None and resp.status == 204

    # -- client operations ---------------------------------------------------

    def write(self, key: str, entry: Entry) -> dict:
        """Send ``entry`` to every replica; succeed if a majority stored it."""
        replicas = self.ring.replicas_for(key)
        results = peers.parallel(lambda n: self._write_replica(n, key, entry), replicas)
        stored_on = [n for n, ok in results if ok]
        detail = {
            "key": key,
            "timestamp": entry.timestamp,
            "replicas": replicas,
            "stored_on": stored_on,
        }
        if len(stored_on) < quorum(len(replicas)):
            raise Unavailable(detail)
        return detail

    def read(self, key: str) -> Optional[Entry]:
        """Ask every replica for its version, return the newest (None if absent).

        Replicas that turn out to be stale are repaired in the background.
        """
        replicas = self.ring.replicas_for(key)
        results = peers.parallel(lambda n: self._read_replica(n, key, False), replicas)
        answered = [(n, e) for n, (ok, e) in results if ok]
        if len(answered) < quorum(len(replicas)):
            raise Unavailable({"key": key, "replicas": replicas,
                               "answered": [n for n, _ in answered]})

        holder, newest = None, None
        for n, e in answered:
            if e is not None and e.newer_than(newest):
                holder, newest = n, e
        if newest is None:
            return None
        if not newest.deleted:
            ok, newest = self._read_replica(holder, key, True)
            if not ok or newest is None:
                raise Unavailable({"key": key, "replicas": replicas, "holder": holder})

        stale = [n for n, e in answered if newest.newer_than(e)]
        for n in stale:
            self._background(self._write_replica, n, key, newest)
        return None if newest.deleted else newest

    # -- ring maintenance -----------------------------------------------------

    def add_joining_node(self, node: str) -> dict:
        """Called on the seed node when ``node`` wants to join."""
        self.ring.remove_node(node)  # in case it rejoins under the same name
        self._broadcast("PUT", f"/internal/ring/{node}", exclude=(node,))
        info = {"nodes": self.ring.nodes(), "replicas": self.ring.replicas}
        self.ring.add_node(node)
        return info

    def remove_dead_node(self, node: str) -> dict:
        """Drop a node that crashed without leaving, then re-replicate."""
        self.ring.remove_node(node)
        removed = self._broadcast("DELETE", f"/internal/ring/{node}")
        return {"removed": node, "notified": removed, "balance": self.balance_cluster()}

    def _digests(self) -> dict[str, Optional[dict]]:
        """Fetch ``{key: [timestamp, deleted]}`` from every peer (None if down)."""
        def fetch(n):
            resp = self._call(n, "GET", "/internal/digest")
            return resp.json() if resp is not None and resp.status == 200 else None
        return dict(peers.parallel(fetch, self._peers()))

    def balance(self) -> dict:
        """Anti-entropy for this node.

        For every key this node or a peer knows about: pull the newest
        version if this node is a replica, then push it to replicas that
        have an older one (or none).
        """
        digests = self._digests()
        reachable = {n: d for n, d in digests.items() if d is not None}
        keys = set(self.store.digest())
        for d in reachable.values():
            keys.update(d)

        pulled = pushed = 0
        for key in sorted(keys):
            replicas = self.ring.replicas_for(key)
            mine = self.store.get(key)

            if self.name in replicas:
                newest_peer, newest = None, mine
                for n, d in reachable.items():
                    if key in d and peers.entry_from_digest(d[key]).newer_than(newest):
                        newest_peer, newest = n, peers.entry_from_digest(d[key])
                if newest_peer:
                    ok, entry = self._read_replica(newest_peer, key, True)
                    if ok and entry and self.store.apply(key, entry):
                        pulled += 1
                        mine = entry

            if mine is None:
                continue
            for n in replicas:
                if n == self.name or n not in reachable:
                    continue
                theirs = reachable[n].get(key)
                if theirs is None or mine.newer_than(peers.entry_from_digest(theirs)):
                    if self._write_replica(n, key, mine):
                        pushed += 1

        return {"pulled": pulled, "pushed": pushed,
                "unreachable": sorted(n for n, d in digests.items() if d is None)}

    def balance_cluster(self) -> dict:
        def run(n):
            if n == self.name:
                return self.balance()
            resp = self._call(n, "POST", "/internal/balance")
            return resp.json() if resp is not None and resp.status == 200 else None
        return dict(peers.parallel(run, self.ring.nodes()))

    def purge(self) -> dict:
        """Forget keys this node is no longer a replica for.

        A key is only dropped once every replica that should hold it has a
        version at least as new as ours, so purge never loses data.
        """
        digests = self._digests()
        dropped, kept = [], []
        for key, version in self.store.digest().items():
            replicas = self.ring.replicas_for(key)
            if self.name in replicas:
                continue
            entry = peers.entry_from_digest(version)
            safe = all(
                digests.get(n) is not None
                and key in digests[n]
                and not entry.newer_than(peers.entry_from_digest(digests[n][key]))
                for n in replicas
            )
            if safe:
                self.store.drop(key)
                dropped.append(key)
            else:
                kept.append(key)
        return {"dropped": dropped, "kept_until_replicated": kept}

    # -- status page ------------------------------------------------------------

    def status_html(self) -> str:
        e = html.escape
        rows = []
        total = 0
        for key, timestamp, deleted, size in self.store.listing():
            total += size
            links = " ".join(
                f'<a href="http://{e(n)}/">{e(n)}</a>' for n in self.ring.replicas_for(key)
            )
            name = (f"<s>{e(key)}</s>" if deleted
                    else f'<a href="/keys/{e(peers.key_path(key))}">{e(key)}</a>')
            rows.append(
                f"<tr><td>{name}</td><td>{_human(size)}</td>"
                f"<td>{timestamp}</td><td>{links}</td></tr>"
            )
        nodes = "".join(
            f'<li><a href="http://{e(n)}/">{e(n)}</a>{" (this node)" if n == self.name else ""}</li>'
            for n in self.ring.nodes()
        )
        return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>MyDHT {e(self.name)}</title>
<style>body{{font-family:system-ui,sans-serif;margin:2rem}}
table{{border-collapse:collapse}}td,th{{border:1px solid #ccc;padding:.3rem .6rem;text-align:left}}</style>
</head><body>
<h1>MyDHT node {e(self.name)}</h1>
<p>Replicas per key: {self.ring.replicas}. Keys on this node: {len(rows)} ({_human(total)}).</p>
<h2>Ring</h2><ul>{nodes}</ul>
<h2>Keys</h2>
<table><tr><th>key</th><th>size</th><th>timestamp (ns)</th><th>replicas</th></tr>
{"".join(rows)}</table>
</body></html>
"""


def _human(size: int) -> str:
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{size} {unit}"
        size //= 1024
    return f"{size} GB"


def _make_handler(node: Node):
    class Handler(_Handler):
        pass

    Handler.node = node
    return Handler


class _Handler(BaseHTTPRequestHandler):
    node: Node
    protocol_version = "HTTP/1.1"
    server_version = "MyDHT/2.0"

    def log_message(self, fmt, *args):
        log.debug("%s %s", self.node.name, fmt % args)

    # -- plumbing ---------------------------------------------------------------

    def _path(self) -> str:
        return unquote(urlsplit(self.path).path)

    def _body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";")[0], 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    return b"".join(chunks)
                chunks.append(self.rfile.read(size))
                self.rfile.readline()
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def _send(self, status: int, body: bytes = b"", content_type="text/plain; charset=utf-8",
              headers=None) -> None:
        # One request per connection keeps things simple: a body we did not
        # read (e.g. on a 405) can never be mistaken for the next request.
        self.close_connection = True
        self.send_response(status)
        self.send_header("Connection", "close")
        if body or status != 204:
            if content_type:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status: int, data) -> None:
        self._send(status, (json.dumps(data, indent=2) + "\n").encode(), "application/json")

    def _text(self, status: int, text: str) -> None:
        self._send(status, (text + "\n").encode())

    def _dispatch(self) -> None:
        path = self._path()
        try:
            if path in self.exact_routes:
                return self.exact_routes[path](self, "")
            for prefix, handler in self.prefix_routes:
                if path.startswith(prefix):
                    return handler(self, path[len(prefix):])
            self._text(404, "not found")
        except Unavailable as e:
            self._json(503, {"error": "not enough replicas answered", **e.detail})
        except Exception:
            log.exception("%s failed to handle %s %s", self.node.name, self.command, self.path)
            self._text(500, "internal error")

    do_GET = do_HEAD = do_PUT = do_DELETE = do_POST = _dispatch

    # -- public endpoints -------------------------------------------------------

    def _root(self, _):
        if self.command not in ("GET", "HEAD"):
            return self._text(405, "method not allowed")
        self._send(200, self.node.status_html().encode(), "text/html; charset=utf-8")

    def _favicon(self, _):
        # Carried over from 2011: put a key called favicon.ico to change the icon.
        self._key("favicon.ico")

    def _ring(self, rest):
        if not rest and self.command in ("GET", "HEAD"):
            return self._json(200, {"nodes": self.node.ring.nodes(),
                                    "replicas": self.node.ring.replicas})
        if rest and self.command == "DELETE":
            return self._json(200, self.node.remove_dead_node(rest))
        self._text(405, "method not allowed")

    def _keys(self, _):
        if self.command not in ("GET", "HEAD"):
            return self._text(405, "method not allowed")
        self._json(200, {
            m.key: {"timestamp": m.timestamp, "deleted": m.deleted, "size": m.size,
                    "replicas": self.node.ring.replicas_for(m.key)}
            for m in self.node.store.listing()
        })

    def _key(self, key):
        if not key:
            return self._keys("")
        if self.command in ("GET", "HEAD"):
            entry = self.node.read(key)
            if entry is None:
                return self._text(404, f"{key} not found")
            content_type = (entry.content_type or mimetypes.guess_type(key)[0]
                            or "application/octet-stream")
            return self._send(200, entry.value, content_type,
                              {peers.TIMESTAMP: str(entry.timestamp)})
        if self.command == "PUT":
            content_type = self.headers.get("Content-Type")
            if content_type == "application/x-www-form-urlencoded":
                content_type = None  # curl -d default, not a real type for the value
            return self._json(201, self.node.write(key, Entry(now(), self._body(), content_type)))
        if self.command == "DELETE":
            return self._json(200, self.node.write(key, Entry(now())))
        self._text(405, "method not allowed")

    def _whereis(self, key):
        self._json(200, self.node.ring.replicas_for(key))

    def _balance(self, _):
        if self.command != "POST":
            return self._text(405, "method not allowed")
        self._json(200, self.node.balance_cluster())

    def _purge(self, _):
        if self.command != "POST":
            return self._text(405, "method not allowed")
        self._json(200, self.node.purge())

    # -- internal endpoints (node to node) -----------------------------------------

    def _internal_key(self, key):
        store = self.node.store
        if self.command in ("GET", "HEAD"):
            entry = store.get(key)
            if entry is None:
                return self._text(404, "no version")
            headers = peers.entry_headers(entry)
            content_type = headers.pop("Content-Type", None)
            return self._send(200, entry.value or b"", content_type, headers)
        if self.command == "PUT":
            body = self._body()
            deleted = self.headers.get(peers.DELETED) == "1"
            store.apply(key, Entry(
                timestamp=int(self.headers[peers.TIMESTAMP]),
                value=None if deleted else body,
                content_type=None if deleted else self.headers.get("Content-Type"),
            ))
            return self._send(204)
        self._text(405, "method not allowed")

    def _internal_digest(self, _):
        self._json(200, self.node.store.digest())

    def _internal_join(self, _):
        self._json(200, self.node.add_joining_node(self._body().decode()))

    def _internal_ring(self, node):
        if self.command == "PUT":
            self.node.ring.add_node(node)
        elif self.command == "DELETE":
            self.node.ring.remove_node(node)
        else:
            return self._text(405, "method not allowed")
        self._send(204)

    def _internal_balance(self, _):
        self._json(200, self.node.balance())

    exact_routes = {
        "/": _root,
        "/favicon.ico": _favicon,
        "/ring": _ring,
        "/keys": _keys,
        "/balance": _balance,
        "/purge": _purge,
        "/internal/digest": _internal_digest,
        "/internal/join": _internal_join,
        "/internal/balance": _internal_balance,
    }
    prefix_routes = [
        ("/ring/", _ring),
        ("/keys/", _key),
        ("/whereis/", _whereis),
        ("/internal/keys/", _internal_key),
        ("/internal/ring/", _internal_ring),
    ]

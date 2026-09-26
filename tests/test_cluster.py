"""End-to-end tests: real nodes on ephemeral ports, talking HTTP."""

import http.client
import logging
import time
import unittest

from mydht import peers
from mydht.node import Node

HOST = "127.0.0.1"

# Unreachable-peer warnings are expected in these tests.
logging.getLogger("mydht").setLevel(logging.ERROR)


def wait_for(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return condition()


class ClusterTestCase(unittest.TestCase):
    """Starts and cleans up nodes; the tests live in subclasses."""

    def setUp(self):
        self.nodes = []

    def tearDown(self):
        for n in self.nodes:
            try:
                n.stop(leave=False)
            except Exception:
                pass

    def start(self, count, replicas=3):
        for _ in range(count):
            self.add_node(replicas=replicas)
        return self.nodes

    def add_node(self, replicas=3, port=0, join=None, **kwargs):
        node = Node(HOST, port, replicas=replicas, timeout=2.0, **kwargs)
        if join is None and self.nodes:
            join = self.nodes[0].name
        node.start(join=join)
        self.nodes.append(node)
        return node

    def crash(self, node):
        node.stop(leave=False)
        self.nodes.remove(node)

    def call(self, node, method, path, body=b"", headers=None):
        return peers.request(node.name, method, path, body, headers)

    def put(self, node, key, value, **headers):
        return self.call(node, "PUT", f"/keys/{peers.key_path(key)}", value, headers)

    def get(self, node, key):
        return self.call(node, "GET", f"/keys/{peers.key_path(key)}")

    def holders(self, key):
        """Nodes whose local store has a live value for key."""
        return {n.name for n in self.nodes
                if n.store.get(key) is not None and not n.store.get(key).deleted}

    def fully_replicated(self, keys):
        by_name = {n.name: n for n in self.nodes}
        ring = self.nodes[0].ring
        return all(
            self.holders(k) >= set(ring.replicas_for(k)) and set(ring.replicas_for(k)) <= set(by_name)
            for k in keys
        )


class ClusterTest(ClusterTestCase):
    # -- basic operations ----------------------------------------------------

    def test_put_get_delete_via_any_node(self):
        a, b, c, d = self.start(4)
        resp = self.put(a, "greeting", b"hej")
        self.assertEqual(resp.status, 201)
        self.assertEqual(len(resp.json()["stored_on"]), 3)
        self.assertEqual(len(self.holders("greeting")), 3)

        for node in (a, b, c, d):
            self.assertEqual(self.get(node, "greeting").body, b"hej")

        self.assertEqual(self.call(c, "DELETE", "/keys/greeting").status, 200)
        for node in (a, b, c, d):
            self.assertEqual(self.get(node, "greeting").status, 404)

    def test_keys_can_contain_slashes_and_spaces(self):
        a, b, _ = self.start(3)
        self.put(a, "photos/2011/sommar bild.jpg", b"\xff\xd8binary")
        resp = self.get(b, "photos/2011/sommar bild.jpg")
        self.assertEqual(resp.body, b"\xff\xd8binary")
        self.assertEqual(resp.headers["Content-Type"], "image/jpeg")

    def test_http_details(self):
        (a,) = self.start(1)
        self.put(a, "doc", b'{"x": 1}', **{"Content-Type": "application/json"})
        head = self.call(a, "HEAD", "/keys/doc")
        self.assertEqual(head.status, 200)
        self.assertEqual(head.headers["Content-Length"], "8")
        self.assertEqual(head.headers["Content-Type"], "application/json")
        self.assertEqual(self.get(a, "missing").status, 404)
        self.assertEqual(self.call(a, "POST", "/keys/doc").status, 405)
        self.assertEqual(self.call(a, "GET", "/nope").status, 404)
        self.assertIn(b"MyDHT node", self.call(a, "GET", "/").body)
        self.assertEqual(self.call(a, "GET", "/ring").json()["nodes"], [a.name])

    def test_chunked_upload(self):
        """What `curl -T -` sends when reading from stdin."""
        (a,) = self.start(1)
        conn = http.client.HTTPConnection(HOST, a.httpd.server_address[1])
        conn.request("PUT", "/keys/streamed", body=iter([b"part one, ", b"part two"]),
                     encode_chunked=True)
        self.assertEqual(conn.getresponse().status, 201)
        conn.close()
        self.assertEqual(self.get(a, "streamed").body, b"part one, part two")

    def test_favicon_comes_from_the_dht(self):
        (a,) = self.start(1)
        self.assertEqual(self.call(a, "GET", "/favicon.ico").status, 404)
        self.put(a, "favicon.ico", b"\x00\x00\x01\x00")
        resp = self.call(a, "GET", "/favicon.ico")
        self.assertEqual(resp.body, b"\x00\x00\x01\x00")

    # -- membership ------------------------------------------------------------

    def test_joining_node_receives_its_keys(self):
        a, _, _ = self.start(3)
        keys = [f"k{i}" for i in range(40)]
        for k in keys:
            self.put(a, k, k.encode())
        new = self.add_node()
        self.assertTrue(all(new.name in n.ring for n in self.nodes))
        self.assertTrue(wait_for(lambda: self.fully_replicated(keys)))
        self.assertTrue(any(new.store.get(k) for k in keys))

    def test_graceful_leave_hands_over_data(self):
        a, b, c, d = self.start(4)
        keys = [f"k{i}" for i in range(40)]
        for k in keys:
            self.put(a, k, k.encode())
        d.stop(leave=True)
        self.nodes.remove(d)
        for n in self.nodes:
            self.assertNotIn(d.name, n.ring)
        self.assertTrue(self.fully_replicated(keys))
        for k in keys:
            self.assertEqual(self.get(b, k).body, k.encode())

    def test_crashed_node_is_tolerated_and_can_be_removed(self):
        a, b, c, d = self.start(4)
        keys = [f"k{i}" for i in range(40)]
        for k in keys:
            self.put(a, k, k.encode())
        self.crash(d)

        # A majority of replicas is still up for every key.
        for k in keys:
            self.assertEqual(self.get(b, k).body, k.encode())
        self.assertEqual(self.put(b, "after-crash", b"x").status, 201)

        resp = self.call(a, "DELETE", f"/ring/{d.name}")
        self.assertEqual(resp.status, 200)
        for n in self.nodes:
            self.assertNotIn(d.name, n.ring)
        self.assertTrue(self.fully_replicated(keys + ["after-crash"]))

    def test_writes_fail_without_a_quorum(self):
        a, b, c = self.start(3)
        self.crash(b)
        self.crash(c)
        resp = self.put(a, "k", b"v")
        self.assertEqual(resp.status, 503)
        self.assertEqual(resp.json()["stored_on"], [a.name])
        self.assertEqual(self.get(a, "k").status, 503)

    # -- repair ------------------------------------------------------------------

    def test_read_repairs_a_stale_replica(self):
        a, b, c = self.start(3)
        self.put(a, "k", b"v1")
        c.store.drop("k")
        self.assertEqual(self.get(a, "k").body, b"v1")
        self.assertTrue(wait_for(lambda: c.store.get("k") is not None))

    def test_balance_does_not_resurrect_deletes(self):
        a, b, c = self.start(3)
        self.put(a, "k", b"v")
        old = c.store.get("k")
        self.call(a, "DELETE", "/keys/k")
        # Pretend c was down during the delete and still has the old value.
        c.store.drop("k")
        c.store.apply("k", old)

        self.call(c, "POST", "/balance")
        for n in (a, b, c):
            self.assertTrue(n.store.get("k").deleted)
        self.assertEqual(self.get(c, "k").status, 404)

    def test_purge_only_drops_keys_that_are_safe_elsewhere(self):
        a, b, c = self.start(3)
        keys = [f"k{i}" for i in range(40)]
        for k in keys:
            self.put(a, k, k.encode())
        self.add_node()
        self.add_node()
        self.assertTrue(wait_for(lambda: self.fully_replicated(keys)))

        for n in self.nodes:
            self.call(n, "POST", "/purge")
        ring = a.ring
        for k in keys:
            self.assertEqual(self.holders(k), set(ring.replicas_for(k)))
            self.assertEqual(self.get(a, k).body, k.encode())


if __name__ == "__main__":
    unittest.main()

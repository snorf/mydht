"""Nodes with --data-dir: restarts, whole-cluster restarts and late seeds."""

import socket
import tempfile

from tests.test_cluster import ClusterTestCase, wait_for


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class PersistenceTest(ClusterTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = self.tmp.name

    def tearDown(self):
        super().tearDown()
        self.tmp.cleanup()

    def add_node(self, **kwargs):
        kwargs.setdefault("data_dir", self.data_dir)
        return super().add_node(**kwargs)

    def restart(self, node, join):
        port = node.httpd.server_address[1]
        self.crash(node)
        return self.add_node(port=port, join=join)

    def test_restarted_node_keeps_its_data_and_catches_up(self):
        a, b, c = self.start(3)
        for k in ("keep", "change", "remove"):
            self.put(a, k, b"v1")

        port = c.httpd.server_address[1]
        self.crash(c)
        # Changes made while c is down.
        self.put(a, "change", b"v2")
        self.call(a, "DELETE", "/keys/remove")

        c = self.add_node(port=port, join=a.name)
        self.assertEqual(c.store.get("keep").value, b"v1")  # read from disk
        self.assertTrue(wait_for(lambda: c.store.get("change").value == b"v2"))
        self.assertTrue(wait_for(lambda: c.store.get("remove").deleted))

    def test_whole_cluster_restart_in_any_order(self):
        a, b, c = self.start(3)
        names = [n.name for n in (a, b, c)]
        ports = [n.httpd.server_address[1] for n in (a, b, c)]
        keys = [f"k{i}" for i in range(30)]
        for k in keys:
            self.put(a, k, k.encode())
        for n in (a, b, c):
            self.crash(n)

        # Each node is told about all three; the first finds nobody and runs alone.
        for port in reversed(ports):
            self.add_node(port=port, join=",".join(names), rejoin_interval=0.1)
        self.assertTrue(wait_for(lambda: all(len(n.ring) == 3 for n in self.nodes)))
        for n in self.nodes:
            for k in keys:
                self.assertEqual(self.get(n, k).body, k.encode())

    def test_node_started_before_its_seed_joins_later(self):
        seed_port = free_port()
        early = self.add_node(join=f"127.0.0.1:{seed_port}", rejoin_interval=0.1)
        self.assertEqual(len(early.ring), 1)
        self.assertEqual(self.put(early, "written-alone", b"x").status, 201)

        seed = self.add_node(port=seed_port, join="")
        self.assertTrue(wait_for(lambda: len(early.ring) == 2 and len(seed.ring) == 2))
        self.assertTrue(wait_for(lambda: seed.store.get("written-alone") is not None))

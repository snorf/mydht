"""End-to-end tests for contrib/mydht-sync against a real cluster."""

import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tests.test_cluster import ClusterTestCase

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "contrib", "mydht-sync")
SHELL = shutil.which("dash") or "sh"


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@unittest.skipUnless(shutil.which("curl"), "needs curl")
class SyncScriptTest(ClusterTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.upstream_dir = os.path.join(self.tmp.name, "upstream")
        os.mkdir(self.upstream_dir)
        handler = partial(QuietHandler, directory=self.upstream_dir)
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.upstream.serve_forever, daemon=True).start()
        self.start(3)

    def tearDown(self):
        self.upstream.shutdown()
        self.upstream.server_close()
        super().tearDown()
        self.tmp.cleanup()

    def set_upstream(self, content: bytes):
        with open(os.path.join(self.upstream_dir, "hosts.conf"), "wb") as f:
            f.write(content)

    def sync(self, node_index, upstream=True, nodes=None, **env):
        """Run the script as node ``node_index``; returns (target content, reloads)."""
        home = os.path.join(self.tmp.name, f"node{node_index}")
        os.makedirs(home, exist_ok=True)
        target = os.path.join(home, "hosts.conf")
        reloads = os.path.join(home, "reloads")
        env = {
            "PATH": os.environ["PATH"],
            "MYDHT_SYNC_KEY": "nextdns/hosts.conf",
            "MYDHT_SYNC_TARGET": target,
            "MYDHT_SYNC_STATE": os.path.join(home, "state"),
            "MYDHT_SYNC_RELOAD": f"echo reload >> {reloads}",
            "MYDHT_SYNC_NODES": nodes or f"http://{self.nodes[node_index].name}",
            "MYDHT_SYNC_UPSTREAM": (f"http://127.0.0.1:{self.upstream.server_address[1]}/hosts.conf"
                                    if upstream else ""),
            "MYDHT_SYNC_SLOTS": "1",
            "MYDHT_SYNC_INTERVAL": "1",
            **env,
        }
        subprocess.run([SHELL, SCRIPT], env=env, check=True, capture_output=True, timeout=60)
        content = Path(target).read_bytes() if os.path.exists(target) else None
        count = len(Path(reloads).read_text().splitlines()) if os.path.exists(reloads) else 0
        return content, count

    def test_publish_and_follow(self):
        self.set_upstream(b"10.0.0.1 nas\n")
        self.assertEqual(self.sync(0), (b"10.0.0.1 nas\n", 1))  # publishes, then pulls
        self.assertEqual(self.sync(1, upstream=False), (b"10.0.0.1 nas\n", 1))
        # Nothing changed: 304, no reload.
        self.assertEqual(self.sync(1, upstream=False), (b"10.0.0.1 nas\n", 1))
        self.assertEqual(self.sync(0), (b"10.0.0.1 nas\n", 1))

        self.set_upstream(b"10.0.0.2 nas\n")
        self.assertEqual(self.sync(2), (b"10.0.0.2 nas\n", 1))  # another node publishes
        self.assertEqual(self.sync(1, upstream=False), (b"10.0.0.2 nas\n", 2))
        self.assertEqual(self.sync(0, upstream=False), (b"10.0.0.2 nas\n", 2))

    def test_only_publishes_in_its_own_slot(self):
        self.set_upstream(b"v1\n")
        self.sync(0)
        self.set_upstream(b"v2\n")
        if time.time() % 60 > 55:
            time.sleep(6)  # don't straddle a slot boundary
        current = int(time.time() // 60) % 2
        self.assertEqual(self.sync(1, MYDHT_SYNC_SLOTS="2", MYDHT_SYNC_SLOT=str(1 - current))[0],
                         b"v1\n")
        self.assertEqual(self.sync(1, MYDHT_SYNC_SLOTS="2", MYDHT_SYNC_SLOT=str(current))[0],
                         b"v2\n")

    def test_empty_or_missing_upstream_is_not_published(self):
        self.set_upstream(b"v1\n")
        self.sync(0)
        self.set_upstream(b"")
        self.assertEqual(self.sync(0)[0], b"v1\n")
        os.remove(os.path.join(self.upstream_dir, "hosts.conf"))  # upstream 404
        self.assertEqual(self.sync(0)[0], b"v1\n")

    def test_falls_back_to_the_next_node(self):
        self.set_upstream(b"v1\n")
        self.sync(0)
        dead = self.nodes[2]
        nodes = f"http://{dead.name} http://{self.nodes[1].name}"
        self.crash(dead)
        self.assertEqual(self.sync(1, upstream=False, nodes=nodes)[0], b"v1\n")

    def test_missing_key_is_not_an_error(self):
        self.assertEqual(self.sync(0, upstream=False), (None, 0))


if __name__ == "__main__":
    unittest.main()

import unittest
from collections import Counter

from mydht.hashring import HashRing

NODES = [f"node{i}:5000" for i in range(5)]
KEYS = [f"key-{i}" for i in range(2000)]


class HashRingTest(unittest.TestCase):
    def test_empty_ring(self):
        self.assertEqual(HashRing().replicas_for("x"), [])

    def test_replicas_are_distinct_and_capped(self):
        ring = HashRing(NODES, replicas=3)
        for key in KEYS[:200]:
            replicas = ring.replicas_for(key)
            self.assertEqual(len(replicas), 3)
            self.assertEqual(len(set(replicas)), 3)
        small = HashRing(NODES[:2], replicas=3)
        self.assertEqual(len(small.replicas_for("x")), 2)

    def test_same_answer_regardless_of_insert_order(self):
        a = HashRing(NODES)
        b = HashRing(reversed(NODES))
        for key in KEYS[:200]:
            self.assertEqual(a.replicas_for(key), b.replicas_for(key))

    def test_keys_spread_reasonably_even(self):
        ring = HashRing(NODES, replicas=1)
        counts = Counter(ring.replicas_for(k)[0] for k in KEYS)
        self.assertEqual(set(counts), set(NODES))
        for n in NODES:
            self.assertGreater(counts[n], len(KEYS) / len(NODES) / 2)

    def test_adding_a_node_moves_few_keys(self):
        ring = HashRing(NODES, replicas=1)
        before = {k: ring.replicas_for(k)[0] for k in KEYS}
        ring.add_node("node5:5000")
        moved = [k for k in KEYS if ring.replicas_for(k)[0] != before[k]]
        # Only keys that now belong to the new node should move.
        self.assertTrue(all(ring.replicas_for(k)[0] == "node5:5000" for k in moved))
        self.assertLess(len(moved), len(KEYS) / 3)

    def test_remove_node(self):
        ring = HashRing(NODES)
        ring.remove_node(NODES[0])
        self.assertNotIn(NODES[0], ring)
        for key in KEYS[:200]:
            self.assertNotIn(NODES[0], ring.replicas_for(key))
        ring.remove_node("never-added:1")  # no-op


if __name__ == "__main__":
    unittest.main()

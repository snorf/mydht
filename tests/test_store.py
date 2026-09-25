import unittest

from mydht.store import Entry, Store


class StoreTest(unittest.TestCase):
    def test_newer_write_wins(self):
        s = Store()
        self.assertTrue(s.apply("k", Entry(2, b"new")))
        self.assertFalse(s.apply("k", Entry(1, b"old")))
        self.assertEqual(s.get("k").value, b"new")

    def test_delete_is_a_tombstone(self):
        s = Store()
        s.apply("k", Entry(1, b"v"))
        s.apply("k", Entry(2))
        self.assertTrue(s.get("k").deleted)
        # An old replica pushing the value back must not resurrect it.
        self.assertFalse(s.apply("k", Entry(1, b"v")))
        self.assertEqual(s.digest(), {"k": [2, True]})

    def test_delete_wins_a_tie(self):
        s = Store()
        s.apply("k", Entry(5, b"v"))
        self.assertTrue(s.apply("k", Entry(5)))
        self.assertFalse(s.apply("k", Entry(5, b"v")))


if __name__ == "__main__":
    unittest.main()

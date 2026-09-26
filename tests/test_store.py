import os
import tempfile
import unittest

from mydht.store import Entry, Meta, SqliteStore, Store, open_store


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



class SqliteStoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "node.sqlite3")

    def tearDown(self):
        self.dir.cleanup()

    def test_same_semantics_as_memory(self):
        s = SqliteStore(self.path)
        self.assertTrue(s.apply("k", Entry(2, b"new", "text/plain")))
        self.assertFalse(s.apply("k", Entry(1, b"old")))
        self.assertEqual(s.get("k"), Entry(2, b"new", "text/plain"))
        self.assertTrue(s.apply("k", Entry(2)))  # delete wins a tie
        self.assertFalse(s.apply("k", Entry(2, b"new")))
        self.assertEqual(s.digest(), {"k": [2, True]})
        s.close()

    def test_survives_reopening(self):
        s = SqliteStore(self.path)
        s.apply("a", Entry(1, b"\x00binary\xff"))
        s.apply("b", Entry(1, b""))
        s.apply("gone", Entry(1))
        s.close()

        s = SqliteStore(self.path)
        self.assertEqual(s.get("a").value, b"\x00binary\xff")
        self.assertEqual(s.get("b").value, b"")  # empty value is not a delete
        self.assertTrue(s.get("gone").deleted)
        self.assertEqual(s.listing(), [Meta("a", 1, False, 8), Meta("b", 1, False, 0),
                                       Meta("gone", 1, True, 0)])
        s.drop("a")
        self.assertIsNone(s.get("a"))
        s.close()

    def test_open_store(self):
        self.assertFalse(open_store(None, "h:1").persistent)
        s = open_store(self.dir.name, "host:50140")
        self.assertTrue(s.persistent)
        s.close()
        self.assertTrue(os.path.exists(os.path.join(self.dir.name, "host_50140.sqlite3")))


if __name__ == "__main__":
    unittest.main()

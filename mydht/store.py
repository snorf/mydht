"""Versioned key/value storage for a single node.

Every entry carries a timestamp and the newest write wins ("last write
wins"). Deletes are stored as tombstones so that anti-entropy does not bring
deleted keys back from a replica that missed the delete.

``Store`` keeps everything in memory. ``SqliteStore`` has the same interface
but keeps the entries in an SQLite file, so a node keeps its data across
restarts.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import NamedTuple, Optional


def now() -> int:
    """Wall-clock timestamp in nanoseconds."""
    return time.time_ns()


@dataclass(frozen=True)
class Entry:
    timestamp: int
    value: Optional[bytes] = None  # None means the key was deleted
    content_type: Optional[str] = None

    @property
    def deleted(self) -> bool:
        return self.value is None

    def newer_than(self, other: Optional["Entry"]) -> bool:
        """True if this entry should replace ``other``.

        On equal timestamps a delete beats a write, so every replica makes
        the same choice.
        """
        if other is None:
            return True
        return (self.timestamp, self.deleted) > (other.timestamp, other.deleted)


class Meta(NamedTuple):
    """What we know about an entry without loading its value."""

    key: str
    timestamp: int
    deleted: bool
    size: int


class Store:
    """In-memory store; everything is lost when the process exits."""

    persistent = False

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, Entry] = {}

    def get(self, key: str) -> Optional[Entry]:
        with self._lock:
            return self._entries.get(key)

    def apply(self, key: str, entry: Entry) -> bool:
        """Store ``entry`` if it is newer than what we have. Returns True if stored."""
        with self._lock:
            if not entry.newer_than(self._entries.get(key)):
                return False
            self._entries[key] = entry
            return True

    def listing(self) -> list[Meta]:
        with self._lock:
            return [Meta(k, e.timestamp, e.deleted, len(e.value or b""))
                    for k, e in sorted(self._entries.items())]

    def digest(self) -> dict[str, list]:
        """``{key: [timestamp, deleted]}`` for every entry, used by anti-entropy."""
        return {m.key: [m.timestamp, m.deleted] for m in self.listing()}

    def drop(self, key: str) -> None:
        """Forget ``key`` entirely (used by purge, not by DELETE)."""
        with self._lock:
            self._entries.pop(key, None)

    def close(self) -> None:
        pass


class SqliteStore(Store):
    """Store backed by an SQLite database at ``path``."""

    persistent = True

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS entries (
                   key          TEXT PRIMARY KEY,
                   timestamp    INTEGER NOT NULL,
                   value        BLOB,
                   content_type TEXT
               )"""
        )

    def get(self, key: str) -> Optional[Entry]:
        with self._lock:
            row = self._db.execute(
                "SELECT timestamp, value, content_type FROM entries WHERE key = ?", (key,)
            ).fetchone()
        return None if row is None else Entry(row[0], row[1], row[2])

    def apply(self, key: str, entry: Entry) -> bool:
        with self._lock:
            row = self._db.execute(
                "SELECT timestamp, value IS NULL FROM entries WHERE key = ?", (key,)
            ).fetchone()
            current = None if row is None else Entry(row[0], None if row[1] else b"")
            if not entry.newer_than(current):
                return False
            self._db.execute(
                "INSERT OR REPLACE INTO entries VALUES (?, ?, ?, ?)",
                (key, entry.timestamp, entry.value, entry.content_type),
            )
            return True

    def listing(self) -> list[Meta]:
        with self._lock:
            rows = self._db.execute(
                "SELECT key, timestamp, value IS NULL, COALESCE(length(value), 0) "
                "FROM entries ORDER BY key"
            ).fetchall()
        return [Meta(k, ts, bool(deleted), size) for k, ts, deleted, size in rows]

    def drop(self, key: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM entries WHERE key = ?", (key,))

    def close(self) -> None:
        with self._lock:
            self._db.close()


def open_store(data_dir: Optional[str], name: str) -> Store:
    """An SQLite store under ``data_dir`` (one file per node name), or memory."""
    if not data_dir:
        return Store()
    filename = name.replace(":", "_").replace("/", "_") + ".sqlite3"
    return SqliteStore(os.path.join(data_dir, filename))

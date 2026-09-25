"""In-memory versioned key/value store for a single node.

Every entry carries a timestamp and the newest write wins ("last write
wins"). Deletes are stored as tombstones so that anti-entropy does not bring
deleted keys back from a replica that missed the delete.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional


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


class Store:
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

    def items(self) -> list[tuple[str, Entry]]:
        with self._lock:
            return sorted(self._entries.items())

    def digest(self) -> dict[str, list]:
        """``{key: [timestamp, deleted]}`` for every entry, used by anti-entropy."""
        with self._lock:
            return {k: [e.timestamp, e.deleted] for k, e in self._entries.items()}

    def drop(self, key: str) -> None:
        """Forget ``key`` entirely (used by purge, not by DELETE)."""
        with self._lock:
            self._entries.pop(key, None)

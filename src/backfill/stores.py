"""Backend-agnostic storage interfaces for the idempotent backfill pattern.

The pattern itself (see core.py) never talks to a concrete database. It only
needs something that satisfies the `Store` protocol below. This module ships
two reference implementations:

- InMemoryStore: a plain dict, useful for tests and demos.
- SQLiteStore: a minimal example of a real persistent backend, using only the
  Python standard library so the package has zero external dependencies.

To plug in BigQuery, Postgres, DynamoDB, etc. in a real project, implement the
same `Store` protocol (get / put) against that engine. The MERGE / upsert
semantics described in core.py stay identical — only the storage layer
changes.
"""

from __future__ import annotations

import sqlite3
from typing import Iterator, Optional, Protocol

from .records import Record


class Store(Protocol):
    """Minimal storage interface required by the backfill pattern.

    A `Store` is keyed by `record.key` and holds at most one record per key
    (the "latest" one, per the dedup-by-latest rule in core.upsert).
    """

    def get(self, key: str) -> Optional[Record]:
        """Return the currently stored record for `key`, or None."""
        ...

    def put(self, record: Record) -> None:
        """Overwrite (or insert) the stored record for `record.key`."""
        ...

    def all_records(self) -> Iterator[Record]:
        """Iterate over all currently stored records (any order)."""
        ...


class InMemoryStore:
    """Dict-backed Store. Good for unit tests and quick demos."""

    def __init__(self) -> None:
        self._data: dict[str, Record] = {}

    def get(self, key: str) -> Optional[Record]:
        return self._data.get(key)

    def put(self, record: Record) -> None:
        self._data[record.key] = record

    def all_records(self) -> Iterator[Record]:
        return iter(self._data.values())

    def __len__(self) -> int:
        return len(self._data)


class SQLiteStore:
    """SQLite-backed Store, as a stand-in for a real persistent backend.

    This mirrors what a MERGE statement does in BigQuery/Postgres: `put`
    performs an "insert, or update if the key already exists" operation
    (`INSERT ... ON CONFLICT ... DO UPDATE`), so repeated calls with the same
    key converge to a single row per key.
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                key TEXT PRIMARY KEY,
                event_date TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def get(self, key: str) -> Optional[Record]:
        row = self._conn.execute(
            "SELECT key, event_date, value, updated_at FROM records WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return Record(key=row[0], event_date=row[1], value=row[2], updated_at=row[3])

    def put(self, record: Record) -> None:
        # This is the SQLite equivalent of a BigQuery/Postgres MERGE:
        # insert the row, or overwrite it in place if the key already exists.
        self._conn.execute(
            """
            INSERT INTO records (key, event_date, value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                event_date = excluded.event_date,
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (record.key, record.event_date, record.value, record.updated_at),
        )
        self._conn.commit()

    def all_records(self) -> Iterator[Record]:
        rows = self._conn.execute(
            "SELECT key, event_date, value, updated_at FROM records"
        ).fetchall()
        return iter(
            Record(key=r[0], event_date=r[1], value=r[2], updated_at=r[3])
            for r in rows
        )

    def __len__(self) -> int:
        (count,) = self._conn.execute("SELECT COUNT(*) FROM records").fetchone()
        return count

    def close(self) -> None:
        self._conn.close()

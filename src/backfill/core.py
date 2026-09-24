"""Idempotent backfill pattern.

The core idea: a daily "sync" job and a weekly "backfill" job should never be
two different code paths. They should be the *same* function, called once
per day either way. Both converge on the same upsert (MERGE / dedup-by-latest)
logic, so running them any number of times, in any order, for the same date
range, always produces the same stored state.

    sync_one_day(date)          -- called by the daily cron
    backfill(start, end)        -- called by the weekly cron / manual re-run
        for day in range(start, end):
            sync_one_day(day)   -- literally the same function

Why this makes re-runs safe
----------------------------
1. `sync_one_day(date)` is a pure function of `date` (plus whatever the
   upstream source returns for that date) -- it has no hidden dependency on
   "today" or on what has already been synced. Calling it twice for the same
   date must produce the same fetched records.
2. `upsert()` is the single write path. It writes records keyed by a stable
   identity (`record.key`), and on conflict keeps only the most recent
   version (by `updated_at`) -- never appends duplicates. This is exactly
   what a `MERGE ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN
   INSERT` statement does in BigQuery/Postgres.
3. Because *both* the daily sync and the weekly backfill call
   `sync_one_day` -> `upsert`, there is only ever one code path that can
   mutate storage. A backfill covering a date range that overlaps
   already-synced days just re-applies the same upsert for those days,
   which is a no-op in terms of final state (same key, same-or-newer data).

The net effect: you can run the daily job every day, then run a full
backfill over the last 90 days because you found a bug upstream, and the
result is identical to what would have happened if the daily job had never
had the bug in the first place. No duplicate rows, no manual cleanup, no
"delete and reinsert" dance.
"""

from __future__ import annotations

import datetime as _dt
from typing import Callable, Iterable, Iterator, Optional

from .records import Record
from .stores import Store


# A `Fetcher` returns the raw records observed for a given date. In a real
# system this would call an upstream API/table. Here it's injected so the
# pattern stays testable and backend-agnostic.
Fetcher = Callable[[_dt.date], Iterable[Record]]


def upsert(store: Store, records: Iterable[Record]) -> int:
    """Write `records` into `store`, deduplicating by latest `updated_at`.

    This is the single MERGE-equivalent write path shared by sync_one_day
    and backfill. For each record:
      - if no record exists for that key, insert it.
      - if a record exists and the incoming one is newer (or equal), it
        overwrites the stored one.
      - if a record exists and is newer than the incoming one, the incoming
        one is dropped (stale data loses).

    Returns the number of records actually written (inserted or updated).
    """
    written = 0
    for record in records:
        existing = store.get(record.key)
        if existing is None or existing.updated_at <= record.updated_at:
            store.put(record)
            written += 1
    return written


def sync_one_day(
    store: Store,
    fetch: Fetcher,
    target_date: Optional[_dt.date] = None,
) -> int:
    """Sync a single day's worth of records into `store`.

    If `target_date` is omitted, defaults to today (UTC). This is the
    function the daily cron calls with no arguments, and the same function
    `backfill` calls once per day in a range.

    Returns the number of records written for that day (see `upsert`).
    """
    day = target_date or _dt.datetime.now(tz=_dt.timezone.utc).date()
    records = fetch(day)
    return upsert(store, records)


def date_range(start: _dt.date, end: _dt.date) -> Iterator[_dt.date]:
    """Inclusive range of dates from `start` to `end`."""
    if end < start:
        raise ValueError(f"end ({end}) must not be before start ({start})")
    current = start
    while current <= end:
        yield current
        current += _dt.timedelta(days=1)


def backfill(
    store: Store,
    fetch: Fetcher,
    start: _dt.date,
    end: _dt.date,
) -> int:
    """Re-sync every day in [start, end] by calling `sync_one_day` per day.

    This is the weekly/on-demand entry point. It deliberately contains no
    write logic of its own -- it is just a loop over `sync_one_day`, which
    is exactly what the daily cron calls. That's what makes running a
    backfill over a range that includes already-synced days safe: each day
    goes through the identical fetch -> upsert path, so the result converges
    to the same state regardless of how many times, or in what order, any
    given day was synced before.

    Returns the total number of records written across the whole range.
    """
    total = 0
    for day in date_range(start, end):
        total += sync_one_day(store, fetch, target_date=day)
    return total

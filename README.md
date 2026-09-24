# idempotent-backfill-pattern

[![CI](https://github.com/daichiz-dev/idempotent-backfill-pattern/actions/workflows/ci.yml/badge.svg)](https://github.com/daichiz-dev/idempotent-backfill-pattern/actions/workflows/ci.yml)

A tiny, dependency-free pattern for data pipelines: make your **daily sync**
job and your **weekly (or on-demand) backfill** job share the exact same
write path, so that re-running either one — for any date, any number of
times, in any order — always converges to the same stored state.

No external database is required to understand or use this. The pattern is
expressed as plain Python functions over a small `Store` interface; a
BigQuery table, a Postgres table, or anything else that can do an
upsert/MERGE can be plugged in behind that interface.

## The problem this solves

A common shape in data engineering:

- A **daily job** calls `sync_one_day()` with no arguments (defaults to
  "today") and writes today's data.
- Weeks later, someone finds a bug in the upstream source, or the daily job
  failed silently for a few days, and a **backfill** is needed for a date
  range.

If the backfill job is a *different* code path from the daily job — its own
fetch logic, its own write logic — it's very easy for the two to drift:
different dedup rules, different column mappings, different handling of
late-arriving data. Re-running a backfill over already-synced days can then
produce duplicate rows, drop data, or silently diverge from what the daily
job would have produced.

## The pattern

```
sync_one_day(store, fetch, target_date=None)
    day = target_date or today()
    records = fetch(day)
    upsert(store, records)          # <- the ONE write path

backfill(store, fetch, start, end)
    for day in date_range(start, end):
        sync_one_day(store, fetch, day)   # <- literally the same function
```

`backfill` contains **no write logic of its own**. It is just a loop that
calls `sync_one_day` once per day in the range — the same function the daily
cron calls with no arguments. There is only one place in the whole codebase
that ever writes to storage: `upsert()`.

```
        ┌─────────────┐        ┌─────────────┐
        │  daily cron │        │ weekly cron │
        │  (no args)  │        │ (start,end) │
        └──────┬──────┘        └──────┬──────┘
               │                      │
               ▼                      ▼
        sync_one_day(day)  <────  backfill loops over
               │                  sync_one_day(day) per day
               ▼
            fetch(day)
               │
               ▼
     ┌───────────────────────┐
     │   upsert(store, recs) │   <- single MERGE / dedup-by-latest
     │   (dedup by key,      │      write path, shared by both
     │   keep newest         │      entry points
     │   updated_at)         │
     └───────────────────────┘
```

### Why this makes re-runs safe

1. **`sync_one_day(date)` is a pure-ish function of `date`.** It doesn't
   depend on "what day it is" or "what has already been synced" — only on
   what `fetch(date)` returns for that date. Calling it twice for the same
   date fetches the same logical records.
2. **`upsert()` is the only write path**, and it's a dedup-by-latest MERGE:
   each record has a stable `key` and an `updated_at`. On conflict, the
   newer `updated_at` wins; a stale re-delivery of old data can never
   clobber a newer write. This mirrors:

   ```sql
   MERGE INTO target T
   USING staged S
   ON T.key = S.key
   WHEN MATCHED AND S.updated_at >= T.updated_at THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT ...
   ```

3. **Both entry points funnel through the same two functions.** Because
   `backfill` is defined in terms of `sync_one_day`, and `sync_one_day` is
   defined in terms of `upsert`, there is no second code path that could
   drift from the first. A backfill over a range that overlaps
   already-synced days just re-applies the identical upsert for those days
   — which, for unchanged upstream data, is a no-op on the final state.

The net result: you can run the daily job every day, then later run
`backfill(store, fetch, ninety_days_ago, today)` because you fixed an
upstream bug, and the result is exactly what you'd have gotten if the fix
had been in place from day one. No duplicate rows. No manual delete-and-
reinsert. No "which days did I already backfill" bookkeeping.

## Layout

```
src/backfill/
  core.py     # Record, sync_one_day(), backfill(), upsert() -- the pattern itself
  stores.py   # Store protocol + InMemoryStore + SQLiteStore reference backends
examples/
  run_example.py   # backfills 5 days, re-runs the same backfill, shows row count is unchanged
tests/
  test_idempotency.py   # same-range-twice, daily-vs-weekly convergence, stale-write rejection
```

`core.py` never imports a specific database client. It only depends on the
`Store` protocol (`get(key)`, `put(record)`, `all_records()`), so swapping in
BigQuery, Postgres, DynamoDB, or anything else means writing one small
adapter class — the sync/backfill logic above does not change at all.

## Usage

```python
import datetime as dt
from backfill import InMemoryStore, Record, backfill, sync_one_day

def fetch(day: dt.date):
    # Replace with a real upstream call. Must be deterministic for a given day.
    yield Record(
        key=f"event:{day.isoformat()}:0",
        event_date=day.isoformat(),
        value="...",
        updated_at=f"{day.isoformat()}T00:00:00Z",
    )

store = InMemoryStore()

# Daily cron:
sync_one_day(store, fetch)  # defaults to today

# Weekly / on-demand backfill:
backfill(store, fetch, dt.date(2026, 1, 1), dt.date(2026, 1, 31))
```

## Running the example and tests

```bash
python examples/run_example.py
pytest tests/
```

Both are dependency-free (SQLite is part of the Python standard library).

## Adapting this to a real backend

Implement `Store` against your database, e.g. for BigQuery:

```python
class BigQueryStore:
    def get(self, key): ...   # SELECT ... WHERE key = @key
    def put(self, record): ... # a single-row MERGE statement (see stores.py's SQLiteStore for the shape)
    def all_records(self): ... # SELECT * FROM table
```

Nothing in `core.py` needs to change. The daily/weekly convergence guarantee
comes from the *shape* of the pattern (one write path, keyed dedup-by-latest
upsert), not from any particular storage engine.

## License

MIT © 2026 daichiz-dev

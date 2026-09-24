from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backfill import InMemoryStore, Record, SQLiteStore, backfill, sync_one_day  # noqa: E402


def make_fetcher(events_per_day: int = 3):
    def fetch(day: dt.date):
        for i in range(events_per_day):
            yield Record(
                key=f"event:{day.isoformat()}:{i}",
                event_date=day.isoformat(),
                value=f"payload-{i}",
                updated_at=f"{day.isoformat()}T00:00:00Z",
            )

    return fetch


def test_backfill_run_twice_is_idempotent_in_memory():
    store = InMemoryStore()
    fetch = make_fetcher()
    start, end = dt.date(2026, 2, 1), dt.date(2026, 2, 7)

    backfill(store, fetch, start, end)
    first_snapshot = sorted(
        (r.key, r.value, r.updated_at) for r in store.all_records()
    )

    backfill(store, fetch, start, end)
    second_snapshot = sorted(
        (r.key, r.value, r.updated_at) for r in store.all_records()
    )

    assert first_snapshot == second_snapshot
    assert len(store) == 7 * 3


def test_backfill_run_twice_is_idempotent_sqlite():
    store = SQLiteStore(":memory:")
    fetch = make_fetcher()
    start, end = dt.date(2026, 3, 1), dt.date(2026, 3, 3)

    backfill(store, fetch, start, end)
    count_after_first = len(store)

    backfill(store, fetch, start, end)
    count_after_second = len(store)

    assert count_after_first == count_after_second == 3 * 3
    store.close()


def test_daily_sync_and_backfill_converge_to_same_state():
    """Running sync_one_day for each day individually (simulating the daily
    cron) must produce the exact same stored state as one backfill() call
    over the same range (simulating the weekly job) -- because backfill is
    just a loop over sync_one_day.
    """
    fetch = make_fetcher()
    start, end = dt.date(2026, 4, 10), dt.date(2026, 4, 14)

    daily_store = InMemoryStore()
    for day_offset in range((end - start).days + 1):
        sync_one_day(daily_store, fetch, start + dt.timedelta(days=day_offset))

    backfill_store = InMemoryStore()
    backfill(backfill_store, fetch, start, end)

    daily_snapshot = sorted(
        (r.key, r.value, r.updated_at) for r in daily_store.all_records()
    )
    backfill_snapshot = sorted(
        (r.key, r.value, r.updated_at) for r in backfill_store.all_records()
    )

    assert daily_snapshot == backfill_snapshot


def test_upsert_keeps_latest_and_drops_stale():
    store = InMemoryStore()
    key = "event:2026-05-01:0"

    stale = Record(key=key, event_date="2026-05-01", value="old", updated_at="2026-05-01T00:00:00Z")
    fresh = Record(key=key, event_date="2026-05-01", value="new", updated_at="2026-05-01T12:00:00Z")

    store.put(stale)
    from backfill import upsert

    # Applying a stale update after a fresh one must not overwrite it.
    upsert(store, [fresh])
    upsert(store, [stale])

    assert store.get(key).value == "new"


def test_backfill_over_single_day_matches_single_sync():
    fetch = make_fetcher(events_per_day=2)
    day = dt.date(2026, 6, 15)

    s1 = InMemoryStore()
    sync_one_day(s1, fetch, day)

    s2 = InMemoryStore()
    backfill(s2, fetch, day, day)

    assert sorted((r.key, r.value) for r in s1.all_records()) == sorted(
        (r.key, r.value) for r in s2.all_records()
    )

"""Demo: backfill a few days, then re-run the exact same backfill again.

Shows that the second run does not change the number of stored records --
the whole point of the pattern. Run with:

    python examples/run_example.py
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backfill import InMemoryStore, Record, backfill  # noqa: E402


def fake_upstream_fetch(day: dt.date):
    """Pretend upstream source: 3 events per day, deterministic per date."""
    for i in range(3):
        yield Record(
            key=f"event:{day.isoformat()}:{i}",
            event_date=day.isoformat(),
            value=f"payload-{i}",
            updated_at=f"{day.isoformat()}T00:00:00Z",
        )


def main() -> None:
    store = InMemoryStore()
    start = dt.date(2026, 1, 1)
    end = dt.date(2026, 1, 5)

    written_first = backfill(store, fake_upstream_fetch, start, end)
    print(f"First backfill: wrote {written_first} records, store now has {len(store)}")

    # Re-run the identical backfill. Because upsert dedups by key and the
    # fetched data for a given date is deterministic, the store size does
    # not change -- only the same rows get overwritten in place.
    written_second = backfill(store, fake_upstream_fetch, start, end)
    print(f"Second backfill: wrote {written_second} records, store now has {len(store)}")

    assert len(store) == 15, "expected 3 events/day * 5 days = 15 records"
    print("Idempotency confirmed: repeated backfill did not change record count.")


if __name__ == "__main__":
    main()

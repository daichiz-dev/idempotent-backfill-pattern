"""The `Record` type shared by core.py (the pattern) and stores.py (backends).

Split into its own module purely to avoid a circular import between
core.py and stores.py, which both need this type.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    """A single upserted record.

    key:         stable identity for the record (e.g. "<source_id>:<date>").
                 Two records with the same key represent the same logical
                 entity observed at different times.
    event_date:  the calendar date (YYYY-MM-DD) this record logically
                 belongs to.
    value:       arbitrary payload, opaque to the pattern.
    updated_at:  ISO 8601 timestamp of when this version was produced.
                 Used by `upsert` to decide which version wins.
    """

    key: str
    event_date: str
    value: str
    updated_at: str

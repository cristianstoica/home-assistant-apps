"""UTC/SI time helpers for pairing and scoring."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_utc(value: datetime | None = None) -> str:
    dt = value or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def isoformat_utc_micro(value: datetime | None = None) -> str:
    """Fixed-width UTC stamp (microseconds always present) for lexical ordering.

    ``isoformat_utc`` omits the fractional-seconds field when
    ``microsecond == 0``, and since ``'.' < 'Z'`` a later same-second stamp
    with microseconds compares lexically SMALLER than a whole-second one.
    The scoring run stamp (``discover_score_work``) relies on SQL string
    comparison as a time ordering, so it must use this fixed-width form.
    Do not switch other call sites: ``floor_hour``-derived ``…:00Z`` values
    are stored DB-wide and would cross-format-compare against new values.
    """
    dt = value or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def floor_hour(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def local_day_start(now: datetime, timezone: str) -> datetime:
    """UTC instant of local midnight of ``now``'s calendar day in ``timezone``."""
    local_now = now.astimezone(ZoneInfo(timezone))
    midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight.astimezone(UTC)


def lead_hours(issued_at: str, valid_at: str) -> int:
    delta = parse_utc(valid_at) - parse_utc(issued_at)
    return int(delta.total_seconds() // 3600)


def day_ahead(issued_at: str, valid_at: str, timezone: str) -> int:
    tz = ZoneInfo(timezone)
    issued_day = parse_utc(issued_at).astimezone(tz).date()
    valid_day = parse_utc(valid_at).astimezone(tz).date()
    return (valid_day - issued_day).days


def window_cutoff(days: int, now: datetime | None = None) -> str:
    return isoformat_utc((now or utc_now()) - timedelta(days=days))


def utc_day_bucket(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).date().isoformat()

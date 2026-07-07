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


def parse_utc(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def floor_hour(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


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

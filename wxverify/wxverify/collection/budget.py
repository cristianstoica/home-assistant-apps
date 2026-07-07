"""Provider budget reservation and source-cap writer."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from zoneinfo import ZoneInfo

import httpx

from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.worker.control import JobDeferred

# Failures that provably occur before anything reaches the provider: DNS
# resolution failure, connection refused/reset, or a timeout while still
# establishing the connection. Read-phase failures (ReadTimeout, ReadError,
# RemoteProtocolError) are deliberately excluded — the request was already
# sent, so the provider plausibly counted the call; never underestimate
# usage against a paid quota.
_REFUNDABLE_TRANSPORT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)


@dataclass(frozen=True)
class Reservation:
    """A successful budget reservation, refundable via ``refund_budget``."""

    source: str
    billing_day: str
    calls: int
    credits: int


def is_refundable_transport_error(exc: BaseException) -> bool:
    """Return True when no HTTP attempt can have reached the provider."""
    return isinstance(exc, _REFUNDABLE_TRANSPORT_ERRORS)


def _billing_day(tz_name: str) -> str:
    return utc_now().astimezone(ZoneInfo(tz_name)).date().isoformat()


def current_billing_day(tz_name: str) -> str:
    return _billing_day(tz_name)


def _next_billing_window(tz_name: str) -> str:
    now_local = utc_now().astimezone(ZoneInfo(tz_name))
    tomorrow = now_local.date() + timedelta(days=1)
    midnight = now_local.replace(
        year=tomorrow.year,
        month=tomorrow.month,
        day=tomorrow.day,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return isoformat_utc(midnight)


def reserve_budget(
    conn: sqlite3.Connection, source: str, calls: int = 1, credits: int | None = None
) -> Reservation:
    source_row = conn.execute(
        """
        SELECT daily_call_limit, daily_credit_limit, billing_tz
        FROM sources
        WHERE source = ?
        """,
        (source,),
    ).fetchone()
    if source_row is None:
        raise ValueError(f"unknown source {source}")
    credit_limit = source_row["daily_credit_limit"]
    billing_tz = str(source_row["billing_tz"])
    credits_to_reserve = credits if credits is not None else 0
    day = _billing_day(billing_tz)
    conn.execute(
        "INSERT OR IGNORE INTO api_budget (source, billing_day) VALUES (?, ?)",
        (source, day),
    )
    cur = conn.execute(
        """
        UPDATE api_budget
        SET calls = calls + ?, credits = credits + ?
        WHERE source = ?
          AND billing_day = ?
          AND calls + ? <= ?
          AND (? IS NULL OR credits + ? <= ?)
        """,
        (
            calls,
            credits_to_reserve,
            source,
            day,
            calls,
            int(source_row["daily_call_limit"]),
            credit_limit,
            credits_to_reserve,
            credit_limit,
        ),
    )
    if cur.rowcount != 1:
        raise JobDeferred(_next_billing_window(billing_tz))
    return Reservation(
        source=source, billing_day=day, calls=calls, credits=credits_to_reserve
    )


def refund_budget(conn: sqlite3.Connection, reservation: Reservation) -> None:
    """Return a reservation whose HTTP attempt never reached the provider.

    Targets the reservation's own billing day (not "today") so a refund
    landing just after the billing rollover cannot bleed into the new day's
    row; floors at zero so a refund can never make counters negative.
    """
    conn.execute(
        """
        UPDATE api_budget
        SET calls = MAX(0, calls - ?), credits = MAX(0, credits - ?)
        WHERE source = ? AND billing_day = ?
        """,
        (
            reservation.calls,
            reservation.credits,
            reservation.source,
            reservation.billing_day,
        ),
    )


def set_source_cap(
    conn: sqlite3.Connection,
    source: str,
    *,
    daily_call_limit: int | None = None,
    daily_credit_limit: int | None = None,
    no_credit_limit: bool = False,
) -> None:
    row = conn.execute("SELECT 1 FROM sources WHERE source = ?", (source,)).fetchone()
    if row is None:
        raise ValueError(f"unknown source {source}")
    if daily_call_limit is not None:
        if daily_call_limit < 0:
            raise ValueError("daily_call_limit must be non-negative")
        conn.execute(
            "UPDATE sources SET daily_call_limit = ? WHERE source = ?",
            (daily_call_limit, source),
        )
    if no_credit_limit:
        conn.execute(
            "UPDATE sources SET daily_credit_limit = NULL WHERE source = ?", (source,)
        )
    elif daily_credit_limit is not None:
        if daily_credit_limit < 0:
            raise ValueError("daily_credit_limit must be non-negative")
        conn.execute(
            "UPDATE sources SET daily_credit_limit = ? WHERE source = ?",
            (daily_credit_limit, source),
        )

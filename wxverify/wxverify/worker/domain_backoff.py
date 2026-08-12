"""SQLite-backed per-domain provider backoff."""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import httpx

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.worker.control import JobDeferred

logger = logging.getLogger(__name__)

_DEFAULT_DELAY_SECONDS = 60
_MAX_DELAY_SECONDS = 3600
# The ladder saturates at _MAX_DELAY_SECONDS once the shift reaches this
# value, so shifting further can only produce an enormous intermediate that
# min() immediately discards. Capping the EXPONENT rather than the result
# keeps every existing delay identical -- verified equal for retry_count
# 0..10000 -- while bounding the work. DERIVED, not chosen: bit_length() of
# the ceiling-to-base ratio is always strictly greater than the ratio, so
# _DEFAULT_DELAY_SECONDS << this shift always exceeds _MAX_DELAY_SECONDS and
# the ladder can never be truncated by a future change to either constant.
_MAX_BACKOFF_SHIFT = (_MAX_DELAY_SECONDS // _DEFAULT_DELAY_SECONDS).bit_length()
# The largest value SQLite's 64-bit signed INTEGER can store. A foreign or
# hand-edited row can already sit here; incrementing past it raises on the
# bind. The count saturates instead: it is a backoff ladder position, not an
# audited total, and the ladder is already at its ceiling well below this.
# A carrier BELOW the column's range (a negative integer, or a finite
# out-of-int64 negative REAL that int() accepts) restarts the ladder at 0
# instead -- coercibility is not the same as bindability.
_MAX_RETRY_COUNT = 2**63 - 1

SOURCE_DOMAINS: dict[str, str] = {
    "open-meteo": "api.open-meteo.com",
    "meteoblue": "my.meteoblue.com",
    "weathercom": "api.weather.com",
    "visualcrossing": "weather.visualcrossing.com",
    "openweathermap": "api.openweathermap.org",
    "weatherapi": "api.weatherapi.com",
    "meteosource": "www.meteosource.com",
    "google": "weather.googleapis.com",
}
OPEN_METEO_HISTORICAL_DOMAIN = "previous-runs-api.open-meteo.com"


def source_domain(source: str, *, historical: bool = False) -> str:
    if source == "open-meteo" and historical:
        return OPEN_METEO_HISTORICAL_DOMAIN
    return SOURCE_DOMAINS[source]


def check_domain_backoff(conn: sqlite3.Connection, domain: str) -> None:
    row = conn.execute(
        "SELECT next_attempt_at FROM domain_backoffs WHERE domain=?", (domain,)
    ).fetchone()
    if row is None:
        return
    next_attempt_at = str(row["next_attempt_at"])
    try:
        due = parse_utc(next_attempt_at)
    except ValueError:
        # An unreadable stamp can only arrive from an imported, restored or
        # hand-edited database. Failing OPEN is self-healing: the attempt
        # proceeds, and either it succeeds and clear_domain_backoff deletes the
        # row, or it is throttled again and record_http_backoff rewrites a
        # well-formed one. Failing closed would wedge this domain permanently,
        # because the fetch that would clear the row never runs.
        logger.warning(
            "unreadable domain backoff stamp ignored domain=%s value=%r",
            domain,
            next_attempt_at,
        )
        return
    if due > utc_now():
        raise JobDeferred(next_attempt_at)


def clear_domain_backoff(conn: sqlite3.Connection, domain: str) -> None:
    conn.execute("DELETE FROM domain_backoffs WHERE domain=?", (domain,))


def record_http_backoff(
    conn: sqlite3.Connection, response: httpx.Response
) -> str | None:
    if response.status_code != 429 and response.status_code < 500:
        return None
    domain = response.url.host or _domain_from_url(str(response.url))
    if domain is None:
        return None
    retry_count = min(max(_retry_count(conn, domain), 0) + 1, _MAX_RETRY_COUNT)
    next_attempt_at = _next_attempt(response, retry_count)
    conn.execute(
        """
        INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
        VALUES (?, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET
            next_attempt_at=excluded.next_attempt_at,
            retry_count=excluded.retry_count
        """,
        (domain, next_attempt_at, retry_count),
    )
    logger.warning(
        "domain backoff activated domain=%s status=%d retry=%d until=%s",
        domain,
        response.status_code,
        retry_count,
        next_attempt_at,
    )
    return next_attempt_at


def _retry_count(conn: sqlite3.Connection, domain: str) -> int:
    row = conn.execute(
        "SELECT retry_count FROM domain_backoffs WHERE domain=?", (domain,)
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(row["retry_count"])
    except (TypeError, ValueError, OverflowError):
        # TEXT, BLOB and REAL-infinity carriers all bind into this INTEGER
        # column and all three raise here. There is no streak to preserve in an
        # unreadable value; returning 0 restarts the ladder and the caller's
        # UPSERT immediately overwrites the row with a well-formed integer.
        return 0


def _next_attempt(response: httpx.Response, retry_count: int) -> str:
    retry_after = response.headers.get("Retry-After")
    parsed = _parse_retry_after(retry_after)
    if parsed is not None:
        return parsed
    shift = min(max(0, retry_count - 1), _MAX_BACKOFF_SHIFT)
    seconds = min(_MAX_DELAY_SECONDS, _DEFAULT_DELAY_SECONDS * (2**shift))
    return isoformat_utc(utc_now() + timedelta(seconds=seconds))


def _parse_retry_after(value: str | None) -> str | None:
    if not value:
        return None
    stripped = value.strip()
    if stripped.isdecimal():
        return isoformat_utc(utc_now() + timedelta(seconds=int(stripped)))
    try:
        parsed = parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return isoformat_utc(parsed)


def _domain_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    return parsed.hostname

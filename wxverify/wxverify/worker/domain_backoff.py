"""SQLite-backed per-domain provider backoff."""

from __future__ import annotations

import sqlite3
from datetime import UTC, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import httpx

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.worker.control import JobDeferred

_DEFAULT_DELAY_SECONDS = 60
_MAX_DELAY_SECONDS = 3600

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
    if parse_utc(next_attempt_at) > utc_now():
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
    retry_count = _retry_count(conn, domain) + 1
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
    return next_attempt_at


def _retry_count(conn: sqlite3.Connection, domain: str) -> int:
    row = conn.execute(
        "SELECT retry_count FROM domain_backoffs WHERE domain=?", (domain,)
    ).fetchone()
    return 0 if row is None else int(row["retry_count"])


def _next_attempt(response: httpx.Response, retry_count: int) -> str:
    retry_after = response.headers.get("Retry-After")
    parsed = _parse_retry_after(retry_after)
    if parsed is not None:
        return parsed
    seconds = min(
        _MAX_DELAY_SECONDS,
        _DEFAULT_DELAY_SECONDS * (2 ** max(0, retry_count - 1)),
    )
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

"""Guards on ``wxverify.worker.domain_backoff``.

The per-domain backoff ladder must stay cheap and total for any stored
``retry_count``, including values that only a foreign, restored, or
hand-edited database can carry: the delay computation must not materialize
an exponent-sized intermediate before bounding it, and the stored count
must be clamped at both ends of SQLite's 64-bit INTEGER range before the
bind -- ``int()`` accepting a value is not the same as the driver binding
it.  Everything here runs against a real in-memory database with the real
schema; only the module clock is frozen, so the returned stamps are exact.
"""

from __future__ import annotations

import sqlite3
import tracemalloc
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import create_schema
from wxverify.worker import domain_backoff
from wxverify.worker.domain_backoff import _next_attempt, record_http_backoff

_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)

_SQLITE_INT64_MAX = 2**63 - 1


def _stamp(seconds: int) -> str:
    """The exact stamp the module emits for a delay of ``seconds``."""
    return isoformat_utc(_NOW + timedelta(seconds=seconds))


def _response(
    status: int, url: str, headers: dict[str, str] | None = None
) -> httpx.Response:
    """A canned HTTP response; the domain row is keyed on the URL's host."""
    return httpx.Response(
        status_code=status,
        headers=headers,
        content=b"",
        request=httpx.Request("GET", url),
    )


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> datetime:
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: _NOW)
    return _NOW


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    create_schema(connection)
    try:
        yield connection
    finally:
        connection.close()


def _seed_backoff_row(
    conn: sqlite3.Connection, domain: str, retry_count: object
) -> None:
    conn.execute(
        "INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)"
        " VALUES (?, ?, ?)",
        (domain, _stamp(0), retry_count),
    )


def _stored_retry_count(conn: sqlite3.Connection, domain: str) -> object:
    row = conn.execute(
        "SELECT retry_count FROM domain_backoffs WHERE domain = ?", (domain,)
    ).fetchone()
    assert row is not None, f"no domain_backoffs row for {domain}"
    return row["retry_count"]


def test_ladder_without_retry_after_matches_established_delay_table(
    frozen_now: datetime,
) -> None:
    """The delay ladder for counts 0..8 is exactly 60, 60, 120, 240, 480,
    960, 1920, 3600, 3600 seconds -- the equivalence pin that fails on any
    off-by-one in the exponent or on a cap tight enough to truncate the
    ladder below its 3600 s ceiling."""
    response = _response(429, "https://ladder.example/v1")
    expected_delays = [60, 60, 120, 240, 480, 960, 1920, 3600, 3600]
    for retry_count, seconds in enumerate(expected_delays):
        assert _next_attempt(response, retry_count) == _stamp(seconds), retry_count


def test_huge_stored_count_delay_is_bounded_in_allocation(
    frozen_now: datetime,
) -> None:
    """A stored count of 2**28 + 1 must produce the ceiling stamp without
    materializing an exponent-sized integer first: the computation stays
    under 1 MB of traced allocation.  An unbounded exponent peaks at
    ~125 MB here -- five orders of magnitude over budget, independent of
    machine speed (the ~1.3 s it also takes is a symptom, not the
    assertion)."""
    response = _response(429, "https://huge-count.example/v1")
    # tracemalloc's peak is process-global. Do not reset or reuse a session
    # this test did not start: reset_peak() sets the peak to the CURRENT
    # traced total, so under an already-tracing harness (PYTHONTRACEMALLOC=1)
    # the budget below would fail on correct code, and the harness's own
    # peak would be destroyed. A fresh start() traces from zero, so the peak
    # already measures only this window.
    if tracemalloc.is_tracing():
        pytest.skip("a pre-existing tracemalloc session owns the process-global peak")
    tracemalloc.start()
    try:
        stamp = _next_attempt(response, 2**28 + 1)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert stamp == _stamp(3600)
    assert peak < 1_000_000, peak


def test_int64_max_stored_count_returns_ceiling_delay(frozen_now: datetime) -> None:
    """The largest count the column can store still yields the 3600 s stamp.

    Do NOT run this against a build without the exponent cap: the uncapped
    expression needs a ~1.15 EB intermediate and exhausts memory rather
    than failing cleanly.  The bounded-allocation test above is its safe
    proxy on such a build."""
    response = _response(429, "https://int64-max.example/v1")
    assert _next_attempt(response, _SQLITE_INT64_MAX) == _stamp(3600)


def test_stored_count_saturates_at_sqlite_integer_bound(
    conn: sqlite3.Connection, frozen_now: datetime
) -> None:
    """A row already at SQLite's INTEGER maximum must still be written:
    the count saturates there instead of incrementing past the bound and
    raising OverflowError at the bind.  It is a ladder position, not an
    audited total, and the ladder ceiling sits far below this value."""
    domain = "saturated.example"
    _seed_backoff_row(conn, domain, _SQLITE_INT64_MAX)
    # Retry-After short-circuits the delay ladder, so this stays cheap in
    # both directions -- the bind is the behavior under test.
    response = _response(429, f"https://{domain}/v1", headers={"Retry-After": "60"})

    stamp = record_http_backoff(conn, response)

    assert stamp == _stamp(60)
    assert _stored_retry_count(conn, domain) == _SQLITE_INT64_MAX
    # Companion pin on the bound itself: one past the column's range does
    # not bind, so saturating anywhere else would fail here.
    with pytest.raises(OverflowError):
        conn.execute(
            "UPDATE domain_backoffs SET retry_count = ? WHERE domain = ?",
            (_SQLITE_INT64_MAX + 1, domain),
        )


def test_negative_out_of_range_stored_count_restarts_ladder(
    conn: sqlite3.Connection, frozen_now: datetime
) -> None:
    """A readable-but-unbindable stored count restarts the ladder at 0.

    ``int(-1e100)`` succeeds, so the value sails through any upper-only
    clamp and raises OverflowError at the bind; only a low clamp brings it
    back inside the column's range.  There is no streak to preserve, so the
    next write stores exactly 1."""
    domain = "negative-real.example"
    _seed_backoff_row(conn, domain, -1e100)
    # The seed must land as REAL: if a future SQLite coerces or rejects it,
    # this turns the test red rather than vacuous.
    typeof = conn.execute(
        "SELECT typeof(retry_count) FROM domain_backoffs WHERE domain = ?",
        (domain,),
    ).fetchone()[0]
    assert typeof == "real"
    response = _response(429, f"https://{domain}/v1", headers={"Retry-After": "60"})

    stamp = record_http_backoff(conn, response)

    assert stamp == _stamp(60)
    assert _stored_retry_count(conn, domain) == 1


def test_ordinary_increment_and_delay_progression_unchanged(
    conn: sqlite3.Connection, frozen_now: datetime
) -> None:
    """Negative control: the live path is untouched by the clamps.  A fresh
    domain's first failure stores count 1 with a 60 s stamp, the second
    stores count 2 with a 120 s stamp -- an off-by-one from the clamp, or a
    clamp applied at the wrong bound, fails here."""
    domain = "fresh.example"
    response = _response(429, f"https://{domain}/v1")

    first = record_http_backoff(conn, response)
    assert first == _stamp(60)
    assert _stored_retry_count(conn, domain) == 1

    second = record_http_backoff(conn, response)
    assert second == _stamp(120)
    assert _stored_retry_count(conn, domain) == 2


def test_shift_cap_derivation_cannot_truncate_the_ladder() -> None:
    """The exponent cap must be large enough that shifting the base delay
    by it reaches the ceiling -- stated over the constants as they are, so
    a future change to either delay constant that leaves the cap too small
    (a silently truncated ladder, which no delay-table test at today's
    values would catch) fails here."""
    assert (
        domain_backoff._DEFAULT_DELAY_SECONDS * 2**domain_backoff._MAX_BACKOFF_SHIFT
        >= domain_backoff._MAX_DELAY_SECONDS
    )

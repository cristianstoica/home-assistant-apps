"""Step 4 (card 7): close-retention cleanup of past display rows.

A synthetic zone with a non-zero offset (``Etc/GMT-14``, UTC+14) separates
"yesterday" from "today" by a local midnight that differs from the UTC
instant's own calendar day, so a UTC-midnight bound (instead of a
``sites.timezone``-aware one) would misclassify the fixture's rows.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.worker.close_chain import (
    DELETE_PAST_DISPLAY_SAMPLES_SQL,
    run_close_retention,
)

# Etc/GMT-14 is UTC+14 (POSIX sign is reversed from the common convention).
# now_utc = 2026-01-05T15:00:00Z -> local = 2026-01-06T05:00:00+14:00, whose
# calendar day's local midnight is 2026-01-06T00:00:00+14:00 ==
# 2026-01-05T10:00:00Z in UTC. Hand-derived, not computed by calling
# local_day_start.
_NOW_UTC = datetime(2026, 1, 5, 15, 0, 0, tzinfo=UTC)
_BOUND = "2026-01-05T10:00:00Z"
_HOUR_BEFORE_MIDNIGHT = "2026-01-05T09:00:00Z"
_EARLIER_YESTERDAY = "2026-01-04T22:00:00Z"
_AT_MIDNIGHT = "2026-01-05T10:00:00Z"
_LATER_TODAY = "2026-01-05T12:00:00Z"

_DISPLAY_VARIABLES = ("cloud_cover", "wind_gust", "wind_direction")
_SCORED_VARIABLES = ("temperature", "wind", "precip")


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", cached_statements=0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(
    conn: sqlite3.Connection, *, name: str, timezone: str, enabled: int
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 10.0, 20.0, 100.0, ?, ?)
            """,
            (name, timezone, enabled),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE is_virtual = 0 ORDER BY id LIMIT 1"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    valid_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, ?, '2026-01-01T00:00:00Z', ?, 24, 1.0, '{}', 'run-1')
        """,
        (site_id, feed_id, variable, valid_at),
    )


def _seed_site_rows(conn: sqlite3.Connection, *, site_id: int, feed_id: int) -> None:
    for variable in _DISPLAY_VARIABLES:
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            valid_at=_HOUR_BEFORE_MIDNIGHT,
        )
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            valid_at=_EARLIER_YESTERDAY,
        )
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            valid_at=_AT_MIDNIGHT,
        )
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            valid_at=_LATER_TODAY,
        )
    for variable in _SCORED_VARIABLES:
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            valid_at=_HOUR_BEFORE_MIDNIGHT,
        )


def _rows(conn: sqlite3.Connection, site_id: int) -> set[tuple[str, str]]:
    return {
        (row["variable"], row["valid_at"])
        for row in conn.execute(
            "SELECT variable, valid_at FROM forecast_samples WHERE site_id = ?",
            (site_id,),
        ).fetchall()
    }


def test_close_retention_prunes_past_display_rows_tz_aware_with_bad_zone_isolation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One ``run_close_retention`` call over three sites.

    Site 1 (``Etc/GMT-14``, enabled): both yesterday display rows (including
    the hour just before local midnight) are gone; the midnight-exact and
    later-today display rows, and all scored rows, survive. Catches:
    ``<=`` used for ``<`` (would also delete the midnight-exact row), a
    UTC-midnight bound (would misclassify ``_HOUR_BEFORE_MIDNIGHT``, which is
    still "today" in UTC, as kept), a delete without the variable bind (would
    also delete the scored row), or an ``enabled = 1`` site filter (site 3
    below pins that this does not happen).

    Site 2 (zone that fails to load, enabled): every row -- display and
    scored, past and present -- survives, and the one-site-at-a-time catch
    logs a WARNING instead of raising. Catches: a fail-open zone handler that
    deletes anyway, and a zone error that escapes and rolls back the whole
    retention run (site 1's deletes, asserted below, happen in the SAME
    ``run_close_retention`` call as site 2's failing zone; if the error
    escaped per-site and hit ``run_close_retention``'s own handler, the
    ``ROLLBACK TO close_retention`` would undo site 1's deletes too).

    Site 3 (``Etc/GMT-14`` again, disabled): loses its yesterday display rows
    exactly like site 1. Catches: a site filter that only processes
    ``enabled = 1`` sites (a disabled site would then keep everything).
    """
    conn = _fresh_conn()
    feed_id = _feed_id(conn)

    site_good_zone = _insert_site(
        conn, name="GoodZoneEnabled", timezone="Etc/GMT-14", enabled=1
    )
    site_bad_zone = _insert_site(
        conn, name="BadZoneEnabled", timezone="Nowhere/Synthetic", enabled=1
    )
    site_disabled = _insert_site(
        conn, name="GoodZoneDisabled", timezone="Etc/GMT-14", enabled=0
    )

    for site_id in (site_good_zone, site_bad_zone, site_disabled):
        _seed_site_rows(conn, site_id=site_id, feed_id=feed_id)
    conn.commit()

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.close_chain"):
        run_close_retention(conn, now_utc=_NOW_UTC)

    # Site 1: yesterday's display rows gone; midnight-exact, later-today and
    # every scored row survive.
    expected_site_good_zone = (
        {(variable, _AT_MIDNIGHT) for variable in _DISPLAY_VARIABLES}
        | {(variable, _LATER_TODAY) for variable in _DISPLAY_VARIABLES}
        | {(variable, _HOUR_BEFORE_MIDNIGHT) for variable in _SCORED_VARIABLES}
    )
    assert _rows(conn, site_good_zone) == expected_site_good_zone

    # Site 2: the zone never loads, so nothing is pruned -- the full
    # as-seeded set survives, display rows included.
    expected_site_bad_zone = (
        {(variable, _HOUR_BEFORE_MIDNIGHT) for variable in _DISPLAY_VARIABLES}
        | {(variable, _EARLIER_YESTERDAY) for variable in _DISPLAY_VARIABLES}
        | {(variable, _AT_MIDNIGHT) for variable in _DISPLAY_VARIABLES}
        | {(variable, _LATER_TODAY) for variable in _DISPLAY_VARIABLES}
        | {(variable, _HOUR_BEFORE_MIDNIGHT) for variable in _SCORED_VARIABLES}
    )
    assert _rows(conn, site_bad_zone) == expected_site_bad_zone
    assert any(
        record.levelno == logging.WARNING
        and "timezone does not load" in record.getMessage()
        and str(site_bad_zone) in record.getMessage()
        for record in caplog.records
    ), "expected the bad-zone WARNING naming the failing site"

    # Site 3: disabled, but in the SAME good zone as site 1 -- it must lose
    # its yesterday display rows exactly like site 1 did.
    assert _rows(conn, site_disabled) == expected_site_good_zone


def test_delete_past_display_samples_sql_seeks_the_full_index_key() -> None:
    """Catches: the delete's WHERE clause losing a bound (site_id, variable
    or valid_at), which would widen the SEARCH key or drop the index
    entirely. No ``ANALYZE`` is run on this fixture -- the plan must hold on
    a schema with no statistics gathered, not only on one the planner has
    been told how to size.
    """
    conn = _fresh_conn()
    feed_id = _feed_id(conn)
    site_id = _insert_site(conn, name="PlanPinSite", timezone="UTC", enabled=1)
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="cloud_cover",
        valid_at=_HOUR_BEFORE_MIDNIGHT,
    )

    plan = [
        str(row["detail"])
        for row in conn.execute(
            "EXPLAIN QUERY PLAN " + DELETE_PAST_DISPLAY_SAMPLES_SQL,
            (site_id, "cloud_cover", _BOUND),
        )
    ]
    assert any(
        "SEARCH forecast_samples USING" in line
        and "INDEX idx_samples_site_var_valid" in line
        and "(site_id=? AND variable=? AND valid_at<?)" in line
        for line in plan
    ), plan

"""The win rate and the history chart read ``forecast_history`` +
``observations`` directly, with persistence rebuilt on read (storage-release
plan 2026-11-02-storage-release.md, 3.5, 3.6, A2-A5; W4, plan line 298).

Written before W4's rewrite exists. ``forecast_history`` itself (W3) is
already built and is used here only as a fixture table -- these tests
exercise the READERS, not the table.

This fixture never writes a row into ``forecast_pairs`` or ``close_pairs``:
today's code (before W4) answers both the win rate and the history chart
from those two tables, so on this fixture it finds nothing and returns
empty results where this suite expects hand-computed ones -- a plain
assertion mismatch, not an exception. The two query-plan pin tests fail at
their own ``ImportError`` instead: the SQL constants they import do not
exist in ``wxverify.scoring.history`` yet.

Names this suite depends on, for the builder:

* ``wxverify.scoring.history._WINRATE_RANGE_SQL`` -- a new module-level SQL
  constant, plan 3.6's win-rate statement: closed and pre-close days of one
  (site, variable, day_ahead) together, joining ``forecast_history`` to
  ``observations`` and (via ``LEFT JOIN``) ``site_days``, searching
  ``forecast_history`` by its primary key.
* ``wxverify.scoring.history._CHART_RANGE_SQL`` -- a new module-level SQL
  constant, plan 3.6's chart range read: the same two joins and the same
  closed/pre-close condition, for one ``feed_id`` rather than the active-feed
  set, also searching ``forecast_history`` by its primary key.
* ``wxverify.scoring.winrate.winrate_with_evidence`` / ``winrate`` keep their
  existing public signatures (``site_id``, ``variable``, ``day_ahead``,
  ``window``) and reply shape (a list of dicts with ``feed_id``, ``source``,
  ``model``, ``covered``, ``comparable``, ``wins``, ``win_rate``); internally
  they now read ``_WINRATE_RANGE_SQL`` instead of ``close_pairs`` /
  ``forecast_pairs``, and merge ``scoring.history.persistence_picks_at``
  (3.5) into the tally in place of a stored persistence row.
* ``wxverify.api.routes.timeseries.timeseries`` (``GET /api/timeseries``)
  keeps its existing reply keys (``site``, ``variable``, ``feed_id``,
  ``valid_at``, ``forecast``, ``observed``, ``provisional_valid_at``,
  ``provisional``, ``provisional_latest_reading_at``, ``missed_days``,
  ``zone_days``) and drops the ``issued_at`` query parameter -- it is no
  longer read, so passing it changes nothing. The feed's series now comes
  from ``_CHART_RANGE_SQL``, with ``scoring.history.persistence_picks_at``
  (3.5) standing in for the series whenever the requested ``feed_id`` is the
  persistence yardstick feed (``feeds.source='virtual'``,
  ``feeds.model='_persistence'``), since ``forecast_history`` itself never
  holds a persistence row (3.3).
* ``wxverify.api.routes.health.worker_status`` (``GET
  /api/worker/status?counts=exact``) reports a new key
  ``forecast_history_rows`` (an exact ``COUNT(*) FROM forecast_history``) in
  place of ``forecast_pairs_rows``.
* With no ``site_days`` record at all (the site has not had its first
  record yet), the pre-close bound both statements compare ``local_date``
  against is the literal date ``9999-12-31``, not ``NULL`` -- passing
  ``NULL`` into ``fh.local_date < ?`` makes the whole comparison, and hence
  the whole ``OR``, evaluate to SQL ``NULL`` (falsy), silently excluding
  every pre-close row.

Real SQLite throughout (``tests.helpers.asof_conn``, or a file database
under ``tmp_path`` wired through ``wxverify.api.app.create_app`` for the
HTTP-level tests); nothing in the code under test is mocked. ``utc_now`` is
pinned so the provisional line never reaches past this fixture's one
``open`` day. Synthetic data only: feed models ``fh-reader-x`` /
``fh-reader-y``, zone UTC, dates built off the shared ``D`` fixture date
from ``tests.helpers``.

Scenario shared by every test in this file (``_build_readers_scenario``):
one hourly observation a day, always at 12:00 UTC, on seven consecutive
days P0..M (P0 is a source-only seed for persistence's one-day lead; it is
never itself a target). The sole available persistence lead at day_ahead 1
is exactly 24 hours (one observation a day), so the win rate's
shortest-lead pick and the chart's longest-lead pick are the same cell
throughout -- this keeps every expected value hand-computable from a single
formula (the previous day's own observation). Feeds X and Y each carry one
``forecast_history`` row a day at ``day_ahead=1``; D, D+1 and D+2 are
``closed``; D+3 (``M``) is ``missed`` even though it carries
``forecast_history`` rows (R8); D-3, D-2 and D-1 carry no ``site_days`` row
at all (pre-close) and precede the site's first record (D).
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    D,
    asof_conn,
    asof_insert_observation,
    asof_make_real_feed,
    asof_persistence_feed_id,
    day_close_site,
    note_day_clock,
    seed_active_publication,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import close_db, get_db
from wxverify.db.site_days import (
    begin_close,
    ensure_site_days,
    finish_close,
    mark_missed,
    start_site_days,
)

# ---------------------------------------------------------------------------
# Shared scenario dates: P0 (persistence-source-only seed), P1-P3 (pre-close,
# no site_days row), C0-C2 (closed), M (missed, but carries rows -- R8),
# OPEN_DAY (the one trailing open record "now" lives inside).
# ---------------------------------------------------------------------------

P0: Final = D - timedelta(days=4)
P1: Final = D - timedelta(days=3)
P2: Final = D - timedelta(days=2)
P3: Final = D - timedelta(days=1)
C0: Final = D
C1: Final = D + timedelta(days=1)
C2: Final = D + timedelta(days=2)
M: Final = D + timedelta(days=3)
OPEN_DAY: Final = D + timedelta(days=4)

_HOUR: Final = 12
_CHART_DAYS: Final = (P1, P2, P3, C0, C1, C2)

# obs(day), one value a day; chosen strictly increasing so persistence's
# always-24h-lead forecast (the previous day's own value) never ties a real
# feed's abs_error on any day below.
_OBS: Final = {
    P0: 10.0,
    P1: 11.0,
    P2: 12.0,
    P3: 13.0,
    C0: 14.0,
    C1: 15.0,
    C2: 16.0,
    M: 17.0,
}

# Real-feed forecast values, by model name then day. Chosen so each day has
# a distinct, tie-free winner among {X, Y, persistence}.
_X_FORECAST: Final = {
    P1: 11.5,
    P2: 14.0,
    P3: 13.0,
    C0: 16.0,
    C1: 15.2,
    C2: 18.0,
    M: 99.0,
}
_Y_FORECAST: Final = {
    P1: 9.0,
    P2: 12.3,
    P3: 10.0,
    C0: 13.9,
    C1: 17.0,
    C2: 16.1,
    M: 88.0,
}

# Hand-computed persistence forecast per day: the previous day's own
# observation (the only lead -- 24h -- that lands at day_ahead 1 here).
_PERSISTENCE_FORECAST: Final = {
    day: _OBS[day - timedelta(days=1)] for day in _CHART_DAYS
}

# Hand-computed per-day winner among X, Y and persistence (smallest
# abs_error), used by the win-rate test.
_WINNER: Final = {P1: "x", P2: "y", P3: "x", C0: "y", C1: "x", C2: "y"}


def _stamp(local_date: date, hour: int) -> str:
    return isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC)
    )


def _insert_forecast_history(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    value: float,
    day_ahead: int = 1,
) -> None:
    conn.execute(
        "INSERT INTO forecast_history"
        " (site_id, feed_id, variable, day_ahead, valid_at, local_date,"
        "  value, lead_hours, issued_at, contributors)"
        " VALUES (?, ?, 'temperature', ?, ?, ?, ?, 36, '2024-01-01T00:00:00Z', NULL)",
        (
            site_id,
            feed_id,
            day_ahead,
            _stamp(local_date, _HOUR),
            local_date.isoformat(),
            value,
        ),
    )


def _close_day(conn: sqlite3.Connection, site_id: int, local_date: date) -> None:
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, local_date, now_utc=t)
    finish_close(conn, site_id, local_date, now_utc=t)


def _miss_day(conn: sqlite3.Connection, site_id: int, local_date: date) -> None:
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    mark_missed(conn, site_id, local_date, now_utc=t, reason="never_attempted")


def _seed_day_score(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    feed_id: int,
    variable: str = "temperature",
    day_ahead: int = 1,
    status: str = "scored",
) -> None:
    conn.execute(
        "INSERT INTO day_scores"
        " (site_id, local_date, feed_id, variable, day_ahead, status,"
        "  download_id, download_date, fetched_at, n, err_n, err_sum,"
        "  abs_sum, sq_sum, hits, false_alarms, misses, correct_negs,"
        "  skill_n, skill_sq_sum, skill_ref_sq_sum, wind_fc_high,"
        "  wind_fc_hours, wind_obs_high)"
        " VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 1, 1, 2.0, 2.0, 4.0,"
        "  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        (site_id, local_date.isoformat(), feed_id, variable, day_ahead, status),
    )


def _build_readers_scenario(
    conn: sqlite3.Connection, *, c2_persistence_status: str = "scored"
) -> tuple[int, int, int, int]:
    """One site with X and Y's forecasts over P1-M, D-D+2 closed, M missed.

    Returns ``(site_id, feed_x, feed_y, persistence_id)``.
    """
    site_id = day_close_site(conn, timezone="UTC", with_records=False)
    feed_x = asof_make_real_feed(conn, "fh-reader-x")
    feed_y = asof_make_real_feed(conn, "fh-reader-y")
    persistence_id = asof_persistence_feed_id(conn)

    for day, value in _OBS.items():
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at=_stamp(day, _HOUR),
            value=value,
            computed_at=_stamp(day, _HOUR),
        )

    for day in (*_CHART_DAYS, M):
        _insert_forecast_history(
            conn,
            site_id=site_id,
            feed_id=feed_x,
            local_date=day,
            value=_X_FORECAST[day],
        )
        _insert_forecast_history(
            conn,
            site_id=site_id,
            feed_id=feed_y,
            local_date=day,
            value=_Y_FORECAST[day],
        )

    start_site_days(
        conn, site_id, C0, now_utc=datetime(C0.year, C0.month, C0.day, tzinfo=UTC)
    )
    ensure_site_days(
        conn,
        site_id,
        now_utc=note_day_clock(
            datetime(OPEN_DAY.year, OPEN_DAY.month, OPEN_DAY.day, _HOUR, tzinfo=UTC)
        ),
    )
    _close_day(conn, site_id, C0)
    _close_day(conn, site_id, C1)
    _close_day(conn, site_id, C2)
    _miss_day(conn, site_id, M)

    # Persistence's day_scores row (plan 3.5): a closed day's hour counts
    # toward a rebuilt persistence cell only where its own persistence
    # day_scores row of the variable and day ahead is 'scored' -- the exact
    # set stage 5 writes for the day's counted variables. This fixture
    # exercises the readers directly, never stage 5 itself, so the row is
    # seeded by hand.
    _seed_day_score(conn, site_id=site_id, local_date=C0, feed_id=persistence_id)
    _seed_day_score(conn, site_id=site_id, local_date=C1, feed_id=persistence_id)
    _seed_day_score(
        conn,
        site_id=site_id,
        local_date=C2,
        feed_id=persistence_id,
        status=c2_persistence_status,
    )

    return site_id, feed_x, feed_y, persistence_id


def _now() -> datetime:
    return datetime(OPEN_DAY.year, OPEN_DAY.month, OPEN_DAY.day, _HOUR, tzinfo=UTC)


async def _idle_worker_coro(_db: object) -> None:
    await asyncio.Event().wait()


def _init_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    close_db()
    config.db_path = str(tmp_path / name)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_coro)
    monkeypatch.setattr("wxverify.api.routes.timeseries.utc_now", _now)


# ---------------------------------------------------------------------------
# 1 & 3. Win rate: hand-computed answers across pre-close + closed days,
# persistence rebuilt at the shortest lead, a missed day shown by neither
# (R8).
# ---------------------------------------------------------------------------


def test_winrate_reads_forecast_history_with_persistence_and_excludes_missed() -> None:
    """Win rate over P1-C2 (3 pre-close, 3 closed), day_ahead 1, window
    "all": X and Y each win 3 of 6 days, persistence (rebuilt at the
    shortest lead, 3.5) wins none, and M's rows (a missed day, R8) count
    nowhere."""
    from wxverify.scoring.winrate import winrate_with_evidence

    conn = asof_conn()
    site_id, feed_x, feed_y, persistence_id = _build_readers_scenario(conn)
    seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=M,
        published_at=datetime(M.year, M.month, M.day, 13, tzinfo=UTC),
        first_record_date=C0,
        window_days=10,
    )

    result = winrate_with_evidence(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    by_feed = {int(row["feed_id"]): row for row in result.rows}

    assert by_feed.keys() == {feed_x, feed_y, persistence_id}, (
        "M-fh-winrate-scope -> at this assertion: correct = exactly X, Y and"
        " persistence counted (6 pre-close/closed days' worth each), mutant"
        " (M's rows leak into the tally, or a pre-close/closed day's rows"
        f" are dropped) = {sorted(by_feed.keys())}"
    )

    x_wins = sum(1 for day in _CHART_DAYS if _WINNER[day] == "x")
    y_wins = sum(1 for day in _CHART_DAYS if _WINNER[day] == "y")
    assert x_wins == 3 and y_wins == 3  # sanity on the fixture itself

    assert by_feed[feed_x]["covered"] == 6
    assert by_feed[feed_x]["comparable"] == 6
    assert by_feed[feed_x]["wins"] == pytest.approx(float(x_wins))
    assert by_feed[feed_x]["win_rate"] == pytest.approx(x_wins / 6)

    assert by_feed[feed_y]["covered"] == 6
    assert by_feed[feed_y]["comparable"] == 6
    assert by_feed[feed_y]["wins"] == pytest.approx(float(y_wins))
    assert by_feed[feed_y]["win_rate"] == pytest.approx(y_wins / 6)

    assert by_feed[persistence_id]["covered"] == 6
    assert by_feed[persistence_id]["comparable"] == 6
    assert by_feed[persistence_id]["wins"] == pytest.approx(0.0), (
        "M-fh-persistence-shortest -> at this assertion: correct = 0.0"
        " persistence wins (its rebuilt abs_error is never the smallest"
        " here), mutant (persistence rebuilt at the wrong lead, double-"
        f" counted, or not merged at all) = {by_feed[persistence_id]['wins']}"
    )
    assert by_feed[persistence_id]["win_rate"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 2 & 3. The history chart: hand-computed series for a real feed and for
# persistence (longest lead), a missed day's rows shown by neither.
# ---------------------------------------------------------------------------


def test_history_chart_reads_forecast_history_for_a_real_feed_and_excludes_a_missed_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The chart for feed X over P1-M: the 6 pre-close/closed days' own
    forecast/observed pairs, oldest first; M's rows (missed, R8) absent."""
    _init_app(tmp_path, monkeypatch, "fh-readers-chart-x.db")
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id, feed_x, _feed_y, _persistence_id = db.write_sync(
            _build_readers_scenario
        )
        response = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "D+1",
                "from": f"{P1.isoformat()}T00:00:00Z",
                "to": f"{M.isoformat()}T23:59:59Z",
            },
        )

    assert response.status_code == 200
    body = response.json()
    expected_valid_at = [_stamp(day, _HOUR) for day in _CHART_DAYS]
    expected_forecast = [_X_FORECAST[day] for day in _CHART_DAYS]
    expected_observed = [_OBS[day] for day in _CHART_DAYS]

    assert body["valid_at"] == expected_valid_at
    assert body["forecast"] == pytest.approx(expected_forecast)
    assert body["observed"] == pytest.approx(expected_observed)
    assert _stamp(M, _HOUR) not in body["valid_at"], (
        "M-fh-chart-missed -> at this assertion: correct = M's hour absent"
        " from the chart (R8: a missed day's forecast_history rows are"
        " shown by neither reader), mutant (the state check is dropped, so"
        f" M's row rides along) = {body['valid_at']}"
    )


def test_history_chart_shows_rebuilt_persistence_at_the_longest_lead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The chart for the persistence yardstick feed: ``forecast_history``
    never stores a persistence row (3.3), so every value here comes from
    ``persistence_picks_at`` -- the previous day's own observation."""
    _init_app(tmp_path, monkeypatch, "fh-readers-chart-persistence.db")
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id, _feed_x, _feed_y, persistence_id = db.write_sync(
            _build_readers_scenario
        )
        response = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": persistence_id,
                "lead": "D+1",
                "from": f"{P1.isoformat()}T00:00:00Z",
                "to": f"{M.isoformat()}T23:59:59Z",
            },
        )

    assert response.status_code == 200
    body = response.json()
    expected_valid_at = [_stamp(day, _HOUR) for day in _CHART_DAYS]
    expected_forecast = [_PERSISTENCE_FORECAST[day] for day in _CHART_DAYS]
    expected_observed = [_OBS[day] for day in _CHART_DAYS]

    assert body["valid_at"] == expected_valid_at, (
        "M-fh-chart-persistence-rows -> at this assertion: correct = one"
        " rebuilt row per pre-close/closed day, mutant (persistence never"
        f" merged in, so the series is empty) = {body['valid_at']}"
    )
    assert body["forecast"] == pytest.approx(expected_forecast), (
        "correct = the previous day's own observation (the only lead"
        " available in this scenario, so shortest == longest here -- a"
        " shortest-vs-longest swap is NOT caught by this assertion; see"
        " test_winrate_shortest_lead_differs_from_chart_longest_lead_pick,"
        f" which uses two observation hours a day) = {body['forecast']}"
    )
    assert body["observed"] == pytest.approx(expected_observed)


def test_history_chart_omits_persistence_cell_for_an_unscored_closed_day() -> None:
    """Paired negative with the rebuilt-persistence test above: a closed
    day's persistence ``day_scores`` row still exists but is not ``scored``
    (3.5) -- that day alone drops its persistence cell; the other closed
    days, whose rows ARE ``scored``, keep theirs."""
    from wxverify.scoring.history import chart_pairs

    conn = asof_conn()
    site_id, _feed_x, _feed_y, persistence_id = _build_readers_scenario(
        conn, c2_persistence_status="not_counted"
    )

    rows = chart_pairs(
        conn,
        site_id=site_id,
        feed_id=persistence_id,
        variable="temperature",
        day_ahead=1,
        low=datetime(P1.year, P1.month, P1.day, tzinfo=UTC),
        high=datetime(M.year, M.month, M.day, 23, 59, 59, tzinfo=UTC),
    )
    valid_ats = [row[0] for row in rows]

    assert _stamp(C2, _HOUR) not in valid_ats, (
        "M-fh-persistence-unscored -> at this assertion: correct = C2's"
        " persistence cell absent, mutant (every closed day's row shown"
        " regardless of day_scores status) = "
        f"{_stamp(C2, _HOUR) in valid_ats}"
    )
    for day in (P1, P2, P3, C0, C1):
        assert _stamp(day, _HOUR) in valid_ats, (
            f"{day} is pre-close or has a 'scored' day_scores row and must"
            f" still show its persistence cell -- {valid_ats}"
        )


# ---------------------------------------------------------------------------
# 1 (edge case named in W4's own description). With no site_days record at
# all, the pre-close bound is the literal 9999-12-31, not NULL/skipped.
# ---------------------------------------------------------------------------


def test_history_chart_includes_everything_as_pre_close_with_no_record_yet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A site with the close on but no ``site_days`` row at all (no record
    has ever started): a stored ``forecast_history`` row is still shown,
    proving the pre-close bound is a real date (9999-12-31) rather than a
    ``NULL`` that would make ``fh.local_date < NULL`` -- and so the whole
    inclusion ``OR`` -- silently false."""
    close_db()
    config.db_path = str(tmp_path / "fh-readers-no-record.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_coro)
    monkeypatch.setattr(
        "wxverify.api.routes.timeseries.utc_now",
        lambda: datetime(P1.year, P1.month, P1.day, _HOUR, tzinfo=UTC),
    )
    app = create_app(root_path="")

    def _build(conn: sqlite3.Connection) -> tuple[int, int]:
        site_id = day_close_site(conn, timezone="UTC", with_records=False)
        feed_x = asof_make_real_feed(conn, "fh-reader-norecord-x")
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at=_stamp(P1, _HOUR),
            value=11.0,
            computed_at=_stamp(P1, _HOUR),
        )
        _insert_forecast_history(
            conn, site_id=site_id, feed_id=feed_x, local_date=P1, value=11.5
        )
        return site_id, feed_x

    with TestClient(app) as client:
        db = get_db()
        site_id, feed_x = db.write_sync(_build)
        response = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "D+1",
                "from": f"{P1.isoformat()}T00:00:00Z",
                "to": f"{P1.isoformat()}T23:59:59Z",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["valid_at"] == [_stamp(P1, _HOUR)], (
        "M-fh-no-record-bound -> at this assertion: correct = the row"
        " present (no site_days row at all means every row is pre-close),"
        " mutant (a NULL pre-close bound instead of 9999-12-31 makes the"
        f" inclusion OR silently false) = {body['valid_at']}"
    )
    assert body["forecast"] == pytest.approx([11.5])
    assert body["observed"] == pytest.approx([11.0])


# ---------------------------------------------------------------------------
# 4. The issued_at query parameter has left the timeseries API.
# ---------------------------------------------------------------------------


def test_timeseries_issued_at_query_parameter_has_no_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Today, passing ``issued_at`` with a literal that matches no stored
    row empties the whole close-on reply (it both filters the starting
    history and suppresses every closed day). After W4 the parameter is
    never read, so the reply is identical whether or not it is passed."""
    _init_app(tmp_path, monkeypatch, "fh-readers-issued-at.db")
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id, feed_x, _feed_y, _persistence_id = db.write_sync(
            _build_readers_scenario
        )
        params = {
            "site": site_id,
            "variable": "temperature",
            "feed_id": feed_x,
            "lead": "D+1",
            "from": f"{P1.isoformat()}T00:00:00Z",
            "to": f"{M.isoformat()}T23:59:59Z",
        }
        baseline = client.get("/api/timeseries", params=params)
        with_bogus_issued_at = client.get(
            "/api/timeseries", params={**params, "issued_at": "1999-01-01T00:00:00Z"}
        )

    assert baseline.status_code == 200
    assert with_bogus_issued_at.status_code == 200
    assert baseline.json()["valid_at"], "fixture produced no rows; the check is vacuous"
    assert with_bogus_issued_at.json() == baseline.json(), (
        "M-fh-issued-at-gone -> at this assertion: correct = an identical"
        " reply whether or not issued_at is passed (it has left the"
        " timeseries API, plan A4), mutant (today's code still reads"
        " issued_at and, matching no row, empties the starting history and"
        f" suppresses every closed day) = {with_bogus_issued_at.json()}"
    )


# ---------------------------------------------------------------------------
# 5. The opt-in exact health count reports forecast_history_rows.
# ---------------------------------------------------------------------------


def test_worker_status_exact_counts_reports_forecast_history_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``GET /api/worker/status?counts=exact`` reports ``forecast_history_rows``
    equal to the seeded row count (A5)."""
    _init_app(tmp_path, monkeypatch, "fh-readers-health.db")
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(_build_readers_scenario)
        expected = db.write_sync(
            lambda conn: int(
                conn.execute("SELECT COUNT(*) AS n FROM forecast_history").fetchone()[
                    "n"
                ]
            )
        )
        response = client.get("/api/worker/status", params={"counts": "exact"})

    assert response.status_code == 200
    body = response.json()
    assert expected > 0, (
        "fixture produced no forecast_history rows; the check is vacuous"
    )
    assert body["forecast_history_rows"] == expected, (
        "M-fh-health-count -> at this assertion: correct = the exact"
        f" forecast_history row count ({expected}), mutant (the key is"
        " missing, still named forecast_pairs_rows, or counts a different"
        f" table) = {body.get('forecast_history_rows')}"
    )


# ---------------------------------------------------------------------------
# Query-plan pins for both statements live in test_query_plan_regressions.py
# (test_winrate_range_sql_seeks_forecast_history_by_primary_key,
# test_chart_range_sql_seeks_forecast_history_by_primary_key) -- kept there
# only, since CI's shipping-SQLite job runs just that file and
# test_index_predicate_reconciliation.py.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 7 & 8. A separate, small scenario with TWO observation hours a day: the
# shared scenario above has only one, so its one persistence lead at
# day_ahead 1 is always both the shortest and the longest (3.5's two picks
# never differ there, see the rebuilt-persistence test's own comment
# above). The win rate's date filter (``_WINRATE_RANGE_SQL``'s
# ``forecast_history.local_date BETWEEN ? AND ?`` clause) is also
# untested on the shared scenario, which only ever queries windows that
# already include every seeded day.
# ---------------------------------------------------------------------------

_LEAD_SOURCE_DAY: Final = D
_LEAD_TARGET_DAY: Final = D + timedelta(days=1)


def _build_two_hour_lead_scenario(conn: sqlite3.Connection) -> tuple[int, int]:
    """One site, one closed day (``_LEAD_TARGET_DAY``), with two observation
    hours a day on the source day (``_LEAD_SOURCE_DAY``) -- giving the
    target's hour-12 instant two different-lead day-ahead-1 persistence
    cells: 24h (source hour 12) and 36h (source hour 0). Returns
    ``(site_id, persistence_id)``.
    """
    site_id = day_close_site(conn, timezone="UTC", with_records=False)
    persistence_id = asof_persistence_feed_id(conn)

    asof_insert_observation(
        conn,
        site_id=site_id,
        valid_at=_stamp(_LEAD_SOURCE_DAY, 0),
        value=10.0,
        computed_at=_stamp(_LEAD_SOURCE_DAY, 0),
    )
    asof_insert_observation(
        conn,
        site_id=site_id,
        valid_at=_stamp(_LEAD_SOURCE_DAY, 12),
        value=20.0,
        computed_at=_stamp(_LEAD_SOURCE_DAY, 12),
    )
    asof_insert_observation(
        conn,
        site_id=site_id,
        valid_at=_stamp(_LEAD_TARGET_DAY, 12),
        value=30.0,
        computed_at=_stamp(_LEAD_TARGET_DAY, 12),
    )

    start_site_days(
        conn,
        site_id,
        _LEAD_TARGET_DAY,
        now_utc=datetime(
            _LEAD_TARGET_DAY.year,
            _LEAD_TARGET_DAY.month,
            _LEAD_TARGET_DAY.day,
            tzinfo=UTC,
        ),
    )
    ensure_site_days(
        conn,
        site_id,
        now_utc=note_day_clock(
            datetime(
                _LEAD_TARGET_DAY.year,
                _LEAD_TARGET_DAY.month,
                _LEAD_TARGET_DAY.day,
                23,
                tzinfo=UTC,
            )
        ),
    )
    _close_day(conn, site_id, _LEAD_TARGET_DAY)
    _seed_day_score(
        conn, site_id=site_id, local_date=_LEAD_TARGET_DAY, feed_id=persistence_id
    )
    return site_id, persistence_id


def test_winrate_shortest_lead_differs_from_chart_longest_lead_pick() -> None:
    """With two observation hours a day, the target hour's day-ahead-1
    persistence cell has two different-lead candidates (24h and 36h): the
    win rate (``winrate_errors``, whose ``_persistence_picks`` call passes
    ``longest=False``) must use the 24h source (the previous day's hour 12,
    20.0); the history chart (``chart_pairs``, whose ``_persistence_picks``
    call passes ``longest=True``) must use the 36h source (the previous
    day's hour 0, 10.0). The shared scenario's one-observation-a-day
    fixture cannot tell these apart -- see
    test_history_chart_shows_rebuilt_persistence_at_the_longest_lead's own
    comment, "shortest == longest here"."""
    from wxverify.scoring.history import chart_pairs, winrate_errors

    conn = asof_conn()
    site_id, persistence_id = _build_two_hour_lead_scenario(conn)

    errors = winrate_errors(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        start=_LEAD_TARGET_DAY,
        end=_LEAD_TARGET_DAY,
    )
    persistence_errors = [e for e in errors if e.feed_id == persistence_id]
    assert len(persistence_errors) == 1, errors
    assert persistence_errors[0].abs_error == 10.0, (
        "M-lead-shortest -> at this assertion: correct = 10.0 (abs(20.0 -"
        " 30.0), the 24h/shortest-lead source), mutant (the chart's"
        " longest-lead pick used instead, giving abs(10.0 - 30.0) = 20.0) ="
        f" {persistence_errors[0].abs_error}"
    )

    series = chart_pairs(
        conn,
        site_id=site_id,
        feed_id=persistence_id,
        variable="temperature",
        day_ahead=1,
        low=None,
        high=None,
    )
    chart_row = next(row for row in series if row[0] == _stamp(_LEAD_TARGET_DAY, 12))
    assert chart_row[1] == 10.0, (
        "M-lead-longest -> at this assertion: correct = 10.0 (the 36h/"
        "longest-lead source), mutant (the win rate's shortest-lead pick"
        f" used instead, giving 20.0) = {chart_row[1]}"
    )


def test_winrate_date_filter_excludes_rows_before_and_after_the_window() -> None:
    """The win rate's date filter (``_WINRATE_RANGE_SQL``'s
    ``forecast_history.local_date BETWEEN ? AND ?`` clause) excludes a
    closed day before the window start (C0) and one after the window end
    (C2), leaving only C1's rows -- on the shared scenario, which seeds a
    real row for X and Y on every one of P1-C2.
    """
    from wxverify.scoring.history import winrate_errors

    conn = asof_conn()
    site_id, feed_x, feed_y, _persistence_id = _build_readers_scenario(conn)

    errors = winrate_errors(
        conn, site_id=site_id, variable="temperature", day_ahead=1, start=C1, end=C1
    )
    real_feed_errors = [e for e in errors if e.feed_id in (feed_x, feed_y)]
    dates = {parse_utc(e.valid_at).date() for e in real_feed_errors}
    assert dates == {C1}, (
        "M-winrate-date-filter -> at this assertion: correct = only C1's"
        " date (narrowed to [C1, C1]), mutant (the BETWEEN bound dropped or"
        f" widened, so C0 and/or C2 leak in) = {sorted(dates)}"
    )
    assert len(real_feed_errors) == 2, "exactly one row each for X and Y on C1"

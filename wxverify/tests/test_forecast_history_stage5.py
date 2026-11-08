"""``forecast_history`` (storage-release plan 2026-11-02-storage-release.md,
3.1) and stage 5's use of it (3.3, 3.6; W3, plan line 290).

Written before the table and stage 5's rewrite exist. Test 1 fails at its
own ``PRAGMA table_xinfo('forecast_history')`` returning no rows (the table
is absent); tests 2-4 fail inside ``score_day`` itself, which still writes
``close_pairs`` and has never heard of ``forecast_history`` (a bare
``sqlite3.OperationalError: no such table: forecast_history`` the moment the
new SELECT this suite issues runs, or simply an always-empty result because
stage 5 never wrote a row there); test 5 fails at its own
``from wxverify.scoring.close_scores import _DELETE_FORECAST_HISTORY_SQL``
with ``ImportError`` -- the name does not exist yet. These are the failures
Step 3 accepts.

Names this suite depends on, for the builder:

* a table ``forecast_history`` in ``create_tables``
  (``wxverify/db/migrations.py``), exactly as plan 3.1 gives it: columns
  ``site_id INTEGER NOT NULL``, ``feed_id INTEGER NOT NULL``,
  ``variable TEXT NOT NULL``, ``day_ahead INTEGER NOT NULL``,
  ``valid_at TEXT NOT NULL``, ``local_date TEXT NOT NULL``,
  ``value REAL NOT NULL``, ``lead_hours INTEGER NOT NULL``,
  ``issued_at TEXT``, ``contributors INTEGER``; primary key
  ``(site_id, feed_id, variable, day_ahead, valid_at)``; ``WITHOUT ROWID``.
* ``score_day`` (``wxverify/scoring/close_scores.py``) writes
  ``forecast_history`` in place of ``close_pairs``: real feeds' rows with
  ``valid_at`` the observation hour's stored text, the mean's rows with
  ``issued_at`` NULL and ``contributors`` set, no persistence rows.
* a module-level SQL constant ``_DELETE_FORECAST_HISTORY_SQL`` in
  ``wxverify/scoring/close_scores.py``: the single parameterised retry-delete
  statement of plan 3.6, taking seven positional parameters in order
  ``(site_id, feed_id, variable, day_ahead, valid_at_lo, valid_at_hi,
  local_date)``, searching ``forecast_history`` by its primary key.

Real SQLite throughout (``tests.helpers.asof_conn`` / a fresh
``:memory:`` connection run through ``run_migrations``); nothing in the code
under test is mocked. Synthetic data only: feed sources
``synthetic-fh-x``/``synthetic-fh-y``, zone UTC, the shared ``D`` fixture
date from ``tests.helpers``. Scored variable is temperature only (wind and
rain truth rows are seeded ineligible, so ``scored_variables`` returns only
``{"temperature"}``, 4.6/6.5) -- this keeps every expected row
hand-computable from two plain functions (``_obs``, the ``_BIAS`` table)
independent of ``close_scores``'s own pair-building helpers.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Final

from tests.helpers import D, asof_conn, day_close_site, note_day_clock
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.site_days import (
    begin_close,
    day_record,
    ensure_site_days,
    finish_close,
    start_site_days,
)
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# Shared scenario: feeds X and Y, temperature only, two day aheads, four
# hours of D -- small enough to hand-compute every expected row.
# ---------------------------------------------------------------------------

_HOURS: Final = (0, 6, 12, 18)
_DAY_AHEADS: Final = (0, 1)
_BIAS: Final = {"X": 1.0, "Y": -0.5}
_FEED_MAX_LEAD_HOURS: Final = 200


def _obs(hour: int) -> float:
    return 10.0 + hour


def _stamp(local_date: date, hour: int) -> str:
    return isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC)
    )


def _label(local_date: date, day_ahead: int) -> str:
    return f"{(local_date - timedelta(days=day_ahead + 1)).isoformat()}T00:00:00Z"


def _download_date(local_date: date, day_ahead: int) -> date:
    return local_date - timedelta(days=day_ahead)


def _make_feed(conn: sqlite3.Connection, name: str) -> int:
    source = f"synthetic-fh-{name.lower()}"
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 0, 360, ?, 0)",
        (source, source, _FEED_MAX_LEAD_HOURS),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )


def _prepare_closing_day(
    conn: sqlite3.Connection, site_id: int, *, first_date, target_date
):
    """Start ``site_days`` at ``first_date``, close every day up to (not
    including) ``target_date``, and leave ``target_date`` in ``closing``
    (mirrors ``tests/test_close_scores.py``'s ``_prepare_closing_day``)."""
    start_dt = datetime(first_date.year, first_date.month, first_date.day, tzinfo=UTC)
    start_site_days(conn, site_id, first_date, now_utc=start_dt)
    far_future = note_day_clock(
        datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        + timedelta(days=2)
    )
    ensure_site_days(conn, site_id, now_utc=far_future)
    d = first_date
    while d < target_date:
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d.isoformat()),
        ).fetchone()
        assert row is not None
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=5)
        begin_close(conn, site_id, d, now_utc=t)
        finish_close(conn, site_id, d, now_utc=t)
        d += timedelta(days=1)
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, target_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, target_date, now_utc=t)
    record = day_record(conn, site_id, target_date)
    assert record is not None and record.state == "closing"
    return record, t


def _seed_truth(
    conn: sqlite3.Connection, site_id: int, local_date: date, *, generation_id: int
) -> None:
    """D's five ``daily_truth`` rows: temperature eligible and well covered
    (so ``scored_variables`` counts it), wind and rain explicitly
    ineligible (so only temperature is scored -- 6.5)."""
    start = f"{local_date.isoformat()}T00:00:00Z"
    end = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
        + timedelta(days=1)
    )
    rows = [
        ("temperature_high", 30.0, True, 24, None, None),
        ("temperature_low", 10.0, True, 24, None, None),
        ("wind_max", 14.5, False, 24, None, None),
        ("precip_total", 0.0, False, 24, 0, 24),
        ("precip_occurrence", 0.0, False, 24, 0, 24),
    ]
    for quantity, value, eligible, covered_hours, wet, dry in rows:
        conn.execute(
            "INSERT INTO daily_truth"
            " (site_id, local_date, quantity, value, eligible, covered_hours,"
            "  expected_slots, wet_hours, dry_hours, rain_threshold_mm,"
            "  day_start_utc, day_end_utc, timezone, tz_generation_id)"
            " VALUES (?, ?, ?, ?, ?, ?, 24, ?, ?, 0.25, ?, ?, 'UTC', ?)",
            (
                site_id,
                local_date.isoformat(),
                quantity,
                value,
                1 if eligible else 0,
                covered_hours,
                wet,
                dry,
                start,
                end,
                generation_id,
            ),
        )


def _seed_observations(
    conn: sqlite3.Connection, site_id: int, local_date: date, hours
) -> None:
    for hour in hours:
        valid_at = _stamp(local_date, hour)
        conn.execute(
            "INSERT INTO observations"
            " (site_id, variable, valid_at, value, n_stations, computed_at)"
            " VALUES (?, 'temperature', ?, ?, 2, ?)",
            (site_id, valid_at, _obs(hour), valid_at),
        )


def _seed_forecasts(
    conn: sqlite3.Connection,
    site_id: int,
    feeds: dict[str, int],
    local_date: date,
    hours,
    day_aheads,
) -> None:
    """Every feed's ``forecast_scored`` row for each day ahead and hour, as
    an inferred download (``download_id`` NULL): ``scored_values_download``
    accepts a NULL ``download_id`` with no ``forecast_downloads`` row
    (``scoring/downloads.py``), which this suite does not need."""
    for day_ahead in day_aheads:
        s = _download_date(local_date, day_ahead)
        label = _label(local_date, day_ahead)
        fetched_at = f"{s.isoformat()}T00:10:04Z"
        for hour in hours:
            valid_at = _stamp(local_date, hour)
            lead_hours = day_ahead * 24 + hour + 1
            for name, feed_id in feeds.items():
                conn.execute(
                    "INSERT INTO forecast_scored"
                    " (site_id, feed_id, variable, valid_at, day_ahead, value,"
                    "  lead_hours, issued_at, download_id, download_date, fetched_at)"
                    " VALUES (?, ?, 'temperature', ?, ?, ?, ?, ?, NULL, ?, ?)",
                    (
                        site_id,
                        feed_id,
                        valid_at,
                        day_ahead,
                        _obs(hour) + _BIAS[name],
                        lead_hours,
                        label,
                        s.isoformat(),
                        fetched_at,
                    ),
                )


def _build_scenario(conn: sqlite3.Connection):
    """One closing day D with X and Y's scored temperature forecasts at day
    aheads 0 and 1, four hours each."""
    site_id = day_close_site(conn, with_records=False)
    generation_id = ensure_site_generation(conn, site_id)
    feeds = {"X": _make_feed(conn, "X"), "Y": _make_feed(conn, "Y")}
    for feed_id in feeds.values():
        _subscribe(conn, site_id, feed_id)
    record, now_utc = _prepare_closing_day(
        conn, site_id, first_date=D - timedelta(days=10), target_date=D
    )
    _seed_truth(conn, site_id, D, generation_id=generation_id)
    _seed_observations(conn, site_id, D, _HOURS)
    _seed_forecasts(conn, site_id, feeds, D, _HOURS, _DAY_AHEADS)
    return site_id, feeds, record, now_utc


def _mean_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_multimodel_mean'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _persistence_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_persistence'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _expected_rows(
    site_id: int,
    feeds: dict[str, int],
    mean_feed_id: int,
    local_date: date,
    hours,
    day_aheads,
) -> list[tuple[object, ...]]:
    """Hand-built expected ``forecast_history`` rows for the scenario above,
    computed straight from ``_obs`` and ``_BIAS`` -- never by calling any of
    ``close_scores``'s pair-building helpers."""
    day_text = local_date.isoformat()
    rows: list[tuple[object, ...]] = []
    for day_ahead in day_aheads:
        for hour in hours:
            valid_at = _stamp(local_date, hour)
            lead_hours = day_ahead * 24 + hour + 1
            member_values: list[float] = []
            for name, feed_id in sorted(feeds.items(), key=lambda kv: kv[1]):
                value = _obs(hour) + _BIAS[name]
                member_values.append(value)
                rows.append(
                    (
                        site_id,
                        feed_id,
                        "temperature",
                        day_ahead,
                        valid_at,
                        day_text,
                        value,
                        lead_hours,
                        _label(local_date, day_ahead),
                        None,
                    )
                )
            mean_value = sum(member_values) / len(member_values)
            rows.append(
                (
                    site_id,
                    mean_feed_id,
                    "temperature",
                    day_ahead,
                    valid_at,
                    day_text,
                    mean_value,
                    lead_hours,
                    None,
                    len(member_values),
                )
            )
    return sorted(rows, key=lambda r: (r[1], r[3], r[4]))


def _forecast_history_rows(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> list[tuple[object, ...]]:
    return [
        (
            int(row["site_id"]),
            int(row["feed_id"]),
            str(row["variable"]),
            int(row["day_ahead"]),
            str(row["valid_at"]),
            str(row["local_date"]),
            float(row["value"]),
            int(row["lead_hours"]),
            row["issued_at"],
            row["contributors"],
        )
        for row in conn.execute(
            "SELECT site_id, feed_id, variable, day_ahead, valid_at, local_date,"
            " value, lead_hours, issued_at, contributors"
            " FROM forecast_history WHERE site_id = ? AND local_date = ?"
            " ORDER BY feed_id, day_ahead, valid_at",
            (site_id, local_date.isoformat()),
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# Test 1: the table exists, with the plan's columns, primary key and
# WITHOUT ROWID.
# ---------------------------------------------------------------------------


def test_forecast_history_table_matches_the_plan() -> None:
    """``forecast_history`` (plan 3.1): exact column set, NOT NULL flags,
    primary key order, and WITHOUT ROWID."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)

    columns = [
        (
            str(row["name"]),
            str(row["type"]).upper(),
            bool(row["notnull"]),
            int(row["pk"]),
        )
        for row in conn.execute("PRAGMA table_xinfo(forecast_history)")
    ]
    assert columns, "forecast_history does not exist (not yet created by create_tables)"

    expected = [
        ("site_id", "INTEGER", True),
        ("feed_id", "INTEGER", True),
        ("variable", "TEXT", True),
        ("day_ahead", "INTEGER", True),
        ("valid_at", "TEXT", True),
        ("local_date", "TEXT", True),
        ("value", "REAL", True),
        ("lead_hours", "INTEGER", True),
        ("issued_at", "TEXT", False),
        ("contributors", "INTEGER", False),
    ]
    actual = [(name, col_type, notnull) for name, col_type, notnull, _pk in columns]
    assert actual == expected, actual

    pk_columns = [
        name for name, _type, _notnull, pk in sorted(columns, key=lambda c: c[3]) if pk
    ]
    assert pk_columns == ["site_id", "feed_id", "variable", "day_ahead", "valid_at"], (
        pk_columns
    )

    schema_row = conn.execute(
        "SELECT sql FROM sqlite_schema"
        " WHERE type = 'table' AND name = 'forecast_history'"
    ).fetchone()
    assert schema_row is not None
    assert "WITHOUT ROWID" in str(schema_row["sql"]).upper()


# ---------------------------------------------------------------------------
# Test 2: stage 5 writes exactly the hand-built rows for one closed day.
# ---------------------------------------------------------------------------


def test_stage5_writes_forecast_history_real_feeds_and_mean_no_persistence() -> None:
    """``score_day`` writes ``forecast_history`` rows equal to the
    hand-built expected set: X and Y's real-feed rows, the mean's rows
    (``issued_at`` NULL, ``contributors`` 2), and no persistence rows even
    though the persistence yardstick feed is considered at D (3.3)."""
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc = _build_scenario(conn)
    mean_feed_id = _mean_feed_id(conn)
    persistence_feed_id = _persistence_feed_id(conn)

    score_day(conn, record=record, attempt=1, truth_version="fh-1", now_utc=now_utc)

    actual = _forecast_history_rows(conn, site_id, D)
    expected = _expected_rows(site_id, feeds, mean_feed_id, D, _HOURS, _DAY_AHEADS)
    assert actual == expected, (actual, expected)

    # Paired positive/negative: the real feeds and the mean DO appear (the
    # table is not simply empty), but the persistence yardstick -- present,
    # considered, and would have pairs under the old close_pairs rule --
    # never does.
    feed_ids_present = {row[1] for row in actual}
    assert feeds["X"] in feed_ids_present and feeds["Y"] in feed_ids_present
    assert mean_feed_id in feed_ids_present
    assert persistence_feed_id not in feed_ids_present, (
        "forecast_history must hold no persistence rows (plan 3.3), but"
        f" feed {persistence_feed_id} (persistence) appears: {actual}"
    )

    for row in actual:
        if row[1] == mean_feed_id:
            assert row[8] is None, f"the mean's issued_at must be NULL: {row}"
            assert row[9] == 2, f"the mean's contributors must be 2: {row}"
        else:
            assert row[8] is not None, f"a real feed's issued_at must be set: {row}"
            assert row[9] is None, f"a real feed's contributors must be NULL: {row}"


# ---------------------------------------------------------------------------
# Test 3: a retry replaces the day's rows exactly (3.6 delete).
# ---------------------------------------------------------------------------


def test_stage5_retry_replaces_forecast_history_rows_exactly() -> None:
    """A second ``score_day`` call for D, after one forecast value changed,
    leaves exactly the second run's rows for D: no duplicates of the old
    value, no leftovers, the changed cell's new value present once."""
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc = _build_scenario(conn)
    mean_feed_id = _mean_feed_id(conn)

    score_day(conn, record=record, attempt=1, truth_version="fh-1", now_utc=now_utc)
    first = _forecast_history_rows(conn, site_id, D)
    assert len(first) == len(_HOURS) * len(_DAY_AHEADS) * (len(feeds) + 1)

    changed_valid_at = _stamp(D, _HOURS[0])
    old_value = _obs(_HOURS[0]) + _BIAS["X"]
    new_value = old_value + 10.0
    updated = conn.execute(
        "UPDATE forecast_scored SET value = ?"
        " WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'"
        " AND day_ahead = 0 AND valid_at = ?",
        (new_value, site_id, feeds["X"], changed_valid_at),
    )
    assert updated.rowcount == 1

    record_after = day_record(conn, site_id, D)
    assert record_after is not None and record_after.state == "closing"
    score_day(
        conn, record=record_after, attempt=1, truth_version="fh-1", now_utc=now_utc
    )
    second = _forecast_history_rows(conn, site_id, D)

    # No duplicates and no leftovers: the row count is unchanged.
    assert len(second) == len(first), (
        "M-fh-retry -> at this assertion: correct = the same row count"
        f" after a retry, mutant (the delete missing a key, leaving"
        f" leftovers) = {len(first)} before vs {len(second)} after"
    )

    x_row = next(
        r
        for r in second
        if r[1] == feeds["X"] and r[3] == 0 and r[4] == changed_valid_at
    )
    assert x_row[6] == new_value, (
        "M-fh-retry2 -> at this assertion: correct = the new value"
        f" ({new_value}), mutant (the retry delete missing this key, so the"
        f" old row and the new row both remain or the old one survives) ="
        f" {x_row[6]}"
    )
    assert not any(
        r[1] == feeds["X"]
        and r[3] == 0
        and r[4] == changed_valid_at
        and r[6] == old_value
        for r in second
    ), "the old value must not survive the retry"

    mean_row = next(
        r
        for r in second
        if r[1] == mean_feed_id and r[3] == 0 and r[4] == changed_valid_at
    )
    expected_mean = (new_value + (_obs(_HOURS[0]) + _BIAS["Y"])) / 2
    assert mean_row[6] == expected_mean, (
        "the mean at the changed cell must reflect the new X value, not the"
        f" old one: got {mean_row[6]}, expected {expected_mean}"
    )


# ---------------------------------------------------------------------------
# Test 4: a neighbouring day's rows are untouched by the retry.
# ---------------------------------------------------------------------------


def test_stage5_retry_does_not_touch_a_neighbouring_days_rows() -> None:
    """Two neighbouring days' ``forecast_history`` rows, seeded directly,
    compare byte-for-byte before and after D's retry.

    The retry delete widens D's ``valid_at`` bounds by one day each side
    (3.6) and relies on ``AND local_date = ?`` to keep the match to D itself
    -- a delete missing that clause would still match nothing outside
    [D-1, D+1]. D-1 and D+1 are INSIDE that widened window (D-1's own
    ``valid_at`` sits exactly at the window's low bound, D+1's at its high
    bound less a day), with the same site/feed/variable/day_ahead the retry
    deletes for D, so only the ``local_date`` clause keeps them out; D-5
    (the previous version of this fixture) sits outside the window on
    ``valid_at`` alone and could never catch a dropped ``local_date``
    clause."""
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc = _build_scenario(conn)

    neighbours = (D - timedelta(days=1), D + timedelta(days=1))
    before: dict[date, list[tuple[object, ...]]] = {}
    for neighbour in neighbours:
        neighbour_valid_at = _stamp(neighbour, 0)
        conn.execute(
            "INSERT INTO forecast_history"
            " (site_id, feed_id, variable, day_ahead, valid_at, local_date,"
            "  value, lead_hours, issued_at, contributors)"
            " VALUES (?, ?, 'temperature', 0, ?, ?, 12.3, 1,"
            "  '2024-01-01T00:00:00Z', NULL)",
            (site_id, feeds["X"], neighbour_valid_at, neighbour.isoformat()),
        )
        rows = _forecast_history_rows(conn, site_id, neighbour)
        assert rows, "each neighbouring-day seed row must exist before the retry"
        before[neighbour] = rows

    score_day(conn, record=record, attempt=1, truth_version="fh-1", now_utc=now_utc)
    record_after = day_record(conn, site_id, D)
    assert record_after is not None and record_after.state == "closing"
    score_day(
        conn, record=record_after, attempt=1, truth_version="fh-1", now_utc=now_utc
    )

    for neighbour in neighbours:
        after = _forecast_history_rows(conn, site_id, neighbour)
        assert after == before[neighbour], (
            "M-fh-neighbour -> at this assertion: correct = the neighbouring"
            f" day {neighbour}'s rows unchanged ({before[neighbour]}), mutant"
            " (the retry delete drops 'AND local_date = ?', so a row inside"
            f" the widened valid_at window but on a different day is wiped) = {after}"
        )


# ---------------------------------------------------------------------------
# The retry delete's query-plan pin lives in test_query_plan_regressions.py
# (test_stage5_retry_delete_seeks_forecast_history_by_primary_key) -- kept
# there only, since CI's shipping-SQLite job runs just that file and
# test_index_predicate_reconciliation.py.
# ---------------------------------------------------------------------------

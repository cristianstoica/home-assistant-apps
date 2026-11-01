"""The rain history fill at upgrade (plan 2026-11-01-rain-release.md, Part C,
C6/C8; owner decision Q1 "b", 2026-11-01).

Written before Part C's schema (``rain_days``, ``rain_forecast_days``) and
``migrate_v16_rain_release``'s fill step exist: every test here fails --
once the migration itself converges -- on a missing table, or on a
row-count/exclusion assertion the fill does not yet satisfy.

A "v15 file" is built the way other migration tests do (see
``tests/test_precip_hour_start_boundary.py``'s ``monkeypatch.setattr(...,
"TARGET_USER_VERSION", n)`` pattern): ``wxverify.db.migrations.
TARGET_USER_VERSION`` is monkeypatched to 15 while ``run_migrations`` builds
the base schema, so the fixture is pinned at v15 regardless of what the
migration chain looks like by the time this file actually runs (if
``migrate_v16_rain_release`` already exists, a bare ``run_migrations`` call
would otherwise jump straight past v15). The monkeypatch is then undone (a
fresh ``monkeypatch`` per test takes care of that) and a second
``run_migrations`` call applies the real v15->v16 upgrade.

Historical rows are seeded directly via ``forecast_downloads``/
``forecast_samples`` (the ``outcome = 'stored'`` shape
``scoring.downloads.scored_download``/``scored_version`` read, the pattern
of ``tests/test_download_record.py``) and plain ``stations``/
``station_observations`` rows (no ``site_days``/``site_day_stations`` --
Part C's rain tables carry no foreign key to ``site_days``, and the plan's
own carry-over note says "no set-asides existed then, so every station
counts"). One ``site_days`` row, direct-inserted in the ``_v10_db`` style,
represents "today" as the single open day, so the fill has an oldest-open-
day boundary to count 60 days back from. Synthetic data only: feed sources
``synthetic-c8-f``/``open-meteo``, station ids ``ISTATION-C8A``/``-C8B``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.runtime_state import set_runtime_state

_TODAY = date(2024, 9, 1)  # the single open day -- the fill's "oldest open day"
_FAR = _TODAY - timedelta(days=60)  # farthest INCLUDED historical date
_TOO_FAR = _TODAY - timedelta(days=61)  # one day past the fill's window

# The three dates the "fewer than 20 filled days" feed gets data for.
_SPARSE_DATES = (
    _TODAY - timedelta(days=5),
    _TODAY - timedelta(days=6),
    _TODAY - timedelta(days=7),
)

# The open-meteo boundary pair: one date fetched before the rain-label
# boundary (excluded), one fetched after it (included) -- same feed.
_OM_BEFORE_DATE = _TODAY - timedelta(days=10)
_OM_AFTER_DATE = _TODAY - timedelta(days=9)
_BOUNDARY_STAMP = isoformat_utc(datetime(2024, 6, 1, tzinfo=UTC))
_OM_BEFORE_FETCHED = isoformat_utc(datetime(2024, 5, 1, tzinfo=UTC))
_OM_AFTER_FETCHED = isoformat_utc(datetime(2024, 7, 1, tzinfo=UTC))


def _insert_feed(conn: sqlite3.Connection, *, source: str, model: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 0, 360, 48, 0)",
        (source, model),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_stored_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    label: str,
    fetched_at: str,
) -> None:
    labels = json.dumps({str(feed_id): [label]})
    conn.execute(
        "INSERT INTO forecast_downloads"
        " (site_id, feed_id, local_date, slot, round_date, round_try,"
        "  started_at, ended_at, fetched_at, outcome, reason, usable, inserted, labels)"
        " VALUES (?, ?, ?, 'test', NULL, NULL, ?, ?, ?, 'stored', NULL, 1, 1, ?)",
        (
            site_id,
            feed_id,
            local_date.isoformat(),
            fetched_at,
            fetched_at,
            fetched_at,
            labels,
        ),
    )


def _seed_full_day_forecast(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    fetched_at: str,
    wet_hour_mm: float,
) -> None:
    """A 'stored' download of ``local_date`` whose 24 hourly samples cover
    the local day exactly, with the whole day's rain in hour 0 (so the
    total is simply ``wet_hour_mm``).
    """
    label = f"{local_date.isoformat()}T00:00:00Z"
    _seed_stored_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=local_date,
        label=label,
        fetched_at=fetched_at,
    )
    for hour in range(24):
        valid_at = isoformat_utc(
            datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
            + timedelta(hours=hour)
        )
        value = wet_hour_mm if hour == 0 else 0.0
        conn.execute(
            "INSERT INTO forecast_samples"
            " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
            "  value, source_raw, model_run_id, fetched_at)"
            " VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-1', ?)",
            (site_id, feed_id, label, valid_at, hour + 1, value, fetched_at),
        )


def _seed_station_truth(
    conn: sqlite3.Connection, site_id: int, local_date: date, *, rain: bool
) -> None:
    """2 stations at the site, each with 24 hours of 'ok' rain: both at 0.3
    mm (-> 'rain') or both at 0.0 mm (-> 'dry')."""
    per_hour = 0.3 if rain else 0.0
    for name in ("C8A", "C8B"):
        cur = conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
            (site_id, f"ISTATION-{name}-{local_date.isoformat()}"),
        )
        station_id = int(cur.lastrowid)
        for hour in range(24):
            valid_at = isoformat_utc(
                datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
                + timedelta(hours=hour)
            )
            conn.execute(
                "INSERT INTO station_observations"
                " (station_id, variable, valid_at, value, qc_flag, source_raw)"
                " VALUES (?, 'precip', ?, ?, 'ok', 'test')",
                (station_id, valid_at, per_hour),
            )


def _build_v15_fixture(monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    import wxverify.db.migrations as migrations

    monkeypatch.setattr(migrations, "TARGET_USER_VERSION", 15)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    migrations.run_migrations(conn)

    site_id = int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES ('C8 Site', 40.0, -105.0, 900.0, 'UTC')"
        ).lastrowid
    )
    # The single open day -- the fill's oldest-open-day boundary.
    stamp = isoformat_utc(datetime(_TODAY.year, _TODAY.month, _TODAY.day, tzinfo=UTC))
    start = stamp
    end = isoformat_utc(
        datetime(_TODAY.year, _TODAY.month, _TODAY.day, tzinfo=UTC) + timedelta(days=1)
    )
    conn.execute(
        "INSERT INTO site_days"
        " (site_id, local_date, state, day_start_utc, day_end_utc, timezone,"
        "  created_at, state_changed_at)"
        " VALUES (?, ?, 'open', ?, ?, 'UTC', ?, ?)",
        (site_id, _TODAY.isoformat(), start, end, stamp, stamp),
    )

    f = _insert_feed(conn, source="synthetic-c8-f", model="synthetic-c8-f")
    om = _insert_feed(conn, source="open-meteo", model="icon_seamless")

    # 1. The 60-day boundary pair, for feed F.
    for d in (_FAR, _TOO_FAR):
        _seed_full_day_forecast(
            conn,
            site_id=site_id,
            feed_id=f,
            local_date=d,
            fetched_at=isoformat_utc(datetime(2024, 6, 15, tzinfo=UTC)),
            wet_hour_mm=1.0,
        )
        _seed_station_truth(conn, site_id, d, rain=True)

    # 2. The sparse feed: exactly 3 dates, well inside the window.
    for d in _SPARSE_DATES:
        _seed_full_day_forecast(
            conn,
            site_id=site_id,
            feed_id=f,
            local_date=d,
            fetched_at=isoformat_utc(datetime(2024, 6, 15, tzinfo=UTC)),
            wet_hour_mm=1.0,
        )
        _seed_station_truth(conn, site_id, d, rain=True)

    # 3. Open-meteo's rain-label boundary: one date before it (must be
    #    excluded), one date after it (must be included).
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _BOUNDARY_STAMP)
    _seed_full_day_forecast(
        conn,
        site_id=site_id,
        feed_id=om,
        local_date=_OM_BEFORE_DATE,
        fetched_at=_OM_BEFORE_FETCHED,
        wet_hour_mm=1.0,
    )
    _seed_station_truth(conn, site_id, _OM_BEFORE_DATE, rain=True)
    _seed_full_day_forecast(
        conn,
        site_id=site_id,
        feed_id=om,
        local_date=_OM_AFTER_DATE,
        fetched_at=_OM_AFTER_FETCHED,
        wet_hour_mm=1.0,
    )
    _seed_station_truth(conn, site_id, _OM_AFTER_DATE, rain=True)

    conn.commit()
    return conn


def _rain_forecast_dates(conn: sqlite3.Connection, feed_id: int) -> set[str]:
    return {
        str(row["local_date"])
        for row in conn.execute(
            "SELECT local_date FROM rain_forecast_days"
            " WHERE feed_id = ? AND day_ahead = 0",
            (feed_id,),
        )
    }


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def test_fills_up_to_60_past_days_and_no_further(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _build_v15_fixture(monkeypatch)
    import wxverify.db.migrations as migrations

    migrations.run_migrations(conn)  # real TARGET_USER_VERSION: v15 -> v16

    f = _feed_id(conn, "synthetic-c8-f", "synthetic-c8-f")
    dates = _rain_forecast_dates(conn, f)
    assert _FAR.isoformat() in dates, "60 days back must be filled"
    assert _TOO_FAR.isoformat() not in dates, "61 days back must NOT be filled"

    rain_days = {
        str(row["local_date"])
        for row in conn.execute("SELECT local_date FROM rain_days")
    }
    assert _FAR.isoformat() in rain_days
    assert _TOO_FAR.isoformat() not in rain_days


def test_open_meteo_rain_before_the_boundary_is_excluded_after_is_included(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _build_v15_fixture(monkeypatch)
    import wxverify.db.migrations as migrations

    migrations.run_migrations(conn)

    om = _feed_id(conn, "open-meteo", "icon_seamless")
    dates = _rain_forecast_dates(conn, om)
    assert _OM_BEFORE_DATE.isoformat() not in dates, (
        "rain stored before the 0.16.12 label boundary must be left out"
    )
    assert _OM_AFTER_DATE.isoformat() in dates, (
        "rain stored after the boundary must be filled"
    )


def test_a_model_with_fewer_than_20_filled_days_keeps_exactly_its_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sparse feed has data on exactly 3 dates in-window: the fill must
    write exactly those 3 rows, never padding to 20 (or any other count)
    just because the badge threshold is 20 -- the fill does not imply 60,
    or even 20, usable days for every model.
    """
    conn = _build_v15_fixture(monkeypatch)
    import wxverify.db.migrations as migrations

    migrations.run_migrations(conn)

    f = _feed_id(conn, "synthetic-c8-f", "synthetic-c8-f")
    dates = _rain_forecast_dates(conn, f)
    sparse_isos = {d.isoformat() for d in _SPARSE_DATES}
    assert sparse_isos <= dates
    # Only the boundary-pair date (_FAR) and the 3 sparse dates were seeded
    # for F; _TOO_FAR must be excluded (test above), so F's total filled
    # count is exactly 4 -- never bumped up toward the 20-day badge
    # threshold.
    assert len(dates) == 4


def test_a_second_migration_run_changes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _build_v15_fixture(monkeypatch)
    import wxverify.db.migrations as migrations

    migrations.run_migrations(conn)
    before_forecast = {
        tuple(row)
        for row in conn.execute(
            "SELECT site_id, local_date, feed_id, day_ahead, total_mm"
            " FROM rain_forecast_days"
            " ORDER BY site_id, local_date, feed_id, day_ahead"
        )
    }
    before_days = {
        tuple(row)
        for row in conn.execute(
            "SELECT site_id, local_date, outcome, stations_full, stations_partial"
            " FROM rain_days ORDER BY site_id, local_date"
        )
    }

    # Force the fill to run again on the same, already-filled data (the
    # version gate alone would skip it on a second plain run_migrations
    # call; this exercises the fill step's own convergence directly).
    conn.execute("PRAGMA user_version = 15")
    migrations.run_migrations(conn)

    after_forecast = {
        tuple(row)
        for row in conn.execute(
            "SELECT site_id, local_date, feed_id, day_ahead, total_mm"
            " FROM rain_forecast_days"
            " ORDER BY site_id, local_date, feed_id, day_ahead"
        )
    }
    after_days = {
        tuple(row)
        for row in conn.execute(
            "SELECT site_id, local_date, outcome, stations_full, stations_partial"
            " FROM rain_days ORDER BY site_id, local_date"
        )
    }
    assert after_forecast == before_forecast
    assert after_days == before_days


def _sample_index_sql(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row["name"]): str(row["sql"])
        for row in conn.execute(
            "SELECT name, sql FROM sqlite_master"
            " WHERE type = 'index' AND tbl_name = 'forecast_samples'"
            " AND sql IS NOT NULL"
        )
    }


def test_the_upgrade_keeps_no_temporary_index_and_remakes_the_four_it_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """migrate_v16 runs its carry-over and fill with a temporary index and
    drops four forecast_samples indexes before its delete: after
    run_migrations the temporary index is gone and the four are back, with
    the definitions they had before."""
    conn = _build_v15_fixture(monkeypatch)
    monkeypatch.undo()  # the real TARGET_USER_VERSION for the upgrade
    import wxverify.db.migrations as migrations

    before = _sample_index_sql(conn)
    assert set(migrations.V16_DROPPED_SAMPLE_INDEXES) <= set(before), (
        "fixture sanity: the v15 file must hold the four indexes"
    )
    assert migrations.V16_FETCHED_INDEX not in before

    migrations.run_migrations(conn)

    after = _sample_index_sql(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 16
    assert migrations.V16_FETCHED_INDEX not in after
    for name in migrations.V16_DROPPED_SAMPLE_INDEXES:
        assert after.get(name) == before[name], f"{name} must be made again"


@pytest.mark.parametrize("failing_step", ["fill_rain_history", "keep_newest_samples"])
def test_a_failed_upgrade_rolls_back_both_index_steps(
    monkeypatch: pytest.MonkeyPatch, failing_step: str
) -> None:
    """A step that raises while the temporary index exists (the fill) or
    while the four are dropped (the delete): the rollback leaves the v15
    file's indexes as they were, with no temporary index."""
    conn = _build_v15_fixture(monkeypatch)
    monkeypatch.undo()  # the real TARGET_USER_VERSION for the upgrade
    import wxverify.collection.forecast_store as forecast_store
    import wxverify.db.migrations as migrations

    before = _sample_index_sql(conn)

    def _fail(_conn: sqlite3.Connection) -> int:
        raise RuntimeError(f"synthetic {failing_step} failure")

    monkeypatch.setattr(forecast_store, failing_step, _fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 15
    assert _sample_index_sql(conn) == before

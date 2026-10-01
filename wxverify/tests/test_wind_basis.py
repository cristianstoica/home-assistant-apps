"""The per-site wind basis state machine and its writer/reader gates
(plan §8-9, §15.1 T20-T28, T109, T110).

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test (mirrors test_forecast_service.py), except T24 which drives the real
``Database.replace_from`` swap path and therefore needs the async
``wxverify.db.connection.Database`` plus real tmp-path files.
"""

from __future__ import annotations

import ast
import asyncio
import logging
import re
import sqlite3
from pathlib import Path

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.db.wind_basis import (
    LEGACY_WIND_SQL,
    PAIR_MAX_SOURCE_PREFIX,
    PAIR_MAX_WIND_SQL,
    STATES,
    init_wind_basis,
    read_wind_cursor,
    set_wind_basis_state,
    transition_wind_basis,
    wind_basis_key,
    wind_blocked_key,
    wind_cursor_key,
    wind_done_at_key,
    wind_report_key,
)
from wxverify.scoring.consensus import (
    insert_station_observation,
    materialize_consensus,
    write_station_observation,
)
from wxverify.scoring.engine import _distinct_cells  # noqa: PLC2701 - direct unit test
from wxverify.scoring.pairing import compute_real_model_pairs
from wxverify.scoring.persistence import compute_persistence_pairs
from wxverify.worker.processor import _enabled_stations  # noqa: PLC2701


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str = "Testsite") -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds (source, model, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours)"
        " VALUES ('example-src', ?, 1, 360, 48)",
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


# --- T20 ---------------------------------------------------------------


def test_init_wind_basis_staging_stays_unchanged_no_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _make_db()
    site = _make_site(conn)
    set_wind_basis_state(conn, site, "staging")
    with caplog.at_level(logging.INFO):
        init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "staging"
    assert caplog.records == []


def test_init_wind_basis_unknown_value_resets_to_staging_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """mutant_denylist_states -> at this assertion: correct = "staging",
    mutant leaves an allowlist-only implementation returning the raw
    unknown value unchanged (no WARNING emitted either)."""
    conn = _make_db()
    site = _make_site(conn)
    set_runtime_state(conn, wind_basis_key(site), "bogus-state")
    with caplog.at_level(logging.WARNING):
        init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "staging"
    assert any(
        r.levelno == logging.WARNING and "bogus-state" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.parametrize("stored", ["switching", "rescoring"])
def test_init_wind_basis_mid_switch_with_old_rows_restarts_as_switching(
    stored: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A switch in progress, but old wind rows reappear (e.g. a restored
    partial import): restart the switch from the purge phase.
    mutant_forgets_downgrade -> at the cursor assertion: correct =
    ("purge", None), mutant leaves the cursor untouched (phase/cursor_date
    from before, or absent)."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_wind_basis_state(conn, site, stored)
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', 10.0, 'ok', '10 km/h', ?)",
        (station, isoformat_utc()),
    )
    with caplog.at_level(logging.INFO):
        init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "switching"
    cursor = read_wind_cursor(conn, site)
    assert cursor.phase == "purge"
    assert cursor.cursor_date is None
    assert any(r.levelno == logging.INFO for r in caplog.records)


def test_init_wind_basis_pair_max_with_old_rows_resets_to_staging_and_clears_keys() -> (
    None
):
    """A downgrade/old import: pair_max claims the new figure but old rows
    exist. mutant_keeps_keys -> at the keys assertion: correct = (None,
    None, None, None), mutant leaves at least one of the four stamped."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST002")
    set_wind_basis_state(conn, site, "pair_max")
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', 10.0, 'ok', '10 km/h', ?)",
        (station, isoformat_utc()),
    )
    init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "staging"
    assert get_runtime_state(conn, wind_cursor_key(site)) is None
    assert get_runtime_state(conn, wind_blocked_key(site)) is None
    assert get_runtime_state(conn, wind_report_key(site)) is None
    assert get_runtime_state(conn, wind_done_at_key(site)) is None


def test_init_wind_basis_absent_with_legacy_wind_becomes_staging() -> None:
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST003")
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', 10.0, 'ok', '10 km/h', ?)",
        (station, isoformat_utc()),
    )
    init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "staging"


def test_init_wind_basis_absent_without_legacy_wind_becomes_pair_max() -> None:
    """A brand new site, or one born after the switch: no legacy rows to
    carry means it starts served from the new figure.
    mutant_default_staging -> at this assertion: correct = "pair_max",
    mutant = "staging"."""
    conn = _make_db()
    site = _make_site(conn)
    _make_station(conn, site, "KTEST004")
    init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "pair_max"


def test_init_wind_basis_pair_max_without_legacy_rows_stays_pair_max() -> None:
    conn = _make_db()
    site = _make_site(conn)
    _make_station(conn, site, "KTEST005")
    set_wind_basis_state(conn, site, "pair_max")
    init_wind_basis(conn)
    assert get_runtime_state(conn, wind_basis_key(site)) == "pair_max"


# --- T21 -----------------------------------------------------------------


def _seed_published_wind_pair(conn: sqlite3.Connection, site: int, feed: int) -> None:
    generation = ensure_published_generation(conn, site)
    conn.execute(
        "INSERT INTO forecast_pairs"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  day_ahead, forecast, observed, error, abs_error, sq_error,"
        "  first_known_at, tz_generation_id)"
        " VALUES (?, ?, 'wind', '2026-07-01T00:00:00Z', '2026-07-01T03:00:00Z',"
        "  3, 0, 10.0, 12.0, -2.0, 2.0, 4.0, '2026-07-01T00:00:00Z', ?)",
        (site, feed, generation),
    )


def _legacy_row(conn: sqlite3.Connection, station: int, value: float = 10.0) -> None:
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', ?, 'ok', '10 km/h', ?)",
        (station, value, isoformat_utc()),
    )


def _pair_max_row(conn: sqlite3.Connection, station: int, value: float = 20.0) -> None:
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', ?, 'ok', 'pair-max 20.0', ?)",
        (station, value, isoformat_utc()),
    )


@pytest.mark.parametrize(
    ("state", "expect_value"),
    [
        ("staging", 10.0),
        ("switching", 20.0),
        ("rescoring", 20.0),
        ("pair_max", 20.0),
    ],
)
def test_materialize_consensus_wind_picks_the_states_own_basis(
    state: str, expect_value: float
) -> None:
    """mutant_missing_else_0 (unknown state) is covered by the next test;
    this one pins staging->legacy, the other three->pair-max. Two
    DIFFERENT stations carry the two shapes (a single station_observations
    row is keyed on (station_id, variable, valid_at), so one station
    cannot hold both at once) -- mirrors a real site with old and new
    rows briefly coexisting mid-migration.
    mutant_gate_after_invalidation would still produce the right consensus
    VALUE here (the row clause filters correctly either way), so it is
    killed by the invalidation-ran assertion below, not by this value."""
    conn = _make_db()
    site = _make_site(conn)
    feed = _make_feed(conn, "wind-model")
    legacy_station = _make_station(conn, site, "KTEST001")
    pair_max_station = _make_station(conn, site, "KTEST002")
    set_wind_basis_state(conn, site, state)
    _legacy_row(conn, legacy_station, 10.0)
    _pair_max_row(conn, pair_max_station, 20.0)
    _seed_published_wind_pair(conn, site, feed)

    materialize_consensus(
        conn, site_id=site, variable="wind", valid_at="2026-07-01T03:00:00Z"
    )

    row = conn.execute(
        "SELECT value FROM observations WHERE site_id=? AND variable='wind'"
        " AND valid_at='2026-07-01T03:00:00Z'",
        (site,),
    ).fetchone()
    assert row is not None
    assert row["value"] == expect_value

    # _invalidate_consensus_dependents ran: the seeded published pair is gone.
    pair_count = conn.execute(
        "SELECT COUNT(*) FROM forecast_pairs WHERE site_id=? AND variable='wind'",
        (site,),
    ).fetchone()[0]
    assert pair_count == 0


def test_materialize_consensus_wind_unknown_state_deletes_observation() -> None:
    """mutant_missing_else_0 -> at this assertion: correct = None (row
    deleted), mutant (ELSE 0 dropped, falling through to the last WHEN or a
    default admit) leaves the row present."""
    conn = _make_db()
    site = _make_site(conn)
    legacy_station = _make_station(conn, site, "KTEST001")
    pair_max_station = _make_station(conn, site, "KTEST002")
    set_runtime_state(conn, wind_basis_key(site), "bogus-state")
    _legacy_row(conn, legacy_station, 10.0)
    _pair_max_row(conn, pair_max_station, 20.0)
    # Seed a pre-existing observations row directly (bypassing the insert
    # guard, which would itself refuse an unknown state) so the delete
    # branch has something to delete.
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value,"
        " n_stations, rejected_stations, computed_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', 15.0, 1, 0, ?)",
        (site, isoformat_utc()),
    )

    materialize_consensus(
        conn, site_id=site, variable="wind", valid_at="2026-07-01T03:00:00Z"
    )

    row = conn.execute(
        "SELECT value FROM observations WHERE site_id=? AND variable='wind'"
        " AND valid_at='2026-07-01T03:00:00Z'",
        (site,),
    ).fetchone()
    assert row is None


# --- T22 -------------------------------------------------------------------


def test_null_source_raw_wind_row_counts_as_old_in_staging_excluded_in_pair_max() -> (
    None
):
    """mutant_not_legacy -> at this assertion: in staging, correct = 10.0
    (the NULL-source_raw row counts as legacy and is picked up), mutant
    (``PAIR_MAX_WIND_SQL`` written as ``NOT LEGACY_WIND_SQL`` instead of its
    own substr test) would make a NULL evaluate falsy in both clauses and
    exclude the row from staging's own figure too."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_wind_basis_state(conn, site, "staging")
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', '2026-07-01T03:00:00Z', 10.0, 'ok', NULL, ?)",
        (station, isoformat_utc()),
    )
    materialize_consensus(
        conn, site_id=site, variable="wind", valid_at="2026-07-01T03:00:00Z"
    )
    row = conn.execute(
        "SELECT value FROM observations WHERE site_id=? AND variable='wind'", (site,)
    ).fetchone()
    assert row is not None
    assert row["value"] == 10.0

    set_wind_basis_state(conn, site, "pair_max")
    materialize_consensus(
        conn, site_id=site, variable="wind", valid_at="2026-07-01T03:00:00Z"
    )
    row = conn.execute(
        "SELECT value FROM observations WHERE site_id=? AND variable='wind'", (site,)
    ).fetchone()
    assert row is None


# --- T23 ---------------------------------------------------------------


@pytest.mark.parametrize("state", ["switching", "rescoring", "pair_max"])
def test_insert_guard_refuses_wind_outside_staging(state: str) -> None:
    """mutant_denylist -> at this assertion: correct = False (every
    non-staging state refused), a ``state != 'switching'`` denylist mutant
    would instead ACCEPT the write in ``rescoring``/``pair_max``, making
    this assert False fail with True."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_wind_basis_state(conn, site, state)
    result = insert_station_observation(
        conn,
        station_id=station,
        variable="wind",
        valid_at="2026-07-01T03:00:00Z",
        value=10.0,
        source_raw="pair-max 10.0",
    )
    assert result is False
    row = conn.execute(
        "SELECT 1 FROM station_observations WHERE station_id=? AND variable='wind'",
        (station,),
    ).fetchone()
    assert row is None


def test_insert_guard_refuses_wind_for_unknown_state() -> None:
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_runtime_state(conn, wind_basis_key(site), "bogus-state")
    result = insert_station_observation(
        conn,
        station_id=station,
        variable="wind",
        valid_at="2026-07-01T03:00:00Z",
        value=10.0,
        source_raw="pair-max 10.0",
    )
    assert result is False


def test_insert_guard_accepts_wind_in_staging() -> None:
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_wind_basis_state(conn, site, "staging")
    result = insert_station_observation(
        conn,
        station_id=station,
        variable="wind",
        valid_at="2026-07-01T03:00:00Z",
        value=10.0,
        source_raw="10 km/h",
    )
    assert result is True
    row = conn.execute(
        "SELECT value FROM station_observations WHERE station_id=? AND variable='wind'",
        (station,),
    ).fetchone()
    assert row is not None
    assert row["value"] == 10.0


def test_insert_guard_refuses_wind_for_missing_station() -> None:
    conn = _make_db()
    result = insert_station_observation(
        conn,
        station_id=999_999,
        variable="wind",
        valid_at="2026-07-01T03:00:00Z",
        value=10.0,
        source_raw="10 km/h",
    )
    assert result is False


@pytest.mark.parametrize("state", ["staging", "switching", "rescoring", "pair_max"])
def test_insert_guard_always_accepts_temperature(state: str) -> None:
    """mutant_guard_every_variable -> at this assertion: correct = True in
    every state (temperature is never gated), mutant (the wind guard
    accidentally covers every ``variable``) would refuse this in any
    non-staging state."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_wind_basis_state(conn, site, state)
    result = insert_station_observation(
        conn,
        station_id=station,
        variable="temperature",
        valid_at="2026-07-01T03:00:00Z",
        value=15.0,
        source_raw="15.0",
    )
    assert result is True


# --- T24 -----------------------------------------------------------------


def _build_legacy_shaped_replacement(tmp_path: Path, filename: str) -> Path:
    """A standalone, fully-migrated replacement db carrying a legacy wind
    row with NO ``wind_basis`` key -- as if the migration that introduced
    the key had never run on it (a genuine 0.16.5-shaped import)."""
    path = tmp_path / filename
    db = Database(str(path))
    try:
        conn = db._conn  # noqa: SLF001 - building fixture content directly
        site_cur = conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
        )
        site_id = int(site_cur.lastrowid)  # type: ignore[arg-type]
        station_cur = conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'KTEST001', 0.0, 0.0, 0.0, 1)",
            (site_id,),
        )
        assert station_cur.lastrowid is not None
        # Legacy row, planted directly: bypasses the key machinery, exactly
        # mirroring how a pre-wind-basis import would arrive.
        conn.execute(
            "INSERT INTO station_observations (station_id, variable, valid_at,"
            " value, qc_flag, source_raw, fetched_at) VALUES"
            " ((SELECT id FROM stations WHERE site_id=?), 'wind',"
            " '2026-07-01T03:00:00Z', 10.0, 'ok', '10 km/h', ?)",
            (site_id, isoformat_utc()),
        )
        # init_wind_basis already ran once at Database() construction (when
        # the site didn't exist yet); clear the key it would have left so
        # this file truly carries no wind_basis key for the new site.
        conn.execute(
            "DELETE FROM runtime_state WHERE key = ?", (wind_basis_key(site_id),)
        )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
    finally:
        db.close()
    return path


def test_replace_from_reopen_runs_init_wind_basis_on_the_imported_file(
    tmp_path: Path,
) -> None:
    """Drives the real import-swap path end to end: ``replace_from`` ->
    ``_open`` -> ``run_migrations`` -> ``init_wind_basis``.
    mutant_init_not_wired -> at this assertion: correct = "staging" (the
    swapped-in file's legacy row is recognized on reopen), mutant (init
    removed from run_migrations, or only run at process-start) leaves the
    key absent, which ``wind_basis_state`` reads back as "pair_max"."""

    async def _run() -> str:
        db = Database(str(tmp_path / "live.db"))
        try:
            new_path = _build_legacy_shaped_replacement(tmp_path, "legacy-import.db")
            backup_path = tmp_path / "backup.db.bak"
            await db.replace_from(new_path, backup_path)
            row = await db.read(
                lambda conn: conn.execute(
                    "SELECT value FROM runtime_state WHERE key LIKE 'wind_basis:%'"
                ).fetchone()
            )
            return None if row is None else str(row["value"])
        finally:
            db.close()

    state = asyncio.run(_run())
    assert state == "staging"


# --- T25 -------------------------------------------------------------------


def test_write_station_observation_stamps_fetched_at_no_materialize() -> None:
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    changed = write_station_observation(
        conn,
        station_id=station,
        variable="temperature",
        valid_at="2026-07-01T03:00:00Z",
        value=15.0,
        source_raw="15.0",
    )
    assert changed is True
    row = conn.execute(
        "SELECT fetched_at FROM station_observations WHERE station_id=?", (station,)
    ).fetchone()
    assert row["fetched_at"] is not None
    # Unguarded body never materializes: no observations row appears.
    obs = conn.execute(
        "SELECT 1 FROM observations WHERE site_id=? AND variable='temperature'",
        (site,),
    ).fetchone()
    assert obs is None


def test_write_station_observation_returns_false_on_no_change() -> None:
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    write_station_observation(
        conn,
        station_id=station,
        variable="temperature",
        valid_at="2026-07-01T03:00:00Z",
        value=15.0,
        source_raw="15.0",
    )
    changed_again = write_station_observation(
        conn,
        station_id=station,
        variable="temperature",
        valid_at="2026-07-01T03:00:00Z",
        value=15.0,
        source_raw="15.0",
    )
    assert changed_again is False


def test_insert_station_observation_does_materialize() -> None:
    """mutant_materialize_left_in_split_body -> at this assertion: correct
    = a present ``observations`` row (``insert_station_observation``
    materializes), mutant (materialize moved into
    ``write_station_observation`` instead, double-materializing there and
    not here, or omitted from both) would leave it absent if omitted
    entirely from the guarded caller."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    insert_station_observation(
        conn,
        station_id=station,
        variable="temperature",
        valid_at="2026-07-01T03:00:00Z",
        value=15.0,
        source_raw="15.0",
    )
    obs = conn.execute(
        "SELECT value FROM observations WHERE site_id=? AND variable='temperature'",
        (site,),
    ).fetchone()
    assert obs is not None
    assert obs["value"] == 15.0


# --- T26 -----------------------------------------------------------------


def _seed_station_trust_wind_rows(conn: sqlite3.Connection, site: int) -> None:
    """One legacy-shaped row and one pair-max-shaped row, on two different
    stations (a single row is keyed on (station_id, variable, valid_at) so
    one station cannot carry both shapes at the same hour), both joined
    against the one ``observations`` consensus row for the hour."""
    legacy_station = _make_station(conn, site, "KTEST001")
    pair_max_station = _make_station(conn, site, "KTEST002")
    conn.execute(
        "INSERT INTO station_observations (station_id, variable, valid_at,"
        " value, qc_flag, source_raw, fetched_at) VALUES"
        " (?, 'wind', '2026-07-01T03:00:00Z', 11.0, 'ok', '11 km/h', ?)",
        (legacy_station, isoformat_utc()),
    )
    conn.execute(
        "INSERT INTO station_observations (station_id, variable, valid_at,"
        " value, qc_flag, source_raw, fetched_at) VALUES"
        " (?, 'wind', '2026-07-01T03:00:00Z', 12.0, 'ok', 'pair-max 12.0', ?)",
        (pair_max_station, isoformat_utc()),
    )
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value,"
        " n_stations, rejected_stations, computed_at) VALUES"
        " (?, 'wind', '2026-07-01T03:00:00Z', 10.0, 1, 0, ?)",
        (site, isoformat_utc()),
    )


@pytest.mark.parametrize(
    ("state", "expect_station"),
    [
        ("staging", "KTEST001"),  # legacy shape admitted
        ("switching", None),  # open_clause blocks wind entirely
        ("rescoring", "KTEST002"),  # pair-max shape admitted
        ("pair_max", "KTEST002"),  # pair-max shape admitted
        ("bogus-state", None),  # row_clause ELSE 0 + open_clause both block
    ],
)
def test_station_trust_reader_follows_wind_basis_state(
    state: str, expect_station: str | None
) -> None:
    """mutant_clause_dropped -> at this assertion: a reader missing either
    ``wind_station_row_clause`` (the shape CASE) or ``wind_open_clause``
    (the state-membership gate) would show BOTH stations' wind rows, or
    show the wrong-shaped one, in at least one state here."""
    from wxverify.web.context import load_station_trust

    conn = _make_db()
    site = _make_site(conn)
    if state in STATES:
        set_wind_basis_state(conn, site, state)
    else:
        set_runtime_state(conn, wind_basis_key(site), state)
    _seed_station_trust_wind_rows(conn, site)
    rows = load_station_trust(conn)
    wind_stations = {r.station for r in rows if r.variable == "wind"}
    assert wind_stations == (set() if expect_station is None else {expect_station})


def _seed_real_model_candidate(
    conn: sqlite3.Connection, site: int, feed: int, variable: str
) -> None:
    conn.execute(
        "INSERT INTO forecast_samples (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, ?, '2026-07-01T00:00:00Z', '2026-07-01T03:00:00Z', 3,"
        " 10.0, '{}', 'run-x', ?)",
        (site, feed, variable, isoformat_utc()),
    )
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value,"
        " n_stations, rejected_stations, computed_at) VALUES"
        " (?, ?, '2026-07-01T03:00:00Z', 12.0, 1, 0, ?)",
        (site, variable, isoformat_utc()),
    )


@pytest.mark.parametrize(
    ("state", "wind_visible"),
    [
        ("staging", True),
        ("switching", False),
        ("rescoring", True),
        ("pair_max", True),
        ("bogus-state", False),
    ],
)
def test_real_model_pairing_reader_follows_wind_basis_state(
    state: str, wind_visible: bool
) -> None:
    conn = _make_db()
    site = _make_site(conn)
    feed = _make_feed(conn, "real-model")
    if state in STATES:
        set_wind_basis_state(conn, site, state)
    else:
        set_runtime_state(conn, wind_basis_key(site), state)
    _seed_real_model_candidate(conn, site, feed, "wind")
    _seed_real_model_candidate(conn, site, feed, "temperature")
    delta = compute_real_model_pairs(conn, site)
    variables = {str(op.values[2]) for op in delta.ops}
    assert ("wind" in variables) is wind_visible
    assert "temperature" in variables


def _seed_persistence_candidate(conn: sqlite3.Connection, site: int) -> None:
    conn.execute(
        "INSERT INTO feeds (source, model, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('virtual', '_persistence', 1, 60, 168, 1)"
        " ON CONFLICT(source, model) DO NOTHING"
    )
    for variable, hour in (
        ("wind", 0),
        ("wind", 24),
        ("temperature", 0),
        ("temperature", 24),
    ):
        valid_at = f"2026-07-0{1 + hour // 24}T00:00:00Z"
        conn.execute(
            "INSERT INTO observations (site_id, variable, valid_at, value,"
            " n_stations, rejected_stations, computed_at) VALUES"
            " (?, ?, ?, 10.0, 1, 0, ?)",
            (site, variable, valid_at, isoformat_utc()),
        )


@pytest.mark.parametrize(
    ("state", "wind_visible"),
    [
        ("staging", True),
        ("switching", False),
        ("rescoring", True),
        ("pair_max", True),
        ("bogus-state", False),
    ],
)
def test_persistence_pairing_reader_follows_wind_basis_state(
    state: str, wind_visible: bool
) -> None:
    conn = _make_db()
    site = _make_site(conn)
    if state in STATES:
        set_wind_basis_state(conn, site, state)
    else:
        set_runtime_state(conn, wind_basis_key(site), state)
    _seed_persistence_candidate(conn, site)
    delta = compute_persistence_pairs(conn, site)
    variables = {str(op.values[2]) for op in delta.ops}
    assert ("wind" in variables) is wind_visible
    assert "temperature" in variables


@pytest.mark.parametrize(
    ("state", "wind_visible"),
    [
        ("staging", True),
        ("switching", False),
        ("rescoring", True),
        ("pair_max", True),
        ("bogus-state", False),
    ],
)
def test_distinct_cells_reader_follows_wind_basis_state(
    state: str, wind_visible: bool
) -> None:
    conn = _make_db()
    site = _make_site(conn)
    feed = _make_feed(conn, "wind-model")
    if state in STATES:
        set_wind_basis_state(conn, site, state)
    else:
        set_runtime_state(conn, wind_basis_key(site), state)
    _seed_published_wind_pair(conn, site, feed)
    # Also a temperature cell, always visible, to prove the clause is
    # variable-scoped and not an accidental whole-site filter.
    generation = ensure_published_generation(conn, site)
    conn.execute(
        "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, day_ahead, forecast, observed, error,"
        " abs_error, sq_error, first_known_at, tz_generation_id) VALUES"
        " (?, ?, 'temperature', '2026-07-01T00:00:00Z',"
        " '2026-07-01T03:00:00Z', 3, 0, 10.0, 12.0, -2.0, 2.0, 4.0,"
        " '2026-07-01T00:00:00Z', ?)",
        (site, feed, generation),
    )
    cells = _distinct_cells(conn, site)
    variables = {c.variable for c in cells}
    assert ("wind" in variables) is wind_visible
    assert "temperature" in variables


# --- T27 -----------------------------------------------------------------


def test_watermark_excludes_wind_written_rows() -> None:
    """A lane-written wind row later than the site's last temperature row
    must not move the watermark the live fetch path uses to decide what's
    new. mutant_filter_dropped -> at this assertion: correct =
    '2026-07-01T03:00:00Z' (the temperature row), mutant (the
    ``variable != 'wind'`` filter dropped from the subselect) = the later
    wind timestamp."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    conn.execute(
        "INSERT INTO station_observations (station_id, variable, valid_at,"
        " value, qc_flag, source_raw, fetched_at) VALUES"
        " (?, 'temperature', '2026-07-01T03:00:00Z', 15.0, 'ok', '15.0', ?)",
        (station, isoformat_utc()),
    )
    conn.execute(
        "INSERT INTO station_observations (station_id, variable, valid_at,"
        " value, qc_flag, source_raw, fetched_at) VALUES"
        " (?, 'wind', '2026-07-01T09:00:00Z', 12.0, 'ok', 'pair-max 12.0', ?)",
        (station, isoformat_utc()),
    )
    targets = _enabled_stations(conn, site)
    assert len(targets) == 1
    assert targets[0].obs_watermark_at == "2026-07-01T03:00:00Z"


def test_watermark_is_none_when_only_wind_rows_exist() -> None:
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    conn.execute(
        "INSERT INTO station_observations (station_id, variable, valid_at,"
        " value, qc_flag, source_raw, fetched_at) VALUES"
        " (?, 'wind', '2026-07-01T09:00:00Z', 12.0, 'ok', 'pair-max 12.0', ?)",
        (station, isoformat_utc()),
    )
    targets = _enabled_stations(conn, site)
    assert targets[0].obs_watermark_at is None


# --- T28 -----------------------------------------------------------------


def test_transition_wind_basis_cas_fails_on_wrong_expected() -> None:
    """mutant_unconditional_set -> at this assertion: correct = False (the
    stale expected value is refused and nothing changes), mutant (an
    unconditional SET instead of the WHERE value=expected CAS) would
    return True and advance the state anyway."""
    conn = _make_db()
    site = _make_site(conn)
    set_wind_basis_state(conn, site, "staging")
    ok = transition_wind_basis(conn, site, expected="switching", new="rescoring")
    assert ok is False
    assert get_runtime_state(conn, wind_basis_key(site)) == "staging"


def test_transition_wind_basis_succeeds_on_matching_expected() -> None:
    conn = _make_db()
    site = _make_site(conn)
    set_wind_basis_state(conn, site, "staging")
    ok = transition_wind_basis(conn, site, expected="staging", new="switching")
    assert ok is True
    assert get_runtime_state(conn, wind_basis_key(site)) == "switching"


@pytest.mark.parametrize(
    ("expected", "new"),
    [
        ("staging", "rescoring"),  # skips switching
        ("switching", "staging"),  # backward
        ("pair_max", "staging"),  # backward from the terminal state
    ],
)
def test_transition_wind_basis_raises_on_illegal_moves(expected: str, new: str) -> None:
    conn = _make_db()
    site = _make_site(conn)
    set_wind_basis_state(conn, site, expected)
    with pytest.raises(ValueError, match="illegal wind basis transition"):
        transition_wind_basis(conn, site, expected=expected, new=new)


def test_pair_max_source_prefix_is_exactly_nine_chars_and_in_both_sql_fragments() -> (
    None
):
    assert len(PAIR_MAX_SOURCE_PREFIX) == 9
    assert PAIR_MAX_SOURCE_PREFIX in PAIR_MAX_WIND_SQL
    assert PAIR_MAX_SOURCE_PREFIX in LEGACY_WIND_SQL


# --- T109 ------------------------------------------------------------------


def test_insert_guard_reads_state_per_call_not_cached_at_job_start() -> None:
    """A job enqueued while the site was staging, but run after a switch has
    moved it to switching, must still see (and obey) the CURRENT state: the
    guard re-reads ``wind_state_sql`` on every call, it is never cached in a
    long-lived object across calls. mutant_cached_at_job_start -> at this
    assertion: correct = False (the second call, after the state moved,
    is refused), mutant (state captured once and reused) = True."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    set_wind_basis_state(conn, site, "staging")

    first = insert_station_observation(
        conn,
        station_id=station,
        variable="wind",
        valid_at="2026-07-01T03:00:00Z",
        value=10.0,
        source_raw="10 km/h",
    )
    assert first is True

    # The switch starts between the two calls -- no job-scoped object is
    # involved; the guard must observe this on its very next invocation.
    set_wind_basis_state(conn, site, "switching")

    second = insert_station_observation(
        conn,
        station_id=station,
        variable="wind",
        valid_at="2026-07-01T04:00:00Z",
        value=11.0,
        source_raw="pair-max 11.0",
    )
    assert second is False
    row = conn.execute(
        "SELECT 1 FROM station_observations WHERE station_id=? AND variable='wind'"
        " AND valid_at='2026-07-01T04:00:00Z'",
        (station,),
    ).fetchone()
    assert row is None


# --- T110 --------------------------------------------------------------


_WXVERIFY_SRC = Path(__file__).resolve().parent.parent / "wxverify"


def test_insert_into_station_observations_appears_exactly_once() -> None:
    """mutant_second_insert_statement -> at this assertion: correct = 1 (the
    single unguarded write body in ``scoring/consensus.py``), mutant (a
    second ``INSERT INTO station_observations`` added anywhere in the
    package, e.g. bypassing the guard) = 2 or more."""
    pattern = re.compile(r"insert\s+into\s+station_observations", re.IGNORECASE)
    hits: list[tuple[Path, int]] = []
    for path in sorted(_WXVERIFY_SRC.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        collapsed = re.sub(r"\s+", " ", text)
        for match in pattern.finditer(collapsed):
            hits.append((path, match.start()))
    assert len(hits) == 1, hits


class _WriteStationObservationCallFinder(ast.NodeVisitor):
    def __init__(self) -> None:
        self.callers: list[str] = []
        self._current_function: str | None = None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        outer = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = outer

    visit_AsyncFunctionDef = visit_FunctionDef  # noqa: N815

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        func = node.func
        name = func.id if isinstance(func, ast.Name) else None
        if name == "write_station_observation":
            self.callers.append(self._current_function or "<module>")
        self.generic_visit(node)


def test_write_station_observation_called_only_from_the_two_allowed_sites() -> None:
    """mutant_new_caller -> at this assertion: correct = every call site is
    inside ``scoring/consensus.py`` (``insert_station_observation``) or
    ``worker/wind_days.py``; mutant (a third module calls it directly,
    bypassing the guard) adds a file this assertion does not allow."""
    allowed_files = {"consensus.py", "wind_days.py"}
    violations: list[str] = []
    for path in sorted(_WXVERIFY_SRC.rglob("*.py")):
        if path.name == "consensus.py":
            # The definition site itself also calls it once, from
            # insert_station_observation -- that's expected and checked
            # separately below.
            pass
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        finder = _WriteStationObservationCallFinder()
        finder.visit(tree)
        if finder.callers and path.name not in allowed_files:
            violations.append(f"{path}: {finder.callers}")
    assert violations == []

    consensus_tree = ast.parse(
        (_WXVERIFY_SRC / "scoring" / "consensus.py").read_text(encoding="utf-8")
    )
    finder = _WriteStationObservationCallFinder()
    finder.visit(consensus_tree)
    assert finder.callers == ["insert_station_observation"]

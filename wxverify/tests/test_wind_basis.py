"""The station wind writer and readers, and the pair-max row predicate
(plan §8-9, §15.1 T23-T28, T110).

No package code reads a site's old ``wind_basis`` key any more. Tests that
store one write it by its literal name and expect the same outcome whatever
it holds.

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test (mirrors test_forecast_service.py), except T24 which drives the real
``Database.replace_from`` swap path and therefore needs the async
``wxverify.db.connection.Database`` plus real tmp-path files.
"""

from __future__ import annotations

import ast
import asyncio
import re
import sqlite3
from pathlib import Path

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.db.wind_basis import PAIR_MAX_SOURCE_PREFIX, PAIR_MAX_WIND_SQL
from wxverify.scoring.consensus import (
    insert_station_observation,
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


# --- T23 ---------------------------------------------------------------

# Every value a site's ``wind_basis`` key held before the removal, a junk
# value, and no key at all. No package code reads the key any more, so every
# test parametrized over these expects the same outcome for each.
_STORED_WIND_KEY_VALUES: list[str | None] = [
    "staging",
    "switching",
    "rescoring",
    "pair_max",
    "bogus",
    None,
]


def _store_wind_key(conn: sqlite3.Connection, site: int, value: str | None) -> None:
    """Write the site's wind_basis key by its literal name; None writes none."""
    key = f"wind_basis:{site}"
    if value is None:
        assert get_runtime_state(conn, key) is None, "precondition: no key"
        return
    set_runtime_state(conn, key, value)


@pytest.mark.parametrize("state", _STORED_WIND_KEY_VALUES)
def test_insert_guard_refuses_wind_whatever_the_stored_key(state: str | None) -> None:
    """Correct = False in every state: the wind lane writes every wind row
    through ``write_station_observation``. A guard that still admits wind in
    some state (the old ``staging`` allowlist) returns True for that id; no
    guard at all (plan M1) returns True for every id."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    _store_wind_key(conn, site, state)
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


@pytest.mark.parametrize("state", _STORED_WIND_KEY_VALUES)
def test_insert_guard_always_accepts_temperature(state: str | None) -> None:
    """mutant_guard_every_variable -> at this assertion: correct = True in
    every state (temperature is never gated), mutant (the wind guard
    accidentally covers every ``variable``) would refuse this in every
    state."""
    conn = _make_db()
    site = _make_site(conn)
    station = _make_station(conn, site, "KTEST001")
    _store_wind_key(conn, site, state)
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
    """A standalone, fully-migrated replacement db carrying an old-figure
    wind row (``source_raw`` ``"10 km/h"``) and no ``wind_basis`` key, the
    shape of an import from before the pair-max figure."""
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
        # Old-figure row, planted directly, as such an import carries it.
        conn.execute(
            "INSERT INTO station_observations (station_id, variable, valid_at,"
            " value, qc_flag, source_raw, fetched_at) VALUES"
            " ((SELECT id FROM stations WHERE site_id=?), 'wind',"
            " '2026-07-01T03:00:00Z', 10.0, 'ok', '10 km/h', ?)",
            (site_id, isoformat_utc()),
        )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
    finally:
        db.close()
    return path


def test_replace_from_reopen_writes_no_wind_basis_key_on_the_imported_file(
    tmp_path: Path,
) -> None:
    """Drives the real import-swap path end to end: ``replace_from`` ->
    ``_open`` -> ``run_migrations``. Opening a database writes no wind key
    (plan §3 item 1), so the swapped-in file's old wind row leaves the site
    without one.
    mutant_open_writes_wind_key -> at this assertion: correct = None (no
    ``wind_basis:`` key after the reopen), mutant (``run_migrations``
    calling the removed open-time wind initializer again) = "staging", the
    swapped-in file's legacy row recognized on reopen."""

    async def _run() -> str | None:
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
    assert state is None


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


@pytest.mark.parametrize("state", _STORED_WIND_KEY_VALUES)
def test_station_trust_reader_ignores_wind_basis_state(state: str | None) -> None:
    """Station trust tests neither the stored key nor ``source_raw`` for wind
    (plan §5.3 item 10), so both stations' wind rows show in every state. A
    reader that still gates wind on the state, or on the row's shape, drops
    one or both stations for some id; one that excludes wind (plan M18)
    drops both for every id."""
    from wxverify.web.context import load_station_trust

    conn = _make_db()
    site = _make_site(conn)
    _store_wind_key(conn, site, state)
    _seed_station_trust_wind_rows(conn, site)
    rows = load_station_trust(conn)
    wind_stations = {r.station for r in rows if r.variable == "wind"}
    assert wind_stations == {"KTEST001", "KTEST002"}


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


@pytest.mark.parametrize("state", _STORED_WIND_KEY_VALUES)
def test_real_model_pairing_reader_ignores_wind_basis_state(
    state: str | None,
) -> None:
    conn = _make_db()
    site = _make_site(conn)
    feed = _make_feed(conn, "real-model")
    _store_wind_key(conn, site, state)
    _seed_real_model_candidate(conn, site, feed, "wind")
    _seed_real_model_candidate(conn, site, feed, "temperature")
    delta = compute_real_model_pairs(conn, site)
    variables = {str(op.values[2]) for op in delta.ops}
    assert "wind" in variables
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


@pytest.mark.parametrize("state", _STORED_WIND_KEY_VALUES)
def test_persistence_pairing_reader_ignores_wind_basis_state(
    state: str | None,
) -> None:
    conn = _make_db()
    site = _make_site(conn)
    _store_wind_key(conn, site, state)
    _seed_persistence_candidate(conn, site)
    delta = compute_persistence_pairs(conn, site)
    variables = {str(op.values[2]) for op in delta.ops}
    assert "wind" in variables
    assert "temperature" in variables


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


@pytest.mark.parametrize("state", _STORED_WIND_KEY_VALUES)
def test_distinct_cells_reader_ignores_wind_basis_state(state: str | None) -> None:
    conn = _make_db()
    site = _make_site(conn)
    feed = _make_feed(conn, "wind-model")
    _store_wind_key(conn, site, state)
    _seed_published_wind_pair(conn, site, feed)
    # Also a temperature cell: the reader returns wind's cell alongside the
    # other variables', not in place of them.
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
    assert "wind" in variables
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


def test_pair_max_source_prefix_is_nine_chars_and_in_the_sql_fragment() -> None:
    assert len(PAIR_MAX_SOURCE_PREFIX) == 9
    assert PAIR_MAX_SOURCE_PREFIX in PAIR_MAX_WIND_SQL


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

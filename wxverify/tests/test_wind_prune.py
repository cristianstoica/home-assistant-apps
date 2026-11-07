"""``wxverify.db.switch.prune_wind_history``, the one-time wind-history
prune (health-release plan 2026-11-07, §7 Part G; test IDs G1-G3, WP10).

Real SQLite throughout, via ``wxverify.db.connection.init_db`` on a tmp-file
database. Zone is ``UTC`` throughout (no DST, so margins compare cleanly).
Stations are synthetic: A (``ISTATION01``) and B (``ISTATION02``) at site 1,
C (``ISTATION03``) at site 2 (D, ``ISTATION04``, unused), E and F
(``ISTATION05``/``ISTATION06``) at site 3, unused.
"""

from __future__ import annotations

import ast
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from tests.helpers import D, day_close_site, note_day_clock
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.site_days import (
    begin_close,
    ensure_site_days,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.db.switch import prune_wind_history

_ROOT = Path(__file__).resolve().parents[1]
_PACKAGE_ROOT = _ROOT / "wxverify"

# F, the site's first closed day (plan 4.1): reuse the shared fixture's D so
# no second synthetic calendar date needs inventing.
F: Final = D
F_START: Final = datetime(F.year, F.month, F.day, tzinfo=UTC)
# One instant into F+1, used with ensure_site_days to grow the record set to
# F, F+1 and F+2 (site_days._fill creates through current_local_date + 1).
_F1_NOON: Final = F_START + timedelta(days=1, hours=12)
# Half an hour after F has ended -- a legal begin_close/finish_close instant.
_CLOSE_AT: Final = F_START + timedelta(days=1, minutes=30)

MARGIN: Final = timedelta(seconds=1200)

STATION_A: Final = "ISTATION01"
STATION_B: Final = "ISTATION02"
STATION_C: Final = "ISTATION03"
STATION_D: Final = "ISTATION04"
STATION_E: Final = "ISTATION05"
STATION_F: Final = "ISTATION06"


def _init_tmp_db(base: Path) -> sqlite3.Connection:
    close_db()
    base.mkdir(parents=True, exist_ok=True)
    db_path = base / "wxverify.db"
    config.db_path = str(db_path)
    options_path = base / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _station_id(conn: sqlite3.Connection, pws_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = ?", (pws_id,)
    ).fetchone()
    assert row is not None
    return int(row[0])


def _build_site(
    conn: sqlite3.Connection,
    *,
    station_ids: tuple[str, str],
    close_f: bool,
) -> int:
    site_id = day_close_site(
        conn,
        timezone="UTC",
        close_on=True,
        with_records=False,
        station_ids=station_ids,
    )
    start_site_days(conn, site_id, F, now_utc=F_START)
    ensure_site_days(conn, site_id, now_utc=note_day_clock(_F1_NOON))
    records = conn.execute(
        "SELECT local_date FROM site_days WHERE site_id = ? ORDER BY local_date",
        (site_id,),
    ).fetchall()
    assert [r[0] for r in records] == [
        F.isoformat(),
        (F + timedelta(days=1)).isoformat(),
        (F + timedelta(days=2)).isoformat(),
    ]
    if close_f:
        begin_close(conn, site_id, F, now_utc=_CLOSE_AT)
        finish_close(conn, site_id, F, now_utc=_CLOSE_AT)
    return site_id


def _insert_history_row(
    conn: sqlite3.Connection,
    station_id: int,
    obs_at: datetime,
    *,
    fetched_at: datetime | None,
    speed_kmh: float = 5.0,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh, fetched_at)"
        " VALUES (?, ?, 'five_minute', ?, ?, ?)",
        (
            station_id,
            isoformat_utc(obs_at),
            obs_at.date().isoformat(),
            speed_kmh,
            None if fetched_at is None else isoformat_utc(fetched_at),
        ),
    )


def _seed_history(conn: sqlite3.Connection, station_id: int) -> dict[str, datetime]:
    """The three fixture categories of G1 for one station.

    ``old`` is before the margin (pruned); ``near`` is inside the 1200 s
    margin before F's start (kept); ``inside`` is inside F itself (kept);
    ``set_old`` has ``fetched_at`` set, dated before F (kept regardless of
    date -- P2's own rows, never this prune's).
    """
    times = {
        "old": F_START - MARGIN - timedelta(seconds=800),
        "near": F_START - timedelta(seconds=600),
        "inside": F_START + timedelta(seconds=1800),
        "set_old": F_START - timedelta(seconds=5000),
    }
    _insert_history_row(conn, station_id, times["old"], fetched_at=None)
    _insert_history_row(conn, station_id, times["near"], fetched_at=None)
    _insert_history_row(conn, station_id, times["inside"], fetched_at=None)
    _insert_history_row(conn, station_id, times["set_old"], fetched_at=times["set_old"])
    return times


def _build_fixture(
    conn: sqlite3.Connection, *, site1_closed: bool = True
) -> dict[str, int]:
    """The G1 fixture: site 1's F open or closed per ``site1_closed``, site
    2's closed, site 3's open and rowless. C (site 2) also carries one
    boundary row exactly at F's start minus the 1200 s margin."""
    site1 = _build_site(conn, station_ids=(STATION_A, STATION_B), close_f=site1_closed)
    site2 = _build_site(conn, station_ids=(STATION_C, STATION_D), close_f=True)
    site3 = _build_site(conn, station_ids=(STATION_E, STATION_F), close_f=False)
    a_id = _station_id(conn, STATION_A)
    b_id = _station_id(conn, STATION_B)
    c_id = _station_id(conn, STATION_C)
    _seed_history(conn, a_id)
    _seed_history(conn, b_id)
    _seed_history(conn, c_id)
    # C's boundary row, exactly at F's start minus the margin: never less
    # than the bound, so it must survive.
    boundary = F_START - MARGIN
    _insert_history_row(conn, c_id, boundary, fetched_at=None)
    return {
        "site1": site1,
        "site2": site2,
        "site3": site3,
        "a": a_id,
        "b": b_id,
        "c": c_id,
        "boundary": boundary,
    }


def _row_exists(conn: sqlite3.Connection, station_id: int, obs_at: datetime) -> bool:
    row = conn.execute(
        "SELECT 1 FROM station_wind_records WHERE station_id = ? AND obs_at = ?",
        (station_id, isoformat_utc(obs_at)),
    ).fetchone()
    return row is not None


def _all_rows(conn: sqlite3.Connection, station_id: int) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM station_wind_records WHERE station_id = ?"
            " ORDER BY obs_at, source",
            (station_id,),
        )
    ]


# ---------------------------------------------------------------------------
# G1
# ---------------------------------------------------------------------------


def test_g1_prune_deletes_only_the_margin_bounded_null_rows_of_done_sites(
    tmp_path: Path,
) -> None:
    """G1 (plan §7): the rule, on the whole fixture.

    M -> at the ``deleted`` assertion: correct = 1 (only C's ``old`` row,
    before F's start minus the 1200 s margin, at a site whose F is closed,
    is deleted), mutant = a different count (``<=`` for ``<`` would also
    take C's boundary row; the margin dropped would also take C's ``near``
    row; the ``fetched_at IS NULL`` filter dropped would also take C's
    ``set_old`` row; the done-state check dropped, or a delete not scoped
    to the site's stations, would both widen the count the same way --
    A's and B's ``old`` rows at site 1, whose F is open, are what catch
    either one; site 3 owns no rows at all, so it cannot).
    M -> at the ``kept_sites`` assertion: correct = ``(site1,)`` (only site
    1's F is open; site 2's is closed and site 3 owns no ``fetched_at``-null
    row at all, so it is never even considered), mutant = a different tuple
    (the done-state check dropped would make this empty; the command's old
    all-or-nothing refusal carried over would make it ``(site1, site2)``
    even though site 2's F is done).
    """
    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn, site1_closed=False)
    a_id, b_id, c_id = ids["a"], ids["b"], ids["c"]
    boundary = ids["boundary"]

    a_before = _all_rows(conn, a_id)
    b_before = _all_rows(conn, b_id)

    result = prune_wind_history(conn)

    assert result.deleted == 1
    assert result.kept_sites == (ids["site1"],)

    # C (site 2, closed): only its old row is gone.
    assert not _row_exists(conn, c_id, F_START - MARGIN - timedelta(seconds=800))
    assert _row_exists(conn, c_id, boundary)  # exactly the bound: kept
    assert _row_exists(conn, c_id, F_START - timedelta(seconds=600))  # near
    assert _row_exists(conn, c_id, F_START + timedelta(seconds=1800))  # inside
    assert _row_exists(conn, c_id, F_START - timedelta(seconds=5000))  # set_old

    # A and B (site 1, open F): every row of theirs survives untouched.
    assert _all_rows(conn, a_id) == a_before
    assert _all_rows(conn, b_id) == b_before


# ---------------------------------------------------------------------------
# G2
# ---------------------------------------------------------------------------


def test_g2_missed_counts_as_done(tmp_path: Path) -> None:
    """G2 (plan §7): site 1's F marked missed, not closed.

    M -> at the ``a_old_gone``/``b_old_gone``/``c_old_gone`` assertions:
    correct = gone (``DONE_STATES`` is ``{'closed', 'missed'}``, so a
    missed F is done), mutant = present (a check for ``closed`` alone would
    leave A's and B's old rows at site 1 untouched).
    """
    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn, site1_closed=False)
    a_id, b_id, c_id = ids["a"], ids["b"], ids["c"]

    mark_missed(conn, ids["site1"], F, now_utc=_CLOSE_AT, reason="never_attempted")

    result = prune_wind_history(conn)

    assert result.deleted == 3  # A's, B's and C's old rows
    assert result.kept_sites == ()
    a_old_gone = not _row_exists(conn, a_id, F_START - MARGIN - timedelta(seconds=800))
    b_old_gone = not _row_exists(conn, b_id, F_START - MARGIN - timedelta(seconds=800))
    c_old_gone = not _row_exists(conn, c_id, F_START - MARGIN - timedelta(seconds=800))
    assert a_old_gone
    assert b_old_gone
    assert c_old_gone
    assert _row_exists(conn, a_id, F_START - timedelta(seconds=600))  # near, kept
    assert _row_exists(conn, b_id, F_START - timedelta(seconds=600))  # near, kept


# ---------------------------------------------------------------------------
# G3
# ---------------------------------------------------------------------------


def test_g3_a_second_run_right_after_g1_deletes_nothing(tmp_path: Path) -> None:
    """G3 (plan §7): right after G1's call, a second call.

    M -> at the ``deleted`` assertion: correct = 0 (the bound depends only
    on F, which does not move between the two calls), mutant = nonzero (a
    bound that moves on each run, e.g. one keyed off ``now`` instead of F's
    ``day_start_utc``, would find more to delete on the second call too).
    """
    conn = _init_tmp_db(tmp_path)
    _build_fixture(conn, site1_closed=False)

    first = prune_wind_history(conn)
    assert first.deleted == 1
    before = {
        station: _all_rows(conn, station)
        for station in (
            _station_id(conn, STATION_A),
            _station_id(conn, STATION_B),
            _station_id(conn, STATION_C),
        )
    }

    second = prune_wind_history(conn)

    assert second.deleted == 0
    after = {
        station: _all_rows(conn, station)
        for station in (
            _station_id(conn, STATION_A),
            _station_id(conn, STATION_B),
            _station_id(conn, STATION_C),
        )
    }
    assert after == before


# ---------------------------------------------------------------------------
# WP10, re-targeted (plan §7)
# ---------------------------------------------------------------------------


_ALLOWED_STATION_WIND_RECORDS_FILES: Final = {
    "db/migrations.py",
    "api/routes/db_transfer.py",
    "db/station_readings.py",
    "worker/day_values.py",
    "db/switch.py",
}
_ALLOWED_ENCLOSING_FUNCTIONS: Final = {
    "worker/wind_days.py": {"_persist_day"},
    "__main__.py": {"_day_has_raw_temperature"},
}


def test_wp10_station_wind_records_and_the_prune_call_are_scoped() -> None:
    """WP10, re-targeted (plan §7, §12.3, §12.5): text and ``ast`` scans of
    the package.

    M -> at the ``call_sites`` assertion: correct = ``["db/migrations.py"]``
    (the version 18 upgrade is the only caller of ``prune_wind_history``),
    mutant = that list plus the file of a new caller (a mutant that has any
    other module call the prune too would also be found by this scan, which
    walks every ``.py`` file under the package, not just
    ``db/migrations.py``).

    M -> at the ``reference_files`` assertion: correct = ``{"db/migrations.py"}``
    (every ``ast.Name`` reference in a ``Load`` context, every
    ``ast.Attribute`` access and every import ``ast.alias`` named
    ``prune_wind_history``, package-wide, points at only one file), mutant =
    that set plus the file of a new reference that is not itself a call
    (e.g. a re-export, or the name passed to another function without
    being called) -- the ``call_sites`` scan above only walks ``ast.Call``
    nodes and would miss such a reference. ``db/switch.py``'s own ``def
    prune_wind_history`` is an ``ast.FunctionDef``, not a ``Name``,
    ``Attribute`` or ``alias``, so it does not count here.
    """
    out_of_scope: list[str] = []
    call_sites: list[str] = []
    reference_files: set[str] = set()
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        rel = path.relative_to(_PACKAGE_ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        has_table = "station_wind_records" in text
        if has_table and rel not in _ALLOWED_STATION_WIND_RECORDS_FILES:
            allowed_funcs = _ALLOWED_ENCLOSING_FUNCTIONS.get(rel)
            if allowed_funcs is None:
                out_of_scope.append(rel)
            else:
                tree = ast.parse(text)
                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        segment = ast.get_source_segment(text, node) or ""
                        if (
                            "station_wind_records" in segment
                            and node.name not in allowed_funcs
                        ):
                            out_of_scope.append(f"{rel}:{node.name}")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            is_call = isinstance(node, ast.Call)
            is_plain_name = (
                is_call
                and isinstance(node.func, ast.Name)
                and node.func.id == "prune_wind_history"
            )
            is_attr = (
                is_call
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "prune_wind_history"
            )
            if is_plain_name or is_attr:
                call_sites.append(rel)
            if (
                isinstance(node, ast.Name)
                and node.id == "prune_wind_history"
                and isinstance(node.ctx, ast.Load)
            ):
                reference_files.add(rel)
            if isinstance(node, ast.Attribute) and node.attr == "prune_wind_history":
                reference_files.add(rel)
            if isinstance(node, ast.alias) and node.name == "prune_wind_history":
                reference_files.add(rel)
    assert out_of_scope == []
    assert call_sites == ["db/migrations.py"]
    assert reference_files == {"db/migrations.py"}

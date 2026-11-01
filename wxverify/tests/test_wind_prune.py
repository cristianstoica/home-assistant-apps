"""The wind-history prune (switch-over plan 2026-10-22-daily-close-switch-
over.md section 12; test IDs WP1-WP10 of 17.10).

Written before P6's code exists: ``wxverify.db.switch`` (``history_prune_
refusals``, ``prune_station_history``) and the CLI's ``wind-history prune``
subcommand are both new, so every test below either imports a name from a
module that does not exist yet (deferred into the test body, so the file's
other tests still collect) or exercises a CLI subcommand argparse does not
yet know. WP10 is a pure text/ast scan and needs no import of its own; it
fails today because the one call site it expects does not exist anywhere in
the package. Real SQLite throughout, via ``wxverify.db.connection.init_db``
on a tmp-file database (plan 17.1). Zone is ``UTC`` throughout (no DST, so
margins compare cleanly). Stations are synthetic: A (``ISTATION01``) and B
(``ISTATION02``) at site 1, C (``ISTATION03``) at site 2 (D, ``ISTATION04``,
unused), E and F (``ISTATION05``/``ISTATION06``) at site 3, unused.
"""

from __future__ import annotations

import ast
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

import pytest

from tests.helpers import D, day_close_site, note_day_clock
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.site_days import (
    begin_close,
    ensure_site_days,
    finish_close,
    start_site_days,
)

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
    """The three fixture categories of 17.10 for one station.

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
    """The WP fixture of 17.10's preamble, with site 1's close state toggled
    for WP2's case."""
    site1 = _build_site(conn, station_ids=(STATION_A, STATION_B), close_f=site1_closed)
    site2 = _build_site(conn, station_ids=(STATION_C, STATION_D), close_f=True)
    site3 = _build_site(conn, station_ids=(STATION_E, STATION_F), close_f=False)
    a_id = _station_id(conn, STATION_A)
    b_id = _station_id(conn, STATION_B)
    c_id = _station_id(conn, STATION_C)
    _seed_history(conn, a_id)
    _seed_history(conn, b_id)
    _seed_history(conn, c_id)
    return {
        "site1": site1,
        "site2": site2,
        "site3": site3,
        "a": a_id,
        "b": b_id,
        "c": c_id,
    }


def _row_count(conn: sqlite3.Connection, station_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM station_wind_records WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    return int(row[0])


def _row_exists(conn: sqlite3.Connection, station_id: int, obs_at: datetime) -> bool:
    row = conn.execute(
        "SELECT 1 FROM station_wind_records WHERE station_id = ? AND obs_at = ?",
        (station_id, isoformat_utc(obs_at)),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# WP1
# ---------------------------------------------------------------------------


def test_wp1_refusals_name_the_close_when_it_is_off(tmp_path: Path) -> None:
    """WP1 (plan 17.10): ``history_prune_refusals`` with the key absent.

    M53 -> at this assertion: correct = 1 (the close-off check contributes
    exactly one refusal line, and there are no sites to add a second),
    mutant = 0 (M53 removes the close check; with no site owning a
    fetched_at-null row either, the mutant's list is empty).
    """
    from wxverify.db.switch import history_prune_refusals

    conn = _init_tmp_db(tmp_path)
    refusals = history_prune_refusals(conn)
    assert len(refusals) == 1, refusals


# ---------------------------------------------------------------------------
# WP2
# ---------------------------------------------------------------------------


def test_wp2_refusal_tracks_f_state_and_never_names_a_rowless_site(
    tmp_path: Path,
) -> None:
    """WP2 (plan 17.10): site 1's F open, then closed; site 3 (no rows)
    never named either time.

    M54 -> at the ``open`` assertion: correct = 1 (site 1 owns a
    fetched_at-null row and its F is still open, so it is refused),
    mutant = 0 (M54 drops the F-state check entirely, so no site is ever
    refused on that ground).
    """
    from wxverify.db.switch import history_prune_refusals

    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn, site1_closed=False)

    open_refusals = history_prune_refusals(conn)
    assert len(open_refusals) == 1
    assert any(str(ids["site1"]) in line for line in open_refusals)
    assert not any(str(ids["site3"]) in line for line in open_refusals)

    begin_close(conn, ids["site1"], F, now_utc=_CLOSE_AT)
    finish_close(conn, ids["site1"], F, now_utc=_CLOSE_AT)

    closed_refusals = history_prune_refusals(conn)
    assert closed_refusals == []


# ---------------------------------------------------------------------------
# WP3
# ---------------------------------------------------------------------------


def test_wp3_prune_deletes_only_the_margin_bounded_null_rows_of_one_station(
    tmp_path: Path,
) -> None:
    """WP3 (plan 17.10): ``prune_station_history`` for station A.

    M55 -> at the ``set_old_kept`` assertion: correct = present (a row
    with ``fetched_at`` set is never this prune's, whatever its date),
    mutant = absent (M55 drops the ``fetched_at IS NULL`` filter, so A's
    ``set_old`` row is swept up with the rest).
    M56 -> at the ``near_kept`` assertion: correct = present (the delete's
    lower bound is F's start minus the 1200 s margin, so A's ``near`` row,
    inside the margin, survives), mutant = absent (M56 moves the bound to
    F's start itself with no margin, so ``near``, which is before F's
    start, is deleted too).
    M57 -> at the ``b_untouched`` assertion: correct = unchanged row count
    for B (the delete is scoped to ``station_id = A``), mutant = fewer
    rows for B (M57 drops the station filter, so A's delete also removes
    B's identically-dated rows).
    """
    from wxverify.db.switch import prune_station_history

    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn)
    a_id, b_id, c_id = ids["a"], ids["b"], ids["c"]

    b_before = _row_count(conn, b_id)
    c_before = _row_count(conn, c_id)

    deleted = prune_station_history(conn, station_id=a_id, now_utc=_CLOSE_AT)
    assert deleted == 1

    assert not _row_exists(conn, a_id, F_START - MARGIN - timedelta(seconds=800))
    assert _row_exists(conn, a_id, F_START - timedelta(seconds=600))  # near_kept
    assert _row_exists(conn, a_id, F_START + timedelta(seconds=1800))
    assert _row_exists(conn, a_id, F_START - timedelta(seconds=5000))  # set_old_kept

    assert _row_count(conn, b_id) == b_before  # b_untouched
    assert _row_count(conn, c_id) == c_before


# ---------------------------------------------------------------------------
# WP4
# ---------------------------------------------------------------------------


def test_wp4_command_requires_a_flag_before_opening_the_database(
    tmp_path: Path,
) -> None:
    """WP4 (plan 17.10): the command with neither ``--dry-run`` nor
    ``--restored-backup``, ``--db`` naming no file.

    M58 -> at the exit-code assertion: correct = 1 (the flag check runs
    before any database open, so a missing file is never created),
    mutant = 0 (M58 moves the flag check after the open, so with no
    conflicting flag state the run falls through to exit 0 -- and the
    database file this test asserts absent would exist instead).
    """
    from wxverify.__main__ import main

    db_path = tmp_path / "wxverify.db"
    with pytest.raises(SystemExit) as exc:
        main(["--db", str(db_path), "wind-history", "prune"])
    assert exc.value.code == 1
    assert not db_path.exists()


# ---------------------------------------------------------------------------
# WP5
# ---------------------------------------------------------------------------


def test_wp5_dry_run_reads_only_and_leaves_the_file_unchanged(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """WP5 (plan 17.10): ``--dry-run``, then ``--dry-run --json``, with
    site 1's F open.

    M59 -> at the ``hash_unchanged`` assertion: correct = equal (a
    read-only flag deletes nothing), mutant = not equal (M59 makes
    ``--dry-run`` delete, so the file's bytes change).
    """
    from wxverify.__main__ import main

    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn, site1_closed=False)
    close_db()
    db_path = tmp_path / "wxverify.db"
    before_hash = db_path.read_bytes()

    exit_code = main(["--db", str(db_path), "wind-history", "prune", "--dry-run"])
    out = capsys.readouterr().out
    assert exit_code == 0
    assert STATION_A in out
    assert str(ids["site1"]) in out

    exit_code_json = main(
        ["--db", str(db_path), "wind-history", "prune", "--dry-run", "--json"]
    )
    json_out = capsys.readouterr().out
    assert exit_code_json == 0
    assert str(ids["site1"]) in json_out

    after_hash = db_path.read_bytes()
    assert after_hash == before_hash  # hash_unchanged

    # M59 survives the F-open case above: with F open, history_prune_
    # refusals is never empty, so M59's "delete through a second connection
    # when refusals is empty" branch never even runs -- F open already
    # refuses on its own, by coincidence agreeing with the correct
    # read-only behaviour. Repeat with every site CLOSED, so refusals is
    # empty and there ARE rows a real run would delete (each station's
    # "old" row): only then can the mutant's dry-run delete actually show.
    closed_dir = tmp_path / "closed"
    closed_conn = _init_tmp_db(closed_dir)
    _build_fixture(closed_conn, site1_closed=True)
    close_db()
    closed_db_path = closed_dir / "wxverify.db"
    closed_before_hash = closed_db_path.read_bytes()

    closed_exit_code = main(
        ["--db", str(closed_db_path), "wind-history", "prune", "--dry-run"]
    )
    closed_out = capsys.readouterr().out
    assert closed_exit_code == 0
    assert STATION_A in closed_out
    assert "refusal" not in closed_out

    closed_after_hash = closed_db_path.read_bytes()
    assert closed_after_hash == closed_before_hash, (
        "M59 -> at this assertion: correct = equal (--dry-run deletes "
        "nothing even once every refusal is clear), mutant = not equal "
        "(M59 deletes through a second connection once refusals is empty)"
    )

    # Paired positive: the same closed-F setup, on a fresh copy, truly has
    # deletable rows -- a real run (no --dry-run) removes them. This proves
    # the hash-unchanged assertion above was not vacuous (there was
    # something to delete, and --dry-run correctly left it alone).
    real_dir = tmp_path / "closed_real"
    real_conn = _init_tmp_db(real_dir)
    real_ids = _build_fixture(real_conn, site1_closed=True)
    old_a_at = F_START - MARGIN - timedelta(seconds=800)
    assert _row_exists(real_conn, real_ids["a"], old_a_at)
    close_db()
    real_db_path = real_dir / "wxverify.db"

    real_exit_code = main(
        [
            "--db",
            str(real_db_path),
            "wind-history",
            "prune",
            "--restored-backup",
            "wp5-real",
        ]
    )
    assert real_exit_code == 0

    conn_after = _init_tmp_db(real_dir)
    assert not _row_exists(conn_after, real_ids["a"], old_a_at), (
        "the paired positive: a real run on the same closed-F setup "
        "deletes the row a dry run must leave alone"
    )


# ---------------------------------------------------------------------------
# WP6
# ---------------------------------------------------------------------------


def test_wp6_restored_backup_name_is_logged_and_a_repeat_deletes_nothing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WP6 (plan 17.10): ``--restored-backup bk1``; then the same again.

    M60 -> at the ``logged_with_name`` assertion: correct = present (each
    station's INFO line carries the backup name the owner passed),
    mutant = absent (M60 drops the backup name from the line, so it never
    appears).
    """
    import logging

    from wxverify.__main__ import main

    monkeypatch.setattr("wxverify.__main__._configure_logging", lambda: None)
    conn = _init_tmp_db(tmp_path)
    _build_fixture(conn)
    close_db()
    db_path = tmp_path / "wxverify.db"

    caplog.set_level(logging.INFO)
    exit_code = main(
        ["--db", str(db_path), "wind-history", "prune", "--restored-backup", "bk1"]
    )
    assert exit_code == 0
    logged_with_name = any("bk1" in record.message for record in caplog.records)
    assert logged_with_name

    caplog.clear()
    exit_code_again = main(
        ["--db", str(db_path), "wind-history", "prune", "--restored-backup", "bk1"]
    )
    assert exit_code_again == 0


# ---------------------------------------------------------------------------
# WP7
# ---------------------------------------------------------------------------


class _Stop(Exception):
    pass


def test_wp7_a_stop_mid_run_leaves_the_rest_for_a_repeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WP7 (plan 17.10): a wrapper raises ``Stop`` on the second
    ``prune_station_history`` call; then the command again.

    M61 -> at the ``first_station_pruned`` assertion (once a repeat
    command has run to completion): correct = gone (each station is its
    own transaction, so the first station's delete survives the second
    station's failure), mutant = present (M61 puts every station in one
    transaction, so the first station's delete rolls back with the
    second's failure).

    NOTE (gap): ``wxverify.__main__.prune_station_history`` is this
    report's assumption for where the CLI binds P6's name -- P6 has not
    landed, so the real binding site is unconfirmed. The assumption is
    modelled on ``processor.py``'s module-level binding of
    ``daily_close_on`` (plan 6.2) and on ``jobs purge``'s existing
    structure (``__main__.py:387-424``), the form 17.10's command is cast
    in (12.5).
    """
    from wxverify.__main__ import main

    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn)
    close_db()
    db_path = tmp_path / "wxverify.db"

    calls = {"n": 0}

    def _wrapper(
        conn: sqlite3.Connection, *, station_id: int, now_utc: datetime
    ) -> int:
        from wxverify.db.switch import prune_station_history as _real

        calls["n"] += 1
        if calls["n"] == 2:
            raise _Stop("WP7: forced stop on the second station")
        return _real(conn, station_id=station_id, now_utc=now_utc)

    monkeypatch.setattr("wxverify.__main__.prune_station_history", _wrapper)

    with pytest.raises(_Stop):
        main(
            ["--db", str(db_path), "wind-history", "prune", "--restored-backup", "bk1"]
        )

    # The real oracle, BEFORE any repeat run: a repeat always re-prunes
    # whatever rows still match, so checking only after the repeat cannot
    # tell "each station committed on its own" from "one shared
    # transaction" -- both converge to the same end state once the repeat
    # runs. ``Stop`` is an ``Exception`` (a ``BaseException``), and
    # ``Database.write_sync`` rolls back on any ``BaseException``
    # (``db/connection.py``): correct code commits station A's delete in
    # its OWN ``write_sync`` before station B's call raises, so A's delete
    # survives; M61 puts every station in ONE shared ``write_sync``, so the
    # same ``Stop`` rolls A's delete back along with B's half-done work.
    conn_mid = _init_tmp_db(tmp_path)
    mid_a_gone = not _row_exists(
        conn_mid, ids["a"], F_START - MARGIN - timedelta(seconds=800)
    )
    close_db()
    assert mid_a_gone, (
        "M61 -> at this assertion (before any repeat run): correct = True "
        "(station A's delete, committed in its own write_sync, survives "
        "station B's Stop), mutant = False (M61's single shared write_sync "
        "rolls station A's delete back along with the failure)"
    )

    monkeypatch.undo()
    exit_code = main(
        ["--db", str(db_path), "wind-history", "prune", "--restored-backup", "bk1"]
    )
    assert exit_code == 0

    conn2 = _init_tmp_db(tmp_path)
    assert not _row_exists(
        conn2, ids["a"], F_START - MARGIN - timedelta(seconds=800)
    )  # first_station_pruned


# ---------------------------------------------------------------------------
# WP8
# ---------------------------------------------------------------------------


def test_wp8_a_refusing_guard_deletes_nothing_for_that_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WP8 (plan 17.10): a wrapper on P1's ``check_day_write`` in
    ``db/switch.py`` that refuses.

    M62 -> at the ``nothing_deleted`` assertion: correct = unchanged row
    count (the guard is asked before the delete, and a refusal raises
    before any row is removed), mutant = fewer rows (M62 never asks the
    guard, so the delete runs regardless).

    NOTE (gap): same assumption as WP7 on where P6 binds the guard's name
    inside ``db/switch.py`` -- modelled on 12.5's ``check_day_write(conn,
    site_id, ..., WriteKind.HISTORY_PRUNE, now_utc=now_utc)`` call.
    """
    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn)
    close_db()

    def _refuse(*args: object, **kwargs: object) -> bool:
        return False

    monkeypatch.setattr("wxverify.db.switch.check_day_write", _refuse)

    from wxverify.db.switch import prune_station_history

    conn2 = _init_tmp_db(tmp_path)
    before = _row_count(conn2, ids["a"])
    with pytest.raises(Exception):  # noqa: B017, PT011
        prune_station_history(conn2, station_id=ids["a"], now_utc=_CLOSE_AT)
    assert _row_count(conn2, ids["a"]) == before  # nothing_deleted


# ---------------------------------------------------------------------------
# WP9
# ---------------------------------------------------------------------------


def test_wp9_a_race_after_the_last_delete_fails_the_final_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WP9 (plan 17.10): a wrapper that, on the last call, inserts a
    matching row on a second connection.

    M63 -> at the exit-code assertion: correct = 1, with 1 printed
    (correct re-reads the final count after every station's delete, so
    the race's row is seen), mutant = 0 (M63 exits 0 without reading the
    final count, so the race is never noticed).

    NOTE (gap): same binding assumption as WP7/WP8; the race is
    constructed with a second ``sqlite3.connect`` on the same file,
    inserted before the wrapper's own write_sync commits, to make the
    rendezvous deterministic rather than timing-dependent.
    """
    from wxverify.__main__ import main

    conn = _init_tmp_db(tmp_path)
    ids = _build_fixture(conn)
    close_db()
    db_path = tmp_path / "wxverify.db"

    def _wrapper(
        conn: sqlite3.Connection, *, station_id: int, now_utc: datetime
    ) -> int:
        from wxverify.db.switch import prune_station_history as _real

        result = _real(conn, station_id=station_id, now_utc=now_utc)
        if station_id == ids["c"]:  # the last station processed, site 2's C
            conn.execute(
                "INSERT INTO station_wind_records"
                " (station_id, obs_at, source, local_date, speed_kmh, fetched_at)"
                " VALUES (?, ?, 'five_minute', ?, 5.0, NULL)",
                (
                    ids["a"],
                    isoformat_utc(F_START - MARGIN - timedelta(seconds=900)),
                    (F_START - MARGIN - timedelta(seconds=900)).date().isoformat(),
                ),
            )
        return result

    monkeypatch.setattr("wxverify.__main__.prune_station_history", _wrapper)

    exit_code = main(
        ["--db", str(db_path), "wind-history", "prune", "--restored-backup", "bk1"]
    )
    assert exit_code == 1


# ---------------------------------------------------------------------------
# WP10
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
    """WP10 (plan 17.10, 12.3, 12.5): text and ``ast`` scans of the package.

    M64 -> at the ``call_sites`` assertion: correct = ``["__main__.py"]``
    (the CLI command is the only caller of ``prune_station_history``),
    mutant = that list plus the file B2 lives in (M64 has B2 call the
    prune too, which this scan -- walking every ``.py`` file under the
    package -- would also find, since it checks every file for the call,
    not just ``__main__.py``).
    """
    out_of_scope: list[str] = []
    call_sites: list[str] = []
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
                and node.func.id == "prune_station_history"
            )
            is_attr = (
                is_call
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "prune_station_history"
            )
            if is_plain_name or is_attr:
                call_sites.append(rel)
    assert out_of_scope == []
    assert call_sites == ["__main__.py"]

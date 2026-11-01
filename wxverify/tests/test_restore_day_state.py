"""#71's day-state check (switch-over plan 2026-10-22-daily-close-switch-
over.md section 13.4; test IDs RS1-RS5 of 17.14).

These pin #71's read-only probe text, copied verbatim from 13.4, not P6's
code (18.1: "RS1 to RS5 have no mutant"). They may legitimately pass before
P6 lands (17's Step 3 note) since nothing here imports a P6 name: ``site_
days``, ``close_try_log`` and ``runtime_state`` are all P1/P3 tables, and
the probe itself is #71's SQL, not wxverify's.

Each test opens its own tmp-file database as #71 opens its file (``mode=
ro``, ``PRAGMA query_only=ON``), runs the exact lines of 13.4 with ``exec``,
and reproduces 13.4's post-stop comparison (the two ``grep`` calls) in
Python rather than a subprocess, since both inputs are already in memory.
Zone is ``UTC`` throughout; synthetic dates only.
"""

from __future__ import annotations

import io
import sqlite3
from contextlib import redirect_stdout
from pathlib import Path
from typing import Final

from tests.helpers import day_close_site, note_day_clock
from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.db.site_days import begin_close, ensure_site_days, finish_close

# The lines inserted into <scratch>/tools/probe.py just before con.close()
# (plan 13.4), copied verbatim.
_PROBE_TAIL: Final = """
tables = {r[0] for r in con.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table'"
)}
r = one("SELECT value FROM runtime_state WHERE key = 'daily_close_on_at'")
print("close_on", r[0] if r else "absent")
if "site_days" in tables:
    for row in con.execute(
        "SELECT site_id, min(local_date) FROM site_days"
        " GROUP BY site_id ORDER BY site_id"
    ):
        print("first_day", *row)
    for row in con.execute(
        "SELECT site_id, local_date, state, done_at, missed_reason"
        " FROM site_days"
        " WHERE state IN ('closed', 'missed') ORDER BY site_id, local_date"
    ):
        print("done", *row)
    s = (one(
        "SELECT value FROM runtime_state WHERE key = 'worker_started_at'"
    ) or (None,))[0]
    t = (one(
        "SELECT value FROM runtime_state WHERE key = 'scheduler_last_tick_at'"
    ) or (None,))[0]
    print("bad_instants", one(
        "SELECT (julianday(?) IS NULL) + (julianday(?) IS NULL)"
        " + (SELECT count(*) FROM site_days WHERE date(local_date) IS NOT"
        " local_date OR julianday(day_end_utc) IS NULL)"
        " + (SELECT count(*) FROM site_days WHERE retry_at IS NOT NULL AND"
        " julianday(retry_at) IS NULL)"
        " + (SELECT count(*) FROM close_try_log WHERE julianday(tried_at)"
        " IS NULL)",
        s, t,
    )[0])
    print("tick_ran", one("SELECT julianday(?) > julianday(?)", t, s)[0])
    first_due_gone = (
        "julianday(d.day_end_utc) + 7.0 / 24 <= julianday(?)"
        " AND NOT EXISTS (SELECT 1 FROM site_days AS n WHERE n.site_id ="
        " d.site_id AND n.local_date = date(d.local_date, '+1 day') AND"
        " n.zone_changeover = 1)"
    )
    print("stale_open", one(
        "SELECT count(*) FROM site_days AS d JOIN sites AS x ON x.id ="
        " d.site_id"
        " AND x.enabled = 1 WHERE d.state IN ('open', 'closing', 'retry_due')"
        " AND CASE WHEN d.state = 'retry_due' OR d.attempts = 2"
        " THEN julianday(d.retry_at) < julianday(?) ELSE " + first_due_gone
        + " END",
        s, s,
    )[0])
    print("old_fills", one(
        "SELECT count(*) FROM close_try_log AS g JOIN site_days AS d"
        " ON d.site_id = g.site_id AND d.local_date = g.local_date"
        " WHERE g.slot = 'gap_fill' AND julianday(g.tried_at) >= julianday(?)"
        " AND CASE WHEN g.close_attempt = 2"
        " THEN julianday(d.retry_at) < julianday(?) ELSE " + first_due_gone
        + " END",
        s, s, s,
    )[0])
"""


def _run_probe(con: sqlite3.Connection) -> str:
    """Runs 13.4's lines with ``exec``, #71's ``one`` bound to ``con``."""

    def one(sql: str, *params: object) -> tuple[object, ...] | None:
        return con.execute(sql, params).fetchone()

    buf = io.StringIO()
    with redirect_stdout(buf):
        exec(_PROBE_TAIL, {"con": con, "one": one})  # noqa: S102
    return buf.getvalue()


def _readonly(db_path: Path) -> sqlite3.Connection:
    """#71's own open: ``mode=ro``, ``PRAGMA query_only=ON``."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.execute("PRAGMA query_only=ON")
    return con


def _done_lines(probe_output: str) -> set[str]:
    return {line for line in probe_output.splitlines() if line.startswith("done ")}


def _field(probe_output: str, name: str) -> str | None:
    for line in probe_output.splitlines():
        if line.startswith(name + " "):
            return line[len(name) + 1 :]
    return None


# ---------------------------------------------------------------------------
# RS1
# ---------------------------------------------------------------------------


def test_rs1_a_pre_switch_schema_prints_close_on_absent_and_nothing_else(
    tmp_path: Path,
) -> None:
    """RS1 (plan 13.4, row 1 of the table, PASS): a pre-switch schema (no
    ``site_days``, no ``close_try_log``) prints ``close_on absent`` and
    nothing else.
    """
    db_path = tmp_path / "pre_switch.db"
    con = sqlite3.connect(str(db_path))
    con.execute(
        "CREATE TABLE runtime_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    con.commit()
    con.close()

    ro = _readonly(db_path)
    output = _run_probe(ro)
    ro.close()

    assert output == "close_on absent\n"


# ---------------------------------------------------------------------------
# RS2
# ---------------------------------------------------------------------------


def _build_post_switch_db(base: Path) -> tuple[sqlite3.Connection, Path, int]:
    close_db()
    base.mkdir(parents=True, exist_ok=True)
    db_path = base / "wxverify.db"
    config.db_path = str(db_path)
    options_path = base / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone="UTC", close_on=True, with_records=False)
    return conn, db_path, site_id


def test_rs2_a_post_switch_backup_after_a_correct_first_tick_passes(
    tmp_path: Path,
) -> None:
    """RS2 (plan 13.4, row 2, PASS): a post-switch backup and a run whose
    first tick moved the old open days to ``missed``, holding also a day
    still ``retry_due`` whose ``retry_at`` is after S, a retry gap-fill
    row tried after S for a day whose ``retry_at`` is after S, and a day
    still ``open`` that ended seven hours before S and is followed by a
    changeover day.
    """
    conn, db_path, site_id = _build_post_switch_db(tmp_path)
    s = "2024-06-10T00:00:00.000000Z"
    t = "2024-06-10T00:01:00.000000Z"
    from wxverify.db.runtime_state import set_runtime_state

    set_runtime_state(conn, "worker_started_at", s)
    set_runtime_state(conn, "scheduler_last_tick_at", t)

    _seed_days(conn, site_id)

    # Day A: already closed well before S -- a "done" line in both pre and
    # post (unchanged; the PASS table's "0 missing done lines").
    close_at = _dt("2024-06-06T00:30:00Z")
    begin_close(conn, site_id, _date("2024-06-05"), now_utc=close_at)
    finish_close(conn, site_id, _date("2024-06-05"), now_utc=close_at)

    # Day C: retry_due, retry_at AFTER S -- not yet due, excluded from
    # stale_open.
    conn.execute(
        "UPDATE site_days SET state='retry_due', attempts=1, retry_at=?"
        " WHERE site_id=? AND local_date=?",
        ("2024-06-10T01:00:00Z", site_id, "2024-06-07"),
    )
    # Day D: a retry gap-fill row for Day C, tried after S.
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id=? ORDER BY id LIMIT 1", (site_id,)
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO close_try_log"
        " (site_id, local_date, station_id, kind, slot, close_attempt,"
        "  tried_at, outcome, readings_kept, readings_refused)"
        " VALUES (?, ?, ?, 'hourly', 'gap_fill', 2, ?, 'ok', 1, 0)",
        (site_id, "2024-06-07", station_id, "2024-06-10T00:30:00Z"),
    )

    # Day E: open, ended 7+ hours before S, followed by a changeover day --
    # unjudged, excluded from stale_open.
    conn.execute(
        "UPDATE site_days SET zone_changeover=1 WHERE site_id=? AND local_date=?",
        (site_id, "2024-06-09"),
    )

    pre = _run_probe(_readonly(db_path))

    # Day B: simulate the first tick moving an old open day to missed.
    conn.execute(
        "UPDATE site_days SET state='missed', missed_reason='never_attempted',"
        " done_at=? WHERE site_id=? AND local_date=?",
        ("2024-06-10T00:01:00.000000Z", site_id, "2024-06-06"),
    )

    post = _run_probe(_readonly(db_path))

    missing_done = _done_lines(pre) - _done_lines(post)
    assert missing_done == set()
    assert _field(post, "close_on") is not None
    assert _field(post, "bad_instants") == "0"
    assert _field(post, "tick_ran") == "1"
    assert _field(post, "stale_open") == "0"
    assert _field(post, "old_fills") == "0"


# ---------------------------------------------------------------------------
# RS3
# ---------------------------------------------------------------------------


def _date(s: str) -> object:
    from datetime import date

    return date.fromisoformat(s)


def _dt(s: str) -> object:
    from wxverify.core.timeutil import parse_utc

    return parse_utc(s)


_F0_START: Final = "2024-06-05T00:00:00Z"
_FILL_THROUGH: Final = "2024-06-11T00:00:00Z"


def _seed_days(conn: sqlite3.Connection, site_id: int) -> None:
    """Site days 06-05 through 06-11, every one ``open`` by default."""
    from wxverify.db.site_days import start_site_days

    start_site_days(conn, site_id, _date("2024-06-05"), now_utc=_dt(_F0_START))
    # ensure_site_days may create a record ahead of its own now_utc (a
    # look-ahead margin), so check_day_close_records's bound must be raised
    # to this call's clock BEFORE the rows are written, or a correctly-built
    # fixture fails that generic helper check instead of a real assertion.
    ensure_site_days(conn, site_id, now_utc=note_day_clock(_dt(_FILL_THROUGH)))


def test_rs3_each_divergence_fails(tmp_path: Path) -> None:
    """RS3 (plan 13.4, row 2, FAIL each time): an old day still ``open``
    (stale_open above 0); a day still ``retry_due`` whose ``retry_at`` is
    before S (stale_open above 0); a retry gap-fill row tried after S for
    a day whose ``retry_at`` is before S (old_fills above 0)."""
    conn, db_path, site_id = _build_post_switch_db(tmp_path)
    s = "2024-06-10T00:00:00.000000Z"
    t = "2024-06-10T00:01:00.000000Z"
    from wxverify.db.runtime_state import set_runtime_state

    set_runtime_state(conn, "worker_started_at", s)
    set_runtime_state(conn, "scheduler_last_tick_at", t)
    _seed_days(conn, site_id)

    # An old day left open: ended well over seven hours before S, no
    # changeover day following it.
    conn.execute(
        "UPDATE site_days SET state='open' WHERE site_id=? AND local_date=?",
        (site_id, "2024-06-05"),
    )
    post_open = _run_probe(_readonly(db_path))
    assert int(_field(post_open, "stale_open") or "0") > 0

    close_db()
    conn, db_path, site_id = _build_post_switch_db(tmp_path / "second")
    set_runtime_state(conn, "worker_started_at", s)
    set_runtime_state(conn, "scheduler_last_tick_at", t)
    _seed_days(conn, site_id)
    conn.execute(
        "UPDATE site_days SET state='retry_due', attempts=1, retry_at=?"
        " WHERE site_id=? AND local_date=?",
        ("2024-06-09T00:00:00Z", site_id, "2024-06-07"),
    )
    post_retry = _run_probe(_readonly(db_path))
    assert int(_field(post_retry, "stale_open") or "0") > 0

    close_db()
    conn, db_path, site_id = _build_post_switch_db(tmp_path / "third")
    set_runtime_state(conn, "worker_started_at", s)
    set_runtime_state(conn, "scheduler_last_tick_at", t)
    _seed_days(conn, site_id)
    conn.execute(
        "UPDATE site_days SET state='retry_due', attempts=1, retry_at=?"
        " WHERE site_id=? AND local_date=?",
        ("2024-06-09T00:00:00Z", site_id, "2024-06-07"),
    )
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id=? ORDER BY id LIMIT 1", (site_id,)
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO close_try_log"
        " (site_id, local_date, station_id, kind, slot, close_attempt,"
        "  tried_at, outcome, readings_kept, readings_refused)"
        " VALUES (?, ?, ?, 'hourly', 'gap_fill', 2, ?, 'ok', 1, 0)",
        (site_id, "2024-06-07", station_id, "2024-06-10T00:30:00Z"),
    )
    post_fill = _run_probe(_readonly(db_path))
    assert int(_field(post_fill, "old_fills") or "0") > 0


# ---------------------------------------------------------------------------
# RS4
# ---------------------------------------------------------------------------


def test_rs4_unparseable_instants_and_a_stale_tick_are_cant_tell(
    tmp_path: Path,
) -> None:
    """RS4 (plan 13.4, row 2, CAN'T TELL): ``scheduler_last_tick_at``
    before S; ``worker_started_at`` unreadable; a ``retry_at`` that does
    not parse."""
    conn, db_path, site_id = _build_post_switch_db(tmp_path)
    from wxverify.db.runtime_state import set_runtime_state

    _seed_days(conn, site_id)

    set_runtime_state(conn, "worker_started_at", "2024-06-10T00:00:00.000000Z")
    set_runtime_state(conn, "scheduler_last_tick_at", "2024-06-09T00:00:00.000000Z")
    post_stale_tick = _run_probe(_readonly(db_path))
    assert _field(post_stale_tick, "tick_ran") == "0"

    set_runtime_state(conn, "worker_started_at", "not-a-timestamp")
    set_runtime_state(conn, "scheduler_last_tick_at", "2024-06-10T00:01:00.000000Z")
    post_unreadable = _run_probe(_readonly(db_path))
    assert int(_field(post_unreadable, "bad_instants") or "0") > 0

    set_runtime_state(conn, "worker_started_at", "2024-06-10T00:00:00.000000Z")
    conn.execute(
        "UPDATE site_days SET state='retry_due', attempts=1, retry_at='not-a-timestamp'"
        " WHERE site_id=? AND local_date=?",
        (site_id, "2024-06-07"),
    )
    post_bad_retry = _run_probe(_readonly(db_path))
    assert int(_field(post_bad_retry, "bad_instants") or "0") > 0


# ---------------------------------------------------------------------------
# RS5
# ---------------------------------------------------------------------------


def test_rs5_the_read_only_probe_changes_nothing(tmp_path: Path) -> None:
    """RS5 (plan 13.4): every file's hash unchanged and its ``-wal`` 0
    bytes after the probe."""
    conn, db_path, _site_id = _build_post_switch_db(tmp_path)
    close_db()

    before_hash = db_path.read_bytes()
    ro = _readonly(db_path)
    _run_probe(ro)
    ro.close()
    after_hash = db_path.read_bytes()

    assert after_hash == before_hash
    wal_path = db_path.with_name(db_path.name + "-wal")
    if wal_path.exists():
        assert wal_path.stat().st_size == 0

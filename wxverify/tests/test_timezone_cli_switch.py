"""The ``timezone change`` command after the switch (plan
``2026-10-22-daily-close-switch-over.md`` §10.4, §17.9, TC1).

While the daily close is off, ``timezone change`` must refuse with "a zone
change can be entered once the daily close is on" (R16) instead of its
current close-off branch, which applies a prospective change through
``apply_prospective_change``. ``wxverify.__main__`` is not changed yet, so
the close-off half of this test fails today for exactly that reason: it
still prints "--effective-from is required while the daily close is off"
(or, when given, applies the change) rather than the plan's retirement
message -- the literal mutant M52 describes ("timezone change still calls
apply_prospective_change with the close off").

The close-on half is an unchanged regression pin (paired positive): P1's
``enter_zone_change`` path is untouched by this plan, so it must keep
working exactly as today, both before and after M52's fix lands.

Harness copied from ``tests/test_timezone_cli.py``'s DO2
(``test_do2_change_enters_a_zone_change_while_the_close_is_on``): a tmp-file
DB through ``init_db``/``close_db``, ``main(["--db", ...])``, and
``tests.helpers.day_close_site`` for the close-on fixture. Synthetic zones
only (``UTC``, ``Etc/GMT-14``); no live site, station or coordinate.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.__main__ import main
from wxverify.db.connection import close_db, get_db, init_db

_START_TZ = "UTC"
_CHANGE_TZ = "Etc/GMT-14"


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _generations() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM timezone_generations ORDER BY id"
            ).fetchall()
        ]

    return get_db().read_sync(_read)


def test_tc1_change_is_refused_while_off_and_works_as_before_while_on(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import datetime as _dt

    from tests.helpers import NOW, D, close_off, day_close_site

    conn = _init_tmp_db(tmp_path)
    close_off(conn)
    monkeypatch.setattr("wxverify.__main__.utc_now", lambda: NOW)

    # --- Key absent: the close is off (plan 10.4, R16). M52. ---------------
    site_id = get_db().write_sync(
        lambda c: day_close_site(
            c, timezone=_START_TZ, close_on=False, with_records=False
        )
    )
    generations_before = _generations()

    rc_off = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            _CHANGE_TZ,
        ]
    )
    out_off = capsys.readouterr().out

    assert rc_off == 1, (
        "M52 -> at this assertion: correct = exit 1 while the close is "
        f"off, mutant = {rc_off!r}"
    )
    assert "a zone change can be entered once the daily close is on" in out_off, (
        "M52 -> at this assertion: correct = "
        "'a zone change can be entered once the daily close is on' in the "
        f"output, mutant = {out_off!r} (still the close-off "
        "--effective-from message or a successful prospective change)"
    )
    assert "Traceback" not in out_off
    assert _generations() == generations_before, (
        "M52 -> at this assertion: correct = timezone_generations unchanged "
        f"by a refused close-off attempt, mutant = {_generations()!r} vs "
        f"before {generations_before!r}"
    )

    # --- Key set: the close is on. Unchanged regression pin (P1's own
    # path, not part of this plan's fix) -- a paired positive next to the
    # close-off refusal above. ----------------------------------------------
    # stations.pws_station_id is UNIQUE across the whole db (not just per
    # site); the close-off site above already took the default ids, so
    # this second site needs its own.
    site_id_on = get_db().write_sync(
        lambda c: day_close_site(
            c,
            timezone=_START_TZ,
            close_on=True,
            with_records=True,
            station_ids=("ISTATION03", "ISTATION04"),
        )
    )
    generations_before_on = _generations()

    rc_on = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id_on),
            "--timezone",
            _CHANGE_TZ,
        ]
    )
    out_on = capsys.readouterr().out
    changeover_date = (D + _dt.timedelta(days=1)).isoformat()
    first_full_day = (D + _dt.timedelta(days=2)).isoformat()

    assert rc_on == 0, f"the close-on path must keep working unchanged: {out_on!r}"
    assert changeover_date in out_on
    assert first_full_day in out_on
    assert "a zone change can be entered once the daily close is on" not in out_on
    assert _generations() == generations_before_on, (
        "entering a zone change while the close is on must not touch "
        f"timezone_generations: before={generations_before_on!r}, "
        f"after={_generations()!r}"
    )

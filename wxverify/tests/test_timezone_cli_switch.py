"""The ``timezone change`` command, the daily close always on (TC1).

The ``enter_zone_change`` path applies a zone change unconditionally now
(the close-off branch and its refusal are gone): a regression pin (paired
positive) that it keeps working exactly as before this cleanup.

Harness copied from ``tests/test_timezone_cli.py``'s DO2
(``test_do2_change_enters_a_zone_change_while_the_close_is_on``): a tmp-file
DB through ``init_db``/``close_db``, ``main(["--db", ...])``, and
``tests.helpers.day_close_site`` for the fixture. Synthetic zones only
(``UTC``, ``Etc/GMT-14``); no live site, station or coordinate.
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


def test_tc1_change_enters_a_zone_change(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import datetime as _dt

    from tests.helpers import NOW, D, day_close_site

    _init_tmp_db(tmp_path)
    monkeypatch.setattr("wxverify.__main__.utc_now", lambda: NOW)

    # A regression pin that this path keeps working unchanged.
    site_id_on = get_db().write_sync(
        lambda c: day_close_site(
            c,
            timezone=_START_TZ,
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

    assert rc_on == 0, f"the zone change path must keep working unchanged: {out_on!r}"
    assert changeover_date in out_on
    assert first_full_day in out_on
    assert _generations() == generations_before_on, (
        "entering a zone change must not touch timezone_generations:"
        f" before={generations_before_on!r}, after={_generations()!r}"
    )

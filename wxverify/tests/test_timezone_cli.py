"""Smoke coverage for the `wxverify timezone change` operator command.
`status` and `correct` are retired to a retirement message in
`wxverify.__main__.main` and have no tests here.

These prove the invocation path exists and behaves: `change`
reaches the generation machinery, and a rejected operation exits non-zero
with a message rather than a traceback.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.__main__ import main
from wxverify.api.schemas import SiteUpdate
from wxverify.db.connection import close_db, get_db, init_db

#: Synthetic zones — deliberately not any real deployment's timezone.
_START_TZ = "UTC"
_PROSPECTIVE_TZ = "Etc/GMT-5"


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES ('Synthetic Site', 10, 20, 100, ?, 1)
            """,
            (_START_TZ,),
        ).lastrowid
    )


def _generations() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM timezone_generations ORDER BY id"
            ).fetchall()
        ]

    return get_db().read_sync(_read)


def test_do2_change_enters_a_zone_change(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """DO2: ``timezone change`` enters the zone change through
    ``enter_zone_change`` instead of ``apply_prospective_change``,
    ``--effective-from`` is forbidden, and an unknown zone is refused the
    same way. A fourth sub-case, on a second database whose helper site has
    ``with_records=False`` and was started at D+1 (as DS15c starts it),
    prints a first full day with no changeover date.

    ``wxverify.__main__`` imports ``utc_now``, so this monkeypatch
    fixes the clock the CLI reads. The ``site_days`` rows the change
    makes are checked by DS15 and DS15c in ``tests/test_site_days.py``.
    """
    import datetime as _dt

    from tests.helpers import NOW, D, day_close_site

    _init_tmp_db(tmp_path)
    monkeypatch.setattr("wxverify.__main__.utc_now", lambda: NOW)
    site_id = get_db().write_sync(
        lambda c: day_close_site(c, timezone=_START_TZ, with_records=True)
    )

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            "Etc/GMT-1",
        ]
    )
    out = capsys.readouterr().out
    changeover_date = (D + _dt.timedelta(days=1)).isoformat()
    first_full_day = (D + _dt.timedelta(days=2)).isoformat()

    assert rc == 0
    assert changeover_date in out
    assert first_full_day in out
    assert [(r["mode"], r["state"]) for r in _generations()] == [
        ("initial", "published")
    ]

    rc2 = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            "Etc/GMT-2",
            "--effective-from",
            "2026-09-01T00:00:00Z",
        ]
    )
    out2 = capsys.readouterr().out
    assert rc2 == 1
    assert "Traceback" not in out2
    assert "leave out --effective-from" in out2
    assert [(r["mode"], r["state"]) for r in _generations()] == [
        ("initial", "published")
    ]

    rc3 = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            "Not/AZone",
        ]
    )
    out3 = capsys.readouterr().out
    assert rc3 == 1
    assert "Traceback" not in out3
    assert [(r["mode"], r["state"]) for r in _generations()] == [
        ("initial", "published")
    ]

    second_dir = tmp_path / "second"
    second_dir.mkdir()
    _init_tmp_db(second_dir)
    second_site_id = get_db().write_sync(
        lambda c: day_close_site(c, timezone=_START_TZ, with_records=False)
    )
    from wxverify.db.site_days import start_site_days

    get_db().write_sync(
        lambda c: start_site_days(
            c, second_site_id, D + _dt.timedelta(days=1), now_utc=NOW
        )
    )

    rc4 = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(second_site_id),
            "--timezone",
            "Etc/GMT-14",
        ]
    )
    out4 = capsys.readouterr().out
    assert rc4 == 0
    assert first_full_day in out4
    assert changeover_date not in out4


def test_do2b_change_on_an_unknown_site(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """DO2: an unknown site is refused. The existence check
    inside ``enter_zone_change``, via ``_require_site_timezone``, runs
    before any date logic, so ``_timezone_change``'s ``DayStateError``
    catch reports it with a message instead of a traceback."""
    from tests.helpers import day_close_site

    _init_tmp_db(tmp_path)
    get_db().write_sync(
        lambda c: day_close_site(c, timezone=_START_TZ, with_records=False)
    )

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            "404",
            "--timezone",
            "Etc/GMT-1",
        ]
    )
    captured = capsys.readouterr()

    assert rc == 1
    assert "site 404 does not exist" in captured.out
    assert "Traceback" not in captured.out
    assert "Traceback" not in captured.err


def test_site_update_still_cannot_carry_a_timezone() -> None:
    """The other half: the CLI is the ONLY way in, because a normal site
    edit has no timezone field to rewrite history with."""
    assert "timezone" not in SiteUpdate.model_fields

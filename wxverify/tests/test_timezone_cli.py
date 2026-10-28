"""Smoke coverage for the `wxverify timezone` operator commands (§13/§20).

These prove the §20 step-2 invocation path exists and behaves: each command
runs end to end against a temporary DB, the two mutating ones reach the
generation machinery, and a rejected operation exits non-zero with a message
rather than a traceback. The behavioural oracles for the correction chain
itself live with the verification suites.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.__main__ import main
from wxverify.api.schemas import SiteUpdate
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.tz_generations import published_pointer_key

#: Synthetic zones — deliberately not any real deployment's timezone.
_START_TZ = "UTC"
_CORRECTED_TZ = "Etc/GMT-3"
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


def test_status_reports_nothing_before_a_site_has_a_generation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)

    rc = main(["--db", config.db_path, "timezone", "status"])

    assert rc == 0
    assert "no timezone generations" in capsys.readouterr().out


def test_status_unknown_site_exits_one_with_a_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _init_tmp_db(tmp_path)

    rc = main(["--db", config.db_path, "timezone", "status", "--site-id", "404"])

    assert rc == 1
    assert "site 404 does not exist" in capsys.readouterr().out


def test_correct_starts_a_building_generation_and_says_what_happens(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "correct",
            "--site-id",
            str(site_id),
            "--timezone",
            _CORRECTED_TZ,
        ]
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert "retrospective correction started" in out
    assert "background job" in out
    assert "retroactively" in out

    rows = _generations()
    assert [(r["mode"], r["state"]) for r in rows] == [
        ("initial", "published"),
        ("retrospective_correction", "building"),
    ]
    # Readers keep serving the previous generation: the pointer has not moved.
    pointer = get_db().read_sync(
        lambda c: c.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (published_pointer_key(site_id),),
        ).fetchone()
    )
    assert int(pointer["value"]) == int(rows[0]["id"])
    # ...and the rebuild is queued rather than run inline.
    queued = get_db().read_sync(
        lambda c: c.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type = 'timezone_correction'"
        ).fetchone()
    )
    assert int(queued["n"]) == 1


def test_status_json_surfaces_generations_and_reconciliation_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    main(
        [
            "--db",
            config.db_path,
            "timezone",
            "correct",
            "--site-id",
            str(site_id),
            "--timezone",
            _CORRECTED_TZ,
        ]
    )
    capsys.readouterr()
    # The chain fills these as it walks the days; status must show them.
    get_db().write_sync(
        lambda c: c.execute(
            """
            UPDATE timezone_generations
            SET examined_count = 10, changed_count = 4,
                unchanged_count = 6, excluded_count = 0
            WHERE state = 'building'
            """
        )
    )

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "status",
            "--site-id",
            str(site_id),
            "--json",
        ]
    )
    out = capsys.readouterr().out
    # CLI logging shares stdout, so the payload starts at the first brace.
    payload = json.loads(out[out.index("{") :])

    assert rc == 0
    generations = payload["generations"]
    assert [g["mode"] for g in generations] == [
        "initial",
        "retrospective_correction",
    ]
    assert [g["published_pointer"] for g in generations] == [True, False]
    building = generations[1]
    assert building["timezone"] == _CORRECTED_TZ
    assert (
        building["examined"],
        building["changed"],
        building["unchanged"],
        building["excluded"],
    ) == (10, 4, 6, 0)


def test_correct_refuses_a_second_concurrent_build(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    argv = [
        "--db",
        config.db_path,
        "timezone",
        "correct",
        "--site-id",
        str(site_id),
        "--timezone",
        _CORRECTED_TZ,
    ]
    assert main(argv) == 0
    capsys.readouterr()

    rc = main(argv)
    out = capsys.readouterr().out

    assert rc == 1
    assert "already has a correction building" in out
    assert "Traceback" not in out
    assert len(_generations()) == 2


def test_correct_rejects_an_unknown_timezone(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "correct",
            "--site-id",
            str(site_id),
            "--timezone",
            "Not/AZone",
        ]
    )

    assert rc == 1
    assert "unknown IANA timezone" in capsys.readouterr().out
    # The rejected operation left nothing behind, not even a seeded initial.
    assert _generations() == []


def test_change_closes_the_prior_generation_and_reports_the_interval(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            _PROSPECTIVE_TZ,
            "--effective-from",
            "2026-09-01T00:00:00Z",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert "prospective change applied" in out
    assert "2026-09-01T00:00:00Z" in out
    assert "no rebuild runs" in out

    rows = _generations()
    assert [(r["mode"], r["state"]) for r in rows] == [
        ("initial", "published"),
        ("prospective_change", "published"),
    ]
    assert str(rows[0]["effective_to"]) == "2026-09-01T00:00:00Z"
    assert str(rows[1]["effective_from"]) == "2026-09-01T00:00:00Z"
    pointer = get_db().read_sync(
        lambda c: c.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (published_pointer_key(site_id),),
        ).fetchone()
    )
    assert int(pointer["value"]) == int(rows[1]["id"])


def test_change_rejects_an_unparseable_effective_from(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            _PROSPECTIVE_TZ,
            "--effective-from",
            "next tuesday",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 1
    assert "Traceback" not in out
    assert _generations() == []


def test_do1b_change_without_effective_from_while_the_close_is_off(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §13.7 DO1b: while the close is off, ``--effective-from`` stays
    required, but through a custom check with its own message -- not
    argparse's bare ``required=True``, which exits 2 with argparse's own
    usage text and no such message. The flag is declared without
    ``required=True`` (`__main__.py:204`); whether it may or must be given
    is decided inside ``_timezone_change``'s write transaction, which reads
    the daily close and raises this ``ValueError`` when it is off."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    rc = main(
        [
            "--db",
            config.db_path,
            "timezone",
            "change",
            "--site-id",
            str(site_id),
            "--timezone",
            _PROSPECTIVE_TZ,
        ]
    )
    out = capsys.readouterr().out

    assert rc == 1
    assert "--effective-from is required while the daily close is off" in out
    assert "Traceback" not in out
    assert _generations() == []


def test_do2_change_enters_a_zone_change_while_the_close_is_on(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.7 DO2: while the close is on, ``timezone change`` enters the
    zone change through ``enter_zone_change`` instead of
    ``apply_prospective_change``, ``--effective-from`` is forbidden, and an
    unknown zone is refused the same way. A fourth sub-case, on a second
    database whose helper site has ``with_records=False`` and was started
    at D+1 (as DS15c starts it), prints a first full day with no
    changeover date.

    ``wxverify.__main__`` imports ``utc_now`` (P1), so this monkeypatch
    fixes the clock the CLI reads. The ``site_days`` rows the change
    makes are checked by DS15 and DS15c in ``tests/test_site_days.py``.
    """
    import datetime as _dt

    from tests.helpers import NOW, D, day_close_site

    _init_tmp_db(tmp_path)
    monkeypatch.setattr("wxverify.__main__.utc_now", lambda: NOW)
    site_id = get_db().write_sync(
        lambda c: day_close_site(
            c, timezone=_START_TZ, close_on=True, with_records=True
        )
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
        lambda c: day_close_site(
            c, timezone=_START_TZ, close_on=True, with_records=False
        )
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


def test_do2b_change_on_an_unknown_site_while_the_close_is_on(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §13.7 DO2: an unknown site is refused the same way while the
    close is on. The existence check inside ``enter_zone_change``
    (`site_days.py:1227`, via ``_require_site_timezone``) runs before any
    date logic, so the CLI's ``DayStateError`` catch (`__main__.py:567`)
    reports it with a message instead of a traceback."""
    from tests.helpers import day_close_site

    _init_tmp_db(tmp_path)
    get_db().write_sync(
        lambda c: day_close_site(
            c, timezone=_START_TZ, close_on=True, with_records=False
        )
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
    """§13's other half: the CLI is the ONLY way in, because a normal site
    edit has no timezone field to rewrite history with."""
    assert "timezone" not in SiteUpdate.model_fields

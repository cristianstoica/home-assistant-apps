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


def test_site_update_still_cannot_carry_a_timezone() -> None:
    """§13's other half: the CLI is the ONLY way in, because a normal site
    edit has no timezone field to rewrite history with."""
    assert "timezone" not in SiteUpdate.model_fields

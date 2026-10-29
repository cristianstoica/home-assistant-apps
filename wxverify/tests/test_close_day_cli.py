"""The offline close CLI (plan 2026-10-21-daily-close-close-job.md section
15.12): CL1 to CL5.

Written before ``close-day`` exists as a subcommand and fails first for
that reason: ``main([...,"close-day",...])`` today raises ``SystemExit(2)``
from argparse's own "invalid choice" refusal, before any of the command's
own checks run -- exactly plan 8.8's "close-day not yet a command" failure.
Each test calls ``main()`` the way it will once piece 8 lands (expecting an
``int`` return, never wrapped in ``pytest.raises(SystemExit)``), so the
SAME test body goes green once the command exists, rather than needing a
rewrite.

Each source file is a version-12 file (today's narrow ``jobs`` CHECK, the
five ``close_*`` tables absent), built the way
``tests/test_close_schema.py``'s own ``_build_v12_file`` does -- migrating a
fresh on-disk file up and then undoing piece 1's additions -- copied locally
per this file's own-helpers rule, plus a site and two synthetic stations
(A, B). Unlike that helper, the file built here is never opened through
``init_db`` afterward: 15.12 says "the source file unchanged" means its
bytes and ``user_version``, read through a ``mode=ro`` connection, are
equal before and after the command runs.

Section 16.2 names the mutant each test ID kills; the mapping is given
inline on the exact assertion it kills at.

Synthetic data only: station ids ``ISTATION01``/``ISTATION02``, site name
``Testsite``, zone ``UTC``, the fixed synthetic ``tests.helpers.D``. No live
id, coordinate, site name or real zone name appears.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import D
from wxverify import config
from wxverify.__main__ import main
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation

_CLOSE_TABLES = (
    "close_attempts",
    "close_station_days",
    "close_publications",
    "close_stage_log",
    "close_try_log",
)

_V12_JOBS_DDL = """
    CREATE TABLE jobs_v12 (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
        job_key TEXT,
        payload TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK(status IN ('pending','running','completed','failed')),
        next_attempt_at TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 5,
        last_error TEXT,
        result TEXT,
        created_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        updated_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        CHECK (
            (type = 'catchup' AND site_id IS NULL)
            OR (
                type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                         'pair_and_score','backfill_site',
                         'forecast_record','record_gap_scan',
                         'verification_run','timezone_correction')
                AND site_id IS NOT NULL
            )
        )
    );
"""


def _build_v12_file(
    path: Path, *, site_name: str = "Testsite"
) -> tuple[int, dict[str, int]]:
    """Write a version-12 database to ``path`` with one site and stations A
    and B (``ISTATION01``/``ISTATION02``), returning ``(site_id,
    {pws_id: station_id})``. Copied locally from
    ``tests/test_close_schema.py``'s own ``_build_v12_file`` (15.3) per this
    file's own-helpers rule.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    for table in _CLOSE_TABLES:
        conn.execute(f"DROP TABLE {table}")
    conn.execute("SAVEPOINT narrow_jobs")
    try:
        conn.execute(_V12_JOBS_DDL)
        conn.execute("INSERT INTO jobs_v12 SELECT * FROM jobs")
        conn.execute("DROP TABLE jobs")
        conn.execute("ALTER TABLE jobs_v12 RENAME TO jobs")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe"
            " ON jobs(type, COALESCE(site_id, -1), job_key)"
            " WHERE status IN ('pending','running') AND job_key IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site"
            " ON jobs(type, job_key, site_id, id)"
        )
    except BaseException:
        conn.execute("ROLLBACK TO narrow_jobs")
        conn.execute("RELEASE narrow_jobs")
        raise
    conn.execute("RELEASE narrow_jobs")

    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (site_name,),
    ).lastrowid
    assert site_id is not None
    # Every real site has a published timezone generation -- a new site
    # gets it at creation (wxverify/api/routes/sites.py), and migrate_v4
    # seeds it for pre-existing sites. Seeding it here matches both paths;
    # it is never set by a migration on this file, because the site is
    # inserted after run_migrations already ran.
    ensure_published_generation(conn, int(site_id))
    stations: dict[str, int] = {}
    for pws_id in ("ISTATION01", "ISTATION02"):
        station_id = conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
            (site_id, pws_id),
        ).lastrowid
        assert station_id is not None
        stations[pws_id] = int(station_id)
    conn.execute("PRAGMA user_version = 12")
    conn.commit()
    conn.close()
    return int(site_id), stations


def _seed_observations_full_day(path: Path, site_id: int, local_date) -> None:  # type: ignore[no-untyped-def]
    """Seed one hourly ``observations`` row per quantity's input variable
    (``temperature``, ``wind``, ``precip``, ``verification/truth.py:68``)
    for every hour of ``local_date``, a full, gap-free day stage 4 can
    judge from stored hours alone (``--hours stored``, CL3)."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON")
    for hour in range(24):
        valid_at = isoformat_utc(
            datetime(
                local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
            )
        )
        for variable, value in (
            ("temperature", 10.0 + hour * 0.1),
            ("wind", 5.0),
            ("precip", 0.0),
        ):
            conn.execute(
                "INSERT INTO observations"
                " (site_id, variable, valid_at, value, n_stations, computed_at)"
                " VALUES (?, ?, ?, ?, 2, ?)",
                (site_id, variable, valid_at, value, valid_at),
            )
    conn.commit()
    conn.close()


def _seed_wind_only_raw_rows(path: Path, station_id: int, local_date) -> None:  # type: ignore[no-untyped-def]
    """Seed ``station_wind_records`` rows for every hour of ``local_date``
    carrying only ``speed_kmh`` (``temp_c``/``precip_total_mm`` NULL), the
    shape P2's migration copies forward from before P2's collection
    (CL5)."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON")
    for hour in range(24):
        obs_at = isoformat_utc(
            datetime(
                local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
            )
        )
        conn.execute(
            "INSERT INTO station_wind_records"
            " (station_id, obs_at, source, local_date, speed_kmh, fetched_at)"
            " VALUES (?, ?, 'hourly', ?, 5.0, ?)",
            (station_id, obs_at, local_date.isoformat(), obs_at),
        )
    conn.commit()
    conn.close()


def _seed_existing_site_day(path: Path, site_id: int, local_date) -> None:  # type: ignore[no-untyped-def]
    """Seed an existing ``site_days`` row for ``site_id``/``local_date``
    directly (CL4's "a site that already has a record"): P1's own table, a
    precondition ``open_day_on_copy`` itself refuses on, not something this
    test's own code under test builds."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON")
    start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    stamp = isoformat_utc(start)
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, created_at, state_changed_at)"
        " VALUES (?, ?, 'open', ?, ?, 'UTC', ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            stamp,
            isoformat_utc(start + timedelta(days=1)),
            stamp,
            stamp,
        ),
    )
    conn.commit()
    conn.close()


def _file_bytes_and_version(path: Path) -> tuple[bytes, int]:
    data = path.read_bytes()
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()
    return data, int(version)


# ---------------------------------------------------------------------------
# CL1
# ---------------------------------------------------------------------------


def test_cl1_refuses_the_add_ons_own_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CL1: ``PATH`` the add-on's own path (``config.db_path`` set to the
    test file). Exit 1; no copy made; the source file unchanged (8.8).

    Calls ``main()`` the way it will once ``close-day`` exists (an
    ``int`` return expected), so today's ``SystemExit(2)`` from argparse's
    own "invalid choice" refusal fails this test for the right reason
    (close-day not yet a command), without needing a rewrite once piece 8
    lands.

    M69 -> at the rc assertion below: correct = 1 (close-day refuses PATH
    when it resolves to ``config.db_path``, before opening it for
    writing), mutant M69 (close-day skips its database-path check) = 0,
    with the live database written to.
    """
    path = tmp_path / "wxverify.db"
    site_id, _stations = _build_v12_file(path)
    monkeypatch.setattr(config, "db_path", str(path))
    before_names = sorted(p.name for p in tmp_path.iterdir())
    before_bytes, before_version = _file_bytes_and_version(path)

    rc = main(
        [
            "--db",
            str(path),
            "close-day",
            "--site",
            str(site_id),
            "--date",
            D.isoformat(),
            "--hours",
            "stored",
        ]
    )

    assert rc == 1, (
        "M69 -> at this assertion: correct = 1 (close-day refuses when"
        " PATH resolves to the add-on's own database), mutant M69"
        " (close-day skips its database-path check) = 0, running against"
        " the live database"
    )
    after_bytes, after_version = _file_bytes_and_version(path)
    assert (after_bytes, after_version) == (before_bytes, before_version), (
        "the source file must be byte-for-byte and user_version unchanged"
    )
    after_names = sorted(p.name for p in tmp_path.iterdir())
    assert after_names == before_names, "no copy file may be made"


# ---------------------------------------------------------------------------
# CL2
# ---------------------------------------------------------------------------


def test_cl2_refuses_beside_a_recently_running_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CL2: a file whose ``worker_last_loop_at`` is 5 minutes old. Exit 1;
    no copy made; the source file unchanged.

    ``PATH`` is deliberately NOT ``config.db_path`` here, so only the
    heartbeat check (not CL1's path check) can be what refuses this file.

    M70 -> at the rc assertion below: correct = 1 (a heartbeat under 10
    minutes old refuses the file as beside a live worker), mutant M70
    (close-day skips its heartbeat check) = 0, with a copy made beside a
    database a real add-on process may still be writing.
    """
    path = tmp_path / "wxverify.db"
    site_id, _stations = _build_v12_file(path)
    monkeypatch.setattr(config, "db_path", str(tmp_path / "other.db"))

    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON")
    recent = isoformat_utc(datetime.now(UTC) - timedelta(minutes=5))
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_last_loop_at', ?)",
        (recent,),
    )
    conn.commit()
    conn.close()

    before_names = sorted(p.name for p in tmp_path.iterdir())
    before_bytes, before_version = _file_bytes_and_version(path)

    rc = main(
        [
            "--db",
            str(path),
            "close-day",
            "--site",
            str(site_id),
            "--date",
            D.isoformat(),
            "--hours",
            "stored",
        ]
    )

    assert rc == 1, (
        "M70 -> at this assertion: correct = 1 (a worker_last_loop_at"
        " under 10 minutes old refuses the file), mutant M70 (close-day"
        " skips its heartbeat check) = 0, writing beside a live worker"
    )
    after_bytes, after_version = _file_bytes_and_version(path)
    assert (after_bytes, after_version) == (before_bytes, before_version), (
        "the source file must be byte-for-byte and user_version unchanged"
    )
    after_names = sorted(p.name for p in tmp_path.iterdir())
    assert after_names == before_names, "no copy file may be made"


# ---------------------------------------------------------------------------
# CL3
# ---------------------------------------------------------------------------


def test_cl3_closes_a_past_day_offline_from_stored_hours(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CL3: a file with D's ``observations`` and no raw readings and no
    site-day record; ``--hours stored``. A new file made beside it, at
    ``TARGET_USER_VERSION``, holding ``OFFLINE_COPY_KEY``; D ``closed`` in
    the new file; the source file unchanged; its truth from the stored
    hours; the transport saw no request; no publication; the printout
    names the new file and each stage's duration (8.8).

    Plan ambiguity: 15.12 does not give the printout's exact wording, so
    this only checks the new file's name and the word "stage" appear in
    it, rather than pinning an exact format.

    M71/M72 -> at the transport/truth-value assertions below: correct = no
    gap-fill request and a temperature-high value read from the seeded
    hourly ``observations`` (stage 2/3 never ran, since there are no raw
    readings to call or rebuild from), mutant M71 (close-day runs stage 2)
    = a gap-fill request is made (the transport's handler raises),
    mutant M72 (``--hours stored`` runs stage 3) = ``write_hourly_values``
    finds no raw readings for the day and the truth value differs (or the
    attempt fails and D never closes).

    M73 -> at the source-file and new-file-exists assertions: correct =
    the source file byte-for-byte unchanged and exactly one new file
    beside it, mutant M73 (close-day runs on PATH itself instead of a copy
    it makes) = the source file modified and no new file created.
    """
    from tests.helpers import install_p4_standins, patch_weathercom_transport
    from wxverify.worker.close_p4 import ScoreSummary

    path = tmp_path / "wxverify.db"
    site_id, _stations = _build_v12_file(path)
    _seed_observations_full_day(path, site_id, D)
    monkeypatch.setattr(config, "db_path", str(tmp_path / "other.db"))

    def _handler(request: object) -> object:
        raise AssertionError("no gap-fill request may be made by the offline close")

    patch_weathercom_transport(monkeypatch, _handler)  # type: ignore[arg-type]
    standins = install_p4_standins(monkeypatch)
    standins["score_day"].result = ScoreSummary(scored={}, absent={})

    before_names = sorted(p.name for p in tmp_path.iterdir())
    before_bytes, before_version = _file_bytes_and_version(path)

    rc = main(
        [
            "--db",
            str(path),
            "close-day",
            "--site",
            str(site_id),
            "--date",
            D.isoformat(),
            "--hours",
            "stored",
        ]
    )

    assert rc == 0, "a successful offline close must exit 0"

    after_bytes, after_version = _file_bytes_and_version(path)
    assert (after_bytes, after_version) == (before_bytes, before_version), (
        "M73 -> at this assertion: correct = the source file byte-for-byte"
        " and user_version unchanged (close-day never writes PATH itself),"
        " mutant M73 (close-day runs on PATH itself) = the source file"
        " modified"
    )

    after_names = sorted(p.name for p in tmp_path.iterdir())
    new_names = [name for name in after_names if name not in before_names]
    assert len(new_names) == 1, (
        "M73 -> at this assertion: correct = exactly one new file beside"
        " the source, mutant M73 (close-day runs on PATH itself instead"
        " of a copy it makes) = no new file"
    )
    new_path = tmp_path / new_names[0]

    new_conn = sqlite3.connect(f"file:{new_path}?mode=ro", uri=True)
    new_conn.row_factory = sqlite3.Row
    try:
        from wxverify.db.migrations import TARGET_USER_VERSION

        new_version = new_conn.execute("PRAGMA user_version").fetchone()[0]
        assert new_version == TARGET_USER_VERSION, (
            "the new file must be migrated all the way to TARGET_USER_VERSION"
        )
        offline_key = new_conn.execute(
            "SELECT value FROM runtime_state WHERE key = 'offline_close_copy_made_at'"
        ).fetchone()
        assert offline_key is not None, "the new file must hold OFFLINE_COPY_KEY"

        day = new_conn.execute(
            "SELECT state FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
        assert day is not None and day["state"] == "closed", (
            "D must be closed in the new file"
        )

        high = new_conn.execute(
            "SELECT value FROM daily_truth WHERE site_id = ? AND local_date = ?"
            " AND quantity = 'temperature_high'",
            (site_id, D.isoformat()),
        ).fetchone()
        assert high is not None and high["value"] == pytest.approx(12.3), (
            "M71/M72 -> at this assertion: correct = the truth value read"
            " from the 24 seeded hourly observations (12.3, the day's max),"
            " mutant M72 (--hours stored runs stage 3) = a different value"
            " or no row at all, since there are no raw readings to rebuild"
            " hours from"
        )

        publications = new_conn.execute(
            "SELECT COUNT(*) FROM close_publications WHERE site_id = ? AND cutoff = ?",
            (site_id, D.isoformat()),
        ).fetchone()
        assert publications[0] == 0, "the offline close must open no publication"
    finally:
        new_conn.close()

    assert standins["publish"].calls == [], (
        "the offline close never runs stage 7 -- close_p4.publish must not be called"
    )

    out = capsys.readouterr().out
    assert new_names[0] in out, "the printout must name the new file"
    assert out.count("stage") + out.count("Stage") >= 4, (
        "the printout must mention each of stages 3-6's duration"
    )


# ---------------------------------------------------------------------------
# CL4
# ---------------------------------------------------------------------------


def test_cl4_refuses_raw_rebuild_with_no_temperature_and_an_existing_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CL4: the same with ``--hours raw``; then a source file whose site
    already has a record. Exit 1, "no raw reading of that day has a
    temperature"; then exit 1 with P1's ``DayStateError`` message; the
    source file unchanged both times (8.8).

    First case: CL3's own file (D's ``observations``, no raw readings at
    all), run with ``--hours raw`` instead -- there being literally no raw
    reading of any kind, none can have a temperature. Second case: a fresh
    file whose site already has a ``site_days`` record for D, seeded
    directly (P1's own table, the precondition ``open_day_on_copy`` itself
    refuses on).
    """
    from tests.helpers import patch_weathercom_transport

    def _handler(request: object) -> object:
        raise AssertionError("no gap-fill request may be made by the offline close")

    # Case (i): no raw readings of any kind.
    path_i = tmp_path / "no_raw.db"
    site_i, _stations_i = _build_v12_file(path_i, site_name="TestsiteNoRaw")
    _seed_observations_full_day(path_i, site_i, D)
    monkeypatch.setattr(config, "db_path", str(tmp_path / "other.db"))
    patch_weathercom_transport(monkeypatch, _handler)  # type: ignore[arg-type]
    before_bytes_i, before_version_i = _file_bytes_and_version(path_i)

    rc_i = main(
        [
            "--db",
            str(path_i),
            "close-day",
            "--site",
            str(site_i),
            "--date",
            D.isoformat(),
            "--hours",
            "raw",
        ]
    )
    assert rc_i == 1, "no raw reading of the day has a temperature -- must refuse"
    after_bytes_i, after_version_i = _file_bytes_and_version(path_i)
    assert (after_bytes_i, after_version_i) == (before_bytes_i, before_version_i)

    # Case (ii): the site already has a site_days record.
    path_ii = tmp_path / "already_has_record.db"
    site_ii, _stations_ii = _build_v12_file(path_ii, site_name="TestsiteAlready")
    _seed_existing_site_day(path_ii, site_ii, D - timedelta(days=5))
    before_bytes_ii, before_version_ii = _file_bytes_and_version(path_ii)

    rc_ii = main(
        [
            "--db",
            str(path_ii),
            "close-day",
            "--site",
            str(site_ii),
            "--date",
            D.isoformat(),
            "--hours",
            "raw",
        ]
    )
    assert rc_ii == 1, (
        "a site that already has a day record must refuse (DayStateError)"
    )
    after_bytes_ii, after_version_ii = _file_bytes_and_version(path_ii)
    assert (after_bytes_ii, after_version_ii) == (before_bytes_ii, before_version_ii)


# ---------------------------------------------------------------------------
# CL5
# ---------------------------------------------------------------------------


def test_cl5_refuses_a_raw_rebuild_from_copied_wind_history_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CL5: a file with D's ``observations`` (temperature, rain and wind)
    and, as D's only raw readings, wind rows of stations A and B with
    ``temp_c`` and ``precip_total_mm`` NULL, as P2's migration copies them;
    ``--hours raw``. Exit 1, "no raw reading of that day has a
    temperature"; in the new file D's ``observations`` rows equal the
    source's, row for row, and D is not ``closed``; the source file
    unchanged (8.8).

    M84 -> at the observations-equality assertion below: correct = the new
    file's ``observations`` rows for D equal the source's, row for row,
    and D is not closed (the refusal happens before stage 3 writes anything,
    8.8), mutant M84 (``close-day --hours raw`` refuses only when the day
    has NO raw reading at all, of any quantity) = the rebuild goes on
    using the wind-only rows, overwriting D's temperature and rain
    observations with a day that has none.
    """
    from tests.helpers import patch_weathercom_transport

    def _handler(request: object) -> object:
        raise AssertionError("no gap-fill request may be made by the offline close")

    path = tmp_path / "wxverify.db"
    site_id, stations = _build_v12_file(path)
    _seed_observations_full_day(path, site_id, D)
    _seed_wind_only_raw_rows(path, stations["ISTATION01"], D)
    _seed_wind_only_raw_rows(path, stations["ISTATION02"], D)
    monkeypatch.setattr(config, "db_path", str(tmp_path / "other.db"))
    patch_weathercom_transport(monkeypatch, _handler)  # type: ignore[arg-type]

    source_conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    source_conn.row_factory = sqlite3.Row
    try:
        source_observations = [
            dict(row)
            for row in source_conn.execute(
                "SELECT variable, valid_at, value FROM observations"
                " WHERE site_id = ? ORDER BY variable, valid_at",
                (site_id,),
            )
        ]
    finally:
        source_conn.close()
    assert len(source_observations) == 72, "premise: 24 hours * 3 variables seeded"

    before_names = sorted(p.name for p in tmp_path.iterdir())
    before_bytes, before_version = _file_bytes_and_version(path)

    rc = main(
        [
            "--db",
            str(path),
            "close-day",
            "--site",
            str(site_id),
            "--date",
            D.isoformat(),
            "--hours",
            "raw",
        ]
    )

    assert rc == 1, "no raw reading of the day has a temperature -- must refuse"
    after_bytes, after_version = _file_bytes_and_version(path)
    assert (after_bytes, after_version) == (before_bytes, before_version), (
        "the source file must be byte-for-byte and user_version unchanged"
    )

    after_names = sorted(p.name for p in tmp_path.iterdir())
    new_names = [name for name in after_names if name not in before_names]
    assert len(new_names) == 1, "a copy must still be made before the refusal (8.8)"
    new_path = tmp_path / new_names[0]

    new_conn = sqlite3.connect(f"file:{new_path}?mode=ro", uri=True)
    new_conn.row_factory = sqlite3.Row
    try:
        new_observations = [
            dict(row)
            for row in new_conn.execute(
                "SELECT variable, valid_at, value FROM observations"
                " WHERE site_id = ? ORDER BY variable, valid_at",
                (site_id,),
            )
        ]
        assert new_observations == source_observations, (
            "M84 -> at this assertion: correct = the new file's"
            " observations rows for D equal the source's row for row"
            " (the refusal happens before stage 3 writes anything), mutant"
            " M84 (close-day --hours raw refuses only when the day has NO"
            " raw reading at all) = the rebuild runs from the wind-only"
            " rows and D's temperature/rain observations are overwritten"
            " or removed"
        )
        day = new_conn.execute(
            "SELECT state FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
        assert day is not None and day["state"] != "closed", "D must not be closed"
    finally:
        new_conn.close()

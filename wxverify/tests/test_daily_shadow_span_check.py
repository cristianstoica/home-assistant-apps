"""Span rows stop rain (§13.3, ``2026-10-09-forecast-time-alignment.md`` §10.4.4).

Covers the rain run's span check -- ``main()``'s first precip step, before
any other read of the copy: a copy that holds any ``station_precip_spans``
row stops the run in ERROR before :func:`daily_shadow.load_station_rain` is
ever called. Every case runs both ``--counts-only --variable precip`` and
``--freeze-prepare --variable precip`` on one rain fixture on which both
runs complete, with ``--house-station`` on the ``--freeze-prepare`` run
only (the parser rejects it in any other mode).

Every rain record row carries the literal ``app_version`` ``"0.16.7"``
(mapped by :data:`daily_shadow.RAIN_RULES`), written by hand with
``daily_quantities.displayed`` computed via a direct call to
:func:`wxverify.forecast.aggregate.displayed_daily`, following
``tests/test_daily_shadow_rain_rules.py``'s own fixture pattern.

The ``station_precip_spans`` table does not exist yet in
``db/migrations.py``'s ``create_tables`` (checked against this worktree's
HEAD). A case that needs the table present creates it by hand with
``CREATE TABLE IF NOT EXISTS`` using #115 §10.4.4's own DDL; a case that
needs it absent drops it with ``DROP TABLE IF EXISTS`` after building the
fixture, so these tests hold whether or not the production migration
lands first.
"""

from __future__ import annotations

import contextlib
import io
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.aggregate import displayed_daily
from wxverify.verification import daily_shadow
from wxverify.verification.record import resolve_snapshot_utc

COMMIT_SHA = "a" * 40
_DAY = date(2035, 6, 10)

# #115 §10.4.4's own DDL (never the production migration's copy, which
# does not exist yet in this worktree). The columns, types and
# constraints are copied exactly; the plan's ``--`` comments are not.
_SPANS_DDL = """
CREATE TABLE IF NOT EXISTS station_precip_spans (
    id INTEGER PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    valid_at TEXT NOT NULL,
    span_start TEXT NOT NULL,
    span_end TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('gap','day_start','decrease')),
    amount_mm REAL CHECK (amount_mm IS NULL OR amount_mm >= 0),
    previous_total_mm REAL,
    total_mm REAL NOT NULL CHECK (total_mm >= 0),
    source_raw TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    UNIQUE (station_id, valid_at)
);
"""


# --- fixture: one matched rain record row + the samples it replays -----------


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(
    tmp_path: Path, *, name: str = "span-check-site"
) -> tuple[Path, daily_shadow.Site]:
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, name)
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=site_id)
    finally:
        conn.close()
    return copy, site


def _insert_precip_row(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    snapshot_date: date,
    app_version: str,
    daily_quantities: str,
    policy: str = '{"rain_threshold_mm": 0.2}',
) -> None:
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", snapshot_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                'on_time', ?, 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            snapshot_utc,
            snapshot_date.isoformat(),
            policy,
            app_version,
            daily_quantities,
        ),
    )


def _insert_precip_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    values: list[float],
) -> None:
    assert len(values) == 24
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    for hour, value in enumerate(values):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


def _precip_daily_quantities(*, total_mm: float, wet_hours: float, feed_id: int) -> str:
    return json.dumps(
        {
            "displayed": {
                "total_mm": total_mm,
                "wet_hours": wet_hours,
                "extrema_feed_ids": [feed_id],
            }
        }
    )


def _wet_dry_values(*, wet_count: int) -> list[float]:
    return [0.5] * wet_count + [0.0] * (24 - wet_count)


def _build_matched_rain_fixture(tmp_path: Path) -> tuple[Path, daily_shadow.Site]:
    """One mapped (0.16.7) rain record row, matched to its own samples.

    Mirrors ``test_daily_shadow_rain_rules.py``'s ``_build_matched_run``: the
    sample values feed ``displayed_daily`` directly, so the record row's
    ``daily_quantities`` is internally consistent (F1 passes by
    construction) and both ``--counts-only`` and ``--freeze-prepare`` runs
    on it complete without stopping anywhere but the span check.
    """
    copy, site = _make_copy(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, "model-a")
        day_before = _DAY - timedelta(days=1)
        issued_at = f"{day_before.isoformat()}T06:00:00Z"
        fetched_at = f"{day_before.isoformat()}T06:05:00Z"
        values = _wet_dry_values(wet_count=6)
        _insert_precip_day_samples(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=_DAY,
            issued_at=issued_at,
            fetched_at=fetched_at,
            values=values,
        )
        displayed = displayed_daily("precip", [values], rain_threshold_mm=0.2)
        assert displayed["total_mm"] is not None
        assert displayed["wet_hours"] is not None
        _insert_precip_row(
            conn,
            site=site,
            snapshot_date=_DAY,
            app_version="0.16.7",
            daily_quantities=_precip_daily_quantities(
                total_mm=displayed["total_mm"],
                wet_hours=displayed["wet_hours"],
                feed_id=feed_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return copy, site


def _set_deterministic_now(copy: Path, *, mtime: datetime) -> None:
    stamp = mtime.timestamp()
    import os

    os.utime(copy, (stamp, stamp))


def _result_payload(out_text: str) -> dict[str, object]:
    lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(lines) == 1
    return json.loads(lines[0][len(daily_shadow.RESULT_PREFIX) :])


# --- recorders: wrap open_copy_readonly and load_station_rain by name --------


class _Recorders:
    """Wraps ``open_copy_readonly`` and ``load_station_rain`` once; call
    :meth:`reset` before each run to count that run's calls alone.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.open_calls: list[Path] = []
        self.load_calls: list[dict[str, object]] = []
        real_open = daily_shadow.open_copy_readonly
        real_load = daily_shadow.load_station_rain

        def traced_open(copy: Path) -> sqlite3.Connection:
            self.open_calls.append(copy)
            return real_open(copy)

        def traced_load(
            conn: sqlite3.Connection,
            *,
            site_id: int,
            first_day: date | None,
            last_day: date | None,
            known_by: datetime | None,
        ) -> dict[str, dict[date, list[object]]]:
            self.load_calls.append(
                {
                    "site_id": site_id,
                    "first_day": first_day,
                    "last_day": last_day,
                    "known_by": known_by,
                }
            )
            return cast(
                "dict[str, dict[date, list[object]]]",
                real_load(
                    conn,
                    site_id=site_id,
                    first_day=first_day,
                    last_day=last_day,
                    known_by=known_by,
                ),
            )

        monkeypatch.setattr(daily_shadow, "open_copy_readonly", traced_open)
        monkeypatch.setattr(daily_shadow, "load_station_rain", traced_load)

    def reset(self) -> None:
        self.open_calls.clear()
        self.load_calls.clear()


def _run_mode(
    *,
    copy: Path,
    out: Path,
    monkeypatch: pytest.MonkeyPatch,
    site_id: int,
    mode: str,
    house_station: str | None,
) -> tuple[int, str, str]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        f"--{mode}",
    ]
    if house_station is not None:
        argv.extend(["--house-station", house_station])
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _run_both_modes(
    *,
    copy: Path,
    out_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    site_id: int,
    recorders: _Recorders,
) -> dict[str, tuple[int, str, str, list[Path], list[dict[str, object]]]]:
    """Run ``--counts-only`` then ``--freeze-prepare`` on the same copy.

    ``--house-station`` only on the ``--freeze-prepare`` run (reviewer note
    1: the parser rejects it in any other mode). Returns, per mode, the
    exit code, stdout, stderr and THAT run's own open/load recorder calls
    (the lists are cleared before each run).
    """
    results: dict[str, tuple[int, str, str, list[Path], list[dict[str, object]]]] = {}
    for mode, house_station in (
        ("counts-only", None),
        ("freeze-prepare", "SYN-HOUSE"),
    ):
        recorders.reset()
        out = out_root / mode
        exit_code, out_text, err_text = _run_mode(
            copy=copy,
            out=out,
            monkeypatch=monkeypatch,
            site_id=site_id,
            mode=mode,
            house_station=house_station,
        )
        results[mode] = (
            exit_code,
            out_text,
            err_text,
            list(recorders.open_calls),
            list(recorders.load_calls),
        )
    return results


def _insert_station(
    conn: sqlite3.Connection, *, site_id: int, pws_station_id: str
) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations
            (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (?, ?, 10.0, 20.0, 100.0, 1)
        """,
        (site_id, pws_station_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_span_row(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    valid_at: str,
    fetched_at: str,
) -> None:
    conn.execute(_SPANS_DDL)
    span_end = isoformat_utc(
        datetime.fromisoformat(valid_at.replace("Z", "+00:00")) + timedelta(hours=1)
    )
    span_start = isoformat_utc(
        datetime.fromisoformat(span_end.replace("Z", "+00:00")) - timedelta(hours=6)
    )
    conn.execute(
        """
        INSERT INTO station_precip_spans
            (station_id, valid_at, span_start, span_end, kind, amount_mm,
             previous_total_mm, total_mm, source_raw, fetched_at)
        VALUES (?, ?, ?, ?, 'gap', 1.0, 0.5, 1.5, '1.5 mm precipTotal', ?)
        """,
        (station_id, valid_at, span_start, span_end, fetched_at),
    )


def _assert_error_only_run_dir(payload: dict[str, object], out: Path) -> Path:
    assert payload["exit_code"] == 1
    run_dir = out / cast(str, payload["run_dir"])
    assert {entry.name for entry in run_dir.iterdir()} == {"error.json"}
    return run_dir


def _load_error(run_dir: Path) -> dict[str, object]:
    error = cast("dict[str, object]", json.loads((run_dir / "error.json").read_text()))
    assert set(error) == {
        "stage",
        "error",
        "sqlite_errorcode",
        "copy_sha256",
        "manifest",
    }
    return error


# --- (a) no table --------------------------------------------------------------


def test_case_a_no_table_both_runs_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No ``station_precip_spans`` table: both modes complete, no error.json.

    Drops the table after building the fixture (plan :1324), so this test
    holds whether or not the table already exists in ``create_tables`` by
    the time it runs. Paired with case (c)'s stop: it proves a span row,
    not some other property of (c)'s fixture, is what makes (c) stop.
    """
    copy, site = _build_matched_rain_fixture(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        conn.execute("DROP TABLE IF EXISTS station_precip_spans")
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    recorders = _Recorders(monkeypatch)

    results = _run_both_modes(
        copy=copy,
        out_root=tmp_path / "out",
        monkeypatch=monkeypatch,
        site_id=site.site_id,
        recorders=recorders,
    )

    for mode, (exit_code, out_text, err_text, _opens, _loads) in results.items():
        assert exit_code == 0, (mode, err_text)
        payload = _result_payload(out_text)
        assert payload["exit_code"] == 0
        run_dir = (tmp_path / "out" / mode) / cast(str, payload["run_dir"])
        assert not (run_dir / "error.json").exists()

    _, _, _, _opens, freeze_loads = results["freeze-prepare"]
    assert len(freeze_loads) >= 1


# --- (b) empty table ------------------------------------------------------------


def test_case_b_empty_table_both_runs_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The table exists and holds no row: both modes complete, as in (a).

    Kills M144 (ERROR whenever the table exists, whatever its row count):
    the correct code's first query counts 1 (the table's own
    ``sqlite_master`` row), then the second query counts 0 rows and the
    check passes, so this run's ``exit_code`` is 0 and it writes no
    ``error.json``; M144 ends in ERROR as soon as the table exists at all,
    so it would give ``exit_code`` 1 with an ``error.json`` here instead --
    the two diverge at the ``exit_code`` and ``error.json``-presence
    assertions below.
    """
    copy, site = _build_matched_rain_fixture(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        conn.execute(_SPANS_DDL)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    recorders = _Recorders(monkeypatch)

    results = _run_both_modes(
        copy=copy,
        out_root=tmp_path / "out",
        monkeypatch=monkeypatch,
        site_id=site.site_id,
        recorders=recorders,
    )

    for mode, (exit_code, out_text, err_text, _opens, _loads) in results.items():
        assert exit_code == 0, (mode, err_text)
        payload = _result_payload(out_text)
        assert payload["exit_code"] == 0
        run_dir = (tmp_path / "out" / mode) / cast(str, payload["run_dir"])
        assert not (run_dir / "error.json").exists()


# --- (c) one row on a station of the run's site --------------------------------


def test_case_c_one_row_on_run_site_station_stops_both_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One span row on the run's own site: both modes ERROR at span_check.

    Kills M141 (the span check skipped): the correct run raises
    ``SpanRowsPresentError`` and never reaches ``load_station_rain``, so
    ``exit_code`` is 1, the run directory holds only ``error.json`` and
    ``load_calls`` is empty; M141 skips the check entirely, so the run
    proceeds to ``load_station_rain`` (``load_calls`` non-empty) and
    completes with ``exit_code`` 0 -- the two diverge at the ``exit_code``
    and ``load_calls`` assertions below.

    Kills M142 (counts only rows fetched by the run's latest decision
    time): this row's ``fetched_at`` ('2099-01-01T00:00:00.000Z') is after
    every decision time of the run, so the correct code's unfiltered count
    still finds it and stops; M142's as-of filter excludes it (not yet
    known as of any decision time), so the run would pass the check and
    complete with ``exit_code`` 0 instead -- same divergence.
    """
    copy, site = _build_matched_rain_fixture(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        station_id = _insert_station(
            conn, site_id=site.site_id, pws_station_id="SYN-SPAN-1"
        )
        _insert_span_row(
            conn,
            station_id=station_id,
            valid_at="2035-06-10T12:00:00.000Z",
            fetched_at="2099-01-01T00:00:00.000Z",
        )
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    before_sha = daily_shadow.sha256_file(copy)
    recorders = _Recorders(monkeypatch)

    results = _run_both_modes(
        copy=copy,
        out_root=tmp_path / "out",
        monkeypatch=monkeypatch,
        site_id=site.site_id,
        recorders=recorders,
    )

    for mode, (exit_code, out_text, err_text, opens, loads) in results.items():
        assert exit_code == 1, (mode, err_text)
        payload = _result_payload(out_text)
        run_dir = _assert_error_only_run_dir(payload, tmp_path / "out" / mode)
        error = _load_error(run_dir)
        assert error["stage"] == "span_check", mode
        assert error["error"] == (
            "SpanRowsPresentError: the copy holds 1 station_precip_spans "
            "rows; rain reads no copy that holds span rows"
        ), mode
        assert error["sqlite_errorcode"] is None, mode
        assert set(payload) == {"exit_code", "run_dir"}, mode
        assert len(opens) == 1, (mode, opens)
        assert loads == [], (mode, loads)

    after_sha = daily_shadow.sha256_file(copy)
    assert after_sha == before_sha


# --- (d) the same row on a second site's station only ---------------------------


def test_case_d_row_on_second_site_station_stops_both_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A span row on a DIFFERENT site's station, fetched before every
    decision time: still ERROR, exactly as in (c).

    Kills M143 (the span check counts only rows on the run's site's
    stations): the plan's rule is "about the copy, not one site's
    stations" -- the correct code's query has no site/station filter at
    all, so it still finds this row belonging to a station on an unrelated
    site and stops; M143 filters by the run's own site's stations, which
    this row is NOT on, so M143 would find 0 rows, pass the check and let
    the run complete with ``exit_code`` 0 instead of 1 -- the two diverge
    at the ``exit_code`` assertion below. The early ``fetched_at``
    ('2000-01-01T00:00:00.000Z', before every decision time) additionally
    distinguishes this case from M142's as-of-filter trap (that one is
    pinned by case (c) instead, whose row is fetched in the future).
    """
    copy, site = _build_matched_rain_fixture(tmp_path)
    other_site_conn = sqlite3.connect(copy)
    try:
        other_site_conn.row_factory = sqlite3.Row
        other_site_id = asof_make_site(other_site_conn, "span-check-other-site")
        station_id = _insert_station(
            other_site_conn, site_id=other_site_id, pws_station_id="SYN-SPAN-2"
        )
        _insert_span_row(
            other_site_conn,
            station_id=station_id,
            valid_at="2035-06-10T12:00:00.000Z",
            fetched_at="2000-01-01T00:00:00.000Z",
        )
        other_site_conn.commit()
    finally:
        other_site_conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    before_sha = daily_shadow.sha256_file(copy)
    recorders = _Recorders(monkeypatch)

    results = _run_both_modes(
        copy=copy,
        out_root=tmp_path / "out",
        monkeypatch=monkeypatch,
        site_id=site.site_id,
        recorders=recorders,
    )

    for mode, (exit_code, out_text, err_text, opens, loads) in results.items():
        assert exit_code == 1, (mode, err_text)
        payload = _result_payload(out_text)
        run_dir = _assert_error_only_run_dir(payload, tmp_path / "out" / mode)
        error = _load_error(run_dir)
        assert error["stage"] == "span_check", mode
        assert error["error"] == (
            "SpanRowsPresentError: the copy holds 1 station_precip_spans "
            "rows; rain reads no copy that holds span rows"
        ), mode
        assert error["sqlite_errorcode"] is None, mode
        assert set(payload) == {"exit_code", "run_dir"}, mode
        assert len(opens) == 1, (mode, opens)
        assert loads == [], (mode, loads)

    after_sha = daily_shadow.sha256_file(copy)
    assert after_sha == before_sha


# --- (e) no table, but an index of the same name --------------------------------


def test_case_e_index_named_station_precip_spans_errors_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No table, but an index named ``station_precip_spans`` on
    ``stations(id)``: the first query counts 1, the second raises
    ``sqlite3.OperationalError`` ("no such table").

    Both runs ERROR at stage ``span_check`` with ``sqlite_errorcode`` 1
    (``SQLITE_ERROR``). This is not one of M141-M144's named cases, but it
    pins case (e) of the plan's "Span rows stop rain" test spec (:1329):
    the check does not special-case "a schema object of this name that
    isn't a table" -- it trusts the first count and lets the second
    query's own failure surface as an ordinary ``sqlite3.Error``, never
    silently swallowed or reclassified.

    Drops the table before creating the index (plan :1324), so this test
    holds whether or not the table already exists by the time it runs.
    """
    copy, site = _build_matched_rain_fixture(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        conn.execute("DROP TABLE IF EXISTS station_precip_spans")
        conn.execute("CREATE INDEX station_precip_spans ON stations(id)")
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    before_sha = daily_shadow.sha256_file(copy)
    recorders = _Recorders(monkeypatch)

    results = _run_both_modes(
        copy=copy,
        out_root=tmp_path / "out",
        monkeypatch=monkeypatch,
        site_id=site.site_id,
        recorders=recorders,
    )

    for mode, (exit_code, out_text, err_text, opens, loads) in results.items():
        assert exit_code == 1, (mode, err_text)
        payload = _result_payload(out_text)
        run_dir = _assert_error_only_run_dir(payload, tmp_path / "out" / mode)
        error = _load_error(run_dir)
        assert error["stage"] == "span_check", mode
        error_text = cast(str, error["error"])
        assert error_text.startswith("OperationalError: "), mode
        assert "no such table" in error_text, mode
        assert error["sqlite_errorcode"] == 1, mode
        assert len(opens) == 1, (mode, opens)
        assert loads == [], (mode, loads)

    after_sha = daily_shadow.sha256_file(copy)
    assert after_sha == before_sha


# --- reviewer note 1: --house-station is rejected outside --freeze-prepare -----


def test_house_station_rejected_with_counts_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--house-station`` with ``--counts-only`` is an argparse error (exit 2).

    Paired with the (a)/(b)/(c)/(d)/(e) ``--counts-only`` runs above, which
    never pass ``--house-station`` -- this is what proves that omission is
    deliberate and load-bearing, not an accident that happens to work: the
    parser actively refuses the flag in this mode rather than silently
    accepting and ignoring it.
    """
    copy, site = _build_matched_rain_fixture(tmp_path)
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    out = tmp_path / "out"
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site.site_id),
        "--out",
        str(out),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--counts-only",
        "--house-station",
        "SYN-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with (
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
        pytest.raises(SystemExit) as excinfo,
    ):
        daily_shadow.main(argv)
    assert excinfo.value.code == 2
    assert "usage:" in stderr.getvalue()

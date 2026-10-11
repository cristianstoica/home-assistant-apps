"""Plan §15.3 "Incumbent from the record" and its across-a-gap fixtures.

Integration coverage for ``daily_shadow``'s ``--incumbent-from-record``
flag: the incumbent-memory chain reads each earlier day's own persisted
``forecast_of_record.daily_quantities`` row (``daily_method.record_decision``,
``daily_method.remembered_incumbent``) instead of the live S0 chain. Named
mutants (not run by this file): M80, M91, M92, M93b, M94, M95, M96, M99.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "d" * 40
_APP_VERSION = "0.16.7"
_FEED_A = "syn-a"
_FEED_B = "syn-b"

# Synthetic base date. Day n (n >= 1) is _BASE + (n - 1) days, so day 1 is
# S0 (the first snapshot date with a record row) in every fixture below,
# except the two 60-date-boundary fixtures, which start at day 30, so S0
# is day 30 there.
_BASE = date(2034, 1, 1)

# Each fixture's one discriminating day: the station is wet, SYN-a forecasts
# dry (a miss), SYN-b forecasts wet. One miss at RAIN_MISS_WEIGHT=2.0 is a
# 2-point penalty gap, inside RAIN_SWITCH_MARGIN=3, so an incumbent SYN-a is
# never replaced on score alone while that miss stays inside the 60-day
# scoring window (RAIN_WINDOW_DAYS). _DISC is within range of every checked
# date at or before 41; _DISC_FAR is within range of checked dates 90-91,
# where _DISC has rolled out of the window.
_DISC = 22
_DISC_FAR = 32

_NOT_READY: dict[str, object] = {
    "method": "selector",
    "fallback_reason": "not_ready",
}
_DAY30_SELECTS_A: dict[str, object] = {
    "method": "selector",
    "selected_feed": "example-src/syn-a",
}
_DAY30_SELECTS_B: dict[str, object] = {
    "method": "selector",
    "selected_feed": "example-src/syn-b",
}


def _d(n: int) -> date:
    return _BASE + timedelta(days=n - 1)


def _at_noon(n: int) -> datetime:
    day = _d(n)
    return datetime(day.year, day.month, day.day, 12, tzinfo=UTC)


def _rain_hours(*, wet_count: int) -> list[float]:
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


def _displayed_json(extra: Mapping[str, object] | None) -> str:
    """One day's ``daily_quantities`` JSON, with ``displayed`` extended.

    The base ``displayed`` object always carries the daily-quantity keys a
    real record row has; ``extra`` merges in the method-memory keys
    (``method``, ``fallback_reason``, ``selected_feed``) a given case
    needs, or is omitted entirely for a plain row.
    """
    displayed: dict[str, object] = {
        "total_mm": 0.0,
        "wet_hours": 0.0,
        "extrema_feed_ids": [],
    }
    if extra is not None:
        displayed.update(extra)
    return json.dumps({"displayed": displayed})


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "incumbent-from-record-site")
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
    site: Site,
    local_date: date,
    displayed_extra: Mapping[str, object] | None,
) -> None:
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", local_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                'on_time', '{"rain_threshold_mm": 0.2}', 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            local_date.isoformat(),
            snapshot_utc,
            local_date.isoformat(),
            _APP_VERSION,
            _displayed_json(displayed_extra),
        ),
    )


def _insert_forecast_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    wet_count: int,
) -> None:
    values = _rain_hours(wet_count=wet_count)
    day_before = local_date - timedelta(days=1)
    issued_at = isoformat_utc(
        datetime(day_before.year, day_before.month, day_before.day, 6, tzinfo=UTC)
    )
    fetched_at = isoformat_utc(
        datetime(day_before.year, day_before.month, day_before.day, 6, 5, tzinfo=UTC)
    )
    issued = datetime.fromisoformat(issued_at)
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
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-record', ?)
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


def _make_station(
    conn: sqlite3.Connection, *, site_id: int, pws_station_id: str
) -> int:
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)"
        " VALUES (?, ?, 0.0, 0.0, 900.0, 1, '2026-01-01T00:00:00.000Z')",
        (site_id, pws_station_id),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _insert_station_day(
    conn: sqlite3.Connection, *, station_id: int, local_date: date, wet_count: int
) -> None:
    values = _rain_hours(wet_count=wet_count)
    cumulative = 0.0
    for hour, value in enumerate(values):
        cumulative += value
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        fetched = valid + timedelta(minutes=5)
        source_raw = f"{round(cumulative, 10)} mm precipTotal"
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, source_raw,
                 fetched_at)
            VALUES (?, 'precip', ?, ?, 'ok', ?, ?)
            """,
            (
                station_id,
                isoformat_utc(valid),
                value,
                source_raw,
                isoformat_utc(fetched),
            ),
        )


def _insert_case_day(
    conn: sqlite3.Connection,
    *,
    site: Site,
    station_id: int,
    local_date: date,
    truth_wet_count: int,
    model_wet_counts: dict[int, int],
    displayed_extra: Mapping[str, object] | None,
) -> None:
    """One calendar day: station truth, each listed feed's forecast, and
    the day's own forecast_of_record row."""
    _insert_station_day(
        conn, station_id=station_id, local_date=local_date, wet_count=truth_wet_count
    )
    for feed_id, wet_count in model_wet_counts.items():
        _insert_forecast_day(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=local_date,
            wet_count=wet_count,
        )
    _insert_precip_row(
        conn, site=site, local_date=local_date, displayed_extra=displayed_extra
    )


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _set_deterministic_now(copy: Path, *, mtime: datetime) -> date:
    """Fix the copy's mtime so main()'s ``copied_at`` is deterministic.

    Returns the ``last_complete`` date main() will derive from it.
    """
    stamp = mtime.timestamp()
    os.utime(copy, (stamp, stamp))
    return latest_knowable_target("UTC", mtime)


def _run_main(
    *,
    copy: Path,
    out: Path,
    monkeypatch: pytest.MonkeyPatch,
    site_id: int,
    incumbent_from_record: bool,
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
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    if incumbent_from_record:
        argv.append("--incumbent-from-record")
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _result_payload(out_text: str) -> dict[str, object]:
    lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(lines) == 1
    payload = json.loads(lines[0][len(daily_shadow.RESULT_PREFIX) :])
    return cast(dict[str, object], payload)


def _read_lines_jsonl(run_dir: Path) -> list[dict[str, object]]:
    path = run_dir / "lines.jsonl"
    out: list[dict[str, object]] = []
    for line in path.read_text().splitlines():
        if line.strip():
            out.append(cast(dict[str, object], json.loads(line)))
    return out


def _line_for(
    lines: list[dict[str, object]], *, snapshot_date: date, lead: int
) -> dict[str, object]:
    for entry in lines:
        if (
            entry["snapshot_date"] == snapshot_date.isoformat()
            and entry["lead"] == lead
        ):
            return entry
    raise AssertionError(f"no line for {snapshot_date} lead {lead}")


def _build_fixture(
    tmp_path: Path,
    *,
    name: str,
    days: range,
    disc_day: int,
    overrides: Mapping[int, Mapping[str, object] | None],
) -> tuple[Path, int]:
    """One station, two feeds, dry every day but ``disc_day``.

    A day mapped to ``None`` in ``overrides`` has no row at all (and no
    forecast or station data). Every other day in ``days`` gets a row;
    ``overrides.get(day)`` merges into its ``displayed`` object, or the
    day gets a plain row when it has no entry.
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_a = asof_make_real_feed(conn, _FEED_A)
        feed_b = asof_make_real_feed(conn, _FEED_B)
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id="SYN-PC-RECORD"
        )
        for n in days:
            if n in overrides and overrides[n] is None:
                continue
            truth = 4 if n == disc_day else 0
            b_wet = 4 if n == disc_day else 0
            _insert_case_day(
                conn,
                site=site,
                station_id=station_id,
                local_date=_d(n),
                truth_wet_count=truth,
                model_wet_counts={feed_a: 0, feed_b: b_wet},
                displayed_extra=overrides.get(n),
            )
        conn.commit()
    finally:
        conn.close()
    return copy, site.site_id


def _run_record_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    name: str,
    days: range,
    disc_day: int,
    overrides: Mapping[int, Mapping[str, object] | None],
    checked_day: int,
) -> list[dict[str, object]]:
    copy, site_id = _build_fixture(
        tmp_path, name=name, days=days, disc_day=disc_day, overrides=overrides
    )
    out = tmp_path / f"{name}-out"
    _set_deterministic_now(copy, mtime=_at_noon(checked_day + 1))
    exit_code, stdout, stderr = _run_main(
        copy=copy,
        out=out,
        monkeypatch=monkeypatch,
        site_id=site_id,
        incumbent_from_record=True,
    )
    assert exit_code == 0, stderr
    run_dir = out / cast(str, _result_payload(stdout)["run_dir"])
    return _read_lines_jsonl(run_dir)


def _assert_margin_premise(lines: list[dict[str, object]], *, checked_day: int) -> None:
    """SYN-b scores 0, SYN-a scores 2 (one miss, inside the margin): the
    incumbent question, not the raw score, decides the pick at this date."""
    line = _line_for(lines, snapshot_date=_d(checked_day), lead=0)
    selection = cast(dict[str, object], line["selection"])
    ranked = cast(list[dict[str, object]], selection["ranked"])
    assert [(r["key"], r["penalty"]) for r in ranked] == [
        ("example-src/syn-b", "0"),
        ("example-src/syn-a", "2"),
    ]
    assert selection["best"] == "example-src/syn-b"


def _assert_incumbent(
    lines: list[dict[str, object]],
    *,
    checked_day: int,
    expected_model: str | None,
    expected_selected_on: date | None,
    expected_selected: str | None,
    expected_outcome: str | None,
    expected_unrecognised_on: date | None = None,
) -> None:
    line = _line_for(lines, snapshot_date=_d(checked_day), lead=0)
    incumbent = cast(dict[str, object], line["incumbent"])
    selection = cast(dict[str, object], line["selection"])
    assert incumbent["model"] == expected_model
    assert incumbent["selected_on"] == (
        None if expected_selected_on is None else expected_selected_on.isoformat()
    )
    assert incumbent["unrecognised_on"] == (
        None
        if expected_unrecognised_on is None
        else expected_unrecognised_on.isoformat()
    )
    assert selection["selected"] == expected_selected
    assert selection["incumbent_outcome"] == expected_outcome
    assert line["incumbent_row_unrecognised"] == (expected_unrecognised_on is not None)


def test_incumbent_from_record_reads_the_stored_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Days 2-29's record rows say SYN-a, what the live S0 chain would
    itself record there; day 30's record row says SYN-b instead, a value
    the chain did not produce. With --incumbent-from-record, day 30 reads
    SYN-a from the row chain and stays kept by the margin, then day 31
    reads SYN-b straight from day 30's overridden row; without the flag,
    the live chain keeps SYN-a through both days instead."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_B,
        **{n: _DAY30_SELECTS_A for n in range(2, 30)},
    }
    copy, site_id = _build_fixture(
        tmp_path,
        name="record-override.db",
        days=range(1, 32),
        disc_day=_DISC,
        overrides=overrides,
    )
    out_with = tmp_path / "with-flag"
    out_without = tmp_path / "without-flag"

    _set_deterministic_now(copy, mtime=_at_noon(32))
    exit_with, stdout_with, stderr_with = _run_main(
        copy=copy,
        out=out_with,
        monkeypatch=monkeypatch,
        site_id=site_id,
        incumbent_from_record=True,
    )
    assert exit_with == 0, stderr_with
    run_dir_with = out_with / cast(str, _result_payload(stdout_with)["run_dir"])
    lines_with = _read_lines_jsonl(run_dir_with)
    _assert_margin_premise(lines_with, checked_day=30)
    _assert_incumbent(
        lines_with,
        checked_day=30,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(29),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    _assert_margin_premise(lines_with, checked_day=31)
    _assert_incumbent(
        lines_with,
        checked_day=31,
        expected_model="example-src/syn-b",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-b",
        expected_outcome="kept",
    )
    # M80 -> at incumbent["model"]: correct "example-src/syn-b" (read
    # straight from day 30's overridden record row, which disagrees with
    # the row chain's own day 30 pick just confirmed above), mutant
    # "example-src/syn-a" (an ignored flag falls back to the live S0
    # chain, which keeps SYN-a by the margin -- the same value the
    # without-flag run below produces).

    _set_deterministic_now(copy, mtime=_at_noon(32))
    exit_without, stdout_without, stderr_without = _run_main(
        copy=copy,
        out=out_without,
        monkeypatch=monkeypatch,
        site_id=site_id,
        incumbent_from_record=False,
    )
    assert exit_without == 0, stderr_without
    run_dir_without = out_without / cast(
        str, _result_payload(stdout_without)["run_dir"]
    )
    lines_without = _read_lines_jsonl(run_dir_without)
    _assert_margin_premise(lines_without, checked_day=30)
    _assert_incumbent(
        lines_without,
        checked_day=30,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(29),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    _assert_incumbent(
        lines_without,
        checked_day=31,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )


def test_incumbent_from_record_reaches_the_60_day_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; days 31-89 are a not_ready gap in the record.
    Day 90 is offset 60 from day 30 -- exactly INCUMBENT_LOOKBACK_DATES --
    so the look-back still reaches it and SYN-a (kept by the margin) is
    the incumbent."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: _NOT_READY for n in range(31, 90)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-boundary-90.db",
        days=range(30, 91),
        disc_day=_DISC_FAR,
        overrides=overrides,
        checked_day=90,
    )
    _assert_margin_premise(lines, checked_day=90)
    _assert_incumbent(
        lines,
        checked_day=90,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    # M92 -> at incumbent["model"]: correct "example-src/syn-a" (day 30,
    # at offset 60, is still inside the look-back), mutant None (a
    # 59-date mutant excludes day 30, leaving every date 31-89 a gap with
    # nothing to find).


def test_incumbent_from_record_past_the_60_day_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same fixture one day later: day 91 is offset 61 from day 30 --
    past INCUMBENT_LOOKBACK_DATES -- so the look-back no longer reaches
    it; with no incumbent, SYN-b's lower score decides outright."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: _NOT_READY for n in range(31, 91)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-boundary-91.db",
        days=range(30, 92),
        disc_day=_DISC_FAR,
        overrides=overrides,
        checked_day=91,
    )
    _assert_margin_premise(lines, checked_day=90)
    _assert_incumbent(
        lines,
        checked_day=90,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    _assert_margin_premise(lines, checked_day=91)
    _assert_incumbent(
        lines,
        checked_day=91,
        expected_model=None,
        expected_selected_on=None,
        expected_selected="example-src/syn-b",
        expected_outcome=None,
    )
    # M91 -> at incumbent["model"]: correct None (day 30, at offset 61, is
    # past the look-back), mutant "example-src/syn-a" (a 61-date mutant
    # still reaches day 30 through the unbroken not_ready run on 31-90).


def test_incumbent_from_record_reads_through_an_error_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; days 31-40 carry an error fallback_reason, a
    gap. The look-back reads through it to day 30's selection."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{
            n: {"method": "selector", "fallback_reason": "error"} for n in range(31, 41)
        },
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-error.db",
        days=range(1, 42),
        disc_day=_DISC,
        overrides=overrides,
        checked_day=41,
    )
    _assert_margin_premise(lines, checked_day=41)
    _assert_incumbent(
        lines,
        checked_day=41,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    # M94 -> at incumbent["model"]: correct "example-src/syn-a" (an error
    # row is a gap, so the look-back reads through days 31-40 to day 30),
    # mutant None (sorting an error row as a clear stops the look-back
    # right at day 40, finding nothing).


def test_incumbent_from_record_reads_through_a_no_method_key_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; days 31-40 carry a displayed object with no
    "method" key at all, also a gap. The look-back reads through it to
    day 30's selection."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: {} for n in range(31, 41)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-no-method.db",
        days=range(1, 42),
        disc_day=_DISC,
        overrides=overrides,
        checked_day=41,
    )
    _assert_margin_premise(lines, checked_day=41)
    _assert_incumbent(
        lines,
        checked_day=41,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    # No named mutant covers this branch: a displayed object missing the
    # "method" key is its own gap path in record_decision, distinct from
    # the fallback_reason gap exercised above (M94) and the lookback-depth
    # mutants above (M91/M92); this case pins that branch directly.


def test_incumbent_from_record_reads_through_a_missing_row_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; days 31-40 have no record row at all. The
    look-back reads through the missing dates to day 30's selection."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: None for n in range(31, 41)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-no-rows.db",
        days=range(1, 42),
        disc_day=_DISC,
        overrides=overrides,
        checked_day=41,
    )
    _assert_margin_premise(lines, checked_day=41)
    _assert_incumbent(
        lines,
        checked_day=41,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    # M96 -> at incumbent["model"]: correct "example-src/syn-a" (the
    # look-back reads past the missing rows on 31-40 to day 30), mutant
    # None (reading only day 40 -- which has no row -- finds nothing and
    # stops there instead of continuing the walk).


def test_incumbent_from_record_no_eligible_clears_the_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; day 35 is a no_eligible clear; days 36-40 are
    a not_ready gap. The clear at 35 ends the memory outright, so the gap
    after it never reaches back to day 30: with no incumbent, SYN-b's
    lower score decides outright. Days 31-34 have no record row at all,
    the same way the missing-row case above does, so the look-back from
    day 35 reaches day 30's selection."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: None for n in range(31, 35)},
        35: {"method": "selector", "fallback_reason": "no_eligible"},
        **{n: _NOT_READY for n in range(36, 41)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-no-eligible.db",
        days=range(1, 42),
        disc_day=_DISC,
        overrides=overrides,
        checked_day=41,
    )
    _assert_incumbent(
        lines,
        checked_day=35,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    _assert_margin_premise(lines, checked_day=41)
    _assert_incumbent(
        lines,
        checked_day=41,
        expected_model=None,
        expected_selected_on=None,
        expected_selected="example-src/syn-b",
        expected_outcome=None,
    )
    # M93b -> at incumbent["model"]: correct None (a no_eligible row is a
    # clear in daily_method.record_decision, ending the look-back right
    # at day 35), mutant "example-src/syn-a" (sorting it as a gap instead
    # lets the look-back read on through to day 30's selection).


def test_incumbent_from_record_current_clears_the_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; day 35 carries method "current", a clear;
    days 36-40 are a not_ready gap. The clear ends the memory outright,
    so with no incumbent, SYN-b's lower score decides outright. Days
    31-34 have no record row at all, the same way the missing-row case
    above does, so the look-back from day 35 reaches day 30's
    selection."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: None for n in range(31, 35)},
        35: {"method": "current"},
        **{n: _NOT_READY for n in range(36, 41)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-current.db",
        days=range(1, 42),
        disc_day=_DISC,
        overrides=overrides,
        checked_day=41,
    )
    _assert_incumbent(
        lines,
        checked_day=35,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    _assert_margin_premise(lines, checked_day=41)
    _assert_incumbent(
        lines,
        checked_day=41,
        expected_model=None,
        expected_selected_on=None,
        expected_selected="example-src/syn-b",
        expected_outcome=None,
    )
    # M95 -> at incumbent["model"]: correct None ("current" is a clear,
    # ending the look-back right at day 35), mutant "example-src/syn-a"
    # (sorting it as a gap instead lets the look-back read on through to
    # day 30's selection).


def test_incumbent_from_record_unrecognised_ends_the_lookback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Day 30 selects SYN-a; day 35 carries method "selector" with
    neither a selected model nor a fallback_reason -- unrecognised; days
    36-40 are a not_ready gap. The unrecognised row ends the memory and
    is reported by date, so with no incumbent, SYN-b's lower score
    decides outright. Days 31-34 have no record row at all, so the
    look-back would otherwise read through them to day 30's
    selection."""
    overrides: dict[int, Mapping[str, object] | None] = {
        30: _DAY30_SELECTS_A,
        **{n: None for n in range(31, 35)},
        35: {"method": "selector"},
        **{n: _NOT_READY for n in range(36, 41)},
    }
    lines = _run_record_case(
        tmp_path,
        monkeypatch,
        name="gap-unrecognised.db",
        days=range(1, 42),
        disc_day=_DISC,
        overrides=overrides,
        checked_day=41,
    )
    _assert_incumbent(
        lines,
        checked_day=35,
        expected_model="example-src/syn-a",
        expected_selected_on=_d(30),
        expected_selected="example-src/syn-a",
        expected_outcome="kept",
    )
    _assert_margin_premise(lines, checked_day=41)
    _assert_incumbent(
        lines,
        checked_day=41,
        expected_model=None,
        expected_selected_on=None,
        expected_selected="example-src/syn-b",
        expected_outcome=None,
        expected_unrecognised_on=_d(35),
    )
    # M99 -> the first assertion to fail is incumbent["model"] in
    # _assert_incumbent (checked first): correct None, mutant
    # "example-src/syn-a" (sorting the unrecognised row as a gap instead
    # lets the look-back read on through to day 30's selection);
    # unrecognised_on (correct day 35's date, mutant None), selected
    # (correct "example-src/syn-b", mutant "example-src/syn-a") and the
    # incumbent_row_unrecognised mark (correct True, mutant False) also
    # diverge, so the run never records that it crossed an unrecognised
    # row at all.


def test_incumbent_from_record_rejects_temperature(tmp_path: Path) -> None:
    """--incumbent-from-record only applies to the precip, freeze-prepare
    run it reads back; combined with --variable temperature it is an
    argparse-level usage error before anything is opened."""
    argv = [
        "--copy",
        str(tmp_path / "unused.db"),
        "--site-id",
        "1",
        "--out",
        str(tmp_path / "unused-out"),
        "--variable",
        "temperature",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
        "--incumbent-from-record",
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
    assert (
        "error: --incumbent-from-record is allowed only with --freeze-prepare "
        "--variable precip"
    ) in stderr.getvalue()
    assert stdout.getvalue() == ""


def test_incumbent_from_record_rejects_counts_only(tmp_path: Path) -> None:
    """--incumbent-from-record combined with --counts-only (not
    --freeze-prepare) is the same argparse-level usage error."""
    argv = [
        "--copy",
        str(tmp_path / "unused.db"),
        "--site-id",
        "1",
        "--out",
        str(tmp_path / "unused-out"),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--counts-only",
        "--incumbent-from-record",
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
    assert (
        "error: --incumbent-from-record is allowed only with --freeze-prepare "
        "--variable precip"
    ) in stderr.getvalue()
    assert stdout.getvalue() == ""

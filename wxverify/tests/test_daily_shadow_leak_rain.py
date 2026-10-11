"""§6.6 / §10.1 / §15.3 — the rain leak test.

Rain's leak test (§10.1, oracle file ``.tmp/rain-leak-oracle.md``) runs the
real pipeline twice with the arm forced — Arm A as built, Arm B by giving
one in-scope ``precip`` reading a NULL ``fetched_at`` so G-a fails — and
asserts that none of changes (i)-(iii) move the (s, 2) line except the one
field each change is allowed to move. Every expected value below is a
literal copied from the oracle text or derived from the fixture's own
construction, never computed by calling ``rain_case``/``rain_lines``/
``daily_shadow.main`` (the functions under test). The production readers
``RainEvents.full``/``RainEvents.as_of``, and one direct SQL query on
``forecast_samples``, are used only to confirm a change actually
produced the fixture state the oracle describes (a "landed" check), never
to derive the pipeline's expected output.

Fixture shape, one feed/model, one station ("SYN-RL-1", plus change
(iii)'s "SYN-RL-2"), UTC site (so every day is a 24-hour day with no DST
question):

* a dry day from s-67 to s+2 inclusive: every hour 0.0 mm, fetched 5
  minutes after its hour, with a matching ``daily_truth`` row for both
  ``precip_total`` and ``precip_occurrence`` (value 0.0, eligible) — a
  superset of rain's 60-day training window (K-59 to K) and of the
  case's own target (s+2), so every day in range can resolve to "dry"
  rather than "unknown" (§6.6);
* Arm B's G-a trigger: day K-1's last hour (23:00) gets a NULL
  ``fetched_at`` instead of the normal one;
* a dry "recorded" ``forecast_of_record`` row at every snapshot from
  s-67 to s, leads 0-5 (a superset of the oracle's "s-37 to s" and
  "s-67 to s" readings), with one feed run per target date, shared by
  every row that reads it (every hour 0.0 mm,
  issued and fetched well before the earliest snapshot that can read
  it), so every row's replay reproduces its own ``displayed`` values
  exactly (F1 passes) and the window's training days carry real,
  counted feed data — with one model, any non-empty day set makes the
  selector eligible (§10.1, F3-1);
* the case, (s, 2): one of the above rows (snapshot s, lead 2, target
  s+2, status ``recorded``), so the control is well-formed.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import os
import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.aggregate import displayed_daily
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "c" * 40
_APP_VERSION = "0.16.7"
_FEED_NAME = "model-rl"
_STATION_ID_TEXT = "SYN-RL-1"
_EXTRA_STATION_ID_TEXT = "SYN-RL-2"
# Fixed so every copy (built at a different wall-clock moment) gets the
# same station row; the stations table's created_at defaults to now.
_STATION_CREATED_AT = "2026-01-01T00:00:00.000Z"
_RAIN_THRESHOLD_MM = 0.2
_SNAPSHOT_TIME_SHORT = "07:00"
_SNAPSHOT_TIME_SQL = "07:00:00"

_S = date(2035, 6, 1)
_K = _S - timedelta(days=1)
_K_MINUS_1 = _K - timedelta(days=1)
_TARGET = _S + timedelta(days=2)
# A superset of rain's 60-day training window and of the oracle's "s-37
# to s" / "s-67 to s" record-row readings (§10.1; the date-range decision
# that resolved the ambiguity between them).
_WINDOW_FIRST = _S - timedelta(days=67)
_RECORD_LEADS = range(6)

# The one day-s (K+1) hour change (i)/Arm A raises by 5 mm, and the one
# target-day (s+2) hour change (ii)'s late run raises by 5 mm.
_CHANGE_I_HOUR = 2
_CHANGE_II_HOUR = 10


def _snapshot_utc(local_date: date) -> datetime:
    return resolve_snapshot_utc("UTC", local_date, _SNAPSHOT_TIME_SHORT)


_T_S = _snapshot_utc(_S)


def _date_range(first: date, last: date) -> list[date]:
    """Every local date from ``first`` to ``last``, inclusive."""
    out: list[date] = []
    day = first
    while day <= last:
        out.append(day)
        day += timedelta(days=1)
    return out


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, f"rain-leak-{name}")
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


def _make_station(
    conn: sqlite3.Connection, *, site_id: int, pws_station_id: str
) -> int:
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)"
        " VALUES (?, ?, 0.0, 0.0, 900.0, 1, ?)",
        (site_id, pws_station_id, _STATION_CREATED_AT),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _insert_station_readings(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    hours: Sequence[int],
    increments: Sequence[float],
    fetched_ats: Sequence[datetime | None],
) -> None:
    """One station-day's ``precip`` readings, each hour's running total.

    ``increments[i]`` is hour ``hours[i]``'s own (non-cumulative) reading;
    ``source_raw`` carries the running total so
    ``daily_method.station_day_total`` always parses a running total. A
    ``None`` in ``fetched_ats`` writes a NULL ``fetched_at`` — Arm B's G-a
    trigger.
    """
    assert len(hours) == len(increments) == len(fetched_ats)
    cumulative = 0.0
    for hour, increment, fetched_at in zip(hours, increments, fetched_ats, strict=True):
        cumulative += increment
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
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
                increment,
                source_raw,
                None if fetched_at is None else isoformat_utc(fetched_at),
            ),
        )


def _dry_day_fetched_ats(
    local_date: date, *, null_hour: int | None
) -> list[datetime | None]:
    out: list[datetime | None] = []
    for hour in range(24):
        if null_hour is not None and hour == null_hour:
            out.append(None)
            continue
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        out.append(valid + timedelta(minutes=5))
    return out


def _insert_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    local_date: date,
    quantity: str,
    value: float | None,
    eligible: bool,
) -> None:
    day_start_dt = datetime(
        local_date.year, local_date.month, local_date.day, 0, tzinfo=UTC
    )
    day_start = isoformat_utc(day_start_dt)
    day_end = isoformat_utc(day_start_dt + timedelta(days=1))
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, wet_hours, dry_hours, rain_threshold_mm,
             day_start_utc, day_end_utc, timezone, stale, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 24, 24, NULL, NULL, ?, ?, ?, 'UTC', 0, ?)
        """,
        (
            site_id,
            local_date.isoformat(),
            quantity,
            value,
            1 if eligible else 0,
            _RAIN_THRESHOLD_MM,
            day_start,
            day_end,
            generation_id,
        ),
    )


def _update_daily_truth_value(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    local_date: date,
    quantity: str,
    value: float,
) -> None:
    conn.execute(
        """
        UPDATE daily_truth SET value = ?
        WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?
              AND quantity = ?
        """,
        (value, site_id, generation_id, local_date.isoformat(), quantity),
    )


def _insert_feed_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    values: Sequence[float],
    issued_at: datetime,
    fetched_at: datetime,
    run_id: str,
) -> None:
    assert len(values) == 24
    issued_iso = isoformat_utc(issued_at)
    fetched_iso = isoformat_utc(fetched_at)
    for hour, value in enumerate(values):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead_hours = max(1, int((valid - issued_at).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', ?, ?)
            """,
            (
                site_id,
                feed_id,
                issued_iso,
                isoformat_utc(valid),
                lead_hours,
                value,
                run_id,
                fetched_iso,
            ),
        )


def _insert_precip_row(
    conn: sqlite3.Connection,
    *,
    site: Site,
    snapshot_date: date,
    lead: int,
    target_date: date,
    app_version: str,
    daily_quantities: str,
) -> None:
    snapshot_utc = isoformat_utc(_snapshot_utc(snapshot_date))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, ?, ?, ?, 'precip', ?, 'recorded',
                'on_time', ?, 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            _SNAPSHOT_TIME_SQL,
            snapshot_utc,
            target_date.isoformat(),
            lead,
            json.dumps({"rain_threshold_mm": _RAIN_THRESHOLD_MM}),
            app_version,
            daily_quantities,
        ),
    )


def _precip_daily_quantities(
    *, total_mm: float, wet_hours: float, extrema_feed_ids: list[int]
) -> str:
    return json.dumps(
        {
            "displayed": {
                "total_mm": total_mm,
                "wet_hours": wet_hours,
                "extrema_feed_ids": extrema_feed_ids,
            }
        }
    )


def _insert_dry_feed_run(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    target_date: date,
    run_id: str,
    issued_at: datetime,
    fetched_at: datetime,
) -> dict[str, float | None]:
    """One feed run, every hour 0.0 mm; returns its ``displayed_daily``."""
    values = [0.0] * 24
    _insert_feed_samples(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=target_date,
        values=values,
        issued_at=issued_at,
        fetched_at=fetched_at,
        run_id=run_id,
    )
    return displayed_daily("precip", [values], rain_threshold_mm=_RAIN_THRESHOLD_MM)


def _build_record_matrix(
    conn: sqlite3.Connection,
    *,
    site: Site,
    feed_id: int,
    first_snapshot: date,
    last_snapshot: date,
) -> None:
    """A dry ``recorded`` row at every (snapshot, lead) in range, leads 0-5.

    Each distinct target date gets one feed run, issued and fetched well
    before the earliest snapshot that can read it (``target`` minus 5
    days, lead 5's own snapshot), so every row's replay reproduces its own
    ``displayed`` values exactly (F1 passes) whatever its lead. This gives
    the rain window's training days real, counted feed data: with one
    model, any non-empty day set makes the selector eligible (§10.1,
    F3-1).
    """
    displayed_by_target: dict[date, dict[str, float | None]] = {}
    for snapshot in _date_range(first_snapshot, last_snapshot):
        for lead in _RECORD_LEADS:
            target = snapshot + timedelta(days=lead)
            displayed = displayed_by_target.get(target)
            if displayed is None:
                fetched_at = _snapshot_utc(target - timedelta(days=5)) - timedelta(
                    hours=1
                )
                issued_at = fetched_at - timedelta(hours=1)
                displayed = _insert_dry_feed_run(
                    conn,
                    site_id=site.site_id,
                    feed_id=feed_id,
                    target_date=target,
                    run_id=f"base-{target.isoformat()}",
                    issued_at=issued_at,
                    fetched_at=fetched_at,
                )
                displayed_by_target[target] = displayed
            _insert_precip_row(
                conn,
                site=site,
                snapshot_date=snapshot,
                lead=lead,
                target_date=target,
                app_version=_APP_VERSION,
                daily_quantities=_precip_daily_quantities(
                    total_mm=cast(float, displayed["total_mm"]),
                    wet_hours=cast(float, displayed["wet_hours"]),
                    extrema_feed_ids=[feed_id],
                ),
            )


def _build_fixture(
    tmp_path: Path,
    *,
    name: str,
    arm: Literal["A", "B"],
    change_i: bool = False,
    change_ii: bool = False,
    change_iii: bool = False,
) -> tuple[Path, Site, int]:
    """One copy of the leak test's fixture (§10.1), the given arm and
    changes applied. Returns the copy path, its ``Site`` and the
    station's id (change iii's extra station, when built, is not
    returned — its id plays no further role once inserted)."""
    assert not (change_iii and arm == "B"), "change (iii) is Arm A only"
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, _FEED_NAME)
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id=_STATION_ID_TEXT
        )

        # Station readings and published truth: every hour of every day
        # from s-67 to s+2, dry throughout, so every day in range can
        # resolve to "dry" rather than "unknown" (§6.6).
        for day in _date_range(_WINDOW_FIRST, _TARGET):
            null_hour = 23 if (arm == "B" and day == _K_MINUS_1) else None
            increments = [0.0] * 24
            if change_i and arm == "A" and day == _S:
                increments[_CHANGE_I_HOUR] = 5.0
            fetched_ats = _dry_day_fetched_ats(day, null_hour=null_hour)
            _insert_station_readings(
                conn,
                station_id=station_id,
                local_date=day,
                hours=list(range(24)),
                increments=increments,
                fetched_ats=fetched_ats,
            )
            for quantity in ("precip_total", "precip_occurrence"):
                _insert_daily_truth(
                    conn,
                    site_id=site.site_id,
                    generation_id=site.generation_id,
                    local_date=day,
                    quantity=quantity,
                    value=0.0,
                    eligible=True,
                )

        if change_i and arm == "B":
            # Change (i), Arm B: edit the published rows of day K and day
            # s (= K+1) for both rain quantities — metadata only, the
            # underlying station readings are untouched.
            for local_date in (_K, _S):
                _update_daily_truth_value(
                    conn,
                    site_id=site.site_id,
                    generation_id=site.generation_id,
                    local_date=local_date,
                    quantity="precip_total",
                    value=5.0,
                )
                _update_daily_truth_value(
                    conn,
                    site_id=site.site_id,
                    generation_id=site.generation_id,
                    local_date=local_date,
                    quantity="precip_occurrence",
                    value=1.0,
                )

        if change_iii:
            extra_station_id = _make_station(
                conn,
                site_id=site.site_id,
                pws_station_id=_EXTRA_STATION_ID_TEXT,
            )
            increments_iii = [0.5] + [0.0] * 23
            fetched_ats_iii: list[datetime | None] = [
                _T_S + timedelta(hours=1, minutes=hour) for hour in range(24)
            ]
            _insert_station_readings(
                conn,
                station_id=extra_station_id,
                local_date=_K,
                hours=list(range(24)),
                increments=increments_iii,
                fetched_ats=fetched_ats_iii,
            )

        # The record rows: a dry "recorded" row at every snapshot from
        # s-67 to s, leads 0-5 — this is what gives the window's ~60
        # training days real, F1-passing feed data.
        _build_record_matrix(
            conn,
            site=site,
            feed_id=feed_id,
            first_snapshot=_WINDOW_FIRST,
            last_snapshot=_S,
        )

        if change_ii:
            # Change (ii): one later-issued run of the same feed, fetched
            # AFTER T(s), equal to the base run except one hour +5mm.
            late_values = [0.0] * 24
            late_values[_CHANGE_II_HOUR] = 5.0
            _insert_feed_samples(
                conn,
                site_id=site.site_id,
                feed_id=feed_id,
                local_date=_TARGET,
                values=late_values,
                issued_at=_T_S - timedelta(minutes=1),
                fetched_at=_T_S + timedelta(minutes=1),
                run_id="late",
            )

        conn.commit()
    finally:
        conn.close()
    return copy, site, station_id


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _run_main(
    copy: Path,
    out_dir: Path,
    *,
    monkeypatch: pytest.MonkeyPatch,
    site_id: int,
) -> tuple[Path, dict[str, object]]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    stamp = datetime(
        _TARGET.year, _TARGET.month, _TARGET.day, 12, tzinfo=UTC
    ) + timedelta(days=2)
    os.utime(copy, (stamp.timestamp(), stamp.timestamp()))
    last_complete = latest_knowable_target("UTC", stamp)
    assert last_complete >= _TARGET, last_complete
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out_dir),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-RL-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    assert exit_code == 0, stdout.getvalue() + stderr.getvalue()
    for line in stdout.getvalue().splitlines():
        if line.startswith(daily_shadow.RESULT_PREFIX):
            result = cast(
                "dict[str, object]",
                json.loads(line[len(daily_shadow.RESULT_PREFIX) :]),
            )
            run_dir = out_dir / cast(str, result["run_dir"])
            header = cast(
                "dict[str, object]", json.loads((run_dir / "header.json").read_text())
            )
            return run_dir, header
    raise AssertionError(
        f"no {daily_shadow.RESULT_PREFIX!r} line in stdout: {stdout.getvalue()!r}"
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(text) for text in path.read_text().splitlines() if text]


def _find_line(
    lines: list[dict[str, object]], *, snapshot: date, lead: int
) -> dict[str, object]:
    matches = [
        line
        for line in lines
        if line["snapshot_date"] == snapshot.isoformat() and line["lead"] == lead
    ]
    assert len(matches) == 1, (snapshot, lead, lines)
    return matches[0]


def _canonical(obj: dict[str, object], *, excluding: str | None = None) -> str:
    trimmed = {k: v for k, v in obj.items() if k != excluding}
    return json.dumps(trimmed, sort_keys=True)


def _rain_events(copy: Path, *, site_id: int) -> daily_shadow.RainEvents:
    """The run's own ``RainEvents`` reader, direct from the copy — used
    only to confirm a change landed in the station data, never to derive
    a pipeline expected value."""
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        readings = daily_shadow.load_station_rain(
            conn, site_id=site_id, first_day=None, last_day=None, known_by=None
        )
    finally:
        conn.close()
    store = daily_shadow.StationDayStore(readings, timezone="UTC")
    return daily_shadow.RainEvents(store, timezone="UTC")


def test_leak_liveness_before_any_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both arms produce a matched (s, 2) line with a live persistence
    value and ``training_asof_differs`` 0 — and are byte-identical to
    each other before any change (oracle §10.1 liveness, §15.3)."""

    def _run_and_check(*, arm: Literal["A", "B"]) -> dict[str, object]:
        copy, site, _station_id = _build_fixture(tmp_path, name=f"live-{arm}", arm=arm)
        run_dir, header = _run_main(
            copy,
            tmp_path / f"out-live-{arm}",
            monkeypatch=monkeypatch,
            site_id=site.site_id,
        )
        assert header["arm"] == arm, header
        lines = _read_jsonl(run_dir / "lines.jsonl")
        line = _find_line(lines, snapshot=_S, lead=2)

        assert line["unmatched"] is None, line["unmatched"]
        assert line["training_asof_differs"] == 0, line["training_asof_differs"]
        assert line["method"] == "selector", line["method"]
        selection = cast("dict[str, object]", line["selection"])
        assert selection["no_eligible"] is False, selection
        assert selection["ranked"], selection
        event = cast("dict[str, object]", line["event"])
        assert event["outcome"] == "dry", event
        assert line["published_reason"] == {
            "precip_occurrence": None,
            "precip_total": None,
        }, line["published_reason"]
        published_truth = cast("dict[str, object]", line["published_truth"])
        for quantity in ("precip_occurrence", "precip_total"):
            row = published_truth[quantity]
            assert isinstance(row, dict), published_truth
            assert cast("dict[str, object]", row)["value"] == 0.0, published_truth
        symbols = cast("dict[str, object]", line["symbols"])
        persistence_symbol = cast("dict[str, object]", symbols["persistence"])
        assert persistence_symbol["members"] == [_K.isoformat()], persistence_symbol
        assert "no_eligible" not in cast("list[object]", persistence_symbol["flags"]), (
            persistence_symbol
        )
        amounts = cast("dict[str, object]", line["amounts"])
        persistence_amount = cast("dict[str, object]", amounts["persistence"])
        assert persistence_amount["members"] == [_K.isoformat()], persistence_amount
        assert "no_eligible" not in cast("list[object]", persistence_amount["flags"]), (
            persistence_amount
        )
        return line

    line_a = _run_and_check(arm="A")
    line_b = _run_and_check(arm="B")
    assert _canonical(line_a) == _canonical(line_b)


def test_leak_change_i_arm_a_window_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Change (i), Arm A: raising day s's hour 2 by 5 mm makes day s
    rain-nearby as of T(s) (the landed check, via ``RainEvents`` directly)
    but the (s, 2) line stays byte-identical (M146: a mutant window
    reaching day s would move persistence onto it)."""
    before_copy, before_site, _ = _build_fixture(tmp_path, name="ci-a-before", arm="A")
    before_dir, before_header = _run_main(
        before_copy,
        tmp_path / "out-ci-a-before",
        monkeypatch=monkeypatch,
        site_id=before_site.site_id,
    )
    after_copy, after_site, _ = _build_fixture(
        tmp_path, name="ci-a-after", arm="A", change_i=True
    )
    after_dir, after_header = _run_main(
        after_copy,
        tmp_path / "out-ci-a-after",
        monkeypatch=monkeypatch,
        site_id=after_site.site_id,
    )

    assert before_header["arm"] == "A"
    assert after_header["arm"] == "A"

    # Landed check: as of T(s), day s has too few known hours to be dry
    # (only hours 0-6 are fetched by T(s) = 07:00, short of the 22 a dry
    # day needs) in the baseline copy, and is rain-nearby in the changed
    # copy, where the raised hour alone clears the single-station trigger
    # whatever its hour count.
    before_events = _rain_events(before_copy, site_id=before_site.site_id)
    after_events = _rain_events(after_copy, site_id=after_site.site_id)
    assert before_events.as_of(_S, _T_S).event.outcome == "unknown"
    assert after_events.as_of(_S, _T_S).event.outcome == "rain_nearby"

    before_lines = _read_jsonl(before_dir / "lines.jsonl")
    after_lines = _read_jsonl(after_dir / "lines.jsonl")
    before_line = _find_line(before_lines, snapshot=_S, lead=2)
    after_line = _find_line(after_lines, snapshot=_S, lead=2)
    assert _canonical(before_line) == _canonical(after_line)
    assert before_line["training_asof_differs"] == 0
    assert after_line["training_asof_differs"] == 0

    before_grid = _read_jsonl(before_dir / "trigger_grid.jsonl")
    after_grid = _read_jsonl(after_dir / "trigger_grid.jsonl")
    before_cell = _find_line(before_grid, snapshot=_S, lead=2)
    after_cell = _find_line(after_grid, snapshot=_S, lead=2)
    assert _canonical(before_cell) == _canonical(after_cell)


def test_leak_change_i_arm_b_published_rows_inert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Change (i), Arm B: editing the published rows of day K and day s
    leaves the (s, 2) line's comparison fields untouched, because they
    read only day s+2's rows (M162: a mutant that reads them at the
    snapshot date would move them). The landed check is the (s, 0)
    line, whose target is day s (= K+1): it carries the changed rows."""
    before_copy, before_site, _ = _build_fixture(tmp_path, name="ci-b-before", arm="B")
    before_dir, before_header = _run_main(
        before_copy,
        tmp_path / "out-ci-b-before",
        monkeypatch=monkeypatch,
        site_id=before_site.site_id,
    )
    after_copy, after_site, _ = _build_fixture(
        tmp_path, name="ci-b-after", arm="B", change_i=True
    )
    after_dir, after_header = _run_main(
        after_copy,
        tmp_path / "out-ci-b-after",
        monkeypatch=monkeypatch,
        site_id=after_site.site_id,
    )

    assert before_header["arm"] == "B"
    assert after_header["arm"] == "B"

    before_lines = _read_jsonl(before_dir / "lines.jsonl")
    after_lines = _read_jsonl(after_dir / "lines.jsonl")
    before_s2 = _find_line(before_lines, snapshot=_S, lead=2)
    after_s2 = _find_line(after_lines, snapshot=_S, lead=2)
    assert _canonical(before_s2) == _canonical(after_s2)
    assert before_s2["training_asof_differs"] == 0
    assert before_s2["published_truth"] == after_s2["published_truth"]
    assert before_s2["published_reason"] == after_s2["published_reason"]

    before_grid = _read_jsonl(before_dir / "trigger_grid.jsonl")
    after_grid = _read_jsonl(after_dir / "trigger_grid.jsonl")
    before_cell = _find_line(before_grid, snapshot=_S, lead=2)
    after_cell = _find_line(after_grid, snapshot=_S, lead=2)
    assert _canonical(before_cell) == _canonical(after_cell)

    # Landed check: the (s, 0) line's target is day K+1 (= s); it carries
    # the changed published rows, each field equal to the value written.
    after_s0 = _find_line(after_lines, snapshot=_S, lead=0)
    published = cast("dict[str, object]", after_s0["published_truth"])
    precip_total = published["precip_total"]
    assert isinstance(precip_total, dict)
    precip_occurrence = published["precip_occurrence"]
    assert isinstance(precip_occurrence, dict)
    assert precip_total == {
        "value": 5.0,
        "eligible": True,
        "stale": False,
        "rain_threshold_mm": _RAIN_THRESHOLD_MM,
    }, published
    assert precip_occurrence == {
        "value": 1.0,
        "eligible": True,
        "stale": False,
        "rain_threshold_mm": _RAIN_THRESHOLD_MM,
    }, published


@pytest.mark.parametrize("arm", ["A", "B"])
def test_leak_change_ii_forecast_cutoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arm: Literal["A", "B"]
) -> None:
    """Change (ii), either arm: a later-issued run of the same feed,
    fetched after T(s), leaves the (s, 2) line untouched — the as-of
    loader's ``fetched_at`` cut-off (``forecast/data.py:130``) excludes it
    (M150: removing the cutoff would let it in, since its ``issued_at``
    alone (:131) does not exclude it)."""
    before_copy, before_site, _ = _build_fixture(
        tmp_path, name=f"cii-before-{arm}", arm=arm
    )
    before_dir, before_header = _run_main(
        before_copy,
        tmp_path / f"out-cii-before-{arm}",
        monkeypatch=monkeypatch,
        site_id=before_site.site_id,
    )
    after_copy, after_site, _ = _build_fixture(
        tmp_path, name=f"cii-after-{arm}", arm=arm, change_ii=True
    )
    after_dir, after_header = _run_main(
        after_copy,
        tmp_path / f"out-cii-after-{arm}",
        monkeypatch=monkeypatch,
        site_id=after_site.site_id,
    )

    assert before_header["arm"] == arm
    assert after_header["arm"] == arm

    # Landed check: the late run exists, issued by T(s) but fetched after
    # it — the precondition M150's cut-off removal would stop excluding.
    conn = sqlite3.connect(after_copy)
    try:
        row = conn.execute(
            "SELECT issued_at, fetched_at FROM forecast_samples"
            " WHERE model_run_id = 'late' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    late_issued = datetime.fromisoformat(row[0])
    late_fetched = datetime.fromisoformat(row[1])
    assert late_issued <= _T_S, (late_issued, _T_S)
    assert late_fetched > _T_S, (late_fetched, _T_S)

    before_lines = _read_jsonl(before_dir / "lines.jsonl")
    after_lines = _read_jsonl(after_dir / "lines.jsonl")
    before_line = _find_line(before_lines, snapshot=_S, lead=2)
    after_line = _find_line(after_lines, snapshot=_S, lead=2)
    assert _canonical(before_line) == _canonical(after_line)
    assert before_line["training_asof_differs"] == 0
    assert after_line["training_asof_differs"] == 0

    before_grid = _read_jsonl(before_dir / "trigger_grid.jsonl")
    after_grid = _read_jsonl(after_dir / "trigger_grid.jsonl")
    before_cell = _find_line(before_grid, snapshot=_S, lead=2)
    after_cell = _find_line(after_grid, snapshot=_S, lead=2)
    assert _canonical(before_cell) == _canonical(after_cell)


def test_leak_change_iii_new_station_full_vs_asof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Change (iii), Arm A only: a second station reading day K alone,
    fetched after T(s), makes day K rain-nearby in full (0.5 mm, one
    hour) but leaves it dry as of T(s) (the landed check, via
    ``RainEvents`` directly: M147/M148/M149 would each erase that gap).
    The (s, 2) line stays byte-identical except ``training_asof_differs``,
    which goes from 0 to exactly 1."""
    before_copy, before_site, _ = _build_fixture(tmp_path, name="ciii-before", arm="A")
    before_dir, before_header = _run_main(
        before_copy,
        tmp_path / "out-ciii-before",
        monkeypatch=monkeypatch,
        site_id=before_site.site_id,
    )
    after_copy, after_site, _ = _build_fixture(
        tmp_path, name="ciii-after", arm="A", change_iii=True
    )
    after_dir, after_header = _run_main(
        after_copy,
        tmp_path / "out-ciii-after",
        monkeypatch=monkeypatch,
        site_id=after_site.site_id,
    )

    assert before_header["arm"] == "A"
    assert after_header["arm"] == "A"

    # Landed check: day K is rain-nearby in full (the extra station's
    # single 0.5 mm hour), only after the extra station is added.
    after_events = _rain_events(after_copy, site_id=after_site.site_id)
    assert after_events.full(_K).event.outcome == "rain_nearby"
    full_amount = after_events.full(_K).amount.amount
    assert full_amount is not None and math.isclose(full_amount, 0.5)

    before_lines = _read_jsonl(before_dir / "lines.jsonl")
    after_lines = _read_jsonl(after_dir / "lines.jsonl")
    before_line = _find_line(before_lines, snapshot=_S, lead=2)
    after_line = _find_line(after_lines, snapshot=_S, lead=2)
    assert _canonical(before_line, excluding="training_asof_differs") == _canonical(
        after_line, excluding="training_asof_differs"
    )
    before_differs = before_line["training_asof_differs"]
    after_differs = after_line["training_asof_differs"]
    assert before_differs == 0, before_differs
    assert after_differs == 1, after_differs

    before_grid = _read_jsonl(before_dir / "trigger_grid.jsonl")
    after_grid = _read_jsonl(after_dir / "trigger_grid.jsonl")
    before_cell = _find_line(before_grid, snapshot=_S, lead=2)
    after_cell = _find_line(after_grid, snapshot=_S, lead=2)
    assert _canonical(before_cell) == _canonical(after_cell)

    # Landed check, moved below the line/grid assertions (it calls the
    # same production ``StationDayStore.as_of`` that M147 changes, so a
    # setup check placed before the kill assertions would stop the test
    # there under that mutant instead of at the plan's kill point): day K
    # is still dry as of T(s), the gap the extra station's late
    # ``fetched_at`` is supposed to preserve.
    assert after_events.as_of(_K, _T_S).event.outcome == "dry"

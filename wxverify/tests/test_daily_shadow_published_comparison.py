"""§6.1 / §15.3 — the published-rain-rows comparison.

The comparison (``published_comparison()``) is a read-only side report
built in ``daily_shadow``'s stage one: it never feeds ``decisions`` and
never changes a line's ``scored``/``f1``/``control``/``event`` state. It
reads the published rows from ``daily_truth`` only through
``published_reasons()``/``_published_reason()`` (``truth_absent`` →
``threshold_mismatch`` → ``truth_stale`` → ``truth_not_eligible``, in that
order, for scored lines only) and recomputes a "wet" flag from the
published ``precip_occurrence`` row instead of the station-derived event.

Every expected value pinned below is a literal copied from the plan text
(oracle file ``.tmp/published-comparison-oracle.md``, §6.1 lines 159-171
and §15.3 lines 1412-1421) — never computed by calling
``symbol_report``/``chance_report``/``amount_report``/``published_comparison``
or any other production reporting function. Each run gets its own fresh
frozen copy.

Fixture shape, one feed/model, one station, four case days D1-D4 plus 20
training days (so a single-model run is unconditionally provisional —
``select_model()`` requires ``len(at_min) >= 2`` for the non-provisional
path — and so the training window has exactly ``CLIM_MIN_DAYS`` = 20
station-derived days, the minimum R-clim needs):

* D1 — 8 wet hours (0.25 mm each) → control total 2.00 mm, wet_hours 8;
  station event ``rain_nearby``, observed amount 2.0 mm.
* D2 — 0 wet hours → control total 0.00 mm; station event ``dry``,
  observed amount 0.0 mm.
* D3 — 6 wet hours → control total 1.50 mm; station event ``rain_nearby``
  built from a *different* station total (1.0 mm, deliberately not equal
  to the control) so the comparison's observed value is never
  accidentally equal to the control's.
* D4 — 7 wet hours → control total 1.75 mm; station event ``dry``,
  observed amount 0.0 mm.

Production names used — all public, none underscore-prefixed:
``daily_shadow.main``, ``daily_shadow.RecordRow``, ``daily_shadow.Site``,
``daily_shadow.Unmatched``, ``daily_shadow.RAIN_RULES``,
``daily_shadow.CLIM_MIN_DAYS``, ``daily_shadow.RESULT_PREFIX``,
``daily_shadow.control_value``, ``daily_shadow.load_station_rain``,
``daily_shadow.StationDayStore``, ``daily_shadow.RainEvents``,
``daily_shadow.package_manifest``, ``daily_shadow.load_site``,
``daily_method.climatology_chance``, ``aggregate.displayed_daily``,
``record.resolve_snapshot_utc``, ``simulate.latest_knowable_target``,
``timeutil.isoformat_utc``, ``migrations.run_migrations``,
``tz_generations.ensure_published_generation``.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import os
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast import daily_method
from wxverify.forecast.aggregate import displayed_daily
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import RecordRow, Site, Unmatched
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "b" * 40
_APP_VERSION = "0.16.7"
_TRAINING_APP_VERSION = "0.15.0"
_FEED_NAME = "model-a"
_STATION_ID_TEXT = "SYN-PC-1"
# Fixed so Run 1 and Run 2 (built at different wall-clock moments) get the
# same header station_set; the stations table's created_at column defaults
# to the current timestamp if not given explicitly.
_STATION_CREATED_AT = "2026-01-01T00:00:00.000Z"

_D1 = date(2035, 9, 1)
_D2 = _D1 + timedelta(days=1)
_D3 = _D1 + timedelta(days=2)
_D4 = _D1 + timedelta(days=3)
_CASE_DAYS = (_D1, _D2, _D3, _D4)
_TRAINING_DAYS = [_D1 - timedelta(days=n) for n in range(20, 0, -1)]

# D1..D4 wet-hour counts for the control (0.25 mm each): 2.00 / 0.00 / 1.50 / 1.75 mm.
_WET_HOURS = {_D1: 8, _D2: 0, _D3: 6, _D4: 7}
_CONTROL_TOTAL_MM = {_D1: 2.0, _D2: 0.0, _D3: 1.5, _D4: 1.75}
# Station totals for the case days' own events: D3 is deliberately 1.0 mm,
# not the control's 1.5 mm.
_STATION_WET_HOURS = {_D1: 8, _D2: 0, _D3: 4, _D4: 0}
_STATION_TOTAL_MM = {_D1: 2.0, _D2: 0.0, _D3: 1.0, _D4: 0.0}
_STATION_EVENT = {_D1: "rain_nearby", _D2: "dry", _D3: "rain_nearby", _D4: "dry"}


def _rain_hours(*, wet_count: int) -> list[float]:
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


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


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "published-comparison-site")
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
    snapshot_date: date,
    app_version: str,
    daily_quantities: str,
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
                'on_time', '{"rain_threshold_mm": 0.2}', 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            snapshot_utc,
            snapshot_date.isoformat(),
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
    values: list[float],
) -> None:
    assert len(values) == 24
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
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-pc', ?)
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
        " VALUES (?, ?, 0.0, 0.0, 900.0, 1, ?)",
        (site_id, pws_station_id, _STATION_CREATED_AT),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _insert_station_day(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    values: list[float],
    fetched_offset_minutes: int,
) -> None:
    """24 ``precip`` readings for one station-day with a valid running total.

    ``source_raw`` is written as ``"<cumulative> mm precipTotal"`` for
    every hour, so :func:`wxverify.forecast.daily_method.station_day_total`
    always parses a running total (never the ``unparsed_total`` problem);
    the running total is non-decreasing, so ``counter_reset`` never fires
    either. ``fetched_at`` is each hour's valid time plus
    ``fetched_offset_minutes``.
    """
    assert len(values) == 24
    cumulative = 0.0
    for hour, value in enumerate(values):
        cumulative += value
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        fetched = valid + timedelta(minutes=fetched_offset_minutes)
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


def _insert_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    local_date: date,
    quantity: str,
    value: float | None,
    eligible: bool,
    stale: bool = False,
    rain_threshold_mm: float | None = 0.2,
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
        VALUES (?, ?, ?, ?, ?, 24, 24, NULL, NULL, ?, ?, ?, 'UTC', ?, ?)
        """,
        (
            site_id,
            local_date.isoformat(),
            quantity,
            value,
            1 if eligible else 0,
            rain_threshold_mm,
            day_start,
            day_end,
            1 if stale else 0,
            generation_id,
        ),
    )


def _build_base_fixture(tmp_path: Path, *, name: str) -> tuple[Path, Site, int, int]:
    """One feed, one station, D1-D4 case rows/events, 20 training days."""
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, _FEED_NAME)
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id=_STATION_ID_TEXT
        )

        for day in _CASE_DAYS:
            values = _rain_hours(wet_count=_WET_HOURS[day])
            _insert_precip_day_samples(
                conn,
                site_id=site.site_id,
                feed_id=feed_id,
                local_date=day,
                values=values,
            )
            displayed = displayed_daily("precip", [values], rain_threshold_mm=0.2)
            total_mm = cast(float, displayed["total_mm"])
            wet_hours = cast(float, displayed["wet_hours"])
            assert total_mm == _CONTROL_TOTAL_MM[day]
            assert wet_hours == _WET_HOURS[day]
            _insert_precip_row(
                conn,
                site=site,
                snapshot_date=day,
                app_version=_APP_VERSION,
                daily_quantities=_precip_daily_quantities(
                    total_mm=total_mm, wet_hours=wet_hours, extrema_feed_ids=[feed_id]
                ),
            )
            station_values = _rain_hours(wet_count=_STATION_WET_HOURS[day])
            _insert_station_day(
                conn,
                station_id=station_id,
                local_date=day,
                values=station_values,
                fetched_offset_minutes=5,
            )

        for day in _TRAINING_DAYS:
            zero_values = [0.0] * 24
            _insert_precip_day_samples(
                conn,
                site_id=site.site_id,
                feed_id=feed_id,
                local_date=day,
                values=zero_values,
            )
            _insert_precip_row(
                conn,
                site=site,
                snapshot_date=day,
                app_version=_TRAINING_APP_VERSION,
                daily_quantities=_precip_daily_quantities(
                    total_mm=0.0, wet_hours=0.0, extrema_feed_ids=[feed_id]
                ),
            )
            _insert_station_day(
                conn,
                station_id=station_id,
                local_date=day,
                values=zero_values,
                fetched_offset_minutes=5,
            )

        conn.commit()
    finally:
        conn.close()
    return copy, site, feed_id, station_id


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _set_deterministic_now(copy: Path) -> date:
    """Fix the copy's mtime so main()'s last-complete target is D4 or later.

    Reviewer note L1: the fixture's D1-D4 cases must all be knowable by the
    stamp main() reads, so every caller is checked against that floor before
    any run result is trusted.
    """
    stamp = datetime(_D4.year, _D4.month, _D4.day, 12, tzinfo=UTC) + timedelta(days=2)
    os.utime(copy, (stamp.timestamp(), stamp.timestamp()))
    last_complete = latest_knowable_target("UTC", stamp)
    assert last_complete >= _D4, last_complete
    return last_complete


def _run_main(
    copy: Path, out_dir: Path, *, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[Path, dict[str, object]]:
    """Run ``--freeze-prepare`` and return ``(run_dir, header)``.

    ``run_dir`` is the actual subdirectory ``main()`` wrote (resolved from
    the ``daily_shadow_result`` stdout line's ``run_dir``, relative to
    ``out_dir``), never ``out_dir`` itself.
    """
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
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
        "SYN-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    assert exit_code == 0, stdout.getvalue() + stderr.getvalue()
    for line in stdout.getvalue().splitlines():
        if line.startswith(daily_shadow.RESULT_PREFIX):
            result = cast(
                "dict[str, object]", json.loads(line[len(daily_shadow.RESULT_PREFIX) :])
            )
            run_dir = out_dir / cast(str, result["run_dir"])
            header = cast(
                "dict[str, object]", json.loads((run_dir / "header.json").read_text())
            )
            return run_dir, header
    raise AssertionError(
        f"no {daily_shadow.RESULT_PREFIX!r} line in stdout: {stdout.getvalue()!r}"
    )


def _find_line(lines: list[dict[str, object]], day: date) -> dict[str, object]:
    matches = [
        line
        for line in lines
        if line["snapshot_date"] == day.isoformat() and line["lead"] == 0
    ]
    assert len(matches) == 1, (day, lines)
    return matches[0]


def _read_lines_jsonl(run_dir: Path) -> list[dict[str, object]]:
    path = run_dir / "lines.jsonl"
    return [json.loads(text) for text in path.read_text().splitlines() if text]


# ---------------------------------------------------------------------------
# Liveness: the fixture itself gives the controls/events the oracle assumes,
# proven directly against production code, never through a report function.
# ---------------------------------------------------------------------------


def test_fixture_controls_match_the_plans_literal_values() -> None:
    """D1-D4 controls are 2.00 / 0.00 / 1.50 / 1.75 mm (oracle, fixture text)."""
    for day in _CASE_DAYS:
        values = _rain_hours(wet_count=_WET_HOURS[day])
        displayed = displayed_daily("precip", [values], rain_threshold_mm=0.2)
        row = RecordRow(
            snapshot_date=day,
            target_date=day,
            lead=0,
            snapshot_utc=isoformat_utc(resolve_snapshot_utc("UTC", day, "07:00")),
            status="recorded",
            write_path="on_time",
            app_version=_APP_VERSION,
            methodology_version=1,
            tz_generation_id=1,
            policy='{"rain_threshold_mm": 0.2}',
            candidates=None,
            daily_quantities=_precip_daily_quantities(
                total_mm=cast(float, displayed["total_mm"]),
                wet_hours=cast(float, displayed["wet_hours"]),
                extrema_feed_ids=[1],
            ),
        )
        control = daily_shadow.control_value(
            row, variable="precip", rain_rules=daily_shadow.RAIN_RULES
        )
        assert not isinstance(control, Unmatched), (day, control)
        assert control.amount == _CONTROL_TOTAL_MM[day], (day, control.amount)


def test_fixture_events_match_the_plans_literal_values(tmp_path: Path) -> None:
    """D1-D4 station events: rain_nearby/2.0, dry/0.0, rain_nearby/1.0, dry/0.0."""
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="events.db")
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        readings = daily_shadow.load_station_rain(
            conn, site_id=site.site_id, first_day=_D1, last_day=_D4, known_by=None
        )
        store = daily_shadow.StationDayStore(readings, timezone=site.timezone)
        events = daily_shadow.RainEvents(store, timezone=site.timezone)
        for day in _CASE_DAYS:
            built = events.full(day)
            assert built.event.outcome == _STATION_EVENT[day], (day, built.event)
            assert built.amount.amount == _STATION_TOTAL_MM[day], (day, built.amount)
    finally:
        conn.close()


def test_fixture_training_window_has_exactly_clim_min_days(tmp_path: Path) -> None:
    """D1's training window has exactly CLIM_MIN_DAYS=20 station-derived days.

    R-clim's minimum (``climatology_chance``) is driven purely by the count
    of station-derived ``events`` entries known by the case's decision
    time, never by forecast-sample counts.
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="clim.db")
    known_by = datetime(_D1.year, _D1.month, _D1.day, 7, tzinfo=UTC)
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        readings = daily_shadow.load_station_rain(
            conn,
            site_id=site.site_id,
            first_day=_TRAINING_DAYS[0],
            last_day=_TRAINING_DAYS[-1],
            known_by=known_by,
        )
        store = daily_shadow.StationDayStore(readings, timezone=site.timezone)
        events = daily_shadow.RainEvents(store, timezone=site.timezone)
        window: dict[date, bool] = {
            day: events.as_of(day, known_by).event.outcome == "rain_nearby"
            for day in _TRAINING_DAYS
        }
        chance = daily_method.climatology_chance(
            window, min_days=daily_shadow.CLIM_MIN_DAYS
        )
        assert chance.known_days == 20, chance
        assert chance.fallback_reason is None, chance
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Run 1 — the published rows agree with the events on every case day.
# ---------------------------------------------------------------------------


def _insert_run1_truth(conn: sqlite3.Connection, *, site: Site) -> None:
    occurrence = {_D1: 1.0, _D2: 0.0, _D3: 1.0, _D4: 0.0}
    total = {_D1: 2.0, _D2: 0.0, _D3: 1.0, _D4: 0.0}
    for day in _CASE_DAYS:
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=day,
            quantity="precip_occurrence",
            value=occurrence[day],
            eligible=True,
        )
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=day,
            quantity="precip_total",
            value=total[day],
            eligible=True,
        )


def test_run1_agrees_on_every_case_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 1 (oracle): full agreement; comparison equals decisions everywhere.

    Pins: ``agreement`` has 4 cases, all ``both_wet``/``both_dry``,
    ``share_agreeing == 1.0`` at lead "0"; ``symbol``/``chance``/``amount``
    sub-dicts of the comparison equal the header's own ``decisions`` dicts
    for every name and every lead (the comparison never decides anything);
    ``counts.published_comparison`` shows every case ``usable`` with every
    reason at 0. D4 is shown at 1.75 mm but its published occurrence is
    0.0 (dry), so the control's symbol has one false alarm, not zero.
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run1.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run1_truth(conn, site=site)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    run_dir, header = _run_main(
        copy, tmp_path / "run1-out", monkeypatch=monkeypatch, site_id=site.site_id
    )

    decisions = cast(dict[str, object], header["decisions"])
    comparison = cast(dict[str, object], header["published_comparison"])

    agreement = cast(dict[str, object], comparison["agreement"])
    lead0 = cast(dict[str, object], agreement["0"])
    assert lead0["cases"] == 4, lead0
    assert lead0["both_wet"] == 2, lead0
    assert lead0["both_dry"] == 2, lead0
    assert lead0["event_only_wet"] == 0, lead0
    assert lead0["published_only_wet"] == 0, lead0
    assert lead0["share_agreeing"] == 1.0, lead0

    for name in ("symbol", "chance", "amount"):
        comparison_part = cast(dict[str, object], comparison[name])
        decisions_part = cast(dict[str, object], decisions[name])
        key = "sources" if name != "chance" else "brier_per_lead"
        # Equal as whole dicts: every name (source) and every lead "0"-"7".
        assert comparison_part[key] == decisions_part[key], (
            name,
            comparison_part[key],
            decisions_part[key],
        )

    symbol_sources = cast(dict[str, object], comparison["symbol"])["sources"]
    symbol_sources = cast(dict[str, object], symbol_sources)
    control_symbol0 = cast(dict[str, object], symbol_sources["control"])["0"]
    control_symbol0 = cast(dict[str, object], control_symbol0)
    assert control_symbol0["cases"] == 4, control_symbol0
    assert control_symbol0["misses"] == 0, control_symbol0
    assert control_symbol0["false_alarms"] == 1, control_symbol0
    assert control_symbol0["shown"] == 3, control_symbol0
    assert control_symbol0["dry_shown_share"] == 0.5, control_symbol0
    assert control_symbol0["cost_per_case"] == 0.25, control_symbol0

    briers = cast(dict[str, object], comparison["chance"])["brier_per_lead"]
    control_brier0 = cast(dict[str, object], briers)["control"]
    assert cast(dict[str, object], control_brier0)["0"] == 0.25, control_brier0

    amount_sources = cast(dict[str, object], comparison["amount"])["sources"]
    amount_sources = cast(dict[str, object], amount_sources)
    control_amount0 = cast(dict[str, object], amount_sources["control"])["0"]
    control_amount0 = cast(dict[str, object], control_amount0)
    assert control_amount0["loss"] == 0.5625, control_amount0
    assert control_amount0["mae"] == 0.5625, control_amount0
    assert control_amount0["bias"] == 0.5625, control_amount0

    counts = cast(dict[str, object], header["counts"])
    counts_pc = cast(dict[str, object], counts["published_comparison"])
    lead0_counts = cast(dict[str, object], counts_pc["0"])
    for quantity in ("precip_occurrence", "precip_total"):
        quantity_counts = cast(dict[str, object], lead0_counts[quantity])
        assert quantity_counts["usable"] == 4, (quantity, quantity_counts)
        for reason in daily_shadow.PUBLISHED_REASONS:
            assert quantity_counts[reason] == 0, (quantity, reason, quantity_counts)

    lines = _read_lines_jsonl(run_dir)
    for day in _CASE_DAYS:
        line = _find_line(lines, day)
        assert line["published_reason"] == {
            "precip_occurrence": None,
            "precip_total": None,
        }, (day, line)
    for line in lines:
        if line["snapshot_date"] in {d.isoformat() for d in _CASE_DAYS} and (
            line["lead"] == 0
        ):
            continue
        assert line["published_reason"] is None, line

    # --- Run-level liveness (gap 7): each D1-D4 lead-0 line is scored, its
    # control is matched (no f1_mismatch), and it has CH-Q, S-Q, R-clim and
    # a served amount; J = 1 and the ranking is provisional.
    for day in _CASE_DAYS:
        line = _find_line(lines, day)
        event = cast(dict[str, object], line["event"])
        assert line["control"] is not None, (day, line)
        assert event["outcome"] != "unknown", (day, line)
        assert line["unmatched"] is None, (day, line)
        assert line["f1_ok"] is True, (day, line)
        served = cast(dict[str, object], line["served"])
        assert served["symbol"] is not None, (day, served)  # S-Q
        assert served["chance"] is not None, (day, served)  # CH-Q
        assert served["amount"] is not None, (day, served)
        clim = cast(dict[str, object], line["clim"])
        assert clim["chance"] is not None, (day, clim)  # R-clim
        selection = cast(dict[str, object], line["selection"])
        ranked = cast(list[object], selection["ranked"])
        assert len(ranked) == 1, (day, selection)  # J = 1
        assert selection["provisional"] is True, (day, selection)

    # --- Gap 8: leads "1"-"7" have 0 cases in the comparison.
    for lead in daily_shadow.RECORD_LEADS:
        if lead == 0:
            continue
        agreement_lead = cast(dict[str, object], agreement[str(lead)])
        assert agreement_lead["cases"] == 0, (lead, agreement_lead)
        symbol_sources_lead = cast(dict[str, object], symbol_sources["control"])
        assert cast(dict[str, object], symbol_sources_lead[str(lead)])["cases"] == 0, (
            lead,
            symbol_sources_lead,
        )


# ---------------------------------------------------------------------------
# Run 2 — the published rows disagree with the events on every case day,
# and the comparison still never changes decisions/unmatched/counts.
# ---------------------------------------------------------------------------


def _insert_run2_truth(conn: sqlite3.Connection, *, site: Site) -> None:
    occurrence = {_D1: 1.0, _D2: 1.0, _D3: 0.0, _D4: 0.0}
    total = {_D1: 2.0, _D2: 0.5, _D3: 0.0, _D4: 0.0}
    for day in _CASE_DAYS:
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=day,
            quantity="precip_occurrence",
            value=occurrence[day],
            eligible=True,
        )
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=day,
            quantity="precip_total",
            value=total[day],
            eligible=True,
        )


def test_run2_disagrees_on_every_case_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 2 (oracle): full disagreement; share_agreeing == 0.5 at lead "0".

    D1 both_wet, D2 published_only_wet, D3 event_only_wet, D4 both_dry.
    Pins the published-amount ``loss_by_weight`` literal keys/values.
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run2.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run2_truth(conn, site=site)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    _run_dir, header = _run_main(
        copy, tmp_path / "run2-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    comparison = cast(dict[str, object], header["published_comparison"])

    agreement = cast(dict[str, object], comparison["agreement"])
    lead0 = cast(dict[str, object], agreement["0"])
    assert lead0["cases"] == 4, lead0
    assert lead0["both_wet"] == 1, lead0
    assert lead0["both_dry"] == 1, lead0
    assert lead0["event_only_wet"] == 1, lead0
    assert lead0["published_only_wet"] == 1, lead0
    assert lead0["share_agreeing"] == 0.5, lead0

    symbol_sources = cast(dict[str, object], comparison["symbol"])["sources"]
    symbol_sources = cast(dict[str, object], symbol_sources)
    control_symbol0 = cast(dict[str, object], symbol_sources["control"])["0"]
    control_symbol0 = cast(dict[str, object], control_symbol0)
    assert control_symbol0["cases"] == 4, control_symbol0
    assert control_symbol0["missing"] == 0, control_symbol0
    assert control_symbol0["misses"] == 1, control_symbol0
    assert control_symbol0["false_alarms"] == 2, control_symbol0
    assert control_symbol0["shown"] == 3, control_symbol0
    assert control_symbol0["dry_shown_share"] == 1.0, control_symbol0
    assert control_symbol0["cost_per_case"] == 1.0, control_symbol0

    briers = cast(dict[str, object], comparison["chance"])["brier_per_lead"]
    control_brier0 = cast(dict[str, object], briers)["control"]
    assert cast(dict[str, object], control_brier0)["0"] == 0.75, control_brier0

    amount_sources = cast(dict[str, object], comparison["amount"])["sources"]
    amount_sources = cast(dict[str, object], amount_sources)
    control_amount0 = cast(dict[str, object], amount_sources["control"])["0"]
    control_amount0 = cast(dict[str, object], control_amount0)
    expected_loss_by_weight = {"1.0": 0.9375, "2.0": 1.0625, "3.0": 1.1875}
    assert control_amount0["cases"] == 4, control_amount0
    assert control_amount0["loss"] == 1.0, control_amount0
    assert control_amount0["loss_by_weight"] == expected_loss_by_weight
    assert control_amount0["mae"] == 0.9375, control_amount0
    assert control_amount0["bias"] == 0.6875, control_amount0


_HEADER_KEYS_ALLOWED_TO_DIFFER = frozenset(
    {"copy", "gates", "arm", "t2", "replay_time", "published_comparison"}
)


def test_published_comparison_decides_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 1 vs Run 2 (M160): it decides nothing.

    The published rows differ completely between the two runs (full
    agreement vs full disagreement), yet everything the run actually
    *decides on* is untouched: ``lines.jsonl`` objects are byte-identical
    except ``published_truth``; ``trigger_grid.jsonl``, ``station_days.jsonl``
    and ``fault_states.json`` are byte-identical; the header differs only in
    ``copy``, ``gates``, ``arm``, ``t2``, ``replay_time`` and
    ``published_comparison`` (and ``published_comparison`` must differ).
    """
    copy1, site1, _f1, _s1 = _build_base_fixture(tmp_path, name="m160-run1.db")
    conn = sqlite3.connect(copy1)
    try:
        _insert_run1_truth(conn, site=site1)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy1)
    run_dir1, header1 = _run_main(
        copy1,
        tmp_path / "m160-run1-out",
        monkeypatch=monkeypatch,
        site_id=site1.site_id,
    )

    copy2, site2, _f2, _s2 = _build_base_fixture(tmp_path, name="m160-run2.db")
    conn = sqlite3.connect(copy2)
    try:
        _insert_run2_truth(conn, site=site2)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy2)
    run_dir2, header2 = _run_main(
        copy2,
        tmp_path / "m160-run2-out",
        monkeypatch=monkeypatch,
        site_id=site2.site_id,
    )

    assert header1["decisions"] == header2["decisions"]
    assert header1["unmatched"] == header2["unmatched"]
    assert header1["rain_event_counts"] == header2["rain_event_counts"]
    counts1 = cast(dict[str, object], header1["counts"])
    counts2 = cast(dict[str, object], header2["counts"])
    assert counts1["cases"] == counts2["cases"]
    assert counts1["record_rows"] == counts2["record_rows"]
    assert header1["published_comparison"] != header2["published_comparison"]

    # The header differs in exactly a subset of the allowed keys.
    assert set(header1) == set(header2), (set(header1), set(header2))
    differing = {key for key in header1 if header1[key] != header2[key]}
    assert differing <= _HEADER_KEYS_ALLOWED_TO_DIFFER, differing
    assert "published_comparison" in differing, differing

    # lines.jsonl: byte-identical objects, except published_truth; same
    # count and order.
    lines1 = _read_lines_jsonl(run_dir1)
    lines2 = _read_lines_jsonl(run_dir2)
    assert len(lines1) == len(lines2)
    for line1, line2 in zip(lines1, lines2, strict=True):
        stripped1 = {k: v for k, v in line1.items() if k != "published_truth"}
        stripped2 = {k: v for k, v in line2.items() if k != "published_truth"}
        # Re-serialised the way production writes each line (daily_shadow's
        # ``_dumps_lines``: ``json.dumps(_jsonable(line), sort_keys=True)``),
        # so a field that changed type (e.g. 0 vs 0.0 vs False) is caught —
        # dict ``==`` would treat those as equal.
        dump1 = json.dumps(stripped1, sort_keys=True)
        dump2 = json.dumps(stripped2, sort_keys=True)
        assert dump1 == dump2, (stripped1, stripped2)

    # trigger_grid.jsonl, station_days.jsonl and fault_states.json:
    # byte-identical files.
    for name in ("trigger_grid.jsonl", "station_days.jsonl", "fault_states.json"):
        bytes1 = (run_dir1 / name).read_bytes()
        bytes2 = (run_dir2 / name).read_bytes()
        assert bytes1 == bytes2, name


# ---------------------------------------------------------------------------
# Run 3 / 3b / 3c / 3d — threshold mismatch, null threshold, stale-and-
# mismatched, and a site-level threshold change that must not matter.
# ---------------------------------------------------------------------------


def _insert_run3_truth(
    conn: sqlite3.Connection,
    *,
    site: Site,
    d3_threshold: float | None,
    d3_stale: bool,
) -> None:
    occurrence = {_D1: 1.0, _D2: 0.0, _D3: 1.0, _D4: 0.0}
    total = {_D1: 2.0, _D2: 0.0, _D3: 1.0, _D4: 0.0}
    for day in _CASE_DAYS:
        threshold = d3_threshold if day == _D3 else 0.2
        stale = d3_stale if day == _D3 else False
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=day,
            quantity="precip_occurrence",
            value=occurrence[day],
            eligible=True,
            stale=stale,
            rain_threshold_mm=threshold,
        )
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=day,
            quantity="precip_total",
            value=total[day],
            eligible=True,
            stale=stale,
            rain_threshold_mm=threshold,
        )


def _assert_d3_threshold_mismatch_and_rest_null(lines: list[dict[str, object]]) -> None:
    for day in _CASE_DAYS:
        line = _find_line(lines, day)
        published_reason = cast(dict[str, object], line["published_reason"])
        expected = "threshold_mismatch" if day == _D3 else None
        assert published_reason["precip_occurrence"] == expected, (
            day,
            published_reason,
        )
        assert published_reason["precip_total"] == expected, (day, published_reason)


def _assert_run3_counts(header: dict[str, object]) -> None:
    counts = cast(dict[str, object], header["counts"])
    counts_pc = cast(dict[str, object], counts["published_comparison"])
    lead0_counts = cast(dict[str, object], counts_pc["0"])
    for quantity in ("precip_occurrence", "precip_total"):
        quantity_counts = cast(dict[str, object], lead0_counts[quantity])
        assert quantity_counts["usable"] == 3, (quantity, quantity_counts)
        assert quantity_counts["threshold_mismatch"] == 1, (quantity, quantity_counts)
        for reason in daily_shadow.PUBLISHED_REASONS:
            if reason == "threshold_mismatch":
                continue
            assert quantity_counts[reason] == 0, (quantity, reason, quantity_counts)


def test_run3_threshold_mismatch_on_d3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 3 (oracle): D3's published rows carry threshold 0.3, control's is 0.2.

    ``threshold_mismatch`` on D3 for both quantities; D1/D2/D4 null (usable).
    ``counts.published_comparison`` shows 3 usable and 1 threshold_mismatch
    per quantity at lead "0". The control symbol is ``cases`` 3, ``misses``
    0, ``false_alarms`` 1, ``shown`` 2, ``dry_shown_share`` 0.5 and
    ``cost_per_case`` 1/3 (to 1e-12); the agreement is ``cases`` 3,
    ``both_wet`` 1, ``both_dry`` 2. ``unmatched``, ``decisions``,
    ``counts.cases``, ``counts.record_rows`` and ``rain_event_counts`` equal
    Run 1's, and every ``lines.jsonl`` object equals Run 1's except
    ``published_truth`` and ``published_reason``.
    """
    copy1, site1, _f1, _s1 = _build_base_fixture(tmp_path, name="run1-for-run3.db")
    conn = sqlite3.connect(copy1)
    try:
        _insert_run1_truth(conn, site=site1)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy1)
    run_dir1, header1 = _run_main(
        copy1,
        tmp_path / "run1-for-run3-out",
        monkeypatch=monkeypatch,
        site_id=site1.site_id,
    )
    lines1 = _read_lines_jsonl(run_dir1)

    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run3.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run3_truth(conn, site=site, d3_threshold=0.3, d3_stale=False)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    run_dir, header = _run_main(
        copy, tmp_path / "run3-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)
    _assert_d3_threshold_mismatch_and_rest_null(lines)
    _assert_run3_counts(header)

    comparison = cast(dict[str, object], header["published_comparison"])
    symbol_sources = cast(dict[str, object], comparison["symbol"])["sources"]
    control_symbol0 = cast(
        dict[str, object], cast(dict[str, object], symbol_sources)["control"]
    )["0"]
    control_symbol0 = cast(dict[str, object], control_symbol0)
    assert control_symbol0["cases"] == 3, control_symbol0
    assert control_symbol0["misses"] == 0, control_symbol0
    assert control_symbol0["false_alarms"] == 1, control_symbol0
    assert control_symbol0["shown"] == 2, control_symbol0
    assert control_symbol0["dry_shown_share"] == 0.5, control_symbol0
    cost_per_case = cast(float, control_symbol0["cost_per_case"])
    assert math.isclose(cost_per_case, 1 / 3, rel_tol=0.0, abs_tol=1e-12), (
        control_symbol0
    )

    agreement = cast(dict[str, object], comparison["agreement"])
    lead0 = cast(dict[str, object], agreement["0"])
    assert lead0["cases"] == 3, lead0
    assert lead0["both_wet"] == 1, lead0
    assert lead0["both_dry"] == 2, lead0

    assert header["unmatched"] == header1["unmatched"]
    assert header["decisions"] == header1["decisions"]
    counts = cast(dict[str, object], header["counts"])
    counts1 = cast(dict[str, object], header1["counts"])
    assert counts["cases"] == counts1["cases"]
    assert counts["record_rows"] == counts1["record_rows"]
    assert header["rain_event_counts"] == header1["rain_event_counts"]

    assert len(lines) == len(lines1)
    for line, line1 in zip(lines, lines1, strict=True):
        stripped = {
            k: v
            for k, v in line.items()
            if k not in ("published_truth", "published_reason")
        }
        stripped1 = {
            k: v
            for k, v in line1.items()
            if k not in ("published_truth", "published_reason")
        }
        assert stripped == stripped1, (stripped, stripped1)


def test_run3b_null_threshold_is_also_threshold_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 3b (oracle): D3's published rows carry a NULL rain_threshold_mm.

    ``_published_reason``'s own condition (``row.rain_threshold_mm is None
    or row.rain_threshold_mm != threshold_mm``) treats null the same as a
    mismatched number — still ``threshold_mismatch``.
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run3b.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run3_truth(conn, site=site, d3_threshold=None, d3_stale=False)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    run_dir, header = _run_main(
        copy, tmp_path / "run3b-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)
    _assert_d3_threshold_mismatch_and_rest_null(lines)
    _assert_run3_counts(header)


def test_run3c_stale_and_mismatched_gives_threshold_mismatch_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 3c (oracle): D3's rows are both stale AND threshold-mismatched.

    ``PUBLISHED_REASONS`` order is tried ``threshold_mismatch`` before
    ``truth_stale``, so D3 stays ``threshold_mismatch``, never ``truth_stale``.
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run3c.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run3_truth(conn, site=site, d3_threshold=0.3, d3_stale=True)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    run_dir, header = _run_main(
        copy, tmp_path / "run3c-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)
    _assert_d3_threshold_mismatch_and_rest_null(lines)
    _assert_run3_counts(header)


def test_run3d_site_threshold_change_does_not_affect_reasons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 3d (oracle): the SITE's rain_threshold_mm changes to 0.3.

    The comparison's threshold check reads the record row's OWN policy
    (``control.policy.rain_threshold_mm``, still 0.2 — unchanged by this
    run), never ``sites.rain_threshold_mm``. Run 1's rows (threshold 0.2)
    are unchanged here, so every reason stays null.
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run3d.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run1_truth(conn, site=site)
        conn.execute(
            "UPDATE sites SET rain_threshold_mm = 0.3 WHERE id = ?",
            (site.site_id,),
        )
        row = conn.execute(
            "SELECT rain_threshold_mm FROM sites WHERE id = ?", (site.site_id,)
        ).fetchone()
        assert row is not None, row
        assert row[0] == 0.3, row
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    run_dir, header = _run_main(
        copy, tmp_path / "run3d-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)
    for day in _CASE_DAYS:
        line = _find_line(lines, day)
        published_reason = cast(dict[str, object], line["published_reason"])
        assert published_reason["precip_occurrence"] is None, (day, published_reason)
        assert published_reason["precip_total"] is None, (day, published_reason)

    counts = cast(dict[str, object], header["counts"])
    counts_pc = cast(dict[str, object], counts["published_comparison"])
    lead0_counts = cast(dict[str, object], counts_pc["0"])
    for quantity in ("precip_occurrence", "precip_total"):
        quantity_counts = cast(dict[str, object], lead0_counts[quantity])
        assert quantity_counts["usable"] == 4, (quantity, quantity_counts)
        for reason in daily_shadow.PUBLISHED_REASONS:
            assert quantity_counts[reason] == 0, (quantity, reason, quantity_counts)


# ---------------------------------------------------------------------------
# Run 4 — missing row (D1), stale (D2), ineligible/null (D3), mixed (D4).
# ---------------------------------------------------------------------------


def test_run4_missing_stale_and_ineligible_published_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 4 (oracle): D1 no row, D2 stale, D3 ineligible, D4 total ineligible.

    Reasons — occurrence: D1 truth_absent, D2 truth_stale, D3
    truth_not_eligible, D4 null (usable); total: D1 truth_absent, D2
    truth_stale, D3 truth_not_eligible, D4 truth_not_eligible. Only D4's
    occurrence is usable, so the comparison's own ``cases``/``amount``
    population at lead "0" has exactly 1 case, and ``amount.sources`` is
    empty (D4 has no usable published total to compare against).
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run4.db")
    conn = sqlite3.connect(copy)
    try:
        # D1: no published row for either quantity.
        # D2: Run 1's values, both rows stale.
        for quantity, value in (("precip_occurrence", 0.0), ("precip_total", 0.0)):
            _insert_daily_truth(
                conn,
                site_id=site.site_id,
                generation_id=site.generation_id,
                local_date=_D2,
                quantity=quantity,
                value=value,
                eligible=True,
                stale=True,
            )
        # D3: occurrence ineligible with a null value; total ineligible with
        # a value present anyway (still not eligible).
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=_D3,
            quantity="precip_occurrence",
            value=None,
            eligible=False,
        )
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=_D3,
            quantity="precip_total",
            value=0.8,
            eligible=False,
        )
        # D4: occurrence usable (Run 1's value); total ineligible.
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=_D4,
            quantity="precip_occurrence",
            value=0.0,
            eligible=True,
        )
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            generation_id=site.generation_id,
            local_date=_D4,
            quantity="precip_total",
            value=0.0,
            eligible=False,
        )
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    copy1, site1, _f1, _s1 = _build_base_fixture(tmp_path, name="run1-for-run4.db")
    conn = sqlite3.connect(copy1)
    try:
        _insert_run1_truth(conn, site=site1)
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy1)
    _run_dir1, header1 = _run_main(
        copy1,
        tmp_path / "run1-for-run4-out",
        monkeypatch=monkeypatch,
        site_id=site1.site_id,
    )

    run_dir, payload = _run_main(
        copy, tmp_path / "run4-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)

    expected_occurrence_reason = {
        _D1: "truth_absent",
        _D2: "truth_stale",
        _D3: "truth_not_eligible",
        _D4: None,
    }
    expected_total_reason = {
        _D1: "truth_absent",
        _D2: "truth_stale",
        _D3: "truth_not_eligible",
        _D4: "truth_not_eligible",
    }
    for day in _CASE_DAYS:
        line = _find_line(lines, day)
        published_reason = cast(dict[str, object], line["published_reason"])
        occurrence_got = published_reason["precip_occurrence"]
        total_got = published_reason["precip_total"]
        assert occurrence_got == expected_occurrence_reason[day], (day, line)
        assert total_got == expected_total_reason[day], (day, line)
    d1_line = _find_line(lines, _D1)
    expected_d1_truth = {"precip_occurrence": None, "precip_total": None}
    assert d1_line["published_truth"] == expected_d1_truth, d1_line

    comparison = cast(dict[str, object], payload["published_comparison"])
    agreement = cast(dict[str, object], comparison["agreement"])
    lead0_agreement = cast(dict[str, object], agreement["0"])
    assert lead0_agreement["cases"] == 1, lead0_agreement
    assert lead0_agreement["both_dry"] == 1, lead0_agreement
    assert lead0_agreement["share_agreeing"] == 1.0, lead0_agreement

    amount = cast(dict[str, object], comparison["amount"])
    amount_cases = cast(dict[str, object], amount["cases"])
    for lead in daily_shadow.RECORD_LEADS:
        assert amount_cases[str(lead)] == 0, (lead, amount_cases)
    assert amount["sources"] == {}, amount

    symbol_sources = cast(dict[str, object], comparison["symbol"])["sources"]
    control_symbol0 = cast(
        dict[str, object], cast(dict[str, object], symbol_sources)["control"]
    )["0"]
    control_symbol0 = cast(dict[str, object], control_symbol0)
    assert control_symbol0["cases"] == 1, control_symbol0
    assert control_symbol0["misses"] == 0, control_symbol0
    assert control_symbol0["false_alarms"] == 1, control_symbol0
    assert control_symbol0["shown"] == 1, control_symbol0
    assert control_symbol0["dry_shown_share"] == 1.0, control_symbol0
    assert control_symbol0["cost_per_case"] == 1.0, control_symbol0

    briers = cast(dict[str, object], comparison["chance"])["brier_per_lead"]
    control_brier0 = cast(dict[str, object], briers)["control"]
    assert cast(dict[str, object], control_brier0)["0"] == 1.0, control_brier0
    chance_cases = cast(dict[str, object], comparison["chance"])["cases"]
    assert cast(dict[str, object], chance_cases)["0"] == 1, chance_cases

    counts = cast(dict[str, object], payload["counts"])
    counts_pc = cast(dict[str, object], counts["published_comparison"])
    lead0_counts = cast(dict[str, object], counts_pc["0"])
    occurrence_counts = cast(dict[str, object], lead0_counts["precip_occurrence"])
    assert occurrence_counts["usable"] == 1, occurrence_counts
    assert occurrence_counts["truth_absent"] == 1, occurrence_counts
    assert occurrence_counts["truth_stale"] == 1, occurrence_counts
    assert occurrence_counts["truth_not_eligible"] == 1, occurrence_counts
    assert occurrence_counts["threshold_mismatch"] == 0, occurrence_counts
    total_counts = cast(dict[str, object], lead0_counts["precip_total"])
    assert total_counts["usable"] == 0, total_counts
    assert total_counts["truth_absent"] == 1, total_counts
    assert total_counts["truth_stale"] == 1, total_counts
    assert total_counts["truth_not_eligible"] == 2, total_counts
    assert total_counts["threshold_mismatch"] == 0, total_counts

    # As in Run 3: unmatched, decisions, counts.cases, counts.record_rows
    # and rain_event_counts equal Run 1's, and every lines.jsonl object
    # equals Run 1's except published_truth and published_reason.
    assert payload["unmatched"] == header1["unmatched"]
    assert payload["decisions"] == header1["decisions"]
    counts1 = cast(dict[str, object], header1["counts"])
    assert counts["cases"] == counts1["cases"]
    assert counts["record_rows"] == counts1["record_rows"]
    assert payload["rain_event_counts"] == header1["rain_event_counts"]

    lines1 = _read_lines_jsonl(_run_dir1)
    assert len(lines) == len(lines1)
    for line, line1 in zip(lines, lines1, strict=True):
        stripped = {
            k: v
            for k, v in line.items()
            if k not in ("published_truth", "published_reason")
        }
        stripped1 = {
            k: v
            for k, v in line1.items()
            if k not in ("published_truth", "published_reason")
        }
        assert stripped == stripped1, (stripped, stripped1)


# ---------------------------------------------------------------------------
# Run 5 — a non-binary precip_occurrence value is truth_not_eligible.
# ---------------------------------------------------------------------------


def test_run5_non_binary_occurrence_value_is_not_eligible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run 5 (oracle): D4's published occurrence value is 0.5 (not 0.0/1.0).

    ``_published_reason``'s own rule — eligible, non-stale, but
    ``quantity == "precip_occurrence" and row.value not in (0.0, 1.0)`` —
    gives ``truth_not_eligible`` even though ``eligible`` is True. D4's
    total is unaffected (still Run 1's usable 0.0 mm).
    """
    copy, site, _feed_id, _station_id = _build_base_fixture(tmp_path, name="run5.db")
    conn = sqlite3.connect(copy)
    try:
        _insert_run1_truth(conn, site=site)
        conn.execute(
            "UPDATE daily_truth SET value = 0.5 WHERE site_id = ? AND local_date = ?"
            " AND quantity = 'precip_occurrence'",
            (site.site_id, _D4.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy)

    run_dir, header = _run_main(
        copy, tmp_path / "run5-out", monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)

    d4_line = _find_line(lines, _D4)
    published_reason = cast(dict[str, object], d4_line["published_reason"])
    assert published_reason["precip_occurrence"] == "truth_not_eligible", d4_line
    assert published_reason["precip_total"] is None, d4_line

    for day in (_D1, _D2, _D3):
        line = _find_line(lines, day)
        published_reason = cast(dict[str, object], line["published_reason"])
        assert published_reason["precip_occurrence"] is None, (day, published_reason)
        assert published_reason["precip_total"] is None, (day, published_reason)

    comparison = cast(dict[str, object], header["published_comparison"])
    symbol_sources = cast(dict[str, object], comparison["symbol"])["sources"]
    control_symbol0 = cast(
        dict[str, object], cast(dict[str, object], symbol_sources)["control"]
    )["0"]
    control_symbol0 = cast(dict[str, object], control_symbol0)
    assert control_symbol0["cases"] == 3, control_symbol0
    assert control_symbol0["misses"] == 0, control_symbol0
    assert control_symbol0["false_alarms"] == 0, control_symbol0
    assert control_symbol0["shown"] == 2, control_symbol0
    assert control_symbol0["dry_shown_share"] == 0.0, control_symbol0
    assert control_symbol0["cost_per_case"] == 0.0, control_symbol0

    amount_cases = cast(dict[str, object], comparison["amount"])["cases"]
    assert cast(dict[str, object], amount_cases)["0"] == 3, amount_cases

    agreement = cast(dict[str, object], comparison["agreement"])
    lead0_agreement = cast(dict[str, object], agreement["0"])
    assert lead0_agreement["cases"] == 3, lead0_agreement
    assert lead0_agreement["both_wet"] == 2, lead0_agreement
    assert lead0_agreement["both_dry"] == 1, lead0_agreement

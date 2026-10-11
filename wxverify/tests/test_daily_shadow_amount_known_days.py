"""§6.6 / §15.3:1411 — amount on known days.

A precip ``--freeze-prepare`` run over four evaluation dates: a rain-nearby
day (observed amount 2.0 mm), a dry day (0 mm), an unknown day, and a
rain-nearby day whose only wet station has partial coverage
(``amount_not_available``, §6.6), plus one earlier training date (D1 minus
one day) so the selector has history to serve from (below). The training
date's record row is from before wet hours were recorded, so its line is
unmatched as ``pre_wet_hours``, carries no amount loss of either kind, and
still gives the selector its history. The selector then serves an amount on
all four evaluation dates, including the unknown one. Five lead-0 cases;
``event_unknown`` once; ``amount_not_available`` once. The amount has a loss
(control and served) on the first two evaluation dates only; the unknown
date has no amount loss and is counted once as ``event_unknown`` (header
``counts.cases["0"]``); the fourth date has no amount loss and is counted
once as ``amount_not_available``. The amount has no verdict (§11.3:
``amount_report``'s own dict has no ``verdict`` key, unlike the
held-out-verdict machinery that exists only for temperature).

Kills mutant M21 (plan :1473) — an unknown day's amount scored as 0 — M21b
(plan :1474) — the same unknown day's served amount loss computed instead
of staying null — and M68 (plan :1521) — a rain-nearby day with no
contributing station given the amount 0.0 instead of
``amount_not_available``.

M21 and M21b are two distinct mutants at two code sites. M21:
:func:`wxverify.forecast.daily_method.observed_amount`'s unknown branch
(returns ``ObservedAmount(None, ...)``, mutated to amount ``0.0``) — its
first failing assertion is the per-day ``observed_amount["amount"]`` loop
below, on D3. M21b: ``daily_shadow._observed``'s
``if line.outcome == "unknown": return None`` branch (mutated to return
``0.0``) leaves the serialized ``observed_amount`` field untouched — the
per-day loop cannot see it — so its first failing assertion is D3's
``served_amount_loss is None`` check below, the one run-level consumer of
``_observed`` gated only on ``line.served.amount is not None and y is not
None`` (``rain_line_json``), never on ``line.scored``/``line.outcome !=
"unknown"`` the way every other consumer is. M68 only touches
``observed_amount``'s ``amount_not_available`` branch; its first failing
assertion is the same per-day loop, on D4.
``tests.test_daily_shadow_batch1_mutants``'s
``test_m21_an_unknown_days_amount_is_not_scored`` stays in place as
supplemental direct coverage of M21b.

One station (``SYN-AK-1``), one feed. ``forecast_samples`` exist for the
feed on every case day, with hand-derived wet-hour patterns (0.25 mm/hour)
that make the F1 replay (``f1_ok``, ``daily_shadow.py:1602-1631``) of the
stored control row's ``total_mm``/``wet_hours`` match exactly, so every
evaluation-date line reaches the scored path (``unmatched is None``,
``control is not None``). One training day (24 hourly station readings at
0.0 mm, zero precip samples, a zero-total ``forecast_of_record`` row at an
app version below ``WET_HOURS_FIRST``) gives the single feed one counted
day in the selector's window (``select_model``, ``daily_method.py:1296-
1306``: with only one feed, ``at_min`` never reaches two models, so any
model with at least one counted day is ``eligible`` — provisional);
``served_rain`` (``daily_method.py:1673-1712``) then serves that feed's
own target-day total as the case's served amount on every one of D1-D4,
including the unknown day D3. The feed's target-day total is replayed from
that day's own ``forecast_samples`` the same way F1 is, so it equals the
stored control total exactly (``_CONTROL_TOTAL_MM``); ``served`` is
``ServedRain(no_eligible=True)`` (amount ``None``) only when there is no
eligible history at all, which is no longer this fixture's case for
D1-D4, so both ``served_amount_loss`` and ``control_amount_loss`` are
useful discriminators on those four dates and both are asserted. The
training day is also scored as its own lead-0 case: the real run's
``first`` case-target bound is ``rain_s0`` (``daily_shadow.py:7083``, the
earliest ``forecast_of_record`` snapshot date for the site's published
generation, defined ``daily_shadow.py:6618-6629``), which is the training
day's own snapshot date, so the training day's own target (itself, at lead
0) falls inside ``[first, last]`` by construction — not through
``select_cases`` (``daily_shadow.py:2074-2101``), which keeps every
(s, L) whose target is in ``[first, last]`` and has no rule of its own
for the training day. The training line's
record row has ``status == "recorded"``, so it still counts toward
``recorded_map``/``training_days`` (``daily_shadow.py:2104``,
``daily_method.py:254``) — the only thing D1-D4's own selector history
reads — even though its own ``control_value`` is unmatched; the header's
lead-0 ``counts.cases["0"]["cases"]`` is therefore 5 (4 evaluation dates +
1 training day), not 4; ``event_unknown`` and ``amount_not_available`` stay
at 1 each because the training day's station event is dry (known, amount
forced to 0.0 — neither reason fires), and ``rain_case_counts`` counts
those two reasons from ``line.outcome``/``line.day.amount.reason``, never
from ``line.unmatched``. Hours and totals are hand-derived against the
production rule constants
(``RAIN_ADEQUATE_HOURS=22``, ``RAIN_PAIR_HUNDREDTHS=20``,
``RAIN_SINGLE_HUNDREDTHS=50``, ``RAIN_MIN_DRY_STATIONS=1``,
``daily_shadow.py:216-219``) and the pure functions they parametrize
(:func:`wxverify.forecast.daily_method.rain_nearby_event`,
:func:`wxverify.forecast.daily_method.observed_amount`,
``daily_method.py:774-894``):

* D1 — 24 hourly readings, 2.0 mm all on hour 23 (hours=24, hundredths=200).
  ``counted`` (hours>=1), ``hours>=22`` -> adequate. ``one = 200>=50`` ->
  rain_nearby. ``observed_amount`` calls ``_contributing`` with
  ``wet_hundredths=RAIN_PAIR_HUNDREDTHS`` (``daily_shadow.py:3539-3544``),
  so it keeps the station at hours>=22, hundredths>=20 -> single
  contributing station -> amount = 200/100 = 2.0 mm.
* D2 — 24 hourly readings, all 0.0 (hours=24, hundredths=0). Adequate,
  ``two``/``one`` both false (0 < 20 and 0 < 50) -> not rain_nearby;
  ``len(adequate)=1 >= min_dry_stations=1`` -> dry -> amount forced to 0.0.
* D3 — no station reading at all. ``counted=()`` -> ``adequate=()`` ->
  not rain_nearby, ``len(adequate)=0 < 1`` -> unknown -> amount None.
* D4 — 5 hourly readings (hours 0-4), 2.0 mm all on hour 4 (hours=5,
  hundredths=200). ``counted`` (hours>=1) but hours=5<22 -> NOT adequate
  (it is ``partial``). ``one = 200>=50`` is still computed over every
  ``counted`` station regardless of hours -> rain_nearby. ``_contributing``
  requires ``hours>=22`` -> empty -> ``amount_not_available``.

The training day has 24 hourly station readings at 0.0 mm (hours=24,
hundredths=0) — the same shape as D2 — so its station event is ``dry`` and
its observed amount is forced to 0.0 under M21, M21b and M68 alike (the
``dry`` branch is untouched by any of the three mutants), which is why its
own line's values are identical whichever mutant is live and its
assertions can sit anywhere in the function without disturbing the "first
failing assertion" claims below.

A recorded control row (``forecast_of_record``, status ``recorded``) exists
for every case day, with a ``total_mm`` distinct from the observed amount
on D1/D2 so a real (non-zero) loss is unambiguous. The training day's own
``forecast_of_record`` row carries a zero ``total_mm`` too, but at an app
version below ``WET_HOURS_FIRST``, so ``control_value`` never reaches the
``total_mm`` comparison at all — the line is unmatched as ``pre_wet_hours``
before any amount is read, and its ``control_amount_loss`` stays null for
that reason alone (§7.2, ``daily_shadow.py:567-575``, ``rain_line_json``'s
``if line.control is not None and line.outcome != "unknown"`` gate,
``daily_shadow.py:4948``). Its ``served_amount_loss`` stays null for an
unrelated reason: a ``recorded`` row with an unmatched control still runs
``select_model``/``served_rain`` in full (``daily_shadow.py:3890-3913``),
but the training day is the earliest date in the fixture, so no earlier
date gives the selector a counted day — ``select_model`` returns
``no_eligible`` (``daily_method.py:1313-1330``), ``served_rain`` returns
``ServedRain(no_eligible=True)`` with amount ``None``
(``daily_method.py:1691-1692``), and ``rain_line_json`` leaves the loss
null because ``line.served.amount is not None`` fails
(``daily_shadow.py:4956``) — never because of the line's own
``pre_wet_hours`` match status. The served amount on
D1/D2 equals the control total exactly (both are replayed from the same
``forecast_samples``), so ``served_amount_loss`` and ``control_amount_loss``
hand-derive to the same literal on each day (``amount_loss(F, y,
under_weight=W_AMOUNT)`` = ``1.5*max(y-F, 0) + max(F-y, 0)``, W_AMOUNT=1.5):
D1 — F=2.5 (10 wet hours * 0.25 mm/hour, ``_CONTROL_WET_HOURS``), y=2.0 ->
``1.5*max(2.0-2.5, 0) + max(2.5-2.0, 0) = 0 + 0.5 = 0.5``. D2 — F=0.5 (2 wet
hours * 0.25 mm/hour), y=0.0 -> ``1.5*max(0.0-0.5, 0) + max(0.5-0.0, 0) = 0
+ 0.5 = 0.5``. D3/D4 have no observed amount (``y`` is ``None``), so both
losses are null regardless of the served/control amount.

Divergences claimed below, each ``<mutant> -> at <assertion>: correct =
<literal>, mutant = <literal>``:

M21 -> at the per-day loop's ``observed["amount"] == _STATION_AMOUNT[day]``
(D3): correct = None, mutant = 0.0 (the plan's literal: "scores it with
0") -- also at ``observed_amount["amount"] is None`` for D3 below.
M21b -> at ``d3_line["served_amount_loss"] is None``: correct =
None, mutant = 1.0 (``_observed`` returning 0.0 for D3 instead of None
makes ``rain_line_json`` compute ``amount_loss(line.served.amount, 0.0,
under_weight=W_AMOUNT)`` = ``amount_loss(1.0, 0.0, under_weight=1.5)`` =
``max(1.0-0.0, 0) = 1.0`` instead of staying null).
M68 -> at the per-day loop's ``observed["amount"] == _STATION_AMOUNT[day]``
(D4): correct = None, mutant = 0.0 (the plan's literal: "a rain-nearby day
with no contributing station given the amount 0.0 instead of
amount_not_available") -- also at ``observed_amount["amount"] is None`` for
D4 below.

Production names used — all public, none underscore-prefixed:
``daily_shadow.main``, ``daily_shadow.RAIN_RULES``, ``daily_shadow.control_value``,
``daily_shadow.RESULT_PREFIX``, ``daily_shadow.package_manifest``,
``daily_shadow.load_site``, ``daily_shadow.load_station_rain``,
``daily_shadow.StationDayStore``, ``daily_shadow.RainEvents``,
``daily_method.AMOUNT_METHOD``, ``timeutil.isoformat_utc``,
``record.resolve_snapshot_utc``, ``simulate.latest_knowable_target``,
``migrations.run_migrations``, ``tz_generations.ensure_published_generation``.
"""

from __future__ import annotations

import contextlib
import io
import json
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
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "b" * 40
_APP_VERSION = "0.16.7"
# Below WET_HOURS_FIRST (daily_shadow.py:93, (0, 16, 0)) so the training
# row's control is unmatched as "pre_wet_hours" (daily_shadow.py:567-575) —
# the same precedent as test_daily_shadow_published_comparison.py:76.
_TRAINING_APP_VERSION = "0.15.0"
_STATION_ID_TEXT = "SYN-AK-1"
_STATION_CREATED_AT = "2026-01-01T00:00:00.000Z"

_D1 = date(2036, 3, 1)
_D2 = _D1 + timedelta(days=1)
_D3 = _D1 + timedelta(days=2)
_D4 = _D1 + timedelta(days=3)
_CASE_DAYS = (_D1, _D2, _D3, _D4)
# One dry training day before D1, so the single feed has one counted day in
# the selector's window and `served` is eligible on every case day (it is
# also its own lead-0 case, see the module docstring).
_TRAINING_DAY = _D1 - timedelta(days=1)

_STATION_EVENT = {
    _D1: "rain_nearby",
    _D2: "dry",
    _D3: "unknown",
    _D4: "rain_nearby",
}
_STATION_AMOUNT = {_D1: 2.0, _D2: 0.0, _D3: None, _D4: None}
_STATION_REASON = {_D1: None, _D2: None, _D3: None, _D4: "amount_not_available"}
# Distinct from the observed amount on D1/D2 so the loss is unambiguously
# non-zero; D3/D4 have no observed amount, so no amount loss is computed
# for them; their totals are still checked by F1 and by the served-amount
# assertion.
_CONTROL_TOTAL_MM = {_D1: 2.5, _D2: 0.5, _D3: 1.0, _D4: 3.0}
# The feed's forecast_samples wet-hour count per case day (0.25 mm/hour),
# chosen so the F1 replay of these samples reproduces _CONTROL_TOTAL_MM
# exactly: 10*0.25=2.5, 2*0.25=0.5, 4*0.25=1.0, 12*0.25=3.0.
_CONTROL_WET_HOURS = {_D1: 10, _D2: 2, _D3: 4, _D4: 12}


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "amount-known-days-site")
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


def _insert_precip_row(
    conn: sqlite3.Connection,
    *,
    site: Site,
    snapshot_date: date,
    total_mm: float,
    wet_hours: float,
    feed_id: int,
    app_version: str = _APP_VERSION,
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
            _precip_daily_quantities(
                total_mm=total_mm, wet_hours=wet_hours, extrema_feed_ids=[feed_id]
            ),
        ),
    )


def _rain_hours(*, wet_count: int) -> list[float]:
    """``wet_count`` hours at 0.25 mm, then zeros, for a 24-hour day."""
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


def _insert_precip_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    values: list[float],
) -> None:
    """24 hourly ``forecast_samples`` for the feed, issued the day before.

    ``valid_at`` falls inside ``local_date`` and the snapshot is read at
    07:00 the same day, so the F1 replay's display-day index for every
    sample is 0 (``display_day_index``) — matching the stored control
    row's ``display_lead=0`` — and ``f1_ok`` (``daily_shadow.py:1602-1631``)
    rebuilds exactly these values for lead 0.
    """
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
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-akd', ?)
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


def _insert_station_hours(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    hour_values: dict[int, float],
) -> None:
    """One ``ok`` reading per listed hour, with a valid running total.

    ``source_raw`` carries ``"<cumulative> mm precipTotal"`` for every hour
    inserted (never the ``unparsed_total`` problem, since every reading's
    ``source_raw`` parses); the running total is non-decreasing (never
    ``counter_reset``). ``hours`` (``station_day_total``) is exactly
    ``len(hour_values)`` — the count of distinct hours present, not the
    count of non-zero readings.
    """
    cumulative = 0.0
    for hour in sorted(hour_values):
        cumulative += hour_values[hour]
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
                hour_values[hour],
                source_raw,
                isoformat_utc(fetched),
            ),
        )


def _build_fixture(tmp_path: Path, *, name: str) -> tuple[Path, Site, int, int]:
    """One feed, one station; D1/D2 full coverage, D3 no reading, D4
    partial; plus one dry training day before D1."""
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, "model-a")
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id=_STATION_ID_TEXT
        )

        d1_hours = {h: 0.0 for h in range(24)}
        d1_hours[23] = 2.0
        _insert_station_hours(
            conn, station_id=station_id, local_date=_D1, hour_values=d1_hours
        )

        d2_hours = {h: 0.0 for h in range(24)}
        _insert_station_hours(
            conn, station_id=station_id, local_date=_D2, hour_values=d2_hours
        )

        # D3: deliberately no station reading at all (unknown day).

        d4_hours = {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 2.0}
        _insert_station_hours(
            conn, station_id=station_id, local_date=_D4, hour_values=d4_hours
        )

        # Training day: dry (24 hourly station readings at 0.0 mm, zero
        # precip samples, a zero-total forecast_of_record row at an app
        # version below WET_HOURS_FIRST) — gives the single feed one
        # counted day in the selector's window, so `served` is eligible on
        # D1-D4, while its own record row is unmatched as "pre_wet_hours".
        training_hours = {h: 0.0 for h in range(24)}
        _insert_station_hours(
            conn,
            station_id=station_id,
            local_date=_TRAINING_DAY,
            hour_values=training_hours,
        )
        _insert_precip_day_samples(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=_TRAINING_DAY,
            values=_rain_hours(wet_count=0),
        )
        _insert_precip_row(
            conn,
            site=site,
            snapshot_date=_TRAINING_DAY,
            total_mm=0.0,
            wet_hours=0.0,
            feed_id=feed_id,
            app_version=_TRAINING_APP_VERSION,
        )

        for day in _CASE_DAYS:
            _insert_precip_day_samples(
                conn,
                site_id=site.site_id,
                feed_id=feed_id,
                local_date=day,
                values=_rain_hours(wet_count=_CONTROL_WET_HOURS[day]),
            )
            _insert_precip_row(
                conn,
                site=site,
                snapshot_date=day,
                total_mm=_CONTROL_TOTAL_MM[day],
                wet_hours=float(_CONTROL_WET_HOURS[day]),
                feed_id=feed_id,
            )

        conn.commit()
    finally:
        conn.close()
    return copy, site, feed_id, station_id


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _set_deterministic_now(copy: Path) -> date:
    """Fix the copy's mtime so main()'s last-complete target is D4 or later."""
    stamp = datetime(_D4.year, _D4.month, _D4.day, 12, tzinfo=UTC) + timedelta(days=2)
    os.utime(copy, (stamp.timestamp(), stamp.timestamp()))
    last_complete = latest_knowable_target("UTC", stamp)
    assert last_complete >= _D4, last_complete
    return last_complete


def _run_main(
    copy: Path, out_dir: Path, *, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[Path, dict[str, object]]:
    """Run ``--freeze-prepare`` and return ``(run_dir, header)``."""
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
# Liveness: the fixture's station readings really produce the four station
# events the oracle assumes, proven through the real loading pipeline
# (load_station_rain -> StationDayStore -> RainEvents), never by calling
# observed_amount/rain_nearby_event directly to derive the test's own
# expected values.
# ---------------------------------------------------------------------------


def test_fixture_station_events_match_the_derived_values(tmp_path: Path) -> None:
    """D1-D4 station events: rain_nearby/2.0, dry/0.0, unknown/None, and
    rain_nearby/None+amount_not_available.
    """
    copy, site, _feed_id, _station_id = _build_fixture(tmp_path, name="events.db")
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
            assert built.amount.amount == _STATION_AMOUNT[day], (day, built.amount)
            assert built.amount.reason == _STATION_REASON[day], (day, built.amount)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# The full run: amount scored on known days, no amount loss on the unknown
# or amount_not_available day, and no verdict for the amount at all.
# ---------------------------------------------------------------------------


def test_amount_scored_on_known_days_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site, _feed_id, _station_id = _build_fixture(tmp_path, name="copy.db")
    last_complete = _set_deterministic_now(copy)
    assert last_complete >= _D4

    out_dir = tmp_path / "out"
    run_dir, header = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )
    lines = _read_lines_jsonl(run_dir)

    for day in _CASE_DAYS:
        line = _find_line(lines, day)
        # Proof the scored path is reached: the F1 replay of the day's
        # forecast_samples matches the stored control row, so the line is
        # never discarded as f1_mismatch (daily_shadow.py:3813-3817).
        assert line["unmatched"] is None, (day, line)
        assert line["control"] is not None, (day, line)
        observed = cast("dict[str, object]", line["observed_amount"])
        assert observed["amount"] == _STATION_AMOUNT[day], (day, observed)
        assert observed["reason"] == _STATION_REASON[day], (day, observed)
        # Liveness: the training day gives the single feed an eligible
        # selection, so every case day — including the unknown day D3 —
        # has a served amount (daily_method.served_rain), hand-derived per
        # date as _CONTROL_TOTAL_MM (D1 2.5, D2 0.5, D3 1.0, D4 3.0): the
        # feed's own target-day total, replayed from that day's
        # forecast_samples the same way the control row's F1 replay is.
        served = cast("dict[str, object]", line["served"])
        assert served["amount"] == _CONTROL_TOTAL_MM[day], (day, served)

    d1_line = _find_line(lines, _D1)
    d2_line = _find_line(lines, _D2)
    d3_line = _find_line(lines, _D3)
    d4_line = _find_line(lines, _D4)

    d3_observed = cast("dict[str, object]", d3_line["observed_amount"])
    d4_observed = cast("dict[str, object]", d4_line["observed_amount"])

    # M21 -> at the per-day loop's observed["amount"] == _STATION_AMOUNT[day]
    # (D3): correct = None, mutant = 0.0 (the unknown day's amount scored
    # as 0) -- also at observed_amount["amount"] is None for D3 below.
    assert d3_observed["amount"] is None, d3_observed
    assert d3_observed["reason"] is None, d3_observed
    assert d3_observed["contributing"] == [], d3_observed

    # M68 -> at the per-day loop's observed["amount"] == _STATION_AMOUNT[day]
    # (D4): correct = None, mutant = 0.0 (a rain-nearby day with no
    # contributing station given the amount 0.0 instead of
    # amount_not_available) -- also at observed_amount["amount"] is None for
    # D4 below.
    assert d4_observed["amount"] is None, d4_observed
    assert d4_observed["reason"] == "amount_not_available", d4_observed
    assert d4_observed["contributing"] == [], d4_observed

    # The amount has a loss on D1/D2 only, and no loss (null) on D3/D4 —
    # §15.3:1411. F (the served/control amount, both replayed from the
    # same forecast_samples) is _CONTROL_TOTAL_MM: D1 2.5, D2 0.5.
    # amount_loss(F, y, under_weight=1.5) = 1.5*max(y-F, 0) + max(F-y, 0):
    # D1: F=2.5, y=2.0 -> 1.5*max(2.0-2.5, 0) + max(2.5-2.0, 0) = 0.5.
    # D2: F=0.5, y=0.0 -> 1.5*max(0.0-0.5, 0) + max(0.5-0.0, 0) = 0.5.
    assert d1_line["control_amount_loss"] == 0.5, d1_line
    assert d2_line["control_amount_loss"] == 0.5, d2_line
    assert d3_line["control_amount_loss"] is None, d3_line
    assert d4_line["control_amount_loss"] is None, d4_line

    # M21b -> at served_amount_loss is None for D3: correct = None,
    # mutant = 1.0 (daily_shadow._observed returning 0.0 instead of None
    # for the unknown day). served F equals control F on D1/D2 (same
    # replay), so served_amount_loss hand-derives to the same 0.5 literal;
    # D3/D4 have no observed amount, so both stay null.
    assert d1_line["served_amount_loss"] == 0.5, d1_line
    assert d2_line["served_amount_loss"] == 0.5, d2_line
    assert d3_line["served_amount_loss"] is None, d3_line
    assert d4_line["served_amount_loss"] is None, d4_line

    # The header's per-lead case counts for lead 0 (rain_case_counts,
    # daily_shadow.py ~:4810-4865, landed at header["counts"]["cases"],
    # daily_shadow.py:6763) show 5 cases (4 evaluation dates + 1 training
    # day, since the training day's own target date is also a lead-0 case
    # — the run's `first` bound is rain_s0, the training day's own
    # snapshot date, daily_shadow.py:7083/6618-6629), and exactly one
    # event_unknown (D3) and exactly one amount_not_available (D4): the
    # training day's station event is dry, so it adds to neither (counted
    # from line.outcome/line.day.amount.reason, never from line.unmatched).
    counts = cast("dict[str, object]", header["counts"])
    cases = cast("dict[str, object]", counts["cases"])
    lead0 = cast("dict[str, object]", cases["0"])
    assert lead0["cases"] == 5, lead0
    assert lead0["event_unknown"] == 1, lead0
    assert lead0["amount_not_available"] == 1, lead0

    # The training line itself: unmatched as "pre_wet_hours" (its app
    # version is below WET_HOURS_FIRST, daily_shadow.py:567-575), so
    # control_value never reaches the total_mm comparison and
    # control_amount_loss stays null for that reason alone
    # (rain_line_json's `if line.control is not None and line.outcome !=
    # "unknown"` gate, daily_shadow.py:4948). served_amount_loss stays
    # null for an unrelated reason: a recorded row with an unmatched
    # control still runs select_model/served_rain in full
    # (daily_shadow.py:3890-3913), but the training day is the earliest
    # date in the fixture, so select_model returns no_eligible
    # (daily_method.py:1313-1330), served_rain returns
    # ServedRain(no_eligible=True) with amount None
    # (daily_method.py:1691-1692), and rain_line_json leaves the loss null
    # because `line.served.amount is not None` fails
    # (daily_shadow.py:4956) -- never because of pre_wet_hours. Placed
    # last so it cannot shift any of the "first failing assertion" claims
    # above: its own observed amount is 0.0 (dry), untouched by M21/M21b/
    # M68, and its served amount is None (no eligible history before the
    # first case), so none of these three assertions is ever the first to
    # fail under those mutants.
    training_line = _find_line(lines, _TRAINING_DAY)
    assert training_line["unmatched"] == "pre_wet_hours", training_line
    assert training_line["control_amount_loss"] is None, training_line
    assert training_line["served_amount_loss"] is None, training_line

    # The header's unmatched tally (daily_shadow.py:6766, rain_counts) also
    # records the training line once, by its own reason, at lead 0 — the
    # one line among these five that is not "matched".
    unmatched_counts = cast("dict[str, object]", header["unmatched"])
    unmatched_rain = cast("dict[str, object]", unmatched_counts["rain"])
    unmatched_pre_wet_hours = cast("dict[str, object]", unmatched_rain["pre_wet_hours"])
    assert unmatched_pre_wet_hours["0"] == 1, unmatched_pre_wet_hours

    # The amount has no verdict (§11.3): decisions.amount (amount_report's
    # own dict) carries no "verdict" key at all — that machinery exists
    # only for the held-out temperature path (verification_verdicts /
    # build_verdict in wxverify/monitor.py), never for rain.
    decisions = cast("dict[str, object]", header["decisions"])
    amount_report_dict = cast("dict[str, object]", decisions["amount"])
    assert "verdict" not in amount_report_dict, amount_report_dict

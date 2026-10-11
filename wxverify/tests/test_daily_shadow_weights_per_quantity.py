"""§15.3 "Weights per quantity" — the high is scored with w_high, not w_amount.

A temperature ``--heldout`` run and a precip ``--freeze-prepare`` run, each
over the same two lead-0 target dates (D1, D2), hand-crafted so each line's
``control``/``truth`` (temperature) or ``control.amount``/observed amount
(precip) are exactly the plan's literals (plan :1436):

* D1 — high 20.0C vs observed (published truth) 22.0C; amount 2.0 mm vs
  observed amount 4.0 mm (a rain-nearby day, two stations at 4.5 and
  3.5 mm).
* D2 — high 22.0C vs observed 20.0C; amount 4.0 mm vs observed 2.0 mm (two
  stations at 2.5 and 1.5 mm).

Confirmed from :func:`daily_shadow.control_value` (daily_shadow.py:549-621,
via :func:`daily_shadow.narrow_policy` 462-482 and
:func:`daily_shadow.narrow_daily_quantities` 485-522): a temperature record
row's ``control.high``/``control.low`` and a precip record row's
``control.amount`` come straight from the row's own ``daily_quantities``
JSON (``displayed.high_c``/``displayed.low_c``/``displayed.total_mm``), with
no other transform, provided ``f1_ok`` (1602-1631) confirms the row's
``extrema_feed_ids`` replay to those same stored values — which is why every
record row here is paired with 24 hourly ``forecast_samples`` at a constant
value (temperature) or a wet-hour count (precip) that reproduces the stored
total exactly. :func:`daily_shadow.temperature_lines` (2600-2721) is the one
place a ``TempLine``'s ``control``/``truth`` are set: ``stored = control.high
if quantity == "temperature_high" else control.low`` (2639) and ``observed =
published.value`` (2656), where ``published`` comes from
:func:`daily_shadow.load_published_truth` (1763-1788) reading the
``daily_truth`` table — so the two observed values 22.0C/20.0C are exactly
the ``daily_truth.value`` rows this fixture inserts for ``temperature_high``
at D1/D2. For precip, :func:`daily_shadow.rain_line_json`'s
``observed_amount`` (4937-5008, the ``line.day.amount`` field) comes from
:func:`daily_method.observed_amount` (daily_method.py:862-894): with two
contributing stations, the mean of the top two hundredths — D1
``(450+350)/200 = 4.0`` mm, D2 ``(250+150)/200 = 2.0`` mm — independently
matching the plan's own stated observed amounts.

The weight under test: :meth:`daily_shadow.TempParams.weights`
(daily_shadow.py:2317-2321) is ``if quantity == "temperature_high": return
self.w_high, 1.0; return self.low_under, self.low_over`` — the exact mutation
site named by mutant M39 (plan :1501, "the high scored with w_amount (one
weight, 1.5, for both quantities)"), which replaces ``self.w_high`` (1.0,
``W_HIGH``) with ``W_AMOUNT`` (1.5) in that first branch only, leaving the
``1.0`` second element (the high's over_weight) untouched.
:func:`daily_shadow.temperature_loss` (2377-2384) calls
``daily_method.asymmetric_loss(forecast, observed, under_weight=under,
over_weight=over)``, and :func:`daily_method.asymmetric_loss`
(daily_method.py:75-81) is ``under_weight * max(observed - forecast, 0) +
over_weight * max(forecast - observed, 0)``. :func:`daily_shadow.
temperature_line_json` (3324-3355) computes each line's
``control_loss = temperature_loss(line.quantity, line.control, line.truth)``
(forecast = control, observed = truth):

* D1 (correct, under=w_high=1.0, over=1.0): ``1.0*max(22.0-20.0,0) +
  1.0*max(20.0-22.0,0) = 2.0 + 0.0 = 2.0``. Under M39 (under=W_AMOUNT=1.5,
  over still 1.0): ``1.5*max(22.0-20.0,0) + 1.0*max(20.0-22.0,0) = 3.0 +
  0.0 = 3.0`` — the plan's own stated mutant literal.
* D2 (correct): ``1.0*max(20.0-22.0,0) + 1.0*max(22.0-20.0,0) = 0.0 + 2.0 =
  2.0``. Under M39 the under_weight multiplies the (now zero) first term,
  so D2's loss is unchanged at 2.0 — D2 is deliberately a paired positive
  that confirms the loss formula on the opposite sign, not a second kill of
  M39; only D1's assertion distinguishes the two literals.

Divergence claimed below, in the required form:

M39 -> at ``d1_high["control_loss"] == 2.0``: correct = 2.0, mutant = 3.0 —
forced by :meth:`TempParams.weights` using ``self.w_high`` (1.0) as the
high's under_weight against D1's fixture values (control 20.0, truth 22.0,
observed > forecast so the under_weight branch is the one that fires).

The precip amount's own weight, :data:`daily_shadow.W_AMOUNT` (1.5), is a
separate module constant from :data:`daily_shadow.W_HIGH` (1.0) and is never
read by :meth:`TempParams.weights` under the unmutated code — the precip
assertions below are a paired confirmation that the amount's own, unrelated
weight produces the plan's stated losses (3.0, 2.0) via
:func:`daily_shadow.amount_loss` (daily_shadow.py:4097-4099, ``under_weight *
max(observed - forecast, 0) + max(forecast - observed, 0)``, under_weight
always ``W_AMOUNT``, never swapped by M39):

* D1: ``1.5*max(4.0-2.0,0) + max(2.0-4.0,0) = 3.0 + 0.0 = 3.0``.
* D2: ``1.5*max(2.0-4.0,0) + max(4.0-2.0,0) = 0.0 + 2.0 = 2.0``.

Every value is exact in binary floating point (0.25 mm/hour steps, 4.5/3.5
and 2.5/1.5 mm halves, differences of whole or half degrees), so every
assertion below uses exact ``==`` rather than ``pytest.approx``.

One site, one feed, two synthetic stations (``SYN-RAIN-A``,
``SYN-RAIN-B``) — no real station IDs, coordinates or place names. No
training-history fixture is needed: with zero seeded training pairs, every
composer's regime for temperature falls back to ``"no_eligible"``
(:func:`daily_shadow.load_published_truth`'s caller,
``_temperature_heldout``, daily_shadow.py:6452-6612, mirroring the
precedent in ``tests/test_daily_shadow_heldout_wiring.py``'s own
``_make_fixture`` docstring) and the precip selector likewise returns
``ServedRain(no_eligible=True)`` (daily_method.py:1691-1692, mirroring
``tests/test_daily_shadow_amount_known_days.py``'s own training-day line) —
neither path is read by this test's assertions, which are the control and
the observed/published truth only.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import cast

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.record import resolve_snapshot_utc

_APP_VERSION = "0.16.7"

_D1 = date(2036, 5, 1)
_D2 = date(2036, 5, 2)
_DAYS = (_D1, _D2)

_TEMP_CONTROL: dict[date, float] = {_D1: 20.0, _D2: 22.0}
_TEMP_TRUTH: dict[date, float] = {_D1: 22.0, _D2: 20.0}
_TEMP_CONTROL_LOSS: dict[date, float] = {_D1: 2.0, _D2: 2.0}

_AMOUNT_CONTROL: dict[date, float] = {_D1: 2.0, _D2: 4.0}
_AMOUNT_WET_HOURS: dict[date, int] = {_D1: 8, _D2: 16}  # 0.25 mm/hour * count
_STATION_A_MM: dict[date, float] = {_D1: 4.5, _D2: 2.5}
_STATION_B_MM: dict[date, float] = {_D1: 3.5, _D2: 1.5}
_AMOUNT_OBSERVED: dict[date, float] = {_D1: 4.0, _D2: 2.0}
_AMOUNT_CONTROL_LOSS: dict[date, float] = {_D1: 3.0, _D2: 2.0}


def _temperature_daily_quantities(
    *, high_c: float, low_c: float, extrema_feed_ids: list[int]
) -> str:
    return json.dumps(
        {
            "displayed": {
                "high_c": high_c,
                "low_c": low_c,
                "extrema_feed_ids": extrema_feed_ids,
            }
        }
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


def _insert_temperature_row(
    conn: sqlite3.Connection,
    *,
    site: Site,
    snapshot_date: date,
    high_c: float,
    low_c: float,
    feed_id: int,
) -> None:
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", snapshot_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'temperature', 0, 'recorded',
                'on_time', '{"rain_threshold_mm": 0.2}', 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            snapshot_utc,
            snapshot_date.isoformat(),
            _APP_VERSION,
            _temperature_daily_quantities(
                high_c=high_c, low_c=low_c, extrema_feed_ids=[feed_id]
            ),
        ),
    )


def _insert_precip_row(
    conn: sqlite3.Connection,
    *,
    site: Site,
    snapshot_date: date,
    total_mm: float,
    wet_hours: float,
    feed_id: int,
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
            _APP_VERSION,
            _precip_daily_quantities(
                total_mm=total_mm, wet_hours=wet_hours, extrema_feed_ids=[feed_id]
            ),
        ),
    )


def _insert_hourly_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    local_date: date,
    values: list[float],
) -> None:
    """24 hourly ``forecast_samples``, issued the day before at 06:00 UTC.

    ``valid_at`` falls inside ``local_date`` and the snapshot is read at
    07:00 the same UTC day, so the F1 replay's display-day index for every
    sample is 0 (``display_day_index``) -- matching the stored control
    row's ``display_lead=0`` -- and ``f1_ok`` (daily_shadow.py:1602-1631)
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
            VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-wpq', ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


def _rain_hours(*, wet_count: int) -> list[float]:
    """``wet_count`` hours at 0.25 mm, then zeros, for a 24-hour day."""
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


def _insert_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    local_date: date,
    quantity: str,
    value: float,
) -> None:
    day_start = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 0, tzinfo=UTC)
    )
    day_end = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 23, tzinfo=UTC)
    )
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             stale, tz_generation_id)
        VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 'UTC', 0, ?)
        """,
        (
            site_id,
            local_date.isoformat(),
            quantity,
            value,
            day_start,
            day_end,
            tz_generation_id,
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
    conn: sqlite3.Connection, *, station_id: int, local_date: date, total_mm: float
) -> None:
    """24 hourly ``ok`` readings, the whole day's total on hour 23.

    Matches ``tests/test_daily_shadow_amount_known_days.py``'s own
    ``_insert_station_hours`` fixture shape (one spike hour, 23 zero
    hours), so ``hours`` (``station_day_total``) is 24 -- at or above
    ``RAIN_ADEQUATE_HOURS`` (22, daily_shadow.py:216) -- and the station is
    ``counted``/adequate with hundredths exactly ``round(total_mm * 100)``.
    """
    hour_values = {hour: 0.0 for hour in range(24)}
    hour_values[23] = total_mm
    for hour in sorted(hour_values):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        fetched = valid + timedelta(minutes=5)
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
                f"{round(hour_values[hour], 10)} mm precipTotal",
                isoformat_utc(fetched),
            ),
        )


def _build_fixture() -> tuple[sqlite3.Connection, Site]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "weights-per-quantity-site")
    ensure_published_generation(conn, site_id)
    feed_id = asof_make_real_feed(conn, "model-a")
    site = daily_shadow.load_site(conn, site_id=site_id)

    for day in _DAYS:
        high_c = _TEMP_CONTROL[day]
        _insert_temperature_row(
            conn,
            site=site,
            snapshot_date=day,
            high_c=high_c,
            low_c=high_c,
            feed_id=feed_id,
        )
        _insert_hourly_samples(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            local_date=day,
            values=[high_c] * 24,
        )
        for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
            _insert_daily_truth(
                conn,
                site_id=site_id,
                tz_generation_id=site.generation_id,
                local_date=day,
                quantity=quantity,
                value=_TEMP_TRUTH[day],
            )

    station_a = _make_station(conn, site_id=site_id, pws_station_id="SYN-RAIN-A")
    station_b = _make_station(conn, site_id=site_id, pws_station_id="SYN-RAIN-B")
    for day in _DAYS:
        wet_count = _AMOUNT_WET_HOURS[day]
        _insert_precip_row(
            conn,
            site=site,
            snapshot_date=day,
            total_mm=_AMOUNT_CONTROL[day],
            wet_hours=float(wet_count),
            feed_id=feed_id,
        )
        _insert_hourly_samples(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="precip",
            local_date=day,
            values=_rain_hours(wet_count=wet_count),
        )
        _insert_station_day(
            conn, station_id=station_a, local_date=day, total_mm=_STATION_A_MM[day]
        )
        _insert_station_day(
            conn, station_id=station_b, local_date=day, total_mm=_STATION_B_MM[day]
        )

    conn.commit()
    return conn, site


def _run_temperature_heldout(conn: sqlite3.Connection, site: Site) -> dict[str, bytes]:
    note = daily_shadow.FreezeNote(
        site_id=site.site_id,
        first_h_date=_D1,
        excluded_h_dates=(),
        station_set=(),
        db_import=None,
        manifest={},
        constants={},
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )
    return daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="heldout",
        first=_D1,
        last=_D2,
        h_first=_D1,
        last_complete=_D2,
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        house_station=note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )


def _run_precip_freeze_prepare(
    conn: sqlite3.Connection, site: Site
) -> dict[str, bytes]:
    return daily_shadow.run(
        conn,
        site=site,
        variable="precip",
        mode="freeze-prepare",
        first=_D1,
        last=_D2,
        h_first=None,
        last_complete=_D2,
        excluded_h_dates=(),
        t2_last=_D2,
        note=None,
        house_station=None,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )


def _read_lines(files: dict[str, bytes]) -> list[dict[str, object]]:
    text = files["lines.jsonl"].decode("utf-8")
    return [json.loads(entry) for entry in text.splitlines() if entry]


def test_temperature_high_weighted_by_w_high() -> None:
    """§15.3: the high's control loss uses ``w_high`` (1.0), never ``w_amount``.

    Kills M39 (plan :1501) at D1's ``control_loss`` assertion -- see the
    module docstring for the full derivation and the divergence claim.
    """
    conn, site = _build_fixture()
    try:
        files = _run_temperature_heldout(conn, site)
    finally:
        conn.close()
    lines = _read_lines(files)

    for day in _DAYS:
        matches = [
            line
            for line in lines
            if line["target"] == day.isoformat()
            and line["lead"] == 0
            and line["quantity"] == "temperature_high"
        ]
        assert len(matches) == 1, (day, lines)
        line = matches[0]
        # Fixture precondition: the line is scored (F1 matched, not
        # discarded) before any loss value means anything.
        assert line["unmatched"] is None, (day, line)
        assert line["control"] is not None, (day, line)
        assert line["truth"] is not None, (day, line)

        assert line["control"] == _TEMP_CONTROL[day], (day, line)
        assert line["truth"] == _TEMP_TRUTH[day], (day, line)
        # M39 -> at this assertion for D1: correct = 2.0, mutant = 3.0.
        # D2's loss (also 2.0) is unaffected by M39 -- see the module
        # docstring -- so it is a paired positive, not a second kill.
        assert line["control_loss"] == _TEMP_CONTROL_LOSS[day], (day, line)


def test_precip_amount_weighted_by_w_amount() -> None:
    """§15.3: the amount's control loss uses ``w_amount`` (1.5), unaffected by M39.

    A paired confirmation that the amount's own weight (a separate
    constant from the high's, :data:`daily_shadow.W_AMOUNT` vs
    :data:`daily_shadow.W_HIGH`) produces the plan's stated losses -- see
    the module docstring for the full derivation.
    """
    conn, site = _build_fixture()
    try:
        files = _run_precip_freeze_prepare(conn, site)
    finally:
        conn.close()
    lines = _read_lines(files)

    for day in _DAYS:
        matches = [
            line
            for line in lines
            if line["target"] == day.isoformat() and line["lead"] == 0
        ]
        assert len(matches) == 1, (day, lines)
        line = matches[0]
        # Fixture precondition: the line is scored before any loss value
        # means anything.
        assert line["unmatched"] is None, (day, line)
        control = line["control"]
        assert control is not None, (day, line)
        control_dict = cast("dict[str, object]", control)
        observed = line["observed_amount"]
        assert observed is not None, (day, line)
        observed_dict = cast("dict[str, object]", observed)
        assert observed_dict["amount"] is not None, (day, line)

        assert control_dict["amount"] == _AMOUNT_CONTROL[day], (day, line)
        assert observed_dict["amount"] == _AMOUNT_OBSERVED[day], (day, line)
        assert line["control_amount_loss"] == _AMOUNT_CONTROL_LOSS[day], (day, line)

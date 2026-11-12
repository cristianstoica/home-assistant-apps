"""Pure core of the Open-Meteo archive fill: ``collection/om_archive.py``'s
``served_run``/``stitch``/``day_cells``, and ``feeds/open_meteo.py``'s
``run_samples``/``snap_run`` wrappers (plan §4, T1-T3).

No database, no network, no clock: every time is passed in explicitly.
Synthetic data only; the clock-change test uses the stand-in zone
``America/Denver`` (already used in ``api/app.py``), never a real site zone.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from wxverify.collection.om_archive import (
    ALLOWANCE_MINUTES,
    Cell,
    day_cells,
    older_run,
    served_run,
    stitch,
)
from wxverify.core.timeutil import local_day_slots
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.open_meteo import run_samples, snap_run
from wxverify.feeds.seam import NormalizedSample


def _sample(
    *, model: str, variable: str, issued_at: str, valid_at: str, value: float
) -> NormalizedSample:
    return NormalizedSample(
        model=model,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=1,
        value=value,
        source_raw=str(value),
        model_run_id=f"{model}:{issued_at}",
    )


# ---------------------------------------------------------------------------
# T1 -- served_run: table of (model, fetch time) -> R1, including the
# boundary inclusive at R + A and the run just before it.
# Catches a fresher run than the add-on's own download would have held.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("model", "fetch_time", "expected_r1"),
    [
        # icon_eu: 3h cadence, allowance 240 min. Exactly at R + A: included.
        (
            "icon_eu",
            datetime(2026, 6, 2, 4, 0, tzinfo=UTC),
            datetime(2026, 6, 2, 0, 0, tzinfo=UTC),
        ),
        # One minute before R + A: the run one cadence before R.
        (
            "icon_eu",
            datetime(2026, 6, 2, 3, 59, tzinfo=UTC),
            datetime(2026, 6, 1, 21, 0, tzinfo=UTC),
        ),
        # gem_global: 12h cadence, allowance 420 min. Exactly at R + A.
        (
            "gem_global",
            datetime(2026, 6, 2, 7, 0, tzinfo=UTC),
            datetime(2026, 6, 2, 0, 0, tzinfo=UTC),
        ),
        # One minute before: the run one cadence (12h) before R.
        (
            "gem_global",
            datetime(2026, 6, 2, 6, 59, tzinfo=UTC),
            datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        ),
    ],
)
def test_t1_served_run_table(
    model: str, fetch_time: datetime, expected_r1: datetime
) -> None:
    assert served_run(model, fetch_time) == expected_r1


@pytest.mark.parametrize("model", sorted(ALLOWANCE_MINUTES))
def test_t1_served_run_never_fresher_than_the_allowance(model: str) -> None:
    """Property: R1 + A[model] <= fetch_at always, for every model and every
    hour of a day. Catches an off-by-one in the floor/inclusive boundary
    that would pick a run still within its allowance (a fresher forecast
    than the add-on's own download would have held)."""
    allowance = timedelta(minutes=ALLOWANCE_MINUTES[model])
    base = datetime(2026, 6, 2, 0, 0, tzinfo=UTC)
    for hour in range(24):
        fetch_at = base + timedelta(hours=hour)
        r1 = served_run(model, fetch_at)
        assert r1 + allowance <= fetch_at, (model, fetch_at)


def test_t1_older_run_is_one_cadence_before() -> None:
    r1 = datetime(2026, 6, 2, 0, 0, tzinfo=UTC)
    assert older_run("icon_eu", r1) == r1 - timedelta(hours=3)
    assert older_run("gem_global", r1) == r1 - timedelta(hours=12)


# ---------------------------------------------------------------------------
# T2 -- stitch: a short newer run plus an older long run fills the hours
# past the short run's end from the older one; a key both runs hold takes
# the newer value; a run with no values (never generated past where it
# ends) contributes nothing; an all-null reply is no run, never zeros.
# Catches the reply's two-run shape (the live reply's own stitching).
# ---------------------------------------------------------------------------


def _hourly_series(
    *,
    model: str,
    variable: str,
    issued_at: str,
    start: datetime,
    hours: int,
    value: float,
) -> list[NormalizedSample]:
    out: list[NormalizedSample] = []
    for i in range(hours):
        valid_at = (start + timedelta(hours=i)).isoformat().replace("+00:00", "Z")
        out.append(
            _sample(
                model=model,
                variable=variable,
                issued_at=issued_at,
                valid_at=valid_at,
                value=value,
            )
        )
    return out


def test_t2_stitch_prefers_newer_and_falls_back_to_older_past_its_end() -> None:
    fetch_at = datetime(2026, 6, 2, 6, 30, tzinfo=UTC)
    max_lead_hours = 10  # window: [06:00, 16:00)

    # Older, long run: issued 00:00Z, covers 01:00..12:00 (value 1.0).
    older = _hourly_series(
        model="icon_eu",
        variable="temperature",
        issued_at="2026-06-02T00:00:00Z",
        start=datetime(2026, 6, 2, 1, 0, tzinfo=UTC),
        hours=12,
        value=1.0,
    )
    # Newer, short run: issued 06:00Z, covers 07:00..09:00 only (value 2.0).
    # Nothing is generated past 09:00 -- "trailing nulls end a run" is
    # modelled as no sample at all, never a zero.
    newer = _hourly_series(
        model="icon_eu",
        variable="temperature",
        issued_at="2026-06-02T06:00:00Z",
        start=datetime(2026, 6, 2, 7, 0, tzinfo=UTC),
        hours=3,
        value=2.0,
    )

    stitched = stitch([newer, older], fetch_at, max_lead_hours)
    by_hour = {s.valid_at: s.value for s in stitched}

    # Before the newer run starts: only the older run has it.
    assert by_hour["2026-06-02T06:00:00Z"] == 1.0
    # The overlap (07:00-09:00): the newer run wins.
    assert by_hour["2026-06-02T07:00:00Z"] == 2.0
    assert by_hour["2026-06-02T08:00:00Z"] == 2.0
    assert by_hour["2026-06-02T09:00:00Z"] == 2.0
    # Past the short run's end: falls back to the older run.
    assert by_hour["2026-06-02T10:00:00Z"] == 1.0
    assert by_hour["2026-06-02T11:00:00Z"] == 1.0
    assert by_hour["2026-06-02T12:00:00Z"] == 1.0
    # Nothing past the older run's own end (12:00) and nothing before the
    # request window (06:00).
    assert "2026-06-02T13:00:00Z" not in by_hour
    assert "2026-06-02T05:00:00Z" not in by_hour


def test_t2_an_empty_reply_contributes_nothing_never_a_zero() -> None:
    """An all-null reply (after conversion, no samples at all) is no run:
    it must not be read as a run of zeros."""
    fetch_at = datetime(2026, 6, 2, 6, 30, tzinfo=UTC)
    assert stitch([[]], fetch_at, 10) == []
    assert stitch([], fetch_at, 10) == []


# ---------------------------------------------------------------------------
# T3 -- conversion and mapping: run_samples converts exactly as the
# adapter does (rain shifted a label earlier, temperature unshifted, wind
# km/h -> m/s, JMA's trace negative rain floored to 0); day_cells maps a
# clock-change day to 23 or 25 hours at the right day ahead.
# Catches a fill-side conversion that differs from the live adapter's.
# ---------------------------------------------------------------------------


def test_t3_run_samples_matches_the_adapters_own_conversion() -> None:
    issued_at = "2026-06-02T00:00:00Z"
    data = {
        "hourly": {
            "time": ["2026-06-02T13:00"],
            "temperature_2m": [10.0],
            "precipitation": [1.5],
            "wind_speed_10m": [36.0],
        }
    }
    samples = run_samples("icon_eu", issued_at, data)
    by_variable = {s.variable: s for s in samples}

    # Temperature keeps the provider's own hour label.
    assert by_variable["temperature"].valid_at == "2026-06-02T13:00:00Z"
    assert by_variable["temperature"].value == 10.0

    # Rain is moved to the START of its hour: Open-Meteo's 13:00 label (the
    # hour ending at 13:00) becomes 12:00.
    assert by_variable["precip"].valid_at == "2026-06-02T12:00:00Z"
    assert by_variable["precip"].value == 1.5

    # Wind km/h -> m/s: 36 km/h is exactly 10 m/s.
    assert by_variable["wind"].value == kmh_to_ms(36.0) == 10.0


def test_t3_jma_trace_negative_rain_floors_to_zero() -> None:
    issued_at = "2026-06-02T00:00:00Z"
    data = {
        "hourly": {
            "time": ["2026-06-02T13:00"],
            "precipitation": [-0.05],
        }
    }
    samples = run_samples("jma_gsm", issued_at, data)
    assert len(samples) == 1
    assert samples[0].variable == "precip"
    assert samples[0].value == 0.0


def test_t3_snap_run_matches_the_adapters_own_estimate() -> None:
    fetch_time = "2026-06-02T07:30:00Z"
    assert snap_run("gem_global", fetch_time) == "2026-06-02T00:00:00Z"
    assert snap_run("icon_eu", fetch_time) == "2026-06-02T06:00:00Z"


@pytest.mark.parametrize(
    ("local_date", "expected_hours"),
    [
        (date(2026, 3, 8), 23),  # spring forward: a 23-hour local day
        (date(2026, 11, 1), 25),  # fall back: a 25-hour local day
    ],
)
def test_t3_day_cells_clock_change_day_gets_23_or_25_hours(
    local_date: date, expected_hours: int
) -> None:
    zone = ZoneInfo("America/Denver")
    start, end, _ = local_day_slots(local_date, "America/Denver")
    download_date = local_date - timedelta(days=2)
    issued_at = "2026-06-01T00:00:00Z"  # irrelevant to day_cells; only valid_at matters

    samples: list[NormalizedSample] = []
    instant = start
    while instant < end:
        samples.append(
            _sample(
                model="icon_eu",
                variable="temperature",
                issued_at=issued_at,
                valid_at=instant.isoformat().replace("+00:00", "Z"),
                value=1.0,
            )
        )
        instant += timedelta(hours=1)

    cells = day_cells(samples, download_date, zone)
    key = Cell("temperature", 2, local_date)
    assert key in cells
    assert len(cells[key]) == expected_hours
    # Ordered by hour.
    valid_ats = [s.valid_at for s in cells[key]]
    assert valid_ats == sorted(valid_ats)


def test_t3_day_cells_keeps_only_day_ahead_0_through_max() -> None:
    zone = ZoneInfo("UTC")
    download_date = date(2026, 6, 2)
    samples = [
        _sample(
            model="icon_eu",
            variable="temperature",
            issued_at="2026-06-02T00:00:00Z",
            valid_at="2026-06-02T01:00:00Z",  # day ahead 0
            value=1.0,
        ),
        _sample(
            model="icon_eu",
            variable="temperature",
            issued_at="2026-06-02T00:00:00Z",
            valid_at="2026-06-10T01:00:00Z",  # day ahead 8: dropped
            value=2.0,
        ),
    ]
    cells = day_cells(samples, download_date, zone)
    assert Cell("temperature", 0, date(2026, 6, 2)) in cells
    assert Cell("temperature", 8, date(2026, 6, 10)) not in cells

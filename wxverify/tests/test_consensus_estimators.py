"""Part A per-variable estimator oracles (plan section A6).

Covers the pure estimator functions (`_p90`, `_low_trim`, `_mean`), the
allowlist dispatch inside `compute_consensus`, the wind anti-regression
discriminator that proves p90 is actually wired (not silently routed back to
median/MAD), the temperature no-op regression pin, the precip gate pin, and
one `materialize_consensus` integration path over a real tmp SQLite DB.

Every expected value below is hand-derived from the arithmetic in the plan
(`docs/plans/2026-07-21-wxverify-baseline-estimators-and-migration.md` §A6),
not read off the implementation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.scoring.consensus import (
    StationReading,
    _low_trim,
    _mean,
    _p90,
    compute_consensus,
    insert_station_observation,
)


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


# ---------------------------------------------------------------------------
# Pure estimator oracles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        # k=(10-1)*0.9=8.1 -> between s[8]=9 and s[9]=10 -> 9+(10-9)*0.1=9.1
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 9.1),
        ([5.0], 5.0),
        ([4, 4, 4], 4.0),
    ],
)
def test_p90_pure(values: list[float], expected: float) -> None:
    assert _p90(values) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1, 2, 3, 4], 3.0),  # drop lowest(1) -> mean(2,3,4)=3.0
        ([2, 8], 8.0),  # drop lowest(2) -> mean(8)=8.0
        ([5.0], 5.0),  # n<2 -> mean of all
    ],
)
def test_low_trim_pure(values: list[float], expected: float) -> None:
    assert _low_trim(values) == pytest.approx(expected)


def test_mean_pure() -> None:
    assert _mean([2.0, 4.0, 6.0]) == pytest.approx(4.0)


# ---------------------------------------------------------------------------
# Dispatch oracles: wind -> p90
# ---------------------------------------------------------------------------


def _wind_readings(values: list[float]) -> list[StationReading]:
    return [
        StationReading(
            station_id=index + 1,
            dem_elevation_m=500.0,
            variable="wind",
            valid_at="2026-01-01T00:00:00Z",
            value=value,
        )
        for index, value in enumerate(values)
    ]


def test_wind_dispatch_uses_p90_where_it_differs_from_median() -> None:
    """A monotonic set where median (5.5) and p90 (9.1) genuinely differ."""
    values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    result = compute_consensus(
        _wind_readings(values),
        variable="wind",
        site_elevation_m=500.0,
        mad_floor=1.0,  # MAD_FLOORS["wind"]; p90 ignores it
    )
    assert result is not None
    assert result.value == pytest.approx(_p90(values))
    assert result.n_stations == len(values)
    assert result.rejected_stations == 0


def test_wind_anti_regression_p90_keeps_high_exposure_station() -> None:
    """Load-bearing discriminator (plan A6): a monotonic set alone cannot
    catch a silent regression back to median/MAD, because nothing in it
    exceeds the MAD band. This asymmetric fixture does: 20 m/s is far
    outside the MAD band of [3,3,3,3] and would be REJECTED by median/MAD
    (leaving n_stations=4, rejected=1), but p90 must KEEP it.

    Arithmetic: sorted=[3,3,3,3,20]; k=(5-1)*0.9=3.6; between s[3]=3 and
    s[4]=20 -> 3 + (20-3)*0.6 = 3 + 10.2 = 13.2.
    """
    result = compute_consensus(
        _wind_readings([3.0, 3.0, 3.0, 3.0, 20.0]),
        variable="wind",
        site_elevation_m=500.0,
        mad_floor=1.0,  # MAD_FLOORS["wind"]; p90 ignores it
    )
    assert result is not None
    assert result.value == pytest.approx(13.2)
    assert result.n_stations == 5
    assert result.rejected_stations == 0


# ---------------------------------------------------------------------------
# Temperature no-op regression pin
# ---------------------------------------------------------------------------


def test_temperature_dispatch_unchanged_lapse_normalization_pin() -> None:
    """Regression pin (plan A6): reuses the exact scenario from
    ``test_consensus_lapse_normalization_oracle`` (tests/test_m1_m5.py:1766+)
    to prove temperature still routes through the same lapse-adjusted
    median/MAD path, byte-identical to 0.6.0.

    Arithmetic (site elevation 500 m, LAPSE=0.0065):
      Station A: dem=800, raw=17.0 -> 17.0+0.0065*300=18.95
      Station B: dem=200, raw=22.0 -> 22.0+0.0065*(-300)=20.05
      Station C: dem=500, raw=20.5 -> 20.5
      median([18.95, 20.05, 20.5]) = 20.05; MAD band keeps all 3 inliers.
    """
    readings = [
        StationReading(
            station_id=1,
            dem_elevation_m=800.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=17.0,
        ),
        StationReading(
            station_id=2,
            dem_elevation_m=200.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=22.0,
        ),
        StationReading(
            station_id=3,
            dem_elevation_m=500.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=20.5,
        ),
    ]
    result = compute_consensus(
        readings,
        variable="temperature",
        site_elevation_m=500.0,
        mad_floor=0.5,  # MAD_FLOORS["temperature"]
    )
    assert result is not None
    assert result.rejected_stations == 0
    assert result.n_stations == 3
    assert result.value == pytest.approx(20.05)


# ---------------------------------------------------------------------------
# Precip gate pin (critical): median/MAD ships in R1, low_trim is NOT wired
# ---------------------------------------------------------------------------


def _precip_readings() -> list[StationReading]:
    # [1,2,3,4]: median/MAD rejects nothing (max deviation 1.5 <= band 4.45)
    # and low_trim differs from it, so the two paths are cleanly separable
    # by BOTH value and n_stations/rejected_stations.
    return [
        StationReading(
            station_id=index + 1,
            dem_elevation_m=500.0,
            variable="precip",
            valid_at="2026-01-01T00:00:00Z",
            value=value,
        )
        for index, value in enumerate([1.0, 2.0, 3.0, 4.0])
    ]


def test_precip_gate_pins_median_mad_not_low_trim() -> None:
    """Precip GATE pin (plan A4, load-bearing): fails loudly if someone
    wires low_trim into _precip_estimator before the B6 gate clears.

    median/MAD arithmetic: sorted=[1,2,3,4]; median=2.5;
      deviations=[1.5,0.5,0.5,1.5]; MAD=1.0; floor(0.3) -> effective 1.0;
      band=3*1.4826*1.0=4.4478; all 4 inliers -> median([1,2,3,4])=2.5.
    low_trim([1,2,3,4]) would instead give mean(2,3,4)=3.0 with
    n_stations=3, rejected=1 (per plan A2 post-gate formula) -- neither of
    which this asserts.
    """
    result = compute_consensus(
        _precip_readings(),
        variable="precip",
        site_elevation_m=500.0,
        mad_floor=0.3,  # MAD_FLOORS["precip"]
    )
    assert result is not None
    assert result.value == pytest.approx(2.5)
    assert result.n_stations == 4
    assert result.rejected_stations == 0
    # Explicit anti-regression: this is NOT what low_trim would produce.
    assert result.value != pytest.approx(_low_trim([1.0, 2.0, 3.0, 4.0]))


@pytest.mark.xfail(reason="precip low_trim wired in Part B / B6", strict=True)
def test_precip_post_gate_low_trim_value_documents_intended_b6_behavior() -> None:
    """Documents the intended post-gate behavior (plan A4/A2): once B6 wires
    low_trim into _precip_estimator, this flips from xfail to xpass -- that
    flip is the signal the gated pin above needs updating alongside it.

    low_trim([1,2,3,4]) = mean(2,3,4) = 3.0; per A2's post-gate formula,
    n_stations=len(values)-1=3, rejected_stations=1.
    """
    result = compute_consensus(
        _precip_readings(),
        variable="precip",
        site_elevation_m=500.0,
        mad_floor=0.3,  # MAD_FLOORS["precip"]
    )
    assert result is not None
    assert result.value == pytest.approx(3.0)
    assert result.n_stations == 3
    assert result.rejected_stations == 1


# ---------------------------------------------------------------------------
# Unknown-variable allowlist: no silent fallthrough to median
# ---------------------------------------------------------------------------


def _humidity_readings() -> list[StationReading]:
    # StationReading has no variable allowlist of its own -- the model
    # accepts any string; the allowlist lives in compute_consensus's
    # _ESTIMATORS dispatch map. Same 4-value shape as the precip fixture so
    # the only injected difference between this and the paired positive is
    # the `variable` dispatch key itself.
    return [
        StationReading(
            station_id=index + 1,
            dem_elevation_m=500.0,
            variable="humidity",
            valid_at="2026-01-01T00:00:00Z",
            value=value,
        )
        for index, value in enumerate([1.0, 2.0, 3.0, 4.0])
    ]


def test_unknown_variable_returns_none_not_silent_median() -> None:
    """Negative half of the allowlist pair: an unallowlisted variable must
    return None, never silently inherit the median/MAD path."""
    result = compute_consensus(
        _humidity_readings(),
        variable="humidity",
        site_elevation_m=500.0,
        mad_floor=0.3,
    )
    assert result is None


def test_known_variable_same_shape_still_dispatches_paired_positive() -> None:
    """Paired positive for the allowlist test above: identical reading shape
    (4 stations, same values, same valid_at), the ONLY difference is the
    `variable` dispatch key. Proves the None above is the allowlist working,
    not an incidental empty-input/empty-dispatch-map artifact."""
    result = compute_consensus(
        _precip_readings(),
        variable="precip",
        site_elevation_m=500.0,
        mad_floor=0.3,
    )
    assert result is not None


def test_empty_readings_returns_none() -> None:
    result = compute_consensus(
        [],
        variable="wind",
        site_elevation_m=500.0,
        mad_floor=1.0,
    )
    assert result is None


# ---------------------------------------------------------------------------
# materialize_consensus integration: real tmp DB, dispatch reached through
# the actual write/invalidation path.
# ---------------------------------------------------------------------------


def test_materialize_consensus_wind_p90_through_real_write_path(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Estimator Site', 47, 25, 500, 'UTC')
            """
        ).lastrowid
    )
    valid_at = "2026-01-01T00:00:00Z"
    values = [3.0, 3.0, 3.0, 3.0, 20.0]
    for index, value in enumerate(values):
        station_id = int(
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, ?, 47, 25, 500)
                """,
                (site_id, f"STATION{index}"),
            ).lastrowid
        )
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at=valid_at,
            value=value,
            source_raw=f"{value} m/s",
        )
    row = conn.execute(
        """
        SELECT value, n_stations, rejected_stations
        FROM observations
        WHERE site_id=? AND variable='wind' AND valid_at=?
        """,
        (site_id, valid_at),
    ).fetchone()
    assert row is not None
    # Same [3,3,3,3,20] discriminator as the pure-dispatch test above: proves
    # the real write path reaches p90 (13.2), not median/MAD (which would
    # reject the 20.0 station and land on 3.0 with rejected_stations=1).
    assert row["value"] == pytest.approx(13.2)
    assert row["n_stations"] == 5
    assert row["rejected_stations"] == 0

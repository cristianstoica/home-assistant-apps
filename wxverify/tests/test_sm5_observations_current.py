"""S-M5 Bucket-1-D contract tests: GET /api/observations/current.

Six oracles from the hoare ruling:

1. Cold station (sps.health_state='cold', no station_current_obs row)
   → all obs fields null; health_state == "cold".

2. Offline-with-last-good (station_current_obs row present, sps.health_state='offline')
   → obs fields retain their non-null last-good values; health_state == "offline".
   Anti-conflation assertion: non-null obs alongside a non-online state.

3. Empty registry (no enabled stations) → response is [].

4. ?station= filter: returns only that station's row; a disabled station named
   via ?station= is still excluded (enabled=1 is ANDed, not replaced);
   ?station=<nonexistent> → [].

5. Units — NATIVE (km/h / hPa / mm) are returned unchanged.
   Specific oracle: wind_speed seeded as 18.0 km/h comes back as 18.0, NOT 5.0
   (the m/s-converted value). A future accidental kmh_to_ms in the route must
   fail this test.

6. Additive keys: error_count is int when the sps row exists, null when the
   LEFT JOIN misses; last_poll_at is present in the shape.

Cold (1) and Offline-with-last-good (2) form the required paired positive/negative:
cold-null is meaningful only because the offline-non-null case can go red — the
pair ensures neither assertion is vacuous.

Isolation: per-test tmp-file DB via TestClient + the standard _init_tmp_db pattern
(close_db → config.db_path → init_db via create_app). The idle-worker stub keeps
the background task from interfering with the DB state.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_OBS_TIME = "2026-07-10T11:55:00Z"
_NOW_ISO = "2026-07-10T12:00:00Z"

# Synthetic station IDs (opsec: public repo — never live IVATRA*/IDORNA* IDs)
_PWS_COLD = "ISTATION01"
_PWS_OFFLINE = "ISTATION02"
_PWS_DISABLED = "ISTATION03"
_PWS_UNITS = "ISTATION04"

# Units oracle: 18.0 km/h is unambiguous vs m/s conversion (÷3.6 → 5.0).
_WIND_SPEED_KMH = 18.0
_WIND_SPEED_IF_CONVERTED_MS = 5.0  # what an accidental kmh_to_ms would produce
_WIND_GUST_KMH = 27.0
_PRESSURE_HPA = 1013.25
_PRECIP_RATE_MM = 0.4
_PRECIP_TOTAL_MM = 2.1

# Synthetic neighborhood (opsec: public repo)
_NEIGHBORHOOD = "Synthetic Test Valley"


# ---------------------------------------------------------------------------
# Idle worker stub (mirrors test_m1_m5.py)
# ---------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


# ---------------------------------------------------------------------------
# Shared seed helpers (real SQLite; every test owns its own DB)
# ---------------------------------------------------------------------------


def _seed_site(conn: sqlite3.Connection, *, name: str = "SITE-A") -> int:
    return int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES (?, 47.0, 25.0, 900.0, 'UTC')",
            (name,),
        ).lastrowid
    )


def _seed_station(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    pws_id: str,
    enabled: int = 1,
) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 47.0, 25.0, 900.0, ?)",
            (site_id, pws_id, enabled),
        ).lastrowid
    )


def _seed_poll_state(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    health_state: str,
    next_poll_at: str = _NOW_ISO,
    last_poll_at: str | None = None,
    last_error: str | None = None,
    error_count: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, last_poll_at,"
        "  last_error, error_count, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            station_id,
            health_state,
            next_poll_at,
            last_poll_at,
            last_error,
            error_count,
            _NOW_ISO,
        ),
    )


def _seed_current_obs(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    obs_time_utc: str = _OBS_TIME,
    temp: float = 22.5,
    humidity: float = 65.0,
    dewpt: float = 15.3,
    wind_speed: float = _WIND_SPEED_KMH,
    wind_gust: float = _WIND_GUST_KMH,
    wind_dir: float = 270.0,
    pressure: float = _PRESSURE_HPA,
    precip_rate: float = _PRECIP_RATE_MM,
    precip_total: float = _PRECIP_TOTAL_MM,
    uv: float = 3.0,
    neighborhood: str = _NEIGHBORHOOD,
) -> None:
    conn.execute(
        "INSERT INTO station_current_obs"
        " (station_id, obs_time_utc, temp, humidity, dewpt,"
        "  wind_speed, wind_gust, wind_dir, pressure,"
        "  precip_rate, precip_total, uv, neighborhood, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            station_id,
            obs_time_utc,
            temp,
            humidity,
            dewpt,
            wind_speed,
            wind_gust,
            wind_dir,
            pressure,
            precip_rate,
            precip_total,
            uv,
            neighborhood,
            _NOW_ISO,
        ),
    )


# ---------------------------------------------------------------------------
# Oracle 1 — Cold station: no station_current_obs row → all obs fields null
# ---------------------------------------------------------------------------


def test_cold_station_obs_fields_are_null(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cold station with sps row but NO station_current_obs → obs fields are null.

    Paired positive: test_offline_station_retains_last_good_obs (oracle 2), which
    proves non-null obs is possible — so this null assertion is not vacuous.

    Arrange: one enabled station; station_poll_state.health_state='cold';
             no station_current_obs row.
    Act: GET /api/observations/current
    Assert: single row; all obs fields null; health_state == "cold";
            error_count is int (sps row exists); last_poll_at is present.
    """
    close_db()
    config.db_path = str(tmp_path / "cold.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _seed_site(conn)
            station_id = _seed_station(conn, site_id, pws_id=_PWS_COLD)
            _seed_poll_state(
                conn,
                station_id,
                health_state="cold",
                error_count=0,
            )
            # Deliberately NO station_current_obs insert — that is the precondition.
            return station_id

        station_id = db.write_sync(_seed)

        resp = client.get("/api/observations/current")

        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        row = rows[0]

        assert row["station_id"] == station_id
        assert row["pws_station_id"] == _PWS_COLD
        assert row["health_state"] == "cold", (
            f"health_state must be 'cold', got {row['health_state']!r}"
        )

        # All obs fields must be null — the LEFT JOIN miss on station_current_obs.
        obs_fields = (
            "temp",
            "humidity",
            "dewpt",
            "wind_speed",
            "wind_gust",
            "wind_dir",
            "pressure",
            "precip_rate",
            "precip_total",
            "uv",
            "obs_time_utc",
            "neighborhood",
        )
        for field in obs_fields:
            assert row[field] is None, (
                f"Cold station: obs field '{field}' must be null, got {row[field]!r}"
            )

        # Additive keys (oracle 6): sps row exists → error_count is int.
        assert isinstance(row["error_count"], int), (
            "error_count must be int when sps row exists, "
            f"got {type(row['error_count'])}"
        )
        assert "last_poll_at" in row, "last_poll_at key must be present in the shape"


# ---------------------------------------------------------------------------
# Oracle 2 — Offline-with-last-good: retained obs are non-null alongside
#            health_state == "offline"  (anti-conflation assertion)
# ---------------------------------------------------------------------------


def test_offline_station_retains_last_good_obs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Offline station with an existing station_current_obs row → obs retained.

    Anti-conflation oracle (§13-D): the route must NOT null out obs fields based
    on health_state. This test goes red if someone later adds "if health_state !=
    'online': zero/null the obs fields" to the route.

    Paired negative: test_cold_station_obs_fields_are_null (oracle 1) — proves
    null obs IS returned when the LEFT JOIN misses, making this non-null
    assertion meaningful.

    Arrange: one enabled station; station_poll_state.health_state='offline';
             station_current_obs row with known values.
    Act: GET /api/observations/current
    Assert: obs fields equal the seeded last-good values; health_state == "offline".
    """
    close_db()
    config.db_path = str(tmp_path / "offline.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _seed_site(conn)
            station_id = _seed_station(conn, site_id, pws_id=_PWS_OFFLINE)
            _seed_poll_state(
                conn,
                station_id,
                health_state="offline",
                last_poll_at=_OBS_TIME,
                last_error="upstream timeout",
                error_count=3,
            )
            _seed_current_obs(
                conn,
                station_id,
                obs_time_utc=_OBS_TIME,
                temp=21.0,
                humidity=70.0,
                dewpt=14.8,
                wind_speed=12.0,
                wind_gust=18.0,
                wind_dir=180.0,
                pressure=1015.0,
                precip_rate=0.0,
                precip_total=1.2,
                uv=2.0,
                neighborhood=_NEIGHBORHOOD,
            )
            return station_id

        station_id = db.write_sync(_seed)

        resp = client.get("/api/observations/current")

        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        row = rows[0]

        assert row["station_id"] == station_id
        assert row["health_state"] == "offline", (
            f"health_state must be 'offline', got {row['health_state']!r}"
        )

        # Anti-conflation: obs fields must be the RETAINED non-null last-good values.
        assert row["temp"] == pytest.approx(21.0), (
            "offline station must retain last-good temp, not null it"
        )
        assert row["humidity"] == pytest.approx(70.0)
        assert row["dewpt"] == pytest.approx(14.8)
        assert row["wind_speed"] == pytest.approx(12.0)
        assert row["wind_gust"] == pytest.approx(18.0)
        assert row["wind_dir"] == pytest.approx(180.0)
        assert row["pressure"] == pytest.approx(1015.0)
        assert row["precip_rate"] == pytest.approx(0.0)
        assert row["precip_total"] == pytest.approx(1.2)
        assert row["uv"] == pytest.approx(2.0)
        assert row["obs_time_utc"] == _OBS_TIME
        assert row["neighborhood"] == _NEIGHBORHOOD

        # Additive keys (oracle 6).
        assert row["error_count"] == 3, (
            f"error_count must be the seeded int 3, got {row['error_count']!r}"
        )
        assert row["last_poll_at"] == _OBS_TIME


# ---------------------------------------------------------------------------
# Oracle 3 — Empty registry → []
# ---------------------------------------------------------------------------


def test_empty_registry_returns_empty_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No enabled stations → GET /api/observations/current returns [].

    Paired positive: every other oracle in this suite seeds at least one enabled
    station and asserts a non-empty response, making this [] meaningful.
    """
    close_db()
    config.db_path = str(tmp_path / "empty.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        resp = client.get("/api/observations/current")
        assert resp.status_code == 200
        assert resp.json() == [], (
            "Empty registry must return [] (no enabled stations seeded)"
        )


# ---------------------------------------------------------------------------
# Oracle 4 — ?station= filter
# ---------------------------------------------------------------------------


def test_station_filter_returns_only_named_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """?station=<id> returns only that station's row (enabled=1 stations, exact match).

    Three sub-cases in one seeded DB:
    A. ?station=<id of ISTATION01> → exactly 1 row for ISTATION01.
    B. ?station=<id of disabled ISTATION03> → [] (enabled=1 AND is enforced).
    C. ?station=<nonexistent id> → [].

    The no-filter request is also asserted first to confirm the baseline row count
    (two enabled stations), ensuring the filter assertions aren't vacuously satisfied
    by an accidental empty registry.
    """
    close_db()
    config.db_path = str(tmp_path / "filter.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int, int]:
            site_id = _seed_site(conn)
            # Two enabled stations.
            id_a = _seed_station(conn, site_id, pws_id=_PWS_COLD, enabled=1)
            id_b = _seed_station(conn, site_id, pws_id=_PWS_OFFLINE, enabled=1)
            # One disabled station.
            id_disabled = _seed_station(conn, site_id, pws_id=_PWS_DISABLED, enabled=0)
            # Seed poll state for enabled stations so we get real rows back.
            _seed_poll_state(conn, id_a, health_state="online")
            _seed_poll_state(conn, id_b, health_state="cold")
            # No poll state for disabled — it must never appear regardless.
            return id_a, id_b, id_disabled

        id_a, id_b, id_disabled = db.write_sync(_seed)

        # Baseline: no filter → both enabled stations returned.
        resp = client.get("/api/observations/current")
        assert resp.status_code == 200
        all_ids = {row["station_id"] for row in resp.json()}
        assert all_ids == {id_a, id_b}, (
            f"No-filter baseline must return both enabled stations, got {all_ids}"
        )

        # Sub-case A: filter to the first enabled station.
        resp = client.get("/api/observations/current", params={"station": id_a})
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1, (
            f"?station={id_a} must return exactly 1 row, got {len(rows)}"
        )
        assert rows[0]["station_id"] == id_a
        assert rows[0]["pws_station_id"] == _PWS_COLD

        # Sub-case B: disabled station named via ?station= is still excluded.
        resp = client.get("/api/observations/current", params={"station": id_disabled})
        assert resp.status_code == 200
        assert resp.json() == [], (
            f"Disabled station (id={id_disabled}) must be excluded even when named"
            " via ?station= — enabled=1 is ANDed, not replaced"
        )

        # Sub-case C: nonexistent station id → [].
        nonexistent_id = 99999
        resp = client.get(
            "/api/observations/current", params={"station": nonexistent_id}
        )
        assert resp.status_code == 200
        assert resp.json() == [], (
            f"?station={nonexistent_id} (nonexistent) must return []"
        )


# ---------------------------------------------------------------------------
# Oracle 5 — Units: NATIVE km/h / hPa / mm returned unchanged
# ---------------------------------------------------------------------------


def test_wind_pressure_precip_returned_in_native_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Route returns stored native units (km/h, hPa, mm) WITHOUT conversion.

    Pins the ratified ruling: obs values are stored in units='m' form (km/h
    wind, hPa pressure, mm precip) and the route must pass them through as-is.
    A future accidental `kmh_to_ms` in the route would produce 5.0 for
    wind_speed and 7.5 for wind_gust instead of the seeded 18.0 / 27.0 — this
    test catches that regression by name in the assertion message.

    Seeded values chosen so km/h vs m/s is numerically unambiguous:
      wind_speed: 18.0 km/h  →  expected 18.0  (converted m/s would be 5.0)
      wind_gust:  27.0 km/h  →  expected 27.0  (converted m/s would be 7.5)
      pressure:   1013.25 hPa  →  expected 1013.25
      precip_rate:  0.4 mm   →  expected 0.4
      precip_total: 2.1 mm   →  expected 2.1
    """
    close_db()
    config.db_path = str(tmp_path / "units.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _seed_site(conn)
            station_id = _seed_station(conn, site_id, pws_id=_PWS_UNITS)
            _seed_poll_state(conn, station_id, health_state="online")
            _seed_current_obs(
                conn,
                station_id,
                wind_speed=_WIND_SPEED_KMH,  # 18.0 km/h
                wind_gust=_WIND_GUST_KMH,  # 27.0 km/h
                pressure=_PRESSURE_HPA,  # 1013.25 hPa
                precip_rate=_PRECIP_RATE_MM,  # 0.4 mm
                precip_total=_PRECIP_TOTAL_MM,  # 2.1 mm
            )
            return station_id

        station_id = db.write_sync(_seed)

        resp = client.get("/api/observations/current")
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        row = rows[0]
        assert row["station_id"] == station_id

        # Wind speed: assert NATIVE 18.0 km/h, not the m/s-converted 5.0.
        assert row["wind_speed"] == pytest.approx(_WIND_SPEED_KMH), (
            f"wind_speed must be native km/h ({_WIND_SPEED_KMH}), "
            f"NOT m/s-converted ({_WIND_SPEED_IF_CONVERTED_MS}); "
            f"got {row['wind_speed']!r} — check for accidental kmh_to_ms in route"
        )
        assert row["wind_speed"] != pytest.approx(_WIND_SPEED_IF_CONVERTED_MS), (
            "wind_speed must not equal the m/s-converted value "
            f"({_WIND_SPEED_IF_CONVERTED_MS}) — route is converting units"
        )

        # Wind gust: 27.0 km/h native (m/s-converted would be 7.5).
        assert row["wind_gust"] == pytest.approx(_WIND_GUST_KMH), (
            f"wind_gust must be native km/h ({_WIND_GUST_KMH}), "
            f"got {row['wind_gust']!r}"
        )

        # Pressure in hPa.
        assert row["pressure"] == pytest.approx(_PRESSURE_HPA), (
            f"pressure must be native hPa ({_PRESSURE_HPA}), got {row['pressure']!r}"
        )

        # Precip in mm.
        assert row["precip_rate"] == pytest.approx(_PRECIP_RATE_MM), (
            f"precip_rate must be native mm ({_PRECIP_RATE_MM}), "
            f"got {row['precip_rate']!r}"
        )
        assert row["precip_total"] == pytest.approx(_PRECIP_TOTAL_MM), (
            f"precip_total must be native mm ({_PRECIP_TOTAL_MM}), "
            f"got {row['precip_total']!r}"
        )


# ---------------------------------------------------------------------------
# Oracle 6 — Additive keys: error_count null when LEFT JOIN misses sps row
# ---------------------------------------------------------------------------


def test_error_count_null_when_poll_state_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """error_count is null when the station has no station_poll_state row.

    The LEFT JOIN on station_poll_state may miss if the station was just inserted
    before its first poll cycle. The route's error_count expression:
        None if row['error_count'] is None else int(row['error_count'])
    must preserve null in that case, not cast None to int (which would raise) or
    default to 0 (which would be wrong).

    Paired positive: every other oracle seeds a poll_state row and confirms
    error_count is a non-null int — so this null assertion is meaningful.
    """
    close_db()
    config.db_path = str(tmp_path / "no-sps.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _seed_site(conn)
            # Enabled station with NO station_poll_state row (simulates pre-first-poll).
            station_id = _seed_station(conn, site_id, pws_id=_PWS_COLD, enabled=1)
            return station_id

        station_id = db.write_sync(_seed)

        resp = client.get("/api/observations/current")
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        row = rows[0]

        assert row["station_id"] == station_id
        assert row["error_count"] is None, (
            "error_count must be null when the LEFT JOIN misses station_poll_state; "
            f"got {row['error_count']!r}"
        )
        assert "last_poll_at" in row, "last_poll_at key must be present even when null"
        assert row["last_poll_at"] is None, (
            "last_poll_at must be null when station_poll_state row is absent"
        )
        assert row["health_state"] is None, (
            "health_state must be null when station_poll_state row is absent"
        )

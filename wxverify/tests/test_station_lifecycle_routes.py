"""Station lifecycle route tests (plan 2026-10-20-daily-close-day-state-and-
guard.md §13.5, test IDs LR1-LR12).

Routes run through ``TestClient(create_app(root_path=""))`` with the worker
idled and CSRF headers, exactly as the existing route tests in
``tests/test_m1_m5.py`` do. Station validation and elevation lookups are
faked (network adapters, not the code under test). ``utc_now`` is faked in
``api/routes/stations.py`` and ``api/routes/sites.py`` -- both modules
currently have no such attribute, so patching it is itself part of the red
proof for every test below: ``AttributeError`` until P1 adds the import.

Real SQLite only (a per-test tmp-file database through the app's own
``init_db``/``create_app`` lifespan); nothing in the code under test is
mocked. Station ids are synthetic (``ISTATION01``, ``ISTATION02``, ...),
zones are ``UTC``/``Etc/GMT`` only, dates are built from ``D``/``NOW``
(``tests/helpers.py``).

``tests/helpers.py`` only exposes the fixture's station ids as the private
``_DAY_CLOSE_STATION_A``/``_DAY_CLOSE_STATION_B`` constants, not as public
``ISTATION01``/``ISTATION02`` names -- the literals below are hardcoded to
match those private constants' values rather than importing a name that
does not exist.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    NOW,
    D,
    asof_insert_observation,
    asof_make_site,
    day_close_site,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc_micro
from wxverify.db.connection import close_db, get_db
from wxverify.obs.pws_adapter import PwsStation

_STATION_A = "ISTATION01"
_STATION_B = "ISTATION02"

# Plan §13.5 LR2, 8.7's full "station belongs to another site" message.
_ANOTHER_SITE_TEXT = (
    "station belongs to another site: a station stays with the site it was "
    "first added to, even after it is removed"
)


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


async def _fake_validate_station(station_id: str, api_key: str) -> PwsStation:
    assert api_key == "secret-weather"
    return PwsStation(station_id=station_id, lat=40.1, lon=-104.9)


async def _fake_lookup_elevation_m(lat: float, lon: float) -> float:
    return 910.0


def _setup_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> object:
    close_db()
    config.db_path = str(tmp_path / f"{name}.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", _fake_validate_station
    )
    monkeypatch.setattr(
        "wxverify.api.routes.stations.lookup_elevation_m", _fake_lookup_elevation_m
    )
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    # Both modules import utc_now (P1); patched here so lifecycle-dated
    # fixtures run against the test's synthetic NOW, not the real clock.
    monkeypatch.setattr("wxverify.api.routes.stations.utc_now", lambda: NOW)
    monkeypatch.setattr("wxverify.api.routes.sites.utc_now", lambda: NOW)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    csrf = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": csrf}


def _station_db_id(site_id: int, pws_id: str) -> int:
    row = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT id FROM stations WHERE site_id=? AND pws_station_id=?",
            (site_id, pws_id),
        ).fetchone()
    )
    assert row is not None, f"station {pws_id} not found at site {site_id}"
    return int(row["id"])


def _jobs_for_site(site_id: int) -> list[str]:
    rows = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT type FROM jobs WHERE site_id = ?", (site_id,)
        ).fetchall()
    )
    return [str(r["type"]) for r in rows]


def _station_changes(station_id: int) -> list[tuple[str, str, str]]:
    rows = get_db().read_sync(
        lambda conn: conn.execute(
            """
            SELECT change, effective_date, reason FROM station_changes
            WHERE station_id = ? ORDER BY id
            """,
            (station_id,),
        ).fetchall()
    )
    return [
        (str(r["change"]), str(r["effective_date"]), str(r["reason"])) for r in rows
    ]


def _day_list(site_id: int, local_date: str) -> set[int]:
    rows = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT station_id FROM site_day_stations WHERE site_id=? AND local_date=?",
            (site_id, local_date),
        ).fetchall()
    )
    return {int(r["station_id"]) for r in rows}


def _seed_site(
    site_id_holder: list[int], *, close_on: bool, with_records: bool
) -> None:
    def _seed(conn: sqlite3.Connection) -> int:
        return day_close_site(
            conn, timezone="UTC", close_on=close_on, with_records=with_records
        )

    site_id_holder.append(get_db().write_sync(_seed))


def _seed_other_site(pws_ids: list[str]) -> int:
    """Build a standalone site holding enabled stations ``pws_ids``.

    ``day_close_site`` always creates ``ISTATION01``/``ISTATION02``
    (helpers.py:646-647), so calling it twice for "this site" and "another
    site" collides on the global ``pws_station_id`` uniqueness. This builds
    the other site by hand instead, with ids that don't collide.
    """

    def _seed(conn: sqlite3.Connection) -> int:
        site_id = asof_make_site(conn, "lr2-other-site")
        for pws_id in pws_ids:
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, ?, 0.0, 0.0, 0.0, 1)
                """,
                (site_id, pws_id),
            )
        return site_id

    return get_db().write_sync(_seed)


def test_lr1_create_station_queues_no_job_and_counts_from_the_next_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr1")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]

        resp = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "ISTATION03"},
            headers=headers,
        )
        assert resp.status_code == 200

        assert _jobs_for_site(site_id) == [], (
            "R2 (switch-over plan 7.2): create_station queues no job"
        )

        station_id = _station_db_id(site_id, "ISTATION03")
        changes = _station_changes(station_id)
        assert changes == [
            ("added", (D + timedelta(days=1)).isoformat(), "create_station")
        ]

        day_plus_1 = (D + timedelta(days=1)).isoformat()
        day_0 = D.isoformat()
        assert station_id in _day_list(site_id, day_plus_1)
        assert station_id not in _day_list(site_id, day_0)


def test_lr2_duplicate_station_id_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr2")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=False, with_records=False)
        site_id = site_ids[0]

        other_held_id = "ISTATION04"
        other_retire_id = "ISTATION05"
        other_site_id = _seed_other_site([other_held_id, other_retire_id])

        def _snapshot() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
            return get_db().read_sync(
                lambda conn: (
                    [
                        dict(r)
                        for r in conn.execute("SELECT * FROM stations ORDER BY id")
                    ],
                    [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM station_changes ORDER BY id"
                        )
                    ],
                )
            )

        # Sub-case: held by an enabled station of THIS site.
        before = _snapshot()
        resp = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": _STATION_B},
            headers=headers,
        )
        assert resp.status_code == 409
        assert _snapshot() == before

        # Sub-case: held by a turned-off station of this site.
        client.put(
            f"/api/sites/{site_id}/stations/{_station_db_id(site_id, _STATION_B)}",
            json={"enabled": False},
            headers=headers,
        )
        before = _snapshot()
        resp = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": _STATION_B},
            headers=headers,
        )
        assert resp.status_code == 409
        assert _snapshot() == before

        # Sub-case: held at another site (enabled there).
        before = _snapshot()
        resp = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": other_held_id},
            headers=headers,
        )
        assert resp.status_code == 409
        assert resp.json() == {"error": _ANOTHER_SITE_TEXT}
        assert "delete" not in resp.text
        assert _snapshot() == before

        # Sub-case: retired (deleted today) at another site.
        other_retire_db_id = _station_db_id(other_site_id, other_retire_id)
        client.delete(
            f"/api/sites/{other_site_id}/stations/{other_retire_db_id}",
            headers=headers,
        )
        before = _snapshot()
        resp = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": other_retire_id},
            headers=headers,
        )
        assert resp.status_code == 409
        assert resp.json() == {"error": _ANOTHER_SITE_TEXT}
        assert "delete" not in resp.text
        assert _snapshot() == before


def test_lr3_readd_a_retired_station_at_the_same_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr3")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]
        b_id = _station_db_id(site_id, _STATION_B)

        # B's readings on the open day D-1 and the closed day D-3, and the
        # site's observations on those hours set to a value their consensus
        # does not give (plan §13.5 LR3): seeded before the first request.
        lr3_station_value = 12.3
        lr3_obs_value = 999.0
        d_minus_1 = (D - timedelta(days=1)).isoformat()
        d_minus_3 = (D - timedelta(days=3)).isoformat()

        def _seed_readings(conn: sqlite3.Connection) -> None:
            for day in (d_minus_1, d_minus_3):
                valid_at = f"{day}T12:00:00Z"
                conn.execute(
                    """
                    INSERT INTO station_observations
                        (station_id, variable, valid_at, value, qc_flag, source_raw)
                    VALUES (?, 'temperature', ?, ?, 'ok', 'test_seed')
                    """,
                    (b_id, valid_at, lr3_station_value),
                )
                asof_insert_observation(
                    conn,
                    site_id=site_id,
                    valid_at=valid_at,
                    value=lr3_obs_value,
                    computed_at=None,
                )

        get_db().write_sync(_seed_readings)

        assert _jobs_for_site(site_id) == []

        def _snapshot() -> dict[str, list[dict[str, object]]]:
            return get_db().read_sync(
                lambda conn: {
                    "observations": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM observations WHERE site_id = ? "
                            "ORDER BY variable, valid_at",
                            (site_id,),
                        ).fetchall()
                    ],
                    "station_observations": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM station_observations WHERE station_id = ? "
                            "ORDER BY variable, valid_at",
                            (b_id,),
                        ).fetchall()
                    ],
                    "forecast_pairs": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM forecast_pairs WHERE site_id = ? "
                            "ORDER BY variable, valid_at, feed_id",
                            (site_id,),
                        ).fetchall()
                    ],
                    "daily_truth": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM daily_truth WHERE site_id = ? "
                            "ORDER BY quantity, local_date",
                            (site_id,),
                        ).fetchall()
                    ],
                }
            )

        before = _snapshot()

        client.delete(f"/api/sites/{site_id}/stations/{b_id}", headers=headers)

        first = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": _STATION_B},
            headers=headers,
        )
        second = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": _STATION_B},
            headers=headers,
        )
        client.delete(f"/api/sites/{site_id}/stations/{b_id}", headers=headers)
        third = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": _STATION_B},
            headers=headers,
        )

        assert (first.status_code, second.status_code, third.status_code) == (
            200,
            409,
            200,
        )

        row = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT retired_at, enabled FROM stations WHERE id = ?", (b_id,)
            ).fetchone()
        )
        assert row["retired_at"] is None
        assert bool(row["enabled"]) is True

        changes = _station_changes(b_id)
        assert [c[0] for c in changes] == [
            "added",
            "retired",
            "turned_on",
            "retired",
            "turned_on",
        ]
        assert all(c[2] == "create_station" for c in changes if c[0] == "turned_on")

        assert _jobs_for_site(site_id) == []

        after = _snapshot()
        assert before == after

        day_0 = D.isoformat()
        day_plus_1 = (D + timedelta(days=1)).isoformat()
        assert b_id not in _day_list(site_id, day_0)
        assert b_id in _day_list(site_id, day_plus_1)


def test_lr4_toggle_writes_only_on_an_actual_flip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr4")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]
        b_id = _station_db_id(site_id, _STATION_B)

        before = _station_changes(b_id)
        client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": True},
            headers=headers,
        )
        assert _station_changes(b_id) == before

        client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": False},
            headers=headers,
        )
        client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": True},
            headers=headers,
        )

        changes = _station_changes(b_id)
        day_plus_1 = (D + timedelta(days=1)).isoformat()
        assert changes[-2][0] == "turned_off"
        assert changes[-2][1] == D.isoformat()
        assert changes[-1][0] == "turned_on"
        assert changes[-1][1] == day_plus_1


def test_lr5_toggle_off_does_not_touch_closed_or_open_day_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr5")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]
        b_id = _station_db_id(site_id, _STATION_B)

        # B's readings on the open day D-1 and the closed day D-3, and the
        # site's observations on those hours set to a value their consensus
        # does not give (plan §13.5 LR5): seeded before the request.
        lr5_station_value = 12.3
        lr5_obs_value = 999.0
        d_minus_1 = (D - timedelta(days=1)).isoformat()
        d_minus_3 = (D - timedelta(days=3)).isoformat()

        def _seed_readings(conn: sqlite3.Connection) -> None:
            for day in (d_minus_1, d_minus_3):
                valid_at = f"{day}T12:00:00Z"
                conn.execute(
                    """
                    INSERT INTO station_observations
                        (station_id, variable, valid_at, value, qc_flag, source_raw)
                    VALUES (?, 'temperature', ?, ?, 'ok', 'test_seed')
                    """,
                    (b_id, valid_at, lr5_station_value),
                )
                asof_insert_observation(
                    conn,
                    site_id=site_id,
                    valid_at=valid_at,
                    value=lr5_obs_value,
                    computed_at=None,
                )

        get_db().write_sync(_seed_readings)

        def _snapshot() -> dict[str, list[dict[str, object]]]:
            return get_db().read_sync(
                lambda conn: {
                    "observations": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM observations WHERE site_id = ? "
                            "ORDER BY variable, valid_at",
                            (site_id,),
                        ).fetchall()
                    ],
                    "forecast_pairs": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM forecast_pairs WHERE site_id = ? "
                            "ORDER BY variable, valid_at, feed_id",
                            (site_id,),
                        ).fetchall()
                    ],
                    "daily_truth": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM daily_truth WHERE site_id = ? "
                            "ORDER BY quantity, local_date",
                            (site_id,),
                        ).fetchall()
                    ],
                }
            )

        before = _snapshot()
        resp = client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": False},
            headers=headers,
        )
        assert resp.status_code == 200
        after = _snapshot()
        assert before == after


def test_lr6_cannot_turn_off_or_remove_last_enabled_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr6")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=False, with_records=False)
        site_id = site_ids[0]
        a_id = _station_db_id(site_id, _STATION_A)
        b_id = _station_db_id(site_id, _STATION_B)

        client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": False},
            headers=headers,
        )

        resp = client.put(
            f"/api/sites/{site_id}/stations/{a_id}",
            json={"enabled": False},
            headers=headers,
        )
        assert resp.status_code == 409
        assert "site must retain at least one enabled station" in resp.text

        resp2 = client.delete(f"/api/sites/{site_id}/stations/{a_id}", headers=headers)
        assert resp2.status_code == 409
        assert "site must retain at least one enabled station" in resp2.text

        row = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT enabled, retired_at FROM stations WHERE id=?", (a_id,)
            ).fetchone()
        )
        assert bool(row["enabled"]) is True
        assert row["retired_at"] is None


def test_lr7_cannot_empty_the_current_days_list_while_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 LR7: "site with A counting today and C added today; turn
    off A; remove A". Turning B off is immediate (effective D, LR4), which
    removes it from today's list; adding C only counts from D+1 (LR1), so
    neither B nor C counts today -- A is today's only counted station, and
    turning it off or removing it must trip check 2."""
    app = _setup_app(tmp_path, monkeypatch, "lr7")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]
        a_id = _station_db_id(site_id, _STATION_A)
        b_id = _station_db_id(site_id, _STATION_B)

        client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": False},
            headers=headers,
        )
        client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "ISTATION06"},
            headers=headers,
        )

        day_0 = D.isoformat()
        assert _day_list(site_id, day_0) == {a_id}

        resp = client.put(
            f"/api/sites/{site_id}/stations/{a_id}",
            json={"enabled": False},
            headers=headers,
        )
        assert resp.status_code == 409
        assert "site must keep at least one station counting today" in resp.text

        resp2 = client.delete(f"/api/sites/{site_id}/stations/{a_id}", headers=headers)
        assert resp2.status_code == 409
        assert "site must keep at least one station counting today" in resp2.text


def test_lr7_check_two_does_not_run_while_off(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 LR7, off half: the same "only A counts today" setup, but
    with the close off check 2 must not run -- turning A off answers 200."""
    app = _setup_app(tmp_path, monkeypatch, "lr7b")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=False, with_records=False)
        site_id = site_ids[0]
        a_id = _station_db_id(site_id, _STATION_A)
        b_id = _station_db_id(site_id, _STATION_B)

        client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": False},
            headers=headers,
        )
        client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "ISTATION06"},
            headers=headers,
        )

        resp = client.put(
            f"/api/sites/{site_id}/stations/{a_id}",
            json={"enabled": False},
            headers=headers,
        )
        assert resp.status_code == 200


def test_lr8_delete_is_a_retire_not_an_erase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr8")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]
        b_id = _station_db_id(site_id, _STATION_B)

        # B's readings, wind records and wind days on the open day D-1 (plan
        # §13.5 LR8): seeded before the delete so the retire-not-erase
        # comparison actually compares non-empty state, not two empty lists.
        lr8_station_value = 12.3
        lr8_obs_value = 999.0
        lr8_wind_speed = 9.0
        d_minus_1 = (D - timedelta(days=1)).isoformat()
        lr8_valid_at = f"{d_minus_1}T12:00:00Z"

        def _seed_lr8(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO station_observations
                    (station_id, variable, valid_at, value, qc_flag, source_raw)
                VALUES (?, 'temperature', ?, ?, 'ok', 'test_seed')
                """,
                (b_id, lr8_valid_at, lr8_station_value),
            )
            asof_insert_observation(
                conn,
                site_id=site_id,
                valid_at=lr8_valid_at,
                value=lr8_obs_value,
                computed_at=None,
            )
            conn.execute(
                "INSERT INTO station_wind_records (station_id, obs_at, source,"
                " speed_kmh) VALUES (?, ?, 'five_minute', ?)",
                (b_id, lr8_valid_at, lr8_wind_speed),
            )
            conn.execute(
                """
                INSERT INTO station_wind_days
                    (station_id, local_date, status, record_count, pair_hours,
                     attempts, next_attempt_at, last_error, last_ok_at, refetch_at,
                     refetched, updated_at)
                VALUES (?, ?, 'fetched', 0, 0, 0, NULL, NULL, NULL, NULL, 0, ?)
                """,
                (b_id, d_minus_1, isoformat_utc_micro(NOW)),
            )

        get_db().write_sync(_seed_lr8)

        def _station_rows(conn: sqlite3.Connection) -> dict[str, object]:
            return {
                "obs": [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM station_observations WHERE station_id=? "
                        "ORDER BY variable, valid_at",
                        (b_id,),
                    ).fetchall()
                ],
                "site_obs": [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM observations WHERE site_id=? "
                        "ORDER BY variable, valid_at",
                        (site_id,),
                    ).fetchall()
                ],
                "wind_records": [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM station_wind_records WHERE station_id=? "
                        "ORDER BY obs_at",
                        (b_id,),
                    ).fetchall()
                ],
                "wind_days": [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM station_wind_days WHERE station_id=? "
                        "ORDER BY local_date",
                        (b_id,),
                    ).fetchall()
                ],
            }

        before = get_db().read_sync(_station_rows)
        assert before["obs"]
        assert before["site_obs"]
        assert before["wind_records"]
        assert before["wind_days"]
        resp = client.delete(f"/api/sites/{site_id}/stations/{b_id}", headers=headers)
        assert resp.status_code == 200
        assert resp.json() == {"deleted": True}
        after = get_db().read_sync(_station_rows)
        assert before == after

        row = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT enabled, retired_at FROM stations WHERE id=?", (b_id,)
            ).fetchone()
        )
        assert row is not None
        assert bool(row["enabled"]) is False
        assert row["retired_at"] == isoformat_utc_micro(NOW)

        changes = _station_changes(b_id)
        assert changes[-1] == ("retired", D.isoformat(), "delete_station")

        put_resp = client.put(
            f"/api/sites/{site_id}/stations/{b_id}",
            json={"enabled": True},
            headers=headers,
        )
        assert put_resp.status_code == 404
        delete_resp = client.delete(
            f"/api/sites/{site_id}/stations/{b_id}", headers=headers
        )
        assert delete_resp.status_code == 404


def test_lr9_retired_station_hidden_turned_off_one_stays_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr9")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=False, with_records=False)
        site_id = site_ids[0]
        b_id = _station_db_id(site_id, _STATION_B)

        add_e = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "ISTATION05"},
            headers=headers,
        )
        assert add_e.status_code == 200
        e_id = _station_db_id(site_id, "ISTATION05")
        client.put(
            f"/api/sites/{site_id}/stations/{e_id}",
            json={"enabled": False},
            headers=headers,
        )

        client.delete(f"/api/sites/{site_id}/stations/{b_id}", headers=headers)

        listed = client.get(f"/api/sites/{site_id}/stations", headers=headers)
        listed_ids = {row["pws_station_id"] for row in listed.json()}
        assert "ISTATION05" in listed_ids
        assert _STATION_B not in listed_ids

        page = client.get("/sites")
        assert "ISTATION05" in page.text
        assert _STATION_B not in page.text


def test_lr10_new_site_record_only_while_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.core.timeutil import isoformat_utc
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

    app = _setup_app(tmp_path, monkeypatch, "lr10")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        get_db().write_sync(
            lambda conn: set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
        )

        resp = client.post(
            "/api/sites",
            json={
                "name": "Changeover Site",
                "forecast_lat": 10.0,
                "forecast_lon": 20.0,
                "elevation_m": 100.0,
                "timezone": "Etc/GMT-14",
            },
            headers=headers,
        )
        assert resp.status_code == 200
        site_id = resp.json()["id"]

        rows = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT local_date FROM site_days WHERE site_id=?", (site_id,)
            ).fetchall()
        )
        # NOW is D 12:00Z; Etc/GMT-14 is UTC+14, so the local date at creation
        # is D+1 -- a new site starts fresh at its next local date, D+2
        # (plan 4.6, 8.3).
        day_plus_2 = (D + timedelta(days=2)).isoformat()
        assert [r["local_date"] for r in rows] == [day_plus_2]


def test_lr10_off_site_gets_no_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr10b")
    with TestClient(app) as client:
        headers = _csrf_headers(client)

        resp = client.post(
            "/api/sites",
            json={
                "name": "Off Site",
                "forecast_lat": 10.0,
                "forecast_lon": 20.0,
                "elevation_m": 100.0,
                "timezone": "UTC",
            },
            headers=headers,
        )
        assert resp.status_code == 200
        site_id = resp.json()["id"]

        rows = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT local_date FROM site_days WHERE site_id=?", (site_id,)
            ).fetchall()
        )
        assert [r["local_date"] for r in rows] == []


def test_lr11_remove_confirmation_wording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr11")
    with TestClient(app) as client:
        _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=False, with_records=False)

        page = client.get("/sites")
        assert page.status_code == 200
        assert "Adding it again brings it back from the next day." in page.text
        assert "stored observations are deleted" not in page.text


def test_lr12_failed_readd_leaves_every_table_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _setup_app(tmp_path, monkeypatch, "lr12")
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        site_ids: list[int] = []
        _seed_site(site_ids, close_on=True, with_records=True)
        site_id = site_ids[0]
        b_id = _station_db_id(site_id, _STATION_B)

        client.delete(f"/api/sites/{site_id}/stations/{b_id}", headers=headers)

        def _snapshot(conn: sqlite3.Connection) -> dict[str, object]:
            return {
                "stations": [
                    dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")
                ],
                "station_changes": [
                    dict(r)
                    for r in conn.execute("SELECT * FROM station_changes ORDER BY id")
                ],
                "site_days": [
                    dict(r)
                    for r in conn.execute("SELECT * FROM site_days ORDER BY local_date")
                ],
                "site_day_stations": [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM site_day_stations "
                        "ORDER BY local_date, station_id"
                    )
                ],
                "jobs": [
                    dict(r) for r in conn.execute("SELECT * FROM jobs ORDER BY id")
                ],
            }

        before = get_db().read_sync(_snapshot)

        import wxverify.api.routes.stations as stations_module

        def _raise(*args: object, **kwargs: object) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(stations_module, "record_station_change", _raise)

        with pytest.raises(RuntimeError):
            client.post(
                f"/api/sites/{site_id}/stations",
                json={"pws_station_id": _STATION_B},
                headers=headers,
            )

        after = get_db().read_sync(_snapshot)
        assert before == after

        row = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT enabled, retired_at FROM stations WHERE id=?", (b_id,)
            ).fetchone()
        )
        assert bool(row["enabled"]) is False
        assert row["retired_at"] is not None

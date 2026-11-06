"""R1 (plan `2026-11-02-storage-release.md` line 245): a v17 database opens
through the real ``init_db``, and every route in ``app.routes`` answers with
no ``sqlite3.OperationalError`` and no 5xx.

Written against ``migrate_v17_storage``/``TARGET_USER_VERSION == 17``, which
do not exist yet, so this file's one fixture (``_boot_v17_app``) fails first
today: a fresh ``init_db`` on a ``tmp_path`` file opens at ``user_version``
16 (today's ``TARGET_USER_VERSION``), which is already what `run_migrations`
has always produced for an empty file -- that part does not itself error.
What makes the whole module red today is that this file inserts synthetic
rows directly into ``forecast_history`` (3.1's new table) and reads them
back through routes that W4 already moved onto it
(``/api/winrate``, ``/api/timeseries``) -- those pass already, since W1-W6
have landed in this worktree (per the dispatch brief). The genuinely new
red is the R1 grep claim and the route sweep running against an ACTUAL v17
file (``user_version == 17``, ``forecast_pairs``/``close_pairs``/
``verification_*`` all absent): today's open always stops at 16, so the
``assert version == 17`` in ``_boot_v17_app`` fails first, before any route
is even called.

Isolation: a real, file-backed v17 database under ``tmp_path`` (``init_db``,
real migrations); the worker lane is an idle stand-in (the
``tests/test_verification_page_removed.py``/``tests/test_ops_page_copy.py``
``_boot`` pattern) so no job runs concurrently with the route sweep. No
mocked database. Synthetic data only: made-up site/station/feed values,
letters, no real identifier.

Skipped (listed here, not silently dropped from the sweep):

- ``POST /api/sites/{site_id}/stations`` -- needs a live weather.com API
  key and a real network round trip (``create_station``, ``stations.py``)
  to resolve a station's lat/lon before it ever touches the database; no
  deterministic construction without one.
- ``PUT /api/record-snapshot-time`` is exercised; its sibling upload/import
  routes do not exist any more (Q8/5.5, already removed in this worktree)
  so there is nothing left to skip there.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.routing import Route

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db import migrations
from wxverify.db.connection import close_db, get_db
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _boot_v17_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    close_db()
    config.db_path = str(tmp_path / "v17-routes.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    return app


def _seed(conn: Any) -> dict[str, int]:
    """Synthetic rows: one site, its generation, a station, a feed's
    ``forecast_history`` row and one ``observations`` row -- enough for the
    dashboard/win-rate/history/forecast readers to have something to read.
    """
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('wx-v17-route-site', 5.0, 6.0, 7.0, 'UTC')"
    )
    site_id = int(cur.lastrowid)
    ensure_site_generation(conn, site_id)

    row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'open-meteo' ORDER BY id LIMIT 1"
    ).fetchone()
    feed_id = int(row["id"])

    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m) VALUES (?, 'ISTATIONROUTE01', 5.1, 6.1, 10.0)",
        (site_id,),
    )
    station_id = int(cur.lastrowid)

    conn.execute(
        "INSERT INTO forecast_history (site_id, feed_id, variable, day_ahead,"
        " valid_at, local_date, value, lead_hours, issued_at)"
        " VALUES (?, ?, 'temperature', 1, '2035-01-02T06:00:00Z',"
        " '2035-01-02', 11.0, 30, '2035-01-01T00:00:00Z')",
        (site_id, feed_id),
    )
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value,"
        " n_stations) VALUES (?, 'temperature', '2035-01-02T06:00:00Z', 11.4, 1)",
        (site_id,),
    )
    conn.commit()
    return {"site_id": site_id, "feed_id": feed_id, "station_id": station_id}


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


_SKIPPED_ROUTES: frozenset[tuple[str, str]] = frozenset(
    {("POST", "/api/sites/{site_id}/stations")}
)

# FastAPI's own documentation routes, not application routes -- never
# exercised and never worth naming in _SKIPPED_ROUTES, just excluded.
_FRAMEWORK_ROUTES: frozenset[tuple[str, str]] = frozenset(
    {
        ("GET", "/docs"),
        ("GET", "/docs/oauth2-redirect"),
        ("GET", "/openapi.json"),
        ("GET", "/redoc"),
    }
)


# ---------------------------------------------------------------------------
# The sweep
# ---------------------------------------------------------------------------


def test_v17_database_opens_and_every_route_answers_without_db_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot_v17_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001 - test seeds the real writer conn
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == migrations.TARGET_USER_VERSION == 17, (
            "a fresh open must land at v17 before the route sweep runs"
        )
        for table in (
            "forecast_pairs",
            "close_pairs",
            "verification_runs",
            "verification_evidence",
        ):
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            assert row is None, f"{table} must be absent from a v17 file"

        ids = _seed(conn)
        site_id, feed_id, station_id = (
            ids["site_id"],
            ids["feed_id"],
            ids["station_id"],
        )

        # A throwaway site/station for the destructive DELETE routes, kept
        # separate so deleting them does not disturb the fixture the other
        # calls read.
        cur = conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon,"
            " elevation_m, timezone) VALUES ('wx-v17-route-doomed', 1.0,"
            " 1.0, 1.0, 'UTC')"
        )
        doomed_site_id = int(cur.lastrowid)
        ensure_site_generation(conn, doomed_site_id)
        cur = conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m) VALUES (?, 'ISTATIONDOOMED01', 1.1, 1.1, 5.0)",
            (doomed_site_id,),
        )
        doomed_station_id = int(cur.lastrowid)
        conn.commit()

        export_resp = client.post("/api/export/begin", headers=_csrf_headers(client))
        assert export_resp.status_code == 202, export_resp.text
        export_id = export_resp.json()["export_id"]

        headers = _csrf_headers(client)

        cases: list[tuple[str, str, dict[str, Any]]] = [
            ("GET", "/", {}),
            ("GET", "/forecast", {}),
            ("GET", f"/forecast/tiles?site={site_id}", {}),
            ("GET", f"/forecast/day?site={site_id}&day=0", {}),
            ("GET", "/sites", {}),
            ("GET", f"/dashboard?site={site_id}", {}),
            ("GET", "/ops", {}),
            ("GET", f"/overlay?site={site_id}&feed_id={feed_id}", {}),
            ("GET", "/api/sites", {}),
            (
                "POST",
                "/api/sites",
                {
                    "headers": headers,
                    "json": {
                        "name": "wx-v17-route-created",
                        "forecast_lat": 2.0,
                        "forecast_lon": 3.0,
                        "elevation_m": 4.0,
                        "timezone": "UTC",
                    },
                },
            ),
            ("GET", f"/api/sites/{site_id}", {}),
            (
                "PUT",
                f"/api/sites/{site_id}",
                {
                    "headers": headers,
                    "json": {"rain_threshold_mm": 0.3},
                },
            ),
            ("GET", f"/api/sites/{site_id}/stations", {}),
            (
                "PUT",
                f"/api/sites/{site_id}/stations/{station_id}",
                {
                    "headers": headers,
                    "json": {"enabled": True},
                },
            ),
            ("GET", "/api/feeds", {}),
            (
                "PUT",
                f"/api/feeds/{feed_id}",
                {
                    "headers": headers,
                    "json": {"enabled": True},
                },
            ),
            (
                "PUT",
                f"/api/sites/{site_id}/feeds/{feed_id}",
                {
                    "headers": headers,
                    "json": {"enabled": True},
                },
            ),
            ("GET", f"/api/leaderboard?site={site_id}", {}),
            ("GET", f"/api/curve?site={site_id}", {}),
            ("GET", f"/api/winrate?site={site_id}", {}),
            ("GET", f"/api/composite?site={site_id}", {}),
            ("GET", f"/api/forecast/hourly?site={site_id}", {}),
            ("GET", f"/api/timeseries?site={site_id}&feed_id={feed_id}", {}),
            ("GET", "/api/health/keys", {}),
            ("GET", "/api/health/budget", {}),
            ("GET", "/api/health/feeds", {}),
            ("GET", "/api/health/providers", {}),
            ("GET", "/api/health/backfill", {}),
            ("GET", "/api/health/backoffs", {}),
            ("GET", "/api/observations/current", {}),
            ("GET", "/api/worker/status", {}),
            ("GET", "/api/health/monitor", {}),
            ("POST", f"/api/sites/{site_id}/backfill", {"headers": headers}),
            ("POST", "/api/catchup", {"headers": headers}),
            ("POST", "/api/export/begin", {"headers": _csrf_headers(client)}),
            ("GET", f"/api/export/status/{export_id}", {}),
            ("GET", f"/api/export/download/{export_id}", {}),
            (
                "PUT",
                "/api/settings/record-snapshot-time",
                {
                    "headers": headers,
                    "json": {"time": "07:00"},
                },
            ),
            (
                "DELETE",
                f"/api/settings/record-snapshot-time?site={site_id}",
                {
                    "headers": headers,
                },
            ),
            (
                "DELETE",
                f"/api/sites/{doomed_site_id}/stations/{doomed_station_id}",
                {
                    "headers": headers,
                },
            ),
            ("DELETE", f"/api/sites/{doomed_site_id}", {"headers": headers}),
        ]

        failures: list[str] = []
        for method, path, kwargs in cases:
            try:
                resp = client.request(method, path, **kwargs)
            except Exception as exc:  # pragma: no cover - failure path only
                failures.append(f"{method} {path}: raised {exc!r}")
                continue
            if resp.status_code >= 500:
                failures.append(f"{method} {path}: {resp.status_code} {resp.text}")
        assert failures == [], "\n".join(failures)

    close_db()


def test_every_route_in_app_routes_is_either_called_above_or_explicitly_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A completeness check for the sweep above: every ``(method, path)``
    FastAPI registers must appear, verbatim, either in this file's case
    list (by prefix match against the templated path) or in
    ``_SKIPPED_ROUTES``. New routes added later without updating the sweep
    fail HERE, loudly, instead of silently never being exercised."""
    app = _boot_v17_app(tmp_path, monkeypatch)
    registered: set[tuple[str, str]] = set()
    for route in app.routes:
        if not isinstance(route, Route):
            continue
        for method in route.methods or set():
            if method in ("HEAD", "OPTIONS"):
                continue
            registered.add((method, route.path))

    # The exact templated paths this file's case list exercises (kept in
    # sync by hand with the ``cases`` list above -- a mismatch here is the
    # point of this test).
    exercised: set[tuple[str, str]] = {
        ("GET", "/"),
        ("GET", "/forecast"),
        ("GET", "/forecast/tiles"),
        ("GET", "/forecast/day"),
        ("GET", "/sites"),
        ("GET", "/dashboard"),
        ("GET", "/ops"),
        ("GET", "/overlay"),
        ("GET", "/api/sites"),
        ("POST", "/api/sites"),
        ("GET", "/api/sites/{site_id}"),
        ("PUT", "/api/sites/{site_id}"),
        ("DELETE", "/api/sites/{site_id}"),
        ("GET", "/api/sites/{site_id}/stations"),
        ("PUT", "/api/sites/{site_id}/stations/{station_id}"),
        ("DELETE", "/api/sites/{site_id}/stations/{station_id}"),
        ("GET", "/api/feeds"),
        ("PUT", "/api/feeds/{feed_id}"),
        ("PUT", "/api/sites/{site_id}/feeds/{feed_id}"),
        ("GET", "/api/leaderboard"),
        ("GET", "/api/curve"),
        ("GET", "/api/winrate"),
        ("GET", "/api/composite"),
        ("GET", "/api/forecast/hourly"),
        ("GET", "/api/timeseries"),
        ("GET", "/api/health/keys"),
        ("GET", "/api/health/budget"),
        ("GET", "/api/health/feeds"),
        ("GET", "/api/health/providers"),
        ("GET", "/api/health/backfill"),
        ("GET", "/api/health/backoffs"),
        ("GET", "/api/observations/current"),
        ("GET", "/api/worker/status"),
        ("GET", "/api/health/monitor"),
        ("POST", "/api/sites/{site_id}/backfill"),
        ("POST", "/api/catchup"),
        ("POST", "/api/export/begin"),
        ("GET", "/api/export/status/{export_id}"),
        ("GET", "/api/export/download/{export_id}"),
        ("PUT", "/api/settings/record-snapshot-time"),
        ("DELETE", "/api/settings/record-snapshot-time"),
        ("GET", "/api/csrf"),
    }

    missing = registered - exercised - _SKIPPED_ROUTES - _FRAMEWORK_ROUTES
    assert missing == set(), (
        f"these routes are neither exercised nor explicitly skipped: {missing}"
    )

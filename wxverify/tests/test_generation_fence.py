"""Generation-fence discrimination tests.

A database replace (``Database.replace_from``) can land while a request or
job is mid-flight, after it has already read from -- or reserved budget
against -- the database that is about to be swapped out from under it. Every
write downstream of that read is bound (via ``FencedWriter``) to the
generation captured at read time, so a write submitted after the swap is
rejected with ``StaleGenerationError`` instead of silently landing against
whatever now owns those row ids in the replacement database.

Each scenario here builds its own live database and its own tiny,
fully-migrated replacement file rather than reusing fixtures across files,
matching how the rest of this suite is organized.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import (
    Database,
    FencedWriter,
    StaleGenerationError,
    close_db,
    get_db,
)
from wxverify.feeds.seam import CostEstimate, FetchResult, ForecastRequest
from wxverify.obs.pws_adapter import PwsStation
from wxverify.worker.feed_fetch import DownloadSlot, fetch_feed_once


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites "
        "(name, forecast_lat, forecast_lon, elevation_m, timezone, enabled) "
        "VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)",
        (name,),
    )
    site_id = cur.lastrowid
    assert site_id is not None
    return int(site_id)


def _open_meteo_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds"
            " WHERE source='open-meteo' AND is_virtual=0"
            " ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )


def _make_station(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
        VALUES (?, ?, 40.0, -105.0, 900.0)
        """,
        (site_id, pws_station_id),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _build_replacement_db(tmp_path: Path, filename: str, site_name: str) -> Path:
    """A standalone, fully-migrated database file suitable as a
    ``replace_from`` swap target -- built via a throwaway ``Database``
    instance (never the process-wide singleton) so migrations run exactly
    as they would on a real import.
    """
    path = tmp_path / filename
    db = Database(str(path))
    try:
        _make_site(db._conn, site_name)  # noqa: SLF001
        db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
    finally:
        db.close()
    return path


class _BlockingAdapter:
    """A forecast adapter whose fetch blocks until released, so a test can
    land a replace_from while a fetch is in flight."""

    def __init__(self, entered: asyncio.Event, release: asyncio.Event) -> None:
        self._entered = entered
        self._release = release

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        self._entered.set()
        await self._release.wait()
        return FetchResult(samples=[])


def test_fetch_feed_fence_discards_a_write_after_a_replace(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A database replace landing mid-fetch must discard that fetch's write
    instead of letting it land against whatever now owns the target site
    and feed ids -- and must say so out loud: the provider call underneath
    it was already paid for (a reserved budget call), so silently losing
    the result would hide a real cost from the operator.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = Database(str(db_path))

    async def _run() -> None:
        try:
            site_id = await db.write(lambda conn: _make_site(conn, "Original Site"))
            feed_id = await db.read(_open_meteo_feed_id)
            writer = FencedWriter(db, db.generation)

            entered = asyncio.Event()
            release = asyncio.Event()
            adapter = _BlockingAdapter(entered, release)

            def _adapter_builder(
                source: str, client: httpx.AsyncClient
            ) -> _BlockingAdapter:
                return adapter

            task = asyncio.create_task(
                fetch_feed_once(
                    db,
                    site_id,
                    feed_id,
                    writer=writer,
                    adapter_builder=_adapter_builder,
                    slot=DownloadSlot(
                        slot="unscheduled",
                        round_date=None,
                        round_try=None,
                        started_at=utc_now(),
                    ),
                )
            )
            await asyncio.wait_for(entered.wait(), timeout=5.0)

            new_path = _build_replacement_db(
                tmp_path, "replacement.db", "Different Site"
            )
            backup_path = tmp_path / "backup.db.bak"

            with caplog.at_level(logging.WARNING, logger="wxverify.collection.budget"):
                await db.replace_from(new_path, backup_path)
                release.set()
                with pytest.raises(StaleGenerationError):
                    await asyncio.wait_for(task, timeout=5.0)

            sample_count = await db.read(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) AS n FROM forecast_samples"
                ).fetchone()["n"]
            )
            assert sample_count == 0, (
                "the discarded fetch's samples must never land in the "
                "replacement database"
            )

            job_count = await db.read(
                lambda conn: conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()[
                    "n"
                ]
            )
            assert job_count == 0, (
                "no orphaned job bookkeeping may appear in the replacement database"
            )

            discard_warnings = [
                r
                for r in caplog.records
                if r.levelno == logging.WARNING
                and "discarding result of a completed provider call" in r.getMessage()
            ]
            assert len(discard_warnings) == 1, (
                f"expected exactly one discard warning; got: "
                f"{[r.getMessage() for r in caplog.records]}"
            )
            msg = discard_warnings[0].getMessage()
            assert "source=open-meteo" in msg
            assert "calls=1" in msg
        finally:
            db.close()

    asyncio.run(_run())


def test_create_station_fence_rejects_a_write_after_a_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A database replace landing mid-request must reject the station write
    instead of attaching it to whatever site now owns that id in the
    replacement database -- the same fence, and the same disposition, as
    the worker path.
    """
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")

    entered = threading.Event()
    release = threading.Event()

    async def _blocked_validate_station(station_id: str, api_key: str) -> PwsStation:
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.01)
        return PwsStation(station_id=station_id, lat=40.1, lon=-104.9)

    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", _blocked_validate_station
    )

    # network guard: idle the worker so the lifespan never reaches a provider
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site = client.post(
            "/api/sites",
            json={
                "name": "Original Site",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
            },
            headers=headers,
        )
        assert site.status_code == 200
        site_id = site.json()["id"]

        outcome: dict[str, Any] = {}

        def _drive_request() -> None:
            try:
                outcome["response"] = client.post(
                    f"/api/sites/{site_id}/stations",
                    json={"pws_station_id": "SYN1"},
                    headers=headers,
                )
            except Exception as exc:
                outcome["exception"] = exc

        thread = threading.Thread(target=_drive_request)
        thread.start()
        assert entered.wait(timeout=5.0), "validate_station was never entered"

        db = get_db()
        new_path = _build_replacement_db(tmp_path, "replacement.db", "Different Site")
        backup_path = tmp_path / "backup.db.bak"
        client.portal.call(db.replace_from, new_path, backup_path)

        release.set()
        thread.join(timeout=5.0)
        assert not thread.is_alive(), "the driving request thread never finished"

        assert isinstance(outcome.get("exception"), StaleGenerationError), (
            f"expected a StaleGenerationError from the blocked request; "
            f"got: {outcome!r}"
        )

        live_conn = sqlite3.connect(config.db_path)
        try:
            live_conn.row_factory = sqlite3.Row
            count = live_conn.execute(
                "SELECT COUNT(*) AS n FROM stations WHERE site_id=?", (site_id,)
            ).fetchone()["n"]
        finally:
            live_conn.close()
        assert count == 0, (
            "no station row may land against the replacement database's "
            "site of the same id"
        )

"""A database error inside a download's save rolls the whole save back.

This file once tested the fetch-triggered scoring gate: a probe on every
download that decided whether to call `enqueue_scoring_run`, which enqueues
nothing with the daily close on. A later release deleted the gate with the
close-off code it fed.
What stays is the live-path rollback test: an error raised inside the save,
after the samples are written, leaves nothing stored and no transaction
open. The error is now raised by the save's first-download check
(`first_recorded_download`), which decides whether the download writes
scored values. Synthetic fixtures: 'Testsite' at 12.3456/65.4321, dates in
2035.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from wxverify import config
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.feeds.seam import CostEstimate, FetchResult, NormalizedSample
from wxverify.worker import feed_fetch as feed_fetch_module
from wxverify.worker.feed_fetch import DownloadSlot, fetch_feed_once

_SITE_NAME = "Testsite"
_LAT = 12.3456
_LON = 65.4321


def _seed_site(conn: sqlite3.Connection, *, site_id: int = 1) -> int:
    conn.execute(
        "INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES (?, ?, ?, ?, 100.0, 'UTC', 1)",
        (site_id, _SITE_NAME, _LAT, _LON),
    )
    return site_id


def _feed_model(conn: sqlite3.Connection, feed_id: int) -> str:
    row = conn.execute("SELECT model FROM feeds WHERE id = ?", (feed_id,)).fetchone()
    assert row is not None
    return str(row["model"])


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])


@pytest.mark.parametrize("fenced", [False, True], ids=["unfenced", "fenced"])
def test_save_error_rolls_back_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fenced: bool
) -> None:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db_obj = init_db(str(db_path))
    conn = db_obj._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    feed_id = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND is_virtual=0"
        " ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    model = _feed_model(conn, feed_id)
    db = get_db()
    # Both writer paths share db_obj._conn, so one assertion target covers
    # both parametrized cases.
    writer = FencedWriter(db) if fenced else None

    class _Adapter:
        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            sample = NormalizedSample(
                model=model,
                variable="temperature",
                issued_at="2035-03-01T00:00:00Z",
                valid_at="2035-03-02T00:30:00Z",
                lead_hours=1,
                value=10.0,
                source_raw="{}",
                model_run_id="run-1",
            )
            return FetchResult(samples=[sample], grid=None)

    def _build(source: str, client: Any) -> _Adapter:
        return _Adapter()

    exc = sqlite3.DatabaseError("database disk image is malformed")
    exc.sqlite_errorcode = sqlite3.SQLITE_CORRUPT  # type: ignore[attr-defined]
    calls: list[int] = []

    def _raise(*args: Any, **kwargs: Any) -> None:
        # Raised after the samples are written: the save checks for an
        # earlier download of the day once the samples are stored.
        calls.append(_count(conn, "forecast_samples"))
        raise exc

    monkeypatch.setattr(feed_fetch_module, "first_recorded_download", _raise)

    with pytest.raises(sqlite3.DatabaseError) as excinfo:
        asyncio.run(
            fetch_feed_once(
                db,
                site_id,
                feed_id,
                writer=writer,
                adapter_builder=_build,
                slot=DownloadSlot(
                    slot="unscheduled",
                    round_date=None,
                    round_try=None,
                    started_at=utc_now(),
                ),
            )
        )

    assert excinfo.value is exc
    assert calls == [1], (
        f"the error must be raised once, after the sample is written: {calls!r}"
    )
    assert _count(conn, "forecast_samples") == 0
    assert _count(conn, "forecast_scored") == 0
    assert _count(conn, "forecast_downloads") == 0
    assert conn.in_transaction is False

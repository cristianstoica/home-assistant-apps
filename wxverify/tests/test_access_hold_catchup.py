"""Catch-up and backfill never read or write the ``hourly`` access hold
(wxverify 0.16.13 round 2, commit 3; plan CB1-CB2).

CB1 — a refused (401) station-history call: one call; neither module writes
      a ``weathercom_access_hold:*`` row. Each module's OWN job outcome is
      asserted as the code actually behaves today, not assumed:

      * catchup's per-site try/except (``run_catchup``) swallows the
        re-raised ``HTTPStatusError`` at the site level and logs a WARNING,
        so the catchup job itself completes normally (``result="ok"``) --
        it is NOT failed or retried.
      * backfill has no such per-site guard around
        ``fetch_station_history_window``, so the same re-raised
        ``HTTPStatusError`` propagates to ``run_claimed_job``'s generic
        failure path and the job genuinely fails/retries (``fail()``:
        status stays ``pending``, ``retry_count`` becomes 1,
        ``next_attempt_at`` set).

      This is a deliberate split from a single "the job fails" oracle --
      see the plan-mismatch note in the hand-back report.

CB2 — an ``hourly`` hold already in force: catch-up and backfill each still
      make their call, because neither reads ``check_access_hold`` for any
      kind -- their own ``_reserve_station_history_call`` only checks
      ``check_domain_backoff``.

Every station id below is synthetic: ``ISTATION0{1,2}``.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.obs.pws_adapter import PwsObservation
from wxverify.worker.access_hold import record_access_refused
from wxverify.worker.processor import run_claimed_job

_API_KEY = "0123456789abcdef0123456789abcdef"
_TZ = "UTC"


@pytest.fixture(autouse=True)
def _weathercom_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)


@pytest.fixture(autouse=True)
def _fast_pace(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_pace(site_id: int, station_id: int, ordinal: int) -> None:
        return None

    monkeypatch.setattr("wxverify.worker.catchup.pace_station_call", _fake_pace)
    monkeypatch.setattr("wxverify.worker.backfill.pace_station_call", _fake_pace)


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _seed_site(conn, *, name: str = "SITE-A") -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 0.0, 0.0, 0.0, ?)",
        (name, _TZ),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station(conn, site_id: int, *, pws_id: str) -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, ?, 0.0, 0.0, 0.0)",
        (site_id, pws_id),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _station_row(conn, station_id: int):  # type: ignore[no-untyped-def]
    row = conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()
    assert row is not None
    return row


def _job_row(conn, job_id: int):  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT status, next_attempt_at, retry_count, result FROM jobs WHERE id=?",
        (job_id,),
    ).fetchone()
    assert row is not None
    return row


def _hold_row_count(conn) -> int:  # type: ignore[no-untyped-def]
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM runtime_state"
            " WHERE key LIKE 'weathercom_access_hold:%'"
        ).fetchone()[0]
    )


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request(
        "GET", "https://api.weather.com/v2/pws/observations/hourly/7day"
    )
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("refused", request=request, response=response)


async def _run_main_job(db: Database) -> int:  # type: ignore[no-untyped-def]
    job = await db.write(claim_next_job)
    assert job is not None
    await run_claimed_job(db, job, lane="main")
    return job.id


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.catchup.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.backfill.utc_now", lambda: when)


# ---------------------------------------------------------------------------
# CB1 — a 401 writes no access-hold row, in either module
# ---------------------------------------------------------------------------


def test_cb1_catchup_refusal_writes_no_access_hold_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """catchup's per-site try/except swallows the refusal: one call; the
    station's ``last_error``/``error_count`` are updated; the catchup job
    completes normally (``result="ok"``), it is NOT failed/retried; and no
    ``weathercom_access_hold:*`` row is written.

    Mutant (catchup writes a refused-access row) -> at "no access-hold row"
    below: correct = 0, mutant = 1 (a raw ``runtime_state`` insert for
    ``weathercom_access_hold:hourly`` added to the HTTPStatusError branch
    in ``_fetch_missing_station_history``).
    """
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id="ISTATION01")
        enqueue_if_absent(conn, "catchup", None, "catchup", {})

        calls = {"n": 0}

        async def fake_fetch_range(*args: object, **kwargs: object) -> object:
            calls["n"] += 1
            raise _http_status_error(401)

        with patch(
            "wxverify.worker.catchup.fetch_hourly_history_range", fake_fetch_range
        ):
            job_id = await _run_main_job(db)

        assert calls["n"] == 1

        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "completed", "catchup swallows the refusal per-site"
        assert job_row["result"] == "ok"
        assert job_row["retry_count"] == 0

        station_row = _station_row(conn, station_id)
        assert station_row["last_error"] is not None
        assert station_row["error_count"] == 1

        assert _hold_row_count(conn) == 0, "no access-hold row"

    asyncio.run(run())


def test_cb1_backfill_refusal_writes_no_access_hold_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """backfill has no per-site guard, so the re-raised ``HTTPStatusError``
    propagates to ``run_claimed_job``'s generic failure path: one call; the
    station's ``last_error``/``error_count`` are updated; the backfill job
    genuinely fails and is scheduled to retry (status stays ``pending``,
    ``retry_count`` becomes 1, ``next_attempt_at`` set); and no
    ``weathercom_access_hold:*`` row is written.

    Mutant (backfill writes a refused-access row) -> at "no access-hold
    row" below: correct = 0, mutant = 1 (a raw ``runtime_state`` insert for
    ``weathercom_access_hold:hourly`` added to the HTTPStatusError branch
    in ``fetch_station_history_window``).
    """
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id="ISTATION02")
        enqueue_if_absent(
            conn, "backfill_site", site_id, f"backfill:{site_id}", {"site_id": site_id}
        )

        calls = {"n": 0}

        async def fake_fetch_range(*args: object, **kwargs: object) -> object:
            calls["n"] += 1
            raise _http_status_error(401)

        with patch(
            "wxverify.worker.backfill.fetch_hourly_history_range", fake_fetch_range
        ):
            job_id = await _run_main_job(db)

        assert calls["n"] == 1

        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "pending", "backfill genuinely fails/retries"
        assert job_row["retry_count"] == 1
        assert job_row["next_attempt_at"] is not None
        assert job_row["result"] is None

        station_row = _station_row(conn, station_id)
        assert station_row["last_error"] is not None
        assert station_row["error_count"] == 1

        assert _hold_row_count(conn) == 0, "no access-hold row"

    asyncio.run(run())


# ---------------------------------------------------------------------------
# CB2 — an hourly hold in force does not stop either module's call
# ---------------------------------------------------------------------------


def _disable_all_feeds(conn, site_id: int) -> None:  # type: ignore[no-untyped-def]
    """Disable every seeded feed for ``site_id``.

    CB2 seeds no feeds of its own, but ``init_db`` seeds the default feed
    roster, and both ``run_catchup`` and ``run_backfill_site`` fetch any
    *enabled* feed due for the site regardless of whether the test cares
    about it. Disabling them keeps each CB2 test's one real network
    surface to weather.com's station-history call (patched below) with no
    live Open-Meteo request left unstubbed.
    """
    feed_ids = [row["id"] for row in conn.execute("SELECT id FROM feeds").fetchall()]
    conn.executemany(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
        [(site_id, feed_id) for feed_id in feed_ids],
    )


def _success_fetch_range():  # type: ignore[no-untyped-def]
    calls = {"n": 0}

    async def fake(*args: object, **kwargs: object) -> list[PwsObservation]:
        calls["n"] += 1
        return [
            PwsObservation(
                variable="temperature",
                valid_at=str(kwargs["window_start"]),
                value=10.0,
                source_raw="{}",
            )
        ]

    return calls, fake


def test_cb2_catchup_still_calls_while_hourly_is_held(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A hold already covering ``hourly`` does not stop catchup's station-
    history call: catchup reserves and checks only
    ``check_domain_backoff``, never ``check_access_hold``.

    Mutant (catchup reads the hourly hold) -> at "catchup still made its
    call" below: correct = 1, mutant = 0 (a ``check_access_hold(conn,
    "hourly")`` call added to ``_reserve_station_history_call``, which
    would raise ``AccessHeld`` before any call).
    """
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, pws_id="ISTATION01")
        _disable_all_feeds(conn, site_id)
        record_access_refused(conn, "hourly", 401)
        enqueue_if_absent(conn, "catchup", None, "catchup", {})

        calls, fake = _success_fetch_range()
        with patch("wxverify.worker.catchup.fetch_hourly_history_range", fake):
            job_id = await _run_main_job(db)

        assert calls["n"] == 1, "catchup still made its call"

        # The hold-ignoring call succeeded, so the catchup job completes
        # normally -- same success path as a plain run with no hold
        # (run_catchup's loop reaches the end with no exception and
        # returns None, so _complete_and_continue marks it completed with
        # the "ok" success marker; processor.py:539).
        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "completed"
        assert job_row["result"] == "ok"
        assert job_row["retry_count"] == 0

    asyncio.run(run())


def test_cb2_backfill_still_calls_while_hourly_is_held(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A hold already covering ``hourly`` does not stop backfill's station-
    history call: backfill reserves and checks only
    ``check_domain_backoff``, never ``check_access_hold``.

    Mutant (backfill reads the hourly hold) -> at "backfill still made its
    call" below: correct = 1, mutant = 0 (a ``check_access_hold(conn,
    "hourly")`` call added to ``_reserve_station_history_call``, which
    would raise ``AccessHeld`` before any call).
    """
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, pws_id="ISTATION02")
        _disable_all_feeds(conn, site_id)
        record_access_refused(conn, "hourly", 401)
        enqueue_if_absent(
            conn, "backfill_site", site_id, f"backfill:{site_id}", {"site_id": site_id}
        )

        calls, fake = _success_fetch_range()
        with patch("wxverify.worker.backfill.fetch_hourly_history_range", fake):
            job_id = await _run_main_job(db)

        assert calls["n"] == 1, "backfill still made its call"

        # The hold-ignoring call succeeded and no feed is due (all
        # disabled above), so run_backfill_site returns a continuation
        # (the 30-day window is not covered by one 7-day chunk) but the
        # claimed job itself still completes normally: _complete_and_continue
        # is the only path that writes the "ok" success marker, and it runs
        # for every non-exceptional return, continuation or not
        # (backfill.py:79-149 builds the continuation; processor.py:361-379
        # completes the claimed job in the same write transaction that
        # enqueues it).
        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "completed"
        assert job_row["result"] == "ok"
        assert job_row["retry_count"] == 0

    asyncio.run(run())

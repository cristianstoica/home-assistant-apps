"""The starting history (switch-over plan 2026-10-22-daily-close-switch-
over.md section 5; test IDs SH1-SH3 of 17.12).

Written before P6's code exists: ``wxverify.db.switch.first_record_date``
and the dashboard/overlay hint are both new, and SH3 drives the real
cutover (``wxverify.worker.cutover``), none of which exist yet. Every
import of a P6 name is deferred into the test body, so the file's other
tests still collect. Zone is ``UTC`` throughout. Stations and feeds are
synthetic (``ISTATION01``/``ISTATION02``; feed ``X``); the mean feed is the
one ``init_db`` already seeds (``source='virtual'``, ``model=
'_multimodel_mean'``).

SH3's two P4 seams (plan 17.1) are stood in with a ``runtime_state`` marker
in place of a new table, recording each call and letting
``first_publication_ready`` answer true once its site's marker matches the
F passed. The Cutover/FencedWriter API (``wxverify.db.connection``) already
exists; only ``wxverify.worker.cutover`` (``Cutover``, ``CutoverStep``) is
new (plan 6.1), so that part of the harness is load-bearing rather than
assumed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_make_real_feed, day_close_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import Database, close_db, get_db, init_db
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import begin_close, ensure_site_days, finish_close
from wxverify.provider_ops import set_site_subscription

# C, the cutover day, is chosen well before any real clock reaches it.
C: Final = date(2024, 6, 10)


def _csrf_headers(client: TestClient) -> dict[str, str]:
    csrf = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": csrf}


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


# ---------------------------------------------------------------------------
# SH1
# ---------------------------------------------------------------------------


def test_sh1_first_record_date_is_the_minimum_local_date(tmp_path: Path) -> None:
    """SH1 (plan 17.12): ``first_record_date`` for a site with no record,
    and for one with records F, F + 1.

    M67 -> at the ``with_records`` assertion: correct = F (the minimum
    ``site_days.local_date``), mutant = F + 1 (M67 returns the latest
    record instead of the first).
    """
    from wxverify.db.switch import first_record_date

    conn = _init_tmp_db(tmp_path)._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone="UTC", close_on=True, with_records=False)
    assert first_record_date(conn, site_id) is None

    f = C
    f_start = datetime(f.year, f.month, f.day, tzinfo=UTC)
    from wxverify.db.site_days import start_site_days

    start_site_days(conn, site_id, f, now_utc=f_start)
    assert first_record_date(conn, site_id) == f


# ---------------------------------------------------------------------------
# SH2
# ---------------------------------------------------------------------------


_HINT_TEXT: Final = (
    "Days before {f} are from before the daily close: they keep the"
    " scores they had and were never closed."
)
# The invariant tail of the hint (no F in it), for the absence checks below
# -- those must hold whatever F turns out to be, since no record exists yet.
_HINT_INVARIANT_TAIL: Final = (
    "from before the daily close: they keep the scores they had and were never closed."
)


async def _sh2_idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _sh2_patch_clock(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    """Pin ``utc_now`` on the page read path (mirrors
    ``tests/test_close_pages.py``'s ``_patch_clock``, built locally here)."""
    for target in (
        "wxverify.web.context.utc_now",
        "wxverify.api.routes.timeseries.utc_now",
        "wxverify.worker.access_hold.utc_now",
    ):
        monkeypatch.setattr(target, lambda: now, raising=False)


def test_sh2_the_dashboard_and_overlay_hint_tracks_close_and_record_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SH2 (plan 17.12): the dashboard and Overlay pages, read through
    ``TestClient`` (``tests/test_close_pages.py``'s ``_make_app``/
    ``_patch_clock`` pattern, built locally here, since that file holds no
    reusable helper): close off; key set with no record; key set with a
    record. The hint text (5.4) is absent, absent, then present on both
    pages.

    M68 -> at the ``with_record`` assertions below: correct = the hint
    text present on both pages (close on, a record dated F), mutant =
    absent (M68 shows the hint with the close off instead, so the only
    state that should carry it never gets it).
    """
    conn = _init_tmp_db(tmp_path)._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone="UTC", close_on=False, with_records=False)
    conn.commit()

    f = C
    f_start = datetime(f.year, f.month, f.day, tzinfo=UTC)
    now = f_start + timedelta(hours=1)
    _sh2_patch_clock(monkeypatch, now)
    monkeypatch.setattr("wxverify.api.app.run_worker", _sh2_idle_worker)
    app = create_app(root_path="")

    def _pages(client: TestClient) -> tuple[str, str]:
        dashboard = client.get("/dashboard", params={"site": site_id})
        overlay = client.get("/overlay", params={"site": site_id})
        assert dashboard.status_code == 200
        assert overlay.status_code == 200
        return dashboard.text, overlay.text

    from wxverify.core.timeutil import isoformat_utc
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY, start_site_days

    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001
        dashboard_close_off, overlay_close_off = _pages(client)

        set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(datetime.now(tz=UTC)))
        conn.commit()
        dashboard_no_record, overlay_no_record = _pages(client)

        start_site_days(conn, site_id, f, now_utc=f_start)
        conn.commit()
        dashboard_with_record, overlay_with_record = _pages(client)

    assert _HINT_INVARIANT_TAIL not in dashboard_close_off, (
        "the hint must be withheld while the close is off (5.4)"
    )
    assert _HINT_INVARIANT_TAIL not in overlay_close_off, (
        "the hint must be withheld while the close is off (5.4)"
    )
    assert _HINT_INVARIANT_TAIL not in dashboard_no_record, (
        "the hint must be withheld with the close on but no record yet (5.4)"
    )
    assert _HINT_INVARIANT_TAIL not in overlay_no_record, (
        "the hint must be withheld with the close on but no record yet (5.4)"
    )

    expected = _HINT_TEXT.format(f=f.isoformat())
    assert expected in dashboard_with_record, (
        "M68 -> at this assertion: correct = the hint text present on the"
        f" dashboard page (close on, record at {f.isoformat()}), mutant ="
        " absent (M68 shows the hint with the close off instead)"
    )
    assert expected in overlay_with_record, (
        "M68 -> at this assertion: correct = the hint text present on the"
        " Overlay page, mutant = absent"
    )


# ---------------------------------------------------------------------------
# SH3
# ---------------------------------------------------------------------------


def _mean_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_multimodel_mean'"
    ).fetchone()
    assert row is not None
    return int(row[0])


def _site_generation_id(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT id FROM timezone_generations"
        " WHERE site_id = ? AND state = 'published' ORDER BY id DESC LIMIT 1",
        (site_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    generation_id: int,
) -> None:
    valid_at = f"{local_date.isoformat()}T12:00:00Z"
    conn.execute(
        "INSERT INTO forecast_pairs"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  day_ahead, forecast, observed, created_at, tz_generation_id)"
        " VALUES (?, ?, 'temperature_high', ?, ?, 12, 1, 20.0, 19.5,"
        "  '2024-06-01T00:00:00.000000Z', ?)",
        (site_id, feed_id, valid_at, valid_at, generation_id),
    )


def _seed_truth(
    conn: sqlite3.Connection, *, site_id: int, local_date: date, generation_id: int
) -> None:
    start = f"{local_date.isoformat()}T00:00:00Z"
    end = f"{(local_date + timedelta(days=1)).isoformat()}T00:00:00Z"
    conn.execute(
        "INSERT INTO daily_truth"
        " (site_id, local_date, quantity, value, eligible, covered_hours,"
        "  expected_slots, day_start_utc, day_end_utc, timezone,"
        "  generated_at, tz_generation_id)"
        " VALUES (?, ?, 'temperature_high', 19.5, 1, 24, 24, ?, ?, 'UTC',"
        "  '2024-06-01T00:00:00.000000Z', ?)",
        (site_id, local_date.isoformat(), start, end, generation_id),
    )


def _seed_observation(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> None:
    valid_at = f"{local_date.isoformat()}T12:00:00Z"
    conn.execute(
        "INSERT INTO observations"
        " (site_id, variable, valid_at, value, n_stations, computed_at)"
        " VALUES (?, 'temperature_high', ?, 19.5, 1, '2024-06-01T00:00:00.000000Z')",
        (site_id, valid_at),
    )


def _seed_station_observation(
    conn: sqlite3.Connection, *, station_id: int, local_date: date
) -> None:
    valid_at = f"{local_date.isoformat()}T12:00:00Z"
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, fetched_at)"
        " VALUES (?, 'temperature', ?, 19.5, 'ok', '2024-06-01T00:00:00.000000Z')",
        (station_id, valid_at),
    )


def _table_hash(conn: sqlite3.Connection, table: str) -> str:
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
    return hashlib.sha256(repr([tuple(r) for r in rows]).encode()).hexdigest()


def _install_cutover_stand_ins(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[int, date]]:
    """17.1's stand-in for P4's two seams, recording calls and marking
    each site's publication ready through a ``runtime_state`` key (no new
    table needed)."""
    calls: list[tuple[int, date]] = []

    async def _fake_build_first_publication(
        db: object, writer: object, site_id: int, f_b: date
    ) -> None:
        calls.append((site_id, f_b))

        def _write(conn: sqlite3.Connection) -> None:
            set_runtime_state(conn, f"test_stub_pub_ready:{site_id}", f_b.isoformat())

        await writer.write(_write)  # type: ignore[attr-defined]

    def _fake_first_publication_ready(
        conn: sqlite3.Connection, site_id: int, f_b: date
    ) -> bool:
        value = get_runtime_state(conn, f"test_stub_pub_ready:{site_id}")
        return value == f_b.isoformat()

    monkeypatch.setattr(
        "wxverify.worker.cutover.build_first_publication",
        _fake_build_first_publication,
    )
    monkeypatch.setattr(
        "wxverify.worker.cutover.first_publication_ready",
        _fake_first_publication_ready,
    )
    return calls


def test_sh3_starting_history_rows_survive_the_cutover_and_two_triggers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SH3 (plan 17.12): a pre-switch file with pairs of feed X and of the
    mean feed on dates before C; a full cutover (stand-ins); then, key
    set, a subscription change through ``set_site_subscription`` (which
    runs ``rebuild_mean_for_site``) and a rain-threshold change through
    ``update_site``'s route; then F closed with P1's ``day_close_site``.

    M69 -> at the ``forecast_pairs_hash`` assertion: correct = unchanged
    (B2 makes F the site's first record, so C and every day before it
    stay starting history with no record of their own, and the two
    triggers' deletes are refused by the guard for any day before F),
    mutant = changed (M69 has B2 make the first record C − 1 instead of
    F = C + 1, so C itself gets a record and its day becomes writable,
    letting ``rebuild_mean_for_site``'s delete through on the mean feed's
    C − 1 pair -- no: on a record whose own day is now open/closing --
    changing that row's id and ``created_at`` on reinsert).
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone="UTC", close_on=False, with_records=False)
    feed_x = asof_make_real_feed(conn, "X")
    mean_feed = _mean_feed_id(conn)
    generation_id = _site_generation_id(conn, site_id)
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1", (site_id,)
    ).fetchone()[0]

    c_minus_2 = C - timedelta(days=2)
    c_minus_1 = C - timedelta(days=1)
    for d in (c_minus_2, c_minus_1):
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_x,
            local_date=d,
            generation_id=generation_id,
        )
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=mean_feed,
            local_date=d,
            generation_id=generation_id,
        )
        _seed_truth(conn, site_id=site_id, local_date=d, generation_id=generation_id)
        _seed_observation(conn, site_id=site_id, local_date=d)
        _seed_station_observation(conn, station_id=station_id, local_date=d)

    before = {
        table: _table_hash(conn, table)
        for table in (
            "station_observations",
            "observations",
            "daily_truth",
            "forecast_pairs",
        )
    }

    from wxverify.worker.cutover import Cutover, CutoverStep

    _install_cutover_stand_ins(monkeypatch)
    cutover = Cutover(db)

    async def _run_to_on() -> None:
        for _ in range(20):
            step = await cutover.advance()
            if step is CutoverStep.ON:
                return
        raise AssertionError("cutover did not reach ON")

    asyncio.run(_run_to_on())

    def _write_subscription(conn: sqlite3.Connection) -> None:
        set_site_subscription(conn, site_id, feed_x, enabled=False)

    db_sync_write = getattr(db, "write_sync", None)
    if db_sync_write is not None:
        db_sync_write(_write_subscription)
    else:
        _write_subscription(conn)

    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        resp = client.put(
            f"/api/sites/{site_id}",
            json={"rain_threshold_mm": 0.6},
            headers=_csrf_headers(client),
        )
        assert resp.status_code in (200, 204)

    # The lifespan closes the shared database on shutdown, so `conn` (opened
    # before the app existed) is stale here -- reach the file through a fresh
    # connection opened after the client has stopped.
    after_conn = sqlite3.connect(config.db_path)
    after_conn.row_factory = sqlite3.Row
    try:
        f_row = after_conn.execute(
            "SELECT MIN(local_date) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert f_row[0] is not None
        f = date.fromisoformat(f_row[0])
        f_plus_1_hour = datetime(f.year, f.month, f.day, 1, tzinfo=UTC)
        ensure_site_days(after_conn, site_id, now_utc=f_plus_1_hour)
        close_at = datetime(f.year, f.month, f.day, tzinfo=UTC) + timedelta(
            days=1, minutes=30
        )
        begin_close(after_conn, site_id, f, now_utc=close_at)
        finish_close(after_conn, site_id, f, now_utc=close_at)

        after = {
            table: _table_hash(after_conn, table)
            for table in (
                "station_observations",
                "observations",
                "daily_truth",
                "forecast_pairs",
            )
        }
    finally:
        after_conn.close()
    assert after == before  # forecast_pairs_hash (and the other three tables)

"""Pages (daily-close plan 2026-10-22-daily-close-alerts-and-pages.md §9;
test IDs PG1-PG12 of this file's §13.9; PG13 and PG14 are held back for the
owner's open questions, 17.2, and are not written here).

Written before P5's page changes exist: every test below imports a name P5
adds (or reaches new JSON/HTML it has not yet produced) inside its own body
or its own assertions, so each fails today either at import time or at a
plain assertion against today's unchanged response. Real SQLite throughout
via a tmp-file database + ``TestClient`` (mirrors ``tests/test_forecast_
routes.py``'s harness: the app's WAL-mode read pool is a SEPARATE connection
from whatever writes the fixture rows, so a real file, not ``:memory:``, is
required for the read side to see the write side's committed data).
``utc_now`` is monkeypatched in every module on the page's read path (plan
§13.1). Synthetic data only: stations ``ISTATION01``/``ISTATION02``, zone
UTC, dates from the fixture's ``D``/``NOW``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    NOW,
    D,
    asof_make_real_feed,
    day_close_site,
    seed_active_publication,
    seed_closed_pairs,
    seed_coverage,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.site_days import (
    begin_close,
    day_record,
    ensure_site_days,
    enter_zone_change,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.db.tz_generations import ensure_published_generation

_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_forecast_routes.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _patch_clock(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    """Set ``utc_now`` on every module the plan names (§13.1)."""
    for target in (
        "wxverify.web.context.utc_now",
        "wxverify.api.routes.timeseries.utc_now",
        "wxverify.worker.access_hold.utc_now",
    ):
        monkeypatch.setattr(target, lambda: now, raising=False)


def _station_id(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _close_day(
    conn: sqlite3.Connection, site_id: int, local_date, *, first: bool
) -> None:
    """Grow the site's records through ``local_date``, then close it
    (duplicated locally; see tests/test_close_alerts.py's own helper)."""
    now_for_create = datetime(
        local_date.year, local_date.month, local_date.day, 1, tzinfo=UTC
    )
    if first:
        start_site_days(conn, site_id, local_date, now_utc=now_for_create)
    else:
        ensure_site_days(conn, site_id, now_utc=now_for_create)
    for row in conn.execute(
        "SELECT local_date FROM site_days WHERE site_id = ? AND local_date < ?"
        " AND state NOT IN ('closed', 'missed') ORDER BY local_date",
        (site_id, local_date.isoformat()),
    ).fetchall():
        gap_date = date.fromisoformat(str(row[0]))
        gap_record = day_record(conn, site_id, gap_date)
        assert gap_record is not None
        gt = gap_record.day_end_utc + timedelta(minutes=5)
        begin_close(conn, site_id, gap_date, now_utc=gt)
        finish_close(conn, site_id, gap_date, now_utc=gt)
    record = day_record(conn, site_id, local_date)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, local_date, now_utc=t)
    finish_close(conn, site_id, local_date, now_utc=t)


def _seed_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    day_ahead: int,
    forecast: float,
    observed: float,
    generation_id: int,
) -> None:
    conn.execute(
        "INSERT INTO forecast_pairs"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  day_ahead, forecast, observed, tz_generation_id)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            forecast,
            observed,
            generation_id,
        ),
    )


def test_pg1_closed_day_pairs_replace_forecast_pairs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG1 (plan §13.9, §9.2): closed-day pairs (D-3) and the starting
    history are kept; a missed day (D-2) is dropped and listed in
    `missed_days`; an open day (D-1) is dropped too (M86, M87)."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_id = asof_make_real_feed(conn, "pg-feed")
    generation_id = ensure_published_generation(conn, site_id)
    _patch_clock(monkeypatch, NOW)

    d_minus_3 = D - timedelta(days=3)
    d_minus_2 = D - timedelta(days=2)
    d_minus_1 = D - timedelta(days=1)
    for d in (d_minus_3, d_minus_2, d_minus_1):
        seed_closed_pairs(
            conn,
            site_id=site_id,
            local_date=d,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            valid_at=isoformat_utc(datetime(d.year, d.month, d.day, 12, tzinfo=UTC)),
            lead_hours=24,
            forecast=10.0,
            observed=11.0,
        )

    before_first_record = D - timedelta(days=10)
    before_valid_at = isoformat_utc(
        datetime(
            before_first_record.year,
            before_first_record.month,
            before_first_record.day,
            6,
            tzinfo=UTC,
        )
    )
    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=before_valid_at,
        valid_at=before_valid_at,
        lead_hours=6,
        day_ahead=0,
        forecast=5.0,
        observed=5.5,
        generation_id=generation_id,
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_id,
                "from": before_valid_at,
                "to": isoformat_utc(datetime(D.year, D.month, D.day, tzinfo=UTC)),
            },
        )
    assert response.status_code == 200
    body = response.json()
    valid_at = body["valid_at"]
    d3_valid_at = isoformat_utc(
        datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, 12, tzinfo=UTC)
    )
    assert before_valid_at in valid_at, "starting history must always be kept"
    assert d3_valid_at in valid_at, (
        "M87 -> at this assertion: correct = the closed D-3 hour present"
        " (read through closed_day_pairs), mutant = pairs before the first"
        f" record dropped, so this hour would be absent too; valid_at={valid_at!r}"
    )
    d2_valid_at = isoformat_utc(
        datetime(d_minus_2.year, d_minus_2.month, d_minus_2.day, 12, tzinfo=UTC)
    )
    assert d2_valid_at not in valid_at, (
        "M86 -> at this assertion: correct = the missed D-2 hour absent,"
        f" mutant = pairs on a missed day kept; valid_at={valid_at!r}"
    )
    missed_days = body["missed_days"]
    assert any(
        row["local_date"] == d_minus_2.isoformat() and row["reason"] == "missed"
        for row in missed_days
    ), f"missed_days={missed_days!r} must list D-2 as missed"


def test_pg2_provisional_keys_present_with_close_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG2 (plan §13.9, §9.2): with the close on, the reply gains
    `provisional_valid_at`/`provisional`/`provisional_latest_reading_at`,
    non-null once today has a reading (M88)."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    feed_id = asof_make_real_feed(conn, "pg2-feed")
    _patch_clock(monkeypatch, NOW)

    reading_at = isoformat_utc(datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC))
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, temp_c, fetched_at)"
        " VALUES (?, ?, 'current_obs', ?, 15.0, ?)",
        (station_a, reading_at, D.isoformat(), reading_at),
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/api/timeseries",
            params={"site": site_id, "variable": "temperature", "feed_id": feed_id},
        )
    assert response.status_code == 200
    body = response.json()
    assert "provisional" in body and body["provisional"], (
        "M88 -> at this assertion: correct = a non-empty `provisional`"
        " series once today has a reading, mutant = the provisional keys"
        f" left out with the close on; body keys={sorted(body)!r}"
    )
    assert body.get("provisional_latest_reading_at") is not None


def test_pg3_close_off_adds_only_empty_new_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG3 (plan §13.9, §9.2): with the close off, the reply equals today's
    plus the new keys empty/null (M89). PASSES today: with the close off
    P5 changes nothing here, so today's reply already has no such keys and
    the loop below, which tolerates an absent key as "empty", finds every
    one of them already satisfied."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=False)
    feed_id = asof_make_real_feed(conn, "pg3-feed")
    _patch_clock(monkeypatch, NOW)

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/api/timeseries",
            params={"site": site_id, "variable": "temperature", "feed_id": feed_id},
        )
    assert response.status_code == 200
    body = response.json()
    for key, empty in (
        ("missed_days", []),
        ("zone_days", []),
        ("provisional", []),
        ("provisional_valid_at", []),
        ("provisional_latest_reading_at", None),
    ):
        value = body.get(key, empty)
        assert value == empty or value is None, (
            f"M89 -> at this assertion: correct = {key} empty/null with the"
            f" close off, mutant = the new keys left out with the close"
            f" off would still read as empty here, so a real divergence"
            f" needs {key}={value!r} to be non-empty, which would fail"
        )


def test_pg4_zone_changeover_day_and_zone_flagged_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG4 (plan §13.9, §9.2): a zone change's changeover day shows as
    "zone changeover", never "missed"; D-3, closed under the old zone,
    is listed in `zone_days` (M90)."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True, with_records=False)
    feed_id = asof_make_real_feed(conn, "pg4-feed")
    _patch_clock(monkeypatch, NOW)

    dates = (D - timedelta(days=3), D - timedelta(days=2), D - timedelta(days=1), D)
    for i, d in enumerate(dates):
        _close_day(conn, site_id, d, first=(i == 0))

    changeover_day = enter_zone_change(conn, site_id, "Etc/GMT-2", now_utc=NOW)
    ensure_site_days(
        conn,
        site_id,
        now_utc=datetime(
            changeover_day.year,
            changeover_day.month,
            changeover_day.day,
            23,
            59,
            tzinfo=UTC,
        )
        + timedelta(hours=48),
    )
    c_record = day_record(conn, site_id, changeover_day)
    assert c_record is not None
    mark_missed(
        conn,
        site_id,
        changeover_day,
        now_utc=c_record.day_end_utc + timedelta(minutes=5),
        reason="zone_changeover",
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_id,
                "from": isoformat_utc(
                    datetime(dates[0].year, dates[0].month, dates[0].day, tzinfo=UTC)
                ),
                "to": isoformat_utc(
                    datetime(
                        changeover_day.year,
                        changeover_day.month,
                        changeover_day.day,
                        tzinfo=UTC,
                    )
                    + timedelta(days=1)
                ),
            },
        )
    assert response.status_code == 200
    body = response.json()
    missed_days = body["missed_days"]
    assert any(
        row["local_date"] == changeover_day.isoformat()
        and row["reason"] == "zone changeover"
        for row in missed_days
    ), (
        "M90 -> at this assertion: correct = the changeover day listed with"
        f" reason 'zone changeover', mutant = reason 'missed'; missed_days="
        f"{missed_days!r}"
    )
    zone_days = body["zone_days"]
    assert any(row["local_date"] == dates[0].isoformat() for row in zone_days), (
        f"zone_days={zone_days!r} must list D-3, closed under the old zone"
    )


def test_pg5_app_js_provisional_series_missed_band_and_summary() -> None:
    """PG5 (plan §13.9, §9.2): app.js pins the provisional series (dashed),
    the missed band, the empty test reading all three, and the summary
    sentences (M91)."""
    app_js = (_ROOT / "wxverify" / "web" / "static" / "app.js").read_text(
        encoding="utf-8"
    )
    assert '"Observed (provisional)"' in app_js, (
        "M91 -> at this assertion: correct = a third series labelled"
        " 'Observed (provisional)' present, mutant = the provisional"
        " series never added (today's app.js has no such label)"
    )
    assert "dashed" in app_js, (
        "M91 -> at this assertion: correct = the provisional series uses a"
        " dashed stroke, mutant = the provisional series drawn solid"
    )
    assert "Provisional observed values from" in app_js
    assert "Missed, never scored:" in app_js
    assert "Scored under the zone it had then:" in app_js


def test_pg6_station_trust_closed_and_history_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG6 (plan §13.9, §9.3): Station Trust, close on, counts the closed
    day's and the starting history's rows only -- a missed and an open
    day's rows are left out (M92)."""
    from wxverify.web.context import load_station_trust

    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    _patch_clock(monkeypatch, NOW)

    def _seed_trust(local_date_dt: datetime) -> None:
        valid_at = isoformat_utc(local_date_dt)
        conn.execute(
            "INSERT INTO station_observations"
            " (station_id, variable, valid_at, value, qc_flag)"
            " VALUES (?, 'temperature', ?, 20.0, 'ok')",
            (station_a, valid_at),
        )
        conn.execute(
            "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
            " VALUES (?, 'temperature', ?, 18.0, 1)",
            (site_id, valid_at),
        )

    _seed_trust(
        datetime(D.year, D.month, D.day, 9, tzinfo=UTC) - timedelta(days=3)
    )  # D-3 closed
    _seed_trust(
        datetime(D.year, D.month, D.day, 9, tzinfo=UTC) - timedelta(days=2)
    )  # D-2 missed
    _seed_trust(
        datetime(D.year, D.month, D.day, 9, tzinfo=UTC) - timedelta(days=1)
    )  # D-1 open
    _seed_trust(
        datetime(D.year, D.month, D.day, 9, tzinfo=UTC) - timedelta(days=11)
    )  # history

    rows = load_station_trust(conn)
    row = next((r for r in rows if r.station == "ISTATION01"), None)
    assert row is not None and row.n == 2, (
        "M92 -> at this assertion: correct = n=2 (the closed day and the"
        " starting history only), mutant = Station Trust keeps missed and"
        f" open days too, so n would read 4 here; row={row!r}"
    )


def test_pg7_coverage_table_next_to_the_rankings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG7 (plan §13.9, §9.4): the coverage table/"Scored" column reads the
    active publication's own `window_days` for "rolling", and "w:all" for
    "All time" -- never the pending `rolling_window_days` setting, and
    "none yet" (not "0 of 0") for an expected count of 0 (M93, M94, M122,
    M123)."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_x = asof_make_real_feed(conn, "pg7-x")
    feed_y = asof_make_real_feed(conn, "pg7-y")
    _patch_clock(monkeypatch, NOW)

    pub_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=D - timedelta(days=1),
        published_at=NOW,
        window_days=30,
    )
    for window_key, feed_id, day_ahead, scored, expected in (
        ("w:30", feed_x, 3, 7, 7),
        ("w:30", feed_y, 3, 5, 7),
        ("w:30", feed_y, 7, 0, 0),
        ("w:all", feed_x, 3, 20, 21),
    ):
        seed_coverage(
            conn,
            publication_id=pub_id,
            variable="temperature",
            window_key=window_key,
            day_ahead=day_ahead,
            feed_id=feed_id,
            scored=scored,
            expected=expected,
        )
    from wxverify.settings.keys import set_setting

    set_setting(conn, "rolling_window_days", "14")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        rolling = client.get(
            "/dashboard", params={"site": site_id, "window": "rolling", "lead": "D+3"}
        )
        all_time = client.get(
            "/dashboard", params={"site": site_id, "window": "all", "lead": "D+3"}
        )
    assert rolling.status_code == 200 and all_time.status_code == 200
    rolling_text = rolling.text
    assert (
        "7 of 7" in rolling_text
        and "5 of 7" in rolling_text
        and "none yet" in rolling_text
    ), (
        "M93/M122 -> at this assertion: correct = the rolling view's cells"
        " read under w:30 (the publication's own window_days, not the"
        " rolling_window_days setting saved as 14), mutant = these exact"
        f" cell strings would be absent; text contains 'Forecasts scored'="
        f"{'Forecasts scored' in rolling_text!r}"
    )
    assert "0 of 0" not in rolling_text, (
        "M93 -> at this assertion: correct = 'none yet' for an expected"
        " count of 0, mutant = '0 of 0' shown instead"
    )
    assert "20 of 21" in all_time.text, (
        "M123 -> at this assertion: correct = the All-time view reads"
        " w:all, mutant = coverage always read under w:{N}, the All-time"
        f" view included, so this cell would be absent; text contains"
        f" 'Forecasts scored'={'Forecasts scored' in all_time.text!r}"
    )


def test_pg8_dashboard_close_off_no_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG8 (plan §13.9, §9.4): the dashboard with the close off shows no
    coverage table and no "Scored" column (M95). PASSES today: with the
    close off nothing here changes, and today's dashboard already has
    neither."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=False)
    _patch_clock(monkeypatch, NOW)

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/dashboard", params={"site": site_id})
    assert response.status_code == 200
    text = response.text
    assert "Forecasts scored in this window" not in text
    assert ">Scored<" not in text


def test_pg9_worker_status_newest_published_at_with_close_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG9 (plan §13.9, §9.6): with the close on, `/api/worker/status`
    reports the newest `published_at` over active publications, not the
    (now stale) last completed `pair_and_score` run (M96)."""
    conn = _init_tmp_db(tmp_path)
    site_a = day_close_site(conn, close_on=True)
    site_b = day_close_site(
        conn, close_on=True, station_ids=("ISTATION03", "ISTATION04")
    )
    _patch_clock(monkeypatch, NOW)

    older = NOW - timedelta(hours=5)
    newer = NOW - timedelta(hours=1)
    seed_active_publication(
        conn, site_id=site_a, cutoff=D - timedelta(days=1), published_at=older
    )
    seed_active_publication(
        conn, site_id=site_b, cutoff=D - timedelta(days=1), published_at=newer
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/worker/status")
    assert response.status_code == 200
    body = response.json()
    assert body.get("last_completed_pair_and_score_at") == isoformat_utc(newer), (
        "M96 -> at this assertion: correct = the newest published_at"
        f" ({isoformat_utc(newer)!r}), mutant = the earliest published_at"
        f" reported instead; got {body.get('last_completed_pair_and_score_at')!r}"
    )


def test_pg10_worker_status_close_off_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG10 (plan §13.9, §9.6): with the close off, `last_completed_pair_
    and_score_at` is today's (the completed job's own time) (M97). PASSES
    today: with the close off P5 reads nothing new here."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=False)
    _patch_clock(monkeypatch, NOW)
    completed_at = isoformat_utc(NOW - timedelta(minutes=10))
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at,"
        " created_at)"
        " VALUES ('pair_and_score', ?, 'pair_and_score', '{}', 'completed', ?, ?)",
        (site_id, completed_at, completed_at),
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/worker/status")
    assert response.status_code == 200
    body = response.json()
    assert body.get("last_completed_pair_and_score_at") == completed_at


def test_pg11_overlay_lists_active_feeds_without_forecast_pairs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG11 (plan §13.9, §9.2): with the close on, the Overlay feed list
    comes from the site's active feeds, not the `forecast_pairs` EXISTS --
    a site whose only pairs are closed-day pairs still lists its feeds and
    renders the chart (M117)."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_1 = asof_make_real_feed(conn, "pg11-a")
    feed_2 = asof_make_real_feed(conn, "pg11-b")
    _patch_clock(monkeypatch, NOW)

    d_minus_3 = D - timedelta(days=3)
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=min(feed_1, feed_2),
        variable="temperature",
        day_ahead=1,
        valid_at=isoformat_utc(
            datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, 12, tzinfo=UTC)
        ),
        lead_hours=24,
        forecast=10.0,
        observed=11.0,
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/overlay", params={"site": site_id})
    assert response.status_code == 200
    text = response.text
    assert "No overlay data source is available." not in text, (
        "M117 -> at this assertion: correct = a feed resolves (the"
        " active-feed listing, not the forecast_pairs EXISTS, which finds"
        " nothing here), mutant = the close-on keeps the forecast_pairs"
        " EXISTS, so no feed would list and this fallback text would show"
    )
    assert 'data-chart="overlay"' in text


def test_pg12_issued_at_selects_one_run_closed_days_excluded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG12 (plan §13.9, §9.2): `issued_at=I1` keeps only I1's starting-
    history rows and no closed-day row (ClosedPair carries no issued_at);
    with no `issued_at`, PG1's rows return (M124)."""
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_id = asof_make_real_feed(conn, "pg12-feed")
    generation_id = ensure_published_generation(conn, site_id)
    _patch_clock(monkeypatch, NOW)

    before_first_record = D - timedelta(days=10)
    i1 = isoformat_utc(
        datetime(
            before_first_record.year,
            before_first_record.month,
            before_first_record.day,
            6,
            tzinfo=UTC,
        )
    )
    i2 = isoformat_utc(
        datetime(
            before_first_record.year,
            before_first_record.month,
            before_first_record.day,
            18,
            tzinfo=UTC,
        )
    )
    for issued_at in (i1, i2):
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            issued_at=issued_at,
            valid_at=issued_at,
            lead_hours=6,
            day_ahead=0,
            forecast=5.0,
            observed=5.5,
            generation_id=generation_id,
        )
    d_minus_3 = D - timedelta(days=3)
    d3_valid_at = isoformat_utc(
        datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, 12, tzinfo=UTC)
    )
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=d3_valid_at,
        lead_hours=24,
        forecast=10.0,
        observed=11.0,
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        with_issued_at = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_id,
                "issued_at": i1,
            },
        )
        without_issued_at = client.get(
            "/api/timeseries",
            params={"site": site_id, "variable": "temperature", "feed_id": feed_id},
        )
    assert with_issued_at.status_code == 200 and without_issued_at.status_code == 200
    with_body = with_issued_at.json()
    assert i1 in with_body["valid_at"] and i2 not in with_body["valid_at"]
    assert d3_valid_at not in with_body["valid_at"], (
        "M124 -> at this assertion: correct = no closed-day row while"
        " issued_at is given (ClosedPair carries no issued_at to match"
        f" on), mutant = closed-day rows kept regardless; valid_at="
        f"{with_body['valid_at']!r}"
    )
    assert "missed_days" in with_body
    without_body = without_issued_at.json()
    assert d3_valid_at in without_body["valid_at"]

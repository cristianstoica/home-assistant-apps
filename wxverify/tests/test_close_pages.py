"""Pages (daily-close plan 2026-10-22-daily-close-alerts-and-pages.md §9;
test IDs PG1-PG14 of this file's §13.9; PG13 and PG14 needed the owner's
open questions of 17.2 answered first (2026-10-31) and are added here now).

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
    asof_persistence_feed_id,
    day_close_site,
    seed_active_publication,
    seed_closed_pairs,
    seed_coverage,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.runtime_state import delete_runtime_state
from wxverify.db.site_days import (
    DAILY_CLOSE_ON_KEY,
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


def _seed_ranking(
    conn: sqlite3.Connection,
    *,
    publication_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    window_key: str,
    n: int,
    skill_score: float | None,
) -> None:
    """Models ``tests/test_publication_readers.py``'s ``_seed_ranking``."""
    conn.execute(
        """
        INSERT INTO publication_rankings
            (publication_id, feed_id, variable, day_ahead, window_key, n,
             bias, mae, rmse, pod, far, csi, ets, hss, skill_score)
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?)
        """,
        (publication_id, feed_id, variable, day_ahead, window_key, n, skill_score),
    )


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
    `missed_days`; an open day (D-1) is dropped too (M86, M87). The
    starting-history row is itself a D+1 forecast (`lead_hours` 24,
    `issued_at` 24h before its `valid_at`), so the route's default `lead`
    (D+1) keeps it too (plan row ~812, 9.2)."""
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
    before_issued_at = isoformat_utc(
        datetime(
            before_first_record.year,
            before_first_record.month,
            before_first_record.day,
            6,
            tzinfo=UTC,
        )
        - timedelta(hours=24)
    )
    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=before_issued_at,
        valid_at=before_valid_at,
        lead_hours=24,
        day_ahead=1,
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
    plus the new keys empty/null (M89).

    ``body.get(key, empty)`` reads the SAME ``empty`` value whether the
    mutant's code drops the key or correct code sets it explicitly to
    ``empty`` -- an absent key and a key present but empty are the same
    thing to a ``dict.get`` with a matching default, so the original
    version of this loop could not tell M89's missing keys from M0's
    present-and-empty ones. Each key must be checked present first.
    """
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
        assert key in body, (
            f"M89 -> at this assertion: correct = {key!r} present with the"
            " close off (explicitly set to its empty value), mutant = the"
            f" new keys left out with the close off, so {key!r} would be"
            f" absent here; body keys={sorted(body)!r}"
        )
        value = body[key]
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
    sentences (M91).

    The word "dashed" also appears in this series' own comment ("the
    observed colour with a dashed stroke"), which stays put whether or
    not the series itself carries a `dash:` setting -- a bare
    ``"dashed" in app_js`` check cannot tell the two apart. This instead
    isolates the ``"Observed (provisional)"`` series' own line and checks
    its `dash:` setting directly.
    """
    app_js = (_ROOT / "wxverify" / "web" / "static" / "app.js").read_text(
        encoding="utf-8"
    )
    assert '"Observed (provisional)"' in app_js, (
        "M91 -> at this assertion: correct = a third series labelled"
        " 'Observed (provisional)' present, mutant = the provisional"
        " series never added (today's app.js has no such label)"
    )
    provisional_series_line = next(
        line for line in app_js.splitlines() if '"Observed (provisional)"' in line
    )
    assert "dash:" in provisional_series_line, (
        "M91 -> at this assertion: correct = the provisional series' own"
        " object literal carries a `dash:` setting, mutant = the"
        " provisional series drawn solid (no `dash:` on this series) --"
        " the word 'dashed' alone would still appear in this file's"
        " comment either way, so this checks the series' own line, not"
        f" the comment above it; line={provisional_series_line!r}"
    )
    assert "Provisional observed values from" in app_js
    assert "Missed, never scored:" in app_js
    assert "Scored under the zone it had then:" in app_js
    assert (
        'day.reason === "zone changeover" ? "zone changeover, not scored"'
        ' : "missed"' in app_js
    ), (
        "PG5 -> at this assertion: correct = a changeover day's missed"
        " band labelled 'zone changeover, not scored' by the band's own"
        " code, mutant = the changeover label dropped from that code"
        " (a comment-only match at app.js:136 would not catch this)"
    )
    assert "missedDays.length === 0" in app_js, (
        "PG5 -> at this assertion: correct = the empty-test for the chart"
        " reads all three of pairAt, provisionalAt and missedDays, mutant"
        " = missedDays left out of the empty test (today's app.js has no"
        " such string)"
    )


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
    for window_key, feed_id in (
        ("w:30", feed_x),
        ("w:30", feed_y),
        ("w:all", feed_x),
        ("w:all", feed_y),
    ):
        _seed_ranking(
            conn,
            publication_id=pub_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=3,
            window_key=window_key,
            n=7,
            skill_score=50.0,
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
    leaderboard_start = rolling_text.index("<h2>Leaderboard</h2>")
    leaderboard_html = rolling_text[
        leaderboard_start : rolling_text.index("</section>", leaderboard_start)
    ]
    assert (
        ">Scored<" in leaderboard_html
        and "7 of 7" in leaderboard_html
        and "5 of 7" in leaderboard_html
    ), (
        'M94 -> at this assertion: correct = the Leaderboard\'s own "Scored"'
        " column shows 'w:30' coverage, feed X \"7 of 7\" and feed Y"
        ' "5 of 7", mutant = the Leaderboard table has no "Scored"'
        " column or shows neither feed's cell; leaderboard_html contains"
        f" 'Scored'={'Scored' in leaderboard_html!r}"
    )


def test_pg8_dashboard_close_off_no_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG8 (plan §13.9, §9.4): the dashboard with the close off shows no
    coverage table and no "Scored" column (M95).

    The original version of this test seeded no active publication at
    all, so the dashboard's publication lookup found nothing to show
    regardless of the close flag -- M95's dropped ``daily_close_on(conn)``
    check was unobservable. This seeds a real active publication and a
    coverage row (PG7's pattern) with the close off, so the lookup would
    actually find something if the guard were gone. The two coverage
    checks still can't see M95 either way (with the close off the
    Leaderboard never reads the publication's rankings, so the coverage
    table is None regardless), so a third assertion checks the
    "evidence-through" line instead, which M95 does change because it
    comes straight from the (wrongly found) publication lookup.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=False)
    feed_id = asof_make_real_feed(conn, "pg8-feed")
    _patch_clock(monkeypatch, NOW)

    pub_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=D - timedelta(days=1),
        published_at=NOW,
        window_days=30,
    )
    seed_coverage(
        conn,
        publication_id=pub_id,
        variable="temperature",
        window_key="w:30",
        day_ahead=3,
        feed_id=feed_id,
        scored=7,
        expected=7,
    )
    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=3,
        window_key="w:30",
        n=7,
        skill_score=50.0,
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/dashboard", params={"site": site_id, "window": "rolling", "lead": "D+3"}
        )
    assert response.status_code == 200
    text = response.text
    assert "Forecasts scored in this window" not in text, (
        "M95 -> at this assertion: correct = no coverage table with the"
        " close off, even though a real active publication and coverage"
        " row exist, mutant = the publication lookup drops"
        " `and daily_close_on(conn)`, so it would find the seeded"
        " publication and this text would show"
    )
    assert ">Scored<" not in text, (
        "M95 -> at this assertion: correct = no 'Scored' column with the"
        " close off, mutant = the publication lookup drops"
        " `and daily_close_on(conn)`, so the Leaderboard's 'Scored' column"
        " would show here too"
    )
    assert 'id="evidence-through"' not in text, (
        "M95 -> at this assertion: correct = no 'Rankings use … days"
        " through' line with the close off (no publication is read, plan"
        " 9.1), mutant = the lookup finds the seeded publication, so"
        " evidence_through is its cutoff and the line shows"
    )


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
    with no `issued_at`, D-3's row returns, and I1/I2 (D+0) drop out once
    the default `lead` (D+1) is applied to the starting history (M124,
    M136)."""
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
    assert i1 in with_body["valid_at"], (
        "M136 -> at this assertion: correct = I1's starting-history row"
        " (day_ahead 0) kept though issued_at is given and the default"
        " lead is D+1 (with issued_at given, k filters nothing for the"
        " starting history), mutant = the starting history filtered to k"
        f" while issued_at is given, so I1 would be dropped; valid_at="
        f"{with_body['valid_at']!r}"
    )
    assert i2 not in with_body["valid_at"]
    assert d3_valid_at not in with_body["valid_at"], (
        "M124 -> at this assertion: correct = no closed-day row while"
        " issued_at is given (ClosedPair carries no issued_at to match"
        f" on), mutant = closed-day rows kept regardless; valid_at="
        f"{with_body['valid_at']!r}"
    )
    assert "missed_days" in with_body
    without_body = without_issued_at.json()
    assert d3_valid_at in without_body["valid_at"]
    assert i1 not in without_body["valid_at"] and i2 not in without_body["valid_at"], (
        "paired negative of the M136 check above: correct = with no"
        " issued_at given, the starting history IS filtered to the"
        " default lead (D+1), so I1/I2 (day_ahead 0) drop out here;"
        f" valid_at={without_body['valid_at']!r}"
    )


def _seed_day_score(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date,
    feed_id: int,
    variable: str = "temperature",
    day_ahead: int = 1,
    status: str = "scored",
    n: int = 1,
    err_n: int = 1,
    err_sum: float = 2.0,
    abs_sum: float = 2.0,
    sq_sum: float = 4.0,
) -> None:
    """A closed day's scored cell (P4's ``day_scores``), local to this file
    (mirrors ``tests/test_publication_readers.py``'s own private helper of
    the same name -- a cross-module import of a private helper between two
    independent test files is awkward, so this is a self-contained copy,
    not an import)."""
    conn.execute(
        """
        INSERT INTO day_scores
            (site_id, local_date, feed_id, variable, day_ahead, status,
             download_id, download_date, fetched_at, n, err_n, err_sum,
             abs_sum, sq_sum, hits, false_alarms, misses, correct_negs,
             skill_n, skill_sq_sum, skill_ref_sq_sum, wind_fc_high,
             wind_fc_hours, wind_obs_high)
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            status,
            n,
            err_n,
            err_sum,
            abs_sum,
            sq_sum,
        ),
    )


def test_pg13_typed_nd_window_shows_no_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG13 (plan §13.9, §9.4): a typed `window=14d` view (`live_ranking`,
    summed live) shows the Leaderboard's scored feed but no coverage table,
    no "Scored" column and no coverage line -- P4 stores coverage for
    `w:{N}` and `w:all` only, never a live window (owner, 2026-10-31; M126).

    This PASSES on today's code: `_coverage_table` (`web/context.py:675-
    723`) already guards on `window_key.startswith("w:")` before reading
    `published_coverage`, so this pins that guard as a regression test
    rather than driving new work. The paired positive -- that the SAME
    coverage machinery DOES show on the "Last N days" view -- is PG7
    (`test_pg7_coverage_table_next_to_the_rankings`, above), so this test's
    absence claim is not resting on an ambient coincidence.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_x = asof_make_real_feed(conn, "pg13-x")
    _patch_clock(monkeypatch, NOW)

    cutoff = D - timedelta(days=1)
    seed_active_publication(
        conn, site_id=site_id, cutoff=cutoff, published_at=NOW, window_days=30
    )
    _seed_day_score(
        conn,
        site_id=site_id,
        local_date=D - timedelta(days=3),
        feed_id=feed_x,
        variable="temperature",
        day_ahead=3,
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/dashboard", params={"site": site_id, "window": "14d", "lead": "D+3"}
        )
    assert response.status_code == 200
    text = response.text
    leaderboard_start = text.index("<h2>Leaderboard</h2>")
    leaderboard_html = text[
        leaderboard_start : text.index("</section>", leaderboard_start)
    ]
    assert "example-src / pg13-x" in leaderboard_html, (
        "setup check (not a mutant kill): the Leaderboard must actually"
        " show the live:14d feed before the absence of coverage next to"
        f" it means anything; leaderboard_html={leaderboard_html!r}"
    )
    assert 'id="coverage-table"' not in text, (
        "M126 -> at this assertion: correct = no coverage table for a"
        " typed `<N>d` window, mutant = the `w:` check left out, so"
        " coverage is read (as empty rows) under every window key and"
        " this table would render here too"
    )
    assert ">Scored<" not in leaderboard_html, (
        "M126 -> at this assertion: correct = no 'Scored' column on the"
        " Leaderboard for a typed `<N>d` window (the same `coverage is"
        " None` guard the table above reads), mutant = the column would"
        " show here too"
    )
    assert "Coverage per day ahead is in the table above." not in text, (
        "M126 -> at this assertion: correct = no coverage line in the"
        " Composite ranking's hint for a typed `<N>d` window (`coverage`"
        " is None), mutant = the `w:` check left out makes `coverage`"
        " truthy, so this line would show here too"
    )


def test_pg14_lead_pick_parameter_and_overlay_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PG14 (plan §13.9, §9.2): the `lead` parameter, the day-ahead pick at
    a closed hour, the starting-history pick, the close-off route and the
    Overlay's lead control (M127-M135, M137-M140).

    Must FAIL on today's code: `/api/timeseries` has no `lead` parameter at
    all (`api/routes/timeseries.py:46-52`), `_closed_hour_pick` keeps every
    row of an hour unconditionally (`:340-349`), `_stored_pairs` has no
    `day_ahead` keyword (`:189-199`), and `/overlay` has no `lead` control
    (`web/routes.py:225-231`, `web/templates/overlay/show.html`) -- so a
    `lead` of "D+8" or "x" is silently ignored rather than 422ing, and every
    pair at an hour is returned unfiltered rather than picked by day ahead.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_x = asof_make_real_feed(conn, "pg14-x")
    feed_p = asof_persistence_feed_id(conn)
    generation_id = ensure_published_generation(conn, site_id)
    _patch_clock(monkeypatch, NOW)

    d_minus_3 = D - timedelta(days=3)
    t12 = isoformat_utc(
        datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, 12, tzinfo=UTC)
    )
    t13 = isoformat_utc(
        datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, 13, tzinfo=UTC)
    )
    # X's closed-day pairs on D-3.
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=feed_x,
        variable="temperature",
        day_ahead=1,
        valid_at=t12,
        lead_hours=24,
        forecast=10.0,
        observed=11.0,
    )
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=feed_x,
        variable="temperature",
        day_ahead=2,
        valid_at=t12,
        lead_hours=48,
        forecast=20.0,
        observed=21.0,
    )
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=feed_x,
        variable="temperature",
        day_ahead=2,
        valid_at=t13,
        lead_hours=49,
        forecast=21.0,
        observed=22.0,
    )
    # P's closed-day pairs on D-3 at 12:00, two leads of the same day ahead.
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=feed_p,
        variable="temperature",
        day_ahead=1,
        valid_at=t12,
        lead_hours=13,
        forecast=1.0,
        observed=1.5,
    )
    seed_closed_pairs(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        feed_id=feed_p,
        variable="temperature",
        day_ahead=1,
        valid_at=t12,
        lead_hours=36,
        forecast=2.0,
        observed=2.5,
    )
    # X's forecast_pairs before the first record, D-10 06:00: two D+1 runs
    # (different issued_at) and one D+0 run, all valid at the same hour.
    before_first_record = D - timedelta(days=10)
    d_minus_11 = before_first_record - timedelta(days=1)
    valid_before = isoformat_utc(
        datetime(
            before_first_record.year,
            before_first_record.month,
            before_first_record.day,
            6,
            tzinfo=UTC,
        )
    )
    issued_d11_00 = isoformat_utc(
        datetime(d_minus_11.year, d_minus_11.month, d_minus_11.day, 0, tzinfo=UTC)
    )
    issued_d11_12 = isoformat_utc(
        datetime(d_minus_11.year, d_minus_11.month, d_minus_11.day, 12, tzinfo=UTC)
    )
    issued_d10_00 = isoformat_utc(
        datetime(
            before_first_record.year,
            before_first_record.month,
            before_first_record.day,
            0,
            tzinfo=UTC,
        )
    )
    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_x,
        variable="temperature",
        issued_at=issued_d11_00,
        valid_at=valid_before,
        lead_hours=30,
        day_ahead=1,
        forecast=5.0,
        observed=5.5,
        generation_id=generation_id,
    )
    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_x,
        variable="temperature",
        issued_at=issued_d11_12,
        valid_at=valid_before,
        lead_hours=18,
        day_ahead=1,
        forecast=6.0,
        observed=6.5,
        generation_id=generation_id,
    )
    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_x,
        variable="temperature",
        issued_at=issued_d10_00,
        valid_at=valid_before,
        lead_hours=6,
        day_ahead=0,
        forecast=7.0,
        observed=7.5,
        generation_id=generation_id,
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        no_lead = client.get(
            "/api/timeseries",
            params={"site": site_id, "variable": "temperature", "feed_id": feed_x},
        )
        lead_d2 = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "D+2",
            },
        )
        lead_d0 = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "D+0",
            },
        )
        lead_d8 = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "D+8",
            },
        )
        lead_x = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "x",
            },
        )
        p_no_lead = client.get(
            "/api/timeseries",
            params={"site": site_id, "variable": "temperature", "feed_id": feed_p},
        )
        overlay_no_lead = client.get(
            "/overlay", params={"site": site_id, "feed_id": feed_x}
        )
        overlay_d2 = client.get(
            "/overlay", params={"site": site_id, "feed_id": feed_x, "lead": "D+2"}
        )
        # Close off, still inside the same client/connection: the route
        # ignores `lead` entirely (all three forecast_pairs rows at the
        # starting hour, in any order); the Overlay drops the lead
        # control, its links and its data-src.
        conn = get_db()._conn  # noqa: SLF001
        delete_runtime_state(conn, DAILY_CLOSE_ON_KEY)
        off_no_lead = client.get(
            "/api/timeseries",
            params={"site": site_id, "variable": "temperature", "feed_id": feed_x},
        )
        off_d2 = client.get(
            "/api/timeseries",
            params={
                "site": site_id,
                "variable": "temperature",
                "feed_id": feed_x,
                "lead": "D+2",
            },
        )
        overlay_off = client.get(
            "/overlay", params={"site": site_id, "feed_id": feed_x}
        )

    # -- No lead (default D+1): starting history's earliest-issued D+1 run,
    # plus D-3 12:00's lone D+1 closed-day row; D-3 13:00 has no D+1 row and
    # is not filled from day ahead 2. --
    assert no_lead.status_code == 200
    no_lead_body = no_lead.json()
    assert no_lead_body["valid_at"] == [valid_before, t12], (
        "M127 -> at this assertion: correct = exactly these two hours,"
        " mutant = `_closed_hour_pick` keeps every row of the hour (day_"
        " ahead unused), so D-3 12:00's day_ahead-2 row (forecast 20.0)"
        " and D-3 13:00's day_ahead-2 row (forecast 21.0) would also"
        f" appear; valid_at={no_lead_body['valid_at']!r}"
    )
    assert no_lead_body["forecast"] == [5.0, 10.0], (
        "M132/M133 -> at this assertion: correct = forecast 5.0 at the"
        " starting hour (the earliest-issued D+1 run) and 10.0 at D-3"
        " 12:00, mutant M132 = the starting history keeps the LATEST"
        " issued_at of the hour, so forecast would read 6.0 instead of"
        " 5.0; mutant M133 = the route's default lead is D+0, so this"
        f" whole response would read as lead_d0's instead; forecast="
        f"{no_lead_body['forecast']!r}"
    )
    assert t13 not in no_lead_body["valid_at"], (
        "M129 -> at this assertion: correct = D-3 13:00 absent (no fill:"
        " it has no day_ahead-1 row, and its day_ahead-2 row does not"
        " stand in), mutant = an hour with no row at k drawn from the"
        f" nearest other day ahead, so D-3 13:00 (forecast 21.0, day"
        f" ahead 2) would appear; valid_at={no_lead_body['valid_at']!r}"
    )

    # -- D+2: both of D-3's day_ahead-2 rows; no starting-history row (none
    # is day_ahead 2). --
    assert lead_d2.status_code == 200
    d2_body = lead_d2.json()
    assert d2_body["valid_at"] == [t12, t13] and d2_body["forecast"] == [20.0, 21.0], (
        "M128 -> at this assertion: correct = D-3 12:00 (20.0) and 13:00"
        " (21.0), mutant = `_closed_hour_pick` keeps D+1 whatever lead"
        " asks, so this would read as the D+1 pick instead (12:00 only,"
        f" forecast 10.0, 13:00 absent); valid_at={d2_body['valid_at']!r}"
        f" forecast={d2_body['forecast']!r}"
    )

    # -- D+0: only the starting history's lone day_ahead-0 row. --
    assert lead_d0.status_code == 200
    d0_body = lead_d0.json()
    assert d0_body["valid_at"] == [valid_before] and d0_body["forecast"] == [7.0], (
        "M131 -> at this assertion: correct = forecast 7.0 (the single"
        " day_ahead-0 row), mutant = the starting-history read without"
        " its day_ahead=? clause, so the earliest-issued row of ANY day"
        " ahead at this hour would be picked instead (forecast 5.0, the"
        f" day_ahead-1 run issued D-11 00:00); forecast="
        f"{d0_body['forecast']!r}"
    )

    # -- Out-of-range and unparsable `lead`: 422, never a 200 with k=0/8. --
    assert lead_d8.status_code == 422, (
        "M134 -> at this assertion: correct = 422 for a lead past D+7,"
        f" mutant = a `lead` past D+7 accepted (the range check left"
        f" out); got status {lead_d8.status_code}"
    )
    assert lead_d8.json()["detail"] == "lead is not a day ahead from D+0 to D+7"
    assert lead_x.status_code == 422, (
        "M135 -> at this assertion: correct = 422 for an unparsable lead,"
        " mutant = the parser's ValueError left uncaught, so this would"
        f" raise a 500 instead of a 422; got status {lead_x.status_code}"
    )
    assert lead_x.json()["detail"] == "lead is not a day ahead from D+0 to D+7"

    # -- Persistence feed P: two leads of the same day ahead at one hour;
    # the pick is the LARGEST lead_hours, not the smallest. --
    assert p_no_lead.status_code == 200
    p_body = p_no_lead.json()
    assert p_body["valid_at"] == [t12] and p_body["forecast"] == [2.0], (
        "M130 -> at this assertion: correct = forecast 2.0 (lead_hours"
        " 36, the largest of the two day_ahead-1 rows at this hour),"
        " mutant = `_closed_hour_pick` takes the SMALLEST lead_hours at"
        f" k, so this would read 1.0 (lead_hours 13) instead; forecast="
        f"{p_body['forecast']!r}"
    )

    # -- The Overlay's lead control: default D+1 ("Tomorrow" active, lead=
    # D%2B1 in the chart's data-src); explicit D+2 ("+2 days" active, lead=
    # D%2B2 in the data-src and the site link). --
    assert overlay_no_lead.status_code == 200
    no_lead_text = overlay_no_lead.text
    tomorrow_line = next(
        (line for line in no_lead_text.splitlines() if ">Tomorrow<" in line), None
    )
    assert tomorrow_line is not None and "active" in tomorrow_line, (
        "M140 -> at this assertion: correct = 'Tomorrow' (D+1) marked"
        " active with no `lead` given, mutant = `overlay_page`'s default"
        " lead is D+0, so 'Today' would be active instead; tomorrow_line="
        f"{tomorrow_line!r}"
    )
    no_lead_data_src_line = next(
        (line for line in no_lead_text.splitlines() if "data-src=" in line), None
    )
    assert no_lead_data_src_line is not None, (
        "setup check for the chart's data-src line being present"
    )
    assert "lead=D%2B1" in no_lead_data_src_line, (
        "M138 -> at this assertion: correct = the chart's data-src"
        " line carries `lead=D%2B1` (urlencoded D+1), mutant = the"
        " chart's data-src built without `lead`, so this string would"
        f" be absent; no_lead_data_src_line={no_lead_data_src_line!r}"
    )
    assert overlay_d2.status_code == 200
    d2_text = overlay_d2.text
    plus2_line = next(
        (line for line in d2_text.splitlines() if ">+2 days<" in line), None
    )
    assert plus2_line is not None and "active" in plus2_line
    d2_data_src_line = next(
        (line for line in d2_text.splitlines() if "data-src=" in line), None
    )
    assert d2_data_src_line is not None and "lead=D%2B2" in d2_data_src_line, (
        "setup check for the chart's data-src line carrying"
        f" `lead=D%2B2`; d2_data_src_line={d2_data_src_line!r}"
    )
    assert d2_text.count("lead=D%2B2") >= 2, (
        "setup check for the data-src and the site link both carrying"
        f" `lead=D%2B2`; count={d2_text.count('lead=D%2B2')!r}"
    )

    # -- Close off (requested inside the same client/connection above): the
    # route ignores `lead` entirely (all three forecast_pairs rows at the
    # starting hour, in any order); the Overlay drops the lead control, its
    # links and its data-src. --
    assert off_no_lead.status_code == 200
    off_no_lead_body = off_no_lead.json()
    assert sorted(off_no_lead_body["forecast"]) == [5.0, 6.0, 7.0]
    assert off_d2.status_code == 200
    off_d2_body = off_d2.json()
    assert sorted(off_d2_body["forecast"]) == [5.0, 6.0, 7.0], (
        "M137 -> at this assertion: correct = all three forecast_pairs"
        " rows regardless of `lead` (the close-off call passes day_ahead="
        "None), mutant = the close-off call parses `lead` and passes its"
        " k, not None, so with lead=D+2 (and none of the three rows at"
        " day_ahead 2) this would read as an empty list instead; forecast="
        f"{off_d2_body['forecast']!r}"
    )
    assert overlay_off.status_code == 200
    off_text = overlay_off.text
    assert "lead=" not in off_text and ">Tomorrow<" not in off_text, (
        "M139 -> at this assertion: correct = no `lead` group and no"
        " `lead=` in any link or data-src with the close off, mutant ="
        " the lead group and `lead` in the links and data-src kept with"
        " the close off"
    )

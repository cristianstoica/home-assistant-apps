"""Tests: the wind-history lane's weather.com quota, group 3 of plan
section 15.1 (2026-09-30).

Covers T90-T96, T112 and T116 against ``wxverify.collection.wind_quota``,
``wxverify.collection.budget`` and ``wxverify.obs.pws_adapter``'s rate
limiter/deadline helper, as code already written.

Isolation: T90-T92, T112 and T116 drive the pure quota functions directly
over a ``:memory:`` connection + ``run_migrations`` (``_make_db``), mirroring
``tests/test_wind_auth_hold.py``'s sync harness. T93 and T95 are plain
sync/async unit tests of ``WeathercomRateLimiter``/``_get_with_deadline`` with
no database at all. T94 and T96 drive ``run_wind_days`` end to end against a
real tmp-path ``Database``/``FencedWriter`` + ``httpx.MockTransport``,
reusing ``tests/test_wind_auth_hold.py``'s async harness
(``_init_tmp_db``, ``_freeze``, ``_run_wind_days``).

Every station id, site name and coordinate below is synthetic (public
repo): pws station ids ``KTEST001``-``KTEST006``, site name "Testsite",
timezone "UTC", coordinates 0.0/0.0, API key
"0123456789abcdef0123456789abcdef".
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.collection.budget import Reservation
from wxverify.collection.wind_quota import (
    WIND_BACKFILL_CALLS_KEY,
    WIND_LIVE_CALLS_KEY,
    add_lane_calls,
    backfill_headroom,
    lane_counter_key,
    read_lane_counter,
)
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations, seed_default_sources
from wxverify.db.runtime_state import set_runtime_state
from wxverify.obs.pws_adapter import (
    WeathercomRateLimiter,
    _get_with_deadline,  # noqa: PLC2701 -- private, exercised directly
)
from wxverify.settings.keys import set_setting
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    WindHeadroomExhausted,
    _reserve_wind_day_call,
    run_wind_days,
)

_TZ = "UTC"
_API_KEY = "0123456789abcdef0123456789abcdef"


# ---------------------------------------------------------------------------
# Shared sync harness (":memory:" + run_migrations)
# ---------------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    seed_default_sources(conn)
    conn.commit()
    return conn


def _make_site(conn: sqlite3.Connection, *, enabled: bool = True) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone,"
        " enabled) VALUES ('Testsite', 0.0, 0.0, 0.0, ?, ?)",
        (_TZ, 1 if enabled else 0),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _make_station(
    conn: sqlite3.Connection, site_id: int, pws_id: str, *, enabled: bool = True
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        " enabled) VALUES (?, ?, 0.0, 0.0, 0.0, ?)",
        (site_id, pws_id, 1 if enabled else 0),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _set_source(
    conn: sqlite3.Connection, *, daily_call_limit: int, billing_tz: str = "UTC"
) -> None:
    conn.execute(
        "UPDATE sources SET daily_call_limit = ?, billing_tz = ?"
        " WHERE source = 'weathercom'",
        (daily_call_limit, billing_tz),
    )
    conn.commit()


def _set_budget(conn: sqlite3.Connection, billing_day: str, calls: int) -> None:
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls) VALUES"
        " ('weathercom', ?, ?)"
        " ON CONFLICT(source, billing_day) DO UPDATE SET calls = excluded.calls",
        (billing_day, calls),
    )
    conn.commit()


def _set_lane_counter(
    conn: sqlite3.Connection, key: str, entries: dict[str, int]
) -> None:
    set_runtime_state(conn, key, json.dumps(entries))
    conn.commit()


def _set_interval(conn: sqlite3.Connection, minutes: int) -> None:
    set_setting(conn, "obs_interval_minutes", str(minutes))
    conn.commit()


def _budget_calls(conn: sqlite3.Connection, billing_day: str) -> int:
    row = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom' AND billing_day=?",
        (billing_day,),
    ).fetchone()
    return 0 if row is None else int(row["calls"])


# ---------------------------------------------------------------------------
# Shared async harness (real dispatch + MockTransport, tmp-path file DB) --
# identical in shape to tests/test_wind_auth_hold.py's.
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- tests inspect the real writer connection
    seed_default_sources(conn)
    conn.commit()
    return conn


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """Freeze ``utc_now()`` in every module the wind-days job calls it from."""
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)


def _run_wind_days(
    site_id: int, handler: object, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


# ---------------------------------------------------------------------------
# T90 -- backfill_headroom's formula, its None cases and its mutant kills.
# ---------------------------------------------------------------------------


def _t90_fixture(conn: sqlite3.Connection) -> tuple[int, date, str]:
    """2 enabled stations on an enabled site, + a disabled station on it and
    an enabled station on a disabled site -- ``lane_live_calls`` must count
    only the first two."""
    site_id = _make_site(conn)
    _make_station(conn, site_id, "KTEST001")
    _make_station(conn, site_id, "KTEST002")
    _make_station(conn, site_id, "KTEST003", enabled=False)
    other_site_id = _make_site(conn, enabled=False)
    _make_station(conn, other_site_id, "KTEST004")
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    _set_budget(conn, today.isoformat(), 600)
    _set_budget(conn, yesterday.isoformat(), 1200)
    _set_lane_counter(conn, WIND_BACKFILL_CALLS_KEY, {yesterday.isoformat(): 100})
    _set_lane_counter(conn, WIND_LIVE_CALLS_KEY, {yesterday.isoformat(): 30})
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)  # half the billing day left
    return site_id, today, now.isoformat()


def test_t90_backfill_headroom_formula_at_stored_limit_3000() -> None:
    conn = _make_db()
    _t90_fixture(conn)
    _set_source(conn, daily_call_limit=3000)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    headroom = backfill_headroom(conn, now=now)

    assert headroom == 156


def test_t90_backfill_headroom_formula_at_stored_limit_1000() -> None:
    conn = _make_db()
    _t90_fixture(conn)
    _set_source(conn, daily_call_limit=1000)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    headroom = backfill_headroom(conn, now=now)

    assert headroom == -344


def test_t90_no_sources_row_returns_none() -> None:
    conn = _make_db()
    conn.execute("DELETE FROM sources WHERE source = 'weathercom'")
    conn.commit()
    _t90_fixture(conn)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) is None


def test_t90_unloadable_billing_timezone_returns_none() -> None:
    conn = _make_db()
    _t90_fixture(conn)
    _set_source(conn, daily_call_limit=3000, billing_tz="Not/AZone")
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) is None


def test_t90_no_previous_billing_day_row_returns_none() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "KTEST001")
    _set_source(conn, daily_call_limit=3000)
    # No api_budget row for the previous billing day at all.
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) is None


# ---------------------------------------------------------------------------
# T91 -- the lane counters: which one increments, refund decrements the
# right one, two-billing-day retention, unreadable-value reset + WARNING.
# ---------------------------------------------------------------------------


def test_t91_non_exempt_reservation_increments_backfill_only() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    _set_source(conn, daily_call_limit=5000)
    yesterday = date(2026, 3, 9)
    _set_budget(conn, yesterday.isoformat(), 0)
    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)

    with (
        patch("wxverify.worker.wind_days.utc_now", lambda: now),
        patch("wxverify.collection.budget.utc_now", lambda: now),
    ):
        reservation = _reserve_wind_day_call(conn, site_id, station_id, exempt=False)

    assert reservation is not None
    assert read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY) == {"2026-03-10": 1}
    assert read_lane_counter(conn, WIND_LIVE_CALLS_KEY) == {}


def test_t91_exempt_reservation_increments_live_only() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    _set_source(conn, daily_call_limit=5000)
    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)

    with (
        patch("wxverify.worker.wind_days.utc_now", lambda: now),
        patch("wxverify.collection.budget.utc_now", lambda: now),
    ):
        reservation = _reserve_wind_day_call(conn, site_id, station_id, exempt=True)

    assert reservation is not None
    assert read_lane_counter(conn, WIND_LIVE_CALLS_KEY) == {"2026-03-10": 1}
    assert read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY) == {}


def test_t91_a_refund_decrements_the_counter_its_reservation_incremented() -> None:
    conn = _make_db()
    reservation = Reservation(
        source="weathercom", billing_day="2026-03-10", calls=1, credits=0
    )
    _set_lane_counter(conn, WIND_LIVE_CALLS_KEY, {"2026-03-10": 3})
    add_lane_calls(conn, lane_counter_key(exempt=True), reservation.billing_day, -1)
    conn.commit()

    assert read_lane_counter(conn, WIND_LIVE_CALLS_KEY) == {"2026-03-10": 2}
    assert read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY) == {}


def test_t91_counter_keeps_only_the_two_newest_billing_days() -> None:
    conn = _make_db()
    add_lane_calls(conn, WIND_BACKFILL_CALLS_KEY, "2026-03-08", 1)
    add_lane_calls(conn, WIND_BACKFILL_CALLS_KEY, "2026-03-09", 1)
    add_lane_calls(conn, WIND_BACKFILL_CALLS_KEY, "2026-03-10", 1)
    conn.commit()

    assert read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY) == {
        "2026-03-09": 1,
        "2026-03-10": 1,
    }


def test_t91_an_unreadable_counter_resets_to_empty_with_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _make_db()
    set_runtime_state(conn, WIND_BACKFILL_CALLS_KEY, "not json {{{")
    conn.commit()

    with caplog.at_level(logging.WARNING):
        counter = read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY)

    assert counter == {}
    assert any(
        "unreadable wind lane counter" in rec.message for rec in caplog.records
    ), "a WARNING must be logged on an unreadable counter value"

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        add_lane_calls(conn, WIND_BACKFILL_CALLS_KEY, "2026-03-10", 1)
    conn.commit()

    assert read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY) == {"2026-03-10": 1}
    assert any("unreadable wind lane counter" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# T92 -- a non-exempt row at headroom 0 raises WindHeadroomExhausted before
# reserve_budget: api_budget is left unchanged and no call is spent.
# ---------------------------------------------------------------------------


def test_t92_headroom_exhausted_raises_before_reserve_budget_runs() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    # T112's exact headroom-0 fixture: stored 1200, one station (9 calls),
    # previous day 991 calls and no counter entries, nothing spent today,
    # one minute into the billing day.
    _set_source(conn, daily_call_limit=1200)
    yesterday = date(2026, 3, 9)
    today = date(2026, 3, 10)
    _set_budget(conn, yesterday.isoformat(), 991)
    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)

    with (
        patch("wxverify.worker.wind_days.utc_now", lambda: now),
        patch("wxverify.collection.budget.utc_now", lambda: now),
        pytest.raises(WindHeadroomExhausted),
    ):
        _reserve_wind_day_call(conn, site_id, station_id, exempt=False)

    assert _budget_calls(conn, today.isoformat()) == 0, (
        "no reservation may have been made before the headroom check raised"
    )


# ---------------------------------------------------------------------------
# T93 -- the rate limiter with a fake (injectable) clock.
# ---------------------------------------------------------------------------


def test_t93_thirty_requests_fit_in_sixty_seconds_then_the_31st_waits() -> None:
    limiter = WeathercomRateLimiter(clock=lambda: 0.0)
    for _ in range(30):
        assert limiter.try_acquire(backfill=False) == 0.0
    wait = limiter.try_acquire(backfill=False)
    assert wait > 0.0


def test_t93_the_21st_backfill_request_waits_while_live_still_fits() -> None:
    limiter = WeathercomRateLimiter(clock=lambda: 0.0)
    for _ in range(20):
        assert limiter.try_acquire(backfill=True) == 0.0
    # Only 20 of the allowed 30 total requests have been made, so a non-
    # backfill request still fits; the 21st *backfill* request must not.
    wait_backfill = limiter.try_acquire(backfill=True)
    assert wait_backfill > 0.0
    wait_live = limiter.try_acquire(backfill=False)
    assert wait_live == 0.0


def test_t93_a_new_event_loop_gets_a_fresh_limiter() -> None:
    from wxverify.obs.pws_adapter import weathercom_rate_limiter

    seen: list[int] = []

    async def _grab() -> None:
        seen.append(id(weathercom_rate_limiter()))

    asyncio.run(_grab())
    asyncio.run(_grab())

    assert seen[0] != seen[1], "a new loop must not reuse the old limiter instance"


# ---------------------------------------------------------------------------
# T95 -- _get_with_deadline's backfill parameter has no default; the rate
# limiter's wait is outside the deadline's asyncio.timeout.
# ---------------------------------------------------------------------------


def test_t95_get_with_deadline_backfill_parameter_has_no_default() -> None:
    sig = inspect.signature(_get_with_deadline)
    assert sig.parameters["backfill"].default is inspect.Parameter.empty


def test_t95_a_slow_fake_limiter_does_not_cause_provider_deadline_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Shrink the real 5s margin so the deadline (read_seconds + margin) sits
    # well under the fake limiter's 0.2s sleep. At the real margin the
    # deadline is generous enough that the mutant this test targets --
    # acquire() awaited inside asyncio.timeout instead of before it -- does
    # not actually exceed the deadline, so the test would pass either way.
    monkeypatch.setattr(
        "wxverify.obs.pws_adapter.PROVIDER_DEADLINE_MARGIN_SECONDS", 0.01
    )

    class _SlowLimiter:
        async def acquire(self, *, backfill: bool) -> None:
            await asyncio.sleep(0.2)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"observations": []})

    async def _run() -> httpx.Response:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch(
                "wxverify.obs.pws_adapter.weathercom_rate_limiter",
                lambda: _SlowLimiter(),
            ):
                return await _get_with_deadline(
                    client,
                    "https://api.weather.com/v2/pws/observations/current",
                    params={},
                    read_seconds=0.01,
                    backfill=True,
                )

    # The deadline (0.01s read + a shrunk 0.01s margin) is far shorter than
    # the fake limiter's 0.2s sleep; if the acquire() wait were inside
    # asyncio.timeout, this would raise ProviderDeadlineExceeded instead of
    # returning the response.
    response = asyncio.run(_run())
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# T96 -- a simulated pair_max day at the default interval (180 min) makes
# 54 all_1day calls (6 stations x (ceil(1440/180)=8 interval calls + 1 final
# call)).
# ---------------------------------------------------------------------------


def test_t96_a_simulated_day_makes_54_all_1day_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_ids = [_make_station(conn, site_id, f"KTEST{i:03d}") for i in range(1, 7)]
    start = datetime(2026, 3, 10, 0, 0, 0, tzinfo=UTC)

    all_1day_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal all_1day_calls
        all_1day_calls += 1
        return httpx.Response(200, json={"observations": []})

    now = start
    step = timedelta(minutes=5)  # WIND_DAYS_SPACING
    end = start + timedelta(days=1)
    while now < end:
        _freeze(monkeypatch, now)
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)
        now += step

    interval_calls = math.ceil(1440 / 180)
    expected_all_1day = len(station_ids) * (interval_calls + 1)
    assert all_1day_calls == expected_all_1day, (
        f"got {all_1day_calls} all_1day calls, expected {expected_all_1day}"
        f" ({len(station_ids)} stations x ({interval_calls} interval calls"
        " + 1 final call))"
    )


# ---------------------------------------------------------------------------
# T112 -- the lane at headroom 0 with a stored cap below the allowance.
# ---------------------------------------------------------------------------


def test_t112_headroom_zero_with_stored_cap_below_the_allowance() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "KTEST001")
    _set_source(conn, daily_call_limit=1200, billing_tz="UTC")
    previous = date(2026, 3, 9)
    _set_budget(conn, previous.isoformat(), 991)
    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)

    headroom = backfill_headroom(conn, now=now)

    assert headroom == 0


# ---------------------------------------------------------------------------
# T116 -- the lane's live cost counts from the first day, once.
# ---------------------------------------------------------------------------


def _t116_fixture(conn: sqlite3.Connection) -> int:
    site_id = _make_site(conn)
    for i in range(1, 7):
        _make_station(conn, site_id, f"KTEST{i:03d}")
    _set_source(conn, daily_call_limit=3000, billing_tz="UTC")
    today = date(2026, 3, 10)
    _set_budget(conn, today.isoformat(), 500)
    return site_id


def test_t116_day_one_no_counter_entries_yet() -> None:
    conn = _make_db()
    _t116_fixture(conn)
    previous = date(2026, 3, 9)
    _set_budget(conn, previous.isoformat(), 1100)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) == 223


def test_t116_day_two_counters_present_give_the_same_headroom() -> None:
    conn = _make_db()
    _t116_fixture(conn)
    previous = date(2026, 3, 9)
    _set_budget(conn, previous.isoformat(), 1254)
    _set_lane_counter(conn, WIND_BACKFILL_CALLS_KEY, {previous.isoformat(): 100})
    _set_lane_counter(conn, WIND_LIVE_CALLS_KEY, {previous.isoformat(): 54})
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) == 223


def test_t116_day_two_at_a_30_minute_interval() -> None:
    conn = _make_db()
    _t116_fixture(conn)
    _set_interval(conn, 30)
    previous = date(2026, 3, 9)
    _set_budget(conn, previous.isoformat(), 1254)
    _set_lane_counter(conn, WIND_BACKFILL_CALLS_KEY, {previous.isoformat(): 100})
    _set_lane_counter(conn, WIND_LIVE_CALLS_KEY, {previous.isoformat(): 54})
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) == 103


def test_t116_a_20_minute_interval_falls_back_to_the_180_default() -> None:
    conn = _make_db()
    _t116_fixture(conn)
    _set_interval(conn, 20)  # below the floor of 30 -> reads as the default, 180
    previous = date(2026, 3, 9)
    _set_budget(conn, previous.isoformat(), 1254)
    _set_lane_counter(conn, WIND_BACKFILL_CALLS_KEY, {previous.isoformat(): 100})
    _set_lane_counter(conn, WIND_LIVE_CALLS_KEY, {previous.isoformat(): 54})
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    assert backfill_headroom(conn, now=now) == 223

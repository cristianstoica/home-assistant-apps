"""Alert 5, a station silent today (daily-close plan 2026-10-22-daily-close-
alerts-and-pages.md §6.3, §7.3; test IDs SL1-SL8 in this file's §13.5).

Written before ``wxverify.close_monitor.station_silent`` exists: every test
below imports it (or a name it will add) inside its own body, so the rest of
this file keeps collecting -- each then fails at its own
``ModuleNotFoundError`` or ``ImportError``, the failure Step 3 accepts.

Real SQLite throughout (``tests.helpers.asof_conn``, real migrations);
nothing in the code under test is mocked. The only fakes are the clock
(``wxverify.worker.current_obs.utc_now`` and
``wxverify.worker.access_hold.utc_now``, both monkeypatched to a fixed
instant so ``persist_poll_result``/``record_access_refused`` write a known
``last_poll_at``/``since``/``until``) and the httpx responses built locally,
never over the network. Synthetic data only: stations ``ISTATION01``/
``ISTATION02`` (via ``day_close_site``), zone UTC, dates built from the
fixture's ``D``/``NOW``. P1's own functions move every day state; no test
writes ``site_days`` directly.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import httpx
import pytest

from tests.helpers import NOW, D, asof_conn, day_close_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.runtime_state import delete_runtime_state
from wxverify.db.site_days import (
    DAILY_CLOSE_ON_KEY,
    begin_close,
    daily_close_on,
    day_record,
    ensure_site_days,
    finish_close,
    start_site_days,
)
from wxverify.worker.access_hold import clear_access_refused, record_access_refused
from wxverify.worker.current_obs import (
    Health,
    PollOutcome,
    classify_current_obs,
    persist_poll_result,
)

_WEATHER_COM_URL = httpx.URL("https://api.weather.com/v2/pws/observations/current")


def _station_id(conn, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _fake_response(
    status: int, content: bytes | None = None, json_data: object | None = None
) -> httpx.Response:
    """Build an httpx.Response without a network call."""
    if json_data is not None:
        body = json.dumps(json_data).encode()
    elif content is not None:
        body = content
    else:
        body = b""
    return httpx.Response(
        status_code=status,
        content=body,
        headers={"content-type": "application/json"} if json_data is not None else {},
        request=httpx.Request("GET", _WEATHER_COM_URL),
    )


def _persist_online(
    monkeypatch: pytest.MonkeyPatch,
    conn,
    site_id: int,
    station_id: int,
    *,
    poll_at: datetime,
    obs_instant: str,
) -> None:
    monkeypatch.setattr("wxverify.worker.current_obs.utc_now", lambda: poll_at)
    resp = _fake_response(
        200, json_data={"observations": [{"obsTimeUtc": obs_instant}]}
    )
    outcome = classify_current_obs(resp)
    persist_poll_result(conn, site_id, station_id, outcome)


def _persist_offline(
    monkeypatch: pytest.MonkeyPatch,
    conn,
    site_id: int,
    station_id: int,
    *,
    poll_at: datetime,
) -> None:
    monkeypatch.setattr("wxverify.worker.current_obs.utc_now", lambda: poll_at)
    outcome = classify_current_obs(_fake_response(204, content=b""))
    persist_poll_result(conn, site_id, station_id, outcome)


def _close_day(conn, site_id: int, local_date, *, first: bool) -> None:
    """Grow the site's records through ``local_date``, then close it (mirrors
    ``tests/test_close_alerts.py``'s own ``_close_day``, duplicated here since
    that file is another writer's and is never imported from)."""
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


def _set_answered(conn, station_id: int, local_date) -> None:
    """A 204-equivalent 5-minute try: ``last_ok_at`` set, no raw readings."""
    stamp = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC)
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, last_ok_at, updated_at)"
        " VALUES (?, ?, 'fetched', ?, ?)",
        (station_id, local_date.isoformat(), stamp, stamp),
    )


def _set_usable(conn, station_id: int, local_date, variables: tuple[str, ...]) -> None:
    valid_at = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC)
    )
    for variable in variables:
        conn.execute(
            "INSERT INTO station_observations"
            " (station_id, variable, valid_at, value, qc_flag)"
            " VALUES (?, ?, ?, 1.0, 'ok')",
            (station_id, variable, valid_at),
        )


def test_sl1_online_age_ignores_now_threshold_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL1 (plan §13.5, §6.3): an ONLINE station's age is last_poll_at minus
    last_obstime, never `now`, and the threshold trips at >= 2 h (M44, M45).

    This also gives the paired positive for SL6's absence check: here
    STATION_SILENT_PROVISIONAL defaults True and "provisional" IS in the
    message.
    """
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_a = _station_id(conn, site_id, "ISTATION01")

    poll_at = datetime(D.year, D.month, D.day, 14, 0, tzinfo=UTC)
    obs_instant = poll_at - timedelta(hours=2)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_a,
        poll_at=poll_at,
        obs_instant=isoformat_utc(obs_instant),
    )

    # `now` is an hour BEFORE the poll itself: if the age were computed from
    # `now` (M44) rather than from `last_poll_at`, it would read 1 h, under
    # the threshold, and never trip.
    now = poll_at - timedelta(hours=1)
    expected = (
        f"station ISTATION01: newest observation 2 h old at {isoformat_utc(poll_at)}"
        " (threshold 2 h, provisional)"
    )
    alert = station_silent(conn, now, close_on=False)
    assert alert.tripped and expected in (alert.detail or ""), (
        f"M44/M45 -> at this assertion: correct = tripped, {expected!r} in"
        f" detail (age = last_poll_at minus last_obstime = 2 h exactly, >="
        f" the threshold, ignoring `now`), mutant = tripped={alert.tripped},"
        f" detail={alert.detail!r} (M44: an age computed from `now`, 1 h"
        " before the poll, would read 1 h and never trip; M45: '>' in place"
        " of '>=' would not trip exactly at the threshold either)"
    )

    # Paired boundary check: 1 s under the threshold never trips (not a
    # mutant-kill claim on its own -- it rules out an "always trips" oracle).
    conn2 = asof_conn()
    site_id2 = day_close_site(conn2, close_on=False)
    station_a2 = _station_id(conn2, site_id2, "ISTATION01")
    obs_instant2 = poll_at - timedelta(hours=2) + timedelta(seconds=1)
    _persist_online(
        monkeypatch,
        conn2,
        site_id2,
        station_a2,
        poll_at=poll_at,
        obs_instant=isoformat_utc(obs_instant2),
    )
    ok_alert = station_silent(conn2, poll_at, close_on=False)
    assert not ok_alert.tripped


def test_sl2_online_back_off_uses_last_poll_at_not_now(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL2 (plan §13.5, §6.3): ONLINE with a 30-minute-old reading, then
    `now` 5 h later with no further poll (a back-off), stays ok (M44, M46)."""
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_a = _station_id(conn, site_id, "ISTATION01")

    poll_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    obs_instant = poll_at - timedelta(minutes=30)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_a,
        poll_at=poll_at,
        obs_instant=isoformat_utc(obs_instant),
    )

    now = poll_at + timedelta(hours=5)  # 5 h later, no further poll (a back-off)
    alert = station_silent(conn, now, close_on=False)
    assert not alert.tripped, (
        "M44/M46 -> at this assertion: correct = tripped=False (age is"
        " last_poll_at minus last_obstime, 30 min, under the 2 h"
        " threshold), mutant = tripped=True (an online station trips when"
        " `now` minus `last_poll_at` -- 5 h here -- reaches the threshold)"
    )


def test_sl3_offline_age_from_now_and_never_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL3 (plan §13.5, §6.3): OFFLINE's age is `now` minus `last_obstime`,
    not `last_poll_at` minus `last_obstime` (M47); a station with no
    reading yet trips "no observation yet"."""
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")

    now = datetime(D.year, D.month, D.day, 14, 0, tzinfo=UTC)
    obs_instant = now - timedelta(hours=2)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_a,
        poll_at=obs_instant,
        obs_instant=isoformat_utc(obs_instant),
    )
    # The OFFLINE poll itself happens soon after the ONLINE reading: if the
    # age were (wrongly) measured from `last_poll_at` (M47), it would read
    # 10 min here, not 2 h.
    offline_poll_at = obs_instant + timedelta(minutes=10)
    _persist_offline(monkeypatch, conn, site_id, station_a, poll_at=offline_poll_at)

    # B was never polled with a reading at all: OFFLINE from the start, so
    # last_obstime stays NULL.
    _persist_offline(monkeypatch, conn, site_id, station_b, poll_at=now)

    alert = station_silent(conn, now, close_on=False)
    expected_a = (
        "station ISTATION01: weather.com reports no current data; newest"
        " observation 2 h old"
    )
    expected_b = "station ISTATION02: no observation yet"
    detail = alert.detail or ""
    assert alert.tripped and expected_a in detail and expected_b in detail, (
        f"M47 -> at this assertion: correct = tripped, both {expected_a!r}"
        f" and {expected_b!r} present (OFFLINE age is `now` minus"
        " `last_obstime`, 2 h, not `last_poll_at` minus `last_obstime`,"
        f" 10 min), mutant = tripped={alert.tripped}, detail={detail!r}"
    )


def test_sl4_transient_and_terminal_never_judged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL4 (plan §13.5, §6.3): a TRANSIENT (503, and a transport error) and
    a TERMINAL (404) answer after an old reading are never judged (M48)."""
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_a = _station_id(conn, site_id, "ISTATION01")

    now = datetime(D.year, D.month, D.day, 14, 0, tzinfo=UTC)
    old_obs = now - timedelta(hours=10)  # well past the threshold if judged

    outcomes = (
        classify_current_obs(_fake_response(503)),
        PollOutcome(Health.TRANSIENT, error="connect error"),
        classify_current_obs(_fake_response(404)),
    )
    for outcome in outcomes:
        _persist_online(
            monkeypatch,
            conn,
            site_id,
            station_a,
            poll_at=old_obs,
            obs_instant=isoformat_utc(old_obs),
        )
        monkeypatch.setattr("wxverify.worker.current_obs.utc_now", lambda: now)
        persist_poll_result(conn, site_id, station_a, outcome)
        alert = station_silent(conn, now, close_on=False)
        assert not alert.tripped, (
            "M48 -> at this assertion: correct = tripped=False (a"
            " transient or terminal answer is a failure, never judged as"
            " silence), mutant = tripped=True (the 10 h-old reading left"
            f" behind by the ONLINE answer would be judged; outcome="
            f"{outcome.health})"
        )


def test_sl5_current_obs_hold_suppresses_then_clears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL5 (plan §13.5, §6.3): SL3's OFFLINE station is ok while the
    current_obs hold is in force, and trips again once it clears (M49)."""
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_a = _station_id(conn, site_id, "ISTATION01")

    now = datetime(D.year, D.month, D.day, 14, 0, tzinfo=UTC)
    obs_instant = now - timedelta(hours=2)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_a,
        poll_at=obs_instant,
        obs_instant=isoformat_utc(obs_instant),
    )
    _persist_offline(
        monkeypatch,
        conn,
        site_id,
        station_a,
        poll_at=obs_instant + timedelta(minutes=10),
    )

    monkeypatch.setattr(
        "wxverify.worker.access_hold.utc_now", lambda: now - timedelta(minutes=5)
    )
    record_access_refused(conn, "current_obs", 401)

    held = station_silent(conn, now, close_on=False)
    assert not held.tripped, (
        "M49 -> at this assertion: correct = tripped=False while"
        " current_obs is held (the key problem is alert 6's), mutant ="
        f" tripped={held.tripped} (the current_obs refusal not read)"
    )

    clear_access_refused(conn, "current_obs")
    cleared = station_silent(conn, now, close_on=False)
    expected = (
        "station ISTATION01: weather.com reports no current data; newest"
        " observation 2 h old"
    )
    assert cleared.tripped and expected in (cleared.detail or ""), (
        f"M49 -> at this assertion: correct = tripped, {expected!r} in"
        f" detail once the hold clears, mutant = tripped={cleared.tripped}"
        " (if the refusal were still read it would stay ok)"
    )


def test_sl6_provisional_mark_false_drops_the_word(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL6 (plan §13.5, §7.3): with STATION_SILENT_PROVISIONAL set False by
    the test, "provisional" is absent (M50). Paired positive: SL1 (same
    scenario, mark left at its default True) has "provisional" present."""
    import wxverify.close_monitor as close_monitor_module
    from wxverify.close_monitor import station_silent

    monkeypatch.setattr(close_monitor_module, "STATION_SILENT_PROVISIONAL", False)

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_a = _station_id(conn, site_id, "ISTATION01")

    poll_at = datetime(D.year, D.month, D.day, 14, 0, tzinfo=UTC)
    obs_instant = poll_at - timedelta(hours=2)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_a,
        poll_at=poll_at,
        obs_instant=isoformat_utc(obs_instant),
    )

    alert = station_silent(conn, poll_at, close_on=False)
    assert alert.tripped and "provisional" not in (alert.detail or ""), (
        "M50 -> at this assertion: correct = 'provisional' absent from"
        f" detail, mutant = detail={alert.detail!r} (the mark not read,"
        " so 'provisional' would still show, as it does in SL1 with the"
        " mark at its default True)"
    )


def test_sl7_alert3_every_variable_suppresses_once_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SL7 (plan §13.5, §6.3): with the close on, B's silence line shows
    while alert 3 does not yet name it for every variable, and disappears
    once alert 3 does (M51)."""
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")

    dates = (D - timedelta(days=6), D - timedelta(days=5), D - timedelta(days=4))
    for i, d in enumerate(dates):
        _set_answered(conn, station_b, d)
        _set_usable(conn, station_b, d, ("wind",))  # wind stays usable so far
        _close_day(conn, site_id, d, first=(i == 0))

    obs_instant = NOW - timedelta(hours=10)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_b,
        poll_at=obs_instant,
        obs_instant=isoformat_utc(obs_instant),
    )
    _persist_offline(
        monkeypatch,
        conn,
        site_id,
        station_b,
        poll_at=obs_instant + timedelta(minutes=5),
    )

    not_every = station_silent(conn, NOW, close_on=True)
    assert not_every.tripped and "ISTATION02" in (not_every.detail or ""), (
        "M51 -> at this assertion: correct = tripped (alert 3 does not yet"
        " name ISTATION02 for every variable -- wind stays usable),"
        f" mutant = tripped={not_every.tripped}, detail="
        f"{not_every.detail!r} (alert 3's stations left out even when it"
        " does not cover every variable)"
    )

    conn.execute(
        "DELETE FROM station_observations WHERE station_id = ? AND variable = 'wind'",
        (station_b,),
    )
    every = station_silent(conn, NOW, close_on=True)
    assert not every.tripped, (
        "M51 -> at this assertion: correct = tripped=False (alert 3 now"
        " names ISTATION02 for every variable, so alert 5 leaves it out),"
        f" mutant = tripped={every.tripped} (alert 3's stations not left"
        " out with the close on)"
    )


def test_sl8_close_off_never_suppresses(monkeypatch: pytest.MonkeyPatch) -> None:
    """SL8 (plan §13.5, §6.3): SL7's second case (alert 3 would name B for
    every variable) with the close off: the days are built with the close
    on, then the close is turned off, and the line still shows, since
    alert 3 does not exist with the close off (M52)."""
    from wxverify.close_monitor import station_silent

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")

    dates = (D - timedelta(days=6), D - timedelta(days=5), D - timedelta(days=4))
    for i, d in enumerate(dates):
        _set_answered(conn, station_b, d)  # nothing usable on any day
        _close_day(conn, site_id, d, first=(i == 0))

    delete_runtime_state(conn, DAILY_CLOSE_ON_KEY)
    assert not daily_close_on(conn)

    obs_instant = NOW - timedelta(hours=10)
    _persist_online(
        monkeypatch,
        conn,
        site_id,
        station_b,
        poll_at=obs_instant,
        obs_instant=isoformat_utc(obs_instant),
    )
    _persist_offline(
        monkeypatch,
        conn,
        site_id,
        station_b,
        poll_at=obs_instant + timedelta(minutes=5),
    )

    alert = station_silent(conn, NOW, close_on=False)
    assert alert.tripped and "ISTATION02" in (alert.detail or ""), (
        "M52 -> at this assertion: correct = tripped (no alert 3 exists"
        " with the close off, so nothing suppresses the line), mutant ="
        f" tripped={alert.tripped}, detail={alert.detail!r} (alert 3's"
        " stations left out even with the close off)"
    )

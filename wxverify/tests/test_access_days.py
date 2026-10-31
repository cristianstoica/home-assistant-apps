"""P5 plan §13.6: alert 6's close part (AC1-AC7).

Fixtures and time per plan §13.1: ``day_close_site`` (tests/helpers.py) gives
a UTC site with stations A (``ISTATION01``) and B (``ISTATION02``), D-3
``closed``, D-2 ``missed`` (``never_attempted``), D-1 ``open`` and ended, D
current, ``NOW`` = D 12:00 UTC. 0.16.13's access-refused rows are written
with ``record_access_refused``/``clear_access_refused`` with
``wxverify.worker.access_hold.utc_now`` monkeypatched (the module's own bare
import; see tests/test_access_hold.py's ``_freeze``). The close's part of
alert 6 is read through ``build_verdict`` (budget group), reusing
``access_days_affected`` only through that route -- the new function name is
imported inside each test body so this module collects before Step 4 piece 2
lands it (plan §13.1, §15 Step 3).

Every row fails on today's code except AC6, which pins 0.16.13's unchanged
detail with the close off -- no new code runs on that path, so it already
holds.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import D, asof_conn, day_close_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.monitor import build_verdict
from wxverify.worker.access_hold import record_access_refused

# conftest.py's autouse ``_check_day_close_records`` fixture already covers
# every connection ``day_close_site`` builds in this module; no test here
# calls ``note_day_clock``, so no extra bound is needed.


def _freeze(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: now)


def _verdict(conn: sqlite3.Connection, now: datetime) -> dict[str, object]:
    return build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=True,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )


def _cond(conn: sqlite3.Connection, now: datetime) -> dict[str, object]:
    body = _verdict(conn, now)
    conditions = body["conditions"]
    assert isinstance(conditions, list)
    return next(c for c in conditions if c["id"] == "weathercom_access_refused")


def _site_name(conn: sqlite3.Connection, site_id: int) -> str:
    row = conn.execute("SELECT name FROM sites WHERE id = ?", (site_id,)).fetchone()
    assert row is not None
    return str(row[0])


def _set_next_five_minute_try(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date,
    next_attempt_at: datetime | None,
) -> None:
    conn.execute(
        """
        INSERT INTO station_wind_days
            (station_id, local_date, status, attempts, record_count, pair_hours,
             next_attempt_at, refetched, updated_at)
        VALUES (?, ?, 'pending', 0, 0, 0, ?, 0, '2024-01-01T00:00:00Z')
        """,
        (
            station_id,
            local_date.isoformat(),
            None if next_attempt_at is None else isoformat_utc(next_attempt_at),
        ),
    )


def _station_id(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_ac1_next_try_before_and_after_until(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC1 (plan §13.6, §6.4): the five_minute next-try clause (M53).

    The refusal is recorded at D 09:00; ``until`` = D 10:00. The site's next
    scheduled 5-minute try at D 09:30 lies before ``until``, so the clause
    reads the generic "first scheduled download after <until>" text; moved
    to D 15:00 (after ``until``) it names that time.
    """
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    until = record_access_refused(conn, "five_minute", 401)
    assert until == isoformat_utc(refusal_at + timedelta(hours=1))

    before_until = datetime(D.year, D.month, D.day, 9, 30, tzinfo=UTC)
    _set_next_five_minute_try(
        conn, station_id=station_a, local_date=D, next_attempt_at=before_until
    )
    _set_next_five_minute_try(
        conn, station_id=station_b, local_date=D, next_attempt_at=before_until
    )
    cond = _cond(conn, refusal_at + timedelta(minutes=5))
    detail = str(cond["detail"])
    assert "5-minute readings at the first scheduled download after" in detail, (
        "M53 -> at this assertion: correct = the detail carries the close's"
        " 'next try' clause for five_minute when the close is on, mutant ="
        " the days-affected/next-try part never appended, so the clause is"
        " absent from the detail."
    )

    conn.execute("DELETE FROM station_wind_days")
    after_until = datetime(D.year, D.month, D.day, 15, 0, tzinfo=UTC)
    _set_next_five_minute_try(
        conn, station_id=station_a, local_date=D, next_attempt_at=after_until
    )
    _set_next_five_minute_try(
        conn, station_id=station_b, local_date=D, next_attempt_at=after_until
    )
    cond2 = _cond(conn, refusal_at + timedelta(minutes=5))
    detail2 = str(cond2["detail"])
    assert f"5-minute readings at {isoformat_utc(after_until)}" in detail2, (
        "M53 -> at this assertion: correct = the clause names"
        f" {isoformat_utc(after_until)!r} once the site's next try moves"
        " after `until`, mutant = the close's next-try/days-affected part"
        " never appended, so neither clause text appears."
    )


def test_ac2_days_affected_spans_two_days(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC2 (plan §13.6, §6.4): days affected is not limited to the current
    day (M54): a refusal run spanning midnight lists D-1 (closing) and D
    (collecting).
    """
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    site_name = _site_name(conn, site_id)

    from wxverify.db.site_days import begin_close

    d_minus_1 = D - timedelta(days=1)
    refusal_at = datetime(
        d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, 30, tzinfo=UTC
    )
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "five_minute", 401)

    now = datetime(D.year, D.month, D.day, 0, 20, tzinfo=UTC)
    begin_close(
        conn,
        site_id,
        d_minus_1,
        now_utc=datetime(D.year, D.month, D.day, 0, 5, tzinfo=UTC),
    )

    cond = _cond(conn, now)
    detail = str(cond["detail"])
    assert f"{site_name} {d_minus_1.isoformat()} (closing)" in detail, (
        "M54 -> at this assertion: correct = the days-affected list includes"
        f" '{site_name} {d_minus_1.isoformat()} (closing)', mutant = days"
        " affected limited to the current day, so D-1 never appears."
    )
    assert f"{site_name} {D.isoformat()} (collecting)" in detail, (
        "M54 -> at this assertion: correct = the days-affected list also"
        f" includes '{site_name} {D.isoformat()} (collecting)', mutant ="
        " days affected limited to the current day, so only one day (or"
        " neither, depending where the limit lands) would appear here."
    )


def test_ac3_current_obs_clause_after_until(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC3 (plan §13.6, §6.4): the current_obs clause reads "current
    conditions at <until>" only while the hold is in force; after it, with
    no new try, it reverts to 0.16.13's "waiting for the next try" (M55).
    """
    conn = asof_conn()
    day_close_site(conn, close_on=True)

    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    until = record_access_refused(conn, "current_obs", 403)

    in_force = _cond(conn, refusal_at + timedelta(minutes=5))
    in_force_detail = str(in_force["detail"])
    assert f"current conditions at {until}" in in_force_detail, (
        f"M55 -> at this assertion: correct = 'current conditions at {until}'"
        " while the hold is in force, mutant = the 'current conditions at"
        " <until>' clause shown even after `until` has passed, so it would"
        " also appear at the later check below."
    )

    after_hold = _cond(conn, refusal_at + timedelta(hours=1, minutes=5))
    after_detail = str(after_hold["detail"])
    assert "waiting for the next try" in after_detail, (
        "M55 -> at this assertion: correct = 0.16.13's 'waiting for the"
        " next try' once `until` has passed with no new try, mutant = the"
        " 'current conditions at <until>' clause kept past `until`, so this"
        " text would be absent and the stale clause would still show."
    )
    assert f"current conditions at {until}" not in after_detail, (
        f"M55 -> at this assertion: correct = 'current conditions at"
        f" {until}' is gone once `until` has passed, mutant = the clause"
        " shown after `until`, so it would still appear here."
    )


def test_ac4_hourly_clause_reads_retry_at(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC4 (plan §13.6, §6.4): the hourly clause reads the gap fill's next
    try from the day's own ``retry_at``, not a fixed or unrelated time
    (M56).
    """
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    site_name = _site_name(conn, site_id)

    from wxverify.db.site_days import begin_close, fail_close

    d_minus_1 = D - timedelta(days=1)
    day_end = datetime(D.year, D.month, D.day, tzinfo=UTC)
    begin_close(conn, site_id, d_minus_1, now_utc=day_end + timedelta(minutes=5))
    fail_close(
        conn,
        site_id,
        d_minus_1,
        now_utc=day_end + timedelta(minutes=10),
        reason="network",
    )
    row = conn.execute(
        "SELECT retry_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert row is not None and row[0] is not None
    retry_at = str(row[0])

    refusal_at = day_end + timedelta(minutes=15)
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "hourly", 401)

    cond = _cond(conn, refusal_at + timedelta(minutes=1))
    detail = str(cond["detail"])
    assert f"hourly gap fill at {retry_at}" in detail, (
        f"M56 -> at this assertion: correct = 'hourly gap fill at {retry_at}'"
        " (the day's own retry_at), mutant = the gap fill's next try not"
        " read from retry_at, so a different (or absent) time would appear"
        " here."
    )
    assert f"{site_name} {d_minus_1.isoformat()} (retry due {retry_at})" in detail, (
        f"M56 -> at this assertion: correct = the days-affected state text"
        f" names retry_at ('retry due {retry_at}'), mutant = retry_at not"
        " read for the clause or the state text, so this exact pairing"
        " would not appear."
    )


def test_ac5_days_affected_caps_at_three(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC5 (plan §13.6, §6.4): the days-affected list caps at 3 days, then
    "and N more" (M57).

    ``day_close_site``'s fixture already gives 4 qualifying days (D-3
    closed, D-2 missed, D-1 open, D open; none is done more than 7 days
    ago). ``ACCESS_HOLD_SECONDS`` is monkeypatched to 6 days so that ONE
    ``record_access_refused`` call at D-3's start keeps a window open
    (``since``..``until``) spanning all four days at the test's ``now`` --
    no production code changes, only the test's own fixture-setup budget.
    """
    monkeypatch.setattr(
        "wxverify.worker.access_hold.ACCESS_HOLD_SECONDS", 6 * 24 * 3600
    )
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    site_name = _site_name(conn, site_id)

    d_minus_3 = D - timedelta(days=3)
    refusal_at = datetime(
        d_minus_3.year, d_minus_3.month, d_minus_3.day, 0, 0, tzinfo=UTC
    )
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "hourly", 401)

    now = datetime(D.year, D.month, D.day, 0, 10, tzinfo=UTC)
    cond = _cond(conn, now)
    detail = str(cond["detail"])
    assert "and 1 more" in detail, (
        "M57 -> at this assertion: correct = the 4-day run ends 'and 1"
        " more' once 3 days are listed, mutant = no cap of 3 days, so every"
        " one of the 4 days is listed and this suffix never appears."
    )
    assert detail.count(f"{site_name} ") <= 3, (
        "M57 -> at this assertion: correct = at most 3 '<site> <date>"
        " (<state>)' entries are listed, mutant = no cap of 3 days, so"
        " 4 entries would be counted here."
    )


def test_ac6_detail_unchanged_with_close_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC6 (plan §13.6, §6.4): with the close off, the detail equals
    0.16.13's line exactly -- no close-part text is appended (M58).

    This row PASSES on today's code: with the close off, P5 adds nothing to
    this path (§6.4, "with the close off the detail is 0.16.13's,
    unchanged"), so today's ``_access_refused_detail`` already produces this
    exact text.
    """
    conn = asof_conn()
    day_close_site(conn, close_on=False)

    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    until = record_access_refused(conn, "five_minute", 401)

    cond = _cond(conn, refusal_at + timedelta(minutes=5))
    detail = str(cond["detail"])
    expected = (
        f"weather.com access refused (HTTP 401): 5-minute readings; since"
        f" {isoformat_utc(refusal_at)}; held until {until}"
    )
    assert detail == expected, (
        f"M58 -> at this assertion: correct = {expected!r} (0.16.13's line,"
        " unchanged), mutant = the close part appended even with the close"
        " off, so this exact string would gain a trailing clause."
    )


def test_ac7_no_station_named_with_alert3_and_gap_fill_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC7 (plan §13.6, §5.4, §6.4): alert 6's detail never names a station,
    even when alert 3 and the nightly gap-fill pause apply to the same day
    (M59).

    Also checks what P5 adds on this row: ``close_alerts`` (imported here,
    inside the test body, since it does not exist on today's code) does not
    count this refused day against the station for alert 3 -- a day lost to
    a refused key is skipped, never counted with nothing usable (§5.4) --
    and the close-on days-affected clause of AC1 is present in alert 6's
    detail. Both are P5 additions, so this row fails on today's code (the
    import raises ``ModuleNotFoundError``, and the days-affected text is
    absent) and passes only once P5 lands.
    """
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    site_name = _site_name(conn, site_id)

    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "five_minute", 401)

    now = refusal_at + timedelta(minutes=5)
    cond = _cond(conn, now)
    detail = str(cond["detail"])
    assert "ISTATION01" not in detail and "ISTATION02" not in detail, (
        "M59 -> at this assertion: correct = no pws_station_id appears in"
        " alert 6's detail, mutant = stations named in alert 6's detail,"
        " so a station id would appear here."
    )
    assert "station " not in detail, (
        "M59 -> at this assertion: correct = the detail never says"
        " 'station ...', mutant = stations named in alert 6's detail, so"
        " that word would appear in this text."
    )
    assert f"days affected: {site_name} {D.isoformat()} (collecting)" in detail, (
        "M59 -> at this assertion: correct = the close-on days-affected"
        f" clause names '{site_name} {D.isoformat()} (collecting)', mutant ="
        " stations named in alert 6's detail in place of the days-affected"
        " clause, so this exact text would be absent here."
    )

    alerts = close_alerts(conn, now)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    assert not nothing_usable.tripped, (
        "M59 -> at this assertion: correct = alert 3 does not trip on a day"
        " lost to a refused key (the day is skipped, never counted with"
        " nothing usable, §5.4), mutant = a refused day counted against the"
        f" station, tripping alert 3 here too; actual tripped="
        f"{nothing_usable.tripped}"
    )

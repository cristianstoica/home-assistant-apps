"""Alert 6's close part (AC1-AC5, AC7).

Fixtures and time: ``day_close_site`` (tests/helpers.py) gives
a UTC site with stations A (``ISTATION01``) and B (``ISTATION02``), D-3
``closed``, D-2 ``missed`` (``never_attempted``), D-1 ``open`` and ended, D
current, ``NOW`` = D 12:00 UTC. 0.16.13's access-refused rows are written
with ``record_access_refused``/``clear_access_refused`` with
``wxverify.worker.access_hold.utc_now`` monkeypatched (the module's own bare
import; see tests/test_access_hold.py's ``_freeze``). The close's part of
alert 6 is read through ``build_verdict`` (budget group), reusing
``access_days_affected`` only through that route.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from tests.helpers import (
    NOW,
    D,
    asof_conn,
    day_close_site,
    disable_station_pacing,
    patch_weathercom_transport,
    write_weathercom_key,
)
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.site_days import (
    begin_close,
    ensure_site_days,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.monitor import build_verdict
from wxverify.worker.access_hold import read_access_record, record_access_refused
from wxverify.worker.day_values import fill_gaps

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


#: GAP_DAY: the day this test's gap fill runs over (mirrors
#: ``tests/test_gap_fill.py``'s own ``GAP_DAY``, D-1).
_FT1_GAP_DAY = D - timedelta(days=1)
#: The frozen instant ``wxverify.worker.access_hold.utc_now`` reads when this
#: test's refusal is recorded -- unrelated to ``NOW``/``D`` (the day-close
#: clock ``fill_gaps`` itself reads through its own ``now_utc`` argument).
#: Both parametrized runs below freeze to this SAME instant, in their own
#: fresh ``monkeypatch`` fixture, so ``since``/``until`` are directly
#: comparable literals rather than two live wall-clock reads that could
#: straddle a second.
_FT1_REFUSAL_AT = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)


def _init_fill_gaps_db(base: Path) -> Database:
    """A fresh real SQLite file under ``base`` (``init_db``, real
    migrations), closing any process-global ``Database`` first -- the
    pattern of ``tests/test_gap_fill.py``'s ``_init_tmp_db``."""
    close_db()
    base.mkdir(parents=True, exist_ok=True)
    db_path = base / "wxverify.db"
    config.db_path = str(db_path)
    return init_db(str(db_path))


def _build_ft1_gap_day_site(conn: sqlite3.Connection) -> int:
    """A site with D-3 closed, D-2 missed and ``_FT1_GAP_DAY`` (D-1) moved
    to ``closing`` with nothing filled -- station A (``ISTATION01``) has a
    full day of gap hours, so ``fill_gaps`` always calls out for it (the
    layout of ``tests/test_gap_fill.py``'s ``_build_base_site``, trimmed
    to the one station this test needs)."""
    site_id = day_close_site(conn, timezone="UTC", with_records=False)
    d_minus_3 = D - timedelta(days=3)
    start_site_days(
        conn,
        site_id,
        d_minus_3,
        now_utc=datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, tzinfo=UTC),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)
    d3_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, d_minus_3.isoformat()),
    ).fetchone()
    assert d3_row is not None
    t1 = parse_utc(d3_row[0]) + timedelta(minutes=30)
    begin_close(conn, site_id, d_minus_3, now_utc=t1)
    finish_close(conn, site_id, d_minus_3, now_utc=t1)

    d_minus_2 = D - timedelta(days=2)
    d2_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, d_minus_2.isoformat()),
    ).fetchone()
    assert d2_row is not None
    t2 = parse_utc(d2_row[0]) + timedelta(minutes=30)
    mark_missed(conn, site_id, d_minus_2, now_utc=t2, reason="never_attempted")

    begin_close(conn, site_id, _FT1_GAP_DAY, now_utc=NOW)
    conn.commit()
    return site_id


_FT1_REASON_BODY = json.dumps(
    {
        "errors": [
            {
                "error": {
                    "code": "SYNTHETIC-CODE",
                    "message": "apikey is not authorized for this product",
                }
            }
        ]
    }
).encode()


@pytest.mark.parametrize(
    ("case", "reply_content", "reply_headers", "expected_reason"),
    [
        (
            "with_reason",
            _FT1_REASON_BODY,
            {"content-type": "application/json"},
            "apikey is not authorized for this product",
        ),
        ("empty_body", b"", {}, None),
    ],
    ids=["with_reason", "empty_body"],
)
def test_ft1_hourly_download_path_stores_providers_masked_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    reply_content: bytes,
    reply_headers: dict[str, str],
    expected_reason: str | None,
) -> None:
    """FT1: a weather.com 403 reaching
    the REAL hourly download inside ``fill_gaps`` -- never
    ``provider_reason``/``record_access_refused`` called directly by this
    test -- stores the provider's own masked reason on the hourly hold;
    an empty body stores no reason. Two separate parametrized runs, each
    with its own fresh ``monkeypatch`` fixture (so each gets its own,
    un-wrapped patch of ``wxverify.worker.day_values.httpx.AsyncClient`` --
    patching it a second time inside ONE test would wrap the first
    handler instead of replacing it, since ``patch_weathercom_transport``
    captures ``httpx.AsyncClient`` at call time, which is already the
    first patch's lambda by then) -- both freeze
    ``wxverify.worker.access_hold.utc_now`` to the SAME literal instant
    (``_FT1_REFUSAL_AT``), so ``since``/``until`` are checked against
    literals computed from it, never two live wall-clock reads that could
    straddle a second.

    Drives ``wxverify.worker.day_values.fill_gaps`` over a site whose
    station A has a full day of gap hours (``_build_ft1_gap_day_site``),
    with the hourly download's client patched onto an
    ``httpx.MockTransport`` (``tests.helpers.patch_weathercom_transport``,
    the harness ``tests/test_gap_fill.py``'s GF4 case uses for its own
    401/403/500/network outcomes) -- so it is ``_http_failed`` itself
    that calls ``provider_reason`` on the real response and then
    ``record_access_refused`` with its result.

    Catches: only `_http_status_error` changed so the provider's reason
    is never shown on the hold, or the hourly caller
    passing `reason=None` instead of the real `provider_reason` result --
    at the "the hold row's reason" assertion below: correct =
    ``expected_reason``, mutant = the other run's reason.
    """
    _freeze(monkeypatch, _FT1_REFUSAL_AT)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, content=reply_content, headers=reply_headers)

    db = _init_fill_gaps_db(tmp_path / case)
    conn = db._conn  # noqa: SLF001 - test inspects the real writer connection
    write_weathercom_key(monkeypatch, tmp_path / case)
    disable_station_pacing(monkeypatch)
    site_id = _build_ft1_gap_day_site(conn)
    writer = FencedWriter(db)
    patch_weathercom_transport(monkeypatch, handler)
    asyncio.run(
        fill_gaps(db, writer, site_id=site_id, local_date=_FT1_GAP_DAY, now_utc=NOW)
    )

    record = read_access_record(conn, "hourly")
    assert record is not None
    assert record.reason == expected_reason, (
        "mutant -> at this assertion: correct ="
        f" {expected_reason!r}, mutant = {record.reason!r}"
    )
    assert record.status == 403

    frozen = _FT1_REFUSAL_AT.replace(microsecond=0)
    expected_until = isoformat_utc(frozen + timedelta(hours=1))
    assert record.since == isoformat_utc(frozen), (
        "mutant -> at this assertion: correct ="
        f" {isoformat_utc(frozen)!r} (the reason's presence must not move"
        f" since), mutant = {record.since!r}"
    )
    assert record.until == expected_until, (
        "mutant -> at this assertion: correct ="
        f" {expected_until!r} (the reason's presence must not move until),"
        f" mutant = {record.until!r}"
    )


def test_ac1_next_try_before_and_after_until(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC1: the five_minute next-try clause (M53).

    The refusal is recorded at D 09:00; ``until`` = D 10:00. The site's next
    scheduled 5-minute try at D 09:30 lies before ``until``, so the clause
    reads the generic "first scheduled download after <until>" text; moved
    to D 15:00 (after ``until``) it names that time.
    """
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    until = record_access_refused(conn, "five_minute", 401, reason=None)
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
    """AC2: days affected is not limited to the current
    day (M54): a refusal run spanning midnight lists D-1 (closing) and D
    (collecting).
    """
    conn = asof_conn()
    site_id = day_close_site(conn)
    site_name = _site_name(conn, site_id)

    from wxverify.db.site_days import begin_close

    d_minus_1 = D - timedelta(days=1)
    refusal_at = datetime(
        d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, 30, tzinfo=UTC
    )
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "five_minute", 401, reason=None)

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
    """AC3: the current_obs clause reads "current
    conditions at <until>" only while the hold is in force; after it, with
    no new try, it reverts to 0.16.13's "waiting for the next try" (M55).
    """
    conn = asof_conn()
    day_close_site(conn)

    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    until = record_access_refused(conn, "current_obs", 403, reason=None)

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
    """AC4: the hourly clause reads the gap fill's next
    try from the day's own ``retry_at``, not a fixed or unrelated time
    (M56).
    """
    conn = asof_conn()
    site_id = day_close_site(conn)
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
    record_access_refused(conn, "hourly", 401, reason=None)

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
    """AC5: the days-affected list caps at 3 days, then
    "and N more" (M57).

    ``day_close_site``'s fixture already gives 5 qualifying days (D-3
    closed, D-2 missed, D-1 open, D open, D+1 open -- the buffer day
    ``ensure_site_days`` fills through; none is done more than 7 days
    ago). ``ACCESS_HOLD_SECONDS`` is monkeypatched to 6 days so that ONE
    ``record_access_refused`` call at D-3's start keeps a window open
    (``since``..``until``) spanning all five days at the test's ``now`` --
    no production code changes, only the test's own fixture-setup budget.
    """
    monkeypatch.setattr(
        "wxverify.worker.access_hold.ACCESS_HOLD_SECONDS", 6 * 24 * 3600
    )
    conn = asof_conn()
    site_id = day_close_site(conn)
    site_name = _site_name(conn, site_id)

    d_minus_3 = D - timedelta(days=3)
    refusal_at = datetime(
        d_minus_3.year, d_minus_3.month, d_minus_3.day, 0, 0, tzinfo=UTC
    )
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "hourly", 401, reason=None)

    now = datetime(D.year, D.month, D.day, 0, 10, tzinfo=UTC)
    cond = _cond(conn, now)
    detail = str(cond["detail"])
    assert "and 2 more" in detail, (
        "M57 -> at this assertion: correct = the 5-day run ends 'and 2"
        " more' once 3 days are listed, mutant = no cap of 3 days, so every"
        " one of the 5 days is listed and this suffix never appears."
    )
    assert detail.count(f"{site_name} ") <= 3, (
        "M57 -> at this assertion: correct = at most 3 '<site> <date>"
        " (<state>)' entries are listed, mutant = no cap of 3 days, so"
        " 4 entries would be counted here."
    )


def _mark_answered(conn: sqlite3.Connection, station_id: int, local_date) -> None:
    """A 204-equivalent 5-minute try: answered, but no raw readings stored
    (mirrors ``tests/test_close_alerts.py``'s ``_set_answered``, kept local
    to this module since that name is private to its own file)."""
    stamp = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC)
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, last_ok_at, updated_at)"
        " VALUES (?, ?, 'fetched', ?, ?)",
        (station_id, local_date.isoformat(), stamp, stamp),
    )


def _close_plain(conn: sqlite3.Connection, site_id: int, local_date) -> None:
    """Close ``local_date`` just after its own end, with no other state
    change (mirrors ``tests/test_close_alerts.py``'s ``_close_day``, kept
    local for the same reason as ``_mark_answered``)."""
    from wxverify.db.site_days import begin_close, day_record, finish_close

    record = day_record(conn, site_id, local_date)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, local_date, now_utc=t)
    finish_close(conn, site_id, local_date, now_utc=t)


def test_ac7_no_station_named_with_alert3_and_gap_fill_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC7: alert 6's detail never names a station,
    even when alert 3 has a real nothing-usable finding at the same time
    (M59: ``access_days_affected`` also appends ``station <id>`` for each
    alert-3 station to alert 6's days).

    The original version of this test never gave alert 3 anything to find
    (the fixture's only closed day, D-3, has real presence), so
    ``_nothing_usable`` was always empty and M59's extra append was always
    the empty tuple -- unobservable regardless of the bug. This instead
    builds the site from scratch (``with_records=False``) with D-6, D-5
    and D-4 closed, each with station A answered but with no usable
    reading at all (``_mark_answered``, 3 is
    ``close_monitor.STATION_NOTHING_USABLE_DAYS``), then D-3 closed
    plainly (skipped either way, never breaking or extending the run,
    exactly as ``tests/test_close_alerts.py``'s own ``_close_day``
    documents) and D-2 ``missed``, matching ``day_close_site``'s own
    ``with_records=True`` layout from D-3 on. A direct sanity check
    confirms ``_nothing_usable`` is genuinely non-empty before the real
    assertions run.
    """
    from wxverify.close_monitor import _nothing_usable
    from wxverify.db.site_days import ensure_site_days, mark_missed, start_site_days

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    station_a = _station_id(conn, site_id, "ISTATION01")
    site_name = _site_name(conn, site_id)

    d_minus_6 = D - timedelta(days=6)
    start_site_days(
        conn,
        site_id,
        d_minus_6,
        now_utc=datetime(d_minus_6.year, d_minus_6.month, d_minus_6.day, tzinfo=UTC),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)
    for offset in (6, 5, 4):
        d = D - timedelta(days=offset)
        _mark_answered(conn, station_a, d)
        _close_plain(conn, site_id, d)
    d_minus_3 = D - timedelta(days=3)
    _close_plain(conn, site_id, d_minus_3)
    d_minus_2 = D - timedelta(days=2)
    d2_record_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_2.isoformat()),
    ).fetchone()
    assert d2_record_row is not None
    from wxverify.core.timeutil import parse_utc

    mark_missed(
        conn,
        site_id,
        d_minus_2,
        now_utc=parse_utc(d2_record_row[0]) + timedelta(minutes=30),
        reason="never_attempted",
    )

    sanity_now = datetime(D.year, D.month, D.day, 9, 5, tzinfo=UTC)
    findings = _nothing_usable(conn, sanity_now)
    assert findings, (
        "fixture sanity check (not a mutant kill): alert 3 must have a real"
        " nothing-usable finding here, or M59's extra append has nothing to"
        " add and this test cannot distinguish it from correct code -- got"
        f" {findings!r}."
    )

    from wxverify.close_monitor import close_alerts

    alerts_before = close_alerts(conn, sanity_now)
    nothing_usable_before = next(
        a for a in alerts_before if a.id == "station_nothing_usable"
    )
    expected_before = (
        "station ISTATION01: nothing usable for temperature, rain and wind"
        f" since {d_minus_6.isoformat()}"
    )
    assert (
        nothing_usable_before.tripped
        and nothing_usable_before.count == 1
        and nothing_usable_before.detail == expected_before
    ), (
        f"fixture check: correct = tripped, count=1, detail={expected_before!r}"
        f" for the D-6..D-4 run, got tripped={nothing_usable_before.tripped},"
        f" count={nothing_usable_before.count},"
        f" detail={nothing_usable_before.detail!r}. This pins alert 3's own"
        " reading of this fixture before the refusal below is written. The"
        " refusal that follows is recorded on D itself (today, never a done"
        " day -- D is still `collecting`, per the detail checked below), so"
        " it can never be a day alert 3 counts or skips either way; a repeat"
        " of this same assertion after the refusal would prove only that the"
        " refusal adds nothing to alert 3's D-6..D-4 reading, not that a day"
        " lost to it is skipped."
    )

    refusal_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _freeze(monkeypatch, refusal_at)
    record_access_refused(conn, "five_minute", 401, reason=None)

    now = refusal_at + timedelta(minutes=5)
    cond = _cond(conn, now)
    detail = str(cond["detail"])
    assert "ISTATION01" not in detail and "ISTATION02" not in detail, (
        "M59 -> at this assertion: correct = no pws_station_id appears in"
        " alert 6's detail even though alert 3 has a real finding for"
        " ISTATION01 (confirmed above), mutant = stations named in alert"
        " 6's detail, so a station id would appear here."
    )
    assert "station " not in detail, (
        "M59 -> at this assertion: correct = the detail never says"
        " 'station ...', mutant = stations named in alert 6's detail, so"
        " that word would appear in this text (the mutant's own literal"
        " text is 'station <id>')."
    )
    assert f"days affected: {site_name} {D.isoformat()} (collecting)" in detail, (
        "M59 -> at this assertion: correct = the close-on days-affected"
        f" clause names '{site_name} {D.isoformat()} (collecting)', mutant ="
        " stations named in alert 6's detail in place of the days-affected"
        " clause, so this exact text would be absent here."
    )

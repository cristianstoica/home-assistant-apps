"""Day-state tests for ``wxverify.db.site_days`` (daily-close plan §13.2).

Every test here is written before ``wxverify/db/site_days.py`` exists and
fails first for that reason: the module-level import below raises
``ModuleNotFoundError``. Section 14 of the plan names the mutant each test
ID must kill; that mapping is not repeated in the docstrings here.

Real SQLite only (``tests.helpers.asof_conn``, real ``run_migrations``, real
writers). Station ids are synthetic (``ISTATION01``, ``ISTATION02``, ...),
zones are ``UTC``/``Etc/GMT+-N`` only, and dates are the fixed synthetic
``tests.helpers.D``. Time is faked at ``now_utc`` arguments directly for the
functions under test here -- they take no other clock.
"""

from __future__ import annotations

import ast
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import wxverify.db.site_days
from tests.helpers import (
    NOW,
    D,
    asof_conn,
    asof_insert_pair,
    asof_make_real_feed,
    asof_make_site,
    day_close_site,
    note_day_clock,
)
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import (
    DAILY_CLOSE_ON_KEY,
    OFFLINE_COPY_KEY,
    DayStateError,
    WriteKind,
    begin_close,
    check_date_write,
    check_day_write,
    closed_through,
    current_day_has_other_station,
    daily_close_on,
    daily_close_on_at,
    day_for_instant,
    day_record,
    day_station_ids,
    ensure_site_days,
    enter_zone_change,
    fail_close,
    filter_day_writes,
    finish_close,
    local_clock_utc,
    mark_missed,
    open_day_clause,
    open_day_on_copy,
    record_station_change,
    start_site_days,
    writable_pairs_clause,
    zone_flagged,
)

_ALL_KINDS: tuple[WriteKind, ...] = (
    WriteKind.READING,
    WriteKind.GAP_FILL,
    WriteKind.HOURLY,
    WriteKind.TRUTH,
    WriteKind.PAIRS,
    WriteKind.DAY_SCORES,
    WriteKind.CLOSE_RECORD,
    WriteKind.RAW_PRUNE,
    WriteKind.HISTORY_PRUNE,
)

# Table 5.2, state-keyed. RAW_PRUNE's "yes" case (a done day whose next day
# is done too, 7 days after the later done_at) is pinned separately by DS22;
# here every fixture state refuses it, which is also correct for every state
# this table exercises (open/closing/retry_due never satisfy 5.4; the
# "closed" fixture's next day, D-2, is missed -- done -- but RAW_PRUNE is
# still refused because 7 days have not passed since the later done_at; the
# "missed" fixture's next day, D-1, is still open).
_TABLE_5_2: dict[str, dict[WriteKind, bool]] = {
    "open": {
        WriteKind.READING: True,
        WriteKind.GAP_FILL: False,
        WriteKind.HOURLY: True,
        WriteKind.TRUTH: True,
        WriteKind.PAIRS: True,
        WriteKind.DAY_SCORES: False,
        WriteKind.CLOSE_RECORD: False,
        WriteKind.RAW_PRUNE: False,
        WriteKind.HISTORY_PRUNE: False,
    },
    "closing": {
        WriteKind.READING: False,
        WriteKind.GAP_FILL: True,
        WriteKind.HOURLY: True,
        WriteKind.TRUTH: True,
        WriteKind.PAIRS: True,
        WriteKind.DAY_SCORES: True,
        WriteKind.CLOSE_RECORD: True,
        WriteKind.RAW_PRUNE: False,
        WriteKind.HISTORY_PRUNE: False,
    },
    "retry_due": dict.fromkeys(_ALL_KINDS, False),
    "closed": dict.fromkeys(_ALL_KINDS, False),
    "missed": dict.fromkeys(_ALL_KINDS, False),
    # Before the first record only HISTORY_PRUNE is allowed, and only with
    # the close on (4.8, 7.1, plan row :919).
    "no_record": {**dict.fromkeys(_ALL_KINDS, False), WriteKind.HISTORY_PRUNE: True},
    "close_off": {
        WriteKind.READING: True,
        WriteKind.GAP_FILL: False,
        WriteKind.HOURLY: True,
        WriteKind.TRUTH: True,
        WriteKind.PAIRS: True,
        WriteKind.DAY_SCORES: False,
        WriteKind.CLOSE_RECORD: False,
        WriteKind.RAW_PRUNE: False,
        WriteKind.HISTORY_PRUNE: False,
    },
}


def _record_day_end(conn: sqlite3.Connection, site_id: int, local_date: date) -> str:
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return str(row[0])


def _record_day_start(conn: sqlite3.Connection, site_id: int, local_date: date) -> str:
    row = conn.execute(
        "SELECT day_start_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return str(row[0])


def _day_row(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> dict[str, object]:
    row = conn.execute(
        "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return dict(row)


def _ds8_fixture(
    state: str,
) -> tuple[sqlite3.Connection, int, str, date]:
    """Build one of DS8's state fixtures; return (conn, site_id, instant, date)."""
    if state == "close_off":
        conn = asof_conn()
        site_id = day_close_site(conn, close_on=False, with_records=False)
        return conn, site_id, isoformat_utc(NOW), D

    conn = asof_conn()
    site_id = day_close_site(conn)  # UTC, close on, D-3 closed, D-2 missed
    if state == "open":
        return conn, site_id, isoformat_utc(NOW), D
    if state == "closed":
        d_minus_3 = D - timedelta(days=3)
        return (
            conn,
            site_id,
            _record_day_start(conn, site_id, d_minus_3),
            d_minus_3,
        )
    if state == "missed":
        d_minus_2 = D - timedelta(days=2)
        return (
            conn,
            site_id,
            _record_day_start(conn, site_id, d_minus_2),
            d_minus_2,
        )
    if state == "no_record":
        before_first = D - timedelta(days=100)
        instant = datetime(
            before_first.year, before_first.month, before_first.day, tzinfo=UTC
        )
        return conn, site_id, isoformat_utc(instant), before_first
    if state in ("closing", "retry_due"):
        d_minus_1 = D - timedelta(days=1)
        begin_close(conn, site_id, d_minus_1, now_utc=NOW)
        if state == "retry_due":
            fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
        return conn, site_id, _record_day_start(conn, site_id, d_minus_1), d_minus_1
    raise AssertionError(state)


@pytest.mark.parametrize("state", list(_TABLE_5_2))
@pytest.mark.parametrize("kind", _ALL_KINDS)
def test_ds8_check_day_write_matches_table_5_2(state: str, kind: WriteKind) -> None:
    conn, site_id, instant, _local_date = _ds8_fixture(state)
    assert (
        check_day_write(conn, site_id, instant, kind, now_utc=NOW)
        is _TABLE_5_2[state][kind]
    )


@pytest.mark.parametrize("state", list(_TABLE_5_2))
@pytest.mark.parametrize("kind", _ALL_KINDS)
def test_ds8_check_date_write_matches_table_5_2(state: str, kind: WriteKind) -> None:
    conn, site_id, _instant, local_date = _ds8_fixture(state)
    assert (
        check_date_write(conn, site_id, local_date, kind, now_utc=NOW)
        is _TABLE_5_2[state][kind]
    )


def test_ds8_instant_spellings_land_on_same_day() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    start = _record_day_end(conn, site_id, D - timedelta(days=2))  # D-1's start
    assert start.endswith("Z")
    naive = start.removesuffix("Z")
    spellings = (
        start,
        f"{naive}+00:00",
        f"{naive[:-0]}.000000Z" if "." in naive else f"{naive}.000000Z",
    )
    for spelling in spellings:
        assert check_day_write(conn, site_id, spelling, WriteKind.READING, now_utc=NOW)
        assert day_for_instant(conn, site_id, spelling).local_date == d_minus_1  # type: ignore[union-attr]


def test_ds8_end_instant_lands_on_next_day_and_last_end_is_refused() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d2_end = _record_day_end(conn, site_id, D - timedelta(days=2))
    assert day_for_instant(conn, site_id, d2_end).local_date == D - timedelta(days=1)  # type: ignore[union-attr]

    latest = conn.execute(
        "SELECT MAX(local_date) FROM site_days WHERE site_id = ?", (site_id,)
    ).fetchone()[0]
    last_end = _record_day_end(conn, site_id, date.fromisoformat(latest))
    for kind in _ALL_KINDS:
        assert check_day_write(conn, site_id, last_end, kind, now_utc=NOW) is False


def test_ds8_unparseable_instant_refused_for_every_kind() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    for kind in _ALL_KINDS:
        assert (
            check_day_write(conn, site_id, "not-an-instant", kind, now_utc=NOW) is False
        )


def test_ds8_history_prune_before_first_record_needs_close_on() -> None:
    before_first = isoformat_utc(
        datetime(D.year, D.month, D.day, tzinfo=UTC) - timedelta(days=100)
    )

    conn_on = asof_conn()
    site_on = day_close_site(conn_on)
    assert check_day_write(
        conn_on, site_on, before_first, WriteKind.HISTORY_PRUNE, now_utc=NOW
    )
    for kind in _ALL_KINDS:
        if kind is WriteKind.HISTORY_PRUNE:
            continue
        assert (
            check_day_write(conn_on, site_on, before_first, kind, now_utc=NOW) is False
        )

    conn_off = asof_conn()
    site_off = day_close_site(conn_off, close_on=False, with_records=False)
    assert (
        check_day_write(
            conn_off, site_off, before_first, WriteKind.HISTORY_PRUNE, now_utc=NOW
        )
        is False
    )


def test_ds1_ensure_site_days_current_local_day_in_gmt_minus_14() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, timezone="Etc/GMT-14", with_records=False)
    ensure_site_days(conn, site_id, now_utc=NOW)

    rows = conn.execute(
        "SELECT local_date, day_start_utc, day_end_utc, timezone, zone_changeover,"
        " attempts FROM site_days WHERE site_id = ?",
        (site_id,),
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["local_date"] == (D + timedelta(days=2)).isoformat()
    assert row["day_start_utc"] == isoformat_utc(
        datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=1, hours=10)
    )
    assert row["day_end_utc"] == isoformat_utc(
        datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=2, hours=10)
    )
    assert row["timezone"] == "Etc/GMT-14"
    assert row["zone_changeover"] == 0
    assert row["attempts"] == 0
    assert day_station_ids(conn, site_id, D + timedelta(days=2)) == frozenset(
        {
            int(
                conn.execute(
                    "SELECT id FROM stations WHERE pws_station_id='ISTATION01'"
                ).fetchone()[0]
            ),
            int(
                conn.execute(
                    "SELECT id FROM stations WHERE pws_station_id='ISTATION02'"
                ).fetchone()[0]
            ),
        }
    )


def test_ds2_ensure_site_days_fills_gap_and_is_idempotent() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)  # UTC, D-3..D+1
    later = note_day_clock(
        datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC) + timedelta(days=3)
    )
    ensure_site_days(conn, site_id, now_utc=later)

    rows = conn.execute(
        "SELECT local_date, day_start_utc, day_end_utc FROM site_days"
        " WHERE site_id = ? ORDER BY local_date",
        (site_id,),
    ).fetchall()
    dates = [r["local_date"] for r in rows]
    assert dates[0] == (D - timedelta(days=3)).isoformat()
    assert dates[-1] == (D + timedelta(days=4)).isoformat()
    for prev, cur in zip(rows, rows[1:], strict=False):
        assert prev["day_end_utc"] == cur["day_start_utc"]

    before = conn.execute(
        "SELECT local_date, day_start_utc, day_end_utc FROM site_days"
        " WHERE site_id = ? ORDER BY local_date",
        (site_id,),
    ).fetchall()
    ensure_site_days(conn, site_id, now_utc=later)
    after = conn.execute(
        "SELECT local_date, day_start_utc, day_end_utc FROM site_days"
        " WHERE site_id = ? ORDER BY local_date",
        (site_id,),
    ).fetchall()
    assert [dict(r) for r in before] == [dict(r) for r in after]


def test_ds3_begin_close_open_to_closing_writes_exact_columns() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    before = _day_row(conn, site_id, d_minus_1)

    clock = note_day_clock(NOW + timedelta(minutes=5))
    attempt = begin_close(conn, site_id, d_minus_1, now_utc=clock)
    assert attempt == 1

    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "closing"
    assert after["attempts"] == 1
    assert after["state_changed_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    changed = ("state", "attempts", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_begin_close_too_early_raises_with_other_conditions_satisfied() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    # Close out D-1 too, so every record earlier than D is done and D's
    # date-order and zone_changeover conditions are already satisfied --
    # only the day-not-ended check can refuse the call below.
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    before = _day_row(conn, site_id, D)

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, D, now_utc=NOW)

    assert _day_row(conn, site_id, D) == before


def test_ds3_begin_close_refuses_out_of_order_with_other_conditions_satisfied() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    # D-1 is left open by the fixture. D has ended and is not a changeover
    # day, so only the date-order check can refuse the call below.
    after_d_ended = datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=1)
    before = _day_row(conn, site_id, D)

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, D, now_utc=after_d_ended)

    assert _day_row(conn, site_id, D) == before


def test_ds3_begin_close_refuses_changeover_day_with_other_conditions_satisfied() -> (
    None
):
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    conn.execute(
        "UPDATE site_days SET zone_changeover = 1 WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    )
    after_d_ended = datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=1)
    before = _day_row(conn, site_id, D)

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, D, now_utc=after_d_ended)

    assert _day_row(conn, site_id, D) == before


def test_ds3_begin_close_retry_due_to_closing_writes_exact_columns() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
    retry_at = parse_utc(str(_day_row(conn, site_id, d_minus_1)["retry_at"]))

    before_due = _day_row(conn, site_id, d_minus_1)
    with pytest.raises(DayStateError):
        begin_close(conn, site_id, d_minus_1, now_utc=retry_at - timedelta(minutes=1))
    assert _day_row(conn, site_id, d_minus_1) == before_due

    before = _day_row(conn, site_id, d_minus_1)
    attempt = begin_close(conn, site_id, d_minus_1, now_utc=retry_at)
    assert attempt == 2

    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "closing"
    assert after["attempts"] == 2
    assert after["state_changed_at"] != before["state_changed_at"]
    changed = ("state", "attempts", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_finish_close_writes_exact_columns() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    before = _day_row(conn, site_id, d_minus_1)

    clock = note_day_clock(NOW + timedelta(minutes=5))
    finish_close(conn, site_id, d_minus_1, now_utc=clock)

    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "closed"
    assert after["done_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert after["state_changed_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    changed = ("state", "done_at", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_fail_close_attempt1_writes_exact_columns() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    before = _day_row(conn, site_id, d_minus_1)

    clock = note_day_clock(NOW + timedelta(minutes=5))
    outcome = fail_close(conn, site_id, d_minus_1, now_utc=clock, reason="x")
    assert outcome == "retry_due"

    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "retry_due"
    assert after["retry_at"] == isoformat_utc(clock + timedelta(hours=1))
    assert after["last_failure"] == "x"
    assert after["state_changed_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    changed = ("state", "retry_at", "last_failure", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_fail_close_attempt2_writes_exact_columns_and_returns_missed() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
    retry_at = parse_utc(str(_day_row(conn, site_id, d_minus_1)["retry_at"]))
    begin_close(conn, site_id, d_minus_1, now_utc=retry_at)
    before = _day_row(conn, site_id, d_minus_1)

    clock = note_day_clock(retry_at + timedelta(minutes=5))
    outcome = fail_close(conn, site_id, d_minus_1, now_utc=clock, reason="y")
    assert outcome == "missed"

    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "missed"
    assert after["missed_reason"] == "retry_failed"
    assert after["last_failure"] == "y"
    assert after["done_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert after["state_changed_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    changed = ("state", "missed_reason", "last_failure", "done_at", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_mark_missed_open_never_attempted_writes_exact_columns() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    before = _day_row(conn, site_id, d_minus_1)

    clock = note_day_clock(NOW + timedelta(minutes=5))
    mark_missed(conn, site_id, d_minus_1, now_utc=clock, reason="never_attempted")

    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "missed"
    assert after["missed_reason"] == "never_attempted"
    assert after["done_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert after["state_changed_at"] == clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    changed = ("state", "missed_reason", "done_at", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_mark_missed_too_early_raises_with_other_conditions_satisfied() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    before = _day_row(conn, site_id, D)

    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, D, now_utc=NOW, reason="never_attempted")

    assert _day_row(conn, site_id, D) == before


def test_ds3_mark_missed_refuses_out_of_order_with_other_conditions_satisfied() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    after_d_ended = datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=1)
    before = _day_row(conn, site_id, D)

    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, D, now_utc=after_d_ended, reason="never_attempted")

    assert _day_row(conn, site_id, D) == before


def test_ds3_mark_missed_reason_must_match_changeover_flag() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    after_d_ended = datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=1)

    # D is an ordinary day (zone_changeover = 0): "zone_changeover" does not
    # fit it -- the negative half of the pair.
    before = _day_row(conn, site_id, D)
    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, D, now_utc=after_d_ended, reason="zone_changeover")
    assert _day_row(conn, site_id, D) == before

    conn.execute(
        "UPDATE site_days SET zone_changeover = 1 WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    )

    # Flipped to a changeover day: "never_attempted" no longer fits it...
    before = _day_row(conn, site_id, D)
    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, D, now_utc=after_d_ended, reason="never_attempted")
    assert _day_row(conn, site_id, D) == before

    # ...but "zone_changeover" does, and writes through -- the positive
    # half of the pair, which fails if the flag check above is ever wrong
    # in the other direction.
    mark_missed(conn, site_id, D, now_utc=after_d_ended, reason="zone_changeover")
    after = _day_row(conn, site_id, D)
    assert after["state"] == "missed"
    assert after["missed_reason"] == "zone_changeover"
    assert after["done_at"] is not None


def test_ds3_mark_missed_retry_due_writes_exact_columns_and_wrong_reason_raises() -> (
    None
):
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
    retry_at = parse_utc(str(_day_row(conn, site_id, d_minus_1)["retry_at"]))

    before = _day_row(conn, site_id, d_minus_1)
    with pytest.raises(DayStateError):
        mark_missed(
            conn, site_id, d_minus_1, now_utc=retry_at, reason="never_attempted"
        )
    assert _day_row(conn, site_id, d_minus_1) == before

    mark_missed(conn, site_id, d_minus_1, now_utc=retry_at, reason="retry_not_run")
    after = _day_row(conn, site_id, d_minus_1)
    assert after["state"] == "missed"
    assert after["missed_reason"] == "retry_not_run"
    assert after["done_at"] is not None
    assert after["state_changed_at"] != before["state_changed_at"]
    changed = ("state", "missed_reason", "done_at", "state_changed_at")
    assert {k: v for k, v in after.items() if k not in changed} == {
        k: v for k, v in before.items() if k not in changed
    }


def test_ds3_open_and_closing_day_never_listed_moves_raise_unchanged() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)

    # open -> closed directly: never listed.
    before = _day_row(conn, site_id, d_minus_1)
    with pytest.raises(DayStateError):
        finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    assert _day_row(conn, site_id, d_minus_1) == before

    # open -> retry_due/missed via fail_close: never listed.
    with pytest.raises(DayStateError):
        fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
    assert _day_row(conn, site_id, d_minus_1) == before

    # open -> closing: allowed, used below to reach the next state.
    attempt = begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    assert attempt == 1

    # closing -> open: never listed (start_site_days refuses a second first
    # record regardless, but the day itself must stay untouched).
    before = _day_row(conn, site_id, d_minus_1)
    with pytest.raises(DayStateError):
        start_site_days(conn, site_id, d_minus_1, now_utc=NOW)
    assert _day_row(conn, site_id, d_minus_1) == before

    # closing -> closing again via begin_close: never listed.
    with pytest.raises(DayStateError):
        begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    assert _day_row(conn, site_id, d_minus_1) == before

    # closing -> missed via mark_missed: never listed.
    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, d_minus_1, now_utc=NOW, reason="never_attempted")
    assert _day_row(conn, site_id, d_minus_1) == before


def test_ds3_retry_due_day_never_listed_moves_raise_unchanged() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
    before = _day_row(conn, site_id, d_minus_1)

    with pytest.raises(DayStateError):
        finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    assert _day_row(conn, site_id, d_minus_1) == before

    with pytest.raises(DayStateError):
        fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="y")
    assert _day_row(conn, site_id, d_minus_1) == before


@pytest.mark.parametrize("state_date", [D - timedelta(days=3), D - timedelta(days=2)])
def test_ds3_done_days_never_reopen(state_date: date) -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)  # D-3 closed, D-2 missed
    before = _day_row(conn, site_id, state_date)

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, state_date, now_utc=NOW)
    assert _day_row(conn, site_id, state_date) == before

    with pytest.raises(DayStateError):
        fail_close(conn, site_id, state_date, now_utc=NOW, reason="x")
    assert _day_row(conn, site_id, state_date) == before

    with pytest.raises(DayStateError):
        finish_close(conn, site_id, state_date, now_utc=NOW)
    assert _day_row(conn, site_id, state_date) == before

    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, state_date, now_utc=NOW, reason="never_attempted")
    assert _day_row(conn, site_id, state_date) == before


def test_ds3b_finish_close_refuses_stale_truth_then_succeeds() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)

    gen_id = conn.execute("SELECT MAX(id) FROM timezone_generations").fetchone()[0]
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone, stale,
             tz_generation_id)
        VALUES (?, ?, 'temperature_high', 10.0, 1, 24, 24, ?, ?, 'UTC', 1, ?)
        """,
        (
            site_id,
            d_minus_1.isoformat(),
            isoformat_utc(NOW),
            isoformat_utc(NOW + timedelta(hours=1)),
            gen_id,
        ),
    )

    with pytest.raises(DayStateError):
        finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    state = conn.execute(
        "SELECT state FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()[0]
    assert state == "closing"

    conn.execute(
        "UPDATE daily_truth SET stale = 0 WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    )
    finish_close(conn, site_id, d_minus_1, now_utc=NOW)
    state = conn.execute(
        "SELECT state FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()[0]
    assert state == "closed"


def test_ds4_date_order_enforced_for_begin_close_and_mark_missed() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    d_minus_1 = D - timedelta(days=1)
    start_site_days(
        conn,
        site_id,
        d_minus_1,
        now_utc=datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, tzinfo=UTC),
    )
    later = note_day_clock(
        datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC) + timedelta(days=1)
    )
    ensure_site_days(conn, site_id, now_utc=later)

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, D, now_utc=later)
    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, D, now_utc=later, reason="never_attempted")

    mark_missed(conn, site_id, d_minus_1, now_utc=later, reason="never_attempted")
    attempt = begin_close(conn, site_id, D, now_utc=later)
    assert attempt == 1


def test_ds5_closed_through() -> None:
    conn_fresh = asof_conn()
    site_fresh = day_close_site(conn_fresh, with_records=False)
    start_site_days(
        conn_fresh,
        site_fresh,
        D,
        now_utc=datetime(D.year, D.month, D.day, tzinfo=UTC),
    )
    assert closed_through(conn_fresh, site_fresh) is None

    conn = asof_conn()
    site_id = day_close_site(conn)  # D-3 closed, D-2 missed, D-1 open
    assert closed_through(conn, site_id) == D - timedelta(days=2)

    begin_close(conn, site_id, D - timedelta(days=1), now_utc=NOW)
    finish_close(conn, site_id, D - timedelta(days=1), now_utc=NOW)
    assert closed_through(conn, site_id) == D - timedelta(days=1)


def test_ds6_verdict_and_state_are_independent_facts() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    gen_id = conn.execute("SELECT MAX(id) FROM timezone_generations").fetchone()[0]
    day_start = _record_day_end(conn, site_id, D - timedelta(days=2))
    day_end = _record_day_end(conn, site_id, d_minus_1)
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, wet_hours, dry_hours, rain_threshold_mm,
             day_start_utc, day_end_utc, timezone, stale, tz_generation_id)
        VALUES (?, ?, 'precip_total', 1.0, 1, 24, 24, 4, 20, 0.2, ?, ?, 'UTC', 0, ?)
        """,
        (site_id, d_minus_1.isoformat(), day_start, day_end, gen_id),
    )
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, exclusion_reason,
             covered_hours, expected_slots, day_start_utc, day_end_utc,
             timezone, stale, tz_generation_id)
        VALUES (?, ?, 'wind_max', NULL, 0, 'too_thin', 3, 24, ?, ?, 'UTC', 0, ?)
        """,
        (site_id, d_minus_1.isoformat(), day_start, day_end, gen_id),
    )
    before = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?"
            " ORDER BY quantity",
            (site_id, d_minus_1.isoformat()),
        )
    ]

    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    finish_close(conn, site_id, d_minus_1, now_utc=NOW)

    after = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?"
            " ORDER BY quantity",
            (site_id, d_minus_1.isoformat()),
        )
    ]
    assert before == after
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    assert record.state == "closed"
    assert record.missed_reason is None


def test_ds7_retry_timing_and_third_attempt_refused() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    t = _record_day_end(conn, site_id, d_minus_1)
    t_dt = datetime.fromisoformat(t.removesuffix("Z")).replace(tzinfo=UTC) + timedelta(
        minutes=30
    )

    begin_close(conn, site_id, d_minus_1, now_utc=t_dt)
    fail_close(
        conn, site_id, d_minus_1, now_utc=t_dt + timedelta(minutes=10), reason="x"
    )
    row = conn.execute(
        "SELECT state, attempts, retry_at, last_failure FROM site_days"
        " WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert row["state"] == "retry_due"
    assert row["attempts"] == 1
    assert row["retry_at"] == isoformat_utc(t_dt + timedelta(minutes=70))
    assert row["last_failure"] == "x"

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, d_minus_1, now_utc=t_dt + timedelta(minutes=69))

    attempt = begin_close(
        conn, site_id, d_minus_1, now_utc=t_dt + timedelta(minutes=70)
    )
    assert attempt == 2

    fail_close(
        conn, site_id, d_minus_1, now_utc=t_dt + timedelta(minutes=80), reason="y"
    )
    row = conn.execute(
        "SELECT state, missed_reason, last_failure, done_at FROM site_days"
        " WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert row["state"] == "missed"
    assert row["missed_reason"] == "retry_failed"
    assert row["last_failure"] == "y"
    assert row["done_at"] is not None


def _connect_day_close_file(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def test_ds7b_restart_does_not_reset_attempts_or_allow_a_third(tmp_path: Path) -> None:
    db_path = tmp_path / "ds7b.db"
    conn = _connect_day_close_file(db_path)
    run_migrations(conn)
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    conn.commit()
    conn.close()  # the stop: nothing further runs on this connection

    # A new connection to the same file reads the record back.
    reopened = _connect_day_close_file(db_path)
    read_back = reopened.execute(
        "SELECT state, attempts FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert read_back["state"] == "closing"
    assert read_back["attempts"] == 1

    with pytest.raises(DayStateError):
        begin_close(reopened, site_id, d_minus_1, now_utc=NOW)

    fail_close(reopened, site_id, d_minus_1, now_utc=NOW, reason="x")
    retry_at_str = reopened.execute(
        "SELECT retry_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()[0]
    retry_at = parse_utc(str(retry_at_str))
    attempt = begin_close(reopened, site_id, d_minus_1, now_utc=retry_at)
    assert attempt == 2
    reopened.commit()
    reopened.close()

    # A third connection to the same file still sees attempts = 2 and still
    # refuses a third attempt.
    last = _connect_day_close_file(db_path)
    with pytest.raises(DayStateError):
        begin_close(last, site_id, d_minus_1, now_utc=retry_at)
    attempts = last.execute(
        "SELECT attempts FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()[0]
    assert attempts == 2
    last.close()


def test_ds7c_mark_missed_retry_not_run_timing_and_wrong_state() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_1 = D - timedelta(days=1)
    begin_close(conn, site_id, d_minus_1, now_utc=NOW)
    fail_close(conn, site_id, d_minus_1, now_utc=NOW, reason="x")
    retry_at_str = conn.execute(
        "SELECT retry_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()[0]
    retry_at = datetime.fromisoformat(retry_at_str.removesuffix("Z")).replace(
        tzinfo=UTC
    )

    with pytest.raises(DayStateError):
        mark_missed(
            conn,
            site_id,
            d_minus_1,
            now_utc=retry_at - timedelta(minutes=1),
            reason="retry_not_run",
        )
    mark_missed(conn, site_id, d_minus_1, now_utc=retry_at, reason="retry_not_run")
    row = conn.execute(
        "SELECT state, done_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert row["state"] == "missed"
    assert row["done_at"] is not None

    with pytest.raises(DayStateError):
        mark_missed(conn, site_id, D, now_utc=NOW, reason="retry_not_run")


def test_ds9_filter_day_writes_matches_check_day_write() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_3_instant = isoformat_utc(NOW - timedelta(days=3))
    d_minus_1_instant = isoformat_utc(NOW)
    before_first = isoformat_utc(
        datetime(D.year, D.month, D.day, tzinfo=UTC) - timedelta(days=100)
    )
    instants = [d_minus_3_instant, d_minus_1_instant, "not-an-instant", before_first]

    allowed = filter_day_writes(conn, site_id, instants, WriteKind.HOURLY, now_utc=NOW)
    assert allowed == frozenset({d_minus_1_instant})


def test_ds10_writable_pairs_clause() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    feed_id = asof_make_real_feed(conn, "model-x")
    d_minus_3 = D - timedelta(days=3)
    d_minus_1 = D - timedelta(days=1)
    for day in (d_minus_3, d_minus_1):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=isoformat_utc(
                datetime(day.year, day.month, day.day, 6, tzinfo=UTC)
            ),
            issued_at=isoformat_utc(
                datetime(day.year, day.month, day.day, 0, tzinfo=UTC)
            ),
            forecast=10.0,
            observed=9.0,
            first_known_at=None,
        )

    clause = writable_pairs_clause(conn, site_id, now_utc=NOW)
    conn.execute(
        f"DELETE FROM forecast_pairs WHERE site_id = ? AND {clause}", (site_id,)
    )
    remaining = {
        r["valid_at"][:10]
        for r in conn.execute(
            "SELECT valid_at FROM forecast_pairs WHERE site_id = ?", (site_id,)
        )
    }
    assert remaining == {d_minus_3.isoformat()}


def test_ds10_writable_pairs_clause_none_site_id_is_per_site() -> None:
    conn = asof_conn()
    site_a = day_close_site(conn)
    site_b = asof_make_site(conn, "second-site")
    # Plan row :921 wants a second site whose D-1 is also closed, so the
    # None-site_id clause must keep site_b's D-1 pairs while still deleting
    # site_a's (D-1 open).
    d_minus_1 = D - timedelta(days=1)
    start_site_days(
        conn,
        site_b,
        d_minus_1,
        now_utc=datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, tzinfo=UTC),
    )
    ensure_site_days(conn, site_b, now_utc=NOW)
    b1_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_b, d_minus_1.isoformat()),
    ).fetchone()
    assert b1_row is not None
    t_b = parse_utc(b1_row[0]) + timedelta(minutes=30)
    begin_close(conn, site_b, d_minus_1, now_utc=t_b)
    finish_close(conn, site_b, d_minus_1, now_utc=t_b)

    feed_id = asof_make_real_feed(conn, "model-y")
    for site_id in (site_a, site_b):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=isoformat_utc(
                datetime(D.year, D.month, D.day, 6, tzinfo=UTC) - timedelta(days=1)
            ),
            issued_at=isoformat_utc(
                datetime(D.year, D.month, D.day, 0, tzinfo=UTC) - timedelta(days=1)
            ),
            forecast=10.0,
            observed=9.0,
            first_known_at=None,
        )

    clause = writable_pairs_clause(conn, None, now_utc=NOW)
    conn.execute(f"DELETE FROM forecast_pairs WHERE {clause}")
    remaining_sites = {
        r["site_id"] for r in conn.execute("SELECT site_id FROM forecast_pairs")
    }
    assert remaining_sites == {site_b}


def test_ds11_station_list_rule() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    c_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'ISTATION03', 0, 0, 0, 1)",
            (site_id,),
        ).lastrowid
    )
    e_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'ISTATION05', 0, 0, 0, 1)",
            (site_id,),
        ).lastrowid
    )
    f_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'ISTATION06', 0, 0, 0, 1)",
            (site_id,),
        ).lastrowid
    )
    changed_at = isoformat_utc_micro(NOW)
    conn.execute(
        "INSERT INTO station_changes (site_id, station_id, change,"
        " effective_date, reason, changed_at) VALUES (?, ?, 'added', ?, 'test', ?)",
        (site_id, c_id, (D + timedelta(days=1)).isoformat(), changed_at),
    )
    conn.execute(
        "INSERT INTO station_changes (site_id, station_id, change,"
        " effective_date, reason, changed_at) VALUES (?, ?, 'retired', ?, 'test', ?)",
        (site_id, c_id, D.isoformat(), isoformat_utc_micro(NOW + timedelta(seconds=1))),
    )
    tie_changed_at = isoformat_utc_micro(NOW + timedelta(seconds=2))
    turned_off_id = int(
        conn.execute(
            "INSERT INTO station_changes (site_id, station_id, change,"
            " effective_date, reason, changed_at) VALUES (?, ?, 'turned_off', ?,"
            " 'test', ?)",
            (site_id, e_id, D.isoformat(), tie_changed_at),
        ).lastrowid
    )
    turned_on_id = int(
        conn.execute(
            "INSERT INTO station_changes (site_id, station_id, change,"
            " effective_date, reason, changed_at) VALUES (?, ?, 'turned_on', ?,"
            " 'test', ?)",
            (site_id, e_id, D.isoformat(), tie_changed_at),
        ).lastrowid
    )
    assert turned_on_id > turned_off_id

    start_site_days(
        conn, site_id, D, now_utc=datetime(D.year, D.month, D.day, tzinfo=UTC)
    )
    ensure_site_days(conn, site_id, now_utc=NOW)

    ids = day_station_ids(conn, site_id, D + timedelta(days=1))
    assert e_id in ids
    assert c_id not in ids
    assert f_id not in ids


def test_ds12_record_station_change_effective_dates() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=False)
    station_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
            (site_id,),
        ).fetchone()[0]
    )
    added_date = record_station_change(
        conn, site_id, station_id, "added", reason="create_station", now_utc=NOW
    )
    assert added_date == (D + timedelta(days=1)).isoformat()
    off_date = record_station_change(
        conn, site_id, station_id, "turned_off", reason="update_station", now_utc=NOW
    )
    assert off_date == D.isoformat()
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        == 0
    )

    conn2 = asof_conn()
    site2 = day_close_site(conn2, timezone="Etc/GMT-14", with_records=False)
    station2 = int(
        conn2.execute(
            "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
            (site2,),
        ).fetchone()[0]
    )
    added2 = record_station_change(
        conn2, site2, station2, "added", reason="create_station", now_utc=NOW
    )
    assert added2 == (D + timedelta(days=2)).isoformat()
    off2 = record_station_change(
        conn2, site2, station2, "turned_off", reason="update_station", now_utc=NOW
    )
    assert off2 == (D + timedelta(days=1)).isoformat()


def test_ds13_recompute_after_each_change() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    a_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE pws_station_id = 'ISTATION01'"
        ).fetchone()[0]
    )
    b_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
        ).fetchone()[0]
    )

    def _list(day: date) -> frozenset[int]:
        return day_station_ids(conn, site_id, day)

    d_minus_1 = D - timedelta(days=1)
    d_plus_1 = D + timedelta(days=1)

    record_station_change(
        conn, site_id, a_id, "retired", reason="delete_station", now_utc=NOW
    )
    assert _list(D) == frozenset({b_id})
    assert _list(d_plus_1) == frozenset({b_id})
    assert _list(d_minus_1) == frozenset({a_id, b_id})
    assert _list(D - timedelta(days=3)) == frozenset({a_id, b_id})
    assert _list(D - timedelta(days=2)) == frozenset({a_id, b_id})

    c_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'ISTATION03', 0, 0, 0, 1)",
            (site_id,),
        ).lastrowid
    )
    record_station_change(
        conn, site_id, c_id, "added", reason="create_station", now_utc=NOW
    )
    assert c_id in _list(d_plus_1)
    assert c_id not in _list(D)

    record_station_change(
        conn, site_id, a_id, "turned_on", reason="update_station", now_utc=NOW
    )
    assert _list(d_plus_1) == frozenset({a_id, b_id, c_id})
    assert _list(D) == frozenset({b_id})

    record_station_change(
        conn, site_id, b_id, "retired", reason="delete_station", now_utc=NOW
    )
    assert _list(D) == frozenset()
    assert _list(d_plus_1) == frozenset({a_id, c_id})
    assert _list(d_minus_1) == frozenset({a_id, b_id})


def test_ds14_current_day_has_other_station() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    a_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE pws_station_id = 'ISTATION01'"
        ).fetchone()[0]
    )
    b_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
        ).fetchone()[0]
    )

    assert current_day_has_other_station(conn, site_id, b_id, now_utc=NOW) is True
    record_station_change(
        conn, site_id, b_id, "retired", reason="delete_station", now_utc=NOW
    )
    assert current_day_has_other_station(conn, site_id, a_id, now_utc=NOW) is False

    conn_off = asof_conn()
    site_off = day_close_site(conn_off, close_on=False, with_records=False)
    station_off = int(
        conn_off.execute(
            "SELECT id FROM stations WHERE site_id = ? LIMIT 1", (site_off,)
        ).fetchone()[0]
    )
    assert (
        current_day_has_other_station(conn_off, site_off, station_off, now_utc=NOW)
        is True
    )

    conn_no_record = asof_conn()
    site_no_record = day_close_site(conn_no_record, with_records=False)
    station_no_record = int(
        conn_no_record.execute(
            "SELECT id FROM stations WHERE site_id = ? LIMIT 1", (site_no_record,)
        ).fetchone()[0]
    )
    assert (
        current_day_has_other_station(
            conn_no_record, site_no_record, station_no_record, now_utc=NOW
        )
        is True
    )

    other_station_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'ISTATION09', 0, 0, 0, 1)",
            (site_id,),
        ).lastrowid
    )
    assert (
        current_day_has_other_station(conn, site_id, other_station_id, now_utc=NOW)
        is True
    )


def test_ds15_enter_zone_change_creates_changeover_day() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)  # UTC
    d_minus_3_before = dict(
        conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, (D - timedelta(days=3)).isoformat()),
        ).fetchone()
    )
    d_before = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date <= ?"
            " ORDER BY local_date",
            (site_id, D.isoformat()),
        )
    ]

    c = enter_zone_change(conn, site_id, "Etc/GMT-1", now_utc=NOW)
    assert c == D + timedelta(days=1)

    row = conn.execute(
        "SELECT day_start_utc, day_end_utc, timezone, zone_changeover FROM"
        " site_days WHERE site_id = ? AND local_date = ?",
        (site_id, c.isoformat()),
    ).fetchone()
    assert row["day_start_utc"] == isoformat_utc(
        datetime(c.year, c.month, c.day, tzinfo=UTC)
    )
    assert row["day_end_utc"] == isoformat_utc(
        datetime(c.year, c.month, c.day, 23, tzinfo=UTC)
    )
    assert row["timezone"] == "Etc/GMT-1"
    assert row["zone_changeover"] == 1

    zone_rows = conn.execute(
        "SELECT effective_date FROM site_zone_changes WHERE site_id = ?", (site_id,)
    ).fetchall()
    assert len(zone_rows) == 1
    assert zone_rows[0]["effective_date"] == c.isoformat()

    timezone_now = conn.execute(
        "SELECT timezone FROM sites WHERE id = ?", (site_id,)
    ).fetchone()[0]
    assert timezone_now == "Etc/GMT-1"

    after = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date <= ?"
            " ORDER BY local_date",
            (site_id, D.isoformat()),
        )
    ]
    assert before_equal_unchanged(d_before, after)
    assert (
        dict(
            conn.execute(
                "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
                (site_id, (D - timedelta(days=3)).isoformat()),
            ).fetchone()
        )
        == d_minus_3_before
    )


def before_equal_unchanged(before: list[dict], after: list[dict]) -> bool:
    return before == after


def test_ds15c_enter_zone_change_when_no_day_has_started() -> None:
    for timezone, t_hour, expected_c_offset, expected_zone in (
        ("Etc/GMT-14", 12, 2, "Etc/GMT-14"),
        ("Etc/GMT+12", 6, 0, "Etc/GMT+12"),
    ):
        conn = asof_conn()
        site_id = day_close_site(conn, with_records=False)
        t = datetime(D.year, D.month, D.day, t_hour, tzinfo=UTC)
        start_site_days(conn, site_id, D + timedelta(days=1), now_utc=t)

        c = enter_zone_change(conn, site_id, timezone, now_utc=t)
        expected_c = D + timedelta(days=expected_c_offset)
        assert c == expected_c

        rows = conn.execute(
            "SELECT local_date, zone_changeover, timezone FROM site_days"
            " WHERE site_id = ?",
            (site_id,),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["local_date"] == c.isoformat()
        assert rows[0]["zone_changeover"] == 0
        assert rows[0]["timezone"] == timezone

        zone_count = conn.execute(
            "SELECT COUNT(*) FROM site_zone_changes WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        assert zone_count == 1
        sites_tz = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()[0]
        assert sites_tz == expected_zone


def test_ds16_enter_zone_change_refusals_and_acceptance() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    with pytest.raises(ValueError, match="close"):
        enter_zone_change(conn, site_id, "Etc/GMT-1", now_utc=NOW)

    conn = asof_conn()
    site_id = day_close_site(conn)
    with pytest.raises(ValueError):
        enter_zone_change(conn, site_id, "Not/AZone", now_utc=NOW)
    with pytest.raises(ValueError):
        enter_zone_change(conn, site_id, "UTC", now_utc=NOW)

    before = [
        dict(r)
        for r in conn.execute("SELECT * FROM site_days WHERE site_id = ?", (site_id,))
    ]
    zone_rows_before = conn.execute(
        "SELECT COUNT(*) FROM site_zone_changes WHERE site_id = ?", (site_id,)
    ).fetchone()[0]
    tz_before = conn.execute(
        "SELECT timezone FROM sites WHERE id = ?", (site_id,)
    ).fetchone()[0]

    # Plan row :928 doesn't list UTC -> Etc/GMT+12 as a refusal: current is
    # D (day_end_utc = D+1 00:00Z); C = D+1; local_day_slots(D+1, GMT+12)[1]
    # (GMT+12 = UTC-12) is D+2 12:00Z, so the span is 36h -- accepted, not
    # refused -- so there is no refusal case to assert here.

    # Each site below is built genuinely in its starting zone via
    # day_close_site(timezone=...), so day_start_utc/day_end_utc are
    # correctly computed for that zone (not just a relabeled column).
    conn_a = asof_conn()
    site_a = day_close_site(conn_a, timezone="Etc/GMT+12")  # UTC-12
    # current = D (NOW=D 12:00Z is D 00:00 local); day_end_utc = D+1 12:00Z.
    # C = D+1. Etc/GMT-14 = UTC+14; local_day_slots(D+1, GMT-14)[1] =
    # D+1 10:00Z. span = D+1 10:00Z - D+1 12:00Z = -2h <= 0h: refused.
    with pytest.raises(ValueError):
        enter_zone_change(conn_a, site_a, "Etc/GMT-14", now_utc=NOW)  # -2h span

    conn_b = asof_conn()
    site_b = day_close_site(conn_b, timezone="Etc/GMT-14")  # UTC+14
    # current = D+1 (NOW=D 12:00Z is D+1 02:00 local); day_end_utc = D+1
    # 10:00Z. C = D+2. Etc/GMT+12 = UTC-12; local_day_slots(D+2, GMT+12)[1]
    # = D+3 12:00Z. span = D+3 12:00Z - D+1 10:00Z = 50h >= 48h: refused.
    with pytest.raises(ValueError):
        enter_zone_change(conn_b, site_b, "Etc/GMT+12", now_utc=NOW)  # 50h span

    conn_c = asof_conn()
    site_c = day_close_site(conn_c, timezone="Etc/GMT-12")  # UTC+12
    # current = D+1 (NOW=D 12:00Z is exactly D+1 00:00 local, the start of
    # D+1); day_end_utc = D+1 12:00Z. C = D+2. Etc/GMT+12 = UTC-12;
    # local_day_slots(D+2, GMT+12)[1] = D+3 12:00Z. span = D+3 12:00Z -
    # D+1 12:00Z = 48h, the >= 48h boundary: refused.
    with pytest.raises(ValueError):
        enter_zone_change(conn_c, site_c, "Etc/GMT+12", now_utc=NOW)  # 48h span

    after = [
        dict(r)
        for r in conn.execute("SELECT * FROM site_days WHERE site_id = ?", (site_id,))
    ]
    zone_rows_after = conn.execute(
        "SELECT COUNT(*) FROM site_zone_changes WHERE site_id = ?", (site_id,)
    ).fetchone()[0]
    tz_after = conn.execute(
        "SELECT timezone FROM sites WHERE id = ?", (site_id,)
    ).fetchone()[0]
    assert before == after
    assert zone_rows_before == zone_rows_after
    assert tz_before == tz_after

    conn_d = asof_conn()
    site_d = day_close_site(conn_d, timezone="Etc/GMT-12")  # UTC+12
    # Same current/day_end_utc as conn_c above (D+1, day_end_utc D+1
    # 12:00Z). C = D+2. Etc/GMT+11 = UTC-11; local_day_slots(D+2, GMT+11)[1]
    # = D+3 11:00Z. span = D+3 11:00Z - D+1 12:00Z = 47h: accepted.
    c = enter_zone_change(
        conn_d, site_d, "Etc/GMT+11", now_utc=NOW
    )  # 47h span: accepted
    assert c == D + timedelta(days=2)


def test_ds16b_changeover_zero_when_zones_agree_and_second_change_wins() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    c = enter_zone_change(conn, site_id, "Etc/GMT0", now_utc=NOW)  # UTC == GMT0
    row = conn.execute(
        "SELECT zone_changeover FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, c.isoformat()),
    ).fetchone()
    assert row["zone_changeover"] == 0

    conn2 = asof_conn()
    site2 = day_close_site(conn2)
    c1 = enter_zone_change(conn2, site2, "Etc/GMT-1", now_utc=NOW)
    c2 = enter_zone_change(conn2, site2, "UTC", now_utc=NOW + timedelta(seconds=1))
    assert c1 == c2
    row2 = conn2.execute(
        "SELECT zone_changeover, timezone FROM site_days"
        " WHERE site_id = ? AND local_date = ?",
        (site2, c2.isoformat()),
    ).fetchone()
    assert row2["zone_changeover"] == 0
    assert row2["timezone"] == "UTC"
    zone_rows = conn2.execute(
        "SELECT COUNT(*) FROM site_zone_changes WHERE site_id = ?", (site2,)
    ).fetchone()[0]
    assert zone_rows == 2


def test_ds17_changeover_day_states() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    c = enter_zone_change(
        conn, site_id, "Etc/GMT-1", now_utc=NOW
    )  # c = D+1, changeover

    # D-1 and D (left open by day_close_site/enter_zone_change) must be
    # done before c (D+1) can be marked missed -- 4.5/table 4.7's
    # date-order rule -- so the refusals below test the changeover rule
    # itself, not an unrelated "earlier day not done".
    d_minus_1 = D - timedelta(days=1)
    mark_missed(
        conn,
        site_id,
        d_minus_1,
        now_utc=parse_utc(_record_day_end(conn, site_id, d_minus_1)),
        reason="never_attempted",
    )
    mark_missed(
        conn,
        site_id,
        D,
        now_utc=parse_utc(_record_day_end(conn, site_id, D)),
        reason="never_attempted",
    )

    before_end_instant = isoformat_utc(datetime(c.year, c.month, c.day, 12, tzinfo=UTC))
    before_end = note_day_clock(datetime(c.year, c.month, c.day, 12, tzinfo=UTC))
    assert check_day_write(
        conn, site_id, before_end_instant, WriteKind.READING, now_utc=before_end
    )

    with pytest.raises(DayStateError):
        begin_close(conn, site_id, c, now_utc=NOW + timedelta(days=2))
    with pytest.raises(DayStateError):
        mark_missed(
            conn, site_id, c, now_utc=NOW + timedelta(days=2), reason="never_attempted"
        )

    mark_missed(
        conn, site_id, c, now_utc=NOW + timedelta(days=2), reason="zone_changeover"
    )
    row = conn.execute(
        "SELECT state, missed_reason FROM site_days"
        " WHERE site_id = ? AND local_date = ?",
        (site_id, c.isoformat()),
    ).fetchone()
    assert row["state"] == "missed"
    assert row["missed_reason"] == "zone_changeover"

    with pytest.raises(DayStateError):
        mark_missed(
            conn,
            site_id,
            c + timedelta(days=1),
            now_utc=NOW + timedelta(days=3),
            reason="zone_changeover",
        )


def test_ds17b_zone_flag_after_a_change() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_3 = D - timedelta(days=3)
    before = dict(
        conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d_minus_3.isoformat()),
        ).fetchone()
    )
    truth_before = conn.execute(
        "SELECT COUNT(*) FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_3.isoformat()),
    ).fetchone()[0]

    assert zone_flagged(conn, site_id, d_minus_3) is False
    c = enter_zone_change(conn, site_id, "Etc/GMT-1", now_utc=NOW)

    assert zone_flagged(conn, site_id, d_minus_3) is True
    assert zone_flagged(conn, site_id, D) is True
    assert zone_flagged(conn, site_id, c) is False

    after = dict(
        conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d_minus_3.isoformat()),
        ).fetchone()
    )
    truth_after = conn.execute(
        "SELECT COUNT(*) FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_3.isoformat()),
    ).fetchone()[0]
    assert before == after
    assert truth_before == truth_after


def test_ds18_daily_close_on_not_cached() -> None:
    conn = asof_conn()
    asof_make_site(conn, "no-close-site")
    assert daily_close_on(conn) is False
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    assert daily_close_on(conn) is True

    conn2 = asof_conn()
    assert daily_close_on(conn2) is False
    set_runtime_state(conn2, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    assert daily_close_on(conn2) is True
    assert daily_close_on(conn2) is True  # a cached first read must not stick


def test_ds19_import_rule_of_site_days_module() -> None:
    source_path = Path(wxverify.db.site_days.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    allowed_third_party = {"wxverify.core.timeutil", "wxverify.db.runtime_state"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                assert top not in ("wxverify",) or alias.name in allowed_third_party, (
                    f"unexpected import {alias.name!r} in db/site_days.py"
                )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.startswith("wxverify"):
                assert module in allowed_third_party, (
                    f"unexpected import from {module!r} in db/site_days.py"
                )


def test_ds20_close_off_site_takes_no_write_but_table_tables_stay_empty() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=False)
    ensure_site_days(conn, site_id, now_utc=NOW)
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM site_day_stations WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM site_zone_changes WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        == 0
    )

    for kind in _ALL_KINDS:
        expected = kind in (
            WriteKind.READING,
            WriteKind.HOURLY,
            WriteKind.TRUTH,
            WriteKind.PAIRS,
        )
        assert (
            check_day_write(conn, site_id, isoformat_utc(NOW), kind, now_utc=NOW)
            is expected
        )
        assert (
            check_day_write(conn, site_id, "not-an-instant", kind, now_utc=NOW)
            is expected
        )

    assert writable_pairs_clause(conn, site_id, now_utc=NOW) == "1"
    assert (
        open_day_clause(
            conn, site_expr="observations.site_id", instant_expr="observations.valid_at"
        )
        == "1"
    )


def test_ds21_open_day_clause_per_site_and_validation() -> None:
    conn = asof_conn()
    site_a = day_close_site(conn)
    site_b_conn = asof_conn()
    site_b_conn.close()
    asof_make_site(conn, "second-open-day-site")
    # second-open-day-site stays close-off; only site_a has records.

    clause = open_day_clause(conn, site_expr="o.site_id", instant_expr="o.valid_at")
    for site_id, local_date, expect_open in (
        (site_a, D - timedelta(days=3), False),  # closed
        (site_a, D - timedelta(days=1), True),  # open
    ):
        instant = isoformat_utc(
            datetime(local_date.year, local_date.month, local_date.day, 6, tzinfo=UTC)
        )
        conn.execute(
            "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
            " VALUES (?, 'temperature', ?, 1.0, 1)",
            (site_id, instant),
        )
        row = conn.execute(
            f"SELECT 1 FROM observations AS o WHERE o.site_id = ? AND o.valid_at = ?"
            f" AND {clause}",
            (site_id, instant),
        ).fetchone()
        assert (row is not None) is expect_open

    with pytest.raises(ValueError):
        open_day_clause(conn, site_expr="site_id", instant_expr="valid_at")

    conn_off = asof_conn()
    day_close_site(conn_off, close_on=False, with_records=False)
    assert (
        open_day_clause(conn_off, site_expr="o.site_id", instant_expr="o.valid_at")
        == "1"
    )


def test_ds22_raw_prune_counted_from_the_later_done_at() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_3 = D - timedelta(days=3)
    d_minus_2 = D - timedelta(days=2)
    d_minus_1 = D - timedelta(days=1)

    t2 = datetime.fromisoformat(
        conn.execute(
            "SELECT done_at FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d_minus_2.isoformat()),
        )
        .fetchone()[0]
        .removesuffix("Z")
    ).replace(tzinfo=UTC)

    just_before = note_day_clock(t2 + timedelta(days=7) - timedelta(seconds=1))
    assert (
        check_date_write(
            conn, site_id, d_minus_3, WriteKind.RAW_PRUNE, now_utc=just_before
        )
        is False
    )
    at_bound = note_day_clock(t2 + timedelta(days=7))
    assert (
        check_date_write(
            conn, site_id, d_minus_3, WriteKind.RAW_PRUNE, now_utc=at_bound
        )
        is True
    )

    assert (
        check_date_write(
            conn, site_id, d_minus_2, WriteKind.RAW_PRUNE, now_utc=at_bound
        )
        is False
    )  # D-1 still open

    begin_close(conn, site_id, d_minus_1, now_utc=at_bound)
    finish_close(conn, site_id, d_minus_1, now_utc=at_bound)
    t3 = at_bound
    before_7 = note_day_clock(t3 + timedelta(days=7) - timedelta(seconds=1))
    assert (
        check_date_write(
            conn, site_id, d_minus_2, WriteKind.RAW_PRUNE, now_utc=before_7
        )
        is False
    )
    after_7 = note_day_clock(t3 + timedelta(days=7))
    assert (
        check_date_write(conn, site_id, d_minus_2, WriteKind.RAW_PRUNE, now_utc=after_7)
        is True
    )


def test_ds23_local_clock_utc() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn, timezone="Etc/GMT-14", with_records=False)
    ensure_site_days(conn, site_id, now_utc=NOW)
    record = day_record(conn, site_id, D + timedelta(days=2))
    assert record is not None

    assert local_clock_utc(record, 23, 0) == datetime(
        D.year, D.month, D.day, tzinfo=UTC
    ) + timedelta(days=2, hours=9)
    assert local_clock_utc(record, 23, 59) == datetime(
        D.year, D.month, D.day, tzinfo=UTC
    ) + timedelta(days=2, hours=9, minutes=59)

    conn2 = asof_conn()
    site2 = day_close_site(conn2)
    c = enter_zone_change(conn2, site2, "Etc/GMT-1", now_utc=NOW)
    changeover_record = day_record(conn2, site2, c)
    assert changeover_record is not None
    assert local_clock_utc(changeover_record, 23, 0) == datetime(
        c.year, c.month, c.day, 22, tzinfo=UTC
    )

    # Local 00:00 on the changeover day C is before the record's own
    # day_start_utc (C 00:00Z, since the changeover day starts later than
    # an ordinary local midnight: C 00:00 local under Etc/GMT-1 is D
    # 23:00Z) -- outside bounds, unlike local 00:00 on an ordinary record
    # (its own day_start_utc, always inside bounds).
    with pytest.raises(ValueError):
        local_clock_utc(changeover_record, 0, 0)


def test_ds24_start_site_days_refuses_a_second_first_record_and_a_past_date() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    with pytest.raises(DayStateError):
        start_site_days(conn, site_id, D, now_utc=NOW)

    conn2 = asof_conn()
    site2 = day_close_site(conn2, with_records=False)
    with pytest.raises(DayStateError):
        start_site_days(conn2, site2, D - timedelta(days=1), now_utc=NOW)

    start_site_days(conn2, site2, D, now_utc=NOW)
    row = conn2.execute(
        "SELECT MIN(local_date), MAX(local_date) FROM site_days WHERE site_id = ?",
        (site2,),
    ).fetchone()
    assert row[0] == D.isoformat()
    assert row[1] == (D + timedelta(days=1)).isoformat()


def test_ds25_daily_close_on_at() -> None:
    conn = asof_conn()
    asof_make_site(conn, "ds25-site")
    assert daily_close_on_at(conn) is None

    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    value = daily_close_on_at(conn)
    assert value == NOW
    assert value.tzinfo is not None

    conn2 = asof_conn()
    set_runtime_state(conn2, DAILY_CLOSE_ON_KEY, "not-a-stamp")
    with pytest.raises(ValueError):
        daily_close_on_at(conn2)


def test_ds26_open_day_on_copy(tmp_path: Path) -> None:
    live_path = tmp_path / "live.db"
    live_conn = sqlite3.connect(str(live_path))
    live_conn.row_factory = sqlite3.Row
    live_conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(live_conn)
    day_close_site(live_conn, close_on=False, with_records=False)
    live_conn.commit()
    live_conn.close()

    def _new_copy(name: str) -> tuple[sqlite3.Connection, Path, int]:
        copy_path = tmp_path / name
        copy_conn = sqlite3.connect(str(copy_path))
        copy_conn.row_factory = sqlite3.Row
        copy_conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(copy_conn)
        site_id = day_close_site(copy_conn, close_on=False, with_records=False)
        copy_conn.commit()
        return copy_conn, copy_path, site_id

    marked_conn, marked_path, marked_site = _new_copy("marked.db")
    set_runtime_state(marked_conn, OFFLINE_COPY_KEY, isoformat_utc(NOW))
    marked_conn.commit()

    d_minus_5 = D - timedelta(days=5)
    open_day_on_copy(
        marked_conn, marked_site, d_minus_5, now_utc=NOW, live_db_path=live_path
    )
    row = marked_conn.execute(
        "SELECT state, day_start_utc, day_end_utc, timezone FROM site_days"
        " WHERE site_id = ?",
        (marked_site,),
    ).fetchone()
    assert row["state"] == "open"
    assert row["day_start_utc"] == isoformat_utc(
        datetime(d_minus_5.year, d_minus_5.month, d_minus_5.day, tzinfo=UTC)
    )
    assert row["day_end_utc"] == isoformat_utc(
        datetime(d_minus_5.year, d_minus_5.month, d_minus_5.day, tzinfo=UTC)
        + timedelta(days=1)
    )
    assert row["timezone"] == "UTC"
    assert get_runtime_state(marked_conn, DAILY_CLOSE_ON_KEY) == isoformat_utc(NOW)

    with pytest.raises(DayStateError):
        open_day_on_copy(
            marked_conn, marked_site, d_minus_5, now_utc=NOW, live_db_path=live_path
        )

    with pytest.raises(DayStateError):
        open_day_on_copy(
            marked_conn, marked_site, d_minus_5, now_utc=NOW, live_db_path=marked_path
        )

    unmarked_conn, unmarked_path, unmarked_site = _new_copy("unmarked.db")
    with pytest.raises(DayStateError):
        open_day_on_copy(
            unmarked_conn, unmarked_site, d_minus_5, now_utc=NOW, live_db_path=live_path
        )
    assert (
        unmarked_conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (unmarked_site,)
        ).fetchone()[0]
        == 0
    )
    assert get_runtime_state(unmarked_conn, DAILY_CLOSE_ON_KEY) is None

    fresh_conn, fresh_path, fresh_site = _new_copy("fresh.db")
    set_runtime_state(fresh_conn, OFFLINE_COPY_KEY, isoformat_utc(NOW))
    fresh_conn.commit()
    with pytest.raises(DayStateError):
        start_site_days(fresh_conn, fresh_site, d_minus_5, now_utc=NOW)


def test_ds26b_seed_reaches_a_copys_past_day(tmp_path: Path) -> None:
    from wxverify.db.migrations import migrate_v11_daily_close_days

    db_path = tmp_path / "ds26b.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "ds26b-site")
    a_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, 'ISTATION01', 0, 0, 0, 1)",
            (site_id,),
        ).lastrowid
    )
    conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, 'ISTATION02', 0, 0, 0, 0)",
        (site_id,),
    )
    conn.commit()

    migrate_at = datetime(D.year, D.month, D.day, 0, 30, tzinfo=UTC)
    import wxverify.db.migrations as migrations_module

    original_utc_now = migrations_module.utc_now
    migrations_module.utc_now = lambda: migrate_at  # type: ignore[assignment]
    try:
        migrate_v11_daily_close_days(conn)
    finally:
        migrations_module.utc_now = original_utc_now
    conn.commit()

    set_runtime_state(conn, OFFLINE_COPY_KEY, isoformat_utc(NOW))
    conn.commit()

    d_minus_30 = D - timedelta(days=30)
    other_live_path = tmp_path / "ds26b-live.db"
    open_day_on_copy(
        conn, site_id, d_minus_30, now_utc=NOW, live_db_path=other_live_path
    )

    assert day_station_ids(conn, site_id, d_minus_30) == frozenset({a_id})

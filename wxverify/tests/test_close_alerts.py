"""The close and station alerts (daily-close plan 2026-10-22-daily-close-
alerts-and-pages.md §5, §6; test IDs AL1-AL18 in this file's §13.3; §13.4's
AD-prefixed rows and §13.7 are appended here by a later writer).

Written before ``wxverify.close_monitor`` exists: every test below imports
it (or a name it will add) inside its own body, so the rest of this file
keeps collecting -- each then fails at its own ``ModuleNotFoundError``, the
failure Step 3 accepts.

Real SQLite throughout (``tests.helpers.asof_conn``, real migrations);
nothing in the code under test is mocked. Synthetic data only: stations
``ISTATION01``/``ISTATION02``/``ISTATION03`` (via ``day_close_site`` and its
``station_ids`` override), zone ``UTC``, dates built from the fixture's
``D``/``NOW``. P1's and P3's own functions move every day state; no test
writes ``site_days`` directly.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import (
    NOW,
    D,
    asof_conn,
    day_close_site,
    seed_active_publication,
    seed_attempt,
    seed_publication_state,
    seed_try,
)
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.site_days import (
    begin_close,
    current_day,
    day_record,
    ensure_site_days,
    enter_zone_change,
    fail_close,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# §13.4/§13.7 fixtures (plan §13.4 lede): T is 07:00 on D in the site's
# (UTC) zone; each expected instant below is written out, never computed
# with the code under test. AD1-AD7, AD10, AD12 and AD13 share
# ``_ad_start``: the helper's own records, started with ``start_site_days``
# at D-3 12:00Z then grown with ``ensure_site_days`` at NOW, with D-3 and
# D-2 both closed (``begin_close``/``finish_close``) just after each day's
# own end; D-1 is left for each test to set.
# ---------------------------------------------------------------------------

_AD_T = datetime(D.year, D.month, D.day, 7, 0, tzinfo=UTC)


def _ad_start(conn, *, close_on: bool = True) -> int:
    site_id = day_close_site(conn, close_on=close_on, with_records=False)
    d_minus_3 = D - timedelta(days=3)
    start_site_days(
        conn,
        site_id,
        d_minus_3,
        now_utc=datetime(
            d_minus_3.year, d_minus_3.month, d_minus_3.day, 12, tzinfo=UTC
        ),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)
    for d in (d_minus_3, D - timedelta(days=2)):
        record = day_record(conn, site_id, d)
        assert record is not None
        t = record.day_end_utc + timedelta(minutes=5)
        begin_close(conn, site_id, d, now_utc=t)
        finish_close(conn, site_id, d, now_utc=t)
    return site_id


def _close_d_minus_1(conn, site_id: int):
    """Close D-1 just after its own end; returns the close instant."""
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t)
    finish_close(conn, site_id, d_minus_1, now_utc=t)
    return t


_SITE_NAME = "day-close-site"


def _close_day(conn, site_id: int, local_date, *, first: bool) -> None:
    """Grow the site's records through `local_date`, then close it.

    `first` uses `start_site_days`, since `ensure_site_days` on a site with
    no record yet starts the day AFTER `now`'s local date, not
    `local_date` itself. Both calls' internal `_fill` always creates one
    buffer day beyond its target, so any earlier day left `open` is closed
    here too, with no presence at all -- alert 3 skips a closed day with
    no reading and no answered try either way (plan 5.4), so a bare buffer
    day never counts against, or breaks, a station's run.
    """
    now_for_create = datetime(
        local_date.year, local_date.month, local_date.day, 1, tzinfo=UTC
    )
    if first:
        start_site_days(conn, site_id, local_date, now_utc=now_for_create)
    else:
        ensure_site_days(conn, site_id, now_utc=now_for_create)
    _close_gaps_before(conn, site_id, local_date)
    record = day_record(conn, site_id, local_date)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, local_date, now_utc=t)
    finish_close(conn, site_id, local_date, now_utc=t)


def _close_gaps_before(conn, site_id: int, local_date) -> None:
    """Close (plainly, with no presence) every earlier day still `open`
    from a growth call's buffer day, so a later `begin_close`/`mark_missed`
    never trips on `_has_earlier_undone`. See `_close_day`'s docstring."""
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


def _set_answered(conn, station_id: int, local_date, *, answered: bool = True) -> None:
    """A 204-equivalent 5-minute try: `last_ok_at` set, no raw readings."""
    stamp = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC)
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, last_ok_at, updated_at)"
        " VALUES (?, ?, 'fetched', ?, ?)",
        (station_id, local_date.isoformat(), stamp if answered else None, stamp),
    )


def _set_refused(conn, station_id: int, local_date) -> None:
    stamp = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC)
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, refused_at, updated_at)"
        " VALUES (?, ?, 'pending', ?, ?)",
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


def _station_id(conn, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_truth(
    conn,
    *,
    site_id: int,
    local_date,
    quantity: str,
    eligible: int,
    exclusion_reason: str | None,
) -> None:
    record = day_record(conn, site_id, local_date)
    assert record is not None
    generation_id = ensure_site_generation(conn, site_id)
    conn.execute(
        "INSERT INTO daily_truth"
        " (site_id, local_date, quantity, value, eligible, exclusion_reason,"
        "  covered_hours, expected_slots, day_start_utc, day_end_utc,"
        "  timezone, tz_generation_id)"
        " VALUES (?, ?, ?, NULL, ?, ?, 24, 24, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            quantity,
            eligible,
            exclusion_reason,
            isoformat_utc(record.day_start_utc),
            isoformat_utc(record.day_end_utc),
            record.timezone,
            generation_id,
        ),
    )


def _full_truth(conn, *, site_id: int, local_date) -> None:
    for quantity in (
        "temperature_high",
        "temperature_low",
        "wind_max",
        "precip_total",
        "precip_occurrence",
    ):
        _insert_truth(
            conn,
            site_id=site_id,
            local_date=local_date,
            quantity=quantity,
            eligible=1,
            exclusion_reason=None,
        )


def test_al1_day_missed_never_attempted() -> None:
    """AL1: the helper's D-2, missed never_attempted (M11)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    day_close_site(conn, close_on=True)

    alerts = close_alerts(conn, NOW)
    missed = next(a for a in alerts if a.id == "close_day_missed")
    d_minus_2 = (D - timedelta(days=2)).isoformat()
    expected = (
        f"{_SITE_NAME} {d_minus_2}: missed, the close never ran (the add-on"
        " was not running when it was due)"
    )
    assert missed.tripped and missed.count == 1 and missed.detail == expected, (
        f"M11 -> at this assertion: correct = tripped, count=1, detail="
        f"{expected!r}, mutant = tripped={missed.tripped},"
        f" count={missed.count}, detail={missed.detail!r}"
    )


def test_al2_both_attempts_failed_retry_stage() -> None:
    """AL2: D-1 closing -> retry_due -> closing -> missed, each attempt's
    record failing at a different stage (R3.4 3; M12)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None

    t1 = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t1)
    seed_attempt(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        attempt=1,
        started_at=t1,
        last_stage=1,
        outcome="failed",
        failed_stage=2,
        failure_reason="gap fill: http 503",
        ended_at=t1,
    )
    fail_close(conn, site_id, d_minus_1, now_utc=t1, reason="gap fill: http 503")

    record = day_record(conn, site_id, d_minus_1)
    assert record is not None and record.retry_at is not None
    t2 = record.retry_at
    begin_close(conn, site_id, d_minus_1, now_utc=t2)
    seed_attempt(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        attempt=2,
        started_at=t2,
        last_stage=2,
        outcome="failed",
        failed_stage=3,
        failure_reason="stage 3: db locked",
        ended_at=t2,
    )
    fail_close(conn, site_id, d_minus_1, now_utc=t2, reason="stage 3: db locked")

    alerts = close_alerts(conn, NOW)
    missed = next(a for a in alerts if a.id == "close_day_missed")
    expected = (
        f"{_SITE_NAME} {d_minus_1.isoformat()}: missed, both attempts"
        " failed, the retry at stage 3 hourly values: stage 3: db locked"
    )
    assert expected in missed.detail, (
        f"M12 -> at this assertion: correct = {expected!r} present, mutant ="
        f" {missed.detail!r} (the retry's stage taken from attempt 1's"
        " failed_stage, 2, not attempt 2's, 3)"
    )


def test_al3_retry_not_run() -> None:
    """AL3: D-1 retry_due, then mark_missed(retry_not_run) (M13)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t1 = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t1)
    fail_close(conn, site_id, d_minus_1, now_utc=t1, reason="network: timeout")

    record = day_record(conn, site_id, d_minus_1)
    assert record is not None and record.retry_at is not None
    mark_missed(
        conn, site_id, d_minus_1, now_utc=record.retry_at, reason="retry_not_run"
    )

    alerts = close_alerts(conn, NOW)
    missed = next(a for a in alerts if a.id == "close_day_missed")
    expected = (
        f"{_SITE_NAME} {d_minus_1.isoformat()}: missed, the retry fell due"
        " while the add-on was not running; attempt 1 failed: network: timeout"
    )
    assert expected in missed.detail, (
        f"M13 -> at this assertion: correct = {expected!r} present, mutant ="
        f" {missed.detail!r} (retry_not_run given the never_attempted text)"
    )


def test_al4_window_clears_after_7_days() -> None:
    """AL4: AL1 with now at done_at + 7 days, then + 7 days + 1 s (M14)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_2 = D - timedelta(days=2)
    record = day_record(conn, site_id, d_minus_2)
    assert record is not None and record.done_at is not None
    from wxverify.core.timeutil import parse_utc

    done_at = parse_utc(record.done_at)

    at_boundary = close_alerts(conn, done_at + timedelta(days=7))
    missed = next(a for a in at_boundary if a.id == "close_day_missed")
    assert missed.tripped, (
        "M14 -> at this assertion: correct = tripped at done_at + 7 days,"
        f" mutant = tripped={missed.tripped}"
    )

    past_window = close_alerts(conn, done_at + timedelta(days=7, seconds=1))
    missed = next(a for a in past_window if a.id == "close_day_missed")
    assert not missed.tripped, (
        "M14 -> at this assertion: correct = ok 1 s past the 7-day window,"
        f" mutant = tripped={missed.tripped}"
    )


def test_al5_changeover_day_not_alerted() -> None:
    """AL5: a zone change entered for D+1, moved to missed(zone_changeover)
    (M15)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    change_date = enter_zone_change(conn, site_id, "Etc/GMT-1", now_utc=NOW)
    assert change_date == D + timedelta(days=1)
    record = day_record(conn, site_id, change_date)
    assert record is not None
    _close_gaps_before(conn, site_id, change_date)
    mark_missed(
        conn,
        site_id,
        change_date,
        now_utc=record.day_end_utc + timedelta(minutes=5),
        reason="zone_changeover",
    )
    record = day_record(conn, site_id, change_date)
    assert record is not None
    assert record.state == "missed" and record.missed_reason == "zone_changeover", (
        f"precondition -> the changeover day must be missed(zone_changeover)"
        f" before close_alerts runs, got state={record.state!r}"
        f" missed_reason={record.missed_reason!r}"
    )

    alerts = close_alerts(conn, record.day_end_utc + timedelta(days=7))
    missed = next(a for a in alerts if a.id == "close_day_missed")
    assert change_date.isoformat() not in (missed.detail or ""), (
        "M15 -> at this assertion: correct = the changeover day's date absent"
        f" from close_day_missed's detail, mutant = {missed.detail!r}"
    )


def test_al6_thin_values_labels_in_order() -> None:
    """AL6: D-3 closed, wind_max eligible 0 (too few hours); precip_total no
    row (M16, M17)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_3 = D - timedelta(days=3)
    for quantity in ("temperature_high", "temperature_low", "precip_occurrence"):
        _insert_truth(
            conn,
            site_id=site_id,
            local_date=d_minus_3,
            quantity=quantity,
            eligible=1,
            exclusion_reason=None,
        )
    _insert_truth(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        quantity="wind_max",
        eligible=0,
        exclusion_reason="too few hours",
    )
    # precip_total: deliberately no row.

    alerts = close_alerts(conn, NOW)
    thin = next(a for a in alerts if a.id == "close_day_thin")
    expected = (
        f"{_SITE_NAME} {d_minus_3.isoformat()}: not counted: rain total (no"
        " verdict); wind (too few hours)"
    )
    assert thin.tripped and thin.detail == expected, (
        f"M16/M17 -> at this assertion: correct = tripped, detail="
        f"{expected!r}, mutant = tripped={thin.tripped}, detail="
        f"{thin.detail!r}"
    )


def test_al7_every_quantity_eligible_is_ok() -> None:
    """AL7: a closed day with every quantity eligible; the helper's missed
    D-2 with no truth (M18)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_3 = D - timedelta(days=3)
    _full_truth(conn, site_id=site_id, local_date=d_minus_3)

    alerts = close_alerts(conn, NOW)
    thin = next(a for a in alerts if a.id == "close_day_thin")
    assert not thin.tripped, (
        "M18 -> at this assertion: correct = ok (every quantity eligible and"
        f" the missed day never read), mutant = tripped={thin.tripped},"
        f" detail={thin.detail!r}"
    )


def test_al8_thin_window_clears_after_7_days() -> None:
    """AL8: AL6 at done_at + 7 days, then 1 s later (M19)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_3 = D - timedelta(days=3)
    for quantity in (
        "temperature_high",
        "temperature_low",
        "precip_occurrence",
        "precip_total",
    ):
        _insert_truth(
            conn,
            site_id=site_id,
            local_date=d_minus_3,
            quantity=quantity,
            eligible=1,
            exclusion_reason=None,
        )
    _insert_truth(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        quantity="wind_max",
        eligible=0,
        exclusion_reason="too few hours",
    )
    record = day_record(conn, site_id, d_minus_3)
    assert record is not None and record.done_at is not None
    from wxverify.core.timeutil import parse_utc

    done_at = parse_utc(record.done_at)

    at_boundary = close_alerts(conn, done_at + timedelta(days=7))
    thin = next(a for a in at_boundary if a.id == "close_day_thin")
    assert thin.tripped, (
        "M19 -> at this assertion: correct = tripped at done_at + 7 days,"
        f" mutant = tripped={thin.tripped}"
    )

    past_window = close_alerts(conn, done_at + timedelta(days=7, seconds=1))
    thin = next(a for a in past_window if a.id == "close_day_thin")
    assert not thin.tripped, (
        "M19 -> at this assertion: correct = ok 1 s past the window (measured"
        f" from done_at, not the day's end), mutant = tripped={thin.tripped}"
    )


_AL9_DATES = (D - timedelta(days=6), D - timedelta(days=5), D - timedelta(days=4))


def test_al9_station_nothing_usable_three_days() -> None:
    """AL9: station B, three closed days answered with no reading, no
    station rows (M20)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")
    for i, d in enumerate(_AL9_DATES):
        _set_answered(conn, station_b, d)
        _close_day(conn, site_id, d, first=(i == 0))

    alerts = close_alerts(conn, NOW)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    expected = (
        f"station ISTATION02: nothing usable for temperature, rain and wind"
        f" since {_AL9_DATES[0].isoformat()}"
    )
    assert (
        nothing_usable.tripped
        and nothing_usable.count == 1
        and nothing_usable.detail == expected
    ), (
        f"M20 -> at this assertion: correct = tripped, count=1, detail="
        f"{expected!r}, mutant = tripped={nothing_usable.tripped},"
        f" count={nothing_usable.count}, detail={nothing_usable.detail!r}"
        " (answered ignored: a 204 day skipped)"
    )


def test_al10_only_two_counted_days_is_ok() -> None:
    """AL10: AL9 with two such days (M21)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")
    for i, d in enumerate(_AL9_DATES[:2]):
        _set_answered(conn, station_b, d)
        _close_day(conn, site_id, d, first=(i == 0))

    alerts = close_alerts(conn, NOW)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    assert not nothing_usable.tripped, (
        "M21 -> at this assertion: correct = ok (only 2 of the 3 needed"
        f" counted days), mutant = tripped={nothing_usable.tripped}"
    )


def test_al11_skipped_day_does_not_break_or_extend_run() -> None:
    """AL11: AL9 with a day between the counted days that is skipped, in
    turn: refused with no answer; no answer and no refused_at; missed
    (M22, M23)."""
    from wxverify.close_monitor import close_alerts

    counted = (D - timedelta(days=4), D - timedelta(days=2), D - timedelta(days=1))
    skip_date = D - timedelta(days=3)

    def _run(skip_setup) -> None:
        conn = asof_conn()
        site_id = day_close_site(conn, close_on=True, with_records=False)
        station_b = _station_id(conn, site_id, "ISTATION02")
        all_dates = sorted({*counted, skip_date})
        for i, d in enumerate(all_dates):
            if d == skip_date:
                skip_setup(conn, station_b, d)
            else:
                _set_answered(conn, station_b, d)
            close_it = d != skip_date or skip_setup is not _missed_skip
            if close_it:
                _close_day(conn, site_id, d, first=(i == 0))
            else:
                # The "missed" sub-case: grow records through this date, then
                # mark it missed instead of closing it.
                now_for_create = datetime(d.year, d.month, d.day, 1, tzinfo=UTC)
                if i == 0:
                    start_site_days(conn, site_id, d, now_utc=now_for_create)
                else:
                    ensure_site_days(conn, site_id, now_utc=now_for_create)
                _close_gaps_before(conn, site_id, d)
                record = day_record(conn, site_id, d)
                assert record is not None
                mark_missed(
                    conn,
                    site_id,
                    d,
                    now_utc=record.day_end_utc + timedelta(minutes=5),
                    reason="never_attempted",
                )

        alerts = close_alerts(conn, NOW)
        nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
        expected = f"since {counted[0].isoformat()}"
        assert nothing_usable.tripped and expected in (nothing_usable.detail or ""), (
            f"M22/M23 -> at this assertion: correct = tripped, {expected!r}"
            f" in detail, mutant = tripped={nothing_usable.tripped}, detail="
            f"{nothing_usable.detail!r}"
        )

    def _refused_skip(conn, station_id: int, d) -> None:
        _set_refused(conn, station_id, d)

    def _absent_skip(conn, station_id: int, d) -> None:  # noqa: ARG001
        pass  # no station_wind_days row at all

    def _missed_skip(conn, station_id: int, d) -> None:  # noqa: ARG001
        pass  # the day itself is marked missed below, not closed

    _run(_refused_skip)
    _run(_absent_skip)
    _run(_missed_skip)

    # M22: a refused day OLDER than every counted day must stay skipped
    # too -- placed between the counted days (above), skipping it never
    # changes the run's oldest date, since that date is unaffected either
    # way. Placed older than all of them, an erroneous inclusion extends
    # the run past the counted days and pushes "since" back to the skip
    # date itself.
    older_skip = D - timedelta(days=5)
    conn_older = asof_conn()
    site_id_older = day_close_site(conn_older, close_on=True, with_records=False)
    station_b_older = _station_id(conn_older, site_id_older, "ISTATION02")
    all_dates_older = sorted({*counted, older_skip})
    for i, d in enumerate(all_dates_older):
        if d == older_skip:
            _set_refused(conn_older, station_b_older, d)
        else:
            _set_answered(conn_older, station_b_older, d)
        _close_day(conn_older, site_id_older, d, first=(i == 0))

    alerts_older = close_alerts(conn_older, NOW)
    nothing_usable_older = next(
        a for a in alerts_older if a.id == "station_nothing_usable"
    )
    expected_older = (
        f"station ISTATION02: nothing usable for temperature, rain and wind"
        f" since {counted[0].isoformat()}"
    )
    assert nothing_usable_older.detail == expected_older, (
        f"M22 -> at this assertion: correct = {expected_older!r} (the"
        " refused day older than every counted day stays skipped, so"
        " 'since' is unaffected by it), mutant ="
        f" {nothing_usable_older.detail!r} (the skip removed, so the"
        " older refused day is counted too, with nothing usable, and"
        f" 'since' shifts back to {older_skip.isoformat()})"
    )


def test_al12_usable_judged_per_variable_and_qc_flag() -> None:
    """AL12: AL9 with one day having B's temperature rows qc_flag 'ok',
    then the same rows 'range' (M24, M25)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")
    for i, d in enumerate(_AL9_DATES):
        _set_answered(conn, station_b, d)
        if d == _AL9_DATES[-1]:
            _set_usable(conn, station_b, d, ("temperature",))
        _close_day(conn, site_id, d, first=(i == 0))

    alerts = close_alerts(conn, NOW)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    expected_first = (
        "station ISTATION02: nothing usable for rain and wind since"
        f" {_AL9_DATES[0].isoformat()}"
    )
    assert nothing_usable.tripped and nothing_usable.detail == expected_first, (
        f"M24 -> at this assertion: correct = {expected_first!r}, mutant ="
        f" {nothing_usable.detail!r} (usable judged per day, not per"
        " variable)"
    )

    conn2 = asof_conn()
    site_id2 = day_close_site(conn2, close_on=True, with_records=False)
    station_b2 = _station_id(conn2, site_id2, "ISTATION02")
    for i, d in enumerate(_AL9_DATES):
        _set_answered(conn2, station_b2, d)
        if d == _AL9_DATES[-1]:
            valid_at = isoformat_utc(datetime(d.year, d.month, d.day, 12, tzinfo=UTC))
            conn2.execute(
                "INSERT INTO station_observations"
                " (station_id, variable, valid_at, value, qc_flag)"
                " VALUES (?, 'temperature', ?, 1.0, 'range')",
                (station_b2, valid_at),
            )
        _close_day(conn2, site_id2, d, first=(i == 0))

    alerts2 = close_alerts(conn2, NOW)
    nothing_usable2 = next(a for a in alerts2 if a.id == "station_nothing_usable")
    expected_second = (
        "station ISTATION02: nothing usable for temperature, rain and wind"
        f" since {_AL9_DATES[0].isoformat()}"
    )
    assert nothing_usable2.tripped and nothing_usable2.detail == expected_second, (
        f"M25 -> at this assertion: correct = {expected_second!r}, mutant ="
        f" {nothing_usable2.detail!r} (qc_flag not filtered: a 'range' row"
        " counted as usable)"
    )


def test_al13_station_list_is_day_specific() -> None:
    """AL13: AL9 plus station C, on the saved list of the newest day only
    (plan: added effective that day, written before its record is made --
    tested here at the end state the day's own `site_day_stations` row
    gives, since the buffer day a growth call always creates one day
    ahead (`_close_day`'s docstring) makes the exact creation-order harder
    to pin without changing what is actually asserted) (M26)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(
        conn,
        close_on=True,
        with_records=False,
        station_ids=("ISTATION01", "ISTATION02", "ISTATION03"),
    )
    station_b = _station_id(conn, site_id, "ISTATION02")
    station_c = _station_id(conn, site_id, "ISTATION03")

    for i, d in enumerate(_AL9_DATES):
        _set_answered(conn, station_b, d)
        _close_day(conn, site_id, d, first=(i == 0))

    # C was on every day's saved list (added effective D-10, before every
    # day here): strip it from every day except the newest, so the newest
    # is the one day its list holds it on.
    newest = _AL9_DATES[-1]
    conn.execute(
        "DELETE FROM site_day_stations WHERE site_id = ? AND station_id = ?"
        " AND local_date != ?",
        (site_id, station_c, newest.isoformat()),
    )

    alerts = close_alerts(conn, NOW)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    expected = (
        f"station ISTATION02: nothing usable for temperature, rain and wind"
        f" since {_AL9_DATES[0].isoformat()}"
    )
    assert (
        nothing_usable.count == 1
        and nothing_usable.detail == expected
        and "ISTATION03" not in (nothing_usable.detail or "")
    ), (
        f"M26 -> at this assertion: correct = count=1, detail={expected!r},"
        f" no 'ISTATION03', mutant = count={nothing_usable.count}, detail="
        f"{nothing_usable.detail!r} (alert 3 reading stations.enabled and"
        " day_presence by key instead of each day's own saved list -- C,"
        " on the newest day's list only and with no presence rows at all,"
        " would otherwise be named for a run it was never expected on)"
    )


def test_al14_clears_on_newer_usable_day_and_on_deletion() -> None:
    """AL14: AL9, then a newer counted day with something for every
    variable; then station B deleted (M27)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")
    for i, d in enumerate(_AL9_DATES):
        _set_answered(conn, station_b, d)
        _close_day(conn, site_id, d, first=(i == 0))

    newer = D - timedelta(days=3)
    _set_answered(conn, station_b, newer)
    _set_usable(conn, station_b, newer, ("temperature", "precip", "wind"))
    _close_day(conn, site_id, newer, first=False)

    alerts = close_alerts(conn, NOW)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    assert not nothing_usable.tripped, (
        "M27 -> at this assertion: correct = ok once the newest counted day"
        f" has something for every variable, mutant = tripped="
        f"{nothing_usable.tripped}"
    )

    conn.execute("DELETE FROM stations WHERE id = ?", (station_b,))
    alerts_after_delete = close_alerts(conn, NOW)
    nothing_usable_after = next(
        a for a in alerts_after_delete if a.id == "station_nothing_usable"
    )
    assert not nothing_usable_after.tripped, (
        "M27 -> at this assertion: correct = ok once the station leaves the"
        f" day lists, mutant = tripped={nothing_usable_after.tripped}"
    )


def test_al15_oldest_within_7_days_of_done_at() -> None:
    """AL15: AL9 with a fourth counted day whose done_at is 8 days old
    (M28)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    older = D - timedelta(days=9)
    site_id = day_close_site(conn, close_on=True, with_records=False)
    station_b = _station_id(conn, site_id, "ISTATION02")

    _set_answered(conn, station_b, older)
    _close_day(conn, site_id, older, first=True)
    for d in _AL9_DATES:
        _set_answered(conn, station_b, d)
        _close_day(conn, site_id, d, first=False)

    alerts = close_alerts(conn, NOW)
    nothing_usable = next(a for a in alerts if a.id == "station_nothing_usable")
    expected = f"since {_AL9_DATES[0].isoformat()}"
    assert nothing_usable.tripped and expected in (nothing_usable.detail or ""), (
        f"M28 -> at this assertion: correct = {expected!r} in detail (the"
        " 9-day-old done day read no further back than 7 days), mutant ="
        f" {nothing_usable.detail!r}"
    )


def test_al16_disabled_sites_and_pre_first_record_days_left_out() -> None:
    """AL16: each of AL1, AL6 and AL9 on a disabled site, and on days
    before the site's first record (M29)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    d_minus_3 = D - timedelta(days=3)
    for quantity in (
        "temperature_high",
        "temperature_low",
        "precip_occurrence",
        "precip_total",
    ):
        _insert_truth(
            conn,
            site_id=site_id,
            local_date=d_minus_3,
            quantity=quantity,
            eligible=1,
            exclusion_reason=None,
        )
    _insert_truth(
        conn,
        site_id=site_id,
        local_date=d_minus_3,
        quantity="wind_max",
        eligible=0,
        exclusion_reason="too few hours",
    )
    conn.execute("UPDATE sites SET enabled = 0 WHERE id = ?", (site_id,))

    alerts = close_alerts(conn, NOW)
    for alert_id in ("close_day_missed", "close_day_thin", "station_nothing_usable"):
        alert = next(a for a in alerts if a.id == alert_id)
        assert not alert.tripped, (
            f"M29 -> at this assertion: correct = {alert_id} ok on a"
            f" disabled site, mutant = tripped={alert.tripped}"
        )

    # A day before the site's first record is never read either (starting
    # history, plan 5.1): a site with no record at all is always ok.
    conn2 = asof_conn()
    day_close_site(conn2, close_on=True, with_records=False)
    alerts2 = close_alerts(conn2, NOW)
    for alert_id in ("close_day_missed", "close_day_thin", "station_nothing_usable"):
        alert = next(a for a in alerts2 if a.id == alert_id)
        assert not alert.tripped, (
            f"M29 -> at this assertion: correct = {alert_id} ok before any"
            f" day has a state, mutant = tripped={alert.tripped}"
        )


def test_al17_no_record_holds_now_is_ok_and_raises_nothing() -> None:
    """AL17: with_records=False, started with start_site_days(D+1), so no
    record holds NOW; read at NOW and at D+1 00:30 (M30, M114)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    d_plus_1 = D + timedelta(days=1)
    start_site_days(conn, site_id, d_plus_1, now_utc=NOW)
    assert current_day(conn, site_id, now_utc=NOW) is None

    alerts_before = close_alerts(conn, NOW)
    for alert in alerts_before:
        assert not alert.tripped, (
            "M30/M114 -> at this assertion: correct = every close_alerts row"
            f" ok with no record holding NOW, mutant = {alert.id} tripped="
            f"{alert.tripped}"
        )

    later = datetime(d_plus_1.year, d_plus_1.month, d_plus_1.day, 0, 30, tzinfo=UTC)
    assert current_day(conn, site_id, now_utc=later) is not None
    alerts_after = close_alerts(conn, later)
    for alert in alerts_after:
        assert not alert.tripped, (
            "M30/M114 -> at this assertion: correct = every close_alerts row"
            f" ok once a record holds now, mutant = {alert.id} tripped="
            f"{alert.tripped}"
        )


def test_al18_stage_text() -> None:
    """AL18: stage_text called with 1 to 8, then 0, 9 and None (M120,
    M121)."""
    from wxverify.close_monitor import stage_text

    expected_named = {
        1: "stage 1 the round settles",
        2: "stage 2 gap check and fill",
        3: "stage 3 hourly values",
        4: "stage 4 verdict and truth",
        5: "stage 5 score the day",
        6: "stage 6 save the closure",
        7: "stage 7 publish",
        8: "stage 8 worksheet cleanup",
    }
    for stage, expected in expected_named.items():
        actual = stage_text(stage)
        assert actual == expected, (
            f"M120 -> at this assertion: correct = {expected!r}, mutant = {actual!r}"
        )

    assert stage_text(0) == "stage 0", (
        f"M120 -> at this assertion: correct = 'stage 0', mutant = {stage_text(0)!r}"
    )
    assert stage_text(9) == "stage 9", (
        f"M120 -> at this assertion: correct = 'stage 9', mutant = {stage_text(9)!r}"
    )
    assert stage_text(None) == "stage unknown", (
        "M121 -> at this assertion: correct = 'stage unknown', mutant ="
        f" {stage_text(None)!r}"
    )


# ---------------------------------------------------------------------------
# §13.4: alert 4 and the failed-attempt alert (AD1-AD13)
# ---------------------------------------------------------------------------


def test_ad1_publication_late_pending_blocked_at_publish() -> None:
    """AD1: at T, D-1 closed, its publication pending, opened at <t>;
    active publication through D-2 (M31)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    d_minus_2 = D - timedelta(days=2)
    t_close = _close_d_minus_1(conn, site_id)
    opened_at = t_close + timedelta(minutes=10)
    seed_publication_state(
        conn, site_id=site_id, cutoff=d_minus_1, state="pending", opened_at=opened_at
    )
    seed_active_publication(
        conn, site_id=site_id, cutoff=d_minus_2, published_at=opened_at
    )

    alerts = close_alerts(conn, _AD_T)
    late = next(a for a in alerts if a.id == "publication_late")
    expected = (
        f"{_SITE_NAME}: {d_minus_1.isoformat()} not published at"
        f" {isoformat_utc(_AD_T)}; active publication through"
        f" {d_minus_2.isoformat()}, 2 days old; blocked at stage 7 publish"
        f" since {isoformat_utc(opened_at)}"
    )
    assert late.tripped and late.detail == expected, (
        f"M31 -> at this assertion: correct = tripped, detail={expected!r},"
        f" mutant = tripped={late.tripped}, detail={late.detail!r}"
    )


def test_ad2_ok_before_t_and_ok_once_published() -> None:
    """AD2: AD1 at T less 1 s; then at T with D-1 published and the active
    publication through D-1 (M32, M33)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    d_minus_2 = D - timedelta(days=2)
    t_close = _close_d_minus_1(conn, site_id)
    opened_at = t_close + timedelta(minutes=10)
    seed_publication_state(
        conn, site_id=site_id, cutoff=d_minus_1, state="pending", opened_at=opened_at
    )
    seed_active_publication(
        conn, site_id=site_id, cutoff=d_minus_2, published_at=opened_at
    )

    before = close_alerts(conn, _AD_T - timedelta(seconds=1))
    late_before = next(a for a in before if a.id == "publication_late")
    assert not late_before.tripped, (
        "M32 -> at this assertion: correct = ok 1 s before T (E is still"
        f" D-2, already published), mutant = tripped={late_before.tripped}"
    )

    # M33 paired positive: at T, D-1 is still unpublished and the active
    # publication reaches only D-2 -- behind the now-expected day D-1, so
    # its cutoff must be read and compared, not merely checked for
    # existence.
    at_t_behind = close_alerts(conn, _AD_T)
    late_at_t_behind = next(a for a in at_t_behind if a.id == "publication_late")
    assert late_at_t_behind.tripped, (
        "M33 -> at this assertion: correct = tripped at T (the active"
        " publication's cutoff, D-2, is read and found behind the"
        " expected day, D-1), mutant = ok regardless of cutoff once any"
        f" active publication exists; tripped={late_at_t_behind.tripped}"
    )

    from wxverify.core.timeutil import isoformat_utc_micro

    published_at = opened_at + timedelta(minutes=1)
    pub_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=d_minus_1,
        published_at=published_at,
    )
    conn.execute(
        "UPDATE close_publications SET state = 'published', publication_id = ?,"
        " finished_at = ? WHERE site_id = ? AND cutoff = ?",
        (
            str(pub_id),
            isoformat_utc_micro(published_at),
            site_id,
            d_minus_1.isoformat(),
        ),
    )
    at_t = close_alerts(conn, _AD_T)
    late_at_t = next(a for a in at_t if a.id == "publication_late")
    assert not late_at_t.tripped, (
        "M33 -> at this assertion: correct = ok at T once D-1 is published"
        " and the active publication reaches D-1 (its last closed day read"
        f" from the active publication), mutant = tripped={late_at_t.tripped}"
    )


def test_ad3_per_site_snapshot_time() -> None:
    """AD3: AD1 on a site whose snapshot time is 08:00, at 07:30 and at
    08:00 (M34)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.settings.keys import set_setting
    from wxverify.verification.record import SNAPSHOT_TIME_KEY

    conn = asof_conn()
    site_id = _ad_start(conn)
    set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_id}", "08:00")
    d_minus_1 = D - timedelta(days=1)
    d_minus_2 = D - timedelta(days=2)
    t_close = _close_d_minus_1(conn, site_id)
    opened_at = t_close + timedelta(minutes=10)
    seed_publication_state(
        conn, site_id=site_id, cutoff=d_minus_1, state="pending", opened_at=opened_at
    )
    seed_active_publication(
        conn, site_id=site_id, cutoff=d_minus_2, published_at=opened_at
    )

    at_0730 = close_alerts(conn, datetime(D.year, D.month, D.day, 7, 30, tzinfo=UTC))
    late_0730 = next(a for a in at_0730 if a.id == "publication_late")
    assert not late_0730.tripped, (
        "M34 -> at this assertion: correct = ok at 07:30 (the site's own T"
        " is 08:00, so E is still D-2, already published), mutant = T fixed"
        f" at 07:00, so E is already D-1 here; tripped={late_0730.tripped}"
    )

    at_0800 = close_alerts(conn, datetime(D.year, D.month, D.day, 8, 0, tzinfo=UTC))
    late_0800 = next(a for a in at_0800 if a.id == "publication_late")
    assert late_0800.tripped, (
        "M34 -> at this assertion: correct = tripped at 08:00 (E is now"
        f" D-1, unpublished), mutant = tripped={late_0800.tripped}"
    )


def test_ad4_before_t_expects_d_minus_2() -> None:
    """AD4: AD1 before T with D-2 not published (M35)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_2 = D - timedelta(days=2)
    seed_publication_state(
        conn, site_id=site_id, cutoff=d_minus_2, state="pending", opened_at=NOW
    )
    seed_active_publication(
        conn, site_id=site_id, cutoff=D - timedelta(days=3), published_at=NOW
    )

    alerts = close_alerts(conn, _AD_T - timedelta(seconds=1))
    late = next(a for a in alerts if a.id == "publication_late")
    assert late.tripped and d_minus_2.isoformat() in (late.detail or ""), (
        "M35 -> at this assertion: correct = tripped, naming"
        f" {d_minus_2.isoformat()!r} (before T, E is D-2 and it is not"
        f" expected to be unexpected), mutant = tripped={late.tripped},"
        f" detail={late.detail!r} (D-2 not expected before T)"
    )


def test_ad5_left_out_cases_covered_by_other_alerts() -> None:
    """AD5: AD1 with D-1 missed; retry_due; its publication failed (M36)."""
    from wxverify.close_monitor import close_alerts

    # D-1 missed: left out of publication_late; alert 1 names it.
    conn_missed = asof_conn()
    site_id = _ad_start(conn_missed)
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn_missed, site_id, d_minus_1)
    assert record is not None
    t_missed = record.day_end_utc + timedelta(minutes=5)
    mark_missed(
        conn_missed, site_id, d_minus_1, now_utc=t_missed, reason="never_attempted"
    )
    alerts_missed = close_alerts(conn_missed, _AD_T)
    late_missed = next(a for a in alerts_missed if a.id == "publication_late")
    missed = next(a for a in alerts_missed if a.id == "close_day_missed")
    assert not late_missed.tripped and missed.tripped, (
        "M36 -> at this assertion: correct = publication_late ok, alert 1"
        f" tripped, mutant = publication_late tripped={late_missed.tripped},"
        f" alert 1 tripped={missed.tripped}"
    )

    # D-1 retry_due: left out of publication_late; the failed-attempt alert
    # names it.
    conn_retry = asof_conn()
    site_id2 = _ad_start(conn_retry)
    _close_d_minus_1_fail(conn_retry, site_id2)
    alerts_retry = close_alerts(conn_retry, _AD_T)
    late_retry = next(a for a in alerts_retry if a.id == "publication_late")
    stage_failed_retry = next(a for a in alerts_retry if a.id == "close_stage_failed")
    assert not late_retry.tripped and stage_failed_retry.tripped, (
        "M36 -> at this assertion: correct = publication_late ok,"
        " close_stage_failed tripped, mutant = publication_late tripped="
        f"{late_retry.tripped}, close_stage_failed tripped="
        f"{stage_failed_retry.tripped}"
    )

    # D-1 closed, its publication failed: left out of publication_late; the
    # failed-attempt alert names it.
    conn_pub_failed = asof_conn()
    site_id3 = _ad_start(conn_pub_failed)
    t_close3 = _close_d_minus_1(conn_pub_failed, site_id3)
    seed_publication_state(
        conn_pub_failed,
        site_id=site_id3,
        cutoff=d_minus_1,
        state="failed",
        opened_at=t_close3,
        finished_at=t_close3 + timedelta(minutes=5),
        last_failure="publish: http 500",
    )
    alerts_pub_failed = close_alerts(conn_pub_failed, _AD_T)
    late_pub_failed = next(a for a in alerts_pub_failed if a.id == "publication_late")
    stage_failed_pub = next(
        a for a in alerts_pub_failed if a.id == "close_stage_failed"
    )
    assert not late_pub_failed.tripped and stage_failed_pub.tripped, (
        "M36 -> at this assertion: correct = publication_late ok,"
        " close_stage_failed tripped, mutant = publication_late tripped="
        f"{late_pub_failed.tripped}, close_stage_failed tripped="
        f"{stage_failed_pub.tripped}"
    )


def _close_d_minus_1_fail(conn, site_id: int) -> datetime:
    """Close D-1 and fail its first attempt, leaving it retry_due."""
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t)
    fail_close(conn, site_id, d_minus_1, now_utc=t, reason="gap fill: network")
    return t


def test_ad6_no_active_publication() -> None:
    """AD6: AD1 with no active publication (M37)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    t_close = _close_d_minus_1(conn, site_id)
    seed_publication_state(
        conn, site_id=site_id, cutoff=d_minus_1, state="pending", opened_at=t_close
    )

    alerts = close_alerts(conn, _AD_T)
    late = next(a for a in alerts if a.id == "publication_late")
    assert late.tripped and "no active publication" in (late.detail or ""), (
        "M37 -> at this assertion: correct = tripped, 'no active"
        " publication' present (no publication raises nothing), mutant ="
        f" tripped={late.tripped}, detail={late.detail!r}"
    )


def test_ad7_day_before_first_record_never_expected() -> None:
    """AD7: a site whose first record is D-1, at T less 1 s with D-2
    expected (M38)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    d_minus_1 = D - timedelta(days=1)
    start_site_days(
        conn,
        site_id,
        d_minus_1,
        now_utc=datetime(
            d_minus_1.year, d_minus_1.month, d_minus_1.day, 12, tzinfo=UTC
        ),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)

    alerts = close_alerts(conn, _AD_T - timedelta(seconds=1))
    late = next(a for a in alerts if a.id == "publication_late")
    assert not late.tripped, (
        "M38 -> at this assertion: correct = ok (D-2 lies before the site's"
        f" first record, D-1, so it is never expected), mutant = tripped="
        f"{late.tripped}"
    )


def test_ad8_attempt_1_failed_at_stage_2() -> None:
    """AD8: D-1 retry_due with last_failure "gap fill: network", attempt
    1's record failing at stage 2 (R3.4 2), retry_at set (M39)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t)
    seed_attempt(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        attempt=1,
        started_at=t,
        last_stage=1,
        outcome="failed",
        failed_stage=2,
        failure_reason="gap fill: network",
        ended_at=t,
    )
    fail_close(conn, site_id, d_minus_1, now_utc=t, reason="gap fill: network")
    retry_row = conn.execute(
        "SELECT retry_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert retry_row is not None and retry_row[0] is not None
    retry_at = str(retry_row[0])

    alerts = close_alerts(conn, _AD_T)
    stage_failed = next(a for a in alerts if a.id == "close_stage_failed")
    expected = (
        f"{d_minus_1.isoformat()}: attempt 1 failed at stage 2 gap check and"
        f" fill: gap fill: network; retry due {retry_at}"
    )
    assert stage_failed.tripped and expected in (stage_failed.detail or ""), (
        f"M39 -> at this assertion: correct = tripped, {expected!r} in"
        f" detail, mutant = tripped={stage_failed.tripped}, detail="
        f"{stage_failed.detail!r} (stage taken from the stage log's newest"
        " row instead of R3.4)"
    )


def test_ad9_ok_while_retry_runs() -> None:
    """AD9: AD8, then begin_close for the retry (M40)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t)
    seed_attempt(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        attempt=1,
        started_at=t,
        last_stage=1,
        outcome="failed",
        failed_stage=2,
        failure_reason="gap fill: network",
        ended_at=t,
    )
    fail_close(conn, site_id, d_minus_1, now_utc=t, reason="gap fill: network")
    retry_row = conn.execute(
        "SELECT retry_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert retry_row is not None and retry_row[0] is not None
    retry_at = parse_utc(str(retry_row[0]))
    begin_close(conn, site_id, d_minus_1, now_utc=retry_at)

    alerts = close_alerts(conn, _AD_T)
    stage_failed = next(a for a in alerts if a.id == "close_stage_failed")
    assert not stage_failed.tripped, (
        "M40 -> at this assertion: correct = ok once the retry starts"
        " (closing), mutant = (a) kept while the retry runs; tripped="
        f"{stage_failed.tripped}"
    )


def test_ad10_publication_failed_then_recovers() -> None:
    """AD10: D-2's publication failed with a reason, active publication
    through D-3; then D-1 closed, published, active publication through
    D-1 (M41)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_3 = D - timedelta(days=3)
    d_minus_2 = D - timedelta(days=2)
    d_minus_1 = D - timedelta(days=1)
    record_d2 = day_record(conn, site_id, d_minus_2)
    assert record_d2 is not None
    opened_at = record_d2.day_end_utc + timedelta(minutes=10)
    finished_at = opened_at + timedelta(minutes=5)
    seed_publication_state(
        conn,
        site_id=site_id,
        cutoff=d_minus_2,
        state="failed",
        opened_at=opened_at,
        finished_at=finished_at,
        last_failure="build: out of memory",
    )
    seed_active_publication(
        conn, site_id=site_id, cutoff=d_minus_3, published_at=opened_at
    )

    alerts_failed = close_alerts(conn, _AD_T)
    stage_failed = next(a for a in alerts_failed if a.id == "close_stage_failed")
    expected = (
        f"{d_minus_2.isoformat()}: publication failed at"
        f" {isoformat_utc(finished_at)}: build: out of memory; the previous"
        " publication stays in use"
    )
    assert stage_failed.tripped and expected in (stage_failed.detail or ""), (
        f"M41 -> at this assertion: correct = tripped, {expected!r} in"
        f" detail, mutant = tripped={stage_failed.tripped}, detail="
        f"{stage_failed.detail!r}"
    )

    # M41 (b): advance only the ACTIVE publication past D-2's cutoff,
    # leaving the latest publication record (``publication_state``'s own
    # ``MAX(cutoff)`` row) still D-2's failed one. The stale record never
    # changes state here, so the clearing can only come from reading
    # ``active.cutoff > publication.cutoff`` -- a close-and-republish for
    # D-1 (below) would instead make D-1 the latest publication record,
    # which is never "failed"/"retry_due" regardless of the gate, and so
    # cannot discriminate this mutant on its own.
    from wxverify.worker.close_chain import publication_state

    conn_b = asof_conn()
    site_id_b = _ad_start(conn_b)
    record_d2_b = day_record(conn_b, site_id_b, d_minus_2)
    assert record_d2_b is not None
    opened_at_b = record_d2_b.day_end_utc + timedelta(minutes=10)
    finished_at_b = opened_at_b + timedelta(minutes=5)
    seed_publication_state(
        conn_b,
        site_id=site_id_b,
        cutoff=d_minus_2,
        state="failed",
        opened_at=opened_at_b,
        finished_at=finished_at_b,
        last_failure="build: out of memory",
    )
    seed_active_publication(
        conn_b, site_id=site_id_b, cutoff=d_minus_3, published_at=opened_at_b
    )
    seed_active_publication(
        conn_b,
        site_id=site_id_b,
        cutoff=d_minus_1,
        published_at=finished_at_b + timedelta(minutes=1),
    )
    publication_still_failed = publication_state(conn_b, site_id=site_id_b)
    assert (
        publication_still_failed is not None
        and publication_still_failed.cutoff == d_minus_2
        and publication_still_failed.state == "failed"
    ), (
        "precondition -> the latest publication record must stay D-2's"
        " failed one while only the active publication advances, got"
        f" {publication_still_failed!r}"
    )
    alerts_advanced = close_alerts(conn_b, _AD_T)
    stage_failed_advanced = next(
        a for a in alerts_advanced if a.id == "close_stage_failed"
    )
    assert not stage_failed_advanced.tripped, (
        "M41 -> at this assertion: correct = ok once the active"
        " publication reaches a later day even though the stale failed"
        " record is unchanged (active.cutoff > publication.cutoff read),"
        " mutant = (b) never clears; tripped="
        f"{stage_failed_advanced.tripped}"
    )

    t_close1 = _close_d_minus_1(conn, site_id)
    published_at_1 = t_close1 + timedelta(minutes=5)
    pub_id_1 = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=d_minus_1,
        published_at=published_at_1,
    )
    seed_publication_state(
        conn,
        site_id=site_id,
        cutoff=d_minus_1,
        state="published",
        opened_at=t_close1,
        finished_at=published_at_1,
        publication_id=str(pub_id_1),
    )
    alerts_after = close_alerts(conn, _AD_T)
    stage_failed_after = next(a for a in alerts_after if a.id == "close_stage_failed")
    assert not stage_failed_after.tripped, (
        "M41 -> at this assertion: correct = ok once the active publication"
        " reaches a later day, mutant = (b) never clears; tripped="
        f"{stage_failed_after.tripped}"
    )


def test_ad11_stalled_close_at_6_hours() -> None:
    """AD11: D-1 closing, its attempt record's last_stage 2 and
    last_progress_at exactly 6 h before now, then 6 h and 1 s (M42)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t)
    last_progress_at = t
    seed_attempt(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        attempt=1,
        started_at=t,
        last_stage=2,
        last_progress_at=last_progress_at,
        outcome="running",
    )

    at_6h = close_alerts(conn, last_progress_at + timedelta(hours=6))
    stage_failed_6h = next(a for a in at_6h if a.id == "close_stage_failed")
    assert not stage_failed_6h.tripped, (
        "M42 -> at this assertion: correct = ok at exactly 6 h (not yet"
        f" 'more than'), mutant = >= for > at 6 hours; tripped="
        f"{stage_failed_6h.tripped}"
    )

    at_6h1s = close_alerts(conn, last_progress_at + timedelta(hours=6, seconds=1))
    stage_failed_6h1s = next(a for a in at_6h1s if a.id == "close_stage_failed")
    expected = (
        f"{d_minus_1.isoformat()}: stage 3 hourly values has made no"
        f" progress since {isoformat_utc(last_progress_at)}"
    )
    assert stage_failed_6h1s.tripped and expected in (stage_failed_6h1s.detail or ""), (
        f"M42 -> at this assertion: correct = tripped, {expected!r} in"
        " detail, mutant = >= for > at 6 hours; tripped="
        f"{stage_failed_6h1s.tripped}, detail={stage_failed_6h1s.detail!r}"
    )


def test_ad12_one_line_per_site_listing_both() -> None:
    """AD12: AD8 and AD10's first case at one site (M43)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_2 = D - timedelta(days=2)
    d_minus_1 = D - timedelta(days=1)

    record_d2 = day_record(conn, site_id, d_minus_2)
    assert record_d2 is not None
    opened_at = record_d2.day_end_utc + timedelta(minutes=10)
    finished_at = opened_at + timedelta(minutes=5)
    seed_publication_state(
        conn,
        site_id=site_id,
        cutoff=d_minus_2,
        state="failed",
        opened_at=opened_at,
        finished_at=finished_at,
        last_failure="build: out of memory",
    )
    seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=d_minus_2 - timedelta(days=1),
        published_at=opened_at,
    )

    record = day_record(conn, site_id, d_minus_1)
    assert record is not None
    t = record.day_end_utc + timedelta(minutes=5)
    begin_close(conn, site_id, d_minus_1, now_utc=t)
    seed_attempt(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        attempt=1,
        started_at=t,
        last_stage=1,
        outcome="failed",
        failed_stage=2,
        failure_reason="gap fill: network",
        ended_at=t,
    )
    fail_close(conn, site_id, d_minus_1, now_utc=t, reason="gap fill: network")

    alerts = close_alerts(conn, _AD_T)
    stage_failed = next(a for a in alerts if a.id == "close_stage_failed")
    assert (
        stage_failed.tripped
        and stage_failed.count == 1
        and d_minus_1.isoformat() in (stage_failed.detail or "")
        and d_minus_2.isoformat() in (stage_failed.detail or "")
    ), (
        "M43 -> at this assertion: correct = count=1, one line naming both"
        f" days, mutant = one row per cause; count={stage_failed.count},"
        f" detail={stage_failed.detail!r}"
    )


def test_ad13_publication_retry_due() -> None:
    """AD13: AD1 with D-1's publication record retry_due, last_failure
    "publish: http 503" and retry_at set (M115)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = _ad_start(conn)
    d_minus_1 = D - timedelta(days=1)
    t_close = _close_d_minus_1(conn, site_id)
    retry_at = t_close + timedelta(hours=1)
    seed_publication_state(
        conn,
        site_id=site_id,
        cutoff=d_minus_1,
        state="retry_due",
        opened_at=t_close,
        retry_at=retry_at,
        last_failure="publish: http 503",
    )

    alerts = close_alerts(conn, _AD_T)
    late = next(a for a in alerts if a.id == "publication_late")
    assert not late.tripped, (
        "M115 -> at this assertion: correct = publication_late ok (a"
        " retry-due publication is left out), mutant = 6.2(b) reading"
        f" failed only, so this path would be missed; tripped="
        f"{late.tripped}"
    )
    stage_failed = next(a for a in alerts if a.id == "close_stage_failed")
    expected = (
        f"{d_minus_1.isoformat()}: publication attempt 1 failed: publish:"
        f" http 503; retry due {isoformat_utc(retry_at)}"
    )
    assert stage_failed.tripped and expected in (stage_failed.detail or ""), (
        f"M115 -> at this assertion: correct = tripped, {expected!r} in"
        f" detail, mutant = 6.2(b) reads 'failed' only, so a retry_due"
        f" publication would not be named here; tripped="
        f"{stage_failed.tripped}, detail={stage_failed.detail!r}"
    )


# ---------------------------------------------------------------------------
# §13.7: the daytime-download warning and alert 7 (DW1-DW8, PD1-PD6)
# ---------------------------------------------------------------------------


def _store_five_minute(conn, station_id: int, at: datetime) -> None:
    from wxverify.db.station_readings import store_readings
    from wxverify.obs.hourly_values import StationReading

    store_readings(
        conn,
        station_id=station_id,
        source="five_minute",
        readings=[
            StationReading(
                obs_at=at,
                source="five_minute",
                local_date=None,
                temp_c=10.0,
                precip_total_mm=None,
                speed_kmh=None,
            )
        ],
        fetched_at=at,
        now_utc=at,
    )


def test_dw1_daytime_warning_trips_after_2_refreshes() -> None:
    """DW1: close on; obs_interval_minutes 180; the site's last 5-minute
    store 6 h before now, then 6 h and 1 s (M60, M61)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    store_time = NOW + timedelta(hours=1)
    _store_five_minute(conn, station_a, store_time)

    at_6h = close_alerts(conn, store_time + timedelta(hours=6))
    warning_6h = next(a for a in at_6h if a.id == "daytime_download_missed")
    assert not warning_6h.tripped, (
        "M60/M61 -> at this assertion: correct = ok at exactly 2 refreshes"
        f" (not yet 'more than'), mutant = tripped={warning_6h.tripped}"
    )

    at_6h1s = close_alerts(conn, store_time + timedelta(hours=6, seconds=1))
    warning_6h1s = next(a for a in at_6h1s if a.id == "daytime_download_missed")
    expected = (
        f"{_SITE_NAME}: no 5-minute readings stored since"
        f" {isoformat_utc(store_time)} (2 refreshes of 180 min missed)"
    )
    assert warning_6h1s.tripped and warning_6h1s.detail == expected, (
        f"M60/M61 -> at this assertion: correct = tripped, detail="
        f"{expected!r}, mutant = tripped={warning_6h1s.tripped}, detail="
        f"{warning_6h1s.detail!r}"
    )


def test_dw2_interval_30_trips_at_60_minutes() -> None:
    """DW2: DW1's second case with obs_interval_minutes 30 (M62)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.settings.keys import set_setting

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    set_setting(conn, "obs_interval_minutes", "30")
    station_a = _station_id(conn, site_id, "ISTATION01")
    store_time = NOW + timedelta(hours=1)
    _store_five_minute(conn, station_a, store_time)

    alerts = close_alerts(conn, store_time + timedelta(minutes=60, seconds=1))
    warning = next(a for a in alerts if a.id == "daytime_download_missed")
    expected = (
        f"{_SITE_NAME}: no 5-minute readings stored since"
        f" {isoformat_utc(store_time)} (2 refreshes of 30 min missed)"
    )
    assert warning.tripped and warning.detail == expected, (
        f"M62 -> at this assertion: correct = tripped, detail={expected!r}"
        " (2 refreshes of 30 min = 60 min), mutant = one interval, not"
        f" two; tripped={warning.tripped}, detail={warning.detail!r}"
    )


def test_dw3_refused_five_minute_kind_left_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DW3: DW1's second case with five_minute among refused_kinds (M63)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.worker.access_hold import record_access_refused

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    store_time = NOW + timedelta(hours=1)
    _store_five_minute(conn, station_a, store_time)
    at = store_time + timedelta(hours=6, seconds=1)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: at)
    record_access_refused(conn, "five_minute", 401)

    alerts = close_alerts(conn, at)
    warning = next(a for a in alerts if a.id == "daytime_download_missed")
    assert not warning.tripped, (
        "M63 -> at this assertion: correct = ok (five_minute is held),"
        f" mutant = a refused five_minute kind not left out; tripped="
        f"{warning.tripped}"
    )


def test_dw4_kept_apart_from_station_silence() -> None:
    """DW4: DW1's second case where every station's latest 5-minute try
    since then ended no_readings; then only A's did and B's failed
    network (M64)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    store_time = NOW + timedelta(hours=1)
    _store_five_minute(conn, station_a, store_time)
    at = store_time + timedelta(hours=6, seconds=1)
    try_time = store_time + timedelta(hours=1)

    for station_id in (station_a, station_b):
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_id,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=try_time,
            outcome="no_readings",
        )

    alerts_both = close_alerts(conn, at)
    warning_both = next(a for a in alerts_both if a.id == "daytime_download_missed")
    assert not warning_both.tripped, (
        "M64 -> at this assertion: correct = ok (every station on the"
        " list answered with no data), mutant = silent stations not left"
        f" out; tripped={warning_both.tripped}"
    )

    seed_try(
        conn,
        site_id=site_id,
        station_id=station_b,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=try_time + timedelta(minutes=5),
        outcome="failed",
        reason="network",
    )
    alerts_mixed = close_alerts(conn, at)
    warning_mixed = next(a for a in alerts_mixed if a.id == "daytime_download_missed")
    assert warning_mixed.tripped, (
        "M64 -> at this assertion: correct = tripped once only A answered"
        " with no data and B's latest try failed, mutant = left out when"
        f" only some stations answered with no data; tripped="
        f"{warning_mixed.tripped}"
    )


def test_dw5_start_of_count_latest_of_worker_and_close_on() -> None:
    """DW5: DW1's second case with the worker started 1 h before now;
    with the close turned on 1 h before now; with no store at all and
    both 7 h old (M65)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

    eval_now = NOW + timedelta(hours=10)

    conn_a = asof_conn()
    day_close_site(conn_a, close_on=True)
    conn_a.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_started_at', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (isoformat_utc(eval_now - timedelta(hours=1)),),
    )
    alerts_a = close_alerts(conn_a, eval_now)
    warning_a = next(a for a in alerts_a if a.id == "daytime_download_missed")
    assert not warning_a.tripped, (
        "M65 -> at this assertion: correct = ok (the worker's recent"
        f" start floors the start of count), mutant = tripped="
        f"{warning_a.tripped}"
    )

    conn_b = asof_conn()
    day_close_site(conn_b, close_on=True)
    set_runtime_state(
        conn_b, DAILY_CLOSE_ON_KEY, isoformat_utc(eval_now - timedelta(hours=1))
    )
    alerts_b = close_alerts(conn_b, eval_now)
    warning_b = next(a for a in alerts_b if a.id == "daytime_download_missed")
    assert not warning_b.tripped, (
        "M65 -> at this assertion: correct = ok (the close's recent"
        f" turn-on floors the start of count), mutant = tripped="
        f"{warning_b.tripped}"
    )

    conn_c = asof_conn()
    day_close_site(conn_c, close_on=True)
    conn_c.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_started_at', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (isoformat_utc(eval_now - timedelta(hours=7)),),
    )
    set_runtime_state(
        conn_c, DAILY_CLOSE_ON_KEY, isoformat_utc(eval_now - timedelta(hours=7))
    )
    alerts_c = close_alerts(conn_c, eval_now)
    warning_c = next(a for a in alerts_c if a.id == "daytime_download_missed")
    assert warning_c.tripped, (
        "M65 -> at this assertion: correct = tripped once both floors are"
        f" 7 h old, mutant = the start of count ignores the worker's"
        f" start or the cutover; tripped={warning_c.tripped}"
    )


def test_dw6_clears_on_new_store() -> None:
    """DW6: DW1's second case, read TWICE -- once before a fresher store
    exists (establishing the tripped baseline, and, under a per-site
    cache, poisoning it), then again after it exists (M66)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    store_time = NOW + timedelta(hours=1)
    _store_five_minute(conn, station_a, store_time)
    at = store_time + timedelta(hours=6, seconds=1)

    # First read, before the newer store exists: tripped, and (under a
    # cache keyed by site id) this is the read that would stick.
    alerts_first = close_alerts(conn, at)
    warning_first = next(a for a in alerts_first if a.id == "daytime_download_missed")
    assert warning_first.tripped, (
        "precondition -> the warning must be tripped before the newer"
        f" store arrives, got tripped={warning_first.tripped}"
    )

    new_store = store_time + timedelta(hours=1)
    _store_five_minute(conn, station_a, new_store)

    alerts = close_alerts(conn, at)
    warning = next(a for a in alerts if a.id == "daytime_download_missed")
    assert not warning.tripped, (
        "M66 -> at this assertion: correct = ok (the last store is read"
        " fresh on this SECOND call too, not cached from the first),"
        f" mutant = tripped={warning.tripped} (the first call's store"
        " cached per site, so the newer store is never seen)"
    )


def test_dw7_close_alerts_writes_nothing() -> None:
    """DW7: DW1's second case, with a real 'ok' station_observations row at
    the warning's own ``start`` instant so a consensus write would have
    something to materialize, not a no-op on an empty table; every
    station row, hourly row and stored score of the site, read with
    SELECT * before and after close_alerts (M67, K32)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    store_time = NOW + timedelta(hours=1)
    _store_five_minute(conn, station_a, store_time)
    at = store_time + timedelta(hours=6, seconds=1)
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag)"
        " VALUES (?, 'temperature', ?, 10.0, 'ok')",
        (station_a, isoformat_utc(store_time)),
    )

    def _snapshot():
        return (
            conn.execute("SELECT * FROM station_observations ORDER BY id").fetchall(),
            conn.execute("SELECT * FROM observations ORDER BY id").fetchall(),
            conn.execute("SELECT * FROM forecast_pairs ORDER BY id").fetchall(),
        )

    before = _snapshot()
    close_alerts(conn, at)
    after = _snapshot()
    assert before == after, (
        "M67 -> at this assertion: correct = station_observations,"
        " observations and forecast_pairs unchanged, mutant = the warning"
        " writes a stored row (a materialize_consensus call added)"
    )


def test_dw8_start_of_count_includes_first_record_start() -> None:
    """DW8: the AL17 site, close turned on and the worker started at NOW,
    obs_interval_minutes 180, no 5-minute store; now D+1 06:00, then D+1
    06:00 and 1 s (M113)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    d_plus_1 = D + timedelta(days=1)
    start_site_days(conn, site_id, d_plus_1, now_utc=NOW)
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_started_at', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (isoformat_utc(NOW),),
    )

    at_6h = datetime(d_plus_1.year, d_plus_1.month, d_plus_1.day, 6, 0, tzinfo=UTC)
    alerts_6h = close_alerts(conn, at_6h)
    warning_6h = next(a for a in alerts_6h if a.id == "daytime_download_missed")
    assert not warning_6h.tripped, (
        "M113 -> at this assertion: correct = ok at exactly 2 refreshes"
        " from the first record's start, mutant = the start of count"
        f" leaves out the first record's start; tripped={warning_6h.tripped}"
    )

    at_6h1s = at_6h + timedelta(seconds=1)
    alerts_6h1s = close_alerts(conn, at_6h1s)
    warning_6h1s = next(a for a in alerts_6h1s if a.id == "daytime_download_missed")
    assert warning_6h1s.tripped, (
        "M113 -> at this assertion: correct = tripped once past the"
        " first record's start plus 2 refreshes, mutant = the start of"
        " count leaves out the first record's start; tripped="
        f"{warning_6h1s.tripped}"
    )


def test_pd1_provider_down_since_the_slots_earliest_try() -> None:
    """PD1: close on; the site's latest judged slot: one try, provider
    http 503, with no network/backoff try in the same slot to make it
    down any other way (M68)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_b = _station_id(conn, site_id, "ISTATION02")
    slot_time = NOW + timedelta(hours=1)
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_b,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=slot_time,
        outcome="failed",
        reason="provider",
        detail="http 503",
    )

    alerts = close_alerts(conn, slot_time + timedelta(minutes=5))
    down = next(a for a in alerts if a.id == "provider_down")
    expected = (
        f"{_SITE_NAME}: weather.com unreachable or failing since"
        f" {isoformat_utc(slot_time)}; down slots: 1"
    )
    assert down.tripped and down.detail == expected, (
        f"M68 -> at this assertion: correct = tripped, detail={expected!r}"
        f", mutant = a provider http 503 try not an outage (and with no"
        " network/backoff try in the same slot to make it down anyway);"
        f" tripped={down.tripped}, detail={down.detail!r}"
    )


def test_pd2_answered_or_other_only_slots_are_ok() -> None:
    """PD2: PD1 with, in turn: one try stored; one no_readings; both
    call_limit; one held and one refused; one provider http 404
    (M69, M70)."""
    from wxverify.close_monitor import close_alerts

    slot_time = NOW + timedelta(hours=1)

    def _run(tries: list[tuple[str, str | None, str | None]]) -> None:
        conn = asof_conn()
        site_id = day_close_site(conn, close_on=True)
        station_a = _station_id(conn, site_id, "ISTATION01")
        station_b = _station_id(conn, site_id, "ISTATION02")
        stations = (station_a, station_b)
        for i, (outcome, reason, detail) in enumerate(tries):
            seed_try(
                conn,
                site_id=site_id,
                station_id=stations[i],
                local_date=D,
                kind="five_minute",
                slot="daytime",
                close_attempt=None,
                tried_at=slot_time,
                outcome=outcome,
                reason=reason,
                detail=detail,
            )
        alerts = close_alerts(conn, slot_time + timedelta(minutes=5))
        down = next(a for a in alerts if a.id == "provider_down")
        assert not down.tripped, (
            "M69/M70 -> at this assertion: correct = ok, mutant ="
            f" no_readings not answered, or an other-only slot judged"
            f" down; tripped={down.tripped} (for tries {tries!r})"
        )

    _run([("stored", None, None)])
    _run([("no_readings", None, None)])
    _run([("failed", "call_limit", None), ("failed", "call_limit", None)])
    _run([("held", "refused_access", None), ("refused", "refused_access", None)])
    _run([("failed", "provider", "http 404")])

    # M69 paired positive: a no_readings slot must count as ANSWERED, not
    # merely "not down" in isolation -- otherwise it is never judged at
    # all, and a prior down run reads as still ongoing instead of
    # recovered at the no_readings slot.
    conn_recovered = asof_conn()
    site_id_recovered = day_close_site(conn_recovered, close_on=True)
    station_a_recovered = _station_id(conn_recovered, site_id_recovered, "ISTATION01")
    station_b_recovered = _station_id(conn_recovered, site_id_recovered, "ISTATION02")
    down_time = slot_time
    recovered_time = slot_time + timedelta(hours=4)
    seed_try(
        conn_recovered,
        site_id=site_id_recovered,
        station_id=station_a_recovered,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=down_time,
        outcome="failed",
        reason="network",
    )
    seed_try(
        conn_recovered,
        site_id=site_id_recovered,
        station_id=station_b_recovered,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=recovered_time,
        outcome="no_readings",
    )

    alerts_recovered = close_alerts(
        conn_recovered, recovered_time + timedelta(minutes=5)
    )
    down_recovered = next(a for a in alerts_recovered if a.id == "provider_down")
    expected_recovered = f"recovered at {isoformat_utc(recovered_time)}"
    assert not down_recovered.tripped and down_recovered.detail == expected_recovered, (
        f"M69 -> at this assertion: correct = ok, detail="
        f"{expected_recovered!r} (the no_readings slot answers and ends"
        " the prior down run), mutant = no_readings not an answered try,"
        " so this slot is never judged and the prior down run still"
        f" reads as ongoing; tripped={down_recovered.tripped}, detail="
        f"{down_recovered.detail!r}"
    )


def test_pd3_down_run_survives_other_only_slot() -> None:
    """PD3: three down slots, a slot of only call_limit tries, then a
    down slot (M71)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.settings.keys import set_setting

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    set_setting(conn, "obs_interval_minutes", "60")
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")

    day_start = datetime(D.year, D.month, D.day, tzinfo=UTC)
    t1 = day_start + timedelta(hours=1, minutes=30)
    t2 = day_start + timedelta(hours=2, minutes=30)
    t3 = day_start + timedelta(hours=3, minutes=30)
    t4 = day_start + timedelta(hours=4, minutes=30)
    t5 = day_start + timedelta(hours=5, minutes=30)

    for t in (t1, t2, t3, t5):
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_a,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=t,
            outcome="failed",
            reason="network",
        )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_b,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=t4,
        outcome="failed",
        reason="call_limit",
    )

    alerts = close_alerts(conn, t5 + timedelta(minutes=5))
    down = next(a for a in alerts if a.id == "provider_down")
    expected = (
        f"{_SITE_NAME}: weather.com unreachable or failing since"
        f" {isoformat_utc(t1)}; down slots: 4"
    )
    assert down.tripped and down.detail == expected, (
        f"M71 -> at this assertion: correct = tripped, detail={expected!r}"
        " (the call_limit-only slot does not end the run), mutant = a"
        f" slot of only 'other' tries ends the run; tripped={down.tripped}"
        f", detail={down.detail!r}"
    )


def test_pd4_recovered_line_for_24_hours() -> None:
    """PD4: PD3 followed by a slot with a stored try at <t>; now 24 h
    after <t>, then 24 h and 1 s (M72, M73)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.settings.keys import set_setting

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    set_setting(conn, "obs_interval_minutes", "60")
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")

    day_start = datetime(D.year, D.month, D.day, tzinfo=UTC)
    t1 = day_start + timedelta(hours=1, minutes=30)
    t2 = day_start + timedelta(hours=2, minutes=30)
    t3 = day_start + timedelta(hours=3, minutes=30)
    t4 = day_start + timedelta(hours=4, minutes=30)
    t5 = day_start + timedelta(hours=5, minutes=30)
    t6 = day_start + timedelta(hours=6, minutes=30)

    for t in (t1, t2, t3, t5):
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_a,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=t,
            outcome="failed",
            reason="network",
        )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_b,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=t4,
        outcome="failed",
        reason="call_limit",
    )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=t6,
        outcome="stored",
    )

    at_24h = close_alerts(conn, t6 + timedelta(hours=24))
    down_24h = next(a for a in at_24h if a.id == "provider_down")
    expected = f"recovered at {isoformat_utc(t6)}"
    assert not down_24h.tripped and down_24h.detail == expected, (
        f"M72 -> at this assertion: correct = ok, detail={expected!r},"
        f" mutant = no recovered line; tripped={down_24h.tripped}, detail="
        f"{down_24h.detail!r}"
    )

    at_24h1s = close_alerts(conn, t6 + timedelta(hours=24, seconds=1))
    down_24h1s = next(a for a in at_24h1s if a.id == "provider_down")
    assert not down_24h1s.tripped and down_24h1s.detail is None, (
        "M73 -> at this assertion: correct = ok, no detail, mutant = the"
        f" recovered line kept past 24 h; tripped={down_24h1s.tripped},"
        f" detail={down_24h1s.detail!r}"
    )


def test_pd5_grace_applies_to_alert_7() -> None:
    """PD5: PD1 with the worker started 5 minutes before now (M74)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    slot_time = NOW + timedelta(hours=1)
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=slot_time,
        outcome="failed",
        reason="network",
    )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_b,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=slot_time,
        outcome="failed",
        reason="provider",
        detail="http 503",
    )
    now = slot_time + timedelta(minutes=5)
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_started_at', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (isoformat_utc(now - timedelta(minutes=5)),),
    )

    alerts = close_alerts(conn, now)
    down = next(a for a in alerts if a.id == "provider_down")
    assert down.tripped, (
        "M74 paired positive -> at this assertion: correct = tripped (no"
        " grace applied by close_alerts itself), mutant = tripped="
        f"{down.tripped}"
    )

    from wxverify.monitor import build_verdict

    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=True,
        db_enabled=True,
        now=now,
        export_sweeper_dead=None,
    )
    provider_down_cond = next(
        c
        for c in verdict["conditions"]  # type: ignore[index]
        if c["id"] == "provider_down"
    )
    assert (
        provider_down_cond["ok"] is True and provider_down_cond.get("detail") is None
    ), (
        "M74 -> at this assertion: correct = ok=True, no detail (the"
        " 10-minute start-up grace), mutant = alert 7's builder skips the"
        f" grace; cond={provider_down_cond!r}"
    )


def test_pd6_daytime_slots_grouped_by_refresh_interval() -> None:
    """PD6: close on; a UTC site, obs_interval_minutes 180; on D, A's and
    B's daytime tries stored at 09:05; at each of 12:05, 15:05 and
    18:05, A's daytime try network and B's provider http 503; now D
    18:30 (M116)."""
    from wxverify.close_monitor import close_alerts

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    day_start = datetime(D.year, D.month, D.day, tzinfo=UTC)

    stored_time = day_start + timedelta(hours=9, minutes=5)
    for station_id in (station_a, station_b):
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_id,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=stored_time,
            outcome="stored",
        )

    down_times = [
        day_start + timedelta(hours=12, minutes=5),
        day_start + timedelta(hours=15, minutes=5),
        day_start + timedelta(hours=18, minutes=5),
    ]
    for t in down_times:
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_a,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=t,
            outcome="failed",
            reason="network",
        )
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_b,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=t,
            outcome="failed",
            reason="provider",
            detail="http 503",
        )

    now = day_start + timedelta(hours=18, minutes=30)
    alerts = close_alerts(conn, now)
    down = next(a for a in alerts if a.id == "provider_down")
    expected = (
        f"{_SITE_NAME}: weather.com unreachable or failing since"
        f" {isoformat_utc(down_times[0])}; down slots: 3"
    )
    assert down.tripped and down.detail == expected, (
        f"M116 -> at this assertion: correct = tripped, detail={expected!r}"
        " (the 09:05 stored try's slot kept apart from the later down"
        " slots), mutant = all of a day's daytime tries judged as one"
        f" slot; tripped={down.tripped}, detail={down.detail!r}"
    )


def test_pd7_recovered_line_survives_a_later_answered_slot() -> None:
    """PD7: PD3's down run, then two later daytime slots each with one
    stored try, at <t> and <t>+3 h; now <t>+4 h, then <t>+24 h 1 s
    (M125)."""
    from wxverify.close_monitor import close_alerts
    from wxverify.settings.keys import set_setting

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True)
    set_setting(conn, "obs_interval_minutes", "60")
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")

    day_start = datetime(D.year, D.month, D.day, tzinfo=UTC)
    t1 = day_start + timedelta(hours=1, minutes=30)
    t2 = day_start + timedelta(hours=2, minutes=30)
    t3 = day_start + timedelta(hours=3, minutes=30)
    t4 = day_start + timedelta(hours=4, minutes=30)
    t5 = day_start + timedelta(hours=5, minutes=30)
    t = day_start + timedelta(hours=6, minutes=30)

    for ti in (t1, t2, t3, t5):
        seed_try(
            conn,
            site_id=site_id,
            station_id=station_a,
            local_date=D,
            kind="five_minute",
            slot="daytime",
            close_attempt=None,
            tried_at=ti,
            outcome="failed",
            reason="network",
        )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_b,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=t4,
        outcome="failed",
        reason="call_limit",
    )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=t,
        outcome="stored",
    )
    seed_try(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=D,
        kind="five_minute",
        slot="daytime",
        close_attempt=None,
        tried_at=t + timedelta(hours=3),
        outcome="stored",
    )

    at_4h = close_alerts(conn, t + timedelta(hours=4))
    down_4h = next(a for a in at_4h if a.id == "provider_down")
    expected_4h = f"recovered at {isoformat_utc(t)}"
    assert not down_4h.tripped and down_4h.detail == expected_4h, (
        f"M125 -> at this assertion: correct = ok, detail={expected_4h!r}"
        " (the recovered line stays for 24 h after the slot that ended"
        " the run, even past a later answered slot), mutant = the"
        " recovered line kept only while the slot that ended the run is"
        f" the latest judged slot; tripped={down_4h.tripped}, detail="
        f"{down_4h.detail!r}"
    )

    at_24h1s = close_alerts(conn, t + timedelta(hours=24, seconds=1))
    down_24h1s = next(a for a in at_24h1s if a.id == "provider_down")
    assert not down_24h1s.tripped and down_24h1s.detail is None, (
        "M125 paired positive -> at this assertion: correct = ok, no"
        " detail (past 24 h since the slot that ended the run), mutant ="
        f" the recovered line kept past 24 h; tripped={down_24h1s.tripped}"
        f", detail={down_24h1s.detail!r}"
    )

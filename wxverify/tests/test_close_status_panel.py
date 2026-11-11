"""Ops and the close status panel: test IDs OP2-OP9.

Real SQLite throughout (``tests.helpers.asof_conn``). ``utc_now`` is
monkeypatched on ``wxverify.web.context`` wherever a test needs a fixed
`now`. Synthetic data only: stations ``ISTATION01``/
``ISTATION02``, zone UTC, dates from the fixture's ``D``/``NOW``.
"""

from __future__ import annotations

import sqlite3
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
    seed_stage_log,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.site_days import (
    begin_close,
    day_record,
    ensure_site_days,
    fail_close,
    finish_close,
    start_site_days,
)
from wxverify.worker.close_chain import record_collection_try


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


def _station_id(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_op2_close_status_shows_every_day_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP2: "Close Status", with rows for D
    (collecting), D-1 (waiting for the close), D-2 (missed: the close
    never ran), D-3 (closed) (M99)."""
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)

    from wxverify.web.context import load_close_status

    panel = load_close_status(conn, NOW)
    assert panel is not None
    row = next(r for r in panel.rows if r.site_id == site_id)
    by_date = {d.local_date: d for d in row.days}
    d, d1, d2, d3 = (
        D.isoformat(),
        (D - timedelta(days=1)).isoformat(),
        (D - timedelta(days=2)).isoformat(),
        (D - timedelta(days=3)).isoformat(),
    )
    assert by_date[d].state == "collecting", by_date.get(d)
    assert by_date[d1].state == "waiting for the close", (
        "M99 -> at this assertion: correct = 'waiting for the close' for"
        " D-1 (open, past its end), mutant = open days past their end not"
        f" shown; got {by_date[d1].state!r}"
    )
    assert "missed: the close never ran" in by_date[d2].state
    assert by_date[d3].state == "closed"


def test_op3_failed_attempt_shows_retry_and_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP3: D-1 retry_due with "gap fill: network",
    attempt 1 failing at stage 2 and a stage-log row -- State, Stage,
    Failure and the stage line all read from the stored records (M100)."""
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
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
        last_stage=2,
        outcome="failed",
        failed_stage=2,
        failure_reason="gap fill: network",
        ended_at=t + timedelta(minutes=3),
    )
    seed_stage_log(
        conn,
        site_id=site_id,
        local_date=d_minus_1,
        stage=2,
        attempt=1,
        job_id=1,
        enqueued_at=t,
        started_at=t + timedelta(seconds=5),
        ended_at=t + timedelta(minutes=3),
        queue_wait_s=5.0,
        duration_s=175.0,
        outcome="failed",
        reason="gap fill: network",
    )
    fail_close(
        conn,
        site_id,
        d_minus_1,
        now_utc=t + timedelta(minutes=3),
        reason="gap fill: network",
    )

    from wxverify.web.context import load_close_status

    panel = load_close_status(conn, NOW)
    assert panel is not None
    row = next(r for r in panel.rows if r.site_id == site_id)
    day = next(d for d in row.days if d.local_date == d_minus_1.isoformat())
    retry_at_text = isoformat_utc(t + timedelta(minutes=3, hours=1))
    assert retry_at_text in (day.state or ""), (
        "M100 -> at this assertion: correct = retry_at shown in the state"
        f" text, mutant = retry_at not shown; got {day.state!r}"
    )
    assert day.retry_at == retry_at_text, (
        "M100 -> at this assertion: correct = `day.retry_at` ="
        f" {retry_at_text!r} (`CloseDayRow.retry_at`), mutant ="
        " `retry_at` always `None` -- the check above only reads"
        " `_close_day_state`'s own text, built straight from the raw"
        " ``site_days`` record and never touched by this mutant, so it"
        f" cannot see the field's own value go missing; got {day.retry_at!r}"
    )
    assert day.failure == "gap fill: network"
    assert day.stage is not None and "stage 2 gap check and fill" in day.stage
    assert day.stage_line == (
        "stage 2 gap check and fill, started"
        f" {isoformat_utc(t + timedelta(seconds=5))}, waited 5 s in the"
        f" queue, ended {isoformat_utc(t + timedelta(minutes=3))}"
    ), (
        "OP3 -> at this assertion: correct = the stage-log row's started,"
        " queue wait and ended times in the stage line, mutant = a wrong"
        f" or missing field; got {day.stage_line!r}"
    )


def test_op4_publication_state_published_then_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP4: a closed day's publication shows
    "published <t>", then "publication failed <t>: <reason>" (M101)."""
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)
    from wxverify.web.context import load_close_status

    conn = asof_conn()
    site_id = day_close_site(conn)
    d_minus_3 = D - timedelta(days=3)
    published_at = NOW - timedelta(hours=2)
    pub_id = seed_active_publication(
        conn, site_id=site_id, cutoff=d_minus_3, published_at=published_at
    )
    seed_publication_state(
        conn,
        site_id=site_id,
        cutoff=d_minus_3,
        state="published",
        opened_at=published_at - timedelta(minutes=5),
        finished_at=published_at,
        publication_id=str(pub_id),
    )
    panel = load_close_status(conn, NOW)
    assert panel is not None
    row = next(r for r in panel.rows if r.site_id == site_id)
    day = next(d for d in row.days if d.local_date == d_minus_3.isoformat())
    expected_published = f"published {isoformat_utc(published_at)}"
    assert day.publication == expected_published, (
        f"M101 -> at this assertion: correct = {expected_published!r},"
        f" mutant = the publication state not read; got {day.publication!r}"
    )

    conn2 = asof_conn()
    site_id2 = day_close_site(conn2)
    failed_at = NOW - timedelta(hours=1)
    seed_publication_state(
        conn2,
        site_id=site_id2,
        cutoff=d_minus_3,
        state="failed",
        opened_at=failed_at - timedelta(minutes=5),
        finished_at=failed_at,
        last_failure="network",
    )
    panel2 = load_close_status(conn2, NOW)
    assert panel2 is not None
    row2 = next(r for r in panel2.rows if r.site_id == site_id2)
    day2 = next(d for d in row2.days if d.local_date == d_minus_3.isoformat())
    expected_failed = f"publication failed {isoformat_utc(failed_at)}: network"
    assert day2.publication == expected_failed, (
        f"M101 -> at this assertion: correct = {expected_failed!r}, mutant"
        f" = the publication state not read; got {day2.publication!r}"
    )


def test_op5_zone_changeover_and_own_zone_marks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP5: a changeover day still to come shows "zone
    changes on <date>: that day is not scored"; D-3, closed under the old
    zone, shows "scored under its own zone, Etc/GMT-14" (M102)."""
    from wxverify.db.site_days import enter_zone_change
    from wxverify.web.context import load_close_status

    conn = asof_conn()
    site_id = day_close_site(conn, timezone="Etc/GMT-14")
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)

    changeover_day = enter_zone_change(conn, site_id, "Etc/GMT-2", now_utc=NOW)
    panel = load_close_status(conn, NOW)
    assert panel is not None
    row = next(r for r in panel.rows if r.site_id == site_id)
    d_minus_3 = D - timedelta(days=3)
    day3 = next(d for d in row.days if d.local_date == d_minus_3.isoformat())
    assert "scored under its own zone, Etc/GMT-14" in day3.marks, (
        "M102 -> at this assertion: correct = D-3's marks name its own"
        f" zone, mutant = the changeover day still to come not shown;"
        f" marks={day3.marks!r}"
    )
    expected_note = (
        f"zone changes on {changeover_day.isoformat()}: that day is not scored"
    )
    assert any(
        expected_note in (d.marks or ()) or expected_note in (row.note or "")
        for d in row.days
    ) or expected_note in (row.note or ""), (
        f"M102 -> at this assertion: correct = {expected_note!r} present"
        " somewhere on the panel, mutant = the changeover day still to"
        f" come not shown"
    )


def test_op6_no_record_holds_now_shows_not_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP6: the AL17 site (no record
    holds `now`) shows "no day closed yet; the first day has not started"
    and no collecting row; Observation Status "no day closed yet"; never
    "Close status unavailable" (M103)."""
    from wxverify.db.site_days import current_day, start_site_days
    from wxverify.web.context import load_close_status, load_observation_health

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    d_plus_1 = D + timedelta(days=1)
    start_site_days(conn, site_id, d_plus_1, now_utc=NOW)
    assert current_day(conn, site_id, now_utc=NOW) is None
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)

    panel = load_close_status(conn, NOW)
    assert panel is not None
    row = next(r for r in panel.rows if r.site_id == site_id)
    assert row.note is not None and "no day closed yet" in row.note, (
        f"M103 -> at this assertion: correct = note mentions 'no day"
        f" closed yet', mutant = 'no day closed yet' missing; note="
        f"{row.note!r}"
    )
    assert "the first day has not started" in row.note
    assert row.days == ()
    assert "Close status unavailable" not in (
        panel.alert_lines and "".join(panel.alert_lines) or ""
    )

    health_rows = load_observation_health(conn)
    health_row = next(h for h in health_rows if h.site_id == site_id)
    assert (
        health_row.status == "no day closed yet" and health_row.closed_through is None
    )


def test_op7_observation_status_closed_through_header_and_cell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP7: "Closed through" header and the cell D-2,
    once D-2 (not just D-3) is closed (M104)."""
    from wxverify.web.context import load_observation_health

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)
    dates = (D - timedelta(days=4), D - timedelta(days=3), D - timedelta(days=2))
    for i, d in enumerate(dates):
        _close_day(conn, site_id, d, first=(i == 0))

    rows = load_observation_health(conn)
    row = next(r for r in rows if r.site_id == site_id)
    d_minus_2 = (D - timedelta(days=2)).isoformat()
    assert row.closed_through == d_minus_2, (
        "M104 -> at this assertion: correct = closed_through reads D-2 (the"
        " close's own record), mutant = 'Closed through' read from"
        f" last_obs_at instead (which this fixture never sets); got"
        f" {row.closed_through!r}"
    )


def test_op8_alert_lines_include_each_tripped_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP8: the panel's alert lines include AL1's
    missed-day line, alert 7's recovered line and alert 6's days-affected
    line, all in the same read (M105)."""
    from wxverify.worker.access_hold import record_access_refused

    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)
    monkeypatch.setattr(
        "wxverify.worker.access_hold.utc_now", lambda: NOW - timedelta(hours=1)
    )
    from wxverify.web.context import load_close_status

    conn = asof_conn()
    site_id = day_close_site(conn)  # D-2 missed -> AL1
    station_a = _station_id(conn, site_id, "ISTATION01")

    # Alert 7: a down evening slot on D-2, then an answered one on D-1 (recovered).
    d_minus_2 = D - timedelta(days=2)
    d_minus_1 = D - timedelta(days=1)
    record_collection_try(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=d_minus_2.isoformat(),
        kind="five_minute",
        slot="18:00",
        close_attempt=None,
        tried_at=isoformat_utc(
            datetime(d_minus_2.year, d_minus_2.month, d_minus_2.day, 18, tzinfo=UTC)
        ),
        outcome="failed",
        reason="network",
        detail=None,
        readings_kept=0,
        readings_refused=0,
    )
    record_collection_try(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=d_minus_1.isoformat(),
        kind="five_minute",
        slot="18:00",
        close_attempt=None,
        tried_at=isoformat_utc(
            datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 18, tzinfo=UTC)
        ),
        outcome="stored",
        reason=None,
        detail=None,
        readings_kept=4,
        readings_refused=0,
    )

    # Alert 6: a current_obs refusal in force.
    record_access_refused(conn, "current_obs", 401, reason=None)

    panel = load_close_status(conn, NOW)
    assert panel is not None
    lines = "\n".join(panel.alert_lines)
    assert "close_day_missed" in lines or "missed, the close never ran" in lines, (
        f"M105 -> at this assertion: correct = AL1's line present, mutant"
        f" = alert 6's days list (and the other alert lines) not on the"
        f" panel; lines={lines!r}"
    )
    assert "recovered at" in lines
    assert "weathercom_access_refused" in lines or "access refused" in lines


def test_op9_load_close_status_catches_any_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OP9: on a stand-in `close_alerts` that raises,
    the panel shows "Close status unavailable: <Type>: <msg>"; the other
    panels still render (M106)."""
    monkeypatch.setattr("wxverify.web.context.utc_now", lambda: NOW)

    def _boom(conn: object, now: object) -> object:  # noqa: ARG001
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr("wxverify.close_monitor.close_alerts", _boom, raising=False)
    from wxverify.web.context import load_ops

    conn = asof_conn()
    day_close_site(conn)

    context = load_ops(conn)
    assert context.get("close_panel") is not None
    assert "Close status unavailable: RuntimeError: synthetic failure" in str(
        context
    ), (
        "M106 -> at this assertion: correct = the wrapping try catches the"
        " stand-in's RuntimeError and shows 'Close status unavailable:"
        " RuntimeError: synthetic failure', mutant = no wrapping try, so"
        f" the exception would propagate instead of being caught; context="
        f"{context!r}"
    )
    assert (
        context.get("backfill") is not None
        or context.get("observation_health") is not None
    )

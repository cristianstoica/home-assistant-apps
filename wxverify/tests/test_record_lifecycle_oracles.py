"""§17 family 3 — record lifecycle, gap-scan traversal, and the durable
failure signal.

The per-item tests in §4 pin each W1 change where it landed. This file closes
the family's remaining ground, all of which is invisible from any single-date,
single-chunk fixture:

* a partial day completes on an in-window reconstruction, per identity, with
  the already-written rows untouched;
* window close writes the FULL identity set as ``missed``, each row carrying
  the reason its own as-of-T assessment supports;
* the scan traverses from the log's ORIGIN, so a hole behind a later complete
  day is still visited, and an old log is entered at the capped origin;
* the post-window-close gate is asserted on the predicate itself over
  constructed ``(now, snapshot_utc)`` pairs, including the sub-01:00 DST
  fall-back pair that is its only reachable trigger;
* the record enqueue holds for an hour after a success and fails CLOSED on an
  unreadable completion stamp;
* a failed date is contained, named durably, and trips the operator condition
  on the same tick, while worker control signals are NOT contained;
* the signal merges across chunks in separate transactions, and clears.

Synthetic fixtures only: invented site names, ``America/Denver`` for the DST
pair, UTC elsewhere, invented feed models.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import wxverify.verification.record as record_mod
import wxverify.worker.scheduler as scheduler_mod
from tests.test_forecast_record import (
    _conn,
    _insert_temp_day,
    _make_feed,
    _make_site,
    _row_count,
    _seed_grid_for,
    _snapshot_t,
)
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.queue import (
    claim_next_job,
    enqueue_if_absent,
    enqueue_if_absent_with_cooldown,
)
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.monitor import build_verdict
from wxverify.verification.methodology import LATE_WRITE_WINDOW_HOURS
from wxverify.verification.record import (
    GAP_SCAN_LOOKBACK_DAYS,
    MISSED_NO_CANDIDATES,
    MISSED_WINDOW_CLOSED,
    RECORD_DAY_COUNT,
    RECORD_VARIABLES,
    build_forecast_record,
    expected_record_identities,
    gap_scan_degraded_sites,
    gap_scan_failures_key,
    resolve_snapshot_utc,
    run_record_gap_scan,
    sites_with_record_gap,
)
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.processor import dispatch
from wxverify.worker.scheduler import (
    RECORD_RETRY_INTERVAL,
    _enqueue_due_forecast_records,
    record_window_open,
)

_DAY = date(2035, 6, 15)

#: The full day grid: every variable at every display lead.
_GRID = len(RECORD_VARIABLES) * RECORD_DAY_COUNT


def _rows(conn: sqlite3.Connection, site_id: int, day: date) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT variable, display_lead, status, missed_reason, write_path,
               selected_feed_ids
        FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = ?
        ORDER BY variable, display_lead
        """,
        (site_id, day.isoformat()),
    ).fetchall()


def _identities(rows: list[sqlite3.Row]) -> set[tuple[str, int]]:
    return {(str(r["variable"]), int(r["display_lead"])) for r in rows}


def _seed_variable_across_the_grid(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    snapshot_day: date,
    variable: str,
) -> None:
    """Make ``variable`` knowable at T for every display lead of one day."""
    issued = datetime.combine(snapshot_day, datetime.min.time(), UTC) + timedelta(
        hours=6
    )
    for offset in range(RECORD_DAY_COUNT):
        _insert_temp_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            local_date=snapshot_day + timedelta(days=offset),
            issued_at=isoformat_utc(issued),
            fetched_at=isoformat_utc(issued + timedelta(minutes=5)),
            value=10.0,
            variable=variable,
        )


def _begin_the_log(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """A complete on-time record for ``_DAY``: the log's origin."""
    _seed_grid_for(conn, site_id=site_id, feed_id=feed_id, day=_DAY)
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=_snapshot_t(_DAY) + timedelta(minutes=5)
    )


# ---------------------------------------------------------------------------
# Completion and per-identity idempotence
# ---------------------------------------------------------------------------


def test_a_partial_day_completes_on_an_in_window_reconstruction() -> None:
    """§4: a day left partial is finished by a later in-window build, and the
    rows that were already there are not rewritten.

    ``write_path`` is the discriminator that makes both halves non-vacuous at
    once: the survivors must still read ``on_time`` (they were not replaced)
    and the refilled identities must read ``late_reconstruction`` (they were
    genuinely written by the second build, not merely counted).

    Kills, one at a time:
    - ``ON CONFLICT ... DO NOTHING`` widened to ``DO UPDATE`` (survivors flip
      to ``late_reconstruction``);
    - the builder's completeness early-out reading presence instead of the
      identity set (the second build returns at once, leaving the day short);
    - the early-out dropped entirely AND the insert made unconditional
      (survivors flip, same first kill from the other side).
    """
    conn = _conn()
    site_id = _make_site(conn, "site-partial")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _begin_the_log(conn, site_id, feed_id)
    assert len(_rows(conn, site_id, _DAY)) == _GRID

    # Amputate one whole variable's leads 5..7 -- a partial day of the exact
    # shape an interrupted write leaves behind.
    holes = {("precip", 5), ("precip", 6), ("precip", 7)}
    for variable, lead in holes:
        conn.execute(
            """
            DELETE FROM forecast_of_record
            WHERE site_id = ? AND snapshot_local_date = ?
              AND variable = ? AND display_lead = ?
            """,
            (site_id, _DAY.isoformat(), variable, lead),
        )
    assert len(_rows(conn, site_id, _DAY)) == _GRID - len(holes)

    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=_snapshot_t(_DAY) + timedelta(hours=3)
    )

    rows = _rows(conn, site_id, _DAY)
    assert _identities(rows) == expected_record_identities()
    paths = {
        (str(r["variable"]), int(r["display_lead"])): str(r["write_path"]) for r in rows
    }
    assert {k for k, v in paths.items() if v == "late_reconstruction"} == holes
    assert {k for k, v in paths.items() if v == "on_time"} == (
        expected_record_identities() - holes
    )
    assert all(str(r["status"]) == "recorded" for r in rows)


# ---------------------------------------------------------------------------
# Window close: the full identity set, each row with its own reason
# ---------------------------------------------------------------------------


def test_window_close_writes_the_full_identity_set_with_per_identity_reasons() -> None:
    """§4/§17 family 9: ``missed`` rows are total over the day's identities and
    each carries the reason ITS OWN as-of-T assessment supports.

    The fixture makes two of the three variables knowable at T across every
    display lead and leaves the third with nothing, so the two reasons are
    both present and are not interchangeable: a single-reason implementation
    reports 24 of one token and fails on whichever half it dropped.

    Kills, one at a time:
    - the reason map collapsed to a constant ``MISSED_WINDOW_CLOSED``;
    - the reason map collapsed to a constant ``MISSED_NO_CANDIDATES``;
    - ``assess_record_day`` returning presence over the SAMPLED identities
      instead of over ``expected_record_identities()`` (the precip keys go
      missing and ``_write_missed_rows`` raises on the unmapped identity);
    - the reason inverted (``if had_candidates`` -> ``if not had_candidates``).
    """
    conn = _conn()
    site_id = _make_site(conn, "site-reasons")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _begin_the_log(conn, site_id, feed_id)

    # Nine days past the origin, so none of the origin grid's samples (which
    # reach seven days forward) is still in this day's future at ITS T -- the
    # unseeded variable has to be genuinely candidate-less, not merely
    # under-seeded.
    gap_day = _DAY + timedelta(days=9)
    for variable in ("temperature", "wind"):
        _seed_variable_across_the_grid(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            snapshot_day=gap_day,
            variable=variable,
        )

    # Past gap_day's window close, still inside the following day's.
    now = _snapshot_t(gap_day) + timedelta(hours=LATE_WRITE_WINDOW_HOURS + 1)
    assert run_record_gap_scan(conn, site_id, {}, now=now) is None

    rows = _rows(conn, site_id, gap_day)
    assert _identities(rows) == expected_record_identities()
    assert len(rows) == _GRID == 24
    assert all(str(r["status"]) == "missed" for r in rows)
    reasons = {
        (str(r["variable"]), int(r["display_lead"])): str(r["missed_reason"])
        for r in rows
    }
    assert {k for k, v in reasons.items() if v == MISSED_WINDOW_CLOSED} == {
        (variable, lead)
        for variable in ("temperature", "wind")
        for lead in range(RECORD_DAY_COUNT)
    }
    assert {k for k, v in reasons.items() if v == MISSED_NO_CANDIDATES} == {
        ("precip", lead) for lead in range(RECORD_DAY_COUNT)
    }


# ---------------------------------------------------------------------------
# Traversal: from the origin, and its two bounds
# ---------------------------------------------------------------------------


def test_gap_scan_visits_a_partial_day_behind_a_later_complete_day() -> None:
    """§4: the traversal lower bound is the log's ORIGIN, not its tail.

    Kills: deriving ``start`` from ``MAX(snapshot_local_date)`` (or from the
    latest COMPLETE day) -- the hole at ``_DAY+1`` sits behind a complete
    ``_DAY+2`` and is never visited, so it stays absent forever.
    """
    conn = _conn()
    site_id = _make_site(conn, "site-behind")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _begin_the_log(conn, site_id, feed_id)

    hole = _DAY + timedelta(days=1)
    later = _DAY + timedelta(days=2)
    _seed_grid_for(conn, site_id=site_id, feed_id=feed_id, day=later)
    build_forecast_record(
        conn, site_id, later.isoformat(), now=_snapshot_t(later) + timedelta(minutes=5)
    )
    assert _rows(conn, site_id, hole) == []

    now = _snapshot_t(later) + timedelta(hours=LATE_WRITE_WINDOW_HOURS + 1)
    assert run_record_gap_scan(conn, site_id, {}, now=now) is None

    hole_rows = _rows(conn, site_id, hole)
    assert _identities(hole_rows) == expected_record_identities()
    assert all(str(r["status"]) == "missed" for r in hole_rows)
    # Non-vacuity from the other side: the complete day behind which the hole
    # sat was left exactly as the on-time build wrote it.
    assert {str(r["write_path"]) for r in _rows(conn, site_id, later)} == {"on_time"}


def test_gap_scan_enters_an_old_log_at_the_capped_origin() -> None:
    """§4: an origin older than ``GAP_SCAN_LOOKBACK_DAYS`` is clamped, so the
    walk does not grow with the age of the log.

    Kills: dropping the ``max(..., window_start)`` clamp -- the scan reaches
    back to the log's true first date and writes ``missed`` rows there.
    """
    conn = _conn()
    site_id = _make_site(conn, "site-old-log")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    origin = _DAY - timedelta(days=GAP_SCAN_LOOKBACK_DAYS + 10)
    _seed_grid_for(conn, site_id=site_id, feed_id=feed_id, day=origin)
    build_forecast_record(
        conn,
        site_id,
        origin.isoformat(),
        now=_snapshot_t(origin) + timedelta(minutes=5),
    )

    now = _snapshot_t(_DAY) + timedelta(hours=2)
    today = now.astimezone(UTC).date()
    expected_first = today - timedelta(days=GAP_SCAN_LOOKBACK_DAYS)
    run_record_gap_scan(conn, site_id, {}, now=now)

    written = conn.execute(
        """
        SELECT MIN(snapshot_local_date) AS first FROM forecast_of_record
        WHERE site_id = ? AND status = 'missed'
        """,
        (site_id,),
    ).fetchone()
    assert str(written["first"]) == expected_first.isoformat()
    # Nothing between the true origin and the capped one was touched.
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS n FROM forecast_of_record
            WHERE site_id = ? AND snapshot_local_date > ?
              AND snapshot_local_date < ?
            """,
            (site_id, origin.isoformat(), expected_first.isoformat()),
        ).fetchone()["n"]
        == 0
    )


def test_a_generation_with_no_rows_is_still_not_scanned() -> None:
    """The other traversal bound: no origin means no gap (the no-backfill
    rule). Paired with a positive so the zero is not ambient.

    Kills: deriving the origin from site creation or the generation's
    ``effective_from`` when the log has not begun.
    """
    conn = _conn()
    site_id = _make_site(conn, "site-fresh")
    ensure_published_generation(conn, site_id)
    now = _snapshot_t(_DAY) + timedelta(hours=LATE_WRITE_WINDOW_HOURS + 1)
    assert run_record_gap_scan(conn, site_id, {}, now=now) is None
    assert _row_count(conn, site_id) == 0

    # Paired positive on the SAME connection and clock: once the log has an
    # origin the identical call does write.
    feed_id = _make_feed(conn, "model-a")
    _begin_the_log(conn, site_id, feed_id)
    assert run_record_gap_scan(conn, site_id, {}, now=now) is None
    assert _row_count(conn, site_id) > _GRID


# ---------------------------------------------------------------------------
# The post-window-close gate, asserted on the predicate
# ---------------------------------------------------------------------------


def test_the_record_window_gate_is_inclusive_and_closes_past_the_window() -> None:
    """``record_window_open`` over constructed pairs, never over an enqueue
    count: the scheduler re-derives both instants from the same local day, so
    an enqueue-count assertion reads zero whether the guard is there or not.

    Kills: ``now <= snapshot + window`` -> ``<`` (the exact boundary closes);
    the guard replaced by ``True`` (the 25 h pair opens).
    """
    t = datetime(2035, 6, 15, 7, 0, tzinfo=UTC)
    window = timedelta(hours=LATE_WRITE_WINDOW_HOURS)
    assert record_window_open(t, t) is True
    assert record_window_open(t + window - timedelta(seconds=1), t) is True
    assert record_window_open(t + window, t) is True  # inclusive boundary
    assert record_window_open(t + window + timedelta(seconds=1), t) is False


def test_the_dst_fall_back_day_is_the_gates_reachable_trigger() -> None:
    """The only pair the scheduler can actually construct that closes the
    window: a sub-01:00 snapshot time on a fall-back day, where consecutive
    Ts lie 25 h apart, evaluated at the last instant whose LOCAL date is
    still the fall-back day.

    The DST day and the ordinary day are built by the same expression, so the
    False/True split is attributable to the transition alone.

    Kills: the guard replaced by ``True``; ``<=`` -> ``<`` is NOT killed here
    (this pair is 30 minutes past the boundary, not on it) -- the boundary
    kill lives in the predicate test above.
    """
    tz_name = "America/Denver"
    tz = ZoneInfo(tz_name)
    wall_clock = "00:30"

    def _last_instant_of(day: date) -> datetime:
        """The last UTC instant whose LOCAL date is still ``day``.

        Anchored on the next calendar day's local midnight so the arithmetic
        never lands inside a transition -- this is the instant at which the
        scheduler last derives ``local_today == day``.
        """
        nxt = day + timedelta(days=1)
        local_midnight = datetime(nxt.year, nxt.month, nxt.day, tzinfo=tz)
        return (local_midnight - timedelta(seconds=1)).astimezone(UTC)

    fall_back = date(2035, 11, 4)
    ordinary = date(2035, 11, 6)

    fb_t = resolve_snapshot_utc(tz_name, fall_back, wall_clock)
    fb_next_t = resolve_snapshot_utc(tz_name, fall_back + timedelta(days=1), wall_clock)
    # Non-vacuity: this is the 25-hour day, and only on it does the last
    # in-day instant exceed T by more than the window.
    assert fb_next_t - fb_t == timedelta(hours=25)
    assert record_window_open(_last_instant_of(fall_back), fb_t) is False

    ord_t = resolve_snapshot_utc(tz_name, ordinary, wall_clock)
    ord_next_t = resolve_snapshot_utc(tz_name, ordinary + timedelta(days=1), wall_clock)
    assert ord_next_t - ord_t == timedelta(hours=24)
    assert record_window_open(_last_instant_of(ordinary), ord_t) is True


# ---------------------------------------------------------------------------
# Enqueue cadence
# ---------------------------------------------------------------------------


def test_the_record_enqueue_holds_for_an_hour_after_a_successful_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§4 change 4: the completeness gate stays false all day on an ordinary
    partial day, so the success cooldown is what keeps the due-check from
    re-enqueuing at worker-loop cadence.

    Kills: dropping ``success_cooldown=RECORD_RETRY_INTERVAL`` from the record
    due-check (the second tick, ten minutes later, enqueues again).
    """
    conn = _conn()
    site_id = _make_site(conn, "site-cadence")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    today = datetime.now(UTC).date()
    # One variable only: the day is written but never complete, so the
    # completeness gate stays open for the whole run of this test.
    _seed_variable_across_the_grid(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        snapshot_day=today,
        variable="temperature",
    )
    clock = {"now": resolve_snapshot_utc("UTC", today, "07:00") + timedelta(minutes=1)}
    monkeypatch.setattr(scheduler_mod, "utc_now", lambda: clock["now"])
    monkeypatch.setattr("wxverify.db.queue.utc_now", lambda: clock["now"])

    def _jobs() -> int:
        return int(
            conn.execute(
                """
                SELECT COUNT(*) AS n FROM jobs
                WHERE type = 'forecast_record' AND site_id = ?
                """,
                (site_id,),
            ).fetchone()["n"]
        )

    def _settle() -> None:
        conn.execute(
            """
            UPDATE jobs SET status = 'completed', updated_at = ?
            WHERE type = 'forecast_record' AND site_id = ? AND status != 'completed'
            """,
            (isoformat_utc(clock["now"]), site_id),
        )

    _enqueue_due_forecast_records(conn)
    assert _jobs() == 1
    _settle()

    clock["now"] += timedelta(minutes=10)
    _enqueue_due_forecast_records(conn)
    assert _jobs() == 1, "a completed run inside the cooldown must not re-enqueue"

    clock["now"] += RECORD_RETRY_INTERVAL
    _enqueue_due_forecast_records(conn)
    assert _jobs() == 2, "past the cooldown the incomplete day is retried"


def test_an_unreadable_completion_stamp_suppresses_only_its_own_day(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """§4 change 4: the success cooldown fails CLOSED, unlike the failure
    cooldown beside it -- failing open here is an unbounded tier-0 loop, and
    what bounds the suppression is the date-scoped ``job_key``.

    Both halves in one case, because the suppression is only defensible if it
    is confined: the day with the corrupt stamp is held, and the NEXT day's
    key is unaffected on the same call sequence.

    Kills: the ``except ValueError`` arm returning nothing (falling through to
    enqueue) instead of ``return None``; the ``success_cooldown`` branch
    borrowing the failure cooldown's fail-OPEN handling.
    """
    conn = _conn()
    site_id = _make_site(conn, "site-corrupt-stamp")
    day = _DAY
    held = f"record:{day.isoformat()}"
    free = f"record:{(day + timedelta(days=1)).isoformat()}"
    enqueue_if_absent(conn, "forecast_record", site_id, held, {})
    conn.execute(
        """
        UPDATE jobs SET status = 'completed', updated_at = 'not-a-timestamp'
        WHERE type = 'forecast_record' AND site_id = ? AND job_key = ?
        """,
        (site_id, held),
    )

    with caplog.at_level(logging.WARNING, logger="wxverify.db.queue"):
        suppressed = enqueue_if_absent_with_cooldown(
            conn,
            "forecast_record",
            site_id,
            held,
            {},
            cooldown=timedelta(hours=1),
            success_cooldown=RECORD_RETRY_INTERVAL,
        )
    assert suppressed is None
    assert "success cooldown: unreadable updated_at" in caplog.text

    admitted = enqueue_if_absent_with_cooldown(
        conn,
        "forecast_record",
        site_id,
        free,
        {},
        cooldown=timedelta(hours=1),
        success_cooldown=RECORD_RETRY_INTERVAL,
    )
    assert admitted is not None
    keys = [
        str(r["job_key"])
        for r in conn.execute(
            "SELECT job_key FROM jobs WHERE site_id = ? ORDER BY id", (site_id,)
        ).fetchall()
    ]
    assert keys == [held, free]


# ---------------------------------------------------------------------------
# sites_with_record_gap keeps PRESENCE semantics
# ---------------------------------------------------------------------------


def test_a_day_closed_as_missed_is_not_an_operator_gap() -> None:
    """§4: the operator alarm asks "did the record run at all today", so a day
    the gap scan closed with 24 ``missed`` rows is NOT a gap -- there is
    nothing left for an operator to do about it.

    The fourth case of the helper's set; the other three (a partial day past
    T+slack counts 0, a day with no rows counts 1, a log not begun counts 0)
    are in ``test_sites_with_record_gap``.

    Kills: ``sites_with_record_gap`` swept along with §4 change 4 to call
    ``record_day_complete`` (a day of ``missed`` rows is complete under that
    predicate too, so the kill is the OTHER swap -- counting only
    ``status = 'recorded'`` rows, which makes every closed day alarm forever).
    """
    conn = _conn()
    site_id = _make_site(conn, "site-closed-day")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _begin_the_log(conn, site_id, feed_id)

    gap_day = _DAY + timedelta(days=1)
    now = _snapshot_t(gap_day) + timedelta(hours=LATE_WRITE_WINDOW_HOURS + 1)
    # Before the scan the expected day is genuinely missing: the alarm is on.
    at_gap_day = _snapshot_t(gap_day) + timedelta(hours=2)
    assert sites_with_record_gap(conn, at_gap_day) == 1

    run_record_gap_scan(conn, site_id, {}, now=now)
    rows = _rows(conn, site_id, gap_day)
    assert len(rows) == _GRID
    assert all(str(r["status"]) == "missed" for r in rows)
    assert sites_with_record_gap(conn, at_gap_day) == 0


# ---------------------------------------------------------------------------
# Per-date failure containment and the durable signal
# ---------------------------------------------------------------------------


def _fail_on(monkeypatch: pytest.MonkeyPatch, target: date, exc: BaseException) -> None:
    real = record_mod.assess_record_day

    def _fake(
        conn: sqlite3.Connection,
        site_id: int,
        local_date: date,
        *,
        as_of: str,
    ) -> dict[tuple[str, int], bool]:
        if local_date == target:
            raise exc
        return real(conn, site_id, local_date, as_of=as_of)

    monkeypatch.setattr(record_mod, "assess_record_day", _fake)


def _signal(conn: sqlite3.Connection, site_id: int) -> dict[str, object] | None:
    raw = get_runtime_state(conn, gap_scan_failures_key(site_id))
    if raw is None:
        return None
    parsed: dict[str, object] = json.loads(raw)
    return parsed


def _failed_dates(conn: sqlite3.Connection, site_id: int) -> set[str]:
    signal = _signal(conn, site_id)
    assert signal is not None, "no durable gap-scan failure signal"
    dates = signal["dates"]
    assert isinstance(dates, dict)
    return set(dates)


def _degraded_condition(conn: sqlite3.Connection, now: datetime) -> dict[str, object]:
    """The ``record_gap_scan_degraded`` condition as the operator reads it."""
    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=now,
    )
    conditions = verdict["conditions"]
    assert isinstance(conditions, list)
    matches = [
        c
        for c in conditions
        if isinstance(c, dict) and c.get("id") == "record_gap_scan_degraded"
    ]
    assert len(matches) == 1, "record_gap_scan_degraded must be present"
    return matches[0]


def _two_gap_days(conn: sqlite3.Connection) -> tuple[int, date, date, datetime]:
    site_id = _make_site(conn, "site-contained")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _begin_the_log(conn, site_id, feed_id)
    first = _DAY + timedelta(days=1)
    second = _DAY + timedelta(days=2)
    now = _snapshot_t(second) + timedelta(hours=LATE_WRITE_WINDOW_HOURS + 1)
    return site_id, first, second, now


def test_a_failed_date_is_contained_named_durably_and_trips_the_condition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§4 change 7: one date's failure costs that date only.

    Kills, one at a time:
    - the per-date ``SAVEPOINT``/``ROLLBACK TO`` replaced by a chunk-level
      rollback (the healthy date loses its rows too);
    - the ``except Exception`` arm re-raising (the job fails, no signal);
    - the failed date recorded as ``missed``/``window_closed`` anyway (the
      failed date gains 24 rows asserting a lost write that was never shown);
    - ``record_gap_scan_degraded`` gated on ``FAILED_JOB_AGE_HOURS`` or any
      freshness window (the condition does not trip on the same tick).
    """
    conn = _conn()
    site_id, healthy, broken, now = _two_gap_days(conn)
    _fail_on(monkeypatch, broken, RuntimeError("assessment blew up"))

    assert run_record_gap_scan(conn, site_id, {}, now=now) is None

    assert _identities(_rows(conn, site_id, healthy)) == expected_record_identities()
    assert _rows(conn, site_id, broken) == []

    assert _failed_dates(conn, site_id) == {broken.isoformat()}

    tripped, newest = gap_scan_degraded_sites(conn)
    assert (tripped, newest is not None) == (1, True)
    condition = _degraded_condition(conn, now)
    assert (condition["ok"], condition["count"]) == (False, 1)


def test_the_failure_signal_survives_a_clean_continuation_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§4 change 7: the signal is merged per date, not replaced per chunk.

    A single-chunk fixture cannot show this at all -- the write is the same
    either way. Two chunks in separate transactions can: the first records a
    failure, the second is clean and must not erase it.

    Kills, one at a time:
    - ``_merge_gap_scan_failures`` writing ``{"dates": failed}`` (replace per
      chunk -- the clean second chunk blanks the key);
    - the key deleted whenever a chunk reports no failures;
    - ``cleared`` widened from the dates this chunk visited to all dates.
    """
    conn = _conn()
    site_id, broken, clean, now = _two_gap_days(conn)
    _fail_on(monkeypatch, broken, RuntimeError("assessment blew up"))
    monkeypatch.setattr(record_mod, "GAP_SCAN_MAX_DATES", 2)

    first_chunk = run_record_gap_scan(conn, site_id, {}, now=now)
    conn.commit()
    assert first_chunk is not None
    assert str(first_chunk["after_date"]) == broken.isoformat()
    assert _failed_dates(conn, site_id) == {broken.isoformat()}

    # The continuation chunk starts past the failed date, so it is clean
    # without disarming anything -- the injected failure is never reached.
    second_chunk = run_record_gap_scan(conn, site_id, first_chunk, now=now)
    conn.commit()
    assert second_chunk is None
    assert _identities(_rows(conn, site_id, clean)) == expected_record_identities()

    assert _failed_dates(conn, site_id) == {broken.isoformat()}
    assert gap_scan_degraded_sites(conn)[0] == 1


def test_a_later_successful_assessment_clears_the_signal_and_the_condition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The clearing half: without it the family pins a signal that can never
    turn off.

    Kills, one at a time:
    - ``cleared`` never populated on the success path (the entry stands
      forever);
    - the key retained with an empty ``dates`` object (``gap_scan_degraded_sites``
      would still have to special-case it, and any reader counting keys alarms).
    """
    conn = _conn()
    site_id, healthy, broken, now = _two_gap_days(conn)
    _fail_on(monkeypatch, broken, RuntimeError("assessment blew up"))
    run_record_gap_scan(conn, site_id, {}, now=now)
    assert gap_scan_degraded_sites(conn)[0] == 1
    assert _rows(conn, site_id, broken) == []

    monkeypatch.undo()
    assert run_record_gap_scan(conn, site_id, {}, now=now) is None

    assert _identities(_rows(conn, site_id, broken)) == expected_record_identities()
    assert _signal(conn, site_id) is None
    assert gap_scan_degraded_sites(conn) == (0, None)
    assert _degraded_condition(conn, now)["ok"] is True


# ---------------------------------------------------------------------------
# Worker control signals are NOT contained (driven through the processor)
# ---------------------------------------------------------------------------


def _tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    return init_db(config.db_path)._conn  # noqa: SLF001


def _seed_scan_job(conn: sqlite3.Connection) -> tuple[int, date, date, datetime]:
    site_id, first, second, now = _two_gap_days(conn)
    enqueue_if_absent(conn, "record_gap_scan", site_id, "gapscan:test", {})
    return site_id, first, second, now


def _dispatch_the_scan(conn: sqlite3.Connection) -> None:
    job = claim_next_job(conn)
    assert job is not None and job.type == "record_gap_scan"
    db = get_db()
    asyncio.run(dispatch(db, FencedWriter(db, db.generation), job))


@pytest.mark.parametrize(
    ("signal", "expected"),
    [
        (JobDeferred("2035-06-18T07:00:00Z"), JobDeferred),
        (JobCancelled(), JobCancelled),
    ],
    ids=["deferral", "cancellation"],
)
def test_a_control_signal_on_a_later_date_is_not_contained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    signal: BaseException,
    expected: type[BaseException],
) -> None:
    """§4 change 7: the containment carve-out, driven through the processor so
    the deferral/cancellation is observed where the worker observes it.

    Absorbing a control signal into the durable failure state would turn a
    deferral into a recorded failure and lose the processor's ordered
    dispatch; and because the write transaction unwinds, the chunk's EARLIER
    writes must be gone rather than half-committed.

    Kills, one at a time:
    - ``except (JobDeferred, JobCancelled, StaleGenerationError): raise``
      deleted (the bare ``except Exception`` swallows the signal, the job
      completes, and the healthy date's rows survive);
    - the per-date ``SAVEPOINT`` released rather than rolled back on the
      control path (the earlier date's writes commit).
    """
    conn = _tmp_db(tmp_path)
    try:
        site_id, healthy, broken, _now = _seed_scan_job(conn)
        _fail_on(monkeypatch, broken, signal)
        # The processor calls the scan with the ambient clock.
        now = _snapshot_t(broken) + timedelta(hours=LATE_WRITE_WINDOW_HOURS + 1)
        monkeypatch.setattr(record_mod, "utc_now", lambda: now)

        with pytest.raises(expected):
            _dispatch_the_scan(conn)

        # The transaction unwound: nothing this chunk wrote survives, and no
        # durable failure was recorded for a control signal.
        assert _rows(conn, site_id, healthy) == []
        assert _rows(conn, site_id, broken) == []
        assert _signal(conn, site_id) is None
        assert gap_scan_degraded_sites(conn) == (0, None)
        # Non-vacuity: the log's origin is still there, so "no rows" above is
        # about this chunk, not an empty database.
        assert len(_rows(conn, site_id, _DAY)) == _GRID
    finally:
        close_db()

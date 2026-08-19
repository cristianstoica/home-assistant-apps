"""Item 7 (§1.2/D18): the nightly verification trigger clears the settlement
boundary on every date class.

The existing coverage in `test_verification_trigger_status.py` is
insensitive by construction: its two fixture instants (`_AFTER_TRIGGER`
2026-06-06T12:00Z, `_BEFORE_TRIGGER` 2026-06-06T07:00Z) sit on the same side
of both 02:00 local and 05:00 local, so a trigger reverted to 02:00 is
invisible to it. O40/O41 close that gap directly.

Synthetic fixtures only: `America/Denver` for the DST classes, `Etc/GMT+7`
for the fixed-offset control.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_make_site
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.methodology import CONSENSUS_LAG_HOURS
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.runs import (
    expected_trigger_date,
    record_trigger_decision,
    trigger_decision_blocks,
)
from wxverify.worker import scheduler as scheduler_module
from wxverify.worker.scheduler import (
    VERIFICATION_TRIGGER_LOCAL_TIME,
    _enqueue_due_verification_runs,  # noqa: SLF001
)

_TZ = "America/Denver"
_FIXED_TZ = "Etc/GMT+7"


def _margin(timezone: str, local_date: date, trigger_time: str) -> timedelta:
    """Time between the trigger instant and the previous local day's
    settlement instant (day-end + the consensus lag). Positive means the
    trigger fires after the previous day is settled; negative means it
    fires too early and would read an unsettled day.
    """
    trigger_utc = resolve_snapshot_utc(timezone, local_date, trigger_time)
    previous_day_end = local_day_bounds(
        local_date - timedelta(days=1), timezone
    ).end_utc
    settled_at = previous_day_end + timedelta(hours=CONSENSUS_LAG_HOURS)
    return trigger_utc - settled_at


# ---------------------------------------------------------------------------
# O40 — the configured trigger, with the old value as the separating control.
# ---------------------------------------------------------------------------


def test_configured_trigger_clears_settlement_on_every_date_class() -> None:
    """M29 target: reverting the trigger to '02:00' fails this half.

    The negative control lives in the SAME test, on the SAME fixtures, so
    the two halves cannot be selected, skipped, or deleted independently:
    proving `_margin`'s arithmetic can go negative (old '02:00' value) is
    what makes the strictly-positive assertions on the configured trigger
    non-vacuous.
    """
    ordinary = date(2026, 6, 15)
    spring_forward = date(2026, 3, 8)
    fall_back = date(2026, 11, 1)

    ordinary_margin = _margin(_TZ, ordinary, VERIFICATION_TRIGGER_LOCAL_TIME)
    spring_margin = _margin(_TZ, spring_forward, VERIFICATION_TRIGGER_LOCAL_TIME)
    fall_margin = _margin(_TZ, fall_back, VERIFICATION_TRIGGER_LOCAL_TIME)

    assert ordinary_margin > timedelta(0)
    assert spring_margin > timedelta(0)
    assert fall_margin > timedelta(0)

    assert ordinary_margin == timedelta(hours=2)
    assert spring_margin == timedelta(hours=1)
    assert fall_margin == timedelta(hours=3)

    # Negative control: the OLD trigger value, on the same two fixtures,
    # must fire too early (a negative margin). If `_margin` always agreed
    # with the strictly-positive assertions above regardless of trigger
    # value, they would be vacuous.
    assert _margin(_TZ, ordinary, "02:00") < timedelta(0)
    assert _margin(_TZ, spring_forward, "02:00") < timedelta(0)


# ---------------------------------------------------------------------------
# O41 — fixed-offset control, and the reader/scheduler agreement.
# ---------------------------------------------------------------------------


def test_fixed_offset_margin_is_pinned_independent_of_dst() -> None:
    """(a) No transitions on `Etc/GMT+7`: pins the arithmetic on its own,
    with no DST reasoning involved."""
    margin = _margin(_FIXED_TZ, date(2026, 6, 15), VERIFICATION_TRIGGER_LOCAL_TIME)
    assert margin == timedelta(hours=2)


def test_expected_trigger_date_agrees_with_the_shared_trigger_constant() -> None:
    """(b) Fixed instants straddling 05:00 local, deliberately NOT derived
    from `VERIFICATION_TRIGGER_LOCAL_TIME`: deriving them would move with a
    mutated constant and silently forfeit this test's M29 kill."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (_TZ, site_id))

    # America/Denver is UTC-6 in June: 04:59/05:01 local is 10:59Z/11:01Z.
    before_utc = datetime(2026, 6, 15, 10, 59, tzinfo=UTC)
    after_utc = datetime(2026, 6, 15, 11, 1, tzinfo=UTC)

    assert expected_trigger_date(conn, site_id, before_utc) == "2026-06-14"
    assert expected_trigger_date(conn, site_id, after_utc) == "2026-06-15"


def test_trigger_decision_blocks_a_second_run_for_an_already_fired_date() -> None:
    """(c) A `run_started` decision must stop a re-enqueue regardless of
    where the trigger time itself sits."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    record_trigger_decision(
        conn,
        site_id,
        trigger_date="2026-06-15",
        decision="run_started",
        reason=None,
    )

    assert trigger_decision_blocks(conn, site_id, "2026-06-15", held=False) is True


# ---------------------------------------------------------------------------
# Behavior oracle: the scheduler itself, driven at real local instants
# straddling the configured trigger — not the arithmetic that produces it.
# ---------------------------------------------------------------------------


def test_scheduler_fires_at_the_configured_local_time_and_not_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drives `_enqueue_due_verification_runs` through the real (unpatched)
    `VERIFICATION_TRIGGER_LOCAL_TIME`. America/Denver is UTC-6 in June:
    05:01 local on 2026-06-15 is 11:01Z (past the 05:00 trigger); 02:01
    local the same day is 08:01Z (before it). A production edit that
    reverts the trigger to '02:00' makes the second tick enqueue too --
    exactly M29 -- and this fails independently of `_margin`'s arithmetic,
    because it never calls `_margin` at all."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (_TZ, site_id))
    conn.commit()

    before_trigger_utc = datetime(2026, 6, 15, 8, 1, tzinfo=UTC)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: before_trigger_utc)
    _enqueue_due_verification_runs(conn)
    jobs_before = conn.execute(
        "SELECT payload FROM jobs WHERE type = 'verification_run' AND site_id = ?",
        (site_id,),
    ).fetchall()
    assert jobs_before == []

    after_trigger_utc = datetime(2026, 6, 15, 11, 1, tzinfo=UTC)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: after_trigger_utc)
    _enqueue_due_verification_runs(conn)
    jobs_after = conn.execute(
        "SELECT payload FROM jobs WHERE type = 'verification_run' AND site_id = ?",
        (site_id,),
    ).fetchall()
    assert len(jobs_after) == 1
    assert "2026-06-15" in str(jobs_after[0]["payload"])

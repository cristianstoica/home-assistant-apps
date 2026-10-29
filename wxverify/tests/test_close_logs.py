"""The logs and records: LG3 only (plan 2026-10-21-daily-close-close-job.md
section 15.10). LG1, LG2 and LG4-LG6 need the stage bodies and the chain
helpers of later pieces of this plan.

Written before ``wxverify/worker/close_chain.py`` exists and fails first for
that reason: the inline import of ``record_collection_try`` below raises
``ImportError``. Section 16.2 of the plan names no mutant for LG3 beyond
the try-log's own words and shape, which the assertions below pin
directly.

Real SQLite only (``tests.helpers.asof_conn``/``day_close_site``, real
``run_migrations``). Station ids are synthetic (``ISTATION01``,
``ISTATION02``), zone ``UTC``, and the date is the fixed synthetic
``tests.helpers.D``.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.helpers import D, asof_conn, day_close_site
from wxverify.core.timeutil import isoformat_utc_micro

# Every outcome and reason of 11.2 and P2 section 4.5: stored, no_readings,
# held, refused, failed (with refused_access, call_limit, backoff, provider,
# network), plus the gap-fill additions: filled, nothing_to_fill,
# not_on_day, skipped.
_CASES: tuple[tuple[str, str, int | None, str, str | None], ...] = (
    ("five_minute", "19:40", None, "stored", None),
    ("five_minute", "daytime", None, "no_readings", None),
    ("five_minute", "19:45", None, "held", "refused_access"),
    ("five_minute", "19:50", None, "refused", "refused_access"),
    ("five_minute", "19:55", None, "failed", "network"),
    ("five_minute", "20:00", None, "failed", "provider"),
    ("five_minute", "20:05", None, "failed", "backoff"),
    ("five_minute", "20:10", None, "failed", "call_limit"),
    ("hourly", "gap_fill", 1, "filled", None),
    ("hourly", "gap_fill", 1, "nothing_to_fill", None),
    ("hourly", "gap_fill", 1, "not_on_day", None),
    ("hourly", "gap_fill", 1, "skipped", None),
)


@pytest.mark.parametrize(("kind", "slot", "close_attempt", "outcome", "reason"), _CASES)
def test_lg3_every_outcome_and_reason_is_stored_as_given(
    kind: str,
    slot: str,
    close_attempt: int | None,
    outcome: str,
    reason: str | None,
) -> None:
    """LG3: each of P2's outcomes and reasons, plus the gap-fill additions,
    round-trips through ``record_collection_try`` unchanged -- the try log
    has no CHECK on ``outcome``/``reason`` (12.3's notes), so the writer
    itself must not alter the words it is given.
    """
    from wxverify.worker.close_chain import record_collection_try

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
        (site_id,),
    ).fetchone()[0]
    tried_at = isoformat_utc_micro()

    record_collection_try(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=D.isoformat(),
        kind=kind,
        slot=slot,
        close_attempt=close_attempt,
        tried_at=tried_at,
        outcome=outcome,
        reason=reason,
        detail=None,
        readings_kept=2,
        readings_refused=1,
    )

    row = conn.execute(
        "SELECT kind, slot, close_attempt, outcome, reason, readings_kept,"
        " readings_refused FROM close_try_log"
    ).fetchone()
    assert row is not None
    assert row["kind"] == kind
    assert row["slot"] == slot
    assert row["close_attempt"] == close_attempt
    assert row["outcome"] == outcome
    assert row["reason"] == reason
    assert row["readings_kept"] == 2
    assert row["readings_refused"] == 1


def test_lg3_a_gap_fill_slot_without_an_attempt_is_refused() -> None:
    """LG3: a ``gap_fill`` slot with no ``close_attempt`` breaks 12.3's
    ``CHECK((slot = 'gap_fill') = (close_attempt IS NOT NULL))`` --
    ``record_collection_try`` does one INSERT and nothing else, so the
    constraint's own refusal reaches the caller unmodified.
    """
    from wxverify.worker.close_chain import record_collection_try

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
        (site_id,),
    ).fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError):
        record_collection_try(
            conn,
            site_id=site_id,
            station_id=station_id,
            local_date=D.isoformat(),
            kind="hourly",
            slot="gap_fill",
            close_attempt=None,
            tried_at=isoformat_utc_micro(),
            outcome="filled",
            reason=None,
            detail=None,
            readings_kept=0,
            readings_refused=0,
        )

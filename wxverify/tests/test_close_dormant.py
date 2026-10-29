"""Landing dormant: DO5 only (plan 2026-10-21-daily-close-close-job.md
section 15.2). DO1-DO4 need ``enqueue_due_close_work`` and
``settle_interrupted_closes``, which land with later pieces of this plan.

Written before ``wxverify/worker/close_chain.py`` exists and fails first for
that reason: the inline import of ``record_collection_try`` below raises
``ImportError``. Section 16.2 of the plan names the mutant this test ID
must kill; that mapping is also given inline on the assertion, in the
file's own style.

Real SQLite only (``tests.helpers.asof_conn``/``day_close_site``, real
``run_migrations``). Station ids are synthetic (``ISTATION01``,
``ISTATION02``), zone ``UTC``, and the date is the fixed synthetic
``tests.helpers.D``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from tests.helpers import D, asof_conn, day_close_site
from wxverify.core.timeutil import isoformat_utc_micro


def test_do5_record_collection_try_writes_its_row_with_the_close_off() -> None:
    """DO5: with the close off, ``record_collection_try`` still inserts one
    ``close_try_log`` row carrying exactly the given values -- the writer
    does not gate on ``daily_close_on`` (11.2, 15.2): P2's evening tries
    call it unconditionally, before this release's switch is ever turned
    on.

    M6 -> at the row-count assertion below: correct = 1 (the writer inserts
    regardless of ``daily_close_on``), mutant (``record_collection_try``
    returns without writing while the close is off) = 0.
    """
    from wxverify.worker.close_chain import record_collection_try

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
        (site_id,),
    ).fetchone()[0]
    tried_at = isoformat_utc_micro(datetime(D.year, D.month, D.day, 19, 40, tzinfo=UTC))

    record_collection_try(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=D.isoformat(),
        kind="five_minute",
        slot="19:40",
        close_attempt=None,
        tried_at=tried_at,
        outcome="stored",
        reason=None,
        detail=None,
        readings_kept=3,
        readings_refused=0,
    )

    rows = conn.execute(
        "SELECT site_id, local_date, station_id, kind, slot, close_attempt,"
        " tried_at, outcome, reason, detail, readings_kept, readings_refused"
        " FROM close_try_log"
    ).fetchall()
    assert len(rows) == 1, (
        "mutant -> at this assertion: correct = 1 (the writer inserts"
        " regardless of daily_close_on), mutant (record_collection_try"
        " returns without writing while the close is off) = 0"
    )
    row = rows[0]
    assert row["site_id"] == site_id
    assert row["local_date"] == D.isoformat()
    assert row["station_id"] == station_id
    assert row["kind"] == "five_minute"
    assert row["slot"] == "19:40"
    assert row["close_attempt"] is None
    assert row["tried_at"] == tried_at
    assert row["outcome"] == "stored"
    assert row["reason"] is None
    assert row["detail"] is None
    assert row["readings_kept"] == 3
    assert row["readings_refused"] == 0

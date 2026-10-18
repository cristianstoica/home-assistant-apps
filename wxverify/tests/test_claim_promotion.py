"""Tests: C2, the chain-chunk promotion rule in ``db/queue.py`` (§5.2,
2026-10-17-morning-queue-delays.md).

T8-T16 and T22 (numbered after the C3 tests, but a C2 test -- §6). Each
test hand-inserts ``jobs`` rows with every timing column set explicitly:
the column DEFAULT reads SQLite's own clock (db/migrations.py), years
before the frozen instant below, so a defaulted stamp would sort ahead of
every hand-stamped row and silently pass without exercising the rule
(§6's T22 note).

Only ``wxverify.core.timeutil.utc_now`` is frozen (the ``frozen_now``
fixture), never ``queue``'s imported ``utc_now`` name -- that name is
never read by ``_claim``/``reclaim_all_stale`` (§3.2), so freezing it
would leave this suite's own clock-dependent assertions testing nothing.

Every site name, station id and coordinate here is synthetic (public
repo): site name "Testsite", timezone "UTC", coordinates 0.0/0.0 -- the
same placeholders ``tests/test_wind_days_lane.py`` documents.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import (
    CHAIN_PROMOTION_AGE,
    claim_next_current_obs_job,
    claim_next_job,
    reclaim_all_stale,
)
from wxverify.monitor import PENDING_OVERDUE_MINUTES, STUCK_RUNNING_MINUTES
from wxverify.worker.verification_run import DAY_CHUNK_BUDGET_SECONDS

#: Years away from the real date, on a whole second (no fractional-seconds
#: ambiguity -- isoformat_utc only omits the field at microsecond == 0).
_FROZEN = datetime(2031, 5, 5, 10, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Shared harness
# ---------------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    )
    conn.commit()
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _stamp(age: timedelta) -> str:
    """ISO-8601 UTC stamp ``age`` before the frozen instant."""
    return isoformat_utc(_FROZEN - age)


def _insert_job(
    conn: sqlite3.Connection,
    job_type: str,
    site_id: int,
    *,
    created_at: str,
    updated_at: str,
    next_attempt_at: str | None,
    status: str = "pending",
    retry_count: int = 0,
) -> int:
    """Insert a ``jobs`` row with every timing column set explicitly (no
    column DEFAULT is ever relied on -- see the module docstring)."""
    cur = conn.execute(
        """
        INSERT INTO jobs
            (type, site_id, payload, status, next_attempt_at, retry_count,
             created_at, updated_at)
        VALUES (?, ?, '{}', ?, ?, ?, ?, ?)
        """,
        (
            job_type,
            site_id,
            status,
            next_attempt_at,
            retry_count,
            created_at,
            updated_at,
        ),
    )
    conn.commit()
    job_id = cur.lastrowid
    assert job_id is not None, "INSERT must produce a rowid"
    return job_id


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> datetime:
    """Freeze ``timeutil.utc_now`` only -- never ``queue``'s ``utc_now`` name."""
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: _FROZEN)
    return _FROZEN


# =============================================================================
# T8-T10 -- the promotion age itself: sign, unit, and basis column
# =============================================================================


def test_young_chain_does_not_preempt_tier_one(frozen_now: datetime) -> None:
    """A chain row whose basis is 1s inside the plan's 5-minute promotion
    age stays tier 2 even though its ``created_at`` is far older than the
    competing ``fetch_feed`` row's. The age is a literal here, not
    ``CHAIN_PROMOTION_AGE``, so this test also catches the constant
    moved off 5 minutes (e.g. to 4 or 6).

    mutant -> at the claim-type assertion: correct = 'fetch_feed' (the
    chain is 1s too young to promote, so tier 1 claims first), mutant
    (promotion computed with the wrong sign or unit, so it fires a second
    early) = 'verification_run' (promoted, and its 1h-old created_at then
    wins the tier-1 FIFO tie-break).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    basis = _stamp(timedelta(minutes=4, seconds=59))
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=_stamp(timedelta(hours=1)),
        updated_at=basis,
        next_attempt_at=basis,
    )
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=_stamp(timedelta(minutes=10)),
        updated_at=_stamp(timedelta(minutes=10)),
        next_attempt_at=_stamp(timedelta(minutes=1)),
    )

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.type == "fetch_feed"


def test_chain_at_exactly_the_promotion_age_is_promoted(
    frozen_now: datetime,
) -> None:
    """The boundary: a basis of exactly now minus the plan's 5-minute
    promotion age is promoted (a strict ``>`` keeps tier 2 only for
    younger rows -- §5.2's edge case). The age is a literal here, not
    ``CHAIN_PROMOTION_AGE``, so this test also catches the constant
    moved off 5 minutes (e.g. to 4 or 6).

    mutant -> at the claim-type assertion: correct = 'verification_run'
    (basis == now-A is not > now-A, so the CASE falls to ELSE 1 and the
    chain's 1h-old created_at wins tier-1 FIFO), mutant ('>=' used where
    '>' is meant, or the real wall clock read instead of the frozen one --
    which is years before the frozen instant, so nothing would be
    promoted either way) = 'fetch_feed'.
    """
    conn = _make_db()
    site_id = _make_site(conn)
    basis = _stamp(timedelta(minutes=5))
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=_stamp(timedelta(hours=1)),
        updated_at=basis,
        next_attempt_at=basis,
    )
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=_stamp(timedelta(minutes=10)),
        updated_at=_stamp(timedelta(minutes=10)),
        next_attempt_at=_stamp(timedelta(minutes=1)),
    )

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.type == "verification_run"


def test_chain_promotion_ages_on_basis_not_created_at(
    frozen_now: datetime,
) -> None:
    """The chain row's ``created_at`` is 1h old (far older than the
    fetch's), but its basis (``next_attempt_at``) is only 1 min old --
    well inside ``CHAIN_PROMOTION_AGE`` -- so it must stay tier 2.

    mutant -> at the claim-type assertion: correct = 'fetch_feed' (a
    1-min-old basis keeps the chain in tier 2 regardless of its 1h-old
    created_at), mutant (promotion aged on created_at instead of basis) =
    'verification_run' (its 1h-old created_at both wrongly promotes it
    and wins the tier-1 FIFO tie-break against the fetch's newer
    created_at).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    chain_basis = _stamp(timedelta(minutes=1))
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=_stamp(timedelta(hours=1)),
        updated_at=chain_basis,
        next_attempt_at=chain_basis,
    )
    fetch_due = _stamp(timedelta(seconds=30))
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=_stamp(timedelta(minutes=2)),
        updated_at=fetch_due,
        next_attempt_at=fetch_due,
    )

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.type == "fetch_feed"


# =============================================================================
# T11 -- COALESCE(next_attempt_at, updated_at): a NULL next_attempt_at
# must age on updated_at, not bypass the age check altogether.
# =============================================================================


@pytest.mark.parametrize(
    ("updated_at_age", "expect_type"),
    [
        pytest.param(
            timedelta(minutes=6), "verification_run", id="old-updated-at-promoted"
        ),
        pytest.param(
            timedelta(minutes=1), "fetch_feed", id="young-updated-at-not-promoted"
        ),
    ],
)
def test_null_next_attempt_at_ages_on_updated_at(
    frozen_now: datetime, updated_at_age: timedelta, expect_type: str
) -> None:
    """A chain row with ``next_attempt_at IS NULL`` must fall back to
    ``updated_at`` for its age, exactly like the "old-updated-at" case
    here -- the "young-updated-at" case is the one that actually kills the
    missing-COALESCE mutant; the other is its paired positive.

    mutant -> at the claim-type assertion (young-updated-at case only):
    correct = 'fetch_feed' (updated_at 1 min old keeps the NULL-basis
    chain in tier 2), mutant (no COALESCE, so SQL's ``NULL > ?`` is NULL,
    never TRUE, and the CASE falls straight to ELSE 1 regardless of age)
    = 'verification_run' (wrongly promoted, and its 1h-old created_at
    then wins the tier-1 FIFO tie-break it should never have reached).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=_stamp(timedelta(hours=1)),
        updated_at=_stamp(updated_at_age),
        next_attempt_at=None,
    )
    fetch_due = _stamp(timedelta(seconds=30))
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=_stamp(timedelta(minutes=2)),
        updated_at=fetch_due,
        next_attempt_at=fetch_due,
    )

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.type == expect_type


# =============================================================================
# T12 -- a promoted chain never outranks tier 0.
# =============================================================================


def test_promoted_chain_never_outranks_tier_zero(frozen_now: datetime) -> None:
    """A promoted chain row (old enough, and with a FIFO-older
    ``created_at`` than the competing ``forecast_record``) must still
    lose to ``forecast_record``, which is tier 0 regardless of age.

    mutant -> at the claim-type assertion: correct = 'forecast_record'
    (tier 0 always wins), mutant (promotion misrouted into tier 0 instead
    of tier 1's ELSE branch) = 'verification_run' (its older created_at
    then wins the shared tier-0 FIFO tie-break against the record).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    chain_basis = _stamp(timedelta(minutes=10))
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=_stamp(timedelta(hours=2)),
        updated_at=chain_basis,
        next_attempt_at=chain_basis,
    )
    record_due = _stamp(timedelta(minutes=1))
    _insert_job(
        conn,
        "forecast_record",
        site_id,
        created_at=_stamp(timedelta(minutes=5)),
        updated_at=record_due,
        next_attempt_at=record_due,
    )

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.type == "forecast_record"


# =============================================================================
# T13 -- a promoted chain joins tier 1 FIFO by created_at, not a tier of
# its own and not ordered by its own basis.
# =============================================================================


def test_promoted_chain_joins_tier_one_fifo_by_created_at(
    frozen_now: datetime,
) -> None:
    """Three tier-1-eligible rows: ``pair_and_score`` (oldest created_at,
    due now, and whose own ``next_attempt_at`` is deliberately newer than
    the chain's basis), a promoted chain row (middle created_at, old
    basis), and ``fetch_feed`` (newest created_at). Correct FIFO claims
    pair, then chain, then fetch.

    mutant -> at the claim-order assertion: correct = ['pair_and_score',
    'verification_run', 'fetch_feed'] (FIFO by created_at: pair's 3h-old
    created_at, the chain's 90min-old created_at, then the fetch's 5min-
    old created_at), mutant A (promotion routed into a tier between 0 and
    1, ahead of every tier-1 row) = ['verification_run', 'pair_and_score',
    'fetch_feed'] (the chain claims first despite its younger created_at
    than pair's); mutant B (tier-1 FIFO ordered by
    COALESCE(next_attempt_at, updated_at) instead of created_at) =
    ['verification_run', 'pair_and_score', 'fetch_feed'] (the chain's
    10min-old basis sorts ahead of pair's own 2min-old next_attempt_at and
    the fetch's 1min-old next_attempt_at) -- both mutants land on the same
    wrong order, which is why one assertion kills both.
    """
    conn = _make_db()
    site_id = _make_site(conn)
    pair_due = _stamp(timedelta(minutes=2))
    _insert_job(
        conn,
        "pair_and_score",
        site_id,
        created_at=_stamp(timedelta(hours=3)),
        updated_at=pair_due,
        next_attempt_at=pair_due,
    )
    chain_basis = _stamp(timedelta(minutes=10))
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=_stamp(timedelta(minutes=90)),
        updated_at=chain_basis,
        next_attempt_at=chain_basis,
    )
    fetch_due = _stamp(timedelta(minutes=1))
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=_stamp(timedelta(minutes=5)),
        updated_at=fetch_due,
        next_attempt_at=fetch_due,
    )

    claimed = [claim_next_job(conn) for _ in range(3)]
    assert [job.type for job in claimed if job is not None] == [
        "pair_and_score",
        "verification_run",
        "fetch_feed",
    ]


# =============================================================================
# T14 -- timezone_correction shares the chain-type promotion rule.
# =============================================================================


def test_timezone_correction_gets_the_same_promotion_as_verification_run(
    frozen_now: datetime,
) -> None:
    """``timezone_correction``'s basis is exactly now minus the plan's
    5-minute promotion age (the same boundary as the verification_run
    case above), so it must promote into tier 1 and win the created_at
    tie-break against a newer fetch, exactly like ``verification_run``
    does. The age is a literal here, not ``CHAIN_PROMOTION_AGE``, so this
    test also catches the constant moved off 5 minutes (e.g. to 4 or 6).

    mutant -> at the claim-type assertion: correct =
    'timezone_correction' (promoted, and its older created_at wins tier-1
    FIFO), mutant (the age-based promotion rule wired for
    verification_run only, leaving timezone_correction stuck at tier 2
    regardless of age) = 'fetch_feed' (tier 1 claims ahead of a tz chunk
    that never promotes).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    basis = _stamp(timedelta(minutes=5))
    _insert_job(
        conn,
        "timezone_correction",
        site_id,
        created_at=_stamp(timedelta(hours=1)),
        updated_at=basis,
        next_attempt_at=basis,
    )
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=_stamp(timedelta(minutes=10)),
        updated_at=_stamp(timedelta(minutes=10)),
        next_attempt_at=_stamp(timedelta(minutes=1)),
    )

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.type == "timezone_correction"


# =============================================================================
# T15 -- the current-obs lane's own type_clause survives the splice.
# =============================================================================


def test_promoted_chain_never_claims_on_the_current_obs_lane(
    frozen_now: datetime,
) -> None:
    """A promoted chain row is still excluded from the current-obs lane --
    the extra ``promote_after`` bind must not splice over or drop the
    lane's own ``type_clause``.

    mutant -> at this assertion: correct = None (no fetch_current_obs
    rows exist, and the chain row never matches the lane's type_clause),
    mutant (``_CLAIM_ORDER_BY``'s extra ``?`` placeholder spliced so the
    lane's type_clause is lost from the WHERE) = the verification_run job
    (it becomes the only pending row an unfiltered query can return).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    old_basis = _stamp(timedelta(minutes=10))
    _insert_job(
        conn,
        "verification_run",
        site_id,
        created_at=old_basis,
        updated_at=old_basis,
        next_attempt_at=old_basis,
    )

    claimed = claim_next_current_obs_job(conn)
    assert claimed is None


# =============================================================================
# T16 -- the wait-bound arithmetic, pinned against the monitor thresholds.
# =============================================================================


def test_promotion_age_and_budget_pinned_under_monitor_thresholds() -> None:
    """§5.2's wait-bound argument (A + J under the monitor's pending-
    overdue arm, and the chunk budget under its stuck-running arm) only
    holds while these four constants keep their current relationship. No
    import exists between ``queue.py`` and ``verification_run.py`` /
    ``monitor.py`` to enforce it at the type level (§5.2's "no new
    imports between layers"), so this test is the only thing pinning it.

    mutant -> at the pending-overdue assertion: correct = True (300s +
    300s = 600s < 900s), mutant (``CHAIN_PROMOTION_AGE`` or
    ``DAY_CHUNK_BUDGET_SECONDS`` raised enough that their sum reaches or
    passes ``PENDING_OVERDUE_MINUTES * 60``) = False.
    mutant -> at the stuck-running assertion: correct = True (300s <
    1200s), mutant (``DAY_CHUNK_BUDGET_SECONDS`` raised to or past
    ``STUCK_RUNNING_MINUTES * 60``) = False.
    """
    promotion_s = CHAIN_PROMOTION_AGE.total_seconds()
    assert promotion_s + DAY_CHUNK_BUDGET_SECONDS < PENDING_OVERDUE_MINUTES * 60
    assert DAY_CHUNK_BUDGET_SECONDS < STUCK_RUNNING_MINUTES * 60


# =============================================================================
# T22 -- reclaim gives a running chain row a fresh basis (lands in C2's
# commit; numbered after the C3 tests per §6).
# =============================================================================


@pytest.mark.parametrize("chain_type", ["verification_run", "timezone_correction"])
def test_reclaim_gives_a_running_chain_row_a_fresh_basis(
    frozen_now: datetime, chain_type: str
) -> None:
    """``reclaim_all_stale`` re-pends every running row, but only a chain
    row's ``next_attempt_at`` moves to the reclaim time -- ``created_at``,
    ``retry_count``, and every other type's ``next_attempt_at`` (here
    ``fetch_current_obs``'s) stay put. The fresh basis then makes the
    chain wait behind the pending fetch, instead of being promoted at
    once on its old basis.

    mutant -> at the chain next_attempt_at assertion: correct =
    ``isoformat_utc()`` at the frozen instant, mutant (reclaim left
    unchanged, the pre-C2 shape -- :454-463 before this change; also
    killed, on the timezone_correction parametrize case only, by a
    ``_CHAIN_TYPES_SQL`` narrowed to ``verification_run`` alone) = the
    row's original now-10min next_attempt_at.
    mutant -> at the current-obs next_attempt_at assertion: correct = the
    original now-10min stamp (untouched), mutant (the CASE's type filter
    dropped so the UPDATE stamps every type) = the frozen instant's
    stamp.
    mutant -> at the claim-order assertion: correct = ['fetch_feed',
    chain_type] (the chain's brand-new fresh basis keeps it in tier 2
    behind the pending fetch), mutant (reclaim left unchanged) =
    [chain_type, 'fetch_feed'] (the chain's stale now-10min basis
    promotes it ahead of the fetch, and its older created_at wins the
    tier-1 tie-break).
    mutant -> at the rowcount assertion: correct = 2 (only the two
    running rows re-pend), mutant (the ``WHERE status = 'running'``
    predicate dropped or widened) = 3 (the already-pending fetch_feed row
    is counted too).
    mutant -> at the retry_count assertion: correct = 2 (unchanged),
    mutant (reclaim resets retry_count alongside next_attempt_at) = 0.
    """
    conn = _make_db()
    site_id = _make_site(conn)
    chain_created = _stamp(timedelta(minutes=10))
    chain_updated = _stamp(timedelta(minutes=2))
    chain_id = _insert_job(
        conn,
        chain_type,
        site_id,
        status="running",
        created_at=chain_created,
        updated_at=chain_updated,
        next_attempt_at=chain_created,
        retry_count=2,
    )
    fetch_due = _stamp(timedelta(minutes=1))
    _insert_job(
        conn,
        "fetch_feed",
        site_id,
        created_at=fetch_due,
        updated_at=fetch_due,
        next_attempt_at=fetch_due,
    )
    obs_basis = _stamp(timedelta(minutes=10))
    obs_id = _insert_job(
        conn,
        "fetch_current_obs",
        site_id,
        status="running",
        created_at=obs_basis,
        updated_at=obs_basis,
        next_attempt_at=obs_basis,
    )

    reclaimed = reclaim_all_stale(conn)
    assert reclaimed == 2

    chain_row = conn.execute(
        "SELECT created_at, next_attempt_at, retry_count FROM jobs WHERE id = ?",
        (chain_id,),
    ).fetchone()
    assert chain_row["next_attempt_at"] == isoformat_utc()
    assert chain_row["created_at"] == chain_created
    assert chain_row["retry_count"] == 2

    obs_row = conn.execute(
        "SELECT next_attempt_at FROM jobs WHERE id = ?", (obs_id,)
    ).fetchone()
    assert obs_row["next_attempt_at"] == obs_basis

    first = claim_next_job(conn)
    second = claim_next_job(conn)
    assert first is not None and second is not None
    assert [first.type, second.type] == ["fetch_feed", chain_type]

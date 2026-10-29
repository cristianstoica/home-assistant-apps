"""The logs and records: LG1 to LG6 (plan 2026-10-21-daily-close-close-job.md
section 15.10).

LG3 (unchanged from the prior batch) is written before
``wxverify/worker/close_chain.py`` exists and fails first for that reason:
the inline import of ``record_collection_try`` raises ``ImportError``.

LG1 and LG2 drive the real night chain end to end
(``tests.helpers.install_round_state_standin``, the pattern of
``tests/test_close_timeline.py``), so each reaches that call before doing
anything else -- a ``ModuleNotFoundError`` today, since
``wxverify.worker.close_p4`` does not exist. LG4 calls ``main()`` with the
not-yet-registered ``close-day`` subcommand, which fails argument parsing
with ``SystemExit(2)`` (there is no such subparser at ``a4a919c``) before
anything it builds matters. LG5 imports ``close_chain.run_close_retention``
inline, an ``ImportError`` at once (``close_chain.py`` holds only
``record_collection_try`` today). LG6 imports ``close_chain.close_status``
inline, for the same reason.

Section 16.2 of the plan names the mutant each test ID kills; that mapping
is given inline on the assertion, in ``tests/test_close_timeline.py``'s own
style.

Real SQLite only (``tests.helpers.asof_conn``/``day_close_site``/
``standard_night``, real ``run_migrations`` via ``init_db`` on a
``tmp_path`` file where a test needs the async ``Database``). Station ids
are synthetic (``ISTATION01``/``ISTATION02``), the zone is ``UTC``, and
every date is the fixed synthetic ``tests.helpers.D``/the derived round
date ``R``. No live id, coordinate, site name or real zone name appears.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    asof_conn,
    clear_close_chain_log_map,
    day_close_site,
    disable_other_enqueuers,
    install_p4_standins,
    install_round_state_standin,
    note_day_clock,
    patch_close_clock,
    run_due_jobs,
    standard_night,
)
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import claim_next_job
from wxverify.db.site_days import begin_close, fail_close, finish_close, mark_missed
from wxverify.worker.close_p4 import ScoreSummary
from wxverify.worker.day_values import GapFillResult, StationFill
from wxverify.worker.processor import run_claimed_job

R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test file,
    never across the whole suite."""
    clear_close_chain_log_map()


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


# ---------------------------------------------------------------------------
# LG1
# ---------------------------------------------------------------------------


def _stage_log_row(conn, site_id: int, local_date, stage: int):  # type: ignore[no-untyped-def]
    return conn.execute(
        "SELECT attempt, job_id, enqueued_at, started_at, ended_at, queue_wait_s,"
        " duration_s, outcome, reason, detail FROM close_stage_log"
        " WHERE site_id = ? AND local_date = ? AND stage = ?",
        (site_id, local_date.isoformat(), stage),
    ).fetchone()


def _job_id_for_key(conn, job_type: str, key_suffix: str) -> int:  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT id FROM jobs WHERE type = ? AND job_key LIKE '%' || ?",
        (job_type, key_suffix),
    ).fetchone()
    assert row is not None, f"no {job_type} job ending {key_suffix!r}"
    return int(row[0])


def test_lg1_the_stage_log_rows_match_the_jobs_that_ran(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LG1: HO1 (the standard night, ``run_due_jobs`` from R 00:40Z). One
    stage-log row per stage: ``attempt``, ``job_id`` (NULL for stage 1),
    ``enqueued_at`` and ``started_at`` copied from the job row,
    ``queue_wait_s``/``duration_s`` their differences, with ``queue_wait_s``
    0 for every stage of HO1 (the clock does not move inside
    ``run_due_jobs``, and the job stamps follow it, 15.1), ``outcome``
    ``completed``; stage 5's ``detail`` holds the stand-in's summary; stage
    7's the identifier (11.1).

    Plan section 16.2 names no mutant for LG1 directly tied to this
    HO1-shaped case: every job here is enqueued and claimed at the SAME
    frozen clock instant (``run_due_jobs`` never advances the clock on its
    own), so a stage's ``created_at`` and claim-time ``updated_at`` are
    equal under BOTH the correct code and M59 (``started_at`` taken from
    ``created_at``) -- M59 is equivalent under this fixture alone. See
    ``test_lg1b_queue_wait_uses_the_claim_time_not_the_enqueue_time`` below
    for the smallest added case that actually kills it (a plan gap: HO1's
    own "queue_wait_s 0" framing cannot discriminate started_at's source).
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        # score_day must return a ScoreSummary, not a plain dict: stage 5
        # reads summary.scored/summary.absent as attributes, and a dict
        # raises AttributeError there, failing the attempt at stage 5 and
        # stopping the chain before it ever reaches day_publish.
        standins["score_day"].result = ScoreSummary(
            scored={"temperature_high": 1}, absent={}
        )
        standins["publish"].result = f"pub-{D.isoformat()}"
        standins["clean_worksheets"].result = 0

        clock.set(BOUND)
        claims = await run_due_jobs(db, clock)
        claim_types = [t for t, _k, _o in claims]
        assert claim_types == [
            "day_close",
            "day_close",
            "day_close",
            "day_close",
            "day_close",
            "day_publish",
            "day_publish",
        ], "HO1's own claim-order precondition must hold before this test's own checks"

        conn = db._conn  # noqa: SLF001
        d_end_row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
        assert d_end_row is not None

        stage1 = _stage_log_row(conn, site_id, D, 1)
        assert stage1 is not None, "stage 1's own stage-log row must exist"
        assert stage1["job_id"] is None, "stage 1 has no job (8.1: job_id NULL)"
        assert stage1["enqueued_at"] == d_end_row["day_end_utc"], (
            "stage 1's enqueued_at must be D's own day_end_utc"
        )
        assert stage1["started_at"] == d_end_row["day_end_utc"], (
            "stage 1's started_at must be D's own day_end_utc too"
        )
        assert stage1["queue_wait_s"] == 0.0, "stage 1's queue wait is always 0 (11.1)"
        assert stage1["duration_s"] > 0, (
            "stage 1's duration is the real span from D's end to the attempt's"
            " start (11.1), not 0"
        )
        assert stage1["outcome"] == "completed"

        for stage in (2, 3, 4, 5, 6):
            job_id = _job_id_for_key(conn, "day_close", f":{stage}")
            job_row = conn.execute(
                "SELECT created_at, updated_at FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            row = _stage_log_row(conn, site_id, D, stage)
            assert row is not None, f"stage {stage}'s stage-log row must exist"
            assert row["attempt"] == 1
            assert row["job_id"] == job_id, (
                f"stage {stage}'s job_id must be the job that actually ran it"
            )
            assert row["enqueued_at"] == job_row["created_at"], (
                f"stage {stage}'s enqueued_at must be copied from the job's"
                " created_at (11.1)"
            )
            assert row["started_at"] == job_row["updated_at"], (
                f"stage {stage}'s started_at must be copied from the job's"
                " updated_at at its claim (11.1)"
            )
            assert row["queue_wait_s"] == 0.0, (
                f"stage {stage}'s queue_wait_s must be 0 (the clock never"
                " moves inside run_due_jobs, 15.1)"
            )
            assert row["outcome"] == "completed"

        for stage, key_suffix in ((7, ":7"), (8, ":8")):
            job_id = _job_id_for_key(conn, "day_publish", key_suffix)
            row = _stage_log_row(conn, site_id, D, stage)
            assert row is not None, f"stage {stage}'s stage-log row must exist"
            assert row["job_id"] == job_id
            assert row["outcome"] == "completed"

        stage5 = _stage_log_row(conn, site_id, D, 5)
        detail5 = json.loads(stage5["detail"])
        assert detail5 == {"scored": {"temperature_high": 1}, "absent": {}}, (
            "stage 5's detail must hold P4's score summary (11.1)"
        )
        stage7 = _stage_log_row(conn, site_id, D, 7)
        assert f"pub-{D.isoformat()}" in stage7["detail"], (
            "stage 7's detail must hold P4's publication identifier (11.1)"
        )

    asyncio.run(_run())


def test_lg1b_queue_wait_uses_the_claim_time_not_the_enqueue_time(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LG1's own plan gap (16.2 names no mutant for LG1 and HO1's "clock
    never moves" framing makes M59 equivalent there, see the test above):
    the smallest added case that actually distinguishes ``started_at`` =
    the job's claim-time ``updated_at`` from ``started_at`` = the job's
    ``created_at``. Stage 2's job is enqueued at the standard night's
    bound, then the clock is moved 5 minutes BEFORE it is claimed, so
    ``created_at`` and ``updated_at`` diverge.

    M59 -> at the assertion below: correct = ``started_at`` equals the
    job's claim-time ``updated_at`` (5 minutes after enqueue) and
    ``queue_wait_s`` == 300.0, mutant M59 (the stage log's ``started_at``
    taken from ``created_at``) = ``started_at`` equals the job's
    ``created_at`` instead and ``queue_wait_s`` == 0.0.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        # The clock starts at NOW, well before D's own day_end_utc/bound --
        # move it to the bound first (the pattern of TL3/TL8/HO1), or no
        # attempt is ever due and stage 2's job is never created.
        clock.set(BOUND)
        claims = await run_due_jobs(db, clock, stop_before=("day_close", 2))
        assert claims == [], "the standard night's stage 1 needs no job claim"

        conn = db._conn  # noqa: SLF001
        stage2_job_id = _job_id_for_key(conn, "day_close", ":2")
        enqueued_at = conn.execute(
            "SELECT created_at FROM jobs WHERE id = ?", (stage2_job_id,)
        ).fetchone()["created_at"]

        clock.advance(timedelta(minutes=5))
        job = await db.write(claim_next_job)
        assert job is not None and job.id == stage2_job_id
        await run_claimed_job(db, job, lane="main")

        claimed_at = conn.execute(
            "SELECT updated_at FROM jobs WHERE id = ?", (stage2_job_id,)
        ).fetchone()["updated_at"]
        assert claimed_at != enqueued_at, (
            "the clock advance between enqueue and claim must actually move"
            " the job's updated_at, or this fixture fails to discriminate"
            " anything"
        )

        row = _stage_log_row(conn, site_id, D, 2)
        assert row is not None
        assert (row["started_at"], row["queue_wait_s"]) == (claimed_at, 300.0), (
            "M59 -> at this assertion: correct = (started_at, queue_wait_s)"
            f" == ({claimed_at!r}, 300.0) (started_at copied from the job's"
            " claim-time updated_at), mutant M59 (started_at taken from"
            f" created_at) == ({enqueued_at!r}, 0.0) instead"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# LG2
# ---------------------------------------------------------------------------


def _station_fill_with_counts(  # type: ignore[no-untyped-def]
    outcome: str,
    *,
    reason: str | None = None,
    hours_left=(),
    readings_kept: int,
    readings_refused: int,
):
    """A ``StationFill`` carrying explicit ``readings_kept``/
    ``readings_refused`` (plan 11.2: piece 3 adds these two required fields
    to ``StationFill``, ``worker/day_values.py``). At ``a4a919c`` the
    dataclass does not have them yet, so this raises ``TypeError`` once
    reached -- but ``install_round_state_standin`` below raises
    ``ModuleNotFoundError`` first, before this is ever called, matching
    every other P3 test file's own "fails first" reason.
    """
    return StationFill(
        outcome=outcome,
        reason=reason,
        detail=None,
        hours_filled={} if outcome != "filled" else {"temperature": (NOW,)},
        hours_left=hours_left,
        next_allowed_at=None,
        readings_kept=readings_kept,  # type: ignore[call-arg]
        readings_refused=readings_refused,  # type: ignore[call-arg]
    )


def test_lg2_the_gap_fill_try_rows_carry_the_station_fills_own_counts(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LG2: AT9 (i) -- A ``failed``/``network``, B ``nothing_to_fill``. The
    gap-fill try-log rows: A ``failed``/``network``, ``slot`` ``gap_fill``,
    ``close_attempt`` 1, ``readings_kept``/``readings_refused`` 0 and 0; B's
    ``readings_kept`` the ``kept`` of B's store and ``readings_refused`` 0
    (11.2).

    M60 -> at the A row-exists assertion below: correct = A's try-log row
    exists with outcome ``failed``/``network`` (the gap fill is logged even
    for a failed station), mutant M60 (the gap-fill try rows not written
    for a failed fill) = no row for A at all.

    M100 -> at the B readings_kept assertion below: correct = B's
    ``readings_kept`` equals the distinctive, nonzero count this test gives
    B's ``StationFill`` (7), mutant M100 (stage 2 passes
    ``readings_kept=0``/``readings_refused=0`` regardless of the station's
    ``StationFill``) = 0 instead.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        station_a = stations["ISTATION01"]
        station_b = stations["ISTATION02"]
        gap_hour = datetime(D.year, D.month, D.day, 21, tzinfo=UTC)
        a_fill = _station_fill_with_counts(
            "failed",
            reason="network",
            hours_left=(gap_hour,),
            readings_kept=0,
            readings_refused=0,
        )
        b_fill = _station_fill_with_counts(
            "nothing_to_fill", readings_kept=7, readings_refused=0
        )
        result = GapFillResult(
            ended=True, stations={station_a: a_fill, station_b: b_fill}
        )

        async def _standin(  # type: ignore[no-untyped-def]
            db, writer, *, site_id, local_date, now_utc, skip_station_ids=frozenset()
        ):
            return result

        monkeypatch.setattr("wxverify.worker.close_chain.fill_gaps", _standin)

        # The clock starts at NOW, well before D's own day_end_utc/bound --
        # move it to the bound first (the pattern of TL3/TL8/HO1), or no
        # attempt is ever due and the gap fill is never reached.
        clock.set(BOUND)
        await run_due_jobs(db, clock)
        conn = db._conn  # noqa: SLF001

        a_row = conn.execute(
            "SELECT outcome, reason, slot, close_attempt, readings_kept,"
            " readings_refused FROM close_try_log"
            " WHERE site_id = ? AND local_date = ? AND station_id = ?"
            " AND slot = 'gap_fill'",
            (site_id, D.isoformat(), station_a),
        ).fetchone()
        assert a_row is not None, (
            "M60 -> at this assertion: correct = a try-log row for A exists"
            " (the gap fill is logged even for a station that fails), mutant"
            " M60 (the gap-fill try rows not written for a failed fill) ="
            " no row"
        )
        assert (a_row["outcome"], a_row["reason"]) == ("failed", "network")
        assert a_row["close_attempt"] == 1
        assert (a_row["readings_kept"], a_row["readings_refused"]) == (0, 0)

        b_row = conn.execute(
            "SELECT outcome, readings_kept, readings_refused FROM close_try_log"
            " WHERE site_id = ? AND local_date = ? AND station_id = ?"
            " AND slot = 'gap_fill'",
            (site_id, D.isoformat(), station_b),
        ).fetchone()
        assert b_row is not None
        assert b_row["readings_kept"] == 7, (
            "M100 -> at this assertion: correct = 7 (stage 2 passes the"
            " station's own StationFill.readings_kept through unchanged),"
            " mutant M100 (readings_kept=0/readings_refused=0 regardless of"
            " StationFill) = 0"
        )
        assert b_row["readings_refused"] == 0

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# LG4
# ---------------------------------------------------------------------------

#: 11.4's field order for the truth-version hash, per quantity row.
_TRUTH_VERSION_FIELDS: tuple[str, ...] = (
    "quantity",
    "value",
    "eligible",
    "exclusion_reason",
    "covered_hours",
    "expected_slots",
    "peak_window_ok",
    "wet_hours",
    "dry_hours",
    "rain_threshold_mm",
    "day_start_utc",
    "day_end_utc",
    "timezone",
)


def _truth_version_from_rows(conn, site_id: int, local_date) -> str:  # type: ignore[no-untyped-def]
    """Independently compute 11.4's truth-version hash from
    ``daily_truth``'s stored rows, for comparison against whatever stage 4
    saved on the attempt record. 11.4 does not spell out whether the hash
    covers one combined JSON array or a hash-per-row reduced some other
    way; this builds ONE JSON array of per-row field dicts, in ``quantity``
    order, and hashes ITS serialisation -- the plain, single reading of
    "serialised as JSON with sorted keys and no spaces" -- and is flagged
    as an interpretive choice in the hand-back message.
    """
    rows = conn.execute(
        "SELECT " + ", ".join(_TRUTH_VERSION_FIELDS) + " FROM daily_truth"  # noqa: S608
        " WHERE site_id = ? AND local_date = ? ORDER BY quantity",
        (site_id, local_date.isoformat()),
    ).fetchall()
    payload = [{field: row[field] for field in _TRUTH_VERSION_FIELDS} for row in rows]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


#: Copied locally from ``tests/test_close_day_cli.py``'s own
#: ``_CLOSE_TABLES`` (CL3) per this file's own-helpers rule.
_LG4_CLOSE_TABLES = (
    "close_attempts",
    "close_station_days",
    "close_publications",
    "close_stage_log",
    "close_try_log",
)

#: Copied locally from ``tests/test_close_day_cli.py``'s own
#: ``_V12_JOBS_DDL`` (CL3) per this file's own-helpers rule.
_LG4_V12_JOBS_DDL = """
    CREATE TABLE jobs_v12 (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
        job_key TEXT,
        payload TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK(status IN ('pending','running','completed','failed')),
        next_attempt_at TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 5,
        last_error TEXT,
        result TEXT,
        created_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        updated_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        CHECK (
            (type = 'catchup' AND site_id IS NULL)
            OR (
                type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                         'pair_and_score','backfill_site',
                         'forecast_record','record_gap_scan',
                         'verification_run','timezone_correction')
                AND site_id IS NOT NULL
            )
        )
    );
"""


def _build_v12_file(
    path: Path, *, site_name: str = "Testsite"
) -> tuple[int, dict[str, int]]:
    """Write a version-12 database to ``path`` with one site and stations A
    and B (``ISTATION01``/``ISTATION02``), returning ``(site_id,
    {pws_id: station_id})``. Copied locally from
    ``tests/test_close_day_cli.py``'s own ``_build_v12_file`` (CL3, itself
    copied from ``tests/test_close_schema.py``'s 15.3) per this file's
    own-helpers rule.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    for table in _LG4_CLOSE_TABLES:
        conn.execute(f"DROP TABLE {table}")
    conn.execute("SAVEPOINT narrow_jobs")
    try:
        conn.execute(_LG4_V12_JOBS_DDL)
        conn.execute("INSERT INTO jobs_v12 SELECT * FROM jobs")
        conn.execute("DROP TABLE jobs")
        conn.execute("ALTER TABLE jobs_v12 RENAME TO jobs")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe"
            " ON jobs(type, COALESCE(site_id, -1), job_key)"
            " WHERE status IN ('pending','running') AND job_key IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site"
            " ON jobs(type, job_key, site_id, id)"
        )
    except BaseException:
        conn.execute("ROLLBACK TO narrow_jobs")
        conn.execute("RELEASE narrow_jobs")
        raise
    conn.execute("RELEASE narrow_jobs")

    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (site_name,),
    ).lastrowid
    assert site_id is not None
    stations: dict[str, int] = {}
    for pws_id in ("ISTATION01", "ISTATION02"):
        station_id = conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
            (site_id, pws_id),
        ).lastrowid
        assert station_id is not None
        stations[pws_id] = int(station_id)
    conn.execute("PRAGMA user_version = 12")
    conn.commit()
    conn.close()
    return int(site_id), stations


def _seed_observations_full_day(path: Path, site_id: int, local_date) -> None:  # type: ignore[no-untyped-def]
    """Seed one hourly ``observations`` row per quantity's input variable
    (``temperature``, ``wind``, ``precip``, ``verification/truth.py:68``)
    for every hour of ``local_date``, a full, gap-free day stage 4 can
    judge from stored hours alone (``--hours stored``, CL3). Copied locally
    from ``tests/test_close_day_cli.py`` per this file's own-helpers rule.
    """
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON")
    for hour in range(24):
        valid_at = isoformat_utc(
            datetime(
                local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
            )
        )
        for variable, value in (
            ("temperature", 10.0 + hour * 0.1),
            ("wind", 5.0),
            ("precip", 0.0),
        ):
            conn.execute(
                "INSERT INTO observations"
                " (site_id, variable, valid_at, value, n_stations, computed_at)"
                " VALUES (?, ?, ?, ?, 2, ?)",
                (site_id, variable, valid_at, value, valid_at),
            )
    conn.commit()
    conn.close()


def test_lg4_the_truth_version_is_a_pure_function_of_the_truth_rows(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LG4: three source files as CL3's -- a version-12 file (15.12's
    preamble, built with CL3's own ``_build_v12_file``/
    ``_seed_observations_full_day``, copied locally above) holding D's
    ``observations`` (temperature, rain and wind), no raw readings, no
    site-day record -- in their own folders: the first and second
    identical, the third with D's highest hourly ``temperature`` value in
    ``observations`` raised by 1.0; ``close-day --hours stored`` run on
    each; D's ``close_attempts.truth_version`` read in each new file. The
    first equal to the SHA-256 this test computes from that file's
    ``daily_truth`` rows of D (11.4); the second equal to the first; the
    third different, its high's ``value`` 1.0 higher.

    M61 -> at the equality assertion for file 1 below: correct =
    ``truth_version`` equals this test's own hash of the same rows
    (sorted-key JSON), mutant M61 (the truth version built with unsorted
    keys) = a different hex digest for the identical row set, since
    ``dict`` insertion order need not match ``quantity`` field-name sort
    order.

    At ``a4a919c`` ``main()`` has no ``close-day`` subparser at all, so
    calling it raises ``SystemExit(2)`` from argument parsing -- the
    present-day failure, before anything this test builds matters.
    """
    from wxverify.__main__ import main  # noqa: PLC0415

    def _build_lg4_source(dir_path, *, high_bump: float = 0.0):  # type: ignore[no-untyped-def]
        """A source file as CL3's, in its own folder. ``high_bump`` raises
        D's highest hourly ``temperature`` value (hour 23, the day's peak
        under ``_seed_observations_full_day``'s ``10.0 + hour * 0.1``) by
        that amount (the third file's case)."""
        dir_path.mkdir(parents=True, exist_ok=True)
        db_path = dir_path / "wxverify.db"
        site_id, _stations = _build_v12_file(db_path)
        _seed_observations_full_day(db_path, site_id, D)
        if high_bump:
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                "UPDATE observations SET value = value + ?"
                " WHERE site_id = ? AND variable = 'temperature'"
                " AND valid_at = ?",
                (
                    high_bump,
                    site_id,
                    isoformat_utc(datetime(D.year, D.month, D.day, 23, tzinfo=UTC)),
                ),
            )
            conn.commit()
            conn.close()
        return db_path, site_id

    src1, site1 = _build_lg4_source(tmp_path / "a")
    src2, site2 = _build_lg4_source(tmp_path / "b")
    src3, site3 = _build_lg4_source(tmp_path / "c", high_bump=1.0)
    monkeypatch.setattr(config, "db_path", str(tmp_path / "other.db"))

    new_files: dict[str, object] = {}
    for label, src, site_id in (
        ("a", src1, site1),
        ("b", src2, site2),
        ("c", src3, site3),
    ):
        before = set(src.parent.iterdir())
        exit_code = main(
            [
                "--db",
                str(src),
                "close-day",
                "--site",
                str(site_id),
                "--date",
                D.isoformat(),
                "--hours",
                "stored",
            ]
        )
        assert exit_code == 0, f"close-day must exit 0 for file {label!r}"
        after = set(src.parent.iterdir())
        created = after - before
        assert len(created) == 1, "close-day must make exactly one new file"
        new_files[label] = created.pop()

    def _read_version(path) -> str:  # type: ignore[no-untyped-def]
        new_conn = sqlite3.connect(path)
        new_conn.row_factory = sqlite3.Row
        row = new_conn.execute(
            "SELECT truth_version FROM close_attempts"
            " WHERE local_date = ? ORDER BY attempt DESC LIMIT 1",
            (D.isoformat(),),
        ).fetchone()
        version = str(row["truth_version"])
        computed = _truth_version_from_rows(new_conn, 1, D)
        new_conn.close()
        return version, computed

    version_a, computed_a = _read_version(new_files["a"])
    version_b, computed_b = _read_version(new_files["b"])
    version_c, _computed_c = _read_version(new_files["c"])

    assert version_a == computed_a, (
        "M61 -> at this assertion: correct = truth_version equal to this"
        " test's own sorted-key-JSON hash of the same rows, mutant M61"
        " (the truth version built with unsorted keys) = a different digest"
    )
    assert version_b == version_a, (
        "two closes of the same hours must give the same version"
    )
    assert version_c != version_a, (
        "a day whose high is 1.0 higher must get a different truth version"
    )


# ---------------------------------------------------------------------------
# LG5
# ---------------------------------------------------------------------------


def test_lg5_the_90_day_cutoff_keeps_the_boundary_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LG5: log rows at 90 days before stage 6's clock minus 1 s, exactly,
    plus 1 s. The oldest deleted in each log, the other two kept (11.6).

    M62 -> at the close_stage_log-survivors assertion below: correct = the
    EXACT-cutoff and the plus-1s rows survive, the minus-1s row is gone,
    mutant M62 (the 90-day delete compares ``ended_at`` as text against an
    ``isoformat_utc`` cutoff) = the exact-cutoff row is ALSO deleted, since
    its stored ``isoformat_utc_micro`` stamp (with a trailing
    ``.000000Z``) sorts lexicographically BELOW the seconds-only cutoff
    string at the same instant (``.`` < ``Z``), so a raw string compare
    misclassifies an exactly-90-day-old row as older than the cutoff.

    Imports ``close_chain.run_close_retention`` inline: at ``a4a919c`` that
    raises ``ImportError`` at once (``close_chain.py`` holds only
    ``record_collection_try`` today).
    """
    from wxverify.worker.close_chain import run_close_retention  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    now = datetime(D.year, D.month, D.day, 12, tzinfo=UTC)
    cutoff = now - timedelta(days=90)
    minus1 = cutoff - timedelta(seconds=1)
    exact = cutoff
    plus1 = cutoff + timedelta(seconds=1)

    for idx, when in enumerate((minus1, exact, plus1)):
        stamp = isoformat_utc_micro(when)
        conn.execute(
            "INSERT INTO close_stage_log (site_id, local_date, attempt, stage,"
            " job_id, enqueued_at, started_at, ended_at, queue_wait_s,"
            " duration_s, outcome) VALUES (?, ?, 1, 1, NULL, ?, ?, ?, 0, 0,"
            " 'completed')",
            (site_id, (D - timedelta(days=idx)).isoformat(), stamp, stamp, stamp),
        )
        station_id = conn.execute(
            "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
            (site_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO close_try_log (site_id, local_date, station_id, kind,"
            " slot, close_attempt, tried_at, outcome, readings_kept,"
            " readings_refused) VALUES (?, ?, ?, 'five_minute', 'daytime',"
            " NULL, ?, 'stored', 0, 0)",
            (site_id, (D - timedelta(days=idx)).isoformat(), station_id, stamp),
        )
    conn.commit()

    run_close_retention(conn, now_utc=now)

    stage_log_survivors = {
        row[0]
        for row in conn.execute(
            "SELECT ended_at FROM close_stage_log WHERE site_id = ?", (site_id,)
        ).fetchall()
    }
    assert stage_log_survivors == {
        isoformat_utc_micro(exact),
        isoformat_utc_micro(plus1),
    }, (
        "M62 -> at this assertion: correct = the exact-cutoff and plus-1s"
        " rows survive, the minus-1s row is gone, mutant M62 (ended_at"
        " compared as text against an isoformat_utc cutoff) = the"
        " exact-cutoff row is wrongly deleted too"
    )
    try_log_survivors = {
        row[0]
        for row in conn.execute(
            "SELECT tried_at FROM close_try_log WHERE site_id = ?", (site_id,)
        ).fetchall()
    }
    assert try_log_survivors == {
        isoformat_utc_micro(exact),
        isoformat_utc_micro(plus1),
    }, "the same 90-day cutoff rule must hold for close_try_log.tried_at"


# ---------------------------------------------------------------------------
# LG6
# ---------------------------------------------------------------------------


def _insert_attempt_row(  # type: ignore[no-untyped-def]
    conn,
    site_id: int,
    local_date,
    attempt: int,
    *,
    last_stage: int,
    outcome: str,
    failed_stage=None,
    failure_reason=None,
    truth_version=None,
    started_at: str,
) -> None:
    ended_at = started_at if outcome != "running" else None
    conn.execute(
        "INSERT INTO close_attempts (site_id, local_date, attempt, started_at,"
        " last_stage, last_progress_at, outcome, failed_stage, failure_reason,"
        " ended_at, truth_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            attempt,
            started_at,
            last_stage,
            started_at,
            outcome,
            failed_stage,
            failure_reason,
            ended_at,
            truth_version,
        ),
    )


def test_lg6_close_status_fills_every_field_14_2_promises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LG6: ``close_status`` for a day in each state of 14.2's table:
    ``open`` before its end, ``open`` after its end, ``closing`` at stage
    3, ``retry_due``, ``closing`` attempt 2, ``closed``, ``missed`` with
    each reason (``retry_failed``, ``never_attempted``, ``retry_not_run``,
    ``zone_changeover``). Each field present or ``None`` exactly as 14.2's
    "exists in" column says; ``blocking_stage`` 1, 3, the failed stage,
    None; the publication item only on ``closed`` (14.2).

    M63 -> at the first ``closing`` assertion below: correct =
    ``blocking_stage`` equals the current attempt's own ``last_stage`` + 1
    (3), mutant M63 (``close_status.blocking_stage`` taken as
    ``last_stage`` in ``closing``) = the attempt's bare ``last_stage``
    instead (2) -- a different, discriminating literal. Caught again at
    the attempt-2 ``closing`` assertion (correct 6, mutant 5). The
    ``retry_due`` path reads the failed attempt's ``failed_stage``, never
    ``last_stage``, so M63 never reaches it.

    Imports ``close_chain.close_status`` inline: at ``a4a919c`` that raises
    ``ImportError`` at once.
    """
    from wxverify.worker.close_chain import close_status  # noqa: PLC0415

    conn = asof_conn()

    def _close_d_minus_1(target_site_id: int) -> None:
        """Close D - 1 so a later ``begin_close``/``mark_missed`` on D does
        not trip ``_has_earlier_undone`` (db/site_days.py:757, 887): each
        fresh ``day_close_site`` fixture leaves D - 1 ``open`` (D - 3 is
        closed and D - 2 is marked ``missed``, both already done -- only
        D - 1 is not)."""
        d_minus_1 = D - timedelta(days=1)
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (target_site_id, d_minus_1.isoformat()),
        ).fetchone()
        assert row is not None
        day_end_d1 = datetime.fromisoformat(row["day_end_utc"].replace("Z", "+00:00"))
        t_close = note_day_clock(day_end_d1 + timedelta(minutes=30))
        begin_close(conn, target_site_id, d_minus_1, now_utc=t_close)
        finish_close(conn, target_site_id, d_minus_1, now_utc=t_close)

    site_id = day_close_site(conn, close_on=True, with_records=True)
    _close_d_minus_1(site_id)
    d_end_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    day_end = datetime.fromisoformat(d_end_row["day_end_utc"].replace("Z", "+00:00"))
    conn.commit()

    clock_before = day_end - timedelta(hours=1)
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: clock_before)
    status_before_end = close_status(conn, site_id=site_id, local_date=D)
    assert status_before_end is not None
    assert status_before_end.state == "open"
    assert status_before_end.blocking_stage is None, (
        "an open day before its own end has no blocking_stage at all (14.2)"
    )

    clock_after = note_day_clock(day_end + timedelta(minutes=5))
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: clock_after)
    status_after_end = close_status(conn, site_id=site_id, local_date=D)
    assert status_after_end is not None
    assert status_after_end.state == "open"
    assert status_after_end.blocking_stage == 1, (
        "an open day past its own end is waiting on stage 1 (the round),"
        " blocking_stage 1 (14.2)"
    )

    begin_close(conn, site_id, D, now_utc=clock_after)
    _insert_attempt_row(
        conn,
        site_id,
        D,
        1,
        last_stage=2,
        outcome="running",
        started_at=isoformat_utc(clock_after),
    )
    conn.commit()
    status_closing = close_status(conn, site_id=site_id, local_date=D)
    assert status_closing is not None
    assert status_closing.state == "closing"
    assert status_closing.blocking_stage == 3, (
        "in closing, blocking_stage is the current attempt's last_stage + 1"
        " (14.2): last_stage 2 here must give 3. M63 -> at this assertion:"
        " correct = 3, mutant M63 = 2."
    )

    fail_time = note_day_clock(clock_after + timedelta(minutes=10))
    fail_close(conn, site_id, D, now_utc=fail_time, reason="network")
    conn.execute(
        "UPDATE close_attempts SET outcome = 'failed', failed_stage = 4,"
        " failure_reason = 'network', ended_at = ? WHERE site_id = ?"
        " AND local_date = ? AND attempt = 1",
        (isoformat_utc(fail_time), site_id, D.isoformat()),
    )
    conn.commit()
    status_retry_due = close_status(conn, site_id=site_id, local_date=D)
    assert status_retry_due is not None
    assert status_retry_due.state == "retry_due"
    assert status_retry_due.retry_at is not None
    assert status_retry_due.last_failure is not None
    assert status_retry_due.blocking_stage == 4, (
        "in retry_due, blocking_stage is the failed attempt's own"
        " failed_stage (14.2), not last_stage: failed_stage 4 here"
    )

    retry_due_at = note_day_clock(fail_time + timedelta(hours=1))
    begin_close(conn, site_id, D, now_utc=retry_due_at)
    _insert_attempt_row(
        conn,
        site_id,
        D,
        2,
        last_stage=5,
        outcome="running",
        started_at=isoformat_utc(retry_due_at),
    )
    conn.commit()
    status_closing2 = close_status(conn, site_id=site_id, local_date=D)
    assert status_closing2 is not None
    assert status_closing2.state == "closing"
    assert status_closing2.attempts == 2
    assert status_closing2.retry_at is not None, (
        "retry_at is kept once closing re-enters for attempt 2 (14.2)"
    )
    assert status_closing2.last_failure is not None, (
        "last_failure is kept once closing re-enters for attempt 2 (14.2)"
    )
    assert status_closing2.blocking_stage == 6, (
        "M63 -> at this assertion: correct = 6 (attempt 2's own last_stage 5,"
        " plus 1), mutant M63 (blocking_stage taken as last_stage in closing)"
        " = 5 instead (attempt 2's bare last_stage, deliberately one apart"
        " from attempt 1's failed_stage 4 so the two literals discriminate)"
    )

    # Free ISTATION01/ISTATION02 (station ids are globally unique) for reuse
    # by the states built below -- cascading deletes every site_days/
    # close_attempts/station row for this site; check_day_close_records
    # tolerates a deleted site (tests/helpers.py:807-808).
    conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
    conn.commit()

    # Reset to a fresh closed-day scenario on a second site, since the
    # retry_due site above cannot cleanly reach `closed` without P3's own
    # stage bodies to run the retry.
    site_closed = day_close_site(
        conn, close_on=True, with_records=True, station_ids=("ISTATION03", "ISTATION04")
    )
    _close_d_minus_1(site_closed)
    d_end_row2 = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_closed, D.isoformat()),
    ).fetchone()
    day_end2 = datetime.fromisoformat(d_end_row2["day_end_utc"].replace("Z", "+00:00"))
    close_time = note_day_clock(day_end2 + timedelta(minutes=20))
    begin_close(conn, site_closed, D, now_utc=close_time)
    finish_close(conn, site_closed, D, now_utc=close_time)
    _insert_attempt_row(
        conn,
        site_closed,
        D,
        1,
        last_stage=6,
        outcome="closed",
        truth_version="deadbeef",
        started_at=isoformat_utc(close_time),
    )
    conn.execute(
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at, started_at, finished_at, publication_id) VALUES"
        " (?, ?, 1, 'published', ?, ?, ?, 'pub-x')",
        (
            site_closed,
            D.isoformat(),
            isoformat_utc(close_time),
            isoformat_utc(close_time),
            isoformat_utc(close_time),
        ),
    )
    conn.commit()
    status_closed = close_status(conn, site_id=site_closed, local_date=D)
    assert status_closed is not None
    assert status_closed.state == "closed"
    assert status_closed.blocking_stage is None, (
        "a closed day has no blocking_stage (14.2)"
    )
    assert status_closed.publication is not None, (
        "the publication item is present only once the day is closed (14.2)"
    )

    # Free ISTATION03/ISTATION04 for reuse by the never_attempted case below.
    conn.execute("DELETE FROM sites WHERE id = ?", (site_closed,))
    conn.commit()

    # A missed day, never_attempted: open, past its own end, no attempt ever
    # made, on a fourth site.
    site_never_attempted = day_close_site(
        conn, close_on=True, with_records=True, station_ids=("ISTATION03", "ISTATION04")
    )
    _close_d_minus_1(site_never_attempted)
    na_end_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_never_attempted, D.isoformat()),
    ).fetchone()
    na_day_end = datetime.fromisoformat(
        na_end_row["day_end_utc"].replace("Z", "+00:00")
    )
    na_now = note_day_clock(na_day_end + timedelta(minutes=5))
    mark_missed(conn, site_never_attempted, D, now_utc=na_now, reason="never_attempted")
    conn.commit()
    status_never_attempted = close_status(
        conn, site_id=site_never_attempted, local_date=D
    )
    assert status_never_attempted is not None
    assert status_never_attempted.state == "missed"
    assert status_never_attempted.missed_reason == "never_attempted"
    assert status_never_attempted.attempts == 0, (
        "never_attempted means no close attempt was ever made (14.2)"
    )
    assert status_never_attempted.done_at is not None
    assert status_never_attempted.retry_at is None, (
        "never_attempted is not among the states retry_at exists in (14.2)"
    )
    assert status_never_attempted.last_failure is None, (
        "never_attempted is not among the states last_failure exists in (14.2)"
    )
    assert status_never_attempted.blocking_stage is None, (
        "never_attempted falls to the derivation rule's 'none of the above'"
        " case, so blocking_stage is None (14.2)"
    )

    # Free ISTATION03/ISTATION04 again for reuse by the zone_changeover case.
    conn.execute("DELETE FROM sites WHERE id = ?", (site_never_attempted,))
    conn.commit()

    # A missed day, zone_changeover: open, flagged a changeover day, past
    # its own end, on a fifth site.
    site_changeover = day_close_site(
        conn, close_on=True, with_records=True, station_ids=("ISTATION03", "ISTATION04")
    )
    _close_d_minus_1(site_changeover)
    conn.execute(
        "UPDATE site_days SET zone_changeover = 1 WHERE site_id = ? AND local_date = ?",
        (site_changeover, D.isoformat()),
    )
    co_end_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_changeover, D.isoformat()),
    ).fetchone()
    co_day_end = datetime.fromisoformat(
        co_end_row["day_end_utc"].replace("Z", "+00:00")
    )
    co_now = note_day_clock(co_day_end + timedelta(minutes=5))
    mark_missed(conn, site_changeover, D, now_utc=co_now, reason="zone_changeover")
    conn.commit()
    status_changeover = close_status(conn, site_id=site_changeover, local_date=D)
    assert status_changeover is not None
    assert status_changeover.state == "missed"
    assert status_changeover.missed_reason == "zone_changeover"
    assert status_changeover.attempts == 0, (
        "zone_changeover means no close attempt was ever made (14.2)"
    )
    assert status_changeover.done_at is not None
    assert status_changeover.retry_at is None, (
        "zone_changeover is not among the states retry_at exists in (14.2)"
    )
    assert status_changeover.last_failure is None, (
        "zone_changeover is not among the states last_failure exists in (14.2)"
    )
    assert status_changeover.blocking_stage is None, (
        "zone_changeover falls to the derivation rule's 'none of the above'"
        " case, so blocking_stage is None (14.2)"
    )

    # Free ISTATION03/ISTATION04 one more time for reuse by the
    # retry_not_run case below.
    conn.execute("DELETE FROM sites WHERE id = ?", (site_changeover,))
    conn.commit()

    # A missed day, retry_failed, on a third site.
    site_missed = day_close_site(
        conn, close_on=True, with_records=True, station_ids=("ISTATION05", "ISTATION06")
    )
    _close_d_minus_1(site_missed)
    d_end_row3 = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_missed, D.isoformat()),
    ).fetchone()
    day_end3 = datetime.fromisoformat(d_end_row3["day_end_utc"].replace("Z", "+00:00"))
    t1 = note_day_clock(day_end3 + timedelta(minutes=5))
    t2 = note_day_clock(t1 + timedelta(hours=1, minutes=5))
    begin_close(conn, site_missed, D, now_utc=t1)
    fail_close(conn, site_missed, D, now_utc=t1, reason="network")
    _insert_attempt_row(
        conn,
        site_missed,
        D,
        1,
        last_stage=2,
        outcome="failed",
        failed_stage=3,
        failure_reason="network",
        started_at=isoformat_utc(t1),
    )
    begin_close(conn, site_missed, D, now_utc=t2)
    fail_close(conn, site_missed, D, now_utc=t2, reason="network")
    _insert_attempt_row(
        conn,
        site_missed,
        D,
        2,
        last_stage=4,
        outcome="failed",
        failed_stage=5,
        failure_reason="network",
        started_at=isoformat_utc(t2),
    )
    conn.commit()
    status_missed = close_status(conn, site_id=site_missed, local_date=D)
    assert status_missed is not None
    assert status_missed.state == "missed"
    assert status_missed.missed_reason == "retry_failed"
    assert status_missed.done_at is not None
    assert status_missed.blocking_stage == 5, (
        "in missed/retry_failed, blocking_stage is the failed attempt's own"
        " failed_stage -- attempt 2's (5), not attempt 1's (3)"
    )

    # Free ISTATION05/ISTATION06 for reuse by the retry_not_run case below.
    conn.execute("DELETE FROM sites WHERE id = ?", (site_missed,))
    conn.commit()

    # A missed day, retry_not_run: retry_due, its retry_at come and gone
    # with no second attempt made, on a sixth site.
    site_retry_not_run = day_close_site(
        conn, close_on=True, with_records=True, station_ids=("ISTATION05", "ISTATION06")
    )
    _close_d_minus_1(site_retry_not_run)
    rnr_end_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_retry_not_run, D.isoformat()),
    ).fetchone()
    rnr_day_end = datetime.fromisoformat(
        rnr_end_row["day_end_utc"].replace("Z", "+00:00")
    )
    rnr_t1 = note_day_clock(rnr_day_end + timedelta(minutes=5))
    begin_close(conn, site_retry_not_run, D, now_utc=rnr_t1)
    fail_close(conn, site_retry_not_run, D, now_utc=rnr_t1, reason="network")
    _insert_attempt_row(
        conn,
        site_retry_not_run,
        D,
        1,
        last_stage=2,
        outcome="failed",
        failed_stage=3,
        failure_reason="network",
        started_at=isoformat_utc(rnr_t1),
    )
    rnr_t2 = note_day_clock(rnr_t1 + timedelta(hours=1, minutes=5))
    mark_missed(conn, site_retry_not_run, D, now_utc=rnr_t2, reason="retry_not_run")
    conn.commit()
    status_retry_not_run = close_status(conn, site_id=site_retry_not_run, local_date=D)
    assert status_retry_not_run is not None
    assert status_retry_not_run.state == "missed"
    assert status_retry_not_run.missed_reason == "retry_not_run"
    assert status_retry_not_run.attempts == 1, (
        "retry_not_run follows exactly one failed attempt (14.2)"
    )
    assert status_retry_not_run.done_at is not None
    assert status_retry_not_run.retry_at is not None, (
        "retry_at is kept in missed/retry_not_run (14.2)"
    )
    assert status_retry_not_run.last_failure is not None, (
        "retry_not_run keeps attempt 1's failure reason -- mark_missed"
        " leaves last_failure as it was (14.2, db/site_days.py:903-910)"
    )
    assert status_retry_not_run.blocking_stage is None, (
        "retry_not_run falls to the derivation rule's 'none of the above'"
        " case, so blocking_stage is None (14.2)"
    )

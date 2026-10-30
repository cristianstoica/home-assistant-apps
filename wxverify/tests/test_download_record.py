"""Tests for the download record and the scored download (P4 plan
§18.4) -- ``plans/2026-10-22-daily-close-pairs-and-publication.md``.

DL1-DL17, one test per row. Every assertion that kills a mutant named
in the row's last column (plan §19.2) carries a message of the form
``"M<n> -> at this assertion: correct = <X>, mutant = <Y>"``.

``wxverify.scoring.downloads`` (``scored_download``, ``scored_version``,
``ScoredDownload``) does not exist before piece 2 (plan §20 Step 4), and
the ``forecast_downloads``/``forecast_value_changes`` tables do not
exist before piece 1 -- this whole file fails at collection with
``ModuleNotFoundError`` until then, which the plan's Step 3 accepts.

Two constructions are used, by row:
- DL1, DL2, DL9 (round part), DL16, DL17 drive a real download through
  the job pipeline (``tick``/``run_due_jobs``/``claim_next_job``), since
  these rows test what the WRITE path (``_persist_fetch_success``/
  ``_fetch_feed``) records.
- DL3-DL8, DL9 (catch-up part), DL10-DL15 seed ``forecast_downloads``/
  ``forecast_samples`` directly by plain INSERT (plan §18.1's "seed
  helpers by plain INSERT") and call ``scored_download``/
  ``scored_version`` directly, since these rows test the READ/inference
  logic over already-stored data, not how a download row gets written.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    STANDARD_NIGHT_R,
    D,
    MovingClock,
    ProviderConnectError,
    ProviderEmpty,
    ProviderHttpError,
    ProviderReply,
    disable_other_enqueuers,
    install_provider_standin,
    meteoblue_member_sample,
    meteoblue_package_feed_id,
    patch_close_clock,
    round_schedule_sample,
    seed_round_schedule_feeds,
    standard_night,
    subscribe_meteoblue_package,
    tick,
)
from wxverify.collection.budget import set_source_cap
from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.core.timeutil import is_canonical_utc_stamp, isoformat_utc
from wxverify.db.queue import claim_next_job, complete, enqueue_if_absent
from wxverify.db.runtime_state import set_runtime_state
from wxverify.feeds.seam import FetchResult
from wxverify.provider_ops import set_site_subscription
from wxverify.scoring.downloads import (  # P4 piece 2 (plan §4.2, §4.3, §4.7)
    ScoredDownload,
    scored_download,
    scored_version,
)
from wxverify.worker.processor import run_claimed_job

R = STANDARD_NIGHT_R  # 2024-06-11 (plan §15.1's standard night: R = D + 1)
S = R  # the generic "day S" most rows are written against


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    subscribe: tuple[str, ...] = ("X",),
    clock_start: datetime | None = None,
):
    """P3's standard night plus P4's round-schedule feeds, the provider
    stand-in and the synthetic-source domain-backoff patch installed, and
    a :class:`MovingClock` patched over every close-chain module. Returns
    ``(db, conn, site_id, feed_ids, standin, clock)``.
    """
    db, _writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001 -- tests read the real writer conn directly
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=subscribe)
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    start = clock_start or datetime(D.year, D.month, D.day, 17, 0, tzinfo=UTC)
    clock = MovingClock(start)
    patch_close_clock(monkeypatch, clock)
    return db, conn, site_id, feed_ids, standin, clock


def _downloads(conn: sqlite3.Connection, feed_id: int) -> list[sqlite3.Row]:
    """Every ``forecast_downloads`` row of ``feed_id``, in id order (4.2)."""
    return list(
        conn.execute(
            "SELECT * FROM forecast_downloads WHERE feed_id=? ORDER BY id",
            (feed_id,),
        )
    )


def _stamp(dt: datetime) -> str:
    return isoformat_utc(dt)


def _seed_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    slot: str,
    started_at: str,
    ended_at: str,
    fetched_at: str | None,
    outcome: str,
    reason: str | None = None,
    usable: int = 0,
    inserted: int = 0,
    labels: dict[str, list[str]] | None = None,
    round_date: date | None = None,
    round_try: int | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO forecast_downloads"
        " (site_id, feed_id, local_date, slot, round_date, round_try,"
        "  started_at, ended_at, fetched_at, outcome, reason, usable,"
        "  inserted, labels)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            feed_id,
            local_date.isoformat(),
            slot,
            round_date.isoformat() if round_date else None,
            round_try,
            started_at,
            ended_at,
            fetched_at,
            outcome,
            reason,
            usable,
            inserted,
            json.dumps(labels or {}),
        ),
    )
    return int(cur.lastrowid or 0)


def _seed_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
    fetched_at: str | None,
    source_raw: str = "seeded:raw",
) -> None:
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  value, source_raw, model_run_id, fetched_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            value,
            source_raw,
            issued_at,
            fetched_at,
        ),
    )


def _seed_job(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    job_key: str,
    updated_at: str,
    result: str | None,
) -> None:
    conn.execute(
        "INSERT INTO jobs"
        " (type, site_id, job_key, payload, status, result, created_at,"
        "  updated_at)"
        " VALUES ('fetch_feed', ?, ?, '{}', 'completed', ?, ?, ?)",
        (site_id, job_key, result, updated_at, updated_at),
    )


# ---------------------------------------------------------------------------
# DL1-DL2, DL9 (round part), DL16-DL17: the real write path
# ---------------------------------------------------------------------------


def test_dl1_download_row_and_package_member_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL1: a stored download with labels L1 (X) and, for P, L2 (P1) and
    L3 (P2); X's reply also carries a sample of label L0 only, at a lead
    below 1 hour (not usable)."""
    db, conn, site_id, feed_ids, standin, clock = _setup(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    subscribe_meteoblue_package(conn, site_id)
    conn.commit()

    x_model = "synthetic-round-x"
    l1 = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    l0 = _stamp(datetime(R.year, R.month, R.day, 0, 1, tzinfo=UTC))
    valid_l1 = _stamp(
        datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC) + timedelta(hours=6)
    )
    valid_l0 = _stamp(datetime(R.year, R.month, R.day, 0, 1, tzinfo=UTC))
    standin.program(
        x_model,
        ProviderReply(
            samples=(
                round_schedule_sample(x_model, "temperature", l1, valid_l1, 6, 10.0),
                round_schedule_sample(x_model, "temperature", l0, valid_l0, 0, 5.0),
            )
        ),
    )
    p1_label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    p2_label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    p1_valid = _stamp(
        datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC) + timedelta(hours=6)
    )
    standin.program(
        "meteoblue",
        ProviderReply(
            samples=(
                meteoblue_member_sample(
                    "dl1-member-a", "temperature", p1_label, p1_valid, 6, 11.0
                ),
                meteoblue_member_sample(
                    "dl1-member-b", "temperature", p2_label, p1_valid, 6, 12.0
                ),
            )
        ),
    )

    package_id = meteoblue_package_feed_id(conn)
    for round_feed_id in (feed_ids["X"], package_id):
        enqueue_if_absent(
            conn,
            "fetch_feed",
            site_id,
            f"round:{round_feed_id}",
            {
                "feed_id": round_feed_id,
                "slot": "round",
                "round_date": R.isoformat(),
                "round_try": 1,
            },
        )
    conn.commit()
    for _ in range(2):
        job = claim_next_job(conn)
        assert job is not None, "expected a round job for X and the package"
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    x_rows = _downloads(conn, feed_ids["X"])
    assert len(x_rows) == 1, (
        f"M17 -> at this assertion: correct = 1, mutant = {len(x_rows)}"
    )
    x_labels = json.loads(x_rows[0]["labels"])
    expected_x = {str(feed_ids["X"]): [l1]}
    assert x_labels == expected_x, (
        f"M17 -> at this assertion: correct = {expected_x!r}, mutant = {x_labels!r}"
    )
    assert x_rows[0]["usable"] == 1, "X's download row is missing its usable count"
    assert x_rows[0]["inserted"] == 1, "X's download row is missing its inserted count"

    p_rows = _downloads(conn, package_id)
    assert len(p_rows) == 1, "no download row was recorded for the meteoblue package"
    p1_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteoblue' AND model='dl1-member-a'"
        ).fetchone()["id"]
    )
    p2_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteoblue' AND model='dl1-member-b'"
        ).fetchone()["id"]
    )
    p_labels = json.loads(p_rows[0]["labels"])
    expected_p = {str(p1_id): [p1_label], str(p2_id): [p2_label]}
    assert p_labels == expected_p, (
        f"M18 -> at this assertion: correct = {expected_p!r}, mutant = {p_labels!r}"
    )


def test_dl2_each_failure_outcome_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL2: each failure outcome -- 404, 500, ConnectError, BudgetExhausted,
    BackoffActive, ineligible, unavailable, empty reply."""
    import wxverify.worker.domain_backoff as domain_backoff
    import wxverify.worker.processor as processor_module

    db, conn, site_id, feed_ids, standin, clock = _setup(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    source = "synthetic-round-x"
    feed_id = feed_ids["X"]

    def _finish_leftover() -> None:
        # A failed try re-pends its job (status back to 'pending', with a
        # retry's future next_attempt_at) -- enqueue_if_absent's dedupe
        # query only checks status, not next_attempt_at, so the prior
        # case's re-pended job would dedupe this case's enqueue if left in
        # place. Finish it through the queue's own complete() first.
        leftover = conn.execute(
            "SELECT id FROM jobs WHERE type='fetch_feed' AND job_key=?"
            " AND site_id IS ? AND status IN ('pending','running')",
            (f"fetch:{feed_id}", site_id),
        ).fetchone()
        if leftover is not None:
            complete(conn, int(leftover["id"]))
            conn.commit()

    def _run_one() -> sqlite3.Row:
        _finish_leftover()
        result = enqueue_if_absent(
            conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
        )
        assert result.created, "dedupe hit -- a leftover job from the prior case"
        job = claim_next_job(conn)
        assert job is not None, "no fetch_feed job was claimable"
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()
        rows = _downloads(conn, feed_id)
        return rows[-1]

    # 1. 404 -> failed.
    standin.program(source, ProviderHttpError(404))
    row = _run_one()
    assert row["outcome"] == "failed", (
        f"M19 -> at this assertion: correct = 'failed', mutant = {row['outcome']!r}"
    )
    assert row["fetched_at"] is None, (
        f"M19 -> at this assertion: correct = None, mutant = {row['fetched_at']!r}"
    )

    # 2. 500 -> backoff (sets the domain's backoff for the next case).
    standin.program(source, ProviderHttpError(500))
    row = _run_one()
    assert row["outcome"] == "backoff", (
        f"M19 -> at this assertion: correct = 'backoff', mutant = {row['outcome']!r}"
    )
    assert row["fetched_at"] is None, "backoff must leave fetched_at unset"

    # 3. BackoffActive -> backoff, no adapter call (the 500's backoff still
    # active; nothing programmed for this case, since no call is made).
    calls_before = standin.calls.get(source, 0)
    row = _run_one()
    assert row["outcome"] == "backoff", (
        f"M19 -> at this assertion: correct = 'backoff', mutant = {row['outcome']!r}"
    )
    assert standin.calls.get(source, 0) == calls_before, (
        "BackoffActive must make no adapter call"
    )
    domain_backoff.clear_domain_backoff(conn, domain_backoff.SOURCE_DOMAINS[source])
    conn.commit()

    # 4. ConnectError -> failed.
    standin.program(source, ProviderConnectError())
    row = _run_one()
    assert row["outcome"] == "failed", (
        f"M19 -> at this assertion: correct = 'failed', mutant = {row['outcome']!r}"
    )
    assert row["fetched_at"] is None, "ConnectError must leave fetched_at unset"

    # 5. BudgetExhausted -> budget.
    set_source_cap(conn, source, daily_call_limit=0)
    conn.commit()
    row = _run_one()
    assert row["outcome"] == "budget", (
        f"M19 -> at this assertion: correct = 'budget', mutant = {row['outcome']!r}"
    )
    assert row["fetched_at"] is None, "BudgetExhausted must leave fetched_at unset"
    set_source_cap(conn, source, daily_call_limit=10000)
    conn.commit()

    # 6. ineligible -> site_feed_state disabled after the job was enqueued.
    _finish_leftover()
    result = enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    assert result.created, "dedupe hit -- a leftover job from the prior case"
    conn.execute(
        "UPDATE site_feed_state SET enabled=0 WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    )
    conn.commit()
    job = claim_next_job(conn)
    assert job is not None, "no fetch_feed job was claimable after disabling the feed"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()
    row = _downloads(conn, feed_id)[-1]
    assert row["outcome"] == "ineligible", (
        f"M19 -> at this assertion: correct = 'ineligible', mutant = {row['outcome']!r}"
    )
    conn.execute(
        "UPDATE site_feed_state SET enabled=1 WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    )
    conn.commit()

    # 7. unavailable -> the adapter builder itself fails.
    def _raising_builder(src: str, client: object) -> object:
        raise RuntimeError("synthetic adapter-build failure")

    monkeypatch.setattr(processor_module, "build_adapter", _raising_builder)
    row = _run_one()
    assert row["outcome"] == "unavailable", (
        f"M19 -> at this assertion: correct = 'unavailable',"
        f" mutant = {row['outcome']!r}"
    )
    assert row["fetched_at"] is None, "unavailable must leave fetched_at unset"
    monkeypatch.setattr(processor_module, "build_adapter", standin.build)

    # 8. empty reply -> no_usable, with fetched_at set (the one failure
    # outcome that still stamps a receipt time, 4.2).
    standin.program(source, ProviderEmpty())
    row = _run_one()
    assert row["outcome"] == "no_usable", (
        f"M19 -> at this assertion: correct = 'no_usable', mutant = {row['outcome']!r}"
    )
    assert row["fetched_at"] is not None, (
        f"M19 -> at this assertion: correct = a stamp, mutant = {row['fetched_at']!r}"
    )


def test_dl9_catchup_writes_no_download_row_and_the_round_is_scored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL9: a catch-up download (historical, ` previous_day1` rows) at
    00:05 on S, then the round at 00:10."""
    db, conn, site_id, feed_ids, standin, clock = _setup(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    feed_id = feed_ids["X"]
    catchup_fetched_at = _stamp(datetime(R.year, R.month, R.day, 0, 5, tzinfo=UTC))
    catchup_issued_at = _stamp(
        datetime(R.year, R.month, R.day, 0, 5, tzinfo=UTC) - timedelta(days=1)
    )
    catchup_valid_at = _stamp(datetime(R.year, R.month, R.day, 0, 5, tzinfo=UTC))
    from wxverify.feeds.seam import NormalizedSample

    catchup_sample = NormalizedSample(
        model="synthetic-round-x",
        variable="temperature",
        issued_at=catchup_issued_at,
        valid_at=catchup_valid_at,
        lead_hours=6,
        value=9.0,
        source_raw="synthetic-round-x:raw previous_day1",
        model_run_id=catchup_issued_at,
    )
    persist_fetch_result(
        conn,
        site_id=site_id,
        source="synthetic-round-x",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[catchup_sample]),
        fetched_at=catchup_fetched_at,
        advance_last_run_at=False,
    )
    conn.commit()
    before = _downloads(conn, feed_id)
    assert before == [], (
        f"M27 -> at this assertion: correct = 0 rows, mutant = {len(before)} row(s)"
    )

    round_label = _stamp(datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC))
    valid_at = _stamp(
        datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC) + timedelta(hours=6)
    )
    standin.program(
        "synthetic-round-x",
        ProviderReply(
            samples=(
                round_schedule_sample(
                    "synthetic-round-x", "temperature", round_label, valid_at, 6, 10.0
                ),
            )
        ),
    )
    enqueue_if_absent(
        conn,
        "fetch_feed",
        site_id,
        f"round:{feed_id}",
        {
            "feed_id": feed_id,
            "slot": "round",
            "round_date": R.isoformat(),
            "round_try": 1,
        },
    )
    conn.commit()
    # The round's try is dated by its END (plan 4.2): advance the clock to
    # the round's own time (R 00:10Z) before running it, the way a real
    # round would run -- left at the setup's D 17:00Z, the row would date
    # to D instead of R and scored_download would find nothing on S.
    clock.set(datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC))
    job = claim_next_job(conn)
    assert job is not None, "no round job was claimable for X"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()

    after = _downloads(conn, feed_id)
    assert len(after) == 1, (
        f"M27 -> at this assertion: correct = 1 (the round only), mutant = {len(after)}"
    )
    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result is not None, "scored_download found no download for X on S"
    assert result.fetched_at == after[0]["fetched_at"], (
        f"M27 -> at this assertion: correct = {after[0]['fetched_at']!r}"
        f" (the round), mutant = {result.fetched_at!r}"
    )


def test_dl16_payload_with_no_slot_runs_unscheduled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL16: a fetch_feed job for X with payload {"feed_id"} only, as
    queued before the upgrade, run by run_due_jobs."""
    db, conn, site_id, feed_ids, standin, clock = _setup(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    feed_id = feed_ids["X"]
    standin.program(
        "synthetic-round-x",
        ProviderReply(
            samples=(
                round_schedule_sample(
                    "synthetic-round-x",
                    "temperature",
                    _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC)),
                    _stamp(datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)),
                    6,
                    10.0,
                ),
            )
        ),
    )
    enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    conn.commit()
    job = claim_next_job(conn)
    assert job is not None, "no fetch_feed job was claimable for the no-slot payload"
    assert job.status != "completed", "the job was already completed before being run"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()
    completed = conn.execute(
        "SELECT status, result FROM jobs WHERE id=?", (job.id,)
    ).fetchone()
    assert completed["status"] == "completed" and completed["result"] == "ok", (
        f"M129 -> at this assertion: correct = completed/ok, mutant ="
        f" status={completed['status']!r} result={completed['result']!r}"
    )
    rows = _downloads(conn, feed_id)
    assert len(rows) == 1, (
        f"M129 -> at this assertion: correct = 1, mutant = {len(rows)}"
    )
    assert rows[0]["slot"] == "unscheduled", (
        f"M129 -> at this assertion: correct = 'unscheduled',"
        f" mutant = {rows[0]['slot']!r}"
    )
    assert rows[0]["outcome"] == "stored", (
        f"expected outcome 'stored', got {rows[0]['outcome']!r}"
    )
    assert rows[0]["round_date"] is None and rows[0]["round_try"] is None, (
        f"M129 -> at this assertion: correct = (None, None), mutant ="
        f" ({rows[0]['round_date']!r}, {rows[0]['round_try']!r})"
    )


_DL17_SCORING_UTC_NOW_MODULES: tuple[str, ...] = (
    "wxverify.scoring.consensus",
    "wxverify.scoring.split",
    "wxverify.verification.truth",
    "wxverify.scoring.pairing",
    "wxverify.scoring.persistence",
    "wxverify.scoring.multimodel",
    "wxverify.provider_ops",
)


def _patch_dl17_clock(monkeypatch: pytest.MonkeyPatch, clock: MovingClock) -> None:
    """``wxverify.worker.wind_days`` imports ``utc_now`` BY NAME (:56) and
    is not in ``_CLOSE_CLOCK_MODULES`` (helpers.py:874) -- without this
    patch, DL17's minute-by-minute tick loop eventually runs the
    scheduler-enqueued ``fetch_obs``/``wind-days`` job (``scheduler.py``'s
    ``_enqueue_due_wind_days``), whose handler ``run_wind_days`` calls
    ``_start_job`` (wind_days.py:653), which reads ``utc_now()`` directly
    from this unpatched module instead of taking a ``now`` argument from
    its caller. That real clock then reaches ``_lane_today``'s
    ``ensure_site_days(conn, site_id, now_utc=now)`` (:490) and creates a
    site_days record dated by today's real date, failing the close
    teardown's ``check_day_close_records`` for a fixture reason, not a
    DL17 one. Found by grepping ``ensure_site_days(`` callers and the
    by-name ``utc_now`` importers in ``wind_days.py``; every OTHER
    ``utc_now()`` call on this path (``scheduler.py``'s
    ``_enqueue_due_wind_days``, ``_lane_today``'s own ``now`` parameter)
    is already patched via a module in ``_CLOSE_CLOCK_MODULES``.

    DL17's tick loop also runs scoring jobs, and ``wxverify.scoring.consensus``,
    ``wxverify.scoring.split``, ``wxverify.verification.truth``, and, through
    ``pair_and_score``, ``wxverify.scoring.pairing``,
    ``wxverify.scoring.persistence`` and ``wxverify.scoring.multimodel`` each
    import ``utc_now`` BY NAME and reach site_days' day-record writers
    (``ensure_site_days``/``filter_day_writes``/``check_date_write``/
    ``check_day_write``), or pass ``now_utc=utc_now()`` into them -- the same
    leak fixed for TR4 in ``test_close_triggers.py``'s
    ``_patch_tr4_close_clock``. Without these extra patches, those modules'
    real clock reaches the same day-record writers and fails the close
    teardown for a fixture reason, not a DL17 one.

    ``wxverify.provider_ops`` also imports ``utc_now`` BY NAME: enabling a
    subscription (``set_site_subscription`` -> ``rebuild_mean_for_site`` ->
    ``_invalidate_mean_for_site``) calls ``writable_pairs_clause(conn,
    site_id, now_utc=utc_now())`` (provider_ops.py:885), which with the
    close on reaches site_days' ``_ensure`` and creates a day record dated
    by today's real date -- the same fixture-reason teardown failure.
    """
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", clock.now, raising=False)
    for module in _DL17_SCORING_UTC_NOW_MODULES:
        monkeypatch.setattr(f"{module}.utc_now", clock.now, raising=False)


def test_dl17_enable_time_fetch_runs_unscheduled_and_covers_the_next_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL17: X unsubscribed; at S 12:20Z set_site_subscription enables it,
    which queues its fetch through enqueue_fetch_for_feed; the job runs;
    ticks every minute to S 18:09Z."""
    db, conn, site_id, feed_ids, standin, clock = _setup(
        tmp_path,
        monkeypatch,
        subscribe=(),
        clock_start=datetime(R.year, R.month, R.day, 12, 20, tzinfo=UTC),
    )
    _patch_dl17_clock(monkeypatch, clock)
    feed_id = feed_ids["X"]
    standin.program(
        "synthetic-round-x",
        ProviderReply(
            samples=(
                round_schedule_sample(
                    "synthetic-round-x",
                    "temperature",
                    _stamp(datetime(R.year, R.month, R.day, 12, 20, tzinfo=UTC)),
                    _stamp(datetime(R.year, R.month, R.day, 18, 20, tzinfo=UTC)),
                    6,
                    10.0,
                ),
            )
        ),
    )

    def _enable(c: sqlite3.Connection) -> None:
        set_site_subscription(c, site_id, feed_id, enabled=True, enqueue_on_enable=True)

    asyncio.run(db.write(_enable))
    conn.commit()
    job = claim_next_job(conn)
    assert job is not None, "enabling X did not enqueue an unscheduled fetch job"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()

    minute = datetime(R.year, R.month, R.day, 12, 21, tzinfo=UTC)
    end = datetime(R.year, R.month, R.day, 18, 9, tzinfo=UTC)
    while minute <= end:
        asyncio.run(tick(db, clock, at=minute))
        minute += timedelta(minutes=1)

    rows = _downloads(conn, feed_id)
    assert len(rows) == 1, (
        f"M129 -> at this assertion: correct = 1, mutant = {len(rows)}"
    )
    assert rows[0]["slot"] == "unscheduled", (
        f"M129 -> at this assertion: correct = 'unscheduled',"
        f" mutant = {rows[0]['slot']!r}"
    )
    assert standin.calls.get("synthetic-round-x", 0) == 1, (
        f"M129 -> at this assertion: correct = 1 call, mutant ="
        f" {standin.calls.get('synthetic-round-x', 0)} calls"
    )


def test_dl18_started_and_ended_whole_second_fetched_at_keeps_microseconds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL18: the clock (patch_close_clock, whose list includes
    wxverify.core.timeutil) at an instant whose microsecond is 250000: a
    stored download of X, then X's HTTP 500."""
    clock_instant = datetime(R.year, R.month, R.day, 12, 20, 0, 250000, tzinfo=UTC)
    db, conn, site_id, feed_ids, standin, clock = _setup(
        tmp_path,
        monkeypatch,
        subscribe=("X",),
        clock_start=clock_instant,
    )
    feed_id = feed_ids["X"]
    source = "synthetic-round-x"

    def _run_one() -> sqlite3.Row:
        result = enqueue_if_absent(
            conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
        )
        assert result.created, "dedupe hit -- a leftover job from the prior case"
        job = claim_next_job(conn)
        assert job is not None, "no fetch_feed job was claimable"
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()
        rows = _downloads(conn, feed_id)
        return rows[-1]

    issued_at = _stamp(clock_instant)
    valid_at = _stamp(clock_instant + timedelta(hours=6))
    standin.program(
        source,
        ProviderReply(
            samples=(
                round_schedule_sample(
                    source, "temperature", issued_at, valid_at, 6, 10.0
                ),
            )
        ),
    )
    stored_row = _run_one()
    assert stored_row["outcome"] == "stored", (
        f"the stored case must record outcome 'stored', got {stored_row['outcome']!r}"
    )

    standin.program(source, ProviderHttpError(500))
    backoff_row = _run_one()
    assert backoff_row["outcome"] == "backoff", (
        f"the 500 case must record outcome 'backoff', got {backoff_row['outcome']!r}"
    )

    for row in (stored_row, backoff_row):
        started_ok = is_canonical_utc_stamp(row["started_at"])
        assert started_ok, (
            f"M142 -> at this assertion: correct = True, mutant = {started_ok!r}"
        )
        ended_ok = is_canonical_utc_stamp(row["ended_at"])
        assert ended_ok, (
            f"M142 -> at this assertion: correct = True, mutant = {ended_ok!r}"
        )

    sample_row = conn.execute(
        "SELECT fetched_at FROM forecast_samples WHERE feed_id=? LIMIT 1",
        (feed_id,),
    ).fetchone()
    assert sample_row is not None, "the stored download must have written a sample"
    assert stored_row["fetched_at"] == sample_row["fetched_at"], (
        "the stored row's fetched_at must equal its sample's fetched_at"
        f" ({sample_row['fetched_at']!r}), got {stored_row['fetched_at']!r}"
    )
    fetched_at_canonical = is_canonical_utc_stamp(stored_row["fetched_at"])
    assert not fetched_at_canonical, (
        "fetched_at must keep the clock's microseconds (proving the test"
        f" clock reached the writer), but is_canonical_utc_stamp ="
        f" {fetched_at_canonical!r} for {stored_row['fetched_at']!r}"
    )


# ---------------------------------------------------------------------------
# DL3-DL8, DL9 (round part reused above), DL10-DL15: read/inference logic
# over seeded rows
# ---------------------------------------------------------------------------


def test_dl3_earlier_download_of_an_already_stored_label_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL3: X's 00:10 download brings label L, already stored by an 18:10
    download the day before; then a 06:10 download with new label L'."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    s_minus_1 = S - timedelta(days=1)
    l_label = _stamp(
        datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
    )
    l_valid = _stamp(
        datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
        + timedelta(hours=6)
    )
    l_fetched = l_label
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=l_label,
        valid_at=l_valid,
        lead_hours=6,
        value=10.0,
        fetched_at=l_fetched,
    )
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=s_minus_1,
        slot="18:10",
        started_at=l_fetched,
        ended_at=l_fetched,
        fetched_at=l_fetched,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [l_label]},
    )
    round_fetched = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="round",
        round_date=S,
        round_try=1,
        started_at=round_fetched,
        ended_at=round_fetched,
        fetched_at=round_fetched,
        outcome="stored",
        usable=1,
        inserted=0,
        labels={str(feed_id): [l_label]},
    )
    lp_label = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    lp_valid = _stamp(
        datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC) + timedelta(hours=6)
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=lp_label,
        valid_at=lp_valid,
        lead_hours=6,
        value=20.0,
        fetched_at=lp_label,
    )
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="06:10",
        started_at=lp_label,
        ended_at=lp_label,
        fetched_at=lp_label,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [lp_label]},
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result is not None, "scored_download found no download for X on S"
    assert result.fetched_at == round_fetched, (
        f"M20 -> at this assertion: correct = {round_fetched!r} (the 00:10"
        f" download), mutant = {result.fetched_at!r}"
    )

    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=result,
        start_utc=datetime(
            s_minus_1.year, s_minus_1.month, s_minus_1.day, 0, 0, tzinfo=UTC
        ),
        end_utc=datetime(S.year, S.month, S.day, 23, 59, tzinfo=UTC),
    )
    key = ("temperature", l_valid)
    assert key in version, (
        f"M21 -> at this assertion: correct = L's value present, mutant ="
        f" {list(version.keys())!r}"
    )
    assert version[key].value == 10.0, (
        f"M21 -> at this assertion: correct = 10.0, mutant = {version[key].value!r}"
    )


def test_dl4_a_failed_round_falls_back_to_the_next_stored_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL4: X's round fails all tries; its 06:10 download stores L'."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    for minute, outcome in (
        (10, "failed"),
        (15, "failed"),
        (22, "failed"),
        (30, "failed"),
    ):
        when = _stamp(datetime(S.year, S.month, S.day, 0, minute, tzinfo=UTC))
        _seed_download(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            local_date=S,
            slot="round",
            round_date=S,
            round_try=(minute // 7) + 1,
            started_at=when,
            ended_at=when,
            fetched_at=None,
            outcome=outcome,
            reason="synthetic failure",
        )
    lp_fetched = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="06:10",
        started_at=lp_fetched,
        ended_at=lp_fetched,
        fetched_at=lp_fetched,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [lp_fetched]},
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result is not None, (
        "M22 -> at this assertion: correct = the 06:10 download, mutant = None"
    )
    assert result.fetched_at == lp_fetched, (
        f"M22 -> at this assertion: correct = {lp_fetched!r}, mutant ="
        f" {result.fetched_at!r}"
    )


def test_dl5_no_download_stored_all_day_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL5: no download of X stored all day S."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    when = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="round",
        round_date=S,
        round_try=1,
        started_at=when,
        ended_at=when,
        fetched_at=None,
        outcome="failed",
        reason="synthetic failure",
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    if result is not None:
        pytest.fail(f"M23 -> at this assertion: correct = None, mutant = {result!r}")

    # Paired positive: seed a genuinely stored download the next day and
    # confirm it IS found, so the absence above is not an ambient no-op.
    tomorrow = S + timedelta(days=1)
    stored_at = _stamp(
        datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 10, tzinfo=UTC)
    )
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=tomorrow,
        slot="round",
        round_date=tomorrow,
        round_try=1,
        started_at=stored_at,
        ended_at=stored_at,
        fetched_at=stored_at,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [stored_at]},
    )
    conn.commit()
    positive = scored_download(
        conn, site_id=site_id, feed_id=feed_id, local_date=tomorrow
    )
    assert positive is not None, "fixture sanity: a stored download must be found"
    assert positive.fetched_at == stored_at, (
        f"expected the stored download's fetched_at {stored_at!r},"
        f" got {positive.fetched_at!r}"
    )


def test_dl6_a_later_download_of_the_same_label_cannot_add_hours(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL6: label L stored at 00:10 for hours h1-h168; a 06:10 download of
    L adds h169."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    label = _stamp(datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC))
    first_fetch = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    second_fetch = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    h1_valid = _stamp(datetime(S.year, S.month, S.day, 1, 0, tzinfo=UTC))
    h168_valid = _stamp(
        datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC) + timedelta(hours=168)
    )
    h169_valid = _stamp(
        datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC) + timedelta(hours=169)
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label,
        valid_at=h1_valid,
        lead_hours=1,
        value=1.0,
        fetched_at=first_fetch,
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label,
        valid_at=h168_valid,
        lead_hours=168,
        value=16.8,
        fetched_at=first_fetch,
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label,
        valid_at=h169_valid,
        lead_hours=169,
        value=16.9,
        fetched_at=second_fetch,
    )
    conn.commit()

    download = ScoredDownload(
        download_id=None,
        feed_id=feed_id,
        local_date=S,
        fetched_at=first_fetch,
        labels={str(feed_id): [label]},
        source="recorded",
    )
    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=download,
        start_utc=datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC),
        end_utc=datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC)
        + timedelta(hours=200),
    )
    assert ("temperature", h169_valid) not in version, (
        f"M24 -> at this assertion: correct = h169 absent, mutant = h169 present"
        f" ({version.get(('temperature', h169_valid))!r})"
    )
    assert ("temperature", h1_valid) in version, "h1 is missing from the version"
    assert ("temperature", h168_valid) in version, "h168 is missing from the version"


def test_dl7_two_labels_in_one_download_resolve_to_the_newer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL7: one download with labels L1 and L2 both giving hour h."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    fetched_at = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    l1 = _stamp(datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC))
    l2 = _stamp(datetime(S.year, S.month, S.day, 0, 5, tzinfo=UTC))
    h_valid = _stamp(datetime(S.year, S.month, S.day, 6, 0, tzinfo=UTC))
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=l1,
        valid_at=h_valid,
        lead_hours=6,
        value=10.0,
        fetched_at=fetched_at,
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=l2,
        valid_at=h_valid,
        lead_hours=6,
        value=20.0,
        fetched_at=fetched_at,
    )
    conn.commit()

    download = ScoredDownload(
        download_id=None,
        feed_id=feed_id,
        local_date=S,
        fetched_at=fetched_at,
        labels={str(feed_id): sorted([l1, l2])},
        source="recorded",
    )
    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=download,
        start_utc=datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC),
        end_utc=datetime(S.year, S.month, S.day, 23, 59, tzinfo=UTC),
    )
    key = ("temperature", h_valid)
    assert key in version, "hour h is missing from the version"
    assert version[key].value == 20.0, (
        f"M25 -> at this assertion: correct = 20.0 (L2, the newer), mutant ="
        f" {version[key].value!r}"
    )


def test_dl8_excluded_samples_never_reach_the_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL8: samples with lead above max_lead_hours, an invalid value, and
    a rain sample before the label boundary.

    Lead 0 is left out of this row -- ``forecast_samples``' own CHECK
    (``lead_hours >= 1``) already forbids it, so a lead-0 sample can never
    exist to seed.
    """
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    label = _stamp(datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC))
    fetched_at = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    above_max_valid = _stamp(
        datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC) + timedelta(hours=200)
    )
    invalid_valid = _stamp(
        datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC) + timedelta(hours=5)
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label,
        valid_at=above_max_valid,
        lead_hours=200,
        value=10.0,
        fetched_at=fetched_at,
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label,
        valid_at=invalid_valid,
        lead_hours=5,
        value=999.0,
        fetched_at=fetched_at,
    )
    conn.commit()

    download = ScoredDownload(
        download_id=None,
        feed_id=feed_id,
        local_date=S,
        fetched_at=fetched_at,
        labels={str(feed_id): [label]},
        source="recorded",
    )
    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=download,
        start_utc=datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC),
        end_utc=datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC)
        + timedelta(hours=250),
    )
    assert version == {}, (
        f"M26 -> at this assertion: correct = {{}}, mutant = {version!r}"
    )

    # The rain-label boundary, on a real meteoblue-sourced feed (the
    # filter only restricts open-meteo/meteoblue sources, 5.2).
    rain_feed_id = int(
        conn.execute(
            "INSERT INTO feeds"
            " (source, model, enabled, default_subscribed,"
            "  fetch_interval_minutes, max_lead_hours, is_virtual)"
            " VALUES ('meteoblue', 'dl8-rain-member', 1, 0, 1440, 168, 0)"
        ).lastrowid
    )
    rain_valid = _stamp(
        datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC) + timedelta(hours=6)
    )
    rain_fetched = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    boundary = _stamp(datetime(S.year, S.month, S.day, 0, 20, tzinfo=UTC))
    set_runtime_state(conn, "precip_hour_start_boundary", boundary)
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=rain_feed_id,
        variable="precip",
        issued_at=label,
        valid_at=rain_valid,
        lead_hours=6,
        value=1.0,
        fetched_at=rain_fetched,
    )
    conn.commit()

    rain_download = ScoredDownload(
        download_id=None,
        feed_id=rain_feed_id,
        local_date=S,
        fetched_at=rain_fetched,
        labels={str(rain_feed_id): [label]},
        source="recorded",
    )
    rain_version = scored_version(
        conn,
        site_id=site_id,
        feed_id=rain_feed_id,
        download=rain_download,
        start_utc=datetime(S.year, S.month, S.day, 0, 0, tzinfo=UTC),
        end_utc=datetime(S.year, S.month, S.day, 23, 59, tzinfo=UTC),
    )
    assert rain_version == {}, (
        f"M26 -> at this assertion: correct = {{}} (rain before the"
        f" boundary excluded), mutant = {rain_version!r}"
    )


def test_dl10_a_job_ending_after_the_first_new_row_does_not_win(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL10: download_record_since at S 08:00Z; seeded rows of X with
    fetched_at S 00:10Z (labels L) and S 06:10Z (L'); X's completed
    fetch_feed job, result "ok", ending S 00:12Z; a recorded download at
    S 12:10Z."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    since = _stamp(datetime(S.year, S.month, S.day, 8, 0, tzinfo=UTC))
    set_runtime_state(conn, "download_record_since", since)
    fetch_10 = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    fetch_06 = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    label_l = fetch_10
    label_lp = fetch_06
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label_l,
        valid_at=_stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC)),
        lead_hours=6,
        value=1.0,
        fetched_at=fetch_10,
        source_raw="synthetic-round-x:raw",
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=label_lp,
        valid_at=_stamp(datetime(S.year, S.month, S.day, 12, 10, tzinfo=UTC)),
        lead_hours=6,
        value=2.0,
        fetched_at=fetch_06,
        source_raw="synthetic-round-x:raw",
    )
    job_end = _stamp(datetime(S.year, S.month, S.day, 0, 12, tzinfo=UTC))
    _seed_job(
        conn,
        site_id=site_id,
        job_key=f"fetch:{feed_id}",
        updated_at=job_end,
        result="ok",
    )
    recorded_at = _stamp(datetime(S.year, S.month, S.day, 12, 10, tzinfo=UTC))
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="12:10",
        started_at=recorded_at,
        ended_at=recorded_at,
        fetched_at=recorded_at,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [recorded_at]},
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result is not None, "scored_download found no inferred download for X on S"
    assert result.fetched_at == fetch_10, (
        f"M28 -> at this assertion: correct = {fetch_10!r} (00:10, the"
        f" first new row), mutant = {result.fetched_at!r}"
    )
    assert result.source == "inferred", (
        f"expected source 'inferred', got {result.source!r}"
    )
    assert result.download_id is None, (
        f"expected download_id None (inferred), got {result.download_id!r}"
    )


def test_dl11_historical_rows_excluded_from_the_inference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL11: the seeded rows at S 00:10Z are ` previous_day1` rows, and a
    forward row at S 03:10Z."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    since = _stamp(datetime(S.year, S.month, S.day, 8, 0, tzinfo=UTC))
    set_runtime_state(conn, "download_record_since", since)
    historical_fetch = _stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC))
    forward_fetch = _stamp(datetime(S.year, S.month, S.day, 3, 10, tzinfo=UTC))
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=historical_fetch,
        valid_at=_stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC)),
        lead_hours=6,
        value=1.0,
        fetched_at=historical_fetch,
        source_raw="synthetic-round-x:raw previous_day1",
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=forward_fetch,
        valid_at=_stamp(datetime(S.year, S.month, S.day, 9, 10, tzinfo=UTC)),
        lead_hours=6,
        value=2.0,
        fetched_at=forward_fetch,
        source_raw="synthetic-round-x:raw",
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result is not None, "scored_download found no inferred download for X on S"
    assert result.fetched_at == forward_fetch, (
        f"M29 -> at this assertion: correct = {forward_fetch!r} (03:10,"
        f" historical excluded), mutant = {result.fetched_at!r}"
    )


def test_dl12_install_day_earlier_of_inferred_and_recorded_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL12: the install day S, download_record_since S 08:00Z: (i) seeded
    rows at 06:10Z and a recorded stored row at 09:00Z; (ii) no seeded
    row, the recorded row at 09:00Z."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    since = _stamp(datetime(S.year, S.month, S.day, 8, 0, tzinfo=UTC))
    set_runtime_state(conn, "download_record_since", since)
    inferred_fetch = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=inferred_fetch,
        valid_at=_stamp(datetime(S.year, S.month, S.day, 12, 10, tzinfo=UTC)),
        lead_hours=6,
        value=1.0,
        fetched_at=inferred_fetch,
        source_raw="synthetic-round-x:raw",
    )
    recorded_at = _stamp(datetime(S.year, S.month, S.day, 9, 0, tzinfo=UTC))
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="unscheduled",
        started_at=recorded_at,
        ended_at=recorded_at,
        fetched_at=recorded_at,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [recorded_at]},
    )
    conn.commit()

    result_i = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result_i is not None, (
        "scored_download found no download for X on the install day"
    )
    assert result_i.fetched_at == inferred_fetch, (
        f"M30 -> at this assertion: correct = {inferred_fetch!r} (the"
        f" earlier, inferred, download), mutant = {result_i.fetched_at!r}"
    )

    # (ii): a fresh feed with no seeded sample, only the recorded row.
    other_id = feed_ids["Y"]
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=other_id,
        local_date=S,
        slot="unscheduled",
        started_at=recorded_at,
        ended_at=recorded_at,
        fetched_at=recorded_at,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(other_id): [recorded_at]},
    )
    conn.commit()
    result_ii = scored_download(conn, site_id=site_id, feed_id=other_id, local_date=S)
    assert result_ii is not None, (
        "scored_download found no download for Y on the install day"
    )
    assert result_ii.fetched_at == recorded_at, (
        f"M30 -> at this assertion: correct = {recorded_at!r} (the"
        f" recorded download, no inference candidate), mutant ="
        f" {result_ii.fetched_at!r}"
    )
    assert result_ii.source == "recorded", (
        f"expected source 'recorded', got {result_ii.source!r}"
    )


def test_dl13_null_fetched_at_never_inferred(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL13: a seeded row with fetched_at NULL at S."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(
        tmp_path, monkeypatch, subscribe=("X", "Y")
    )
    feed_id = feed_ids["X"]
    since = _stamp(datetime(S.year, S.month, S.day, 8, 0, tzinfo=UTC))
    set_runtime_state(conn, "download_record_since", since)
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=_stamp(datetime(S.year, S.month, S.day, 0, 10, tzinfo=UTC)),
        valid_at=_stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC)),
        lead_hours=6,
        value=1.0,
        fetched_at=None,
        source_raw="synthetic-round-x:raw",
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    if result is not None:
        pytest.fail(f"M31 -> at this assertion: correct = None, mutant = {result!r}")

    # Paired positive: a real recorded download on the SAME feed is found.
    recorded_at = _stamp(datetime(S.year, S.month, S.day, 9, 0, tzinfo=UTC))
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=S,
        slot="unscheduled",
        started_at=recorded_at,
        ended_at=recorded_at,
        fetched_at=recorded_at,
        outcome="stored",
        usable=1,
        inserted=1,
        labels={str(feed_id): [recorded_at]},
    )
    conn.commit()
    positive = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert positive is not None, "fixture sanity: the recorded download must be found"
    assert positive.fetched_at == recorded_at, (
        f"expected the recorded download's fetched_at {recorded_at!r},"
        f" got {positive.fetched_at!r}"
    )


def test_dl14_a_qualifying_job_carries_the_newest_prior_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL14: download_record_since at S 08:00Z; X's rows of label L stored
    at S−1 18:10Z and of L' at S 06:10Z; no row of X with fetched_at in S
    before 06:10Z; X's completed fetch_feed job, result "ok", ending S
    00:12Z."""
    db, conn, site_id, feed_ids, _standin, _clock = _setup(tmp_path, monkeypatch)
    feed_id = feed_ids["X"]
    since = _stamp(datetime(S.year, S.month, S.day, 8, 0, tzinfo=UTC))
    set_runtime_state(conn, "download_record_since", since)
    s_minus_1 = S - timedelta(days=1)
    l_fetch = _stamp(
        datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
    )
    lp_fetch = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=l_fetch,
        valid_at=_stamp(
            datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
            + timedelta(hours=6)
        ),
        lead_hours=6,
        value=1.0,
        fetched_at=l_fetch,
        source_raw="synthetic-round-x:raw",
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=lp_fetch,
        valid_at=_stamp(
            datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC) + timedelta(hours=6)
        ),
        lead_hours=6,
        value=2.0,
        fetched_at=lp_fetch,
        source_raw="synthetic-round-x:raw",
    )
    job_end = _stamp(datetime(S.year, S.month, S.day, 0, 12, tzinfo=UTC))
    _seed_job(
        conn,
        site_id=site_id,
        job_key=f"fetch:{feed_id}",
        updated_at=job_end,
        result="ok",
    )
    conn.commit()

    result = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result is not None, "scored_download found no inferred download for X on S"
    assert result.fetched_at == job_end, (
        f"M124 -> at this assertion: correct = {job_end!r} (the job),"
        f" mutant = {result.fetched_at!r}"
    )
    assert result.source == "inferred" and result.download_id is None, (
        f"expected source='inferred', download_id=None; got"
        f" source={result.source!r} download_id={result.download_id!r}"
    )
    expected_labels = {str(feed_id): [l_fetch]}
    assert result.labels == expected_labels, (
        f"M124 -> at this assertion: correct = {expected_labels!r} (L,"
        f" carried from before the job), mutant = {result.labels!r}"
    )

    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=result,
        start_utc=datetime(
            s_minus_1.year, s_minus_1.month, s_minus_1.day, 0, 0, tzinfo=UTC
        ),
        end_utc=datetime(S.year, S.month, S.day, 23, 59, tzinfo=UTC),
    )
    key = (
        "temperature",
        _stamp(
            datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
            + timedelta(hours=6)
        ),
    )
    assert key in version and version[key].value == 1.0, (
        f"M124 -> at this assertion: correct = L's value present, mutant ="
        f" {version.get(key)!r}"
    )


def test_dl15_a_job_qualifies_only_when_completed_ok_and_the_right_feed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL15: as DL14, but the 00:12Z job is (i) cancelled (result NULL),
    (ii) Y's, or (iii) X's and the only one, with no row of X stored in S
    at all."""
    since_dt = datetime(S.year, S.month, S.day, 8, 0, tzinfo=UTC)
    lp_fetch = _stamp(datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC))
    s_minus_1 = S - timedelta(days=1)
    l_fetch = _stamp(
        datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
    )
    job_end = _stamp(datetime(S.year, S.month, S.day, 0, 12, tzinfo=UTC))

    # (i): the job is cancelled (result NULL).
    db, conn, site_id, feed_ids, _standin, _clock = _setup(
        tmp_path / "i", monkeypatch, subscribe=("X", "Y")
    )
    feed_id = feed_ids["X"]
    set_runtime_state(conn, "download_record_since", _stamp(since_dt))
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=l_fetch,
        valid_at=_stamp(
            datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
            + timedelta(hours=6)
        ),
        lead_hours=6,
        value=1.0,
        fetched_at=l_fetch,
        source_raw="synthetic-round-x:raw",
    )
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at=lp_fetch,
        valid_at=_stamp(
            datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC) + timedelta(hours=6)
        ),
        lead_hours=6,
        value=2.0,
        fetched_at=lp_fetch,
        source_raw="synthetic-round-x:raw",
    )
    _seed_job(
        conn,
        site_id=site_id,
        job_key=f"fetch:{feed_id}",
        updated_at=job_end,
        result=None,
    )
    conn.commit()
    result_i = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=S)
    assert result_i is not None, "scored_download found no download for X on S (case i)"
    assert result_i.fetched_at == lp_fetch, (
        f"M125 -> at this assertion: correct = {lp_fetch!r} (06:10, the"
        f" cancelled job does not qualify), mutant = {result_i.fetched_at!r}"
    )

    # (ii): the job is Y's, not X's.
    db2, conn2, site2_id, feed2_ids, _standin2, _clock2 = _setup(
        tmp_path / "ii", monkeypatch, subscribe=("X", "Y")
    )
    x_id = feed2_ids["X"]
    y_id = feed2_ids["Y"]
    set_runtime_state(conn2, "download_record_since", _stamp(since_dt))
    _seed_sample(
        conn2,
        site_id=site2_id,
        feed_id=x_id,
        variable="temperature",
        issued_at=l_fetch,
        valid_at=_stamp(
            datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
            + timedelta(hours=6)
        ),
        lead_hours=6,
        value=1.0,
        fetched_at=l_fetch,
        source_raw="synthetic-round-x:raw",
    )
    _seed_sample(
        conn2,
        site_id=site2_id,
        feed_id=x_id,
        variable="temperature",
        issued_at=lp_fetch,
        valid_at=_stamp(
            datetime(S.year, S.month, S.day, 6, 10, tzinfo=UTC) + timedelta(hours=6)
        ),
        lead_hours=6,
        value=2.0,
        fetched_at=lp_fetch,
        source_raw="synthetic-round-x:raw",
    )
    _seed_job(
        conn2,
        site_id=site2_id,
        job_key=f"fetch:{y_id}",
        updated_at=job_end,
        result="ok",
    )
    conn2.commit()
    result_ii = scored_download(conn2, site_id=site2_id, feed_id=x_id, local_date=S)
    assert result_ii is not None, (
        "scored_download found no download for X on S (case ii)"
    )
    assert result_ii.fetched_at == lp_fetch, (
        f"M125 -> at this assertion: correct = {lp_fetch!r} (06:10, Y's"
        f" job does not qualify for X), mutant = {result_ii.fetched_at!r}"
    )

    # (iii): X's job, the only one, with no row of X stored in S at all.
    db3, conn3, site3_id, feed3_ids, _standin3, _clock3 = _setup(
        tmp_path / "iii", monkeypatch, subscribe=("X",)
    )
    x3_id = feed3_ids["X"]
    set_runtime_state(conn3, "download_record_since", _stamp(since_dt))
    _seed_sample(
        conn3,
        site_id=site3_id,
        feed_id=x3_id,
        variable="temperature",
        issued_at=l_fetch,
        valid_at=_stamp(
            datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 10, tzinfo=UTC)
            + timedelta(hours=6)
        ),
        lead_hours=6,
        value=1.0,
        fetched_at=l_fetch,
        source_raw="synthetic-round-x:raw",
    )
    _seed_job(
        conn3,
        site_id=site3_id,
        job_key=f"fetch:{x3_id}",
        updated_at=job_end,
        result="ok",
    )
    conn3.commit()
    result_iii = scored_download(conn3, site_id=site3_id, feed_id=x3_id, local_date=S)
    assert result_iii is not None, (
        "scored_download found no download for X on S (case iii)"
    )
    assert result_iii.fetched_at == job_end, (
        f"M125 -> at this assertion: correct = {job_end!r} (the job, no"
        f" X row in S at all), mutant = {result_iii.fetched_at!r}"
    )

"""Tests for the value-change log (P4 plan §18.5) --
``plans/2026-10-22-daily-close-pairs-and-publication.md``.

VC1-VC6, one test per row. Every assertion that kills a mutant named in
the row's last column (plan §19.2) carries a message of the form
``"M<n> -> at this assertion: correct = <X>, mutant = <Y>"``.

``forecast_value_changes`` does not exist before piece 1 (plan §20 Step
4), and the writer that fills it (inside ``_persist_fetch_success``)
does not exist before piece 2 -- this whole file fails at collection
and at run time (``forecast_value_changes`` unresolved on the real
SQLite file) until then, which the plan's Step 3 accepts.

Every download in these tests is driven through the real job pipeline
(``tick``/``claim_next_job``/``run_claimed_job``) for feed X, since the
value-change log is written as a side effect of
``_persist_fetch_success``'s own transaction (5.2) -- there is no pure
function to seed its inputs against directly, unlike the download
record's own ``scored_download``/``scored_version`` (§18.4).
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    STANDARD_NIGHT_R,
    D,
    MovingClock,
    ProviderHttpError,
    ProviderReply,
    disable_other_enqueuers,
    install_provider_standin,
    patch_close_clock,
    round_schedule_sample,
    seed_round_schedule_feeds,
    standard_night,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.queue import claim_next_job, complete, enqueue_if_absent
from wxverify.worker.processor import run_claimed_job

R = STANDARD_NIGHT_R  # 2024-06-11 (plan §15.1's standard night: R = D + 1)
S = R
SOURCE = "synthetic-round-x"


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """P3's standard night plus P4's round-schedule feeds, the provider
    stand-in and the synthetic-source domain-backoff patch installed, and
    a :class:`MovingClock` patched over every close-chain module. Returns
    ``(db, conn, site_id, feed_id, standin, clock)`` with only X
    subscribed.
    """
    db, _writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001 -- tests read the real writer conn directly
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=("X",))
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    clock = MovingClock(datetime(D.year, D.month, D.day, 17, 0, tzinfo=UTC))
    patch_close_clock(monkeypatch, clock)
    return db, conn, site_id, feed_ids["X"], standin, clock


def _stamp(dt: datetime) -> str:
    return isoformat_utc(dt)


def _run_round(db, conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """One round try (round_try 1, round_date R) for ``feed_id``, claimed
    and run to completion -- the job is enqueued directly rather than
    through the scheduler's round enqueuer (``worker/round.py``), which
    does not exist before piece 3 (plan sec 20 Step 4); these rows test
    the value-change log piece 2 writes, not the round's own schedule."""
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
    job = claim_next_job(conn)
    assert job is not None, "no round job was claimable for the feed"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()


def _run_unscheduled(db, conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """One unscheduled fetch for ``feed_id``, claimed and run to
    completion -- the simplest way to drive a second (or third) download
    of X without depending on the round's retry/bound mechanics, which
    the value-change log does not care about (5.2: "for every stored
    download -- round, daytime or unscheduled").

    A failed try (e.g. VC4's 404 between the two stored downloads)
    re-pends its job -- status back to 'pending', with a retry's future
    next_attempt_at -- and enqueue_if_absent's dedupe query only checks
    status, not next_attempt_at, so that leftover job would dedupe the
    next unscheduled fetch. Finish it through the queue's own complete()
    first.
    """
    leftover = conn.execute(
        "SELECT id FROM jobs WHERE type='fetch_feed' AND job_key=?"
        " AND site_id IS ? AND status IN ('pending','running')",
        (f"fetch:{feed_id}", site_id),
    ).fetchone()
    if leftover is not None:
        complete(conn, int(leftover["id"]))
        conn.commit()
    result = enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    assert result.created, "dedupe hit -- a leftover job from the prior download"
    job = claim_next_job(conn)
    assert job is not None, "no unscheduled fetch job was claimable"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()


def _value_change_rows(conn: sqlite3.Connection, download_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM forecast_value_changes"
            " WHERE download_id=? ORDER BY variable",
            (download_id,),
        )
    )


def _last_download_id(conn: sqlite3.Connection, feed_id: int) -> int:
    row = conn.execute(
        "SELECT id FROM forecast_downloads WHERE feed_id=? ORDER BY id DESC LIMIT 1",
        (feed_id,),
    ).fetchone()
    assert row is not None, f"no forecast_downloads row found for feed_id={feed_id}"
    return int(row["id"])


def _two_variable_reply(
    label: str,
    base_valid: datetime,
    *,
    temperature: float,
    wind: float,
    temperature_hour7: float | None = None,
) -> ProviderReply:
    """``temperature_hour7`` defaults to ``temperature`` (both hours carry
    the same value); pass it explicitly to change only hour 6's value
    between two replies, as VC1 needs (one temperature hour changed, not
    both)."""
    if temperature_hour7 is None:
        temperature_hour7 = temperature
    return ProviderReply(
        samples=(
            round_schedule_sample(
                SOURCE, "temperature", label, _stamp(base_valid), 6, temperature
            ),
            round_schedule_sample(
                SOURCE,
                "temperature",
                label,
                _stamp(base_valid + timedelta(hours=1)),
                7,
                temperature_hour7,
            ),
            round_schedule_sample(SOURCE, "wind", label, _stamp(base_valid), 6, wind),
            round_schedule_sample(
                SOURCE,
                "wind",
                label,
                _stamp(base_valid + timedelta(hours=1)),
                7,
                wind,
            ),
        )
    )


# ---------------------------------------------------------------------------
# VC1-VC4, VC6
# ---------------------------------------------------------------------------


def test_vc1_a_changed_value_is_logged_per_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC1: two downloads of X, the second with the same label and
    window, one temperature value changed by 0.5."""
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    base_valid = datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)

    standin.program(
        SOURCE, _two_variable_reply(label, base_valid, temperature=10.0, wind=5.0)
    )
    _run_round(db, conn, site_id, feed_id)

    standin.program(
        SOURCE,
        _two_variable_reply(
            label, base_valid, temperature=10.5, wind=5.0, temperature_hour7=10.0
        ),
    )
    _run_unscheduled(db, conn, site_id, feed_id)

    second_id = _last_download_id(conn, feed_id)
    rows = {row["variable"]: row for row in _value_change_rows(conn, second_id)}
    assert "temperature" in rows and "wind" in rows, (
        f"expected value-change rows for temperature and wind, got {sorted(rows)!r}"
    )

    temp_row = rows["temperature"]
    assert temp_row["hours_changed"] == 1, (
        f"M32 -> at this assertion: correct = 1, mutant = {temp_row['hours_changed']}"
    )
    max_abs_change = temp_row["max_abs_change"]
    assert abs(max_abs_change - 0.5) < 1e-9, (
        f"M32 -> at this assertion: correct = 0.5, mutant = {max_abs_change!r}"
    )
    assert temp_row["labels_changed"] == 0, (
        f"expected labels_changed 0 (same label), got {temp_row['labels_changed']!r}"
    )

    wind_row = rows["wind"]
    assert wind_row["hours_changed"] == 0, (
        f"M32 -> at this assertion: correct = 0, mutant = {wind_row['hours_changed']}"
    )


def test_vc2_a_forward_moving_window_is_not_a_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC2: the second download's window moved forward 6 hours with no
    value changed."""
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    first_base = datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)

    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(
                    SOURCE, "temperature", label, _stamp(first_base), 6, 10.0
                ),
                round_schedule_sample(
                    SOURCE,
                    "temperature",
                    label,
                    _stamp(first_base + timedelta(hours=1)),
                    7,
                    11.0,
                ),
                round_schedule_sample(
                    SOURCE,
                    "temperature",
                    label,
                    _stamp(first_base + timedelta(hours=2)),
                    8,
                    12.0,
                ),
            )
        ),
    )
    _run_round(db, conn, site_id, feed_id)

    # Second download's window shifted forward by one hour: overlaps at
    # first_base + 1h and + 2h (2 hours), with the SAME values; + 3h is
    # new and never compared.
    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(
                    SOURCE,
                    "temperature",
                    label,
                    _stamp(first_base + timedelta(hours=1)),
                    6,
                    11.0,
                ),
                round_schedule_sample(
                    SOURCE,
                    "temperature",
                    label,
                    _stamp(first_base + timedelta(hours=2)),
                    7,
                    12.0,
                ),
                round_schedule_sample(
                    SOURCE,
                    "temperature",
                    label,
                    _stamp(first_base + timedelta(hours=3)),
                    8,
                    13.0,
                ),
            )
        ),
    )
    _run_unscheduled(db, conn, site_id, feed_id)

    second_id = _last_download_id(conn, feed_id)
    rows = _value_change_rows(conn, second_id)
    temp_row = next(r for r in rows if r["variable"] == "temperature")
    assert temp_row["hours_compared"] == 2, (
        f"M33 -> at this assertion: correct = 2, mutant = {temp_row['hours_compared']}"
    )
    assert temp_row["hours_changed"] == 0, (
        f"M33 -> at this assertion: correct = 0, mutant = {temp_row['hours_changed']}"
    )


def test_vc3_a_new_label_sets_labels_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC3: the second download carries a new label."""
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label_1 = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    label_2 = _stamp(datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC))
    valid_at = _stamp(datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC))

    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(
                    SOURCE, "temperature", label_1, valid_at, 6, 10.0
                ),
            )
        ),
    )
    _run_round(db, conn, site_id, feed_id)

    valid_at_2 = _stamp(datetime(R.year, R.month, R.day, 12, 0, tzinfo=UTC))
    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(
                    SOURCE, "temperature", label_2, valid_at_2, 6, 10.0
                ),
            )
        ),
    )
    _run_unscheduled(db, conn, site_id, feed_id)

    second_id = _last_download_id(conn, feed_id)
    rows = _value_change_rows(conn, second_id)
    temp_row = next(r for r in rows if r["variable"] == "temperature")
    assert temp_row["labels_changed"] == 1, (
        f"M34 -> at this assertion: correct = 1, mutant = {temp_row['labels_changed']}"
    )


def test_vc4_the_comparison_is_with_the_last_stored_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC4: a failed download between the two."""
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    valid_at = _stamp(datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC))

    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(SOURCE, "temperature", label, valid_at, 6, 10.0),
            )
        ),
    )
    _run_round(db, conn, site_id, feed_id)
    first_id = _last_download_id(conn, feed_id)

    standin.program(SOURCE, ProviderHttpError(404))
    _run_unscheduled(db, conn, site_id, feed_id)
    failed_id = _last_download_id(conn, feed_id)
    assert failed_id != first_id, "the failed download did not create its own row"

    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(SOURCE, "temperature", label, valid_at, 6, 10.5),
            )
        ),
    )
    _run_unscheduled(db, conn, site_id, feed_id)
    third_id = _last_download_id(conn, feed_id)

    rows = _value_change_rows(conn, third_id)
    temp_row = next(r for r in rows if r["variable"] == "temperature")
    assert temp_row["previous_download_id"] == first_id, (
        f"M35 -> at this assertion: correct = {first_id} (the last stored"
        f" download), mutant = {temp_row['previous_download_id']!r}"
    )
    assert temp_row["hours_changed"] == 1, (
        f"M35 -> at this assertion: correct = 1, mutant = {temp_row['hours_changed']}"
    )


def test_vc6_a_revised_value_under_a_stored_label_is_logged_not_stored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC6: a download with the provider's revised value under a label
    already stored."""
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    valid_at = _stamp(datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC))

    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(SOURCE, "temperature", label, valid_at, 6, 10.0),
            )
        ),
    )
    _run_round(db, conn, site_id, feed_id)

    standin.program(
        SOURCE,
        ProviderReply(
            samples=(
                round_schedule_sample(SOURCE, "temperature", label, valid_at, 6, 15.0),
            )
        ),
    )
    _run_unscheduled(db, conn, site_id, feed_id)
    second_id = _last_download_id(conn, feed_id)

    stored_value = conn.execute(
        "SELECT value FROM forecast_samples"
        " WHERE feed_id=? AND issued_at=? AND valid_at=?",
        (feed_id, label, valid_at),
    ).fetchone()
    assert stored_value is not None, (
        "no forecast_samples row found for the stored label"
    )
    assert stored_value["value"] == 10.0, (
        f"M37 -> at this assertion: correct = 10.0 (the first value kept),"
        f" mutant = {stored_value['value']!r}"
    )

    rows = _value_change_rows(conn, second_id)
    temp_row = next(r for r in rows if r["variable"] == "temperature")
    assert temp_row["hours_changed"] == 1, (
        f"M37 -> at this assertion: correct = 1 (the reply's revised value"
        f" is still compared), mutant = {temp_row['hours_changed']}"
    )


# ---------------------------------------------------------------------------
# VC5: the publication swap's 90-day cleanup
# ---------------------------------------------------------------------------


def test_vc5_the_swap_deletes_value_change_rows_older_than_90_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC5: a publication swap with value-change rows 89 and 91 days old."""
    from wxverify.scoring.publication import publish  # P4 piece 6 (plan §8)

    db, writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001
    feed_id = int(
        conn.execute("SELECT id FROM feeds ORDER BY id LIMIT 1").fetchone()["id"]
    )

    now = datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC)
    old_ended = _stamp(now - timedelta(days=91))
    kept_ended = _stamp(now - timedelta(days=89))

    old_download_id = int(
        conn.execute(
            "INSERT INTO forecast_downloads"
            " (site_id, feed_id, local_date, slot, started_at, ended_at,"
            "  fetched_at, outcome, usable, inserted, labels)"
            " VALUES (?, ?, ?, 'unscheduled', ?, ?, ?, 'stored', 1, 1, '{}')",
            (
                site_id,
                feed_id,
                (D - timedelta(days=91)).isoformat(),
                old_ended,
                old_ended,
                old_ended,
            ),
        ).lastrowid
    )
    kept_download_id = int(
        conn.execute(
            "INSERT INTO forecast_downloads"
            " (site_id, feed_id, local_date, slot, started_at, ended_at,"
            "  fetched_at, outcome, usable, inserted, labels)"
            " VALUES (?, ?, ?, 'unscheduled', ?, ?, ?, 'stored', 1, 1, '{}')",
            (
                site_id,
                feed_id,
                (D - timedelta(days=89)).isoformat(),
                kept_ended,
                kept_ended,
                kept_ended,
            ),
        ).lastrowid
    )
    conn.execute(
        "INSERT INTO forecast_value_changes"
        " (download_id, previous_download_id, feed_id, variable,"
        "  hours_compared, hours_changed, max_abs_change,"
        "  first_changed_valid_at, labels_changed)"
        " VALUES (?, NULL, ?, 'temperature', 1, 0, 0.0, NULL, 0)",
        (old_download_id, feed_id),
    )
    conn.execute(
        "INSERT INTO forecast_value_changes"
        " (download_id, previous_download_id, feed_id, variable,"
        "  hours_compared, hours_changed, max_abs_change,"
        "  first_changed_valid_at, labels_changed)"
        " VALUES (?, NULL, ?, 'temperature', 1, 0, 0.0, NULL, 0)",
        (kept_download_id, feed_id),
    )
    conn.commit()

    d_minus_1 = D - timedelta(days=1)
    asyncio.run(
        publish(db, writer, site_id=site_id, cutoff=d_minus_1, attempt=1, now_utc=now)
    )

    remaining = {
        row["download_id"]
        for row in conn.execute("SELECT download_id FROM forecast_value_changes")
    }
    assert old_download_id not in remaining, (
        f"M36 -> at this assertion: correct = deleted (91 days old),"
        f" mutant = still present (ids remaining: {sorted(remaining)!r})"
    )
    assert kept_download_id in remaining, (
        f"M36 -> at this assertion: correct = kept (89 days old),"
        f" mutant = deleted (ids remaining: {sorted(remaining)!r})"
    )


# ---------------------------------------------------------------------------
# VC7: the swap's delete list subquery, insertion order reversed, plus the
# query-plan pin for ``_DELETE_OLD_VALUE_CHANGES_SQL`` itself (data-layer
# fix: the list subquery now seeks ``forecast_downloads`` by rowid from the
# delete log's own minimum ``download_id`` instead of walking every download
# ever stored).
# ---------------------------------------------------------------------------


def test_vc7_the_delete_list_follows_ended_at_not_insertion_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC7(i): three downloads inserted in an order that does NOT match
    their age -- a download with no value-change rows at all (95 days old)
    inserted first (lowest id), then the 89-day-old download, then the
    91-day-old download (so the 91-day-old row has the HIGHER id despite
    being chronologically older than the 89-day-old one). The publish swap
    must still delete only the 91-day-old row by its ``ended_at``, not by
    id order -- this catches a rewrite that bounds the delete by id/
    insertion order as a stand-in for age, which would get the wrong answer
    here precisely because id order and age order disagree.
    """
    from wxverify.scoring.publication import publish  # P4 piece 6 (plan sec 8)

    db, writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001
    feed_id = int(
        conn.execute("SELECT id FROM feeds ORDER BY id LIMIT 1").fetchone()["id"]
    )

    now = datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC)
    no_vc_ended = _stamp(now - timedelta(days=95))
    kept_ended = _stamp(now - timedelta(days=89))
    old_ended = _stamp(now - timedelta(days=91))

    def _insert_download(ended_at: str, local_date) -> int:
        return int(
            conn.execute(
                "INSERT INTO forecast_downloads"
                " (site_id, feed_id, local_date, slot, started_at, ended_at,"
                "  fetched_at, outcome, usable, inserted, labels)"
                " VALUES (?, ?, ?, 'unscheduled', ?, ?, ?, 'stored', 1, 1, '{}')",
                (
                    site_id,
                    feed_id,
                    local_date.isoformat(),
                    ended_at,
                    ended_at,
                    ended_at,
                ),
            ).lastrowid
        )

    # Inserted first -> lowest id, but the OLDEST download of the three
    # (95 days); no value-change row at all.
    no_vc_download_id = _insert_download(no_vc_ended, D - timedelta(days=95))
    # Inserted second (89 days old).
    kept_download_id = _insert_download(kept_ended, D - timedelta(days=89))
    # Inserted third -> the HIGHEST id, despite being 91 days old --
    # chronologically older than kept_download_id, which has a lower id.
    old_download_id = _insert_download(old_ended, D - timedelta(days=91))
    assert old_download_id > kept_download_id > no_vc_download_id, (
        "the fixture's own id order must disagree with its age order, or"
        " this test cannot discriminate an id-ordered rewrite from the"
        " correct ended_at-ordered delete"
    )

    conn.execute(
        "INSERT INTO forecast_value_changes"
        " (download_id, previous_download_id, feed_id, variable,"
        "  hours_compared, hours_changed, max_abs_change,"
        "  first_changed_valid_at, labels_changed)"
        " VALUES (?, NULL, ?, 'temperature', 1, 0, 0.0, NULL, 0)",
        (kept_download_id, feed_id),
    )
    conn.execute(
        "INSERT INTO forecast_value_changes"
        " (download_id, previous_download_id, feed_id, variable,"
        "  hours_compared, hours_changed, max_abs_change,"
        "  first_changed_valid_at, labels_changed)"
        " VALUES (?, NULL, ?, 'temperature', 1, 0, 0.0, NULL, 0)",
        (old_download_id, feed_id),
    )
    conn.commit()

    d_minus_1 = D - timedelta(days=1)
    asyncio.run(
        publish(db, writer, site_id=site_id, cutoff=d_minus_1, attempt=1, now_utc=now)
    )

    remaining = {
        row["download_id"]
        for row in conn.execute("SELECT download_id FROM forecast_value_changes")
    }
    assert old_download_id not in remaining, (
        "correct = the 91-day-old row deleted (ended_at < the 90-day"
        " cutoff, regardless of its id being the HIGHEST of the three),"
        f" old statement (an id-ordered rewrite) = still present"
        f" (ids remaining: {sorted(remaining)!r})"
    )
    assert remaining == {kept_download_id}, (
        "correct = only the 89-day-old row remains (the no-value-change"
        " download never had a row to begin with, and is untouched),"
        f" old statement (an id-ordered rewrite) = {sorted(remaining)!r}"
    )

    download_count = conn.execute(
        "SELECT COUNT(*) FROM forecast_downloads WHERE id IN (?, ?, ?)",
        (no_vc_download_id, kept_download_id, old_download_id),
    ).fetchone()[0]
    assert download_count == 3, (
        "correct = all three forecast_downloads rows untouched (the swap"
        " deletes only from forecast_value_changes), old statement ="
        f" {download_count} rows found"
    )


def test_vc7_the_delete_plan_searches_forecast_downloads_by_primary_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VC7(ii): ``_DELETE_OLD_VALUE_CHANGES_SQL``'s own query plan, pinned
    directly -- the list subquery must never walk ``forecast_downloads``
    (a scan whose cost grows with every download ever stored, since
    nothing else prunes that table) and must instead seek it by its
    integer primary key from the delete log's own minimum ``download_id``.
    Matched on table name plus SCAN/SEARCH and PRIMARY KEY only, never the
    whole plan line, since the shipping SQLite build differs from the dev
    one (project convention, ``tests/test_wind_pair_max.py`` and others).
    """
    from wxverify.scoring.publication import _DELETE_OLD_VALUE_CHANGES_SQL

    db, _writer, _site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001
    stamp = _stamp(datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC))

    plan_rows = conn.execute(
        f"EXPLAIN QUERY PLAN {_DELETE_OLD_VALUE_CHANGES_SQL}", (stamp,)
    ).fetchall()
    details = [str(row["detail"]) for row in plan_rows]

    scans_downloads = [d for d in details if "forecast_downloads" in d and "SCAN" in d]
    assert scans_downloads == [], (
        "correct = no plan line walks forecast_downloads (the list"
        " subquery seeks it by rowid from the delete log's own minimum"
        " download_id), old statement = a SCAN line naming"
        f" forecast_downloads (e.g. its covering index), got {details!r}"
    )

    searches_by_pk = [
        d
        for d in details
        if "forecast_downloads" in d and "SEARCH" in d and "PRIMARY KEY" in d
    ]
    assert searches_by_pk, (
        "correct = a SEARCH line naming forecast_downloads USING"
        " (INTEGER) PRIMARY KEY, old statement = no such line (only the"
        f" full-table SCAN), got {details!r}"
    )

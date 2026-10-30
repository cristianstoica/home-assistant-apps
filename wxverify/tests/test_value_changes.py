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
from wxverify.db.queue import claim_next_job, enqueue_if_absent
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
    download -- round, daytime or unscheduled")."""
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
    label: str, base_valid: datetime, *, temperature: float, wind: float
) -> ProviderReply:
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
                temperature,
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
        SOURCE, _two_variable_reply(label, base_valid, temperature=10.5, wind=5.0)
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

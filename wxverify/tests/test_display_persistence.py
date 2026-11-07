"""Tests for card 7's display-only variables (``cloud_cover``, ``wind_gust``,
``wind_direction``) staying out of everything the scored pipeline counts.

Covers ``persist_fetch_result`` (``wxverify/collection/forecast_fetcher.py``)
counting only scored-roster samples in ``usable``/``labels``, and
``_reply_cells`` (``wxverify/worker/feed_fetch.py``) skipping display samples
before the value-change log is written.

Every download is driven through the real job pipeline (``claim_next_job``/
``run_claimed_job``), copying ``tests/test_value_changes.py``'s ``_setup``
(:58), ``_run_round`` (:80) and ``_run_unscheduled`` (:105) -- there is no
pure function to seed a download's samples against directly. Synthetic data
only: site "S" in UTC, feed X's synthetic source/model pair
(``tests.helpers.seed_round_schedule_feeds``) and the real seeded
``meteoblue``/``multimodel`` package row, as the sibling test files already
use. Each test's docstring says in one line which breakage it catches.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    ROUND_SCHEDULE_SOURCES,
    STANDARD_NIGHT_R,
    D,
    MovingClock,
    ProviderReply,
    disable_other_enqueuers,
    install_provider_standin,
    meteoblue_member_sample,
    patch_close_clock,
    round_schedule_sample,
    seed_round_schedule_feeds,
    standard_night,
    subscribe_meteoblue_package,
)
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.collection.forecast_validation import DISPLAY_VARIABLES
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.queue import claim_next_job, complete, enqueue_if_absent
from wxverify.feeds.seam import NormalizedSample
from wxverify.worker.processor import run_claimed_job

R = STANDARD_NIGHT_R  # 2024-06-11 (standard night: R = D + 1)
SOURCE = ROUND_SCHEDULE_SOURCES["X"]


# ---------------------------------------------------------------------------
# Local helpers, copied from tests/test_value_changes.py's own (:58, :80,
# :105) -- that file is another agent's and is left untouched.
# ---------------------------------------------------------------------------


def _stamp(dt: datetime) -> str:
    return isoformat_utc(dt)


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Standard night plus feed X, the provider stand-in and a moving clock
    patched over the close-chain modules. Returns ``(db, conn, site_id,
    feed_id, standin, clock)`` with only X subscribed.
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


def _setup_meteoblue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Standard night plus the real meteoblue package feed subscribed, the
    provider stand-in and a moving clock. Returns ``(db, conn, site_id,
    package_feed_id, standin, clock)``.
    """
    db, _writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001
    package_id = subscribe_meteoblue_package(conn, site_id)
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    clock = MovingClock(datetime(D.year, D.month, D.day, 17, 0, tzinfo=UTC))
    patch_close_clock(monkeypatch, clock)
    return db, conn, site_id, package_id, standin, clock


def _run_round(db, conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """One round try (round_try 1, round_date R) for ``feed_id``, claimed
    and run to completion."""
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
    """One unscheduled fetch for ``feed_id``, claimed and run to completion."""
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


def _last_download_id(conn: sqlite3.Connection, feed_id: int) -> int:
    row = conn.execute(
        "SELECT id FROM forecast_downloads WHERE feed_id=? ORDER BY id DESC LIMIT 1",
        (feed_id,),
    ).fetchone()
    assert row is not None, f"no forecast_downloads row found for feed_id={feed_id}"
    return int(row["id"])


def _download_row(conn: sqlite3.Connection, download_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM forecast_downloads WHERE id=?", (download_id,)
    ).fetchone()
    assert row is not None, f"no forecast_downloads row found for id={download_id}"
    return row


def _download_labels(
    conn: sqlite3.Connection, download_id: int
) -> dict[str, list[str]]:
    row = _download_row(conn, download_id)
    parsed = json.loads(str(row["labels"]))
    assert isinstance(parsed, dict)
    return parsed


def _scored_rows(conn: sqlite3.Connection, feed_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        "SELECT * FROM forecast_scored WHERE feed_id=?"
        " ORDER BY variable, valid_at, day_ahead",
        (feed_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _value_change_rows(conn: sqlite3.Connection, download_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM forecast_value_changes"
            " WHERE download_id=? ORDER BY variable",
            (download_id,),
        )
    )


def _last_usable_fetch_at(conn: sqlite3.Connection, site_id: int, feed_id: int):
    row = conn.execute(
        "SELECT last_usable_fetch_at FROM site_feed_state"
        " WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    assert row is not None, "no site_feed_state row found"
    return row["last_usable_fetch_at"]


def _last_error(conn: sqlite3.Connection, site_id: int, feed_id: int):
    row = conn.execute(
        "SELECT last_error FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    assert row is not None, "no site_feed_state row found"
    return row["last_error"]


def _scored_reply(
    label: str, base_valid: datetime, *, temperature: float, wind: float
) -> tuple[NormalizedSample, ...]:
    """Temperature and wind at two consecutive hours (leads 6 and 7)."""
    return (
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
            SOURCE, "wind", label, _stamp(base_valid + timedelta(hours=1)), 7, wind
        ),
    )


def _display_samples(
    label: str,
    base_valid: datetime,
    *,
    cloud_cover: float,
    wind_gust: float,
    wind_direction: float,
) -> tuple[NormalizedSample, ...]:
    """The three display variables (card 7) at the same two hours, valid
    values of each's own range (``DISPLAY_VALUE_RANGES``)."""
    hours = (base_valid, base_valid + timedelta(hours=1))
    leads = (6, 7)
    samples: list[NormalizedSample] = []
    for valid_at, lead in zip(hours, leads, strict=True):
        samples.append(
            round_schedule_sample(
                SOURCE, "cloud_cover", label, _stamp(valid_at), lead, cloud_cover
            )
        )
        samples.append(
            round_schedule_sample(
                SOURCE, "wind_gust", label, _stamp(valid_at), lead, wind_gust
            )
        )
        samples.append(
            round_schedule_sample(
                SOURCE, "wind_direction", label, _stamp(valid_at), lead, wind_direction
            )
        )
    return tuple(samples)


# ---------------------------------------------------------------------------
# Step 3, bullet 1: two fresh files, same scored reply, one with display
# samples added.
# ---------------------------------------------------------------------------


def test_display_samples_added_do_not_move_scored_usable_labels_or_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: ``usable``/``labels`` counting display samples, or a
    display row reaching ``forecast_scored`` -- any of which would make the
    two files' rows differ though their scored reply is identical.

    ``standard_night`` closes the process-global ``Database`` before
    opening its own (``db/connection.py:init_db``), so building file2
    would close file1's connection -- every value read from file1 is
    captured before file2 is built, never read from ``conn1`` afterward.
    """
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    base_valid = datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)
    scored = _scored_reply(label, base_valid, temperature=10.0, wind=5.0)
    display = _display_samples(
        label, base_valid, cloud_cover=42.0, wind_gust=12.5, wind_direction=270.0
    )

    db1, conn1, site_id1, feed_id1, standin1, _clock1 = _setup(
        tmp_path / "file1", monkeypatch
    )
    standin1.program(SOURCE, ProviderReply(samples=scored))
    _run_round(db1, conn1, site_id1, feed_id1)

    download1 = _download_row(conn1, _last_download_id(conn1, feed_id1))
    download1_fields = {
        field: download1[field] for field in ("usable", "labels", "outcome")
    }
    last_usable_1 = _last_usable_fetch_at(conn1, site_id1, feed_id1)
    scored1 = _scored_rows(conn1, feed_id1)

    db2, conn2, site_id2, feed_id2, standin2, _clock2 = _setup(
        tmp_path / "file2", monkeypatch
    )
    standin2.program(SOURCE, ProviderReply(samples=scored + display))
    _run_round(db2, conn2, site_id2, feed_id2)

    # Non-vacuous: file2 really holds the display rows.
    display_count = conn2.execute(
        "SELECT COUNT(*) FROM forecast_samples WHERE feed_id=?"
        " AND variable IN ('cloud_cover', 'wind_gust', 'wind_direction')",
        (feed_id2,),
    ).fetchone()[0]
    assert display_count == 6, (
        f"expected 6 display rows (3 variables x 2 hours) in file2, got {display_count}"
    )

    download2 = _download_row(conn2, _last_download_id(conn2, feed_id2))
    for field, value1 in download1_fields.items():
        value2 = download2[field]
        assert value1 == value2, (
            f"download.{field} differs: file1={value1!r}, file2={value2!r}"
            " -- display samples must not move it"
        )

    assert last_usable_1 == _last_usable_fetch_at(conn2, site_id2, feed_id2), (
        "last_usable_fetch_at differs between the two files"
    )

    scored2 = _scored_rows(conn2, feed_id2)
    assert scored1 == scored2, (
        f"forecast_scored rows differ: file1={scored1!r}, file2={scored2!r}"
    )
    assert len(scored1) > 0, "the scored reply must have produced scored rows"


# ---------------------------------------------------------------------------
# Step 3, bullet 1 (second half): a second round changing only display
# values writes no forecast_value_changes row for a display variable, with
# a paired positive that a changed scored value does.
# ---------------------------------------------------------------------------


def test_second_round_changing_only_display_values_logs_no_display_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: ``_reply_cells`` without its scored-roster filter, which
    would start comparing display samples and write a
    ``forecast_value_changes`` row whose ``variable`` is a display name.
    """
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    base_valid = datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)

    scored = _scored_reply(label, base_valid, temperature=10.0, wind=5.0)
    display_1 = _display_samples(
        label, base_valid, cloud_cover=42.0, wind_gust=12.5, wind_direction=270.0
    )
    standin.program(SOURCE, ProviderReply(samples=scored + display_1))
    _run_round(db, conn, site_id, feed_id)

    # Round 2: same scored values, different display values only.
    display_2 = _display_samples(
        label, base_valid, cloud_cover=80.0, wind_gust=30.0, wind_direction=10.0
    )
    standin.program(SOURCE, ProviderReply(samples=scored + display_2))
    _run_unscheduled(db, conn, site_id, feed_id)
    second_id = _last_download_id(conn, feed_id)
    rows_round2 = _value_change_rows(conn, second_id)

    variables_round2 = {row["variable"] for row in rows_round2}
    assert variables_round2 == {"temperature", "wind"}, (
        "non-vacuous: the comparison must still write rows for the scored"
        f" variables even though display values changed; got {variables_round2!r}"
    )
    assert not (variables_round2 & set(DISPLAY_VARIABLES)), (
        f"a forecast_value_changes row named a display variable: {variables_round2!r}"
    )
    for row in rows_round2:
        assert row["hours_changed"] == 0, (
            "the scored values did not change between round 1 and round 2, so"
            f" hours_changed must be 0 for {row['variable']!r}, got"
            f" {row['hours_changed']!r}"
        )

    # Paired positive: a round that DOES change a scored value writes a
    # changed row, so the absence above is not because nothing is compared.
    changed_scored = _scored_reply(label, base_valid, temperature=15.0, wind=5.0)
    standin.program(SOURCE, ProviderReply(samples=changed_scored + display_2))
    _run_unscheduled(db, conn, site_id, feed_id)
    third_id = _last_download_id(conn, feed_id)
    rows_round3 = {row["variable"]: row for row in _value_change_rows(conn, third_id)}
    assert rows_round3["temperature"]["hours_changed"] == 2, (
        "paired positive: a changed temperature must be logged as changed,"
        f" got {rows_round3['temperature']['hours_changed']!r}"
    )


# ---------------------------------------------------------------------------
# Step 3, bullet 2: a display-only reply is a no_usable download, blocking
# nothing of the day's first scored write for a later scored reply.
# ---------------------------------------------------------------------------


def test_display_only_reply_is_no_usable_and_does_not_block_a_later_scored_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: display samples counted in ``usable``/``labels``, which
    would make the display-only download the day's first and block its
    later scored rows from being written.
    """
    db, conn, site_id, feed_id, standin, _clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    base_valid = datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)

    display_only = _display_samples(
        label, base_valid, cloud_cover=42.0, wind_gust=12.5, wind_direction=270.0
    )
    standin.program(SOURCE, ProviderReply(samples=display_only))
    _run_round(db, conn, site_id, feed_id)

    assert _last_error(conn, site_id, feed_id) == NO_USABLE_SAMPLES_SENTINEL, (
        "a display-only reply must stamp the no-usable-samples sentinel"
    )
    first_id = _last_download_id(conn, feed_id)
    first_download = _download_row(conn, first_id)
    assert first_download["outcome"] == "no_usable", (
        f"expected outcome='no_usable', got {first_download['outcome']!r}"
    )
    labels = _download_labels(conn, first_id)
    assert labels.get(str(feed_id), []) == [], (
        f"expected empty labels for the display-only download, got {labels!r}"
    )
    assert _scored_rows(conn, feed_id) == [], (
        "a display-only reply must write no forecast_scored rows"
    )

    scored = _scored_reply(label, base_valid, temperature=10.0, wind=5.0)
    standin.program(SOURCE, ProviderReply(samples=scored))
    _run_unscheduled(db, conn, site_id, feed_id)

    scored_rows = _scored_rows(conn, feed_id)
    assert len(scored_rows) > 0, (
        "a later same-day scored reply must still write its forecast_scored"
        " rows -- the earlier display-only download must not have claimed"
        " the day's first-download slot"
    )


# ---------------------------------------------------------------------------
# Step 3, bullet 2 (meteoblue package): one member fully scored, one member
# display-only.
# ---------------------------------------------------------------------------


def test_meteoblue_package_member_display_only_blocks_until_a_later_scored_reply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: a meteoblue package member's display samples counted in
    ``usable``/``labels``, which would block that member's later scored
    reply from writing its ``forecast_scored`` rows.
    """
    db, conn, site_id, package_id, standin, _clock = _setup_meteoblue(
        tmp_path, monkeypatch
    )
    label = _stamp(datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC))
    base_valid = datetime(R.year, R.month, R.day, 6, 0, tzinfo=UTC)
    valid_at_6 = _stamp(base_valid)
    valid_at_7 = _stamp(base_valid + timedelta(hours=1))

    member_a_samples = (
        meteoblue_member_sample("A", "temperature", label, valid_at_6, 6, 10.0),
        meteoblue_member_sample("A", "temperature", label, valid_at_7, 7, 10.0),
        meteoblue_member_sample("A", "wind", label, valid_at_6, 6, 5.0),
        meteoblue_member_sample("A", "wind", label, valid_at_7, 7, 5.0),
    )
    member_b_display_samples = (
        meteoblue_member_sample("B", "cloud_cover", label, valid_at_6, 6, 42.0),
        meteoblue_member_sample("B", "wind_gust", label, valid_at_6, 6, 12.5),
        meteoblue_member_sample("B", "wind_direction", label, valid_at_6, 6, 270.0),
    )
    standin.program(
        "meteoblue",
        ProviderReply(samples=member_a_samples + member_b_display_samples),
    )
    _run_round(db, conn, site_id, package_id)

    member_a_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteoblue' AND model='A'"
        ).fetchone()["id"]
    )
    member_b_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteoblue' AND model='B'"
        ).fetchone()["id"]
    )

    first_download_id = _last_download_id(conn, package_id)
    labels = _download_labels(conn, first_download_id)
    assert labels.get(str(member_b_id), []) == [], (
        f"expected member B's labels to be empty, got {labels!r}"
    )
    assert labels.get(str(member_a_id), []) != [], (
        "non-vacuous: member A (fully scored) must have a non-empty label,"
        " proving the empty list above is not a blanket default"
    )
    assert _scored_rows(conn, member_b_id) == [], (
        "member B (display-only) must write no forecast_scored rows"
    )
    assert _scored_rows(conn, member_a_id) != [], (
        "member A (fully scored) must write its forecast_scored rows"
    )

    member_b_scored_samples = (
        meteoblue_member_sample("B", "temperature", label, valid_at_6, 6, 12.0),
        meteoblue_member_sample("B", "temperature", label, valid_at_7, 7, 12.0),
        meteoblue_member_sample("B", "wind", label, valid_at_6, 6, 6.0),
        meteoblue_member_sample("B", "wind", label, valid_at_7, 7, 6.0),
    )
    standin.program("meteoblue", ProviderReply(samples=member_b_scored_samples))
    _run_unscheduled(db, conn, site_id, package_id)

    assert _scored_rows(conn, member_b_id) != [], (
        "member B's later same-day scored reply must write its"
        " forecast_scored rows -- the earlier display-only round must not"
        " have claimed the day's first-download slot for member B"
    )

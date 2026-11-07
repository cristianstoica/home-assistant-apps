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
import logging
import math
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
from wxverify.collection.forecast_fetcher import (
    NO_USABLE_SAMPLES_SENTINEL,
    persist_fetch_result,
    register_feed_if_needed,
)
from wxverify.collection.forecast_validation import DISPLAY_VARIABLES
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import claim_next_job, complete, enqueue_if_absent
from wxverify.feeds.seam import FetchResult, NormalizedSample
from wxverify.provider_ops import bad_sample_count_sql
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


# ---------------------------------------------------------------------------
# §8 Part H (plan 2026-11-07-health-release.md): out-of-range display
# values dropped before they reach forecast_samples. H1-H4 drive
# persist_fetch_result directly on a bare conn (no job pipeline needed):
# the drop lives entirely inside that function, and a fresh site/feed is
# all it reads.
# ---------------------------------------------------------------------------


def _fresh_feed_conn(model: str) -> tuple[sqlite3.Connection, int, int]:
    """A fresh ``:memory:`` conn, one UTC site and one meteoblue feed
    registered under ``model``. Returns ``(conn, site_id, feed_id)``."""
    conn = sqlite3.connect(":memory:", cached_statements=0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('PartHSite', 10.0, 20.0, 100.0, 'UTC')"
        ).lastrowid
    )
    feed_id = register_feed_if_needed(
        conn, "meteoblue", model, fetch_interval_minutes=1440
    )
    conn.commit()
    return conn, site_id, feed_id


def _h_persist(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    samples: tuple[NormalizedSample, ...],
):
    return persist_fetch_result(
        conn,
        site_id=site_id,
        source="meteoblue",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=list(samples)),
        fetched_at="2026-01-01T00:00:00Z",
    )


def _dropped_records(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == "wxverify.collection.forecast_fetcher"
        and record.getMessage().startswith("display values out of range dropped")
    ]


def test_h1_out_of_range_cloud_cover_dropped_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """H1 (plan §8), the owner's case. Catches: no drop (every row would
    persist); ``<``/``<=`` swapped (the in-bound 0/100 rows dropped
    instead of the out-of-range ones); one log line written per sample
    instead of one per call; low and high swapped in the message; the
    line logged at DEBUG instead of INFO.
    """
    conn, site_id, feed_id = _fresh_feed_conn("H1")
    label = "2026-01-01T00:00:00Z"
    cloud_samples = tuple(
        meteoblue_member_sample(
            "H1", "cloud_cover", label, f"2026-01-01T0{hour}:00:00Z", 6 + hour, value
        )
        for hour, value in enumerate((-4.0, 0.0, 100.0, 106.0))
    )
    scored_sample = meteoblue_member_sample(
        "H1", "temperature", label, "2026-01-01T10:00:00Z", 6, 10.0
    )

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.collection.forecast_fetcher"):
        _h_persist(conn, site_id, feed_id, cloud_samples + (scored_sample,))

    stored = sorted(
        row["value"]
        for row in conn.execute(
            "SELECT value FROM forecast_samples WHERE feed_id=?"
            " AND variable='cloud_cover'",
            (feed_id,),
        )
    )
    assert stored == [0.0, 100.0], (
        f"expected only the in-range cloud_cover rows stored, got {stored!r}"
    )

    records = _dropped_records(caplog)
    assert len(records) == 1, f"expected exactly one drop record, got {len(records)!r}"
    assert records[0].getMessage() == (
        "display values out of range dropped source=meteoblue"
        " cloud_cover count=2 low=-4 high=106"
    )


def test_h2_two_fields_out_of_range_in_one_reply_write_one_line_naming_both(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """H2 (plan §8), two fields, one line. Catches: a separate log line
    written per dropped field instead of one line naming all of them; a
    drop record written even when every display value in a reply is in
    range (the paired positive below); the drop placed before the
    ``labels`` entry -- the first reply's feed, whose only samples are
    display values that all get dropped, would then map to something
    other than an empty tuple.
    """
    conn, site_id, feed_id = _fresh_feed_conn("H2")
    label = "2026-01-01T00:00:00Z"
    samples = (
        meteoblue_member_sample(
            "H2", "cloud_cover", label, "2026-01-01T06:00:00Z", 6, 106.0
        ),
        meteoblue_member_sample(
            "H2", "wind_direction", label, "2026-01-01T07:00:00Z", 7, 361.0
        ),
    )

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.collection.forecast_fetcher"):
        outcome = _h_persist(conn, site_id, feed_id, samples)

    assert outcome.labels == {feed_id: ()}, (
        "the feed's only samples are display values, all dropped -- its"
        " labels entry must still map to an empty tuple"
    )

    records = _dropped_records(caplog)
    assert len(records) == 1, f"expected exactly one drop record, got {len(records)!r}"
    assert records[0].getMessage() == (
        "display values out of range dropped source=meteoblue"
        " cloud_cover count=1 low=106 high=106;"
        " wind_direction count=1 low=361 high=361"
    )

    # Paired positive: a reply with every display value in range writes no
    # such record -- the absence above is not because nothing was checked.
    conn2, site_id2, feed_id2 = _fresh_feed_conn("H2b")
    in_range_samples = (
        meteoblue_member_sample(
            "H2b", "cloud_cover", label, "2026-01-01T06:00:00Z", 6, 50.0
        ),
        meteoblue_member_sample(
            "H2b", "wind_direction", label, "2026-01-01T07:00:00Z", 7, 180.0
        ),
    )
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.collection.forecast_fetcher"):
        _h_persist(conn2, site_id2, feed_id2, in_range_samples)
    assert _dropped_records(caplog) == [], (
        "a reply with every display value in range must write no drop record"
    )


def test_h3_scored_out_of_range_values_still_stored_and_counted_as_bad() -> None:
    """H3 (plan §8), scored values unchanged. Catches: the display drop
    reaching scored-roster variables -- an out-of-range temperature or
    wind sample must still be stored (as today) and counted by
    ``bad_sample_count_sql``.
    """
    conn, site_id, feed_id = _fresh_feed_conn("H3")
    label = "2026-01-01T00:00:00Z"
    samples = (
        meteoblue_member_sample(
            "H3", "temperature", label, "2026-01-01T06:00:00Z", 6, 75.0
        ),
        meteoblue_member_sample("H3", "wind", label, "2026-01-01T07:00:00Z", 7, -1.0),
    )
    _h_persist(conn, site_id, feed_id, samples)

    stored = sorted(
        row["value"]
        for row in conn.execute(
            "SELECT value FROM forecast_samples WHERE feed_id=?", (feed_id,)
        )
    )
    assert stored == [-1.0, 75.0], (
        f"expected both out-of-range scored rows stored, got {stored!r}"
    )

    sql = bad_sample_count_sql("?")
    row = conn.execute(sql, (site_id, feed_id)).fetchone()
    assert int(row["bad_sample_count"]) == 2


def test_h4_out_of_range_display_values_move_nothing_else_nan_included(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """H4 (plan §8), nothing else moves, the NaN case included. Catches:
    the range test run before the NaN skip -- a NaN sample counted into
    the tally, or an out-of-range sample's drop blocked by a NaN check
    running first (caught here by the drop log, which the range test
    alone never reads: a NaN reaching the tally would show as
    ``count=2 low=nan high=106`` instead of ``count=1 low=106 high=106``).
    (The drop-placed-before-``labels``-entry case is H2's, not this one:
    member B here always has in-range ``wind_gust``/``wind_direction``
    samples that create its ``labels`` entry regardless of the cloud_cover
    drop's placement.)
    """
    label = "2026-01-01T00:00:00Z"

    def _build(cloud_value: float):
        conn, site_id, feed_id = _fresh_feed_conn("H4pkg")
        member_a = (
            meteoblue_member_sample(
                "H4A", "temperature", label, "2026-01-01T06:00:00Z", 6, 10.0
            ),
            meteoblue_member_sample(
                "H4A", "temperature", label, "2026-01-01T07:00:00Z", 7, 10.0
            ),
            meteoblue_member_sample(
                "H4A", "wind", label, "2026-01-01T06:00:00Z", 6, 5.0
            ),
            meteoblue_member_sample(
                "H4A", "wind", label, "2026-01-01T07:00:00Z", 7, 5.0
            ),
        )
        member_b = (
            meteoblue_member_sample(
                "H4B", "cloud_cover", label, "2026-01-01T06:00:00Z", 6, cloud_value
            ),
            meteoblue_member_sample(
                "H4B", "wind_gust", label, "2026-01-01T06:00:00Z", 6, 10.0
            ),
            meteoblue_member_sample(
                "H4B", "wind_direction", label, "2026-01-01T06:00:00Z", 6, 180.0
            ),
            meteoblue_member_sample(
                "H4B", "cloud_cover", label, "2026-01-01T07:00:00Z", 7, math.nan
            ),
        )
        caplog.clear()
        with caplog.at_level(
            logging.INFO, logger="wxverify.collection.forecast_fetcher"
        ):
            outcome = _h_persist(conn, site_id, feed_id, member_a + member_b)
        records = _dropped_records(caplog)
        return outcome, conn, site_id, feed_id, records

    (
        outcome_in_range,
        conn_in_range,
        site_id_in_range,
        feed_id_in_range,
        records_in_range,
    ) = _build(50.0)
    (
        outcome_out_of_range,
        conn_out_of_range,
        site_id_out_of_range,
        feed_id_out_of_range,
        records_out_of_range,
    ) = _build(106.0)

    assert records_in_range == [], (
        "the in-range variant's only cloud_cover values are 50 (in range)"
        " and NaN (skipped), so it must write no drop record"
    )
    assert len(records_out_of_range) == 1, (
        f"expected exactly one drop record, got {len(records_out_of_range)!r}"
    )
    assert records_out_of_range[0].getMessage() == (
        "display values out of range dropped source=meteoblue"
        " cloud_cover count=1 low=106 high=106"
    ), (
        "the NaN cloud_cover sample must not be counted into the tally --"
        " a count of 2 or a low/high of nan would mean the range test ran"
        " before the NaN skip"
    )

    assert (
        outcome_in_range.usable_sample_count == outcome_out_of_range.usable_sample_count
    ), "usable_sample_count must not move with the display drop"
    assert outcome_in_range.labels == outcome_out_of_range.labels, (
        "labels must not move with the display drop"
    )
    member_b_id = next(
        member_feed_id
        for member_feed_id, label_tuple in outcome_in_range.labels.items()
        if label_tuple == ()
    )
    assert outcome_in_range.labels[member_b_id] == (), (
        "member B (display-only) must map to an empty labels tuple, dropped"
        " display rows included"
    )

    def _state(conn: sqlite3.Connection, site_id: int, feed_id: int):
        row = conn.execute(
            "SELECT last_error, last_usable_fetch_at FROM site_feed_state"
            " WHERE site_id=? AND feed_id=?",
            (site_id, feed_id),
        ).fetchone()
        assert row is not None
        return row["last_error"], row["last_usable_fetch_at"]

    assert _state(conn_in_range, site_id_in_range, feed_id_in_range) == _state(
        conn_out_of_range, site_id_out_of_range, feed_id_out_of_range
    ), "last_error/last_usable_fetch_at must not move with the display drop"

    # The NaN cloud_cover sample is skipped, never tallied, in both variants:
    # only the real out-of-range 106 value (out_of_range variant) is dropped.
    cloud_rows_out_of_range = conn_out_of_range.execute(
        "SELECT COUNT(*) FROM forecast_samples"
        " WHERE feed_id=? AND variable='cloud_cover'",
        (member_b_id,),
    ).fetchone()[0]
    assert cloud_rows_out_of_range == 0, (
        "the out-of-range 106 row must not be stored, and the NaN row must"
        " be skipped rather than stored either"
    )
    cloud_rows_in_range = conn_in_range.execute(
        "SELECT COUNT(*) FROM forecast_samples"
        " WHERE feed_id=? AND variable='cloud_cover'",
        (member_b_id,),
    ).fetchone()[0]
    assert cloud_rows_in_range == 1, (
        "the in-range 50 row must be stored; the NaN row is skipped, so"
        " only one cloud_cover row reaches forecast_samples"
    )

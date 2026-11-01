"""Shared test helpers for wxverify's pytest suite.

Kept deliberately small: only helpers actually reused across multiple test
modules belong here. Anything used by a single file stays local to it.
"""

from __future__ import annotations

import contextlib
import hashlib
import inspect
import json
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.core.units import ms_to_kmh
from wxverify.db.connection import (  # noqa: SLF001
    _READ_POOL_SIZE,
    Database,
    FencedWriter,
    close_db,
    init_db,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import claim_next_job
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.switch import ensure_site_generation
from wxverify.feeds.seam import CostEstimate, FetchResult, NormalizedSample
from wxverify.scoring.consensus import materialize_consensus, write_station_observation
from wxverify.verification.decision import ContinuousLead, OccurrenceLead
from wxverify.verification.methodology import (
    CONTINUOUS_BASELINES,
    OCCURRENCE_BASELINES,
)
from wxverify.worker.processor import run_claimed_job
from wxverify.worker.scheduler import scheduler_tick


def occurrence_baseline_set(
    per_lead: Mapping[int, OccurrenceLead],
) -> dict[str, dict[int, OccurrenceLead]]:
    """Fan one series out over every required occurrence baseline (§8).

    The baseline gate validates a required SET, so a fixture that supplies
    only one baseline no longer reaches a verdict. Fixtures whose subject
    is not the gate use this to satisfy the set with a single series.
    """
    return {
        name: {lead: dict(series) for lead, series in per_lead.items()}
        for name in OCCURRENCE_BASELINES
    }


def continuous_baseline_set(
    quantity: str, per_lead: Mapping[int, ContinuousLead]
) -> dict[str, dict[str, dict[int, ContinuousLead]]]:
    """Fan one series out over every required continuous baseline (§8)."""
    return {
        name: {quantity: {lead: dict(series) for lead, series in per_lead.items()}}
        for name in CONTINUOUS_BASELINES
    }


def assert_read_pool_at_rest(db: Database) -> None:
    """Assert the read pool is back to its steady state: exactly
    ``_READ_POOL_SIZE`` connections queued, all of them distinct objects.

    Only valid once every dispatched read or drain has actually settled --
    calling this while a connection is still checked out (a read in flight,
    or a drain mid-cancellation-recovery) fails even against a correct
    implementation, since the pool is legitimately short during that window.
    A new recovery branch that forgets to publish a connection back (or
    publishes the same one twice) has nowhere to hide from this check.
    """
    pool = db._read_pool  # noqa: SLF001
    assert pool.qsize() == _READ_POOL_SIZE, (
        f"expected {_READ_POOL_SIZE} connections at rest, found {pool.qsize()}"
    )
    conns = list(pool._queue)  # noqa: SLF001
    ids = {id(conn) for conn in conns}
    assert len(ids) == len(conns) == _READ_POOL_SIZE, (
        "pooled connections must all be distinct objects -- a duplicate id "
        "means the same connection was published to the pool more than once"
    )


class _TagCollector(HTMLParser):
    def __init__(self, tag_name: str) -> None:
        super().__init__()
        self._tag_name = tag_name
        self.matches: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == self._tag_name:
            self.matches.append({key: value or "" for key, value in attrs})


def collect_tags(html: str, tag_name: str) -> list[dict[str, str]]:
    """Return the attribute dict of every ``<tag_name>`` element in ``html``,
    in document order.
    """
    parser = _TagCollector(tag_name)
    parser.feed(html)
    return parser.matches


class _DivNestingParser(HTMLParser):
    """Tracks ``<div>`` open/close depth to answer one question: is the
    element with ``id == target_id`` nested inside a ``<div>`` carrying
    ``ancestor_attr``?
    """

    def __init__(self, target_id: str, ancestor_attr: str) -> None:
        super().__init__()
        self._target_id = target_id
        self._ancestor_attr = ancestor_attr
        self._open_ancestor_flags: list[bool] = []
        self.found = False
        self.is_descendant = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "div":
            return
        attr_map = {key: value or "" for key, value in attrs}
        if attr_map.get("id") == self._target_id:
            self.found = True
            if any(self._open_ancestor_flags):
                self.is_descendant = True
        self._open_ancestor_flags.append(self._ancestor_attr in attr_map)

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._open_ancestor_flags:
            self._open_ancestor_flags.pop()


class _SummaryStatusParser(HTMLParser):
    """Finds the ``<p class="summary-status">`` inside ``<div id=summary_id>``
    and captures its attributes and text content.
    """

    def __init__(self, summary_id: str) -> None:
        super().__init__()
        self._summary_id = summary_id
        self._in_summary_div = False
        self._summary_div_depth = 0
        self._div_depth = 0
        self.attrs: dict[str, str] | None = None
        self.text = ""
        self._in_status_p = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        if tag == "div":
            self._div_depth += 1
            if attr_map.get("id") == self._summary_id:
                self._in_summary_div = True
                self._summary_div_depth = self._div_depth
        elif (
            tag == "p"
            and self._in_summary_div
            and "summary-status" in (attr_map.get("class", ""))
        ):
            self.attrs = attr_map
            self._in_status_p = True

    def handle_data(self, data: str) -> None:
        if self._in_status_p:
            self.text += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "p":
            self._in_status_p = False
        elif tag == "div":
            if self._in_summary_div and self._div_depth == self._summary_div_depth:
                self._in_summary_div = False
            self._div_depth -= 1


def assert_summary_status_pre_mounted_empty(html: str, *, summary_id: str) -> None:
    """Assert ``<div id=summary_id>`` already contains an EMPTY
    ``role="status" aria-live="polite"`` paragraph in the server-rendered
    HTML, not just something the client mounts after its first render.

    A live region created only by client-side JS on first update announces
    nothing on that very first update, because screen readers only pick up
    changes inside a region that already existed when the update happened --
    the template must render this node itself, empty, from the start.
    """
    parser = _SummaryStatusParser(summary_id)
    parser.feed(html)
    assert parser.attrs is not None, (
        f'no <p class="summary-status" ...> pre-mounted inside <div id="{summary_id}">'
    )
    assert parser.attrs.get("role") == "status"
    assert parser.attrs.get("aria-live") == "polite"
    assert parser.text == "", (
        f"summary-status node inside #{summary_id} must render EMPTY, "
        f"found text {parser.text!r}"
    )


def assert_summary_mount_not_nested_in_chart(html: str, *, summary_id: str) -> None:
    """Assert ``<div id=summary_id>`` exists in ``html`` and is never a
    descendant of a ``[data-chart]`` container.

    Chart containers get ``innerHTML = ""`` on every client-side render, so a
    summary mount living inside one would be destroyed on first paint -- this
    must hold at the template level, not just happen to be true today.
    """
    parser = _DivNestingParser(summary_id, "data-chart")
    parser.feed(html)
    assert parser.found, f'no <div id="{summary_id}"> found in the rendered HTML'
    assert not parser.is_descendant, (
        f'<div id="{summary_id}"> is nested inside a [data-chart] container -- '
        "it must be a sibling"
    )


# ---------------------------------------------------------------------------
# As-of / outcome-knowability fixture builders (plan §6, §18.1, §18.6).
# Shared by test_asof_leakage.py and test_asof_live_equivalence.py.
# All values are synthetic (fake site names, fake source/model ids).
# ---------------------------------------------------------------------------


def asof_conn() -> sqlite3.Connection:
    """Fresh fully-migrated in-memory database (real datastore, no mocks)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def asof_make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def asof_make_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 48)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def asof_persistence_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def asof_insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
    fetched_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-x', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, lead_hours, value, fetched_at),
    )


def asof_insert_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    issued_at: str,
    forecast: float,
    observed: float,
    first_known_at: str | None,
    day_ahead: int = 1,
    lead_hours: int = 6,
    generation_id: int | None = None,
) -> None:
    """Generation-bound pair insert for crafting knowability scenarios.

    ``generation_id=None`` binds the row to the site's PUBLISHED generation
    (seeding it on first use); pass an explicit id to plant decoys under a
    non-published generation.
    """
    if generation_id is None:
        generation_id = ensure_site_generation(conn, site_id)
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             first_known_at, tz_generation_id)
        VALUES (?, ?, 'temperature', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error**2,
            first_known_at,
            generation_id,
        ),
    )


def evidence_digest(conn: sqlite3.Connection, run_id: int) -> str:
    """sha256 over every ``verification_evidence`` and
    ``verification_day_context`` row for ``run_id`` (plan §5.2).
    """
    digest = hashlib.sha256()

    ev_cols = [
        r["name"] for r in conn.execute("PRAGMA table_info(verification_evidence)")
    ]
    quoted_ev = ", ".join(f"quote({c})" for c in ev_cols)
    for row in conn.execute(
        f"SELECT {quoted_ev} FROM verification_evidence"  # noqa: S608
        " WHERE run_id = ? ORDER BY id",
        (run_id,),
    ):
        digest.update("|".join(str(v) for v in row).encode())
        digest.update(b"\n")

    ctx_cols = [
        r["name"] for r in conn.execute("PRAGMA table_info(verification_day_context)")
    ]
    quoted_ctx = ", ".join(f"quote({c})" for c in ctx_cols)
    for row in conn.execute(
        f"SELECT {quoted_ctx} FROM verification_day_context"  # noqa: S608
        " WHERE run_id = ? ORDER BY snapshot_local_date",
        (run_id,),
    ):
        digest.update("|".join(str(v) for v in row).encode())
        digest.update(b"\n")

    return digest.hexdigest()


def asof_insert_observation(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    valid_at: str,
    value: float,
    computed_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', ?, ?, 3, ?)
        """,
        (site_id, valid_at, value, computed_at),
    )


def alive_after_wait(
    refs: Sequence[Callable[[], object]], timeout: float = 2.0
) -> list[bool]:
    """Which weak referents are still alive, after waiting up to ``timeout``
    seconds for all of them to die."""
    deadline = time.monotonic() + timeout
    while any(r() is not None for r in refs) and time.monotonic() < deadline:
        time.sleep(0.01)
    return [r() is not None for r in refs]


def seed_pair_max_wind(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    valid_at: str,
    value: float,
) -> None:
    """One pair-max station wind row (value in m/s) and its consensus hour."""
    write_station_observation(
        conn,
        station_id=station_id,
        variable="wind",
        valid_at=valid_at,
        value=value,
        source_raw=f"pair-max {round(ms_to_kmh(value), 2)} km/h",
    )
    materialize_consensus(conn, site_id=site_id, variable="wind", valid_at=valid_at)


# ---------------------------------------------------------------------------
# v8-shape station_wind_days (old-wind-method-removal plan §10.5). Shared by
# test_v9_old_wind_state_migration.py and test_db_transfer.py's D21: puts a
# migrated-to-8 database back in the exact v8 shape the owner's database has,
# with ``legacy_hours`` between ``pair_hours`` and ``attempts``.
# ---------------------------------------------------------------------------

_V8_STATION_WIND_DAYS_DDL = """
    CREATE TABLE station_wind_days (
        station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
        local_date TEXT NOT NULL,        -- YYYY-MM-DD in the site time zone
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK (status IN
                ('pending','partial','fetched','unavailable','failed')),
        record_count INTEGER NOT NULL DEFAULT 0,   -- records inside the day
        pair_hours INTEGER NOT NULL DEFAULT 0,     -- hours with a pair
        -- old wind rows on the day; NULL until counted
        legacy_hours INTEGER,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TEXT,
        -- always via core/error_sanitize.sanitized_exception
        last_error TEXT,
        last_ok_at TEXT,                 -- last successful persist, a 204 included
        refetch_at TEXT,                 -- reconcile refetch due time
        refetched INTEGER NOT NULL DEFAULT 0 CHECK (refetched IN (0, 1)),
        updated_at TEXT NOT NULL,
        PRIMARY KEY (station_id, local_date)
    )
"""


def make_v8_wind_state(conn: sqlite3.Connection) -> None:
    """Put a migrated database back in the v8 ``station_wind_days`` shape.

    Drops the table (safe: nothing references or indexes it, plan §6.9) and
    recreates it from a frozen copy of the v8 DDL, verbatim, so
    ``legacy_hours`` sits between ``pair_hours`` and ``attempts`` as on the
    owner's database -- not via ``ALTER TABLE ... ADD COLUMN``, which would
    put the column last. Asserts ``legacy_hours`` is present as an
    anti-vacuity guard, then sets ``PRAGMA user_version = 8``.
    """
    conn.execute("DROP TABLE station_wind_days")
    conn.execute(_V8_STATION_WIND_DAYS_DDL)
    columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(station_wind_days)")
    }
    assert "legacy_hours" in columns, "v8 DDL copy is missing legacy_hours"
    conn.execute("PRAGMA user_version = 8")


# ---------------------------------------------------------------------------
# Daily-close day-state fixture (plan 2026-10-20-daily-close-day-state-and-
# guard.md §13.1). Station ids are synthetic (ISTATION01, ISTATION02, ...),
# zones are UTC/Etc/GMT only, and D is a fixed synthetic date well in the
# past so the real clock never catches up with a test's faked one.
# ---------------------------------------------------------------------------

D: Final = date(2024, 6, 10)
NOW: Final = datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC)

_DAY_CLOSE_STATION_A = "ISTATION01"
_DAY_CLOSE_STATION_B = "ISTATION02"

_day_close_conns: list[sqlite3.Connection] = []
_day_close_latest_clock: datetime = NOW


def day_close_site(
    conn: sqlite3.Connection,
    *,
    timezone: str = "UTC",
    close_on: bool = True,
    with_records: bool = True,
    station_ids: tuple[str, str] = (_DAY_CLOSE_STATION_A, _DAY_CLOSE_STATION_B),
) -> int:
    """Build the daily-close plan's shared fixture site (plan §13.1).

    Makes a site under ``timezone`` with two enabled stations, A and B,
    defaulting to ``ISTATION01`` and ``ISTATION02`` (override with
    ``station_ids``), each seeded with an ``added`` station_changes row
    dated D-10 (``reason="test_seed"``). With ``close_on``, sets
    ``DAILY_CLOSE_ON_KEY``. With ``with_records``, also starts the site at
    D-3 and fills through its next local date, then moves D-3 to
    ``closed`` and D-2 to ``missed`` (``never_attempted``), each 30 minutes
    after that day's own ``day_end_utc``.
    """
    # wxverify.db.site_days arrives in Step 4 piece 2 of the daily-close
    # plan; importing it inside this function body, rather than at module
    # top, keeps every other test module that imports this file collecting
    # until then.
    from wxverify.db.site_days import (
        DAILY_CLOSE_ON_KEY,
        begin_close,
        ensure_site_days,
        finish_close,
        mark_missed,
        start_site_days,
    )

    site_id = asof_make_site(conn, "day-close-site")
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (timezone, site_id))
    ensure_site_generation(conn, site_id)

    seed_date = (D - timedelta(days=10)).isoformat()
    seed_changed_at = isoformat_utc_micro(
        datetime(D.year, D.month, D.day, tzinfo=UTC) - timedelta(days=10)
    )
    for pws_id in station_ids:
        cur = conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
            VALUES (?, ?, 0.0, 0.0, 0.0, 1)
            """,
            (site_id, pws_id),
        )
        assert cur.lastrowid is not None
        station_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO station_changes
                (site_id, station_id, change, effective_date, reason, changed_at)
            VALUES (?, ?, 'added', ?, 'test_seed', ?)
            """,
            (site_id, station_id, seed_date, seed_changed_at),
        )

    if close_on:
        set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))

    if with_records:
        d_minus_3 = D - timedelta(days=3)
        start_site_days(
            conn,
            site_id,
            d_minus_3,
            now_utc=datetime(
                d_minus_3.year, d_minus_3.month, d_minus_3.day, tzinfo=UTC
            ),
        )
        ensure_site_days(conn, site_id, now_utc=NOW)

        d3_row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d_minus_3.isoformat()),
        ).fetchone()
        assert d3_row is not None
        t1 = parse_utc(d3_row[0]) + timedelta(minutes=30)
        begin_close(conn, site_id, d_minus_3, now_utc=t1)
        finish_close(conn, site_id, d_minus_3, now_utc=t1)

        d_minus_2 = D - timedelta(days=2)
        d2_row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d_minus_2.isoformat()),
        ).fetchone()
        assert d2_row is not None
        t2 = parse_utc(d2_row[0]) + timedelta(minutes=30)
        mark_missed(conn, site_id, d_minus_2, now_utc=t2, reason="never_attempted")

    _day_close_conns.append(conn)
    return site_id


def note_day_clock(t: datetime) -> datetime:
    """Raise the helper's latest-known test clock to ``t`` when later, and
    return ``t`` unchanged (plan §13.1).

    Any ``now_utc`` later than :data:`NOW` that reaches ``ensure_site_days``
    -- directly, or through the guard -- must be passed through this first
    (``now_utc=note_day_clock(t)`` works inline), so
    :func:`check_day_close_records` knows how far forward new records are
    allowed to run. A forgotten call can only fail an otherwise-correct
    test: without it the bound stays at :data:`NOW`, which is always too
    early, never late enough to let a wrong fill pass.
    """
    global _day_close_latest_clock  # noqa: PLW0603
    if t > _day_close_latest_clock:
        _day_close_latest_clock = t
    return t


def check_day_close_records() -> None:
    """Assert every connection :func:`day_close_site` built holds no
    ``site_days`` record dated past the bound the test's clocks allow
    (plan §13.1).

    Imports nothing from ``wxverify.db.site_days``, so it shares no bug with
    the code under test: every read here is plain SQL against the
    ``site_days`` table.
    """
    global _day_close_latest_clock  # noqa: PLW0603
    try:
        clock_stamp = isoformat_utc(_day_close_latest_clock)
        for conn in _day_close_conns:
            try:
                site_rows = conn.execute(
                    "SELECT DISTINCT site_id FROM site_days"
                ).fetchall()
            except sqlite3.ProgrammingError:
                continue  # the test closed this connection; nothing to check
            for (site_id,) in site_rows:
                tz_row = conn.execute(
                    "SELECT timezone FROM sites WHERE id = ?", (site_id,)
                ).fetchone()
                if tz_row is None:
                    continue  # the test deleted the site itself
                bounds = [
                    _day_close_latest_clock.astimezone(ZoneInfo(tz_row[0])).date()
                ]
                holder = conn.execute(
                    """
                    SELECT local_date FROM site_days
                    WHERE site_id = ?
                      AND julianday(day_start_utc) <= julianday(?)
                      AND julianday(?) < julianday(day_end_utc)
                    """,
                    (site_id, clock_stamp, clock_stamp),
                ).fetchone()
                if holder is not None:
                    bounds.append(date.fromisoformat(holder[0]))
                bound = max(bounds) + timedelta(days=1)
                latest = conn.execute(
                    "SELECT MAX(local_date) FROM site_days WHERE site_id = ?",
                    (site_id,),
                ).fetchone()[0]
                assert latest is not None
                assert date.fromisoformat(latest) <= bound, (
                    f"site {site_id} holds a site_days record dated {latest},"
                    f" past the {bound.isoformat()} bound for the latest test"
                    f" clock {clock_stamp} -- a missing note_day_clock() call,"
                    " or ensure_site_days run on the real clock?"
                )
    finally:
        _day_close_conns.clear()
        _day_close_latest_clock = NOW


# ---------------------------------------------------------------------------
# P3 close-chain test helpers and fixtures (plan 2026-10-21-daily-close-
# close-job.md §15.1). Shared by every file under tests/test_close_*.py that
# exercises the night chain itself (test_close_timeline.py onward).
#
# None of P3's own names (close_chain.enqueue_due_close_work,
# close_chain.settle_interrupted_closes, every close_p4 name) are imported
# at this module's top level: they do not exist yet, and every test module
# importing this file -- including the whole pre-close-chain suite -- must
# keep collecting. Each helper below that needs one of them imports it
# inside its own body instead.
# ---------------------------------------------------------------------------


def _job_stamp(when: datetime) -> str:
    """``jobs.created_at``/``updated_at``'s own DEFAULT form, from a clock
    instant: ``%Y-%m-%dT%H:%M:%S.mmmZ`` (milliseconds, 3 digits; plan
    §15.1's "job stamps follow the clock"), matching SQLite's
    ``strftime('%Y-%m-%dT%H:%M:%fZ','now')`` (``db/migrations.py:1564-1567``).
    """
    dt = when.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# Every module whose own code a close-chain test runs and that imports
# ``utc_now`` BY NAME (plan §15.1). ``wxverify.core.timeutil`` itself is
# included so every other reader of the real name sees the same clock.
# P1's and P2's modules take ``now_utc`` as an explicit parameter instead
# (confirmed: neither ``db/site_days.py`` nor ``worker/day_values.py``
# imports ``utc_now``), so they are not listed; a test passes
# ``clock.now()`` through their ``now_utc=`` parameter directly.
_CLOSE_CLOCK_MODULES: Final[tuple[str, ...]] = (
    "wxverify.core.timeutil",
    "wxverify.worker.scheduler",
    "wxverify.worker.processor",
    "wxverify.worker.close_chain",
    # wxverify.db.queue imports utc_now BY NAME too (queue.py:13) and uses it
    # in the success/failure cooldowns, fail()'s retry stamp and the claim
    # cutoff -- without this entry those reads hit the real 2026 clock
    # against a close-chain test's 2024 job stamps, so a success cooldown
    # never applies and a completed job is claimed again every tick
    # (TL7's record job claimed 500 times before this was added).
    "wxverify.db.queue",
    # wxverify.verification.record imports utc_now BY NAME too (record.py:31)
    # and uses it in compute_forecast_record's `at = now or utc_now()` default
    # -- without this entry a build reads the real 2026 clock against a
    # close-chain test's 2024 snapshot, so `at` lands far past
    # snapshot_utc + LATE_WRITE_WINDOW_HOURS and the build raises
    # JobCancelled on every record job.
    "wxverify.verification.record",
    # wxverify.collection.budget imports utc_now BY NAME too (budget.py:15)
    # and uses it to check a feed's daily call budget -- without this entry
    # the round's tests cannot move the clock past the budget day boundary.
    "wxverify.collection.budget",
    # wxverify.worker.domain_backoff imports utc_now BY NAME too
    # (domain_backoff.py:13) and uses it to check a domain's backoff end --
    # without this entry the round's tests cannot move the clock past the
    # backoff end to let a retry through.
    "wxverify.worker.domain_backoff",
)


class MovingClock:
    """One clock object for a close-chain test (plan §15.1's "a moving
    clock"), plus the bookkeeping :func:`patch_close_clock` and
    :func:`tick`/:func:`run_due_jobs` need for the job-stamp rule: every
    ``jobs`` row a tick or a job run inserts (id above the highest id this
    clock has already stamped) gets ``created_at``/``updated_at`` set to
    the clock's current instant, in :func:`_job_stamp`'s form, instead of
    SQLite's real-time DEFAULT.
    """

    def __init__(self, start: datetime) -> None:
        self._now = start
        self._high_water: int | None = None
        note_day_clock(start)

    def now(self) -> datetime:
        return self._now

    def set(self, when: datetime) -> datetime:
        """Move the clock to ``when`` and raise
        :func:`check_day_close_records`'s bound to match (every clock
        move must, or a later ``ensure_site_days`` that catches up to it
        fails that autouse check instead of a real assertion)."""
        self._now = when
        return note_day_clock(when)

    def advance(self, delta: timedelta) -> datetime:
        return self.set(self._now + delta)

    def prime(self, conn: sqlite3.Connection) -> None:
        """Fix the high-water mark at the jobs table's current max id, so
        every row inserted from this point on is this clock's to stamp,
        instead of keeping SQLite's real-time DEFAULT. Idempotent and
        monotonic (a later call only raises the mark), so a test may
        prime explicitly before seeding a job of its own -- so that seed
        gets stamped on the next call too -- and :func:`tick` /
        :func:`run_due_jobs` may still prime again, harmlessly, right
        before their own insert.
        """
        row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM jobs").fetchone()
        floor = int(row[0])
        if self._high_water is None or floor > self._high_water:
            self._high_water = floor

    def stamp_new_jobs(self, conn: sqlite3.Connection) -> None:
        """Stamp every ``jobs`` row inserted since the high-water mark
        (primed by :meth:`prime`, called by the caller BEFORE the insert
        this stamps -- never here, which would raise the mark past the
        very rows it is meant to catch) with ``created_at``/``updated_at``
        = now, in :func:`_job_stamp`'s form, instead of SQLite's
        real-time DEFAULT. Falls back to priming now only on a clock
        that has never primed at all.
        """
        if self._high_water is None:
            self.prime(conn)
        stamp = _job_stamp(self._now)
        conn.execute(
            "UPDATE jobs SET created_at = ?, updated_at = ? WHERE id > ?",
            (stamp, stamp, self._high_water),
        )
        row = conn.execute("SELECT MAX(id) FROM jobs").fetchone()
        new_max = row[0]
        if new_max is not None and int(new_max) > self._high_water:
            self._high_water = int(new_max)


def patch_close_clock(monkeypatch: pytest.MonkeyPatch, clock: MovingClock) -> None:
    """Replace ``utc_now`` with ``clock.now`` in ``core.timeutil`` and every
    module of :data:`_CLOSE_CLOCK_MODULES` that imports it by name, as
    ``tests/test_claim_promotion.py:108``'s ``frozen_now`` does for one
    module (plan §15.1). ``close_chain`` does not import ``utc_now`` by
    name yet (it holds only ``record_collection_try`` today), so that one
    patch is inert until piece 3/4 add the import -- ``raising=False``
    tolerates that rather than failing every test that patches the clock.
    """
    for module in _CLOSE_CLOCK_MODULES:
        monkeypatch.setattr(f"{module}.utc_now", clock.now, raising=False)


class Stop(BaseException):
    """Raised by a stand-in to simulate a stop mid-stage (plan §15.1): a
    ``BaseException`` subclass, deliberately NOT an ``Exception``, so it
    passes through every ``except Exception`` in the dispatch path the way
    a real process kill would, instead of being caught and turned into a
    failed attempt.
    """


async def tick(db: Database, clock: MovingClock, at: datetime | None = None) -> None:
    """One ``scheduler_tick``, optionally moving ``clock`` to ``at`` first,
    with the job-stamp rule applied to any row it inserted (plan §15.1).

    Primes ``clock`` right BEFORE ``scheduler_tick``'s own insert, not
    after: priming after would raise the high-water mark past the very
    rows this tick is meant to stamp, and they would keep SQLite's
    real-time DEFAULT instead.
    """
    if at is not None:
        clock.set(at)
    await db.write(clock.prime)
    await db.write(scheduler_tick)
    await db.write(clock.stamp_new_jobs)


def _pending_stage_job_exists(
    conn: sqlite3.Connection, job_type: str, stage: int
) -> bool:
    """Whether a ``pending`` job of ``job_type`` keyed to end in
    ``:{stage}`` exists (every ``day_close``/``day_publish`` key ends in
    its stage, plan §8.1) -- :func:`run_due_jobs`'s ``stop_before`` check.
    """
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE type = ? AND status = 'pending'"
        " AND job_key LIKE '%' || ? LIMIT 1",
        (job_type, f":{stage}"),
    ).fetchone()
    return row is not None


def _job_outcome(conn: sqlite3.Connection, job_id: int) -> str:
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row is not None, f"job {job_id} vanished"
    return str(row[0])


# Guards tests/helpers.py's run_due_jobs against a job that keeps coming
# due at the same (unmoved) clock instant, which would loop forever; far
# above any real night's job count (checked against every test caller).
_MAX_CLAIMS_PER_TICK = 500


async def run_due_jobs(
    db: Database,
    clock: MovingClock,
    *,
    stop_before: tuple[str, int] | None = None,
) -> list[tuple[str, str | None, str]]:
    """Tick and claim until nothing is due at the clock's current time
    (plan §15.1), each job run with ``claim_next_job``/``run_claimed_job``
    as production does (the pattern of
    ``tests/test_verification_day_pipeline.py:1020``). Returns each
    claim's ``(type, job_key, outcome)`` in claim order, ``outcome`` being
    the job row's ``status`` after it ran (``completed`` or ``failed``).

    ``stop_before`` is a ``(job_type, stage)`` pair: after each tick and
    its stamping, and before each claim, the helper returns without
    claiming if a ``pending`` job of that type whose key ends in
    ``:{stage}`` exists. That job and every other due job stay ``pending``,
    stamped on the clock -- a test that then wants to run one job alone
    claims it itself with ``claim_next_job``/``run_claimed_job``, with no
    further tick.

    Raises ``AssertionError`` if one call makes more than
    ``_MAX_CLAIMS_PER_TICK`` claims without reaching ``stop_before`` or an
    empty queue, instead of looping forever on a job that keeps re-queuing
    itself at the clock's unmoved instant.
    """
    claims: list[tuple[str, str | None, str]] = []
    stop_type, stop_stage = stop_before if stop_before is not None else (None, None)

    def _stage_pending(c: sqlite3.Connection) -> bool:
        assert stop_type is not None and stop_stage is not None
        return _pending_stage_job_exists(c, stop_type, stop_stage)

    while True:
        await tick(db, clock)
        if stop_before is not None and await db.read(_stage_pending):
            return claims
        job = await db.write(claim_next_job)
        if job is None:
            return claims
        await run_claimed_job(db, job, lane="main")
        await db.write(clock.stamp_new_jobs)

        def _read_outcome(c: sqlite3.Connection, job_id: int = job.id) -> str:
            return _job_outcome(c, job_id)

        outcome = await db.read(_read_outcome)
        claims.append((job.type, job.job_key, outcome))
        if len(claims) > _MAX_CLAIMS_PER_TICK:
            raise AssertionError(
                f"run_due_jobs made {_MAX_CLAIMS_PER_TICK} claims at one "
                "clock time without reaching its stop_before "
                f"({stop_before!r}) or an empty queue; last claims "
                f"(type, job_key, outcome): {claims[-5:]!r}"
            )


class RoundStateStandIn:
    """``close_p4.round_state`` stand-in (plan §14.1, stage 1 only; §15.1
    "P4."; §18.13). Answers per ``(site_id, round_date)``, programmed with
    :meth:`set_answer`; :meth:`raise_for_site` makes every call for one
    site raise instead (TL10's per-site fault), leaving other sites
    unaffected. A ``(site_id, round_date)`` with no programmed answer calls
    through to P4's real ``round_state`` instead of raising ``KeyError``.
    Every call is recorded in :attr:`calls`, in call order.
    """

    def __init__(self, call_through: Callable[..., object] | None = None) -> None:
        self.calls: list[tuple[int, date]] = []
        self._answers: dict[tuple[int, date], tuple[bool, datetime]] = {}
        self._site_raises: dict[int, BaseException | type[BaseException]] = {}
        self._call_through = call_through

    def set_answer(
        self, site_id: int, round_date: date, *, settled: bool, bound_utc: datetime
    ) -> None:
        self._answers[(site_id, round_date)] = (settled, bound_utc)

    def raise_for_site(
        self, site_id: int, exc: BaseException | type[BaseException]
    ) -> None:
        self._site_raises[site_id] = exc

    def __call__(
        self, conn: sqlite3.Connection, *, site_id: int, round_date: date
    ) -> object:
        self.calls.append((site_id, round_date))
        if site_id in self._site_raises:
            raise self._site_raises[site_id]
        answer = self._answers.get((site_id, round_date))
        if answer is None:
            assert self._call_through is not None
            return self._call_through(conn, site_id=site_id, round_date=round_date)
        settled, bound_utc = answer
        from wxverify.worker.close_p4 import RoundState  # P3 name, §14.1

        return RoundState(settled=settled, bound_utc=bound_utc)


def install_round_state_standin(monkeypatch: pytest.MonkeyPatch) -> RoundStateStandIn:
    """Patch ``close_p4.round_state`` with a fresh :class:`RoundStateStandIn`
    (plan §15.1 "P4."; §18.13). ``close_p4.py`` does not exist until piece
    3, so this raises ``ModuleNotFoundError`` until then -- the right
    failure for any TL test that calls it before that piece lands.
    """
    from wxverify.worker import close_p4

    standin = RoundStateStandIn(close_p4.round_state)
    monkeypatch.setattr("wxverify.worker.close_p4.round_state", standin, raising=False)
    return standin


class RecordingStandIn:
    """Generic recorder for a sync P4 call point (plan §14.1's
    ``score_day``; §15.1 "P4."; §18.13). Records every call's ``kwargs`` in
    :attr:`calls`; returns :attr:`result` (calling it first if it is a
    plain callable) or raises :attr:`raises` when the test sets one. When
    neither is set, calls through to P4's real body. Deliberately ignorant
    of P4's own return types -- the test that configures ``result`` builds
    those itself, from ``close_p4`` directly.
    """

    def __init__(self, call_through: Callable[..., object] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.result: object = None
        self.raises: BaseException | type[BaseException] | None = None
        self._call_through = call_through

    def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        if self.result is None and self._call_through is not None:
            return self._call_through(*args, **kwargs)
        result = self.result
        if callable(result) and not isinstance(result, type):
            return result(*args, **kwargs)
        return result


class AsyncRecordingStandIn(RecordingStandIn):
    """:class:`RecordingStandIn` for an async P4 call point (``publish``,
    ``clean_worksheets``, plan §14.1; §18.13). Awaits what the call-through,
    or a callable ``result``, returns when that value is awaitable.
    """

    async def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        if self.result is None and self._call_through is not None:
            value = self._call_through(*args, **kwargs)
        else:
            result = self.result
            if callable(result) and not isinstance(result, type):
                value = result(*args, **kwargs)
            else:
                value = result
        if inspect.isawaitable(value):
            return await value
        return value


def install_p4_standins(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, RecordingStandIn]:
    """Patch ``close_p4``'s other three call points -- ``score_day``,
    ``publish``, ``clean_worksheets`` -- with generic recorders that call
    through to P4's real body when the test leaves neither ``result`` nor
    ``raises`` set (plan §15.1 "P4."; §18.13; stage 1's ``round_state`` has
    its own :func:`install_round_state_standin`, since its answer is keyed
    by round date and may need to fault one site only). Raises
    ``ModuleNotFoundError`` until piece 3 adds ``close_p4.py``.
    """
    from wxverify.worker import close_p4

    score_day = RecordingStandIn(close_p4.score_day)
    publish = AsyncRecordingStandIn(close_p4.publish)
    clean_worksheets = AsyncRecordingStandIn(close_p4.clean_worksheets)
    monkeypatch.setattr("wxverify.worker.close_p4.score_day", score_day, raising=False)
    monkeypatch.setattr("wxverify.worker.close_p4.publish", publish, raising=False)
    monkeypatch.setattr(
        "wxverify.worker.close_p4.clean_worksheets", clean_worksheets, raising=False
    )
    return {
        "score_day": score_day,
        "publish": publish,
        "clean_worksheets": clean_worksheets,
    }


def clear_close_chain_log_map() -> None:
    """Clear ``close_chain``'s per-``(site, step)`` last-ERROR-instant map
    (plan §8.5's flood guard; §15.1 "The log map.") before the test, so no
    test inherits another's. A plain function, not a fixture (the whole
    suite must stay untouched by a P3 fixture) -- each P3 test file wires
    this into its own autouse fixture to request it explicitly.
    Tolerates the map (or the module attribute) not existing yet: that is
    the expected state before piece 4 lands.
    """
    with contextlib.suppress(ModuleNotFoundError, AttributeError):
        from wxverify.worker import close_chain

        close_chain._LAST_SITE_STEP_ERROR.clear()  # type: ignore[attr-defined] # noqa: SLF001


def patch_weathercom_transport(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    """Patch the gap fill's HTTP client onto an ``httpx.MockTransport``
    wrapping ``handler`` (plan §15.1 "The provider."; the pattern of
    ``tests/test_gap_fill.py:394-397``, since ``_Fill.client()`` builds a
    bare ``httpx.AsyncClient()``, ``worker/day_values.py:487-491``).
    """
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        "wxverify.worker.day_values.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # noqa: ARG005
    )


def write_weathercom_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    key: str | None = "ci-placeholder",
) -> Path:
    """Write an ``options.json`` holding (``key`` given) or omitting
    (``key=None``, AT16) a ``weathercom_key``, and point
    ``config.options_path`` at it (plan §15.1 "The weather.com key."; the
    pattern of ``tests/test_gap_fill.py:93-103``).
    """
    options_path = tmp_path / "options.json"
    body: dict[str, object] = {} if key is None else {"weathercom_key": key}
    options_path.write_text(json.dumps(body), encoding="utf-8")
    monkeypatch.setattr(config, "options_path", str(options_path))
    return options_path


def disable_station_pacing(monkeypatch: pytest.MonkeyPatch) -> None:
    """No-op the gap fill's between-call pacing sleep (plan §15.1 "No
    pacing sleep."; the real ``pace_station_call`` sleeps between a fill's
    calls, ``worker/station_pacing.py:71-74``, imported by name at
    ``worker/day_values.py:81``).
    """

    async def _no_pace(site_id: int, station_id: int, ordinal: int) -> None:
        return None

    monkeypatch.setattr("wxverify.worker.day_values.pace_station_call", _no_pace)


_NON_CLOSE_ENQUEUERS: Final[tuple[str, ...]] = (
    "enqueue_due_round_tries",
    "enqueue_due_daytime_downloads",
    "_enqueue_due_wind_days",
    "_enqueue_due_forecast_records",
)


def disable_other_enqueuers(
    monkeypatch: pytest.MonkeyPatch, *, keep: Sequence[str] = ()
) -> None:
    """No-op every one of ``scheduler_tick``'s non-close enqueuers except
    those named in ``keep`` (plan §15.1 "Other job kinds."; TL7 keeps
    ``_enqueue_due_forecast_records``). ``_enqueue_due_obs``,
    ``_enqueue_due_verification_runs`` and ``_enqueue_due_score_refresh``
    (the routine download's clock, the nightly replay trigger and the score
    backstop) are retired (R1, R12, R17) and no longer exist to no-op.
    Without this, the first tick of a standard night would also queue a
    ``fetch_feed`` job per seeded open-meteo feed, which ``run_due_jobs``
    would then claim.
    """
    for name in _NON_CLOSE_ENQUEUERS:
        if name in keep:
            continue
        monkeypatch.setattr(
            f"wxverify.worker.scheduler.{name}",
            lambda *_args, **_kwargs: None,
        )


def stub_fetch_feed(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
    """Replace ``dispatch``'s real ``fetch_feed`` handler
    (``worker/processor.py:720``) with a stub recording ``(site_id,
    feed_id)`` and returning ``None`` (plan §15.1 "Other job kinds.",
    TL8). Without this a seeded ``fetch_feed`` job would run through its
    real handler and trip the autouse network guard.
    """
    calls: list[tuple[int, int]] = []

    async def _stub(
        db: Database,
        writer: FencedWriter,
        site_id: int,
        feed_id: int,
        *,
        payload: dict[str, object],
    ) -> None:
        calls.append((site_id, feed_id))

    monkeypatch.setattr("wxverify.worker.processor._fetch_feed", _stub)
    return calls


def stub_wind_days(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Replace ``dispatch``'s real wind-lane handler
    (``worker/processor.py:700``) with a stub recording ``site_id`` and
    returning ``None`` (plan §15.1 "Other job kinds.", TL3).
    """
    calls: list[int] = []

    async def _stub(db: Database, writer: FencedWriter, site_id: int) -> None:
        calls.append(site_id)

    monkeypatch.setattr("wxverify.worker.processor.run_wind_days", _stub)
    return calls


def failed_scope_count(conn: sqlite3.Connection, *, now_utc: datetime) -> int:
    """AT13/AT14/PB8/PB9's failed-scope count (plan §15.1 "The
    failed-scope count."): the health check's failed arm alone
    (``FAILED_SCOPES_SQL``, run with the same cutoff
    ``_pipeline_conditions`` uses, ``monitor.py:720,723``), never
    ``build_verdict``'s in-flight arm, which would also count a job a
    tick left ``pending``, stamped on the test clock, as overdue at a read
    hours or days later.
    """
    from wxverify.monitor import FAILED_JOB_AGE_HOURS, FAILED_SCOPES_SQL, _count

    cutoff = isoformat_utc(now_utc - timedelta(hours=FAILED_JOB_AGE_HOURS))
    return _count(conn, FAILED_SCOPES_SQL, (cutoff,))


def _seed_five_minute_day(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> None:
    """Every hour of ``local_date`` paired at minutes 0 and 5 with
    ``five_minute`` readings (the full-day shape of
    ``tests/test_gap_fill.py``'s ``_seed_full_day``): no gaps, nothing for
    the gap fill to call.
    """
    for hour in range(24):
        for minute, speed in ((0, 5.0), (5, 5.5)):
            when = datetime(
                local_date.year,
                local_date.month,
                local_date.day,
                hour,
                minute,
                tzinfo=UTC,
            )
            conn.execute(
                "INSERT INTO station_wind_records"
                " (station_id, obs_at, source, local_date, speed_kmh, temp_c,"
                "  precip_total_mm, fetched_at)"
                " VALUES (?, ?, 'five_minute', ?, ?, 10.0, 0.0, ?)",
                (
                    station_id,
                    isoformat_utc(when),
                    local_date.isoformat(),
                    speed,
                    isoformat_utc(when),
                ),
            )


#: R, the round date of :data:`D` (plan §4.1: "R = D + 1"), and the
#: standard night's round bound, R 00:40Z (plan §15.1 "Standard night").
STANDARD_NIGHT_R: Final = D + timedelta(days=1)
STANDARD_NIGHT_ROUND_BOUND: Final = datetime(
    STANDARD_NIGHT_R.year,
    STANDARD_NIGHT_R.month,
    STANDARD_NIGHT_R.day,
    0,
    40,
    tzinfo=UTC,
)


def standard_night(
    tmp_path: Path,
) -> tuple[Database, FencedWriter, int, dict[str, int]]:
    """Plan §15.1's standard night: site S in ``UTC``, day ``D`` ``open``,
    ``D - 1`` ``closed``, ``R = D + 1``, ``worker_started_at`` ``D``
    12:00Z (:data:`NOW`), stations A and B on ``D``'s list with full
    5-minute readings on ``D`` (so the gap fill, once reached, finds
    nothing to fill for either and never needs the weather.com key or a
    transport). Returns ``(db, writer, site_id, {pws_id: station_id})``.

    A fresh real SQLite file under ``tmp_path`` (``init_db``, real
    migrations), closing any process-global ``Database`` first, exactly
    as ``tests/test_gap_fill.py``'s own tmp-path harness does.
    """
    from wxverify.db.site_days import begin_close, finish_close

    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- tests build the fixture on the real writer conn

    site_id = day_close_site(conn, timezone="UTC", close_on=True, with_records=True)
    stations = {
        str(row["pws_station_id"]): int(row["id"])
        for row in conn.execute(
            "SELECT id, pws_station_id FROM stations WHERE site_id = ? ORDER BY id",
            (site_id,),
        )
    }

    d_minus_1 = D - timedelta(days=1)
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert row is not None
    t1 = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, d_minus_1, now_utc=note_day_clock(t1))
    finish_close(conn, site_id, d_minus_1, now_utc=t1)

    for station_id in stations.values():
        _seed_five_minute_day(conn, station_id, D)

    set_runtime_state(conn, "worker_started_at", isoformat_utc(NOW))
    conn.commit()
    return db, FencedWriter(db, db.generation), site_id, stations


# ---------------------------------------------------------------------------
# P4 §18.1: fixtures for the scored round and daytime downloads
# (`tests/test_round_schedule.py`). Built on P3's fixtures above --
# `standard_night`, `MovingClock`/`patch_close_clock`, `run_due_jobs`,
# `tick`, `Stop` -- which this section reuses, never copies.
# ---------------------------------------------------------------------------

#: P4 §18.1's real feeds X, Y, Z (360-minute interval) and G (720-minute),
#: each its own synthetic ``source``/``model`` pair (so `UNIQUE(source,
#: model)` never collides with a seeded real feed). Not meteoblue: that
#: source's package mechanics (4.2, 4.4) are hardcoded to the literal
#: `source='meteoblue'` in today's code (`worker/scheduler.py:285`,
#: `db/migrations.py:1950-1951`), so the test's package stand-in ("P") has
#: to be the real seeded `meteoblue`/`multimodel` row instead
#: (`meteoblue_package_feed_id`), not a synthetic row of its own.
_ROUND_SCHEDULE_FEED_SPECS: Final[dict[str, tuple[str, int]]] = {
    "X": ("synthetic-round-x", 360),
    "Y": ("synthetic-round-y", 360),
    "Z": ("synthetic-round-z", 360),
    "G": ("synthetic-round-g", 720),
}

#: ``{"X": "synthetic-round-x", ...}`` -- each feed's own ``source`` string,
#: for a test that needs to program :class:`ProviderStandIn` or read
#: :attr:`ProviderStandIn.calls` by name instead of spelling the source out.
ROUND_SCHEDULE_SOURCES: Final[dict[str, str]] = {
    name: source for name, (source, _interval) in _ROUND_SCHEDULE_FEED_SPECS.items()
}


def seed_round_schedule_feeds(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    subscribe: Sequence[str] = ("X", "Y", "Z", "G"),
) -> dict[str, int]:
    """Insert P4 §18.1's feeds X, Y, Z and G: real feeds (``is_virtual=0``),
    each its own synthetic ``source``/``model`` pair, each with its own
    ``sources`` row (a 10000 call/day cap, matching open-meteo's, plan
    §3.2) so :func:`wxverify.collection.budget.reserve_budget` accepts it
    without a test having to seed one by hand. Subscribes every name in
    ``subscribe`` at ``site_id`` through ``site_feed_state`` (plain
    INSERT, plan §18.1's "seed helpers by plain INSERT"). Returns
    ``{"X": feed_id, "Y": feed_id, "Z": feed_id, "G": feed_id}``.
    """
    ids: dict[str, int] = {}
    for name, (source, interval) in _ROUND_SCHEDULE_FEED_SPECS.items():
        conn.execute(
            "INSERT OR IGNORE INTO sources"
            " (source, daily_call_limit, daily_credit_limit, billing_tz)"
            " VALUES (?, 10000, NULL, 'UTC')",
            (source,),
        )
        feed_id = int(
            conn.execute(
                "INSERT INTO feeds"
                " (source, model, enabled, default_subscribed,"
                "  fetch_interval_minutes, max_lead_hours, is_virtual)"
                " VALUES (?, ?, 1, 0, ?, 168, 0)",
                (source, source, interval),
            ).lastrowid
        )
        ids[name] = feed_id
        if name in subscribe:
            conn.execute(
                "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
                " VALUES (?, ?, 1, 0)",
                (site_id, feed_id),
            )
    return ids


def meteoblue_package_feed_id(conn: sqlite3.Connection) -> int:
    """P4 §18.1's package ``P``: the id of the real seeded
    ``meteoblue``/``multimodel`` feed row (``config.FEED_SEEDS``). Its
    members ``P1``/``P2`` are not pre-seeded rows -- each is created the
    first time a reply's samples carry its model name
    (``_feed_id_for_sample``, ``collection/forecast_fetcher.py:273-285``);
    build such a sample with :func:`meteoblue_member_sample`.
    """
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'meteoblue' AND model = 'multimodel'"
    ).fetchone()
    assert row is not None, "meteoblue/multimodel feed is not seeded"
    return int(row["id"])


def subscribe_meteoblue_package(conn: sqlite3.Connection, site_id: int) -> int:
    """Subscribe ``site_id`` to the real meteoblue package feed (P4
    §18.1's "P"), by plain INSERT. Returns its feed id."""
    feed_id = meteoblue_package_feed_id(conn)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )
    return feed_id


def meteoblue_member_sample(
    model: str,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
) -> NormalizedSample:
    """One meteoblue package member's sample (P4 §18.1's "P1"/"P2"): its
    ``model`` differs from the package row's ``multimodel``, so
    ``_feed_id_for_sample`` registers it under a new feed row the first
    time it is persisted (``collection/forecast_fetcher.py:273-285``).
    """
    return NormalizedSample(
        model=model,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=value,
        source_raw=f"meteoblue:{model}",
        model_run_id=issued_at,
    )


def round_schedule_sample(
    model: str,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
) -> NormalizedSample:
    """One sample for a non-package feed (X, Y, Z or G): ``model`` equal to
    the fetched feed's own model, so ``_feed_id_for_sample`` keeps it on
    that feed's own row (``collection/forecast_fetcher.py:273-285``).
    """
    return NormalizedSample(
        model=model,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=value,
        source_raw=f"{model}:raw",
        model_run_id=issued_at,
    )


# -- The provider stand-in (P4 §18.1's "the provider") ----------------------


@dataclass(frozen=True)
class ProviderReply:
    """A stand-in adapter call's successful outcome: usable samples (P4
    §18.1's "a reply with given run labels and values")."""

    samples: tuple[NormalizedSample, ...]


@dataclass(frozen=True)
class ProviderEmpty:
    """An HTTP 200 with no usable sample (P4 §18.1's "an empty reply")."""


@dataclass(frozen=True)
class ProviderHttpError:
    """An HTTP failure status (P4 §18.1's "an HTTP 404", "500" or "a 429
    with Retry-After")."""

    status: int
    retry_after: str | None = None


@dataclass(frozen=True)
class ProviderConnectError:
    """A transport failure (P4 §18.1's "a ConnectError")."""


ProviderOutcome = (
    ProviderReply | ProviderEmpty | ProviderHttpError | ProviderConnectError
)


class _StandInAdapter:
    """One ``ForecastAdapter`` for one ``source``, answering from
    :class:`ProviderStandIn`'s queue for that source (P4 §18.1)."""

    def __init__(self, standin: ProviderStandIn, source: str) -> None:
        self._standin = standin
        self._source = source

    def estimate_cost(self, req: object) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: object) -> FetchResult:
        outcome = self._standin._consume(self._source)  # noqa: SLF001
        if isinstance(outcome, ProviderReply):
            return FetchResult(samples=list(outcome.samples), grid=None)
        if isinstance(outcome, ProviderEmpty):
            return FetchResult(samples=[], grid=None)
        if isinstance(outcome, ProviderHttpError):
            request = httpx.Request(
                "GET", f"https://{self._source}.example.invalid/forecast"
            )
            headers = (
                {"Retry-After": outcome.retry_after}
                if outcome.retry_after is not None
                else {}
            )
            response = httpx.Response(outcome.status, headers=headers, request=request)
            raise httpx.HTTPStatusError(
                str(outcome.status), request=request, response=response
            )
        if isinstance(outcome, ProviderConnectError):
            raise httpx.ConnectError("synthetic connect error")
        raise AssertionError(f"ProviderStandIn: unknown outcome {outcome!r}")


class ProviderStandIn:
    """P4 §18.1's "the provider": one adapter stand-in, shared across every
    source, answering per ``(source, call)`` from a per-source programmed
    queue, and counting calls per source in :attr:`calls`. Install with
    :func:`install_provider_standin`.
    """

    def __init__(self) -> None:
        self._queues: dict[str, list[ProviderOutcome]] = {}
        self.calls: dict[str, int] = {}

    def program(self, source: str, *outcomes: ProviderOutcome) -> None:
        """Append ``outcomes`` to ``source``'s queue, consumed in order,
        one per adapter call for that source. A call made with the queue
        empty raises ``AssertionError`` naming the source and the call
        number, instead of answering with a guessed default -- a test
        that reaches this has under-programmed its own scenario.
        """
        self._queues.setdefault(source, []).extend(outcomes)

    def build(self, source: str, client: httpx.AsyncClient) -> _StandInAdapter:
        return _StandInAdapter(self, source)

    def _consume(self, source: str) -> ProviderOutcome:
        count = self.calls.get(source, 0) + 1
        self.calls[source] = count
        queue = self._queues.get(source)
        if not queue:
            raise AssertionError(
                f"ProviderStandIn: no programmed outcome for source={source!r}"
                f" (call {count})"
            )
        return queue.pop(0)


def install_provider_standin(monkeypatch: pytest.MonkeyPatch) -> ProviderStandIn:
    """Patch the real dispatch path's adapter builder
    (``worker/processor.py``'s module-level ``build_adapter``, imported by
    name at ``worker/processor.py:48`` and called from ``_fetch_feed`` at
    ``:1270``) with a fresh :class:`ProviderStandIn` (P4 §18.1's "the
    provider, registered through `adapter_builder`"). Every ``fetch_feed``
    job -- round, daytime or unscheduled -- then calls the stand-in
    instead of a real provider; nothing opens a socket.

    Also installs :func:`install_test_source_domains`, since every caller
    of this stand-in drives a real download through the job pipeline for
    one of P4 §18.1's synthetic feed sources, and without that mapping
    ``source_domain`` raises a bare ``KeyError`` before any call is made.
    """
    standin = ProviderStandIn()
    monkeypatch.setattr("wxverify.worker.processor.build_adapter", standin.build)
    install_test_source_domains(monkeypatch)
    return standin


def install_test_source_domains(monkeypatch: pytest.MonkeyPatch) -> None:
    """Map P4 §18.1's synthetic feed sources (X/Y/Z/G,
    :data:`ROUND_SCHEDULE_SOURCES`) in ``worker.domain_backoff.SOURCE_DOMAINS``
    (today keyed by real provider names only -- ``open-meteo``, ``meteoblue``,
    etc.) onto ``"{source}.example.invalid"``, not onto the bare source name.
    Without this, a real download run through the job pipeline for one of
    these feeds hits a bare ``KeyError`` in ``source_domain``, called
    unconditionally by ``_reserve_feed_call``/``_persist_fetch_success`` on
    every attempt (success or failure) -- not the production behaviour under
    test. The mapped value must match ``_StandInAdapter.fetch_forecast``'s
    request host (``f"https://{source}.example.invalid/forecast"``, above):
    ``record_http_backoff`` keys a domain-backoff row on ``response.url.host``,
    so mapping the bare source name here would make ``check_domain_backoff``
    look up a domain the stand-in's 429/500 never actually records under,
    and a programmed backoff would never be seen. The real ``meteoblue``
    source already has an entry and needs no patch.
    """
    from wxverify.worker.domain_backoff import SOURCE_DOMAINS

    for source in ROUND_SCHEDULE_SOURCES.values():
        monkeypatch.setitem(SOURCE_DOMAINS, source, f"{source}.example.invalid")


# ---------------------------------------------------------------------------
# P4 §18.1
#
# Shared fixture for PD1's record half (tests/test_p4_dormant.py). The
# replay-chain drivers that used to live here (``build_verification_replay_site``,
# ``drive_verification_chain``, ``drive_verification_chain_until_phase``) left
# with their last users, WC7 (tests/test_worksheet_cleanup.py) and PD4's
# cleanup-after-a-replay-publish half (tests/test_p4_dormant.py), when R12
# retired the replay chain (``worker/verification_run.py``). Append-only:
# nothing above this line changes.
# ---------------------------------------------------------------------------


def build_record_site_with_sample(
    conn: sqlite3.Connection, *, close_on: bool
) -> tuple[int, int]:
    """A site with one subscribed feed and one usable temperature sample
    valid at D 06:00Z (day ahead 0 from D) -- for driving
    ``build_forecast_record`` directly. Copied from
    ``tests/test_record_publication.py``'s own ``_build_site_with_sample``
    (that file is another agent's and is left untouched), since PD1's
    record half (``tests/test_p4_dormant.py``) needs the identical shape
    with the close off. Returns ``(site_id, feed_id)``.
    """
    site_id = day_close_site(conn, close_on=close_on, with_records=False)
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('synthetic-pd1-record', 'synthetic-pd1-record', 1, 0, 360, 168, 0)"
    )
    assert cur.lastrowid is not None
    feed_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )
    issued_at = f"{D.isoformat()}T00:00:00Z"
    valid_at = f"{D.isoformat()}T06:00:00Z"
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value,"
        "  source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, 'temperature', ?, ?, 6, 12.0, 'synthetic', ?, ?)",
        (site_id, feed_id, issued_at, valid_at, issued_at, issued_at),
    )
    conn.commit()
    return site_id, feed_id


# ---------------------------------------------------------------------------
# P5's seven role seeds (daily-close plan 2026-10-22-daily-close-alerts-and-
# pages.md §13.1). Each writes through P3's or P4's own writer where one
# exists outside a running close, else a plain INSERT with that table's own
# columns and stamp forms, so a renamed column changes one place. Station
# ids and dates are synthetic; see ``day_close_site`` above for D/NOW.
# ---------------------------------------------------------------------------


def seed_attempt(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    attempt: int,
    started_at: datetime,
    last_stage: int = 1,
    last_progress_at: datetime | None = None,
    outcome: str = "running",
    failed_stage: int | None = None,
    failure_reason: str | None = None,
    ended_at: datetime | None = None,
    truth_version: str | None = None,
) -> None:
    """R3.1's attempt record (one ``close_attempts`` row, P3 11.3).

    The row behind P3's ``AttemptRecord``: a close attempt (1 or 2) of a
    site-day, from which P3 derives R3.4's blocking stage and which holds a
    running stage's ``last_progress_at``. Plain INSERT: no P3 writer builds
    one outside a running close. ``last_progress_at`` defaults to
    ``started_at``.
    """
    conn.execute(
        "INSERT INTO close_attempts"
        " (site_id, local_date, attempt, started_at, last_stage,"
        "  last_progress_at, outcome, failed_stage, failure_reason, ended_at,"
        "  truth_version)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            attempt,
            isoformat_utc_micro(started_at),
            last_stage,
            isoformat_utc_micro(last_progress_at or started_at),
            outcome,
            failed_stage,
            failure_reason,
            None if ended_at is None else isoformat_utc_micro(ended_at),
            truth_version,
        ),
    )


def seed_stage_log(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    stage: int,
    attempt: int,
    job_id: int | None,
    enqueued_at: datetime,
    started_at: datetime,
    ended_at: datetime,
    queue_wait_s: float,
    duration_s: float,
    outcome: str,
    reason: str | None = None,
    detail: str = "{}",
) -> None:
    """R3.5's stage-log row (one ``close_stage_log`` row, P3 11.1).

    One finished stage run: its start, end, queue wait and outcome
    (``completed``, ``failed``, ``superseded`` or ``skipped``). Plain
    INSERT: P3 writes this only from inside a running close. ``job_id`` is
    None for stage 1 only (the table's own CHECK).
    """
    conn.execute(
        "INSERT INTO close_stage_log"
        " (site_id, local_date, attempt, stage, job_id, enqueued_at,"
        "  started_at, ended_at, queue_wait_s, duration_s, outcome, reason,"
        "  detail)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            attempt,
            stage,
            job_id,
            isoformat_utc_micro(enqueued_at),
            isoformat_utc_micro(started_at),
            isoformat_utc_micro(ended_at),
            queue_wait_s,
            duration_s,
            outcome,
            reason,
            detail,
        ),
    )


def seed_try(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    kind: str,
    slot: str,
    close_attempt: int | None,
    tried_at: datetime,
    outcome: str,
    reason: str | None = None,
    detail: str | None = None,
    readings_kept: int = 0,
    readings_refused: int = 0,
) -> None:
    """R3.6's collection-try row (one ``close_try_log`` row, P3 11.2).

    Thin wrapper over P3's own writer, ``record_collection_try``
    (``worker/close_chain.py``), which exists outside a running close: one
    weather.com collection try, its station, download kind, slot (evening
    ``HH:MM``, ``daytime``, or ``gap_fill`` with ``close_attempt``), outcome
    and reading counts.
    """
    from wxverify.worker.close_chain import record_collection_try

    record_collection_try(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date.isoformat(),
        kind=kind,
        slot=slot,
        close_attempt=close_attempt,
        tried_at=isoformat_utc_micro(tried_at),
        outcome=outcome,
        reason=reason,
        detail=detail,
        readings_kept=readings_kept,
        readings_refused=readings_refused,
    )


def seed_publication_state(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff: date,
    attempt: int = 1,
    state: str,
    opened_at: datetime,
    started_at: datetime | None = None,
    retry_at: datetime | None = None,
    finished_at: datetime | None = None,
    publication_id: str | None = None,
    last_failure: str | None = None,
) -> None:
    """R3.7's publication record (one ``close_publications`` row, P3 11.5).

    The site-day's publication state (``pending``, ``building``,
    ``published``, ``retry_due``, ``failed`` or ``superseded``), its
    ``opened_at``, ``started_at``, ``retry_at``, ``finished_at`` and
    ``last_failure``. Plain INSERT: P3's own reader, ``publication_record``,
    is read-only, and the only writer runs inside a running close.
    """
    conn.execute(
        "INSERT INTO close_publications"
        " (site_id, cutoff, attempt, state, opened_at, started_at, retry_at,"
        "  finished_at, publication_id, last_failure)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            cutoff.isoformat(),
            attempt,
            state,
            isoformat_utc_micro(opened_at),
            None if started_at is None else isoformat_utc_micro(started_at),
            None if retry_at is None else isoformat_utc(retry_at),
            None if finished_at is None else isoformat_utc_micro(finished_at),
            publication_id,
            last_failure,
        ),
    )


def seed_active_publication(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff: date,
    published_at: datetime,
    first_record_date: date | None = None,
    window_days: int = 7,
    window_start_date: date | None = None,
    wind_start_date: date | None = None,
    methodology_version: int = 1,
    scoring_rule: str = "synthetic-rule",
    wind_method: str = "synthetic-wind",
    wind_constants: str = "{}",
    build_started_at: datetime | None = None,
) -> int:
    """R4.1's active publication (one ``publications`` row, P4 §16.2).

    What ``active_publication(conn, site_id=…, at_utc=…)`` resolves: the
    site's publication through ``cutoff``, published at ``published_at``.
    Plain INSERT: P4 writes this row only from inside ``publish``/
    ``publish_first``, which run a real build. Returns the new row's id.
    ``first_record_date``/``window_start_date``/``wind_start_date`` default
    to ``cutoff`` minus ``window_days`` - 1 when not given.
    """
    first_record_date = first_record_date or (cutoff - timedelta(days=window_days - 1))
    window_start_date = window_start_date or first_record_date
    wind_start_date = wind_start_date or first_record_date
    build_started_at = build_started_at or published_at
    cur = conn.execute(
        "INSERT INTO publications"
        " (site_id, cutoff, first_record_date, window_days, window_start_date,"
        "  wind_start_date, settings, methodology_version, scoring_rule,"
        "  wind_method, wind_constants, build_started_at, published_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            cutoff.isoformat(),
            first_record_date.isoformat(),
            window_days,
            window_start_date.isoformat(),
            wind_start_date.isoformat(),
            json.dumps({"rolling_window_days": window_days, "min_n": 0}),
            methodology_version,
            scoring_rule,
            wind_method,
            wind_constants,
            isoformat_utc(build_started_at),
            isoformat_utc(published_at),
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def seed_coverage(
    conn: sqlite3.Connection,
    *,
    publication_id: int,
    variable: str,
    window_key: str,
    day_ahead: int,
    feed_id: int,
    scored: int,
    expected: int,
    starting_days: int = 0,
) -> None:
    """R4.2's coverage cell (one ``publication_coverage`` row, P4 §7.4).

    What ``published_coverage(conn, pub=…, variable=…, window_key=…)``
    returns, keyed by ``(feed_id, day_ahead)``: the forecasts scored and
    expected, with the starting days apart. Plain INSERT: P4 writes this
    table only during a build.
    """
    conn.execute(
        "INSERT INTO publication_coverage"
        " (publication_id, variable, window_key, day_ahead, feed_id, scored,"
        "  expected, starting_days)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            publication_id,
            variable,
            window_key,
            day_ahead,
            feed_id,
            scored,
            expected,
            starting_days,
        ),
    )


def seed_closed_pairs(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    feed_id: int,
    variable: str,
    day_ahead: int,
    valid_at: str,
    lead_hours: int,
    forecast: float,
    observed: float,
    download_date: date | None = None,
    fetched_at: str | None = None,
) -> None:
    """R4.3's closed-day pair (one ``close_pairs`` row, over P4's
    ``close_pairs``, P4 §16.2).

    What ``closed_day_pairs(conn, site_id=…, local_date=…, feed_id=…)``
    returns: one hour's forecast and observed value for a closed day and
    feed, the forecast and the observed value the scores rest on. Plain
    INSERT: P4 writes ``close_pairs`` only from inside ``score_day``.
    """
    conn.execute(
        "INSERT INTO close_pairs"
        " (site_id, local_date, feed_id, variable, day_ahead, valid_at,"
        "  lead_hours, forecast, observed, download_date, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            valid_at,
            lead_hours,
            forecast,
            observed,
            None if download_date is None else download_date.isoformat(),
            fetched_at,
        ),
    )


# ---------------------------------------------------------------------------
# Moved from ``tests/test_verification_run.py`` (0.16.14 Part 6, Step 4
# piece 2/3: that file's subject, the replay chain, is removed whole in
# piece 5, but ``tests/test_run_input_manifest.py`` imports this name from
# it for kept behaviour). ``ensure_published_generation`` is swapped for
# ``ensure_site_generation`` per the switch-over plan's rule 2; every other
# name and value is unchanged.
# ---------------------------------------------------------------------------

_VR_PERIOD_DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]
_VR_QUANTITY_VALUES = {
    "temperature_high": 21.0,
    "temperature_low": 9.0,
    "wind_max": 6.0,
    "precip_total": 0.0,
    "precip_occurrence": 0.0,
}


def _make_verification_site(conn: sqlite3.Connection) -> tuple[int, list[int]]:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('verify-town', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    feeds = [
        asof_make_real_feed(conn, "model-alpha"),
        asof_make_real_feed(conn, "model-beta"),
    ]
    generation_id = ensure_site_generation(conn, site_id)
    for day in _VR_PERIOD_DAYS:
        for quantity, value in _VR_QUANTITY_VALUES.items():
            conn.execute(
                """
                INSERT INTO daily_truth
                    (site_id, local_date, quantity, value, eligible,
                     covered_hours, expected_slots, wet_hours, dry_hours,
                     rain_threshold_mm, day_start_utc, day_end_utc, timezone,
                     tz_generation_id)
                VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 0.2, ?, ?, 'UTC', ?)
                """,
                (
                    site_id,
                    day,
                    quantity,
                    value,
                    0 if quantity.startswith("precip") else None,
                    24 if quantity.startswith("precip") else None,
                    f"{day}T00:00:00Z",
                    f"{day}T23:59:59Z",
                    generation_id,
                ),
            )
    values = {"temperature": 15.0, "wind": 5.0, "precip": 0.0}
    issued = "2026-05-31T05:00:00Z"
    for feed_index, feed_id in enumerate(feeds):
        for variable, base in values.items():
            for day_offset in range(len(_VR_PERIOD_DAYS) + 8):
                for hour in range(24):
                    total_hours = day_offset * 24 + hour
                    valid = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(
                        hours=total_hours
                    )
                    conn.execute(
                        """
                        INSERT INTO forecast_samples
                            (site_id, feed_id, variable, issued_at, valid_at,
                             lead_hours, value, source_raw, model_run_id,
                             fetched_at)
                        VALUES (?, ?, ?, ?, ?, 6, ?, '{}', 'run-a', ?)
                        """,
                        (
                            site_id,
                            feed_id,
                            variable,
                            issued,
                            valid.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            base + 0.5 * feed_index,
                            issued,
                        ),
                    )
    conn.commit()
    return site_id, feeds


def _advance_samples(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """Move `sample_high_water` only — the routine nightly divergence driver.

    The sample is valid a year past the simulated period, so it lands
    outside every lead bucket and changes the fingerprint without changing
    a single scored value.
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS hi FROM forecast_samples"
    ).fetchone()
    stamp = f"2027-06-01T{int(row['hi']) % 24:02d}:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', '2027-05-31T05:00:00Z', ?, 6, 15.0,
                '{}', 'run-z', '2027-05-31T05:00:00Z')
        """,
        (site_id, feed_id, stamp),
    )

"""Shared test helpers for wxverify's pytest suite.

Kept deliberately small: only helpers actually reused across multiple test
modules belong here. Anything used by a single file stays local to it.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
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
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring.consensus import materialize_consensus, write_station_observation
from wxverify.verification.decision import ContinuousLead, OccurrenceLead
from wxverify.verification.methodology import (
    CONTINUOUS_BASELINES,
    OCCURRENCE_BASELINES,
)
from wxverify.worker.close_p4 import ScoreSummary
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
        generation_id = ensure_published_generation(conn, site_id)
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


# ---------------------------------------------------------------------------
# Synthetic verification-site builder (write-lock-fix plan §5.2).
# Shared by the day-pipeline / day-differential / write-lock-starvation
# tests and by the one-shot T5 golden script. All values are synthetic.
# ---------------------------------------------------------------------------

_SYNTH_TRUTH_DAYS = [
    f"2026-01-{d:02d}" for d in range(1, 25)
]  # 2026-01-01 .. 2026-01-24
_SYNTH_TRUTH_QUANTITIES = {
    "temperature_high": 15.0,
    "temperature_low": 15.0,
    "wind_max": 5.0,
    "precip_total": 0.0,
    "precip_occurrence": 0.0,
}
_SYNTH_FEED_OFFSETS = {"synthetic-a": 0.5, "synthetic-b": 1.0, "synthetic-c": 1.5}
_SYNTH_ANOMALY_CUTOFF = datetime(2026, 1, 16, tzinfo=UTC)
_SYNTH_ANOMALY_VALUES = {"temperature": 45.5, "wind": 35.5, "precip": 2.0}
_SYNTH_BASE_VALUES = {"temperature": 15.0, "wind": 5.0, "precip": 0.0}


def build_synthetic_verification_site(
    conn: sqlite3.Connection,
) -> tuple[int, list[int], int]:
    """Build the write-lock-fix plan's §5.2 synthetic verification site.

    Runs in one ``BEGIN ... COMMIT`` and rolls back on error. Returns
    ``(site_id, [feed_a, feed_b, feed_c], station_id)``.
    """
    if conn.in_transaction:
        conn.commit()
    conn.execute("BEGIN")
    try:
        cur = conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('site-synthetic', 0.0, 0.0, 0.0, 'UTC')
            """
        )
        assert cur.lastrowid is not None
        site_id = int(cur.lastrowid)

        feeds: list[int] = []
        for model in ("synthetic-a", "synthetic-b", "synthetic-c"):
            fcur = conn.execute(
                """
                INSERT INTO feeds
                    (source, model, default_subscribed,
                     fetch_interval_minutes, max_lead_hours)
                VALUES ('synthetic-src', ?, 1, 360, 216)
                """,
                (model,),
            )
            assert fcur.lastrowid is not None
            feeds.append(int(fcur.lastrowid))
        feed_a, feed_b, feed_c = feeds

        generation_id = ensure_published_generation(conn, site_id)

        scur = conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
            VALUES (?, 'SYNTH-LOCKFIX-01', 0.0, 0.0, 0.0, 1)
            """,
            (site_id,),
        )
        assert scur.lastrowid is not None
        station_id = int(scur.lastrowid)

        # Truth.
        for day in _SYNTH_TRUTH_DAYS:
            for quantity, value in _SYNTH_TRUTH_QUANTITIES.items():
                is_precip = quantity.startswith("precip")
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
                        0 if is_precip else None,
                        24 if is_precip else None,
                        f"{day}T00:00:00Z",
                        f"{day}T23:59:59Z",
                        generation_id,
                    ),
                )

        # Samples: one issuance per snapshot day S, hourly out to 192h.
        for feed_id, model in zip(
            feeds, ("synthetic-a", "synthetic-b", "synthetic-c"), strict=True
        ):
            offset = _SYNTH_FEED_OFFSETS[model]
            for day in _SYNTH_TRUTH_DAYS:
                snapshot_date = datetime.fromisoformat(day + "T00:00:00+00:00")
                issued = snapshot_date - timedelta(hours=6)  # (S-1)T18:00:00Z
                for hour in range(1, 193):
                    valid_at = issued + timedelta(hours=hour)
                    for variable, base in _SYNTH_BASE_VALUES.items():
                        if model == "synthetic-a" and valid_at >= _SYNTH_ANOMALY_CUTOFF:
                            value = _SYNTH_ANOMALY_VALUES[variable]
                        else:
                            value = base + offset
                        conn.execute(
                            """
                            INSERT INTO forecast_samples
                                (site_id, feed_id, variable, issued_at, valid_at,
                                 lead_hours, value, source_raw, model_run_id,
                                 fetched_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-synthetic', ?)
                            """,
                            (
                                site_id,
                                feed_id,
                                variable,
                                issued.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                valid_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                hour,
                                value,
                                issued.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            ),
                        )

        # Pairs: temperature, every 6h from 2025-12-02T00:00Z to
        # 2026-01-24T18:00Z, day_ahead 1..7.
        persistence_feed_id = asof_persistence_feed_id(conn)
        pair_feeds = {
            feed_a: _SYNTH_FEED_OFFSETS["synthetic-a"],
            feed_b: _SYNTH_FEED_OFFSETS["synthetic-b"],
            feed_c: _SYNTH_FEED_OFFSETS["synthetic-c"],
            persistence_feed_id: 2.0,
        }
        pair_start = datetime(2025, 12, 2, tzinfo=UTC)
        pair_end = datetime(2026, 1, 24, 18, tzinfo=UTC)
        valid_at = pair_start
        while valid_at <= pair_end:
            for day_ahead in range(1, 8):
                lead_hours = 24 * day_ahead + 6
                issued_at = valid_at - timedelta(hours=lead_hours)
                first_known_at = valid_at + timedelta(hours=1)
                for feed_id, feed_offset in pair_feeds.items():
                    asof_insert_pair(
                        conn,
                        site_id=site_id,
                        feed_id=feed_id,
                        valid_at=valid_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        issued_at=issued_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        forecast=15.0 + feed_offset,
                        observed=15.0,
                        first_known_at=first_known_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        day_ahead=day_ahead,
                        lead_hours=lead_hours,
                        generation_id=generation_id,
                    )
            valid_at += timedelta(hours=6)

        conn.commit()
        return site_id, feeds, station_id
    except Exception:
        conn.rollback()
        raise


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
    ensure_published_generation(conn, site_id)

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
    "P4."). Answers per ``(site_id, round_date)``, programmed with
    :meth:`set_answer`; :meth:`raise_for_site` makes every call for one
    site raise instead (TL10's per-site fault), leaving other sites
    unaffected. Every call is recorded in :attr:`calls`, in call order.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[int, date]] = []
        self._answers: dict[tuple[int, date], tuple[bool, datetime]] = {}
        self._site_raises: dict[int, BaseException | type[BaseException]] = {}

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
        settled, bound_utc = self._answers[(site_id, round_date)]
        from wxverify.worker.close_p4 import RoundState  # P3 name, §14.1

        return RoundState(settled=settled, bound_utc=bound_utc)


def install_round_state_standin(monkeypatch: pytest.MonkeyPatch) -> RoundStateStandIn:
    """Patch ``close_p4.round_state`` with a fresh :class:`RoundStateStandIn`
    (plan §15.1 "P4."). ``close_p4.py`` does not exist until piece 3, so
    this raises ``ModuleNotFoundError`` until then -- the right failure for
    any TL test that calls it before that piece lands.
    """
    standin = RoundStateStandIn()
    monkeypatch.setattr("wxverify.worker.close_p4.round_state", standin, raising=False)
    return standin


class RecordingStandIn:
    """Generic recorder for a sync P4 call point (plan §14.1's
    ``score_day``; §15.1 "P4."). Records every call's ``kwargs`` in
    :attr:`calls`; returns :attr:`result` (calling it first if it is a
    plain callable) or raises :attr:`raises` when the test sets one.
    Deliberately ignorant of P4's own return types -- the test that
    configures ``result`` builds those itself, from ``close_p4`` directly.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.result: object = None
        self.raises: BaseException | type[BaseException] | None = None

    def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        result = self.result
        if callable(result) and not isinstance(result, type):
            return result(*args, **kwargs)
        return result


class AsyncRecordingStandIn(RecordingStandIn):
    """:class:`RecordingStandIn` for an async P4 call point (``publish``,
    ``clean_worksheets``, plan §14.1)."""

    async def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        result = self.result
        if callable(result) and not isinstance(result, type):
            return result(*args, **kwargs)
        return result


def install_p4_standins(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, RecordingStandIn]:
    """Patch ``close_p4``'s other three call points -- ``score_day``,
    ``publish``, ``clean_worksheets`` -- with generic recorders (plan
    §15.1 "P4."; stage 1's ``round_state`` has its own
    :func:`install_round_state_standin`, since its answer is keyed by
    round date and may need to fault one site only). Raises
    ``ModuleNotFoundError`` until piece 3 adds ``close_p4.py``.
    """
    score_day = RecordingStandIn()
    score_day.result = ScoreSummary(scored={}, absent={})
    publish = AsyncRecordingStandIn()
    clean_worksheets = AsyncRecordingStandIn()
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
    "_enqueue_due_feeds",
    "_enqueue_due_obs",
    "_enqueue_due_wind_days",
    "_enqueue_due_forecast_records",
    "_enqueue_due_verification_runs",
    "_enqueue_due_score_refresh",
)


def disable_other_enqueuers(
    monkeypatch: pytest.MonkeyPatch, *, keep: Sequence[str] = ()
) -> None:
    """No-op every one of ``scheduler_tick``'s non-close enqueuers except
    those named in ``keep`` (plan §15.1 "Other job kinds."; DO1/DO2 keep
    ``_enqueue_due_verification_runs``/``_enqueue_due_score_refresh``,
    TL7 keeps ``_enqueue_due_forecast_records``). Without this, the first
    tick of a standard night would also queue a ``fetch_feed`` job per
    seeded open-meteo feed and a ``fetch_obs`` ``obs`` job for the site,
    which ``run_due_jobs`` would then claim.
    """
    for name in _NON_CLOSE_ENQUEUERS:
        if name in keep:
            continue
        monkeypatch.setattr(f"wxverify.worker.scheduler.{name}", lambda conn: None)


def stub_fetch_feed(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
    """Replace ``dispatch``'s real ``fetch_feed`` handler
    (``worker/processor.py:720``) with a stub recording ``(site_id,
    feed_id)`` and returning ``None`` (plan §15.1 "Other job kinds.",
    TL8). Without this a seeded ``fetch_feed`` job would run through its
    real handler and trip the autouse network guard.
    """
    calls: list[tuple[int, int]] = []

    async def _stub(
        db: Database, writer: FencedWriter, site_id: int, feed_id: int
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

"""Shared test helpers for wxverify's pytest suite.

Kept deliberately small: only helpers actually reused across multiple test
modules belong here. Anything used by a single file stays local to it.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser

from wxverify.db.connection import _READ_POOL_SIZE, Database  # noqa: SLF001
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.decision import ContinuousLead, OccurrenceLead
from wxverify.verification.methodology import (
    CONTINUOUS_BASELINES,
    OCCURRENCE_BASELINES,
)


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

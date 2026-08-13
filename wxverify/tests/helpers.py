"""Shared test helpers for wxverify's pytest suite.

Kept deliberately small: only helpers actually reused across multiple test
modules belong here. Anything used by a single file stays local to it.
"""

from __future__ import annotations

import sqlite3
from html.parser import HTMLParser

from wxverify.db.connection import _READ_POOL_SIZE, Database  # noqa: SLF001
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation


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
        VALUES (?, 47.0, 25.0, 900.0, 'UTC')
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

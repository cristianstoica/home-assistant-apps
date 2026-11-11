"""Read-side queries for the Forecast page.

All queries here are read-only and run on the read connection. Three shared
exclusions apply to every sample-facing query (applied explicitly, not
incidentally via intersections):

* virtual feeds (``is_virtual = 1``) never surface on the Forecast page —
  they are scoring baselines, not forward forecasts a user would consume;
* the ``(meteoblue, multimodel)`` package feed is the subscription unit and
  carries no forward samples of its own — the member-model feeds do;
* samples failing :func:`invalid_forecast_sample_sql` (out-of-range values,
  unknown variables, malformed timestamps, ``lead_hours < 1``) are dropped,
  which is also what filters negative precip out of daily totals.

The sample loader adds one more: feeds not served at the site, through
``served_only`` on :func:`load_future_samples` (:func:`served_feed_ids`).
The display loader (:func:`load_display_samples`) reads the display
roster instead, so it drops invalid rows by
:func:`invalid_stored_sample_sql` and always drops unserved feeds.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.collection.forecast_validation import (
    DISPLAY_VARIABLES,
    FORECAST_VARIABLES,
    invalid_forecast_sample_sql,
    invalid_stored_sample_sql,
)
from wxverify.collection.precip_labels import precip_label_eligible_sql
from wxverify.core.timeutil import day_ahead as issue_day_ahead
from wxverify.core.timeutil import parse_utc
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import (
    LeaderboardRow,
    LeaderboardStatus,
    leaderboard_with_status,
    leaderboard_with_status_in_transaction,
)
from wxverify.worker.cadence import parse_fetch_interval_minutes

EXCLUDED_FEEDS_SQL = (
    "f.is_virtual = 0 AND NOT (f.source = 'meteoblue' AND f.model = 'multimodel')"
)


@dataclass(frozen=True)
class FutureSampleRow:
    """One validated, latest-run forecast sample with feed identity attached.

    ``download_date`` is the local date of the latest download that
    brought the sample's run label; None keeps the issue-time day ahead
    (:func:`sample_day_ahead`).
    """

    feed_id: int
    source: str
    model: str
    variable: str
    issued_at: str
    valid_at: str
    value: float
    download_date: date | None = None


FetchState = Literal["fresh", "stale", "unknown"]


@dataclass(frozen=True)
class FeedFreshness:
    """Per-feed collection freshness, judged against the evidence feed's cadence.

    ``fetch_state`` compares ``last_usable_fetch_at`` (when the app last
    completed a forward fetch that returned a usable sample) with twice
    ``fetch_interval_minutes``. Both are read from ``evidence_feed_id``: the
    site's ``(meteoblue, multimodel)`` package for a meteoblue member feed,
    otherwise the feed itself. ``fetch_interval_minutes`` is ``None`` when the
    evidence feed's cadence is unreadable, which reads as stale.
    ``latest_issued_at`` is the feed's newest displayable run; it does not
    decide staleness.
    """

    feed_id: int
    latest_issued_at: str
    fetch_interval_minutes: int | None
    fetch_state: FetchState
    last_usable_fetch_at: str | None
    evidence_feed_id: int

    @property
    def stale(self) -> bool:
        """True when the feed has not been usefully fetched within 2x its cadence."""
        return self.fetch_state == "stale"


@dataclass(frozen=True)
class ForecastRanking:
    """Ranking rows for one cell, with the leaderboard cache status."""

    rows: dict[int, LeaderboardRow]
    status: LeaderboardStatus


def served_feed_ids(conn: sqlite3.Connection, *, site_id: int) -> frozenset[int]:
    """Real feeds served at ``site_id``: the Leaderboard rule, minus virtual
    feeds and the meteoblue package row. Same set as ``roster_feeds``.

    Reads on ``conn`` inside whatever snapshot the caller holds; it opens no
    transaction of its own.
    """
    clause = active_competitor_clause(site_expr=str(int(site_id)))
    rows = conn.execute(
        f"""
        SELECT f.id
        FROM feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = ? AND sfs.feed_id = f.id
        WHERE {EXCLUDED_FEEDS_SQL}
          AND {clause}
        """,
        (site_id,),
    ).fetchall()
    return frozenset(int(row[0]) for row in rows)


def load_future_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    since_valid_at: str,
    served_only: bool,
) -> list[FutureSampleRow]:
    """Load the latest-run future samples for a site.

    Latest-run pick: for each ``(feed, variable, valid_at)`` slot only the
    sample from the newest run survives. ``UNIQUE(site_id, feed_id, variable,
    issued_at, valid_at)`` makes ``issued_at`` unique within a slot, so
    ``ROW_NUMBER() = 1`` over ``issued_at DESC`` selects exactly the row the old
    ``issued_at = (SELECT MAX(...))`` form selected -- with no ties possible and
    no correlated rescan per row. The validity predicate is applied before the
    window function, so an invalid sample from a newer run still cannot shadow a
    valid older one. The statement names idx_samples_site_var_valid with
    ``INDEXED BY``, so the read seeks the site's samples from
    ``since_valid_at`` on; without the hint the planner picks
    idx_samples_slot to skip the window's sort, which walks the site's whole
    history. ``variable IN (...)``, implied by ``NOT invalid``, is still
    stated so that index's key reaches ``valid_at``.
    The rain-label boundary (:func:`precip_label_eligible_sql`) is applied there
    too, so an excluded newer run cannot hide an eligible older one.

    ``served_only`` has no default so each caller states its policy: when
    True, rows whose feed is not in :func:`served_feed_ids` are dropped after
    the read, in Python, leaving the statement untouched. The simulation
    passes False because it filters to its pinned roster.

    Each row also carries ``download_date`` (:func:`_download_dates`);
    the statement selects ``fetched_at`` for it. The statement is
    :func:`future_samples_sql`'s.
    """
    rows = conn.execute(
        future_samples_sql(),
        (site_id, since_valid_at),
    ).fetchall()
    served = served_feed_ids(conn, site_id=site_id) if served_only else None
    kept = [row for row in rows if served is None or int(row["feed_id"]) in served]
    download_dates = _download_dates(conn, site_id=site_id, rows=kept)
    return [
        FutureSampleRow(
            feed_id=int(row["feed_id"]),
            source=str(row["source"]),
            model=str(row["model"]),
            variable=str(row["variable"]),
            issued_at=str(row["issued_at"]),
            valid_at=str(row["valid_at"]),
            value=float(row["value"]),
            download_date=download_date,
        )
        for row, download_date in zip(kept, download_dates, strict=True)
    ]


def future_samples_sql() -> str:
    """The statement :func:`load_future_samples` runs.

    Module-level so a query-plan test plans the text that ships. Both
    select lists carry ``fetched_at``.
    """
    invalid = invalid_forecast_sample_sql("fs")
    eligible = precip_label_eligible_sql("fs", "f")
    variables = ", ".join(f"'{variable}'" for variable in FORECAST_VARIABLES)
    return f"""
        SELECT feed_id, source, model, variable, issued_at, valid_at, value, fetched_at
        FROM (
            SELECT fs.feed_id, f.source, f.model, fs.variable, fs.issued_at,
                   fs.valid_at, fs.value, fs.fetched_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY fs.feed_id, fs.variable, fs.valid_at
                       ORDER BY fs.issued_at DESC
                   ) AS rn
            FROM forecast_samples fs INDEXED BY idx_samples_site_var_valid
            JOIN feeds f ON f.id = fs.feed_id
            WHERE fs.site_id = ?
              AND fs.variable IN ({variables})
              AND fs.valid_at >= ?
              AND {EXCLUDED_FEEDS_SQL}
              AND NOT {invalid}
              AND {eligible}
        )
        WHERE rn = 1
        ORDER BY valid_at, feed_id
        """


_DISPLAY_VARIABLES_SQL: Final = ", ".join(
    f"'{variable}'" for variable in DISPLAY_VARIABLES
)

#: The display rows :func:`load_display_samples` reads. One row per
#: slot (``idx_samples_slot``), so there is no latest-run pick, no rain-label
#: rule and no download date. Module-level so a query-plan test plans the
#: text that ships.
DISPLAY_SAMPLES_SQL: Final = f"""
    SELECT fs.feed_id, f.source, f.model, fs.variable, fs.issued_at,
           fs.valid_at, fs.value
    FROM forecast_samples fs
    JOIN feeds f ON f.id = fs.feed_id
    WHERE fs.site_id = ?
      AND fs.variable IN ({_DISPLAY_VARIABLES_SQL})
      AND fs.valid_at >= ?
      AND {EXCLUDED_FEEDS_SQL}
      AND NOT {invalid_stored_sample_sql("fs")}
    ORDER BY fs.valid_at, fs.feed_id
    """


def load_display_samples(
    conn: sqlite3.Connection, *, site_id: int, since_valid_at: str
) -> list[FutureSampleRow]:
    """Load a site's display samples valid at or after a stamp.

    Runs :data:`DISPLAY_SAMPLES_SQL`: cloud cover, gust and wind direction,
    valid rows only (:func:`invalid_stored_sample_sql`), with the shared feed
    exclusions, ordered by ``valid_at`` then feed. Rows whose feed is not in
    :func:`served_feed_ids` are then dropped in Python, as
    :func:`load_future_samples` does with ``served_only``. ``download_date``
    stays None: display rows are never scored or looked up by day ahead.
    """
    rows = conn.execute(DISPLAY_SAMPLES_SQL, (site_id, since_valid_at)).fetchall()
    served = served_feed_ids(conn, site_id=site_id)
    return [
        FutureSampleRow(
            feed_id=int(row["feed_id"]),
            source=str(row["source"]),
            model=str(row["model"]),
            variable=str(row["variable"]),
            issued_at=str(row["issued_at"]),
            valid_at=str(row["valid_at"]),
            value=float(row["value"]),
        )
        for row in rows
        if int(row["feed_id"]) in served
    ]


def sample_day_ahead(sample: FutureSampleRow, timezone: str) -> int:
    """The day ahead a sample is looked up at.

    From its ``download_date`` when it has one: the days from that date to
    the sample's local valid date in ``timezone``. Otherwise the issue-time
    day ahead, from its run label. The served forecast reads it.
    """
    if sample.download_date is None:
        return issue_day_ahead(sample.issued_at, sample.valid_at, timezone)
    valid_day = parse_utc(sample.valid_at).astimezone(ZoneInfo(timezone)).date()
    return (valid_day - sample.download_date).days


def _download_dates(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    rows: list[sqlite3.Row],
) -> list[date | None]:
    """Each row's download date, in ``rows``' order.

    The ``local_date`` of the latest ``stored`` ``forecast_downloads`` row of
    the site (by ``ended_at``, then ``id``) whose labels for the sample's
    feed include its ``issued_at``; a meteoblue member's labels sit in its
    package's row under the member's key, so every row of the site is
    looked at. No row ends after the read, so no upper bound is needed.
    No download can carry a label before its run was issued, so rows that
    ended before the earliest ``issued_at`` are not read. A label no row
    carries counts from the local date of the sample's ``fetched_at`` in the
    site's zone; a NULL ``fetched_at`` (or a zone that does not load) gives
    None, the issue-time day ahead.
    """
    if not rows:
        return []
    pending: dict[str, set[str]] = {}
    for row in rows:
        pending.setdefault(str(int(row["feed_id"])), set()).add(str(row["issued_at"]))
    earliest = min(str(row["issued_at"]) for row in rows)
    downloads = conn.execute(
        """
        SELECT local_date, labels
        FROM forecast_downloads
        WHERE site_id = ?
          AND outcome = 'stored'
          AND julianday(ended_at) >= julianday(?)
        ORDER BY ended_at DESC, id DESC
        """,
        (site_id, earliest),
    ).fetchall()
    found: dict[tuple[str, str], date] = {}
    for download in downloads:
        try:
            local_date = date.fromisoformat(str(download["local_date"]))
        except ValueError:
            continue
        for key, labels in _download_labels(download["labels"]).items():
            wanted = pending.get(key)
            if not wanted:
                continue
            for label in labels:
                if label in wanted:
                    wanted.discard(label)
                    found[(key, label)] = local_date
        if not any(pending.values()):
            break
    zone = _site_zone(conn, site_id)
    return [
        found.get((str(int(row["feed_id"])), str(row["issued_at"])))
        or _local_fetch_date(row["fetched_at"], zone)
        for row in rows
    ]


def _download_labels(raw: object) -> dict[str, list[str]]:
    """A download record's ``labels`` JSON object; ``{}`` for anything else.

    Keys that are not strings, values that are not lists and list items
    that are not strings are dropped, as the scoring side reads them.
    """
    if not isinstance(raw, str):
        return {}
    try:
        parsed: object = json.loads(raw)
    except (ValueError, RecursionError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    labels: dict[str, list[str]] = {}
    for key, value in cast(dict[object, object], parsed).items():
        if isinstance(key, str) and isinstance(value, list):
            items = cast(list[object], value)
            labels[key] = [item for item in items if isinstance(item, str)]
    return labels


def _site_zone(conn: sqlite3.Connection, site_id: int) -> ZoneInfo | None:
    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None:
        return None
    try:
        return ZoneInfo(str(row["timezone"]))
    except (ZoneInfoNotFoundError, ValueError):
        return None


def _local_fetch_date(fetched_at: object, zone: ZoneInfo | None) -> date | None:
    if fetched_at is None or zone is None:
        return None
    try:
        return parse_utc(str(fetched_at)).astimezone(zone).date()
    except ValueError:
        return None


def load_feed_freshness(
    conn: sqlite3.Connection, *, site_id: int, now: datetime
) -> dict[int, FeedFreshness]:
    """Per-feed collection freshness vs 2x the evidence feed's fetch interval.

    Candidates are the displayable feeds with at least one valid sample for
    the site (a feed with none has no entry). Each is judged on
    its evidence feed's ``site_feed_state.last_usable_fetch_at`` and its own
    ``fetch_interval_minutes`` (never a global constant), in this order: an
    unreadable interval is ``stale`` (fail closed); a missing or unparseable
    stamp is ``unknown``; a stamp older than ``now - 2 x interval`` is
    ``stale``; otherwise ``fresh`` (the boundary itself is fresh). A meteoblue
    member feed is fetched on its package's cadence, so the package is its
    evidence feed.
    """
    invalid = invalid_forecast_sample_sql("fs")
    grid = ", ".join(f"('{variable}')" for variable in FORECAST_VARIABLES)
    rows = conn.execute(
        f"""
        WITH grid_variables(variable) AS (VALUES {grid}),
        candidates AS (
            SELECT f.id AS feed_id, v.variable
            FROM feeds f, grid_variables v
            WHERE {EXCLUDED_FEEDS_SQL}
        ),
        latest AS (
            SELECT c.feed_id,
                   MAX((
                       SELECT fs.issued_at
                       FROM forecast_samples fs
                       WHERE fs.site_id = ?
                         AND fs.feed_id = c.feed_id
                         AND fs.variable = c.variable
                         AND NOT {invalid}
                       ORDER BY fs.issued_at DESC
                       LIMIT 1
                   )) AS latest_issued_at
            FROM candidates c
            GROUP BY c.feed_id
            HAVING latest_issued_at IS NOT NULL
        )
        SELECT l.feed_id, l.latest_issued_at, e.id AS evidence_feed_id,
               e.fetch_interval_minutes, sfs.last_usable_fetch_at
        FROM latest l
        JOIN feeds f ON f.id = l.feed_id
        LEFT JOIN feeds pkg
          ON f.source = 'meteoblue' AND f.model != 'multimodel'
         AND pkg.source = 'meteoblue' AND pkg.model = 'multimodel'
        JOIN feeds e ON e.id = COALESCE(pkg.id, f.id)
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = ? AND sfs.feed_id = e.id
        """,
        (site_id, site_id),
    ).fetchall()
    out: dict[int, FeedFreshness] = {}
    for row in rows:
        feed_id = int(row["feed_id"])
        evidence_feed_id = int(row["evidence_feed_id"])
        raw_stamp: object = row["last_usable_fetch_at"]
        stamp = raw_stamp if isinstance(raw_stamp, str) else None
        # Foreign/corrupt or out-of-range cadence parses to None and fails
        # closed (stale) rather than dropping the feed from the map or
        # inventing a cadence to judge it against.
        interval = parse_fetch_interval_minutes(
            row["fetch_interval_minutes"],
            context=(
                f"feed freshness feed_id={feed_id} evidence_feed_id={evidence_feed_id}"
            ),
        )
        out[feed_id] = FeedFreshness(
            feed_id=feed_id,
            latest_issued_at=str(row["latest_issued_at"]),
            fetch_interval_minutes=interval,
            fetch_state=_fetch_state(stamp, interval=interval, now=now),
            last_usable_fetch_at=stamp,
            evidence_feed_id=evidence_feed_id,
        )
    return out


def _fetch_state(
    stamp: str | None, *, interval: int | None, now: datetime
) -> FetchState:
    """Apply the freshness precedence: bad interval, then no stamp, then age."""
    if interval is None:
        return "stale"
    if stamp is None:
        return "unknown"
    try:
        fetched = parse_utc(stamp)
    except ValueError:
        return "unknown"
    if fetched < now - timedelta(minutes=2 * interval):
        return "stale"
    return "fresh"


def samples_fingerprint(conn: sqlite3.Connection, *, site_id: int) -> str:
    """Monotonic change token for a site's stored samples: MAX(rowid).

    A sample written new or changed takes the next row id
    (``persist_fetch_result``), which advances the fingerprint. A fetch whose
    samples are all already stored writes nothing and leaves it unchanged,
    which is correct because nothing stored changed. The tiles auto-poll
    reads it through :func:`forecast_fingerprint`, which also follows the
    fetch stamps.
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS fp FROM forecast_samples WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    return str(int(row["fp"]))


def forecast_fingerprint(conn: sqlite3.Connection, *, site_id: int) -> str:
    """Change token for the tiles auto-poll: samples plus usable-fetch stamps.

    A fetch whose samples are all already stored leaves
    :func:`samples_fingerprint` unchanged but advances the evidence feed's
    ``site_feed_state.last_usable_fetch_at``, which moves the "Last fetched"
    label and the stale badges. The token is ``"<samples>-<total>"``, where
    ``total`` sums the whole-second POSIX times of the site's stamps that are
    text and parse; the rest are skipped. The writer sets a stamp to its
    fetch's ``fetched_at`` without forcing it forward, so the sum changes when
    one stamp moves to another whole second, though stamps moving in opposite
    directions can cancel out; a move within the same whole second changes
    neither the token nor the whole-second "Last fetched" label. With no
    parseable stamp the token is :func:`samples_fingerprint` followed by the
    publication part, so a site with no samples, no stamps and no publication
    is ``"0-p0"``. An unchanged token lets the tiles fragment answer 204 and
    leave the open drill-down untouched.

    The token ends in ``-p<id>``, the active publication's id (``0`` for
    none), so an open page swaps its tiles when a publication lands.
    """
    samples = samples_fingerprint(conn, site_id=site_id)
    rows = conn.execute(
        "SELECT last_usable_fetch_at FROM site_feed_state WHERE site_id = ?",
        (site_id,),
    ).fetchall()
    total = 0
    parsed_any = False
    for row in rows:
        raw_stamp: object = row["last_usable_fetch_at"]
        if not isinstance(raw_stamp, str):
            continue
        try:
            fetched = parse_utc(raw_stamp)
        except ValueError:
            continue
        total += int(fetched.timestamp())
        parsed_any = True
    if not parsed_any:
        return samples + _publication_token(conn, site_id=site_id)
    return f"{samples}-{total}" + _publication_token(conn, site_id=site_id)


def _publication_token(conn: sqlite3.Connection, *, site_id: int) -> str:
    """``-p<id>`` of the active publication, ``-p0`` with none."""
    # Deferred: the publication module imports this one.
    from wxverify.scoring.publication import active_publication

    pub = active_publication(conn, site_id=site_id, at_utc=None)
    return f"-p{0 if pub is None else pub.id}"


def forecast_ranking_with_status(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str = "rolling",
) -> ForecastRanking:
    """Skill ranking for one (variable, day_ahead) cell, keyed by feed id.

    Reuses the leaderboard skill computation, then applies the Forecast-page
    exclusions AT the ranking step by design: virtual feeds and the meteoblue
    package feed are removed here explicitly, not left to the intersection
    with fresh samples. Keep the filter here even if it looks redundant.

    The verdict is read inside one read snapshot either way. With no
    transaction open on ``conn`` it calls the ``leaderboard_with_status``
    facade, which opens its own. Inside a forecast route's read snapshot it
    calls ``leaderboard_with_status_in_transaction``, because the facade's
    snapshot refuses to nest; the route's snapshot then covers the verdict
    and every other read the response renders.
    """
    excluded = {
        int(row["id"])
        for row in conn.execute(
            """
            SELECT id FROM feeds
            WHERE is_virtual = 1
               OR (source = 'meteoblue' AND model = 'multimodel')
            """
        ).fetchall()
    }
    if conn.in_transaction:
        result = leaderboard_with_status_in_transaction(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
    else:
        result = leaderboard_with_status(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
    return ForecastRanking(
        rows={row.feed_id: row for row in result.rows if row.feed_id not in excluded},
        status=result.status,
    )

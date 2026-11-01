"""The scored download of a feed's local day, and the forecasts it gives.

Daily-close P4 plan (``plans/2026-10-22-daily-close-pairs-and-publication.md``)
4.1, 4.3 and 4.7. For each feed, site and local day S, the scored download
of S is the first download of S stored for the feed: the earliest ``stored``
row of ``forecast_downloads`` (4.2), or, for a day the record does not
cover, the download inferred from ``forecast_samples`` and the completed
``fetch_feed`` job rows (4.7).

Since the rain release (``plans/2026-11-01-rain-release.md``, Part B) the
close reads the scored values from ``forecast_scored`` (``scored_values``),
which the fetch path writes for a day's first stored download, and
``forecast_samples`` keeps one row per hour, the newest value
(``newest_values``). ``scored_download`` and ``scored_version`` are read
only by the v16 carry-over (``collection/forecast_store.py``), over a file
that can still hold several labels per hour.

Read-only: every function here runs in the caller's snapshot or
transaction and writes nothing.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Literal, cast

from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.collection.precip_labels import precip_label_eligible_sql
from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.db.migrations import DOWNLOAD_RECORD_SINCE_KEY
from wxverify.db.runtime_state import get_runtime_state

logger = logging.getLogger(__name__)

DownloadSource = Literal["recorded", "inferred"]

# A historical row (from the retired catch-up or backfill, plan 3.1) carries
# a source_raw ending in " previous_dayN". Such rows hold past runs; they are
# never a download of S (4.1).
_FORWARD_ROW_SQL: Final = "fs.source_raw NOT GLOB '* previous_day[0-9]*'"

# A meteoblue package member's fetch feed is its package row: the package
# call stores the members' samples and the package's download row (4.2).
_PACKAGE_OF_MEMBER_SQL: Final = """
    SELECT pkg.id
    FROM feeds AS member
    JOIN feeds AS pkg ON pkg.source = 'meteoblue' AND pkg.model = 'multimodel'
    WHERE member.id = ?
      AND member.source = 'meteoblue'
      AND member.model != 'multimodel'
"""


@dataclass(frozen=True, slots=True)
class StoredSample:
    """One stored forecast row of a scored version (4.3)."""

    variable: str
    issued_at: str
    valid_at: str
    lead_hours: int
    value: float
    fetched_at: str


@dataclass(frozen=True)
class ScoredDownload:
    """A feed's scored download of one local day (4.3).

    ``labels`` is the download's labels object as the record stores it: for
    each feed id, as a string, whose samples the download carried, its
    sorted run labels. ``fetched_at`` is the download's receipt stamp: the
    record's ``fetched_at``, or for an inferred download a sample's
    ``fetched_at`` or a job's ``updated_at`` (4.7). ``download_id`` is
    ``None`` for an inferred download.
    """

    download_id: int | None
    feed_id: int
    local_date: date
    fetched_at: str
    labels: dict[str, list[str]]
    source: DownloadSource


def scored_version(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    download: ScoredDownload,
    start_utc: datetime,
    end_utc: datetime,
) -> dict[tuple[str, str], StoredSample]:
    """The forecasts ``download`` gives for ``feed_id`` with ``valid_at`` in
    ``[start_utc, end_utc)``, keyed by ``(variable, valid_at)`` (4.3).

    The feed's rows whose ``issued_at`` is one of the download's labels for
    the feed and whose ``fetched_at`` is set and at or before the
    download's, so rows a later download of the same label added are left
    out; with a lead from 1 to the feed's ``max_lead_hours``, not invalid,
    and past the rain-label boundary. Where two labels give the same hour,
    the newer label's row. Read-only.
    """
    labels = sorted(set(download.labels.get(str(feed_id), [])))
    if not labels:
        return {}
    placeholders = ", ".join("?" for _ in labels)
    rows = conn.execute(
        f"""
        SELECT fs.variable, fs.issued_at, fs.valid_at, fs.lead_hours, fs.value,
               fs.fetched_at
        FROM forecast_samples fs
        JOIN feeds f ON f.id = fs.feed_id
        WHERE fs.site_id = ?
          AND fs.feed_id = ?
          AND fs.issued_at IN ({placeholders})
          AND fs.fetched_at IS NOT NULL
          AND fs.fetched_at <= ?
          AND fs.valid_at >= ?
          AND fs.valid_at < ?
          AND fs.lead_hours BETWEEN 1 AND f.max_lead_hours
          AND NOT {invalid_forecast_sample_sql("fs")}
          AND {precip_label_eligible_sql("fs", "f")}
        ORDER BY fs.issued_at, fs.valid_at
        """,
        (
            site_id,
            feed_id,
            *labels,
            download.fetched_at,
            isoformat_utc(start_utc),
            isoformat_utc(end_utc),
        ),
    ).fetchall()
    version: dict[tuple[str, str], StoredSample] = {}
    for row in rows:
        sample = StoredSample(
            variable=str(row["variable"]),
            issued_at=str(row["issued_at"]),
            valid_at=str(row["valid_at"]),
            lead_hours=int(row["lead_hours"]),
            value=float(row["value"]),
            fetched_at=str(row["fetched_at"]),
        )
        # Rows arrive in label order, so a newer label's row replaces an
        # older label's row for the same hour.
        version[(sample.variable, sample.valid_at)] = sample
    return version


def scored_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
) -> ScoredDownload | None:
    """The first download of ``local_date`` stored for ``feed_id``, or ``None``.

    The earliest ``stored`` row of ``forecast_downloads`` for the feed (or
    its meteoblue package) on that date whose labels for the feed are not
    empty, ordered by ``ended_at``, then ``id`` (4.3). For a date whose
    downloads may lie before ``download_record_since``, the download
    inferred from the stored samples and job rows (4.7); where the date has
    both, the earlier of the two. Read-only.
    """
    fetch_feed_id = _fetch_feed_id(conn, feed_id)
    recorded = _first_recorded_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        fetch_feed_id=fetch_feed_id,
        local_date=local_date,
    )
    inferred = _inferred_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        fetch_feed_id=fetch_feed_id,
        local_date=local_date,
    )
    if inferred is None:
        return recorded
    # Text order is time order for these stamps (plan 15.2); a tie keeps the
    # recorded download.
    if recorded is None or inferred.fetched_at < recorded.fetched_at:
        return inferred
    return recorded


def first_recorded_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
) -> ScoredDownload | None:
    """The first recorded ``stored`` download of ``local_date`` for
    ``feed_id`` (or its meteoblue package) whose labels for the feed are not
    empty, ordered by ``ended_at``, then ``id``; or ``None``.

    The record half of :func:`scored_download`, with no inference. The
    fetch path asks it before a download's row is written, to decide whether
    that download is the day's first and so writes the scored values (rain
    release, Part B). Read-only.
    """
    return _first_recorded_download(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        fetch_feed_id=_fetch_feed_id(conn, feed_id),
        local_date=local_date,
    )


# The scored rows of one feed, day ahead and span. Lead, the shared
# validator and the rain-label boundary are read as scored_version reads
# them, so the close scores what 0.16.14 scored.
_SCORED_ROWS_SQL: Final = f"""
    SELECT fsc.variable, fsc.issued_at, fsc.valid_at, fsc.lead_hours,
           fsc.value, fsc.fetched_at, fsc.download_id, fsc.download_date
    FROM forecast_scored fsc
    JOIN feeds f ON f.id = fsc.feed_id
    WHERE fsc.site_id = ?
      AND fsc.feed_id = ?
      AND fsc.day_ahead = ?
      AND fsc.valid_at >= ?
      AND fsc.valid_at < ?
      AND fsc.lead_hours BETWEEN 1 AND f.max_lead_hours
      AND NOT {invalid_forecast_sample_sql("fsc")}
      AND {precip_label_eligible_sql("fsc", "f")}
    ORDER BY fsc.valid_at, fsc.variable
"""


def scored_values(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    day_ahead: int,
    start_utc: datetime,
    end_utc: datetime,
) -> dict[tuple[str, str], StoredSample]:
    """The scored values of ``feed_id`` at ``day_ahead`` with ``valid_at`` in
    ``[start_utc, end_utc)``, keyed by ``(variable, valid_at)`` (rain
    release, Part B, B4).

    Read from ``forecast_scored``: one row per hour, from the first stored
    download of the local day ``day_ahead`` days before the hour's. A lead
    above the feed's ``max_lead_hours``, an invalid value and rain before
    the rain-label boundary are left out, as :func:`scored_version` leaves
    them out. ``fetched_at`` is the download's. Read-only.
    """
    values: dict[tuple[str, str], StoredSample] = {}
    for row in _scored_rows(conn, site_id, feed_id, day_ahead, start_utc, end_utc):
        sample = StoredSample(
            variable=str(row["variable"]),
            issued_at=str(row["issued_at"]),
            valid_at=str(row["valid_at"]),
            lead_hours=int(row["lead_hours"]),
            value=float(row["value"]),
            fetched_at=str(row["fetched_at"]),
        )
        values[(sample.variable, sample.valid_at)] = sample
    return values


def scored_values_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    day_ahead: int,
    start_utc: datetime,
    end_utc: datetime,
) -> ScoredDownload | None:
    """The download the :func:`scored_values` of the same arguments came
    from, or ``None`` when there are none (B4).

    Every scored row of one feed, local day and day ahead comes from one
    download. With its ``download_id`` set it is ``recorded``, with that
    ``forecast_downloads`` row's labels; with none (a row the v16 carry-over
    wrote from an inferred download) it is ``inferred``, with no labels.
    ``local_date`` is the row's ``download_date`` and ``fetched_at`` its
    ``fetched_at``. Read-only.
    """
    rows = _scored_rows(conn, site_id, feed_id, day_ahead, start_utc, end_utc)
    if not rows:
        return None
    first = rows[0]
    try:
        local_date = date.fromisoformat(str(first["download_date"]))
    except ValueError:
        return None
    raw_id: object = first["download_id"]
    if raw_id is None:
        return ScoredDownload(
            download_id=None,
            feed_id=feed_id,
            local_date=local_date,
            fetched_at=str(first["fetched_at"]),
            labels={},
            source="inferred",
        )
    download_id = int(cast(int, raw_id))
    labels_row = conn.execute(
        "SELECT labels FROM forecast_downloads WHERE id = ?", (download_id,)
    ).fetchone()
    return ScoredDownload(
        download_id=download_id,
        feed_id=feed_id,
        local_date=local_date,
        fetched_at=str(first["fetched_at"]),
        labels={} if labels_row is None else _parse_labels(labels_row["labels"]),
        source="recorded",
    )


def _scored_rows(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    day_ahead: int,
    start_utc: datetime,
    end_utc: datetime,
) -> list[sqlite3.Row]:
    return conn.execute(
        _SCORED_ROWS_SQL,
        (
            site_id,
            feed_id,
            day_ahead,
            isoformat_utc(start_utc),
            isoformat_utc(end_utc),
        ),
    ).fetchall()


def newest_values(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    start_utc: datetime,
    end_utc: datetime,
) -> dict[tuple[str, str], StoredSample]:
    """The feed's stored newest values with ``valid_at`` in ``[start_utc,
    end_utc)``, keyed by ``(variable, valid_at)`` (B3).

    ``forecast_samples`` holds one row per hour, the value the feed last
    gave. Rows with no ``fetched_at``, a lead outside 1 to the feed's
    ``max_lead_hours``, an invalid value, or rain before the rain-label
    boundary are left out, as :func:`scored_version` leaves them out. The
    value-change log reads it before a download's samples are written, as
    the values that download replaces. Read-only.
    """
    rows = conn.execute(
        f"""
        SELECT fs.variable, fs.issued_at, fs.valid_at, fs.lead_hours, fs.value,
               fs.fetched_at
        FROM forecast_samples fs
        JOIN feeds f ON f.id = fs.feed_id
        WHERE fs.site_id = ?
          AND fs.feed_id = ?
          AND fs.fetched_at IS NOT NULL
          AND fs.valid_at >= ?
          AND fs.valid_at < ?
          AND fs.lead_hours BETWEEN 1 AND f.max_lead_hours
          AND NOT {invalid_forecast_sample_sql("fs")}
          AND {precip_label_eligible_sql("fs", "f")}
        """,
        (site_id, feed_id, isoformat_utc(start_utc), isoformat_utc(end_utc)),
    ).fetchall()
    values: dict[tuple[str, str], StoredSample] = {}
    for row in rows:
        sample = StoredSample(
            variable=str(row["variable"]),
            issued_at=str(row["issued_at"]),
            valid_at=str(row["valid_at"]),
            lead_hours=int(row["lead_hours"]),
            value=float(row["value"]),
            fetched_at=str(row["fetched_at"]),
        )
        values[(sample.variable, sample.valid_at)] = sample
    return values


def latest_stored_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
) -> ScoredDownload | None:
    """The feed's latest recorded ``stored`` download whose labels for the
    feed are not empty, ordered by ``ended_at``, then ``id``; or ``None``.

    The value-change log reads it before a new download's row exists, as
    the download that one is compared with (5.2). Only the record is read;
    a download before ``download_record_since`` is never returned.
    Read-only.
    """
    fetch_feed_id = _fetch_feed_id(conn, feed_id)
    rows = conn.execute(
        """
        SELECT id, local_date, fetched_at, labels
        FROM forecast_downloads
        WHERE site_id = ?
          AND feed_id IN (?, ?)
          AND outcome = 'stored'
        ORDER BY ended_at DESC, id DESC
        """,
        (site_id, feed_id, fetch_feed_id),
    )
    for row in rows:
        download = _recorded_download(row, feed_id)
        if download is not None:
            return download
    return None


def _fetch_feed_id(conn: sqlite3.Connection, feed_id: int) -> int:
    """The feed whose download carries ``feed_id``'s samples: its meteoblue
    package for a member, else the feed itself."""
    row = conn.execute(_PACKAGE_OF_MEMBER_SQL, (feed_id,)).fetchone()
    return feed_id if row is None else int(row["id"])


def _first_recorded_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    fetch_feed_id: int,
    local_date: date,
) -> ScoredDownload | None:
    rows = conn.execute(
        """
        SELECT id, local_date, fetched_at, labels
        FROM forecast_downloads
        WHERE site_id = ?
          AND feed_id IN (?, ?)
          AND local_date = ?
          AND outcome = 'stored'
        ORDER BY ended_at, id
        """,
        (site_id, feed_id, fetch_feed_id, local_date.isoformat()),
    )
    for row in rows:
        download = _recorded_download(row, feed_id)
        if download is not None:
            return download
    return None


def _recorded_download(row: sqlite3.Row, feed_id: int) -> ScoredDownload | None:
    """A ``forecast_downloads`` row as a scored download for ``feed_id``;
    ``None`` when its labels for the feed are empty or it does not read."""
    labels = _parse_labels(row["labels"])
    if not labels.get(str(feed_id)):
        return None
    fetched_at: object = row["fetched_at"]
    if not isinstance(fetched_at, str):
        return None
    try:
        local_date = date.fromisoformat(str(row["local_date"]))
    except ValueError:
        return None
    return ScoredDownload(
        download_id=int(row["id"]),
        feed_id=feed_id,
        local_date=local_date,
        fetched_at=fetched_at,
        labels=labels,
        source="recorded",
    )


def _parse_labels(raw: object) -> dict[str, list[str]]:
    """The record's ``labels`` JSON object; ``{}`` for anything else. Keys
    that are not strings, values that are not lists, and list items that
    are not strings are dropped."""
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
            labels[key] = sorted({item for item in items if isinstance(item, str)})
    return labels


def _inferred_download(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    fetch_feed_id: int,
    local_date: date,
) -> ScoredDownload | None:
    """The first download of ``local_date`` inferred from ``forecast_samples``
    and the completed ``fetch_feed`` job rows, before
    ``download_record_since`` (4.7); ``None`` when the date lies wholly
    after it, the key is absent or unreadable, or nothing qualifies."""
    since_text = get_runtime_state(conn, DOWNLOAD_RECORD_SINCE_KEY)
    if since_text is None:
        return None
    try:
        since = parse_utc(since_text)
    except ValueError:
        logger.warning(
            "scored download: %s does not parse; no download inferred",
            DOWNLOAD_RECORD_SINCE_KEY,
        )
        return None
    zone = _inference_zone(conn, site_id)
    if zone is None:
        return None
    day_start, day_end, _slots = local_day_slots(local_date, zone)
    if day_start >= since:
        return None
    lower = isoformat_utc(day_start)
    # In S's bounds and before download_record_since, in one bound.
    upper = isoformat_utc(min(day_end, since))

    # Step 1: X, the first download of S that stored a new row.
    x_row = conn.execute(
        f"""
        SELECT MIN(fs.fetched_at) AS x
        FROM forecast_samples fs
        WHERE fs.site_id = ?
          AND fs.feed_id = ?
          AND fs.fetched_at >= ?
          AND fs.fetched_at < ?
          AND {_FORWARD_ROW_SQL}
        """,
        (site_id, feed_id, lower, upper),
    ).fetchone()
    x_value: object = None if x_row is None else x_row["x"]
    x_stamp = x_value if isinstance(x_value, str) else None

    # Step 2: J, an earlier completed job of the feed (its package's for a
    # meteoblue member) that stored no new row.
    j_row = conn.execute(
        """
        SELECT MIN(updated_at) AS j
        FROM jobs
        WHERE type = 'fetch_feed'
          AND job_key = ?
          AND site_id = ?
          AND status = 'completed'
          AND result = 'ok'
          AND updated_at >= ?
          AND updated_at < ?
        """,
        (
            f"fetch:{fetch_feed_id}",
            site_id,
            lower,
            x_stamp if x_stamp is not None else upper,
        ),
    ).fetchone()
    j_value: object = None if j_row is None else j_row["j"]
    if isinstance(j_value, str):
        label_row = conn.execute(
            f"""
            SELECT MAX(fs.issued_at) AS label
            FROM forecast_samples fs
            WHERE fs.site_id = ?
              AND fs.feed_id = ?
              AND fs.fetched_at IS NOT NULL
              AND fs.fetched_at <= ?
              AND {_FORWARD_ROW_SQL}
            """,
            (site_id, feed_id, j_value),
        ).fetchone()
        label: object = None if label_row is None else label_row["label"]
        if isinstance(label, str):
            return ScoredDownload(
                download_id=None,
                feed_id=feed_id,
                local_date=local_date,
                fetched_at=j_value,
                labels={str(feed_id): [label]},
                source="inferred",
            )

    # Step 3: otherwise X.
    if x_stamp is None:
        return None
    x_labels = [
        str(row["issued_at"])
        for row in conn.execute(
            """
            SELECT DISTINCT issued_at
            FROM forecast_samples
            WHERE site_id = ? AND feed_id = ? AND fetched_at = ?
            ORDER BY issued_at
            """,
            (site_id, feed_id, x_stamp),
        )
    ]
    return ScoredDownload(
        download_id=None,
        feed_id=feed_id,
        local_date=local_date,
        fetched_at=x_stamp,
        labels={str(feed_id): x_labels},
        source="inferred",
    )


def _inference_zone(conn: sqlite3.Connection, site_id: int) -> str | None:
    """The zone S's bounds are taken from for the inference (4.7): the
    site's first daily-close record's, else ``sites.timezone``; ``None``
    for a site that does not exist."""
    row = conn.execute(
        "SELECT timezone FROM site_days WHERE site_id = ? ORDER BY local_date LIMIT 1",
        (site_id,),
    ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
    return None if row is None else str(row["timezone"])

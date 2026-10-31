"""The publication: P3's stage 7, and the starting history P6 builds once.

Daily-close P4 plan (``plans/2026-10-22-daily-close-pairs-and-publication.md``)
sections 7 and 8, and the two calls 16.3 gives P6. A publication covers one
site and one cutoff C, the last closed day it includes: rankings for the
windows ``w:{N}`` and ``w:all``, wind weights over the ``TRAINING_DAYS``
dates ending at C, and coverage. All of it is summed from stored rows --
``day_scores`` for closed record dates, ``starting_scores`` and
``starting_wind_days`` for the dates before the site's first record (the
starting history, 7.3) -- and its rows never change once written.

``publish`` (P3's stage 7) and ``publish_first`` (P6's first publication)
read everything in one read-only snapshot, outside the write lock, and swap
the result in with one ``writer.write`` (8.2, 8.3). ``seed_starting_history``
builds the starting history in the caller's transaction and never commits.
Every function reads the clock only through ``now_utc`` (16), so both stamps
of a publication are ``now_utc`` to the whole second (7.1, 15.2).
"""

from __future__ import annotations

import itertools
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import day_record
from wxverify.forecast.data import EXCLUDED_FEEDS_SQL
from wxverify.forecast.wind_blend import (
    DAY_AHEADS,
    FLOOR_MS,
    MIN_FEEDS,
    MIN_OBS_HOURS,
    MIN_RUN_HOURS,
    MIN_TRAINING_DAYS,
    TRAINING_DAYS,
    WIND_METHOD,
    WindTraining,
    compute_wind_weights,
)
from wxverify.scoring.close_scores import SCORING_RULE
from wxverify.scoring.metrics import metrics_from_sums
from wxverify.settings.keys import get_number_setting
from wxverify.verification.methodology import METHODOLOGY_VERSION

#: The swap deletes a site's publication once its successor was published
#: more than this many hours before the new one (7.5).
PUBLICATION_KEEP_HOURS: Final = 192
#: A value-change row is kept this many days by its download's ``ended_at``;
#: the swap deletes older rows (5.2, 8.3).
VALUE_CHANGE_KEEP_DAYS: Final = 90

PublicationFailure = Literal["cutoff_not_closed", "no_site"]

_ALL_WINDOW_KEY: Final = "w:all"
_PUBLICATION_KEEP: Final = timedelta(hours=PUBLICATION_KEEP_HOURS)
_VALUE_CHANGE_KEEP: Final = timedelta(days=VALUE_CHANGE_KEEP_DAYS)
# The constants at ``forecast/wind_blend.py`` a publication's weights are
# built under, saved on its row (7.1).
_WIND_CONSTANTS: Final = json.dumps(
    {
        "DAY_AHEADS": list(DAY_AHEADS),
        "FLOOR_MS": FLOOR_MS,
        "MIN_FEEDS": MIN_FEEDS,
        "MIN_OBS_HOURS": MIN_OBS_HOURS,
        "MIN_RUN_HOURS": MIN_RUN_HOURS,
        "MIN_TRAINING_DAYS": MIN_TRAINING_DAYS,
        "TRAINING_DAYS": TRAINING_DAYS,
    },
    sort_keys=True,
)

_SUM_COLUMNS: Final = (
    "n",
    "err_n",
    "err_sum",
    "abs_sum",
    "sq_sum",
    "hits",
    "false_alarms",
    "misses",
    "correct_negs",
    "skill_n",
    "skill_sq_sum",
    "skill_ref_sq_sum",
)
_PAIR_SUM_COLUMNS: Final = _SUM_COLUMNS[:9]
_SKILL_SUM_COLUMNS: Final = _SUM_COLUMNS[9:]

_PUBLICATION_ID_SQL: Final = (
    "SELECT id FROM publications WHERE site_id = ? AND cutoff = ?"
)
_FIRST_RECORD_SQL: Final = "SELECT MIN(local_date) FROM site_days WHERE site_id = ?"
# Rankings and coverage (8.2 steps 3 and 5): a window's closed record dates.
# A date counts only once its record is ``closed``: a missed date adds
# nothing (7.2), whatever an earlier attempt left in ``day_scores``.
_DAY_SUMS_SQL: Final = """
    SELECT ds.feed_id, ds.variable, ds.day_ahead,
           SUM(ds.n) AS n, SUM(ds.err_n) AS err_n, SUM(ds.err_sum) AS err_sum,
           SUM(ds.abs_sum) AS abs_sum, SUM(ds.sq_sum) AS sq_sum,
           SUM(ds.hits) AS hits, SUM(ds.false_alarms) AS false_alarms,
           SUM(ds.misses) AS misses, SUM(ds.correct_negs) AS correct_negs,
           SUM(ds.skill_n) AS skill_n, SUM(ds.skill_sq_sum) AS skill_sq_sum,
           SUM(ds.skill_ref_sq_sum) AS skill_ref_sq_sum,
           SUM(ds.status = 'scored') AS scored,
           SUM(ds.status != 'not_counted') AS expected
    FROM day_scores ds
    JOIN site_days sd ON sd.site_id = ds.site_id AND sd.local_date = ds.local_date
    WHERE ds.site_id = ? AND ds.local_date >= ? AND ds.local_date <= ?
      AND sd.state = 'closed'
    GROUP BY ds.feed_id, ds.variable, ds.day_ahead
"""
# The same, over a window's starting-history dates (7.3).
_STARTING_SUMS_SQL: Final = """
    SELECT feed_id, variable, day_ahead,
           SUM(n) AS n, SUM(err_n) AS err_n, SUM(err_sum) AS err_sum,
           SUM(abs_sum) AS abs_sum, SUM(sq_sum) AS sq_sum,
           SUM(hits) AS hits, SUM(false_alarms) AS false_alarms,
           SUM(misses) AS misses, SUM(correct_negs) AS correct_negs,
           SUM(skill_n) AS skill_n, SUM(skill_sq_sum) AS skill_sq_sum,
           SUM(skill_ref_sq_sum) AS skill_ref_sq_sum,
           COUNT(*) AS starting_days
    FROM starting_scores
    WHERE site_id = ? AND local_date >= ? AND local_date <= ?
    GROUP BY feed_id, variable, day_ahead
"""
# Wind weights (8.2 step 4), real feeds less the meteoblue package, as
# today's training reads them (``training_pairs_query``).
_CLOSED_WIND_SQL: Final = f"""
    SELECT ds.local_date, ds.feed_id, ds.day_ahead,
           ds.wind_fc_high, ds.wind_fc_hours, ds.wind_obs_high
    FROM day_scores ds
    JOIN site_days sd ON sd.site_id = ds.site_id AND sd.local_date = ds.local_date
    JOIN feeds f ON f.id = ds.feed_id
    WHERE ds.site_id = ? AND ds.variable = 'wind'
      AND ds.local_date >= ? AND ds.local_date <= ?
      AND sd.state = 'closed' AND {EXCLUDED_FEEDS_SQL}
"""
_STARTING_WIND_SQL: Final = f"""
    SELECT sw.local_date, sw.feed_id, sw.day_ahead,
           sw.forecast_high, sw.observed_high
    FROM starting_wind_days sw
    JOIN feeds f ON f.id = sw.feed_id
    WHERE sw.site_id = ? AND sw.local_date >= ? AND sw.local_date <= ?
      AND {EXCLUDED_FEEDS_SQL}
"""

_INSERT_PUBLICATION_SQL: Final = """
    INSERT INTO publications
        (site_id, cutoff, first_record_date, window_days, window_start_date,
         wind_start_date, settings, methodology_version, scoring_rule,
         wind_method, wind_constants, build_started_at, published_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_RANKING_SQL: Final = """
    INSERT INTO publication_rankings
        (publication_id, variable, day_ahead, window_key, feed_id, n, bias,
         mae, rmse, pod, far, csi, ets, hss, skill_score)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_WIND_WEIGHT_SQL: Final = """
    INSERT INTO publication_wind_weights
        (publication_id, day_ahead, feed_id, mae_ms, days, weight)
    VALUES (?, ?, ?, ?, ?, ?)
"""
_INSERT_COVERAGE_SQL: Final = """
    INSERT INTO publication_coverage
        (publication_id, variable, window_key, day_ahead, feed_id, scored,
         expected, starting_days)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
_SITE_PUBLICATIONS_SQL: Final = (
    "SELECT id, published_at FROM publications WHERE site_id = ? ORDER BY cutoff"
)
_DELETE_PUBLICATION_SQL: Final = "DELETE FROM publications WHERE id = ?"
_DELETE_OLD_VALUE_CHANGES_SQL: Final = (
    "DELETE FROM forecast_value_changes WHERE download_id IN"
    " (SELECT id FROM forecast_downloads WHERE ended_at < ?)"
)

# The starting history (7.3): today's pairs of the passed generation, before
# the first record's local midnight, summed per UTC hour here and per local
# date in Python (the zone's dates cannot be computed in SQL).
_SEED_PAIR_SUMS_SQL: Final = """
    SELECT valid_at, feed_id, variable, day_ahead,
           COUNT(*) AS n, COUNT(error) AS err_n, SUM(error) AS err_sum,
           SUM(abs_error) AS abs_sum, SUM(sq_error) AS sq_sum,
           SUM(cat_hit) AS hits, SUM(cat_false) AS false_alarms,
           SUM(cat_miss) AS misses, SUM(cat_correct_neg) AS correct_negs
    FROM forecast_pairs
    WHERE site_id = ? AND tz_generation_id = ? AND valid_at < ?
    GROUP BY valid_at, feed_id, variable, day_ahead
"""
# Today's skill join (``scoring/metrics.py`` ``_paired_skill``), both sides
# of the passed generation.
_SEED_SKILL_SUMS_SQL: Final = """
    SELECT fp.valid_at, fp.feed_id, fp.variable, fp.day_ahead,
           COUNT(*) AS skill_n, SUM(fp.sq_error) AS skill_sq_sum,
           SUM(pp.sq_error) AS skill_ref_sq_sum
    FROM forecast_pairs fp
    JOIN forecast_pairs pp
      ON pp.site_id = fp.site_id
     AND pp.variable = fp.variable
     AND pp.valid_at = fp.valid_at
     AND pp.lead_hours = fp.lead_hours
     AND pp.day_ahead = fp.day_ahead
    WHERE fp.site_id = ? AND fp.tz_generation_id = ? AND fp.valid_at < ?
      AND pp.feed_id = ? AND pp.tz_generation_id = ?
    GROUP BY fp.valid_at, fp.feed_id, fp.variable, fp.day_ahead
"""
_PERSISTENCE_FEED_SQL: Final = (
    "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_persistence'"
)
# Today's wind training read (``forecast/wind_blend.py``
# ``training_pairs_query``), over the passed generation: one row per run and
# day ahead with at least ``MIN_RUN_HOURS`` hours.
_SEED_WIND_RUNS_SQL: Final = f"""
    SELECT fp.feed_id, fp.issued_at, fp.day_ahead,
           MAX(fp.forecast) AS high, MIN(fp.valid_at) AS first_valid
    FROM forecast_pairs fp
    JOIN feeds f ON f.id = fp.feed_id
    WHERE fp.site_id = ? AND fp.tz_generation_id = ? AND fp.variable = 'wind'
      AND fp.valid_at < ? AND {EXCLUDED_FEEDS_SQL}
    GROUP BY fp.feed_id, fp.issued_at, fp.day_ahead
    HAVING COUNT(DISTINCT fp.valid_at) >= {MIN_RUN_HOURS}
"""
_SEED_WIND_OBS_SQL: Final = (
    "SELECT valid_at, value FROM observations"
    " WHERE site_id = ? AND variable = 'wind' AND valid_at < ?"
)
# The dates today's training leaves out of the observed side: an enabled
# station's inventory row still to fetch, retryable, or waiting on a refetch
# (the predicate of ``forecast/wind_blend.py`` ``_UNSETTLED_DAYS_SQL``).
_SEED_UNSETTLED_SQL: Final = """
    SELECT DISTINCT d.local_date
    FROM station_wind_days d JOIN stations st ON st.id = d.station_id
    WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date < ?
      AND (d.status IN ('pending', 'partial')
           OR (d.status = 'failed' AND d.attempts < 3)
           OR (d.refetch_at IS NOT NULL AND d.refetched = 0))
"""
_DELETE_STARTING_SCORES_SQL: Final = "DELETE FROM starting_scores WHERE site_id = ?"
_DELETE_STARTING_WIND_SQL: Final = "DELETE FROM starting_wind_days WHERE site_id = ?"
_INSERT_STARTING_SCORE_SQL: Final = """
    INSERT INTO starting_scores
        (site_id, local_date, feed_id, variable, day_ahead, n, err_n, err_sum,
         abs_sum, sq_sum, hits, false_alarms, misses, correct_negs, skill_n,
         skill_sq_sum, skill_ref_sq_sum)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_STARTING_WIND_SQL: Final = """
    INSERT INTO starting_wind_days
        (site_id, local_date, feed_id, day_ahead, forecast_high, observed_high)
    VALUES (?, ?, ?, ?, ?, ?)
"""

_Number = float | int | None
_Key = tuple[int, str, int]
_Values = tuple[object, ...]


class PublicationFailed(Exception):
    """The publication cannot be built or swapped in (8.2, 8.3).

    Its message is the reason alone, ``cutoff_not_closed`` or ``no_site``,
    which P3 saves as the publication record's ``last_failure``.
    """

    def __init__(self, reason: PublicationFailure) -> None:
        super().__init__(reason)
        self.reason: PublicationFailure = reason


@dataclass(frozen=True, slots=True)
class SeedCounts:
    """The rows ``seed_starting_history`` wrote for the site (7.3)."""

    starting_scores: int
    starting_wind_days: int


@dataclass(frozen=True, slots=True)
class _Build:
    """What the build read and computed, ready for the swap (8.2)."""

    window_days: int
    min_n: int
    first_record_date: date
    window_start: date
    wind_start: date
    rankings: tuple[_Values, ...]
    coverage: tuple[_Values, ...]
    wind_weights: tuple[_Values, ...]


class _Cell:
    """One (feed, variable, day ahead) cell of one window, being summed."""

    __slots__ = ("expected", "scored", "starting_days", "sums")

    def __init__(self) -> None:
        self.sums: dict[str, _Number] = {}
        self.scored = 0
        self.expected = 0
        self.starting_days = 0


async def publish(
    db: Database,
    writer: FencedWriter,
    *,
    site_id: int,
    cutoff: date,
    attempt: int,
    now_utc: datetime,
) -> str:
    """Publish the site's rankings, wind weights and coverage up to ``cutoff``.

    P3's stage 7 call point (8.1, 16.1). A cutoff already published returns
    its id and writes nothing (8.2 step 1). Otherwise one read-only snapshot
    reads the settings, requires ``cutoff``'s record to be ``closed`` (else
    ``PublicationFailed("cutoff_not_closed")``) and builds every row; one
    ``writer.write`` then swaps the publication in (8.3), raising
    ``PublicationFailed("no_site")`` when the site is gone. Returns the
    publication's id as text. ``attempt`` is P3's and not read; both stamps
    are ``now_utc`` to the whole second.
    """
    return await _publish(
        db,
        writer,
        site_id=site_id,
        cutoff=cutoff,
        first_record_date=None,
        now_utc=now_utc,
    )


async def publish_first(
    db: Database,
    writer: FencedWriter,
    *,
    site_id: int,
    first_record_date: date,
    now_utc: datetime,
) -> str:
    """Build the site's first publication, for P6's cutover (16.3).

    As ``publish``, with cutoff ``first_record_date`` - 1 and every window
    date a starting-history date, so no P1 record is needed or read; the row
    saves ``first_record_date``. A repeat for the same date returns the same
    id and writes nothing; a site with no starting scores gets a publication
    with no ranking rows.
    """
    return await _publish(
        db,
        writer,
        site_id=site_id,
        cutoff=_shift(first_record_date, -1),
        first_record_date=first_record_date,
        now_utc=now_utc,
    )


def seed_starting_history(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    first_record_date: date,
    generation_id: int,
    timezone: str,
) -> SeedCounts:
    """Build the site's starting history from today's pairs, once (7.3).

    For every local date in ``timezone`` before ``first_record_date`` that
    has pairs of generation ``generation_id``: ``starting_scores``, the sums
    of 6.4 over that date's ``forecast_pairs`` rows (today's issue-time day
    ahead, every download, the skill sums from today's persistence join), and
    ``starting_wind_days``, the forecast and observed daily highs today's
    wind training reads for it. The site's earlier rows are replaced. In the
    caller's transaction; never commits. Reads no subscription: readers
    apply it (9.1, 9.4).
    """
    tz = ZoneInfo(timezone)
    bound = isoformat_utc(local_day_slots(first_record_date, timezone)[0])
    scores = _seed_scores(conn, site_id, generation_id, tz, bound)
    wind_days = _seed_wind_days(
        conn, site_id, generation_id, tz, bound, first_record_date
    )
    conn.execute(_DELETE_STARTING_SCORES_SQL, (site_id,))
    conn.execute(_DELETE_STARTING_WIND_SQL, (site_id,))
    conn.executemany(_INSERT_STARTING_SCORE_SQL, scores)
    conn.executemany(_INSERT_STARTING_WIND_SQL, wind_days)
    return SeedCounts(starting_scores=len(scores), starting_wind_days=len(wind_days))


# ---------------------------------------------------------------------------
# The build and the swap (section 8)
# ---------------------------------------------------------------------------


async def _publish(
    db: Database,
    writer: FencedWriter,
    *,
    site_id: int,
    cutoff: date,
    first_record_date: date | None,
    now_utc: datetime,
) -> str:
    """Repeat check, build snapshot, swap (8.2, 8.3).

    ``first_record_date`` None is ``publish``: the cutoff's record must be
    ``closed`` and the first record is read. Given, it is ``publish_first``.
    """
    existing = await db.read(lambda conn: _publication_id(conn, site_id, cutoff))
    if existing is not None:
        return str(existing)
    published = _whole_second(now_utc)
    _epoch, build = await writer.read_at_epoch(
        lambda conn: _build(
            conn, site_id=site_id, cutoff=cutoff, first_record_date=first_record_date
        ),
        label="publication_build",
    )
    publication_id = await writer.write(
        lambda conn: _swap(
            conn, site_id=site_id, cutoff=cutoff, build=build, published=published
        )
    )
    return str(publication_id)


def _publication_id(conn: sqlite3.Connection, site_id: int, cutoff: date) -> int | None:
    row = conn.execute(_PUBLICATION_ID_SQL, (site_id, cutoff.isoformat())).fetchone()
    return None if row is None else int(row[0])


def _build(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff: date,
    first_record_date: date | None,
) -> _Build:
    """8.2 steps 2 to 5, in the caller's read-only snapshot. Never writes.

    The settings are read once, here, as ``scoring/engine.py`` reads them,
    and every later value comes from them. ``compute_wind_weights`` is pure.
    """
    window_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    first = first_record_date
    if first is None:
        record = day_record(conn, site_id, cutoff)
        if record is None or record.state != "closed":
            raise PublicationFailed("cutoff_not_closed")
        row = conn.execute(_FIRST_RECORD_SQL, (site_id,)).fetchone()
        if row is None or row[0] is None:
            raise PublicationFailed("cutoff_not_closed")
        first = date.fromisoformat(str(row[0]))
    window_start = _shift(cutoff, -(window_days - 1))
    wind_start = _shift(cutoff, -(TRAINING_DAYS - 1))
    rankings: list[_Values] = []
    coverage: list[_Values] = []
    windows = ((f"w:{window_days}", window_start), (_ALL_WINDOW_KEY, date.min))
    for window_key, start in windows:
        cells = _window_cells(conn, site_id, start=start, cutoff=cutoff, first=first)
        for (feed_id, variable, day_ahead), cell in sorted(cells.items()):
            metrics = metrics_from_sums(cell.sums, variable=variable, min_n=min_n)
            if metrics.n > 0:
                rankings.append(
                    (
                        variable,
                        day_ahead,
                        window_key,
                        feed_id,
                        metrics.n,
                        metrics.bias,
                        metrics.mae,
                        metrics.rmse,
                        metrics.pod,
                        metrics.far,
                        metrics.csi,
                        metrics.ets,
                        metrics.hss,
                        metrics.skill_score,
                    )
                )
            coverage.append(
                (
                    variable,
                    window_key,
                    day_ahead,
                    feed_id,
                    cell.scored,
                    cell.expected,
                    cell.starting_days,
                )
            )
    weights = compute_wind_weights(
        _wind_training(conn, site_id, start=wind_start, cutoff=cutoff, first=first)
    )
    return _Build(
        window_days=window_days,
        min_n=min_n,
        first_record_date=first,
        window_start=window_start,
        wind_start=wind_start,
        rankings=tuple(rankings),
        coverage=tuple(coverage),
        wind_weights=tuple(
            (day_ahead, feed_id, weight.mae_ms, weight.days, weight.weight)
            for (feed_id, day_ahead), weight in sorted(weights.items())
        ),
    )


def _window_cells(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    start: date,
    cutoff: date,
    first: date,
) -> dict[_Key, _Cell]:
    """The sums and coverage counts of every cell of the window [start, cutoff].

    Closed record dates from ``first`` on come from ``day_scores``, with
    ``scored`` and ``expected`` (7.4); dates before ``first`` from
    ``starting_scores``, counted apart as ``starting_days``. A window counts
    calendar dates: a date with no row adds nothing (7.2).
    """
    cells: dict[_Key, _Cell] = {}
    closed_from = max(start, first)
    if closed_from <= cutoff:
        for row in conn.execute(
            _DAY_SUMS_SQL, (site_id, closed_from.isoformat(), cutoff.isoformat())
        ):
            cell = cells.setdefault(_cell_key(row), _Cell())
            _add_sums(cell.sums, row, _SUM_COLUMNS)
            cell.scored += int(row["scored"])
            cell.expected += int(row["expected"])
    starting_to = min(cutoff, _shift(first, -1))
    if start <= starting_to:
        for row in conn.execute(
            _STARTING_SUMS_SQL, (site_id, start.isoformat(), starting_to.isoformat())
        ):
            cell = cells.setdefault(_cell_key(row), _Cell())
            _add_sums(cell.sums, row, _SUM_COLUMNS)
            cell.starting_days += int(row["starting_days"])
    return cells


def _wind_training(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    start: date,
    cutoff: date,
    first: date,
) -> WindTraining:
    """The daily highs of the wind window [start, cutoff] (8.2 step 4).

    Closed dates give ``day_scores``' wind columns, a forecast high counting
    only with at least ``MIN_RUN_HOURS`` paired hours; starting dates give
    ``starting_wind_days``. A date's observed high is the same on every
    feed's row; the highest is kept should two differ.
    """
    feed_highs: dict[tuple[int, int], dict[date, float]] = {}
    observed: dict[date, float] = {}

    def note_observed(local_date: date, value: float | None) -> None:
        if value is None:
            return
        high = float(value)
        held = observed.get(local_date)
        if held is None or high > held:
            observed[local_date] = high

    closed_from = max(start, first)
    if closed_from <= cutoff:
        for row in conn.execute(
            _CLOSED_WIND_SQL, (site_id, closed_from.isoformat(), cutoff.isoformat())
        ):
            local_date = date.fromisoformat(str(row["local_date"]))
            note_observed(local_date, row["wind_obs_high"])
            high, hours = row["wind_fc_high"], row["wind_fc_hours"]
            if high is not None and hours is not None and int(hours) >= MIN_RUN_HOURS:
                key = (int(row["feed_id"]), int(row["day_ahead"]))
                feed_highs.setdefault(key, {})[local_date] = float(high)
    starting_to = min(cutoff, _shift(first, -1))
    if start <= starting_to:
        for row in conn.execute(
            _STARTING_WIND_SQL, (site_id, start.isoformat(), starting_to.isoformat())
        ):
            local_date = date.fromisoformat(str(row["local_date"]))
            note_observed(local_date, row["observed_high"])
            if row["forecast_high"] is not None:
                key = (int(row["feed_id"]), int(row["day_ahead"]))
                feed_highs.setdefault(key, {})[local_date] = float(row["forecast_high"])
    return WindTraining(feed_highs=feed_highs, observed_highs=observed)


def _swap(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff: date,
    build: _Build,
    published: datetime,
) -> int:
    """8.3, in one write: repeat check, site check, the rows, the deletes."""
    existing = _publication_id(conn, site_id, cutoff)
    if existing is not None:
        return existing
    if conn.execute("SELECT 1 FROM sites WHERE id = ?", (site_id,)).fetchone() is None:
        raise PublicationFailed("no_site")
    stamp = isoformat_utc(published)
    settings = json.dumps(
        {"rolling_window_days": build.window_days, "min_n": build.min_n}
    )
    cursor = conn.execute(
        _INSERT_PUBLICATION_SQL,
        (
            site_id,
            cutoff.isoformat(),
            build.first_record_date.isoformat(),
            build.window_days,
            build.window_start.isoformat(),
            build.wind_start.isoformat(),
            settings,
            METHODOLOGY_VERSION,
            SCORING_RULE,
            WIND_METHOD,
            _WIND_CONSTANTS,
            stamp,
            stamp,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("publications insert returned no row id")
    publication_id = int(cursor.lastrowid)
    conn.executemany(
        _INSERT_RANKING_SQL, [(publication_id, *row) for row in build.rankings]
    )
    conn.executemany(
        _INSERT_WIND_WEIGHT_SQL, [(publication_id, *row) for row in build.wind_weights]
    )
    conn.executemany(
        _INSERT_COVERAGE_SQL, [(publication_id, *row) for row in build.coverage]
    )
    _delete_superseded(conn, site_id, published)
    conn.execute(
        _DELETE_OLD_VALUE_CHANGES_SQL, (isoformat_utc(published - _VALUE_CHANGE_KEEP),)
    )
    return publication_id


def _delete_superseded(
    conn: sqlite3.Connection, site_id: int, published: datetime
) -> None:
    """Delete each publication whose successor (the next cutoff) was published
    more than ``PUBLICATION_KEEP_HOURS`` before ``published`` (7.5).

    The new publication is among the rows, so the one it succeeds is kept;
    children go by ``ON DELETE CASCADE``. A successor whose stamp does not
    parse keeps its predecessor.
    """
    rows = conn.execute(_SITE_PUBLICATIONS_SQL, (site_id,)).fetchall()
    for older, successor in itertools.pairwise(rows):
        try:
            successor_at = parse_utc(str(successor["published_at"]))
        except ValueError:
            continue
        if published - successor_at > _PUBLICATION_KEEP:
            conn.execute(_DELETE_PUBLICATION_SQL, (int(older["id"]),))


# ---------------------------------------------------------------------------
# The starting history (7.3)
# ---------------------------------------------------------------------------


def _seed_scores(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    tz: ZoneInfo,
    bound: str,
) -> list[_Values]:
    """``starting_scores`` rows: per local date, feed, variable and day ahead."""
    cells: dict[tuple[date, int, str, int], dict[str, _Number]] = {}
    for row in conn.execute(_SEED_PAIR_SUMS_SQL, (site_id, generation_id, bound)):
        local_date = _local_date(row["valid_at"], tz)
        if local_date is not None:
            key = (local_date, *_cell_key(row))
            _add_sums(cells.setdefault(key, {}), row, _PAIR_SUM_COLUMNS)
    persistence = conn.execute(_PERSISTENCE_FEED_SQL).fetchone()
    if persistence is not None:
        params = (site_id, generation_id, bound, int(persistence["id"]), generation_id)
        for row in conn.execute(_SEED_SKILL_SUMS_SQL, params):
            local_date = _local_date(row["valid_at"], tz)
            if local_date is None:
                continue
            sums = cells.get((local_date, *_cell_key(row)))
            if sums is not None:
                _add_sums(sums, row, _SKILL_SUM_COLUMNS)
    return [
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            *(sums.get(column) for column in _SUM_COLUMNS),
        )
        for (local_date, feed_id, variable, day_ahead), sums in sorted(cells.items())
    ]


def _seed_wind_days(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    tz: ZoneInfo,
    bound: str,
    first_record_date: date,
) -> list[_Values]:
    """``starting_wind_days`` rows, as ``load_wind_training`` reads the highs.

    The forecast high of a (feed, day ahead, date) is the latest run's with
    at least ``MIN_RUN_HOURS`` hours; the observed high is the date's, when
    it has at least ``MIN_OBS_HOURS`` hours and a settled station inventory,
    else NULL. A row whose stamp does not parse is skipped, as there.
    """
    latest: dict[tuple[int, int, date], tuple[datetime, float]] = {}
    for row in conn.execute(_SEED_WIND_RUNS_SQL, (site_id, generation_id, bound)):
        try:
            issued = parse_utc(str(row["issued_at"]))
            target = parse_utc(str(row["first_valid"])).astimezone(tz).date()
        except ValueError:
            continue
        key = (int(row["feed_id"]), int(row["day_ahead"]), target)
        held = latest.get(key)
        if held is None or issued > held[0]:
            latest[key] = (issued, float(row["high"]))
    hours: dict[date, set[datetime]] = {}
    highs: dict[date, float] = {}
    for row in conn.execute(_SEED_WIND_OBS_SQL, (site_id, bound)):
        try:
            moment = parse_utc(str(row["valid_at"]))
        except ValueError:
            continue
        local_date = moment.astimezone(tz).date()
        hours.setdefault(local_date, set()).add(moment)
        value = float(row["value"])
        held_high = highs.get(local_date)
        if held_high is None or value > held_high:
            highs[local_date] = value
    unsettled: set[date] = set()
    for row in conn.execute(
        _SEED_UNSETTLED_SQL, (site_id, first_record_date.isoformat())
    ):
        try:
            unsettled.add(date.fromisoformat(str(row["local_date"])))
        except ValueError:
            continue
    observed = {
        local_date: high
        for local_date, high in highs.items()
        if len(hours[local_date]) >= MIN_OBS_HOURS and local_date not in unsettled
    }
    return [
        (site_id, target.isoformat(), feed_id, day_ahead, high, observed.get(target))
        for (feed_id, day_ahead, target), (_issued, high) in sorted(latest.items())
    ]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _cell_key(row: sqlite3.Row) -> _Key:
    return (int(row["feed_id"]), str(row["variable"]), int(row["day_ahead"]))


def _add_sums(
    into: dict[str, _Number], row: sqlite3.Row, columns: tuple[str, ...]
) -> None:
    """Add ``row``'s sums into ``into``; a NULL adds nothing, as ``SUM`` does."""
    for column in columns:
        value: _Number = row[column]
        held = into.get(column)
        if held is None:
            into[column] = value
        elif value is not None:
            into[column] = held + value


def _local_date(value: object, tz: ZoneInfo) -> date | None:
    try:
        return parse_utc(str(value)).astimezone(tz).date()
    except ValueError:
        return None


def _shift(day: date, days: int) -> date:
    """``day`` moved by ``days``, held inside ``date``'s range."""
    ordinal = day.toordinal() + days
    return date.fromordinal(min(max(ordinal, 1), date.max.toordinal()))


def _whole_second(now_utc: datetime) -> datetime:
    """``now_utc`` as UTC, to the whole second (7.1, 15.2)."""
    aware = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    return aware.astimezone(UTC).replace(microsecond=0)

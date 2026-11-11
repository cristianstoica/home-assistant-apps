"""The publication: the close chain's stage 7.

A publication covers one
site and one cutoff C, the last closed day it includes: rankings for the
windows ``w:{N}`` and ``w:all``, wind weights over the ``TRAINING_DAYS``
dates ending at C, coverage, and the rain ranking of each day ahead with its
selected model (``publication_rain_ranks`` and
``rain_picks``, from ``rain_days`` and ``rain_forecast_days``). The rest is
summed from stored rows --
``day_scores`` for closed record dates, ``starting_scores`` and
``starting_wind_days`` for the dates before the site's first record (the
starting history) -- and its rows never change once written.

``publish`` (stage 7) reads everything in one read-only snapshot,
outside the write lock, and swaps the result in with one ``writer.write``.
Every function reads the
clock only through ``now_utc``, so both stamps of a publication are
``now_utc`` to the whole second.

The readers find the active publication and
read its rows for the leaderboard, composite, served forecast, wind serving
and record, applying each feed's subscription when they read. They
never write and read no clock.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal, cast

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import day_record
from wxverify.db.snapshot import read_snapshot
from wxverify.forecast.data import EXCLUDED_FEEDS_SQL
from wxverify.forecast.rain_pick import (
    RAIN_DAYS_AHEAD,
    WINDOW_DAYS,
    ModelSeries,
    RainRanking,
    forecasts_rain,
    rank_rain,
)
from wxverify.forecast.wind_blend import (
    DAY_AHEADS,
    FLOOR_MS,
    MIN_FEEDS,
    MIN_OBS_HOURS,
    MIN_RUN_HOURS,
    MIN_TRAINING_DAYS,
    TRAINING_DAYS,
    WIND_METHOD,
    FeedWeight,
    WindTraining,
    WindWeights,
    compute_wind_weights,
)
from wxverify.scoring.close_scores import SCORING_RULE, active_real_feeds
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import LeaderboardRow, below_baseline, score_badge
from wxverify.scoring.metrics import metrics_from_sums
from wxverify.settings.keys import get_number_setting
from wxverify.verification.methodology import METHODOLOGY_VERSION

#: The swap deletes a site's publication once its successor was published
#: more than this many hours before the new one.
PUBLICATION_KEEP_HOURS: Final = 192
#: A value-change row is kept this many days by its download's ``ended_at``;
#: the swap deletes older rows.
VALUE_CHANGE_KEEP_DAYS: Final = 90

PublicationFailure = Literal["cutoff_not_closed", "no_site"]

_ALL_WINDOW_KEY: Final = "w:all"
_PUBLICATION_KEEP: Final = timedelta(hours=PUBLICATION_KEEP_HOURS)
_VALUE_CHANGE_KEEP: Final = timedelta(days=VALUE_CHANGE_KEEP_DAYS)
# The constants at ``forecast/wind_blend.py`` a publication's weights are
# built under, saved on its row.
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

_PUBLICATION_ID_SQL: Final = (
    "SELECT id FROM publications WHERE site_id = ? AND cutoff = ?"
)
_FIRST_RECORD_SQL: Final = "SELECT MIN(local_date) FROM site_days WHERE site_id = ?"
# Rankings and coverage: a window's closed record dates.
# A date counts only once its record is ``closed``: a missed date adds
# nothing, whatever an earlier attempt left in ``day_scores``.
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
# The same, over a window's starting-history dates.
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
# Wind weights, real feeds less the meteoblue package.
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
# Rain: the window's known truth days, the models'
# daily totals, and the picks of the 60 cutoffs before.
_RAIN_TRUTH_SQL: Final = """
    SELECT local_date, outcome FROM rain_days
    WHERE site_id = ? AND local_date >= ? AND local_date <= ?
      AND outcome IN ('rain', 'dry')
"""
_RAIN_FORECASTS_SQL: Final = """
    SELECT local_date, feed_id, day_ahead, total_mm FROM rain_forecast_days
    WHERE site_id = ? AND local_date >= ? AND local_date <= ?
"""
_RAIN_FEEDS_SQL: Final = "SELECT id, source, model FROM feeds ORDER BY id"
_RAIN_PICKS_SQL: Final = """
    SELECT cutoff, day_ahead, feed_id FROM rain_picks
    WHERE site_id = ? AND cutoff >= ? AND cutoff < ?
    ORDER BY cutoff
"""
_INSERT_RAIN_RANK_SQL: Final = """
    INSERT INTO publication_rain_ranks
        (publication_id, day_ahead, rank, feed_id, misses, false_alarms,
         common_days, counted_days, provisional, selected, in_group)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_UPSERT_RAIN_PICK_SQL: Final = """
    INSERT INTO rain_picks (site_id, cutoff, day_ahead, feed_id)
    VALUES (?, ?, ?, ?)
    ON CONFLICT (site_id, cutoff, day_ahead) DO UPDATE SET feed_id = excluded.feed_id
"""
_DELETE_OLD_RAIN_PICKS_SQL: Final = (
    "DELETE FROM rain_picks WHERE site_id = ? AND cutoff < ?"
)
_PUBLISHED_RAIN_SQL: Final = """
    SELECT day_ahead, rank, feed_id, provisional, selected, in_group
    FROM publication_rain_ranks
    WHERE publication_id = ?
    ORDER BY day_ahead, rank
"""
#: The incumbent is looked for in the picks of this many cutoffs before; the
#: swap keeps one more day of picks.
_RAIN_PICK_MEMORY_DAYS: Final = 60
_RAIN_PICK_KEEP_DAYS: Final = 61
_SITE_PUBLICATIONS_SQL: Final = (
    "SELECT id, published_at FROM publications WHERE site_id = ? ORDER BY cutoff"
)
_DELETE_PUBLICATION_SQL: Final = "DELETE FROM publications WHERE id = ?"
# forecast_downloads is never pruned, so the lower id bound keeps the walk to
# downloads that can still have value-change rows (every remaining row's
# download_id is at least the MIN, so the bound drops only downloads with
# none). NOT INDEXED stops the planner walking the covering index
# idx_forecast_downloads_day instead. With an empty log MIN is NULL and nothing
# is deleted.
_DELETE_OLD_VALUE_CHANGES_SQL: Final = (
    "DELETE FROM forecast_value_changes WHERE download_id IN"
    " (SELECT id FROM forecast_downloads NOT INDEXED"
    "  WHERE id >= (SELECT MIN(download_id) FROM forecast_value_changes)"
    "    AND ended_at < ?)"
)

# Readers. The defaults the build reads the two ranking
# settings with.
_DEFAULT_WINDOW_DAYS: Final = 30
_DEFAULT_MIN_N: Final = 30
# The latest cutoff published at or before T; T NULL takes the latest.
_ACTIVE_PUBLICATION_SQL: Final = """
    SELECT id, site_id, cutoff, first_record_date, window_days,
           window_start_date, wind_start_date, settings, methodology_version,
           scoring_rule, wind_method, wind_constants, build_started_at,
           published_at
    FROM publications
    WHERE site_id = ?
      AND (? IS NULL OR julianday(published_at) <= julianday(?))
    ORDER BY cutoff DESC
    LIMIT 1
"""
_LATEST_PUBLISHED_AT_SQL: Final = """
    SELECT p.published_at
    FROM publications p
    WHERE p.cutoff = (
        SELECT MAX(q.cutoff) FROM publications q WHERE q.site_id = p.site_id
    )
    ORDER BY julianday(p.published_at) DESC, p.published_at DESC
    LIMIT 1
"""
# A feed switched off now has no row in any read: each read joins the
# feed and its site state to the publication's site, as ``_cached_leaderboard``
# joins them to the cache's.
_ACTIVE_AT_PUBLICATION_SITE: Final = active_competitor_clause(site_expr="p.site_id")
_PUBLISHED_RANKING_SQL: Final = f"""
    SELECT pr.feed_id, f.source, f.model, pr.variable, pr.day_ahead, pr.n,
           pr.bias, pr.mae, pr.rmse, pr.skill_score
    FROM publication_rankings pr
    JOIN publications p ON p.id = pr.publication_id
    JOIN feeds f ON f.id = pr.feed_id
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = p.site_id AND sfs.feed_id = pr.feed_id
    WHERE pr.publication_id = ? AND pr.window_key = ?
      AND pr.variable = ? AND pr.day_ahead = ?
      AND {_ACTIVE_AT_PUBLICATION_SITE}
    ORDER BY f.source, f.model
"""
_PUBLISHED_CELLS_SQL: Final = f"""
    SELECT pr.feed_id, f.source, f.model, pr.variable, pr.day_ahead, pr.n,
           pr.bias, pr.mae, pr.rmse, pr.skill_score
    FROM publication_rankings pr
    JOIN publications p ON p.id = pr.publication_id
    JOIN feeds f ON f.id = pr.feed_id
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = p.site_id AND sfs.feed_id = pr.feed_id
    WHERE pr.publication_id = ? AND pr.window_key = ?
      AND {_ACTIVE_AT_PUBLICATION_SITE}
    ORDER BY f.source, f.model, pr.variable, pr.day_ahead
"""
_PUBLISHED_COVERAGE_SQL: Final = f"""
    SELECT pc.feed_id, pc.day_ahead, pc.scored, pc.expected, pc.starting_days
    FROM publication_coverage pc
    JOIN publications p ON p.id = pc.publication_id
    JOIN feeds f ON f.id = pc.feed_id
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = p.site_id AND sfs.feed_id = pc.feed_id
    WHERE pc.publication_id = ? AND pc.variable = ? AND pc.window_key = ?
      AND {_ACTIVE_AT_PUBLICATION_SITE}
    ORDER BY pc.feed_id, pc.day_ahead
"""
_PUBLISHED_WIND_SQL: Final = f"""
    SELECT pw.feed_id, pw.day_ahead, pw.mae_ms, pw.days, pw.weight
    FROM publication_wind_weights pw
    JOIN publications p ON p.id = pw.publication_id
    JOIN feeds f ON f.id = pw.feed_id
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = p.site_id AND sfs.feed_id = pw.feed_id
    WHERE pw.publication_id = ?
      AND {_ACTIVE_AT_PUBLICATION_SITE}
    ORDER BY pw.feed_id, pw.day_ahead
"""
# The feeds an ``Nd`` view may show, in the leaderboard's order. Two binds:
# the join, and the meteoblue package check inside the clause.
_COMPETITOR_FEEDS_SQL: Final = f"""
    SELECT f.id, f.source, f.model
    FROM feeds f
    LEFT JOIN site_feed_state sfs ON sfs.site_id = ? AND sfs.feed_id = f.id
    WHERE {active_competitor_clause(site_expr="?")}
    ORDER BY f.source, f.model
"""

_Number = float | int | None
_Key = tuple[int, str, int]
_Values = tuple[object, ...]


class PublicationFailed(Exception):
    """The publication cannot be built or swapped in.

    Its message is the reason alone, ``cutoff_not_closed`` or ``no_site``,
    which the close chain saves as the publication record's ``last_failure``.
    """

    def __init__(self, reason: PublicationFailure) -> None:
        super().__init__(reason)
        self.reason: PublicationFailure = reason


@dataclass(frozen=True, slots=True)
class Publication:
    """One ``publications`` row, with ``settings`` parsed into
    ``rolling_window_days`` and ``min_n``."""

    id: int
    site_id: int
    cutoff: date
    first_record_date: date
    window_days: int
    window_start_date: date
    wind_start_date: date
    rolling_window_days: int
    min_n: int
    methodology_version: int
    scoring_rule: str
    wind_method: str
    wind_constants: str
    build_started_at: str
    published_at: str


@dataclass(frozen=True, slots=True)
class ScoredCell:
    """One feed's scores in one (variable, day ahead) cell of a window."""

    feed_id: int
    source: str
    model: str
    variable: str
    day_ahead: int
    n: int
    bias: float | None
    mae: float | None
    rmse: float | None
    skill_score: float | None


@dataclass(frozen=True, slots=True)
class Coverage:
    """One feed's coverage in one cell of a publication's window."""

    scored: int
    expected: int
    starting_days: int


@dataclass(frozen=True, slots=True)
class RainRank:
    """One ranked rain model of a publication's day ahead."""

    feed_id: int
    rank: int
    selected: bool
    in_group: bool


@dataclass(frozen=True, slots=True)
class RainRanks:
    """A publication's rain ranking of one day ahead, in rank order."""

    day_ahead: int
    provisional: bool
    ranked: tuple[RainRank, ...]


@dataclass(frozen=True, slots=True)
class _Build:
    """What the build read and computed, ready for the swap."""

    window_days: int
    min_n: int
    first_record_date: date
    window_start: date
    wind_start: date
    rankings: tuple[_Values, ...]
    coverage: tuple[_Values, ...]
    wind_weights: tuple[_Values, ...]
    #: ``publication_rain_ranks`` rows less the publication id, and one
    #: (day ahead, selected feed or None) per day ahead.
    rain_ranks: tuple[_Values, ...]
    rain_picks: tuple[tuple[int, int | None], ...]


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

    The close chain's stage 7 call point. A cutoff already published returns
    its id and writes nothing. Otherwise one read-only snapshot
    reads the settings, requires ``cutoff``'s record to be ``closed`` (else
    ``PublicationFailed("cutoff_not_closed")``) and builds every row; one
    ``writer.write`` then swaps the publication in, raising
    ``PublicationFailed("no_site")`` when the site is gone. Returns the
    publication's id as text. ``attempt`` is the close chain's and not read;
    both stamps are ``now_utc`` to the whole second.
    """
    return await _publish(
        db,
        writer,
        site_id=site_id,
        cutoff=cutoff,
        now_utc=now_utc,
    )


# ---------------------------------------------------------------------------
# The build and the swap
# ---------------------------------------------------------------------------


async def _publish(
    db: Database,
    writer: FencedWriter,
    *,
    site_id: int,
    cutoff: date,
    now_utc: datetime,
) -> str:
    """Repeat check, build snapshot, swap.

    The cutoff's record must be ``closed`` and the first record is read.
    """
    existing = await db.read(lambda conn: _publication_id(conn, site_id, cutoff))
    if existing is not None:
        return str(existing)
    published = _whole_second(now_utc)
    build = await writer.read_snapshot_only(
        lambda conn: _build(conn, site_id=site_id, cutoff=cutoff),
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
) -> _Build:
    """Build every row, in the caller's read-only snapshot. Never writes.

    The settings are read once, here, and every later value comes from
    them. ``compute_wind_weights`` is pure.
    """
    window_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
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
    rain = _rain_rankings(conn, site_id, cutoff=cutoff)
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
        rain_ranks=tuple(
            (
                ranking.day_ahead,
                ranked.rank,
                ranked.feed_id,
                ranked.misses,
                ranked.false_alarms,
                ranked.common_days,
                ranked.counted_days,
                int(ranking.provisional),
                int(ranked.selected),
                int(ranked.in_group),
            )
            for ranking in rain
            for ranked in ranking.ranked
        ),
        rain_picks=tuple(
            (ranking.day_ahead, ranking.selected_feed_id) for ranking in rain
        ),
    )


def _rain_rankings(
    conn: sqlite3.Connection, site_id: int, *, cutoff: date
) -> list[RainRanking]:
    """The rain ranking of each day ahead 0 to 7 at ``cutoff``.

    Over the site's active real feeds (``active_real_feeds``; no yardstick),
    the window's known truth days from ``cutoff`` - 59 to ``cutoff``
    (``rain_days`` rain or dry) and each feed's daily totals there at the day
    ahead (``rain_forecast_days``). The incumbent of a day ahead is the
    latest ``rain_picks`` row at it among the 60 cutoffs before; a row with
    no feed clears it. Read-only, in the build's snapshot.
    """
    window_start = _shift(cutoff, -(WINDOW_DAYS - 1))
    span = (site_id, window_start.isoformat(), cutoff.isoformat())
    truth = {
        date.fromisoformat(str(row["local_date"])): str(row["outcome"]) == "rain"
        for row in conn.execute(_RAIN_TRUTH_SQL, span)
    }
    totals: dict[tuple[int, int], dict[date, bool]] = {}
    for row in conn.execute(_RAIN_FORECASTS_SQL, span):
        key = (int(row["feed_id"]), int(row["day_ahead"]))
        totals.setdefault(key, {})[date.fromisoformat(str(row["local_date"]))] = (
            forecasts_rain(float(row["total_mm"]))
        )
    active = active_real_feeds(conn, site_id)
    feeds = [
        (int(row["id"]), str(row["source"]), str(row["model"]))
        for row in conn.execute(_RAIN_FEEDS_SQL)
        if int(row["id"]) in active
    ]
    incumbents: dict[int, int | None] = {}
    for row in conn.execute(
        _RAIN_PICKS_SQL,
        (
            site_id,
            _shift(cutoff, -_RAIN_PICK_MEMORY_DAYS).isoformat(),
            cutoff.isoformat(),
        ),
    ):
        # Rows arrive in cutoff order, so the latest row of a day ahead wins.
        feed_id: object = row["feed_id"]
        incumbents[int(row["day_ahead"])] = (
            None if feed_id is None else int(cast(int, feed_id))
        )
    return [
        rank_rain(
            day_ahead=day_ahead,
            truth=truth,
            models=[
                ModelSeries(
                    feed_id=feed_id,
                    source=source,
                    model=model,
                    counted=totals.get((feed_id, day_ahead), {}),
                )
                for feed_id, source, model in feeds
            ],
            incumbent_feed_id=incumbents.get(day_ahead),
        )
        for day_ahead in RAIN_DAYS_AHEAD
    ]


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
    ``scored`` and ``expected``; dates before ``first`` from
    ``starting_scores``, counted apart as ``starting_days``. A window counts
    calendar dates: a date with no row adds nothing.
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
    """The daily highs of the wind window [start, cutoff].

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
    """The swap, in one write: repeat check, site check, the rows, the deletes."""
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
    # The rain ranking, one pick per day ahead, and the picks the next
    # incumbent look-back no longer reaches.
    conn.executemany(
        _INSERT_RAIN_RANK_SQL, [(publication_id, *row) for row in build.rain_ranks]
    )
    conn.executemany(
        _UPSERT_RAIN_PICK_SQL,
        [
            (site_id, cutoff.isoformat(), day_ahead, feed_id)
            for day_ahead, feed_id in build.rain_picks
        ],
    )
    conn.execute(
        _DELETE_OLD_RAIN_PICKS_SQL,
        (site_id, _shift(cutoff, -_RAIN_PICK_KEEP_DAYS).isoformat()),
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
    more than ``PUBLICATION_KEEP_HOURS`` before ``published``.

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
# Readers
# ---------------------------------------------------------------------------


def active_publication(
    conn: sqlite3.Connection, *, site_id: int, at_utc: str | None
) -> Publication | None:
    """The site's publication active at ``at_utc``, or ``None``.

    The latest cutoff among the site's publications whose ``published_at``
    is at or before ``at_utc``; ``at_utc`` None takes the latest cutoff.
    Resolved by cutoff and publication time, never by id. One SELECT on
    ``conn``, inside whatever snapshot the caller holds.
    """
    row = conn.execute(_ACTIVE_PUBLICATION_SQL, (site_id, at_utc, at_utc)).fetchone()
    return None if row is None else _publication_from_row(row)


def latest_published_at(conn: sqlite3.Connection) -> str | None:
    """The newest ``published_at`` over the sites' active publications, or
    ``None`` before the first publication."""
    row = conn.execute(_LATEST_PUBLISHED_AT_SQL).fetchone()
    return None if row is None else str(row["published_at"])


def published_ranking(
    conn: sqlite3.Connection,
    *,
    pub: Publication,
    variable: str,
    day_ahead: int,
    window_key: str,
) -> dict[int, LeaderboardRow]:
    """The publication's ranking for one cell and window, keyed by feed id.

    Only feeds that pass ``active_competitor_clause`` now, in
    ``_cached_leaderboard``'s order (source, model); ``confident`` from the
    publication's ``min_n``. Shared by the leaderboard, the served forecast
    and the record.
    """
    window_days = None if window_key == _ALL_WINDOW_KEY else pub.window_days
    rows = conn.execute(
        _PUBLISHED_RANKING_SQL, (pub.id, window_key, variable, day_ahead)
    ).fetchall()
    return _ranking_rows(
        (_scored_cell(row) for row in rows),
        min_n=pub.min_n,
        window_key=window_key,
        window_days=window_days,
    )


def published_cells(
    conn: sqlite3.Connection, *, pub: Publication, window_key: str
) -> list[ScoredCell]:
    """Every ranking row of the publication's window, for the feeds that
    pass ``active_competitor_clause`` now, ordered by source, model,
    variable and day ahead (the composite's read)."""
    rows = conn.execute(_PUBLISHED_CELLS_SQL, (pub.id, window_key)).fetchall()
    return [_scored_cell(row) for row in rows]


def live_cells(
    conn: sqlite3.Connection, *, pub: Publication, window_days: int
) -> list[ScoredCell]:
    """An ``Nd`` view, summed live over the ``window_days`` dates ending at
    the publication's cutoff.

    As ``_window_cells`` does: ``day_scores`` on the dates from the
    publication's first record whose record is ``closed`` (a missed date's
    rows are never read) and ``starting_scores`` before it, through
    ``metrics_from_sums``
    with the publication's ``min_n``. Never reaches past the cutoff, so
    never an open day. Only feeds that pass ``active_competitor_clause``
    now, ordered by source, model, variable and day ahead; a cell with no
    pair is left out.
    """
    sums = _window_cells(
        conn,
        pub.site_id,
        start=_shift(pub.cutoff, -(window_days - 1)),
        cutoff=pub.cutoff,
        first=pub.first_record_date,
    )
    by_feed: dict[int, list[tuple[str, int]]] = {}
    for feed_id, variable, day_ahead in sums:
        by_feed.setdefault(feed_id, []).append((variable, day_ahead))
    out: list[ScoredCell] = []
    for row in conn.execute(_COMPETITOR_FEEDS_SQL, (pub.site_id, pub.site_id)):
        feed_id = int(row["id"])
        for variable, day_ahead in sorted(by_feed.get(feed_id, [])):
            metrics = metrics_from_sums(
                sums[(feed_id, variable, day_ahead)].sums,
                variable=variable,
                min_n=pub.min_n,
            )
            if metrics.n > 0:
                out.append(
                    ScoredCell(
                        feed_id=feed_id,
                        source=str(row["source"]),
                        model=str(row["model"]),
                        variable=variable,
                        day_ahead=day_ahead,
                        n=metrics.n,
                        bias=metrics.bias,
                        mae=metrics.mae,
                        rmse=metrics.rmse,
                        skill_score=metrics.skill_score,
                    )
                )
    return out


def live_ranking(
    conn: sqlite3.Connection,
    *,
    pub: Publication,
    variable: str,
    day_ahead: int,
    window_days: int,
) -> dict[int, LeaderboardRow]:
    """The ``Nd`` view of :func:`live_cells` for one cell, keyed by feed id,
    as ``live:{N}d`` rows."""
    cells = (
        cell
        for cell in live_cells(conn, pub=pub, window_days=window_days)
        if cell.variable == variable and cell.day_ahead == day_ahead
    )
    return _ranking_rows(
        cells,
        min_n=pub.min_n,
        window_key=f"live:{window_days}d",
        window_days=window_days,
    )


def published_coverage(
    conn: sqlite3.Connection, *, pub: Publication, variable: str, window_key: str
) -> dict[tuple[int, int], Coverage]:
    """The publication's coverage for one variable and window, keyed by
    (feed id, day ahead), for the feeds that pass ``active_competitor_clause``
    now."""
    rows = conn.execute(_PUBLISHED_COVERAGE_SQL, (pub.id, variable, window_key))
    return {
        (int(row["feed_id"]), int(row["day_ahead"])): Coverage(
            scored=int(row["scored"]),
            expected=int(row["expected"]),
            starting_days=int(row["starting_days"]),
        )
        for row in rows
    }


def pending_ranking_settings(
    conn: sqlite3.Connection, *, pub: Publication | None
) -> dict[str, tuple[int, int]]:
    """``(published, saved)`` for each ranking setting whose saved value
    differs from ``pub``'s; ``{}`` for ``pub`` None.

    The saved values are read as the build reads them.
    """
    if pub is None:
        return {}
    pairs = {
        "rolling_window_days": (
            pub.rolling_window_days,
            get_number_setting(
                conn, "rolling_window_days", _DEFAULT_WINDOW_DAYS, minimum=1
            ),
        ),
        "min_n": (
            pub.min_n,
            get_number_setting(conn, "min_n", _DEFAULT_MIN_N, minimum=0),
        ),
    }
    return {key: pair for key, pair in pairs.items() if pair[0] != pair[1]}


def published_wind_weights(
    conn: sqlite3.Connection, *, pub: Publication
) -> WindWeights:
    """The publication's wind weights, for the feeds that pass
    ``active_competitor_clause`` now."""
    return {
        (int(row["feed_id"]), int(row["day_ahead"])): FeedWeight(
            mae_ms=float(row["mae_ms"]),
            days=int(row["days"]),
            weight=float(row["weight"]),
        )
        for row in conn.execute(_PUBLISHED_WIND_SQL, (pub.id,))
    }


def published_wind_serving(
    conn: sqlite3.Connection, *, site_id: int, as_of: str | None
) -> WindWeights:
    """The wind weights served.

    The weights of the publication active at ``as_of`` (the latest with
    ``as_of`` None), ``{}`` with no publication. Inside the caller's
    transaction when one is open; otherwise in one read snapshot.
    """
    if conn.in_transaction:
        return _published_wind_now(conn, site_id=site_id, as_of=as_of)
    with read_snapshot(conn, label="wind_weights"):
        return _published_wind_now(conn, site_id=site_id, as_of=as_of)


def _published_wind_now(
    conn: sqlite3.Connection, *, site_id: int, as_of: str | None
) -> WindWeights:
    pub = active_publication(conn, site_id=site_id, at_utc=as_of)
    return {} if pub is None else published_wind_weights(conn, pub=pub)


def published_rain_ranks(
    conn: sqlite3.Connection, site_id: int
) -> dict[int, RainRanks]:
    """The active publication's rain ranking, by day ahead.

    Read as :func:`published_wind_serving` reads the weights: inside the
    caller's transaction when one is open; otherwise in one read snapshot.
    Empty with no publication, and for a publication with no rain rows; a
    day ahead with no ranked model has no entry, so its tile has no rain
    forecast. Read-only.
    """
    if conn.in_transaction:
        return _published_rain_now(conn, site_id)
    with read_snapshot(conn, label="rain_ranks"):
        return _published_rain_now(conn, site_id)


def _published_rain_now(conn: sqlite3.Connection, site_id: int) -> dict[int, RainRanks]:
    pub = active_publication(conn, site_id=site_id, at_utc=None)
    if pub is None:
        return {}
    rows: dict[int, list[RainRank]] = {}
    provisional: dict[int, bool] = {}
    for row in conn.execute(_PUBLISHED_RAIN_SQL, (pub.id,)):
        day_ahead = int(row["day_ahead"])
        rows.setdefault(day_ahead, []).append(
            RainRank(
                feed_id=int(row["feed_id"]),
                rank=int(row["rank"]),
                selected=int(row["selected"]) == 1,
                in_group=int(row["in_group"]) == 1,
            )
        )
        provisional[day_ahead] = provisional.get(day_ahead, False) or (
            int(row["provisional"]) == 1
        )
    return {
        day_ahead: RainRanks(
            day_ahead=day_ahead,
            provisional=provisional[day_ahead],
            ranked=tuple(ranked),
        )
        for day_ahead, ranked in rows.items()
    }


def _publication_from_row(row: sqlite3.Row) -> Publication:
    """A ``publications`` row as a :class:`Publication`.

    A settings value that is missing or not a whole number in range falls
    back to the row's ``window_days`` and the build's default ``min_n``.
    """
    settings = _settings_object(row["settings"])
    window_days = int(row["window_days"])
    return Publication(
        id=int(row["id"]),
        site_id=int(row["site_id"]),
        cutoff=date.fromisoformat(str(row["cutoff"])),
        first_record_date=date.fromisoformat(str(row["first_record_date"])),
        window_days=window_days,
        window_start_date=date.fromisoformat(str(row["window_start_date"])),
        wind_start_date=date.fromisoformat(str(row["wind_start_date"])),
        rolling_window_days=_setting_int(
            settings, "rolling_window_days", minimum=1, default=window_days
        ),
        min_n=_setting_int(settings, "min_n", minimum=0, default=_DEFAULT_MIN_N),
        methodology_version=int(row["methodology_version"]),
        scoring_rule=str(row["scoring_rule"]),
        wind_method=str(row["wind_method"]),
        wind_constants=str(row["wind_constants"]),
        build_started_at=str(row["build_started_at"]),
        published_at=str(row["published_at"]),
    )


def _settings_object(raw: object) -> dict[str, object]:
    """The ``settings`` JSON object; ``{}`` for anything else."""
    if not isinstance(raw, str):
        return {}
    try:
        parsed: object = json.loads(raw)
    except (ValueError, RecursionError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        key: value
        for key, value in cast(dict[object, object], parsed).items()
        if isinstance(key, str)
    }


def _setting_int(
    settings: dict[str, object], key: str, *, minimum: int, default: int
) -> int:
    value = settings.get(key)
    if isinstance(value, int) and not isinstance(value, bool) and value >= minimum:
        return value
    return default


def _scored_cell(row: sqlite3.Row) -> ScoredCell:
    return ScoredCell(
        feed_id=int(row["feed_id"]),
        source=str(row["source"]),
        model=str(row["model"]),
        variable=str(row["variable"]),
        day_ahead=int(row["day_ahead"]),
        n=int(row["n"]),
        bias=_optional_float(row["bias"]),
        mae=_optional_float(row["mae"]),
        rmse=_optional_float(row["rmse"]),
        skill_score=_optional_float(row["skill_score"]),
    )


def _ranking_rows(
    cells: Iterable[ScoredCell],
    *,
    min_n: int,
    window_key: str,
    window_days: int | None,
) -> dict[int, LeaderboardRow]:
    """Leaderboard rows in the cells' order, ``confident`` as the cache path
    decides it (``n >= min_n`` and a skill score)."""
    return {
        cell.feed_id: LeaderboardRow(
            feed_id=cell.feed_id,
            source=cell.source,
            model=cell.model,
            n=cell.n,
            skill_score=cell.skill_score,
            badge=score_badge(cell.skill_score),
            below_baseline=below_baseline(cell.skill_score),
            confident=cell.n >= min_n and cell.skill_score is not None,
            bias=cell.bias,
            mae=cell.mae,
            rmse=cell.rmse,
            window_key=window_key,
            window_days=window_days,
        )
        for cell in cells
    }


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


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float | str):
        raise TypeError("invalid numeric publication value")
    return float(value)


def _shift(day: date, days: int) -> date:
    """``day`` moved by ``days``, held inside ``date``'s range."""
    ordinal = day.toordinal() + days
    return date.fromordinal(min(max(ordinal, 1), date.max.toordinal()))


def _whole_second(now_utc: datetime) -> datetime:
    """``now_utc`` as UTC, to the whole second."""
    aware = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    return aware.astimezone(UTC).replace(microsecond=0)

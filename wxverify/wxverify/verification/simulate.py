"""Walk-forward simulation: one snapshot day's verification evidence (§8-§10).

For each snapshot local date S the simulator reconstructs the decision
instant T exactly as the forecast-of-record builder does (same wall clock,
same fold-0 resolution, same §6 as-of parameterization of the production
ranking) and emits self-contained evidence rows for every entity of the
§10 candidate family: blend depths 1-4 under the incumbent hourly-skill
ranking, every pinned roster feed alone, the §9 baselines, and the
diagnostic daily-quantity-error ranking family. Rows are append-only per
run (``ON CONFLICT DO NOTHING`` on the row identity) — a retried chunk can
only confirm, never replace.

Leakage rule: prediction INPUTS are as-of-T (samples via ``as_of``,
rankings via the §6 parameterization, persistence via knowable-at-T truth);
SCORING joins the run's pinned CURRENT truth — §8 scores what we know now
about what was predicted then.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import day_ahead as issue_day_ahead
from wxverify.core.timeutil import isoformat_utc, window_cutoff
from wxverify.forecast.aggregate import (
    blend_mean,
    clearing_subset,
    covered_hours,
    display_day_index,
    displayed_daily,
)
from wxverify.forecast.data import (
    FutureSampleRow,
    count_null_availability_samples,
    forecast_ranking,
    load_future_samples,
)
from wxverify.forecast.selection import (
    CellCandidate,
    representative_day_ahead,
    select_cell_feeds,
)
from wxverify.scoring.leaderboard import LeaderboardRow
from wxverify.settings.depth import DEPTH_VARIABLES
from wxverify.verification.asof import pair_knowability_exclusions
from wxverify.verification.coverage import (
    QUANTITY_PRECIP_OCCURRENCE,
    VARIABLE_QUANTITIES,
    QuantityOutcome,
    evaluate_variable,
    local_day_bounds,
)
from wxverify.verification.methodology import (
    CONSENSUS_LAG_HOURS,
    DAILY_RANK_MIN_HISTORY_DAYS,
)
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.runs import RunConfig

#: The simulated horizon matches the record horizon: target day 0..7.
SIM_DAY_COUNT = 8
SIM_VARIABLES: tuple[str, ...] = DEPTH_VARIABLES
#: Headline §10 blend depths.
SIM_DEPTHS: tuple[int, ...] = (1, 2, 3, 4)

#: Evidence-row forecast exclusion reasons OWNED by the simulator (coverage
#: reasons pass through from ``evaluate_variable`` verbatim).
EXCLUDE_NO_SAMPLES = "no_samples"
EXCLUDE_NO_PRIOR_TRUTH = "no_prior_truth"
EXCLUDE_INSUFFICIENT_RANK_HISTORY = "insufficient_rank_history"
EXCLUDE_TRUTH_MISSING = "truth_missing"

#: Displayed-daily key per continuous quantity.
_DISPLAY_KEY: dict[str, str] = {
    "temperature_high": "high_c",
    "temperature_low": "low_c",
    "wind_max": "max_ms",
    "precip_total": "total_mm",
}

#: Occurrence wet/dry decision threshold on 0..1 predicted/truth values.
_OCCURRENCE_THRESHOLD = 0.5


@dataclass(frozen=True)
class _Entity:
    """One §10 entity's per-day forecast, before the truth join."""

    entity_type: str
    entity_key: str
    predicted: float | None
    forecast_eligible: bool
    forecast_exclusion_reason: str | None
    covered_hours: int | None
    realized_contributors: int | None


def classify_occurrence_outcome(
    predicted: float | None, truth_value: float | None
) -> str | None:
    """Contingency class of one occurrence day; None when either side missing."""
    if predicted is None or truth_value is None:
        return None
    predicted_wet = predicted >= _OCCURRENCE_THRESHOLD
    observed_wet = truth_value >= _OCCURRENCE_THRESHOLD
    if predicted_wet:
        return "hit" if observed_wet else "false_alarm"
    return "miss" if observed_wet else "correct_negative"


def latest_knowable_target(timezone: str, snapshot_utc: datetime) -> date:
    """Latest local date whose truth was knowable at T (§10 daily-rank cutoff).

    A target day's outcome is knowable once the local day has ended plus
    the consensus lag — the same Delta_consensus every as-of read applies.
    """
    day = snapshot_utc.astimezone(ZoneInfo(timezone)).date()
    lag = timedelta(hours=CONSENSUS_LAG_HOURS)
    while local_day_bounds(day, timezone).end_utc + lag > snapshot_utc:
        day -= timedelta(days=1)
    return day


def _group_samples(
    samples: list[FutureSampleRow], *, timezone: str, now: datetime
) -> dict[str, dict[int, dict[int, list[FutureSampleRow]]]]:
    """variable -> display day -> feed_id -> samples, anchored at ``now=T``.

    Same grouping rule as the record builder (display-day index against T)
    so the simulated day axis is the axis the page showed at T.
    """
    grouped: dict[str, dict[int, dict[int, list[FutureSampleRow]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for sample in samples:
        day = display_day_index(sample.valid_at, timezone=timezone, now=now)
        if 0 <= day < SIM_DAY_COUNT:
            grouped[sample.variable][day][sample.feed_id].append(sample)
    return grouped


def _blend_hourly(
    feed_ids: list[int], feeds_samples: dict[int, list[FutureSampleRow]]
) -> list[tuple[str, float]]:
    """Equal-weight blended hourly series of the given feeds."""
    per_hour: dict[str, list[float]] = defaultdict(list)
    for feed_id in feed_ids:
        for sample in feeds_samples.get(feed_id, []):
            per_hour[sample.valid_at].append(sample.value)
    out: list[tuple[str, float]] = []
    for valid_at in sorted(per_hour):
        mean = blend_mean(per_hour[valid_at])
        if mean is not None:
            out.append((valid_at, mean))
    return out


def _quantity_outcomes(
    variable: str,
    hourly: list[tuple[str, float]],
    *,
    timezone: str,
    target_date: date,
    rain_threshold_mm: float,
) -> dict[str, QuantityOutcome]:
    return {
        outcome.quantity: outcome
        for outcome in evaluate_variable(
            variable,
            hourly,
            timezone=timezone,
            local_date=target_date,
            rain_threshold_mm=rain_threshold_mm,
        )
    }


def _entities_for_selection(
    *,
    entity_type: str,
    entity_key: str,
    variable: str,
    feed_ids: list[int],
    feeds_samples: dict[int, list[FutureSampleRow]],
    timezone: str,
    target_date: date,
    rain_threshold_mm: float,
) -> dict[str, _Entity]:
    """Per-quantity entity rows for one blended feed selection.

    Displayed values come from the SAME production path the tile used
    (clearing subset, aggregate per feed, then blend); eligibility and the
    occurrence value come from ``evaluate_variable`` over the blended
    hourly series (§5 forecast-side eligibility — record.py parity).

    One feed set describes the scored entity end to end: the clearing
    subset. Coverage, occurrence, eligibility and the contributor count all
    describe the same feeds the displayed value was computed from.
    """
    present = [fid for fid in feed_ids if feeds_samples.get(fid)]
    out: dict[str, _Entity] = {}
    if not present:
        for quantity in VARIABLE_QUANTITIES[variable]:
            out[quantity] = _Entity(
                entity_type=entity_type,
                entity_key=entity_key,
                predicted=None,
                forecast_eligible=False,
                forecast_exclusion_reason=EXCLUDE_NO_SAMPLES,
                covered_hours=None,
                realized_contributors=0,
            )
        return out
    agg_ids, _partial = clearing_subset(
        present,
        {fid: covered_hours(s.valid_at for s in feeds_samples[fid]) for fid in present},
    )
    displayed = displayed_daily(
        variable,
        [[s.value for s in feeds_samples[fid]] for fid in agg_ids],
        rain_threshold_mm=rain_threshold_mm,
    )
    # The scored product IS the displayed product, so ``agg_ids`` -- the
    # clearing subset ``displayed`` is computed over -- is the feed set of
    # record for everything that qualifies it: the blended hourly series,
    # the covered-hour count and the contributor count alike.
    hourly = _blend_hourly(agg_ids, feeds_samples)
    hours = covered_hours(valid_at for valid_at, _ in hourly)
    outcomes = _quantity_outcomes(
        variable,
        hourly,
        timezone=timezone,
        target_date=target_date,
        rain_threshold_mm=rain_threshold_mm,
    )
    for quantity in VARIABLE_QUANTITIES[variable]:
        outcome = outcomes[quantity]
        if quantity == QUANTITY_PRECIP_OCCURRENCE:
            predicted = outcome.value
        else:
            predicted = displayed.get(_DISPLAY_KEY[quantity])
        out[quantity] = _Entity(
            entity_type=entity_type,
            entity_key=entity_key,
            predicted=predicted,
            forecast_eligible=outcome.eligible,
            forecast_exclusion_reason=outcome.exclusion_reason,
            covered_hours=hours,
            realized_contributors=len(agg_ids),
        )
    return out


def _persistence_predictions(
    conn: sqlite3.Connection, cfg: RunConfig, as_of: str, snapshot_local_date: str
) -> dict[str, float | None]:
    """§9 persistence: latest prior scorable truth knowable before T, per quantity."""
    out: dict[str, float | None] = {}
    for quantities in VARIABLE_QUANTITIES.values():
        for quantity in quantities:
            row = conn.execute(
                f"""
                SELECT value FROM daily_truth
                WHERE site_id = ? AND quantity = ? AND tz_generation_id = ?
                  AND eligible = 1 AND value IS NOT NULL
                  AND local_date < ?
                  AND julianday(day_end_utc, '+{CONSENSUS_LAG_HOURS} hours')
                      <= julianday(?)
                  AND (source_max_computed_at IS NULL
                       OR julianday(source_max_computed_at) <= julianday(?))
                ORDER BY local_date DESC LIMIT 1
                """,
                (
                    cfg.site_id,
                    quantity,
                    cfg.tz_generation_id,
                    snapshot_local_date,
                    as_of,
                    as_of,
                ),
            ).fetchone()
            out[quantity] = None if row is None else float(row["value"])
    return out


def _daily_rank_order(
    conn: sqlite3.Connection,
    cfg: RunConfig,
    *,
    quantity: str,
    knowable_through: str,
    as_of: str,
) -> list[int]:
    """§10 diagnostic ranking: feeds by mean daily |error| on this quantity.

    Ranks over THIS run's already-persisted per-feed evidence at lead 1,
    restricted to target days knowable at T; a feed needs
    ``DAILY_RANK_MIN_HISTORY_DAYS`` scorable days to rank. Ties break by
    (source, model), as production ranking does. F-2: like
    ``_persistence_predictions``, a target day whose truth was REVISED
    after T is excluded — the as-of ranker could not have known the
    revised value, and a post-T revision must not flip the rank order.
    """
    rows = conn.execute(
        """
        SELECT e.entity_key, COUNT(*) AS n, AVG(e.abs_error) AS mean_abs
        FROM verification_evidence e
        JOIN daily_truth dt
          ON dt.site_id = ? AND dt.tz_generation_id = ?
         AND dt.quantity = e.quantity
         AND dt.local_date = e.target_local_date
        WHERE e.run_id = ? AND e.entity_type = 'feed' AND e.quantity = ?
          AND e.lead = 1 AND e.forecast_eligible = 1 AND e.truth_eligible = 1
          AND e.abs_error IS NOT NULL AND e.target_local_date <= ?
          AND (dt.source_max_computed_at IS NULL
               OR julianday(dt.source_max_computed_at) <= julianday(?))
        GROUP BY e.entity_key
        HAVING COUNT(*) >= ?
        """,
        (
            cfg.site_id,
            cfg.tz_generation_id,
            cfg.run_id,
            quantity,
            knowable_through,
            as_of,
            DAILY_RANK_MIN_HISTORY_DAYS,
        ),
    ).fetchall()
    by_feed = {f.feed_id: f for f in cfg.roster}
    ranked: list[tuple[float, str, str, int]] = []
    for row in rows:
        feed_id = int(row["entity_key"])
        feed = by_feed.get(feed_id)
        if feed is None:
            continue
        ranked.append((float(row["mean_abs"]), feed.source, feed.model, feed_id))
    ranked.sort()
    return [feed_id for _, _, _, feed_id in ranked]


def _truth_rows(
    conn: sqlite3.Connection, cfg: RunConfig, target_local_date: str
) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT * FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND tz_generation_id = ?
        """,
        (cfg.site_id, target_local_date, cfg.tz_generation_id),
    ).fetchall()
    return {str(row["quantity"]): row for row in rows}


def _insert_evidence(
    conn: sqlite3.Connection,
    cfg: RunConfig,
    *,
    snapshot_local_date: str,
    target_local_date: str,
    lead: int,
    variable: str,
    quantity: str,
    entity: _Entity,
    truth: sqlite3.Row | None,
) -> None:
    if truth is None:
        truth_value = None
        truth_eligible = False
        truth_reason: str | None = EXCLUDE_TRUTH_MISSING
        truth_hours = truth_wet = truth_dry = None
    else:
        truth_value = None if truth["value"] is None else float(truth["value"])
        truth_eligible = bool(truth["eligible"])
        truth_reason = (
            None
            if truth["exclusion_reason"] is None
            else str(truth["exclusion_reason"])
        )
        truth_hours = int(truth["covered_hours"])
        truth_wet = None if truth["wet_hours"] is None else int(truth["wet_hours"])
        truth_dry = None if truth["dry_hours"] is None else int(truth["dry_hours"])
    abs_error: float | None = None
    occurrence_outcome: str | None = None
    if (
        entity.forecast_eligible
        and truth_eligible
        and entity.predicted is not None
        and truth_value is not None
    ):
        abs_error = abs(entity.predicted - truth_value)
        if quantity == QUANTITY_PRECIP_OCCURRENCE:
            occurrence_outcome = classify_occurrence_outcome(
                entity.predicted, truth_value
            )
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             forecast_exclusion_reason, covered_hours, realized_contributors,
             truth_value, truth_eligible, truth_exclusion_reason,
             truth_covered_hours, truth_wet_hours, truth_dry_hours,
             abs_error, occurrence_outcome)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id, snapshot_local_date, lead, variable, quantity,
                    entity_type, entity_key) DO NOTHING
        """,
        (
            cfg.run_id,
            snapshot_local_date,
            target_local_date,
            lead,
            variable,
            quantity,
            entity.entity_type,
            entity.entity_key,
            entity.predicted,
            1 if entity.forecast_eligible else 0,
            entity.forecast_exclusion_reason,
            entity.covered_hours,
            entity.realized_contributors,
            truth_value,
            1 if truth_eligible else 0,
            truth_reason,
            truth_hours,
            truth_wet,
            truth_dry,
            abs_error,
            occurrence_outcome,
        ),
    )


def _snapshot_samples(
    conn: sqlite3.Connection, cfg: RunConfig, local_date: date, as_of: str
) -> list[FutureSampleRow]:
    """The snapshot day's as-of sample set, restricted to the pinned roster."""
    bounds = local_day_bounds(local_date, cfg.timezone)
    roster_ids = {f.feed_id for f in cfg.roster}
    return [
        s
        for s in load_future_samples(
            conn,
            site_id=cfg.site_id,
            since_valid_at=isoformat_utc(bounds.start_utc),
            as_of=as_of,
        )
        if s.feed_id in roster_ids
    ]


def _pass1_truth_source(
    conn: sqlite3.Connection,
    cfg: RunConfig,
    *,
    snapshot_local_date: str,
    target_local_date: str,
    lead: int,
    variable: str,
    quantity: str,
) -> sqlite3.Row | None:
    """The pass-1 row whose truth columns a pass-2 baseline row copies (§7.4a).

    The canonical source is the shallowest simulated depth, which pass 1
    writes for EVERY cell-date including the no-samples case. Returns None
    when the source is absent: pass 2 then writes nothing rather than
    re-reading ``daily_truth`` and inventing a second truth pairing.
    """
    return conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = ? AND target_local_date = ?
          AND lead = ? AND variable = ? AND quantity = ?
          AND entity_type = 'depth' AND entity_key = ?
        """,
        (
            cfg.run_id,
            snapshot_local_date,
            target_local_date,
            lead,
            variable,
            quantity,
            str(SIM_DEPTHS[0]),
        ),
    ).fetchone()


def _insert_baseline_evidence(
    conn: sqlite3.Connection,
    cfg: RunConfig,
    *,
    snapshot_local_date: str,
    target_local_date: str,
    lead: int,
    variable: str,
    quantity: str,
    entity: _Entity,
    source: sqlite3.Row,
) -> None:
    """Write one pass-2 baseline row, copying its truth from ``source``.

    Six truth columns are copied verbatim; ``abs_error`` and
    ``occurrence_outcome`` are entity-specific and re-derived from the
    copied truth against THIS entity's ``predicted``.
    """
    truth_value = (
        None if source["truth_value"] is None else float(source["truth_value"])
    )
    truth_eligible = bool(source["truth_eligible"])
    abs_error: float | None = None
    occurrence_outcome: str | None = None
    if (
        entity.forecast_eligible
        and truth_eligible
        and entity.predicted is not None
        and truth_value is not None
    ):
        abs_error = abs(entity.predicted - truth_value)
        if quantity == QUANTITY_PRECIP_OCCURRENCE:
            occurrence_outcome = classify_occurrence_outcome(
                entity.predicted, truth_value
            )
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             forecast_exclusion_reason, covered_hours, realized_contributors,
             truth_value, truth_eligible, truth_exclusion_reason,
             truth_covered_hours, truth_wet_hours, truth_dry_hours,
             abs_error, occurrence_outcome)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id, snapshot_local_date, lead, variable, quantity,
                    entity_type, entity_key) DO NOTHING
        """,
        (
            cfg.run_id,
            snapshot_local_date,
            target_local_date,
            lead,
            variable,
            quantity,
            entity.entity_type,
            entity.entity_key,
            entity.predicted,
            1 if entity.forecast_eligible else 0,
            entity.forecast_exclusion_reason,
            entity.covered_hours,
            entity.realized_contributors,
            source["truth_value"],
            source["truth_eligible"],
            source["truth_exclusion_reason"],
            source["truth_covered_hours"],
            source["truth_wet_hours"],
            source["truth_dry_hours"],
            abs_error,
            occurrence_outcome,
        ),
    )


def simulate_baseline_day(
    conn: sqlite3.Connection,
    cfg: RunConfig,
    snapshot_local_date: str,
    rosters: dict[tuple[str, int, str], list[int]],
) -> None:
    """Pass 2 (§7 step 4): the all-feed-mean baseline for one snapshot day.

    ``rosters`` is the resolved headline roster per ``(variable, lead,
    quantity)`` cell; a cell absent from it resolved to no floor-clearing
    feed and gets NO baseline row. The product path is re-run per cell (it
    is not a post-hoc average of pass-1 per-feed rows) and only the row for
    the cell's own quantity is persisted, because
    ``_entities_for_selection`` emits every quantity of the variable from
    one call and the insert discards duplicates rather than overwriting.
    """
    local_date = date.fromisoformat(snapshot_local_date)
    snapshot_utc = resolve_snapshot_utc(cfg.timezone, local_date, cfg.wall_clock)
    as_of = isoformat_utc(snapshot_utc)
    samples = _snapshot_samples(conn, cfg, local_date, as_of)
    grouped = _group_samples(samples, timezone=cfg.timezone, now=snapshot_utc)
    for lead in range(SIM_DAY_COUNT):
        target_iso = (local_date + timedelta(days=lead)).isoformat()
        for variable in SIM_VARIABLES:
            feeds_samples = grouped.get(variable, {}).get(lead, {})
            for quantity in VARIABLE_QUANTITIES[variable]:
                feed_ids = rosters.get((variable, lead, quantity))
                if not feed_ids:
                    continue
                built = _entities_for_selection(
                    entity_type="baseline_all_feed_mean",
                    entity_key="all_feed_mean",
                    variable=variable,
                    feed_ids=feed_ids,
                    feeds_samples=feeds_samples,
                    timezone=cfg.timezone,
                    target_date=local_date + timedelta(days=lead),
                    rain_threshold_mm=cfg.rain_threshold_mm,
                )
                source = _pass1_truth_source(
                    conn,
                    cfg,
                    snapshot_local_date=snapshot_local_date,
                    target_local_date=target_iso,
                    lead=lead,
                    variable=variable,
                    quantity=quantity,
                )
                if source is None:
                    continue
                _insert_baseline_evidence(
                    conn,
                    cfg,
                    snapshot_local_date=snapshot_local_date,
                    target_local_date=target_iso,
                    lead=lead,
                    variable=variable,
                    quantity=quantity,
                    entity=built[quantity],
                    source=source,
                )


def simulate_snapshot_day(
    conn: sqlite3.Connection, cfg: RunConfig, snapshot_local_date: str
) -> None:
    """Emit one snapshot day's full evidence set inside the caller's txn."""
    local_date = date.fromisoformat(snapshot_local_date)
    snapshot_utc = resolve_snapshot_utc(cfg.timezone, local_date, cfg.wall_clock)
    as_of = isoformat_utc(snapshot_utc)
    bounds = local_day_bounds(local_date, cfg.timezone)
    since_valid_at = isoformat_utc(bounds.start_utc)
    samples = _snapshot_samples(conn, cfg, local_date, as_of)
    null_availability = count_null_availability_samples(
        conn, site_id=cfg.site_id, since_valid_at=since_valid_at
    )
    grouped = _group_samples(samples, timezone=cfg.timezone, now=snapshot_utc)
    persistence = _persistence_predictions(conn, cfg, as_of, snapshot_local_date)
    knowable_through = latest_knowable_target(cfg.timezone, snapshot_utc).isoformat()
    cutoff = window_cutoff(cfg.window_days, snapshot_utc)

    rank_cache: dict[tuple[str, int], dict[int, LeaderboardRow]] = {}
    exclusions: dict[str, dict[str, int]] = {}
    daily_rank_cache: dict[str, list[int]] = {}
    truth_cache: dict[str, dict[str, sqlite3.Row]] = {}

    for lead in range(SIM_DAY_COUNT):
        target_date = local_date + timedelta(days=lead)
        target_iso = target_date.isoformat()
        if target_iso not in truth_cache:
            truth_cache[target_iso] = _truth_rows(conn, cfg, target_iso)
        truth_by_quantity = truth_cache[target_iso]
        for variable in SIM_VARIABLES:
            feeds_samples = grouped.get(variable, {}).get(lead, {})
            candidates: list[CellCandidate] = []
            for feed in cfg.roster:
                feed_samples = feeds_samples.get(feed.feed_id, [])
                if not feed_samples:
                    continue
                rep = representative_day_ahead(
                    [
                        issue_day_ahead(s.issued_at, s.valid_at, cfg.timezone)
                        for s in feed_samples
                    ]
                )
                key = (variable, rep)
                if key not in rank_cache:
                    # Obligations 1+3: as-of ranking, every declared_*
                    # explicit, restricted to the pinned roster below.
                    rank_cache[key] = forecast_ranking(
                        conn,
                        site_id=cfg.site_id,
                        variable=variable,
                        day_ahead=rep,
                        as_of=as_of,
                        declared_min_n=cfg.min_n,
                        declared_window_days=cfg.window_days,
                    )
                    context_key = f"{variable}:{rep}"
                    if context_key not in exclusions:
                        exclusions[context_key] = pair_knowability_exclusions(
                            conn,
                            site_id=cfg.site_id,
                            variable=variable,
                            day_ahead=rep,
                            as_of=as_of,
                            window_cutoff=cutoff,
                        ).as_reasons()
                row = rank_cache[key].get(feed.feed_id)
                candidates.append(
                    CellCandidate(
                        feed_id=feed.feed_id,
                        source=feed.source,
                        model=feed.model,
                        confident=row.confident if row is not None else False,
                        skill_score=row.skill_score if row is not None else None,
                        pair_n=row.n if row is not None else 0,
                        mae=row.mae if row is not None else None,
                        future_sample_count=len(feed_samples),
                        covered_hours=covered_hours(s.valid_at for s in feed_samples),
                    )
                )

            entities: list[dict[str, _Entity]] = []
            for depth in SIM_DEPTHS:
                selection = select_cell_feeds(candidates, blend_depth=depth)
                entities.append(
                    _entities_for_selection(
                        entity_type="depth",
                        entity_key=str(depth),
                        variable=variable,
                        feed_ids=[c.feed_id for c in selection.feeds],
                        feeds_samples=feeds_samples,
                        timezone=cfg.timezone,
                        target_date=target_date,
                        rain_threshold_mm=cfg.rain_threshold_mm,
                    )
                )
            for feed in cfg.roster:
                entities.append(
                    _entities_for_selection(
                        entity_type="feed",
                        entity_key=str(feed.feed_id),
                        variable=variable,
                        feed_ids=[feed.feed_id],
                        feeds_samples=feeds_samples,
                        timezone=cfg.timezone,
                        target_date=target_date,
                        rain_threshold_mm=cfg.rain_threshold_mm,
                    )
                )
            # The all-feed-mean baseline is NOT built here: §9.2 defines it
            # over the frozen headline roster, which is a period-wide
            # availability statistic and cannot be known inside pass 1. It is
            # built by `simulate_baseline_day` in the `baseline` phase (§7).
            for quantity in VARIABLE_QUANTITIES[variable]:
                value = persistence[quantity]
                entities.append(
                    {
                        quantity: _Entity(
                            entity_type="baseline_persistence",
                            entity_key="persistence",
                            predicted=value,
                            forecast_eligible=value is not None,
                            forecast_exclusion_reason=None
                            if value is not None
                            else EXCLUDE_NO_PRIOR_TRUTH,
                            covered_hours=None,
                            realized_contributors=None,
                        )
                    }
                )
            if variable == "precip":
                entities.append(
                    {
                        QUANTITY_PRECIP_OCCURRENCE: _Entity(
                            entity_type="baseline_always_dry",
                            entity_key="always_dry",
                            predicted=0.0,
                            forecast_eligible=True,
                            forecast_exclusion_reason=None,
                            covered_hours=None,
                            realized_contributors=None,
                        )
                    }
                )
            for quantity in VARIABLE_QUANTITIES[variable]:
                if quantity not in daily_rank_cache:
                    daily_rank_cache[quantity] = _daily_rank_order(
                        conn,
                        cfg,
                        quantity=quantity,
                        knowable_through=knowable_through,
                        as_of=as_of,
                    )
                order = daily_rank_cache[quantity]
                for depth in SIM_DEPTHS:
                    if len(order) < depth:
                        entities.append(
                            {
                                quantity: _Entity(
                                    entity_type="daily_rank_depth",
                                    entity_key=str(depth),
                                    predicted=None,
                                    forecast_eligible=False,
                                    forecast_exclusion_reason=(
                                        EXCLUDE_INSUFFICIENT_RANK_HISTORY
                                    ),
                                    covered_hours=None,
                                    realized_contributors=None,
                                )
                            }
                        )
                        continue
                    built = _entities_for_selection(
                        entity_type="daily_rank_depth",
                        entity_key=str(depth),
                        variable=variable,
                        feed_ids=order[:depth],
                        feeds_samples=feeds_samples,
                        timezone=cfg.timezone,
                        target_date=target_date,
                        rain_threshold_mm=cfg.rain_threshold_mm,
                    )
                    entities.append({quantity: built[quantity]})

            for per_quantity in entities:
                for quantity, entity in per_quantity.items():
                    _insert_evidence(
                        conn,
                        cfg,
                        snapshot_local_date=snapshot_local_date,
                        target_local_date=target_iso,
                        lead=lead,
                        variable=variable,
                        quantity=quantity,
                        entity=entity,
                        truth=truth_by_quantity.get(quantity),
                    )

    conn.execute(
        """
        INSERT INTO verification_day_context
            (run_id, snapshot_local_date, snapshot_utc,
             knowability_exclusions, null_availability_samples)
        VALUES (?,?,?,?,?)
        ON CONFLICT(run_id, snapshot_local_date) DO NOTHING
        """,
        (
            cfg.run_id,
            snapshot_local_date,
            as_of,
            json.dumps(exclusions, separators=(",", ":"), sort_keys=True),
            null_availability,
        ),
    )


__all__ = [
    "EXCLUDE_INSUFFICIENT_RANK_HISTORY",
    "EXCLUDE_NO_PRIOR_TRUTH",
    "EXCLUDE_NO_SAMPLES",
    "EXCLUDE_TRUTH_MISSING",
    "SIM_DAY_COUNT",
    "SIM_DEPTHS",
    "SIM_VARIABLES",
    "classify_occurrence_outcome",
    "latest_knowable_target",
    "simulate_baseline_day",
    "simulate_snapshot_day",
]

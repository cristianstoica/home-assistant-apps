"""Run aggregation, common-core resolution, and verdict persistence (§8/§11).

The aggregate phase resolves each cell's headline roster and strict common
core ONCE, persists the resolution in the run row's ``aggregate_state``
JSON, and writes ``verification_results``. The bootstrap-input builder then
reads that SAME resolution back — the results table and the §12 decision
can never disagree about which days were common.

Cell = (variable, lead, quantity). Headline roster per §8: the four blend
depths, the baselines, and every pinned roster feed at/above the
availability floor; the strict common core is the truth-eligible dates on
which EVERY roster member produced an eligible forecast. Below-floor feeds
and the daily-rank diagnostics score pairwise against the incumbent with
``headline = 0`` — visible, never decision inputs.
"""

from __future__ import annotations

import json
import sqlite3
from typing import cast

from wxverify.verification.coverage import (
    QUANTITY_PRECIP_OCCURRENCE,
    VARIABLE_QUANTITIES,
)
from wxverify.verification.decision import (
    DECISION_LEADS,
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    TempLead,
    VariableInputs,
    Verdict,
)
from wxverify.verification.methodology import (
    CONTINUOUS_BASELINES,
    OCCURRENCE_BASELINES,
    ROSTER_AVAILABILITY_FLOOR,
)
from wxverify.verification.runs import RunConfig, publish_run
from wxverify.verification.simulate import SIM_DAY_COUNT, SIM_DEPTHS, SIM_VARIABLES
from wxverify.verification.stats import (
    Contingency,
    bias,
    ets,
    mae,
    rmse,
)

EntityId = tuple[str, str]
_CellRows = dict[EntityId, dict[str, sqlite3.Row]]

#: Top-level ``aggregate_state`` key holding the PASS-1 (availability-only)
#: resolution (§7 step 3). It carries ``members`` and ``availability`` only —
#: never ``common_dates`` — so the provisional core computed before the
#: all-feed-mean baseline exists has no slot to occupy and cannot reach a
#: ``verification_results`` row. ``_common_dates`` reads the PER-CELL keys,
#: which this one sits beside and never collides with.
PASS1_ROSTER_KEY = "pass1_roster"


def _cell_key(variable: str, lead: int, quantity: str) -> str:
    return f"{variable}|{lead}|{quantity}"


def _load_cell(
    conn: sqlite3.Connection, run_id: int, variable: str, lead: int, quantity: str
) -> _CellRows:
    rows = conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND variable = ? AND lead = ? AND quantity = ?
        """,
        (run_id, variable, lead, quantity),
    ).fetchall()
    out: _CellRows = {}
    for row in rows:
        entity = (str(row["entity_type"]), str(row["entity_key"]))
        out.setdefault(entity, {})[str(row["target_local_date"])] = row
    return out


def _eligible_dates(rows: dict[str, sqlite3.Row]) -> set[str]:
    return {
        d
        for d, row in rows.items()
        if bool(row["forecast_eligible"])
        and bool(row["truth_eligible"])
        and row["predicted"] is not None
        and row["truth_value"] is not None
    }


def _truth_dates(cell: _CellRows) -> set[str]:
    out: set[str] = set()
    for rows in cell.values():
        for d, row in rows.items():
            if bool(row["truth_eligible"]):
                out.add(d)
    return out


def _resolve_cell(
    cell: _CellRows, cfg: RunConfig, quantity: str
) -> tuple[list[EntityId], list[str], dict[EntityId, float]]:
    """Headline roster, strict common core, and per-entity availability."""
    truth = _truth_dates(cell)
    baselines = (
        OCCURRENCE_BASELINES
        if quantity == QUANTITY_PRECIP_OCCURRENCE
        else CONTINUOUS_BASELINES
    )
    members: list[EntityId] = [("depth", str(d)) for d in SIM_DEPTHS]
    for baseline in baselines:
        key = baseline.removeprefix("baseline_")
        if (baseline, key) in cell:
            members.append((baseline, key))
    availability: dict[EntityId, float] = {}
    for feed in cfg.roster:
        entity = ("feed", str(feed.feed_id))
        eligible = _eligible_dates(cell.get(entity, {}))
        rate = len(eligible & truth) / len(truth) if truth else 0.0
        availability[entity] = rate
        if rate >= ROSTER_AVAILABILITY_FLOOR:
            members.append(entity)
    common = set(truth)
    for member in members:
        common &= _eligible_dates(cell.get(member, {}))
    return members, sorted(common), availability


def _continuous_metrics(
    rows: dict[str, sqlite3.Row], dates: list[str]
) -> tuple[float, float, float] | None:
    # §15/F-3: an out-of-range (depth-5/6) incumbent has no simulated rows,
    # so a date can be absent from `rows` even on a headline core — the
    # comparison is then unmeasurable and must fail closed to None, never
    # crash or fabricate a zero.
    if not dates or any(d not in rows for d in dates):
        return None
    errors = [
        float(rows[d]["predicted"]) - float(rows[d]["truth_value"]) for d in dates
    ]
    return mae(errors), bias(errors), rmse(errors)


def _contingency(rows: dict[str, sqlite3.Row], dates: list[str]) -> Contingency | None:
    # NB-1: the SAME fail-closed guard as _continuous_metrics, not a quiet
    # skip. A partially covered core would otherwise be counted over fewer
    # days than the `common_days` written beside it, so the counts would
    # describe a different sample than the column claims — and an empty core
    # would publish 0/0/0/0 where §16 requires null.
    if not dates or any(d not in rows for d in dates):
        return None
    table = Contingency()
    for d in dates:
        outcome = rows[d]["occurrence_outcome"]
        if outcome is not None:
            table = table.add(str(outcome))
    return table


def _pairwise_core(cell: _CellRows, entity: EntityId, incumbent: EntityId) -> list[str]:
    return sorted(
        _eligible_dates(cell.get(entity, {})) & _eligible_dates(cell.get(incumbent, {}))
    )


def _cells(cfg: RunConfig) -> list[tuple[str, int, str]]:
    return [
        (variable, lead, quantity)
        for variable in SIM_VARIABLES
        for lead in range(SIM_DAY_COUNT)
        for quantity in VARIABLE_QUANTITIES[variable]
    ]


def _stored_aggregate_state(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
    """The run's ``aggregate_state``, or ``{}`` when NULL/absent/malformed."""
    row = conn.execute(
        "SELECT aggregate_state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None or row["aggregate_state"] is None:
        return {}
    try:
        parsed: object = json.loads(str(row["aggregate_state"]))
    except ValueError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return cast(dict[str, object], parsed)


def _write_aggregate_state(
    conn: sqlite3.Connection, run_id: int, state: dict[str, object]
) -> None:
    conn.execute(
        "UPDATE verification_runs SET aggregate_state = ? WHERE id = ?",
        (json.dumps(state, separators=(",", ":"), sort_keys=True), run_id),
    )


def resolve_pass1_roster(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """§7 steps 2-3: availability-only resolution, persisted for pass 2.

    ``_resolve_cell``'s strict common core is DISCARDED here — at this point
    the all-feed-mean baseline rows do not exist, so that core ignores the
    baseline's coverage entirely and must never reach a result row.
    """
    state = _stored_aggregate_state(conn, cfg.run_id)
    roster: dict[str, object] = {}
    for variable, lead, quantity in _cells(cfg):
        cell = _load_cell(conn, cfg.run_id, variable, lead, quantity)
        members, _discarded_core, availability = _resolve_cell(cell, cfg, quantity)
        roster[_cell_key(variable, lead, quantity)] = {
            "members": [list(m) for m in members],
            "availability": {f"{t}|{k}": v for (t, k), v in availability.items()},
        }
    state[PASS1_ROSTER_KEY] = roster
    _write_aggregate_state(conn, cfg.run_id, state)


def pass1_baseline_feeds(
    conn: sqlite3.Connection, run_id: int
) -> dict[tuple[str, int, str], list[int]]:
    """Per-cell resolved feed roster the all-feed-mean baseline averages.

    Keyed by ``(variable, lead, quantity)``; a cell whose resolved roster
    holds no ``feed`` member is ABSENT from the mapping, so pass 2 writes no
    baseline row for it rather than a ``no_samples`` row that would collapse
    the strict common core.
    """
    roster = _stored_aggregate_state(conn, run_id).get(PASS1_ROSTER_KEY)
    out: dict[tuple[str, int, str], list[int]] = {}
    if not isinstance(roster, dict):
        return out
    for key, cell in cast(dict[str, object], roster).items():
        if not isinstance(cell, dict):
            continue
        parts = key.split("|")
        if len(parts) != 3 or not parts[1].isdecimal():
            continue
        members = cast(dict[str, object], cell).get("members")
        if not isinstance(members, list):
            continue
        feeds: list[int] = []
        for member in cast(list[object], members):
            pair = cast(list[object], member) if isinstance(member, list) else []
            if len(pair) == 2 and str(pair[0]) == "feed":
                feeds.append(int(str(pair[1])))
        if feeds:
            out[(parts[0], int(parts[1]), parts[2])] = feeds
    return out


def aggregate_run(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """Resolve every cell, write ``verification_results`` + aggregate_state."""
    # MERGE, never replace: `pass1_roster` (§7 step 3) already lives in this
    # column and a fresh dict would delete it on every run.
    state: dict[str, object] = _stored_aggregate_state(conn, cfg.run_id)
    for variable in SIM_VARIABLES:
        # §15: the incumbent is the variable's pinned effective depth.
        incumbent: EntityId = ("depth", str(cfg.incumbent_depth(variable)))
        for lead in range(SIM_DAY_COUNT):
            for quantity in VARIABLE_QUANTITIES[variable]:
                cell = _load_cell(conn, cfg.run_id, variable, lead, quantity)
                members, common, availability = _resolve_cell(cell, cfg, quantity)
                state[_cell_key(variable, lead, quantity)] = {
                    "members": [list(m) for m in members],
                    "common_dates": common,
                }
                member_set = set(members)
                for entity, rows in sorted(cell.items()):
                    headline = entity in member_set
                    dates = (
                        common if headline else _pairwise_core(cell, entity, incumbent)
                    )
                    _write_result(
                        conn,
                        cfg,
                        variable=variable,
                        lead=lead,
                        quantity=quantity,
                        entity=entity,
                        rows=rows,
                        incumbent_rows=cell.get(incumbent, {}),
                        dates=dates,
                        headline=headline,
                        availability=availability.get(entity),
                    )
    _write_aggregate_state(conn, cfg.run_id, state)


def _write_result(
    conn: sqlite3.Connection,
    cfg: RunConfig,
    *,
    variable: str,
    lead: int,
    quantity: str,
    entity: EntityId,
    rows: dict[str, sqlite3.Row],
    incumbent_rows: dict[str, sqlite3.Row],
    dates: list[str],
    headline: bool,
    availability: float | None,
) -> None:
    mae_v = bias_v = rmse_v = ets_v = None
    hits = misses = fas = cns = None
    delta: float | None = None
    if quantity == QUANTITY_PRECIP_OCCURRENCE:
        table = _contingency(rows, dates)
        if table is not None:
            hits, misses, fas, cns = (
                table.hits,
                table.misses,
                table.false_alarms,
                table.correct_negatives,
            )
            ets_v = ets(table)
        inc_table = _contingency(incumbent_rows, dates)
        inc_ets = None if inc_table is None else ets(inc_table)
        if ets_v is not None and inc_ets is not None:
            delta = ets_v - inc_ets
    else:
        metrics = _continuous_metrics(rows, dates)
        if metrics is not None:
            mae_v, bias_v, rmse_v = metrics
        inc = _continuous_metrics(incumbent_rows, dates)
        if metrics is not None and inc is not None and inc[0] != 0:
            delta = (inc[0] - metrics[0]) / inc[0]
    conn.execute(
        """
        INSERT INTO verification_results
            (run_id, variable, lead, quantity, entity_type, entity_key,
             headline, common_days, mae, bias, rmse, hits, misses,
             false_alarms, correct_negatives, ets, availability_rate,
             delta_vs_incumbent, detail)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id, variable, lead, quantity, entity_type, entity_key)
        DO NOTHING
        """,
        (
            cfg.run_id,
            variable,
            lead,
            quantity,
            entity[0],
            entity[1],
            1 if headline else 0,
            len(dates),
            mae_v,
            bias_v,
            rmse_v,
            hits,
            misses,
            fas,
            cns,
            ets_v,
            availability,
            delta,
            json.dumps({"dates_n": len(dates)}, separators=(",", ":")),
        ),
    )


#: Closed set of reasons a below-floor row carries no recommended-blend
#: comparison (§9). ``vs_recommended`` is ALWAYS an object, never JSON null —
#: a bare null cannot carry which of these applied.
PAIRWISE_UNAVAILABLE_REASONS = (
    "no_recommendation",
    "recommended_is_incumbent",
    "insufficient_shared_days",
)


def _unavailable_vs_recommended(reason: str) -> dict[str, object]:
    return {
        "available": False,
        "reason": reason,
        "recommended_entity": None,
        "dates_n": None,
        "mae": None,
        "ets": None,
        "delta_vs_recommended": None,
    }


def _recommended_entities(
    conn: sqlite3.Connection, run_id: int
) -> dict[str, EntityId | str]:
    """Per variable, the recommended entity — or the reason there is none.

    A ``str`` value is one of ``PAIRWISE_UNAVAILABLE_REASONS``; an
    :data:`EntityId` is the depth the run recommends.
    """
    out: dict[str, EntityId | str] = {}
    for row in conn.execute(
        """
        SELECT variable, outcome, recommended_depth, incumbent_depth
        FROM verification_verdicts WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall():
        variable = str(row["variable"])
        recommended = row["recommended_depth"]
        if str(row["outcome"]) != "recommend" or recommended is None:
            out[variable] = "no_recommendation"
        elif int(recommended) == int(row["incumbent_depth"]):
            out[variable] = "recommended_is_incumbent"
        else:
            out[variable] = ("depth", str(int(recommended)))
    return out


def _vs_recommended(
    cell: _CellRows,
    entity: EntityId,
    recommended: EntityId,
    *,
    quantity: str,
) -> dict[str, object]:
    """One below-floor entity's comparison against the recommended blend.

    Sample is the PAIRWISE core of this pair, never the strict common core;
    ``delta_vs_recommended`` uses the same sign convention
    ``delta_vs_incumbent`` already uses — positive means the below-floor
    entity is better.
    """
    dates = _pairwise_core(cell, entity, recommended)
    if not dates:
        return _unavailable_vs_recommended("insufficient_shared_days")
    entity_ref = {"entity_type": recommended[0], "entity_key": recommended[1]}
    mae_v: float | None = None
    ets_v: float | None = None
    delta: float | None = None
    if quantity == QUANTITY_PRECIP_OCCURRENCE:
        table = _contingency(cell.get(entity, {}), dates)
        rec_table = _contingency(cell.get(recommended, {}), dates)
        ets_v = None if table is None else ets(table)
        rec_ets = None if rec_table is None else ets(rec_table)
        if ets_v is not None and rec_ets is not None:
            delta = ets_v - rec_ets
    else:
        metrics = _continuous_metrics(cell.get(entity, {}), dates)
        rec_metrics = _continuous_metrics(cell.get(recommended, {}), dates)
        if metrics is not None:
            mae_v = metrics[0]
        if metrics is not None and rec_metrics is not None and rec_metrics[0] != 0:
            delta = (rec_metrics[0] - metrics[0]) / rec_metrics[0]
    return {
        "available": True,
        "reason": None,
        "recommended_entity": entity_ref,
        "dates_n": len(dates),
        "mae": mae_v,
        "ets": ets_v,
        "delta_vs_recommended": delta,
    }


def write_pairwise_comparisons(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """§8.4/§9: below-floor rows gain a comparison vs the recommended blend.

    Runs AFTER the verdicts exist (the ``pairwise`` phase, between
    ``bootstrap`` and ``publish``) and writes with an UPDATE: ``_write_result``
    carries ``ON CONFLICT … DO NOTHING``, so a re-INSERT would be a silent
    no-op. Re-running over the same run produces byte-identical ``detail``.
    """
    recommended_by_variable = _recommended_entities(conn, cfg.run_id)
    rows = conn.execute(
        """
        SELECT id, variable, lead, quantity, entity_type, entity_key, detail
        FROM verification_results
        WHERE run_id = ? AND headline = 0
        ORDER BY id
        """,
        (cfg.run_id,),
    ).fetchall()
    cells: dict[tuple[str, int, str], _CellRows] = {}
    for row in rows:
        variable = str(row["variable"])
        lead = int(row["lead"])
        quantity = str(row["quantity"])
        recommended = recommended_by_variable.get(variable, "no_recommendation")
        if isinstance(recommended, str):
            comparison = _unavailable_vs_recommended(recommended)
        else:
            key = (variable, lead, quantity)
            if key not in cells:
                cells[key] = _load_cell(conn, cfg.run_id, variable, lead, quantity)
            comparison = _vs_recommended(
                cells[key],
                (str(row["entity_type"]), str(row["entity_key"])),
                recommended,
                quantity=quantity,
            )
        detail = _parse_detail(row["detail"])
        detail["vs_recommended"] = comparison
        conn.execute(
            "UPDATE verification_results SET detail = ? WHERE id = ?",
            (json.dumps(detail, separators=(",", ":")), int(row["id"])),
        )


def _parse_detail(raw: object) -> dict[str, object]:
    """The row's existing ``detail`` object, or ``{}`` when absent/malformed."""
    if raw is None:
        return {}
    try:
        parsed: object = json.loads(str(raw))
    except ValueError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return cast(dict[str, object], parsed)


def _aggregate_state(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
    row = conn.execute(
        "SELECT aggregate_state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None or row["aggregate_state"] is None:
        raise RuntimeError(f"verification run {run_id} has no aggregate state")
    parsed: object = json.loads(str(row["aggregate_state"]))
    if not isinstance(parsed, dict):
        raise RuntimeError(f"verification run {run_id} aggregate state malformed")
    return cast(dict[str, object], parsed)


def _common_dates(state: dict[str, object], key: str) -> list[str]:
    cell = state.get(key)
    if not isinstance(cell, dict):
        return []
    dates = cast(dict[str, object], cell).get("common_dates")
    if not isinstance(dates, list):
        return []
    return [str(d) for d in cast(list[object], dates)]


def stored_cell_resolution(
    conn: sqlite3.Connection, run_id: int, variable: str, lead: int, quantity: str
) -> tuple[list[EntityId], list[str]]:
    """The cell's PERSISTED resolved members and strict common core (§7).

    Read-time derivations (§10, §14a) must score the same resolution the
    headline used, so they read it back from ``aggregate_state`` rather than
    re-deriving it — a re-derivation would silently disagree whenever the
    stored roster and the live config differ. Returns ``([], [])`` for a
    cell the aggregate phase never wrote.
    """
    state = _stored_aggregate_state(conn, run_id)
    key = _cell_key(variable, lead, quantity)
    members: list[EntityId] = []
    cell = state.get(key)
    if isinstance(cell, dict):
        raw = cast(dict[str, object], cell).get("members")
        if isinstance(raw, list):
            for member in cast(list[object], raw):
                if isinstance(member, list):
                    pair = cast(list[object], member)
                    if len(pair) == 2:
                        members.append((str(pair[0]), str(pair[1])))
    return members, _common_dates(state, key)


def _abs_series(
    cell: _CellRows, a: EntityId, b: EntityId, dates: list[str]
) -> ContinuousLead:
    rows_a = cell.get(a, {})
    rows_b = cell.get(b, {})
    out: ContinuousLead = {}
    for d in dates:
        ra, rb = rows_a.get(d), rows_b.get(d)
        if (
            ra is not None
            and rb is not None
            and ra["abs_error"] is not None
            and rb["abs_error"] is not None
        ):
            out[d] = (float(ra["abs_error"]), float(rb["abs_error"]))
    return out


def _class_series(
    cell: _CellRows, a: EntityId, b: EntityId, dates: list[str]
) -> OccurrenceLead:
    rows_a = cell.get(a, {})
    rows_b = cell.get(b, {})
    out: OccurrenceLead = {}
    for d in dates:
        ra, rb = rows_a.get(d), rows_b.get(d)
        if (
            ra is not None
            and rb is not None
            and ra["occurrence_outcome"] is not None
            and rb["occurrence_outcome"] is not None
        ):
            out[d] = (str(ra["occurrence_outcome"]), str(rb["occurrence_outcome"]))
    return out


def prepare_bootstrap_inputs(
    conn: sqlite3.Connection, cfg: RunConfig
) -> list[VariableInputs]:
    """Rebuild the §12 paired series from evidence + the persisted cell
    resolution — read-only; safe on a read connection."""
    state = _aggregate_state(conn, cfg.run_id)
    out: list[VariableInputs] = []
    for variable in SIM_VARIABLES:
        incumbent_depth = cfg.incumbent_depth(variable)
        if incumbent_depth not in SIM_DEPTHS:
            # §15/F-3: a depth-5/6 incumbent has no simulated entity, so
            # every comparison would be vacuous. preskipped_verdicts()
            # supplies the explicit 'skipped' verdict for this variable.
            continue
        incumbent: EntityId = ("depth", str(incumbent_depth))
        cells: dict[tuple[int, str], _CellRows] = {}
        cores: dict[tuple[int, str], list[str]] = {}
        for lead in DECISION_LEADS:
            for quantity in VARIABLE_QUANTITIES[variable]:
                cells[(lead, quantity)] = _load_cell(
                    conn, cfg.run_id, variable, lead, quantity
                )
                cores[(lead, quantity)] = _common_dates(
                    state, _cell_key(variable, lead, quantity)
                )
        candidates: list[CandidateSeries] = []
        for depth in SIM_DEPTHS:
            if depth == incumbent_depth:
                continue
            entity: EntityId = ("depth", str(depth))
            continuous: dict[str, dict[int, ContinuousLead]] = {}
            temp: dict[int, TempLead] = {}
            occurrence: dict[int, OccurrenceLead] = {}
            baseline_continuous: dict[str, dict[str, dict[int, ContinuousLead]]] = {}
            baseline_occurrence: dict[str, dict[int, OccurrenceLead]] = {}
            for lead in DECISION_LEADS:
                for quantity in VARIABLE_QUANTITIES[variable]:
                    cell = cells[(lead, quantity)]
                    dates = cores[(lead, quantity)]
                    if quantity == QUANTITY_PRECIP_OCCURRENCE:
                        series = _class_series(cell, entity, incumbent, dates)
                        if series:
                            occurrence[lead] = series
                        for baseline in OCCURRENCE_BASELINES:
                            base: EntityId = (
                                baseline,
                                baseline.removeprefix("baseline_"),
                            )
                            b_series = _class_series(cell, entity, base, dates)
                            if b_series:
                                baseline_occurrence.setdefault(baseline, {})[lead] = (
                                    b_series
                                )
                    else:
                        series_c = _abs_series(cell, entity, incumbent, dates)
                        if series_c:
                            continuous.setdefault(quantity, {})[lead] = series_c
                        for baseline in CONTINUOUS_BASELINES:
                            base = (
                                baseline,
                                baseline.removeprefix("baseline_"),
                            )
                            b_series_c = _abs_series(cell, entity, base, dates)
                            if b_series_c:
                                baseline_continuous.setdefault(baseline, {}).setdefault(
                                    quantity, {}
                                )[lead] = b_series_c
            if variable == "temperature":
                for lead in DECISION_LEADS:
                    high = continuous.get("temperature_high", {}).get(lead, {})
                    low = continuous.get("temperature_low", {}).get(lead, {})
                    joint: TempLead = {
                        d: (high[d], low[d]) for d in high.keys() & low.keys()
                    }
                    if joint:
                        temp[lead] = joint
            candidates.append(
                CandidateSeries(
                    key=str(depth),
                    continuous=continuous,
                    temp=temp,
                    occurrence=occurrence,
                    baseline_continuous=baseline_continuous,
                    baseline_occurrence=baseline_occurrence,
                )
            )
        out.append(
            VariableInputs(
                variable=variable,
                incumbent_key=str(incumbent_depth),
                candidates=tuple(candidates),
            )
        )
    return out


def preskipped_verdicts(cfg: RunConfig) -> list[Verdict]:
    """Explicit 'skipped' verdicts for incumbents outside SIM_DEPTHS (§15).

    A depth-5/6 incumbent is a valid setting but has no simulated entity —
    every headline comparison would be vacuously insufficient. Rather than
    silently publishing an all-insufficient run, the variable is skipped
    with an explicit machine-readable reason; publish integrity still
    requires one verdict per variable, and these rows satisfy it.
    """
    out: list[Verdict] = []
    for variable in SIM_VARIABLES:
        depth = cfg.incumbent_depth(variable)
        if depth not in SIM_DEPTHS:
            out.append(
                Verdict(
                    variable=variable,
                    outcome="skipped",
                    recommended_key=None,
                    detail={
                        "incumbent": str(depth),
                        "reason": "incumbent_depth_out_of_simulated_range",
                        "simulated_depths": list(SIM_DEPTHS),
                    },
                )
            )
    return out


def finalize_verdicts(
    conn: sqlite3.Connection, cfg: RunConfig, verdicts: list[Verdict]
) -> None:
    """Persist the three per-variable verdicts (one write transaction)."""
    for verdict in verdicts:
        recommended: int | None = None
        if verdict.recommended_key is not None and verdict.recommended_key.isdecimal():
            recommended = int(verdict.recommended_key)
        conn.execute(
            """
            INSERT INTO verification_verdicts
                (run_id, variable, outcome, recommended_depth,
                 incumbent_depth, tested_family)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(run_id, variable) DO NOTHING
            """,
            (
                cfg.run_id,
                verdict.variable,
                verdict.outcome,
                recommended,
                cfg.incumbent_depth(verdict.variable),
                json.dumps(verdict.detail, separators=(",", ":"), sort_keys=True),
            ),
        )


def publish_verified_run(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """Integrity-check then atomically publish the run (§14)."""
    verdicts = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_verdicts WHERE run_id = ?",
        (cfg.run_id,),
    ).fetchone()
    results = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_results WHERE run_id = ?",
        (cfg.run_id,),
    ).fetchone()
    if int(verdicts["n"]) != len(SIM_VARIABLES) or int(results["n"]) == 0:
        raise RuntimeError(
            f"verification run {cfg.run_id} failed integrity: "
            f"verdicts={int(verdicts['n'])} results={int(results['n'])}"
        )
    publish_run(conn, cfg.site_id, cfg.run_id)

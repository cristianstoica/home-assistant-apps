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
from wxverify.verification.methodology import ROSTER_AVAILABILITY_FLOOR
from wxverify.verification.runs import RunConfig, publish_run
from wxverify.verification.simulate import SIM_DAY_COUNT, SIM_DEPTHS, SIM_VARIABLES
from wxverify.verification.stats import (
    Contingency,
    bias,
    ets,
    mae,
    rmse,
)

#: Baseline entity types per quantity kind.
_CONTINUOUS_BASELINES = ("baseline_persistence", "baseline_all_feed_mean")
_OCCURRENCE_BASELINES = (
    "baseline_persistence",
    "baseline_all_feed_mean",
    "baseline_always_dry",
)

EntityId = tuple[str, str]
_CellRows = dict[EntityId, dict[str, sqlite3.Row]]


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
        _OCCURRENCE_BASELINES
        if quantity == QUANTITY_PRECIP_OCCURRENCE
        else _CONTINUOUS_BASELINES
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


def aggregate_run(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """Resolve every cell, write ``verification_results`` + aggregate_state."""
    state: dict[str, object] = {}
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
    conn.execute(
        "UPDATE verification_runs SET aggregate_state = ? WHERE id = ?",
        (json.dumps(state, separators=(",", ":"), sort_keys=True), cfg.run_id),
    )


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
                        for baseline in _OCCURRENCE_BASELINES:
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
                        for baseline in _CONTINUOUS_BASELINES:
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

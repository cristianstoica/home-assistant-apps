"""§10 (W7): the daily-rank redesign conclusion, derived at read time.

0.11.0 §10 obliges the release to SAY SO when the daily-rank diagnostic
outperforms every incumbent-ranking depth. The diagnostic is simulated and
scored but nothing compared it, so the conclusion could not be stated. This
module states it, from retained evidence, with no persistence: one helper
serves both ``GET /api/verification/runs/{run_id}/verdicts`` and the
``/verification`` page, so the two cannot drift.

Three properties of the rule are load-bearing and are implemented here
literally:

* The sample is the PAIRWISE CORE (§16.2), never the strict common core.
  ``daily_rank_depth`` entities are not cell members, so the strict core
  contains dates on which they carry ``predicted = None``;
  ``_continuous_metrics`` would then raise on ``float(None)`` and
  ``_contingency`` would silently score a smaller, entity-favouring day set.
* The quantifier is UNIVERSAL over incumbent depths and EXISTENTIAL over the
  daily-rank family: one weak deep-blend daily-rank entity must not suppress
  a decisive shallow-blend result.
* The comparison is a MATERIALITY statement, not a confidence claim — the
  practical floors, no bootstrap. The recommendation is unchanged by it;
  a ranking basis is not enactable in 0.11.x.
"""

from __future__ import annotations

import sqlite3
from typing import Literal, cast, get_args

from wxverify.verification.coverage import (
    QUANTITY_PRECIP_OCCURRENCE,
    VARIABLE_QUANTITIES,
)
from wxverify.verification.decision import DECISION_LEADS, required_lead_agreement
from wxverify.verification.methodology import (
    ADEQUATE_LEAD_MIN_DAYS,
    OCCURRENCE_MIN_DRY_DAYS,
    OCCURRENCE_MIN_WET_DAYS,
    PRACTICAL_FLOOR_ETS,
    PRACTICAL_FLOOR_RELATIVE_MAE,
)
from wxverify.verification.simulate import (
    EXCLUDE_INSUFFICIENT_RANK_HISTORY,
    EXCLUDE_NO_SAMPLES,
    SIM_DEPTHS,
    SIM_VARIABLES,
)
from wxverify.verification.stats import Contingency, ets

DAILY_RANK_ENTITY_TYPE = "daily_rank_depth"

#: The variable-level conclusion vocabulary, in the precedence order §10
#: change 6 states. All five are new in this release (§16.1). The type is the
#: single definition: ``_fold`` returns it, so every literal at its return
#: sites is checked against this vocabulary and a sixth value or a typo is a
#: type error, not a silently-diverging string.
RankingConclusion = Literal[
    "not_assessable",
    "indicated_all_depths",
    "indicated",
    "indicated_on_subset",
    "not_indicated",
]
RANKING_CONCLUSIONS: tuple[RankingConclusion, ...] = get_args(RankingConclusion)

#: Why a (daily-rank entity, incumbent depth) pair was never adequately
#: compared. The two simulator exclusions are reported verbatim; a pair that
#: exists on both sides but is too thin at every lead is ``thin_leads``.
EXCLUSION_THIN_LEADS = "thin_leads"

_EntityId = tuple[str, str]
_Rows = dict[str, sqlite3.Row]
_Cell = dict[_EntityId, _Rows]


def _eligible_dates(rows: _Rows) -> set[str]:
    return {
        d
        for d, row in rows.items()
        if bool(row["forecast_eligible"])
        and bool(row["truth_eligible"])
        and row["predicted"] is not None
        and row["truth_value"] is not None
    }


def _pairwise_dates(cell: _Cell, left: _EntityId, right: _EntityId) -> list[str]:
    """§16.2 pairwise core — the ONLY admissible sample for this comparison."""
    return sorted(
        _eligible_dates(cell.get(left, {})) & _eligible_dates(cell.get(right, {}))
    )


def _mae(rows: _Rows, dates: list[str]) -> float | None:
    if not dates:
        return None
    total = 0.0
    for d in dates:
        row = rows.get(d)
        if row is None or row["predicted"] is None or row["truth_value"] is None:
            return None
        total += abs(float(row["predicted"]) - float(row["truth_value"]))
    return total / len(dates)


def _table(rows: _Rows, dates: list[str]) -> Contingency | None:
    table = Contingency()
    for d in dates:
        row = rows.get(d)
        if row is None or row["occurrence_outcome"] is None:
            return None
        table = table.add(str(row["occurrence_outcome"]))
    return table


def _wet_dry(table: Contingency) -> tuple[int, int]:
    return table.hits + table.misses, table.false_alarms + table.correct_negatives


def _load_cells(
    conn: sqlite3.Connection, run_id: int, variable: str
) -> dict[tuple[int, str], _Cell]:
    rows = conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND variable = ?
        """,
        (run_id, variable),
    ).fetchall()
    cells: dict[tuple[int, str], _Cell] = {}
    for row in rows:
        key = (int(row["lead"]), str(row["quantity"]))
        entity: _EntityId = (str(row["entity_type"]), str(row["entity_key"]))
        cells.setdefault(key, {}).setdefault(entity, {})[
            str(row["target_local_date"])
        ] = row
    return cells


def _quantity_outcome(
    cell: _Cell, rank: _EntityId, depth: _EntityId, quantity: str
) -> dict[str, object]:
    """One quantity at one lead: adequacy, materiality, and the two metrics.

    The metrics travel with the verdict because they are the only auditable
    record of WHICH sample was scored — an ETS or MAE taken over the strict
    common core is a different number, and without it the sample discipline
    of §16.2 is unfalsifiable from the outside.
    """
    dates = _pairwise_dates(cell, rank, depth)
    out: dict[str, object] = {
        "adequate": False,
        "material": False,
        "dates_n": len(dates),
        "rank": None,
        "incumbent": None,
    }
    if len(dates) < ADEQUATE_LEAD_MIN_DAYS:
        return out
    rank_rows = cell.get(rank, {})
    depth_rows = cell.get(depth, {})
    if quantity == QUANTITY_PRECIP_OCCURRENCE:
        rank_table = _table(rank_rows, dates)
        depth_table = _table(depth_rows, dates)
        if rank_table is None or depth_table is None:
            return out
        wet, dry = _wet_dry(depth_table)
        rank_ets = ets(rank_table)
        depth_ets = ets(depth_table)
        out["rank"] = rank_ets
        out["incumbent"] = depth_ets
        if wet < OCCURRENCE_MIN_WET_DAYS or dry < OCCURRENCE_MIN_DRY_DAYS:
            return out
        if rank_ets is None or depth_ets is None:
            return out
        out["adequate"] = True
        out["material"] = (rank_ets - depth_ets) >= PRACTICAL_FLOOR_ETS
        return out
    rank_mae = _mae(rank_rows, dates)
    depth_mae = _mae(depth_rows, dates)
    out["rank"] = rank_mae
    out["incumbent"] = depth_mae
    if rank_mae is None or depth_mae is None or depth_mae <= 0.0:
        return out
    out["adequate"] = True
    out["material"] = (
        (depth_mae - rank_mae) / depth_mae
    ) >= PRACTICAL_FLOOR_RELATIVE_MAE
    return out


def _pair_result(
    cells: dict[tuple[int, str], _Cell],
    quantities: tuple[str, ...],
    leads: tuple[int, ...],
    rank: _EntityId,
    depth: _EntityId,
) -> dict[str, object]:
    """One (daily-rank entity, incumbent depth) pair, over every lead.

    A lead counts only when EVERY quantity of the variable is adequate on it
    (temperature's two components, precip's two endpoints), and it is a win
    only when every quantity is materially better — a split-endpoint result
    is not a win.
    """
    adequate = 0
    wins = 0
    per_lead: dict[str, dict[str, object]] = {}
    for lead in leads:
        per_quantity = {
            q: _quantity_outcome(cells.get((lead, q), {}), rank, depth, q)
            for q in quantities
        }
        if not any(cells.get((lead, q)) for q in quantities):
            continue
        per_lead[str(lead)] = cast("dict[str, object]", per_quantity)
        if not all(bool(o["adequate"]) for o in per_quantity.values()):
            continue
        adequate += 1
        if all(bool(o["material"]) for o in per_quantity.values()):
            wins += 1
    return {
        "depth": int(depth[1]),
        "compared": adequate > 0,
        "material": adequate > 0 and wins >= required_lead_agreement(adequate),
        "leads_adequate": adequate,
        "leads_material": wins,
        "per_lead": per_lead,
    }


def _exclusion_reason(
    cells: dict[tuple[int, str], _Cell],
    quantities: tuple[str, ...],
    leads: tuple[int, ...],
    rank: _EntityId,
    depth: _EntityId,
) -> str:
    """Why the pair was never adequately compared, most specific first."""
    reasons: set[str] = set()
    for lead in leads:
        for quantity in quantities:
            cell = cells.get((lead, quantity), {})
            for entity in (rank, depth):
                for row in cell.get(entity, {}).values():
                    reason = row["forecast_exclusion_reason"]
                    if reason is not None:
                        reasons.add(str(reason))
    for named in (EXCLUDE_INSUFFICIENT_RANK_HISTORY, EXCLUDE_NO_SAMPLES):
        if named in reasons:
            return named
    return EXCLUSION_THIN_LEADS


def _entity_state(
    dominant_over: list[int], excluded: list[dict[str, object]], beaten: bool
) -> str:
    if beaten:
        return "beaten"
    if not dominant_over:
        return "unassessable"
    return "dominant_on_subset" if excluded else "dominant"


def _fold(entities: list[dict[str, object]]) -> tuple[RankingConclusion, str | None]:
    """§10 change 6's precedence, applied to the per-entity states."""
    states = [str(e["state"]) for e in entities]
    if not states or all(s == "unassessable" for s in states):
        return "not_assessable", None
    if all(s == "dominant" for s in states):
        return "indicated_all_depths", None
    dominant = [e for e in entities if e["state"] == "dominant"]
    if dominant:
        return "indicated", str(dominant[0]["entity_key"])
    subset = [e for e in entities if e["state"] == "dominant_on_subset"]
    if subset:
        return "indicated_on_subset", str(subset[0]["entity_key"])
    return "not_indicated", None


def _variable_conclusion(
    conn: sqlite3.Connection, run_id: int, variable: str, leads: tuple[int, ...]
) -> dict[str, object]:
    cells = _load_cells(conn, run_id, variable)
    quantities = VARIABLE_QUANTITIES[variable]
    present = sorted(
        {
            int(key)
            for cell in cells.values()
            for (etype, key) in cell
            if etype == DAILY_RANK_ENTITY_TYPE
        }
    )
    entities: list[dict[str, object]] = []
    for rank_depth in present:
        rank: _EntityId = (DAILY_RANK_ENTITY_TYPE, str(rank_depth))
        dominant_over: list[int] = []
        excluded: list[dict[str, object]] = []
        beaten = False
        comparisons: list[dict[str, object]] = []
        for incumbent in SIM_DEPTHS:
            depth: _EntityId = ("depth", str(incumbent))
            pair = _pair_result(cells, quantities, leads, rank, depth)
            comparisons.append(pair)
            if not bool(pair["compared"]):
                pair["reason"] = _exclusion_reason(
                    cells, quantities, leads, rank, depth
                )
                excluded.append({"depth": incumbent, "reason": pair["reason"]})
            elif bool(pair["material"]):
                dominant_over.append(incumbent)
            else:
                beaten = True
        entities.append(
            {
                "entity_key": str(rank_depth),
                "state": _entity_state(dominant_over, excluded, beaten),
                "better_than_depths": dominant_over,
                "excluded_depths": excluded,
                "comparisons": comparisons,
            }
        )
    value, selected = _fold(entities)
    return {
        "value": value,
        # Post-hoc selection among at most four is DISCLOSED, not corrected
        # (§10 change 2): the selected key, the count examined, and every
        # entity's own result travel with the conclusion.
        "selected_entity_key": selected,
        "entities_examined": len(entities),
        "entities": entities,
    }


def daily_rank_conclusions(
    conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
) -> dict[str, dict[str, object]]:
    """Per-variable ``ranking_redesign_indicated`` conclusion for one run.

    Derived from retained evidence; nothing is written. The same object is
    served by the verdicts API and rendered by the page.
    """
    span = DECISION_LEADS if leads is None else leads
    return {
        variable: _variable_conclusion(conn, run_id, variable, span)
        for variable in SIM_VARIABLES
    }


__all__ = [
    "DAILY_RANK_ENTITY_TYPE",
    "EXCLUSION_THIN_LEADS",
    "RANKING_CONCLUSIONS",
    "RankingConclusion",
    "daily_rank_conclusions",
]

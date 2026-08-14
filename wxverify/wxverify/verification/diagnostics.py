"""§14a (W13): the observed-wet precip-total MAE, derived at read time.

0.11.0 §11 lists "precip-total MAE restricted to observed-wet common days"
under *Secondary (always displayed, never deciding)* and nothing computed
it. This module computes it from retained evidence, persists nothing, and
is called by BOTH ``/verification`` and
``GET /api/verification/runs/{run_id}/diagnostics`` so the two surfaces
cannot drift.

Three rules are load-bearing and implemented literally:

* The entity set is the cell's RESOLVED MEMBERS — depth entities, the
  baselines present in the cell, and the feeds clearing the availability
  floor (§16.2). ``daily_rank_depth`` entities are never cell members and
  carry ``predicted = None`` / ``abs_error = NULL`` on strict-common-core
  dates; sweeping them in would take a mean over a NULL.
* "Observed wet" is a CLASS test on the truth — ``truth_wet_hours >= 1``,
  which honours the site's configured ``rain_threshold_mm``. It is never a
  comparison of the occurrence-scale threshold against the ``precip_total``
  truth, which is a millimetre quantity.
* The sufficiency threshold is an ANNOTATION, not a suppression: every
  non-empty sample reports its value beside ``low_sample``. A ``value`` of
  ``null`` always carries one of the three ``reason`` strings, so an
  unexplained absence is unrepresentable.

The headline and the ``leads`` grid are two artifacts, not one (§14a
change 2). ``value`` is the run's INCUMBENT precip depth alone — the blend
the site actually publishes — because the page label reads as the
product's error, and a mean pooled over every resolved member folds in the
deliberately-bad baselines and whichever feeds happened to clear the
availability floor that night, so it would move with no product change and
be incomparable across runs. The grid beside it discloses every member,
and no consumer may derive the headline by aggregating it.
"""

from __future__ import annotations

import sqlite3

from wxverify.verification.coverage import QUANTITY_PRECIP_TOTAL
from wxverify.verification.engine import stored_cell_resolution
from wxverify.verification.methodology import OCCURRENCE_MIN_WET_DAYS
from wxverify.verification.runs import run_config_from_row
from wxverify.verification.simulate import SIM_DAY_COUNT, SIM_DEPTHS

#: The variable this diagnostic is defined over (0.11.0 §11).
OBSERVED_WET_VARIABLE = "precip"

#: The ``reason`` reported when the run has no observed-wet common day at
#: all — the one state in which the mean is genuinely undefined.
NO_OBSERVED_WET_DAYS = "no_observed_wet_days"

#: The ``reason`` reported when the run's incumbent precip depth is outside
#: ``SIM_DEPTHS`` — a real configuration state (an operator-set depth of 5
#: or 6) in which no depth entity exists, so the metric has no subject.
INCUMBENT_NOT_SIMULATED = "incumbent_not_simulated"

#: The ``reason`` reported when observed-wet common dates exist but the
#: headline entity is scored on none of them. Unreachable by construction —
#: ``_eligible_dates`` scores every resolved member on every strict
#: common-core date — and declared so that ``value: null`` can never be
#: reasonless.
INCUMBENT_NOT_SCORED = "incumbent_not_scored"

_EntityId = tuple[str, str]
_Errors = dict[tuple[int, _EntityId, str], float]


def _evidence(
    conn: sqlite3.Connection, run_id: int
) -> tuple[_Errors, set[tuple[int, str]]]:
    """Per-(lead, entity, date) ``abs_error`` and the observed-wet day set."""
    rows = conn.execute(
        """
        SELECT lead, entity_type, entity_key, target_local_date,
               truth_wet_hours, abs_error
        FROM verification_evidence
        WHERE run_id = ? AND variable = ? AND quantity = ?
        """,
        (run_id, OBSERVED_WET_VARIABLE, QUANTITY_PRECIP_TOTAL),
    ).fetchall()
    errors: _Errors = {}
    wet: set[tuple[int, str]] = set()
    for row in rows:
        lead = int(str(row["lead"]))
        date = str(row["target_local_date"])
        wet_hours = row["truth_wet_hours"]
        if wet_hours is not None and int(str(wet_hours)) >= 1:
            wet.add((lead, date))
        if row["abs_error"] is not None:
            entity = (str(row["entity_type"]), str(row["entity_key"]))
            errors[(lead, entity, date)] = float(str(row["abs_error"]))
    return errors, wet


def observed_wet_precip_mae(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
    """Mean precip-total ``abs_error`` for the run's incumbent precip depth.

    The returned payload always carries ``value``, ``entity_type`` /
    ``entity_key`` (the headline entity, populated even when ``value`` is
    ``None``), ``sample_days``, ``low_sample`` and ``reason`` together — a
    thin sample is reported with its caveat, never suppressed — plus
    ``observations`` (the denominator of ``value``) and the per-lead
    ``leads`` disclosure grid, which is NOT what ``value`` is derived from.
    """
    errors, wet = _evidence(conn, run_id)
    depth = run_config_from_row(conn, run_id).incumbent_depth(OBSERVED_WET_VARIABLE)
    headline_entity: _EntityId = ("depth", str(depth))
    leads: list[dict[str, object]] = []
    headline_values: list[float] = []
    headline_days: set[str] = set()
    grid_days: set[str] = set()
    for lead in range(SIM_DAY_COUNT):
        members, common = stored_cell_resolution(
            conn, run_id, OBSERVED_WET_VARIABLE, lead, QUANTITY_PRECIP_TOTAL
        )
        dates = [d for d in common if (lead, d) in wet]
        if not dates:
            continue
        grid_days.update(dates)
        for d in dates:
            error = errors.get((lead, headline_entity, d))
            if error is not None:
                headline_values.append(error)
                headline_days.add(d)
        entities: list[dict[str, object]] = []
        for entity in members:
            values = [
                errors[(lead, entity, d)] for d in dates if (lead, entity, d) in errors
            ]
            if not values:
                continue
            entities.append(
                {
                    "entity_type": entity[0],
                    "entity_key": entity[1],
                    "value": sum(values) / len(values),
                    "sample_days": len(values),
                }
            )
        leads.append({"lead": lead, "sample_days": len(dates), "entities": entities})
    # The three reasons are evaluated in this order and no other: two of the
    # states can co-occur and only the first is the useful explanation.
    reason: str | None
    if depth not in SIM_DEPTHS:
        reason = INCUMBENT_NOT_SIMULATED
    elif not grid_days:
        reason = NO_OBSERVED_WET_DAYS
    elif not headline_values:
        reason = INCUMBENT_NOT_SCORED
    else:
        reason = None
    sample_days = len(headline_days)
    return {
        "value": (
            None if reason is not None else sum(headline_values) / len(headline_values)
        ),
        "entity_type": headline_entity[0],
        "entity_key": headline_entity[1],
        "sample_days": sample_days,
        "low_sample": sample_days < OCCURRENCE_MIN_WET_DAYS,
        "reason": reason,
        "observations": len(headline_values),
        "leads": leads,
    }

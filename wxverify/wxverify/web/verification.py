"""Loader for the /verification page (§16): persisted results only.

Reads the site's published verification run — verdicts, headline
results, day-context diagnostics, and the pinned methodology snapshot —
without touching the simulation path. Values that were insufficient,
not applicable, or failed stay ``None`` and render as em-dashes, never
as numeric zero.
"""

from __future__ import annotations

import json
import sqlite3
from typing import cast

from wxverify.settings.depth import effective_blend_depths
from wxverify.verification import methodology
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    published_run_id,
)
from wxverify.web.context import SiteView, load_site, load_sites

#: Operator-facing labels for the verdict outcomes (§16.2), including the
#: explicit 'skipped' outcome for incumbents outside the simulated range.
OUTCOME_LABELS: dict[str, str] = {
    "recommend": "Recommend depth change",
    "retain_incumbent": "Retain incumbent",
    "mixed_by_lead": "Mixed by lead",
    "mixed_by_quantity": "Mixed by quantity",
    "insufficient_evidence": "Insufficient evidence",
    "skipped": "Skipped (incumbent outside simulated range)",
}


def _parse_json(raw: object) -> object:
    if raw is None:
        return None
    return cast(object, json.loads(str(raw)))


def _resolve_site(
    conn: sqlite3.Connection, site_id: int | None, sites: list[SiteView]
) -> SiteView | None:
    if site_id is not None:
        return load_site(conn, site_id)
    return sites[0] if sites else None


def _load_run(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
    row = conn.execute(
        "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"published run {run_id} missing")
    return {
        "id": int(row["id"]),
        "state": str(row["state"]),
        "attempt": int(row["attempt"]),
        "methodology_version": int(row["methodology_version"]),
        "app_version": str(row["app_version"]),
        "period_start": row["period_start"],
        "period_end": row["period_end"],
        "settled_through": row["settled_through"],
        "bootstrap_seed": int(row["bootstrap_seed"]),
        "bootstrap_resamples": int(row["bootstrap_resamples"]),
        "input_fingerprint": str(row["input_fingerprint"]),
        "created_at": str(row["created_at"]),
        "published_at": row["published_at"],
        "config_snapshot": _parse_json(row["config_snapshot"]),
    }


def _load_verdicts(conn: sqlite3.Connection, run_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT variable, outcome, recommended_depth, incumbent_depth,
               tested_family
        FROM verification_verdicts WHERE run_id = ? ORDER BY variable
        """,
        (run_id,),
    ).fetchall()
    return [
        {
            "variable": str(row["variable"]),
            "outcome": str(row["outcome"]),
            "label": OUTCOME_LABELS.get(str(row["outcome"]), str(row["outcome"])),
            "recommended_depth": row["recommended_depth"],
            "incumbent_depth": int(row["incumbent_depth"]),
            "tested_family": _parse_json(row["tested_family"]),
        }
        for row in rows
    ]


def _load_headline(conn: sqlite3.Connection, run_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT variable, lead, quantity, entity_type, entity_key,
               common_days, mae, bias, rmse, ets, availability_rate,
               delta_vs_incumbent
        FROM verification_results
        WHERE run_id = ? AND headline = 1
        ORDER BY variable, quantity, lead, entity_type, entity_key
        """,
        (run_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_day_context(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
    rows = conn.execute(
        """
        SELECT snapshot_local_date, knowability_exclusions,
               null_availability_samples
        FROM verification_day_context
        WHERE run_id = ? ORDER BY snapshot_local_date
        """,
        (run_id,),
    ).fetchall()
    excluded_days = 0
    null_samples = 0
    for row in rows:
        exclusions = _parse_json(row["knowability_exclusions"])
        if isinstance(exclusions, (list, dict)):
            sized = cast("list[object] | dict[str, object]", exclusions)
            if sized:
                excluded_days += 1
        null_samples += int(row["null_availability_samples"])
    return {
        "snapshot_days": len(rows),
        "days_with_exclusions": excluded_days,
        "null_availability_samples": null_samples,
    }


def load_verification(
    conn: sqlite3.Connection, site_id: int | None
) -> dict[str, object]:
    """Everything the /verification page renders, in one read closure."""
    sites = load_sites(conn, include_disabled=False)
    site = _resolve_site(conn, site_id, sites)
    context: dict[str, object] = {
        "sites": sites,
        "site": site,
        "run": None,
        "verdicts": [],
        "headline": [],
        "day_context": None,
        "warnings": {},
        "depths": effective_blend_depths(conn),
        "methodology": {
            "version": methodology.METHODOLOGY_VERSION,
            "bootstrap_resamples": methodology.BOOTSTRAP_RESAMPLES,
            "candidate_ci_level": methodology.CANDIDATE_CI_LEVEL,
            "precip_improvement_ci_level": methodology.PRECIP_IMPROVEMENT_CI_LEVEL,
            "baseline_gate_ci_level": methodology.BASELINE_GATE_CI_LEVEL,
            "non_inferiority_mae_margin": methodology.NON_INFERIORITY_MAE_MARGIN,
            "non_inferiority_ets_margin": methodology.NON_INFERIORITY_ETS_MARGIN,
        },
    }
    if site is None:
        return context
    run_id = published_run_id(conn, site.id)
    failed_newer = conn.execute(
        """
        SELECT 1 FROM verification_runs
        WHERE site_id = ? AND state = 'failed' AND id > ? LIMIT 1
        """,
        (site.id, run_id if run_id is not None else 0),
    ).fetchone()
    stale = False
    if run_id is not None:
        run = _load_run(conn, run_id)
        context["run"] = run
        context["verdicts"] = _load_verdicts(conn, run_id)
        context["headline"] = _load_headline(conn, run_id)
        context["day_context"] = _load_day_context(conn, run_id)
        # Safe on a read connection: capture_config_snapshot only writes
        # when the site has no published tz generation, impossible for a
        # site with a published run.
        current = input_fingerprint(
            conn, site.id, capture_config_snapshot(conn, site.id)
        )
        stale = current != run["input_fingerprint"]
    context["warnings"] = {
        "no_publishable_run": run_id is None,
        "stale_inputs": stale,
        "failed_newer_attempt": failed_newer is not None,
    }
    return context

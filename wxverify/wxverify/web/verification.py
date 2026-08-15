"""Loader for the /verification page (§16): persisted results only.

Reads the site's published verification run — the pinned configuration and
roster snapshot, verdicts with their complete tested family, headline
evidence keyed by variable x lead x quantity x candidate depth, the
non-enactable diagnostics, and the methodology block — without touching the
simulation path. Every number comes from a persisted row of the published
run; nothing is simulated, resampled, or recomputed on the request path
(§19). Values that were insufficient, not applicable, or failed stay
``None`` and render as em-dashes, never as numeric zero.

The measurement contract and the methodology constants come from
``wxverify.verification.contract`` — the same module ``/api/verification/*``
serves — so the page and the API cannot drift (§18.11).
"""

from __future__ import annotations

import json
import sqlite3
from typing import cast

from wxverify.core.timeutil import utc_now
from wxverify.settings.depth import effective_blend_depths
from wxverify.verification import methodology
from wxverify.verification.contract import (
    CONTRACT,
    VERIFICATION_SCHEMA,
    methodology_constants,
)
from wxverify.verification.diagnostics import observed_wet_precip_mae
from wxverify.verification.ranking import daily_rank_conclusions
from wxverify.verification.runs import (
    current_input_fingerprint,
    published_run_id,
    trigger_status,
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

#: §16.2 caveats printed under an outcome so the label is never over-read.
OUTCOME_CAVEATS: dict[str, str] = {
    "retain_incumbent": (
        "No demonstrated improvement over the incumbent under this run's "
        "declared configuration. This is not proof that the incumbent depth "
        "is optimal."
    ),
    "skipped": (
        "Placeholder verdict: the incumbent depth lies outside the simulated "
        "range, so no comparison was made. Not evidence of improvement."
    ),
    "insufficient_evidence": (
        "Too few adequate leads to decide. Not evidence for or against a depth change."
    ),
}

#: §12 gate keys -> operator-facing labels, for both variable shapes.
GATE_LABELS: dict[str, str] = {
    "ci_excludes_zero": "CI excludes zero",
    "lead_stability": "Lead stability",
    "practical_floor": "Practical floor",
    "beats_baselines": "Beats baselines",
    "components_non_inferior": "Components non-inferior",
    "total_material": "Total materially better",
    "occurrence_material": "Occurrence materially better",
    "total_non_inferior": "Total non-inferior",
    "occurrence_non_inferior": "Occurrence non-inferior",
}

#: The §12 gates each variable's decision path declares, in display order.
VARIABLE_GATES: dict[str, tuple[str, ...]] = {
    "temperature": (
        "ci_excludes_zero",
        "lead_stability",
        "practical_floor",
        "beats_baselines",
        "components_non_inferior",
    ),
    "wind": (
        "ci_excludes_zero",
        "lead_stability",
        "practical_floor",
        "beats_baselines",
    ),
    "precip": (
        "total_material",
        "occurrence_material",
        "total_non_inferior",
        "occurrence_non_inferior",
        "beats_baselines",
    ),
}

#: Endpoint keys inside a tested-family candidate record.
ENDPOINT_LABELS: dict[str, str] = {
    "headline": "Headline",
    "total": "Precip total",
    "occurrence": "Precip occurrence",
}

#: Quantity -> tested-family endpoint whose CI/adequacy governs the row.
_QUANTITY_ENDPOINT: dict[str, str] = {
    "temperature_high": "headline",
    "temperature_low": "headline",
    "wind_max": "headline",
    "precip_total": "total",
    "precip_occurrence": "occurrence",
}

#: §16.4 diagnostics methodology v1 declines to define at all. Declared on
#: the page rather than silently omitted, so the divergence stays visible.
#: This list is for METHODOLOGY gaps only — a metric the specification calls
#: always-displayed and the code merely does not implement is a gap to close,
#: never an entry here (that is why the observed-wet precip-total MAE is
#: absent from it: §14a implements the metric instead). Families that exist
#: in methodology v1 but can come up empty for DATA reasons are declared per
#: run from _DATA_UNAVAILABLE_DIAGNOSTICS below.
UNAVAILABLE_DIAGNOSTICS: list[dict[str, str]] = [
    {
        "key": "wet_hour_share",
        "label": "Wet-hour-share verification",
        "reason": (
            "Deferred to methodology version 2: methodology v1 declares neither "
            "the bin edges nor the predicted-vs-observed denominator rule this "
            "diagnostic needs, so this run persists no wet-hour-share evidence."
        ),
    },
]

#: §16.4 diagnostic families methodology v1 DOES produce but which a given
#: run can fail to populate for data reasons. Emitting the reason keeps an
#: empty section from reading as an unimplemented one.
_DATA_UNAVAILABLE_DIAGNOSTICS: dict[str, dict[str, str]] = {
    "d0": {
        "label": "Same-day (D0) skill",
        "reason": (
            "no same-day depth result was scored on this run - the run's "
            "snapshot days carry no D0 pairs that passed the truth gates."
        ),
    },
    "bias_rmse": {
        "label": "Bias and RMSE by lead",
        "reason": (
            "no scored depth result at lead 1 or beyond carries a bias value "
            "on this run, so no bias/RMSE row can be shown."
        ),
    },
    "contingency": {
        "label": "Precip-occurrence contingency tables",
        "reason": (
            "no precip-occurrence result on this run reached a contingency "
            "table - the common days held too few wet or dry events."
        ),
    },
    "daily_rank": {
        "label": "Daily-rank diagnostic",
        "reason": (
            "no (feed, quantity) pair reached the minimum rank history this "
            "run, so no daily-rank row was persisted."
        ),
    },
    "feeds": {
        "label": "Per-feed availability and pairwise comparison",
        "reason": (
            "this run persisted no per-feed result rows, so neither the "
            "availability floor nor the pairwise comparison can be shown."
        ),
    },
}

_BASELINE_PREFIX = "baseline_"


def _parse_json(raw: object) -> object:
    if raw is None:
        return None
    return cast(object, json.loads(str(raw)))


def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(k): v for k, v in cast("dict[object, object]", value).items()}
    return {}


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return cast("list[object]", value)
    return []


def _as_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _as_ci(value: object) -> list[float] | None:
    items = [_as_number(v) for v in _as_list(value)]
    if len(items) != 2 or items[0] is None or items[1] is None:
        return None
    return [items[0], items[1]]


def _gate_state(value: object) -> str:
    if isinstance(value, bool):
        return "pass" if value else "fail"
    return "insufficient"


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
        "tz_generation_id": int(row["tz_generation_id"]),
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


def _snapshot_view(raw: object) -> dict[str, object] | None:
    """The run's pinned configuration + roster, shaped for rendering (§16.1)."""
    snapshot = _as_dict(raw)
    if not snapshot:
        return None
    depths = _as_dict(snapshot.get("blend_depths"))
    sources = _as_dict(snapshot.get("blend_depth_sources"))
    roster: list[dict[str, object]] = []
    for item in _as_list(snapshot.get("roster")):
        feed = _as_dict(item)
        if feed:
            roster.append(
                {
                    "feed_id": feed.get("feed_id"),
                    "source": feed.get("source"),
                    "model": feed.get("model"),
                    "max_lead_hours": feed.get("max_lead_hours"),
                }
            )
    return {
        "timezone": snapshot.get("timezone"),
        "rain_threshold_mm": snapshot.get("rain_threshold_mm"),
        "wall_clock": snapshot.get("wall_clock"),
        "blend_depth": snapshot.get("blend_depth"),
        "blend_depths": [
            {
                "variable": variable,
                "depth": depth,
                "source": sources.get(variable, "global"),
            }
            for variable, depth in sorted(depths.items())
        ],
        "min_n": snapshot.get("min_n"),
        "window_days": snapshot.get("window_days"),
        "tz_generation_id": snapshot.get("tz_generation_id"),
        "roster": roster,
        "roster_size": len(roster),
    }


def _endpoint_view(name: str, raw: object) -> dict[str, object]:
    body = _as_dict(raw)
    adequate = [
        int(number)
        for number in (
            _as_number(value) for value in _as_list(body.get("adequate_leads"))
        )
        if number is not None
    ]
    per_lead = {
        str(lead): _as_number(effect)
        for lead, effect in _as_dict(body.get("per_lead")).items()
    }
    return {
        "name": name,
        "label": ENDPOINT_LABELS.get(name, name),
        "point": _as_number(body.get("pooled_point")),
        "ci": _as_ci(body.get("ci")),
        "adequate_leads": adequate,
        "adequate_count": len(adequate),
        "per_lead": per_lead,
    }


def _baseline_views(raw: object) -> list[dict[str, object]]:
    """Flatten the per-candidate baseline-gate detail (both variable shapes)."""
    out: list[dict[str, object]] = []
    for name, value in sorted(_as_dict(raw).items()):
        body = _as_dict(value)
        if "passed" in body:
            out.append(
                {
                    "name": name.removeprefix(_BASELINE_PREFIX),
                    "scope": None,
                    "state": _gate_state(body.get("passed")),
                    "ci": _as_ci(body.get("ci")),
                }
            )
            continue
        # precip nests one baseline map per improved endpoint.
        for inner_name, inner in sorted(body.items()):
            inner_body = _as_dict(inner)
            out.append(
                {
                    "name": inner_name.removeprefix(_BASELINE_PREFIX),
                    "scope": name,
                    "state": _gate_state(inner_body.get("passed")),
                    "ci": _as_ci(inner_body.get("ci")),
                }
            )
    return out


def _candidate_view(key: str, raw: object, variable: str) -> dict[str, object]:
    record = _as_dict(raw)
    endpoints = [
        _endpoint_view(name, record[name])
        for name in ("headline", "total", "occurrence")
        if name in record
    ]
    conditions = _as_dict(record.get("conditions"))
    # Every gate the variable's §12 path declares is listed, so a gate that
    # was never evaluated reads 'insufficient' instead of vanishing.
    gates = [
        {
            "key": gate,
            "label": GATE_LABELS[gate],
            "state": _gate_state(conditions.get(gate)),
        }
        for gate in VARIABLE_GATES.get(variable, tuple(GATE_LABELS))
    ]
    components = [
        {
            "quantity": name,
            "point": _as_number(_as_dict(value).get("pooled_point")),
            "degraded": bool(_as_dict(value).get("degraded")),
        }
        for name, value in sorted(_as_dict(record.get("components")).items())
    ]
    return {
        "key": key,
        "endpoints": endpoints,
        "endpoint_by_name": {str(e["name"]): e for e in endpoints},
        "gates": gates,
        "baselines": _baseline_views(record.get("baselines")),
        "components": components,
        "insufficient": not conditions,
    }


def _verdict_view(row: sqlite3.Row) -> dict[str, object]:
    outcome = str(row["outcome"])
    variable = str(row["variable"])
    family = _as_dict(_parse_json(row["tested_family"]))
    candidates = [
        _candidate_view(str(key), value, variable)
        for key, value in sorted(_as_dict(family.get("candidates")).items())
    ]
    recommended = row["recommended_depth"]
    primary: dict[str, object] | None = None
    if recommended is not None:
        wanted = str(int(recommended))
        primary = next((c for c in candidates if c["key"] == wanted), None)
    tie_break = _as_dict(family.get("tie_break"))
    return {
        "variable": variable,
        "outcome": outcome,
        "label": OUTCOME_LABELS.get(outcome, outcome),
        "caveat": OUTCOME_CAVEATS.get(outcome),
        "is_placeholder": outcome == "skipped",
        "recommended_depth": recommended,
        "incumbent_depth": int(row["incumbent_depth"]),
        "candidates": candidates,
        "candidate_by_key": {str(c["key"]): c for c in candidates},
        "primary": primary,
        "unresolved": [
            str(k) for k in _as_list(family.get("statistically_unresolved"))
        ],
        "tie_break": tie_break or None,
        "skip_reason": family.get("reason"),
        "tested_keys": [str(c["key"]) for c in candidates],
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
    return [_verdict_view(row) for row in rows]


def _load_results(conn: sqlite3.Connection, run_id: int) -> list[dict[str, object]]:
    """Every persisted result row of the run, in one indexed read."""
    rows = conn.execute(
        """
        SELECT variable, lead, quantity, entity_type, entity_key, headline,
               common_days, mae, bias, rmse, hits, misses, false_alarms,
               correct_negatives, ets, availability_rate, delta_vs_incumbent,
               detail
        FROM verification_results
        WHERE run_id = ?
        ORDER BY variable, quantity, lead, entity_type, entity_key
        """,
        (run_id,),
    ).fetchall()
    # §9: `detail` carries `vs_recommended` on below-floor / pairwise-only
    # rows. Parsed here rather than in the template — a raw JSON string in a
    # view row is one `|tojson` away from being rendered as text.
    out: list[dict[str, object]] = []
    for row in rows:
        view = dict(row)
        view["detail"] = _as_dict(_parse_json(view["detail"]))
        out.append(view)
    return out


def _load_contributor_depths(
    conn: sqlite3.Connection, run_id: int
) -> dict[tuple[str, int, str, str], tuple[int, int]]:
    """Realized contributor depth per candidate-depth cell (§16.3).

    Aggregates the run's already-persisted evidence rows; eligible rows only,
    so an all-ineligible cell yields no entry and renders as an em-dash
    rather than a misleading zero.
    """
    rows = conn.execute(
        """
        SELECT variable, lead, quantity, entity_key,
               MIN(realized_contributors) AS lo,
               MAX(realized_contributors) AS hi
        FROM verification_evidence
        WHERE run_id = ? AND entity_type = 'depth'
          AND forecast_eligible = 1 AND realized_contributors IS NOT NULL
        GROUP BY variable, lead, quantity, entity_key
        """,
        (run_id,),
    ).fetchall()
    return {
        (
            str(row["variable"]),
            int(row["lead"]),
            str(row["quantity"]),
            str(row["entity_key"]),
        ): (int(row["lo"]), int(row["hi"]))
        for row in rows
    }


def _primary_metric(result: dict[str, object]) -> tuple[str, float | None]:
    if str(result["quantity"]) == "precip_occurrence":
        return "ETS", _as_number(result["ets"])
    return "MAE", _as_number(result["mae"])


def _headline_rows(
    results: list[dict[str, object]],
    verdicts: list[dict[str, object]],
    contributors: dict[tuple[str, int, str, str], tuple[int, int]],
) -> list[dict[str, object]]:
    """§16.3 rows: variable x lead x quantity x candidate depth."""
    verdict_by_variable = {str(v["variable"]): v for v in verdicts}
    baselines_by_cell: dict[tuple[str, int, str], list[dict[str, object]]] = {}
    for result in results:
        entity_type = str(result["entity_type"])
        if not entity_type.startswith(_BASELINE_PREFIX):
            continue
        cell = (
            str(result["variable"]),
            int(str(result["lead"])),
            str(result["quantity"]),
        )
        _, value = _primary_metric(result)
        baselines_by_cell.setdefault(cell, []).append(
            {"name": entity_type.removeprefix(_BASELINE_PREFIX), "value": value}
        )

    out: list[dict[str, object]] = []
    for result in results:
        if (
            str(result["entity_type"]) != "depth"
            or int(str(result["lead"])) < 1
            or not int(str(result["headline"]))
        ):
            continue
        variable = str(result["variable"])
        lead = int(str(result["lead"]))
        quantity = str(result["quantity"])
        depth_key = str(result["entity_key"])
        verdict = verdict_by_variable.get(variable)
        incumbent = None if verdict is None else int(str(verdict["incumbent_depth"]))
        is_incumbent = incumbent is not None and depth_key == str(incumbent)
        candidate: dict[str, object] | None = None
        if verdict is not None:
            by_key = cast("dict[str, dict[str, object]]", verdict["candidate_by_key"])
            candidate = by_key.get(depth_key)
        endpoint: dict[str, object] | None = None
        if candidate is not None:
            endpoints = cast(
                "dict[str, dict[str, object]]", candidate["endpoint_by_name"]
            )
            endpoint = endpoints.get(_QUANTITY_ENDPOINT.get(quantity, "headline"))
        if is_incumbent:
            lead_state = "incumbent"
        elif endpoint is None:
            lead_state = "insufficient"
        else:
            adequate = cast("list[int]", endpoint["adequate_leads"])
            lead_state = "adequate" if lead in adequate else "insufficient"
        metric_label, metric_value = _primary_metric(result)
        events = None
        if quantity == "precip_occurrence" and result["hits"] is not None:
            events = {
                "hits": result["hits"],
                "misses": result["misses"],
                "false_alarms": result["false_alarms"],
                "correct_negatives": result["correct_negatives"],
            }
        out.append(
            {
                **result,
                "depth": depth_key,
                "is_incumbent": is_incumbent,
                "metric_label": metric_label,
                "metric_value": metric_value,
                "ci": None if endpoint is None else endpoint["ci"],
                "lead_effect": (
                    None
                    if endpoint is None
                    else cast("dict[str, float | None]", endpoint["per_lead"]).get(
                        str(lead)
                    )
                ),
                "lead_state": lead_state,
                "events": events,
                "contributors": contributors.get((variable, lead, quantity, depth_key)),
                "baselines": baselines_by_cell.get((variable, lead, quantity), []),
                "gates": [] if candidate is None else candidate["gates"],
                "baseline_gates": [] if candidate is None else candidate["baselines"],
            }
        )
    return out


def _common_day_ranges(
    results: list[dict[str, object]],
) -> dict[str, dict[str, int]]:
    """Per-variable strict-common-day range over the decision leads (§16.2)."""
    spans: dict[str, list[int]] = {}
    for result in results:
        if (
            str(result["entity_type"]) != "depth"
            or int(str(result["lead"])) < 1
            or not int(str(result["headline"]))
        ):
            continue
        spans.setdefault(str(result["variable"]), []).append(
            int(str(result["common_days"]))
        )
    return {
        variable: {"min": min(days), "max": max(days)}
        for variable, days in spans.items()
        if days
    }


def _diagnostics(
    results: list[dict[str, object]], day_context: dict[str, object]
) -> dict[str, object]:
    """§16.4: the non-enactable diagnostic sets, visibly separated."""
    d0: list[dict[str, object]] = []
    bias_rmse: list[dict[str, object]] = []
    contingency: list[dict[str, object]] = []
    daily_rank: list[dict[str, object]] = []
    feeds: list[dict[str, object]] = []
    for result in results:
        entity_type = str(result["entity_type"])
        lead = int(str(result["lead"]))
        if entity_type == "depth" and lead == 0:
            d0.append(result)
        if entity_type == "depth" and lead >= 1 and result["bias"] is not None:
            bias_rmse.append(result)
        if (
            str(result["quantity"]) == "precip_occurrence"
            and result["hits"] is not None
        ):
            contingency.append(result)
        if entity_type == "daily_rank_depth":
            daily_rank.append(result)
        if entity_type == "feed":
            rate = _as_number(result["availability_rate"])
            feeds.append(
                {
                    **result,
                    "below_floor": rate is not None
                    and rate < methodology.ROSTER_AVAILABILITY_FLOOR,
                    "pairwise_only": not int(str(result["headline"])),
                    # §9: written by the `pairwise` phase; surfaced here so
                    # the page and the API diagnostics payload agree.
                    "vs_recommended": _as_dict(result.get("detail")).get(
                        "vs_recommended"
                    ),
                }
            )
    families: dict[str, list[dict[str, object]]] = {
        "d0": d0,
        "bias_rmse": bias_rmse,
        "contingency": contingency,
        "daily_rank": daily_rank,
        "feeds": feeds,
    }
    # §14/W11: an empty family is declared with its DATA reason, so the page
    # never leaves the operator to guess whether a section is empty because
    # this run had nothing to put in it or because nothing computes it.
    unavailable: list[dict[str, str]] = [
        *UNAVAILABLE_DIAGNOSTICS,
        *(
            {"key": key, **_DATA_UNAVAILABLE_DIAGNOSTICS[key]}
            for key, rows in families.items()
            if not rows
        ),
    ]
    return {
        **families,
        "day_context": day_context,
        "unavailable": unavailable,
    }


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
    from wxverify.verification.publish_hold import read_publish_hold

    sites = load_sites(conn, include_disabled=False)
    site = _resolve_site(conn, site_id, sites)
    live_depths = effective_blend_depths(conn)
    context: dict[str, object] = {
        "sites": sites,
        "site": site,
        "publish_hold": read_publish_hold(conn),
        "run": None,
        "snapshot": None,
        "verdicts": [],
        "headline": [],
        "common_days": {},
        "diagnostics": None,
        "day_context": None,
        "trigger": None,
        "warnings": {},
        "depths": live_depths,
        "depth_mismatch": False,
        "verification_schema": VERIFICATION_SCHEMA,
        "contract": CONTRACT,
        "methodology": {
            **methodology_constants(),
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
    # §12/§3.1: same derivation the status API serves, so the page and the
    # payload cannot report different trigger states for one site.
    context["trigger"] = trigger_status(conn, site.id, utc_now())
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
        verdicts = _load_verdicts(conn, run_id)
        # §10: the daily-rank conclusion is a first-class conclusion line,
        # derived by the same helper the verdicts API calls.
        conclusions = daily_rank_conclusions(conn, run_id)
        for verdict in verdicts:
            verdict["ranking_redesign_indicated"] = conclusions.get(
                str(verdict["variable"])
            )
        results = _load_results(conn, run_id)
        day_context = _load_day_context(conn, run_id)
        context["run"] = run
        context["snapshot"] = _snapshot_view(run["config_snapshot"])
        context["verdicts"] = verdicts
        context["headline"] = _headline_rows(
            results, verdicts, _load_contributor_depths(conn, run_id)
        )
        context["common_days"] = _common_day_ranges(results)
        context["diagnostics"] = {
            **_diagnostics(results, day_context),
            # §14a: always-displayed secondary metric, derived by the same
            # helper the API diagnostics payload calls.
            "observed_wet_precip_mae": observed_wet_precip_mae(conn, run_id),
        }
        context["day_context"] = day_context
        # §16.1/§16.2: the cards carry the run's PINNED incumbent depth; the
        # live effective depth is a different number whenever settings moved
        # since publication, and the page must not let one read as the other.
        context["depth_mismatch"] = any(
            v["variable"] in live_depths
            and live_depths[str(v["variable"])].depth != v["incumbent_depth"]
            for v in verdicts
        )
        # Read-only by construction (NB-9) — None means the timezone
        # pointer is absent, so staleness is unknown, not true.
        current = current_input_fingerprint(conn, site.id)
        stale = current is not None and current != run["input_fingerprint"]
    context["warnings"] = {
        "no_publishable_run": run_id is None,
        "stale_inputs": stale,
        "failed_newer_attempt": failed_newer is not None,
    }
    return context

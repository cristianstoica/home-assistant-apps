"""Read-only verification API (§16): persisted results only.

Every endpoint serves rows already persisted by the verification run
chain — no simulation or bootstrap work on the request path. Detailed
evidence requires a ``run_id`` so pagination can never cross runs.
Responses carry the dedicated ``verification_schema`` contract version;
insufficient / not-applicable / failed values are ``null``, never numeric
zero.
"""

from __future__ import annotations

import json
import sqlite3
from typing import cast

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from wxverify.api.errors import ApiError
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import get_db
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
from wxverify.web.render import ingress_url

router = APIRouter(prefix="/api/verification", tags=["verification"])

__all__ = ["CONTRACT", "VERIFICATION_SCHEMA", "router"]

_EVIDENCE_PAGE_DEFAULT = 200
_EVIDENCE_PAGE_MAX = 500
_RUNS_PAGE_DEFAULT = 50
_RUNS_PAGE_MAX = 200


def _parse_json(raw: object) -> object:
    if raw is None:
        return None
    return cast(object, json.loads(str(raw)))


def _run_out(row: sqlite3.Row, *, include_snapshot: bool) -> dict[str, object]:
    out: dict[str, object] = {
        "run_id": int(row["id"]),
        "site_id": int(row["site_id"]),
        "state": str(row["state"]),
        "attempt": int(row["attempt"]),
        "methodology_version": int(row["methodology_version"]),
        "app_version": str(row["app_version"]),
        "tz_generation_id": int(row["tz_generation_id"]),
        "period_start": None
        if row["period_start"] is None
        else str(row["period_start"]),
        "period_end": None if row["period_end"] is None else str(row["period_end"]),
        "settled_through": None
        if row["settled_through"] is None
        else str(row["settled_through"]),
        "bootstrap_seed": int(row["bootstrap_seed"]),
        "bootstrap_resamples": int(row["bootstrap_resamples"]),
        "input_fingerprint": str(row["input_fingerprint"]),
        "created_at": str(row["created_at"]),
        "published_at": None
        if row["published_at"] is None
        else str(row["published_at"]),
        "error": None if row["error"] is None else str(row["error"]),
    }
    if include_snapshot:
        out["config_snapshot"] = _parse_json(row["config_snapshot"])
    return out


def _run_row(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None:
        raise ApiError(404, "verification run not found")
    return row


def _site_status(conn: sqlite3.Connection, site_id: int) -> dict[str, object]:
    run_id = published_run_id(conn, site_id)
    published: dict[str, object] | None = None
    stale = False
    if run_id is not None:
        row = _run_row(conn, run_id)
        published = _run_out(row, include_snapshot=False)
        # Cheap staleness check: recompute the input fingerprint against
        # live tables and compare with the published run's pinned one.
        # Read-only by construction (NB-9) — None means the timezone
        # pointer is absent, so staleness is unknown, not true.
        current = current_input_fingerprint(conn, site_id)
        stale = current is not None and current != str(row["input_fingerprint"])
    failed_newer = conn.execute(
        """
        SELECT 1 FROM verification_runs
        WHERE site_id = ? AND state = 'failed' AND id > ? LIMIT 1
        """,
        (site_id, run_id if run_id is not None else 0),
    ).fetchone()
    return {
        "site_id": site_id,
        "published_run": published,
        # §12/§3.1: additive under §16.1 — the nightly trigger's own
        # decision (and the publish hold), so `no_publishable_run` is no
        # longer the operator's only signal. Same derivation the
        # /verification page uses, so the two surfaces cannot disagree.
        "trigger": trigger_status(conn, site_id, utc_now()),
        "warnings": {
            "no_publishable_run": run_id is None,
            "stale_inputs": stale,
            "failed_newer_attempt": failed_newer is not None,
        },
    }


@router.get("/status")
async def verification_status(site: int | None = None) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        if site is not None:
            row = conn.execute("SELECT id FROM sites WHERE id = ?", (site,)).fetchone()
            if row is None:
                raise ApiError(404, "site not found")
            site_ids = [site]
        else:
            site_ids = [
                int(r["id"])
                for r in conn.execute(
                    "SELECT id FROM sites WHERE enabled = 1 ORDER BY id"
                )
            ]
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "contract": CONTRACT,
            "sites": [_site_status(conn, sid) for sid in site_ids],
        }

    return await get_db().read(_read)


@router.get("/runs")
async def list_runs(
    site: int | None = None,
    limit: int = _RUNS_PAGE_DEFAULT,
    offset: int = 0,
) -> dict[str, object]:
    bounded = max(1, min(limit, _RUNS_PAGE_MAX))
    bounded_offset = max(0, offset)

    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        where = "" if site is None else "WHERE site_id = ?"
        params: tuple[object, ...] = () if site is None else (site,)
        rows = conn.execute(
            f"""
            SELECT * FROM verification_runs {where}
            ORDER BY id DESC LIMIT ? OFFSET ?
            """,
            (*params, bounded, bounded_offset),
        ).fetchall()
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "limit": bounded,
            "offset": bounded_offset,
            "runs": [_run_out(row, include_snapshot=False) for row in rows],
        }

    return await get_db().read(_read)


@router.get("/latest")
async def latest_run(request: Request, site: int) -> RedirectResponse:
    def _read(conn: sqlite3.Connection) -> int:
        run_id = published_run_id(conn, site)
        if run_id is None:
            raise ApiError(404, "no published verification run")
        return run_id

    run_id = await get_db().read(_read)
    # Built through the ingress prefix, exactly like every other absolute URL
    # in the app: a hand-concatenated path sends an ingress client outside the
    # mount and 404s. ``ingress_url`` returns the bare path when root_path is
    # empty, so the standalone case is unchanged.
    return RedirectResponse(
        url=ingress_url(request, f"/api/verification/runs/{run_id}"),
        status_code=307,
    )


@router.get("/runs/{run_id}")
async def get_run(run_id: int) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        row = _run_row(conn, run_id)
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "run": _run_out(row, include_snapshot=True),
        }

    return await get_db().read(_read)


@router.get("/runs/{run_id}/verdicts")
async def run_verdicts(run_id: int) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        _run_row(conn, run_id)
        rows = conn.execute(
            """
            SELECT * FROM verification_verdicts
            WHERE run_id = ? ORDER BY variable
            """,
            (run_id,),
        ).fetchall()
        # §10: derived at read time by the SAME helper the page calls, so the
        # two surfaces cannot state different conclusions.
        conclusions = daily_rank_conclusions(conn, run_id)
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "run_id": run_id,
            "verdicts": [
                {
                    "variable": str(row["variable"]),
                    "outcome": str(row["outcome"]),
                    "recommended_depth": None
                    if row["recommended_depth"] is None
                    else int(row["recommended_depth"]),
                    "incumbent_depth": int(row["incumbent_depth"]),
                    "tested_family": _parse_json(row["tested_family"]),
                    "ranking_redesign_indicated": conclusions.get(str(row["variable"])),
                }
                for row in rows
            ],
        }

    return await get_db().read(_read)


def _evidence_filters(
    variable: str | None,
    lead: int | None,
    quantity: str | None,
    entity_type: str | None,
    entity_key: str | None,
    eligibility: str | None,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if variable is not None:
        clauses.append("variable = ?")
        params.append(variable)
    if lead is not None:
        clauses.append("lead = ?")
        params.append(lead)
    if quantity is not None:
        clauses.append("quantity = ?")
        params.append(quantity)
    if entity_type is not None:
        clauses.append("entity_type = ?")
        params.append(entity_type)
    if entity_key is not None:
        clauses.append("entity_key = ?")
        params.append(entity_key)
    if eligibility == "eligible":
        clauses.append("forecast_eligible = 1 AND truth_eligible = 1")
    elif eligibility == "forecast_ineligible":
        clauses.append("forecast_eligible = 0")
    elif eligibility == "truth_ineligible":
        clauses.append("truth_eligible = 0")
    elif eligibility is not None:
        raise ApiError(400, "unknown eligibility filter")
    sql = ""
    if clauses:
        sql = " AND " + " AND ".join(clauses)
    return sql, params


@router.get("/runs/{run_id}/evidence")
async def run_evidence(
    run_id: int,
    variable: str | None = None,
    lead: int | None = None,
    quantity: str | None = None,
    entity_type: str | None = None,
    entity_key: str | None = None,
    eligibility: str | None = None,
    limit: int = _EVIDENCE_PAGE_DEFAULT,
    offset: int = 0,
) -> dict[str, object]:
    bounded = max(1, min(limit, _EVIDENCE_PAGE_MAX))
    bounded_offset = max(0, offset)
    where, params = _evidence_filters(
        variable, lead, quantity, entity_type, entity_key, eligibility
    )

    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        _run_row(conn, run_id)
        rows = conn.execute(
            f"""
            SELECT * FROM verification_evidence
            WHERE run_id = ?{where}
            ORDER BY id LIMIT ? OFFSET ?
            """,
            (run_id, *params, bounded, bounded_offset),
        ).fetchall()
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "run_id": run_id,
            "limit": bounded,
            "offset": bounded_offset,
            "evidence": [dict(row) for row in rows],
        }

    return await get_db().read(_read)


@router.get("/runs/{run_id}/diagnostics")
async def run_diagnostics(
    run_id: int,
    variable: str | None = None,
    lead: int | None = None,
    quantity: str | None = None,
    entity_type: str | None = None,
    headline: int | None = None,
    limit: int = _EVIDENCE_PAGE_DEFAULT,
    offset: int = 0,
) -> dict[str, object]:
    bounded = max(1, min(limit, _EVIDENCE_PAGE_MAX))
    bounded_offset = max(0, offset)
    where, params = _evidence_filters(variable, lead, quantity, entity_type, None, None)
    if headline is not None:
        where += " AND headline = ?"
        params.append(1 if headline else 0)

    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        _run_row(conn, run_id)
        rows = conn.execute(
            f"""
            SELECT * FROM verification_results
            WHERE run_id = ?{where}
            ORDER BY id LIMIT ? OFFSET ?
            """,
            (run_id, *params, bounded, bounded_offset),
        ).fetchall()
        days = conn.execute(
            """
            SELECT snapshot_local_date, snapshot_utc, knowability_exclusions,
                   null_availability_samples
            FROM verification_day_context
            WHERE run_id = ? ORDER BY snapshot_local_date
            """,
            (run_id,),
        ).fetchall()
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "run_id": run_id,
            "limit": bounded,
            "offset": bounded_offset,
            "results": [
                {
                    **dict(row),
                    "detail": _parse_json(row["detail"]),
                }
                for row in rows
            ],
            # §14a: always-displayed secondary metric, from the same helper
            # /verification renders, so the surfaces cannot drift.
            "observed_wet_precip_mae": observed_wet_precip_mae(conn, run_id),
            "day_context": [
                {
                    "snapshot_local_date": str(d["snapshot_local_date"]),
                    "snapshot_utc": str(d["snapshot_utc"]),
                    "knowability_exclusions": _parse_json(d["knowability_exclusions"]),
                    "null_availability_samples": int(d["null_availability_samples"]),
                }
                for d in days
            ],
        }

    return await get_db().read(_read)


@router.get("/runs/{run_id}/methodology")
async def run_methodology(run_id: int) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        row = _run_row(conn, run_id)
        return {
            "verification_schema": VERIFICATION_SCHEMA,
            "run_id": run_id,
            "contract": CONTRACT,
            "constants": methodology_constants(),
            "provenance": _run_out(row, include_snapshot=True),
        }

    return await get_db().read(_read)

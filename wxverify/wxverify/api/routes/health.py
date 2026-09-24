"""Health and ops routes."""

from __future__ import annotations

import sqlite3
from typing import Annotated

from fastapi import APIRouter, Query

from wxverify.api.routes.db_transfer import export_sweeper_death
from wxverify.collection.budget import current_billing_day
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.core.options import load_runtime_options
from wxverify.core.secrets import key_status
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import get_db
from wxverify.db.runtime_state import runtime_status
from wxverify.monitor import build_verdict, error_verdict
from wxverify.provider_ops import provider_health
from wxverify.verification.read_cache import warm_outcome

router = APIRouter(prefix="/api", tags=["health"])

# Named so a test can import the same symbol production runs rather than a
# re-typed copy: a bare COUNT(*) with no WHERE lets the planner pick whichever
# index is smallest, so this stays a table-free scan without an INDEXED BY hint.
FORECAST_SAMPLES_COUNT_SQL = "SELECT COUNT(*) AS n FROM forecast_samples"
FORECAST_PAIRS_COUNT_SQL = "SELECT COUNT(*) AS n FROM forecast_pairs"

# Two probes over one body: `/api/health/feeds` answers with an exact COUNT by
# default, and with a plain EXISTS when the caller opts out of `sample_count`.
# Both variants are formatted from this one template, so the rollup, the join
# order and the published row order cannot drift apart between them -- only
# the sample expression and its alias differ.
_HEALTH_FEEDS_TEMPLATE = """
    WITH feed_rollup AS (
        SELECT m.id AS src_feed_id,
               CASE
                 WHEN m.source='meteoblue' AND m.model!='multimodel'
                 THEN pkg.id
                 ELSE m.id
               END AS display_feed_id
        FROM feeds m
        LEFT JOIN feeds pkg
          ON pkg.source='meteoblue' AND pkg.model='multimodel'
    )
    SELECT s.id AS site_id, s.name AS site_name, f.id AS feed_id,
           s.enabled AS site_enabled,
           f.source, f.model, f.enabled AS feed_enabled,
           f.default_subscribed, f.disabled_reason,
           sfs.enabled AS override_enabled, sfs.last_run_at, sfs.last_error,
           sfs.error_count,
           {probe_prefix}(
               SELECT {probe_select}
               FROM feed_rollup r
               -- CROSS JOIN is load-bearing: it pins feed_rollup as the outer
               -- loop so the probe binds BOTH (site_id, feed_id) and SQLite
               -- can seek a covering index for that pair -- which index it
               -- picks is the planner's choice and is not guaranteed, so no
               -- index name is pinned here. With a plain JOIN the planner
               -- drives from forecast_samples, binds site_id only, and the
               -- seek degrades to an index scan -- measured 5x SLOWER than
               -- the full-table aggregate this replaced.
               CROSS JOIN forecast_samples fs
                 ON fs.site_id = s.id AND fs.feed_id = r.src_feed_id
               WHERE r.display_feed_id = f.id
           ) AS {probe_alias}
    FROM sites s
    JOIN feeds f
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = s.id AND sfs.feed_id = f.id
    WHERE f.is_virtual = 0
      AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
    ORDER BY s.name, f.source, f.model
    """

HEALTH_FEEDS_SQL = _HEALTH_FEEDS_TEMPLATE.format(
    probe_prefix="", probe_select="COUNT(*)", probe_alias="sample_count"
)
HEALTH_FEEDS_HAS_SAMPLES_SQL = _HEALTH_FEEDS_TEMPLATE.format(
    probe_prefix="EXISTS ", probe_select="1", probe_alias="has_samples"
)


@router.get("/health/keys")
async def health_keys() -> dict[str, bool]:
    return key_status()


@router.get("/health/budget")
async def health_budget() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        rows = conn.execute(
            """
            SELECT source, daily_call_limit, daily_credit_limit, billing_tz
            FROM sources s
            ORDER BY s.source
            """
        ).fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            source = str(row["source"])
            budget = conn.execute(
                """
                SELECT calls, credits
                FROM api_budget
                WHERE source = ? AND billing_day = ?
                """,
                (source, current_billing_day(str(row["billing_tz"]))),
            ).fetchone()
            out.append(
                {
                    "source": source,
                    "daily_call_limit": int(row["daily_call_limit"]),
                    "daily_credit_limit": row["daily_credit_limit"],
                    "calls": 0 if budget is None else int(budget["calls"]),
                    "credits": 0 if budget is None else int(budget["credits"]),
                }
            )
        return out

    return await get_db().read(_read)


@router.get("/health/feeds")
async def health_feeds(include_sample_count: bool = True) -> list[dict[str, object]]:
    """Per (site, feed) health rows, ordered by site name.

    ``sample_count`` is an exact lifetime count whose cost grows with the
    retained history. A caller that only needs the ``status`` rungs can pass
    ``?include_sample_count=false``: the response then omits ``sample_count``
    and carries a boolean ``has_samples`` instead, and the statement asks
    ``EXISTS`` rather than counting. Every other field, every ``status``
    value, and the row order are identical in both modes.
    """

    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        rows = conn.execute(
            HEALTH_FEEDS_SQL if include_sample_count else HEALTH_FEEDS_HAS_SAMPLES_SQL
        ).fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            subscribed = bool(
                row["override_enabled"]
                if row["override_enabled"] is not None
                else row["default_subscribed"]
            )
            # The two statements answer the same question about this row; only
            # the default one also reports how many samples there are.
            if include_sample_count:
                has_samples = int(row["sample_count"]) > 0
            else:
                has_samples = bool(row["has_samples"])
            if not bool(row["site_enabled"]):
                status = "site disabled"
            elif not bool(row["feed_enabled"]):
                status = "disabled"
            elif not subscribed:
                status = "not subscribed / available"
            elif row["last_error"] == NO_USABLE_SAMPLES_SENTINEL:
                status = "fetched, 0 usable"
            elif row["last_error"] is not None:
                status = "error"
            elif row["last_run_at"] is None:
                status = "never run / due"
            elif not has_samples:
                status = "ran / no usable data"
            else:
                status = "ok"
            entry: dict[str, object] = {
                "site_id": int(row["site_id"]),
                "site_name": str(row["site_name"]),
                "feed_id": int(row["feed_id"]),
                "source": str(row["source"]),
                "model": str(row["model"]),
                "subscribed": subscribed,
                "status": status,
                "disabled_reason": row["disabled_reason"],
                "last_run_at": row["last_run_at"],
                "last_error": row["last_error"],
                "error_count": int(row["error_count"] or 0),
                "feed_enabled": bool(row["feed_enabled"]),
                "site_enabled": bool(row["site_enabled"]),
            }
            # Last key either way, so the default response keeps the exact
            # field order it publishes today.
            if include_sample_count:
                entry["sample_count"] = int(row["sample_count"])
            else:
                entry["has_samples"] = has_samples
            out.append(entry)
        return out

    return await get_db().read(_read)


@router.get("/health/providers")
async def health_providers(
    site_id: int | None = None,
    source: Annotated[list[str] | None, Query()] = None,
) -> list[dict[str, object]]:
    return await get_db().read(
        lambda conn: provider_health(conn, site_id=site_id, sources=source or [])
    )


@router.get("/health/backfill")
async def health_backfill() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return [
            {
                "site_id": int(row["id"]),
                "status": row["backfill_status"],
                "through": row["backfill_through"],
            }
            for row in conn.execute(
                "SELECT id, backfill_status, backfill_through FROM sites ORDER BY id"
            )
        ]

    return await get_db().read(_read)


def _readable_int(value: object) -> int | None:
    """Diagnostic coercion: an unreadable stored integer renders as null.

    A TEXT, BLOB or REAL-infinity carrier binds into an INTEGER column and raises
    here. This route exists to SHOW the operator the backoff table, so one bad
    row must not 500 the whole read -- and reporting 0 would invent a value the
    database does not contain.
    """
    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError, OverflowError):
        return None


@router.get("/health/backoffs")
async def health_backoffs() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return [
            {
                "domain": str(row["domain"]),
                "next_attempt_at": str(row["next_attempt_at"]),
                "retry_count": _readable_int(row["retry_count"]),
            }
            for row in conn.execute(
                """
                SELECT domain, next_attempt_at, retry_count
                FROM domain_backoffs
                ORDER BY next_attempt_at
                """
            )
        ]

    return await get_db().read(_read)


@router.get("/observations/current")
async def observations_current(
    station: int | None = None,
) -> list[dict[str, object]]:
    """Latest current-obs snapshot per enabled station (READ, guard-exempt).

    One row per ``stations.enabled = 1`` station, LEFT JOINed to
    ``station_current_obs`` (the raw display snapshot) and ``station_poll_state``
    (the current-obs stream's own diagnostics). Two health cases stay distinct:
    a **cold** station has no ``station_current_obs`` row, so its obs fields are
    ``null``; a **previously-online-now-offline** station retains its last-good
    row (non-null obs) with ``health_state="offline"`` — the LEFT JOIN returns
    the retained row, so null-obs and ``offline`` are separate cases. The
    diagnostic fields (``health_state``, ``next_poll_at``, ``last_error``) come
    from ``station_poll_state``, never ``stations.last_error`` (which the hourly
    stream resets on success). Obs values are the stored native ``units:"m"``
    form (km/h wind, hPa, mm) — no conversion in the route. Optional
    ``?station=<id>`` filter; empty registry → ``[]``.

    ``provider_reported_offline`` is ``true`` exactly when the latest persisted
    poll classification is a provider no-data reply (``health_state="offline"``).
    It is always a JSON bool, never ``null``: ``false`` for every other state and
    for a missing ``station_poll_state`` row.
    """

    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        params: list[object] = []
        where = "WHERE st.enabled = 1"
        if station is not None:
            where += " AND st.id = ?"
            params.append(station)
        rows = conn.execute(
            f"""
            SELECT st.id AS station_id, st.pws_station_id AS pws_station_id,
                   sps.health_state AS health_state,
                   sps.next_poll_at AS next_poll_at,
                   sps.last_error AS last_error,
                   sps.error_count AS error_count,
                   sps.last_poll_at AS last_poll_at,
                   co.obs_time_utc AS obs_time_utc,
                   co.temp AS temp, co.humidity AS humidity, co.dewpt AS dewpt,
                   co.wind_speed AS wind_speed, co.wind_gust AS wind_gust,
                   co.wind_dir AS wind_dir, co.pressure AS pressure,
                   co.precip_rate AS precip_rate, co.precip_total AS precip_total,
                   co.uv AS uv, co.neighborhood AS neighborhood
            FROM stations st
            LEFT JOIN station_current_obs co ON co.station_id = st.id
            LEFT JOIN station_poll_state sps ON sps.station_id = st.id
            {where}
            ORDER BY st.id
            """,
            params,
        ).fetchall()
        return [
            {
                "station_id": int(row["station_id"]),
                "pws_station_id": str(row["pws_station_id"]),
                "health_state": row["health_state"],
                "provider_reported_offline": row["health_state"] == "offline",
                "obs_time_utc": row["obs_time_utc"],
                "temp": row["temp"],
                "humidity": row["humidity"],
                "dewpt": row["dewpt"],
                "wind_speed": row["wind_speed"],
                "wind_gust": row["wind_gust"],
                "wind_dir": row["wind_dir"],
                "pressure": row["pressure"],
                "precip_rate": row["precip_rate"],
                "precip_total": row["precip_total"],
                "uv": row["uv"],
                "neighborhood": row["neighborhood"],
                "next_poll_at": row["next_poll_at"],
                "last_error": row["last_error"],
                "error_count": (
                    None if row["error_count"] is None else int(row["error_count"])
                ),
                "last_poll_at": row["last_poll_at"],
            }
            for row in rows
        ]

    return await get_db().read(_read)


@router.get("/worker/status")
async def worker_status(counts: str = Query("")) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
        ).fetchall()
        status: dict[str, object] = {
            "jobs": {str(row["status"]): int(row["n"]) for row in rows}
        }
        status.update(runtime_status(conn))
        for job_type in ("fetch_feed", "fetch_obs", "pair_and_score"):
            row = conn.execute(
                """
                SELECT MAX(updated_at) AS completed_at
                FROM jobs
                WHERE status='completed' AND type=?
                """,
                (job_type,),
            ).fetchone()
            status[f"last_completed_{job_type}_at"] = (
                None if row is None else row["completed_at"]
            )
        # Exact, not estimated, and opt-in: forecast_pairs has five delete
        # paths (multimodel rematerialization on every scoring run among
        # them), so its rowid watermark is not a valid row-count estimate,
        # and this route is a five-minutely coordinator-polled core slice
        # that must not carry the cost of an exact count on every poll.
        if counts == "exact":
            status["forecast_samples_rows"] = int(
                conn.execute(FORECAST_SAMPLES_COUNT_SQL).fetchone()["n"]
            )
            status["forecast_pairs_rows"] = int(
                conn.execute(FORECAST_PAIRS_COUNT_SQL).fetchone()["n"]
            )
        return status

    db = get_db()
    result = await db.read(_read)
    # Read after the await, never inside _read: this does no DB work and must
    # not run under the read lock it is reporting on.
    result["read_timing"] = db.read_timing_snapshot()
    result["read_timing_since"] = db.read_timing_since
    # Also process state, not DB state -- same reasoning as read_timing
    # above, must not run inside _read under the read lock.
    result["generation"] = db.generation
    result["last_import_swap_at"] = db.last_import_swap_at
    # Process state too, and present-and-None before any warm has run rather
    # than absent. One slot shared by the boot warm and every publish-time
    # warm, so a `running` state is only diagnosable against its `at` stamp.
    warm = warm_outcome()
    result["read_cache_warm"] = (
        None
        if warm is None
        else {
            "state": warm.state,
            "at": warm.at,
            "detail": warm.detail,
            "derivations_failed": warm.derivations_failed,
        }
    )
    return result


@router.get("/health/monitor")
async def health_monitor() -> dict[str, object]:
    now = utc_now()
    try:
        opts = load_runtime_options()
        # Process state, not DB state: read here on the event loop and NEVER
        # inside `_read`, which runs under the read lock it would be reporting
        # on -- the same rule the post-await block in `worker_status` follows.
        # The rendered line is composed here because `monitor.py` takes a plain
        # `str | None`: a pure domain module must not import an API-routes one.
        death = export_sweeper_death()
        export_sweeper_dead: str | None = None
        if death is not None:
            export_sweeper_dead = (
                f"export sweeper crashed at {death.at}: {death.detail}"
                if death.reason == "crashed"
                else f"export sweeper returned unexpectedly at {death.at}"
            )

        def _read(conn: sqlite3.Connection) -> dict[str, object]:
            return build_verdict(
                conn,
                pipeline_enabled=opts.monitor_pipeline,
                budget_enabled=opts.monitor_budget,
                db_enabled=opts.monitor_db,
                now=now,
                export_sweeper_dead=export_sweeper_dead,
            )

        return await get_db().read(_read)
    except Exception as exc:  # always-200 belt: never surface a 5xx here
        return error_verdict(now, f"{type(exc).__name__}: {exc}")

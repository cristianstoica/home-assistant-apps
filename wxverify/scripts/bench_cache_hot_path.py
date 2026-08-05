"""Cache hot-path benchmark harness (measure-first gate).

Two measurement environments, kept separate:

1. ``queries`` — opens the exported database READ-ONLY (no enqueue calls, no
   writes) and measures ``EXPLAIN QUERY PLAN`` plus repeated wall-clock timings
   for the two hot-path DISTINCT scans that gate every cache-backed read:
   ``_expected_active_cells`` (composite) and ``_expected_active_feed_ids``
   (leaderboard, one shape per production variable). Row counts relevant to
   the predicates are recorded so the measurements are reproducible.

2. ``endpoint`` — end-to-end in-process ASGI (``TestClient``) measurement of
   ``GET /api/composite?site=<id>&window=rolling`` against a DISPOSABLE COPY
   of the exported database, reusing the idle-worker pattern from
   ``tests/test_composite_cache_backed.py`` so an active worker cannot flip
   the cache stale→fresh mid-run. Records the first-call (cold) duration and
   ``--warm`` warm samples; for EVERY sample it records the declared cache
   status (via ``composite_with_status`` on the same DB) and the
   ``pair_and_score`` jobs-table state before and after. If any warm sample's
   status differs from the rest the run is marked invalid — discard and
   re-run. In-process ASGI excludes uvicorn/network overhead (milliseconds
   against a multi-second budget); that exclusion is recorded in the report.

Run from the add-on directory (``uv sync`` does not install the package)::

    PYTHONPATH=. uv run python scripts/bench_cache_hot_path.py \
        --db /path/to/exported-wxverify.db --out bench-report.json

The production RPi run against a copy of the live DB export is the one that
counts for the latency gate (each DISTINCT query < 2.0 s; composite cold and
warm max < 20.0 s). A workstation run helps query-plan diagnosis only; its
hardware delta is recorded separately by the operator.

The exported DB is never mutated: the query phase opens it read-only and the
endpoint phase runs against a throwaway copy in a temporary directory.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import sqlite3
import statistics
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

_QUERY_REPEATS = 5
_QUERY_GATE_S = 2.0
_ENDPOINT_GATE_S = 20.0

# Mirrors the HA coordinator's _LEADERBOARD_VARIABLES: production polls exactly
# these three shapes at day_ahead=1, so they are always measured even when a
# variable is absent from the export.
REQUIRED_VARIABLES = ("temperature", "wind", "precip")


def _open_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _captured_sql(conn: sqlite3.Connection, run: Any) -> str:
    """Capture the (parameter-expanded) SQL a callable executes on ``conn``."""
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        run()
    finally:
        conn.set_trace_callback(None)
    hot = [s for s in statements if "SELECT DISTINCT" in s]
    return hot[-1] if hot else statements[-1]


def _time_repeated(run: Any, repeats: int) -> list[float]:
    timings: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        run()
        timings.append(time.perf_counter() - start)
    return timings


def _explain(conn: sqlite3.Connection, sql: str) -> list[str]:
    rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
    return [" | ".join(str(value) for value in row) for row in rows]


def _query_measurements(db_path: Path, site_id: int) -> dict[str, Any]:
    from wxverify.scoring.composite import _expected_active_cells
    from wxverify.scoring.leaderboard import (
        _expected_active_feed_ids,
        resolve_window,
    )

    conn = _open_readonly(db_path)
    try:
        resolved = resolve_window(conn, "rolling")
        db_variables = [
            str(row["variable"])
            for row in conn.execute(
                "SELECT DISTINCT variable FROM forecast_pairs WHERE site_id=?"
                " ORDER BY variable",
                (site_id,),
            )
        ]
        variables = sorted(set(db_variables) | set(REQUIRED_VARIABLES))
        measurements: list[dict[str, Any]] = []

        def _measure(name: str, run: Any) -> None:
            timings = _time_repeated(run, _QUERY_REPEATS)
            sql = _captured_sql(conn, run)
            measurements.append(
                {
                    "query": name,
                    "timings_s": timings,
                    "median_s": statistics.median(timings),
                    "max_s": max(timings),
                    "gate_pass": max(timings) < _QUERY_GATE_S,
                    "explain_query_plan": _explain(conn, sql),
                }
            )

        def _count(sql: str, params: tuple[object, ...] = ()) -> int:
            return int(conn.execute(sql, params).fetchone()[0])

        _measure(
            "_expected_active_cells",
            lambda: _expected_active_cells(conn, site_id=site_id, resolved=resolved),
        )
        variable_row_counts: dict[str, int] = {}
        for variable in variables:
            _measure(
                f"_expected_active_feed_ids[{variable}]",
                lambda v=variable: _expected_active_feed_ids(
                    conn, site_id=site_id, variable=v, day_ahead=1, resolved=resolved
                ),
            )
            variable_row_counts[variable] = _count(
                "SELECT COUNT(*) FROM forecast_pairs"
                " WHERE site_id=? AND variable=? AND day_ahead=1 AND valid_at>=?",
                (site_id, variable, resolved.cutoff),
            )

        row_counts = {
            "forecast_pairs_total": _count("SELECT COUNT(*) FROM forecast_pairs"),
            "forecast_pairs_site": _count(
                "SELECT COUNT(*) FROM forecast_pairs WHERE site_id=?", (site_id,)
            ),
            "forecast_pairs_site_in_cutoff": _count(
                "SELECT COUNT(*) FROM forecast_pairs WHERE site_id=? AND valid_at>=?",
                (site_id, resolved.cutoff),
            ),
            "score_cache_site_window": _count(
                "SELECT COUNT(*) FROM score_cache WHERE site_id=? AND window_key=?",
                (site_id, resolved.window_key),
            ),
            "feeds_total": _count("SELECT COUNT(*) FROM feeds"),
        }
        result: dict[str, Any] = {
            "window_key": resolved.window_key,
            "cutoff": resolved.cutoff,
            "variables": variables,
            "variable_row_counts": variable_row_counts,
            "row_counts": row_counts,
            "measurements": measurements,
        }
        missing_required = [
            v for v in REQUIRED_VARIABLES if variable_row_counts[v] == 0
        ]
        if missing_required:
            result["missing_required_variables"] = missing_required
        return result
    finally:
        conn.close()


def _jobs_state(conn: sqlite3.Connection, site_id: int) -> dict[str, int]:
    return {
        str(row["status"]): int(row["n"])
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM jobs"
            " WHERE type='pair_and_score' AND site_id=?"
            " GROUP BY status",
            (site_id,),
        )
    }


def _endpoint_measurements(db_path: Path, site_id: int, warm: int) -> dict[str, Any]:
    # Deferred imports: config paths must be patched before create_app's
    # lifespan runs init_db, and the worker must be replaced with the idle
    # shim from the composite cache-backed suite BEFORE the app starts —
    # the production app unconditionally starts its worker (api/app.py),
    # and an active worker could flip the cache stale→fresh mid-run.
    import asyncio

    from fastapi.testclient import TestClient

    import wxverify.api.app as app_module
    from wxverify import config
    from wxverify.db.connection import close_db, get_db
    from wxverify.scoring.composite import composite_with_status

    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    with TemporaryDirectory(prefix="wxverify-bench-") as workdir:
        work = Path(workdir)
        disposable = work / "wxverify.db"
        shutil.copyfile(db_path, disposable)
        options_path = work / "options.json"
        options_path.write_text("{}", encoding="utf-8")
        close_db()
        config.db_path = str(disposable)
        config.options_path = str(options_path)
        app_module.run_worker = _idle_worker
        app = app_module.create_app(root_path="")

        samples: list[dict[str, Any]] = []
        with TestClient(app) as client:
            db = get_db()

            def _probe_status(conn: sqlite3.Connection) -> str:
                return composite_with_status(
                    conn, site_id=site_id, window="rolling"
                ).status

            for index in range(1 + warm):
                # The status probe runs the same DISTINCT scan and cache read
                # as the endpoint, warming SQLite's page cache — so for the
                # cold sample (index 0) probe AFTER the timed call instead of
                # before. The read does not mutate the cache and the worker is
                # idle, so the declared status is identical either side.
                cache_status: str | None = None
                if index > 0:
                    cache_status = db.read_sync(_probe_status)
                jobs_before = db.read_sync(lambda conn: _jobs_state(conn, site_id))
                start = time.perf_counter()
                response = client.get(
                    "/api/composite", params={"site": site_id, "window": "rolling"}
                )
                elapsed = time.perf_counter() - start
                if index == 0:
                    cache_status = db.read_sync(_probe_status)
                jobs_after = db.read_sync(lambda conn: _jobs_state(conn, site_id))
                samples.append(
                    {
                        "sample": "cold" if index == 0 else f"warm-{index}",
                        "http_status": response.status_code,
                        "duration_s": elapsed,
                        "cache_status": cache_status,
                        "jobs_before": jobs_before,
                        "jobs_after": jobs_after,
                        "rows": len(response.json()),
                    }
                )
        close_db()

    warm_samples = samples[1:]
    warm_durations = [float(s["duration_s"]) for s in warm_samples]
    warm_statuses = {str(s["cache_status"]) for s in warm_samples}
    consistent = len(warm_statuses) <= 1
    http_all_200 = all(s["http_status"] == 200 for s in samples)
    cold_s = float(samples[0]["duration_s"])
    warm_max = max(warm_durations) if warm_durations else 0.0
    # Belt-and-braces alongside the argparse floor: a run with fewer than ten
    # warm samples cannot certify the warm gate.
    run_valid = consistent and http_all_200 and len(warm_samples) >= 10
    return {
        "note": (
            "in-process ASGI: uvicorn/network overhead excluded"
            " (milliseconds against a multi-second budget)"
        ),
        "samples": samples,
        "cold_s": cold_s,
        "warm_p95_s": (
            statistics.quantiles(warm_durations, n=20)[18]
            if len(warm_durations) >= 2
            else warm_max
        ),
        "warm_max_s": warm_max,
        "warm_count": len(warm_samples),
        "warm_statuses_consistent": consistent,
        "http_all_200": http_all_200,
        "run_valid": run_valid,
        "gate_pass": run_valid
        and cold_s < _ENDPOINT_GATE_S
        and warm_max < _ENDPOINT_GATE_S,
    }


def _resolve_site(db_path: Path, site: int | None) -> int:
    if site is not None:
        return site
    conn = _open_readonly(db_path)
    try:
        row = conn.execute(
            "SELECT id FROM sites WHERE enabled=1 ORDER BY id LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise SystemExit("no enabled site in the exported DB; pass --site")
    return int(row["id"])


def main() -> int:
    doc_lines = (__doc__ or "").splitlines()
    parser = argparse.ArgumentParser(description=doc_lines[0] if doc_lines else None)
    parser.add_argument(
        "--db", required=True, type=Path, help="exported wxverify.db snapshot"
    )
    parser.add_argument(
        "--site", type=int, default=None, help="site id (default: first enabled)"
    )
    parser.add_argument(
        "--warm",
        type=int,
        default=10,
        help="warm endpoint samples (default 10, minimum 10)",
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="write the JSON report here"
    )
    parser.add_argument(
        "--skip-endpoint",
        action="store_true",
        help="query plans/timings only (no ASGI endpoint run)",
    )
    args = parser.parse_args()
    if args.warm < 10:
        parser.error("--warm must be >= 10 (at least ten warm samples are required)")

    if not args.db.exists():
        raise SystemExit(f"database not found: {args.db}")
    site_id = _resolve_site(args.db, args.site)

    from wxverify import __version__

    report: dict[str, Any] = {
        "addon_version": __version__,
        "python": sys.version,
        "hardware": {
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "site_id": site_id,
        "queries": _query_measurements(args.db, site_id),
    }
    if not args.skip_endpoint:
        report["endpoint"] = _endpoint_measurements(args.db, site_id, args.warm)

    rendered = json.dumps(report, indent=2)
    if args.out is not None:
        args.out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)

    query_pass = all(m["gate_pass"] for m in report["queries"]["measurements"])
    missing_required = report["queries"].get("missing_required_variables", [])
    endpoint = report.get("endpoint")
    endpoint_pass: bool | None = None
    if endpoint is None:
        endpoint_note = ": skipped"
    else:
        endpoint_pass = bool(endpoint["gate_pass"])
        endpoint_note = f"<{_ENDPOINT_GATE_S}s+consistent+200s: {endpoint_pass}"
    # Verdict precedence: FAIL beats PARTIAL beats PASS. PARTIAL means a gate
    # was not evaluable (endpoint skipped, required variable unmeasurable) and
    # nothing failed, so an unqualified PASS would overstate the result.
    if not query_pass or endpoint_pass is False:
        verdict = "FAIL"
    elif endpoint_pass is None or missing_required:
        reasons: list[str] = []
        if endpoint_pass is None:
            reasons.append("endpoint skipped")
        if missing_required:
            reasons.append("missing required variables: " + ", ".join(missing_required))
        verdict = f"PARTIAL ({'; '.join(reasons)})"
    else:
        verdict = "PASS"
    print(
        f"\nlatency gate: {verdict} (queries<{_QUERY_GATE_S}s: {query_pass};"
        f" endpoint{endpoint_note})",
        file=sys.stderr,
    )
    if verdict == "FAIL":
        return 1
    if verdict.startswith("PARTIAL"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Route-latency-during-rebuild benchmark gate (0.8.8 plan §5).

Measures dashboard route latency WHILE a scoring rebuild is running — the
exact contended scenario behind the 2026-07-26/27 stalls. The 0.8.6 hot-path
harness required an idle worker and all-``hit`` samples, so it was
structurally blind to this defect; this sibling gates it.

Dual-mode, feature-detected:

- **post-fix-batched** — ``wxverify.scoring.rescore`` and
  ``wxverify.worker.score_batches`` both import: the contended hold is the
  public batched orchestrator ``run_batched_scoring``. Routes must answer
  from the stale snapshot between batch transactions (max < 3.0 s,
  p95 < 1.5 s) and no single write-transaction hold may reach 1.0 s.
- **pre-fix-monolithic** — fallback on revisions without the fix: the hold
  is one monolithic ``db.write(_score_all_windows)`` transaction, and the
  pre-fix routes themselves await their rescore enqueue behind it. The
  expected artifact is a SMALL counted-sample set with >= 1 sample spanning
  the hold — under the verdict precedence that single breached sample IS
  the failing-first demonstration (FAIL, never demoted to PARTIAL).

Harness runs in ONE event loop (worker and routes share the loop in
production): app factory + idle-worker shim, mass-staleness forced exactly
as UTC midnight does (all non-``w:all`` ``computed_at`` set to the previous
UTC day), sampling via in-process ASGI. The exported DB is never mutated —
all work happens on a disposable copy. Synthetic defaults only; the
operator supplies the real export path at run time.

Run from the add-on directory (``uv sync`` does not install the package)::

    PYTHONPATH=. uv run python scripts/bench_route_during_rebuild.py \
        --db /path/to/exported-wxverify.db --out bench-rebuild-report.json

Verdict precedence (mirrors bench_cache_hot_path): any COUNTED sample
breaching the max/p95 gate => FAIL (exit 1), evaluated BEFORE the
sample-floor PARTIAL rule; the floor is a PASS-qualifier only (exit 2 when
unmet without a breach), never a FAIL-suppressor.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import platform
import shutil
import sqlite3
import statistics
import sys
import time
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

_ROUTE_MAX_GATE_S = 3.0
_ROUTE_P95_GATE_S = 1.5
_HOLD_GATE_S = 1.0
_MIN_SAMPLES = 30
_MIN_PER_CLASS = 8
_JOBS_DELTA_GATE = 1

# Mirrors the HA coordinator's polled leaderboard shapes (and the sibling
# bench): production polls exactly these three variables at D+1.
REQUIRED_VARIABLES = ("temperature", "wind", "precip")

_ENDPOINT_CLASSES = ("leaderboard", "curve", "composite")


def _detect_mode() -> str:
    """Feature-detect the fix: both new modules import => post-fix mode."""
    try:
        import wxverify.scoring.rescore  # noqa: F401
        import wxverify.worker.score_batches  # noqa: F401
    except ImportError:
        return "pre-fix-monolithic"
    return "post-fix-batched"


def _request_cycle(site_id: int) -> list[tuple[str, str, dict[str, Any]]]:
    cycle: list[tuple[str, str, dict[str, Any]]] = [
        (
            "leaderboard",
            "/api/leaderboard",
            {"site": site_id, "variable": v, "window": "rolling", "lead": "D+1"},
        )
        for v in REQUIRED_VARIABLES
    ]
    cycle.append(
        (
            "curve",
            "/api/curve",
            {
                "site": site_id,
                "variable": "temperature",
                "window": "rolling",
                "lead": "D+1",
            },
        )
    )
    cycle.append(
        ("composite", "/api/composite", {"site": site_id, "window": "rolling"})
    )
    return cycle


def _rows_in_response(endpoint_class: str, payload: Any) -> int:
    if endpoint_class == "curve":
        if isinstance(payload, dict):
            series = payload.get("series")
            return len(series) if isinstance(series, list) else 0
        return 0
    return len(payload) if isinstance(payload, list) else 0


def _count_rescore_jobs(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type='pair_and_score' AND site_id=?",
        (site_id,),
    ).fetchone()
    return int(row[0])


def _force_mass_staleness(conn: sqlite3.Connection) -> int:
    """Set every non-``w:all`` ``computed_at`` to the previous UTC day.

    Exactly the state UTC midnight produces: all rolling rows classify stale
    together, so every sampled route takes the stale/enqueue path.
    """
    from wxverify.core.timeutil import isoformat_utc, utc_now

    stamp = isoformat_utc(utc_now() - timedelta(days=1))
    cur = conn.execute(
        "UPDATE score_cache SET computed_at=? WHERE window_key <> 'w:all'",
        (stamp,),
    )
    return int(cur.rowcount)


async def _run_contended(app: Any, db: Any, site_id: int, mode: str) -> dict[str, Any]:
    import httpx

    holds: list[dict[str, float]] = []
    t0 = time.perf_counter()
    original_run_immediate = db._run_immediate

    def timed_run_immediate(fn: Any) -> Any:
        start = time.perf_counter()
        try:
            return original_run_immediate(fn)
        finally:
            holds.append(
                {
                    "start_offset_s": start - t0,
                    "hold_s": time.perf_counter() - start,
                }
            )

    db._run_immediate = timed_run_immediate

    if mode == "post-fix-batched":
        from wxverify.worker.score_batches import run_batched_scoring

        rebuild = asyncio.create_task(run_batched_scoring(db, site_id))
    else:
        from wxverify.scoring.engine import _score_all_windows

        rebuild = asyncio.create_task(
            db.write(lambda conn: _score_all_windows(conn, site_id))
        )
    rebuild_start_offset = time.perf_counter() - t0
    # Let the rebuild task actually reach its first write before sampling.
    await asyncio.sleep(0)

    samples: list[dict[str, Any]] = []
    cycle = itertools.cycle(_request_cycle(site_id))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://bench"
    ) as client:
        # Overlap proof (anti-vacuity): the loop condition guarantees every
        # counted sample STARTS before rebuild.done(); per-sample offsets and
        # the rebuild start/end are recorded so the report shows the overlap.
        while not rebuild.done():
            endpoint_class, path, params = next(cycle)
            start_offset = time.perf_counter() - t0
            response = await client.get(path, params=params)
            duration = time.perf_counter() - t0 - start_offset
            try:
                payload = response.json()
            except ValueError:
                payload = None
            samples.append(
                {
                    "endpoint_class": endpoint_class,
                    "path": path,
                    "start_offset_s": start_offset,
                    "duration_s": duration,
                    "http_status": response.status_code,
                    "rows": _rows_in_response(endpoint_class, payload),
                    "counted": True,
                }
            )
    await rebuild
    rebuild_end_offset = time.perf_counter() - t0

    if mode == "post-fix-batched":
        from wxverify.scoring.rescore import drain_pending_rescores

        await drain_pending_rescores()
    # Pre-fix mode: no drain — the routes awaited their enqueues inline.

    db._run_immediate = original_run_immediate
    return {
        "samples": samples,
        "holds": holds,
        "rebuild": {
            "start_offset_s": rebuild_start_offset,
            "end_offset_s": rebuild_end_offset,
            "duration_s": rebuild_end_offset - rebuild_start_offset,
        },
    }


async def _bench_async(db_path: Path, site_id: int, mode: str) -> dict[str, Any]:
    import wxverify.api.app as app_module
    from wxverify.db.connection import get_db

    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    app_module.run_worker = _idle_worker
    app = app_module.create_app(root_path="")
    async with app.router.lifespan_context(app):
        db = get_db()
        stale_rows = await db.write(_force_mass_staleness)
        jobs_before = db.read_sync(lambda conn: _count_rescore_jobs(conn, site_id))
        contended = await _run_contended(app, db, site_id, mode)
        jobs_after = db.read_sync(lambda conn: _count_rescore_jobs(conn, site_id))
    contended["stale_rows_forced"] = stale_rows
    contended["jobs"] = {
        "before": jobs_before,
        "after": jobs_after,
        "delta": jobs_after - jobs_before,
    }
    return contended


def _bench(db_path: Path, site_id: int, mode: str) -> dict[str, Any]:
    from wxverify import config
    from wxverify.db.connection import close_db

    with TemporaryDirectory(prefix="wxverify-bench-rebuild-") as workdir:
        work = Path(workdir)
        disposable = work / "wxverify.db"
        shutil.copyfile(db_path, disposable)
        options_path = work / "options.json"
        options_path.write_text("{}", encoding="utf-8")
        close_db()
        config.db_path = str(disposable)
        config.options_path = str(options_path)
        try:
            return asyncio.run(_bench_async(disposable, site_id, mode))
        finally:
            close_db()


def _verdict(result: dict[str, Any], mode: str) -> tuple[str, dict[str, Any]]:
    samples: list[dict[str, Any]] = result["samples"]
    durations = [float(s["duration_s"]) for s in samples]
    p95 = (
        statistics.quantiles(durations, n=20)[18]
        if len(durations) >= 2
        else (max(durations) if durations else 0.0)
    )
    max_duration = max(durations) if durations else 0.0
    breached_samples = [
        s for s in samples if float(s["duration_s"]) >= _ROUTE_MAX_GATE_S
    ]
    latency_pass = not breached_samples and p95 < _ROUTE_P95_GATE_S
    completeness_pass = all(
        s["http_status"] == 200 and int(s["rows"]) > 0 for s in samples
    )
    hold_values = [float(h["hold_s"]) for h in result["holds"]]
    max_hold = max(hold_values) if hold_values else 0.0
    # The hold gate targets the BATCHED rebuild; in pre-fix mode the
    # monolithic hold is the demonstrated defect and the latency gate is
    # what fails, so the hold gate is recorded but only enforced post-fix.
    hold_pass = max_hold < _HOLD_GATE_S if mode == "post-fix-batched" else None
    jobs_delta = int(result["jobs"]["delta"])
    jobs_pass = jobs_delta <= _JOBS_DELTA_GATE
    per_class = {
        cls: sum(1 for s in samples if s["endpoint_class"] == cls)
        for cls in _ENDPOINT_CLASSES
    }
    floor_met = len(samples) >= _MIN_SAMPLES and all(
        count >= _MIN_PER_CLASS for count in per_class.values()
    )
    gates: dict[str, Any] = {
        "route_max_s": max_duration,
        "route_p95_s": p95,
        "route_max_gate_s": _ROUTE_MAX_GATE_S,
        "route_p95_gate_s": _ROUTE_P95_GATE_S,
        "latency_pass": latency_pass,
        "completeness_pass": completeness_pass,
        "max_hold_s": max_hold,
        "hold_gate_s": _HOLD_GATE_S,
        "hold_pass": hold_pass,
        "jobs_delta": jobs_delta,
        "jobs_pass": jobs_pass,
        "sample_count": len(samples),
        "samples_per_class": per_class,
        "sample_floor_met": floor_met,
    }
    # Precedence: any counted breach => FAIL, evaluated BEFORE the floor
    # rule; the floor only qualifies a breach-free run as PASS vs PARTIAL.
    if not latency_pass or not completeness_pass or hold_pass is False or not jobs_pass:
        return "FAIL", gates
    if not floor_met:
        return "PARTIAL (sample floor unmet: rebuild finished too soon)", gates
    return "PASS", gates


def _resolve_site(db_path: Path, site: int | None) -> int:
    if site is not None:
        return site
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
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
        "--out", type=Path, default=None, help="write the JSON report here"
    )
    args = parser.parse_args()

    if not args.db.exists():
        raise SystemExit(f"database not found: {args.db}")
    site_id = _resolve_site(args.db, args.site)
    mode = _detect_mode()

    from wxverify import __version__

    result = _bench(args.db, site_id, mode)
    verdict, gates = _verdict(result, mode)
    report: dict[str, Any] = {
        "mode": mode,
        "addon_version": __version__,
        "python": sys.version,
        "hardware": {
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "site_id": site_id,
        "note": (
            "in-process ASGI, single event loop shared with the rebuild"
            " (mirrors production); uvicorn/network overhead excluded"
        ),
        "stale_rows_forced": result["stale_rows_forced"],
        "rebuild": result["rebuild"],
        "samples": result["samples"],
        "write_transaction_holds": result["holds"],
        "jobs": result["jobs"],
        "gates": gates,
        "verdict": verdict,
    }

    rendered = json.dumps(report, indent=2)
    if args.out is not None:
        args.out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    print(f"\n§5 gate [{mode}]: {verdict}", file=sys.stderr)
    if verdict == "FAIL":
        return 1
    if verdict.startswith("PARTIAL"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Step 3 tests for the closed-day guard in each writer (plan
2026-10-20-daily-close-day-state-and-guard.md, Sec 13.3-13.4).

``wxverify.db.site_days`` arrives in Step 4 piece 2 of the daily-close plan;
importing names from it here, at module top, is deliberate: it is the
authoritative "not built yet" signal for this whole file -- every test below
collection-errors with the same ``ModuleNotFoundError`` until that module
exists, which is the right reason for all of them to be red right now.

Each GW test seeds the shared helper site (``tests.helpers.day_close_site``,
plan Sec 13.1) with the close on: D-3 closed, D-2 missed, D-1 open, D the
current day. It then calls the writer the way its job or route does, and
checks the shared 13.1 oracle (D-3 unchanged across the six day-state tables,
D-1 written, no exception, exactly one WARNING in the 7.2 form) except where
a GW row names an exception. Only synthetic station ids (``ISTATION01``,
``ISTATION02``), the ``UTC``/``Etc/GMT+-N`` zones and a fixed synthetic date
appear anywhere in this file (public repo).
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import pytest
from starlette.testclient import TestClient

from tests.helpers import (
    NOW,
    D,
    asof_conn,
    asof_make_real_feed,
    day_close_site,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db

# The arrives-in-Step-4 import: see the module docstring above.
from wxverify.db.site_days import (
    enter_zone_change,
)
from wxverify.scoring.consensus import materialize_consensus

_STATION_A: Final = "ISTATION01"
_STATION_B: Final = "ISTATION02"

_GUARD_RE: Final = re.compile(
    r"^closed-day guard refused (?P<kind>\S+) writer=(?P<writer>\S+) "
    r"site=(?P<site>\d+) days=(?P<days>\S*) count=(?P<count>\d+)$"
)

_ORACLE_DAY_TABLES: Final = (
    "station_wind_records",
    "station_observations",
    "observations",
    "daily_truth",
    "forecast_pairs",
    "site_days",
)


# ---------------------------------------------------------------------------
# Shared helpers (private to this file; nothing here is added to
# tests/helpers.py or tests/conftest.py).
# ---------------------------------------------------------------------------


def _station_id(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _guard_warnings(caplog: pytest.LogCaptureFixture) -> list[dict[str, str]]:
    hits = []
    for record in caplog.records:
        if record.levelno != logging.WARNING:
            continue
        match = _GUARD_RE.match(record.getMessage())
        if match:
            hits.append(match.groupdict())
    return hits


def _assert_one_guard_warning(
    caplog: pytest.LogCaptureFixture,
    *,
    kind: str,
    site_id: int,
    days: tuple[date, ...],
    count: int,
) -> None:
    hits = _guard_warnings(caplog)
    assert len(hits) == 1, hits
    hit = hits[0]
    assert hit["kind"] == kind, hit
    assert hit["site"] == str(site_id), hit
    assert hit["count"] == str(count), hit
    assert hit["days"] == ",".join(d.isoformat() for d in days), hit


def _assert_no_guard_warning(caplog: pytest.LogCaptureFixture) -> None:
    assert _guard_warnings(caplog) == []


def _rows(
    conn: sqlite3.Connection, sql: str, params: tuple[object, ...]
) -> list[dict[str, object]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _day_bounds(local_date: date) -> tuple[str, str]:
    start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    return isoformat_utc(start), isoformat_utc(start + timedelta(days=1))


def _d3(d: date = D) -> date:
    return d - timedelta(days=3)


def _d1(d: date = D) -> date:
    return d - timedelta(days=1)


# ---------------------------------------------------------------------------
# GW4: consensus's materialize_consensus, called for a D-3 hour.
# ---------------------------------------------------------------------------


def test_gw4_materialize_consensus_leaves_a_closed_days_hour_alone(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.scoring.consensus.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    d3_hour = f"{_d3().isoformat()}T06:00:00Z"

    feed_id = asof_make_real_feed(conn, "gw4-model")
    # A bare forecast_history row (not tests.helpers.seed_forecast_history,
    # which also seeds observations -- this test's own oracle below needs
    # observations EMPTY at d3_hour, to show the guard refused the write).
    conn.execute(
        "INSERT INTO forecast_history"
        " (site_id, feed_id, variable, day_ahead, valid_at, local_date,"
        "  value, lead_hours, issued_at, contributors)"
        " VALUES (?, ?, 'temperature', 1, ?, ?, 5.0, 6, ?, NULL)",
        (
            site_id,
            feed_id,
            d3_hour,
            _d3().isoformat(),
            f"{_d3().isoformat()}T00:00:00Z",
        ),
    )
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             tz_generation_id)
        VALUES (?, ?, 'temperature_high', 10.0, 1, 24, 24, ?, ?, 'UTC', 1)
        """,
        (
            site_id,
            _d3().isoformat(),
            f"{_d3().isoformat()}T00:00:00Z",
            f"{(_d3() + timedelta(days=1)).isoformat()}T00:00:00Z",
        ),
    )
    conn.commit()

    before_hist = _rows(
        conn,
        "SELECT * FROM forecast_history WHERE site_id = ? ORDER BY feed_id, valid_at",
        (site_id,),
    )
    before_truth = _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, _d3().isoformat()),
    )

    caplog.set_level(logging.WARNING)
    materialize_consensus(
        conn, site_id=site_id, variable="temperature", valid_at=d3_hour
    )
    conn.commit()

    assert before_hist == _rows(
        conn,
        "SELECT * FROM forecast_history WHERE site_id = ? ORDER BY feed_id, valid_at",
        (site_id,),
    )
    assert before_truth == _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, _d3().isoformat()),
    )
    obs_row = conn.execute(
        "SELECT 1 FROM observations WHERE site_id = ? AND valid_at = ?",
        (site_id, d3_hour),
    ).fetchone()
    assert obs_row is None, "the consensus write itself must be refused on D-3"
    _assert_one_guard_warning(
        caplog, kind="HOURLY", site_id=site_id, days=(_d3(),), count=1
    )


# No test here for apply_pair_ops/_apply_split_chunk refusing a closed
# day's ops, or for the real-model/persistence/multimodel pair computes
# proposing no ops for a done or recordless day. Nor for
# real_model_pair_pending's probe.


# ---------------------------------------------------------------------------
# GW11: W12 (PUT /api/sites/{id}).
# ---------------------------------------------------------------------------


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


# No test here for update_site's own DELETE of the open day's
# pairs/scores before the PUT returns: update_site only stores the rain
# threshold; it never deletes, re-pairs or re-scores anything.


# ---------------------------------------------------------------------------
# GW13/GW13b/GW14: W14, import over HTTP, resume, and the new 10.6 checks.
# ---------------------------------------------------------------------------


# No test here for the import's rebuild-only-the-open-day behavior, or
# for the import's bad-case bound checks and paired accepted case.


# ---------------------------------------------------------------------------
# GW16: the one allowed writer, DELETE /api/sites/{id}, after a zone change.
# ---------------------------------------------------------------------------


def test_gw16_delete_site_cascades_every_daily_close_table_after_a_zone_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.db.site_days.utc_now", lambda: NOW, raising=False)
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn)
    enter_zone_change(conn, site_id, "Etc/GMT-1", now_utc=NOW)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        caplog.set_level(logging.WARNING)
        resp = client.delete(f"/api/sites/{site_id}", headers=headers)
        assert resp.status_code == 200

    _assert_no_guard_warning(caplog)
    direct = sqlite3.connect(config.db_path)
    try:
        for table in (
            "site_days",
            "site_day_stations",
            "station_changes",
            "site_zone_changes",
        ):
            remaining = direct.execute(
                f"SELECT 1 FROM {table} WHERE site_id = ?",
                (site_id,),  # noqa: S608
            ).fetchone()
            assert remaining is None, f"{table} must have no row of the deleted site"
    finally:
        direct.close()


# ---------------------------------------------------------------------------
# GW17: the source-scan writer-list pin (plan Sec 13.4).
# ---------------------------------------------------------------------------

_DAY_WRITE_TARGETS: Final = frozenset(
    {
        "station_wind_records",
        "station_observations",
        "observations",
        "daily_truth",
        "forecast_pairs",
        "verification_evidence",
        "site_days",
        "site_day_stations",
        "station_changes",
        "site_zone_changes",
    }
)

_SQL_HEAD_RE: Final = re.compile(
    r"^\s*(INSERT(?:\s+OR\s+\w+)?\s+INTO|REPLACE\s+INTO|DELETE\s+FROM|UPDATE)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*|\{[^}]*\})",
    re.IGNORECASE,
)

# Plan 13.4: "No string holds DELETE FROM stations (8.3)" -- checked over
# every string constant and flattened f-string, not the raw source text.
_DELETE_STATIONS_RE: Final = re.compile(r"DELETE\s+FROM\s+stations\b", re.IGNORECASE)

# Plan 13.4's fourth assertion: outside db/migrations.py, a write to any of
# these lies in db/site_days.py (4.7 and 6.4's state functions).
_STATE_TABLES: Final = frozenset(
    {"site_days", "site_day_stations", "station_changes", "site_zone_changes"}
)

# One entry per writer of plan Sec 7.3/7.4 and per migration that writes
# these tables, each with the reason the plan names for it.
_GW17_ALLOWLIST: Final = frozenset(
    {
        # P2's raw-reading writer, through P1's guard (plan 6.4).
        ("wxverify.db.station_readings", "store_readings"),
        ("wxverify.scoring.consensus", "write_station_observation"),  # 7.3 backstop
        ("wxverify.scoring.consensus", "materialize_consensus"),  # 7.3
        # Daily-close plan 7.7: P3's close, stage 3.
        ("wxverify.worker.day_values", "write_hourly_values"),
        # Daily-close plan 10.3: P3's run_close_retention.
        ("wxverify.db.station_readings", "prune_raw_readings"),
        # P1 7.3 row: P3's writers, TRUTH.
        ("wxverify.verification.truth", "write_close_truth"),
        # No entries for the import's _rebuild_all, for update_site, or for
        # _invalidate_mean_for_site: none of them writes a guarded table.
        # P1 state functions, 4.7 and 6.4; also station_changes (its seed
        # migration constant and migrate_v15_switch's keep_read_generations,
        # which wrote it below v15, are both deleted, storage-release plan
        # 4.1/table 1.B).
        ("wxverify.db.site_days", "*"),
        # health-release plan 2026-11-07 §7 Part G: the owner's one-time
        # wind history prune, run once in the version 18 upgrade, through
        # no guard of its own (the bound is always before the site's first
        # record, so the old write-kind guard case is gone).
        ("wxverify.db.switch", "prune_wind_history"),
        # No entries for _invalidate_consensus_dependents and
        # _delete_future_persistence_pairs, or for the old pair-and-score
        # pipeline's own writers (wxverify.scoring.multimodel, .pairing,
        # .persistence and .split), or for the replay/verification-run
        # layer's own writers (runs.py, simulate.py, cleanup.py): none of
        # them writes a guarded table.
        ("wxverify.verification.truth", "mark_daily_truth_stale"),  # 7.3 notes
    }
)


def test_gw17_writer_source_scan_matches_the_allowlist() -> None:
    import ast

    import wxverify

    root = Path(wxverify.__file__).parent
    hits: set[tuple[str, str]] = set()
    hit_targets: dict[tuple[str, str], set[str]] = {}
    stale_module_calls: list[str] = []
    state_table_modules: set[str] = set()

    for py_file in sorted(root.rglob("*.py")):
        module = ".".join(
            ("wxverify", *py_file.relative_to(root).with_suffix("").parts)
        )
        if module.endswith(".__init__"):
            module = module[: -len(".__init__")]
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))

        def _enclosing(n: ast.AST, tree: ast.AST) -> str:
            for parent in ast.walk(tree):
                if isinstance(
                    parent, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and n in ast.walk(parent):
                    return parent.name
            # Not inside any function: name the module-level constant whose
            # value holds it, if any (plan 13.4: "enclosing function or
            # module-level constant").
            for stmt in ast.iter_child_nodes(tree):
                if isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
                    if n in ast.walk(stmt.value) and isinstance(stmt.target, ast.Name):
                        return stmt.target.id
                elif isinstance(stmt, ast.Assign) and (
                    stmt.value is not None and n in ast.walk(stmt.value)
                ):
                    for target in stmt.targets:
                        if isinstance(target, ast.Name):
                            return target.id
            return "<module>"

        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not _DELETE_STATIONS_RE.search(node.value), (
                    f"{module}: must never string-delete the stations table (8.3)"
                )
                match = _SQL_HEAD_RE.match(node.value)
                if match:
                    target = match.group(2)
                    if target in _STATE_TABLES:
                        state_table_modules.add(module)
                    if (
                        target in _DAY_WRITE_TARGETS
                        or (
                            target == "station_wind_days"
                            and "refetch_at" in node.value
                            and match.group(1).upper() == "UPDATE"
                        )
                        or target.startswith("{")
                    ):
                        fn = _enclosing(node, tree)
                        hits.add((module, fn))
                        hit_targets.setdefault((module, fn), set()).add(target)
            if isinstance(node, ast.JoinedStr):
                flat = "".join(
                    "{}" if isinstance(piece, ast.FormattedValue) else piece.value
                    for piece in node.values
                    if isinstance(piece, (ast.Constant, ast.FormattedValue))
                )
                assert not _DELETE_STATIONS_RE.search(flat), (
                    f"{module}: must never string-delete the stations table (8.3)"
                )
                match = _SQL_HEAD_RE.match(flat)
                if match:
                    target = match.group(2)
                    if target in _STATE_TABLES:
                        state_table_modules.add(module)
                    if target in _DAY_WRITE_TARGETS or target.startswith("{"):
                        fn = _enclosing(node, tree)
                        hits.add((module, fn))
                        hit_targets.setdefault((module, fn), set()).add(target)
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "mark_daily_truth_stale"
            ):
                stale_module_calls.append(f"{module}.{_enclosing(node, tree)}")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and (node.func.id == "mark_daily_truth_stale")
            ):
                stale_module_calls.append(f"{module}.{_enclosing(node, tree)}")

    wildcard_modules = {module for module, fn in _GW17_ALLOWLIST if fn == "*"}
    for module in sorted(wildcard_modules):
        relative = module.removeprefix("wxverify.").replace(".", "/")
        module_path = root / f"{relative}.py"
        assert module_path.exists(), (
            f"wildcard allowlist entry {module!r} has no module at {module_path}"
        )
    checked_hits = {
        (module, fn)
        for module, fn in hits
        if module not in wildcard_modules
        or (
            module == "wxverify.db.site_days"
            and not hit_targets.get((module, fn), set()).issubset(_STATE_TABLES)
        )
    }
    unreviewed = sorted(checked_hits - _GW17_ALLOWLIST)
    assert not unreviewed, f"unreviewed daily-close table writers: {unreviewed}"

    listed = {(module, fn) for module, fn in _GW17_ALLOWLIST if fn != "*"}
    disappeared = sorted(listed - hits)
    assert not disappeared, f"listed writers disappeared: {disappeared}"

    assert sorted(stale_module_calls) == [
        "wxverify.scoring.consensus.materialize_consensus",
    ]

    offending_state_modules = sorted(
        state_table_modules - {"wxverify.db.migrations", "wxverify.db.site_days"}
    )
    assert not offending_state_modules, (
        f"site_days/site_day_stations/station_changes/site_zone_changes written "
        f"outside db.site_days: {offending_state_modules}"
    )

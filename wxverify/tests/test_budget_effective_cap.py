"""Tests: the weathercom provisional-allowance effective cap, T111/T113-T115.

`collection/budget.py`'s ``effective_daily_call_limit`` clamps the stored
``sources.daily_call_limit`` at ``PROVIDER_DAILY_CALL_ALLOWANCE`` (weathercom:
1500) on EVERY read, so the stored/configured value is never raised past it
by any writer. §8.6 names five call sites that must apply the clamp:
``reserve_budget`` (T111), ``monitor._budget_conditions``,
``web/context.load_budgets``, ``provider_ops._provider_group`` and
``api/routes/health.health_budget`` (T113), plus an AST pin (T114) proving no
sixth reader exists, and the boot write (T115) that stores the configured
value unclamped and logs a WARNING only when it bites.

Isolation: T111/T113 (non-HTTP readers) use an in-memory sqlite3 connection
+ ``run_migrations`` + ``seed_default_sources``; T113's ops-page/health-route
assertions and T115's boot assertions drive a real app via ``TestClient``
against a ``tmp_path`` file database with an idle worker stand-in, mirroring
``tests/test_sm6_config_options.py``. Synthetic data only (public repo):
site name "Testsite", RFC-5737 n/a (no feed addresses needed), fake feed
model "ci-stub".
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.collection.budget import (
    PROVIDER_DAILY_CALL_ALLOWANCE,
    _billing_day,
    effective_daily_call_limit,
    reserve_budget,
    set_source_cap,
)
from wxverify.db.connection import close_db
from wxverify.db.migrations import run_migrations, seed_default_sources
from wxverify.monitor import _budget_conditions  # noqa: PLC2701
from wxverify.worker.control import JobDeferred

_ALLOWANCE = PROVIDER_DAILY_CALL_ALLOWANCE["weathercom"]  # 1500
assert _ALLOWANCE == 1500, "this suite's fixture math assumes the 1500 allowance"

_WXVERIFY_SRC = Path(__file__).resolve().parent.parent / "wxverify"


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    seed_default_sources(conn)
    conn.commit()
    return conn


def _fill_budget(conn: sqlite3.Connection, source: str, calls: int) -> None:
    row = conn.execute(
        "SELECT billing_tz FROM sources WHERE source = ?", (source,)
    ).fetchone()
    assert row is not None
    day = _billing_day(str(row["billing_tz"]))
    conn.execute(
        "INSERT OR REPLACE INTO api_budget (source, billing_day, calls, credits)"
        " VALUES (?, ?, ?, 0)",
        (source, day, calls),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# T111 -- reserve_budget enforces the effective cap
# ---------------------------------------------------------------------------


def test_reserve_budget_succeeds_just_below_effective_cap() -> None:
    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=3000)
    conn.commit()
    _fill_budget(conn, "weathercom", 1499)

    reservation = reserve_budget(conn, "weathercom", 1)

    assert reservation.calls == 1
    row = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom' AND billing_day=?",
        (reservation.billing_day,),
    ).fetchone()
    assert int(row["calls"]) == 1500


def test_reserve_budget_defers_at_effective_cap_stored_3000() -> None:
    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=3000)
    conn.commit()
    _fill_budget(conn, "weathercom", 1500)

    with pytest.raises(JobDeferred):
        reserve_budget(conn, "weathercom", 1)

    row = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    assert int(row["calls"]) == 1500, "a deferred reservation must not touch calls"


def test_reserve_budget_still_defers_at_1500_after_raising_stored_to_5000() -> None:
    """Kills: the allowance used in place of the stored value (a later
    `set_source_cap` to 5000 would then compute `min(stored, allowance)`
    differently only if the clamp were removed; this proves raising the
    stored value past the allowance changes nothing observable).
    """
    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=3000)
    conn.commit()
    _fill_budget(conn, "weathercom", 1500)
    set_source_cap(conn, "weathercom", daily_call_limit=5000)
    conn.commit()

    with pytest.raises(JobDeferred):
        reserve_budget(conn, "weathercom", 1)


def test_reserve_budget_respects_a_stored_cap_below_the_allowance() -> None:
    """Stored 1200 (below the 1500 allowance): 1199 succeeds, 1200 defers.

    Kills: the allowance used in place of the stored value (would wrongly
    let 1200 reserve, since 1200 < 1500).
    """
    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=1200)
    conn.commit()
    _fill_budget(conn, "weathercom", 1199)

    reserve_budget(conn, "weathercom", 1)  # must not raise

    _fill_budget(conn, "weathercom", 1200)
    with pytest.raises(JobDeferred):
        reserve_budget(conn, "weathercom", 1)


def test_reserve_budget_open_meteo_at_its_own_seeded_limit_still_reserves() -> None:
    """open-meteo carries no provisional allowance entry, so 1500 calls today
    (which WOULD trip weathercom's effective cap) is nowhere near its own
    10000 seeded limit.

    Kills: the allowance applied to every source (would wrongly defer
    open-meteo here too).
    """
    conn = _make_db()
    row = conn.execute(
        "SELECT daily_call_limit FROM sources WHERE source = 'open-meteo'"
    ).fetchone()
    assert int(row["daily_call_limit"]) == 10000
    assert "open-meteo" not in PROVIDER_DAILY_CALL_ALLOWANCE
    _fill_budget(conn, "open-meteo", 1500)

    reserve_budget(conn, "open-meteo", 1)  # must not raise


# ---------------------------------------------------------------------------
# T113 -- every reader reports the effective cap
# ---------------------------------------------------------------------------


def test_budget_conditions_trips_at_effective_cap_not_configured() -> None:
    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=3000)
    conn.commit()
    _fill_budget(conn, "weathercom", 1500)

    conditions = _budget_conditions(conn, now=None)  # type: ignore[arg-type]
    calls_cond = next(c for c in conditions if c.id == "budget_calls")

    assert calls_cond.ok is False
    assert calls_cond.count == 1
    assert calls_cond.detail is not None
    assert "weathercom" in calls_cond.detail
    assert "1500" in calls_cond.detail
    assert "3000" in calls_cond.detail


def test_budget_conditions_not_tripped_one_call_below_effective_cap() -> None:
    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=3000)
    conn.commit()
    _fill_budget(conn, "weathercom", 1499)

    conditions = _budget_conditions(conn, now=None)  # type: ignore[arg-type]
    calls_cond = next(c for c in conditions if c.id == "budget_calls")

    assert calls_cond.ok is True
    assert calls_cond.count == 0
    assert calls_cond.detail is None


def test_load_budgets_reports_effective_and_configured_separately() -> None:
    from wxverify.web.context import load_budgets

    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=3000)
    conn.commit()
    _fill_budget(conn, "weathercom", 1500)

    gauges = load_budgets(conn)
    weathercom = next(g for g in gauges if g.source == "weathercom")

    assert weathercom.daily_call_limit == 1500
    assert weathercom.configured_call_limit == 3000


def test_load_budgets_no_clamp_note_when_stored_below_allowance() -> None:
    from wxverify.web.context import load_budgets

    conn = _make_db()
    set_source_cap(conn, "weathercom", daily_call_limit=1200)
    conn.commit()

    gauges = load_budgets(conn)
    weathercom = next(g for g in gauges if g.source == "weathercom")

    assert weathercom.daily_call_limit == 1200
    assert weathercom.configured_call_limit == 1200


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _boot(tmp_path: Path, name: str, monkeypatch: pytest.MonkeyPatch) -> object:
    close_db()
    config.db_path = str(tmp_path / name)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def test_ops_page_shows_effective_cap_and_configured_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "cap_note.db", monkeypatch)
    with TestClient(app) as client:
        from wxverify.db.connection import get_db

        db = get_db()

        def _setup(conn: sqlite3.Connection) -> None:
            set_source_cap(conn, "weathercom", daily_call_limit=3000)
            _fill_budget(conn, "weathercom", 1500)

        db.write_sync(_setup)
        html = client.get("/ops").text

    assert "1500 / 1500 calls" in html
    assert "configured 3000" in html


def test_ops_page_has_no_clamp_note_when_stored_below_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "cap_no_note.db", monkeypatch)
    with TestClient(app) as client:
        from wxverify.db.connection import get_db

        db = get_db()

        def _setup(conn: sqlite3.Connection) -> None:
            set_source_cap(conn, "weathercom", daily_call_limit=1200)

        db.write_sync(_setup)
        html = client.get("/ops").text

    assert "configured" not in html.split("weathercom")[1].split("</div>")[0], (
        "no clamp note expected when configured == effective"
    )


def test_health_budget_route_reports_effective_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "cap_health.db", monkeypatch)
    with TestClient(app) as client:
        from wxverify.db.connection import get_db

        db = get_db()

        def _setup(conn: sqlite3.Connection) -> None:
            set_source_cap(conn, "weathercom", daily_call_limit=3000)
            _fill_budget(conn, "weathercom", 1500)

        db.write_sync(_setup)
        payload = client.get("/api/health/budget").json()

    weathercom = next(row for row in payload if row["source"] == "weathercom")
    assert weathercom["daily_call_limit"] == 1500


def test_provider_health_reports_effective_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.provider_ops import provider_health

    app = _boot(tmp_path, "cap_provider_health.db", monkeypatch)
    with TestClient(app):
        from wxverify.db.connection import get_db

        db = get_db()

        def _setup(conn: sqlite3.Connection) -> tuple[int, int]:
            set_source_cap(conn, "weathercom", daily_call_limit=3000)
            _fill_budget(conn, "weathercom", 1500)
            site_cur = conn.execute(
                "INSERT INTO sites (name, forecast_lat, forecast_lon,"
                " elevation_m, timezone) VALUES ('Testsite', 0.0, 0.0, 0.0,"
                " 'Etc/UTC')"
            )
            assert site_cur.lastrowid is not None
            feed_cur = conn.execute(
                "INSERT INTO feeds (source, model, enabled, default_subscribed,"
                " fetch_interval_minutes, is_virtual)"
                " VALUES ('weathercom', 'ci-stub', 1, 1, 60, 0)"
            )
            assert feed_cur.lastrowid is not None
            conn.commit()
            return int(site_cur.lastrowid), int(feed_cur.lastrowid)

        site_id, _feed_id = db.write_sync(_setup)

        def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
            return provider_health(conn, site_id=site_id, window_start="2026-01-01")

        groups = db.write_sync(_read)

    weathercom_feed = next(
        f
        for g in groups
        for f in [g]
        if g["source"] == "weathercom"  # type: ignore[index]
    )
    budget = weathercom_feed["budget"]  # type: ignore[index]
    assert isinstance(budget, dict)
    assert budget["daily_call_limit"] == 1500


# ---------------------------------------------------------------------------
# T114 -- source-shape pin: every daily_call_limit reader calls the helper
# ---------------------------------------------------------------------------

_EXPECTED_READERS = {
    ("collection/budget.py", "reserve_budget"),
    ("monitor.py", "_budget_conditions"),
    ("web/context.py", "load_budgets"),
    ("provider_ops.py", "_provider_group"),
    ("api/routes/health.py", "health_budget"),
}


def _subscripts_daily_call_limit(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if (
            isinstance(sub, ast.Subscript)
            and isinstance(sub.slice, ast.Constant)
            and sub.slice.value == "daily_call_limit"
        ):
            return True
    return False


def _calls_effective_helper(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            func = sub.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if name == "effective_daily_call_limit":
                return True
    return False


def test_daily_call_limit_readers_are_exactly_the_named_five() -> None:
    found: set[tuple[str, str]] = set()
    missing_helper: list[str] = []
    for path in sorted(_WXVERIFY_SRC.rglob("*.py")):
        if "/tests/" in str(path):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = str(path.relative_to(_WXVERIFY_SRC))
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if _subscripts_daily_call_limit(node):
                found.add((rel, node.name))
                if not _calls_effective_helper(node):
                    missing_helper.append(f"{rel}:{node.name}")

    assert found == _EXPECTED_READERS, (
        f"daily_call_limit readers changed: found={found} expected="
        f"{_EXPECTED_READERS}; a new reader must call effective_daily_call_limit"
    )
    assert missing_helper == [], (
        f"these readers subscript daily_call_limit but never call "
        f"effective_daily_call_limit: {missing_helper}"
    )


# ---------------------------------------------------------------------------
# T115 -- the boot write: configured value stored unclamped, WARNING only
# when it bites
# ---------------------------------------------------------------------------


def _write_options_json(path: Path, data: dict[str, object]) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _boot_with_cap(
    tmp_path: Path, name: str, cap: int, monkeypatch: pytest.MonkeyPatch
) -> int:
    close_db()
    config.db_path = str(tmp_path / name)
    options_file = tmp_path / f"{name}.options.json"
    _write_options_json(options_file, {"weathercom_daily_call_limit": cap})
    config.options_path = str(options_file)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app):
        from wxverify.db.connection import get_db

        db = get_db()

        def _read(conn: sqlite3.Connection) -> int:
            row = conn.execute(
                "SELECT daily_call_limit FROM sources WHERE source='weathercom'"
            ).fetchone()
            assert row is not None
            return int(row["daily_call_limit"])

        return db.write_sync(_read)


def test_boot_stores_configured_3000_unclamped_and_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="wxverify.api.app"):
        stored = _boot_with_cap(tmp_path, "boot_3000", 3000, monkeypatch)

    assert stored == 3000, "the stored value must stay the configured one, unclamped"
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == "wxverify.api.app"
    ]
    joined = " ".join(r.getMessage() for r in warnings)
    assert "3000" in joined
    assert "1500" in joined


def test_boot_stores_configured_1500_no_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="wxverify.api.app"):
        stored = _boot_with_cap(tmp_path, "boot_1500", 1500, monkeypatch)

    assert stored == 1500
    assert not any(
        r.levelno == logging.WARNING and r.name == "wxverify.api.app"
        for r in caplog.records
    )


def test_boot_stores_configured_1200_no_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="wxverify.api.app"):
        stored = _boot_with_cap(tmp_path, "boot_1200", 1200, monkeypatch)

    assert stored == 1200
    assert not any(
        r.levelno == logging.WARNING and r.name == "wxverify.api.app"
        for r in caplog.records
    )


def test_effective_daily_call_limit_pure_function_clamps_only_the_named_source() -> (
    None
):
    assert effective_daily_call_limit("weathercom", 3000) == 1500
    assert effective_daily_call_limit("weathercom", 1200) == 1200
    assert effective_daily_call_limit("open-meteo", 999999) == 999999

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.options import (
    _env_bool,
    load_runtime_options,
)
from wxverify.db.connection import close_db
from wxverify.monitor import Condition, _grace_active


async def _idle_worker_async(db: object) -> None:  # keep the real worker idle
    await asyncio.Event().wait()


def test_runtime_options_toggles_default_true_and_read_from_options_json(
    tmp_path: Path,
) -> None:
    # Default (no options.json → env fallback with nothing set): all True.
    config.options_path = str(tmp_path / "missing-options.json")
    defaults = load_runtime_options()
    assert defaults.monitor_pipeline is True
    assert defaults.monitor_budget is True
    assert defaults.monitor_db is True

    # Real _from_options_json path: monitor_budget=false flips exactly that one.
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"monitor_budget": False}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    loaded = load_runtime_options()
    assert loaded.monitor_pipeline is True
    assert loaded.monitor_budget is False
    assert loaded.monitor_db is True


def test_env_bool_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WXV_MONITOR_PIPELINE", raising=False)
    assert _env_bool("WXV_MONITOR_PIPELINE") is None
    # Empty string (WXV_MONITOR_PIPELINE=) is a distinct operator state — also None.
    monkeypatch.setenv("WXV_MONITOR_PIPELINE", "")
    assert _env_bool("WXV_MONITOR_PIPELINE") is None
    monkeypatch.setenv("WXV_MONITOR_PIPELINE", "false")
    assert _env_bool("WXV_MONITOR_PIPELINE") is False
    monkeypatch.setenv("WXV_MONITOR_PIPELINE", "true")
    assert _env_bool("WXV_MONITOR_PIPELINE") is True
    monkeypatch.setenv("WXV_MONITOR_PIPELINE", "0")
    assert _env_bool("WXV_MONITOR_PIPELINE") is False
    monkeypatch.setenv("WXV_MONITOR_PIPELINE", "1")
    assert _env_bool("WXV_MONITOR_PIPELINE") is True


def test_env_override_flips_toggle_to_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prove the _from_env wiring: `_env_bool(...) is not False` evaluates to False
    # when the env var is explicitly set to a falsy value.  No options.json exists,
    # so load_runtime_options() falls through to _from_env().  Setting one var to
    # "false" must flip exactly that toggle; the other two (unset) stay True.
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.delenv("WXV_MONITOR_PIPELINE", raising=False)
    monkeypatch.delenv("WXV_MONITOR_BUDGET", raising=False)
    monkeypatch.setenv("WXV_MONITOR_DB", "false")
    opts = load_runtime_options()
    assert opts.monitor_pipeline is True
    assert opts.monitor_budget is True
    assert opts.monitor_db is False


def test_config_yaml_declares_monitor_toggles() -> None:
    repo = Path(__file__).resolve().parents[1]
    config_yaml = (repo / "config.yaml").read_text(encoding="utf-8")
    # options block defaults
    assert "monitor_pipeline: true" in config_yaml
    assert "monitor_budget: true" in config_yaml
    assert "monitor_db: true" in config_yaml
    # schema block types
    assert "monitor_pipeline: bool" in config_yaml
    assert "monitor_budget: bool" in config_yaml
    assert "monitor_db: bool" in config_yaml


def test_monitor_endpoint_envelope_always_200_and_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "monitor-envelope.db")
    # Disable budget via the REAL options path (not a monkeypatch of the read).
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"monitor_budget": False}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        resp = client.get("/api/health/monitor")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body) == {
            "overall",
            "generated_at",
            "grace_active",
            "conditions",
        }
        assert body["overall"] in ("ok", "warning", "critical")
        by_group = {c["group"] for c in body["conditions"]}
        # budget group disabled → every budget condition is skipped
        budget_conds = [c for c in body["conditions"] if c["group"] == "budget"]
        assert budget_conds  # budget conditions still listed
        assert all(c["skipped"] is True for c in budget_conds)
        # a skipped condition never contributes to overall
        assert "budget" in by_group


def test_monitor_endpoint_db_failure_reports_critical_not_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "monitor-dberr.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        # Force every group's read to raise sqlite3.Error by breaking the reader.
        import wxverify.monitor as monitor_mod

        # **kwargs so the patch is call-compatible with all three targets,
        # incl. _pipeline_conditions(conn, now, *, grace_active).
        def _boom(conn: sqlite3.Connection, now: object, **_: object) -> object:
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(monitor_mod, "_pipeline_conditions", _boom)
        monkeypatch.setattr(monitor_mod, "_budget_conditions", _boom)
        monkeypatch.setattr(monitor_mod, "_db_conditions", _boom)
        resp = client.get("/api/health/monitor")
        assert resp.status_code == 200
        body = resp.json()
        assert body["overall"] == "critical"
        db_cond = next(c for c in body["conditions"] if c["id"] == "db_readable")
        assert db_cond["ok"] is False
        assert db_cond["skipped"] is False


def test_monitor_endpoint_non_sqlite3_error_is_critical_via_outer_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A non-sqlite3 exception (KeyError/ValueError) from a group function must
    # NOT be swallowed by the narrow inner `except sqlite3.Error`; it must escape
    # build_verdict and be mapped to the always-200 critical verdict by the route's
    # OUTER try/except Exception — reported as `unexpected_error`, NOT falsely as
    # `db_readable:false`. This pins two things at once: (a) the always-200
    # invariant holds against the whole call graph, and (b) the inner catch stays
    # narrow (a future widening to `except Exception` would turn this into a
    # `db_readable:false`, flipping the assertions below and failing the test).
    close_db()
    config.db_path = str(tmp_path / "monitor-nonsqlite.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        import wxverify.monitor as monitor_mod

        def _boom_value(conn: sqlite3.Connection, now: object, **_: object) -> object:
            raise ValueError("not a sqlite3 error")

        # Inject into the budget group specifically (mirrors the real H2 path:
        # resolve_secret raising a non-sqlite3 error inside _budget_conditions).
        monkeypatch.setattr(monitor_mod, "_budget_conditions", _boom_value)
        resp = client.get("/api/health/monitor")
        assert resp.status_code == 200
        body = resp.json()
        assert body["overall"] == "critical"
        # The failure is reported via the dedicated outer-guard condition, which
        # is DISTINCT from db_readable — proving the narrow inner catch did not
        # mislabel a non-DB error as an unreadable database.
        ids = {c["id"] for c in body["conditions"]}
        assert "unexpected_error" in ids
        assert "db_readable" not in ids  # no false db_readable:false emitted
        err = next(c for c in body["conditions"] if c["id"] == "unexpected_error")
        assert err["ok"] is False
        assert err["skipped"] is False
        assert err["severity"] == "critical"


def test_condition_as_dict_omits_count_and_detail_when_none() -> None:
    # Pin Condition.as_dict() serialization contract: count/detail must be absent
    # when None so the JSON envelope never ships null keys to the HA dashboard.
    # Paired: when both are set, both keys appear with their values.
    skipped_cond = Condition(
        id="feed_stale", group="pipeline", ok=True, skipped=True, severity="warning"
    )
    d = skipped_cond.as_dict()
    assert set(d.keys()) == {"id", "group", "ok", "skipped", "severity"}
    assert "count" not in d
    assert "detail" not in d

    # Positive: present values appear in the dict.
    failure_cond = Condition(
        id="db_readable",
        group="db",
        ok=False,
        skipped=False,
        severity="critical",
        count=3,
        detail="database read raised sqlite3.Error",
    )
    d2 = failure_cond.as_dict()
    assert d2["count"] == 3
    assert d2["detail"] == "database read raised sqlite3.Error"


def _make_runtime_state_conn() -> sqlite3.Connection:
    """Minimal in-memory connection with just the runtime_state table.

    _grace_active only queries runtime_state, so the full migration schema is
    not needed here — a lean connection keeps the test fast and the setup
    obvious.  The updated_at DEFAULT expression is omitted (sqlite3 stdlib
    rejects non-constant column defaults); it is not queried by _grace_active.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE runtime_state"
        " (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)"
    )
    return conn


def test_grace_active_corrupt_started_at_degrades_to_false() -> None:
    # Invariant 4: a non-ISO worker_started_at must not raise — _grace_active
    # must return False (grace inactive) and let build_verdict continue normally.
    # A corrupt value slipping into the DB (e.g. manual edit or future migration
    # bug) would otherwise escape as a ValueError, which the narrow inner
    # `except sqlite3.Error` does NOT catch; the outer guard would then mask all
    # conditions with a single unexpected_error — more opaque than grace-inactive.
    now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    conn = _make_runtime_state_conn()

    conn.execute(
        "INSERT INTO runtime_state(key, value)"
        " VALUES ('worker_started_at', 'NOT-AN-ISO-TIMESTAMP')"
    )
    assert _grace_active(conn, now) is False  # no raise, degrades gracefully

    # Paired positive: a valid recent timestamp within GRACE_MINUTES returns True.
    conn.execute(
        "UPDATE runtime_state SET value = '2026-07-09T11:55:00Z'"
        " WHERE key = 'worker_started_at'"
    )
    assert _grace_active(conn, now) is True  # grace window is active

    # And a timestamp outside the window returns False (confirms the boundary).
    conn.execute(
        "UPDATE runtime_state SET value = '2026-07-09T11:40:00Z'"
        " WHERE key = 'worker_started_at'"
    )
    assert _grace_active(conn, now) is False  # grace expired

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn, asof_make_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.options import (
    _env_bool,
    load_runtime_options,
)
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import close_db, get_db
from wxverify.db.migrations import create_schema
from wxverify.monitor import (
    _CURRENT_OBS_FAILURE_COOLDOWN,  # noqa: PLC2701
    OBS_COLLECTION_DELAY_MINUTES,
    Condition,
    _grace_active,
    build_verdict,
)
from wxverify.worker.processor import _maybe_stamp_runtime_heartbeat
from wxverify.worker.scheduler import (
    _DUE_JOB_FAILURE_COOLDOWN,  # noqa: PLC2701
)


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
    options_path.write_text(json.dumps({"monitor_budget": False}), encoding="utf-8")
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


def test_readme_monitoring_section_rewritten() -> None:
    repo = Path(__file__).resolve().parents[1]
    readme = (repo / "README.md").read_text(encoding="utf-8")
    monitoring = readme.split("## Monitoring", 1)[1].split("\n## ", 1)[0]
    # The false claim is gone: no `watchdog:` supervision key wired to an
    # endpoint. The bare token is intentionally allowed so the prose can keep
    # its accurate "there is no `watchdog:` entry in config.yaml" clarification.
    assert not any(
        "watchdog:" in line and "://" in line
        for line in monitoring.lower().splitlines()
    )
    # Real supervision + the new surface are documented.
    assert "HEALTHCHECK" in monitoring
    assert "/api/health/monitor" in monitoring
    assert "unavailable" in monitoring
    # The HA package (sensor + both automations) is inlined here, not a separate file.
    assert "rest:" in monitoring
    assert "-wxverify:8099" in monitoring
    assert "local-wxverify" in monitoring
    assert "value_json.overall" in monitoring
    assert "persistent_notification.create" in monitoring
    assert "notify.mobile_app_" in monitoring
    assert monitoring.count("alias:") >= 2  # degraded + recovered


def test_monitor_endpoint_envelope_always_200_and_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "monitor-envelope.db")
    # Disable budget via the REAL options path (not a monkeypatch of the read).
    options_path = tmp_path / "options.json"
    options_path.write_text(json.dumps({"monitor_budget": False}), encoding="utf-8")
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


# --- Task 4: Group 1 staleness + liveness conditions -------------------------


def _seed_site(conn: sqlite3.Connection, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES ('S', 40.0, -105.0, 900.0, 'UTC', ?)
            """,
            (enabled,),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
        ).fetchone()["id"]
    )


def _set_feed_state(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    *,
    last_run_at: str | None,
    enabled: int | None = None,
    last_error: str | None = None,
    error_count: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, enabled, last_run_at, last_error, error_count)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            enabled=excluded.enabled,
            last_run_at=excluded.last_run_at,
            last_error=excluded.last_error,
            error_count=excluded.error_count
        """,
        (site_id, feed_id, enabled, last_run_at, last_error, error_count),
    )


def _cond(body: dict[str, object], cond_id: str) -> dict[str, object]:
    conditions = body["conditions"]
    assert isinstance(conditions, list)
    return next(c for c in conditions if c["id"] == cond_id)


def test_feed_stale_trips_on_eligible_null_last_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "feed-stale.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            # Move worker_started_at into the past so grace is NOT active.
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
            # eligible feed, NULL last_run_at → stale
            _set_feed_state(conn, site_id, om, last_run_at=None)
            # Unsubscribe the other six default-subscribed open-meteo models so
            # exactly one eligible stale row remains and count == 1 is valid.
            for sibling in (
                "gfs_global",
                "icon_global",
                "gem_global",
                "meteofrance_arpege_world",
                "jma_gsm",
                "ukmo_global_deterministic_10km",
            ):
                _set_feed_state(
                    conn,
                    site_id,
                    _feed_id(conn, "open-meteo", sibling),
                    last_run_at=None,
                    enabled=0,
                )
            # virtual feed subscribed but excluded by predicate
            virt = _feed_id(conn, "virtual", "_persistence")
            _set_feed_state(conn, site_id, virt, last_run_at=None, enabled=1)

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        feed_stale = _cond(body, "feed_stale")
        assert feed_stale["skipped"] is False
        assert feed_stale["ok"] is False
        assert feed_stale["count"] == 1
        assert body["grace_active"] is False


def test_feed_stale_grace_suppresses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "feed-stale-grace.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            # worker_started_at is stamped ~now by lifespan → grace active.
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
            _set_feed_state(conn, site_id, om, last_run_at=None)

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert body["grace_active"] is True
        assert _cond(body, "feed_stale")["ok"] is True


def test_liveness_no_eligible_work_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "liveness-empty.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # No sites at all → no eligible feed, no eligible obs target.

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        for cid in ("fetch_obs_live", "fetch_feed_live", "pair_score_live"):
            assert _cond(body, cid)["ok"] is True


def test_liveness_disabled_site_and_unsubscribed_feed_do_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "liveness-disabled.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # Disabled site with an enabled station on it. `s.enabled=1` gates
            # every eligible-work count, so an enabled station under a disabled
            # site is NOT eligible work — liveness must stay ok even though a
            # row physically exists (proves the gate, not merely an empty table).
            site_id = _seed_site(conn, enabled=0)
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'FAKE2', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        for cid in ("fetch_obs_live", "fetch_feed_live", "pair_score_live"):
            assert _cond(body, cid)["ok"] is True


def test_fetch_feed_live_trips_when_eligible_feed_but_no_recent_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "feed-live-trip.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _seed_site(conn)
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            _set_feed_state(
                conn,
                site_id,
                _feed_id(conn, "open-meteo", "ecmwf_ifs"),
                last_run_at="2026-07-08T00:00:00Z",
            )
            # No completed fetch_feed job at all → live check trips.
            return site_id

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "fetch_feed_live")["ok"] is False
        assert _cond(body, "pair_score_live")["ok"] is False


def test_obs_stale_uses_sites_last_obs_at(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "obs-stale.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'FAKE1', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )
            # last_obs_at NULL → never-observed → stale.

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "obs_stale")["ok"] is False
        assert _cond(body, "fetch_obs_live")["ok"] is False


def test_obs_stale_at_cutoff_boundary_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Threshold boundary: a site with last_obs_at exactly equal to obs_cutoff
    # must NOT count, because the SQL uses `s.last_obs_at < ?` (strict). A
    # mutant that widened this to `<=` would count the at-boundary site and
    # fail this assertion. Paired with the "one second earlier" test below to
    # pin both sides of the `<` operator.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "obs-stale-at-boundary.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # Eligible site: enabled, with >=1 enabled station.
            site_id = _seed_site(conn)
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'FAKE2', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )
            # obs_cutoff = fixed_now - 12h = 2026-07-09T00:00:00Z exactly.
            conn.execute(
                "UPDATE sites SET last_obs_at='2026-07-09T00:00:00Z' WHERE id=?",
                (site_id,),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "obs_stale")["ok"] is True
        assert _cond(body, "obs_stale")["count"] == 0


def test_obs_stale_one_second_past_cutoff_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Paired: a site with last_obs_at one second OLDER than obs_cutoff must
    # count. Together with the at-boundary test above this pins both sides of
    # the `<` operator so a misplaced `<=` or a reversed comparison would be
    # caught by at least one of the pair.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "obs-stale-past-boundary.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # Eligible site: enabled, with >=1 enabled station.
            site_id = _seed_site(conn)
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'FAKE2', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )
            # obs_cutoff = 2026-07-09T00:00:00Z;
            # 2026-07-08T23:59:59Z is 1 second older → stale.
            conn.execute(
                "UPDATE sites SET last_obs_at='2026-07-08T23:59:59Z' WHERE id=?",
                (site_id,),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "obs_stale")["ok"] is False
        assert _cond(body, "obs_stale")["count"] == 1


def test_feed_stale_old_timestamp_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the `sfs.last_run_at < ?` arm of the feed_stale predicate.  The NULL
    # arm is covered by test_feed_stale_trips_on_eligible_null_last_run; this
    # test targets the second disjunct so a future edit removing the `< ?` clause
    # would leave an eligible stale feed uncounted and fail this assertion.
    close_db()
    config.db_path = str(tmp_path / "feed-stale-old-ts.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # A stale-but-not-null last_run_at (year 2000 → far past cutoff).
            _set_feed_state(
                conn,
                site_id,
                _feed_id(conn, "open-meteo", "ecmwf_ifs"),
                last_run_at="2000-01-01T00:00:00Z",
            )
            # Unsubscribe siblings so count is deterministically 1.
            for sibling in (
                "gfs_global",
                "icon_global",
                "gem_global",
                "meteofrance_arpege_world",
                "jma_gsm",
                "ukmo_global_deterministic_10km",
            ):
                _set_feed_state(
                    conn,
                    site_id,
                    _feed_id(conn, "open-meteo", sibling),
                    last_run_at=None,
                    enabled=0,
                )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        feed_stale = _cond(body, "feed_stale")
        assert feed_stale["ok"] is False
        assert feed_stale["count"] == 1


def test_meteoblue_member_feed_excluded_from_staleness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the `NOT (f.source='meteoblue' AND f.model != 'multimodel')` clause
    # of _ELIGIBLE_FEED_WHERE, which mirrors the byte-identical predicate in
    # scheduler.py/feed_fetch.py.  No seeded feed row exercises this clause
    # (the only seeded meteoblue row is 'multimodel', which is already excluded
    # by COALESCE=0).  This test injects a non-multimodel meteoblue member feed
    # directly, subscribes it with enabled=1, and asserts it does NOT trip
    # feed_stale — paired with an open-meteo feed that DOES trip, so a predicate
    # edit removing the meteoblue exclusion would inflate count to 2 and fail.
    close_db()
    config.db_path = str(tmp_path / "meteoblue-member.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # One eligible open-meteo feed to create a real staleness condition
            # (count must be exactly 1 — the meteoblue member must not add to it).
            _set_feed_state(
                conn,
                site_id,
                _feed_id(conn, "open-meteo", "ecmwf_ifs"),
                last_run_at=None,
            )
            # Unsubscribe open-meteo siblings so they don't inflate the count.
            for sibling in (
                "gfs_global",
                "icon_global",
                "gem_global",
                "meteofrance_arpege_world",
                "jma_gsm",
                "ukmo_global_deterministic_10km",
            ):
                _set_feed_state(
                    conn,
                    site_id,
                    _feed_id(conn, "open-meteo", sibling),
                    last_run_at=None,
                    enabled=0,
                )
            # Insert a non-multimodel meteoblue member feed directly (not in seed
            # data) and subscribe it for this site with NULL last_run_at.  If the
            # predicate clause is working, this row must be excluded.
            conn.execute(
                """
                INSERT INTO feeds (source, model, enabled, default_subscribed,
                                   fetch_interval_minutes, max_lead_hours, is_virtual)
                VALUES ('meteoblue', 'GFS05', 1, 0, 360, 168, 0)
                """
            )
            mb_member_id = int(
                conn.execute(
                    "SELECT id FROM feeds WHERE source='meteoblue' AND model='GFS05'"
                ).fetchone()["id"]
            )
            # Subscribe explicitly (override default_subscribed=0 via sfs.enabled=1).
            _set_feed_state(conn, site_id, mb_member_id, last_run_at=None, enabled=1)

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        feed_stale = _cond(body, "feed_stale")
        # open-meteo/ecmwf_ifs is stale (count=1); meteoblue/GFS05 is excluded.
        assert feed_stale["ok"] is False
        assert feed_stale["count"] == 1


def test_liveness_recent_completed_job_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the _has_completed_within returning True branch (the logical complement
    # of test_fetch_feed_live_trips_when_eligible_feed_but_no_recent_run).
    # Eligible work exists AND a recent completed job exists for each type
    # → all three liveness conditions must be ok=True.
    # Without this, a bug where _has_completed_within always returns False (or
    # where the `not` is accidentally doubled) would leave every liveness trip
    # path exercised but the non-trip path silent.
    #
    # now is injected via monkeypatch so job timestamps are deterministically
    # "recent": jobs seeded with updated_at=FIXED_NOW are within every cutoff
    # window, regardless of when the test runs.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)

    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "liveness-complete.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            # Eligible feed (recently run, so not stale — only testing liveness).
            _set_feed_state(
                conn,
                site_id,
                _feed_id(conn, "open-meteo", "ecmwf_ifs"),
                last_run_at="2026-07-09T11:00:00Z",
            )
            # Eligible obs target.
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'FAKE3', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )
            # Completed jobs for all three types at fixed_now → within every
            # liveness cutoff window (FETCH_OBS_LIVE=8h, FETCH_FEED=12h, PAIR=12h).
            recent = "2026-07-09T12:00:00Z"
            for job_type in ("fetch_obs", "fetch_feed", "pair_and_score"):
                conn.execute(
                    """
                    INSERT INTO jobs (type, site_id, status, updated_at)
                    VALUES (?, ?, 'completed', ?)
                    """,
                    (job_type, site_id, recent),
                )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert body["grace_active"] is False
        for cid in ("fetch_obs_live", "fetch_feed_live", "pair_score_live"):
            c = _cond(body, cid)
            assert c["ok"] is True, f"{cid} should be ok with recent completed job"


# --- Task 4b: Group 1 problem_jobs (failed/stuck/overdue-pending) ------------


def _seed_job(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    status: str,
    updated_at: str,
    next_attempt_at: str | None,
    job_type: str = "fetch_feed",
    job_key: str = "fetch:1",
) -> None:
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status,
                          next_attempt_at, updated_at)
        VALUES (?, ?, ?, '{}', ?, ?, ?)
        """,
        (job_type, site_id, job_key, status, next_attempt_at, updated_at),
    )


def test_problem_jobs_pending_future_deferral_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-defer.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # Pending with a FUTURE next_attempt_at → freshly deferred → NOT overdue.
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2035-01-01T00:00:00Z",
                next_attempt_at="2035-01-01T00:00:00Z",
                job_key="fetch:defer",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is True


def test_problem_jobs_all_three_arms_trip_and_each_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins all three OR arms of the problem_jobs predicate simultaneously.
    # One job per arm; exact count == 3 so that dropping any single arm
    # (removing one OR clause from the SQL) would change the count and fail.
    # fixed_now is injected so cutoffs are deterministic regardless of wall clock.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-all-arms.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # ARM 1: failed job older than FAILED_JOB_AGE_HOURS=48h.
            # failed_cutoff = 2026-07-07T12:00:00Z;
            # updated_at 2026-07-07T00:00:00Z ≤ cutoff.
            _seed_job(
                conn,
                site_id=site_id,
                status="failed",
                updated_at="2026-07-07T00:00:00Z",
                next_attempt_at=None,
                job_key="fetch:failed-old",
            )
            # ARM 2: running job older than STUCK_RUNNING_MINUTES=20m.
            # stuck_cutoff = 2026-07-09T11:40:00Z;
            # updated_at 2026-07-09T11:00:00Z ≤ cutoff.
            _seed_job(
                conn,
                site_id=site_id,
                status="running",
                updated_at="2026-07-09T11:00:00Z",
                next_attempt_at=None,
                job_key="fetch:stuck-running",
            )
            # ARM 3: pending job overdue by PENDING_OVERDUE_MINUTES=15m.
            # pending_cutoff = 2026-07-09T11:45:00Z;
            # next_attempt_at 2020-01-01 ≤ cutoff.
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2020-01-01T00:00:00Z",
                next_attempt_at="2020-01-01T00:00:00Z",
                job_key="fetch:pending-overdue",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        cond = _cond(body, "problem_jobs")
        assert cond["ok"] is False
        assert cond["count"] == 3  # exact: one per arm


def test_problem_jobs_pending_null_next_attempt_at_counts_as_stuck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # HARDENING (diverges from the literal `next_attempt_at IS NOT NULL`):
    # claim_next_job treats `next_attempt_at IS NULL OR next_attempt_at <= now`
    # as claimable, so a STUCK pending job can carry next_attempt_at=NULL. Such a
    # job — old updated_at, NULL attempt — is claimable-but-unclaimed and MUST be
    # counted. The COALESCE(next_attempt_at, updated_at) predicate catches it;
    # the literal `IS NOT NULL` arm would silently miss it. This test pins that.
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-null-attempt.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # Pending, next_attempt_at NULL (claimable now), old updated_at.
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2020-01-01T00:00:00Z",
                next_attempt_at=None,
                job_key="fetch:null-attempt",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] >= 1


def test_problem_jobs_pending_null_next_attempt_at_recent_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Complement to the NULL-attempt trip test: a freshly-enqueued pending job
    # (next_attempt_at=NULL, recent updated_at) is claimable NOW but not yet
    # overdue, so it must NOT count. This pins that COALESCE falls back to
    # updated_at against the 15-min cutoff rather than treating every NULL-attempt
    # pending job as stuck.
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-null-recent.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # Pending, NULL attempt, updated_at far in the FUTURE → within cutoff.
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2035-01-01T00:00:00Z",
                next_attempt_at=None,
                job_key="fetch:null-recent",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is True


def test_problem_jobs_pending_non_iso_next_attempt_at_counts_as_stuck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A pending job with a non-ISO next_attempt_at (e.g. imported/hand-edited
    # from a foreign database) sorts after every real ISO stamp, so the plain
    # `next_attempt_at <= now` comparison never selects it and it is never
    # claimed again. The typeof/GLOB arm folds this sibling blind spot into
    # the same stuck-pending detection as the NULL-attempt case above, even
    # with a RECENT updated_at (so COALESCE alone would not have caught it).
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-non-iso-attempt.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2035-01-01T00:00:00Z",
                next_attempt_at="zzzz-not-a-timestamp",
                job_key="fetch:non-iso-attempt",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] >= 1


def test_problem_jobs_year_prefixed_garbage_next_attempt_at_is_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # '9999-not-a-timestamp' starts with four digits and a dash, so a shape
    # check anchored on the year prefix alone accepts it -- while the value
    # still sorts after every real cutoff. next_attempt_at is non-NULL, so
    # the COALESCE arm never reads updated_at, and the future stamp cannot
    # trip the overdue arm -- only the shape check can produce the count.
    # Such a row escapes both arms and wedges invisibly; the
    # full canonical-prefix check must surface it.
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-year-prefix-garbage.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2035-01-01T00:00:00Z",
                next_attempt_at="9999-not-a-timestamp",
                job_key="fetch:year-prefix-garbage",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] == 1


@pytest.mark.parametrize(
    "next_attempt_at",
    [
        "2099-01-01T00:00:00Z",
        "2099-01-01T00:00:00.123456Z",
        "2099-01-01T00:00:00+00:00",
    ],
    ids=["whole_second_z", "fractional_second_z", "utc_offset_spelling"],
)
def test_problem_jobs_canonical_future_next_attempt_at_is_not_flagged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, next_attempt_at: str
) -> None:
    # Every stamp shape a live write path (or a legitimate foreign import)
    # produces -- whole-second, fractional-second, and the +00:00 offset
    # spelling -- must pass the shape check: a freshly deferred pending job
    # is healthy, not a problem. The fractional case is the sharp edge --
    # a pattern without the trailing wildcard would flag every stamp the
    # microsecond formatter writes.
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-canonical-shape.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2035-01-01T00:00:00Z",
                next_attempt_at=next_attempt_at,
                job_key="fetch:canonical-shape",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is True


def test_problem_jobs_blob_next_attempt_at_still_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The typeof() disjunct must survive any rewrite of the shape check:
    # GLOB on a BLOB does not behave like the type test, so replacing the
    # typeof half with the GLOB alone would let a BLOB carrier through.
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-blob-attempt.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            conn.execute(
                """
                INSERT INTO jobs (type, site_id, job_key, payload, status,
                                  next_attempt_at, updated_at)
                VALUES ('fetch_feed', ?, 'fetch:blob-attempt', '{}', 'pending',
                        x'0001', '2035-01-01T00:00:00Z')
                """,
                (site_id,),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] == 1


def test_problem_jobs_blob_shaped_like_a_canonical_stamp_still_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The typeof() disjunct earns its keep only against a BLOB whose bytes
    # happen to spell a canonical timestamp: GLOB compares raw bytes, so a
    # BLOB carrying the ASCII bytes of '2099-01-01T00:00:00Z' matches the
    # shape pattern (NOT GLOB is false) even though it is not TEXT -- an
    # arbitrary short BLOB (e.g. x'0001') never exercises this, since it
    # fails the GLOB regardless of typeof. Only typeof(...) != 'text' can
    # still flag this row if the GLOB half is dropped.
    close_db()
    config.db_path = str(tmp_path / "problem-jobs-blob-canonical-shape.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            conn.execute(
                """
                INSERT INTO jobs (type, site_id, job_key, payload, status,
                                  next_attempt_at, updated_at)
                VALUES ('fetch_feed', ?, 'fetch:blob-canonical-shape', '{}',
                        'pending', ?, '2035-01-01T00:00:00Z')
                """,
                (site_id, b"2099-01-01T00:00:00Z"),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] == 1


def test_problem_jobs_malformed_and_overdue_row_counted_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A single pending row whose next_attempt_at both fails the shape check
    # AND sorts before the overdue cutoff satisfies both OR-ed disjuncts of
    # the pending arm. The arms form one row predicate, not a sum, so the
    # count must be exactly 1 -- a restructuring into a UNION or summed
    # subcounts would report 2.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-single-count.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # '1999-bogus' fails the shape check and also sorts before the
            # pending cutoff (fixed_now - 15m), so both disjuncts hold.
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2026-07-09T11:59:00Z",
                next_attempt_at="1999-bogus",
                job_key="fetch:both-disjuncts",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] == 1


def test_problem_jobs_flags_parseable_non_canonical_future_stamp_by_design(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Accepted behaviour, pinned on purpose: a space-separated stamp
    ('2026-08-09 12:00:00+00:00') is parseable -- parse_utc reads it, and
    the claim comparison still claims it no later than the canonical
    spelling of the same instant, since a space sorts before 'T' -- but it
    is NOT a shape the app itself ever writes, so the shape check counts it
    for the remaining window of the job's own delay. Over-flagging a stamp
    only a foreign database can carry is the chosen trade against never
    flagging a genuinely wedged row. Loosening the check to admit every
    shape parse_utc accepts is a design change and must not slip through as
    a bug fix."""
    assert parse_utc("2026-08-09 12:00:00+00:00") == datetime(
        2026, 8, 9, 12, 0, tzinfo=UTC
    )

    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-non-canonical-parseable.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # Well in the future relative to fixed_now, recent updated_at:
            # neither the COALESCE arm nor staleness can be what counts it.
            _seed_job(
                conn,
                site_id=site_id,
                status="pending",
                updated_at="2026-07-09T11:59:00Z",
                next_attempt_at="2026-08-09 12:00:00+00:00",
                job_key="fetch:space-separated",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is False
        assert _cond(body, "problem_jobs")["count"] == 1


def test_problem_jobs_failed_old_only_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Per-arm isolation: only a failed job older than FAILED_JOB_AGE_HOURS=48h is
    # present. A broken implementation that omits the failed OR arm would return
    # count=0/ok=True and fail this assertion.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-failed-only.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # failed_cutoff = 2026-07-07T12:00:00Z; 2026-07-07T00:00:00Z ≤ cutoff.
            _seed_job(
                conn,
                site_id=site_id,
                status="failed",
                updated_at="2026-07-07T00:00:00Z",
                next_attempt_at=None,
                job_key="fetch:failed-old",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        cond = _cond(body, "problem_jobs")
        assert cond["ok"] is False
        assert cond["count"] == 1


def test_problem_jobs_failed_recent_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Paired negative for test_problem_jobs_failed_old_only_trips: a failed job
    # updated within the 48h window must NOT count. Without this negative,
    # the trip test above can stay green even if the cutoff is broken so that ALL
    # failed jobs count (the positive alone can't detect that).
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-failed-recent.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # failed_cutoff = 2026-07-07T12:00:00Z;
            # 2026-07-09T06:00:00Z > cutoff → recent.
            _seed_job(
                conn,
                site_id=site_id,
                status="failed",
                updated_at="2026-07-09T06:00:00Z",
                next_attempt_at=None,
                job_key="fetch:failed-recent",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is True


def test_problem_jobs_running_old_only_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Per-arm isolation: only a running job older than STUCK_RUNNING_MINUTES=20m is
    # present. A broken implementation that omits the running OR arm would return
    # count=0/ok=True and fail this assertion.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-running-only.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # stuck_cutoff = 2026-07-09T11:40:00Z; 2026-07-09T11:00:00Z ≤ cutoff.
            _seed_job(
                conn,
                site_id=site_id,
                status="running",
                updated_at="2026-07-09T11:00:00Z",
                next_attempt_at=None,
                job_key="fetch:stuck-running",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        cond = _cond(body, "problem_jobs")
        assert cond["ok"] is False
        assert cond["count"] == 1


def test_problem_jobs_running_recent_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Paired negative for test_problem_jobs_running_old_only_trips: a running job
    # updated within the 20m window must NOT count. Without this negative, a
    # broken predicate that counts ALL running jobs would leave the trip test green
    # but the false-positive undetected.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-running-recent.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # stuck_cutoff = 2026-07-09T11:40:00Z;
            # 2026-07-09T11:50:00Z > cutoff → recent.
            _seed_job(
                conn,
                site_id=site_id,
                status="running",
                updated_at="2026-07-09T11:50:00Z",
                next_attempt_at=None,
                job_key="fetch:running-recent",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is True


def test_problem_jobs_running_at_stuck_cutoff_boundary_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Threshold boundary: a running job with updated_at exactly equal to
    # stuck_cutoff must count because the SQL uses `<=` (not `<`). An off-by-one
    # that changed `<=` to `<` would leave an at-boundary job uncounted and fail.
    # Paired with the "one second newer" test below to pin both sides of the edge.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-stuck-boundary.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # stuck_cutoff = now - 20m = 2026-07-09T11:40:00Z exactly.
            _seed_job(
                conn,
                site_id=site_id,
                status="running",
                updated_at="2026-07-09T11:40:00Z",
                next_attempt_at=None,
                job_key="fetch:stuck-at-boundary",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        cond = _cond(body, "problem_jobs")
        assert cond["ok"] is False
        assert cond["count"] == 1  # at-cutoff is included by <=


def test_problem_jobs_running_one_second_inside_cutoff_does_not_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Paired: a running job updated one second AFTER the stuck_cutoff must NOT
    # count. Together with the at-boundary test this pins both sides of the `<=`
    # operator so a misplaced `<` or `>=` would be caught.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-stuck-inside.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE runtime_state SET value='2000-01-01T00:00:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # stuck_cutoff = 2026-07-09T11:40:00Z;
            # 11:40:01Z is 1 second newer → not stuck.
            _seed_job(
                conn,
                site_id=site_id,
                status="running",
                updated_at="2026-07-09T11:40:01Z",
                next_attempt_at=None,
                job_key="fetch:stuck-inside",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "problem_jobs")["ok"] is True


def test_problem_jobs_grace_suppresses_would_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Grace suppression for problem_jobs: when the worker started within
    # GRACE_MINUTES=10 min of now, a would-trip condition must be forced ok=True
    # with no detail key emitted. Mirrors how feed_stale grace suppression works
    # (test_feed_stale_grace_suppresses) — both are driven by the shared _cond
    # closure inside _pipeline_conditions.
    # A broken _cond that ignores grace_active for problem_jobs would return
    # ok=False and fail the ok assertion; a broken one that drops detail only for
    # other conditions would pass the ok assertion but leave "detail" present.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "problem-jobs-grace.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            # worker_started_at 5 min before fixed_now → within GRACE_MINUTES=10.
            conn.execute(
                "UPDATE runtime_state SET value='2026-07-09T11:55:00Z' "
                "WHERE key='worker_started_at'"
            )
            site_id = _seed_site(conn)
            # A failed job that would definitely trip without grace
            # (failed_cutoff = 2026-07-07T12:00:00Z; 2026-07-07T00:00:00Z ≤ cutoff).
            _seed_job(
                conn,
                site_id=site_id,
                status="failed",
                updated_at="2026-07-07T00:00:00Z",
                next_attempt_at=None,
                job_key="fetch:failed-grace",
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert body["grace_active"] is True
        cond = _cond(body, "problem_jobs")
        assert cond["ok"] is True
        assert "detail" not in cond  # grace suppression drops detail


# --- Task 5: Group 2 budget/provider conditions ------------------------------


def _seed_source_budget(
    conn: sqlite3.Connection,
    source: str,
    *,
    calls: int,
) -> None:
    tz = str(
        conn.execute(
            "SELECT billing_tz FROM sources WHERE source=?", (source,)
        ).fetchone()["billing_tz"]
    )
    from wxverify.collection.budget import current_billing_day

    conn.execute(
        """
        INSERT INTO api_budget (source, billing_day, calls, credits)
        VALUES (?, ?, ?, 0)
        ON CONFLICT(source, billing_day) DO UPDATE SET calls=excluded.calls
        """,
        (source, current_billing_day(tz), calls),
    )


def test_budget_calls_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    close_db()
    config.db_path = str(tmp_path / "budget-calls.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        # google daily_call_limit is 100 (config.SOURCE_SEEDS).
        db.write_sync(lambda c: _seed_source_budget(c, "google", calls=99))
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "budget_calls")["ok"] is True  # 99 == limit-1

        db.write_sync(lambda c: _seed_source_budget(c, "google", calls=100))
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "budget_calls")["ok"] is False  # 100 == limit


def test_feed_errors_disabled_feed_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Standalone negative: a DISABLED feed carrying a real (non-sentinel) error is
    # the ONLY feed with an error in this DB, so feed_errors staying ok proves the
    # active-feed ladder (f.enabled=1) excludes it — not a vacuous pass riding on
    # a prior seed. Owns its full precondition: seeds feeds.enabled=0 explicitly.
    close_db()
    config.db_path = str(tmp_path / "feed-errors-disabled.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            vc = _feed_id(conn, "visualcrossing", "blend")  # default_subscribed=0
            # disabled feed with an error → must NOT trip.
            conn.execute("UPDATE feeds SET enabled=0 WHERE id=?", (vc,))
            _set_feed_state(
                conn,
                site_id,
                vc,
                last_run_at="2026-07-08T00:00:00Z",
                enabled=1,
                last_error="HTTP 500 boom",
                error_count=1,
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "feed_errors")["ok"] is True


def test_feed_errors_active_feed_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Standalone positive on its own DB: an ENABLED, subscribed feed carrying a
    # real (non-sentinel) error trips feed_errors. Owns its full precondition.
    close_db()
    config.db_path = str(tmp_path / "feed-errors-active.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed_active(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # enabled, subscribed
            _set_feed_state(
                conn,
                site_id,
                om,
                last_run_at="2026-07-08T00:00:00Z",
                last_error="HTTP 500 boom",
                error_count=1,
            )

        db.write_sync(_seed_active)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "feed_errors")["ok"] is False


def test_costed_noop_repeated_sentinel_active_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "costed-noop.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL

        def _seed_single(conn: sqlite3.Connection) -> int:
            site_id = _seed_site(conn)
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
            _set_feed_state(
                conn,
                site_id,
                om,
                last_run_at="2026-07-08T00:00:00Z",
                last_error=NO_USABLE_SAMPLES_SENTINEL,
                error_count=1,
            )
            return om

        db.write_sync(_seed_single)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "costed_noop")["ok"] is True  # single occurrence quiet

        def _bump(conn: sqlite3.Connection) -> None:
            site_id = int(conn.execute("SELECT id FROM sites LIMIT 1").fetchone()["id"])
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
            conn.execute(
                "UPDATE site_feed_state SET error_count=3 "
                "WHERE site_id=? AND feed_id=?",
                (site_id, om),
            )

        db.write_sync(_bump)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "costed_noop")["ok"] is False  # error_count>=3 trips


def test_costed_noop_disabled_feed_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "costed-noop-disabled.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL

        def _seed_disabled(conn: sqlite3.Connection) -> None:
            # A DISABLED feed carrying the sentinel + error_count>=N. The active
            # ladder (f.enabled=1) excludes it, so costed_noop must NOT trip.
            site_id = _seed_site(conn)
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
            conn.execute("UPDATE feeds SET enabled=0 WHERE id=?", (om,))
            _set_feed_state(
                conn,
                site_id,
                om,
                last_run_at="2026-07-08T00:00:00Z",
                enabled=1,
                last_error=NO_USABLE_SAMPLES_SENTINEL,
                error_count=5,
            )

        db.write_sync(_seed_disabled)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "costed_noop")["ok"] is True


def test_key_missing_three_cases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "key-missing.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.delenv("WXV_VISUALCROSSING_KEY", raising=False)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        # Case A (negative): unsubscribed keyed provider, no key → NOT tripped.
        def _seed_unsub(conn: sqlite3.Connection) -> None:
            _seed_site(conn)  # visualcrossing blend is default_subscribed=0

        db.write_sync(_seed_unsub)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "key_missing")["ok"] is True

        # Case B (negative): subscribed but feed.enabled=0, key absent → NOT tripped.
        def _seed_sub_disabled(conn: sqlite3.Connection) -> None:
            site_id = int(conn.execute("SELECT id FROM sites LIMIT 1").fetchone()["id"])
            vc = _feed_id(conn, "visualcrossing", "blend")
            conn.execute("UPDATE feeds SET enabled=0 WHERE id=?", (vc,))
            _set_feed_state(conn, site_id, vc, last_run_at=None, enabled=1)

        db.write_sync(_seed_sub_disabled)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "key_missing")["ok"] is True

        # Case C (positive): subscribed AND enabled, key empty → trips.
        def _seed_sub_enabled(conn: sqlite3.Connection) -> None:
            site_id = int(conn.execute("SELECT id FROM sites LIMIT 1").fetchone()["id"])
            vc = _feed_id(conn, "visualcrossing", "blend")
            conn.execute("UPDATE feeds SET enabled=1 WHERE id=?", (vc,))
            _set_feed_state(conn, site_id, vc, last_run_at=None, enabled=1)

        db.write_sync(_seed_sub_enabled)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "key_missing")["ok"] is False


def test_domain_backoffs_active_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "domain-backoffs.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            # A backoff whose next_attempt_at is in the future → still active.
            conn.execute(
                """
                INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
                VALUES ('api.example.com', '2099-01-01T00:00:00Z', 2)
                """
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "domain_backoffs")["ok"] is False
        assert _cond(body, "domain_backoffs")["count"] >= 1


def test_budget_credits_trips_at_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "budget-credits.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        from wxverify.collection.budget import current_billing_day

        def _seed(conn: sqlite3.Connection) -> None:
            # meteoblue is the only seeded source with a non-NULL credit limit
            # (65000, billing_tz='UTC'). Seed today's row at the limit so the
            # `credits >= daily_credit_limit` branch trips.
            tz = str(
                conn.execute(
                    "SELECT billing_tz FROM sources WHERE source='meteoblue'"
                ).fetchone()["billing_tz"]
            )
            conn.execute(
                """
                INSERT INTO api_budget (source, billing_day, calls, credits)
                VALUES ('meteoblue', ?, 0, 65000)
                ON CONFLICT(source, billing_day) DO UPDATE SET
                    credits=excluded.credits
                """,
                (current_billing_day(tz),),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "budget_credits")["ok"] is False


def test_budget_credits_boundary_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins both sides of the `credits >= daily_credit_limit` comparator for
    # meteoblue (the only source with a non-NULL credit limit of 65000).
    # The existing test_budget_credits_trips_at_limit covers exactly-at-limit;
    # this test adds the one-below negative (64999 → ok) so that a broken `>`
    # (strict) in place of `>=` would keep the at-limit test green but fail the
    # boundary negative here — and the positive (65000 trips) ensures a broken
    # `>` cannot hide by also checking the trip direction from a single test.
    close_db()
    config.db_path = str(tmp_path / "budget-credits-boundary.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        from wxverify.collection.budget import current_billing_day

        def _seed_credits(conn: sqlite3.Connection, *, credits: int) -> None:
            tz = str(
                conn.execute(
                    "SELECT billing_tz FROM sources WHERE source='meteoblue'"
                ).fetchone()["billing_tz"]
            )
            conn.execute(
                """
                INSERT INTO api_budget (source, billing_day, calls, credits)
                VALUES ('meteoblue', ?, 0, ?)
                ON CONFLICT(source, billing_day) DO UPDATE SET
                    credits=excluded.credits
                """,
                (current_billing_day(tz), credits),
            )

        db.write_sync(lambda c: _seed_credits(c, credits=64999))
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "budget_credits")["ok"] is True  # one below limit

        db.write_sync(lambda c: _seed_credits(c, credits=65000))
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "budget_credits")["ok"] is False  # at limit


def test_budget_credits_null_limit_never_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the `daily_credit_limit is not None` guard in _budget_conditions.
    # google has daily_credit_limit=NULL in SOURCE_SEEDS (config.py:64).  Seeding
    # an enormous credits value on today's billing day must NOT trip budget_credits
    # because the `is not None` guard short-circuits before the `>=` comparison.
    # A broken impl that dropped the guard (e.g. comparing `credits >= None`)
    # would raise a TypeError or coerce None to 0 so every source trips — this
    # test catches both mutations.
    #
    # Confirmed: google's daily_credit_limit is NULL in SOURCE_SEEDS
    # (wxverify/config.py line 64: SourceSeed("google", 100, None, "UTC")).
    close_db()
    config.db_path = str(tmp_path / "budget-credits-null-limit.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        from wxverify.collection.budget import current_billing_day

        def _seed(conn: sqlite3.Connection) -> None:
            # Confirm at seed time that google's credit limit IS NULL — the oracle
            # is only valid when the source has a genuinely NULL limit.
            row = conn.execute(
                "SELECT daily_credit_limit FROM sources WHERE source='google'"
            ).fetchone()
            assert row is not None
            assert row["daily_credit_limit"] is None, (
                "google must have NULL daily_credit_limit for this oracle to be valid"
            )
            tz = str(
                conn.execute(
                    "SELECT billing_tz FROM sources WHERE source='google'"
                ).fetchone()["billing_tz"]
            )
            # Seed a huge credits value — far beyond any real limit.
            conn.execute(
                """
                INSERT INTO api_budget (source, billing_day, calls, credits)
                VALUES ('google', ?, 0, 9999999)
                ON CONFLICT(source, billing_day) DO UPDATE SET
                    credits=excluded.credits
                """,
                (current_billing_day(tz),),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        # NULL credit limit must never produce a credit-tripped condition.
        assert _cond(body, "budget_credits")["ok"] is True


def test_budget_calls_missing_row_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the `budget is None → calls = 0` default in _budget_conditions.
    # When a source has NO api_budget row for today's billing day, the impl
    # treats calls as 0 and must NOT trip budget_calls.  A broken impl that
    # crashed on a missing row (NoneType attribute access) or treated missing as
    # over-limit would fail this test.  The DB is freshly seeded with no budget
    # rows, so all sources are in the missing-row state.
    close_db()
    config.db_path = str(tmp_path / "budget-calls-missing.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        # No _seed needed: migrations create the sources rows but leave
        # api_budget empty, so every source has no row for today's day.
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "budget_calls")["ok"] is True


def test_domain_backoffs_expired_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the `next_attempt_at > now` comparator on the NEGATIVE side.
    # The existing test_domain_backoffs_active_trips covers a future backoff
    # (positive arm).  This negative seeds a backoff whose next_attempt_at is
    # in the past (already expired) and asserts it does NOT count.  A broken
    # impl that used `>=` (or dropped the comparator entirely) would count the
    # expired row and fail this assertion, while the positive-only test would
    # still pass — the paired negative is what makes the comparator trustworthy.
    fixed_now = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.api.routes.health.utc_now", lambda: fixed_now)

    close_db()
    config.db_path = str(tmp_path / "domain-backoffs-expired.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            # next_attempt_at in 2020 → expired relative to fixed_now 2026-07-09.
            conn.execute(
                """
                INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
                VALUES ('api.example.com', '2020-01-01T00:00:00Z', 1)
                """
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "domain_backoffs")["ok"] is True
        # Confirm no count leak — count must be 0 (or absent) for an expired row.
        cond = _cond(body, "domain_backoffs")
        assert cond.get("count", 0) == 0


def test_domain_backoffs_unreadable_stamp_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the active-backoff count against a stored next_attempt_at that does
    # not parse (only an imported or hand-edited database carries one).  A
    # lexical SQL comparison (`next_attempt_at > ?`) sorts 'not-a-timestamp'
    # above every ISO stamp and reports an active backoff that the worker's own
    # check deliberately ignores — a monitor verdict that disagrees with the
    # worker on the surface whose job is to explain the system's state.  The
    # unreadable row must count as nothing: ok=True, count=0.  The paired tests
    # (test_domain_backoffs_active_trips, test_domain_backoffs_expired_does_not_trip)
    # pin that real future stamps still trip and real past stamps still do not,
    # so this cannot pass by counting nothing at all.
    close_db()
    config.db_path = str(tmp_path / "domain-backoffs-unreadable.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
                VALUES ('unreadable-stamp.example', 'not-a-timestamp', 1)
                """
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        cond = _cond(body, "domain_backoffs")
        assert cond["ok"] is True
        assert cond.get("count", 0) == 0


def test_feed_errors_sentinel_does_not_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the `last_error != NO_USABLE_SAMPLES_SENTINEL` clause in
    # _budget_conditions.  An active feed whose last_error IS the sentinel belongs
    # to costed_noop's domain, not feed_errors.  A broken impl that dropped the
    # `!= sentinel` filter would count this row under feed_errors too, turning
    # ok=True into ok=False — this test catches that mutation.  Paired with the
    # existing test_feed_errors_active_feed_trips (which asserts a real error
    # DOES trip), so both sides of the clause are exercised.
    close_db()
    config.db_path = str(tmp_path / "feed-errors-sentinel.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)
            om = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # enabled, subscribed
            # Active feed with SENTINEL as its last_error.  Must NOT trip
            # feed_errors; it belongs to costed_noop.
            _set_feed_state(
                conn,
                site_id,
                om,
                last_run_at="2026-07-08T00:00:00Z",
                last_error=NO_USABLE_SAMPLES_SENTINEL,
                error_count=1,
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "feed_errors")["ok"] is True


def test_key_missing_weathercom_arm_positive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the weathercom station-based arm of _key_missing_count (positive).
    # When >=1 enabled station exists on an enabled site AND the weathercom key
    # is absent, key_missing must trip.  This arm is structurally distinct from
    # the forecast-source arm (tested in test_key_missing_three_cases): it is
    # gated on the stations table, not on feeds.  A broken impl that omitted the
    # weathercom arm entirely would leave this test failing.
    close_db()
    config.db_path = str(tmp_path / "key-missing-wc-pos.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.delenv("WXV_WEATHERCOM_KEY", raising=False)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)  # enabled site
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'WXCFAKE1', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "key_missing")["ok"] is False


def test_key_missing_weathercom_arm_no_station_negative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Negative for the weathercom arm: no stations seeded at all, weathercom key
    # absent.  Without an enabled station on an enabled site, the weathercom arm
    # must NOT trip — the condition is not exercised.  A broken impl that always
    # counted the weathercom arm regardless of station presence would turn this
    # ok=True into ok=False and fail here.  Paired with the positive above so both
    # sides of the station-gate are exercised.
    close_db()
    config.db_path = str(tmp_path / "key-missing-wc-no-station.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.delenv("WXV_WEATHERCOM_KEY", raising=False)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            # Seed an enabled site with NO stations at all.
            _seed_site(conn)

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "key_missing")["ok"] is True


def test_db_readable_green_on_healthy_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pins the full shape of the green db_readable condition and its contribution
    # to overall. A broken impl that emitted the wrong group, severity, or skipped
    # value, or that failed to count ok=True toward overall=ok, would fail here.
    # Paired with test_db_readable_sqlite_error_composition_invariant (red path)
    # so the suppression logic is trustworthy only when both sides pass.
    close_db()
    config.db_path = str(tmp_path / "db-green.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        body = client.get("/api/health/monitor").json()
        db_cond = _cond(body, "db_readable")
        assert db_cond["ok"] is True
        assert db_cond["skipped"] is False
        assert db_cond["group"] == "db"
        assert db_cond["severity"] == "critical"
        # A green critical condition must not push overall above ok.
        assert body["overall"] == "ok"
        # Exactly one db_readable condition — no duplicates.
        db_readable_conds = [c for c in body["conditions"] if c["id"] == "db_readable"]
        assert len(db_readable_conds) == 1


def test_missing_db_recreation_reports_missing_liveness_documented_limitation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    db_file = tmp_path / "db-missing.db"
    config.db_path = str(db_file)
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)

    async def _no_warm(db: object) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _no_warm)
    app = create_app(root_path="")
    with TestClient(app) as client:
        # close_db() BEFORE deleting so we exercise the init_db() recreation
        # path, not a stale open connection. The boot-time read-cache warm is
        # stubbed out above: it schedules its own background db.read against
        # this same connection, and without the stub it can read through the
        # connection this test closes below, crashing the process instead of
        # failing the test.
        from wxverify.db.connection import close_db as _close

        _close()
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(db_file) + suffix)
            if p.exists():
                p.unlink()
        body = client.get("/api/health/monitor").json()
        # Connection layer recreates an empty readable DB via init_db():
        # db_readable is green. This is the v1 documented limitation
        # (identity-loss is out of db_readable scope). But the fresh
        # runtime_state is empty, so main_worker_liveness has no evidence
        # at all (wording 3) -- in production the real worker's next loop
        # stamp would clear it, but this test's idle worker stub
        # (_idle_worker_async) never stamps -- so overall degrades to
        # warning.
        assert _cond(body, "db_readable")["ok"] is True
        liveness = _cond(body, "main_worker_liveness")
        assert liveness["ok"] is False
        assert liveness["detail"] == (
            "main-worker liveness evidence missing:"
            " no loop stamp, worker start or import time"
        )
        assert body["overall"] == "warning"


def test_normal_shutdown_cancels_and_awaits_the_boot_warm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The fix above stubs warm_read_cache to a no-op so this DB-close test
    # doesn't race a real warm. This is the paired proof that the stub is
    # masking a test-only hazard (a live warm reading through a connection
    # this test closes), not a real lifecycle bug: on an ORDINARY shutdown --
    # no connection closed underneath it -- the lifespan's `finally` actually
    # cancels the warm task and awaits it, rather than leaking it as a
    # detached background task.
    #
    # A blocking fake turns the race into a rendezvous: it signals "entered"
    # then blocks forever on an Event only cancellation can break. If the
    # lifespan's `finally` (api/app.py) ever stopped cancelling/awaiting
    # `warm`, this fake would never observe cancellation and the assertion
    # below would fail for the right reason.
    close_db()
    state = {"entered": False, "cancelled": False}

    async def _blocking_warm(db: object) -> None:
        state["entered"] = True
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    config.db_path = str(tmp_path / "db-shutdown.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _blocking_warm)
    app = create_app(root_path="")
    with TestClient(app) as client:
        client.get("/api/health/monitor")
        # The warm task has had the entire test-client startup plus one
        # request round-trip to reach its blocking await.
        assert state["entered"] is True
        assert state["cancelled"] is False
    # TestClient.__exit__ runs the lifespan's `finally` synchronously before
    # returning, so by here the cancel-and-await has already happened.
    assert state["cancelled"] is True


def test_db_readable_sqlite_error_composition_invariant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Composition invariant: when the db-group read raises sqlite3.Error, the
    # overall verdict must be "critical", the surviving db_readable condition must
    # be ok=False / severity="critical" / group="db", and there must be EXACTLY ONE
    # db_readable in the output (the filter at build_verdict L457 must remove any
    # green one before appending the red one — a broken filter would leak both).
    #
    # Injection: only _db_conditions is patched to raise; pipeline and budget groups
    # run against the real healthy DB. This isolates the db-group error path from
    # the all-groups-fail scenario already covered by
    # test_monitor_endpoint_db_failure_reports_critical_not_500, and ensures the
    # filter only removes db_readable conditions without corrupting other conditions.
    close_db()
    config.db_path = str(tmp_path / "db-composition.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        import wxverify.monitor as monitor_mod

        def _db_boom(conn: sqlite3.Connection, now: object) -> object:
            raise sqlite3.OperationalError("simulated db read failure")

        monkeypatch.setattr(monitor_mod, "_db_conditions", _db_boom)
        body = client.get("/api/health/monitor").json()

    # (a) overall must be critical
    assert body["overall"] == "critical"

    # (b) the surviving db_readable must be the red failure condition
    db_cond = _cond(body, "db_readable")
    assert db_cond["ok"] is False
    assert db_cond["skipped"] is False
    assert db_cond["severity"] == "critical"
    assert db_cond["group"] == "db"

    # (c) exactly ONE db_readable — no green condition leaked through the filter
    db_readable_conds = [c for c in body["conditions"] if c["id"] == "db_readable"]
    assert len(db_readable_conds) == 1, (
        f"expected exactly 1 db_readable condition, got {len(db_readable_conds)}: "
        f"{db_readable_conds}"
    )


def test_key_missing_weathercom_arm_key_present_negative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Negative for the weathercom arm: enabled station present, but the weathercom
    # key IS present → must NOT trip.  Confirms the `not resolve_secret("weathercom")`
    # half of the gate.  A broken impl that ignored the key check and always tripped
    # when a station exists would fail this assertion.  Paired with the positive test
    # (station + no key → trips) to pin both arms of the conjunct.
    close_db()
    config.db_path = str(tmp_path / "key-missing-wc-key-present.db")
    config.options_path = str(tmp_path / "missing-options.json")
    # Inject the weathercom key via env (options.json missing → _from_env() path).
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "fake-wc-key-for-testing")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _seed_site(conn)  # enabled site
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'WXCFAKE2', 40.0, -105.0, 900.0, 1)
                """,
                (site_id,),
            )

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "key_missing")["ok"] is True


# ---------------------------------------------------------------------------
# obs_collection_delayed (D1.3, plan §14.4)
#
# Every case calls build_verdict directly (pipeline_enabled=True,
# budget_enabled=False, db_enabled=False, now=_OCD_N, export_sweeper_dead=None)
# against a bare in-memory schema-current connection, so every boundary is
# exact.
# ---------------------------------------------------------------------------

_OCD_N = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)


def _ocd_t(minutes: float) -> str:
    return isoformat_utc(_OCD_N + timedelta(minutes=minutes))


def _ocd_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _ocd_station(
    conn: sqlite3.Connection, site_id: int, *, pws_id: str, enabled: int = 1
) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 40.0, -105.0, 900.0, ?)",
            (site_id, pws_id, enabled),
        ).lastrowid
    )


def _ocd_poll_state(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    next_poll_at: str | None,
    updated_at: str | None = None,
    health_state: str = "cold",
) -> None:
    if updated_at is None:
        conn.execute(
            "INSERT INTO station_poll_state (station_id, next_poll_at, health_state)"
            " VALUES (?, ?, ?)",
            (station_id, next_poll_at, health_state),
        )
    else:
        conn.execute(
            "INSERT INTO station_poll_state"
            " (station_id, next_poll_at, updated_at, health_state)"
            " VALUES (?, ?, ?, ?)",
            (station_id, next_poll_at, updated_at, health_state),
        )


def _ocd_job(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    *,
    status: str,
    next_attempt_at: str | None = None,
    updated_at: str | None = None,
) -> None:
    key = f"curobs:{station_id}"
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status,"
        " next_attempt_at, updated_at)"
        " VALUES ('fetch_current_obs', ?, ?, '{}', ?, ?,"
        " COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ','now')))",
        (site_id, key, status, next_attempt_at, updated_at),
    )


def _ocd_verdict(
    conn: sqlite3.Connection, *, now: datetime = _OCD_N, pipeline_enabled: bool = True
) -> dict[str, object]:
    return build_verdict(
        conn,
        pipeline_enabled=pipeline_enabled,
        budget_enabled=False,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )


def _ocd(conn: sqlite3.Connection, *, now: datetime = _OCD_N) -> dict[str, object]:
    return _cond(_ocd_verdict(conn, now=now), "obs_collection_delayed")


def test_ocd_due_16_min_overdue_trips_with_full_detail() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due {_ocd_t(-16)}; 0 in backoff"
    )


@pytest.mark.parametrize("order", ["older_station_first", "older_station_second"])
def test_ocd_oldest_among_two_overdue_selects_the_earlier_effective_time(
    order: str,
) -> None:
    """Two overdue stations at -20 min and -16 min: -20 min is earlier (more
    overdue), so it must be `oldest` regardless of insertion order.

    Mutant: `effective < oldest` -> `effective > oldest` -- would instead
    keep the later (-16 min) station as oldest.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    minutes = (-20, -16) if order == "older_station_first" else (-16, -20)
    station_a = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_a, next_poll_at=_ocd_t(minutes[0]))
    station_b = _ocd_station(conn, site_id, pws_id="ISTATION02")
    _ocd_poll_state(conn, station_b, next_poll_at=_ocd_t(minutes[1]))
    cond = _ocd(conn)
    assert cond["count"] == 2
    assert f"oldest due {_ocd_t(-20)}" in cond["detail"]


def test_ocd_due_exactly_15_min_overdue_trips_at_the_boundary() -> None:
    """Mutant: `>=` -> `>` on the threshold -- this exact-boundary row would
    then read ok=True where the correct code trips.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-15))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1


def test_ocd_due_14_min_overdue_is_not_yet_tripped() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-14))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0
    assert "detail" not in cond


def test_ocd_pending_deferral_holds_off_the_only_station() -> None:
    """Mutant: count backoff stations -- this row's station would incorrectly
    add to `overdue` if the code counted a still-in-backoff station.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    _ocd_job(conn, site_id, station_id, status="pending", next_attempt_at=_ocd_t(10))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0
    assert "detail" not in cond


def test_ocd_pending_deferral_itself_becomes_overdue() -> None:
    """Mutant: exclude backoff stations permanently -- this row would
    incorrectly read ok=True forever if a backoff station could never
    graduate into `overdue` once its own deferred time elapses.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-60))
    _ocd_job(conn, site_id, station_id, status="pending", next_attempt_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due {_ocd_t(-16)}; 0 in backoff"
    )


def test_ocd_pending_deferral_not_yet_overdue_itself_stays_ok() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-60))
    _ocd_job(conn, site_id, station_id, status="pending", next_attempt_at=_ocd_t(-14))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_mixed_overdue_and_backoff_detail_counts_both() -> None:
    """Mutant: drop the backoff suffix -- the detail string would lose its
    trailing "; N in backoff" clause, differing from this row's expectation.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    overdue_station = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, overdue_station, next_poll_at=_ocd_t(-16))
    backoff_station = _ocd_station(conn, site_id, pws_id="ISTATION02")
    _ocd_poll_state(conn, backoff_station, next_poll_at=_ocd_t(-16))
    _ocd_job(
        conn, site_id, backoff_station, status="pending", next_attempt_at=_ocd_t(10)
    )
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due {_ocd_t(-16)}; 1 in backoff"
    )


def test_ocd_cooldown_from_recent_failure_suppresses_the_trip() -> None:
    """Mutant: drop the cooldown term -- without it, this row's due basis
    (-30) alone is past the 15-min cutoff and would trip.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-30))
    _ocd_job(conn, site_id, station_id, status="failed", updated_at=_ocd_t(-30))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_cooldown_expired_exactly_at_the_threshold_trips() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-90))
    _ocd_job(conn, site_id, station_id, status="failed", updated_at=_ocd_t(-75))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1


def test_ocd_unparseable_pending_next_attempt_is_ignored() -> None:
    """Mutant: fail closed on an unparseable pending stamp -- this row would
    trip if the garbage stamp were treated as overdue instead of ignored.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-10))
    _ocd_job(conn, site_id, station_id, status="pending", next_attempt_at="garbage")
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_unparseable_due_basis_fails_open_to_alerting() -> None:
    """Mutant: treat an unparseable next_poll_at as not overdue -- this row
    would read ok=True if the garbage due basis failed closed instead of
    open; oldest stays "unknown" since a None due never sets `oldest`.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at="not-a-timestamp")
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due unknown; 0 in backoff"
    )


def test_ocd_dead_poller_no_heartbeat_row_still_trips() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    # No runtime_state row at all for current_obs_poller_last_loop_at.
    cond = _ocd(conn)
    assert cond["ok"] is False


def test_ocd_fresh_poller_heartbeat_does_not_suppress_the_trip() -> None:
    """Mutant: read the poller heartbeat -- a fresh heartbeat would then
    incorrectly hold this row ok=True. Paired with the dead-poller test
    above: both a missing and a live heartbeat trip identically, which is
    the point -- this condition reads only schedule/job rows, never the
    poller's own liveness stamp.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES"
        " ('current_obs_poller_last_loop_at', ?)",
        (isoformat_utc(_OCD_N),),
    )
    cond = _ocd(conn)
    assert cond["ok"] is False


def test_ocd_pending_job_never_claimed_still_measures_lateness() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-20))
    _ocd_job(conn, site_id, station_id, status="pending", next_attempt_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due {_ocd_t(-16)}; 0 in backoff"
    )


def test_ocd_stuck_running_job_does_not_exempt_the_station() -> None:
    """Mutant: exempt stations with a running job -- this row would read
    ok=True if a running job suppressed the due-basis lateness.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-20))
    _ocd_job(conn, site_id, station_id, status="running", updated_at=_ocd_t(-20))
    cond = _ocd(conn)
    assert cond["ok"] is False


def test_ocd_disabled_site_excludes_its_station() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn, enabled=0)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_disabled_station_is_excluded() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01", enabled=0)
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_grace_active_reports_ok_with_count_passed_through_no_detail() -> None:
    conn = _ocd_conn()
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_started_at', ?)",
        (isoformat_utc(_OCD_N - timedelta(minutes=5)),),
    )
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 1
    assert "detail" not in cond


def test_ocd_pipeline_disabled_reports_skipped() -> None:
    conn = _ocd_conn()
    cond = _ocd(conn, now=_OCD_N)
    verdict = _ocd_verdict(conn, pipeline_enabled=False)
    skipped_cond = _cond(verdict, "obs_collection_delayed")
    assert skipped_cond["skipped"] is True
    # sanity: with pipeline enabled the same empty DB reports not-skipped.
    assert cond["skipped"] is False


# --- Extra: monitor-side fallbacks, cooldown edges, the constant pin --------


def test_ocd_missing_poll_state_row_falls_back_to_station_created_at() -> None:
    """No station_poll_state row at all: due basis must fall back to the
    station's own created_at, not to an absent/unparseable value. created_at
    is set 5 min in the future here -- if the fallback did not reach it, the
    NULL due basis would instead read as unconditionally overdue.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)"
        " VALUES (?, 'ISTATION01', 40.0, -105.0, 900.0, 1, ?)",
        (site_id, _ocd_t(5)),
    )
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_null_next_poll_at_falls_back_to_poll_state_updated_at() -> None:
    """next_poll_at is NULL but the station_poll_state row exists: due basis
    must fall back to that row's updated_at, not skip straight past it to
    station.created_at. created_at is seeded overdue (-16) and updated_at is
    seeded not-yet-due (+5) so the two fallbacks diverge.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)"
        " VALUES (?, 'ISTATION01', 40.0, -105.0, 900.0, 1, ?)",
        (site_id, _ocd_t(-16)),
    )
    station_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    _ocd_poll_state(conn, station_id, next_poll_at=None, updated_at=_ocd_t(5))
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_malformed_failure_stamp_is_ignored() -> None:
    """A garbage failure updated_at must not apply any cooldown: the station
    is judged on its due basis alone, exactly as if it had never failed.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    _ocd_job(conn, site_id, station_id, status="failed", updated_at="not-a-timestamp")
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1


def test_ocd_active_job_suppresses_the_failure_cooldown() -> None:
    """The latest job is 'failed' (recent, still inside its cooldown), but an
    OLDER job for the same key is still pending/running -- has_active is
    True, so the cooldown term must be skipped and the station judged on its
    due basis alone (overdue). Without the `not has_active` guard, the
    cooldown would hold it ok=True until the cooldown itself elapses.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    _ocd_job(conn, site_id, station_id, status="running", updated_at=_ocd_t(-20))
    _ocd_job(conn, site_id, station_id, status="failed", updated_at=_ocd_t(-5))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1


def test_ocd_cooldown_overflow_saturates_to_not_ended() -> None:
    """A failure stamp so far in the future that stamp + 1h overflows
    datetime's range must saturate the cooldown end at datetime.max, i.e.
    "has not ended" -- not raise and not treat the cooldown as expired.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    station_id = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, station_id, next_poll_at=_ocd_t(-16))
    _ocd_job(
        conn,
        site_id,
        station_id,
        status="failed",
        updated_at="9999-12-31T23:30:00Z",
    )
    cond = _ocd(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_ocd_cooldown_overflow_station_counts_as_backoff_when_another_trips() -> None:
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    overflow_station = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, overflow_station, next_poll_at=_ocd_t(-16))
    _ocd_job(
        conn,
        site_id,
        overflow_station,
        status="failed",
        updated_at="9999-12-31T23:30:00Z",
    )
    overdue_station = _ocd_station(conn, site_id, pws_id="ISTATION02")
    _ocd_poll_state(conn, overdue_station, next_poll_at=_ocd_t(-16))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due {_ocd_t(-16)}; 1 in backoff"
    )


def test_ocd_fresh_pending_station_does_not_inflate_the_backoff_count() -> None:
    """A station whose due basis is not itself overdue (a fresh pending
    deferral, both in the future) must not be counted as "in backoff" just
    because it sits next to a genuinely overdue station.
    """
    conn = _ocd_conn()
    site_id = _seed_site(conn)
    overdue_station = _ocd_station(conn, site_id, pws_id="ISTATION01")
    _ocd_poll_state(conn, overdue_station, next_poll_at=_ocd_t(-16))
    fresh_station = _ocd_station(conn, site_id, pws_id="ISTATION02")
    _ocd_poll_state(conn, fresh_station, next_poll_at=_ocd_t(30))
    _ocd_job(conn, site_id, fresh_station, status="pending", next_attempt_at=_ocd_t(35))
    cond = _ocd(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == (
        f"1 stations overdue at least {OBS_COLLECTION_DELAY_MINUTES} min;"
        f" oldest due {_ocd_t(-16)}; 0 in backoff"
    )


def test_ocd_failure_cooldown_constant_matches_the_schedulers_enqueue_cooldown() -> (
    None
):
    assert _CURRENT_OBS_FAILURE_COOLDOWN == _DUE_JOB_FAILURE_COOLDOWN


# ---------------------------------------------------------------------------
# WL1-WL16 -- main_worker_liveness monitor condition (§14.12, 0.16.3 commit 9).
# Every case runs on asof_conn() and calls build_verdict with the fixed
# synthetic _WL_N below. Times are minutes relative to _WL_N, and every
# seeded stamp is isoformat_utc(_WL_N + offset), matching the plan's W/S/I/C
# notation.
# ---------------------------------------------------------------------------

_WL_N = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)

_WL_WORDING_3 = (
    "main-worker liveness evidence missing: no loop stamp, worker start or import time"
)


def _wl_stamp(offset_minutes: float) -> str:
    return isoformat_utc(_WL_N + timedelta(minutes=offset_minutes))


def _wl_wording1(offset_minutes: float) -> str:
    return (
        "main-worker liveness evidence stale: last loop stamp"
        f" {_wl_stamp(offset_minutes)};"
        " no main-lane job claimed since then is running"
    )


def _wl_wording2(offset_minutes: float) -> str:
    return (
        "main-worker liveness evidence missing: no loop stamp since worker"
        f" start or import at {_wl_stamp(offset_minutes)};"
        " no main-lane job claimed since then is running"
    )


def _wl_seed_w(conn: sqlite3.Connection, offset_minutes: float) -> None:
    conn.execute(
        "INSERT INTO runtime_state(key, value) VALUES ('worker_started_at', ?)",
        (_wl_stamp(offset_minutes),),
    )


def _wl_seed_s(conn: sqlite3.Connection, value: str) -> None:
    conn.execute(
        "INSERT INTO runtime_state(key, value) VALUES ('worker_last_loop_at', ?)",
        (value,),
    )


def _wl_seed_i(conn: sqlite3.Connection, offset_minutes: float) -> None:
    conn.execute(
        """
        INSERT INTO runtime_state(key, value, updated_at)
        VALUES ('import_rebuild_state', 'done', ?)
        """,
        (_wl_stamp(offset_minutes),),
    )


def _wl_seed_running(
    conn: sqlite3.Connection, site_id: int, job_type: str, offset_minutes: float
) -> None:
    # job_key is nullable (migrations.py), but a synthetic literal keeps the
    # row self-documenting; the jobs CHECK requires site_id for both types
    # used here (fetch_obs, fetch_current_obs).
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, status, updated_at)
        VALUES (?, ?, 'wl-test', 'running', ?)
        """,
        (job_type, site_id, _wl_stamp(offset_minutes)),
    )


def _wl_verdict(
    conn: sqlite3.Connection, *, pipeline_enabled: bool = True
) -> dict[str, object]:
    return build_verdict(
        conn,
        pipeline_enabled=pipeline_enabled,
        budget_enabled=False,
        db_enabled=False,
        now=_WL_N,
        export_sweeper_dead=None,
    )


def test_wl1_stale_loop_stamp_trips_wording1_and_degrades_overall() -> None:
    """W -30, S -16: tripped, wording 1 with <N-16>, no count key, severity
    warning, group pipeline, and overall degrades to warning -- no site is
    seeded, so no other pipeline condition trips alongside it.

    Kills: a severity other than warning on this condition (a mutant naming
    "critical" or "ok" here would fail the severity assertion); count ever
    populated on this condition (the count assertion would fail).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-16))
    verdict = _wl_verdict(conn)
    cond = _cond(verdict, "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording1(-16)
    assert "count" not in cond
    assert cond["severity"] == "warning"
    assert cond["group"] == "pipeline"
    assert verdict["overall"] == "warning"


def test_wl2_threshold_boundary_at_exactly_15_min_trips() -> None:
    """W -30, S exactly -15: tripped -- the threshold uses >=, not >.

    Kills: `>` in place of `>=` on the threshold comparison (this exact
    15-min boundary would read ok instead of tripped).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-15))
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False


def test_wl3_just_inside_threshold_reads_ok() -> None:
    """W -30, S -14: ok -- paired with WL2 so the boundary is pinned from
    both sides.

    Kills: a mutant that trips whenever no running row is current,
    regardless of age (this 14-min-old, job-free evidence would flip to
    tripped).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-14))
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is True
    assert "detail" not in cond


def test_wl4_running_job_claimed_after_the_stamp_keeps_it_ok() -> None:
    """W -30, S -16, running fetch_obs C -15: ok, and problem_jobs is ok too
    (-15 is inside its own 20-min running-arm window).

    Kills: measuring age from max(S, C) instead of from B (S alone) -- C -15
    is newer than S -16, so such a mutant would measure the age from C and
    trip at exactly the 15-min threshold, flipping this fixture from ok to
    tripped.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "wl-site")
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-16))
    _wl_seed_running(conn, site_id, "fetch_obs", -15)
    verdict = _wl_verdict(conn)
    cond = _cond(verdict, "main_worker_liveness")
    assert cond["ok"] is True
    assert "detail" not in cond
    assert _cond(verdict, "problem_jobs")["ok"] is True


def test_wl4_running_job_claimed_exactly_at_the_stamp_counts_as_current() -> None:
    """W -30, S -16, running fetch_obs C -16 (C equals B, and B is already
    stale): ok, with no detail.

    Kills: `>` in place of `>=` on the current-row test (C >= B) -- this C
    equals B exactly, so such a mutant would read this running row as not
    current and trip on the stale S, same as WL6.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "wl-site")
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-16))
    _wl_seed_running(conn, site_id, "fetch_obs", -16)
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is True
    assert "detail" not in cond


def test_wl5_current_job_older_than_stuck_window_stays_ok_here() -> None:
    """W -60, S -40, running fetch_obs C -39: ok here; problem_jobs trips
    instead -- a wedge inside a job stays with the running arm, not this
    condition.

    Kills: a staleness cap on a current row, such as requiring
    `now - C < 20 min` (this C is 39 min old, so such a cap would flip this
    condition to tripped AND double-report the wedge in problem_jobs too).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "wl-site")
    _wl_seed_w(conn, -60)
    _wl_seed_s(conn, _wl_stamp(-40))
    _wl_seed_running(conn, site_id, "fetch_obs", -39)
    verdict = _wl_verdict(conn)
    cond = _cond(verdict, "main_worker_liveness")
    assert cond["ok"] is True
    assert "detail" not in cond
    assert _cond(verdict, "problem_jobs")["ok"] is False


def test_wl6_running_row_claimed_before_the_last_stamp_is_abandoned() -> None:
    """W -60, S -16, running fetch_obs C -20 (claimed before the last
    stamp): tripped, wording 1 with <N-16> -- an abandoned row claimed
    before B cannot mask the condition.

    Kills: counting every running row as current, dropping C >= B (this C
    is older than B, so such a mutant would read ok instead of tripped).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "wl-site")
    _wl_seed_w(conn, -60)
    _wl_seed_s(conn, _wl_stamp(-16))
    _wl_seed_running(conn, site_id, "fetch_obs", -20)
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording1(-16)


def test_wl7_loop_stamp_older_than_import_falls_back_to_wording2() -> None:
    """W -60, S -40, I -20, running fetch_obs C -30 (an exporter's row):
    tripped, wording 2 with <N-20> -- S is older than F, so it is ignored
    and the basis falls back to F=I.

    Kills: leaving I out of F (B would fall back to S at -40 instead, and
    C -30 >= -40, so this fixture would read ok); using S even when it is
    older than F (B would then be S -40, and C -30 >= -40, so this fixture
    would read ok).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "wl-site")
    _wl_seed_w(conn, -60)
    _wl_seed_s(conn, _wl_stamp(-40))
    _wl_seed_i(conn, -20)
    _wl_seed_running(conn, site_id, "fetch_obs", -30)
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording2(-20)


def test_wl8_running_current_obs_poller_row_never_counts_as_current() -> None:
    """W -60, S -16, running fetch_current_obs C -1, and a fresh poller
    heartbeat at N: tripped, wording 1 with <N-16> -- the poller lane's
    running row and heartbeat are outside this condition's evidence.

    Kills: dropping the lane filter from the jobs arm, so a poller row
    counts as current (this C -1 is well inside B -16, so such a mutant
    would read ok instead of tripped); reading the poller heartbeat into B
    (the fresh N-stamped poller heartbeat would then read ok).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "wl-site")
    _wl_seed_w(conn, -60)
    _wl_seed_s(conn, _wl_stamp(-16))
    _wl_seed_running(conn, site_id, "fetch_current_obs", -1)
    conn.execute(
        "INSERT INTO runtime_state(key, value)"
        " VALUES ('current_obs_poller_last_loop_at', ?)",
        (_wl_stamp(0),),
    )
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording1(-16)


def test_wl9_start_only_with_no_loop_stamp_trips_wording2() -> None:
    """W -20 only: tripped, wording 2 with <N-20>.

    Kills: treating missing evidence as ok (this fixture has W but no S,
    C, or I, and would read ok under such a mutant); B = S* only, ignoring
    F (with no S at all, B would be None here, giving wording 3 instead of
    wording 2).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -20)
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording2(-20)


def test_wl10_loop_stamp_older_than_start_is_ignored() -> None:
    """W -20, S -30 (a stamp from before this start): tripped, wording 2
    with <N-20> -- S older than F is not S*, so the basis falls back to F.

    Kills: using S even when it is older than F (would give wording 1 with
    <N-30> instead of wording 2 with <N-20>).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -20)
    _wl_seed_s(conn, _wl_stamp(-30))
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording2(-20)


def test_wl11_unparseable_loop_stamp_is_missing_evidence_not_an_escaped_error() -> None:
    """W -20, S 'garbage': tripped, wording 2 with <N-20>, and build_verdict
    returns normally -- an unparseable stamp becomes missing evidence, not
    a raised ValueError.

    Kills: letting parse_utc's ValueError escape the helper (calling
    _wl_verdict itself would raise instead of returning a verdict).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -20)
    _wl_seed_s(conn, "garbage")
    verdict = _wl_verdict(conn)  # must not raise
    cond = _cond(verdict, "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording2(-20)


def test_wl12_no_evidence_at_all_trips_wording3() -> None:
    """Nothing seeded: tripped, wording 3 exactly.

    Kills: treating missing evidence as ok (an empty runtime_state and no
    running rows would read ok under such a mutant).
    """
    conn = asof_conn()
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _WL_WORDING_3


def test_wl13_fresh_start_inside_15_min_reads_ok() -> None:
    """W -14 only (a fresh start; grace ended at -4): ok.

    Kills: B = S* only, ignoring F (with no S, B would be None here, which
    is always tripped -- this fixture would flip from ok to tripped).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -14)
    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is True
    assert "detail" not in cond


def test_wl14_skipped_when_pipeline_disabled() -> None:
    """pipeline_enabled=False: main_worker_liveness is present with
    skipped True.

    Kills: omitting the id from the skipped tuple (the id would be absent
    from the verdict's conditions entirely, and _cond would raise
    StopIteration instead of returning a skipped condition).
    """
    conn = asof_conn()
    cond = _cond(_wl_verdict(conn, pipeline_enabled=False), "main_worker_liveness")
    assert cond["skipped"] is True
    assert cond["ok"] is True


def test_wl15_evidence_is_read_in_exactly_one_statement() -> None:
    """WL1's seed, traced with set_trace_callback around build_verdict:
    exactly one traced statement contains 'worker_last_loop_at', and that
    statement also contains 'worker_started_at', 'import_rebuild_state' and
    'FROM jobs' -- confirming the one-snapshot compound read.

    Kills: reading the evidence in two statements (a second, separate read
    naming 'worker_last_loop_at' would make the match count 2, or split the
    four substrings across two different statements).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-16))
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        _wl_verdict(conn)
    finally:
        conn.set_trace_callback(None)
    matches = [s for s in statements if "worker_last_loop_at" in s]
    assert len(matches) == 1
    (statement,) = matches
    assert "worker_started_at" in statement
    assert "import_rebuild_state" in statement
    assert "FROM jobs" in statement


def test_wl16_heartbeat_write_failure_reads_as_stale_evidence_not_hung(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A heartbeat write failure (tests/test_m1_m5.py:398-428's pattern)
    logs the ERROR and returns `now` unchanged, leaving S exactly where it
    was: build_verdict then reports wording 1 with <N-16>, never a "hung"
    claim.

    Kills: the heartbeat's failure path writing a fallback stamp by another
    route (S would move, and the verdict would read ok instead of tripped);
    wording that says the worker is hung (the exact-text assertion below).
    """
    conn = asof_conn()
    _wl_seed_w(conn, -30)
    _wl_seed_s(conn, _wl_stamp(-16))
    conn.commit()

    def _fail_heartbeat(_conn: sqlite3.Connection, key: str) -> None:
        raise sqlite3.OperationalError("synthetic heartbeat failure")

    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", _fail_heartbeat
    )

    class FakeDb:
        async def write(self, fn):  # type: ignore[no-untyped-def]
            return fn(conn)

    result = asyncio.run(
        _maybe_stamp_runtime_heartbeat(
            FakeDb(),  # type: ignore[arg-type]
            "worker_last_loop_at",
            0.0,
            1234.0,
        )
    )

    assert result == 1234.0
    assert any(
        r.levelno == logging.ERROR
        and r.getMessage() == "runtime heartbeat write failed key=worker_last_loop_at"
        for r in caplog.records
    )
    row = conn.execute(
        "SELECT value FROM runtime_state WHERE key = 'worker_last_loop_at'"
    ).fetchone()
    assert row["value"] == _wl_stamp(-16)

    cond = _cond(_wl_verdict(conn), "main_worker_liveness")
    assert cond["ok"] is False
    assert cond["detail"] == _wl_wording1(-16)
    assert "hung" not in cond["detail"]

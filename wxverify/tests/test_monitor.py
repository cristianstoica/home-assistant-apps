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
from wxverify.db.connection import close_db, get_db
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


# --- Task 4: Group 1 staleness + liveness conditions -------------------------


def _seed_site(conn: sqlite3.Connection, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES ('S', 47.0, 25.0, 900.0, 'UTC', ?)
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
                VALUES (?, 'FAKE2', 47.0, 25.0, 900.0, 1)
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
                conn, site_id, _feed_id(conn, "open-meteo", "ecmwf_ifs"),
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
                VALUES (?, 'FAKE1', 47.0, 25.0, 900.0, 1)
                """,
                (site_id,),
            )
            # last_obs_at NULL → never-observed → stale.

        db.write_sync(_seed)
        body = client.get("/api/health/monitor").json()
        assert _cond(body, "obs_stale")["ok"] is False
        assert _cond(body, "fetch_obs_live")["ok"] is False


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
                conn, site_id, _feed_id(conn, "open-meteo", "ecmwf_ifs"),
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
                conn, site_id, _feed_id(conn, "open-meteo", "ecmwf_ifs"),
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

    monkeypatch.setattr(
        "wxverify.api.routes.health.utc_now", lambda: fixed_now
    )

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
                conn, site_id, _feed_id(conn, "open-meteo", "ecmwf_ifs"),
                last_run_at="2026-07-09T11:00:00Z",
            )
            # Eligible obs target.
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'FAKE3', 47.0, 25.0, 900.0, 1)
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

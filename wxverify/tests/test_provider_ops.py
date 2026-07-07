from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.__main__ import main
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import (
    claim_next_job,
    complete,
    defer_job,
    enqueue_if_absent,
)
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    GridProvenance,
    NormalizedSample,
)
from wxverify.provider_ops import provider_health, reconcile_catalog
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.domain_backoff import source_domain
from wxverify.worker.processor import dispatch


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path, name: str = "wxverify.db") -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / name
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection, *, enabled: bool = True) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES ('ProviderOps', 47, 25, 900, 'UTC', ?)
            """,
            (1 if enabled else 0,),
        ).lastrowid
    )


def _feed_id(
    conn: sqlite3.Connection, source: str = "open-meteo", model: str | None = None
) -> int:
    if model is None:
        row = conn.execute(
            "SELECT id FROM feeds WHERE source=? ORDER BY id LIMIT 1", (source,)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
        ).fetchone()
    assert row is not None
    return int(row["id"])


def _job_count(conn: sqlite3.Connection, job_type: str = "fetch_feed") -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type=?", (job_type,)
        ).fetchone()["n"]
    )


def _run_claimed_job(conn: sqlite3.Connection) -> int:
    job = claim_next_job(conn)
    assert job is not None
    try:
        asyncio.run(dispatch(get_db(), job))
        complete(conn, job.id)
    except JobDeferred as exc:
        defer_job(conn, job.id, exc.next_attempt_at)
    except JobCancelled:
        complete(conn, job.id)
    return job.id


def test_reconcile_inserts_missing_catalog_rows_without_overwriting(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    conn.execute("UPDATE sources SET daily_call_limit=123 WHERE source='open-meteo'")
    conn.execute(
        """
        UPDATE feeds
        SET fetch_interval_minutes=999
        WHERE source='open-meteo' AND model='ecmwf_ifs'
        """
    )
    conn.execute("DELETE FROM feeds WHERE source='visualcrossing'")
    conn.execute("DELETE FROM sources WHERE source='visualcrossing'")

    first = reconcile_catalog(conn)
    second = reconcile_catalog(conn)

    assert first.sources_inserted == 1
    assert first.feeds_inserted == 1
    assert second.sources_inserted == 0
    assert second.feeds_inserted == 0
    assert (
        conn.execute(
            "SELECT daily_call_limit FROM sources WHERE source='open-meteo'"
        ).fetchone()["daily_call_limit"]
        == 123
    )
    assert (
        conn.execute(
            """
            SELECT fetch_interval_minutes
            FROM feeds
            WHERE source='open-meteo' AND model='ecmwf_ifs'
            """
        ).fetchone()["fetch_interval_minutes"]
        == 999
    )


def test_api_subscription_enable_enqueues_and_disable_is_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "api-provider-ops.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id, feed_id = db.write_sync(
            lambda conn: (_insert_site(conn), _feed_id(conn))
        )
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        enabled = client.put(
            f"/api/sites/{site_id}/feeds/{feed_id}",
            json={"enabled": True},
            headers=headers,
        )
        assert enabled.status_code == 200
        assert enabled.json()["fetch_enqueued"] is True
        assert (
            db.read_sync(
                lambda conn: conn.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM jobs
                    WHERE type='fetch_feed' AND site_id=?
                    """,
                    (site_id,),
                ).fetchone()["n"]
            )
            == 1
        )

        db.write_sync(lambda conn: conn.execute("DELETE FROM jobs"))
        disabled = client.put(
            f"/api/sites/{site_id}/feeds/{feed_id}",
            json={"enabled": False},
            headers=headers,
        )
        assert disabled.status_code == 200
        assert disabled.json()["fetch_enqueued"] is None
        assert db.read_sync(_job_count) == 0


def test_provider_cli_rejects_virtual_and_meteoblue_member_without_side_effects(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    db_path = config.db_path
    site_id = _insert_site(conn)
    virtual_id = _feed_id(conn, "virtual", "_persistence")
    member_id = int(
        conn.execute(
            """
            INSERT INTO feeds
                (source, model, enabled, default_subscribed,
                 fetch_interval_minutes, max_lead_hours, is_virtual)
            VALUES ('meteoblue', 'NEMS4', 1, 0, 1440, 168, 0)
            """
        ).lastrowid
    )

    rc_virtual = main(
        [
            "--db",
            db_path,
            "providers",
            "enable",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(virtual_id),
        ]
    )
    out_virtual = capsys.readouterr().out
    assert rc_virtual == 1
    assert "virtual feeds are subscription-exempt" in out_virtual

    rc_member = main(
        [
            "--db",
            db_path,
            "providers",
            "enable",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(member_id),
        ]
    )
    out_member = capsys.readouterr().out
    assert rc_member == 1
    assert "meteoblue members resolve through the package feed" in out_member

    db = get_db()
    assert (
        db.read_sync(
            lambda conn: conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM site_feed_state
                WHERE feed_id IN (?, ?)
                """,
                (virtual_id, member_id),
            ).fetchone()["n"]
        )
        == 0
    )
    assert db.read_sync(_job_count) == 0


def test_disabled_site_or_feed_skips_fetch_queue_cleanly(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    db_path = config.db_path
    disabled_site_id = _insert_site(conn, enabled=False)
    active_site_id = _insert_site(conn)
    feed_id = _feed_id(conn)
    conn.execute("UPDATE feeds SET enabled=0 WHERE id=?", (feed_id,))

    rc_enable = main(
        [
            "--db",
            db_path,
            "providers",
            "enable",
            "--site-id",
            str(disabled_site_id),
            "--feed-id",
            str(feed_id),
        ]
    )
    enable_out = capsys.readouterr().out
    assert rc_enable == 0
    assert "enabled_fetch_skipped" in enable_out
    assert "site disabled" in enable_out
    assert get_db().read_sync(_job_count) == 0

    rc_fetch = main(
        [
            "--db",
            db_path,
            "providers",
            "fetch",
            "--site-id",
            str(active_site_id),
            "--feed-id",
            str(feed_id),
        ]
    )
    fetch_out = capsys.readouterr().out
    assert rc_fetch == 1
    assert "feed disabled" in fetch_out
    assert get_db().read_sync(_job_count) == 0


def test_provider_health_aggregates_meteoblue_member_samples(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "health-providers.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            site_id = _insert_site(conn)
            package_id = _feed_id(conn, "meteoblue", "multimodel")
            member_id = int(
                conn.execute(
                    """
                    INSERT INTO feeds
                        (source, model, enabled, default_subscribed,
                         fetch_interval_minutes, max_lead_hours, is_virtual)
                    VALUES ('meteoblue', 'NEMS4', 1, 0, 1440, 168, 0)
                    """
                ).lastrowid
            )
            conn.execute(
                """
                INSERT INTO site_feed_state
                    (site_id, feed_id, enabled, last_run_at, error_count)
                VALUES (?, ?, 1, '2035-01-01T00:00:00Z', 0)
                """,
                (site_id, package_id),
            )
            for variable, value in (
                ("temperature", 4.0),
                ("wind", 5.0),
                ("precip", 1.2),
            ):
                conn.execute(
                    """
                    INSERT INTO forecast_samples
                        (site_id, feed_id, variable, issued_at, valid_at,
                         lead_hours, value, source_raw, model_run_id, fetched_at)
                    VALUES (?, ?, ?, '2035-01-01T00:00:00Z',
                            '2035-01-01T03:00:00Z', 3, ?, 'raw',
                            'NEMS4:2035-01-01T00:00:00Z',
                            '2035-01-01T00:05:00Z')
                    """,
                    (site_id, member_id, variable, value),
                )
            return site_id, package_id

        site_id, package_id = db.write_sync(_seed)
        response = client.get(
            f"/api/health/providers?site_id={site_id}&source=meteoblue"
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload[0]["source"] == "meteoblue"
        feed = payload[0]["feeds"][0]
        assert feed["feed_id"] == package_id
        assert feed["sample_count"] == 3
        assert feed["variables"] == ["precip", "temperature", "wind"]
        assert feed["model_run_count"] == 1
        assert feed["bad_sample_count"] == 0
        assert feed["status"] == "ok"


def test_provider_health_reports_error_before_never_run(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo")
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, enabled, last_error, error_count)
        VALUES (?, ?, 1, 'provider boom', 1)
        """,
        (site_id, feed_id),
    )

    payload = provider_health(conn, site_id=site_id, sources=("open-meteo",))
    feed = next(
        item
        for group in payload
        for item in group["feeds"]
        if item["feed_id"] == feed_id
    )

    assert feed["last_run_at"] is None
    assert feed["last_error"] == "provider boom"
    assert feed["status"] == "error"


def test_provider_health_includes_requested_missing_catalog_source(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)
    conn.execute("DELETE FROM feeds WHERE source='visualcrossing'")
    conn.execute("DELETE FROM sources WHERE source='visualcrossing'")

    payload = provider_health(conn, sources=("visualcrossing",))

    assert len(payload) == 1
    assert payload[0]["source"] == "visualcrossing"
    assert payload[0]["source_seeded"] is False
    assert payload[0]["feeds"] == []


def test_doctor_redacts_present_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    db_path = config.db_path
    _insert_site(conn)
    monkeypatch.setenv("WXV_VISUALCROSSING_KEY", "sentinel-value-99999")

    rc = main(
        [
            "--db",
            db_path,
            "providers",
            "doctor",
            "--source",
            "visualcrossing",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 1
    assert "key: present" in out
    assert "sentinel-value-99999" not in out


def test_worker_dispatch_defers_budget_and_backoff_as_pending_jobs(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn)

    conn.execute("UPDATE sources SET daily_call_limit=0 WHERE source='open-meteo'")
    enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    budget_job_id = _run_claimed_job(conn)
    budget_job = conn.execute(
        "SELECT status, next_attempt_at FROM jobs WHERE id=?", (budget_job_id,)
    ).fetchone()
    assert budget_job["status"] == "pending"
    assert budget_job["next_attempt_at"] is not None

    conn.execute("DELETE FROM jobs")
    conn.execute("UPDATE sources SET daily_call_limit=10000 WHERE source='open-meteo'")
    conn.execute(
        """
        INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
        VALUES (?, '2035-01-01T00:00:00Z', 1)
        """,
        (source_domain("open-meteo"),),
    )
    enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    backoff_job_id = _run_claimed_job(conn)
    backoff_job = conn.execute(
        "SELECT status, next_attempt_at FROM jobs WHERE id=?", (backoff_job_id,)
    ).fetchone()
    assert backoff_job["status"] == "pending"
    assert backoff_job["next_attempt_at"] == "2035-01-01T00:00:00Z"


def test_fetch_run_now_unavailable_and_budget_are_clean_cli_reports(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    db_path = config.db_path
    site_id = _insert_site(conn)
    visualcrossing_id = _feed_id(conn, "visualcrossing", "blend")
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 1)",
        (site_id, visualcrossing_id),
    )
    monkeypatch.delenv("WXV_VISUALCROSSING_KEY", raising=False)

    unavailable = main(
        [
            "--db",
            db_path,
            "providers",
            "fetch",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(visualcrossing_id),
            "--run-now",
        ]
    )
    unavailable_out = capsys.readouterr().out
    assert unavailable == 1
    assert "unavailable" in unavailable_out

    conn = get_db()._conn  # noqa: SLF001
    open_meteo_id = _feed_id(conn)
    conn.execute("UPDATE sources SET daily_call_limit=0 WHERE source='open-meteo'")
    budget = main(
        [
            "--db",
            db_path,
            "providers",
            "fetch",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(open_meteo_id),
            "--run-now",
        ]
    )
    budget_out = capsys.readouterr().out
    assert budget == 1
    assert "budget_exhausted" in budget_out


def test_smoke_requires_usable_samples_and_accepts_idempotent_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    db_path = config.db_path
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn)
    empty = False

    class FakeAdapter:
        supports_historical = True

        def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
            if empty:
                return FetchResult(samples=[], grid=None)
            return FetchResult(
                samples=[
                    NormalizedSample(
                        model=req.model,
                        variable=variable,
                        issued_at="2035-01-01T00:00:00Z",
                        valid_at="2035-01-01T03:00:00Z",
                        lead_hours=3,
                        value=value,
                        source_raw=f"{value}",
                        model_run_id=f"{req.model}:2035-01-01T00:00:00Z",
                    )
                    for variable, value in (
                        ("temperature", 5.0),
                        ("wind", 6.0),
                        ("precip", 0.5),
                    )
                ],
                grid=GridProvenance(grid_lat=47.0, grid_lon=25.0),
            )

        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ) -> FetchResult | None:
            return None

    def fake_build_adapter(source: str, client: httpx.AsyncClient) -> FakeAdapter:
        assert source == "open-meteo"
        assert client is not None
        return FakeAdapter()

    monkeypatch.setattr("wxverify.worker.feed_fetch.build_adapter", fake_build_adapter)
    first = main(
        [
            "--db",
            db_path,
            "providers",
            "smoke",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(feed_id),
        ]
    )
    first_out = capsys.readouterr().out
    assert first == 0
    assert "success" in first_out

    second = main(
        [
            "--db",
            db_path,
            "providers",
            "smoke",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(feed_id),
        ]
    )
    second_out = capsys.readouterr().out
    assert second == 0
    assert "inserted=0" in second_out

    empty = True
    no_op = main(
        [
            "--db",
            db_path,
            "providers",
            "smoke",
            "--site-id",
            str(site_id),
            "--feed-id",
            str(feed_id),
        ]
    )
    no_op_out = capsys.readouterr().out
    assert no_op == 1
    assert "200 / 0 usable samples" in no_op_out


def test_jobs_purge_removes_only_old_failed_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    db_path = config.db_path
    conn.execute(
        """
        INSERT INTO jobs
            (type, site_id, job_key, payload, status, updated_at, last_error)
        VALUES
            ('fetch_obs', ?, 'old-failed', '{}', 'failed',
             '2000-01-01T00:00:00Z', 'old failed'),
            ('fetch_obs', ?, 'new-failed', '{}', 'failed',
             strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'new failed')
        """,
        (site_id, site_id),
    )

    dry_run = main(
        [
            "--db",
            db_path,
            "jobs",
            "purge",
            "--failed-older-than-hours",
            "24",
            "--dry-run",
        ]
    )
    dry_run_out = capsys.readouterr().out
    assert dry_run == 0
    assert "would_purge failed_jobs=1 older_than_hours=24" in dry_run_out
    assert (
        get_db().read_sync(
            lambda db_conn: db_conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE status='failed'"
            ).fetchone()["n"]
        )
        == 2
    )

    purged = main(
        [
            "--db",
            db_path,
            "jobs",
            "purge",
            "--failed-older-than-hours",
            "24",
        ]
    )
    purged_out = capsys.readouterr().out
    assert purged == 0
    assert "purged failed_jobs=1 older_than_hours=24" in purged_out
    rows = get_db().read_sync(
        lambda db_conn: db_conn.execute(
            "SELECT job_key FROM jobs WHERE status='failed' ORDER BY job_key"
        ).fetchall()
    )
    assert [row["job_key"] for row in rows] == ["new-failed"]

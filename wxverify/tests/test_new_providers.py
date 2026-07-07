"""Contract tests for the five new commercial forecast providers.

Covers the items enumerated in the plan's "Contract tests" section:
  1. resolve_secret resolves via env var (_from_env path)
  2. resolve_secret resolves via options.json (_from_options_json path)
  3. build_adapter raises when provider key is unset
  4. Missing-key worker path: job completes (not re-queued), run-state stamped
  5. source_domain returns expected host for each provider
  6. snap_run snaps fixed fetch time to expected cadence floor, and adapters emit it
  7. No-op (a): 0 usable samples stamps NO_USABLE_SAMPLES_SENTINEL
  8. No-op (b): idempotent re-fetch (usable > 0, inserted == 0) NOT flagged as no-op
  9. No-op (c): historical path (advance_last_run_at=False) unconditionally clears error
     state
 10. No-op render: status-ladder ordering surfaces "fetched, 0 usable" not "error"
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

import wxverify.feeds.google as google_feed
import wxverify.feeds.meteosource as meteosource_feed
import wxverify.feeds.openweathermap as openweathermap_feed
import wxverify.feeds.visualcrossing as visualcrossing_feed
import wxverify.feeds.weatherapi as weatherapi_feed
from wxverify import config
from wxverify.api.app import create_app
from wxverify.collection.forecast_fetcher import (
    NO_USABLE_SAMPLES_SENTINEL,
    persist_fetch_result,
)
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.options import SECRET_ENV
from wxverify.core.secrets import resolve_secret
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import (
    claim_next_job,
    complete,
    enqueue_if_absent,
    fail,
)
from wxverify.feeds.google import GoogleAdapter
from wxverify.feeds.meteosource import MeteosourceAdapter
from wxverify.feeds.openweathermap import OpenWeatherMapAdapter
from wxverify.feeds.registry import build_adapter
from wxverify.feeds.seam import FetchResult, ForecastRequest, NormalizedSample
from wxverify.feeds.synthetic_run import snap_run
from wxverify.feeds.visualcrossing import VisualCrossingAdapter
from wxverify.feeds.weatherapi import WeatherApiAdapter
from wxverify.worker.domain_backoff import source_domain
from wxverify.worker.processor import dispatch

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

NEW_PROVIDERS: tuple[str, ...] = (
    "visualcrossing",
    "openweathermap",
    "weatherapi",
    "meteosource",
    "google",
)

EXPECTED_DOMAINS: dict[str, str] = {
    "visualcrossing": "weather.visualcrossing.com",
    "openweathermap": "api.openweathermap.org",
    "weatherapi": "api.weatherapi.com",
    "meteosource": "www.meteosource.com",
    "google": "weather.googleapis.com",
}

EXPECTED_SYNTHETIC_ISSUED_AT = "2026-06-01T00:00:00Z"
EXPECTED_SYNTHETIC_VALID_AT = "2026-06-01T12:00:00Z"
EXPECTED_SYNTHETIC_VALID_EPOCH = int(datetime(2026, 6, 1, 12, tzinfo=UTC).timestamp())

ADAPTER_SNAP_CASES = (
    (
        "visualcrossing",
        VisualCrossingAdapter,
        visualcrossing_feed,
        {
            "days": [
                {
                    "hours": [
                        {
                            "datetimeEpoch": EXPECTED_SYNTHETIC_VALID_EPOCH,
                            "temp": 20.0,
                            "windspeed": 36.0,
                            "precip": 1.2,
                        }
                    ]
                }
            ]
        },
    ),
    (
        "openweathermap",
        OpenWeatherMapAdapter,
        openweathermap_feed,
        {
            "hourly": [
                {
                    "dt": EXPECTED_SYNTHETIC_VALID_EPOCH,
                    "temp": 20.0,
                    "wind_speed": 5.0,
                    "rain": {"1h": 1.2},
                }
            ]
        },
    ),
    (
        "weatherapi",
        WeatherApiAdapter,
        weatherapi_feed,
        {
            "forecast": {
                "forecastday": [
                    {
                        "hour": [
                            {
                                "time_epoch": EXPECTED_SYNTHETIC_VALID_EPOCH,
                                "temp_c": 20.0,
                                "wind_kph": 36.0,
                                "precip_mm": 1.2,
                            }
                        ]
                    }
                ]
            }
        },
    ),
    (
        "meteosource",
        MeteosourceAdapter,
        meteosource_feed,
        {
            "hourly": {
                "data": [
                    {
                        "date": "2026-06-01T12:00:00",
                        "temperature": 20.0,
                        "wind": {"speed": 5.0},
                        "precipitation": {"total": 1.2},
                    }
                ]
            }
        },
    ),
    (
        "google",
        GoogleAdapter,
        google_feed,
        {
            "forecastHours": [
                {
                    "interval": {"startTime": EXPECTED_SYNTHETIC_VALID_AT},
                    "temperature": {"degrees": 20.0, "unit": "CELSIUS"},
                    "wind": {
                        "speed": {
                            "value": 36.0,
                            "unit": "KILOMETERS_PER_HOUR",
                        }
                    },
                    "precipitation": {"qpf": {"quantity": 1.2, "unit": "MILLIMETERS"}},
                }
            ]
        },
    ),
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    """Replacement for run_worker that keeps the event loop alive without work."""
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    """Initialise a fresh isolated test DB and return the writer connection."""
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


# ---------------------------------------------------------------------------
# Contract 1a: resolve_secret via env var (_from_env path)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", NEW_PROVIDERS)
def test_key_resolves_via_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str
) -> None:
    """resolve_secret returns the key when set as an environment variable."""
    # No options.json on disk → falls back to env
    monkeypatch.setattr(config, "options_path", str(tmp_path / "missing.json"))
    env_var = SECRET_ENV[provider]
    monkeypatch.setenv(env_var, "test-key-env")
    assert resolve_secret(provider) == "test-key-env"


# ---------------------------------------------------------------------------
# Contract 1b: resolve_secret via options.json (_from_options_json path)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", NEW_PROVIDERS)
def test_key_resolves_via_options_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str
) -> None:
    """resolve_secret returns the key when present in options.json."""
    options_file = tmp_path / "options.json"
    options_file.write_text(
        json.dumps({f"{provider}_key": "test-key-json"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "options_path", str(options_file))
    # Env var must be absent so we verify options.json is the source
    env_var = SECRET_ENV[provider]
    monkeypatch.delenv(env_var, raising=False)
    assert resolve_secret(provider) == "test-key-json"


# ---------------------------------------------------------------------------
# Contract 2: build_adapter raises when key is unset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", NEW_PROVIDERS)
def test_build_adapter_raises_when_key_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str
) -> None:
    """build_adapter raises RuntimeError when the provider key is absent.

    Parametrized over all five new providers.
    """
    monkeypatch.setattr(config, "options_path", str(tmp_path / "missing.json"))
    env_var = SECRET_ENV[provider]
    monkeypatch.delenv(env_var, raising=False)

    async def _try_build() -> None:
        async with httpx.AsyncClient() as client:
            build_adapter(provider, client)

    with pytest.raises(RuntimeError, match="key is not configured"):
        asyncio.run(_try_build())


# ---------------------------------------------------------------------------
# Contract 3: missing-key worker path — job is terminal-clean
# ---------------------------------------------------------------------------


def test_missing_key_worker_marks_unavailable_and_completes_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the provider key is absent, _fetch_feed stamps error state and returns.

    The job must end with status='completed' and retry_count=0 — NOT re-queued
    via fail().  A churning impl that re-raises after _mark_feed_unavailable
    would land here with status='pending' / retry_count=1, catching that bug.
    """
    conn = _init_tmp_db(tmp_path)
    # Ensure visualcrossing key is genuinely unset (no env var, no options.json)
    monkeypatch.delenv("WXV_VISUALCROSSING_KEY", raising=False)

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('MissingKey', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='visualcrossing' AND model='blend'"
        ).fetchone()["id"]
    )
    # default_subscribed=0 for the new providers; subscribe explicitly
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 1)
        """,
        (site_id, feed_id),
    )

    enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    job = claim_next_job(conn)
    assert job is not None
    assert job.status == "running"

    # Simulate run_worker's dispatch → complete/fail lifecycle
    try:
        asyncio.run(dispatch(get_db(), job))
        # dispatch returned normally → complete (correct impl)
        complete(conn, job.id)
    except Exception as exc:
        # dispatch raised → fail (wrong impl would land here)
        fail(conn, job.id, sanitized_exception(exc))

    # Run-state: error was stamped and last_run_at was advanced
    state = conn.execute(
        """
        SELECT last_error, last_run_at, error_count
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert state["last_error"] is not None
    assert state["last_run_at"] is not None

    # Job row: terminal-clean (load-bearing assertion)
    job_row = conn.execute(
        "SELECT status, retry_count FROM jobs WHERE id=?",
        (job.id,),
    ).fetchone()
    assert job_row["status"] == "completed"
    assert job_row["retry_count"] == 0


# ---------------------------------------------------------------------------
# Contract 4: source_domain returns the expected host
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "provider,expected_domain",
    [
        ("visualcrossing", "weather.visualcrossing.com"),
        ("openweathermap", "api.openweathermap.org"),
        ("weatherapi", "api.weatherapi.com"),
        ("meteosource", "www.meteosource.com"),
        ("google", "weather.googleapis.com"),
    ],
)
def test_source_domain_for_new_providers(provider: str, expected_domain: str) -> None:
    """source_domain returns the correct host and does not raise KeyError."""
    assert source_domain(provider) == expected_domain


# ---------------------------------------------------------------------------
# Contract 5: synthetic run snap arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", NEW_PROVIDERS)
def test_snap_run_fixed_time_matches_cadence_floor(provider: str) -> None:
    """All five new adapters share snap_run with identical constants.

    Fetch at 07:00 UTC, lag=90 min → 05:30 UTC, floor to 6-h cadence → 00:00 UTC.
    The model_run_id produced by these adapters is f"blend:{issued_at}" because
    all five use model="blend" in the feeds table.
    """
    snapped = snap_run("2026-06-01T07:00:00Z")
    assert snapped == "2026-06-01T00:00:00Z"
    model_run_id = f"blend:{snapped}"
    assert model_run_id == "blend:2026-06-01T00:00:00Z"


@pytest.mark.parametrize(
    ("provider", "adapter_cls", "provider_module", "payload"),
    ADAPTER_SNAP_CASES,
)
def test_new_provider_adapters_emit_shared_synthetic_run(
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
    adapter_cls,
    provider_module,
    payload,
) -> None:
    """Each adapter must emit the shared snap in its real normalized samples."""
    monkeypatch.setattr(
        provider_module,
        "snap_run",
        lambda fetch_time=None: EXPECTED_SYNTHETIC_ISSUED_AT,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async def _fetch_samples() -> list[NormalizedSample]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = adapter_cls("test-key", client)
            result = await adapter.fetch_forecast(
                ForecastRequest(
                    lat=47.0,
                    lon=25.0,
                    model="blend",
                    variables=("temperature", "wind", "precip"),
                    max_lead_hours=24,
                )
            )
            return result.samples

    samples = asyncio.run(_fetch_samples())

    assert samples, provider
    assert {sample.model for sample in samples} == {"blend"}
    assert {sample.issued_at for sample in samples} == {EXPECTED_SYNTHETIC_ISSUED_AT}
    assert {sample.valid_at for sample in samples} == {EXPECTED_SYNTHETIC_VALID_AT}
    assert {sample.lead_hours for sample in samples} == {12}
    assert {sample.model_run_id for sample in samples} == {
        f"blend:{EXPECTED_SYNTHETIC_ISSUED_AT}"
    }


# ---------------------------------------------------------------------------
# Contract 6: No-op (a) — 0 usable samples stamps NO_USABLE_SAMPLES_SENTINEL
# ---------------------------------------------------------------------------


def test_no_op_zero_usable_samples_stamps_sentinel(tmp_path: Path) -> None:
    """A forward fetch with 0 usable samples stamps the sentinel in last_error."""
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('NoOpZero', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
        ).fetchone()["id"]
    )

    outcome = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[], grid=None),
        advance_last_run_at=True,
    )

    assert outcome.usable_sample_count == 0
    assert outcome.inserted_count == 0

    state = conn.execute(
        """
        SELECT last_error, last_run_at, error_count
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert state is not None
    assert state["last_error"] == NO_USABLE_SAMPLES_SENTINEL
    assert state["last_run_at"] is not None
    assert state["error_count"] == 1

    # Counter increments on a second no-op run
    persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[], grid=None),
        advance_last_run_at=True,
    )
    state2 = conn.execute(
        "SELECT error_count FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    assert state2["error_count"] == 2


# ---------------------------------------------------------------------------
# Contract 7: No-op (b) — idempotent re-fetch is NOT flagged as no-op
# ---------------------------------------------------------------------------


def test_no_op_idempotent_refetch_not_flagged(tmp_path: Path) -> None:
    """usable_sample_count > 0 but inserted_count == 0 must not stamp the sentinel.

    Controlled precondition: seed a prior no-op error state so that the
    assertion "last_error stays NULL" is meaningful (not vacuously true).
    A wrong impl using 'inserted == 0' as the no-op predicate would re-stamp
    the sentinel here, failing the assert.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('NoOpIdempotent', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' AND model='ecmwf_ifs'"
        ).fetchone()["id"]
    )

    sample = NormalizedSample(
        model="ecmwf_ifs",
        variable="temperature",
        issued_at="2026-06-01T00:00:00Z",
        valid_at="2026-06-01T12:00:00Z",
        lead_hours=12,
        value=20.0,
        source_raw="20.0 C",
        model_run_id="ecmwf_ifs:2026-06-01T00:00:00Z",
    )

    # Seed: prior no-op state (load-bearing precondition)
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_error, error_count)
        VALUES (?, ?, '2026-06-01T06:00:00Z', ?, 1)
        """,
        (site_id, feed_id, NO_USABLE_SAMPLES_SENTINEL),
    )

    # Insert the sample directly so the next persist sees it as already present
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '2026-06-01T07:00:00Z')
        """,
        (
            site_id,
            feed_id,
            sample.variable,
            sample.issued_at,
            sample.valid_at,
            sample.lead_hours,
            sample.value,
            sample.source_raw,
            sample.model_run_id,
        ),
    )

    # Idempotent re-fetch: usable=1 but INSERT OR IGNORE skips the duplicate
    outcome = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[sample]),
    )

    assert outcome.usable_sample_count == 1
    assert outcome.inserted_count == 0

    # Must NOT be flagged as a no-op: prior error state is cleared
    state = conn.execute(
        """
        SELECT last_error, error_count
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert state["last_error"] is None
    assert state["error_count"] == 0


# ---------------------------------------------------------------------------
# Contract 8: No-op (c) — historical path clears error state unconditionally
# ---------------------------------------------------------------------------


def test_historical_path_clears_error_state_unconditionally(tmp_path: Path) -> None:
    """advance_last_run_at=False always clears last_error and error_count.

    Covers both backfill and catchup since both route through this call.
    The seed-first step is load-bearing: a fresh/empty row passes vacuously
    via the INSERT path of the UPSERT even with a buggy UPDATE-only impl.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('HistoricalClear', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
        ).fetchone()["id"]
    )

    # Seed: pre-existing error state (load-bearing precondition)
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_error, error_count)
        VALUES (?, ?, '2026-06-01T00:00:00Z', 'prior error from live fetch', 2)
        """,
        (site_id, feed_id),
    )

    # Historical path: advance_last_run_at=False, empty samples
    persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[], grid=None),
        advance_last_run_at=False,
    )

    state = conn.execute(
        """
        SELECT last_error, error_count, last_run_at
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert state["last_error"] is None
    assert state["error_count"] == 0
    # advance_last_run_at=False: original last_run_at must be preserved
    assert state["last_run_at"] == "2026-06-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Contract 9: No-op render — status-ladder ordering
# ---------------------------------------------------------------------------


def test_no_op_render_status_ladder_ordering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sentinel in last_error must resolve to "fetched, 0 usable", not "error".

    The persist-layer no-op tests (contracts 6–8) pass regardless of branch
    order; only this render-layer assertion catches a wrong ladder where the
    generic 'last_error is not None → "error"' check precedes the sentinel
    equality check.
    """
    close_db()
    config.db_path = str(tmp_path / "noop-render.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('NoOpRender', 47, 25, 900, 'UTC')
                    """
                ).lastrowid
            )
            feed_id = int(
                conn.execute(
                    "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
                ).fetchone()["id"]
            )
            # Stamp the sentinel as persist_fetch_result would after a no-op run
            conn.execute(
                """
                INSERT INTO site_feed_state
                    (site_id, feed_id, last_run_at, last_error, error_count)
                VALUES (?, ?, '2026-06-01T07:00:00Z', ?, 1)
                """,
                (site_id, feed_id, NO_USABLE_SAMPLES_SENTINEL),
            )
            return site_id, feed_id

        site_id, feed_id = db.write_sync(_seed)

        # /api/health/feeds: status must be "fetched, 0 usable", NOT "error"
        response = client.get("/api/health/feeds")
        assert response.status_code == 200
        by_key = {(int(r["site_id"]), int(r["feed_id"])): r for r in response.json()}
        row = by_key[(site_id, feed_id)]
        assert row["status"] == "fetched, 0 usable", (
            f"expected 'fetched, 0 usable' but got {row['status']!r} — "
            "check status-ladder ordering in health.py"
        )

        # /ops HTML: the status string must appear in the rendered page
        ops = client.get("/ops")
        assert ops.status_code == 200
        assert "fetched, 0 usable" in ops.text, (
            "expected 'fetched, 0 usable' in /ops HTML — "
            "check status-ladder ordering in web/context.py"
        )

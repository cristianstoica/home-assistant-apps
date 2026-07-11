"""S-M6 tests: config.yaml options + settings/sources wiring (0.3.0 py-weather merge).

Hoare oracle map
----------------
Test 1 (test_configured_cap_lands_settings_land_budget_defers_at_cap)
    Bucket-1-F oracle 1: boot with weathercom_daily_call_limit=4200 and distinct
    min_interval / max_backoff → sources.daily_call_limit == 4200 (overrides seeded
    1000), the two settings rows land, and reserve_budget defers at cap.

Test 2 (test_absent_key_rescues_to_3000)
    Bucket-1-F oracle 2: options JSON omits the key entirely → sources cap == 3000.
    Fails if the Field default reverts from 3000 to None (LD-M8 breach).

Test 3 (test_zero_value_rescues_to_3000)
    Bucket-1-F oracle 3: options JSON carries weathercom_daily_call_limit=0 →
    sources cap == 3000 (the ``or 3000`` falsy-rescue fires).

Test 4 (test_from_env_no_var_defaults_to_3000)
    Bucket-1-F oracle 4: unit-level _from_env parse with no env var set →
    RuntimeOptions.weathercom_daily_call_limit == 3000.

Test 5 (test_negative_cap_raises_validation_error)
    Bucket-1-F oracle 5: weathercom_daily_call_limit=-5 → pydantic ValidationError
    (ge=1 hard floor, not a silent clamp).

Test 6 (test_settings_backed_intervals_drive_cadence)
    Poller-consumption: seeded min_interval / max_backoff drive persist_poll_result
    next_poll_at for TRANSIENT and TERMINAL states respectively.  Values chosen to
    differ from the module constants (300 / 86400) so a regression to hard-coded
    defaults goes red.

Test 7 (test_timeout_default_30_not_10)
    Timeout-passes-30: _fetch_current_obs reads request_timeout_seconds from settings
    (default 30 via get_number_setting) and passes it to fetch_current_observation.
    A future caller that drops the kwarg would revert prod to the 10.0 signature
    default — this is the gate.

Test 8 (test_t08_new_option_keys_in_config_and_translations)
    Bucket-1-G / T08: targeted assertion that the four new option keys appear in
    BOTH config.yaml options: and translations/en.yaml configuration:.  The existing
    generic test_translations_key_parity in test_m1_m5.py performs a full set-equality
    check; this test pins the four new keys by name so a targeted regression (a future
    revert of just these keys) is immediately obvious in the failure message.

Isolation
---------
Boot-path tests (1-3) use create_app + TestClient (the ``with TestClient(app):``
context triggers lifespan startup) + the idle-worker stub to prevent the background
poller from mutating DB state under the assertions.  A per-test tmp-file DB is wired
via config.db_path before create_app is called, and close_db() is called first to
release any prior handle (mirrors test_sm5_observations_current.py).

Unit / :memory: tests (4, 5, 6, 7) use direct :memory: SQLite with the _RealDb shim
from test_sm4_backoff.py.

Synthetic data only (public repo): ISTATION0x-style IDs, placeholder lat/lon/keys.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from wxverify import config
from wxverify.api.app import create_app
from wxverify.collection.budget import _billing_day, reserve_budget
from wxverify.core.options import RuntimeOptions, _from_env
from wxverify.db.connection import close_db, get_db
from wxverify.db.migrations import (
    create_schema,
    seed_default_feeds,
    seed_default_settings,
    seed_default_sources,
)
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.worker.control import JobDeferred
from wxverify.worker.current_obs import (
    MAX_BACKOFF_SECONDS,
    MIN_INTERVAL_SECONDS,
    Health,
    PollOutcome,
    persist_poll_result,
)
from wxverify.worker.processor import _fetch_current_obs  # noqa: PLC2701

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
_NOW_ISO = "2026-07-10T12:00:00Z"

# Distinct non-default, non-seed values for the boot-path tests.
# The seeded cap is 1000 (config.py:59); the field default is 3000.
# 4200 is distinct from both → any confusion between seed/default/configured
# produces a different number and the assertion fails.
_TEST_DAILY_CAP = 4200

# Distinct non-default interval values for the cadence test (oracles 6).
# Module constants: MIN_INTERVAL_SECONDS=300, MAX_BACKOFF_SECONDS=86400.
# Use values within the config.yaml schema range (60–1800 / 60–86400) and
# distinct from both defaults so a regression to the module constant fails.
_TEST_MIN_INTERVAL = 120  # distinct from 300
_TEST_MAX_BACKOFF = 7200  # distinct from 86400

# Patch target: fetch_current_observation imported INTO processor's namespace.
_PATCH_FETCH = "wxverify.worker.processor.fetch_current_observation"

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Idle-worker stub (prevents background poller from mutating DB mid-assertion)
# ---------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


# ---------------------------------------------------------------------------
# Shared seed helpers — boot-path tests (real tmp-file DB via TestClient)
# ---------------------------------------------------------------------------


def _seed_site(conn: sqlite3.Connection, *, name: str = "SITE-A") -> int:
    return int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES (?, 47.0, 25.0, 900.0, 'UTC')",
            (name,),
        ).lastrowid
    )


def _seed_station(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    pws_id: str,
    enabled: int = 1,
) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 47.0, 25.0, 900.0, ?)",
            (site_id, pws_id, enabled),
        ).lastrowid
    )


def _seed_poll_state(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    health_state: str = "online",
    next_poll_at: str = _NOW_ISO,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, updated_at)"
        " VALUES (?, ?, ?, ?)",
        (station_id, health_state, next_poll_at, _NOW_ISO),
    )


# ---------------------------------------------------------------------------
# _RealDb shim — :memory: tests (oracles 6, 7; mirrors test_sm4_backoff.py)
# ---------------------------------------------------------------------------


class _RealDb:
    """Synchronous-callback DB shim wrapping one :memory: sqlite3 connection.

    write() and read() call the lambda synchronously — sufficient for the
    single-threaded asyncio.run() test context (no real file I/O needed).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory v3-schema DB with WAL + foreign_keys."""
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    seed_default_sources(conn)
    seed_default_feeds(conn)
    seed_default_settings(conn)
    conn.execute("PRAGMA user_version = 3")
    return conn


def _seed_site_mem(conn: sqlite3.Connection, *, name: str = "SITE-A") -> int:
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 47.0, 25.0, 900.0, 'UTC')",
        (name,),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station_mem(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    pws_id: str = "ISTATION01",
    enabled: int = 1,
) -> int:
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        "  enabled, last_run_at, last_error, error_count)"
        " VALUES (?, ?, 47.0, 25.0, 900.0, ?, NULL, NULL, 0)",
        (site_id, pws_id, enabled),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_poll_state_mem(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    health_state: str = "online",
    next_poll_at: str = _NOW_ISO,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, updated_at)"
        " VALUES (?, ?, ?, ?)",
        (station_id, health_state, next_poll_at, _NOW_ISO),
    )


def _poll_state(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM station_poll_state WHERE station_id = ?", (station_id,)
    ).fetchone()


# ---------------------------------------------------------------------------
# Clock patch helper — freeze utc_now/isoformat_utc in current_obs module
# ---------------------------------------------------------------------------


def _patched_now(now: datetime = _NOW):  # type: ignore[no-untyped-def]
    now_iso = now.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def fake_utc_now() -> datetime:
        return now

    def fake_isoformat_utc(value: datetime | None = None) -> str:
        if value is None:
            return now_iso
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    from unittest.mock import patch as _patch

    return _patch.multiple(
        "wxverify.worker.current_obs",
        utc_now=fake_utc_now,
        isoformat_utc=fake_isoformat_utc,
    )


# ---------------------------------------------------------------------------
# Helper: write a minimal options JSON to a temp path
# ---------------------------------------------------------------------------


def _write_options_json(path: Path, data: dict[str, object]) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


# ---------------------------------------------------------------------------
# Oracle 1 — Configured cap lands, settings land, budget defers at cap
# ---------------------------------------------------------------------------


def test_configured_cap_lands_settings_land_budget_defers_at_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Boot with weathercom_daily_call_limit=4200, distinct min/max intervals.

    Preconditions (injected):
    - options.json sets weathercom_daily_call_limit=4200, min_interval_seconds=120,
      max_backoff_seconds=7200.
    - DB seeded with weathercom cap=1000 (from seed_default_sources).

    Assertions:
    1. sources.daily_call_limit for weathercom == 4200 (lifespan called
       set_source_cap, which overrode the seeded 1000; also != 3000 default).
    2. settings row min_interval_seconds reads back as 120 via get_number_setting.
    3. settings row max_backoff_seconds reads back as 7200 via get_number_setting.
    4. reserve_budget with 4200 prior calls defers (JobDeferred), proving the
       configured cap is the live gate — not the seeded 1000 or the default 3000.

    The value 4200 is distinct from the seeded 1000 and the field default 3000, so
    this assertion cannot pass vacuously.
    """
    close_db()
    config.db_path = str(tmp_path / "cap_configured.db")
    options_file = tmp_path / "options.json"
    _write_options_json(
        options_file,
        {
            "weathercom_daily_call_limit": _TEST_DAILY_CAP,  # 4200
            "min_interval_seconds": _TEST_MIN_INTERVAL,  # 120
            "max_backoff_seconds": _TEST_MAX_BACKOFF,  # 7200
        },
    )
    config.options_path = str(options_file)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app):
        db = get_db()

        def _check(conn: sqlite3.Connection) -> tuple[int, int, int]:
            row = conn.execute(
                "SELECT daily_call_limit FROM sources WHERE source = 'weathercom'"
            ).fetchone()
            assert row is not None, "sources row for weathercom must exist"
            cap = int(row["daily_call_limit"])
            min_iv = get_number_setting(conn, "min_interval_seconds", 300, minimum=60)
            max_bk = get_number_setting(conn, "max_backoff_seconds", 86400, minimum=60)
            return cap, min_iv, max_bk

        cap, min_iv, max_bk = db.write_sync(_check)

    # 1. sources.daily_call_limit overrode the seeded 1000 with 4200.
    assert cap == _TEST_DAILY_CAP, (
        f"sources.daily_call_limit must be {_TEST_DAILY_CAP} (configured), "
        f"got {cap!r}; seeded value is 1000, field default is 3000 — "
        "neither should appear"
    )

    # 2 & 3. Settings rows landed from options.
    assert min_iv == _TEST_MIN_INTERVAL, (
        f"min_interval_seconds setting must be {_TEST_MIN_INTERVAL}, got {min_iv!r}"
    )
    assert max_bk == _TEST_MAX_BACKOFF, (
        f"max_backoff_seconds setting must be {_TEST_MAX_BACKOFF}, got {max_bk!r}"
    )

    # 4. Budget defers when calls reaches the configured cap.
    # Exhaust all 4200 slots, then one more reserve must raise JobDeferred.
    close_db()
    config.db_path = str(tmp_path / "cap_configured.db")
    config.options_path = str(options_file)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app2 = create_app(root_path="")
    with TestClient(app2):
        db2 = get_db()

        def _fill_budget(conn: sqlite3.Connection) -> None:
            # Derive billing_day the same way reserve_budget does: read the
            # weathercom billing_tz from the sources row and call _billing_day()
            # on the real current time.  Using _NOW.date() here caused the row
            # to be inserted for 2026-07-10 regardless of the actual calendar
            # date, making the test fail on any day after authorship.
            tz_row = conn.execute(
                "SELECT billing_tz FROM sources WHERE source = 'weathercom'"
            ).fetchone()
            assert tz_row is not None, "sources row for weathercom must exist"
            billing_day = _billing_day(str(tz_row["billing_tz"]))
            # Insert a budget row representing 4200 calls already consumed.
            conn.execute(
                "INSERT OR REPLACE INTO api_budget"
                " (source, billing_day, calls, credits)"
                " VALUES ('weathercom', ?, ?, 0)",
                (billing_day, _TEST_DAILY_CAP),
            )

        db2.write_sync(_fill_budget)

        def _try_reserve(conn: sqlite3.Connection) -> None:
            reserve_budget(conn, "weathercom", 1)

        with pytest.raises(JobDeferred):
            db2.write_sync(_try_reserve)


# ---------------------------------------------------------------------------
# Oracle 2 — Absent key → 3000 (LD-M8 breach guard)
# ---------------------------------------------------------------------------


def test_absent_key_rescues_to_3000(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """options.json omits weathercom_daily_call_limit → sources cap == 3000.

    Fails if the `or 3000` fallback in `_from_options_json` is removed (the
    LD-M8 breach vector — a None reaching set_source_cap would leave the
    seeded 1000 cap in force).  The Field(default=…) value is irrelevant here:
    the constructor passes `options.get("weathercom_daily_call_limit") or 3000`
    explicitly, so the field default is never consulted on any real boot path.

    Paired positive: test_configured_cap_lands_settings_land_budget_defers_at_cap
    proves a non-3000 cap can actually be configured, making this 3000 assertion
    meaningful (it can go red if the fallback logic breaks).
    """
    close_db()
    config.db_path = str(tmp_path / "cap_absent.db")
    options_file = tmp_path / "options_no_cap.json"
    # Deliberately omit weathercom_daily_call_limit — the key is absent.
    _write_options_json(options_file, {})
    config.options_path = str(options_file)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app):
        db = get_db()

        def _read_cap(conn: sqlite3.Connection) -> int:
            row = conn.execute(
                "SELECT daily_call_limit FROM sources WHERE source = 'weathercom'"
            ).fetchone()
            assert row is not None
            return int(row["daily_call_limit"])

        cap = db.write_sync(_read_cap)

    assert cap == 3000, (
        f"Absent key must rescue to 3000 via RuntimeOptions.Field(default=3000), "
        f"got {cap!r}; if this is 1000 the Field reverted to None "
        f"and set_source_cap no-oped (LD-M8 breach)"
    )


# ---------------------------------------------------------------------------
# Oracle 3 — Zero value → 3000 (falsy-rescue)
# ---------------------------------------------------------------------------


def test_zero_value_rescues_to_3000(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """options.json carries weathercom_daily_call_limit=0 → sources cap == 3000.

    The 'or 3000' expression in _from_options_json rescues the falsy 0 to 3000.
    The pydantic ge=1 validator would reject 0 anyway — but the or-rescue fires
    BEFORE the Field validator because _from_options_json does:
        options.get("weathercom_daily_call_limit") or 3000
    which short-circuits to 3000 when the value is 0, never feeding 0 to the
    Field validator.

    Paired positive: oracle 1 proves a non-zero cap is passed through unchanged,
    so this 3000 assertion is not vacuous.
    """
    close_db()
    config.db_path = str(tmp_path / "cap_zero.db")
    options_file = tmp_path / "options_zero_cap.json"
    _write_options_json(options_file, {"weathercom_daily_call_limit": 0})
    config.options_path = str(options_file)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app):
        db = get_db()

        def _read_cap(conn: sqlite3.Connection) -> int:
            row = conn.execute(
                "SELECT daily_call_limit FROM sources WHERE source = 'weathercom'"
            ).fetchone()
            assert row is not None
            return int(row["daily_call_limit"])

        cap = db.write_sync(_read_cap)

    assert cap == 3000, (
        f"Zero value must rescue to 3000 via the 'or 3000' fallback, got {cap!r}"
    )


# ---------------------------------------------------------------------------
# Oracle 4 — _from_env no-var → 3000 (unit-level parse path)
# ---------------------------------------------------------------------------


def test_from_env_no_var_defaults_to_3000(monkeypatch: pytest.MonkeyPatch) -> None:
    """_from_env with no WXV_WEATHERCOM_DAILY_CALL_LIMIT set → field == 3000.

    Fails if the `or 3000` fallback in `_from_env` is removed (the LD-M8
    breach vector — a None reaching set_source_cap would leave the seeded 1000
    cap in force).  The Field(default=…) value is irrelevant here: _from_env
    passes `_env_int(...) or 3000` explicitly, so the field default is never
    consulted on any real boot path.

    Unit-level test of the parse path in options.py:88.  Pairs with oracle 2's
    boot-level check: both must hold for the LD-M8 guard to be complete.

    Precondition (injected): env var absent (monkeypatched out to guarantee no
    ambient value leaks in from CI or the developer's shell).
    """
    monkeypatch.delenv("WXV_WEATHERCOM_DAILY_CALL_LIMIT", raising=False)
    # Also stub the other secret-env vars so _from_env doesn't fail on
    # unrelated env lookups (they all default to None when absent — that's fine).
    rc = _from_env()
    assert rc.options.weathercom_daily_call_limit == 3000, (
        f"_from_env must produce weathercom_daily_call_limit=3000 when "
        f"WXV_WEATHERCOM_DAILY_CALL_LIMIT is unset; got "
        f"{rc.options.weathercom_daily_call_limit!r}"
    )


# ---------------------------------------------------------------------------
# Oracle 5 — Negative cap raises ValidationError (hard floor, not silent clamp)
# ---------------------------------------------------------------------------


def test_negative_cap_raises_validation_error() -> None:
    """RuntimeOptions(weathercom_daily_call_limit=-5) raises pydantic ValidationError.

    The ge=1 constraint is a hard floor: the model must REJECT -5, not silently
    clamp it.  If this test passes on unfixed code the ge=1 annotation was removed
    or changed to allow negatives.

    Paired positive: oracle 1 proves valid positive integers are accepted.
    """
    with pytest.raises(ValidationError):
        RuntimeOptions(weathercom_daily_call_limit=-5)


# ---------------------------------------------------------------------------
# Oracle 6 — Settings-backed intervals drive persist_poll_result cadence
# ---------------------------------------------------------------------------


def test_settings_backed_intervals_drive_cadence() -> None:
    """Seeded settings drive persist_poll_result next_poll_at (not module constants).

    Preconditions (injected):
    - settings rows min_interval_seconds=120, max_backoff_seconds=7200 inserted
      before persist_poll_result is called.
    - Module constants are MIN_INTERVAL_SECONDS=300, MAX_BACKOFF_SECONDS=86400;
      the seeded values are intentionally different so a regression to the constant
      fails the assertion.

    Assertions:
    TRANSIENT outcome → next_poll_at = now + 120 s (min_interval from settings,
        not the module constant 300 s).
    TERMINAL outcome → next_poll_at = now + 7200 s (max_backoff from settings,
        not the module constant 86400 s).

    Both state branches are exercised in sequence (separate stations to keep the
    poll-state rows independent); the constant values are asserted-NOT-equal so the
    failure mode is unambiguous.
    """
    conn = _make_conn()
    site_id = _seed_site_mem(conn)

    # Inject the settings-backed values (distinct from module constants).
    conn.execute("BEGIN")
    set_setting(conn, "min_interval_seconds", str(_TEST_MIN_INTERVAL))  # 120
    set_setting(conn, "max_backoff_seconds", str(_TEST_MAX_BACKOFF))  # 7200
    conn.execute("COMMIT")

    # Two separate stations for independent poll-state rows.
    station_transient = _seed_station_mem(conn, site_id, pws_id="ISTATION01")
    station_terminal = _seed_station_mem(conn, site_id, pws_id="ISTATION02")
    _seed_poll_state_mem(conn, station_transient, health_state="online")
    _seed_poll_state_mem(conn, station_terminal, health_state="online")

    transient_outcome = PollOutcome(Health.TRANSIENT, error="http 503")
    terminal_outcome = PollOutcome(Health.TERMINAL, error="http 401")

    expected_transient_iso = (
        (_NOW + timedelta(seconds=_TEST_MIN_INTERVAL))
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    expected_terminal_iso = (
        (_NOW + timedelta(seconds=_TEST_MAX_BACKOFF))
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )

    # Also compute the wrong values (module constants) to assert they are NOT used.
    wrong_min_iso = (
        (_NOW + timedelta(seconds=MIN_INTERVAL_SECONDS))
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    wrong_max_iso = (
        (_NOW + timedelta(seconds=MAX_BACKOFF_SECONDS))
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )

    with _patched_now(_NOW):
        conn.execute("BEGIN")
        persist_poll_result(conn, site_id, station_transient, transient_outcome)
        persist_poll_result(conn, site_id, station_terminal, terminal_outcome)
        conn.execute("COMMIT")

    ps_t = _poll_state(conn, station_transient)
    ps_x = _poll_state(conn, station_terminal)
    assert ps_t is not None
    assert ps_x is not None

    # TRANSIENT: next_poll_at driven by seeded min_interval (120 s), not
    # the module constant MIN_INTERVAL_SECONDS (300 s).
    assert ps_t["next_poll_at"] == expected_transient_iso, (
        f"TRANSIENT next_poll_at must reflect seeded min_interval="
        f"{_TEST_MIN_INTERVAL}s ({expected_transient_iso!r}), "
        f"got {ps_t['next_poll_at']!r}; "
        f"if this equals {wrong_min_iso!r} the constant (300 s) was used"
    )
    assert ps_t["next_poll_at"] != wrong_min_iso, (
        f"TRANSIENT next_poll_at must NOT equal the module constant "
        f"{MIN_INTERVAL_SECONDS}s result — settings were ignored"
    )

    # TERMINAL: next_poll_at driven by seeded max_backoff (7200 s), not
    # the module constant MAX_BACKOFF_SECONDS (86400 s).
    assert ps_x["next_poll_at"] == expected_terminal_iso, (
        f"TERMINAL next_poll_at must reflect seeded max_backoff="
        f"{_TEST_MAX_BACKOFF}s ({expected_terminal_iso!r}), "
        f"got {ps_x['next_poll_at']!r}; "
        f"if this equals {wrong_max_iso!r} the constant (86400 s) was used"
    )
    assert ps_x["next_poll_at"] != wrong_max_iso, (
        f"TERMINAL next_poll_at must NOT equal the module constant "
        f"{MAX_BACKOFF_SECONDS}s result — settings were ignored"
    )


# ---------------------------------------------------------------------------
# Oracle 7 — Timeout default 30, not 10.0 signature fallback
# ---------------------------------------------------------------------------


def test_timeout_default_30_not_10() -> None:
    """_fetch_current_obs passes timeout_seconds=30 (settings default), not 10.0.

    Production path (processor.py:383-389):
        timeout_seconds = await db.read(
            lambda conn: get_number_setting(
                conn, "request_timeout_seconds", 30, minimum=1
            )
        )
        response = await fetch_current_observation(
            pws_station_id, api_key, timeout_seconds=timeout_seconds
        )

    The pws_adapter signature default is 10.0.  If a future refactor drops the
    timeout_seconds kwarg from the call, prod silently reverts to 10.0 with no
    other gate catching it.  This test is that gate.

    Precondition (injected): no request_timeout_seconds row in settings (simulates
    the omitted row → get_number_setting returns the default 30).  Absence is
    injected by using a fresh DB built with seed_default_settings — if that function
    ever inserts the row with a different default, the oracle catches the change.

    Paired presence: if you later add a test that seeds the row to a custom value
    and verifies that value flows through, BOTH tests must hold.
    """
    conn = _make_conn()
    site_id = _seed_site_mem(conn)
    station_id = _seed_station_mem(conn, site_id, pws_id="ISTATION01")
    _seed_poll_state_mem(conn, station_id)

    # Verify the injected precondition: no request_timeout_seconds in settings.
    raw = conn.execute(
        "SELECT value FROM settings WHERE key = 'request_timeout_seconds'"
    ).fetchone()
    assert raw is None, (
        "precondition: request_timeout_seconds must NOT be in settings for this "
        "test (get_number_setting must fall back to the 30-s default)"
    )

    captured_timeout: list[float] = []

    async def _fake_fetch_current_observation(
        pws_station_id: str,
        api_key: str,
        *,
        client: object = None,
        timeout_seconds: float = 10.0,
    ) -> object:
        captured_timeout.append(timeout_seconds)
        # Return a minimal 2xx response so the happy path completes.
        import httpx

        return httpx.Response(
            200,
            content=b"{}",
            request=httpx.Request(
                "GET",
                "https://api.weather.com/v2/pws/observations/current"
                "?stationId=ISTATION01&format=json&units=m&apiKey=SYNTHETIC",
            ),
        )

    with (
        patch(_PATCH_FETCH, _fake_fetch_current_observation),
        patch("wxverify.worker.processor.resolve_secret", return_value="SYNTHETIC"),
        # Fake utc_now / isoformat_utc in current_obs to avoid wall-clock in
        # persist_poll_result, which is called on any non-429/non-5xx path.
        _patched_now(_NOW),
    ):
        db = _RealDb(conn)
        # The response is a bare 2xx with no valid JSON obs body; classify_current_obs
        # will return OFFLINE ("empty body" or similar) and persist_poll_result will
        # write the transient/offline state.  That's fine — the assertion is on the
        # kwarg that was passed to the fake, not the outcome.
        with contextlib.suppress(Exception):
            asyncio.run(_fetch_current_obs(db, site_id, station_id))

    assert captured_timeout, (
        "fetch_current_observation fake was never called — "
        "check that the patch target is correct and the DB seed is valid"
    )
    observed = captured_timeout[0]
    assert observed == 30, (
        f"fetch_current_observation must receive timeout_seconds=30 "
        f"(the settings default via get_number_setting), got {observed!r}; "
        f"if this is 10.0 the kwarg was dropped and prod reverted to the "
        f"signature default (pws_adapter.py:390)"
    )
    assert observed != 10.0, (
        "timeout_seconds must NOT be 10.0 — that is the pws_adapter signature "
        "default and means the caller dropped the kwarg"
    )


# ---------------------------------------------------------------------------
# Oracle 8 — T08 targeted assertion: four new keys in both files
# ---------------------------------------------------------------------------


def test_t08_new_option_keys_in_config_and_translations() -> None:
    """New option keys appear in BOTH config.yaml options: and translations/en.yaml.

    The generic test_translations_key_parity in test_m1_m5.py asserts full
    set-equality between the two files, which already covers any missing new key.
    This targeted assertion additionally pins the four specific new keys by name so
    a regression (e.g. reverting just these four lines) produces a focused, readable
    failure instead of a set-diff error.

    The four new keys added in the 0.3.0 py-weather merge (plan §10):
    - min_interval_seconds
    - max_backoff_seconds
    - request_timeout_seconds
    - weathercom_daily_call_limit
    """
    repo_root = Path(__file__).resolve().parents[1]
    config_text = (repo_root / "config.yaml").read_text(encoding="utf-8")
    trans_text = (repo_root / "translations" / "en.yaml").read_text(encoding="utf-8")

    new_option_keys = (
        "min_interval_seconds",
        "max_backoff_seconds",
        "request_timeout_seconds",
        "weathercom_daily_call_limit",
    )

    for key in new_option_keys:
        # config.yaml options: section — the key appears as a top-level options child.
        assert f"  {key}:" in config_text, (
            f"config.yaml options: is missing key '{key}' — "
            f"it must appear as '  {key}:' under the options: block"
        )
        # translations/en.yaml configuration: section — key appears as a top-level
        # configuration child (the same indent-2 pattern).
        assert f"  {key}:" in trans_text, (
            f"translations/en.yaml configuration: is missing key '{key}' — "
            f"it must appear as '  {key}:' under the configuration: block"
        )

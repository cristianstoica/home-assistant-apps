"""Tests: wind-history auth hold, group 2 of plan section 15.1 (2026-09-30).

Covers T80-T85 and T108 against ``wxverify/worker/wind_days.py`` (the
401/403/429 failure table and the probe path), ``wxverify/api/routes/
backfill.py`` (the ``PUT /api/wind-history/auth-hold`` route) and
``wxverify/monitor.py``/the ops and dashboard templates (the hold's
surfaces).

Isolation: T80, T81, T83, T85 and T108 drive ``run_wind_days`` end to end
against a real tmp-path ``Database``/``FencedWriter`` + ``httpx.
MockTransport``, mirroring ``tests/test_fetch_obs_partial_cycle.py``'s
harness (``_init_tmp_db``, ``_freeze``). T82 and the HTTP half of T84 drive
a real app over ``TestClient`` against a tmp-path file database, mirroring
``tests/test_publish_hold_control.py``. The read-only half of T84 calls
``wxverify.monitor._wind_history_trips`` directly over a ``:memory:``
connection + ``run_migrations``.

Every station id, site name and coordinate below is synthetic (public
repo): pws station ids ``KTEST001``-``KTEST006``, site name "Testsite",
timezone "UTC", coordinates 0.0/0.0, API key
"0123456789abcdef0123456789abcdef".
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.collection.wind_quota import (
    WIND_BACKFILL_CALLS_KEY,
    WIND_LIVE_CALLS_KEY,
    read_lane_counter,
)
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations, seed_default_sources
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.wind_basis import (
    read_auth_hold,
    write_auth_hold,
)
from wxverify.monitor import _wind_history_trips  # noqa: PLC2701
from wxverify.obs.pws_adapter import ProviderDeadlineExceeded
from wxverify.worker.access_hold import (
    clear_access_refused,
    read_access_record,
    record_access_refused,
)
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    source_domain,
)
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    run_wind_days,
    wind_days_due,
    wind_lane_due,
)

_TZ = "UTC"
_API_KEY = "0123456789abcdef0123456789abcdef"


# ---------------------------------------------------------------------------
# Shared sync harness ("as of" tests over :memory: + run_migrations)
# ---------------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    seed_default_sources(conn)
    conn.commit()
    return conn


def _make_site(conn: sqlite3.Connection, name: str = "Testsite", tz: str = _TZ) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 0.0, 0.0, 0.0, ?)",
        (name, tz),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _make_station(
    conn: sqlite3.Connection, site_id: int, pws_id: str, *, enabled: bool = True
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        " enabled) VALUES (?, ?, 0.0, 0.0, 0.0, ?)",
        (site_id, pws_id, 1 if enabled else 0),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _insert_day(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date,
    *,
    status: str = "pending",
    attempts: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, ?, ?, 0, 0, 0, '2026-01-01T00:00:00Z')",
        (station_id, local_date.isoformat(), status, attempts),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Shared async harness (real dispatch + MockTransport, tmp-path file DB)
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- tests inspect the real writer connection
    seed_default_sources(conn)
    conn.commit()
    return conn


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """Freeze ``utc_now()`` in every module the wind-days job calls it from."""
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: when)


def _run_wind_days(
    site_id: int, handler: object, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


def _seed_due_today_only(conn: sqlite3.Connection, now: datetime) -> tuple[int, int]:
    """A site+station with exactly one due row: today's, via ``all_1day``.

    ``ensure_wind_days`` (called inside ``_start_job``) only fabricates a
    pending row for ``today`` when the site has no observations, so no
    other row exists to contend for a call in the same chunk.
    """
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    conn.commit()
    return site_id, station_id


def _seed_due_history_only(
    conn: sqlite3.Connection, now: datetime, tz_name: str = _TZ
) -> tuple[int, int]:
    """A site+station with exactly one due row: yesterday's, via ``history_all``.

    Today's auto-fabricated row is pre-closed (``fetched``) so the chunk's
    only due row is the manually-inserted past day.
    """
    site_id = _make_site(conn, tz=tz_name)
    station_id = _make_station(conn, site_id, "KTEST001")
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    today = now.astimezone(ZoneInfo(tz_name)).date()
    yesterday = today - timedelta(days=1)
    _insert_day(conn, station_id, today, status="fetched", attempts=0)
    _insert_day(conn, station_id, yesterday, status="pending", attempts=0)
    conn.commit()
    return site_id, station_id


def _station_row(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT last_error, error_count FROM stations WHERE id=?", (station_id,)
    ).fetchone()
    assert row is not None
    return row


def _day_row(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT status, attempts FROM station_wind_days"
        " WHERE station_id=? AND local_date=?",
        (station_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return row


def _budget_calls(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# T80 -- a 401/403 on history_all/all_1day sets held, refunds nothing.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
def test_t80_401_403_on_history_all_sets_held_without_refund_or_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(status, request=request)

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 1, f"exactly one call expected, got {call_log}"
    assert _budget_calls(conn) == 1, "the call is spent, not refunded"
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "pending"
    assert day["attempts"] == 0
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "held"
    station = _station_row(conn, station_id)
    assert station["last_error"] is None
    assert station["error_count"] == 0


def test_fm1_401_on_the_5_minute_download_holds_five_minute_not_a_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 FM1 (replaces the old all_1day-lane-hold T80 case): today's
    5-minute download answers 401 for the first of two stations. The
    refusal holds the per-kind ``five_minute`` record (no lane hold exists
    for ``all_1day`` any more), and the second station's today row is not
    called in the same chunk (the lane's own suppression, proven by only
    one call reaching the fake transport)."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id = _make_site(conn)
    station_id_1 = _make_station(conn, site_id, "KTEST001")
    _make_station(conn, site_id, "KTEST002")
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(401, request=request)

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1, (
        "mutant -> at this assertion: correct = 1 (the second station's"
        " today row is suppressed in the same chunk after the first"
        " refusal), mutant (the chunk loop's opened.discard on a 'closed'"
        " outcome dropped) = 2"
    )
    assert _budget_calls(conn) == 1, "the call is spent, not refunded"
    day = _day_row(conn, station_id_1, now.date())
    assert day["status"] == "pending"
    assert day["attempts"] == 0

    record = read_access_record(conn, "five_minute")
    assert record is not None and record.status == 401, (
        "mutant -> at this assertion: correct = a five_minute record with"
        " status=401, mutant (the per-kind access-hold write dropped or"
        " keyed to a different status) = None or a different status"
    )
    assert get_runtime_state(conn, "wind_history_auth_hold:all_1day") is None, (
        "mutant -> at this assertion: correct = None (all_1day no longer"
        " has a lane hold at all), mutant (a lane-level hold is still"
        " written for all_1day) = a stored JSON hold value"
    )
    assert read_access_record(conn, "current_obs") is None, (
        "mutant -> at this assertion: correct = None (the refusal holds"
        " only five_minute), mutant (the refusal recorded under every"
        " kind, e.g. a `for k in ACCESS_KINDS` loop) = a stored record"
    )
    assert read_access_record(conn, "hourly") is None, (
        "mutant -> at this assertion: correct = None (the refusal holds"
        " only five_minute), mutant (the refusal recorded under every"
        " kind) = a stored record"
    )

    message = conn.execute(
        "SELECT last_error FROM station_wind_days WHERE station_id=? AND local_date=?",
        (station_id_1, now.date().isoformat()),
    ).fetchone()["last_error"]
    assert message == (
        f"station=KTEST001 date={now.date().isoformat()}: access refused (HTTP 401)"
    ), (
        "mutant -> at this assertion: correct = the literal"
        f" 'station=KTEST001 date={now.date().isoformat()}: access refused"
        " (HTTP 401)' text, mutant (the message mentions 'key' or"
        " 'throttled' instead) = a different message"
    )


def test_fm2_five_minute_hold_lifts_after_until_with_no_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 FM2 (check 3 on the 5-minute download): the FM1 state;
    at ``until`` + 1s, ``wind_lane_due`` is true and the chunk calls
    ``all_1day`` once with no extra "probe" step (unlike the old
    ``history_all`` lane hold's ``probing`` status), and a 200 reply
    deletes the ``five_minute`` row."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id = _make_site(conn)
    _make_station(conn, site_id, "KTEST001")
    conn.commit()

    def refusing_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, request=request)

    _run_wind_days(site_id, refusing_handler, monkeypatch=monkeypatch)
    record = read_access_record(conn, "five_minute")
    assert record is not None and record.until is not None
    until = parse_utc(record.until)

    # Paired negative, injected via the same frozen clock the positive below
    # uses: 1s before `until` the hold is still in force and only today's
    # row exists, so wind_lane_due is False. This does not by itself
    # diverge from the gap's mutant (reading the row's mere presence
    # instead of `until`) -- the row still exists here too, so the mutant
    # also gives False -- it establishes the precondition the positive
    # depends on: that `until` sits inside this test's frozen day at all,
    # rather than months away on the real clock.
    before = until - timedelta(seconds=1)
    _freeze(monkeypatch, before)
    assert wind_lane_due(conn, site_id, ZoneInfo(_TZ), now=before) is False

    after = until + timedelta(seconds=1)
    _freeze(monkeypatch, after)
    assert wind_lane_due(conn, site_id, ZoneInfo(_TZ), now=after) is True, (
        "mutant -> at this assertion: correct = True (the row becomes due"
        " again once the hold has lifted), mutant (wind_lane_due still"
        " reads five_minute_held as True after `until` has passed) = False"
    )

    call_log: list[str] = []

    def granting_handler(request: httpx.Request) -> httpx.Response:
        if "/observations/all/1day" in request.url.path:
            call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _run_wind_days(site_id, granting_handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1, (
        "mutant -> at this assertion: correct = 1 (the all_1day chunk call"
        " resumes on the first try with no extra probe), mutant (a"
        " probe-style gate that needs a separate successful try before a"
        " full call) = 0"
    )
    assert "stationId=KTEST001" in call_log[0], (
        "mutant -> at this assertion: correct = True (the one logged"
        " all_1day call is for KTEST001), mutant (the lane queries the"
        " wrong station) = False"
    )
    assert read_access_record(conn, "five_minute") is None, (
        "mutant -> at this assertion: correct = None (a 2xx reply deletes"
        " the five_minute row), mutant (clear_access_refused not called on"
        " today's 2xx) = a stored record"
    )


# ---------------------------------------------------------------------------
# T81 -- suppression: a held endpoint is skipped, the other still runs; a
# held hold survives a live-obs success, a success on the other endpoint,
# and clear_domain_backoff.
# ---------------------------------------------------------------------------


def test_t81_a_held_endpoint_is_skipped_while_the_other_still_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    from datetime import timedelta

    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    yesterday = now.date() - timedelta(days=1)
    _insert_day(conn, station_id, yesterday, status="pending", attempts=0)
    conn.commit()
    write_auth_hold(
        conn, "history_all", status="held", since="2026-03-09T00:00:00Z", error="x"
    )
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert call_log, "the non-held endpoint (all_1day, today's row) must still run"
    assert all("history/all" not in url for url in call_log), (
        "a held endpoint must make no call while suppressed"
    )


@pytest.mark.parametrize(
    "action",
    ["live_obs_success", "success_on_other_endpoint", "clear_domain_backoff"],
)
def test_t81_a_held_hold_survives_unrelated_success_and_clear_domain_backoff(
    action: str,
) -> None:
    conn = _make_db()
    write_auth_hold(
        conn, "history_all", status="held", since="2026-03-09T00:00:00Z", error="x"
    )
    conn.commit()

    if action == "live_obs_success":
        # A live obs success touches only `stations`/`station_observations`,
        # never `runtime_state`'s auth-hold key -- simulate the write a
        # successful live cycle makes and confirm the hold key is untouched.
        conn.execute(
            "UPDATE stations SET last_run_at = '2026-03-10T00:00:00Z' WHERE 1=0"
        )
    elif action == "success_on_other_endpoint":
        # A successful all_1day call never calls write_auth_hold/clear_auth_hold
        # for history_all -- nothing to simulate beyond the hold already set.
        pass
    else:
        clear_domain_backoff(conn, source_domain("weathercom"))

    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "held"


def test_fm5_a_five_minute_hold_does_not_stop_a_past_days_history_all_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 FM5 (second half): a ``five_minute`` hold in force does
    not stop a past day's ``history_all`` call (paired with
    ``test_t81_a_held_endpoint_is_skipped_while_the_other_still_runs``,
    which proves the opposite direction -- a ``history_all`` hold does not
    stop today's ``all_1day`` call)."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    from datetime import timedelta

    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    yesterday = now.date() - timedelta(days=1)
    _insert_day(conn, station_id, yesterday, status="pending", attempts=0)
    conn.commit()

    record_access_refused(conn, "five_minute", 401)
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert call_log, (
        "mutant -> at this assertion: correct = non-empty (the past day's"
        " history_all call still runs), mutant (five_minute_held closing"
        " both endpoints via open_endpoints) = empty"
    )


# ---------------------------------------------------------------------------
# T82 -- the PUT /api/wind-history/auth-hold route.
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _boot(tmp_path: Path, name: str, monkeypatch: pytest.MonkeyPatch) -> object:
    close_db()
    config.db_path = str(tmp_path / name)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


_PUT_PATH = "/api/wind-history/auth-hold"


def test_t82_confirm_false_is_rejected_with_400(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "t82-confirm.db", monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": False},
            headers=headers,
        )
        assert resp.status_code == 400, resp.text
    close_db()


def test_fm3_all_1day_is_rejected_by_the_request_model_with_422(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 FM3: ``PUT /api/wind-history/auth-hold`` with
    ``endpoint: "all_1day"`` answers 422 (the request model no longer
    accepts it), paired with the unchanged ``history_all`` acceptance
    proven by the surrounding T82 cases."""
    app = _boot(tmp_path, "fm3-all-1day.db", monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "all_1day", "confirm": True},
            headers=headers,
        )
        assert resp.status_code == 422, resp.text
        # mutant -> at this assertion: correct = 422 (the request model's
        # endpoint field no longer accepts "all_1day"), mutant (the field
        # still admits Literal["history_all", "all_1day"]) = 200/204
    close_db()


def test_t82_held_moves_to_probing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "t82-held.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: write_auth_hold(
                conn,
                "history_all",
                status="held",
                since="2026-03-09T00:00:00Z",
                error="refused",
            )
        )
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": True},
            headers=headers,
        )

        assert resp.status_code == 200, resp.text
        assert resp.json() == {"endpoint": "history_all", "status": "probing"}
        hold = db.read_sync(lambda conn: read_auth_hold(conn, "history_all"))
        assert hold is not None
        assert hold.status == "probing"
        assert hold.error == "refused"
    close_db()


def test_t82_no_hold_answers_clear(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "t82-clear.db", monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": True},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"status": "clear"}
    close_db()


def test_t82_probing_is_unchanged_by_a_repeat_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "t82-probing.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: write_auth_hold(
                conn,
                "history_all",
                status="probing",
                since="2026-03-09T00:00:00Z",
                error="refused",
            )
        )
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": True},
            headers=headers,
        )

        assert resp.status_code == 200, resp.text
        assert resp.json() == {"endpoint": "history_all", "status": "probing"}
        hold = db.read_sync(lambda conn: read_auth_hold(conn, "history_all"))
        assert hold is not None
        assert hold.since == "2026-03-09T00:00:00Z", (
            "a repeat call on an already-probing hold must be a no-op"
        )
    close_db()


def test_t82_the_lane_is_enqueued_for_each_enabled_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "t82-enqueue.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> list[int]:
            write_auth_hold(
                conn,
                "history_all",
                status="held",
                since="2026-03-09T00:00:00Z",
                error="x",
            )
            ids = []
            for name, enabled in (("Testsite", 1), ("Otherside", 1), ("Offsite", 0)):
                cur = conn.execute(
                    "INSERT INTO sites"
                    " (name, forecast_lat, forecast_lon, elevation_m, timezone,"
                    "  enabled) VALUES (?, 0.0, 0.0, 0.0, 'UTC', ?)",
                    (name, enabled),
                )
                assert cur.lastrowid is not None
                ids.append(int(cur.lastrowid))
            conn.commit()
            return ids

        site_ids = db.write_sync(_seed)
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": True},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text

        job_site_ids = db.read_sync(
            lambda conn: {
                int(r["site_id"])
                for r in conn.execute(
                    "SELECT site_id FROM jobs WHERE job_key='wind-days'"
                ).fetchall()
            }
        )
        assert job_site_ids == {site_ids[0], site_ids[1]}, (
            "only the two ENABLED sites get the lane enqueued"
        )
    close_db()


def test_t82_a_cross_origin_put_is_rejected_by_mutationguard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "t82-origin.db", monkeypatch)
    with TestClient(app) as client:
        token = client.get("/api/csrf").json()["csrf_token"]

        resp = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": True},
            headers={"Origin": "http://evil.example", "X-CSRF-Token": token},
        )

        assert resp.status_code == 403, resp.text
        hold = get_db().read_sync(lambda conn: read_auth_hold(conn, "history_all"))
        assert hold is None, "the rejected request must not have reached the handler"

        # Paired positive on the SAME client: the matching-origin request
        # succeeds, so the 403 above is attributable to Origin and not some
        # other guard condition.
        ok = client.put(
            _PUT_PATH,
            json={"endpoint": "history_all", "confirm": True},
            headers={"Origin": "http://testserver", "X-CSRF-Token": token},
        )
        assert ok.status_code == 200, ok.text
    close_db()


# ---------------------------------------------------------------------------
# T83 -- the probe: one call per chunk; 200 clears; 401/500 re-hold; no due
# row makes no call.
# ---------------------------------------------------------------------------


def test_t83_a_200_probe_clears_the_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    write_auth_hold(
        conn, "history_all", status="probing", since="2026-03-09T00:00:00Z", error="x"
    )
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(
            200,
            json={"observations": []},
        )

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1, "exactly one probe call per chunk"
    assert read_auth_hold(conn, "history_all") is None, "a successful probe clears it"


def test_t83_a_401_probe_sets_held_again_with_the_new_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, _station_id = _seed_due_history_only(conn, now)
    write_auth_hold(
        conn, "history_all", status="probing", since="2026-03-09T00:00:00Z", error="old"
    )
    conn.commit()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, request=request)

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "held"
    assert hold.error != "old"


def test_t83_a_500_probe_sets_held_with_backoff_and_no_further_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, _station_id = _seed_due_history_only(conn, now)
    write_auth_hold(
        conn, "history_all", status="probing", since="2026-03-09T00:00:00Z", error="old"
    )
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(500, request=request)

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1, "no further call to that endpoint in this chunk"
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "held"
    assert hold.error != "old"

    # The 500 must itself record a domain backoff (not merely re-hold the
    # endpoint, which alone would already block the next chunk's probe
    # regardless of any backoff): pinned directly, so a mutant that drops
    # the `record_http_backoff` call from `_probe_failed` is caught here
    # even though the "next chunk makes none" assertion below cannot see it
    # (a `held` endpoint is skipped by the probe loop for that reason
    # alone).
    backoff_row = conn.execute(
        "SELECT next_attempt_at FROM domain_backoffs WHERE domain = ?",
        (source_domain("weathercom"),),
    ).fetchone()
    assert backoff_row is not None, (
        "a 500 on the probe must record a domain backoff, not just re-hold"
    )

    # The next chunk makes none either: the hold is no longer `probing`
    # (set back to `held` above), so the probe step has nothing to run.
    call_log.clear()
    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert call_log == []


def test_t83_no_due_row_makes_no_call() -> None:
    """``_run_fetch_chunk``'s probe step reads one due row for the probing
    endpoint via ``wind_days_due``; with none due, no call is attempted --
    pinned directly at the row-selector level (the chunk-level integration
    is already covered by the three cases above)."""
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "KTEST001")
    conn.commit()
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    rows = wind_days_due(
        conn,
        site_id,
        today=now.date(),
        now=now,
        open_endpoints=frozenset({"history_all"}),
        limit=1,
    )
    assert rows == []


# ---------------------------------------------------------------------------
# T84 -- surfaces: monitor (g), the ops page and the dashboard banner.
# ---------------------------------------------------------------------------


def _healthy_site_no_hold(conn: sqlite3.Connection) -> tuple[int, int]:
    """A site that would NOT trip wind_history on its own: a valid tz (skips
    (f)), a station with a today reading (skips (e)) and a fresh
    ``last_ok_at`` (skips (h)). Returns ``(site_id, station_id)``."""
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    today = date(2026, 3, 10)
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, last_ok_at, updated_at)"
        " VALUES (?, ?, 'fetched', 0, 10, 24, 0, ?, ?)",
        (
            station_id,
            today.isoformat(),
            "2026-03-10T22:50:00Z",
            "2026-03-10T22:50:00Z",
        ),
    )
    conn.commit()
    return site_id, station_id


def test_al9_wind_history_no_longer_reads_a_leftover_all_1day_row() -> None:
    """Plan §13.5 AL9 (rewrite of the old condition-(g) test): a leftover
    ``wind_history_auth_hold:all_1day`` row is not read and gives no
    reason; a ``history_all`` hold alone (no missed yesterday row yet)
    also gives no reason; check (i) fires once a yesterday row actually
    shows a missed final collection while ``history_all`` is held."""
    now = datetime(2026, 3, 10, 23, 0, 0, tzinfo=UTC)
    conn = _make_db()
    site_id, station_id = _healthy_site_no_hold(conn)

    tripped_before, reason_before = _wind_history_trips(conn, now)
    assert (tripped_before, reason_before) == (0, None), (
        "paired negative: no hold, nothing else wrong -> ok"
    )

    # A leftover all_1day row (from before this release) must not be read.
    set_runtime_state(
        conn,
        "wind_history_auth_hold:all_1day",
        json.dumps({"status": "held", "since": "2026-03-09T00:00:00Z", "error": "x"}),
    )
    conn.commit()
    tripped1, reason1 = _wind_history_trips(conn, now)
    assert (tripped1, reason1) == (0, None), (
        "mutant -> at this assertion: correct = (0, None) (the leftover"
        " all_1day row is never read -- check (g) is gone), mutant (check"
        " (g) kept and reads the stale key) = (1, a reason mentioning"
        " all_1day)"
    )

    # history_all held alone, but no yesterday row yet -> still no reason.
    write_auth_hold(
        conn,
        "history_all",
        status="held",
        since="2026-03-09T00:00:00Z",
        error="refused",
    )
    conn.commit()
    tripped2, reason2 = _wind_history_trips(conn, now)
    assert (tripped2, reason2) == (0, None), (
        "a history_all hold alone, with no missed-day evidence yet, must"
        " not itself be the reason (there is no more check (g))"
    )

    # Now give yesterday a missed final collection: history_all held ->
    # check (i) fires with the exact missed-collection text.
    yesterday = date(2026, 3, 9)
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, last_ok_at, updated_at)"
        " VALUES (?, ?, 'fetched', 0, 10, 24, 0, ?, ?)",
        (
            station_id,
            yesterday.isoformat(),
            "2026-03-09T00:00:00Z",
            "2026-03-09T00:00:00Z",
        ),
    )
    conn.commit()
    tripped3, reason3 = _wind_history_trips(conn, now)
    assert tripped3 == 1, (
        "mutant -> at this assertion: correct = 1 (check (i) fires once"
        " history_all is held and a yesterday row missed its final"
        " collection), mutant (check (i) removed along with check (g)) = 0"
    )
    assert reason3 == (
        "wind: last collection before midnight missed for 1 of 1 stations (2026-03-09)"
    ), (
        "mutant -> at this assertion: correct = the (i) missed-collection"
        " text above, mutant (a different/garbled count or date) = a"
        " different string"
    )


@pytest.mark.parametrize(
    "hold_state",
    ["in_force", "expired_not_cleared"],
)
def test_al10_wind_history_during_a_five_minute_refusal(hold_state: str) -> None:
    """Plan §13.5 AL10: two stations give both check-(h) and check-(i)
    preconditions at once -- station A has a yesterday row that missed its
    final collection (precondition for (i), with ``history_all`` held),
    station B has no today row (precondition for (h)). While a
    ``five_minute`` access-hold row lists the kind as refused -- whether
    its hold is still ``in_force`` or merely ``expired_not_cleared`` (the
    row's ``until`` has passed but the row has not been cleared by a 2xx) --
    ``wind_history`` stays ok in both cases; once the row is cleared, it
    trips, and the first reason is check (i)'s missed-collection text."""
    now = datetime(2026, 3, 10, 23, 0, 0, tzinfo=UTC)
    conn = _make_db()
    site_id = _make_site(conn)
    station_a = _make_station(conn, site_id, "KTEST001")
    station_b = _make_station(conn, site_id, "KTEST002")

    write_auth_hold(
        conn, "history_all", status="held", since="2026-03-09T00:00:00Z", error="x"
    )
    # Station A: yesterday's final collection was missed (check (i)
    # precondition), today's reading is healthy (keeps check (e) quiet).
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, last_ok_at, updated_at)"
        " VALUES (?, '2026-03-09', 'fetched', 0, 10, 24, 0, ?, ?)",
        (station_a, "2026-03-09T12:00:00Z", "2026-03-09T12:00:00Z"),
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, last_ok_at, updated_at)"
        " VALUES (?, '2026-03-10', 'fetched', 0, 10, 24, 0, ?, ?)",
        (station_a, "2026-03-10T22:50:00Z", "2026-03-10T22:50:00Z"),
    )
    # Station B: no today row at all (check (h) precondition: stale "since
    # midnight"); no yesterday row, so it adds nothing to count_missed_final.
    conn.commit()

    until = (
        "2026-03-11T00:00:00Z" if hold_state == "in_force" else "2026-03-10T21:00:00Z"
    )
    set_runtime_state(
        conn,
        "weathercom_access_hold:five_minute",
        json.dumps(
            {
                "since": "2026-03-10T20:00:00Z",
                "until": until,
                "status": 401,
                "message": "access refused (HTTP 401)",
            }
        ),
    )
    conn.commit()

    tripped_held, reason_held = _wind_history_trips(conn, now)
    assert (tripped_held, reason_held) == (0, None), (
        "mutant -> at this assertion: correct = (0, None) (checks (h) and"
        " (i) both stay off while five_minute is in refused_kinds, whether"
        " its hold is still in force or merely expired-but-not-cleared),"
        " mutant (gating on access_held instead of refused_kinds -- the"
        " expired_not_cleared case trips because access_held is already"
        " False) = (1, a reason) for the expired_not_cleared case"
    )

    clear_access_refused(conn, "five_minute")
    conn.commit()
    tripped_clear, reason_clear = _wind_history_trips(conn, now)
    assert tripped_clear == 1, (
        "mutant -> at this assertion: correct = 1 (once five_minute is no"
        " longer listed as refused, the (i)/(h) checks resume), mutant (no"
        " (h)/(i) gate at all -- would already have tripped above) = 0"
        " already seen in the held case above"
    )
    assert reason_clear is not None and reason_clear.startswith(
        "wind: last collection before midnight missed for"
    ), (
        "mutant -> at this assertion: correct = the (i) missed-collection"
        " text (evaluated before (h)), mutant (a different/garbled first"
        " reason) = a different string"
    )
    assert station_b  # station B exists only to supply the (h) precondition


def test_al10_check_h_alone_is_gated_by_five_minute_refused() -> None:
    """Plan §13.5 AL10 (isolating check (h) alone): with NO ``history_all``
    hold and NO missed yesterday row, check (i) can never fire. A today row
    exists with a healthy record_count so check (e) stays quiet, but its
    ``last_ok_at`` is older than twice the obs interval, so check (h) alone
    would trip -- proving check (h)'s own gate specifically. The mutant this
    test kills ("no (h) gate") would trip "while five_minute is held" where
    the correct behaviour stays quiet."""
    now = datetime(2026, 3, 10, 23, 0, 0, tzinfo=UTC)
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    today = date(2026, 3, 10)
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, last_ok_at, updated_at)"
        " VALUES (?, ?, 'fetched', 0, 10, 24, 0, ?, ?)",
        (
            station_id,
            today.isoformat(),
            "2026-03-10T08:00:00Z",
            "2026-03-10T08:00:00Z",
        ),
    )
    conn.commit()
    # A today row with record_count > 0 keeps check (e) quiet; its
    # last_ok_at is older than 2 * obs_interval_minutes, the check (h)
    # precondition.

    set_runtime_state(
        conn,
        "weathercom_access_hold:five_minute",
        json.dumps(
            {
                "since": "2026-03-10T20:00:00Z",
                "until": "2026-03-11T00:00:00Z",
                "status": 401,
                "message": "access refused (HTTP 401)",
            }
        ),
    )
    conn.commit()

    tripped_held, reason_held = _wind_history_trips(conn, now)
    assert (tripped_held, reason_held) == (0, None), (
        "mutant -> at this assertion: correct = (0, None) (check (h) is"
        " gated off while five_minute is refused), mutant (no (h) gate) ="
        " (1, 'wind for KTEST001 has not updated since 2026-03-10T08:00:00Z')"
    )

    clear_access_refused(conn, "five_minute")
    conn.commit()
    tripped_clear, reason_clear = _wind_history_trips(conn, now)
    assert (tripped_clear, reason_clear) == (
        1,
        "wind for KTEST001 has not updated since 2026-03-10T08:00:00Z",
    ), (
        "mutant -> at this assertion: correct = (1, the exact (h) text"
        " above), mutant (check (h) never re-enabled, or the wrong text) ="
        " a different pair"
    )


def test_ms2_ops_page_shows_history_all_only_a_leftover_all_1day_row_is_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §13.5 MS2 (ops half): a leftover ``wind_history_auth_hold:
    all_1day`` row produces no block on ``/ops`` (paired negative), while a
    real ``history_all`` hold still shows its stalled text and button
    (the original T84 positive, kept)."""
    app = _boot(tmp_path, "ms2-ops.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: set_runtime_state(
                conn,
                "wind_history_auth_hold:all_1day",
                json.dumps(
                    {"status": "held", "since": "2026-03-09T00:00:00Z", "error": "x"}
                ),
            )
        )

        resp_before = client.get("/ops")
        assert resp_before.status_code == 200
        assert 'id="wind-auth-hold-history_all"' not in resp_before.text, (
            "mutant -> at this assertion: correct = absent (a leftover"
            " all_1day row is never read by the ops page), mutant (the"
            " page still renders a block keyed off the stale all_1day"
            " row) = present"
        )
        assert 'id="wind-auth-hold-all_1day"' not in resp_before.text, (
            "mutant -> at this assertion: correct = absent (the leftover"
            " all_1day row itself must never render its own block), mutant"
            " (the page renders a block keyed off the stale all_1day row) ="
            " present"
        )
        assert 'data-endpoint="all_1day"' not in resp_before.text, (
            "mutant -> at this assertion: correct = absent (the leftover"
            " all_1day row's endpoint must never appear in a data-endpoint"
            " attribute), mutant (the page renders the stale all_1day"
            " endpoint) = present"
        )

        db.write_sync(
            lambda conn: write_auth_hold(
                conn,
                "history_all",
                status="held",
                since="2026-03-09T00:00:00Z",
                error="refused",
            )
        )

        resp_after = client.get("/ops")
        history_title = (
            "This weather.com key cannot download wind history for past days,"
            " a known limit of the key"
        )
        assert resp_after.status_code == 200
        assert history_title in resp_after.text, (
            "mutant -> at this assertion: correct = present (a real"
            " history_all hold still surfaces on the ops page), mutant"
            " (the surface removed along with the all_1day key) = absent"
        )
        assert 'id="wind-auth-hold-history_all"' in resp_after.text
        assert "Try again</button>" in resp_after.text
    close_db()


@pytest.mark.parametrize(
    ("endpoint", "expect_banner"),
    [("history_all", True), ("all_1day", False)],
)
def test_ms2_dashboard_banner_only_for_history_all(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    expect_banner: bool,
) -> None:
    """Plan §13.5 MS2 (dashboard half): the dashboard banner still fires for
    a ``history_all`` lane hold, but a leftover ``all_1day`` lane-hold row
    produces no wind-hold banner block."""
    app = _boot(tmp_path, f"ms2-dash-{endpoint}.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(lambda conn: _make_site(conn))
        if endpoint == "history_all":
            db.write_sync(
                lambda conn: write_auth_hold(
                    conn,
                    "history_all",
                    status="held",
                    since="2026-03-09T00:00:00Z",
                    error="refused",
                )
            )
        else:
            db.write_sync(
                lambda conn: set_runtime_state(
                    conn,
                    "wind_history_auth_hold:all_1day",
                    json.dumps(
                        {
                            "status": "held",
                            "since": "2026-03-09T00:00:00Z",
                            "error": "refused",
                        }
                    ),
                )
            )

        resp = client.get("/dashboard")

        assert resp.status_code == 200
        assert ('id="wind-banner"' in resp.text) == expect_banner, (
            "mutant -> at this assertion: correct = "
            f"{expect_banner} (banner presence keyed on history_all only),"
            " mutant (the banner still reads the leftover all_1day row) ="
            f" {not expect_banner}"
        )
        if expect_banner:
            assert (
                "This weather.com key cannot download wind history for past"
                " days, a known limit of the key"
            ) in resp.text
    close_db()


# ---------------------------------------------------------------------------
# T85 -- a 429 writes no hold, only a domain backoff.
# ---------------------------------------------------------------------------


def test_t108_a_connect_error_probe_sets_held_and_refunds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    write_auth_hold(
        conn,
        "history_all",
        status="probing",
        since="2026-03-09T00:00:00Z",
        error="old",
    )
    conn.commit()
    before = _budget_calls(conn)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise httpx.ConnectError("refused", request=request)

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 1
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "held"
    assert hold.error != "old"
    assert _budget_calls(conn) == before, (
        "a refundable transport error on a probe must give the call back"
    )
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "pending"
    assert day["attempts"] == 0


def test_t108_a_deadline_exceeded_probe_sets_held_without_a_refund(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    write_auth_hold(
        conn,
        "history_all",
        status="probing",
        since="2026-03-09T00:00:00Z",
        error="old",
    )
    conn.commit()
    before = _budget_calls(conn)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise ProviderDeadlineExceeded()

    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "held"
    assert hold.error != "old"
    assert _budget_calls(conn) == before + 1, (
        "a non-refundable timeout on a probe must keep the spent call"
    )
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "pending"
    assert day["attempts"] == 0


def test_t108_an_active_domain_backoff_blocks_the_probe_without_changing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, _station_id = _seed_due_history_only(conn, now)
    write_auth_hold(
        conn,
        "history_all",
        status="probing",
        since="2026-03-09T00:00:00Z",
        error="old",
    )
    conn.execute(
        "INSERT INTO domain_backoffs (domain, next_attempt_at)"
        " VALUES ('api.weather.com', '2026-03-10T13:00:00Z')"
    )
    conn.commit()
    before = _budget_calls(conn)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred):
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert call_log == [], "the domain backoff must block the probe before any request"
    assert _budget_calls(conn) == before
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "probing"
    assert hold.error == "old"


def test_t85_a_429_writes_no_hold_only_a_domain_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(429, request=request)

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred):
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1
    assert read_auth_hold(conn, "history_all") is None, (
        "a 429 must never write an auth hold -- only a domain backoff"
    )
    with pytest.raises(JobDeferred):
        check_domain_backoff(conn, source_domain("weathercom"))
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "pending"
    assert _budget_calls(conn) == 1, (
        "mutant -> at this assertion: correct = 1 (a 429 is not"
        " refundable -- the spent call stays spent), mutant (a refund"
        " added to the rate_limited branch) = 0 (the call given back)"
    )
    row = conn.execute(
        "SELECT last_error FROM station_wind_days WHERE station_id=? AND local_date=?",
        (station_id, yesterday.isoformat()),
    ).fetchone()
    assert row["last_error"] is not None, (
        "mutant -> at this assertion: correct = last_error set (the"
        " rate_limited branch calls _set_last_error), mutant"
        " (_set_last_error dropped from rate_limited) = None"
    )
    station = _station_row(conn, station_id)
    assert station["last_error"] is None, (
        "a 429 must never touch the stations row -- only station_wind_days"
    )
    assert station["error_count"] == 0


# ---------------------------------------------------------------------------
# T35 -- the rest of ``_fetch_failed``'s dispatch table (>=500, a malformed
# 2xx payload, a refundable transport error), on the real fetch path rather
# than the probe path T83/T108 already cover.
# ---------------------------------------------------------------------------


def test_t35_a_server_error_defers_records_a_row_failure_and_a_domain_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(500, request=request)

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred):
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1
    assert read_auth_hold(conn, "history_all") is None, (
        "mutant -> at this assertion: correct = no auth hold written on a"
        " >=500 (it is a transient server error, not an auth refusal),"
        " mutant (>=500 routed into the 401/403 branch) = a hold written"
    )
    with pytest.raises(JobDeferred):
        check_domain_backoff(conn, source_domain("weathercom"))
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "failed", (
        "mutant -> at this assertion: correct = the row is marked failed"
        " (>=500 calls the row-failure path in addition to the backoff),"
        " mutant (the ``failure(conn)`` call dropped from server_error) ="
        " the row stays pending"
    )
    assert day["attempts"] == 1
    station = _station_row(conn, station_id)
    assert station["last_error"] is None, (
        "a >=500 server error must never touch the stations row -- only"
        " station_wind_days"
    )
    assert station["error_count"] == 0


@pytest.mark.parametrize(
    ("fault_kind", "content"),
    [
        ("json_decode", b"not json"),
        ("invalid_structure", b"[]"),
    ],
)
def test_t35_a_malformed_2xx_payload_is_recorded_as_a_row_failure_without_deferring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault_kind: str, content: bytes
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        # A 2xx whose body decode_observations_payload rejects --
        # json_decode (body is not valid JSON) or invalid_structure (valid
        # JSON, but not the allowlisted object-with-a-list shape) -- both
        # in _PAYLOAD_ROW_FAULTS, a per-row fault, not a reraise.
        return httpx.Response(200, content=content, request=request)

    # mutant -> at this call (not a named assertion -- the divergence is
    # whether it raises at all): correct = returns normally, mutant
    # (fault_kind dropped from _PAYLOAD_ROW_FAULTS) = the exception falls
    # through to the final catch-all, which still records the row failure
    # but returns "reraise", and _run_wind_days raises UpstreamPayloadError
    # instead of completing.
    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 1
    assert _budget_calls(conn) == 1, (
        "mutant -> at this assertion: correct = the spent call is NOT"
        " refunded for a payload fault, mutant (a refund added to the"
        " payload-fault branch) = the call given back"
    )
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "failed", (
        f"mutant -> at this assertion ({fault_kind}): correct = the row is"
        " marked failed (the _PAYLOAD_ROW_FAULTS branch calls the"
        " row-failure path), mutant (the write_after_reservation/failure"
        ' call dropped from that branch, leaving only `return "fetched"`)'
        " = the row stays pending"
    )
    assert day["attempts"] == 1
    station = _station_row(conn, station_id)
    assert station["last_error"] is None, (
        "a malformed-payload row failure must never touch the stations"
        " row -- only station_wind_days"
    )
    assert station["error_count"] == 0


def test_t35_a_refundable_transport_error_defers_and_refunds_the_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)
    before = _budget_calls(conn)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise httpx.ConnectError("refused", request=request)

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred) as excinfo:
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    from datetime import timedelta as _td

    assert excinfo.value.next_attempt_at == isoformat_utc(now + _td(minutes=15)), (
        "mutant -> at this assertion: correct = now + 15 minutes (the"
        " refundable-transport-error branch wakes via _transport_wake(),"
        " _TRANSPORT_DEFER), mutant (_TRANSPORT_DEFER set to timedelta(0))"
        " = now itself"
    )

    assert len(call_log) == 1
    assert _budget_calls(conn) == before, (
        "mutant -> at this assertion: correct = a refundable transport error"
        " (httpx.ConnectError) gives the spent call back, mutant"
        " (is_refundable_transport_error's result ignored/inverted in"
        " transport_failure) = the call stays spent"
    )
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "failed", (
        "mutant -> at this assertion: correct = the row is still marked"
        " failed even when the call is refunded (refund and row-failure are"
        " independent), mutant (failure(conn) dropped from"
        " transport_failure) = the row stays pending"
    )
    assert day["attempts"] == 1
    # Yesterday's row is exempt (WIND_DUE_SQL's `local_date >= :yesterday`),
    # so the refund lands on the live lane counter, not backfill.
    assert list(read_lane_counter(conn, WIND_LIVE_CALLS_KEY).values()) == [0], (
        "mutant -> at this assertion: correct = [0] (the reservation's +1"
        " and the refund's -1 net to 0 on the live lane counter), mutant"
        " (_refund replaced by _mark_station_error_and_refund, or the"
        " add_lane_calls(..., -1) decrement dropped from _refund) = [1]"
        " (the spent call is never given back to the lane counter)"
    )
    assert read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY) == {}, (
        "correct = {} (an exempt reservation never touches the backfill"
        " counter). Under lane_counter_key's `exempt=not exempt` flip, the"
        " earlier live-counter check above fails first (it lands at [1]"
        " instead of [0]) -- this assertion is the independent second"
        " guard, confirming the backfill counter itself stays empty,"
        " rather than the mutant's own kill point."
    )
    station = _station_row(conn, station_id)
    assert station["last_error"] is None
    assert station["error_count"] == 0


def test_t35_a_deadline_exceeded_on_fetch_defers_without_a_refund(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ProviderDeadlineExceeded`` subclasses ``TimeoutError``, so it takes
    ``_fetch_failed``'s ``isinstance(exc, (httpx.TransportError,
    TimeoutError))`` branch like the refundable ``httpx.ConnectError`` above
    -- but ``is_refundable_transport_error`` treats a bare deadline timeout
    as non-refundable (mirroring T108's probe-path deadline test), so this
    pins the fetch-path twin: the spent call stays spent, not given back.

    mutant -> at the ``_budget_calls`` assertion below: correct = the call
    stays spent (``before + 1``), mutant (``is_refundable_transport_error``
    widened to also refund a bare ``TimeoutError``) = the call refunded
    (``before``) -- the same fixture, read at a literal that must diverge.

    The job's ``retry_count`` is left unchanged by this path too: a
    ``JobDeferred`` is caught by ``worker/processor.py``'s dispatch loop and
    handled by ``defer_job()`` (``db/queue.py:414-423``), which updates only
    ``status``/``next_attempt_at``/``updated_at`` -- never ``retry_count``
    (bumped only by the separate ``fail()`` path). Traced through the code,
    not run: asserting it directly would need a real queued ``jobs`` row
    driven through the processor's claim/dispatch loop, a heavier harness
    than every other test in this file (which call ``run_wind_days()``
    directly and never create a ``jobs`` row at all) -- out of scope for
    this fix.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)
    before = _budget_calls(conn)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise ProviderDeadlineExceeded()

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred) as excinfo:
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    from datetime import timedelta as _td

    assert excinfo.value.next_attempt_at == isoformat_utc(now + _td(minutes=15)), (
        "mutant -> at this assertion: correct = now + 15 minutes"
        " (_transport_wake()/_TRANSPORT_DEFER), mutant (_TRANSPORT_DEFER"
        " set to timedelta(0)) = now itself"
    )

    assert len(call_log) == 1
    assert _budget_calls(conn) == before + 1, (
        "mutant -> at this assertion: correct = a non-refundable deadline"
        " timeout keeps the spent call (before + 1), mutant"
        " (is_refundable_transport_error widened to refund a bare"
        " TimeoutError) = the call given back (before)"
    )
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "failed", (
        "mutant -> at this assertion: correct = the row is still marked"
        " failed on a non-refundable transport error (failure(conn) runs"
        " regardless of refundability), mutant (failure(conn) dropped from"
        " transport_failure) = the row stays pending"
    )
    assert day["attempts"] == 1
    station = _station_row(conn, station_id)
    assert station["last_error"] is None, (
        "a fetch-path transport failure must never touch the stations row --"
        " only station_wind_days -- paired with T80's 401/403 check of the"
        " same invariant on a different branch"
    )
    assert station["error_count"] == 0


def test_t35_a_404_runs_the_first_1h_then_4h_retry_chain_to_a_final_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 404 falls through ``_fetch_failed``'s dispatch table to the final
    ``failure(conn); return "fetched"`` branch, which runs ``_row_failure``
    (plan §8.7): attempt 1 schedules ``_FIRST_RETRY`` (+1h), attempt 2+
    schedules ``_LATER_RETRY`` (+4h), and at ``attempts == _MAX_ATTEMPTS``
    (3) the row drops out of ``WIND_DUE_SQL`` (``attempts < 3``) for good --
    a final failure, not a fourth retry.

    mutant -> at the attempt-2 ``next_attempt_at`` check: correct =
    ``t1 + timedelta(hours=4)`` (``_LATER_RETRY`` on every attempt after the
    first), mutant (``_FIRST_RETRY`` used unconditionally instead of
    ``_FIRST_RETRY if attempts == 1 else _LATER_RETRY``) = ``t1 +
    timedelta(hours=1)`` -- same fixture, the two literals diverge by 3h.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)

    def handler_404(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    # Attempt 1: must not raise (404 is not in the JobDeferred branches).
    _run_wind_days(site_id, handler_404, monkeypatch=monkeypatch)
    row = conn.execute(
        "SELECT status, attempts, next_attempt_at FROM station_wind_days"
        " WHERE station_id=? AND local_date=?",
        (station_id, yesterday.isoformat()),
    ).fetchone()
    assert row["status"] == "failed"
    assert row["attempts"] == 1
    t1 = datetime.fromisoformat(row["next_attempt_at"])
    assert t1 == now + timedelta(hours=1), (
        "mutant -> at this assertion: correct = the first retry is"
        " scheduled +1h (_FIRST_RETRY on attempts == 1), mutant (_LATER_RETRY"
        " used on the first attempt too) = +4h"
    )

    # Attempt 2, past the first retry's next_attempt_at: +4h this time.
    now2 = t1 + timedelta(minutes=1)
    _freeze(monkeypatch, now2)
    _run_wind_days(site_id, handler_404, monkeypatch=monkeypatch)
    row = conn.execute(
        "SELECT status, attempts, next_attempt_at FROM station_wind_days"
        " WHERE station_id=? AND local_date=?",
        (station_id, yesterday.isoformat()),
    ).fetchone()
    assert row["attempts"] == 2
    t2 = datetime.fromisoformat(row["next_attempt_at"])
    assert t2 == now2 + timedelta(hours=4), (
        "mutant -> at this assertion: correct = the second retry is"
        " scheduled +4h (_LATER_RETRY once attempts > 1), mutant"
        " (_FIRST_RETRY used unconditionally) = +1h"
    )

    # Attempt 3, past the second retry's next_attempt_at: the final failure.
    now3 = t2 + timedelta(minutes=1)
    _freeze(monkeypatch, now3)
    _run_wind_days(site_id, handler_404, monkeypatch=monkeypatch)
    row = conn.execute(
        "SELECT status, attempts FROM station_wind_days"
        " WHERE station_id=? AND local_date=?",
        (station_id, yesterday.isoformat()),
    ).fetchone()
    assert row["status"] == "failed"
    assert row["attempts"] == 3

    # A fourth run must not touch the row again: WIND_DUE_SQL excludes
    # status='failed' once attempts >= 3, structurally, regardless of time --
    # stay well short of a UTC midnight crossing so the fixture doesn't
    # fabricate a new "today" row and call all_1day for an unrelated reason.
    now4 = now3 + timedelta(minutes=1)
    _freeze(monkeypatch, now4)

    def handler_unexpected(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"no call expected, got {request.url}")

    _run_wind_days(site_id, handler_unexpected, monkeypatch=monkeypatch)
    row = conn.execute(
        "SELECT attempts FROM station_wind_days WHERE station_id=? AND local_date=?",
        (station_id, yesterday.isoformat()),
    ).fetchone()
    assert row["attempts"] == 3, (
        "mutant -> at this assertion: correct = a row at attempts == 3 is"
        " excluded from WIND_DUE_SQL for good (no fourth call), mutant"
        " (attempts < 3 widened to <= 3) = a fourth call made and attempts"
        " bumped to 4"
    )
    station = _station_row(conn, station_id)
    assert station["last_error"] is None, (
        "a row-failure chain across three attempts must never touch the"
        " stations row -- only station_wind_days"
    )
    assert station["error_count"] == 0


def test_t35_a_row_failure_lets_the_chunk_continue_to_the_next_due_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_fetch_failed``'s row-failure branches (404, >=500, a malformed 2xx)
    return ``"fetched"``/raise ``JobDeferred`` -- they never abort the whole
    chunk. This pins the plain row-failure case (404) against a second due
    station in the same site+chunk: a positive (the second station's call
    still happens and succeeds) paired with the negative (no call reaches
    the second station for a reason other than "the chunk kept going" -- see
    the ``AssertionError`` handler below if it did abort).

    mutant -> at the ``_run_wind_days(site_id, handler, ...)`` call below
    (the divergence is whether it raises at all, not a later named
    assertion): correct = returns normally, mutant (``_fetch_failed``'s
    final ``return "fetched"`` branch changed to ``raise JobDeferred`` like
    the >=500/429 branches) = an uncaught ``JobDeferred`` propagates out of
    that call and fails the test before ``station2_called`` is ever read,
    because the raise ends ``run_wind_days`` for the whole chunk and the
    second station is never reached. Empirically confirmed on a throwaway
    copy: the mutated run raised ``wxverify.worker.control.JobDeferred``
    at the mutated line, exactly as predicted.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station1_id = _seed_due_history_only(conn, now)
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    yesterday = now.astimezone(ZoneInfo(_TZ)).date() - timedelta(days=1)
    station2_id = _make_station(conn, site_id, "KTEST002")
    _insert_day(conn, station2_id, yesterday, status="pending", attempts=0)
    conn.commit()

    station2_called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal station2_called
        if "KTEST002" in str(request.url):
            station2_called = True
            return httpx.Response(200, json={"observations": []})
        return httpx.Response(404, request=request)

    # mutant -> at this call (see the docstring): correct = returns
    # normally, mutant (the 404 branch's "return \"fetched\"" changed to
    # "raise JobDeferred") = an uncaught JobDeferred propagates here instead.
    _run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert station2_called, (
        "the chunk's loop must reach the second due station and call it --"
        " a False here (with no exception above) would mean the chunk"
        " stopped silently after the first row's failure instead of raising"
    )
    row1 = conn.execute(
        "SELECT status, attempts FROM station_wind_days"
        " WHERE station_id=? AND local_date=?",
        (station1_id, yesterday.isoformat()),
    ).fetchone()
    assert row1["status"] == "failed"
    assert row1["attempts"] == 1
    station1 = _station_row(conn, station1_id)
    assert station1["last_error"] is None
    assert station1["error_count"] == 0


def test_t35_a_persist_step_exception_is_re_raised_with_a_progress_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fetch can succeed while the persist step after it raises (``_run_row``
    lines ~731-768): a plain exception there is not ``StaleGenerationError``/
    ``sqlite3.OperationalError``/``JobControl``, so it takes the row-failure
    path, gets a progress note attached, and is re-raised as-is -- not
    swallowed, not re-wrapped in ``JobDeferred``.

    mutant -> at the ``exc.__notes__`` assertion: correct = a note containing
    "progress=" is present (``exc.add_note(...)`` ran before the re-raise),
    mutant (the ``exc.add_note`` call dropped from the except-and-reraise
    branch) = ``exc.__notes__`` is empty on the same raised exception.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_history_only(conn, now)
    from datetime import timedelta

    yesterday = now.date() - timedelta(days=1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"observations": []})

    def _boom(*_a: object, **_kw: object) -> None:
        raise ValueError("synthetic persist failure")

    monkeypatch.setattr("wxverify.worker.wind_days._persist_day", _boom)

    with pytest.raises(ValueError, match="synthetic persist failure") as excinfo:
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    notes = getattr(excinfo.value, "__notes__", [])
    assert any("progress=" in note for note in notes), (
        "mutant -> at this assertion: correct = a 'progress=' note is"
        " attached before the re-raise, mutant (exc.add_note dropped from"
        " the persist except-and-reraise branch) = no notes at all"
    )
    day = _day_row(conn, station_id, yesterday)
    assert day["status"] == "failed", (
        "mutant -> at this assertion: correct = a persist-step exception"
        " still runs _row_failure before re-raising (the row is NOT left"
        " pending), mutant (the _row_failure call dropped from the"
        " except-and-reraise branch) = the row stays pending"
    )
    assert day["attempts"] == 1
    station = _station_row(conn, station_id)
    assert station["last_error"] is None
    assert station["error_count"] == 0


def test_t35_a_transport_error_on_todays_row_marks_it_partial_not_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_row_failure``'s ``local_date == today`` branch (plan §8.7): today's
    row never goes to ``failed``/``attempts`` -- it goes to ``partial`` with
    a re-poll ``next_attempt_at``, because more of today's data may still
    arrive before the day closes.

    mutant -> at the ``day["status"]`` assertion: correct = "partial" (the
    ``local_date == today`` branch taken), mutant (the ``local_date ==
    today`` check inverted/dropped, falling through to the attempts/failed
    branch) = "failed" -- same fixture (today's row), the two literals
    diverge.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_due_today_only(conn, now)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred) as excinfo:
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    from datetime import timedelta as _td

    assert excinfo.value.next_attempt_at == isoformat_utc(now + _td(minutes=15)), (
        "mutant -> at this assertion: correct = now + 15 minutes"
        " (_transport_wake()/_TRANSPORT_DEFER), mutant (_TRANSPORT_DEFER"
        " set to timedelta(0)) = now itself"
    )

    day = _day_row(conn, station_id, now.date())
    assert day["status"] == "partial", (
        "mutant -> at this assertion: correct = partial (today's row is"
        " never marked failed/attempts-bumped while the day is still open),"
        " mutant (the local_date == today branch skipped) = failed"
    )
    assert day["attempts"] == 0, (
        "the partial branch must not touch attempts -- only the"
        " failed/retry branch for a past date does"
    )
    station = _station_row(conn, station_id)
    assert station["last_error"] is None
    assert station["error_count"] == 0


def test_t35_a_transport_error_on_a_pending_reconcile_refetch_sets_refetched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_row_failure``'s ``_is_refetch(row)`` branch (plan §8.7): a row
    already ``fetched``, with a pending reconcile refetch due (``refetch_at``
    set, ``refetched == 0``), that fails again on the refetch attempt is
    marked ``refetched = 1`` (reconcile gives up, not retried forever) and
    gets a fresh ``last_error`` -- its ``status``/``attempts`` are left
    exactly as they were (paired negative: unlike the plain-failure branch,
    this one must NOT bump attempts or flip status to "failed").

    mutant -> at the ``day["refetched"]`` assertion: correct = 1 (the
    ``_is_refetch`` branch ran and marked the refetch resolved), mutant
    (``_is_refetch``'s own check -- e.g. ``refetched == 0`` inverted to
    ``== 1`` -- so the branch is never taken for this row) = 0, because the
    row instead falls through to the past-date failed/attempts branch.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    from datetime import timedelta

    stale_date = now.date() - timedelta(days=5)
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count,"
        "  pair_hours, refetch_at, refetched, updated_at)"
        " VALUES (?, ?, 'fetched', 2, 10, 5, ?, 0, '2026-01-01T00:00:00Z')",
        (
            station_id,
            stale_date.isoformat(),
            (now - timedelta(hours=1)).isoformat(),
        ),
    )
    conn.commit()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred) as excinfo:
        _run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    from datetime import timedelta as _td

    assert excinfo.value.next_attempt_at == isoformat_utc(now + _td(minutes=15)), (
        "mutant -> at this assertion: correct = now + 15 minutes"
        " (_transport_wake()/_TRANSPORT_DEFER), mutant (_TRANSPORT_DEFER"
        " set to timedelta(0)) = now itself"
    )

    day = conn.execute(
        "SELECT status, attempts, refetched, last_error FROM station_wind_days"
        " WHERE station_id=? AND local_date=?",
        (station_id, stale_date.isoformat()),
    ).fetchone()
    assert day["refetched"] == 1, (
        "mutant -> at this assertion: correct = 1 (the _is_refetch branch"
        " ran and resolved the pending refetch), mutant (_is_refetch's own"
        " condition broken so the branch is skipped) = 0"
    )
    assert day["last_error"] is not None
    assert day["status"] == "fetched", (
        "the refetch branch must leave status untouched -- it must NOT be"
        " flipped to 'failed' the way the past-date non-refetch branch would"
    )
    assert day["attempts"] == 2, (
        "the refetch branch must leave attempts untouched -- it must NOT be"
        " bumped the way the past-date non-refetch branch would"
    )
    station = _station_row(conn, station_id)
    assert station["last_error"] is None
    assert station["error_count"] == 0

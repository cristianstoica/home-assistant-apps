"""The refused-access kinds' lanes run independently (plan §13.5 FM4).

``current_obs``, ``hourly`` and ``five_minute`` (today's wind ``all_1day``
download) each hold only their own ``AccessKind``. ``hourly`` is the gap
fill's kind now, with its own hold pinned by GF4 (``test_gap_fill.py``);
only ``current_obs`` and ``five_minute`` run their lanes here.
``test_access_hold_current_obs.py``'s CO1 already proves that a refusal
of one kind leaves the *other kinds' hold rows* absent; what it does not
prove is the new, genuinely cross-lane half of this release -- that the
wind lane's ``five_minute`` hold does not block ``current_obs``'s real
calls, and that a ``current_obs``/``hourly`` hold does not block the wind
lane's real ``all_1day`` call. This file drives the current-obs job and
the wind chunk against one site+station in the same test and
parametrizes over which of the three kinds is held, so a copy-paste
mix-up of the ``AccessKind`` constant at any call site shows up as a
miscounted call somewhere in the matrix, not just as a wrong hold-row
key.

Isolation: a single real tmp-path ``Database`` (``init_db``); the
current-obs job runs through ``run_claimed_job(db, job, lane=
"current_obs")`` with ``wxverify.worker.processor.fetch_current_observation``
monkeypatched; the wind chunk runs through ``run_wind_days`` against an
``httpx.MockTransport`` patched onto
``wxverify.worker.wind_days.httpx.AsyncClient`` -- the same patterns
``test_access_hold_current_obs.py`` and ``test_wind_auth_hold.py`` each
already use, combined onto one site.

Every station id below is synthetic: ``KTEST001``.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.queue import (
    claim_next_current_obs_job,
    enqueue_if_absent,
)
from wxverify.db.runtime_state import set_runtime_state
from wxverify.worker.access_hold import (
    AccessKind,
    access_hold_key,
    read_access_record,
    record_access_refused,
)
from wxverify.worker.processor import run_claimed_job
from wxverify.worker.wind_days import ensure_wind_days, run_wind_days, wind_lane_due

_API_KEY = "0123456789abcdef0123456789abcdef"
_PAST_ISO = "2026-07-10T11:59:00Z"


@pytest.fixture(autouse=True)
def _weathercom_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _seed_site(conn, *, name: str = "Testsite") -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station(conn, site_id: int, *, pws_id: str) -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        " enabled) VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_id),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_poll_state(conn, station_id: int) -> None:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, cadence_events,"
        "  last_error, error_count, updated_at)"
        " VALUES (?, 'online', ?, '[]', NULL, 0, ?)",
        (station_id, _PAST_ISO, _PAST_ISO),
    )


def _online_body() -> bytes:
    return json.dumps(
        {
            "observations": [
                {
                    "obsTimeUtc": "2026-07-10T11:55:00Z",
                    "humidity": 50.0,
                    "winddir": 180.0,
                    "uv": 1.0,
                    "neighborhood": "Test Quarter",
                    "metric": {
                        "temp": 20.0,
                        "dewpt": 10.0,
                        "windSpeed": 5.0,
                        "windGust": 8.0,
                        "pressure": 1012.0,
                        "precipRate": 0.0,
                        "precipTotal": 0.0,
                    },
                }
            ]
        }
    ).encode()


_WEATHER_COM_URL = httpx.URL("https://api.weather.com/v2/pws/observations/current")


def _online_response() -> httpx.Response:
    return httpx.Response(
        200,
        content=_online_body(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", str(_WEATHER_COM_URL)),
    )


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: when)


# ---------------------------------------------------------------------------
# AH1-AH4 (P2 14.7): the five_minute hold checked per row, inside each
# station's own reservation, for today's 5-minute download.
# ---------------------------------------------------------------------------

_AH_DAY = date(2026, 3, 10)
_AH_TZ = "UTC"


def _run_wind_days_chunk(
    db: Database, site_id: int, handler: object, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    real_client = httpx.AsyncClient
    writer = FencedWriter(db, db.generation)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(  # type: ignore[arg-type]
            transport=httpx.MockTransport(handler)
        ),
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


def _insert_today_row(  # type: ignore[no-untyped-def]
    conn, station_id: int, local_date: date, *, next_attempt_at: str
) -> None:
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count,"
        "  pair_hours, next_attempt_at, refetched, updated_at)"
        " VALUES (?, ?, 'partial', 0, 0, 0, ?, 0, '2026-01-01T00:00:00Z')",
        (station_id, local_date.isoformat(), next_attempt_at),
    )
    conn.commit()


def _ah_day_row(conn, station_id: int):  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT * FROM station_wind_days WHERE station_id = ? AND local_date = ?",
        (station_id, _AH_DAY.isoformat()),
    ).fetchone()
    assert row is not None
    return row


def _ah_close_try_rows(conn, station_id: int):  # type: ignore[no-untyped-def]
    return conn.execute(
        "SELECT * FROM close_try_log WHERE station_id = ? AND local_date = ?"
        " ORDER BY id",
        (station_id, _AH_DAY.isoformat()),
    ).fetchall()


def _ah_budget_calls(conn) -> int:  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget"
        " WHERE source = 'weathercom'"
    ).fetchone()
    return int(row["n"])


def _set_hold(  # type: ignore[no-untyped-def]
    conn, kind: AccessKind, *, since: str, until: str, status: int
) -> None:
    """Write ``kind``'s hold row directly, with an exact ``until``.

    Bypasses ``record_access_refused`` (which always derives ``until`` from
    the frozen clock) so AH1-AH3 can pin an ``until`` that does not line up
    with any evening slot.
    """
    set_runtime_state(
        conn,
        access_hold_key(kind),
        json.dumps(
            {
                "since": since,
                "until": until,
                "status": status,
                "message": f"access refused (HTTP {status})",
            }
        ),
    )
    conn.commit()


def test_ah1_a_held_try_sends_nothing_and_moves_to_its_next_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2 14.7 AH1: the ``five_minute`` hold in force until 23:40:12; two
    stations' today rows due at 23:06. No request is sent, the budget is
    unchanged, the hold's own row is unchanged (same ``until``), each row
    moves to its own next slot (23:11 -- not the hold's ``until``), and one
    ``close_try_log`` row per station logs outcome ``held``/reason
    ``refused_access``/detail the hold's ``until``.
    """
    from wxverify.worker.wind_days import evening_slot_instants  # new name (P2)

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    station_a = _seed_station(conn, site_id, pws_id="ISTATION01")
    station_b = _seed_station(conn, site_id, pws_id="ISTATION02")

    slots = evening_slot_instants(_AH_DAY, _AH_TZ)
    slot_2300, slot_2306, slot_2311 = slots[0], slots[1], slots[2]
    original_until = isoformat_utc(slot_2300 + timedelta(minutes=40, seconds=12))
    _set_hold(
        conn,
        "five_minute",
        since=isoformat_utc(slot_2300 - timedelta(hours=2)),
        until=original_until,
        status=401,
    )
    due_2306 = isoformat_utc(slot_2306)
    _insert_today_row(conn, station_a, _AH_DAY, next_attempt_at=due_2306)
    _insert_today_row(conn, station_b, _AH_DAY, next_attempt_at=due_2306)

    before_calls = _ah_budget_calls(conn)
    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _freeze(monkeypatch, slot_2306)
    _run_wind_days_chunk(db, site_id, handler, monkeypatch=monkeypatch)

    assert call_log == [], (
        "mutant -> at this assertion: correct = no request (a held try sends"
        " nothing), mutant = " + repr(call_log)
    )
    assert _ah_budget_calls(conn) == before_calls, "a held try spends no budget"

    record = read_access_record(conn, "five_minute")
    assert record is not None and record.until == original_until, (
        "mutant -> at this assertion: correct ="
        f" {original_until!r} (M43: a held try must never call"
        " record_access_refused, which would overwrite the hold's own"
        " until with one computed from now), mutant ="
        f" {record.until if record is not None else None!r}"
    )

    for station_id in (station_a, station_b):
        row = _ah_day_row(conn, station_id)
        assert row["next_attempt_at"] == isoformat_utc(slot_2311), (
            "mutant -> at this assertion"
            f" (station={station_id}): correct = {isoformat_utc(slot_2311)!r}"
            " (23:11, the row's own next evening slot -- M42: a hold"
            " checked only once at the start of the run, outside each"
            " row's own reservation, never runs this row's per-row write at"
            " all, so the row stays at its original due time), mutant ="
            f" {row['next_attempt_at']!r}"
        )
        rows = _ah_close_try_rows(conn, station_id)
        assert len(rows) == 1 and rows[0]["outcome"] == "held", (
            "mutant -> at this assertion"
            f" (station={station_id}): correct = one close_try_log row,"
            " outcome='held' (M73: a held try logged with outcome"
            f" 'failed'), mutant = {[dict(r) for r in rows]}"
        )
        assert rows[0]["reason"] == "refused_access"
        assert rows[0]["detail"] == original_until
        assert rows[0]["readings_kept"] == 0
        assert rows[0]["readings_refused"] == 0
    close_db()


def test_ah2_collection_resumes_at_the_rows_own_next_slot_not_the_holds_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2 14.7 AH2: the same hold (until 23:40:12); the lane runs at 23:36
    and at 23:40:13. At 23:36 the held rows move to 23:41 (their own next
    slot); at 23:40:13, just after the hold lifts, nothing is due yet (the
    rows are not due again until 23:41, not "as soon as the hold ends");
    at 23:41 each station gets exactly one try.
    """
    from wxverify.worker.wind_days import evening_slot_instants  # new name (P2)

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    station_a = _seed_station(conn, site_id, pws_id="ISTATION01")
    station_b = _seed_station(conn, site_id, pws_id="ISTATION02")

    slots = evening_slot_instants(_AH_DAY, _AH_TZ)
    slot_2300 = slots[0]
    slot_2336 = next(s for s in slots if s == slot_2300 + timedelta(minutes=36))
    slot_2341 = next(s for s in slots if s == slot_2300 + timedelta(minutes=41))
    until = isoformat_utc(slot_2300 + timedelta(minutes=40, seconds=12))
    _set_hold(
        conn,
        "five_minute",
        since=isoformat_utc(slot_2300 - timedelta(hours=2)),
        until=until,
        status=401,
    )
    due_2336 = isoformat_utc(slot_2336)
    _insert_today_row(conn, station_a, _AH_DAY, next_attempt_at=due_2336)
    _insert_today_row(conn, station_b, _AH_DAY, next_attempt_at=due_2336)

    unexpected_calls: list[str] = []

    def no_call_expected(request: httpx.Request) -> httpx.Response:
        # Logged, not raised: a raised exception would just be caught by
        # _run_row's own broad `except Exception` and recorded as a
        # "failed" try instead of surfacing here -- an explicit call-log
        # assertion is the oracle that actually distinguishes "held, no
        # call" from "a call was made (and happened to error)".
        unexpected_calls.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _freeze(monkeypatch, slot_2336)
    _run_wind_days_chunk(db, site_id, no_call_expected, monkeypatch=monkeypatch)
    assert unexpected_calls == [], (
        "mutant -> at this assertion: correct = no request (still held at"
        f" 23:36), mutant = {unexpected_calls}"
    )
    for station_id in (station_a, station_b):
        row = _ah_day_row(conn, station_id)
        assert row["next_attempt_at"] == isoformat_utc(slot_2341), (
            "mutant -> at this assertion"
            f" (station={station_id}): correct = {isoformat_utc(slot_2341)!r}"
            " (23:41, the next slot after 23:36 -- M44: a held row left due"
            " at its old slot instead of moved to its next one), mutant ="
            f" {row['next_attempt_at']!r}"
        )

    just_after_until = slot_2300 + timedelta(minutes=40, seconds=13)
    _freeze(monkeypatch, just_after_until)
    _run_wind_days_chunk(db, site_id, no_call_expected, monkeypatch=monkeypatch)
    assert unexpected_calls == [], (
        "mutant -> at this assertion: correct = no request (23:40:13 is"
        " after the hold's until, 23:40:12, but before the rows' own next"
        " slot, 23:41, so nothing is due yet -- M44: a row left due at its"
        " old 23:36 slot would be due again the instant the hold lifts,"
        f" making a request here), mutant = {unexpected_calls}"
    )
    for station_id in (station_a, station_b):
        row = _ah_day_row(conn, station_id)
        assert row["next_attempt_at"] == isoformat_utc(slot_2341), (
            "mutant -> at this assertion"
            f" (station={station_id}): correct ="
            f" {isoformat_utc(slot_2341)!r} unchanged, mutant ="
            f" {row['next_attempt_at']!r}"
        )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _freeze(monkeypatch, slot_2341)
    _run_wind_days_chunk(db, site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 2, (
        "mutant -> at this assertion: correct = 2 (one try per station once"
        f" their own next slot, 23:41, is reached), mutant = {len(call_log)}"
    )
    close_db()


def test_ah3_a_401_for_the_first_station_holds_the_others_in_the_same_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2 14.7 AH3: a 401 at 23:00 for the first of three stations in one
    run: the hold is written to one hour after the refusal (local 00:00),
    the other stations' rows are held in the SAME run with no request,
    every row's next try is 23:06, and the first station's log row is
    outcome ``refused``/reason ``refused_access``/detail ``http 401`` while
    the others log outcome ``held``/reason ``refused_access``.
    """
    from wxverify.worker.wind_days import evening_slot_instants  # new name (P2)

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    station_1 = _seed_station(conn, site_id, pws_id="ISTATION01")
    station_2 = _seed_station(conn, site_id, pws_id="ISTATION02")
    station_3 = _seed_station(conn, site_id, pws_id="ISTATION03")

    slots = evening_slot_instants(_AH_DAY, _AH_TZ)
    slot_2300, slot_2306 = slots[0], slots[1]
    for station_id in (station_1, station_2, station_3):
        _insert_today_row(
            conn, station_id, _AH_DAY, next_attempt_at=isoformat_utc(slot_2300)
        )
    assert read_access_record(conn, "five_minute") is None, (
        "fixture bug: no hold must pre-exist, so the 401 below is the first"
        " refusal of this run"
    )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        if len(call_log) == 1:
            return httpx.Response(401, request=request)
        return httpx.Response(200, json={"observations": []})

    _freeze(monkeypatch, slot_2300)
    _run_wind_days_chunk(db, site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1, (
        "mutant -> at this assertion: correct = 1 (M42: a hold checked only"
        " once at the start of the run, before the 401 wrote it, would let"
        " the other two stations' reservations through with no held check"
        " at all, giving 3 real calls), mutant = " + repr(len(call_log))
    )

    expected_until = isoformat_utc(slot_2300 + timedelta(seconds=3600))
    record = read_access_record(conn, "five_minute")
    assert record is not None and record.until == expected_until, (
        "mutant -> at this assertion: correct ="
        f" {expected_until!r} (one hour after the 23:00 refusal, local"
        f" 00:00), mutant = {record.until if record is not None else None!r}"
    )

    for station_id in (station_1, station_2, station_3):
        row = _ah_day_row(conn, station_id)
        assert row["next_attempt_at"] == isoformat_utc(slot_2306), (
            "mutant -> at this assertion"
            f" (station={station_id}): correct = {isoformat_utc(slot_2306)!r}"
            " (23:06 for every row, refused and held alike -- M79: a 401"
            " returning 'closed' instead of 'fetched' would make the chunk"
            " loop drop the OTHER stations' rows from this chunk entirely,"
            " via the old all_1day opened.discard, so their next_attempt_at"
            f" would stay at the original 23:00), mutant = {row['next_attempt_at']!r}"
        )

    refused_rows = _ah_close_try_rows(conn, station_1)
    assert len(refused_rows) == 1 and refused_rows[0]["outcome"] == "refused", (
        "mutant -> at this assertion: correct = one close_try_log row,"
        " outcome='refused' (M74: a 401 logged with outcome 'failed'),"
        f" mutant = {[dict(r) for r in refused_rows]}"
    )
    assert refused_rows[0]["reason"] == "refused_access"
    assert refused_rows[0]["detail"] == "http 401"

    for station_id in (station_2, station_3):
        held_rows = _ah_close_try_rows(conn, station_id)
        assert len(held_rows) == 1 and held_rows[0]["outcome"] == "held", (
            "mutant -> at this assertion"
            f" (station={station_id}): correct = one close_try_log row,"
            f" outcome='held', mutant = {[dict(r) for r in held_rows]}"
        )
        assert held_rows[0]["reason"] == "refused_access"
        assert held_rows[0]["detail"] == expected_until
    close_db()


def test_ah4_a_2xx_clears_only_five_minute_not_current_obs_or_hourly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2 14.7 AH4: a 2xx on today's row while ``current_obs`` and
    ``hourly`` are held: only ``five_minute`` is cleared (M45: a 2xx on
    today's row clearing every kind)."""
    from wxverify.worker.wind_days import evening_slot_instants  # new name (P2)

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id, pws_id="ISTATION01")

    slots = evening_slot_instants(_AH_DAY, _AH_TZ)
    slot_2300 = slots[0]
    due_2300 = isoformat_utc(slot_2300)
    _insert_today_row(conn, station_id, _AH_DAY, next_attempt_at=due_2300)

    # A leftover five_minute hold that has already expired: it does not
    # block this try, but it must be CLEARED (deleted) by the 2xx below.
    _set_hold(
        conn,
        "five_minute",
        since=isoformat_utc(slot_2300 - timedelta(hours=3)),
        until=isoformat_utc(slot_2300 - timedelta(hours=1)),
        status=401,
    )
    # Two unrelated kinds, held and NOT yet expired: this call must never
    # touch either.
    _set_hold(
        conn,
        "current_obs",
        since=isoformat_utc(slot_2300 - timedelta(hours=1)),
        until=isoformat_utc(slot_2300 + timedelta(hours=1)),
        status=401,
    )
    _set_hold(
        conn,
        "hourly",
        since=isoformat_utc(slot_2300 - timedelta(hours=1)),
        until=isoformat_utc(slot_2300 + timedelta(hours=1)),
        status=403,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"observations": []})

    _freeze(monkeypatch, slot_2300)
    _run_wind_days_chunk(db, site_id, handler, monkeypatch=monkeypatch)

    assert read_access_record(conn, "five_minute") is None, (
        "mutant -> at this assertion: correct = None (a 2xx on today's row"
        " clears its own five_minute hold), mutant = a stored record"
    )
    for kind, expected_status in (("current_obs", 401), ("hourly", 403)):
        record = read_access_record(conn, kind)
        assert record is not None and record.status == expected_status, (
            "mutant -> at this assertion"
            f" (kind={kind}): correct = a stored record with status="
            f"{expected_status} (M45: a 2xx on today's row clearing EVERY"
            f" kind, not just five_minute), mutant ="
            f" {record.status if record is not None else None!r} or absent"
        )
    close_db()


@pytest.mark.parametrize("held", ["current_obs", "hourly", "five_minute"])
def test_fm4_each_kinds_hold_leaves_the_other_two_lanes_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, held: AccessKind
) -> None:
    """Hold exactly one of ``current_obs``/``hourly``/``five_minute``, then
    run one current-obs job and one wind chunk against 200-answering
    fakes: a held ``current_obs`` or ``five_minute`` lane makes 0 calls
    and its row stays absent from the other's place; a held ``hourly``
    pins that neither lane checks it, so both still make their 1 call.

    Mutant (kind mix-up): any call site using the wrong ``AccessKind``
    constant (e.g. the wind lane checking ``"hourly"`` instead of
    ``"five_minute"``, or the current-obs lane never checking its own
    kind) shows up here as a held lane still making its call, or a
    not-held lane unexpectedly making 0 calls -- a combination none of
    CO1/FM1 (each exercising only its own lane) can catch.
    """
    now = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001

    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id, pws_id="KTEST001")
    _seed_poll_state(conn, station_id)
    enqueue_if_absent(
        conn,
        "fetch_current_obs",
        site_id,
        f"curobs:{station_id}",
        {"station_id": station_id},
    )
    conn.commit()

    record_access_refused(conn, held, 401)
    conn.commit()

    ensure_wind_days(conn, site_id, ZoneInfo("UTC"), now.date())
    conn.commit()

    # P2 4.4/14.7's AH1 rule (the existing "today's rows are not due while
    # five_minute is held" pin): wind_lane_due no longer reads the
    # five_minute hold at all for due-ness -- all_1day is always open, and
    # a held row's `AccessHeld` is caught per row inside the reservation
    # instead (AH1-AH4), not at this due-selector level. So today's
    # still-pending row (created by the ensure_wind_days call just above,
    # while the site has no observations yet, with next_attempt_at NULL --
    # due at once) is due regardless of which kind is held.
    assert wind_lane_due(conn, site_id, ZoneInfo("UTC"), now=now) is True, (
        "mutant -> at this assertion"
        f" (held={held}): correct = True (all_1day is always open under"
        " P2 4.4; a held kind no longer suppresses due-ness), mutant"
        " (wind_lane_due still gates all_1day on the five_minute hold,"
        " 0.16.13's behaviour) = False when held='five_minute'"
    )

    calls = {"current_obs": 0, "five_minute": 0}

    async def _fake_fetch_current_obs(
        pws_station_id: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> httpx.Response:
        calls["current_obs"] += 1
        return _online_response()

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_current_observation", _fake_fetch_current_obs
    )

    def wind_handler(request: httpx.Request) -> httpx.Response:
        calls["five_minute"] += 1
        return httpx.Response(200, json={"observations": []})

    async def run() -> None:
        # Run the wind chunk FIRST, mirroring production job order: today's
        # row already exists (the ensure_wind_days call above ran while the
        # site had no observations yet), so run_wind_days's own internal
        # ensure_wind_days call here is a no-op (`INSERT OR IGNORE`); the
        # current-obs call below running after it only matches how the real
        # worker orders its lanes.
        real_client = httpx.AsyncClient
        writer = FencedWriter(db, db.generation)
        with patch(
            "wxverify.worker.wind_days.httpx.AsyncClient",
            lambda *a, **kw: real_client(  # type: ignore[arg-type]
                transport=httpx.MockTransport(wind_handler)
            ),
        ):
            await run_wind_days(db, writer, site_id, client=None)

        job1 = await db.write(claim_next_current_obs_job)
        assert job1 is not None
        await run_claimed_job(db, job1, lane="current_obs")

    asyncio.run(run())

    for kind in ("current_obs", "five_minute"):
        expected = 0 if kind == held else 1
        assert calls[kind] == expected, (
            f"mutant -> at this assertion (kind={kind}): correct = "
            f"{expected} (only the held kind's lane makes no call), mutant"
            f" (a kind mix-up at one of the three call sites) = "
            f"{1 if kind == held else 0}"
        )

    for kind in ("current_obs", "hourly", "five_minute"):
        record = read_access_record(conn, kind)
        if kind == held:
            assert record is not None and record.status == 401, (
                "mutant -> at this assertion: correct = a stored 401"
                " record for the held kind, mutant (the refusal recorded"
                " under a different kind) = None"
            )
        else:
            assert record is None, (
                f"mutant -> at this assertion (kind={kind}): correct = None"
                " (a successful call never writes a hold for a kind that"
                " was not refused), mutant (the held kind's refusal"
                f" recorded under every kind) = a stored record for {kind}"
            )

    close_db()

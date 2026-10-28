"""The three refused-access kinds' lanes run independently (plan §13.5 FM4).

``current_obs``, ``hourly`` and ``five_minute`` (today's wind ``all_1day``
download) each hold only their own ``AccessKind``. ``test_access_hold_
current_obs.py``'s CO1 and ``test_access_hold_hourly.py``'s HD1 already
prove that a refusal of one kind leaves the *other two kinds' hold rows*
absent; what neither file (nor ``test_wind_auth_hold.py``) proves is the
new, genuinely cross-lane half of this release -- that the wind lane's
``five_minute`` hold does not block ``current_obs``/``hourly``'s real
calls, and that a ``current_obs``/``hourly`` hold does not block the wind
lane's real ``all_1day`` call. This file drives all three lanes against
one site+station in the same test and parametrizes over which kind is
held, so a copy-paste mix-up of the ``AccessKind`` constant at any of the
three call sites shows up as a miscounted call somewhere in the matrix,
not just as a wrong hold-row key.

Isolation: a single real tmp-path ``Database`` (``init_db``); the
current-obs job runs through ``run_claimed_job(db, job, lane=
"current_obs")`` with ``wxverify.worker.processor.fetch_current_observation``
monkeypatched; the hourly cycle runs through ``run_claimed_job(db, job,
lane="main")`` against an ``httpx.MockTransport`` patched onto
``wxverify.worker.processor.httpx.AsyncClient``; the wind chunk runs
through ``run_wind_days`` against an ``httpx.MockTransport`` patched onto
``wxverify.worker.wind_days.httpx.AsyncClient`` -- exactly the three
patterns ``test_access_hold_current_obs.py``, ``test_access_hold_hourly.py``
and ``test_wind_auth_hold.py`` each already use, combined onto one site.

Every station id below is synthetic: ``KTEST001``.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx
import pytest

from wxverify import config
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.queue import (
    claim_next_current_obs_job,
    claim_next_job,
    enqueue_if_absent,
)
from wxverify.worker.access_hold import (
    AccessKind,
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


@pytest.fixture(autouse=True)
def _fast_pace(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_pace(site_id: int, station_id: int, ordinal: int) -> None:
        return None

    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)


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


def _obs_response() -> httpx.Response:
    body = {
        "observations": [
            {"obsTimeUtc": "2026-07-10T11:55:00Z", "metric": {"temp": 20.0}}
        ]
    }
    return httpx.Response(
        200,
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: when)


@pytest.mark.parametrize("held", ["current_obs", "hourly", "five_minute"])
def test_fm4_each_kinds_hold_leaves_the_other_two_lanes_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, held: AccessKind
) -> None:
    """Hold exactly one of ``current_obs``/``hourly``/``five_minute``, then
    run one current-obs job, one hourly cycle and one wind chunk against
    200-answering fakes: the held kind's lane makes 0 calls and its row
    stays absent from the other two kinds' places; the other two each make
    exactly 1 call and their own rows stay absent.

    Mutant (kind mix-up): any call site using the wrong ``AccessKind``
    constant (e.g. the wind lane checking ``"hourly"`` instead of
    ``"five_minute"``, or the current-obs lane never checking its own
    kind) shows up here as a held lane still making its call, or a
    not-held lane unexpectedly making 0 calls -- a combination none of
    CO1/HD1/FM1 (each exercising only its own lane) can catch.
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
    enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})
    conn.commit()

    record_access_refused(conn, held, 401)
    conn.commit()

    ensure_wind_days(conn, site_id, ZoneInfo("UTC"), now.date())
    conn.commit()

    # wind_lane_due reads the same AccessKind the lane itself checks: only a
    # held `five_minute` makes today's still-pending row (created by the
    # ensure_wind_days call just above, while the site has no observations
    # yet) not due; a held `current_obs`/`hourly` leaves `all_1day` open and
    # the lane due again. No history_all hold exists and today's row is the
    # only station_wind_days row in this fixture, so neither of
    # wind_lane_due's other two branches (the history_all finish-check, the
    # missing-today-row check) can produce either literal below for an
    # unrelated reason.
    #
    # The named mutant (wind_days.py:374's `access_held(conn, "five_minute",
    # now)` swapped to read the `hourly` kind instead) diverges from correct
    # at the held="five_minute" and held="hourly" instances of this
    # parametrize; at held="current_obs" neither kind the mutant nor the
    # correct code reads is held, so both give True -- an equivalent
    # instance under this specific mutant, killed instead by its two
    # siblings below.
    expect_due = held != "five_minute"
    mutant_due = held != "hourly"
    assert wind_lane_due(conn, site_id, ZoneInfo("UTC"), now=now) is expect_due, (
        f"mutant -> at this assertion (held={held}): correct = {expect_due}"
        f", mutant (reads the `hourly` kind instead of `five_minute`) ="
        f" {mutant_due}"
    )

    calls = {"current_obs": 0, "hourly": 0, "five_minute": 0}

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

    def hourly_handler(request: httpx.Request) -> httpx.Response:
        calls["hourly"] += 1
        return _obs_response()

    def wind_handler(request: httpx.Request) -> httpx.Response:
        calls["five_minute"] += 1
        return httpx.Response(200, json={"observations": []})

    async def run() -> None:
        # Run the wind chunk FIRST, mirroring production job order: today's
        # row already exists (the ensure_wind_days call above ran while the
        # site had no observations yet), so run_wind_days's own internal
        # ensure_wind_days call here is a no-op (`INSERT OR IGNORE`); the
        # current-obs/hourly calls below running after it only matches how
        # the real worker orders its lanes.
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

        with patch(
            "wxverify.worker.processor.httpx.AsyncClient",
            lambda *a, **kw: real_client(  # type: ignore[arg-type]
                transport=httpx.MockTransport(hourly_handler)
            ),
        ):
            job2 = await db.write(claim_next_job)
            assert job2 is not None
            await run_claimed_job(db, job2, lane="main")

    asyncio.run(run())

    for kind in ("current_obs", "hourly", "five_minute"):
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

"""Current-observation poll: classification state machine + poll-state writes.

The hourly ``_fetch_obs`` stream (processor.py) feeds the scoring pipeline and
owns ``stations.last_error``/``error_count``/``last_run_at``. This module owns a
SEPARATE, independent per-station poll of ``/observations/current`` whose only
job is to learn each station's upload cadence and keep a last-good display
snapshot in ``station_current_obs``, with liveness tracked on
``station_poll_state``'s OWN diagnostic columns. The two streams never touch each
other's diagnostics — that separation is load-bearing (plan §5.9): the read route
surfaces the current-obs terminal reason, and the hourly stream's success reset
must not wipe it.

The classification ordering in ``classify_current_obs`` is load-bearing (§5.6):
429 is checked BEFORE the 4xx-terminal branch (a rate-limit must never flip a
station terminal), and the 204 / empty-body / empty-``observations`` OFFLINE check
runs BEFORE ``response.json()`` (a 204 is 2xx, so ``raise_for_status`` won't fire,
and parsing its empty body would raise and be misread as a transient error).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

import httpx

from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.obs import cadence
from wxverify.obs.pws_adapter import CurrentObservation, current_obs_from_payload

logger = logging.getLogger(__name__)

# The floor poll interval and cold-start learned interval, in seconds. Also the
# transient-retry backoff (retry at the floor, not a long freeze) (plan §5.6).
MIN_INTERVAL_SECONDS = 300
# The terminal / offline park interval: a dead or auth-rejected station is
# re-polled at most once a day (plan §5.6).
MAX_BACKOFF_SECONDS = 86400


class Health(Enum):
    """The four terminal states of a single current-obs poll (plan §5.6)."""

    ONLINE = "online"
    OFFLINE = "offline"
    TERMINAL = "terminal"
    TRANSIENT = "transient"


@dataclass(frozen=True)
class PollOutcome:
    """The classified result of one poll, pre-persistence.

    ``obs`` and ``obs_instant`` are populated only on ``ONLINE``; ``error`` carries
    the diagnostic string for the non-online states (``None`` on ONLINE).
    """

    health: Health
    obs: CurrentObservation | None = None
    obs_instant: str | None = None
    error: str | None = None


def classify_current_obs(response: httpx.Response) -> PollOutcome:
    """Classify a ``/observations/current`` response into a ``PollOutcome``.

    Ordering is load-bearing (plan §5.6) and must not be reordered:

    1. 429 → TRANSIENT (checked FIRST, before any 4xx-terminal branch).
    2. Other non-429 4xx (401/403/404/...) → TERMINAL.
    3. >=500 → TRANSIENT.
    4. 2xx that is 204 / empty body / empty ``observations`` → OFFLINE (checked
       BEFORE ``response.json()`` so a 204's empty body never raises here).
    5. 2xx non-empty payload whose obsTime fails to parse → TRANSIENT (retry at
       floor), NOT the OFFLINE freeze.
    6. otherwise → ONLINE with the parsed snapshot and full-precision instant.

    Transport-level failures (timeouts, connect errors) are handled by the caller,
    which never reaches this function with a response for those.
    """
    status = response.status_code
    if status == 429:
        return PollOutcome(Health.TRANSIENT, error=f"http {status}")
    if 400 <= status < 500:
        return PollOutcome(Health.TERMINAL, error=f"http {status}")
    if status >= 500:
        return PollOutcome(Health.TRANSIENT, error=f"http {status}")

    # 2xx. A 204 or an otherwise empty body must be classified OFFLINE BEFORE we
    # attempt to parse JSON: parsing an empty body raises JSONDecodeError, which
    # would wrongly re-poll a dead station as a transient error.
    if status == 204 or not response.content:
        return PollOutcome(Health.OFFLINE, error="empty body")
    obs = current_obs_from_payload(response.json())
    if obs is None:
        # 2xx with a body but no first observation row ⇒ station is present but
        # reporting nothing ⇒ OFFLINE (freeze), not a transient retry.
        return PollOutcome(Health.OFFLINE, error="no observations")
    if obs.obs_time_utc is None:
        # Live-but-unparseable timestamp ⇒ retry at the floor, do NOT freeze.
        return PollOutcome(Health.TRANSIENT, error="unparseable obstime")
    return PollOutcome(Health.ONLINE, obs=obs, obs_instant=obs.obs_time_utc)


def _load_poll_state(
    conn: sqlite3.Connection, station_id: int
) -> tuple[tuple[str, ...], str | None]:
    """Return the persisted ``(cadence_events, last_obstime)`` for a station.

    An unseeded row (LEFT JOIN due-immediately case) yields ``((), None)``.
    """
    row = conn.execute(
        "SELECT cadence_events, last_obstime FROM station_poll_state "
        "WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    if row is None:
        return (), None
    events_raw = row["cadence_events"]
    events: tuple[str, ...] = ()
    if isinstance(events_raw, str) and events_raw:
        try:
            parsed = json.loads(events_raw)
        except json.JSONDecodeError:
            parsed = []
        if isinstance(parsed, list):
            events = tuple(str(item) for item in parsed)  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    last_obstime = None if row["last_obstime"] is None else str(row["last_obstime"])
    return events, last_obstime


def _online_cadence(
    station_id: int,
    events: tuple[str, ...],
    last_obstime: str | None,
    obs_instant: str,
    now_ts: float,
) -> tuple[tuple[str, ...], int, int]:
    """Advance cadence learning for an ONLINE poll (plan §5.7).

    Appends ``obs_instant`` to the window only when it DIFFERS from the stored
    ``last_obstime`` (a repeat upload carries no new gap), truncates to the newest
    ``cadence.WINDOW_N``, recomputes ``base_interval``, and derives the next-poll
    delay. Returns ``(new_events, learned_interval, next_delay_seconds)``.

    ``cycle_bucket = int(now_ts // learned)``: a coarse wall-clock bucket.
    Successive polls of one station are ~``learned`` apart and so land in different
    buckets ⇒ successive jitter offsets differ; different ``station_id`` values vary
    independently via the blake2b hash key. ``learned`` is clamped
    >= ``MIN_INTERVAL_SECONDS`` (>=300) so ``now_ts // learned`` never divides by
    zero. The delay is ``learned + offset`` (offset is a SIGNED value in
    ``[-span, +span]``), never ``offset`` alone.
    """
    if obs_instant != last_obstime:
        events = (*events, obs_instant)[-cadence.WINDOW_N :]
    learned = cadence.base_interval(events, MIN_INTERVAL_SECONDS)
    cycle_bucket = int(now_ts // learned)
    offset = cadence.obs_cadence_jitter(station_id, cycle_bucket, learned)
    return events, learned, learned + offset


def persist_poll_result(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    outcome: PollOutcome,
) -> None:
    """Persist one poll's classification to the poll-state (and obs) tables.

    All writes are UPSERTs against ``station_poll_state`` (never a bare UPDATE — an
    unseeded row would no-op and leave ``next_poll_at`` NULL ⇒ a tight re-poll
    loop). ``station_current_obs`` is written ONLY on ONLINE (last-good retention
    on every other state). Diagnostics land on ``station_poll_state``'s own
    columns; ``stations`` is never touched here (plan §5.9).
    """
    # Station may have been deleted/disabled between enqueue and run.
    row = conn.execute(
        "SELECT 1 FROM stations WHERE id = ? AND site_id = ? AND enabled = 1",
        (station_id, site_id),
    ).fetchone()
    if row is None:
        return

    now = utc_now()
    now_iso = isoformat_utc(now)

    if outcome.health is Health.ONLINE:
        assert outcome.obs is not None and outcome.obs_instant is not None
        events, last_obstime = _load_poll_state(conn, station_id)
        new_events, learned, delay = _online_cadence(
            station_id, events, last_obstime, outcome.obs_instant, now.timestamp()
        )
        next_poll_at = isoformat_utc(
            now + timedelta(seconds=max(MIN_INTERVAL_SECONDS, delay))
        )
        _upsert_current_obs(conn, station_id, outcome.obs, now_iso)
        _upsert_poll_state_online(
            conn,
            station_id,
            events=new_events,
            last_obstime=outcome.obs_instant,
            learned_interval=learned,
            next_poll_at=next_poll_at,
            now_iso=now_iso,
        )
        return

    if outcome.health is Health.OFFLINE:
        health = "offline"
        next_poll_at = isoformat_utc(now + timedelta(seconds=MAX_BACKOFF_SECONDS))
    elif outcome.health is Health.TERMINAL:
        health = "terminal"
        next_poll_at = isoformat_utc(now + timedelta(seconds=MAX_BACKOFF_SECONDS))
    else:  # TRANSIENT
        health = "transient"
        next_poll_at = isoformat_utc(now + timedelta(seconds=MIN_INTERVAL_SECONDS))

    _upsert_poll_state_failure(
        conn,
        station_id,
        health=health,
        next_poll_at=next_poll_at,
        error=outcome.error,
        now_iso=now_iso,
    )


def _upsert_current_obs(
    conn: sqlite3.Connection,
    station_id: int,
    obs: CurrentObservation,
    now_iso: str,
) -> None:
    conn.execute(
        """
        INSERT INTO station_current_obs (
            station_id, obs_time_utc, temp, humidity, dewpt,
            wind_speed, wind_gust, wind_dir, pressure,
            precip_rate, precip_total, uv, neighborhood, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(station_id) DO UPDATE SET
            obs_time_utc=excluded.obs_time_utc,
            temp=excluded.temp,
            humidity=excluded.humidity,
            dewpt=excluded.dewpt,
            wind_speed=excluded.wind_speed,
            wind_gust=excluded.wind_gust,
            wind_dir=excluded.wind_dir,
            pressure=excluded.pressure,
            precip_rate=excluded.precip_rate,
            precip_total=excluded.precip_total,
            uv=excluded.uv,
            neighborhood=excluded.neighborhood,
            fetched_at=excluded.fetched_at
        """,
        (
            station_id,
            obs.obs_time_utc,
            obs.temp,
            obs.humidity,
            obs.dewpt,
            obs.wind_speed,
            obs.wind_gust,
            obs.wind_dir,
            obs.pressure,
            obs.precip_rate,
            obs.precip_total,
            obs.uv,
            obs.neighborhood,
            now_iso,
        ),
    )


def _upsert_poll_state_online(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    events: tuple[str, ...],
    last_obstime: str,
    learned_interval: int,
    next_poll_at: str,
    now_iso: str,
) -> None:
    conn.execute(
        """
        INSERT INTO station_poll_state (
            station_id, cadence_events, last_obstime, learned_interval_seconds,
            health_state, next_poll_at, last_poll_at, last_error, error_count,
            updated_at
        )
        VALUES (?, ?, ?, ?, 'online', ?, ?, NULL, 0, ?)
        ON CONFLICT(station_id) DO UPDATE SET
            cadence_events=excluded.cadence_events,
            last_obstime=excluded.last_obstime,
            learned_interval_seconds=excluded.learned_interval_seconds,
            health_state='online',
            next_poll_at=excluded.next_poll_at,
            last_poll_at=excluded.last_poll_at,
            last_error=NULL,
            error_count=0,
            updated_at=excluded.updated_at
        """,
        (
            station_id,
            json.dumps(list(events), separators=(",", ":")),
            last_obstime,
            learned_interval,
            next_poll_at,
            now_iso,
            now_iso,
        ),
    )


def _upsert_poll_state_failure(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    health: str,
    next_poll_at: str,
    error: str | None,
    now_iso: str,
) -> None:
    """Persist a non-online poll: cadence window is FROZEN (never appended here).

    ``cadence_events`` / ``last_obstime`` / ``learned_interval_seconds`` are left
    untouched on the existing row (retain last-good learning), and default on an
    unseeded row. ``error_count`` increments on the row's own diagnostic column.
    """
    conn.execute(
        """
        INSERT INTO station_poll_state (
            station_id, health_state, next_poll_at, last_poll_at,
            last_error, error_count, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(station_id) DO UPDATE SET
            health_state=excluded.health_state,
            next_poll_at=excluded.next_poll_at,
            last_poll_at=excluded.last_poll_at,
            last_error=excluded.last_error,
            error_count=station_poll_state.error_count + 1,
            updated_at=excluded.updated_at
        """,
        (station_id, health, next_poll_at, now_iso, error, now_iso),
    )

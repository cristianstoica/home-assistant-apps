# pyright: strict
"""The adaptive per-station polling scheduler and its interruptible main loop.

`poll_station` performs **one** full poll for a station through the injected
seams (so the ``--check`` oracle drives it deterministically) and mutates that
station's in-memory state — returning the seconds until its next poll. `run_loop`
wraps the per-station polls in a single stop-aware timer loop. Every dependency
is an explicit constructor argument (no module globals).

Key invariants (from the plan):

* **Cold start is slow.** Every station starts at the slow/holding cadence
  (``initial_backoff_seconds``) with **no** immediate fast first poll — a healthy,
  positively-confirmed first poll *earns* the fast 300-400s cadence. State is
  in-memory only, so a restart is a cold start: a crash loop cannot re-hammer
  stations that were correctly held slow, while a genuinely healthy station
  re-earns its fast cadence on its first confirmed poll.
* **Reward split.** A *positively-confirmed* poll (primary ``last_reported`` or a
  fallback timestamp advanced) resets ``current_backoff`` to
  ``initial_backoff_seconds`` and schedules the next poll at a random
  ``[healthy_interval_min, healthy_interval_max]``. An *inconclusive-fallback
  accept* is healthy but holds the prior/slow cadence — never the fast reward.
* **Backoff split.** A *transient* unhealthy poll doubles ``current_backoff``
  (first retry ``initial_backoff_seconds * 2``), capped at ``max_backoff_seconds``.
  A *terminal* fault holds at ``max_backoff_seconds`` (no doubling).
* **Interruptible everywhere.** The settle wait and the freshness re-read waits
  all run through the single stop-aware sleeper, so a SIGTERM mid-wait returns
  promptly and starts no further read/poll.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime

from .config import Config
from .errors import TerminalError, TransientError
from .haapi import HaApiClient
from .health import evaluate
from .models import Clock, HealthStatus, Sleeper, Station, WallClock

_log = logging.getLogger("pyweather")

# Bounded best-effort freshness settle: after the initial settle wait and the
# first /states read, re-read up to this many more times if freshness has not
# advanced. Each re-read is preceded by a wait through the single sleeper, capped
# by an explicit total deadline (settle + MAX_FRESHNESS_REREADS * reread_interval).
MAX_FRESHNESS_REREADS = 2


class StationState:
    """Mutable per-station scheduling state (in-memory only; no persistence).

    `current_backoff` is the live backoff value the doubling sequence mutates and
    a confirmed poll resets to ``initial_backoff_seconds`` — pinning the reset
    proves a healthy poll actually mutates stored backoff, not a transient value.
    """

    def __init__(self, station: Station, initial_backoff: int) -> None:
        self.station = station
        self.current_backoff = initial_backoff


class Scheduler:
    """The adaptive polling driver across all configured stations."""

    def __init__(
        self,
        config: Config,
        *,
        api: HaApiClient,
        clock: Clock,
        wall_clock: WallClock,
        sleeper: Sleeper,
        rng: random.Random,
    ) -> None:
        self._config = config
        self._api = api
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleeper = sleeper
        self._rng = rng
        self._states = {
            station.key: StationState(station, config.initial_backoff_seconds)
            for station in config.stations
        }

    def state_for(self, key: str) -> StationState:
        """Return the mutable `StationState` for `key` (oracle introspection)."""
        return self._states[key]

    def _healthy_interval(self) -> int:
        """A random healthy cadence in ``[healthy_interval_min, healthy_interval_max]``."""
        return self._rng.randint(
            self._config.healthy_interval_min, self._config.healthy_interval_max
        )

    def _double_backoff(self, state: StationState) -> int:
        """Double `current_backoff` (capped at ``max_backoff_seconds``); store + return.

        On the first unhealthy poll after a reset, ``current_backoff`` is the
        floor ``initial_backoff_seconds``, so the doubled value is the
        first-retry ``initial_backoff_seconds * 2``. Doubling compounds across
        sequential unhealthy polls until capped.
        """
        doubled = min(state.current_backoff * 2, self._config.max_backoff_seconds)
        state.current_backoff = doubled
        return doubled

    def _read_states_with_freshness(
        self, station: Station, t0: datetime
    ) -> HealthStatus:
        """Settle, GET /states, evaluate; re-read up to the bound if not yet fresh.

        Returns the final `HealthStatus` (never TERMINAL — that propagates as a
        `TerminalError` from the API client). Each inter-read wait runs through
        the single stop-aware sleeper; a stop mid-wait aborts and is reported as
        UNHEALTHY (no further read), so the loop exits promptly without rewarding.
        """
        # Initial settle window before the first read.
        if self._sleeper(float(self._config.settle_seconds)):
            return HealthStatus.UNHEALTHY

        result = evaluate(station, self._api.get_states(), t0)
        if result.discovered < station.expected_sensors:
            _log.info(
                "%s: discovered %d sensors < expected %d (optional shortfall; non-fatal)",
                station.key,
                result.discovered,
                station.expected_sensors,
            )
        rereads = 0
        # Re-read only while the primary signal has not advanced (UNHEALTHY): the
        # inconclusive fallback can never advance by re-reading an unchanged value,
        # so re-reading there is pointless. Bounded by MAX_FRESHNESS_REREADS and
        # the implicit total deadline (settle + n * reread_interval).
        while (
            result.status is HealthStatus.UNHEALTHY and rereads < MAX_FRESHNESS_REREADS
        ):
            rereads += 1
            if self._sleeper(float(self._config.settle_seconds)):
                return HealthStatus.UNHEALTHY
            result = evaluate(station, self._api.get_states(), t0)

        _log.debug(
            "%s: poll outcome %s after %d re-read(s): %s",
            station.key,
            result.status.value,
            rereads,
            result.detail,
        )
        return result.status

    def poll_station(self, key: str) -> int:
        """Run one full poll for `key`, mutate its state, return seconds-to-next-poll.

        Sequence: capture ``t0`` (pre-POST tz-aware UTC), POST ``update_entity``,
        settle + read + freshness re-reads, evaluate, then apply the reward/backoff
        split. A terminal fault holds at ``max_backoff_seconds`` without doubling;
        a transient fault doubles; a confirmed poll resets-and-rewards; an
        inconclusive accept holds the prior interval (never the fast reward).
        """
        state = self._states[key]
        station = state.station
        t0 = self._wall_clock.now()
        try:
            self._api.update_entity(station.update_entity)
            status = self._read_states_with_freshness(station, t0)
        except TerminalError as exc:
            _log.error(
                "%s: terminal fault; holding at max_backoff (%ds): %s",
                station.key,
                self._config.max_backoff_seconds,
                exc,
            )
            return self._config.max_backoff_seconds
        except TransientError as exc:
            _log.warning(
                "%s: transient poll failure; backing off: %s", station.key, exc
            )
            return self._double_backoff(state)

        if status is HealthStatus.CONFIRMED:
            state.current_backoff = self._config.initial_backoff_seconds
            interval = self._healthy_interval()
            _log.info(
                "%s: healthy (confirmed); next poll in %ds", station.key, interval
            )
            return interval
        if status is HealthStatus.INCONCLUSIVE:
            # Healthy but not positively confirmed: hold the prior/slow cadence
            # (the current backoff value), NOT the fast reward and NOT a reset.
            hold = state.current_backoff
            _log.info(
                "%s: healthy (inconclusive accept); holding at %ds (no fast reward)",
                station.key,
                hold,
            )
            return hold
        # UNHEALTHY (missing/unusable core, failed primary freshness, or a stop
        # mid-settle): transient backoff.
        _log.warning("%s: unhealthy; backing off", station.key)
        return self._double_backoff(state)

    def run_loop(self) -> None:
        """Run the adaptive loop across all stations until stop is signalled.

        Cold start: every station begins at the slow ``initial_backoff_seconds``
        holding cadence (no fast first poll). First polls are staggered by
        ``startup_stagger_seconds`` to avoid an 8-request burst. Each station's
        next-poll time is tracked in monotonic seconds; the loop sleeps to the
        nearest due station through the single stop-aware sleeper.
        """
        now = self._clock()
        # Cold start: stagger first polls, each held at the slow initial cadence
        # until a healthy confirmed poll earns the fast cadence.
        next_poll: dict[str, float] = {}
        for index, station in enumerate(self._config.stations):
            next_poll[station.key] = now + index * self._config.startup_stagger_seconds
        _log.info(
            "py-weather starting: %d station(s); cold-start cadence %ds",
            len(self._config.stations),
            self._config.initial_backoff_seconds,
        )
        for station in self._config.stations:
            _log.info("registered station %s -> %s", station.key, station.update_entity)

        while True:
            now = self._clock()
            due_key = min(next_poll, key=lambda k: next_poll[k])
            wait = max(0.0, next_poll[due_key] - now)
            if self._sleeper(wait):
                _log.info("stop signalled; exiting scheduler loop")
                return
            delay = self.poll_station(due_key)
            next_poll[due_key] = self._clock() + float(delay)

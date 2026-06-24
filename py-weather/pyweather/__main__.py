# pyright: strict
"""CLI entrypoint: the live adaptive scheduler + the self-validating ``--check``.

Modes:
  * (default)   — load options, wire the production seams + signals, and run the
                  adaptive per-station polling loop until SIGTERM/SIGINT.
  * ``--check`` — run the offline self-validation oracle (config rejection,
                  entity-id-shape + station-key contract, discovery transform,
                  HA request shaping, binary obstime-presence health, the
                  cadence estimator (clamp + ±15% jitter + stale advisory), the
                  four scheduling rests, the terminal/transient classification
                  incl. 429-on-update_entity precedence, ``/data`` state
                  persistence, and stop-during-sleep interruptibility). All
                  seams are faked — no network, no real sockets/threads. Exit 0
                  only on all-pass.

Diagnostics (startup, station registration, per-poll outcomes, warnings) go to
**stderr** for the HA Log tab; ``--check`` writes its PASS/FAIL report to stderr.
The oracle itself lives in the `check` package; this module keeps only the
live/CLI surface.

The ``SUPERVISOR_TOKEN`` bearer is read from the environment (injected by the
Supervisor when ``homeassistant_api: true``); a missing/blank token fails fast
with a clear message rather than spinning doomed unauthenticated polls.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
from collections.abc import Callable
from types import FrameType
from typing import TYPE_CHECKING, NamedTuple

from . import __version__, config
from .config import ConfigError
from .errors import TerminalError, TransientError
from .models import Config, Sleeper, Station

if TYPE_CHECKING:
    from .haapi import HaApiClient
    from .scheduler import SchedulerRunner
    from .supervisor import SupervisorOptions


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyweather",
        description=(
            "Adaptive Weather.com PWS poller (HA add-on py-weather). Default mode "
            "runs the per-station polling loop; --check self-validates offline."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"py-weather {__version__}"
    )
    parser.add_argument(
        "--options",
        metavar="PATH",
        default=config.DEFAULT_OPTIONS_PATH,
        help=(
            "path to options.json (default /data/options.json). Use a local file "
            "to run --check off-HAOS."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="run the offline self-validation oracle; exit non-zero on mismatch",
    )
    return parser


def configure_logging(level: str) -> None:
    """Send all diagnostics to **stderr** (the HA Log tab stream)."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


# A small retry cap covering a realistic host-reboot boot lag (Core's REST
# sensors may not be loaded the instant the add-on first scans /states).
# A small fixed retry cap (no config surface); worst-case startup wait ≈
# (MAX_DISCOVERY_ATTEMPTS - 1) * settle_seconds before exit.
MAX_DISCOVERY_ATTEMPTS = 5


def _discover_and_persist(
    api: "HaApiClient",
    supervisor: "SupervisorOptions",
    sleeper: Sleeper,
    cfg: Config,
    *,
    log: logging.Logger,
) -> list[Station] | None:
    """Discover stations from /states (bounded retry), then best-effort persist.

    Returns a NON-EMPTY `list[Station]` on success (proceed to `Scheduler`),
    `None` on a SIGTERM/SIGINT during an inter-attempt wait (clean shutdown: no
    persist, no `Scheduler`), and raises `SystemExit(1)` on a terminal fault or
    cap exhaustion (never returns to the caller's `Scheduler` line). The three
    exhaustion variants log three DISTINCT ERROR messages so a transient-only
    window, a mixed window, and a genuine empty fleet are not conflated.

    Every seam is a parameter (no client/sleeper constructed here), so the
    `--check` oracle drives this against `FakeHttp`/`FakeSleeper`/a faked
    `SupervisorSelfClient` with no network and no real signals.
    """
    from .discovery import (
        discover_stations,
        merge_station_counts,
        render_stations_block,
    )
    from .supervisor import to_options_dict

    attempt = 0
    valid_zero_scans = 0
    transient_count = 0
    last_transient: TransientError | None = None
    stations: list[Station] = []

    while True:
        # Pre-read stop gate: a stop already signalled at entry (between handler
        # install and the first read) returns None with ZERO get_states calls.
        # Scoped to attempt 0 so it does not prepend a zero-length wait to the
        # inter-attempt sleeper record on later iterations.
        if attempt == 0 and sleeper(0):
            return None
        try:
            states = api.get_states()
        except TerminalError as exc:
            log.error("discovery: terminal fault reading /states (no retry): %s", exc)
            raise SystemExit(1) from None
        except TransientError as exc:
            transient_count += 1
            last_transient = exc
        else:
            result = discover_stations(states)
            for skipped in result.skipped_entity_ids:
                log.warning(
                    "discovery: excluding %s (its id suffix is not lowercase-alphanumeric, so it "
                    "cannot be an auto-populated or manually-added station; rename the underlying "
                    "sensor to sensor.wu_obstimeutc_<lowercase-alphanumeric> if it should be polled)",
                    skipped,
                )
            stations = result.stations
            if stations:
                # Partial-Core count-stability: ONE confirmation re-read, then take
                # the per-key MAX expected_sensors so a still-loading sibling set is
                # not snapshotted short. Rides the SAME stop-aware sleeper.
                if sleeper(float(cfg.settle_seconds)):
                    return None
                try:
                    confirm = discover_stations(api.get_states())
                except TerminalError as exc:
                    log.error(
                        "discovery: terminal fault on the confirmation read: %s", exc
                    )
                    raise SystemExit(1) from None
                except TransientError:
                    # Best-effort: a confirmation blip must never demote a
                    # successful discovery. Keep the first-read stations as-is.
                    pass
                else:
                    # A non-conforming representative may surface only on the
                    # confirmation read (its obstimeutc arrived late); log it at
                    # WARNING too, so the skipped-id contract covers both reads (a
                    # confirm-only exclusion is otherwise silently dropped).
                    for skipped in confirm.skipped_entity_ids:
                        log.warning(
                            "discovery: excluding %s (its id suffix is not lowercase-alphanumeric, so it "
                            "cannot be an auto-populated or manually-added station; rename the underlying "
                            "sensor to sensor.wu_obstimeutc_<lowercase-alphanumeric> if it should be polled)",
                            skipped,
                        )
                    stations = merge_station_counts(stations, confirm.stations)
                break
            valid_zero_scans += 1
        attempt += 1
        if attempt >= MAX_DISCOVERY_ATTEMPTS:
            if valid_zero_scans >= 1 and transient_count == 0:
                log.error(
                    "discovery: no sensor.wu_obstimeutc_* entities found; check rest.yaml "
                    "or define stations manually"
                )
            elif valid_zero_scans == 0:
                log.error(
                    "discovery: could not read /states after %d attempts (%s); "
                    "the HA API was unreachable, not necessarily empty",
                    MAX_DISCOVERY_ATTEMPTS,
                    last_transient,
                )
            else:
                log.error(
                    "discovery: no sensor.wu_obstimeutc_* entities found in the scans that "
                    "succeeded, but %d API error(s) also occurred (%s); not a "
                    "confirmed empty fleet — check both rest.yaml and the HA API",
                    transient_count,
                    last_transient,
                )
            raise SystemExit(1)
        if sleeper(float(cfg.settle_seconds)):
            return None

    # Best-effort persist: never allowed to prevent the add-on running this
    # session off the in-memory discovered list.
    try:
        supervisor.set_options(to_options_dict(cfg, stations))
        log.info(
            "discovery: persisted %d station(s) into the add-on options",
            len(stations),
        )
    except Exception as exc:  # noqa: BLE001 - persist is best-effort, never fatal
        log.warning(
            "discovery: could not persist the discovered stations (%s); paste this "
            "into the Configuration tab manually:\n%s",
            exc,
            render_stations_block(stations),
        )
    return stations


class StartupDeps(NamedTuple):
    """Factory seams `run_startup` constructs the runtime from.

    Production supplies the real `HaApiClient`/`SupervisorSelfClient`/
    `EventSleeper` constructors, the real `Scheduler`, and the real
    `signal.signal` handler install; the `--check` oracle supplies recording
    fakes so both branches are oracle-observable through one entry point.
    `make_scheduler` takes the (possibly station-resolved) `cfg` and the SAME
    sleeper instance the discovery path used.
    """

    make_api: Callable[[], "HaApiClient"]
    make_supervisor: Callable[[], "SupervisorOptions"]
    make_sleeper: Callable[[], Sleeper]
    make_scheduler: Callable[[Config, Sleeper], "SchedulerRunner"]
    install_signals: Callable[[Sleeper], None]


def run_startup(cfg: Config, deps: StartupDeps, *, log: logging.Logger) -> int:
    """Decide the empty-vs-non-empty path and wire the runtime; return the exit code.

    Non-empty `cfg.stations` (manual, unchanged behavior): build the sleeper,
    install signals, build the `Scheduler` DIRECTLY from `cfg`, run it, return 0 —
    NO /states scan, NO set_options. Empty `cfg.stations` (auto-populate): build
    the api/supervisor/sleeper seams, install signals BEFORE the first discovery
    read, run `_discover_and_persist`; a `None` return is a clean shutdown (no
    `Scheduler`); a fatal outcome has already raised `SystemExit(1)`; otherwise
    build the `Scheduler` from `cfg._replace(stations=...)` and run it.
    """
    if cfg.stations:
        sleeper = deps.make_sleeper()
        deps.install_signals(sleeper)
        deps.make_scheduler(cfg, sleeper).run_loop()
        return 0

    api = deps.make_api()
    supervisor = deps.make_supervisor()
    sleeper = deps.make_sleeper()
    deps.install_signals(sleeper)  # BEFORE the first discovery /states read
    stations = _discover_and_persist(api, supervisor, sleeper, cfg, log=log)
    if stations is None:
        return 0  # clean shutdown: no persist, no Scheduler
    deps.make_scheduler(cfg._replace(stations=tuple(stations)), sleeper).run_loop()
    return 0


def _run_loop(options_path: str) -> int:
    """Load options, wire the production seams + signals, and run until stop."""
    try:
        cfg = config.load(options_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    configure_logging(cfg.log_level)

    token = os.environ.get("SUPERVISOR_TOKEN", "").strip()
    if token == "":
        print(
            "config error: SUPERVISOR_TOKEN is not set "
            "(the add-on needs homeassistant_api: true)",
            file=sys.stderr,
        )
        return 1

    from .haapi import HaApiClient
    from .httpclient import UrllibHttpClient
    from .runtime import EventSleeper, SystemWallClock, UniformJitter, monotonic
    from .scheduler import Scheduler
    from .state import DEFAULT_STATE_PATH, load_state, save_state
    from .supervisor import SupervisorSelfClient

    http = UrllibHttpClient(secrets=(token,))
    timeout = float(cfg.request_timeout_seconds)
    rng = random.Random()
    boot_state = load_state(DEFAULT_STATE_PATH)

    def _make_sleeper() -> EventSleeper:
        return EventSleeper()

    def _install_signals(sleeper: Sleeper) -> None:
        # `sleeper` is the production EventSleeper; bind its request_stop to the
        # signal handlers. The handler install happens BEFORE the first discovery
        # /states read on the empty path (run_startup installs signals first).
        def _handle(_signum: int, _frame: FrameType | None) -> None:
            if isinstance(sleeper, EventSleeper):
                sleeper.request_stop()

        signal.signal(signal.SIGTERM, _handle)
        signal.signal(signal.SIGINT, _handle)

    def _make_scheduler(resolved: Config, sleeper: Sleeper) -> Scheduler:
        return Scheduler(
            resolved,
            api=HaApiClient(http, token, timeout),
            clock=monotonic,
            wall_clock=SystemWallClock(),
            sleeper=sleeper,
            jitter=UniformJitter(rng),
            state=boot_state,
            save=lambda s: save_state(DEFAULT_STATE_PATH, s),
        )

    deps = StartupDeps(
        make_api=lambda: HaApiClient(http, token, timeout),
        make_supervisor=lambda: SupervisorSelfClient(http, token, timeout),
        make_sleeper=_make_sleeper,
        make_scheduler=_make_scheduler,
        install_signals=_install_signals,
    )
    return run_startup(cfg, deps, log=logging.getLogger("pyweather"))


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the requested mode."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.check:
        from .check import run_check

        configure_logging("info")
        return run_check()
    return _run_loop(args.options)


if __name__ == "__main__":
    sys.exit(main())

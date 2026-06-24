# pyright: strict
"""Adaptive-scheduler checks: the three-rest table, cadence-event learning, and
the interruptible waits.

Every case drives the **real** `Scheduler.poll_station` (and, for the loop/stop
cases, `run_loop`) against recording fakes for the HTTP, clock, wall-clock,
sleeper, jitter, and save seams. The assertions read the returned
seconds-to-next-poll, the mutated `StationState.cadence` window, and the recorded
`FakeSleeper.slept` / `FakeHttp.calls` / `FakeSave.saves`, so each oracle fails on
a broken impl rather than on a pre-set expectation.

Scheduling model under test — the rests `poll_station` can return:

* ONLINE  → the learned (jittered) cadence interval.
* OFFLINE → ``cadence.OFFLINE_REPROBE`` (86400).
* TERMINAL → ``max_backoff_seconds`` (86400, via the terminal path).
* TransientError / interrupted-settle → ``min_interval_seconds`` (300).
"""

from __future__ import annotations

from datetime import datetime

from .. import fixtures
from ..cadence import OFFLINE_REPROBE
from ..config import validate
from ..haapi import HaApiClient
from ..httpclient import HttpError
from ..models import Config, StationCadence
from ..scheduler import Scheduler
from .fakes import (
    FakeClock,
    FakeHttp,
    FakeJitter,
    FakeSave,
    FakeSleeper,
    FakeWallClock,
    ok_response,
    states_response,
)
from .report import report

_T0 = datetime.fromisoformat(fixtures.T0_ISO)


def _config(**overrides: object) -> Config:
    """A validated single-station (istation01) `Config`, with optional overrides."""
    opts = fixtures.default_options(
        stations=[
            {
                "key": "istation01",
                "update_entity": "sensor.wu_temp_istation01",
                "expected_sensors": 10,
            }
        ],
        **overrides,
    )
    return validate(opts)


def _scheduler(
    http: FakeHttp,
    *,
    config: Config | None = None,
    sleeper: FakeSleeper | None = None,
    clock: FakeClock | None = None,
    state: dict[str, StationCadence] | None = None,
    jitter: FakeJitter | None = None,
    save: FakeSave | None = None,
) -> Scheduler:
    """Wire a `Scheduler` over the real `HaApiClient` and the given fakes.

    The cadence-model seams (`state`/`jitter`/`save`) are overridable keyword
    params with sensible defaults — a check that does not care about a seam takes
    the default (empty boot state, identity jitter, a throwaway recording save),
    and a check that does overrides exactly that one.
    """
    cfg = config or _config()
    api = HaApiClient(http, fixtures.EXAMPLE_TOKEN, float(cfg.request_timeout_seconds))
    return Scheduler(
        cfg,
        api=api,
        clock=clock or FakeClock(),
        wall_clock=FakeWallClock(_T0),
        sleeper=sleeper or FakeSleeper(),
        jitter=jitter or FakeJitter(),
        state=state or {},
        save=save or FakeSave(),
    )


# The update_entity POST always succeeds in these health-focused cases; the GET
# /states response is what each case scripts. A FakeHttp returns its scripted
# responses by call order, and poll_station issues POST then a single GET.
def _post_then_states(*states_bodies: list[dict[str, object]]) -> FakeHttp:
    """A FakeHttp: a 2xx POST, then one `states_response` per scripted body."""
    responses = [ok_response("")]
    responses += [states_response(b) for b in states_bodies]
    return FakeHttp(*responses)


def check_three_rest_table() -> bool:
    """ONLINE ⇒ learned interval; OFFLINE ⇒ OFFLINE_REPROBE (86400);
    TERMINAL ⇒ max_backoff (86400, but via the terminal path, not OFFLINE)."""
    checks: list[tuple[str, bool]] = []
    # ONLINE with a cold-start window ⇒ MIN (300) under identity jitter.
    online = fixtures.obstime_states("istation01", obstime=fixtures.OBSTIME_T0)
    http = _post_then_states(online)
    sched = _scheduler(http)  # state={} ⇒ cold start
    checks.append(
        ("ONLINE cold-start ⇒ MIN (300)", sched.poll_station("istation01") == 300)
    )
    # OFFLINE ⇒ OFFLINE_REPROBE.
    offline = fixtures.obstime_states("istation01")  # obstime sensor absent
    http2 = _post_then_states(offline)
    sched2 = _scheduler(http2)
    checks.append(
        (
            "OFFLINE ⇒ OFFLINE_REPROBE (86400)",
            sched2.poll_station("istation01") == OFFLINE_REPROBE,
        )
    )
    checks.append(
        (
            "OFFLINE_REPROBE constant is 86400 (once-a-day reprobe contract)",
            OFFLINE_REPROBE == 86400,
        )
    )
    return report("THREE-REST", "three-rest", checks)


def check_cadence_event_recording() -> bool:
    """An obstime CHANGE appends one event; an UNCHANGED obstime appends none.
    Drives two successive polls against the real Scheduler + FakeSave."""
    # poll 1: obstime A ; poll 2: obstime B (changed) ⇒ window has both.
    a = fixtures.obstime_states("istation01", obstime="2026-06-23T19:00:00Z")
    b = fixtures.obstime_states("istation01", obstime="2026-06-23T19:15:00Z")
    http = FakeHttp(
        ok_response(""), states_response(a), ok_response(""), states_response(b)
    )
    save = FakeSave()
    sched = _scheduler(http, save=save)
    sched.poll_station("istation01")
    sched.poll_station("istation01")
    events = sched.state_for("istation01").cadence.events
    checks = [
        (
            "changed obstime across two polls ⇒ 2 events",
            events == ("2026-06-23T19:00:00Z", "2026-06-23T19:15:00Z"),
        ),
        ("each poll cycle issues exactly one debounced save", len(save.saves) == 2),
    ]
    # unchanged twin: same obstime twice ⇒ 1 event only.
    same = fixtures.obstime_states("istation01", obstime="2026-06-23T19:00:00Z")
    http2 = FakeHttp(
        ok_response(""), states_response(same), ok_response(""), states_response(same)
    )
    sched2 = _scheduler(http2, save=FakeSave())
    sched2.poll_station("istation01")
    sched2.poll_station("istation01")
    checks.append(
        (
            "unchanged obstime across two polls ⇒ 1 event (no duplicate)",
            sched2.state_for("istation01").cadence.events == ("2026-06-23T19:00:00Z",),
        )
    )
    return report("CADENCE-EVENT", "cadence-event", checks)


def check_boot_state_resumes() -> bool:
    """A persisted /data window resumes the learned cadence (no cold start), routes
    through the jitter seam, dedupes the boot tail, and prunes unknown ghost keys.

    The boot window is six obstimes at 900s gaps ⇒ median 900 ⇒ base
    ``clamp(900*0.8)`` = 720. Under ``FakeJitter(0.85)`` the ONLINE poll schedules
    ``round(720 * 0.85) = 612`` — proving (a) boot state skips cold start (else
    300), (b) the estimator derives 720 from the window, and (c) the shell routes
    the interval through ``cadence.jittered_interval`` / ``self._jitter`` (else a
    shell calling ``base_interval`` directly returns 720). The read obstime is
    scripted to the persisted window's tail, so the boot-seeded ``last_obstime``
    dedupes it (no append) and the window — and thus 612 — stays well-defined.
    """
    boot_events = fixtures.obstime_series(900, 6)
    tail = boot_events[-1]
    http = FakeHttp(
        ok_response(""),
        states_response(fixtures.obstime_states("istation01", obstime=tail)),
    )
    sched = _scheduler(
        http,
        state={"istation01": StationCadence(events=boot_events)},
        jitter=FakeJitter(0.85),
    )
    delay = sched.poll_station("istation01")
    checks: list[tuple[str, bool]] = [
        (
            "boot window (900s gaps) + FakeJitter(0.85) ⇒ ONLINE poll returns 612",
            delay == 612,
        ),
        ("boot window resumes (not cold-start 300)", delay != 300),
        ("interval is jittered through the seam (not bare base 720)", delay != 720),
    ]

    # persisted-tail dedupe: an ONLINE poll whose obstime == the persisted tail
    # must NOT append (the boot-seeded last_obstime recognises it), so the window
    # length is unchanged and no spurious 0.0s gap enters the median.
    http_dup = FakeHttp(
        ok_response(""),
        states_response(fixtures.obstime_states("istation01", obstime=tail)),
    )
    sched_dup = _scheduler(
        http_dup,
        state={"istation01": StationCadence(events=boot_events)},
    )
    sched_dup.poll_station("istation01")
    checks.append(
        (
            "ONLINE poll whose obstime == persisted tail ⇒ no duplicate appended",
            sched_dup.state_for("istation01").cadence.events == boot_events,
        )
    )

    # unknown-key ("ghost") prune-on-save: a loaded state whose only key is an
    # unknown "ghost" must not seed the configured istation01 (it cold-starts) and
    # must never be persisted (the save key set equals the configured set).
    ghost_save = FakeSave()
    sched_ghost = _scheduler(
        FakeHttp(
            ok_response(""),
            states_response(
                fixtures.obstime_states("istation01", obstime=fixtures.OBSTIME_T0)
            ),
        ),
        state={"ghost": StationCadence(events=fixtures.obstime_series(900, 6))},
        jitter=FakeJitter(),  # identity ⇒ cold-start MIN passes through as 300
        save=ghost_save,
    )
    cold = sched_ghost.poll_station("istation01")
    checks.append(
        (
            "unknown 'ghost' boot key ⇒ configured istation01 still cold-starts at MIN 300 (ghost series not seeded into it)",
            cold == 300,
        )
    )
    checks.append(
        (
            "unknown 'ghost' boot key is pruned on first save (save key set == {istation01})",
            set(ghost_save.saves[-1]) == {"istation01"},
        )
    )
    return report("BOOT-RESUME", "boot", checks)


def _terminal_http(status: int, *, on_get: bool = False) -> FakeHttp:
    """A FakeHttp whose POST (or GET, if `on_get`) raises an `HttpError(status)`.

    For ``on_get=False`` the POST raises; for ``on_get=True`` the POST succeeds
    (2xx) and the GET /states raises — so a GET-path terminal is exercised after a
    successful update_entity.
    """
    if on_get:
        return FakeHttp(ok_response(""), HttpError(f"http {status}", status=status))
    return FakeHttp(HttpError(f"http {status}", status=status))


def check_terminal_path() -> bool:
    """A terminal API fault holds at ``max_backoff_seconds`` (86400).

    401/403 on update_entity, a non-429 4xx on update_entity (404/422), and a
    non-429 4xx on GET /states all return ``max_backoff_seconds``. Negative
    discriminator: the returned value is the full max (86400), never the (much
    smaller) transient floor or a 0/now — so a terminal misrouted to the
    transient branch would fail this.
    """
    checks: list[tuple[str, bool]] = []
    cases = [
        ("401 on update_entity", _terminal_http(401)),
        ("403 on update_entity", _terminal_http(403)),
        ("404 on update_entity (misconfigured target)", _terminal_http(404)),
        ("422 on update_entity", _terminal_http(422)),
        ("404 on GET /states (wrong proxy path)", _terminal_http(404, on_get=True)),
        ("403 on GET /states (revoked token)", _terminal_http(403, on_get=True)),
    ]
    for name, http in cases:
        sched = _scheduler(http, sleeper=FakeSleeper())
        delay = sched.poll_station("istation01")
        checks.append((f"{name} ⇒ holds at max_backoff (86400)", delay == 86400))
    return report("TERMINAL-PATH", "terminal", checks)


def check_transient_path() -> bool:
    """A transient API fault rests at the floor ``min_interval_seconds`` (300).

    Transport failure (status None), 5xx, and 429 on either call all take the
    transient rest: ``poll_station`` returns ``min_interval_seconds`` (300 with
    ``default_options``). Includes the malformed ``/states`` body (non-JSON /
    non-array) as transient. Negative discriminator: the result is the floor
    (300), never ``max_backoff`` (86400) and never ``OFFLINE_REPROBE`` — so a
    transient misrouted to the terminal/offline branch fails this.
    """
    checks: list[tuple[str, bool]] = []
    cases: list[tuple[str, FakeHttp]] = [
        (
            "transport failure (status None) on POST",
            FakeHttp(HttpError("conn reset", status=None)),
        ),
        ("500 on POST", FakeHttp(HttpError("http 500", status=500))),
        (
            "502 on GET /states",
            FakeHttp(ok_response(""), HttpError("http 502", status=502)),
        ),
        (
            "503 on GET /states",
            FakeHttp(ok_response(""), HttpError("http 503", status=503)),
        ),
        ("non-JSON /states body", FakeHttp(ok_response(""), ok_response("not json"))),
        ("non-array /states body", FakeHttp(ok_response(""), ok_response('{"a": 1}'))),
    ]
    for name, http in cases:
        sched = _scheduler(http, sleeper=FakeSleeper())
        delay = sched.poll_station("istation01")
        checks.append((f"{name} ⇒ rests at floor (300)", delay == 300))
    # GET /states 404/422 are TERMINAL (paired discriminator against the 5xx rows).
    for status in (404, 422):
        http = FakeHttp(ok_response(""), HttpError(f"http {status}", status=status))
        sched = _scheduler(http, sleeper=FakeSleeper())
        delay = sched.poll_station("istation01")
        checks.append(
            (
                f"{status} on GET /states ⇒ TERMINAL hold (86400), not transient floor",
                delay == 86400,
            )
        )
    return report("TRANSIENT-PATH", "transient", checks)


def check_429_precedence() -> bool:
    """A 429 on update_entity is transient (precedence over the 4xx-terminal rule).

    A 429 POST returns the transient floor (``min_interval_seconds`` = 300), NOT
    the terminal ``max_backoff`` (86400) — pinning that the ``status == 429`` guard
    is checked before the ``4xx``-on-update_entity terminal rule. Paired
    discriminator: a 404 POST on the same path returns the terminal 86400, so a
    swap of the two guards would flip exactly one of these two assertions. The
    check's value is that the two paths return *different* rests.
    """
    http_429 = FakeHttp(HttpError("http 429", status=429))
    sched_429 = _scheduler(http_429, sleeper=FakeSleeper())
    delay_429 = sched_429.poll_station("istation01")

    http_404 = FakeHttp(HttpError("http 404", status=404))
    sched_404 = _scheduler(http_404, sleeper=FakeSleeper())
    delay_404 = sched_404.poll_station("istation01")

    checks: list[tuple[str, bool]] = [
        (
            "429 on update_entity ⇒ transient floor (300), not terminal max",
            delay_429 == 300,
        ),
        ("paired: 404 on update_entity ⇒ terminal max (86400)", delay_404 == 86400),
    ]
    # 429 on GET /states is also transient (same precedence on the read path).
    http_get_429 = FakeHttp(ok_response(""), HttpError("http 429", status=429))
    sched_get_429 = _scheduler(http_get_429, sleeper=FakeSleeper())
    checks.append(
        (
            "429 on GET /states ⇒ transient floor (300)",
            sched_get_429.poll_station("istation01") == 300,
        )
    )
    return report("429-PRECEDENCE", "429", checks)


def check_stop_during_waits() -> bool:
    """A stop signal mid-settle aborts promptly without a read, resting at the floor.

    Stop during the initial settle wait: ``_read_health`` raises `TransientError`,
    so ``poll_station`` returns the transient floor (``min_interval_seconds`` =
    300) and issues NO GET /states. ``run_loop`` stop during the inter-poll wait
    returns immediately with no poll. Each is proven by the recorded
    `FakeHttp.calls` count + the `FakeSleeper.slept` length.
    """
    checks: list[tuple[str, bool]] = []

    # --- stop during the initial settle wait (first sleeper call) -------------
    stale = fixtures.obstime_states("istation01", obstime=fixtures.OBSTIME_T0)
    http_settle = FakeHttp(ok_response(""), states_response(stale))
    sleeper_settle = FakeSleeper(stop_at=0)  # the very first sleep returns stop
    sched_settle = _scheduler(http_settle, sleeper=sleeper_settle)
    delay_settle = sched_settle.poll_station("istation01")
    # POST issued (call 0); NO GET issued because the settle wait was interrupted.
    get_calls_settle = [c for c in http_settle.calls if c.method == "GET"]
    checks += [
        ("stop mid-settle ⇒ transient floor (300)", delay_settle == 300),
        (
            "stop mid-settle ⇒ no GET /states issued (aborted before read)",
            len(get_calls_settle) == 0,
        ),
        (
            "stop mid-settle ⇒ exactly one sleeper wait recorded",
            len(sleeper_settle.slept) == 1,
        ),
    ]

    # --- run_loop stop during the inter-poll wait -----------------------------
    http_loop = FakeHttp(ok_response(""), states_response(stale))
    sleeper_loop = FakeSleeper(stop_at=0)  # first inter-poll wait returns stop
    sched_loop = _scheduler(http_loop, sleeper=sleeper_loop)
    sched_loop.run_loop()
    checks += [
        (
            "run_loop stop pre-first-poll ⇒ no HTTP calls at all",
            len(http_loop.calls) == 0,
        ),
        (
            "run_loop stop pre-first-poll ⇒ exactly one wait recorded",
            len(sleeper_loop.slept) == 1,
        ),
    ]
    return report("STOP-DURING-WAITS", "stop", checks)

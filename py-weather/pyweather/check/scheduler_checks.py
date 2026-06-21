# pyright: strict
"""Adaptive-scheduler checks: the reward/backoff split, the error taxonomy, and
the interruptible waits.

Every case drives the **real** `Scheduler.poll_station` (and, for the loop/stop
cases, `run_loop`) against recording fakes for the HTTP, clock, wall-clock,
sleeper, and rng seams. The assertions read the returned seconds-to-next-poll and
the mutated `StationState.current_backoff`, plus the recorded `FakeSleeper.slept`
and `FakeHttp.calls`, so each oracle fails on a broken impl rather than on a
pre-set expectation.

Mutation discipline notes are inline per group: each oracle is paired with a
positive/negative twin where the plan calls for one, so a constant-return or a
swapped-branch implementation is caught.
"""

from __future__ import annotations

from datetime import datetime

from .. import fixtures
from ..config import validate
from ..haapi import HaApiClient
from ..httpclient import HttpError, HttpResponse
from ..models import Config
from ..scheduler import MAX_FRESHNESS_REREADS, Scheduler
from .fakes import (
    FakeClock,
    FakeHttp,
    FakeSleeper,
    FakeWallClock,
    SequenceRandom,
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
    rng: SequenceRandom | None = None,
    clock: FakeClock | None = None,
) -> Scheduler:
    """Wire a `Scheduler` over the real `HaApiClient` and the given fakes."""
    cfg = config or _config()
    api = HaApiClient(http, fixtures.EXAMPLE_TOKEN, float(cfg.request_timeout_seconds))
    return Scheduler(
        cfg,
        api=api,
        clock=clock or FakeClock(),
        wall_clock=FakeWallClock(_T0),
        sleeper=sleeper or FakeSleeper(),
        rng=rng or SequenceRandom(350),
    )


# The update_entity POST always succeeds in these health/freshness-focused cases;
# the GET /states response is what each case scripts. A FakeHttp returns its
# scripted responses by call order, and poll_station issues POST then GET(s).
def _post_then_states(*states_bodies: list[dict[str, object]]) -> FakeHttp:
    """A FakeHttp: a 2xx POST, then one `states_response` per scripted body."""
    responses = [ok_response("")]
    responses += [states_response(b) for b in states_bodies]
    return FakeHttp(*responses)


def check_reward_split() -> bool:
    """Confirmed earns the fast cadence + reset; inconclusive holds, never rewards.

    Positive: a confirmed poll (primary ``last_reported`` advanced) returns a
    random interval in ``[min,max]`` AND resets ``current_backoff`` to
    ``initial_backoff_seconds``. Negative twin: an inconclusive-fallback poll
    (representative present, required-core usable, fallback unchanged) returns the
    held ``current_backoff`` (NOT the fast interval) and does NOT touch the rng —
    proving the fast reward is gated on positive confirmation, not on mere health.
    """
    checks: list[tuple[str, bool]] = []

    # --- positive: confirmed ⇒ fast interval + reset -------------------------
    confirmed_states = fixtures.station_states(
        "istation01", temp_last_reported=fixtures.FRESH_ISO
    )
    http = _post_then_states(confirmed_states)
    rng = SequenceRandom(377)
    sched = _scheduler(http, sleeper=FakeSleeper(), rng=rng)
    # Pre-dirty the backoff so the reset is observable (not a coincidental floor).
    sched.state_for("istation01").current_backoff = 4800
    delay = sched.poll_station("istation01")
    checks += [
        (
            "confirmed ⇒ scheduled interval is the rng healthy interval (377)",
            delay == 377,
        ),
        (
            "confirmed ⇒ current_backoff reset to initial_backoff_seconds (300)",
            sched.state_for("istation01").current_backoff == 300,
        ),
        ("confirmed ⇒ rng.randint consulted exactly once", rng.calls == 1),
    ]

    # --- negative twin: inconclusive ⇒ hold, no reward, no rng ----------------
    # last_reported absent + last_updated/last_changed unchanged ⇒ INCONCLUSIVE.
    inconclusive_states = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.OMIT,
        temp_last_updated=fixtures.STALE_ISO,
        temp_last_changed=fixtures.STALE_ISO,
    )
    http2 = _post_then_states(inconclusive_states)
    rng2 = SequenceRandom(377)
    sched2 = _scheduler(http2, sleeper=FakeSleeper(), rng=rng2)
    sched2.state_for("istation01").current_backoff = 4800
    delay2 = sched2.poll_station("istation01")
    checks += [
        (
            "inconclusive ⇒ holds current_backoff (4800), NOT the fast interval",
            delay2 == 4800,
        ),
        (
            "inconclusive ⇒ current_backoff unchanged (held, not reset)",
            sched2.state_for("istation01").current_backoff == 4800,
        ),
        ("inconclusive ⇒ rng.randint NOT consulted (no fast reward)", rng2.calls == 0),
    ]

    # --- paired positive for the fallback path: advanced fallback ⇒ reward ----
    # Proves the inconclusive hold is driven by the unchanged fallback, not by the
    # mere absence of last_reported: an advanced fallback on the SAME shape rewards.
    fallback_advanced = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.OMIT,
        temp_last_updated=fixtures.FRESH_ISO,
    )
    http3 = _post_then_states(fallback_advanced)
    rng3 = SequenceRandom(388)
    sched3 = _scheduler(http3, sleeper=FakeSleeper(), rng=rng3)
    sched3.state_for("istation01").current_backoff = 4800
    delay3 = sched3.poll_station("istation01")
    checks += [
        (
            "advanced fallback ⇒ fast interval (388), proving hold is the unchanged case",
            delay3 == 388,
        ),
        (
            "advanced fallback ⇒ current_backoff reset to 300",
            sched3.state_for("istation01").current_backoff == 300,
        ),
    ]
    return report("REWARD-SPLIT", "reward", checks)


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
    """A terminal API fault holds at ``max_backoff_seconds`` (no doubling, no now).

    401/403 on update_entity, a non-429 4xx on update_entity (404/422), and a
    non-429 4xx on GET /states all return ``max_backoff_seconds`` and leave
    ``current_backoff`` untouched (it is NOT doubled). Negative discriminator: the
    returned value is the full max (86400), never the (much smaller) doubled
    transient value or a 0/now — so a terminal misrouted to the transient branch
    would fail this.
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
        sched.state_for("istation01").current_backoff = 600  # pre-doubled, observable
        delay = sched.poll_station("istation01")
        checks.append((f"{name} ⇒ holds at max_backoff (86400)", delay == 86400))
        checks.append(
            (
                f"{name} ⇒ current_backoff NOT doubled (stays 600)",
                sched.state_for("istation01").current_backoff == 600,
            )
        )
    return report("TERMINAL-PATH", "terminal", checks)


def check_transient_path() -> bool:
    """A transient API fault doubles ``current_backoff`` (first retry initial*2).

    Transport failure (status None), 5xx, and 429 on either call all take the
    transient backoff: the first failure after a reset returns
    ``initial_backoff_seconds * 2`` (600) and stores it. Includes the malformed
    ``/states`` body (non-JSON / non-array) as transient. Negative discriminator:
    the result is the doubled value (600), never ``max_backoff`` (86400) — so a
    transient misrouted to the terminal branch fails this.
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
        checks.append((f"{name} ⇒ doubles to initial*2 (600)", delay == 600))
        checks.append(
            (
                f"{name} ⇒ current_backoff stored as 600 (not max)",
                sched.state_for("istation01").current_backoff == 600,
            )
        )
    # GET /states 404/422 are TERMINAL (paired discriminator against the 5xx rows).
    for status in (404, 422):
        http = FakeHttp(ok_response(""), HttpError(f"http {status}", status=status))
        sched = _scheduler(http, sleeper=FakeSleeper())
        sched.state_for("istation01").current_backoff = 600
        delay = sched.poll_station("istation01")
        checks.append(
            (
                f"{status} on GET /states ⇒ TERMINAL hold (86400), not transient double",
                delay == 86400 and sched.state_for("istation01").current_backoff == 600,
            )
        )
    return report("TRANSIENT-PATH", "transient", checks)


def check_429_precedence() -> bool:
    """A 429 on update_entity is transient (precedence over the 4xx-terminal rule).

    A 429 POST returns the doubled transient value (initial*2 = 600), NOT the
    terminal ``max_backoff`` (86400) — pinning that the ``status == 429`` guard is
    checked before the ``4xx``-on-update_entity terminal rule. Paired
    discriminator: a 404 POST on the same path returns the terminal 86400, so a
    swap of the two guards would flip exactly one of these two assertions.
    """
    http_429 = FakeHttp(HttpError("http 429", status=429))
    sched_429 = _scheduler(http_429, sleeper=FakeSleeper())
    delay_429 = sched_429.poll_station("istation01")

    http_404 = FakeHttp(HttpError("http 404", status=404))
    sched_404 = _scheduler(http_404, sleeper=FakeSleeper())
    delay_404 = sched_404.poll_station("istation01")

    checks: list[tuple[str, bool]] = [
        (
            "429 on update_entity ⇒ transient double (600), not terminal max",
            delay_429 == 600,
        ),
        (
            "429 ⇒ current_backoff stored 600 (transient mutation)",
            sched_429.state_for("istation01").current_backoff == 600,
        ),
        ("paired: 404 on update_entity ⇒ terminal max (86400)", delay_404 == 86400),
    ]
    # 429 on GET /states is also transient (same precedence on the read path).
    http_get_429 = FakeHttp(ok_response(""), HttpError("http 429", status=429))
    sched_get_429 = _scheduler(http_get_429, sleeper=FakeSleeper())
    checks.append(
        (
            "429 on GET /states ⇒ transient double (600)",
            sched_get_429.poll_station("istation01") == 600,
        )
    )
    return report("429-PRECEDENCE", "429", checks)


def check_healthy_interval_bounds() -> bool:
    """A confirmed poll schedules within ``[healthy_interval_min, healthy_interval_max]``.

    Drives the real rng (``random.Random``, default seed) so the bounds assertion
    is over an actual draw, not a scripted constant — then a scripted-edge twin
    pins that the exact ``min`` and ``max`` draws are both honored (the rng is
    consulted with the configured bounds). Repeated draws all stay in range.
    """
    import random as _random

    checks: list[tuple[str, bool]] = []
    cfg = _config()
    in_range = True
    for _ in range(50):
        confirmed = fixtures.station_states(
            "istation01", temp_last_reported=fixtures.FRESH_ISO
        )
        http = _post_then_states(confirmed)
        api = HaApiClient(
            http, fixtures.EXAMPLE_TOKEN, float(cfg.request_timeout_seconds)
        )
        sched = Scheduler(
            cfg,
            api=api,
            clock=FakeClock(),
            wall_clock=FakeWallClock(_T0),
            sleeper=FakeSleeper(),
            rng=_random.Random(),
        )
        delay = sched.poll_station("istation01")
        in_range = in_range and (
            cfg.healthy_interval_min <= delay <= cfg.healthy_interval_max
        )
    checks.append(
        (
            "50 confirmed polls all schedule within [min,max] = [300,400]",
            in_range,
        )
    )
    # Scripted edges: min and max draws are both honored verbatim.
    for edge in (cfg.healthy_interval_min, cfg.healthy_interval_max):
        confirmed = fixtures.station_states(
            "istation01", temp_last_reported=fixtures.FRESH_ISO
        )
        http = _post_then_states(confirmed)
        sched = _scheduler(http, sleeper=FakeSleeper(), rng=SequenceRandom(edge))
        checks.append(
            (
                f"scripted rng edge {edge} is scheduled verbatim",
                sched.poll_station("istation01") == edge,
            )
        )
    return report("HEALTHY-INTERVAL", "interval", checks)


def check_backoff_sequence() -> bool:
    """Sequential unhealthy polls double from initial and cap at ``max_backoff``.

    Drives repeated unhealthy polls (representative present, primary
    ``last_reported`` stale ⇒ UNHEALTHY) and asserts the returned-seconds sequence
    is the plan's exact ``600, 1200, 2400, 4800, 9600, 19200, 38400, 76800, 86400``
    then holds at the ``86400`` cap (the doubling stops at the cap, never exceeds
    it). Each poll re-reads up to ``MAX_FRESHNESS_REREADS`` times, so each unhealthy
    poll scripts ``1 + MAX_FRESHNESS_REREADS`` stale GET bodies.
    """
    cfg = _config()
    stale = fixtures.station_states("istation01", temp_last_reported=fixtures.STALE_ISO)
    # Per poll: 1 POST + (1 + MAX_FRESHNESS_REREADS) GET /states (all stale).
    per_poll_gets = 1 + MAX_FRESHNESS_REREADS
    expected = [600, 1200, 2400, 4800, 9600, 19200, 38400, 76800, 86400, 86400]

    actual: list[int] = []
    # FakeHttp repeats its last scripted response, so a bare [POST, stale-GET]
    # script would only make the FIRST POST 2xx (every later call would replay the
    # stale GET, breaking the second poll's POST). Script the full per-poll
    # interleave (POST then per_poll_gets stale GETs) for the whole run instead.
    total_polls = len(expected)
    responses: list[HttpResponse] = []
    for _ in range(total_polls):
        responses.append(ok_response(""))  # POST
        responses += [states_response(stale)] * per_poll_gets
    api_http = FakeHttp(*responses)
    api = HaApiClient(
        api_http, fixtures.EXAMPLE_TOKEN, float(cfg.request_timeout_seconds)
    )
    sched = Scheduler(
        cfg,
        api=api,
        clock=FakeClock(),
        wall_clock=FakeWallClock(_T0),
        sleeper=FakeSleeper(),
        rng=SequenceRandom(350),
    )
    for _ in range(total_polls):
        actual.append(sched.poll_station("istation01"))

    checks: list[tuple[str, bool]] = [
        (
            f"backoff sequence is the plan's doubling + cap: {expected}",
            actual == expected,
        ),
        (
            "final current_backoff is exactly max_backoff (86400), never exceeded",
            sched.state_for("istation01").current_backoff == 86400,
        ),
        (
            "every value is <= max_backoff (cap never breached)",
            all(v <= cfg.max_backoff_seconds for v in actual),
        ),
    ]
    return report("BACKOFF-SEQUENCE", "backoff", checks)


def check_backoff_reset_after_recovery() -> bool:
    """After backoff, a confirmed poll resets to fast; sequential unhealthy compounds.

    Positive: one unhealthy poll ⇒ 600; then a confirmed poll ⇒ a fast interval in
    ``[min,max]`` AND ``current_backoff`` reset to ``initial_backoff_seconds`` (300)
    — proving recovery clears the accumulated backoff. Negative twin: two
    sequential unhealthy polls ⇒ second poll returns 1200 (proving the doubling
    compounds across polls, not a flat re-double from the floor each time).
    """
    cfg = _config()
    stale = fixtures.station_states("istation01", temp_last_reported=fixtures.STALE_ISO)
    confirmed = fixtures.station_states(
        "istation01", temp_last_reported=fixtures.FRESH_ISO
    )
    per_poll_gets = 1 + MAX_FRESHNESS_REREADS

    # --- positive: unhealthy(600) then confirmed(reset + fast) ----------------
    responses = [ok_response("")] + [states_response(stale)] * per_poll_gets
    responses += [ok_response(""), states_response(confirmed)]  # confirmed poll
    http = FakeHttp(*responses)
    api = HaApiClient(http, fixtures.EXAMPLE_TOKEN, float(cfg.request_timeout_seconds))
    sched = Scheduler(
        cfg,
        api=api,
        clock=FakeClock(),
        wall_clock=FakeWallClock(_T0),
        sleeper=FakeSleeper(),
        rng=SequenceRandom(333),
    )
    first = sched.poll_station("istation01")
    after_backoff = sched.state_for("istation01").current_backoff
    recovered = sched.poll_station("istation01")
    reset_backoff = sched.state_for("istation01").current_backoff
    checks: list[tuple[str, bool]] = [
        ("first unhealthy poll ⇒ 600 (initial*2)", first == 600),
        ("current_backoff after first unhealthy ⇒ 600", after_backoff == 600),
        ("recovery (confirmed) ⇒ fast interval in [300,400]", 300 <= recovered <= 400),
        ("recovery (confirmed) ⇒ current_backoff reset to 300", reset_backoff == 300),
    ]

    # --- negative twin: two sequential unhealthy ⇒ 600 then 1200 --------------
    responses2: list[HttpResponse] = []
    for _ in range(2):
        responses2.append(ok_response(""))
        responses2 += [states_response(stale)] * per_poll_gets
    http2 = FakeHttp(*responses2)
    api2 = HaApiClient(
        http2, fixtures.EXAMPLE_TOKEN, float(cfg.request_timeout_seconds)
    )
    sched2 = Scheduler(
        cfg,
        api=api2,
        clock=FakeClock(),
        wall_clock=FakeWallClock(_T0),
        sleeper=FakeSleeper(),
        rng=SequenceRandom(333),
    )
    d1 = sched2.poll_station("istation01")
    d2 = sched2.poll_station("istation01")
    checks += [
        ("paired negative: 1st sequential unhealthy ⇒ 600", d1 == 600),
        (
            "paired negative: 2nd sequential unhealthy ⇒ 1200 (compounds, not re-floored)",
            d2 == 1200,
        ),
    ]
    return report("BACKOFF-RESET", "reset", checks)


def check_freshness_reread_recovery() -> bool:
    """A slow-but-successful render recovers on a later re-read (re-reads are spaced).

    Drives the real ``_read_states_with_freshness`` loop: the first two GET
    ``/states`` reads are stale (primary ``last_reported`` not advanced) and the
    third is advanced. The poll resolves CONFIRMED (the slow render is NOT falsely
    marked unhealthy) and issues exactly three GETs, each preceded by a sleeper
    wait — proving the bounded re-reads are spaced through the single sleeper and a
    final advance is honored. The confirmed poll earns the fast cadence + reset.

    Mutation discriminator: an impl that read once (no re-read) would see only the
    first stale body and return UNHEALTHY (600), failing the CONFIRMED + 3-GET
    + fast-interval assertions; an impl that did not space the re-reads through the
    sleeper would record fewer than three waits.
    """
    cfg = _config()
    stale = fixtures.station_states("istation01", temp_last_reported=fixtures.STALE_ISO)
    fresh = fixtures.station_states("istation01", temp_last_reported=fixtures.FRESH_ISO)
    # POST(2xx), then GET stale, GET stale, GET fresh (recovery on the final read).
    http = FakeHttp(
        ok_response(""),
        states_response(stale),
        states_response(stale),
        states_response(fresh),
    )
    sleeper = FakeSleeper()  # never stops; records each settle/re-read wait
    sched = _scheduler(http, sleeper=sleeper, rng=SequenceRandom(361))
    sched.state_for("istation01").current_backoff = 4800  # pre-dirty, observe reset
    delay = sched.poll_station("istation01")
    get_calls = [c for c in http.calls if c.method == "GET"]
    checks: list[tuple[str, bool]] = [
        (
            "stale, stale, then-fresh ⇒ CONFIRMED (slow render not falsely unhealthy)",
            delay == 361,
        ),
        (
            "recovery via re-read ⇒ current_backoff reset to 300",
            sched.state_for("istation01").current_backoff == 300,
        ),
        (
            "exactly three GET /states issued (two re-reads after the first read)",
            len(get_calls) == 3,
        ),
        (
            "three sleeper waits recorded (settle + two re-read waits ⇒ re-reads are spaced)",
            len(sleeper.slept) == 3,
        ),
        (
            "each re-read wait equals the settle interval (bounded, spaced)",
            sleeper.slept == [float(cfg.settle_seconds)] * 3,
        ),
    ]
    return report("FRESHNESS-REREAD", "reread", checks)


def check_stop_during_waits() -> bool:
    """A stop signal mid-settle and mid-reread aborts promptly without a reward.

    Stop during the initial settle wait: ``poll_station`` returns the transient
    double (UNHEALTHY, no read issued beyond the POST) and issues NO GET /states.
    Stop during a freshness re-read wait: after the first (stale) read, the
    re-read wait is interrupted, so no further GET is issued and the poll is
    UNHEALTHY. ``run_loop`` stop during the inter-poll wait returns immediately
    with no poll. Each is proven by the recorded `FakeHttp.calls` count + the
    `FakeSleeper.slept` length.
    """
    checks: list[tuple[str, bool]] = []

    # --- stop during the initial settle wait (first sleeper call) -------------
    stale = fixtures.station_states("istation01", temp_last_reported=fixtures.STALE_ISO)
    http_settle = FakeHttp(ok_response(""), states_response(stale))
    sleeper_settle = FakeSleeper(stop_at=0)  # the very first sleep returns stop
    sched_settle = _scheduler(http_settle, sleeper=sleeper_settle)
    delay_settle = sched_settle.poll_station("istation01")
    # POST issued (call 0); NO GET issued because the settle wait was interrupted.
    get_calls_settle = [c for c in http_settle.calls if c.method == "GET"]
    checks += [
        ("stop mid-settle ⇒ UNHEALTHY transient double (600)", delay_settle == 600),
        (
            "stop mid-settle ⇒ no GET /states issued (aborted before read)",
            len(get_calls_settle) == 0,
        ),
        (
            "stop mid-settle ⇒ exactly one sleeper wait recorded",
            len(sleeper_settle.slept) == 1,
        ),
    ]

    # --- stop during the first freshness re-read wait -------------------------
    # settle wait (index 0) proceeds; first GET is stale ⇒ UNHEALTHY ⇒ re-read
    # wait (index 1) is interrupted ⇒ no second GET. So exactly ONE GET.
    http_reread = FakeHttp(
        ok_response(""), states_response(stale), states_response(stale)
    )
    sleeper_reread = FakeSleeper(stop_at=1)  # settle OK, first re-read wait stops
    sched_reread = _scheduler(http_reread, sleeper=sleeper_reread)
    delay_reread = sched_reread.poll_station("istation01")
    get_calls_reread = [c for c in http_reread.calls if c.method == "GET"]
    checks += [
        ("stop mid-reread ⇒ UNHEALTHY transient double (600)", delay_reread == 600),
        (
            "stop mid-reread ⇒ exactly ONE GET /states (re-read aborted)",
            len(get_calls_reread) == 1,
        ),
        (
            "stop mid-reread ⇒ exactly two sleeper waits (settle + interrupted reread)",
            len(sleeper_reread.slept) == 2,
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

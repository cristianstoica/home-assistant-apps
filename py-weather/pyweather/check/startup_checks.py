# pyright: strict
"""Startup-orchestration checks: discover/persist retry/exit/stop + run_startup branch.

Drives the new injectable `__main__._discover_and_persist` and
`__main__.run_startup` seams against recording fakes (`FakeHttp`, `FakeSleeper`,
the `FakeSupervisorSelfClient` here, and recording `StartupDeps` factories). No
network, no real signals, no real `Scheduler` loop. Every fatal/stop case pins
the post-stop / post-fatal invariants (`set_options` zero calls, no `Scheduler`
construction) so the `list[Station] | None` + `SystemExit` contract is exercised,
not just asserted in prose.
"""

from __future__ import annotations

import logging

from .. import config, fixtures
from ..haapi import HaApiClient
from ..httpclient import HttpError
from ..models import Config, Station
from ..supervisor import to_options_dict
from .fakes import FakeHttp, FakeSleeper, states_response
from .report import report

_LOG = logging.getLogger("pyweather.check")

# CONTRACT — hand-mirrored from `config.yaml` `schema:` (config.yaml:47-59).
# This set and `supervisor.to_options_dict` are two co-located code constants;
# the `persist-allowlist` check proves they agree with EACH OTHER, not with
# `config.yaml` (the runtime never parses YAML — stdlib-only). When you add/remove
# a `config.yaml schema:` field you MUST update BOTH this set and
# `to_options_dict` in the same change; the check cannot detect a manifest field
# you forgot to add to both.
_MANIFEST_OPTION_KEYS = {
    "healthy_interval_min",
    "healthy_interval_max",
    "initial_backoff_seconds",
    "max_backoff_seconds",
    "settle_seconds",
    "startup_stagger_seconds",
    "request_timeout_seconds",
    "log_level",
    "stations",
}


class FakeSupervisorSelfClient:
    """A recording `SupervisorSelfClient` double: records each `set_options` body.

    `options_calls` records each posted options dict so an oracle asserts WHETHER
    and WITH WHAT persistence ran. `fail_with` (when set) makes `set_options`
    raise it, driving the best-effort WARNING + paste-block + continue path. No
    network; structurally compatible with the real client's `set_options(options)`.
    """

    def __init__(self, fail_with: Exception | None = None) -> None:
        self.options_calls: list[dict[str, object]] = []
        self._fail_with = fail_with

    def set_options(self, options: dict[str, object]) -> None:
        self.options_calls.append(options)
        if self._fail_with is not None:
            raise self._fail_with


def _discovered_cfg() -> Config:
    """An empty-stations `Config` (the auto-populate trigger), default tuning."""
    return config.validate(fixtures.default_options(stations=[]))


def _api(http: FakeHttp) -> HaApiClient:
    """Wire the real `HaApiClient` over a `FakeHttp` (the production read path)."""
    return HaApiClient(http, fixtures.EXAMPLE_TOKEN, 30.0)


def _discover_and_persist():  # imported lazily to avoid a __main__ import cycle at module load
    from ..__main__ import _discover_and_persist as fn  # type: ignore[import-private]

    return fn


def _max_discovery_attempts() -> int:  # lazy, same __main__-cycle-avoidance reason
    from ..__main__ import MAX_DISCOVERY_ATTEMPTS

    return MAX_DISCOVERY_ATTEMPTS


def check_persist_allowlist_completeness() -> bool:
    """Assert `to_options_dict`'s key set equals `_MANIFEST_OPTION_KEYS` (code↔code).

    The allowlist-completeness guard, scoped to the two co-located CODE constants:
    a field dropped from `to_options_dict` but left in `_MANIFEST_OPTION_KEYS` (or
    vice versa) makes this fail. It does NOT verify against `config.yaml` — the
    runtime never parses YAML (stdlib-only), so a `schema:` field an operator
    forgets to add to BOTH code constants is invisible here (see the CONTRACT
    comment above `_MANIFEST_OPTION_KEYS`). Also pins that omitted-by-operator
    fields persist at their RESOLVED default value, and that `stations` carries the
    discovered list verbatim.
    """
    cfg = _discovered_cfg()
    stations = [
        Station(
            key="istation01",
            update_entity="sensor.wu_temp_istation01",
            expected_sensors=4,
        ),
        Station(
            key="istation02",
            update_entity="sensor.wu_temp_istation02",
            expected_sensors=3,
        ),
    ]
    blob = to_options_dict(cfg, stations)
    checks: list[tuple[str, bool]] = [
        (
            "options-blob key set == manifest option keys ∪ {stations}",
            set(blob) == _MANIFEST_OPTION_KEYS,
        ),
        (
            "omitted-by-operator field persists at the resolved default (settle_seconds=15)",
            blob["settle_seconds"] == 15,
        ),
        (
            "stations carries the discovered list verbatim",
            blob["stations"]
            == [
                {
                    "key": "istation01",
                    "update_entity": "sensor.wu_temp_istation01",
                    "expected_sensors": 4,
                },
                {
                    "key": "istation02",
                    "update_entity": "sensor.wu_temp_istation02",
                    "expected_sensors": 3,
                },
            ],
        ),
    ]
    return report("PERSIST-ALLOWLIST", "persist-allowlist", checks)


def check_discover_retry_and_exit() -> bool:
    """Assert the retry/exit/stop/terminal contract of `_discover_and_persist`.

    Covers: stop-before-first-read (pre-read gate), stop-returns-None
    (mid-inter-attempt-wait), valid-scan-zero-matches exhaustion (empty-fleet
    ERROR), transient-exhaustion (API-failure ERROR), mixed-outcome-exhaustion
    (inconclusive ERROR), terminal-immediate-exit, and the boot-race RECOVERY case
    (zero-match early then a station appears before the cap). The two exhaustion
    arms additionally pin the exact `MAX_DISCOVERY_ATTEMPTS` GET count. Every
    fatal/stop case pins `set_options` zero calls (no persist on a non-resolved
    outcome).
    """
    run = _discover_and_persist()
    MAX_DISCOVERY_ATTEMPTS = _max_discovery_attempts()
    cfg = _discovered_cfg()
    checks: list[tuple[str, bool]] = []

    # --- stop-before-first-read: pre-read gate (attempt==0) -----------------
    http_pre = FakeHttp(states_response(fixtures.station_states("istation01")))
    sup_pre = FakeSupervisorSelfClient()
    sleeper_pre = FakeSleeper(stop_at=0)  # first sleeper(0) gate returns stop
    result_pre = run(_api(http_pre), sup_pre, sleeper_pre, cfg, log=_LOG)
    get_calls_pre = [c for c in http_pre.calls if c.method == "GET"]
    checks += [
        ("stop-before-first-read ⇒ returns None", result_pre is None),
        ("stop-before-first-read ⇒ zero GET /states calls", len(get_calls_pre) == 0),
        (
            "stop-before-first-read ⇒ zero set_options calls",
            sup_pre.options_calls == [],
        ),
    ]

    # --- stop-returns-None: stop on a later inter-attempt wait (stop_at>=1) --
    # zero-match scan on attempt 0 (clean), then stop during the inter-attempt
    # wait. Pre-read gate (attempt 0) does NOT fire (stop_at=1), so the slept
    # record is [pre-gate(0), inter-attempt-wait→stop].
    http_stop = FakeHttp(states_response([]))  # always a clean zero-match array
    sup_stop = FakeSupervisorSelfClient()
    sleeper_stop = FakeSleeper(stop_at=1)
    result_stop = run(_api(http_stop), sup_stop, sleeper_stop, cfg, log=_LOG)
    checks += [
        ("stop-returns-None ⇒ returns None (not a station list)", result_stop is None),
        ("stop-returns-None ⇒ zero set_options calls", sup_stop.options_calls == []),
    ]

    # --- valid-scan-zero-matches: cap exhausted, empty-fleet ERROR ----------
    http_empty = FakeHttp(states_response([]))  # clean zero-match every attempt
    sup_empty = FakeSupervisorSelfClient()
    sleeper_empty = FakeSleeper()  # never stops; runs to exhaustion
    raised_empty = False
    try:
        run(_api(http_empty), sup_empty, sleeper_empty, cfg, log=_LOG)
    except SystemExit as exc:
        raised_empty = exc.code != 0
    checks += [
        ("valid-scan-zero-matches ⇒ raises SystemExit non-zero", raised_empty),
        (
            "valid-scan-zero-matches ⇒ zero set_options calls",
            sup_empty.options_calls == [],
        ),
        # all-zero: exhausted exactly MAX_DISCOVERY_ATTEMPTS reads
        (
            "valid-scan-zero-matches ⇒ exactly MAX_DISCOVERY_ATTEMPTS GET /states",
            len([c for c in http_empty.calls if c.method == "GET"])
            == MAX_DISCOVERY_ATTEMPTS,
        ),
    ]

    # --- transient-exhaustion: every attempt 503 ⇒ API-failure ERROR --------
    http_503 = FakeHttp(HttpError("http 503", status=503))
    sup_503 = FakeSupervisorSelfClient()
    raised_503 = False
    try:
        run(_api(http_503), sup_503, FakeSleeper(), cfg, log=_LOG)
    except SystemExit as exc:
        raised_503 = exc.code != 0
    checks += [
        ("transient-exhaustion ⇒ raises SystemExit non-zero", raised_503),
        ("transient-exhaustion ⇒ zero set_options calls", sup_503.options_calls == []),
        # all-503: exhausted exactly MAX_DISCOVERY_ATTEMPTS reads
        (
            "transient-exhaustion ⇒ exactly MAX_DISCOVERY_ATTEMPTS GET /states",
            len([c for c in http_503.calls if c.method == "GET"])
            == MAX_DISCOVERY_ATTEMPTS,
        ),
    ]

    # --- mixed-outcome-exhaustion: one clean-empty then 503s ----------------
    http_mixed = FakeHttp(
        states_response([]),
        HttpError("http 503", status=503),
        HttpError("http 503", status=503),
        HttpError("http 503", status=503),
        HttpError("http 503", status=503),
    )
    sup_mixed = FakeSupervisorSelfClient()
    raised_mixed = False
    try:
        run(_api(http_mixed), sup_mixed, FakeSleeper(), cfg, log=_LOG)
    except SystemExit as exc:
        raised_mixed = exc.code != 0
    checks += [
        ("mixed-outcome-exhaustion ⇒ raises SystemExit non-zero", raised_mixed),
        (
            "mixed-outcome-exhaustion ⇒ zero set_options calls",
            sup_mixed.options_calls == [],
        ),
    ]

    # --- terminal-immediate-exit: 403 on /states ⇒ exit on first attempt ----
    http_403 = FakeHttp(HttpError("http 403", status=403))
    sup_403 = FakeSupervisorSelfClient()
    raised_403 = False
    try:
        run(_api(http_403), sup_403, FakeSleeper(), cfg, log=_LOG)
    except SystemExit as exc:
        raised_403 = exc.code != 0
    get_calls_403 = [c for c in http_403.calls if c.method == "GET"]
    checks += [
        ("terminal-immediate-exit ⇒ raises SystemExit non-zero", raised_403),
        (
            "terminal-immediate-exit ⇒ exactly one GET /states (no retry)",
            len(get_calls_403) == 1,
        ),
        (
            "terminal-immediate-exit ⇒ zero set_options calls",
            sup_403.options_calls == [],
        ),
    ]

    # --- boot-race recovery: zero on attempt 0, station appears on attempt 1 ----
    # The bounded retry's whole reason to exist (design: a single early-boot scan
    # may see zero sensor.wu_temp_* purely from Core's REST sensors not yet loaded).
    # Proves the loop RECOVERS — zero-match then a station appears before the cap —
    # rather than only proving the exhaustion paths.
    http_recover = FakeHttp(
        states_response([]),  # attempt 0: clean zero-match
        states_response(
            fixtures.station_states("istation01")
        ),  # attempt 1: station appears
        states_response(
            fixtures.station_states("istation01")
        ),  # count-stability confirm read
    )
    sup_recover = FakeSupervisorSelfClient()
    result_recover = run(_api(http_recover), sup_recover, FakeSleeper(), cfg, log=_LOG)
    get_calls_recover = [c for c in http_recover.calls if c.method == "GET"]
    checks += [
        (
            "recovery ⇒ resolves (returns the discovered list, not None/exit)",
            result_recover is not None
            and {s.key for s in result_recover} == {"istation01"},
        ),
        (
            "recovery ⇒ proceeded before the cap (no SystemExit) and persisted once",
            len(sup_recover.options_calls) == 1,
        ),
        (
            "recovery ⇒ re-scanned (zero-then-found took >1 GET)",
            len(get_calls_recover) >= 2,
        ),
    ]
    return report("DISCOVER-RETRY", "discover-retry", checks)


def check_discover_message_discriminators() -> bool:
    """Pin the THREE distinct exhaustion ERROR messages (the discriminator triad).

    Captures the logger output via a recording handler and asserts each
    exhaustion variant logs its OWN message and NOT the others — so a single
    boolean (which cannot separate clean-empty from faulted) would fail. The
    empty-fleet message names `rest.yaml`; the transient message names
    'unreachable' + the last transient; the mixed message names both the count
    and 'not a confirmed empty fleet'.
    """
    run = _discover_and_persist()
    cfg = _discovered_cfg()
    checks: list[tuple[str, bool]] = []

    def _run_capture(http: FakeHttp) -> str:
        records: list[str] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record.getMessage())

        log = logging.getLogger("pyweather.check.capture")
        log.handlers = [_Capture()]
        log.setLevel(logging.ERROR)
        log.propagate = False
        try:
            run(_api(http), FakeSupervisorSelfClient(), FakeSleeper(), cfg, log=log)
        except SystemExit:
            pass
        return " ".join(records)

    empty_msg = _run_capture(FakeHttp(states_response([])))
    transient_msg = _run_capture(FakeHttp(HttpError("http 503", status=503)))
    mixed_msg = _run_capture(
        FakeHttp(
            states_response([]),
            HttpError("http 503", status=503),
            HttpError("http 503", status=503),
            HttpError("http 503", status=503),
            HttpError("http 503", status=503),
        )
    )
    checks += [
        ("empty-fleet ERROR names rest.yaml", "rest.yaml" in empty_msg),
        (
            "empty-fleet ERROR is NOT the unreachable message",
            "unreachable" not in empty_msg,
        ),
        (
            "transient ERROR names the API as unreachable",
            "unreachable" in transient_msg,
        ),
        (
            "transient ERROR does NOT name rest.yaml alone (API path)",
            "not necessarily empty" in transient_msg,
        ),
        (
            "mixed ERROR states not-a-confirmed-empty-fleet",
            "not a confirmed empty fleet" in mixed_msg,
        ),
        ("mixed ERROR names an API error count", "API error" in mixed_msg),
        (
            "mixed ERROR is distinct from the clean empty-fleet message",
            mixed_msg != empty_msg,
        ),
    ]
    return report("DISCOVER-MESSAGES", "discover-messages", checks)


def check_discover_count_stability() -> bool:
    """Assert the per-key count-stability confirmation re-read (max) + station union.

    count-stability: first read has only `sensor.wu_temp_istation01`; the second
    (confirmation) read has temp+humidity+pressure+uv ⇒ resolved
    `expected_sensors == 4` (the fuller second-read count), proving the per-key
    `max` confirmation re-read fired. Sibling-lower twin: second read LOWER than
    first ⇒ count stays at the higher first-read value (max, never last-wins).
    station-set union: a confirm-only `istation02` is unioned into the resolved
    and persisted list. Confirmation TransientError ⇒ first-read stations kept
    (degrade-safe, discovery still resolves).
    """
    run = _discover_and_persist()
    cfg = _discovered_cfg()
    checks: list[tuple[str, bool]] = []

    # --- count grows on confirmation ⇒ max (4, not 1) -----------------------
    first_thin = [{"entity_id": "sensor.wu_temp_istation01", "state": "10.0"}]
    second_full = fixtures.station_states("istation01")  # temp+humidity+pressure+uv = 4
    http_grow = FakeHttp(states_response(first_thin), states_response(second_full))
    sup_grow = FakeSupervisorSelfClient()
    result_grow = run(_api(http_grow), sup_grow, FakeSleeper(), cfg, log=_LOG)
    assert result_grow is not None
    by_key_grow = {s.key: s for s in result_grow}
    checks += [
        (
            "count-stability ⇒ resolved expected_sensors is the fuller second-read count (4)",
            by_key_grow["istation01"].expected_sensors == 4,
        ),
        (
            "count-stability ⇒ persisted once (resolved, best-effort)",
            len(sup_grow.options_calls) == 1,
        ),
    ]

    # --- confirmation LOWER ⇒ keep higher first-read value (max) ------------
    first_full = fixtures.station_states("istation01")  # 4
    second_thin = [{"entity_id": "sensor.wu_temp_istation01", "state": "10.0"}]  # 1
    http_lower = FakeHttp(states_response(first_full), states_response(second_thin))
    result_lower = run(
        _api(http_lower), FakeSupervisorSelfClient(), FakeSleeper(), cfg, log=_LOG
    )
    assert result_lower is not None
    by_key_lower = {s.key: s for s in result_lower}
    checks.append(
        (
            "count-stability ⇒ confirmation lower keeps higher first-read count (4, max)",
            by_key_lower["istation01"].expected_sensors == 4,
        )
    )

    # --- confirm-only key ⇒ unioned into resolved + persisted ---------------
    first_one = [{"entity_id": "sensor.wu_temp_istation01", "state": "10.0"}]
    second_two = [
        {"entity_id": "sensor.wu_temp_istation01", "state": "10.0"},
        {"entity_id": "sensor.wu_temp_istation02", "state": "11.0"},
    ]
    http_union = FakeHttp(states_response(first_one), states_response(second_two))
    sup_union = FakeSupervisorSelfClient()
    result_union = run(_api(http_union), sup_union, FakeSleeper(), cfg, log=_LOG)
    assert result_union is not None
    persisted_keys: set[str] = {
        s["key"]
        for s in sup_union.options_calls[0]["stations"]  # type: ignore[index, union-attr]
    }
    checks += [
        (
            "station-set union ⇒ confirm-only istation02 in resolved list",
            {s.key for s in result_union} == {"istation01", "istation02"},
        ),
        (
            "station-set union ⇒ confirm-only istation02 in persisted body",
            "istation02" in persisted_keys,
        ),
    ]

    # --- confirmation TransientError ⇒ first-read stations kept -------------
    http_blip = FakeHttp(
        states_response(fixtures.station_states("istation01")),
        HttpError("http 503", status=503),  # confirmation read blips
    )
    result_blip = run(
        _api(http_blip), FakeSupervisorSelfClient(), FakeSleeper(), cfg, log=_LOG
    )
    assert result_blip is not None
    checks.append(
        (
            "confirmation blip ⇒ first-read stations kept (discovery still resolves)",
            {s.key for s in result_blip} == {"istation01"},
        )
    )

    # --- stop DURING the confirmation wait ⇒ return None, no second read, no persist ---
    # First read finds istation01 (stations truthy ⇒ reach the confirmation gate at
    # sleeper(settle_seconds)). FakeSleeper(stop_at=1): pre-read gate (index 0) does
    # NOT stop; the confirmation wait (index 1) returns stop ⇒ _discover_and_persist
    # must return None BEFORE the confirmation get_states, and never persist.
    http_confirm_stop = FakeHttp(
        states_response(fixtures.station_states("istation01")),  # first read: found
        states_response(
            fixtures.station_states("istation01")
        ),  # would be the confirm read (must NOT run)
    )
    sup_confirm_stop = FakeSupervisorSelfClient()
    result_confirm_stop = run(
        _api(http_confirm_stop), sup_confirm_stop, FakeSleeper(stop_at=1), cfg, log=_LOG
    )
    get_calls_confirm_stop = [c for c in http_confirm_stop.calls if c.method == "GET"]
    checks += [
        ("stop-during-confirmation-wait ⇒ returns None", result_confirm_stop is None),
        (
            "stop-during-confirmation-wait ⇒ only the first GET /states ran (no confirm read)",
            len(get_calls_confirm_stop) == 1,
        ),
        (
            "stop-during-confirmation-wait ⇒ zero set_options calls",
            sup_confirm_stop.options_calls == [],
        ),
    ]
    return report("DISCOVER-COUNT-STABILITY", "discover-count", checks)


def check_persist_best_effort() -> bool:
    """Assert persist success logs + a persist failure takes the WARNING+paste path.

    Success: a resolved discovery persists the full blob exactly once and returns
    the resolved list (the add-on runs in-memory either way). Failure: a faked
    `set_options` raising still returns the resolved list (never aborts) — the
    best-effort degrade-safe contract — AND emits the operator-recovery WARNING
    naming the Configuration tab plus a rendered `stations:` block carrying the
    discovered row (so deleting that recovery log, or the `render_stations_block`
    arg, fails this check rather than silently passing).
    """
    run = _discover_and_persist()
    cfg = _discovered_cfg()

    http_ok = FakeHttp(
        states_response(fixtures.station_states("istation01")),
        states_response(fixtures.station_states("istation01")),
    )
    sup_ok = FakeSupervisorSelfClient()
    result_ok = run(_api(http_ok), sup_ok, FakeSleeper(), cfg, log=_LOG)

    # Capture the WARNING the persist-failure branch logs (the recovery path).
    # Use a dedicated logger with a recording handler — mirroring `_run_capture`
    # in check_discover_message_discriminators — and pass THAT logger into the run
    # so the assertion binds to the message `_discover_and_persist` actually emits.
    records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    cap_log = logging.getLogger("pyweather.check.persist-capture")
    cap_log.handlers = [_Capture()]
    cap_log.setLevel(logging.WARNING)
    cap_log.propagate = False

    http_fail = FakeHttp(
        states_response(fixtures.station_states("istation01")),
        states_response(fixtures.station_states("istation01")),
    )
    sup_fail = FakeSupervisorSelfClient(fail_with=Exception("persist 403"))
    result_fail = run(_api(http_fail), sup_fail, FakeSleeper(), cfg, log=cap_log)
    warn_msg = " ".join(records)

    checks: list[tuple[str, bool]] = [
        (
            "persist success ⇒ resolved list returned",
            result_ok is not None and {s.key for s in result_ok} == {"istation01"},
        ),
        (
            "persist success ⇒ set_options called exactly once",
            len(sup_ok.options_calls) == 1,
        ),
        (
            "persist FAILURE ⇒ resolved list still returned (never aborts)",
            result_fail is not None and {s.key for s in result_fail} == {"istation01"},
        ),
        (
            "persist FAILURE ⇒ set_options was attempted once",
            len(sup_fail.options_calls) == 1,
        ),
        (
            "persist FAILURE ⇒ WARNING names the Configuration tab",
            "Configuration tab" in warn_msg,
        ),
        (
            "persist FAILURE ⇒ WARNING includes a rendered stations: block with the discovered row",
            "stations:" in warn_msg
            and "key: istation01" in warn_msg
            and "update_entity: sensor.wu_temp_istation01" in warn_msg,
        ),
    ]
    return report("PERSIST-BEST-EFFORT", "persist-best-effort", checks)


def check_skipped_entity_warnings() -> bool:
    """Assert the impure skipped-entity WARNING fires on BOTH the first and confirmation reads.

    The skip TELEMETRY is produced purely by `discover_stations`
    (`skipped_entity_ids`), but the operator-facing WARNING is emitted by the
    IMPURE `_discover_and_persist` caller — once per skipped id on the first read,
    and again for any non-conforming representative that surfaces only on the
    confirmation read. Those two `log.warning(...)` branches are otherwise pinned
    by nothing (the pure-transform checks assert `skipped_entity_ids`, not the
    log), so deleting either branch would silently pass. This drives the REAL
    `_discover_and_persist` through a dedicated WARNING capture logger (mirroring
    `check_persist_best_effort`) and asserts the captured text names BOTH skipped
    ids and the lowercase-alphanumeric contract.

    First read: a conforming `istation01` (so `stations` is truthy ⇒ the
    confirmation path is reached and persist is attempted) ALONGSIDE a
    non-conforming `sensor.wu_temp_back_yard` (underscore suffix). Confirmation
    read: keeps `istation01` and introduces a DIFFERENT non-conforming
    `sensor.wu_temp_UPPER` (uppercase) that surfaces only on the second read — so
    asserting on it pins the confirmation-read branch specifically.
    """
    run = _discover_and_persist()
    cfg = _discovered_cfg()

    records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    cap_log = logging.getLogger("pyweather.check.skip-capture")
    cap_log.handlers = [_Capture()]
    cap_log.setLevel(logging.WARNING)
    cap_log.propagate = False

    first_read = fixtures.station_states("istation01") + [
        {
            "entity_id": "sensor.wu_temp_back_yard",
            "state": "10.0",
        },  # underscore ⇒ skipped (read 1)
    ]
    confirm_read = fixtures.station_states("istation01") + [
        {
            "entity_id": "sensor.wu_temp_UPPER",
            "state": "11.0",
        },  # uppercase ⇒ skipped (read 2 only)
    ]
    http_skip = FakeHttp(states_response(first_read), states_response(confirm_read))
    result_skip = run(
        _api(http_skip), FakeSupervisorSelfClient(), FakeSleeper(), cfg, log=cap_log
    )
    warn_msg = " ".join(records)

    checks: list[tuple[str, bool]] = [
        (
            "skip-warning ⇒ discovery still resolves the conforming station",
            result_skip is not None and {s.key for s in result_skip} == {"istation01"},
        ),
        (
            "first-read skip ⇒ WARNING names sensor.wu_temp_back_yard",
            "sensor.wu_temp_back_yard" in warn_msg,
        ),
        (
            "confirm-only skip ⇒ WARNING names sensor.wu_temp_UPPER (pins the confirmation-read branch)",
            "sensor.wu_temp_UPPER" in warn_msg,
        ),
        (
            "skip WARNING names the lowercase-alphanumeric contract",
            "lowercase-alphanumeric" in warn_msg,
        ),
    ]
    return report("SKIPPED-ENTITY-WARNINGS", "skipped-entity-warnings", checks)


def check_run_startup_branches() -> bool:
    """Assert run_startup's non-empty (manual) vs empty (discover) branch contract.

    Non-empty (the feature's main safety boundary): no /states GET, zero
    set_options, `_discover_and_persist` never entered (make_api never built), and
    make_scheduler called EXACTLY ONCE with the supplied cfg UNCHANGED (manual
    stations straight through, not via _replace). Empty: make_scheduler called
    once with the RESOLVED discovered stations (via _replace). Empty + stop
    (None): NO make_scheduler construction, zero set_options.
    """
    # Lazy import (mirrors the _discover_and_persist()/_max_discovery_attempts()
    # helpers): a MODULE-SCOPE `from ..__main__ import StartupDeps, run_startup`
    # would raise ImportError while `check/__init__.py` imports `startup_checks`
    # — i.e. before `run_check()` runs — so `_guarded` could never fold it to a
    # FAIL (it aborts the whole --check). Importing here makes the not-yet-defined
    # symbol raise DURING the guarded call, which `_guarded` catches as a FAIL.
    from ..__main__ import StartupDeps, run_startup
    from ..httpclient import HttpResponse
    from ..scheduler import SchedulerRunner

    checks: list[tuple[str, bool]] = []

    class _RecordingScheduler:
        """A `SchedulerRunner` stand-in whose `run_loop` is a no-op (records nothing to run)."""

        def run_loop(self) -> None:
            return None

    class _SchedulerFactory:
        """A recording `make_scheduler`: records each (cfg) it was constructed with."""

        def __init__(self) -> None:
            self.built_with: list[Config] = []

        def __call__(self, cfg: Config, sleeper: object) -> SchedulerRunner:
            self.built_with.append(cfg)
            # The oracle never runs the real loop; a structural stand-in suffices.
            # `_RecordingScheduler` implements `run_loop(self) -> None`, so it
            # satisfies `SchedulerRunner` directly — no cast, no `# type: ignore`.
            return _RecordingScheduler()

    class _ApiFactory:
        """A recording `make_api`: records whether the empty branch built an api."""

        def __init__(self, http: FakeHttp) -> None:
            self._http = http
            self.built = 0

        def __call__(self) -> HaApiClient:
            self.built += 1
            return _api(self._http)

    # --- non-empty: manual path, nothing new runs ---------------------------
    manual_cfg = config.validate(
        fixtures.default_options(
            stations=[
                {
                    "key": "istation01",
                    "update_entity": "sensor.wu_temp_istation01",
                    "expected_sensors": 10,
                }
            ]
        )
    )
    http_manual = FakeHttp(states_response([]))
    api_factory = _ApiFactory(http_manual)
    sup_manual = FakeSupervisorSelfClient()
    sched_factory = _SchedulerFactory()
    deps_manual = StartupDeps(
        make_api=api_factory,
        make_supervisor=lambda: sup_manual,
        make_sleeper=lambda: FakeSleeper(),
        make_scheduler=sched_factory,
        install_signals=lambda _sleeper: None,  # type: ignore[unknown-lambda]
    )
    rc_manual = run_startup(manual_cfg, deps_manual, log=_LOG)
    checks += [
        ("non-empty ⇒ run_startup returns 0", rc_manual == 0),
        ("non-empty ⇒ no /states GET (FakeHttp untouched)", http_manual.calls == []),
        ("non-empty ⇒ zero set_options calls", sup_manual.options_calls == []),
        (
            "non-empty ⇒ _discover_and_persist never entered (make_api never built)",
            api_factory.built == 0,
        ),
        (
            "non-empty ⇒ make_scheduler called exactly once",
            len(sched_factory.built_with) == 1,
        ),
        (
            "non-empty ⇒ Scheduler built with the supplied cfg UNCHANGED (no _replace)",
            sched_factory.built_with[0] is manual_cfg,
        ),
    ]

    # --- empty: discover ⇒ Scheduler built with the RESOLVED stations -------
    # This arm ALSO pins the install-before-first-read ordering (design spec:
    # `install_signals(sleeper)` BEFORE the first discovery `/states` read). The
    # recorder is built entirely in the offline harness — no real signals: the
    # `install_signals` fake appends "install", a thin recording `FakeHttp`
    # appends "get" on the first `request(...)`, both into one shared `events`
    # list, and the ordering assertion proves install precedes the first GET.
    empty_cfg = _discovered_cfg()
    events: list[str] = []

    class _OrderRecordingHttp(FakeHttp):
        """A `FakeHttp` that records the first `request(...)` as a "get" event.

        Delegates to the real `FakeHttp.request` for the scripted response/recording;
        appends "get" to the shared `events` list exactly once (the first discovery
        `/states` read) so the install-vs-first-read order is observable offline.
        """

        def request(self, *args: object, **kwargs: object) -> HttpResponse:
            if "get" not in events:
                events.append("get")
            return super().request(*args, **kwargs)  # type: ignore[arg-type]

    http_empty = _OrderRecordingHttp(
        states_response(fixtures.station_states("istation01")),
        states_response(fixtures.station_states("istation01")),
    )
    api_factory2 = _ApiFactory(http_empty)
    sup_empty = FakeSupervisorSelfClient()
    sched_factory2 = _SchedulerFactory()
    deps_empty = StartupDeps(
        make_api=api_factory2,
        make_supervisor=lambda: sup_empty,
        make_sleeper=lambda: FakeSleeper(),
        make_scheduler=sched_factory2,
        install_signals=lambda _sleeper: events.append("install"),  # type: ignore[unknown-lambda]
    )
    rc_empty = run_startup(empty_cfg, deps_empty, log=_LOG)
    checks += [
        ("empty ⇒ run_startup returns 0", rc_empty == 0),
        ("empty ⇒ make_api built (discover path entered)", api_factory2.built == 1),
        (
            "empty ⇒ make_scheduler called exactly once",
            len(sched_factory2.built_with) == 1,
        ),
        (
            "empty ⇒ Scheduler built with the RESOLVED discovered stations (via _replace)",
            len(sched_factory2.built_with) == 1
            and {s.key for s in sched_factory2.built_with[0].stations}
            == {"istation01"},
        ),
        ("empty ⇒ persisted exactly once", len(sup_empty.options_calls) == 1),
        (
            "empty ⇒ install_signals ran BEFORE the first discovery /states read",
            "install" in events
            and "get" in events
            and events.index("install") < events.index("get"),
        ),
    ]

    # --- empty + stop (None) ⇒ no Scheduler construction --------------------
    stop_cfg = _discovered_cfg()
    http_stop = FakeHttp(states_response([]))
    sup_stop = FakeSupervisorSelfClient()
    sched_factory3 = _SchedulerFactory()
    deps_stop = StartupDeps(
        make_api=lambda: _api(http_stop),
        make_supervisor=lambda: sup_stop,
        make_sleeper=lambda: FakeSleeper(stop_at=0),  # pre-read gate fires ⇒ None
        make_scheduler=sched_factory3,
        install_signals=lambda _sleeper: None,  # type: ignore[unknown-lambda]
    )
    rc_stop = run_startup(stop_cfg, deps_stop, log=_LOG)
    checks += [
        ("empty + stop ⇒ run_startup returns 0 (clean shutdown)", rc_stop == 0),
        ("empty + stop ⇒ NO Scheduler construction", sched_factory3.built_with == []),
        ("empty + stop ⇒ zero set_options calls", sup_stop.options_calls == []),
    ]

    # --- empty + fatal (cap exhaustion) ⇒ SystemExit, no Scheduler ----------
    fatal_cfg = _discovered_cfg()
    sched_factory4 = _SchedulerFactory()
    sup_fatal = FakeSupervisorSelfClient()
    deps_fatal = StartupDeps(
        make_api=lambda: _api(FakeHttp(states_response([]))),
        make_supervisor=lambda: sup_fatal,
        make_sleeper=lambda: FakeSleeper(),  # never stops ⇒ exhausts cap
        make_scheduler=sched_factory4,
        install_signals=lambda _sleeper: None,  # type: ignore[unknown-lambda]
    )
    raised_fatal = False
    try:
        run_startup(fatal_cfg, deps_fatal, log=_LOG)
    except SystemExit as exc:
        raised_fatal = exc.code != 0
    checks += [
        ("empty + fatal ⇒ raises SystemExit non-zero", raised_fatal),
        ("empty + fatal ⇒ NO Scheduler construction", sched_factory4.built_with == []),
        ("empty + fatal ⇒ zero set_options calls", sup_fatal.options_calls == []),
    ]
    return report("RUN-STARTUP", "run-startup", checks)

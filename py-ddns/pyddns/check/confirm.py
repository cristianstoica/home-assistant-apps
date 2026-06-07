# pyright: strict
"""Callback-confirmation + startup-self-heal checks (assert on the STATE seam).

These are the heart of the revised ``url`` archetype: a fire is confirmed by a
**post-fire resolve** whose three-way outcome gates persistence, and a confirmed
steady state **suppresses** the next fire. Every assertion reads the `FakeState`
record (``value`` / ``writes``) — the state seam is the contract surface.
"""

from __future__ import annotations

from ipaddress import IPv4Address

from ..models import (
    ApplyAction,
    ApplyResult,
    AzureToken,
    Config,
    Provider,
    ResolveOutcome,
    ResolveStatus,
)
from ..resolver import DnsParseError
from ..updater import Updater
from .fakes import (
    FakeClock,
    FakeIpSource,
    FakeProvider,
    FakeResolver,
    FakeSleeper,
    FakeState,
    with_recording_handler,
)
from .report import report


class _RaisingResolver:
    """A `Resolver` whose ``resolve`` raises `DnsParseError` (Part B/4 driver).

    Stands in for a malformed `name` that slipped past config validation: the
    post-fire resolve inside `_fire_and_confirm` raises, and the contract is that
    `run_once` swallows it (holds the cycle) rather than letting it escape to s6.
    """

    def __init__(self) -> None:
        self.calls = 0

    def resolve(self, name: str) -> ResolveOutcome:
        self.calls += 1
        raise DnsParseError(f"label too long in {name!r}")


_IP_NEW = IPv4Address("203.0.113.50")
_IP_OLD = IPv4Address("203.0.113.10")


def _url_config() -> Config:
    """A minimal valid ``url`` Config for the updater (seams are all injected)."""
    return Config(
        provider=Provider.URL,
        name="home.example.com",
        test_ns="",
        azure=None,
        record_label="",
        url_endpoint="https://dynamicdns.example.com/update/secret",
        url_send_myip=False,
        ttl=60,
        interval_seconds=120,
        drift_reconcile_seconds=0,
        ip_source_urls=("https://api.ipify.org",),
        log_level="info",
        state_path="/data/last_known_ip",
    )


def _azure_config(*, drift_reconcile_seconds: int = 0) -> Config:
    """A minimal valid ``azure`` Config for the API-archetype updater checks."""
    return Config(
        provider=Provider.AZURE,
        name="home.example.com",
        test_ns="",
        azure=AzureToken(
            tenant_id="t",
            subscription_id="sub",
            resource_group="rg",
            zone="example.com",
            client_id="cid",
            client_secret="EXAMPLE~secret~value~do~not~use~0000",
        ),
        record_label="home",
        url_endpoint="",
        url_send_myip=False,
        ttl=60,
        interval_seconds=120,
        drift_reconcile_seconds=drift_reconcile_seconds,
        ip_source_urls=("https://api.ipify.org",),
        log_level="info",
        state_path="/data/last_known_ip",
    )


def _fire_result() -> ApplyResult:
    return ApplyResult(ApplyAction.FIRED_SERVER_DETECTED, "fired", None)


def _make_updater(
    *,
    state: FakeState,
    resolver: FakeResolver,
    ip_source: FakeIpSource,
    provider: FakeProvider,
) -> Updater:
    # The fakes are structural matches for the Protocols; the updater only calls
    # the seam methods, so the duck-typed fakes satisfy it at runtime.
    return Updater(
        _url_config(),
        ip_source=ip_source,  # type: ignore[arg-type]
        provider=provider,  # type: ignore[arg-type]
        resolver=resolver,  # type: ignore[arg-type]
        state=state,  # type: ignore[arg-type]
        clock=FakeClock(),
        sleeper=FakeSleeper(),
    )


def _make_api_updater(
    *,
    config: Config,
    state: FakeState,
    ip_source: FakeIpSource,
    provider: FakeProvider,
    clock: FakeClock,
) -> Updater:
    """Build an ``azure``-archetype updater (the resolver is unused by `_cycle_api`)."""
    return Updater(
        config,
        ip_source=ip_source,  # type: ignore[arg-type]
        provider=provider,  # type: ignore[arg-type]
        resolver=FakeResolver(),  # type: ignore[arg-type]  # unused on the API path
        state=state,  # type: ignore[arg-type]
        clock=clock,  # type: ignore[arg-type]
        sleeper=FakeSleeper(),
    )


def check_callback_confirmation() -> bool:
    """Assert the three confirmation outcomes + steady-state suppression on STATE.

    1. **Confirmed** — fire, then `name` resolves to a concrete value: persist
       that resolved value; the *next* cycle (which still resolves to it)
       **suppresses** the fire (no second ``apply``).
    2. **Unconfirmed** — fire, then `name` resolves NO_RECORD (DNS not yet
       updated): **do not** persist, log the distinct *unconfirmed* diagnostic;
       the next cycle **refires**.
    3. **Inconclusive** — fire, then the resolve FAILs (transient): retry within
       budget, then **hold** last-known unchanged (no persist, no clear), log the
       distinct *inconclusive* diagnostic.
    """
    checks: list[tuple[str, bool]] = []

    # --- 1. Confirmed -> persist + suppress next ---
    confirmed_state = FakeState()  # empty last-known
    confirmed_resolver = FakeResolver(
        ResolveOutcome(ResolveStatus.RESOLVED, _IP_NEW),  # post-fire confirm
        ResolveOutcome(ResolveStatus.RESOLVED, _IP_NEW),  # next cycle steady
    )
    confirmed_provider = FakeProvider(apply_result=_fire_result())
    confirmed = _make_updater(
        state=confirmed_state,
        resolver=confirmed_resolver,
        ip_source=FakeIpSource(_IP_NEW, _IP_NEW),
        provider=confirmed_provider,
    )
    confirmed.run_once()  # first cycle: authoritative -> fire -> confirm
    after_fire_writes = list(confirmed_state.writes)
    confirmed.run_once()  # second cycle: steady -> suppress
    checks += [
        ("confirmed: persisted the RESOLVED value", after_fire_writes == [_IP_NEW]),
        ("confirmed: state holds the resolved value", confirmed_state.value == _IP_NEW),
        (
            "confirmed: fired exactly once (next cycle suppressed)",
            len(confirmed_provider.apply_calls) == 1,
        ),
    ]

    # --- 2. Unconfirmed -> no persist + refire + diagnostic ---
    unconfirmed_state = FakeState(initial=_IP_OLD)
    unconfirmed_resolver = FakeResolver(
        ResolveOutcome(ResolveStatus.NO_RECORD, None),  # post-fire: DNS not updated
        ResolveOutcome(ResolveStatus.NO_RECORD, None),  # next cycle pre-fire check
    )
    unconfirmed_provider = FakeProvider(apply_result=_fire_result())
    unconfirmed = _make_updater(
        state=unconfirmed_state,
        resolver=unconfirmed_resolver,
        ip_source=FakeIpSource(_IP_NEW, _IP_NEW),
        provider=unconfirmed_provider,
    )
    unconfirmed_msgs = with_recording_handler(lambda _h: unconfirmed.run_once())
    after_unconfirmed_writes = list(unconfirmed_state.writes)
    unconfirmed.run_once()  # next cycle: still no record -> refire
    checks += [
        ("unconfirmed: did NOT persist", after_unconfirmed_writes == []),
        ("unconfirmed: last-known held unchanged", unconfirmed_state.value == _IP_OLD),
        (
            "unconfirmed: logged the 'unconfirmed' diagnostic",
            any("unconfirmed" in m for m in unconfirmed_msgs),
        ),
        (
            "unconfirmed: refired on the next cycle",
            len(unconfirmed_provider.apply_calls) == 2,
        ),
    ]

    # --- 3. Inconclusive -> retry within budget, hold unchanged + diagnostic ---
    inconclusive_state = FakeState(initial=_IP_OLD)
    inconclusive_resolver = FakeResolver(
        ResolveOutcome(ResolveStatus.FAILED, None),  # post-fire resolve fails...
        ResolveOutcome(ResolveStatus.FAILED, None),  # ...and the retries also fail
        ResolveOutcome(ResolveStatus.FAILED, None),
    )
    inconclusive_provider = FakeProvider(apply_result=_fire_result())
    inconclusive = _make_updater(
        state=inconclusive_state,
        resolver=inconclusive_resolver,
        ip_source=FakeIpSource(_IP_NEW),
        provider=inconclusive_provider,
    )
    inconclusive_msgs = with_recording_handler(lambda _h: inconclusive.run_once())
    checks += [
        ("inconclusive: did NOT persist", inconclusive_state.writes == []),
        (
            "inconclusive: last-known held unchanged (not cleared)",
            inconclusive_state.value == _IP_OLD,
        ),
        (
            "inconclusive: retried the confirmation resolve within budget",
            inconclusive_resolver.calls >= 2,
        ),
        (
            "inconclusive: logged the 'inconclusive' diagnostic",
            any("inconclusive" in m for m in inconclusive_msgs),
        ),
    ]

    # --- 4. Steady state broken by drift: pre-fire resolve returns Y != X (GAP 2).
    # last-known persisted = X, detected == X (so the detected-change trigger does
    # NOT fire and `detected == last_known` holds), but a later cycle's pre-fire
    # resolve returns RESOLVED -> Y with Y != X. The suppression guard's
    # `outcome.value == last_known` must FAIL, so the cycle re-fires. The
    # discriminator is pinned below: with Y == X the same harness suppresses, so
    # the `== last_known` comparison is what gates it, not a bare `is RESOLVED`.
    refire_state = FakeState(initial=_IP_OLD)  # last-known X
    refire_resolver = FakeResolver(
        ResolveOutcome(
            ResolveStatus.RESOLVED, _IP_NEW
        ),  # pre-fire: now resolves Y != X
        ResolveOutcome(ResolveStatus.RESOLVED, _IP_NEW),  # post-fire confirm -> Y
    )
    refire_provider = FakeProvider(apply_result=_fire_result())
    refire = _make_updater(
        state=refire_state,
        resolver=refire_resolver,
        ip_source=FakeIpSource(_IP_OLD, _IP_OLD),  # detected == last-known X
        provider=refire_provider,
    )
    refire.mark_started()  # not the startup self-heal: steady-state cycle
    refire.run_once()

    suppress_state = FakeState(initial=_IP_OLD)  # same harness, but Y == X
    suppress_resolver = FakeResolver(
        ResolveOutcome(ResolveStatus.RESOLVED, _IP_OLD),  # pre-fire: still resolves X
    )
    suppress_provider = FakeProvider(apply_result=_fire_result())
    suppress = _make_updater(
        state=suppress_state,
        resolver=suppress_resolver,
        ip_source=FakeIpSource(_IP_OLD, _IP_OLD),
        provider=suppress_provider,
    )
    suppress.mark_started()
    suppress.run_once()

    checks += [
        (
            "drift re-fire: resolve Y != last-known re-fires despite populated state",
            len(refire_provider.apply_calls) == 1,
        ),
        (
            "drift re-fire: discriminator — resolve == last-known suppresses (no fire)",
            len(suppress_provider.apply_calls) == 0,
        ),
    ]
    return report("CALLBACK-CONFIRM", "confirm", checks)


def check_startup_self_heal() -> bool:
    """Assert the first cycle is authoritative regardless of local state.

    A ``url`` updater with a populated last-known but ``drift_reconcile_seconds
    == 0`` must still fire on the **first** cycle (the startup self-heal): local
    state never suppresses the first cycle. With the same state on a *subsequent*
    cycle where `name` resolves to last-known, the fire is suppressed.
    """
    checks: list[tuple[str, bool]] = []

    state = FakeState(initial=_IP_OLD)
    # First cycle: even though state == last-known and name resolves to it, the
    # first cycle is authoritative -> it must fire.
    resolver = FakeResolver(
        ResolveOutcome(ResolveStatus.RESOLVED, _IP_OLD),  # post-fire confirm
        ResolveOutcome(ResolveStatus.RESOLVED, _IP_OLD),  # next cycle steady check
    )
    provider = FakeProvider(apply_result=_fire_result())
    updater = _make_updater(
        state=state,
        resolver=resolver,
        ip_source=FakeIpSource(_IP_OLD, _IP_OLD),
        provider=provider,
    )
    updater.run_once()  # first cycle -> authoritative -> fires despite matching state
    fired_on_first = len(provider.apply_calls) == 1
    updater.run_once()  # steady -> suppress
    checks += [
        (
            "startup self-heal: first cycle fires despite matching local state",
            fired_on_first,
        ),
        (
            "startup self-heal: subsequent steady cycle suppresses the fire",
            len(provider.apply_calls) == 1,
        ),
    ]
    return report("STARTUP-SELF-HEAL", "self-heal", checks)


def _wrote_result() -> ApplyResult:
    return ApplyResult(ApplyAction.WROTE_KNOWN_IP, "wrote", None)


def check_api_reconcile() -> bool:
    """Drive the API archetype (`azure`) through the Updater's `_cycle_api` branches.

    Closes the gap where the only Updater-level check used Provider.URL, leaving
    `_cycle_api` with no Updater coverage. Each branch is an isolated updater
    asserting on the FakeState / FakeProvider record:

    * **startup self-heal** — first cycle, ``read_current() -> None``, detected X:
      authoritative read, ``apply(X)``, persist X (heals a missing record on boot).
    * **steady no-write** — subsequent non-authoritative cycle, ``state == detected``:
      no ``read_current``, no ``apply``, no write.
    * **write-on-change** — non-authoritative, ``state != detected`` (Y): ``apply(Y)``
      then persist Y.
    * **skip-no-IP** — ``detect() -> None``: no ``apply``, state held unchanged.
    * **drift re-assert** — clock advanced past ``drift_reconcile_seconds``:
      authoritative ``read_current`` re-read + re-assert even when unchanged.
    """
    checks: list[tuple[str, bool]] = []

    # --- startup self-heal: read_current None + detected X -> apply X, persist X.
    heal_state = FakeState()  # empty last-known
    heal_provider = FakeProvider(read_result=None, apply_result=_wrote_result())
    heal = _make_api_updater(
        config=_azure_config(),
        state=heal_state,
        ip_source=FakeIpSource(_IP_NEW),
        provider=heal_provider,
        clock=FakeClock(),
    )
    heal.run_once()  # first cycle -> authoritative
    checks += [
        (
            "api self-heal: first cycle read authoritative current",
            heal_provider.read_calls == 1,
        ),
        (
            "api self-heal: applied the detected IP",
            heal_provider.apply_calls == [_IP_NEW],
        ),
        ("api self-heal: persisted the detected IP", heal_state.writes == [_IP_NEW]),
    ]

    # --- steady no-write: non-authoritative, state == detected -> no apply/no write.
    steady_state = FakeState(initial=_IP_NEW)
    steady_provider = FakeProvider(read_result=_IP_NEW, apply_result=_wrote_result())
    steady = _make_api_updater(
        config=_azure_config(),
        state=steady_state,
        ip_source=FakeIpSource(_IP_NEW),
        provider=steady_provider,
        clock=FakeClock(),
    )
    steady.mark_started()  # not the startup self-heal
    steady.run_once()
    checks += [
        (
            "api steady: no apply when state == detected",
            steady_provider.apply_calls == [],
        ),
        (
            "api steady: no authoritative read on a steady cycle",
            steady_provider.read_calls == 0,
        ),
        ("api steady: no write on a steady cycle", steady_state.writes == []),
    ]

    # --- write-on-change: non-authoritative, state X != detected Y -> apply Y, persist Y.
    change_state = FakeState(initial=_IP_OLD)
    change_provider = FakeProvider(read_result=_IP_OLD, apply_result=_wrote_result())
    change = _make_api_updater(
        config=_azure_config(),
        state=change_state,
        ip_source=FakeIpSource(_IP_NEW),  # detected Y != last-known X
        provider=change_provider,
        clock=FakeClock(),
    )
    change.mark_started()
    change.run_once()
    checks += [
        (
            "api change: applied the changed IP",
            change_provider.apply_calls == [_IP_NEW],
        ),
        ("api change: persisted the changed IP", change_state.writes == [_IP_NEW]),
    ]

    # --- skip-no-IP: detect() None -> no apply, state held unchanged (not cleared).
    skip_state = FakeState(initial=_IP_OLD)
    skip_provider = FakeProvider(read_result=_IP_OLD, apply_result=_wrote_result())
    skip = _make_api_updater(
        config=_azure_config(),
        state=skip_state,
        ip_source=FakeIpSource(None),  # no valid egress IP this cycle
        provider=skip_provider,
        clock=FakeClock(),
    )
    skip.mark_started()
    skip.run_once()
    checks += [
        ("api skip-no-IP: no apply when no egress IP", skip_provider.apply_calls == []),
        (
            "api skip-no-IP: no authoritative read on a held cycle",
            skip_provider.read_calls == 0,
        ),
        (
            "api skip-no-IP: last-known held unchanged (not cleared)",
            skip_state.value == _IP_OLD,
        ),
        ("api skip-no-IP: no write on a held cycle", skip_state.writes == []),
    ]

    # --- drift re-assert: clock advanced past drift -> authoritative re-read even
    # when unchanged (current == detected) -> re-assert write of the matching IP.
    drift_clock = FakeClock()
    drift_state = FakeState(initial=_IP_NEW)
    drift_provider = FakeProvider(read_result=_IP_NEW, apply_result=_wrote_result())
    drift = _make_api_updater(
        config=_azure_config(drift_reconcile_seconds=100),
        state=drift_state,
        ip_source=FakeIpSource(_IP_NEW),
        provider=drift_provider,
        clock=drift_clock,
    )
    drift.mark_started()  # NOT the startup self-heal; drift must drive the read
    drift_clock.advance(200)  # past the 100s drift cadence
    drift.run_once()
    checks += [
        (
            "api drift: authoritative re-read fired when drift cadence elapsed",
            drift_provider.read_calls == 1,
        ),
        (
            "api drift: re-asserted the matching IP to state",
            drift_state.writes == [_IP_NEW],
        ),
        (
            "api drift: no apply needed when current already matches",
            drift_provider.apply_calls == [],
        ),
    ]
    return report("API-RECONCILE", "api", checks)


def check_run_once_never_raises() -> bool:
    """Pin the s6 "Never raises" contract on the integration path (Part B/4).

    A `url` updater whose post-fire resolve raises `DnsParseError` (a malformed
    `name` that slipped past config validation) must not propagate out of
    `run_once`: the defensive final clause in `run_once` catches it and holds the
    cycle. Driving the real `Updater.run_once()` (not the clause in isolation)
    proves the contract on the path s6 actually runs. Asserts the resolve was
    reached (so the exception genuinely came from the seam, not a no-op).
    """
    checks: list[tuple[str, bool]] = []

    raising = _RaisingResolver()
    provider = FakeProvider(apply_result=_fire_result())
    updater = _make_updater(
        state=FakeState(),  # empty -> first cycle authoritative -> fire+confirm
        resolver=raising,  # type: ignore[arg-type]  # structural Resolver match
        ip_source=FakeIpSource(_IP_NEW),
        provider=provider,
    )

    propagated = False
    try:
        updater.run_once()
    except Exception:  # noqa: BLE001 - the check is that run_once does NOT raise
        propagated = True

    checks += [
        ("run_once swallows a DnsParseError from resolve (no escape)", not propagated),
        ("run_once reached the raising resolve seam", raising.calls >= 1),
    ]
    return report("RUN-ONCE-CONTRACT", "run-once", checks)

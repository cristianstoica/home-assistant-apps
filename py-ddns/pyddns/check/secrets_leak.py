# pyright: strict
"""No-secret-leakage check: a secret must never reach a log line or an error string.

Forces the failure modes that can echo a secret — a ``urllib`` ``HTTPError`` /
``URLError`` whose string carries the secret callback URL, and an Azure auth
failure whose body carries the client secret — and asserts the secret substring
appears in **no** captured output: not the sanitized `HttpError` message, not the
domain `TerminalError`/`TransientError` the provider raises, not any line logged
on the ``pyddns`` logger, and not the redacted plan/detail strings.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from ipaddress import IPv4Address

from .. import fixtures
from ..errors import TerminalError, TransientError
from ..httpclient import HttpError, UrllibHttpClient
from ..models import (
    ApplyAction,
    ApplyResult,
    AzureToken,
    Config,
    Provider,
    ResolveOutcome,
    ResolveStatus,
)
from ..providers import plan_provider
from ..providers.azure import AzureProvider
from ..providers.url import UrlProvider, compose_fire_url
from ..redact import redact_url, sanitize
from ..runtime import monotonic
from ..updater import Updater
from .fakes import (
    FakeClock,
    FakeHttp,
    FakeIpSource,
    FakeProvider,
    FakeResolver,
    FakeSleeper,
    FakeState,
    with_recording_handler,
)
from .report import report

_SECRETS = (
    fixtures.EXAMPLE_URL_SECRET,
    fixtures.EXAMPLE_CLIENT_SECRET,
    fixtures.EXAMPLE_URL_ENDPOINT,
)


def _leaks(text: str) -> bool:
    """True if any tracked secret substring appears in `text`."""
    return any(secret and secret in text for secret in _SECRETS)


def check_no_secret_leakage() -> bool:
    """Assert no secret leaks through any of the secret-bearing failure paths."""
    checks: list[tuple[str, bool]] = []
    captured: list[str] = []

    def _record(text: str) -> None:
        captured.append(text)

    # --- 1. redact_url drops the secret path, keeps only scheme+host ---
    red = redact_url(fixtures.EXAMPLE_URL_ENDPOINT)
    _record(red)
    checks.append(("redact_url(endpoint) leaks no secret", not _leaks(red)))

    # redact_url's malformed/hostless fallback returns <redacted-url> (part of the
    # no-secret-leak guarantee: never echo a URL it could not safely mask) (GAP 5).
    malformed = redact_url("not a url")
    hostless = redact_url("https:///nopath")
    _record(malformed)
    _record(hostless)
    checks += [
        (
            "redact_url(unparseable) returns the safe fallback",
            malformed == "<redacted-url>",
        ),
        (
            "redact_url(hostless) returns the safe fallback",
            hostless == "<redacted-url>",
        ),
    ]

    # --- 2. sanitize() scrubs an echoed secret URL out of an exception string ---
    raw = f"connection refused to {fixtures.EXAMPLE_URL_ENDPOINT}"
    scrubbed = sanitize(raw, (fixtures.EXAMPLE_URL_ENDPOINT,))
    _record(scrubbed)
    checks.append(("sanitize() scrubs an echoed secret URL", not _leaks(scrubbed)))

    # --- 3. UrllibHttpClient sanitizes a URLError whose reason echoes the URL ---
    # Patch urlopen at the urllib boundary to raise a URLError carrying the secret
    # URL in its reason, exercising the real client's sanitize path (no network).
    client = UrllibHttpClient()
    original_urlopen = urllib.request.urlopen

    def _raise_urlerror(*_args: object, **_kwargs: object) -> object:
        raise urllib.error.URLError(f"refused: {fixtures.EXAMPLE_URL_ENDPOINT}")

    setattr(urllib.request, "urlopen", _raise_urlerror)
    try:
        client.request("GET", fixtures.EXAMPLE_URL_ENDPOINT, timeout=1.0)
    except HttpError as exc:
        _record(str(exc))
        checks.append(
            ("UrllibHttpClient URLError message leaks no secret", not _leaks(str(exc)))
        )
    else:
        checks.append(("UrllibHttpClient raised on URLError", False))
    finally:
        setattr(urllib.request, "urlopen", original_urlopen)

    # --- 4. URL provider terminal/transient errors leak no secret ---
    url_http = FakeHttp(HttpError("GET", status=404))
    url_provider = UrlProvider(fixtures.EXAMPLE_URL_ENDPOINT, True, url_http)
    try:
        url_provider.apply(IPv4Address("203.0.113.7"))
    except (TerminalError, TransientError) as exc:
        _record(str(exc))
        checks.append(
            ("UrlProvider error message leaks no secret", not _leaks(str(exc)))
        )
    else:
        checks.append(("UrlProvider raised on 4xx", False))

    # The composed fire URL itself necessarily contains the secret (it is the
    # request target) — but the provider's *log detail* must not. Assert the
    # redacted detail is secret-free while proving the live URL still carries it.
    live_url = compose_fire_url(
        fixtures.EXAMPLE_URL_ENDPOINT, IPv4Address("203.0.113.7"), True
    )
    _record(url_provider.plan(IPv4Address("203.0.113.7")))
    checks.append(
        (
            "UrlProvider.plan() leaks no secret",
            not _leaks(url_provider.plan(IPv4Address("203.0.113.7"))),
        )
    )
    checks.append(
        (
            "(sanity) the live fire URL does carry the secret",
            fixtures.EXAMPLE_URL_SECRET in live_url,
        )
    )

    # --- 5. Azure auth-failure message scrubs the client secret ---
    token = AzureToken(
        tenant_id="t",
        subscription_id="sub",
        resource_group="rg",
        zone="example.com",
        client_id="cid",
        client_secret=fixtures.EXAMPLE_CLIENT_SECRET,
    )
    auth_body = json.dumps(
        {
            "error": "invalid_client",
            "error_description": f"AADSTS7000222 secret {fixtures.EXAMPLE_CLIENT_SECRET} expired",
        }
    )
    azure_http = FakeHttp(HttpError("POST", status=401, body=auth_body))
    azure = AzureProvider(token, "home", 60, azure_http, FakeClock())
    try:
        azure.read_current()
    except TerminalError as exc:
        _record(str(exc))
        checks.append(
            (
                "Azure auth-failure message scrubs the client secret",
                not _leaks(str(exc)),
            )
        )
        checks.append(
            (
                "Azure auth-failure still surfaces AADSTS7000222 code",
                "AADSTS7000222" in str(exc),
            )
        )
    else:
        checks.append(("Azure raised TerminalError on auth failure", False))

    _record(azure.plan(IPv4Address("203.0.113.7")))
    checks.append(
        (
            "Azure.plan() leaks no secret",
            not _leaks(azure.plan(IPv4Address("203.0.113.7"))),
        )
    )

    # --- 6. Nothing logged on the pyddns logger during a failing url cycle leaks ---
    leak_http = FakeHttp(HttpError("GET", status=503))
    leak_provider = UrlProvider(fixtures.EXAMPLE_URL_ENDPOINT, True, leak_http)

    def _run(_handler: object) -> None:
        try:
            leak_provider.apply(IPv4Address("203.0.113.7"))
        except TransientError:
            pass

    logged = with_recording_handler(_run)
    checks.append(
        (
            "no logged line during a failing url cycle leaks a secret",
            not any(_leaks(m) for m in logged),
        )
    )

    # --- 7. flag-set Updater.run_once + plan_provider leak nothing (§10(e)) ------
    # The insecure-skip surface added two new emit sites: the per-cycle WARNING in
    # the updater and the appended plan suffix. Both run with the REAL secret
    # endpoint in scope. Drive a flag-set url Updater (one suppressed steady cycle
    # + one firing cycle) and capture everything it logs; capture the flag-set
    # plan line too. Neither may carry the secret (the WARNING interpolates only
    # `name`; the suffix is a constant string).
    checks += _check_insecure_skip_surface(_record)

    # --- final aggregate: NONE of the captured strings leaked ---
    checks.append(
        (
            "aggregate: no captured output string leaked a secret",
            not any(_leaks(t) for t in captured),
        )
    )
    return report("NO-SECRET-LEAKAGE", "secret", checks)


_IP_NEW = IPv4Address("203.0.113.50")


def _insecure_url_config() -> Config:
    """A flag-set ``url`` Config carrying the *real* secret endpoint in scope."""
    return Config(
        provider=Provider.URL,
        name="home.example.com",
        test_ns="",
        azure=None,
        record_label="",
        url_endpoint=fixtures.EXAMPLE_URL_ENDPOINT,
        url_send_myip=False,
        url_insecure_skip_verify=True,
        ttl=60,
        interval_seconds=120,
        drift_reconcile_seconds=0,
        ip_source_urls=("https://api.ipify.org",),
        log_level="info",
        state_path="/data/last_known_ip",
    )


def _fired() -> ApplyResult:
    return ApplyResult(ApplyAction.FIRED_SERVER_DETECTED, "fired", None)


def _check_insecure_skip_surface(record: object) -> list[tuple[str, bool]]:
    """Drive the two new flag-set emit surfaces with the real secret in scope.

    `record` is `check_no_secret_leakage`'s ``_record`` closure (typed loosely to
    avoid threading its signature); it appends each captured string to the
    aggregate corpus the final assertion scans.
    """
    assert callable(record)
    checks: list[tuple[str, bool]] = []
    cfg = _insecure_url_config()

    # A firing first cycle: empty state -> authoritative -> fire -> confirm.
    fire_updater = Updater(
        cfg,
        ip_source=FakeIpSource(_IP_NEW),  # type: ignore[arg-type]
        provider=FakeProvider(apply_result=_fired()),  # type: ignore[arg-type]
        resolver=FakeResolver(  # type: ignore[arg-type]
            ResolveOutcome(ResolveStatus.RESOLVED, _IP_NEW)
        ),
        state=FakeState(),
        clock=FakeClock(),
        sleeper=FakeSleeper(),
    )
    fire_logged = with_recording_handler(lambda _h: fire_updater.run_once())
    for message in fire_logged:
        record(message)
    checks.append(
        (
            "flag-set firing cycle logs (incl. the WARNING) leak no secret",
            not any(_leaks(m) for m in fire_logged),
        )
    )

    # A suppressed steady cycle: last-known X, name resolves to X, detected == X.
    steady_updater = Updater(
        cfg,
        ip_source=FakeIpSource(_IP_NEW),  # type: ignore[arg-type]
        provider=FakeProvider(apply_result=_fired()),  # type: ignore[arg-type]
        resolver=FakeResolver(  # type: ignore[arg-type]
            ResolveOutcome(ResolveStatus.RESOLVED, _IP_NEW)
        ),
        state=FakeState(initial=_IP_NEW),
        clock=FakeClock(),
        sleeper=FakeSleeper(),
    )
    steady_updater.mark_started()  # non-first cycle -> suppression path
    steady_logged = with_recording_handler(lambda _h: steady_updater.run_once())
    for message in steady_logged:
        record(message)
    checks.append(
        (
            "flag-set suppressed steady cycle logs (incl. the WARNING) leak no secret",
            not any(_leaks(m) for m in steady_logged),
        )
    )

    # The flag-set plan line (with the appended suffix — the surface that changed).
    plan = plan_provider(cfg, UrllibHttpClient(), monotonic, None)
    record(plan)
    checks += [
        (
            "flag-set plan_provider line leaks no secret",
            not _leaks(plan),
        ),
        (
            "(sanity) flag-set plan line carries the DISABLED suffix",
            "TLS cert verification DISABLED" in plan,
        ),
    ]
    return checks

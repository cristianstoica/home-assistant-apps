# pyright: strict
"""Per-request status-handling checks for both provider archetypes."""

from __future__ import annotations

import json
from ipaddress import IPv4Address

from .. import fixtures
from ..errors import TerminalError, TransientError
from ..httpclient import HttpError, parse_retry_after
from ..models import ApplyAction, AzureToken
from ..providers.azure import AzureProvider
from ..providers.url import UrlProvider
from .fakes import FakeClock, FakeHttp, ok_response
from .report import report

_TOKEN_OK = json.dumps({"access_token": "fake-bearer", "expires_in": 3600})


def _token() -> AzureToken:
    return AzureToken(
        tenant_id="t",
        subscription_id="sub",
        resource_group="rg",
        zone="example.com",
        client_id="cid",
        client_secret=fixtures.EXAMPLE_CLIENT_SECRET,
    )


def _azure(http: FakeHttp) -> AzureProvider:
    return AzureProvider(_token(), "home", 60, http, FakeClock())


def _record_body(ip: str) -> str:
    return json.dumps({"properties": {"TTL": 60, "ARecords": [{"ipv4Address": ip}]}})


def check_status_handling() -> bool:  # noqa: C901 - one assertion list, many branches
    """Assert every per-request status branch the plan enumerates.

    Azure: ``Retry-After`` parsing; record ``GET 404`` → ``None`` (create path);
    token auth failure → terminal; ``429``/``5xx`` → transient; management
    ``403`` → terminal; cached-token ``401`` → re-acquire once then retry (and a
    ``401`` after a fresh token → terminal). URL: ``4xx``≠``429`` → terminal;
    ``429``/``5xx`` → transient (carrying ``Retry-After``).
    """
    checks: list[tuple[str, bool]] = []
    ip = IPv4Address("203.0.113.7")

    # --- Retry-After parsing ---
    for case in fixtures.RETRY_AFTER_HEADERS:
        checks.append(
            (
                f"retry-after {case.header!r} -> {case.expected!r}",
                parse_retry_after(case.header) == case.expected,
            )
        )

    # --- Azure: GET 404 -> None (record missing, create path) ---
    http_404 = FakeHttp(ok_response(_TOKEN_OK), HttpError("GET", status=404))
    checks.append(
        ("azure GET 404 -> read_current None", _azure(http_404).read_current() is None)
    )

    # --- Azure: GET 200 -> RESOLVED value ---
    http_get = FakeHttp(
        ok_response(_TOKEN_OK), ok_response(_record_body("198.51.100.4"))
    )
    checks.append(
        (
            "azure GET 200 -> current value",
            _azure(http_get).read_current() == IPv4Address("198.51.100.4"),
        )
    )

    # --- Azure: token auth failure -> terminal ---
    http_authfail = FakeHttp(
        HttpError("POST", status=401, body=json.dumps({"error": "invalid_client"}))
    )
    checks.append(
        (
            "azure token 401 -> TerminalError",
            _expect_terminal(lambda: _azure(http_authfail).read_current()),
        )
    )

    # --- Azure: token 5xx -> transient ---
    http_token5xx = FakeHttp(HttpError("POST", status=503, retry_after=12.0))
    checks.append(
        (
            "azure token 503 -> TransientError",
            _expect_transient(lambda: _azure(http_token5xx).read_current()),
        )
    )

    # --- Azure: management 429 -> transient (carries Retry-After) ---
    http_429 = FakeHttp(
        ok_response(_TOKEN_OK), HttpError("GET", status=429, retry_after=7.0)
    )
    checks.append(
        (
            "azure GET 429 -> TransientError(retry_after=7)",
            _expect_transient_retry(lambda: _azure(http_429).read_current(), 7.0),
        )
    )

    # --- Azure: management 403 -> terminal ---
    http_403 = FakeHttp(ok_response(_TOKEN_OK), HttpError("PUT", status=403))
    checks.append(
        (
            "azure PUT 403 -> TerminalError",
            _expect_terminal(lambda: _azure(http_403).apply(ip)),
        )
    )

    # --- Azure: cached-token 401 -> re-acquire once, then retry succeeds ---
    # Calls in order: prime token POST + GET 200, then GET 401 (cached) ->
    # token POST (refresh) -> GET 200.
    http_reauth = FakeHttp(
        ok_response(_TOKEN_OK),
        ok_response(_record_body("198.51.100.4")),
        HttpError("GET", status=401),
        ok_response(_TOKEN_OK),
        ok_response(_record_body("198.51.100.9")),
    )
    provider_reauth = _azure(http_reauth)
    primed_reauth = _prime(provider_reauth)  # prime the cache with a token
    reauth_value = (
        provider_reauth.read_current() if primed_reauth else None
    )  # this GET hits 401 -> re-acquire -> retry
    methods = [c[0] for c in http_reauth.calls]
    checks += [
        ("azure cached-401 fixture primes cleanly (no escape)", primed_reauth),
        (
            "azure cached-401 re-acquires once then retries",
            reauth_value == IPv4Address("198.51.100.9"),
        ),
        (
            "azure cached-401 made exactly one extra token POST",
            methods.count("POST") == 2,
        ),
    ]

    # --- Azure: 401 after a fresh token -> terminal ---
    # Prime token POST + GET 200; second read 401s; refresh; still 401 -> terminal.
    http_401fresh = FakeHttp(
        ok_response(_TOKEN_OK),
        ok_response(_record_body("198.51.100.4")),
        HttpError("GET", status=401),
        ok_response(_TOKEN_OK),
        HttpError("GET", status=401),
    )
    provider_401 = _azure(http_401fresh)
    primed_401 = _prime(provider_401)  # prime cache
    checks += [
        ("azure 401-after-fresh fixture primes cleanly (no escape)", primed_401),
        (
            "azure 401-after-fresh -> TerminalError",
            primed_401 and _expect_terminal(provider_401.read_current),
        ),
    ]

    # --- Azure: apply None -> SKIPPED_NO_IP (no network) ---
    http_skip = FakeHttp(ok_response(_TOKEN_OK))
    skip_result = _azure(http_skip).apply(None)
    checks.append(
        (
            "azure apply(None) -> SKIPPED_NO_IP without a write",
            skip_result.action is ApplyAction.SKIPPED_NO_IP and http_skip.calls == [],
        )
    )

    # --- URL: 4xx (not 429) -> terminal ---
    http_url4xx = FakeHttp(HttpError("GET", status=404))
    url_provider = UrlProvider(fixtures.EXAMPLE_URL_ENDPOINT, False, http_url4xx)
    checks.append(
        ("url 404 -> TerminalError", _expect_terminal(lambda: url_provider.apply(ip)))
    )

    # --- URL: 429 -> transient (carries Retry-After) ---
    http_url429 = FakeHttp(HttpError("GET", status=429, retry_after=15.0))
    url_429 = UrlProvider(fixtures.EXAMPLE_URL_ENDPOINT, False, http_url429)
    checks.append(
        (
            "url 429 -> TransientError(retry_after=15)",
            _expect_transient_retry(lambda: url_429.apply(ip), 15.0),
        )
    )

    # --- URL: 200 -> FIRED_SERVER_DETECTED ---
    http_url_ok = FakeHttp(ok_response(""))
    url_ok = UrlProvider(fixtures.EXAMPLE_URL_ENDPOINT, False, http_url_ok)
    checks.append(
        (
            "url 200 -> FIRED_SERVER_DETECTED",
            url_ok.apply(ip).action is ApplyAction.FIRED_SERVER_DETECTED,
        )
    )
    return report("STATUS-HANDLING", "status", checks)


def _prime(provider: AzureProvider) -> bool:
    """Run a priming ``read_current`` that must succeed; return ``True`` if it did.

    The cached-401 cases need a prior successful read to seed the token cache. The
    harness-level backstop in ``check/__init__.py`` would catch an escape from
    here too, but this keeps the precise local signal — a *specific* "fixture
    primes cleanly" PASS/FAIL line — and guards the dependent reauth assertions
    from running against a half-primed provider.
    """
    try:
        provider.read_current()
    except (TerminalError, TransientError):
        return False
    return True


def _expect_terminal(op: object) -> bool:
    assert callable(op)
    try:
        op()
    except TerminalError:
        return True
    except Exception:
        return False
    return False


def _expect_transient(op: object) -> bool:
    assert callable(op)
    try:
        op()
    except TransientError:
        return True
    except Exception:
        return False
    return False


def _expect_transient_retry(op: object, expected: float) -> bool:
    assert callable(op)
    try:
        op()
    except TransientError as exc:
        return exc.retry_after == expected
    except Exception:
        return False
    return False

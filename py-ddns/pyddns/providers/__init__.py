# pyright: strict
"""Provider registry/selector: build the archetype `DnsProvider` for a `Config`.

A third provider is additive — drop in ``providers/<name>.py``, add a `Provider`
enum member, and extend `build_provider` / `plan_provider`. The selector returns
the abstract `DnsProvider` seam so the updater never branches on archetype.
"""

from __future__ import annotations

from ipaddress import IPv4Address

from ..config import ConfigError
from ..httpclient import HttpClient, UrllibHttpClient
from ..models import Clock, Config, DnsProvider, Provider
from .azure import AzureProvider
from .url import UrlProvider


def _url_http(config: Config, http: HttpClient) -> HttpClient:
    """The HTTP client the URL provider should use — the single scope chokepoint.

    Returns a **new** cert-verification-disabled `UrllibHttpClient` only when
    `config.url_insecure_skip_verify` is set, else the passed-in (shared,
    verifying) `http` unchanged. This is the *one* place an insecure client is
    built, so azure + ip-source (which keep the shared `http`) can never inherit
    the skip.
    """
    if config.url_insecure_skip_verify:
        return UrllibHttpClient(insecure_skip_verify=True)
    return http


def build_provider(config: Config, http: HttpClient, clock: Clock) -> DnsProvider:
    """Construct the `DnsProvider` for `config.provider`.

    Raises `ConfigError` if the per-provider config invariants `config.validate`
    is supposed to have enforced are somehow unmet (defensive — `validate`
    guarantees the token for azure and the endpoint for url).

    The azure branch always gets the shared verifying `http`; only the URL branch
    may get a cert-skipping client (via `_url_http`, when the flag is set).
    """
    if config.provider is Provider.AZURE:
        if config.azure is None:
            raise ConfigError("azure: required")
        return AzureProvider(config.azure, config.record_label, config.ttl, http, clock)
    return UrlProvider(
        config.url_endpoint, config.url_send_myip, _url_http(config, http)
    )


def plan_provider(
    config: Config, http: HttpClient, clock: Clock, detected_ip: IPv4Address | None
) -> str:
    """Return the redacted dry-run plan line for `config.provider`.

    Builds the concrete provider (azure/url) and asks it to describe its planned
    action without touching the network. The concrete `plan` methods are
    archetype-specific, so this selector reaches past the `DnsProvider` seam.

    The URL branch appends a secret-free ``; TLS cert verification DISABLED``
    suffix when `config.url_insecure_skip_verify` is set, so ``--check --dry-run``
    surfaces the downgrade to the operator. `UrlProvider.plan` itself is unchanged.
    """
    if config.provider is Provider.AZURE:
        if config.azure is None:
            raise ConfigError("azure: required")
        return AzureProvider(
            config.azure, config.record_label, config.ttl, http, clock
        ).plan(detected_ip)
    plan = UrlProvider(
        config.url_endpoint, config.url_send_myip, _url_http(config, http)
    ).plan(detected_ip)
    if config.url_insecure_skip_verify:
        plan += "; TLS cert verification DISABLED"
    return plan

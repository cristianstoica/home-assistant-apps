# pyright: strict
"""Provider registry/selector: build the archetype `DnsProvider` for a `Config`.

A third provider is additive — drop in ``providers/<name>.py``, add a `Provider`
enum member, and extend `build_provider` / `plan_provider`. The selector returns
the abstract `DnsProvider` seam so the updater never branches on archetype.
"""

from __future__ import annotations

from ipaddress import IPv4Address

from ..config import ConfigError
from ..httpclient import HttpClient
from ..models import Clock, Config, DnsProvider, Provider
from .azure import AzureProvider
from .url import UrlProvider


def build_provider(config: Config, http: HttpClient, clock: Clock) -> DnsProvider:
    """Construct the `DnsProvider` for `config.provider`.

    Raises `ConfigError` if the per-provider config invariants `config.validate`
    is supposed to have enforced are somehow unmet (defensive — `validate`
    guarantees the token for azure and the endpoint for url).
    """
    if config.provider is Provider.AZURE:
        if config.azure is None:
            raise ConfigError("azure_token: required when provider=azure")
        return AzureProvider(config.azure, config.record_label, config.ttl, http, clock)
    return UrlProvider(config.url_endpoint, config.url_send_myip, http)


def plan_provider(
    config: Config, http: HttpClient, clock: Clock, detected_ip: IPv4Address | None
) -> str:
    """Return the redacted dry-run plan line for `config.provider`.

    Builds the concrete provider (azure/url) and asks it to describe its planned
    action without touching the network. The concrete `plan` methods are
    archetype-specific, so this selector reaches past the `DnsProvider` seam.
    """
    if config.provider is Provider.AZURE:
        if config.azure is None:
            raise ConfigError("azure_token: required when provider=azure")
        return AzureProvider(
            config.azure, config.record_label, config.ttl, http, clock
        ).plan(detected_ip)
    return UrlProvider(config.url_endpoint, config.url_send_myip, http).plan(
        detected_ip
    )

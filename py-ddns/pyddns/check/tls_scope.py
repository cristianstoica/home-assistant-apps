# pyright: strict
"""TLS-skip scope check: the unverified context is used ONLY on the callback path.

This is the headline-risk tripwire (R1): the cert-verification skip must reach the
URL provider's HTTP client **and nowhere else** — never azure, never the shared
client a verifying URL config uses. Each assertion drives the real
`build_provider(cfg, shared_http, monotonic)` and reads the resulting client's
public `UrllibHttpClient.verifies_tls` predicate (a domain concept, not a private
reach into the SSL context):

* URL + flag true   → the URL provider got a **distinct** insecure client
  (``verifies_tls is False``), not the shared one;
* URL + flag false  → the URL provider got the **shared verifying** client
  (``verifies_tls is True``, and it *is* the same instance);
* any azure config  → the azure provider got the **shared verifying** client;
* a standalone ``UrllibHttpClient(insecure_skip_verify=True)`` does not verify and
  the default ``UrllibHttpClient()`` does (the constructor seam itself).
"""

from __future__ import annotations

from .. import config, fixtures
from ..httpclient import UrllibHttpClient
from ..models import Provider
from ..providers import build_provider
from ..providers.url import UrlProvider
from ..runtime import monotonic
from .report import report


def _url_provider_client(provider: object) -> UrllibHttpClient:
    """The HTTP client a built `UrlProvider` is using (white-box, typed).

    `build_provider` returns the abstract `DnsProvider` seam; the scope assertion
    must read the concrete provider's injected client, so this narrows to
    `UrlProvider` and returns its `_http`. The provider stores the client behind
    the `HttpClient` Protocol, so the concrete `UrllibHttpClient` is asserted here
    (it always is one on the production wiring `_url_http` drives).
    """
    assert isinstance(provider, UrlProvider)
    client = provider._http  # pyright: ignore[reportPrivateUsage]
    assert isinstance(client, UrllibHttpClient)
    return client


def check_tls_scope() -> bool:
    """Assert the cert-verification skip is scoped to the callback path only."""
    checks: list[tuple[str, bool]] = []

    # --- the constructor seam itself ------------------------------------------
    insecure = UrllibHttpClient(insecure_skip_verify=True)
    verifying = UrllibHttpClient()
    checks += [
        (
            "UrllibHttpClient(insecure_skip_verify=True) does not verify TLS",
            insecure.verifies_tls is False,
        ),
        (
            "default UrllibHttpClient() verifies TLS",
            verifying.verifies_tls is True,
        ),
    ]

    # --- URL + flag true: a DISTINCT insecure client reaches the URL provider --
    shared = UrllibHttpClient()
    cfg_flag = config.validate(
        fixtures.example_url_options(
            url={
                "endpoint": fixtures.EXAMPLE_URL_ENDPOINT,
                "insecure_skip_verify": True,
            }
        )
    ).config
    flag_client = _url_provider_client(build_provider(cfg_flag, shared, monotonic))
    checks += [
        (
            "url+flag-true: URL provider's client does NOT verify TLS",
            flag_client.verifies_tls is False,
        ),
        (
            "url+flag-true: URL provider got a distinct client (not the shared one)",
            flag_client is not shared,
        ),
        (
            "url+flag-true: the shared client itself still verifies",
            shared.verifies_tls is True,
        ),
    ]

    # --- URL + flag false: the SHARED verifying client reaches the URL provider -
    shared_verify = UrllibHttpClient()
    cfg_noflag = config.validate(fixtures.example_url_options()).config
    noflag_client = _url_provider_client(
        build_provider(cfg_noflag, shared_verify, monotonic)
    )
    checks += [
        (
            "url+flag-false: URL provider verifies TLS",
            noflag_client.verifies_tls is True,
        ),
        (
            "url+flag-false: URL provider got the shared client (same instance)",
            noflag_client is shared_verify,
        ),
    ]

    # --- any azure config: the SHARED verifying client, never an insecure one ---
    shared_azure = UrllibHttpClient()
    cfg_azure = config.validate(fixtures.example_azure_options()).config
    azure_provider = build_provider(cfg_azure, shared_azure, monotonic)
    checks += [
        (
            "azure config: provider inferred as AZURE",
            cfg_azure.provider is Provider.AZURE,
        ),
        (
            "azure: provider got the shared verifying client (no leak of the skip)",
            _azure_provider_client(azure_provider) is shared_azure,
        ),
        (
            "azure: the shared client verifies TLS",
            shared_azure.verifies_tls is True,
        ),
    ]

    return report("TLS-SCOPE", "tls-scope", checks)


def _azure_provider_client(provider: object) -> UrllibHttpClient:
    """The HTTP client a built `AzureProvider` is using (white-box, typed)."""
    from ..providers.azure import AzureProvider

    assert isinstance(provider, AzureProvider)
    client = provider._http  # pyright: ignore[reportPrivateUsage]
    assert isinstance(client, UrllibHttpClient)
    return client

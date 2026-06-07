# pyright: strict
"""Request-shaping checks: Azure URL/body/token shaping and URL myip-merge."""

from __future__ import annotations

from ipaddress import IPv4Address
from typing import cast
from urllib.parse import parse_qs, urlsplit

from .. import fixtures
from ..models import AzureToken
from ..providers.azure import record_body, record_url
from ..providers.url import compose_fire_url
from .report import report


def _azure_body_is_full_replace(body: dict[str, object], *, ttl: int, ip: str) -> bool:
    """True iff `body` is the ``properties.TTL`` + ``ARecords[0].ipv4Address`` shape."""
    props = body.get("properties")
    if not isinstance(props, dict):
        return False
    props_obj = cast(dict[str, object], props)
    if props_obj.get("TTL") != ttl:
        return False
    records = props_obj.get("ARecords")
    if not isinstance(records, list) or not records:
        return False
    records_list = cast(list[object], records)
    first = records_list[0]
    if not isinstance(first, dict):
        return False
    return cast(dict[str, object], first).get("ipv4Address") == ip


def check_url_endpoint_shaping() -> bool:
    """Assert the Azure record URL/body and the URL ``myip`` merge are well-formed.

    Azure:

    * ``record_url`` targets ``management.azure.com`` with the full
      subscription/RG/zone/A-label path and the **GA** ``2018-05-01`` api-version.
    * ``record_body`` is the full-replace shape (``properties.TTL`` +
      ``ARecords[0].ipv4Address``).

    URL (the key invariant — merge via `urllib.parse`, never naive concat):

    * a pre-existing query string is **preserved** and ``myip`` is **appended**
      when ``url_send_myip`` is set and an IP is known;
    * ``myip`` is **not** added when ``url_send_myip`` is false (server-detect);
    * ``myip`` is **not** added when no IP is known even with the flag set;
    * an existing ``myip`` param is **replaced**, not duplicated.
    """
    token = AzureToken(
        tenant_id="t",
        subscription_id="sub",
        resource_group="rg",
        zone="example.com",
        client_id="cid",
        client_secret="sec",
    )
    ip = IPv4Address("203.0.113.7")
    url = record_url(token, "home")
    parts = urlsplit(url)
    body = record_body(60, ip)
    body_ok = _azure_body_is_full_replace(body, ttl=60, ip="203.0.113.7")

    checks: list[tuple[str, bool]] = [
        (
            "azure URL host is management.azure.com",
            parts.hostname == "management.azure.com",
        ),
        (
            "azure URL path targets the A record-set",
            parts.path == "/subscriptions/sub/resourceGroups/rg"
            "/providers/Microsoft.Network/dnszones/example.com/A/home",
        ),
        (
            "azure URL pins GA api-version 2018-05-01",
            parse_qs(parts.query).get("api-version") == ["2018-05-01"],
        ),
        ("azure body is the full-replace shape", body_ok),
    ]

    # --- URL myip merge against a pre-existing query string ---
    endpoint = "https://dynamicdns.example.com/update/secret?hostname=h&zone=z"

    merged = compose_fire_url(endpoint, ip, send_myip=True)
    mq = parse_qs(urlsplit(merged).query)
    checks += [
        (
            "url merge preserves pre-existing params",
            mq.get("hostname") == ["h"] and mq.get("zone") == ["z"],
        ),
        (
            "url merge appends myip when requested + known",
            mq.get("myip") == ["203.0.113.7"],
        ),
        ("url merge keeps the secret path", urlsplit(merged).path == "/update/secret"),
    ]

    no_myip = compose_fire_url(endpoint, ip, send_myip=False)
    checks.append(
        (
            "url no-myip when flag off (server-detect)",
            "myip" not in parse_qs(urlsplit(no_myip).query),
        )
    )

    no_ip = compose_fire_url(endpoint, None, send_myip=True)
    checks.append(
        ("url no-myip when no IP known", "myip" not in parse_qs(urlsplit(no_ip).query))
    )

    pre_existing = "https://dynamicdns.example.com/update/secret?myip=1.1.1.1"
    replaced = compose_fire_url(pre_existing, ip, send_myip=True)
    rq = parse_qs(urlsplit(replaced).query)
    checks.append(
        (
            "url replaces an existing myip (no duplicate)",
            rq.get("myip") == ["203.0.113.7"],
        )
    )

    # The fixture endpoint's secret must survive composition unchanged.
    fixture_merged = compose_fire_url(fixtures.EXAMPLE_URL_ENDPOINT, ip, send_myip=True)
    checks.append(
        (
            "fixture endpoint secret path preserved through merge",
            fixtures.EXAMPLE_URL_SECRET in fixture_merged,
        )
    )
    return report("URL-SHAPING", "shaping", checks)

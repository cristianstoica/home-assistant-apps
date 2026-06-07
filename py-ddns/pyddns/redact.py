# pyright: strict
"""Secret-safe redaction helpers used everywhere a URL or exception is logged.

The callback ``url_endpoint`` *is* the record-repointing secret: its path encodes
the secret token. A raw ``HTTPError`` / ``URLError`` echoes the requested URL, so
an un-sanitized exception string can leak that secret straight into the HA Log
tab. These helpers are the single chokepoint: log only **scheme + host** (never
path/query), and scrub any provided secrets out of an exception string before it
is logged.
"""

from __future__ import annotations

from urllib.parse import urlsplit


def redact_url(url: str) -> str:
    """Return ``<scheme>://<host>/<redacted>`` — host kept, secret path masked.

    The host is operationally useful (which provider was contacted) and not
    itself a secret; the path/query carries the secret token and is masked. A
    URL that does not parse falls back to ``<redacted-url>`` rather than risk
    echoing it.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<redacted-url>"
    host = parts.hostname
    if not parts.scheme or not host:
        return "<redacted-url>"
    return f"{parts.scheme}://{host}/<redacted>"


def sanitize(message: str, secrets: tuple[str, ...]) -> str:
    """Replace each non-empty secret substring in `message` with ``<redacted>``.

    Used to scrub `urllib` exception strings (which can echo a secret URL or a
    credential) before they reach a log. Each secret is matched as a literal
    substring; empty secrets are skipped (replacing ``""`` would corrupt the
    whole string).
    """
    out = message
    for secret in secrets:
        if secret:
            out = out.replace(secret, "<redacted>")
    return out

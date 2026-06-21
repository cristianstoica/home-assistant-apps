# pyright: strict
"""Secret-safe redaction helper used wherever a transport exception is logged.

The only secret py-weather holds is the ``SUPERVISOR_TOKEN`` bearer (injected by
the Supervisor; py-weather never owns Weather.com credentials — the REST
integration owns external access). The Core-API base URL
(``http://supervisor/core/api/...``) is not a secret. A raw ``HTTPError`` /
``URLError`` from ``urllib`` echoes the request, and a hostile/misconfigured
header value could surface the bearer, so this single chokepoint scrubs the
token out of any string before it reaches a log.
"""

from __future__ import annotations


def sanitize(message: str, secrets: tuple[str, ...]) -> str:
    """Replace each non-empty secret substring in `message` with ``<redacted>``.

    Used to scrub `urllib` exception strings (which can echo a bearer token)
    before they reach a log. Each secret is matched as a literal substring;
    empty secrets are skipped (replacing ``""`` would corrupt the whole string).
    """
    out = message
    for secret in secrets:
        if secret:
            out = out.replace(secret, "<redacted>")
    return out

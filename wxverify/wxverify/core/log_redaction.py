"""Logging filter that redacts secret URL query params from log records.

httpx/httpcore emit full request URLs (with API keys) in their own DEBUG
records, from call sites we do not own. This filter runs the shared
``redact_urls`` scrubber over each record's fully-rendered message so no key
reaches a handler.
"""

from __future__ import annotations

import logging

from wxverify.core.error_sanitize import redact_urls


class RedactUrlSecretsFilter(logging.Filter):
    """Rewrite a record's rendered message, stripping secret URL query params.

    Renders ``record.getMessage()`` (applying any %-args), scrubs it via
    ``redact_urls``, and -- only if the text changed -- replaces ``record.msg``
    with the scrubbed string and clears ``record.args`` so downstream formatting
    is a no-op re-render of already-safe text. Records without a URL are left
    byte-identical (no ``msg``/``args`` mutation), keeping the common path cheap.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = redact_urls(message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True

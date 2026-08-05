"""Shared validation for feed fetch-cadence values.

``feeds.fetch_interval_minutes`` is trusted input on every read path here:
the API bounds it at creation (``ge=1``), but an imported, restored, or
hand-edited database carries its own schema and can hold anything a REAL/TEXT
column will accept, including zero, negative, or absurdly large values. Every
caller that turns this column into a schedule or a retry-delay must treat it
as untrusted and fail closed rather than invent a due-immediately cadence or
overflow a downstream ``timedelta``.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 30-day ceiling: an out-of-range cadence (a foreign/imported row, or a ms
# epoch stamp pasted into a minutes field) feeds timedelta(seconds=...)
# downstream and would raise OverflowError there if left unbounded.
MAX_FETCH_INTERVAL_MINUTES = 30 * 24 * 60


def parse_fetch_interval_minutes(value: object, *, context: str) -> int | None:
    """Convert and range-check a ``fetch_interval_minutes`` value.

    Returns ``None`` for anything not convertible to ``int``, ``<= 0``, or
    beyond the 30-day ceiling, logging a warning that identifies the caller
    via ``context``. Callers must treat ``None`` as "this feed cannot be
    scheduled right now" and fail closed (skip/continue), never invent a
    fallback cadence.
    """
    try:
        minutes = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        logger.warning(
            "cadence: unreadable fetch_interval_minutes=%r (%s)", value, context
        )
        return None
    if minutes <= 0 or minutes > MAX_FETCH_INTERVAL_MINUTES:
        logger.warning(
            "cadence: out-of-range fetch_interval_minutes=%s (%s)", minutes, context
        )
        return None
    return minutes

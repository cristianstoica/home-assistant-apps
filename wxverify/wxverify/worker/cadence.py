"""Feed fetch-cadence validation, and the due rule both due checks use.

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
from typing import Final

logger = logging.getLogger(__name__)

# 30-day ceiling: an out-of-range cadence (a foreign/imported row, or a ms
# epoch stamp pasted into a minutes field) feeds timedelta(seconds=...)
# downstream and would raise OverflowError there if left unbounded.
MAX_FETCH_INTERVAL_MINUTES = 30 * 24 * 60

#: monitor.FEED_STALE_HOURS * 60. Not imported: cadence must not depend on the
#: monitor. A test pins the two together.
FEED_STALE_LIMIT_MINUTES: Final = 720
#: How long before that limit a feed whose interval reaches it is fetched.
FEED_STALE_LEAD_MINUTES: Final = 30


def parse_fetch_interval_minutes(value: object, *, context: str) -> int | None:
    """Convert and range-check a ``fetch_interval_minutes`` value.

    Returns ``None`` for anything that does not read as a whole number of
    minutes, is ``<= 0``, or is beyond the 30-day ceiling, logging a warning
    that identifies the caller via ``context``. Any carrier ``float()``
    accepts is read -- an INTEGER, a REAL, or a TEXT/BLOB spelling such as
    ``'360'``, ``'360.0'`` or ``'1e3'`` -- and is accepted when that
    ``float()`` reading is integral: a decimal spelling within half an ulp
    of an integer rounds onto it and is accepted, while a genuinely
    fractional value like ``1.9`` or ``43200.9`` is rejected rather than
    truncated, never silently scheduled at the floored whole-minute value.
    Callers must treat ``None`` as "this feed cannot be scheduled right now"
    and fail closed (skip/continue), never invent a fallback cadence.
    """
    try:
        as_float = float(value)  # type: ignore[arg-type]
        if as_float != int(as_float):
            raise ValueError("non-integral fetch_interval_minutes")
        minutes = int(as_float)
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


def effective_due_minutes(interval_minutes: int) -> int:
    """Minutes after the last run at which a feed is due.

    An interval within the lead of the stale limit is pulled back to
    limit - lead, so the fetch starts before the feed reads as stale.
    Every other interval is returned unchanged.
    """
    floor = FEED_STALE_LIMIT_MINUTES - FEED_STALE_LEAD_MINUTES
    if floor < interval_minutes <= FEED_STALE_LIMIT_MINUTES:
        return floor
    return interval_minutes

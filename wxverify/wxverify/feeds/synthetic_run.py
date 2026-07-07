"""Synthetic cadence-snapped model-run timestamps for run-less providers.

The five commercial forecast providers (visualcrossing, openweathermap,
weatherapi, meteosource, google) expose no native model-run timestamp, so each
derives a deterministic synthetic ``issued_at`` by flooring the lag-adjusted
fetch time to a fixed cadence boundary -- identical to ``open_meteo._snap_run``
but with shared *scalar* constants rather than per-model dicts. Pinning these
makes ``issued_at`` / ``lead_hours`` / idempotency deterministic across the five
adapters instead of implementation-defined.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now

RUN_CADENCE_HOURS: Final = 6
RUN_AVAILABILITY_LAG_MINUTES: Final = 90


def snap_run(fetch_time: str | None = None) -> str:
    """Return the cadence-snapped synthetic model-run timestamp (ISO-8601 UTC).

    Subtracts the availability lag from the fetch time, then floors the UTC hour
    to the cadence boundary. A fetch at ``2026-06-01T07:00:00Z`` snaps to
    ``2026-06-01T00:00:00Z`` (07:00 - 90m = 05:30, floored to the 6h boundary).
    """
    now = parse_utc(fetch_time) if fetch_time else utc_now()
    lagged = now - timedelta(minutes=RUN_AVAILABILITY_LAG_MINUTES)
    hour = (lagged.hour // RUN_CADENCE_HOURS) * RUN_CADENCE_HOURS
    snapped = lagged.replace(hour=hour, minute=0, second=0, microsecond=0)
    return isoformat_utc(snapped)

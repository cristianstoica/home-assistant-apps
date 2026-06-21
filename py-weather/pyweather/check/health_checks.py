# pyright: strict
"""Health + freshness evaluation checks, driven from fixture ``/states`` payloads.

These exercise the pure `health.evaluate` over a fixed pre-POST ``t0``:
available/unusable/missing/partial states, the absence-vs-present-but-unavailable
distinction, the optional-metric tolerance, the required-core floor, and every
branch of the freshness contract (primary ``last_reported`` advance/not-advance,
offset-form vs ``Z``-form parsing, the ``last_updated`` → ``last_changed``
fallback chain, naive/unparseable primary, present-null/absent-key → fallback,
and the byte-identical CONFIRMED/INCONCLUSIVE split).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .. import fixtures
from ..config import validate
from ..health import evaluate
from ..models import EntityState, HealthStatus, Station
from .report import report

_T0 = datetime.fromisoformat(fixtures.T0_ISO)


def _station(key: str = "istation01", expected: int = 10) -> Station:
    """A single validated `Station` for `key` with `expected` sensors."""
    cfg = validate(
        fixtures.default_options(
            stations=[
                {
                    "key": key,
                    "update_entity": f"sensor.wu_temp_{key}",
                    "expected_sensors": expected,
                }
            ]
        )
    )
    return cfg.stations[0]


def _to_entity_states(raw: list[dict[str, Any]]) -> list[EntityState]:
    """Project raw ``/states`` dicts to `EntityState`s (the GET /states parse).

    Mirrors `haapi._parse_timestamp`: a present-but-null/empty/missing timestamp
    becomes ``None``. Driving this locally keeps the health oracle pure (no HTTP
    seam) while matching the production projection.
    """
    out: list[EntityState] = []
    for obj in raw:
        out.append(
            EntityState(
                entity_id=str(obj["entity_id"]),
                state=str(obj["state"]),
                last_reported=_ts(obj.get("last_reported")),
                last_updated=_ts(obj.get("last_updated")),
                last_changed=_ts(obj.get("last_changed")),
            )
        )
    return out


def _ts(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value if value.strip() != "" else None


def _evaluate(
    raw: list[dict[str, Any]], station: Station | None = None
) -> HealthStatus:
    """Evaluate a raw ``/states`` array for `station` (default istation01) at ``t0``."""
    return evaluate(station or _station(), _to_entity_states(raw), _T0).status


def check_health() -> bool:  # noqa: C901 - one cohesive assertion surface
    """Assert the health predicate over fixture ``/states`` payloads.

    All-available (confirmed via primary advance); a required-core sensor
    unavailable/unknown/none/empty/missing ⇒ unhealthy; the representative absent
    ⇒ unhealthy; optional-metric absence and present-but-unavailable both
    tolerated; the synthetic-shortfall (discovered < expected_sensors) tolerated.
    """
    checks: list[tuple[str, bool]] = []

    # All-available + primary last_reported advanced ⇒ confirmed.
    all_avail = fixtures.station_states(
        "istation01", temp_last_reported=fixtures.FRESH_ISO
    )
    checks.append(
        (
            "all-available + fresh last_reported ⇒ CONFIRMED",
            _evaluate(all_avail) is HealthStatus.CONFIRMED,
        )
    )

    # Required-core unusable states (each makes the station unhealthy).
    for bad in ("unavailable", "unknown", "none", ""):
        unusable = fixtures.station_states(
            "istation01", temp_state=bad, temp_last_reported=fixtures.FRESH_ISO
        )
        checks.append(
            (
                f"required-core temp={bad!r} ⇒ UNHEALTHY",
                _evaluate(unusable) is HealthStatus.UNHEALTHY,
            )
        )
    humidity_bad = fixtures.station_states(
        "istation01",
        humidity_state="unavailable",
        temp_last_reported=fixtures.FRESH_ISO,
    )
    pressure_bad = fixtures.station_states(
        "istation01", pressure_state="unknown", temp_last_reported=fixtures.FRESH_ISO
    )
    checks += [
        (
            "required-core humidity unavailable ⇒ UNHEALTHY",
            _evaluate(humidity_bad) is HealthStatus.UNHEALTHY,
        ),
        (
            "required-core pressure unknown ⇒ UNHEALTHY",
            _evaluate(pressure_bad) is HealthStatus.UNHEALTHY,
        ),
    ]

    # Representative (temp) absent from /states ⇒ unhealthy.
    no_temp = [
        fixtures.state_obj("sensor.wu_humidity_istation01", "60"),
        fixtures.state_obj("sensor.wu_pressure_istation01", "1013"),
        fixtures.state_obj("sensor.wu_uv_istation01", "2"),
    ]
    checks.append(
        (
            "representative temp absent from /states ⇒ UNHEALTHY",
            _evaluate(no_temp) is HealthStatus.UNHEALTHY,
        )
    )

    # Required-core humidity absent (missing entity, not just unavailable).
    missing_humidity = [
        fixtures.state_obj(
            "sensor.wu_temp_istation01", "12.3", last_reported=fixtures.FRESH_ISO
        ),
        fixtures.state_obj("sensor.wu_pressure_istation01", "1013"),
    ]
    checks.append(
        (
            "required-core humidity absent from /states ⇒ UNHEALTHY",
            _evaluate(missing_humidity) is HealthStatus.UNHEALTHY,
        )
    )

    # --- optional-metric tolerance -------------------------------------------
    # (a) uv ABSENT from /states ⇒ still healthy (required-core intact).
    uv_absent = fixtures.station_states(
        "istation01", include_uv=False, temp_last_reported=fixtures.FRESH_ISO
    )
    checks.append(
        (
            "(a) uv ABSENT from /states ⇒ healthy (CONFIRMED, required-core intact)",
            _evaluate(uv_absent) is HealthStatus.CONFIRMED,
        )
    )
    # (b) synthetic station whose expected_sensors exceeds discovered count
    #     (an optional sensor short of expected) ⇒ still healthy. expected=12 but
    #     only 4 sensors discovered (temp/humidity/pressure/uv).
    short = fixtures.station_states("istation01", temp_last_reported=fixtures.FRESH_ISO)
    checks.append(
        (
            "(b) discovered < expected_sensors (optional shortfall) ⇒ healthy",
            evaluate(_station(expected=12), _to_entity_states(short), _T0).status
            is HealthStatus.CONFIRMED,
        )
    )
    # (c) any required-core absent ⇒ unhealthy (re-pin under the tolerance lens).
    checks.append(
        (
            "(c) required-core absent ⇒ UNHEALTHY (tolerance does not cover core)",
            _evaluate(missing_humidity) is HealthStatus.UNHEALTHY,
        )
    )

    # all-core-usable + UV-only-unavailable (present-but-unavailable) ⇒ healthy.
    uv_unavail = fixtures.station_states(
        "istation01", uv_state="unavailable", temp_last_reported=fixtures.FRESH_ISO
    )
    checks.append(
        (
            "all-core-usable + uv present-but-unavailable ⇒ healthy (optional non-fatal)",
            _evaluate(uv_unavail) is HealthStatus.CONFIRMED,
        )
    )
    return report("HEALTH", "health", checks)


def check_freshness() -> bool:  # noqa: C901 - one cohesive assertion surface
    """Assert every branch of the freshness contract.

    Primary path: not-advanced ⇒ unhealthy; advanced ⇒ confirmed; offset-form and
    ``Z``-form both parse + compare; naive/unparseable non-null ⇒ unhealthy.
    Fallback path: present-null and absent-key both route to fallback (not
    primary-path unhealthy); the ``last_updated`` → ``last_changed`` chain is
    exercised; an advanced fallback ⇒ confirmed; an unchanged fallback on an
    otherwise-successful poll ⇒ inconclusive (not unhealthy). Byte-identical
    cases (a) primary-advanced and (b) fallback-unchanged are pinned distinctly.
    """
    station = _station()
    checks: list[tuple[str, bool]] = []

    # Primary path: present, non-null, not advanced (stale) ⇒ UNHEALTHY.
    stale = fixtures.station_states("istation01", temp_last_reported=fixtures.STALE_ISO)
    checks.append(
        (
            "primary last_reported stale (not advanced) ⇒ UNHEALTHY",
            _evaluate(stale, station) is HealthStatus.UNHEALTHY,
        )
    )
    # Primary advanced (offset form +00:00) ⇒ CONFIRMED.
    offset = fixtures.station_states(
        "istation01", temp_last_reported=fixtures.FRESH_ISO
    )
    checks.append(
        (
            "primary last_reported advanced (+00:00 offset) ⇒ CONFIRMED",
            _evaluate(offset, station) is HealthStatus.CONFIRMED,
        )
    )
    # Primary advanced (Z form) ⇒ CONFIRMED (proves Z parses + compares).
    zform = fixtures.station_states(
        "istation01", temp_last_reported=fixtures.FRESH_ISO_Z
    )
    checks.append(
        (
            "primary last_reported advanced (Z form) ⇒ CONFIRMED",
            _evaluate(zform, station) is HealthStatus.CONFIRMED,
        )
    )
    # Primary present, non-null, but NAIVE/unparseable ⇒ UNHEALTHY (real signal).
    naive = fixtures.station_states("istation01", temp_last_reported=fixtures.NAIVE_ISO)
    checks.append(
        (
            "primary last_reported naive/unparseable ⇒ UNHEALTHY (real signal)",
            _evaluate(naive, station) is HealthStatus.UNHEALTHY,
        )
    )

    # --- fallback routing: present-null and absent-key both go to fallback ---
    # last_reported present-but-null + last_updated advanced ⇒ CONFIRMED via fallback
    # (proves present-null is NOT primary-path unhealthy).
    null_lr = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.NULL,
        temp_last_updated=fixtures.FRESH_ISO,
    )
    checks.append(
        (
            "last_reported present-null ⇒ routes to fallback (advanced ⇒ CONFIRMED, not primary-unhealthy)",
            _evaluate(null_lr, station) is HealthStatus.CONFIRMED,
        )
    )
    # last_reported absent (no key) + last_updated advanced ⇒ CONFIRMED via fallback.
    absent_lr = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.OMIT,
        temp_last_updated=fixtures.FRESH_ISO,
    )
    checks.append(
        (
            "last_reported absent (no key) ⇒ routes to fallback (advanced ⇒ CONFIRMED)",
            _evaluate(absent_lr, station) is HealthStatus.CONFIRMED,
        )
    )
    # Fallback chain: last_reported absent AND last_updated absent ⇒ last_changed.
    via_last_changed = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.OMIT,
        temp_last_updated=fixtures.OMIT,
        temp_last_changed=fixtures.FRESH_ISO,
    )
    checks.append(
        (
            "fallback chain: last_reported+last_updated absent ⇒ last_changed used (advanced ⇒ CONFIRMED)",
            _evaluate(via_last_changed, station) is HealthStatus.CONFIRMED,
        )
    )

    # --- byte-identical write cases ------------------------------------------
    # (a) last_reported advanced past t0 even though last_changed/last_updated are
    #     unchanged (identical-value write) ⇒ CONFIRMED.
    identical_primary = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.FRESH_ISO,
        temp_last_updated=fixtures.STALE_ISO,
        temp_last_changed=fixtures.STALE_ISO,
    )
    checks.append(
        (
            "(a) identical-value write: last_reported advanced ⇒ CONFIRMED (even with stale last_updated/changed)",
            _evaluate(identical_primary, station) is HealthStatus.CONFIRMED,
        )
    )
    # (b) last_reported absent + last_updated/last_changed unchanged, but
    #     representative present and required-core usable, POST succeeded ⇒
    #     INCONCLUSIVE (accepted, not backoff, not confirmed).
    identical_fallback = fixtures.station_states(
        "istation01",
        temp_last_reported=fixtures.OMIT,
        temp_last_updated=fixtures.STALE_ISO,
        temp_last_changed=fixtures.STALE_ISO,
    )
    checks.append(
        (
            "(b) last_reported absent + unchanged fallback ⇒ INCONCLUSIVE (accepted, not backoff/confirmed)",
            _evaluate(identical_fallback, station) is HealthStatus.INCONCLUSIVE,
        )
    )
    return report("FRESHNESS", "freshness", checks)

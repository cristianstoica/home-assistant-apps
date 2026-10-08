"""Pre-release coverage check for the Open-Meteo ``icon_eu`` feed (owner-run).

For one enabled site, in this order:

1. ``applicability`` -- a local bounds screen on the site's forecast location,
   read from the owner's own database export. Nothing is sent unless it is
   PASS.
2. One forward request through ``OpenMeteoAdapter.fetch_forecast``: the same
   request a regular fetch of the feed sends.
3. One previous-runs request through ``OpenMeteoAdapter.fetch_historical``:
   the same request setup backfill sends, for the UTC day two days before
   now. No retry, no second window.

Run from the add-on directory (``uv sync`` does not install the package), once
for every enabled site, inside a test window (01:00-02:30, 07:00-08:30,
13:00-14:30 or 19:00-20:30 UTC)::

    PYTHONPATH=. uv run python scripts/icon_eu_coverage_check.py \
        --db <exported-wxverify.db> --site-id N

``--site-id`` may be left out only when exactly one site is enabled.

Only the owner runs this script: its requests carry the site's location, so
agents never run it. One run costs 3 Open-Meteo calls against the provider
allowance, and those calls are not recorded in the add-on's ledger. The
database is a Database Export copy, opened read-only; nothing is written to it
or anywhere else, and no job, budget reservation or backfill state is touched.

Logging is disabled for the run, and only fixed lines print: none carries a
coordinate, a URL, a request parameter, the database path or any part of a raw
response. Exit codes: 0 when every verdict is PASS, 1 when any is FAIL, 3 when
any is CAN'T TELL and none is FAIL, 2 after an ``error:`` line.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sqlite3
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Final, Literal, cast

import httpx

from wxverify.collection.forecast_validation import FORECAST_VALUE_RANGES
from wxverify.config import OPEN_METEO_MAX_LEAD_HOURS
from wxverify.core.timeutil import floor_hour, parse_utc, utc_now
from wxverify.feeds.open_meteo import VARIABLE_MAP, OpenMeteoAdapter
from wxverify.feeds.seam import ForecastRequest, NormalizedSample


class Verdict(StrEnum):
    """One check's outcome, printed as its value."""

    PASS = "PASS"
    FAIL = "FAIL"
    CANT_TELL = "CAN'T TELL"


Reason = Literal[
    "ok",
    "bad shape",
    "wrong times",
    "wrong units",
    "too few steps",
    "too few values",
    "implausible value",
    "gap inside data",
    "http 4xx",
    "rate limited",
    "http 5xx",
    "unexpected http status",
    "redirect",
    "network",
    "adapter rejected",
    "interrupted",
]

MODEL: Final = "icon_eu"
#: The variable tuple a regular fetch sends (``worker.feed_fetch``).
VARIABLES: Final[tuple[str, ...]] = ("temperature", "wind", "precip")
#: The forward response's hourly names, in ``VARIABLE_MAP`` order.
FORWARD_NAMES: Final[tuple[str, ...]] = tuple(
    provider_name
    for variable, provider_name in VARIABLE_MAP.items()
    if variable in VARIABLES
)
MIN_FORWARD_STEPS: Final = 120
MIN_FORWARD_VALUES: Final = 96

_MAX_LEAD_HOURS: Final = OPEN_METEO_MAX_LEAD_HOURS[MODEL]
#: (name, provider name) for each previous-runs column the adapter requests,
#: in the adapter's order (variables in ``VARIABLE_MAP`` order, then days
#: 1 to ``min(7, horizon days)``), mirroring
#: ``feeds.open_meteo._historical_hourly_names`` through public names only.
_PREVIOUS_RUN_COLUMNS: Final[tuple[tuple[str, str], ...]] = tuple(
    (f"{provider_name}_previous_day{day}", provider_name)
    for variable, provider_name in VARIABLE_MAP.items()
    if variable in VARIABLES
    for day in range(1, min(7, _MAX_LEAD_HOURS // 24) + 1)
)
PREVIOUS_RUN_NAMES: Final[tuple[str, ...]] = tuple(
    name for name, _ in _PREVIOUS_RUN_COLUMNS
)
#: The units the adapter's normalization assumes, per provider name.
_PREVIOUS_RUN_UNITS: Final[Mapping[str, str]] = {
    "temperature_2m": "°C",
    "wind_speed_10m": "km/h",
    "precipitation": "mm",
}
_PREVIOUS_RUN_ROWS: Final = 48
_REQUIRED_PREVIOUS_DAYS: Final = (1, 2, 3)

_HOUR: Final = timedelta(hours=1)
_WINDOW_START_HOURS: Final = (1, 7, 13, 19)
_WINDOW_LENGTH: Final = timedelta(minutes=90)
_FORWARD: Final = "forward"
_PREVIOUS_RUNS: Final = "previous-runs"
_REQUEST_LABELS: Final = (_FORWARD, _PREVIOUS_RUNS)


@dataclass(frozen=True)
class Summary:
    """What the forward rules read from one forward response body."""

    steps: int
    nulls: Mapping[str, int]
    interior_null: Mapping[str, bool]
    first_valid: datetime | None
    last_valid: datetime | None
    first_time: datetime | None
    consecutive: bool


@dataclass
class _Run:
    """Shared state for ``main``: which verdict lines have printed."""

    verdicts: list[Verdict] = field(default_factory=list[Verdict])
    printed: set[str] = field(default_factory=set[str])
    request_phase: bool = False


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _minute(value: datetime) -> str:
    """``YYYY-MM-DDTHH:MM`` in UTC, zero-padded whatever the year."""
    v = _as_utc(value)
    return f"{v.year:04d}-{v.month:02d}-{v.day:02d}T{v.hour:02d}:{v.minute:02d}"


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


def applicability(lat: object, lon: object) -> Verdict:
    """The bounds screen for the ``icon_eu`` domain, on values as read.

    CAN'T TELL is checked first: a value that is not a finite ``int`` or
    ``float`` (a ``bool`` is not a number here), or a latitude outside ±90 or a
    longitude outside ±180. PASS is 1° inside the grid on every side (and
    inside the narrower element-package east edge); FAIL is outside the grid;
    the 1° margin band between them is CAN'T TELL.
    """
    lat_value = _finite_number(lat)
    lon_value = _finite_number(lon)
    if (
        lat_value is None
        or lon_value is None
        or abs(lat_value) > 90.0
        or abs(lon_value) > 180.0
    ):
        return Verdict.CANT_TELL
    if 30.5 <= lat_value <= 69.5 and -22.5 <= lon_value <= 44.0:
        return Verdict.PASS
    if lat_value < 29.5 or lat_value > 70.5 or lon_value < -23.5 or lon_value > 62.5:
        return Verdict.FAIL
    return Verdict.CANT_TELL


def in_side_window(now: datetime) -> bool:
    """Whether ``now`` is in [01:00, 02:30), [07:00, 08:30), [13:00, 14:30)
    or [19:00, 20:30) UTC."""
    current = _as_utc(now)
    midnight = current.replace(hour=0, minute=0, second=0, microsecond=0)
    since_midnight = current - midnight
    return any(
        timedelta(hours=hour) <= since_midnight < timedelta(hours=hour) + _WINDOW_LENGTH
        for hour in _WINDOW_START_HOURS
    )


def next_window_start(now: datetime) -> datetime:
    """The first test-window start strictly after ``now``, in UTC."""
    current = _as_utc(now)
    midnight = current.replace(hour=0, minute=0, second=0, microsecond=0)
    return min(
        start
        for start in (
            midnight + timedelta(days=day, hours=hour)
            for day in (0, 1)
            for hour in _WINDOW_START_HOURS
        )
        if start > current
    )


def _parse_stamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _is_whole_hour(value: datetime) -> bool:
    """A whole UTC hour: minute, second and microsecond zero (``value`` is
    UTC-aware)."""
    return value.minute == 0 and value.second == 0 and value.microsecond == 0


def _as_dict(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    return cast(dict[str, object], value)


def _as_list(value: object) -> list[object] | None:
    if not isinstance(value, list):
        return None
    return cast(list[object], value)


def _has_interior_null(values: Sequence[object]) -> bool:
    present = [index for index, value in enumerate(values) if value is not None]
    if not present:
        return False
    return any(values[index] is None for index in range(present[0], present[-1]))


def summarize(payload: object) -> Summary | None:
    """Summarize a forward response body; ``None`` means bad shape (R1).

    R1's shape: ``hourly`` holds ``time`` and the three forward names as lists
    of equal length. ``first_valid``/``last_valid`` are the first and last
    ``hourly.time`` values at which all three names are non-null (``None`` if
    there is none, or if that entry does not parse). ``consecutive`` holds when
    every ``hourly.time`` entry parses, is a whole UTC hour and is exactly one
    hour after the entry before it.
    """
    body = _as_dict(payload)
    hourly = _as_dict(body.get("hourly")) if body is not None else None
    if hourly is None:
        return None
    times = _as_list(hourly.get("time"))
    if times is None:
        return None
    columns: dict[str, list[object]] = {}
    for name in FORWARD_NAMES:
        values = _as_list(hourly.get(name))
        if values is None or len(values) != len(times):
            return None
        columns[name] = values
    parsed = [_parse_stamp(entry) for entry in times]
    stamps = [stamp for stamp in parsed if stamp is not None]
    consecutive = (
        len(stamps) == len(parsed)
        and all(_is_whole_hour(stamp) for stamp in stamps)
        and all(later - earlier == _HOUR for earlier, later in pairwise(stamps))
    )
    complete = [
        index
        for index in range(len(times))
        if all(columns[name][index] is not None for name in FORWARD_NAMES)
    ]
    return Summary(
        steps=len(times),
        nulls={
            name: sum(1 for value in values if value is None)
            for name, values in columns.items()
        },
        interior_null={
            name: _has_interior_null(values) for name, values in columns.items()
        },
        first_valid=parsed[complete[0]] if complete else None,
        last_valid=parsed[complete[-1]] if complete else None,
        first_time=parsed[0] if parsed else None,
        consecutive=consecutive,
    )


def classify(error: Exception) -> tuple[Verdict, Reason]:
    """The verdict for a request that raised; the error text is never used."""
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        if status == 429:
            return Verdict.CANT_TELL, "rate limited"
        if 400 <= status <= 499:
            return Verdict.FAIL, "http 4xx"
        if 300 <= status <= 399:
            return Verdict.CANT_TELL, "redirect"
        if 500 <= status <= 599:
            return Verdict.CANT_TELL, "http 5xx"
        # 1xx, 600 and above: any other non-2xx status (never a PASS or a FAIL).
        return Verdict.CANT_TELL, "unexpected http status"
    if isinstance(error, httpx.TransportError):
        return Verdict.CANT_TELL, "network"
    return Verdict.FAIL, "adapter rejected"


def verdict(
    outcome: Exception | None, summary: Summary | None, fetch_hour: datetime
) -> tuple[Verdict, Reason]:
    """The forward verdict: classification, then R1, R4, R2 and R3 in order.

    ``outcome`` is the exception the forward request raised, or ``None``.
    """
    if outcome is not None:
        return classify(outcome)
    if summary is None:
        return Verdict.FAIL, "bad shape"
    if (
        not summary.consecutive
        or summary.first_time is None
        or abs(summary.first_time - _as_utc(fetch_hour)) > _HOUR
    ):
        return Verdict.FAIL, "wrong times"
    if summary.steps < MIN_FORWARD_STEPS:
        return Verdict.FAIL, "too few steps"
    for name in FORWARD_NAMES:
        if summary.steps - summary.nulls[name] < MIN_FORWARD_VALUES:
            return Verdict.FAIL, "too few values"
    for name in FORWARD_NAMES:
        if summary.interior_null[name]:
            return Verdict.FAIL, "gap inside data"
    return Verdict.PASS, "ok"


def previous_runs_verdict(
    payload: object, samples: Sequence[NormalizedSample], day: date
) -> tuple[Verdict, Reason]:
    """The previous-runs verdict for day ``D``: PR1 to PR4 in order."""
    body = _as_dict(payload)
    hourly = _as_dict(body.get("hourly")) if body is not None else None
    units = _as_dict(body.get("hourly_units")) if body is not None else None
    times = _as_list(hourly.get("time")) if hourly is not None else None
    if hourly is None or units is None or times is None:
        return Verdict.FAIL, "bad shape"
    for name in PREVIOUS_RUN_NAMES:
        values = _as_list(hourly.get(name))
        if values is None or len(values) != len(times):
            return Verdict.FAIL, "bad shape"

    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    if len(times) != _PREVIOUS_RUN_ROWS or any(
        _parse_stamp(entry) != day_start + index * _HOUR
        for index, entry in enumerate(times)
    ):
        return Verdict.FAIL, "wrong times"

    if units.get("time") != "iso8601" or any(
        units.get(name) != _PREVIOUS_RUN_UNITS[provider_name]
        for name, provider_name in _PREVIOUS_RUN_COLUMNS
    ):
        return Verdict.FAIL, "wrong units"

    for sample in samples:
        bounds = FORECAST_VALUE_RANGES.get(sample.variable)
        if bounds is None or not bounds[0] <= sample.value <= bounds[1]:
            return Verdict.FAIL, "implausible value"

    expected = [day_start + hour * _HOUR for hour in range(24)]
    for variable in VARIABLES:
        for previous_day in _REQUIRED_PREVIOUS_DAYS:
            found = [
                _parse_stamp(sample.valid_at)
                for sample in samples
                if sample.variable == variable
                and sample.lead_hours == 24 * previous_day
            ]
            stamps = sorted(stamp for stamp in found if stamp is not None)
            if len(found) != len(stamps) or stamps != expected:
                return Verdict.FAIL, "too few values"
    return Verdict.PASS, "ok"


def exit_code(verdicts: Iterable[Verdict]) -> int:
    """Worst of the printed verdicts: any FAIL 1, else any CAN'T TELL 3, else 0."""
    seen = set(verdicts)
    if Verdict.FAIL in seen:
        return 1
    if Verdict.CANT_TELL in seen:
        return 3
    return 0


def _emit(line: str) -> None:
    print(line, flush=True)


def _emit_error(message: str) -> None:
    print(f"error: {message}", file=sys.stderr, flush=True)


def _emit_verdict(run: _Run, label: str, outcome: tuple[Verdict, Reason]) -> None:
    result, reason = outcome
    _emit(f"{label} verdict: {result} ({reason})")
    run.verdicts.append(result)
    run.printed.add(label)


def _emit_forward_details(summary: Summary) -> None:
    def stamp(value: datetime | None) -> str:
        return _minute(value) if value is not None else "none"

    _emit(f"forward steps: {summary.steps}")
    _emit(f"forward first time: {stamp(summary.first_time)}")
    for name in FORWARD_NAMES:
        _emit(f"forward nulls {name}: {summary.nulls[name]}")
    _emit(f"forward first valid time: {stamp(summary.first_valid)}")
    _emit(f"forward last valid time: {stamp(summary.last_valid)}")


def _emit_previous_run_details(payload: object) -> None:
    body = _as_dict(payload)
    hourly = _as_dict(body.get("hourly")) if body is not None else None
    times = _as_list(hourly.get("time")) if hourly is not None else None
    if hourly is None or times is None:
        return
    _emit(f"previous-runs steps: {len(times)}")
    for name in PREVIOUS_RUN_NAMES:
        values = _as_list(hourly.get(name))
        if values is not None:
            non_null = sum(1 for value in values if value is not None)
            _emit(f"previous-runs non-null {name}: {non_null}")


def _body_json(record: Mapping[str, object]) -> object:
    body = record.get("body")
    if not isinstance(body, bytes):
        return None
    try:
        parsed: object = json.loads(body)
    except ValueError:
        return None
    return parsed


def _read_site(db: str, site_id: int | None) -> tuple[object, object] | str:
    """The selected enabled site's (lat, lon) as read, or an error message."""
    try:
        uri = Path(db).resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            rows: list[tuple[object, object, object]] = conn.execute(
                "SELECT id, forecast_lat, forecast_lon FROM sites WHERE enabled = 1"
            ).fetchall()
        finally:
            conn.close()
    except (sqlite3.Error, OSError, ValueError):
        return "database unreadable"
    if site_id is not None:
        for row_id, lat, lon in rows:
            if row_id == site_id:
                return lat, lon
        return "site not found or disabled"
    if not rows:
        return "no enabled site"
    if len(rows) > 1:
        return f"{len(rows)} enabled sites; pass --site-id"
    _, lat, lon = rows[0]
    return lat, lon


async def _request_phase(
    lat: float,
    lon: float,
    transport: httpx.AsyncBaseTransport | None,
    now: datetime,
    run: _Run,
) -> None:
    """Steps 6 to 8: one client, the forward request, then previous runs."""
    record: dict[str, object] = {}

    async def _record(response: httpx.Response) -> None:
        await response.aread()
        record["status"] = response.status_code
        record["body"] = response.content

    req = ForecastRequest(
        lat=lat,
        lon=lon,
        model=MODEL,
        variables=VARIABLES,
        max_lead_hours=_MAX_LEAD_HOURS,
    )
    async with httpx.AsyncClient(
        transport=transport, event_hooks={"response": [_record]}
    ) as client:
        adapter = OpenMeteoAdapter(client)

        record.clear()
        forward_error: Exception | None = None
        try:
            await adapter.fetch_forecast(req)
        except Exception as exc:
            forward_error = exc
        summary = summarize(_body_json(record)) if forward_error is None else None
        if summary is not None:
            _emit_forward_details(summary)
        _emit_verdict(run, _FORWARD, verdict(forward_error, summary, floor_hour(now)))

        day = now.date() - timedelta(days=2)
        record.clear()
        try:
            result = await adapter.fetch_historical(
                req,
                window_start=f"{day}T00:00:00Z",
                window_end=f"{day + timedelta(days=1)}T00:00:00Z",
            )
        except Exception as exc:
            _emit_verdict(run, _PREVIOUS_RUNS, classify(exc))
            return
        payload = _body_json(record)
        _emit_previous_run_details(payload)
        if result is None:
            outcome: tuple[Verdict, Reason] = (Verdict.FAIL, "bad shape")
        else:
            outcome = previous_runs_verdict(payload, result.samples, day)
        _emit_verdict(run, _PREVIOUS_RUNS, outcome)


def _check(
    db: str,
    site_id: int | None,
    transport: httpx.AsyncBaseTransport | None,
    now: datetime | None,
    run: _Run,
) -> int:
    site = _read_site(db, site_id)
    if isinstance(site, str):
        _emit_error(site)
        return 2
    lat, lon = site
    screen = applicability(lat, lon)
    _emit(f"applicability: {screen}")
    run.verdicts.append(screen)
    if screen is not Verdict.PASS:
        return exit_code(run.verdicts)

    current = _as_utc(now) if now is not None else utc_now()
    if not in_side_window(current):
        _emit_error(
            "outside the test window; next window starts "
            f"{_minute(next_window_start(current))} UTC"
        )
        return 2

    lat_value = _finite_number(lat)
    lon_value = _finite_number(lon)
    if lat_value is None or lon_value is None:  # unreachable after a PASS
        return exit_code([Verdict.CANT_TELL])
    run.request_phase = True
    asyncio.run(_request_phase(lat_value, lon_value, transport, current, run))
    return exit_code(run.verdicts)


def _parse_args(argv: Sequence[str] | None) -> tuple[str, int | None]:
    parser = argparse.ArgumentParser(
        description="Owner-run icon_eu coverage check (sends provider requests)."
    )
    parser.add_argument("--db", required=True, help="Database Export copy")
    parser.add_argument("--site-id", type=int, default=None, help="enabled site id")
    args = parser.parse_args(argv)
    return str(args.db), cast(int | None, args.site_id)


def main(
    argv: Sequence[str] | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    now: datetime | None = None,
) -> int:
    """Run the check; returns the exit code.

    ``transport`` and ``now`` exist for tests; the owner run passes neither.
    argparse's own ``SystemExit(2)`` propagates before logging is touched.
    """
    db, site_id = _parse_args(argv)
    previous_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        run = _Run()
        try:
            return _check(db, site_id, transport, now, run)
        except KeyboardInterrupt:
            if run.request_phase:
                for label in _REQUEST_LABELS:
                    if label not in run.printed:
                        _emit_verdict(run, label, (Verdict.CANT_TELL, "interrupted"))
            return 1 if Verdict.FAIL in run.verdicts else 3
    finally:
        logging.disable(previous_disable)


if __name__ == "__main__":
    sys.exit(main())

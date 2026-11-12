"""The one-off fill of old Open-Meteo forecasts from Open-Meteo's archive.

``fetch_run`` asks Open-Meteo's Single Runs archive for one past model run.
It never raises for a request or reply error, and it logs only a status
code or an exception class name: never the request address, which carries
the site's coordinates, and never the reply.

``check_site`` is the check that the archive rebuilds the add-on's own
downloads: for each recorded Open-Meteo download behind a closed day, it
rebuilds the run the fill would pick for that download's fetch time, the
run one cadence older and the run one cadence newer, compares each with
what was stored, and gives each model a verdict. It writes nothing.

The fill works in units: one feed and one download day S. ``next_unit``
picks the next unit from the ledger (``om_archive_cells``); ``fill_unit``
rebuilds S's download at the round time on S, reserving the budget before
each request, and writes the unit's ``forecast_history`` rows and one ledger
row per (variable, day ahead) of a day before the site's first record, in
one transaction; ``fill_site`` runs units in a loop, as the ``om-archive
fill`` command does. ``remove_fill`` is the undo; no production path calls
it.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from email.utils import parsedate_to_datetime
from typing import Final, Literal, NamedTuple, Protocol, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from wxverify.collection.budget import (
    Reservation,
    current_billing_day,
    effective_daily_call_limit,
    is_refundable_transport_error,
    next_billing_window,
    refund_budget,
    reserve_budget,
)
from wxverify.collection.om_archive import (
    ALLOWANCE_MINUTES,
    FILL_VARIABLES,
    MAX_DAY_AHEAD,
    OLDER_RUNS_REACH,
    REFUSED_UNITS_LIMIT,
    UNIT_PACE_SECONDS,
    CandidateName,
    Cell,
    CheckCounts,
    ModelVerdict,
    RainVerdict,
    day_cells,
    download_verdict,
    fill_call_ceiling,
    fillable_model,
    first_download_date,
    keep_live,
    model_verdict,
    needs_older_run,
    newer_run,
    older_run,
    rain_label_counts,
    rain_label_verdict,
    request_hours,
    served_run,
    stitch,
)
from wxverify.core.error_sanitize import provider_reason
from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc, utc_now
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import DONE_STATES
from wxverify.db.switch import first_record_date, site_generation_id
from wxverify.feeds.open_meteo import (
    RUN_CADENCE_HOURS,
    VARIABLE_MAP,
    OpenMeteoResponse,
    run_samples,
)
from wxverify.feeds.seam import NormalizedSample
from wxverify.scoring.close_scores import (
    TruthRow,
    active_real_feeds,
    hourly_values,
    scored_variables,
)
from wxverify.scoring.downloads import scored_values, scored_values_download
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import check_domain_backoff, source_domain
from wxverify.worker.round import scoring_round_local_time

logger = logging.getLogger(__name__)

#: Open-Meteo's archive of individual model runs.
SINGLE_RUNS_URL: Final = "https://single-runs-api.open-meteo.com/v1/forecast"
#: Per-operation timeouts of one archive request. The read timeout is long
#: because the archive's replies may be slower than the live API's.
REQUEST_TIMEOUT: Final = httpx.Timeout(60.0, connect=5.0)
#: How long a request that may work later waits, unless the reply's
#: ``Retry-After`` asks for longer.
RETRY_WAIT: Final = timedelta(minutes=15)
#: The longest ``Retry-After`` taken as given; a longer one is cut to this.
_RETRY_AFTER_MAX: Final = timedelta(days=1)
#: The archive's ``run`` parameter: the run's UTC start to the minute.
_RUN_PARAM_FORMAT: Final = "%Y-%m-%dT%H:%M"
_ONE_DAY: Final = timedelta(days=1)
_ROUND_HOUR: Final = timedelta(hours=1)

# The recorded downloads behind one feed's closed days, one row per closed
# day and day ahead. Binds: site, feed.
_RECORDED_DOWNLOADS_SQL: Final = """
    SELECT local_date, day_ahead, download_id, download_date, fetched_at
    FROM close_record_downloads
    WHERE site_id = ? AND feed_id = ?
      AND download_source = 'recorded'
      AND download_id IS NOT NULL
      AND fetched_at IS NOT NULL
    ORDER BY local_date, day_ahead
"""
# One cell's stored rows: a key seek on site, feed, variable and day ahead,
# with valid_at bounds one day wider than the day each side; local_date
# decides. Binds: site, feed, variable, day ahead, the two bounds, the date.
_HISTORY_CELL_SQL: Final = """
    SELECT valid_at, value FROM forecast_history
    WHERE site_id = ? AND feed_id = ? AND variable = ? AND day_ahead = ?
      AND valid_at >= ? AND valid_at < ? AND local_date = ?
"""
# The download dates of one feed's scored rows retention still keeps.
# Binds: site, feed, the kept range's start.
_SCORED_DOWNLOAD_DATES_SQL: Final = """
    SELECT DISTINCT download_date FROM forecast_scored
    WHERE site_id = ? AND feed_id = ? AND download_id IS NOT NULL
      AND valid_at >= ?
"""
_SITE_DAYS_SQL: Final = """
    SELECT local_date, state, day_start_utc, day_end_utc
    FROM site_days WHERE site_id = ? ORDER BY local_date
"""
_TRUTH_SQL: Final = """
    SELECT quantity, value, eligible, covered_hours, rain_threshold_mm
    FROM daily_truth
    WHERE site_id = ? AND local_date = ? AND tz_generation_id = ?
"""
_OBSERVATIONS_SQL: Final = """
    SELECT valid_at FROM observations
    WHERE site_id = ? AND variable = ? AND valid_at >= ? AND valid_at < ?
"""
_OPEN_METEO_FEEDS_SQL: Final = """
    SELECT id, model, max_lead_hours FROM feeds
    WHERE source = 'open-meteo' AND is_virtual = 0
    ORDER BY id
"""


# ---------------------------------------------------------------------------
# One archive request
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Run:
    """The archive's run: its samples, converted as the adapter converts a
    download (``run_samples``), each stamped with the run as ``issued_at``."""

    run_at: datetime
    samples: tuple[NormalizedSample, ...]


@dataclass(frozen=True, slots=True)
class NoRun:
    """The archive has no such run: an HTTP 400, or a reply whose requested
    arrays hold no value. Never read as zero rain.

    ``status`` is 400 for an HTTP 400 and None for a reply with no value:
    only 400s count towards a refused unit in the fill.
    """

    status: int | None = None


@dataclass(frozen=True, slots=True)
class Retry:
    """The request may work later, from ``after`` (UTC) on: a 429, a 5xx, a
    timeout, a transport error or an unreadable reply.

    ``refundable`` is True only when the request cannot have reached the
    provider (``is_refundable_transport_error``). ``status`` is the reply's
    HTTP status, or None when there was no readable reply.
    """

    after: datetime
    refundable: bool = False
    status: int | None = None


@dataclass(frozen=True, slots=True)
class Refused:
    """The archive refused the request with any other status (401, 402,
    403, 404, ...)."""

    status: int


FetchOutcome = Run | NoRun | Retry | Refused


def _retry_after(value: str | None, now: datetime) -> datetime | None:
    """The instant a ``Retry-After`` header names (seconds or an HTTP date),
    at most ``_RETRY_AFTER_MAX`` after ``now``; None when it does not read."""
    if not value:
        return None
    text = value.strip()
    latest = now + _RETRY_AFTER_MAX
    if text.isdecimal():
        seconds = int(text)
        if seconds >= _RETRY_AFTER_MAX.total_seconds():
            return latest
        return now + timedelta(seconds=seconds)
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        return None
    instant = (
        parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    )
    return min(instant, latest)


def _retry_at(response: httpx.Response) -> datetime:
    """``RETRY_WAIT`` from now, or later when the reply's ``Retry-After``
    says so."""
    now = utc_now()
    soonest = now + RETRY_WAIT
    asked = _retry_after(response.headers.get("Retry-After"), now)
    return soonest if asked is None or asked < soonest else asked


def _has_values(data: Mapping[str, object]) -> bool:
    """Whether any requested ``hourly`` array of the reply holds a value."""
    hourly = data.get("hourly")
    if not isinstance(hourly, dict):
        return False
    arrays = cast(dict[str, object], hourly)
    for variable in FILL_VARIABLES:
        values = arrays.get(VARIABLE_MAP[variable])
        if isinstance(values, list) and any(
            item is not None for item in cast(list[object], values)
        ):
            return True
    return False


async def fetch_run(
    client: httpx.AsyncClient,
    *,
    model: str,
    run_at: datetime,
    latitude: float,
    longitude: float,
    forecast_hours: int,
) -> FetchOutcome:
    """Ask the archive for ``model``'s run started at ``run_at``.

    The request asks for ``FILL_VARIABLES`` in UTC, ``forecast_hours`` hours
    from the run, at ``latitude``/``longitude`` (no elevation, no cell
    selection, as the adapter asks), with ``REQUEST_TIMEOUT``. The outcome:

    - ``Run``: the reply validated (``OpenMeteoResponse``) and converted
      (``run_samples``, stamped with ``run_at``);
    - ``NoRun``: an HTTP 400 (one WARNING with the status and the masked
      reason, ``provider_reason``), or a reply with no value in any
      requested array;
    - ``Retry``: a 429 or a 5xx (from ``Retry-After`` when that is later
      than ``RETRY_WAIT``), an ``httpx.HTTPError`` or ``TimeoutError``, or a
      reply that does not read (a ``ValueError``, which covers a reply that
      is not JSON and one that does not validate);
    - ``Refused``: any other status.

    Never raises for any of these. Logs and returns only status codes and
    exception class names, never the address or the reply.
    """
    run_text = run_at.astimezone(UTC).strftime(_RUN_PARAM_FORMAT)
    params: dict[str, str | int | float] = {
        "latitude": latitude,
        "longitude": longitude,
        "models": model,
        "run": run_text,
        "hourly": ",".join(VARIABLE_MAP[variable] for variable in FILL_VARIABLES),
        "timezone": "UTC",
        "forecast_hours": forecast_hours,
    }
    try:
        response = await client.get(
            SINGLE_RUNS_URL, params=params, timeout=REQUEST_TIMEOUT
        )
    except (httpx.HTTPError, TimeoutError) as exc:
        logger.warning(
            "om archive request failed model=%s run=%s error=%s",
            model,
            run_text,
            type(exc).__name__,
        )
        return Retry(
            after=utc_now() + RETRY_WAIT,
            refundable=is_refundable_transport_error(exc),
        )
    status = response.status_code
    if status == 400:
        logger.warning(
            "om archive no run model=%s run=%s status=%d reason=%s",
            model,
            run_text,
            status,
            provider_reason(response) or "none",
        )
        return NoRun(status=status)
    if status == 429 or status >= 500:
        logger.warning(
            "om archive retry later model=%s run=%s status=%d",
            model,
            run_text,
            status,
        )
        return Retry(after=_retry_at(response), status=status)
    if not 200 <= status < 300:
        logger.warning(
            "om archive refused model=%s run=%s status=%d",
            model,
            run_text,
            status,
        )
        return Refused(status=status)
    try:
        payload = OpenMeteoResponse.model_validate(response.json())
        data = payload.model_dump()
        if not _has_values(data):
            return NoRun()
        samples = run_samples(model, isoformat_utc(run_at), data)
    except (ValueError, OverflowError) as exc:
        logger.warning(
            "om archive unreadable reply model=%s run=%s status=%d error=%s",
            model,
            run_text,
            status,
            type(exc).__name__,
        )
        return Retry(after=utc_now() + RETRY_WAIT, status=status)
    return Run(run_at=run_at, samples=tuple(samples))


# ---------------------------------------------------------------------------
# The check
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ModelCheck:
    """One model's line of the check: its counts, its downloads that could
    not be rebuilt (``fetch_errors``), the share of its downloads fetched
    in the hour after the round's start (None with no download), and its
    two verdicts."""

    feed_id: int
    model: str
    counts: CheckCounts
    fetch_errors: int
    round_share: float | None
    verdict: ModelVerdict
    rain_verdict: RainVerdict

    @property
    def passed(self) -> bool:
        """PASS or STALE, and RAIN OK: the model may be filled."""
        return self.verdict in ("PASS", "STALE") and self.rain_verdict == "RAIN OK"


@dataclass(frozen=True, slots=True)
class SiteCheck:
    """The check of one site: a line per active Open-Meteo model, or the
    reason the site could not be checked."""

    site_id: int
    models: tuple[ModelCheck, ...]
    problem: str | None = None

    @property
    def passed(self) -> bool:
        """The site was checked, has a model, and every model passed."""
        return (
            self.problem is None
            and bool(self.models)
            and all(model.passed for model in self.models)
        )


@dataclass(slots=True)
class _Download:
    """One recorded download and its stored hours, by (variable, instant).

    ``history_dates`` are the closed days whose ``forecast_history`` rows
    came from it.
    """

    download_id: int
    download_date: date
    fetched_at: datetime
    stored: dict[tuple[str, datetime], float] = field(
        default_factory=dict[tuple[str, datetime], float]
    )
    history_dates: set[date] = field(default_factory=set[date])


@dataclass(frozen=True, slots=True)
class _FeedDownloads:
    feed_id: int
    model: str
    max_lead_hours: int
    downloads: tuple[_Download, ...]


@dataclass(frozen=True, slots=True)
class _CheckInputs:
    """What the check reads from the database, in one read.

    ``closed_hours`` holds, per closed day, the (variable, instant) of each
    observed hour of a variable scored that day: the hours its
    ``forecast_history`` rows can hold. ``kept_start`` is the start of the
    oldest day not yet closed or missed: ``forecast_scored`` keeps the hours
    from it on (None when every day is done).
    """

    latitude: float
    longitude: float
    zone: ZoneInfo
    round_start: time
    closed_hours: Mapping[date, frozenset[tuple[str, datetime]]]
    kept_start: datetime | None
    feeds: tuple[_FeedDownloads, ...]


def check_site_ids(conn: sqlite3.Connection, site_id: int | None) -> list[int]:
    """The sites to check: ``site_id`` when given, else every enabled site,
    by id. Read-only."""
    if site_id is not None:
        return [site_id]
    return [
        int(row["id"])
        for row in conn.execute("SELECT id FROM sites WHERE enabled = 1 ORDER BY id")
    ]


def _closed_hours(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    local_date: date,
    start: datetime,
    end: datetime,
) -> frozenset[tuple[str, datetime]]:
    """The observed hours of the variables scored on one closed day.

    The scored variables come from the day's truth rows (``scored_variables``)
    and the hours from ``observations``, keyed by instant inside the day's
    bounds, as the close pairs them.
    """
    truth = [
        TruthRow(
            quantity=str(row["quantity"]),
            value=None if row["value"] is None else float(row["value"]),
            eligible=int(row["eligible"]) == 1,
            covered_hours=int(row["covered_hours"]),
            rain_threshold_mm=(
                None
                if row["rain_threshold_mm"] is None
                else float(row["rain_threshold_mm"])
            ),
        )
        for row in conn.execute(
            _TRUTH_SQL, (site_id, local_date.isoformat(), generation_id)
        )
    ]
    # Text bounds one day wider than the day, so a stamp in another spelling
    # is still read; the instant decides.
    low = isoformat_utc(start - _ONE_DAY)
    high = isoformat_utc(end + _ONE_DAY)
    hours: set[tuple[str, datetime]] = set()
    for variable in sorted(scored_variables(truth)):
        for row in conn.execute(_OBSERVATIONS_SQL, (site_id, variable, low, high)):
            try:
                instant = parse_utc(str(row["valid_at"]))
            except ValueError:
                continue
            if start <= instant < end:
                hours.add((variable, instant))
    return frozenset(hours)


def _register(
    downloads: dict[int, _Download],
    download_id: int,
    download_date: object,
    fetched_at: object,
) -> _Download | None:
    """The download of ``download_id``, made on first sight; None when its
    date or fetch time does not read."""
    known = downloads.get(download_id)
    if known is not None:
        return known
    try:
        made_on = date.fromisoformat(str(download_date))
        fetched = parse_utc(str(fetched_at))
    except ValueError:
        return None
    download = _Download(
        download_id=download_id, download_date=made_on, fetched_at=fetched
    )
    downloads[download_id] = download
    return download


def _feed_downloads(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    zone_name: str,
    closed: Mapping[date, tuple[datetime, datetime]],
    kept_start: datetime | None,
) -> tuple[_Download, ...]:
    """One feed's recorded downloads, each with its stored hours.

    From ``close_record_downloads`` of closed days (``recorded``, with a
    download id and a fetch time), with that day's ``forecast_history`` rows
    of the cell; and from ``forecast_scored``, read through
    ``scored_values_download`` and ``scored_values`` (the readers' own
    lead, value and rain-label filters) for the hours from ``kept_start``
    on, where the download is ``recorded``. Ordered by download date.
    """
    downloads: dict[int, _Download] = {}
    for row in conn.execute(_RECORDED_DOWNLOADS_SQL, (site_id, feed_id)).fetchall():
        try:
            local_date = date.fromisoformat(str(row["local_date"]))
        except ValueError:
            continue
        bounds = closed.get(local_date)
        if bounds is None:
            continue
        download = _register(
            downloads, int(row["download_id"]), row["download_date"], row["fetched_at"]
        )
        if download is None:
            continue
        download.history_dates.add(local_date)
        day_ahead = int(row["day_ahead"])
        low = isoformat_utc(bounds[0] - _ONE_DAY)
        high = isoformat_utc(bounds[1] + _ONE_DAY)
        for variable in FILL_VARIABLES:
            for stored in conn.execute(
                _HISTORY_CELL_SQL,
                (
                    site_id,
                    feed_id,
                    variable,
                    day_ahead,
                    low,
                    high,
                    local_date.isoformat(),
                ),
            ):
                try:
                    instant = parse_utc(str(stored["valid_at"]))
                except ValueError:
                    continue
                download.stored[(variable, instant)] = float(stored["value"])
    if kept_start is not None:
        dates = {download.download_date for download in downloads.values()}
        for row in conn.execute(
            _SCORED_DOWNLOAD_DATES_SQL, (site_id, feed_id, isoformat_utc(kept_start))
        ).fetchall():
            try:
                dates.add(date.fromisoformat(str(row["download_date"])))
            except ValueError:
                continue
        for download_date in sorted(dates):
            _add_scored_hours(
                conn, downloads, site_id, feed_id, zone_name, download_date, kept_start
            )
    return tuple(
        sorted(
            downloads.values(),
            key=lambda download: (download.download_date, download.download_id),
        )
    )


def _add_scored_hours(
    conn: sqlite3.Connection,
    downloads: dict[int, _Download],
    site_id: int,
    feed_id: int,
    zone_name: str,
    download_date: date,
    kept_start: datetime,
) -> None:
    """The ``forecast_scored`` hours, from ``kept_start`` on, of the
    recorded download made on ``download_date``, at each day ahead."""
    for day_ahead in range(MAX_DAY_AHEAD + 1):
        local_date = download_date + timedelta(days=day_ahead)
        day_start, day_end, _hours = local_day_slots(local_date, zone_name)
        start = max(day_start, kept_start)
        if start >= day_end:
            continue
        scored = scored_values_download(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            day_ahead=day_ahead,
            start_utc=start,
            end_utc=day_end,
        )
        if scored is None or scored.source != "recorded" or scored.download_id is None:
            continue
        download = _register(
            downloads,
            scored.download_id,
            scored.local_date.isoformat(),
            scored.fetched_at,
        )
        if download is None:
            continue
        values = scored_values(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            day_ahead=day_ahead,
            start_utc=start,
            end_utc=day_end,
        )
        for sample in values.values():
            try:
                instant = parse_utc(sample.valid_at)
            except ValueError:
                continue
            download.stored[(sample.variable, instant)] = sample.value


def _check_inputs(conn: sqlite3.Connection, site_id: int) -> _CheckInputs | str:
    """Everything the check of ``site_id`` reads, or the fixed text of why it
    cannot be checked. Read-only."""
    site = conn.execute(
        "SELECT forecast_lat, forecast_lon FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        return "no such site"
    generation_id = site_generation_id(conn, site_id)
    if generation_id is None:
        return "no published time zone generation"
    zone_row = conn.execute(
        "SELECT timezone FROM timezone_generations WHERE id = ?", (generation_id,)
    ).fetchone()
    if zone_row is None:
        return "no published time zone generation"
    zone_name = str(zone_row["timezone"])
    try:
        zone = ZoneInfo(zone_name)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        return "the time zone does not load"
    hour, minute = scoring_round_local_time(conn)
    closed: dict[date, tuple[datetime, datetime]] = {}
    closed_hours: dict[date, frozenset[tuple[str, datetime]]] = {}
    kept_start: datetime | None = None
    for row in conn.execute(_SITE_DAYS_SQL, (site_id,)).fetchall():
        state = str(row["state"])
        try:
            local_date = date.fromisoformat(str(row["local_date"]))
            start = parse_utc(str(row["day_start_utc"]))
            end = parse_utc(str(row["day_end_utc"]))
        except ValueError:
            continue
        if state == "closed":
            closed[local_date] = (start, end)
            closed_hours[local_date] = _closed_hours(
                conn, site_id, generation_id, local_date, start, end
            )
        elif state not in DONE_STATES and kept_start is None:
            kept_start = start
    active = active_real_feeds(conn, site_id)
    feeds = tuple(
        _FeedDownloads(
            feed_id=int(row["id"]),
            model=str(row["model"]),
            max_lead_hours=int(row["max_lead_hours"]),
            downloads=_feed_downloads(
                conn, site_id, int(row["id"]), zone_name, closed, kept_start
            ),
        )
        for row in conn.execute(_OPEN_METEO_FEEDS_SQL).fetchall()
        if int(row["id"]) in active
    )
    return _CheckInputs(
        latitude=float(site["forecast_lat"]),
        longitude=float(site["forecast_lon"]),
        zone=zone,
        round_start=time(hour, minute),
        closed_hours=closed_hours,
        kept_start=kept_start,
        feeds=feeds,
    )


@dataclass(frozen=True, slots=True)
class _Rebuilt:
    """One candidate's rebuild: whether the archive had its own run, the
    stitched reply, and the hours the live download would have scored."""

    own_run: bool
    stitched: list[NormalizedSample]
    kept: list[NormalizedSample]


class _RunSource(Protocol):
    """Where ``_rebuild`` gets the archive's runs of one download: the
    check's ``_RunCache``, or the fill's ``_FillRuns``, which reserves the
    budget before each request."""

    async def get(self, run: datetime) -> FetchOutcome: ...


class _RunCache:
    """The archive's runs for one download, each asked for once."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        inputs: _CheckInputs,
        model: str,
        fetch_at: datetime,
        max_lead_hours: int,
    ) -> None:
        self._client = client
        self._inputs = inputs
        self._model = model
        self._fetch_at = fetch_at
        self._max_lead_hours = max_lead_hours
        self._outcomes: dict[datetime, FetchOutcome] = {}

    async def get(self, run: datetime) -> FetchOutcome:
        known = self._outcomes.get(run)
        if known is not None:
            return known
        outcome = await fetch_run(
            self._client,
            model=self._model,
            run_at=run,
            latitude=self._inputs.latitude,
            longitude=self._inputs.longitude,
            forecast_hours=request_hours(run, self._fetch_at, self._max_lead_hours),
        )
        self._outcomes[run] = outcome
        return outcome


async def _rebuild(
    cache: _RunSource,
    model: str,
    run: datetime,
    fetch_at: datetime,
    max_lead_hours: int,
) -> _Rebuilt | Retry | Refused:
    """The download at ``fetch_at`` rebuilt as served from ``run``.

    ``run``'s samples, then older runs one cadence at a time, no more than
    ``OLDER_RUNS_REACH`` before ``run``, while ``needs_older_run`` says
    so. A ``Retry`` or ``Refused`` on any of them is returned as it is.
    """
    first = await cache.get(run)
    if isinstance(first, Retry | Refused):
        return first
    replies: list[tuple[NormalizedSample, ...] | None] = [
        first.samples if isinstance(first, Run) else None
    ]
    older = run
    while needs_older_run(replies, model, fetch_at, max_lead_hours):
        older = older_run(model, older)
        if run - older > OLDER_RUNS_REACH:
            break
        outcome = await cache.get(older)
        if isinstance(outcome, Retry | Refused):
            return outcome
        replies.append(outcome.samples if isinstance(outcome, Run) else None)
    stitched = stitch(
        [reply for reply in replies if reply is not None], fetch_at, max_lead_hours
    )
    return _Rebuilt(
        own_run=isinstance(first, Run),
        stitched=stitched,
        kept=keep_live(stitched, model, fetch_at, max_lead_hours),
    )


def _by_hour(
    samples: Iterable[NormalizedSample], variable: str
) -> dict[datetime, float]:
    return {
        parse_utc(sample.valid_at): sample.value
        for sample in samples
        if sample.variable == variable
    }


def _stored_by_hour(download: _Download, variable: str) -> dict[datetime, float]:
    return {
        instant: value
        for (stored_variable, instant), value in download.stored.items()
        if stored_variable == variable
    }


def _coverage(
    download: _Download, kept: list[NormalizedSample], inputs: _CheckInputs
) -> tuple[int, int]:
    """The rebuilt hours with no stored hour, and the stored hours with no
    rebuilt one, over the hours a source can hold.

    A rebuilt hour (day ahead 0 to ``MAX_DAY_AHEAD``) counts when it is an
    observed hour of a scored variable on a closed day whose rows came
    from this download, or when it lies in the range ``forecast_scored``
    still keeps.
    """
    rebuilt = {
        (sample.variable, parse_utc(sample.valid_at))
        for cell_values in day_cells(kept, download.download_date, inputs.zone).values()
        for sample in cell_values
    }
    history_hours: set[tuple[str, datetime]] = set()
    for local_date in download.history_dates:
        history_hours |= inputs.closed_hours.get(local_date, frozenset())
    kept_start = inputs.kept_start
    holdable = {
        key
        for key in rebuilt
        if key in history_hours or (kept_start is not None and key[1] >= kept_start)
    }
    stored = set(download.stored)
    return len(holdable - stored), len(stored - rebuilt)


def _in_round_hour(download: _Download, inputs: _CheckInputs) -> bool:
    """Whether the download was fetched in the hour after the round's start
    on its download date, site-local."""
    start = datetime.combine(
        download.download_date, inputs.round_start, tzinfo=inputs.zone
    ).astimezone(UTC)
    return start <= download.fetched_at < start + _ROUND_HOUR


@dataclass(slots=True)
class _ModelState:
    counts: CheckCounts = field(default_factory=CheckCounts)
    fetch_errors: int = 0
    in_round: int = 0
    refused: bool = False


async def _check_download(
    client: httpx.AsyncClient,
    inputs: _CheckInputs,
    feed: _FeedDownloads,
    download: _Download,
    state: _ModelState,
) -> Retry | Refused | None:
    """Rebuild one download three ways and add its outcome to ``state``.

    Returns the ``Retry`` or ``Refused`` that stopped a rebuild (the
    download is then left undecided), else None.
    """
    model = feed.model
    fetch_at = download.fetched_at
    r1 = served_run(model, fetch_at)
    runs: tuple[tuple[CandidateName, datetime], ...] = (
        ("r1", r1),
        ("older", older_run(model, r1)),
        ("newer", newer_run(model, r1)),
    )
    cache = _RunCache(client, inputs, model, fetch_at, feed.max_lead_hours)
    rebuilt: dict[CandidateName, _Rebuilt] = {}
    for name, run in runs:
        result = await _rebuild(cache, model, run, fetch_at, feed.max_lead_hours)
        if isinstance(result, Retry | Refused):
            return result
        rebuilt[name] = result
    candidates: dict[CandidateName, Mapping[datetime, float] | None] = {
        name: _by_hour(result.kept, "temperature") if result.own_run else None
        for name, result in rebuilt.items()
    }
    verdict = download_verdict(_stored_by_hour(download, "temperature"), candidates)
    counts = state.counts
    if verdict == "undecided":
        return None
    counts.decided += 1
    if verdict == "r1":
        counts.r1_match += 1
    elif verdict == "older":
        counts.older_match += 1
        gap = math.ceil((fetch_at - r1).total_seconds() / 60)
        counts.older_gap_minutes = max(counts.older_gap_minutes, gap)
    elif verdict == "newer":
        counts.newer_match += 1
    else:
        counts.no_match += 1
    extra, missing = _coverage(download, rebuilt["r1"].kept, inputs)
    counts.extra_hours += extra
    counts.missing_hours += missing
    if verdict in ("r1", "newer"):
        rain = rain_label_counts(
            _stored_by_hour(download, "precip"),
            _by_hour(rebuilt[verdict].stitched, "precip"),
        )
        counts.wet_hours += rain.wet_hours
        counts.rain_unshifted += rain.unshifted
        counts.rain_minus1 += rain.minus1
        counts.rain_plus1 += rain.plus1
    return None


async def _check_feed(
    client: httpx.AsyncClient,
    inputs: _CheckInputs,
    feed: _FeedDownloads,
    *,
    throttled: bool,
) -> tuple[ModelCheck, bool]:
    """One model's line of the check, and whether the archive has asked
    the check to slow down (a 429), after which no request is made."""
    state = _ModelState()
    known = feed.model in ALLOWANCE_MINUTES and feed.model in RUN_CADENCE_HOURS
    for download in feed.downloads:
        state.counts.downloads += 1
        if _in_round_hour(download, inputs):
            state.in_round += 1
        if not known:
            continue
        if throttled or state.refused:
            state.fetch_errors += 1
            continue
        stopped = await _check_download(client, inputs, feed, download, state)
        if stopped is None:
            continue
        state.fetch_errors += 1
        if isinstance(stopped, Refused):
            state.refused = True
        elif stopped.status == 429:
            throttled = True
    counts = state.counts
    share = None if counts.downloads == 0 else state.in_round / counts.downloads
    return (
        ModelCheck(
            feed_id=feed.feed_id,
            model=feed.model,
            counts=counts,
            fetch_errors=state.fetch_errors,
            round_share=share,
            verdict=model_verdict(counts),
            rain_verdict=rain_label_verdict(counts),
        ),
        throttled,
    )


async def check_site(
    db: Database, site_id: int, *, client: httpx.AsyncClient | None = None
) -> SiteCheck:
    """Check that the archive rebuilds the site's own Open-Meteo downloads.

    For each Open-Meteo feed active at the site, each recorded download
    behind a closed day (or still in ``forecast_scored``) is rebuilt at its
    own fetch time from three runs: the run the fill would pick
    (``served_run``), the run one cadence older and the run one cadence
    newer, each stitched with older runs and kept as the live download
    would have scored it. Its stored temperature decides which run it
    matches (``download_verdict``); its rain, on the matched served or
    newer run, gives the rain-label counts; and its hours give the extra
    and missing counts against the served run's rebuild. A ``Refused``
    stops that model's requests, a 429 stops every request after it; each
    download not rebuilt counts as a fetch error and stays undecided.

    The database is read once, before any request, and nothing is written.
    A site that cannot be checked gives a ``SiteCheck`` with ``problem``
    set. Passing ``client`` reuses it; otherwise one is made and closed.
    """
    if client is None:
        async with httpx.AsyncClient() as owned:
            return await check_site(db, site_id, client=owned)

    def _read(conn: sqlite3.Connection) -> _CheckInputs | str:
        return _check_inputs(conn, site_id)

    inputs = await db.read(_read)
    if isinstance(inputs, str):
        return SiteCheck(site_id=site_id, models=(), problem=inputs)
    models: list[ModelCheck] = []
    throttled = False
    for feed in inputs.feeds:
        line, throttled = await _check_feed(client, inputs, feed, throttled=throttled)
        models.append(line)
    return SiteCheck(site_id=site_id, models=tuple(models))


# ---------------------------------------------------------------------------
# The fill
# ---------------------------------------------------------------------------

#: A ledger row's outcome (``om_archive_cells.outcome``).
FillOutcome = Literal["written", "present", "no_hours", "no_run", "refused"]
#: Why a unit waits: the fill's daily ceiling (or the add-on's own limit),
#: a hold on the add-on's own Open-Meteo domain, a request that may work
#: later, or the unit's wall-clock bound.
DeferReason = Literal["ceiling", "backoff", "retry", "deadline"]
#: Why the command-line fill stopped before the last unit: a deferral it
#: does not wait out, or a unit that could no longer be filled.
PauseReason = Literal["ceiling", "backoff", "retry", "deadline", "skipped"]

#: The wall-clock bound on one unit's requests, in seconds. Past it the
#: unit writes nothing and waits ``RETRY_WAIT``.
UNIT_DEADLINE_SECONDS: Final = 60
#: The command-line fill stops at this many deferrals in a row with no unit
#: done between them, so a reply that never reads cannot keep it retrying.
CLI_RETRY_LIMIT: Final = 5
#: The longest deferral the command-line fill waits out. At a longer one,
#: and at the daily ceiling, it stops; the next run carries on from the
#: ledger.
CLI_LONGEST_WAIT: Final = timedelta(hours=1)
#: The fixed text of each reason the command-line fill stopped early.
PAUSE_TEXT: Final[Mapping[PauseReason, str]] = {
    "ceiling": "the daily call ceiling is reached",
    "backoff": "the add-on's own Open-Meteo hold has not ended",
    "retry": "the archive did not answer",
    "deadline": "the archive answered too slowly",
    "skipped": "a unit could no longer be filled",
}

_OPEN_METEO: Final = "open-meteo"
#: A cell's ``valid_at`` text bounds, from its local date's UTC midnight:
#: the day's widest span in any zone (UTC-12 to UTC+14), one day wider each
#: side. They only let the key seek; ``local_date`` decides.
_CELL_LOW_REACH: Final = timedelta(days=1, hours=14)
_CELL_HIGH_REACH: Final = timedelta(days=2, hours=12)

_SITE_SQL: Final = "SELECT forecast_lat, forecast_lon FROM sites WHERE id = ?"
_ZONE_SQL: Final = "SELECT timezone FROM timezone_generations WHERE id = ?"
_FIRST_PRECIP_SQL: Final = """
    SELECT MIN(valid_at) FROM observations
    WHERE site_id = ? AND variable = 'precip'
"""
# The latest local date of one feed and variable's day-ahead-0 ledger rows:
# its last download day done.
_LEDGER_LATEST_SQL: Final = """
    SELECT MAX(local_date) FROM om_archive_cells
    WHERE site_id = ? AND feed_id = ? AND variable = ? AND day_ahead = 0
"""
_LEDGER_CELL_SQL: Final = """
    SELECT 1 FROM om_archive_cells
    WHERE site_id = ? AND feed_id = ? AND variable = ? AND day_ahead = ?
      AND local_date = ?
"""
# Whether a cell has a forecast_history row: the shape of the close's own
# delete of a day's rows, a key seek on site, feed, variable and day ahead
# within the cell's text bounds; local_date decides.
_HISTORY_PRESENT_SQL: Final = """
    SELECT 1 FROM forecast_history
    WHERE site_id = ? AND feed_id = ? AND variable = ? AND day_ahead = ?
      AND valid_at >= ? AND valid_at < ? AND local_date = ?
    LIMIT 1
"""
# One filled hour. A key that another day's row already holds is left as
# it is, and the hour is not written.
_INSERT_FILLED_SQL: Final = """
    INSERT INTO forecast_history
        (site_id, feed_id, variable, day_ahead, valid_at, local_date,
         value, lead_hours, issued_at, contributors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
    ON CONFLICT (site_id, feed_id, variable, day_ahead, valid_at) DO NOTHING
"""
_INSERT_LEDGER_SQL: Final = """
    INSERT INTO om_archive_cells
        (site_id, feed_id, variable, day_ahead, local_date, download_date,
         outcome, hours, run_at, filled_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
# A refused row for each remaining cell of an excluded model; a cell
# already in the ledger keeps its row.
_INSERT_REFUSED_SQL: Final = """
    INSERT INTO om_archive_cells
        (site_id, feed_id, variable, day_ahead, local_date, download_date,
         outcome, hours, run_at, filled_at)
    VALUES (?, ?, ?, ?, ?, ?, 'refused', 0, NULL, ?)
    ON CONFLICT (site_id, feed_id, variable, day_ahead, local_date) DO NOTHING
"""
_SOURCE_LIMIT_SQL: Final = (
    "SELECT daily_call_limit, billing_tz FROM sources WHERE source = ?"
)
_CALLS_TODAY_SQL: Final = (
    "SELECT calls FROM api_budget WHERE source = ? AND billing_day = ?"
)
_WRITTEN_CELLS_SQL: Final = """
    SELECT site_id, feed_id, variable, day_ahead, local_date
    FROM om_archive_cells
    WHERE outcome = 'written'
    ORDER BY site_id, feed_id, variable, day_ahead, local_date
"""
# One written cell's rows: the statement of the close's own delete of a
# day's rows, one key seek per cell. Binds: site, feed, variable, day
# ahead, the two text bounds, the date.
_DELETE_FILLED_SQL: Final = (
    "DELETE FROM forecast_history"
    " WHERE site_id = ? AND feed_id = ? AND variable = ? AND day_ahead = ?"
    " AND valid_at >= ? AND valid_at < ? AND local_date = ?"
)
_CLEAR_LEDGER_SQL: Final = "DELETE FROM om_archive_cells"


@dataclass(slots=True)
class FillCounts:
    """Counts of the fill: ledger rows by outcome (``written`` counts
    cells, ``rows`` the ``forecast_history`` rows they hold) and counted
    Open-Meteo calls."""

    written: int = 0
    present: int = 0
    no_hours: int = 0
    no_run: int = 0
    refused: int = 0
    rows: int = 0
    calls: int = 0

    def add(self, other: FillCounts) -> None:
        """Add ``other``'s counts to these."""
        self.written += other.written
        self.present += other.present
        self.no_hours += other.no_hours
        self.no_run += other.no_run
        self.refused += other.refused
        self.rows += other.rows
        self.calls += other.calls

    def count(self, outcome: FillOutcome) -> None:
        """Count one ledger row of ``outcome``."""
        if outcome == "written":
            self.written += 1
        elif outcome == "present":
            self.present += 1
        elif outcome == "no_hours":
            self.no_hours += 1
        elif outcome == "no_run":
            self.no_run += 1
        else:
            self.refused += 1


class FillUnit(NamedTuple):
    """One unit of the fill: a feed and a download day S."""

    feed_id: int
    download_date: date


@dataclass(frozen=True, slots=True)
class UnitDone:
    """A unit written: its ledger rows and rows (``counts``), whether any
    run of the archive served it (``found_run``), and whether every request
    got an HTTP 400 (``refused_unit``)."""

    feed_id: int
    model: str
    download_date: date
    counts: FillCounts
    found_run: bool
    refused_unit: bool


@dataclass(frozen=True, slots=True)
class UnitDeferred:
    """A unit that wrote nothing and waits until ``until`` (UTC);
    ``calls`` were counted before it stopped."""

    feed_id: int
    model: str
    download_date: date
    until: datetime
    reason: DeferReason
    calls: int


@dataclass(frozen=True, slots=True)
class ModelExcluded:
    """The model is out of the fill: every remaining cell got a ``refused``
    ledger row (``counts.refused``). ``reason`` is the fixed text of the
    ERROR line."""

    feed_id: int
    model: str
    reason: str
    counts: FillCounts


@dataclass(frozen=True, slots=True)
class UnitSkipped:
    """A unit the fill does not fill (the site, the feed or the day is
    outside the fill); nothing was asked for or written."""

    feed_id: int
    download_date: date
    reason: str


UnitResult = UnitDone | UnitDeferred | ModelExcluded | UnitSkipped


@dataclass(frozen=True, slots=True)
class FeedFill:
    """One feed's counts in a run of ``fill_site``."""

    feed_id: int
    model: str
    counts: FillCounts


@dataclass(frozen=True, slots=True)
class SiteFill:
    """One site's run of ``fill_site``: a line per feed filled, the active
    Open-Meteo models left out of the fill, and why the run stopped early
    (``paused``) or why the site cannot be filled (``problem``)."""

    site_id: int
    feeds: tuple[FeedFill, ...]
    left_out: tuple[str, ...] = ()
    paused: PauseReason | None = None
    problem: str | None = None

    @property
    def finished(self) -> bool:
        """The site was filled and no unit is left."""
        return self.problem is None and self.paused is None


@dataclass(frozen=True, slots=True)
class _FillFeed:
    feed_id: int
    model: str
    max_lead_hours: int


@dataclass(frozen=True, slots=True)
class _FillSite:
    """What the fill reads of one site.

    ``first_record`` is F, the site's first record (``first_record_date``);
    ``first_download`` is S_first (``first_download_date`` of the local
    date of the first rain observation). ``feeds`` are the active Open-Meteo
    feeds the fill fills, by id; ``left_out`` the active ones it does not.
    """

    latitude: float
    longitude: float
    zone_name: str
    zone: ZoneInfo
    round_start: time
    first_record: date
    first_download: date
    feeds: tuple[_FillFeed, ...]
    left_out: tuple[_FillFeed, ...]

    @property
    def last_download(self) -> date:
        """S_last: the day before the first record."""
        return self.first_record - _ONE_DAY


@dataclass(frozen=True, slots=True)
class _UnitPlan:
    """One unit before any request: whether a cell of it has no ledger row
    (``pending``), and whether such a cell also has no ``forecast_history``
    row (``empty``), the only case that asks the archive."""

    site: _FillSite
    feed: _FillFeed
    download_date: date
    fetch_at: datetime
    pending: bool
    empty: bool


class _Hold(Exception):
    """A request not made: the fill waits until ``until`` (a UTC stamp)."""

    def __init__(self, reason: DeferReason, until: str, ceiling: int | None) -> None:
        super().__init__(reason)
        self.reason: DeferReason = reason
        self.until = until
        self.ceiling = ceiling


def _utc_span(first: date, last: date) -> tuple[datetime, datetime]:
    """UTC instants that hold every local day from ``first`` to ``last`` in
    any zone, one day wider each side."""
    low = datetime.combine(first, time(0), tzinfo=UTC) - _CELL_LOW_REACH
    high = datetime.combine(last, time(0), tzinfo=UTC) + _CELL_HIGH_REACH
    return low, high


def _cell_bounds(local_date: date) -> tuple[str, str]:
    """A cell's ``valid_at`` text bounds (``_utc_span`` of its day)."""
    low, high = _utc_span(local_date, local_date)
    return isoformat_utc(low), isoformat_utc(high)


def _unit_cells(download_date: date, first_record: date) -> list[tuple[str, int, date]]:
    """The unit's cells, (variable, day ahead k, D = S + k), of the days
    before the first record, by variable then day ahead."""
    return [
        (variable, day_ahead, local_date)
        for variable in FILL_VARIABLES
        for day_ahead in range(MAX_DAY_AHEAD + 1)
        if (local_date := download_date + timedelta(days=day_ahead)) < first_record
    ]


def _fill_site(conn: sqlite3.Connection, site_id: int) -> _FillSite | str:
    """What the fill reads of ``site_id``, or the fixed text of why the site
    is not filled. Read-only.

    The site needs a published time zone generation (its zone is the
    pre-close days' zone), a first record, and a rain observation.
    """
    site = conn.execute(_SITE_SQL, (site_id,)).fetchone()
    if site is None:
        return "no such site"
    generation_id = site_generation_id(conn, site_id)
    if generation_id is None:
        return "no published time zone generation"
    zone_row = conn.execute(_ZONE_SQL, (generation_id,)).fetchone()
    if zone_row is None:
        return "no published time zone generation"
    zone_name = str(zone_row["timezone"])
    try:
        zone = ZoneInfo(zone_name)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        return "the time zone does not load"
    first_record = first_record_date(conn, site_id)
    if first_record is None:
        return "no day record yet"
    first_row = conn.execute(_FIRST_PRECIP_SQL, (site_id,)).fetchone()
    if first_row is None or first_row[0] is None:
        return "no rain observation"
    try:
        first_observed = parse_utc(str(first_row[0])).astimezone(zone).date()
    except ValueError:
        return "the first rain observation does not read"
    hour, minute = scoring_round_local_time(conn)
    active = active_real_feeds(conn, site_id)
    feeds: list[_FillFeed] = []
    left_out: list[_FillFeed] = []
    for row in conn.execute(_OPEN_METEO_FEEDS_SQL).fetchall():
        feed = _FillFeed(
            feed_id=int(row["id"]),
            model=str(row["model"]),
            max_lead_hours=int(row["max_lead_hours"]),
        )
        if feed.feed_id not in active:
            continue
        (feeds if fillable_model(feed.model) else left_out).append(feed)
    return _FillSite(
        latitude=float(site["forecast_lat"]),
        longitude=float(site["forecast_lon"]),
        zone_name=zone_name,
        zone=zone,
        round_start=time(hour, minute),
        first_record=first_record,
        first_download=first_download_date(first_observed),
        feeds=tuple(feeds),
        left_out=tuple(left_out),
    )


def _next_download_date(
    conn: sqlite3.Connection, site_id: int, feed_id: int, first_download: date
) -> date:
    """The feed's next download day: for each variable, the latest local
    date of its day-ahead-0 ledger rows plus one, or ``first_download``
    with none; the earliest over the variables."""
    starts: list[date] = []
    for variable in FILL_VARIABLES:
        row = conn.execute(_LEDGER_LATEST_SQL, (site_id, feed_id, variable)).fetchone()
        latest = None if row is None or row[0] is None else str(row[0])
        starts.append(
            first_download if latest is None else date.fromisoformat(latest) + _ONE_DAY
        )
    return min(starts)


def next_unit(conn: sqlite3.Connection, site_id: int) -> FillUnit | None:
    """The site's next unit of the fill, or None when none is left.

    Over the active Open-Meteo feeds the fill fills (``fillable_model``),
    the smallest next download day (``_next_download_date``), ties to the
    lower feed id, while it is at most S_last, the day before the first
    record. None also when the site is not filled (``_fill_site``).
    Read-only.
    """
    site = _fill_site(conn, site_id)
    if isinstance(site, str):
        return None
    best: FillUnit | None = None
    for feed in site.feeds:
        start = _next_download_date(conn, site_id, feed.feed_id, site.first_download)
        if start > site.last_download:
            continue
        if best is None or start < best.download_date:
            best = FillUnit(feed_id=feed.feed_id, download_date=start)
    return best


def _cell_in_ledger(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    local_date: date,
) -> bool:
    row = conn.execute(
        _LEDGER_CELL_SQL,
        (site_id, feed_id, variable, day_ahead, local_date.isoformat()),
    ).fetchone()
    return row is not None


def _cell_has_history(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    local_date: date,
) -> bool:
    low, high = _cell_bounds(local_date)
    row = conn.execute(
        _HISTORY_PRESENT_SQL,
        (site_id, feed_id, variable, day_ahead, low, high, local_date.isoformat()),
    ).fetchone()
    return row is not None


def _unit_plan(
    conn: sqlite3.Connection, site_id: int, feed_id: int, download_date: date
) -> _UnitPlan | str:
    """The unit before any request, or the fixed text of why it is not
    filled. Read-only."""
    site = _fill_site(conn, site_id)
    if isinstance(site, str):
        return site
    feed = next((item for item in site.feeds if item.feed_id == feed_id), None)
    if feed is None:
        return "not an active Open-Meteo feed the fill fills"
    if not site.first_download <= download_date <= site.last_download:
        return "the download day is outside the fill"
    pending = empty = False
    for variable, day_ahead, local_date in _unit_cells(
        download_date, site.first_record
    ):
        if _cell_in_ledger(conn, site_id, feed_id, variable, day_ahead, local_date):
            continue
        pending = True
        if not _cell_has_history(
            conn, site_id, feed_id, variable, day_ahead, local_date
        ):
            empty = True
            break
    fetch_at = datetime.combine(
        download_date, site.round_start, tzinfo=site.zone
    ).astimezone(UTC)
    return _UnitPlan(
        site=site,
        feed=feed,
        download_date=download_date,
        fetch_at=fetch_at,
        pending=pending,
        empty=empty,
    )


async def _write[T](
    db: Database, writer: FencedWriter | None, fn: Callable[[sqlite3.Connection], T]
) -> T:
    """One write transaction, through ``writer`` when there is one."""
    if writer is not None:
        return await writer.write(fn)
    return await db.write(fn)


def _reserve_fill_call(conn: sqlite3.Connection) -> Reservation | _Hold:
    """Reserve one counted Open-Meteo call for the fill, or the hold that
    stops it.

    In one transaction: the add-on's own Open-Meteo domain hold
    (``check_domain_backoff``); the fill's ceiling,
    ``fill_call_ceiling(effective_daily_call_limit(...))``, against today's
    counted calls; then ``reserve_budget``. Writes nothing when it holds.
    ``reserve_budget``'s own ``JobDeferred`` propagates, so the transaction
    rolls back.
    """
    try:
        check_domain_backoff(conn, source_domain(_OPEN_METEO))
    except JobDeferred as exc:
        return _Hold("backoff", exc.next_attempt_at, None)
    source = conn.execute(_SOURCE_LIMIT_SQL, (_OPEN_METEO,)).fetchone()
    if source is None:
        raise ValueError(f"unknown source {_OPEN_METEO}")
    billing_tz = str(source["billing_tz"])
    ceiling = fill_call_ceiling(
        effective_daily_call_limit(_OPEN_METEO, int(source["daily_call_limit"]))
    )
    used_row = conn.execute(
        _CALLS_TODAY_SQL, (_OPEN_METEO, current_billing_day(billing_tz))
    ).fetchone()
    used = 0 if used_row is None else int(used_row["calls"])
    if used >= ceiling:
        return _Hold("ceiling", next_billing_window(billing_tz), ceiling)
    return reserve_budget(conn, _OPEN_METEO, 1)


class _FillRuns:
    """The archive's runs for one unit, each asked for once, with one
    counted call reserved before each request.

    A hold raises ``_Hold``. A ``Retry`` that cannot have reached the
    provider is refunded. ``outcomes`` keeps every run asked for; ``calls``
    the counted calls.
    """

    def __init__(
        self,
        db: Database,
        writer: FencedWriter | None,
        client: httpx.AsyncClient,
        *,
        model: str,
        latitude: float,
        longitude: float,
        fetch_at: datetime,
        max_lead_hours: int,
    ) -> None:
        self._db = db
        self._writer = writer
        self._client = client
        self._model = model
        self._latitude = latitude
        self._longitude = longitude
        self._fetch_at = fetch_at
        self._max_lead_hours = max_lead_hours
        self.outcomes: dict[datetime, FetchOutcome] = {}
        self.calls = 0

    async def get(self, run: datetime) -> FetchOutcome:
        known = self.outcomes.get(run)
        if known is not None:
            return known
        try:
            reserved = await _write(self._db, self._writer, _reserve_fill_call)
        except JobDeferred as exc:
            raise _Hold("ceiling", exc.next_attempt_at, None) from None
        if isinstance(reserved, _Hold):
            raise reserved
        self.calls += reserved.calls
        outcome = await fetch_run(
            self._client,
            model=self._model,
            run_at=run,
            latitude=self._latitude,
            longitude=self._longitude,
            forecast_hours=request_hours(run, self._fetch_at, self._max_lead_hours),
        )
        if isinstance(outcome, Retry) and outcome.refundable:

            def _refund(conn: sqlite3.Connection) -> None:
                refund_budget(conn, reserved)

            await _write(self._db, self._writer, _refund)
            self.calls -= reserved.calls
        self.outcomes[run] = outcome
        return outcome


def _write_unit(
    conn: sqlite3.Connection,
    site_id: int,
    plan: _UnitPlan,
    cells: Mapping[Cell, list[NormalizedSample]],
    *,
    found_run: bool,
    newest_run: datetime | None,
) -> FillCounts:
    """Write one unit's rows and ledger rows, in the caller's transaction.

    Re-checks inside it, per cell of a day before the first record (read
    again here): a cell with a ledger row is left alone; a cell with a
    ``forecast_history`` row gets ``present``; with no run in the archive,
    ``no_run``; otherwise each observed hour of D (the stamps
    ``hourly_values`` keeps) that has a kept rebuilt value is one row, and
    the cell is ``written`` with one row or more, else ``no_hours``.
    """
    counts = FillCounts()
    first_record = first_record_date(conn, site_id)
    if first_record is None:
        return counts
    feed_id = plan.feed.feed_id
    download_date = plan.download_date
    filled_at = isoformat_utc(utc_now().replace(microsecond=0))
    span_start, span_end = _utc_span(
        download_date, download_date + timedelta(days=MAX_DAY_AHEAD)
    )
    observed = hourly_values(
        conn,
        site_id,
        frozenset(FILL_VARIABLES),
        start_utc=span_start,
        end_utc=span_end,
    )
    for variable, day_ahead, local_date in _unit_cells(download_date, first_record):
        if _cell_in_ledger(conn, site_id, feed_id, variable, day_ahead, local_date):
            continue
        day_text = local_date.isoformat()
        outcome: FillOutcome
        hours = 0
        run_at: datetime | None = None
        if _cell_has_history(conn, site_id, feed_id, variable, day_ahead, local_date):
            outcome = "present"
        elif not found_run:
            outcome = "no_run"
        else:
            samples = cells.get(Cell(variable, day_ahead, local_date), [])
            stamps = observed.get(variable, {})
            for sample in samples:
                issued = parse_utc(sample.issued_at)
                run_at = issued if run_at is None else max(run_at, issued)
            written_run: datetime | None = None
            for sample in samples:
                hour = stamps.get(parse_utc(sample.valid_at))
                if hour is None:
                    continue
                inserted = conn.execute(
                    _INSERT_FILLED_SQL,
                    (
                        site_id,
                        feed_id,
                        variable,
                        day_ahead,
                        hour.stamp,
                        day_text,
                        sample.value,
                        sample.lead_hours,
                        sample.issued_at,
                    ),
                ).rowcount
                if inserted:
                    hours += 1
                    issued = parse_utc(sample.issued_at)
                    written_run = (
                        issued if written_run is None else max(written_run, issued)
                    )
            if hours:
                outcome = "written"
                run_at = written_run
            else:
                outcome = "no_hours"
                run_at = run_at or newest_run
        conn.execute(
            _INSERT_LEDGER_SQL,
            (
                site_id,
                feed_id,
                variable,
                day_ahead,
                day_text,
                download_date.isoformat(),
                outcome,
                hours,
                None if run_at is None else isoformat_utc(run_at),
                filled_at,
            ),
        )
        counts.count(outcome)
        counts.rows += hours
    return counts


def _write_refused(
    conn: sqlite3.Connection, site_id: int, feed_id: int, first_download: date
) -> int:
    """A ``refused`` ledger row for every cell of the feed, from download
    day ``first_download`` to the day before the first record (read again
    here), that has no ledger row yet; the number written. In the caller's
    transaction."""
    first_record = first_record_date(conn, site_id)
    if first_record is None:
        return 0
    filled_at = isoformat_utc(utc_now().replace(microsecond=0))
    written = 0
    download_date = first_download
    while download_date < first_record:
        for variable, day_ahead, local_date in _unit_cells(download_date, first_record):
            written += conn.execute(
                _INSERT_REFUSED_SQL,
                (
                    site_id,
                    feed_id,
                    variable,
                    day_ahead,
                    local_date.isoformat(),
                    download_date.isoformat(),
                    filled_at,
                ),
            ).rowcount
        download_date += _ONE_DAY
    return written


async def _exclude_model(
    db: Database,
    writer: FencedWriter | None,
    site_id: int,
    plan: _UnitPlan,
    *,
    reason: str,
    calls: int,
) -> ModelExcluded:
    """Take the unit's model out of the fill: ``refused`` ledger rows for
    its remaining cells in one transaction, then one ERROR line."""
    feed = plan.feed
    first_download = plan.site.first_download

    def _refuse(conn: sqlite3.Connection) -> int:
        return _write_refused(conn, site_id, feed.feed_id, first_download)

    refused = await _write(db, writer, _refuse)
    logger.error("om archive fill stopped: model=%s reason=%s", feed.model, reason)
    return ModelExcluded(
        feed_id=feed.feed_id,
        model=feed.model,
        reason=reason,
        counts=FillCounts(refused=refused, calls=calls),
    )


async def fill_unit(
    db: Database,
    site_id: int,
    feed_id: int,
    download_date: date,
    *,
    client: httpx.AsyncClient,
    writer: FencedWriter | None = None,
    refused_units: int = 0,
) -> UnitResult:
    """Fill one unit: the feed's cells of download day ``download_date``.

    Reads the unit first (``_UnitPlan``). With every cell in the ledger it
    returns at once; with no empty cell it writes the ``present`` rows and
    asks nothing. Otherwise it rebuilds S's download at the round time on S
    as the check does (``served_run``, older runs while
    ``needs_older_run`` says so, ``keep_live``, ``day_cells``), with one
    counted call reserved before each request, the requests under one
    ``UNIT_DEADLINE_SECONDS`` bound, and then writes the unit's rows and
    ledger rows in one transaction (``_write_unit``), re-checking each cell
    there.

    - A domain hold, the daily ceiling or the add-on's own limit, a
      ``Retry`` or the deadline: ``UnitDeferred``, nothing written (the
      ceiling logs one INFO line).
    - A ``Refused`` status, or a refused unit (every request an HTTP 400)
      when ``refused_units`` refused units of this model came just before
      it and this one makes ``REFUSED_UNITS_LIMIT``: ``ModelExcluded``.
    - Otherwise ``UnitDone``; a refused unit below the limit writes its
      empty cells as ``no_run``.

    ``refused_units`` is the caller's count of this model's refused units
    in a row (``refused_streak_after``). Writes go through ``writer`` when
    given. Never calls ``record_http_backoff`` or ``clear_domain_backoff``
    and never touches feed error state.
    """

    def _read(conn: sqlite3.Connection) -> _UnitPlan | str:
        return _unit_plan(conn, site_id, feed_id, download_date)

    plan = await db.read(_read)
    if isinstance(plan, str):
        return UnitSkipped(feed_id=feed_id, download_date=download_date, reason=plan)
    feed = plan.feed
    if not plan.pending:
        return UnitDone(
            feed_id=feed_id,
            model=feed.model,
            download_date=download_date,
            counts=FillCounts(),
            found_run=False,
            refused_unit=False,
        )
    if not plan.empty:

        def _present(conn: sqlite3.Connection) -> FillCounts:
            return _write_unit(
                conn, site_id, plan, {}, found_run=False, newest_run=None
            )

        return UnitDone(
            feed_id=feed_id,
            model=feed.model,
            download_date=download_date,
            counts=await _write(db, writer, _present),
            found_run=False,
            refused_unit=False,
        )
    runs = _FillRuns(
        db,
        writer,
        client,
        model=feed.model,
        latitude=plan.site.latitude,
        longitude=plan.site.longitude,
        fetch_at=plan.fetch_at,
        max_lead_hours=feed.max_lead_hours,
    )
    try:
        async with asyncio.timeout(UNIT_DEADLINE_SECONDS):
            rebuilt = await _rebuild(
                runs,
                feed.model,
                served_run(feed.model, plan.fetch_at),
                plan.fetch_at,
                feed.max_lead_hours,
            )
    except _Hold as hold:
        if hold.reason == "ceiling":
            if hold.ceiling is None:
                logger.info("om archive fill paused: daily limit reached")
            else:
                logger.info(
                    "om archive fill paused: daily ceiling %d reached", hold.ceiling
                )
        return _deferred(plan, parse_utc(hold.until), hold.reason, runs.calls)
    except TimeoutError:
        logger.warning(
            "om archive fill unit too slow model=%s download_date=%s",
            feed.model,
            download_date.isoformat(),
        )
        return _deferred(plan, utc_now() + RETRY_WAIT, "deadline", runs.calls)
    if isinstance(rebuilt, Retry):
        return _deferred(plan, rebuilt.after, "retry", runs.calls)
    if isinstance(rebuilt, Refused):
        return await _exclude_model(
            db,
            writer,
            site_id,
            plan,
            reason=f"status {rebuilt.status}",
            calls=runs.calls,
        )
    outcomes = list(runs.outcomes.values())
    found = [outcome.run_at for outcome in outcomes if isinstance(outcome, Run)]
    refused_unit = bool(outcomes) and all(
        isinstance(outcome, NoRun) and outcome.status == 400 for outcome in outcomes
    )
    if refused_unit and refused_units + 1 >= REFUSED_UNITS_LIMIT:
        return await _exclude_model(
            db,
            writer,
            site_id,
            plan,
            reason=f"{REFUSED_UNITS_LIMIT} refused units in a row",
            calls=runs.calls,
        )
    cells = day_cells(rebuilt.kept, download_date, plan.site.zone)
    newest_run = max(found, default=None)

    def _fill(conn: sqlite3.Connection) -> FillCounts:
        return _write_unit(
            conn,
            site_id,
            plan,
            cells,
            found_run=bool(found),
            newest_run=newest_run,
        )

    counts = await _write(db, writer, _fill)
    counts.calls = runs.calls
    return UnitDone(
        feed_id=feed_id,
        model=feed.model,
        download_date=download_date,
        counts=counts,
        found_run=bool(found),
        refused_unit=refused_unit,
    )


def _deferred(
    plan: _UnitPlan, until: datetime, reason: DeferReason, calls: int
) -> UnitDeferred:
    return UnitDeferred(
        feed_id=plan.feed.feed_id,
        model=plan.feed.model,
        download_date=plan.download_date,
        until=until,
        reason=reason,
        calls=calls,
    )


def refused_streak_after(streak: int, result: UnitDone) -> int:
    """A model's count of refused units in a row after ``result``: one more
    after a refused unit, 0 after a unit that found a run, unchanged
    otherwise (a unit that asked nothing, or whose runs held no value)."""
    if result.refused_unit:
        return streak + 1
    if result.found_run:
        return 0
    return streak


def fill_summary_line(model: str, counts: FillCounts, *, done: bool = True) -> str:
    """One feed's summary line: ``om archive fill done: ...`` when no unit
    is left, ``om archive fill so far: ...`` otherwise."""
    head = "done" if done else "so far"
    return (
        f"om archive fill {head}: model={model} written={counts.written}"
        f" rows={counts.rows} present={counts.present}"
        f" no_hours={counts.no_hours} no_run={counts.no_run}"
        f" refused={counts.refused} calls={counts.calls}"
    )


async def fill_site(
    db: Database,
    site_id: int,
    *,
    client: httpx.AsyncClient | None = None,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
    pace_seconds: float = UNIT_PACE_SECONDS,
) -> SiteFill:
    """Run the fill of ``site_id`` unit by unit until none is left, as the
    ``om-archive fill`` command does.

    ``next_unit`` picks each unit and ``fill_unit`` fills it, with
    ``pace_seconds`` between units, each model's refused units counted in a
    row (``refused_streak_after``), and no quiet window. A deferral is
    waited out (``sleep``) while it ends within ``CLI_LONGEST_WAIT`` and
    fewer than ``CLI_RETRY_LIMIT`` came in a row; the daily ceiling, a
    longer deferral or one too many stops the run with ``paused`` set, and
    a skipped unit stops it too. Counts are this run's. Passing ``client``
    reuses it; otherwise one is made and closed.
    """
    if client is None:
        async with httpx.AsyncClient() as owned:
            return await fill_site(
                db, site_id, client=owned, sleep=sleep, pace_seconds=pace_seconds
            )

    def _read_site(conn: sqlite3.Connection) -> _FillSite | str:
        return _fill_site(conn, site_id)

    def _read_next(conn: sqlite3.Connection) -> FillUnit | None:
        return next_unit(conn, site_id)

    site = await db.read(_read_site)
    if isinstance(site, str):
        return SiteFill(site_id=site_id, feeds=(), problem=site)
    models = {feed.feed_id: feed.model for feed in site.feeds}
    totals = {feed.feed_id: FillCounts() for feed in site.feeds}
    streaks: dict[int, int] = {}
    deferrals = 0
    paced = False
    paused: PauseReason | None = None
    while True:
        unit = await db.read(_read_next)
        if unit is None:
            break
        if paced:
            await sleep(pace_seconds)
        result = await fill_unit(
            db,
            site_id,
            unit.feed_id,
            unit.download_date,
            client=client,
            refused_units=streaks.get(unit.feed_id, 0),
        )
        if isinstance(result, UnitSkipped):
            paused = "skipped"
            break
        totals.setdefault(result.feed_id, FillCounts())
        models.setdefault(result.feed_id, result.model)
        if isinstance(result, UnitDeferred):
            totals[result.feed_id].calls += result.calls
            deferrals += 1
            wait = (result.until - utc_now()).total_seconds()
            if (
                result.reason == "ceiling"
                or deferrals >= CLI_RETRY_LIMIT
                or wait > CLI_LONGEST_WAIT.total_seconds()
            ):
                paused = result.reason
                break
            await sleep(max(0.0, wait))
            paced = False
            continue
        deferrals = 0
        paced = True
        totals[result.feed_id].add(result.counts)
        if isinstance(result, UnitDone):
            streaks[result.feed_id] = refused_streak_after(
                streaks.get(result.feed_id, 0), result
            )
    return SiteFill(
        site_id=site_id,
        feeds=tuple(
            FeedFill(feed_id=feed_id, model=models[feed_id], counts=totals[feed_id])
            for feed_id in sorted(totals)
        ),
        left_out=tuple(feed.model for feed in site.left_out),
        paused=paused,
    )


def remove_fill(conn: sqlite3.Connection) -> int:
    """Remove every row the fill wrote, in the caller's open transaction.

    Raises ``RuntimeError`` when ``conn.in_transaction`` is false, before
    anything is run; issues no BEGIN, COMMIT or ROLLBACK. Per ``written``
    ledger row, one key seek deletes that cell's ``forecast_history`` rows
    (the close's own delete statement, with the cell's site, feed,
    variable, day ahead, text bounds and date); the fill wrote only into
    empty cells, so these are exactly its rows. Then every ledger row goes,
    whatever its outcome. Returns the number of ``forecast_history`` rows
    deleted. Reads rows by position, so any ``row_factory`` works.
    """
    if not conn.in_transaction:
        raise RuntimeError("remove_fill must run inside the caller's transaction")
    removed = 0
    for row in conn.execute(_WRITTEN_CELLS_SQL).fetchall():
        day_text = str(row[4])
        low, high = _cell_bounds(date.fromisoformat(day_text))
        removed += conn.execute(
            _DELETE_FILLED_SQL,
            (row[0], row[1], row[2], row[3], low, high, day_text),
        ).rowcount
    conn.execute(_CLEAR_LEDGER_SQL)
    return removed

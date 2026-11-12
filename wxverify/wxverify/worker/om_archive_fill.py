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
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from email.utils import parsedate_to_datetime
from typing import Final, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from wxverify.collection.budget import is_refundable_transport_error
from wxverify.collection.om_archive import (
    ALLOWANCE_MINUTES,
    FILL_VARIABLES,
    MAX_DAY_AHEAD,
    OLDER_RUNS_REACH,
    CandidateName,
    CheckCounts,
    ModelVerdict,
    RainVerdict,
    day_cells,
    download_verdict,
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
from wxverify.db.connection import Database
from wxverify.db.site_days import DONE_STATES
from wxverify.db.switch import site_generation_id
from wxverify.feeds.open_meteo import (
    RUN_CADENCE_HOURS,
    VARIABLE_MAP,
    OpenMeteoResponse,
    run_samples,
)
from wxverify.feeds.seam import NormalizedSample
from wxverify.scoring.close_scores import TruthRow, active_real_feeds, scored_variables
from wxverify.scoring.downloads import scored_values, scored_values_download
from wxverify.worker.round import scoring_round_local_time

logger = logging.getLogger(__name__)

#: Open-Meteo's archive of individual model runs.
SINGLE_RUNS_URL: Final = "https://single-runs-api.open-meteo.com/v1/forecast"
#: Per-operation timeouts of one archive request.
REQUEST_TIMEOUT: Final = httpx.Timeout(20.0, connect=5.0)
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
    arrays hold no value. Never read as zero rain."""


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
        return NoRun()
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
    cache: _RunCache,
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

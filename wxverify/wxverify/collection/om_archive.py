"""Rebuilding a past Open-Meteo download from Open-Meteo's archive of runs.

The add-on's forecast of a day at day ahead k is its first download on the
day k days before. For a day with no stored forecast, that download is
rebuilt from Open-Meteo's Single Runs archive: the run the download would
have been served (``served_run``), older runs for the hours that run does
not reach (``stitch``), only the hours the live download would have scored
(``keep_live``), mapped to local days (``day_cells``). The fill's limits
and the check's verdicts are here too.

Pure: no database, network, clock or file access. Every time is passed in;
a naive datetime is read as UTC.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import Final, Literal, NamedTuple

from wxverify.collection.forecast_validation import FORECAST_VALUE_RANGES
from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc
from wxverify.feeds.open_meteo import RUN_CADENCE_HOURS, snap_run
from wxverify.feeds.seam import NormalizedSample

#: Per model, the minutes after a run starts before Open-Meteo is taken to
#: serve it: the slow end of the measured publication delays plus 30
#: minutes. Too large a value picks an older run than the add-on got, a
#: staler forecast; it never picks a fresher one.
ALLOWANCE_MINUTES: Final[Mapping[str, int]] = {
    "ecmwf_ifs": 600,
    "ukmo_global_deterministic_10km": 600,
    "jma_gsm": 630,
    "gem_global": 420,
    "meteofrance_arpege_world": 360,
    "gfs_global": 360,
    "icon_global": 300,
    "icon_eu": 240,
}
#: The variables the fill writes, by the names the add-on stores.
FILL_VARIABLES: Final[tuple[str, ...]] = ("precip", "temperature", "wind")
#: The most counted Open-Meteo calls the fill makes in one billing day.
FILL_DAILY_CALL_CEILING: Final = 3000
#: Counted calls always left below the limit in force for the add-on's own
#: downloads.
FILL_CALL_RESERVE: Final = 300
#: The wait between two units of the fill.
UNIT_PACE_SECONDS: Final = 6
#: The site-local time the fill's quiet window starts each evening.
QUIET_START_LOCAL: Final = time(22, 30)
#: Added after the night's round and the close's first retry before the
#: fill runs again.
QUIET_END_MARGIN: Final = timedelta(minutes=30)
#: Refused units in a row that exclude a model from the fill.
REFUSED_UNITS_LIMIT: Final = 3
#: How far before the served run older runs are fetched for the hours it
#: does not reach.
OLDER_RUNS_REACH: Final = timedelta(hours=24)
#: The largest day ahead the add-on scores.
MAX_DAY_AHEAD: Final = 7

#: The check's rules. A model's verdict needs this many decided downloads.
CHECK_MIN_DECIDED: Final = 10
#: PASS: the served run matches at least this share of decided downloads.
CHECK_PASS_PERCENT: Final = 90
#: STALE: the served run and the next newer run together match at least
#: this share.
CHECK_STALE_PERCENT: Final = 50
#: A stored temperature equals a rebuilt one within this many degrees C:
#: one 0.1 degrees C step, the step Open-Meteo reports temperature in. On
#: some hours the archive rounds the same run to the step next to the live
#: reply's, so the served run can differ from its download by one step.
TEMPERATURE_MATCH_C: Final = 0.1
#: A stored rain value equals a rebuilt one within this many mm.
RAIN_MATCH_MM: Final = 0.005
#: An hour is wet at this much rain or more, in mm.
WET_HOUR_MM: Final = 0.1
#: The rain-label proof needs this many wet hours.
RAIN_MIN_WET_HOURS: Final = 20
#: Added to the largest gap seen when a TOO FRESH model's allowance is
#: raised.
TOO_FRESH_MARGIN_MINUTES: Final = 30
#: An allowance this large or larger means the archive does not rebuild the
#: model's downloads.
ALLOWANCE_LIMIT_MINUTES: Final = 24 * 60

#: Added to ``TEMPERATURE_MATCH_C`` so a difference of exactly one step
#: matches despite float error (``abs(20.1 - 20.0)`` is
#: ``0.10000000000000142``). Far smaller than a step, so 0.2 degrees C never
#: matches.
_TEMPERATURE_FLOAT_SLACK_C: Final = 1e-6
_HOUR: Final = timedelta(hours=1)
_NO_SHIFT: Final = timedelta(0)
#: How far the adapter files each variable's value before the provider's
#: own hour label: rain goes under the start of its hour
#: (``precip_hour_start``), one hour before Open-Meteo's label; every other
#: variable keeps its label.
_LABEL_SHIFT: Final[Mapping[str, timedelta]] = {"precip": _HOUR}

CandidateName = Literal["r1", "older", "newer"]
DownloadVerdict = Literal["r1", "older", "newer", "no_match", "undecided"]
ModelVerdict = Literal["PASS", "TOO FRESH", "STALE", "NO MATCH", "CAN'T TELL"]
RainVerdict = Literal["RAIN OK", "RAIN LABEL", "RAIN LABEL CAN'T TELL"]
_CANDIDATE_ORDER: Final[tuple[CandidateName, ...]] = ("r1", "older", "newer")


class Cell(NamedTuple):
    """One filled cell of a site and feed: a variable, day ahead and local
    date."""

    variable: str
    day_ahead: int
    local_date: date


class RainCounts(NamedTuple):
    """One download's rain-label counts: its wet hours, and how many of them
    the rebuilt rain matches unshifted, one hour earlier and one hour
    later."""

    wet_hours: int
    unshifted: int
    minus1: int
    plus1: int


@dataclass(slots=True)
class CheckCounts:
    """One model's counts in the check, summed over its recorded downloads.

    ``older_gap_minutes`` is the largest fetch time less the served run, in
    whole minutes rounded up, among the downloads matched by the older run.
    """

    downloads: int = 0
    decided: int = 0
    r1_match: int = 0
    older_match: int = 0
    newer_match: int = 0
    no_match: int = 0
    extra_hours: int = 0
    missing_hours: int = 0
    wet_hours: int = 0
    rain_unshifted: int = 0
    rain_minus1: int = 0
    rain_plus1: int = 0
    older_gap_minutes: int = 0


def _as_utc(value: datetime) -> datetime:
    """``value`` as an aware UTC datetime (a naive value is UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _cadence(model: str) -> timedelta:
    """The model's run cadence, from the adapter's roster. No default: a
    model missing from it raises ``ValueError``."""
    hours = RUN_CADENCE_HOURS.get(model)
    if hours is None:
        raise ValueError(f"open-meteo model has no run cadence: {model}")
    return timedelta(hours=hours)


def served_run(model: str, fetch_at: datetime) -> datetime:
    """The run a download at ``fetch_at`` is taken to have been served.

    The newest run on the model's cadence (UTC hours that are a multiple of
    it) with run + ``ALLOWANCE_MINUTES[model]`` at or before ``fetch_at``.
    A model with no allowance or no cadence raises ``ValueError``.
    """
    minutes = ALLOWANCE_MINUTES.get(model)
    if minutes is None:
        raise ValueError(f"open-meteo model has no allowance: {model}")
    cadence_hours = _cadence(model) // _HOUR
    latest = _as_utc(fetch_at) - timedelta(minutes=minutes)
    hour = (latest.hour // cadence_hours) * cadence_hours
    return latest.replace(hour=hour, minute=0, second=0, microsecond=0)


def older_run(model: str, run: datetime) -> datetime:
    """The model's run one cadence before ``run``."""
    return _as_utc(run) - _cadence(model)


def newer_run(model: str, run: datetime) -> datetime:
    """The model's run one cadence after ``run``."""
    return _as_utc(run) + _cadence(model)


def request_hours(run: datetime, fetch_at: datetime, max_lead_hours: int) -> int:
    """The ``forecast_hours`` to ask of ``run`` for a download at
    ``fetch_at``.

    The archive's hours start at the run, while the live download's start
    at the fetch hour and run ``max_lead_hours`` hours; so the request runs
    from the run to the live download's last hour. Never below 1.
    """
    first = floor_hour(_as_utc(fetch_at))
    return max(1, (first - _as_utc(run)) // _HOUR + max_lead_hours)


def _provider_label(sample: NormalizedSample) -> datetime:
    """The hour label Open-Meteo gave the sample's value."""
    return parse_utc(sample.valid_at) + _LABEL_SHIFT.get(sample.variable, _NO_SHIFT)


def _in_request_window(
    sample: NormalizedSample, fetch_at: datetime, max_lead_hours: int
) -> bool:
    """Whether the live download held the sample's hour: its provider label
    L with fetch hour <= L < fetch hour + ``max_lead_hours`` hours."""
    first = floor_hour(_as_utc(fetch_at))
    label = _provider_label(sample)
    return first <= label < first + timedelta(hours=max_lead_hours)


def stitch(
    replies: Iterable[Iterable[NormalizedSample]],
    fetch_at: datetime,
    max_lead_hours: int,
) -> list[NormalizedSample]:
    """The download's reply rebuilt from one or more runs' samples.

    Each reply is one run's converted samples (``run_samples``); a missing
    value is no sample, so a run ends where its values end and a run with
    no values adds nothing (never a zero). Per variable and hour, the value
    of the newest run (by ``issued_at``) that has one is kept, as the live
    reply puts the newest run first and an older one past its end. Only the
    hours the live download asked for are kept (``_in_request_window``).
    Ordered by hour, then variable.
    """
    chosen: dict[tuple[datetime, str], tuple[datetime, NormalizedSample]] = {}
    for reply in replies:
        for sample in reply:
            if not _in_request_window(sample, fetch_at, max_lead_hours):
                continue
            key = (parse_utc(sample.valid_at), sample.variable)
            run = parse_utc(sample.issued_at)
            held = chosen.get(key)
            if held is None or run > held[0]:
                chosen[key] = (run, sample)
    return [chosen[key][1] for key in sorted(chosen)]


def _live_origin(model: str, fetch_at: datetime) -> datetime:
    """The run stamp the live download's leads count from (``snap_run``)."""
    return parse_utc(snap_run(model, isoformat_utc(_as_utc(fetch_at))))


def keep_live(
    samples: Iterable[NormalizedSample],
    model: str,
    fetch_at: datetime,
    max_lead_hours: int,
) -> list[NormalizedSample]:
    """The samples the live download at ``fetch_at`` would have scored.

    A sample is kept when the live download held its hour
    (``_in_request_window``), its whole-hour lead counted from
    ``snap_run(model, fetch_at)`` is from 1 to ``max_lead_hours``, and its
    value lies within its ``FORECAST_VALUE_RANGES`` range, ends included.
    These are the scored readers' filters, and the lead origin is the
    live one, not the run the value came from. A variable with no range is
    dropped.
    """
    origin = _live_origin(model, fetch_at)
    kept: list[NormalizedSample] = []
    for sample in samples:
        bounds = FORECAST_VALUE_RANGES.get(sample.variable)
        if bounds is None or not bounds[0] <= sample.value <= bounds[1]:
            continue
        if not _in_request_window(sample, fetch_at, max_lead_hours):
            continue
        lead = int((parse_utc(sample.valid_at) - origin).total_seconds() // 3600)
        if 1 <= lead <= max_lead_hours:
            kept.append(sample)
    return kept


def _last_kept_label(
    variable: str, model: str, fetch_at: datetime, max_lead_hours: int
) -> datetime | None:
    """The last hour of ``variable`` that ``keep_live`` can keep, or None
    when it can keep none."""
    first = floor_hour(_as_utc(fetch_at))
    shift = _LABEL_SHIFT.get(variable, _NO_SHIFT)
    span = timedelta(hours=max_lead_hours)
    origin = _live_origin(model, fetch_at)
    last = min(first + span - _HOUR - shift, origin + span)
    earliest = max(first - shift, origin + _HOUR)
    return last if last >= earliest else None


def _reach(samples: Iterable[NormalizedSample]) -> dict[str, datetime]:
    """Each variable's last hour among ``samples``."""
    reach: dict[str, datetime] = {}
    for sample in samples:
        instant = parse_utc(sample.valid_at)
        held = reach.get(sample.variable)
        if held is None or instant > held:
            reach[sample.variable] = instant
    return reach


def _window_covered(
    reach: Mapping[str, datetime], model: str, fetch_at: datetime, max_lead_hours: int
) -> bool:
    """Whether, for each of ``FILL_VARIABLES``, ``reach`` lies at or past the
    last hour of that variable the live download would have scored."""
    for variable in FILL_VARIABLES:
        last = _last_kept_label(variable, model, fetch_at, max_lead_hours)
        if last is None:
            continue
        held = reach.get(variable)
        if held is None or held < last:
            return False
    return True


def needs_older_run(
    replies: Sequence[Sequence[NormalizedSample] | None],
    model: str,
    fetch_at: datetime,
    max_lead_hours: int,
) -> bool:
    """Whether the rebuild should fetch the run one cadence older than the
    last of ``replies``.

    ``replies`` holds one entry per run fetched, newest first: the run's
    converted samples, or None when the archive has no such run. No once
    the stitched reply reaches every hour ``keep_live`` can keep. Yes after
    the served run alone, or when the last run was missing. Otherwise yes
    only when the last run reached past the newer runs, for some variable:
    a run that adds nothing at the far end is taken to mean that every
    older run ends earlier still, so a model whose longest run stops short
    of the live lead bound does not fetch every older run. How far back to
    go (``OLDER_RUNS_REACH``) is the caller's bound.
    """
    found = [reply for reply in replies if reply is not None]
    reach = _reach(stitch(found, fetch_at, max_lead_hours))
    if _window_covered(reach, model, fetch_at, max_lead_hours):
        return False
    if len(replies) < 2 or replies[-1] is None:
        return True
    newer = [reply for reply in replies[:-1] if reply is not None]
    before = _reach(stitch(newer, fetch_at, max_lead_hours))
    return any(
        variable not in before or instant > before[variable]
        for variable, instant in reach.items()
    )


def day_cells(
    values: Iterable[NormalizedSample], download_date: date, zone: tzinfo
) -> dict[Cell, list[NormalizedSample]]:
    """The values by cell: each value's local date D in ``zone`` and day
    ahead k = D - ``download_date``, keeping 0 <= k <= ``MAX_DAY_AHEAD``.

    Each cell's values are ordered by hour. A local day holds the hours
    whose instant falls in it, so a clock-change day holds 23 or 25.
    """
    cells: dict[Cell, list[NormalizedSample]] = {}
    for sample in values:
        local_date = parse_utc(sample.valid_at).astimezone(zone).date()
        day_ahead = (local_date - download_date).days
        if 0 <= day_ahead <= MAX_DAY_AHEAD:
            cells.setdefault(Cell(sample.variable, day_ahead, local_date), []).append(
                sample
            )
    for cell_values in cells.values():
        cell_values.sort(key=lambda sample: parse_utc(sample.valid_at))
    return cells


def fill_call_ceiling(limit_in_force: int) -> int:
    """The fill's daily ceiling of counted calls: ``FILL_DAILY_CALL_CEILING``
    or ``FILL_CALL_RESERVE`` below the limit in force, whichever is lower.
    At 0 or below the fill makes no call."""
    return min(FILL_DAILY_CALL_CEILING, limit_in_force - FILL_CALL_RESERVE)


def quiet_until(local_now: datetime, quiet_end: time) -> datetime | None:
    """The end of the quiet window holding ``local_now``, or None outside it.

    The window runs from ``QUIET_START_LOCAL`` each evening until the next
    ``quiet_end``, both site-local; the start is in it and the end is not.
    ``local_now`` must carry the site's zone; the end returned carries the
    same zone. ``quiet_end`` must be before ``QUIET_START_LOCAL``. Raises
    ``ValueError`` otherwise.
    """
    if local_now.tzinfo is None:
        raise ValueError("local_now must carry the site's time zone")
    if not quiet_end < QUIET_START_LOCAL:
        raise ValueError("the quiet window must end before it starts again")
    wall = local_now.time()
    if wall >= QUIET_START_LOCAL:
        end_date = local_now.date() + timedelta(days=1)
    elif wall < quiet_end:
        end_date = local_now.date()
    else:
        return None
    return datetime.combine(end_date, quiet_end, tzinfo=local_now.tzinfo)


def _temperatures_match(
    stored: Mapping[datetime, float], rebuilt: Mapping[datetime, float] | None
) -> bool:
    """Whether every stored temperature hour has a rebuilt value within
    ``TEMPERATURE_MATCH_C``, one 0.1 degrees C step.

    The archive rounds some hours of a run to the step next to the live
    reply's, so a difference of one step matches; ``_TEMPERATURE_FLOAT_SLACK_C``
    keeps exactly one step matching despite float error. A difference of
    0.2 degrees C does not match.
    """
    if rebuilt is None:
        return False
    bound = TEMPERATURE_MATCH_C + _TEMPERATURE_FLOAT_SLACK_C
    for instant, value in stored.items():
        candidate = rebuilt.get(instant)
        if candidate is None or abs(candidate - value) > bound:
            return False
    return True


def download_verdict(
    stored: Mapping[datetime, float],
    candidates: Mapping[CandidateName, Mapping[datetime, float] | None],
) -> DownloadVerdict:
    """Which rebuilt run one recorded download matches, by temperature.

    ``stored`` is the download's stored temperature by hour; each candidate
    (the served run ``r1``, the run one cadence older and the one newer,
    each rebuilt as the fill rebuilds it) is its rebuilt temperature by
    hour, or None when the archive has no run at that time. A candidate
    matches when every stored hour equals its rebuilt hour within
    ``TEMPERATURE_MATCH_C``, one 0.1 degrees C step: on some hours the
    archive rounds the same run to the step next to the live reply's.
    Exactly one match decides the download; none decides it as
    ``no_match``. Undecided: no stored hour, no archive run
    for ``r1``, or two or more matches (past a short run's end the
    candidates can be equal).
    """
    if not stored or candidates.get("r1") is None:
        return "undecided"
    matched: list[CandidateName] = [
        name
        for name in _CANDIDATE_ORDER
        if _temperatures_match(stored, candidates.get(name))
    ]
    if not matched:
        return "no_match"
    if len(matched) == 1:
        return matched[0]
    return "undecided"


def _rain_equal(stored: float, rebuilt: float | None) -> bool:
    return rebuilt is not None and abs(stored - rebuilt) <= RAIN_MATCH_MM


def rain_label_counts(
    stored: Mapping[datetime, float], rebuilt: Mapping[datetime, float]
) -> RainCounts:
    """One download's rain-label counts against its matched rebuilt rain.

    A stored rain hour is wet when it, or the rebuilt value at the same
    hour, is ``WET_HOUR_MM`` or more. Over the wet hours, the rebuilt rain
    is compared with the stored within ``RAIN_MATCH_MM`` three ways: at the
    same hour; shifted one hour earlier (the rebuilt value of the next
    hour); and shifted one hour later (the rebuilt value of the hour
    before). Only right labels make the unshifted count the highest.
    """
    wet = unshifted = minus1 = plus1 = 0
    for instant, value in stored.items():
        same = rebuilt.get(instant)
        if value < WET_HOUR_MM and (same is None or same < WET_HOUR_MM):
            continue
        wet += 1
        unshifted += _rain_equal(value, same)
        minus1 += _rain_equal(value, rebuilt.get(instant + _HOUR))
        plus1 += _rain_equal(value, rebuilt.get(instant - _HOUR))
    return RainCounts(wet_hours=wet, unshifted=unshifted, minus1=minus1, plus1=plus1)


def allowance_needed(counts: CheckCounts) -> int:
    """The allowance, in minutes, that would stop a TOO FRESH model's fill
    picking the run newer than the one served: the largest gap among its
    older-matched downloads plus ``TOO_FRESH_MARGIN_MINUTES``."""
    return counts.older_gap_minutes + TOO_FRESH_MARGIN_MINUTES


def model_verdict(counts: CheckCounts) -> ModelVerdict:
    """One model's verdict from its counts, the rules taken in order.

    CAN'T TELL with fewer than ``CHECK_MIN_DECIDED`` decided downloads;
    TOO FRESH when any download matched the older run (NO MATCH when the
    allowance that would fix it reaches ``ALLOWANCE_LIMIT_MINUTES``); PASS
    when the served run matched at least ``CHECK_PASS_PERCENT`` of decided
    downloads; STALE when the served and the newer run together matched at
    least ``CHECK_STALE_PERCENT``; NO MATCH otherwise.
    """
    decided = counts.decided
    if decided < CHECK_MIN_DECIDED:
        return "CAN'T TELL"
    if counts.older_match > 0:
        if allowance_needed(counts) >= ALLOWANCE_LIMIT_MINUTES:
            return "NO MATCH"
        return "TOO FRESH"
    if counts.r1_match * 100 >= CHECK_PASS_PERCENT * decided:
        return "PASS"
    if (counts.r1_match + counts.newer_match) * 100 >= CHECK_STALE_PERCENT * decided:
        return "STALE"
    return "NO MATCH"


def rain_label_verdict(counts: CheckCounts) -> RainVerdict:
    """One model's rain-label verdict: RAIN LABEL CAN'T TELL with fewer than
    ``RAIN_MIN_WET_HOURS`` wet hours; RAIN OK when the unshifted count is
    strictly higher than both shifted counts; RAIN LABEL otherwise."""
    if counts.wet_hours < RAIN_MIN_WET_HOURS:
        return "RAIN LABEL CAN'T TELL"
    if (
        counts.rain_unshifted > counts.rain_minus1
        and counts.rain_unshifted > counts.rain_plus1
    ):
        return "RAIN OK"
    return "RAIN LABEL"

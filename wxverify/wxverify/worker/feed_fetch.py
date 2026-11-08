"""Reusable forward forecast fetch path."""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    refund_budget,
    reserve_budget,
    write_after_reservation,
)
from wxverify.collection.forecast_fetcher import (
    PersistOutcome,
    persist_fetch_result,
    write_scored_values,
)
from wxverify.collection.forecast_validation import (
    DISPLAY_VARIABLES,
    FORECAST_VARIABLES,
)
from wxverify.core import timeutil
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.queue import Job
from wxverify.db.site_days import day_for_instant
from wxverify.feeds.registry import build_adapter
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastAdapter,
    ForecastRequest,
    ProviderAnswerRefused,
)
from wxverify.scoring.downloads import (
    ScoredDownload,
    StoredSample,
    first_recorded_download,
    latest_stored_download,
    newest_values,
)
from wxverify.worker.cadence import parse_fetch_interval_minutes
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.run_log import log_model_runs

AdapterBuilder = Callable[[str, httpx.AsyncClient], ForecastAdapter]

T = TypeVar("T")

logger = logging.getLogger(__name__)

# The value-change log counts an hour as changed when its value differs by
# more than this (daily-close P4 plan 5.2).
VALUE_CHANGE_TOLERANCE: Final = 1e-9

# A forward download try's outcome in forecast_downloads (plan 4.2).
DownloadOutcome = Literal[
    "stored",
    "no_usable",
    "failed",
    "budget",
    "backoff",
    "ineligible",
    "unavailable",
    "refused",
]


@dataclass(frozen=True)
class DownloadSlot:
    """Which try of a feed's schedule one forward download is (plan 4.2).

    ``slot`` is ``"round"``, a daytime slot as local ``"HH:MM"``, or
    ``"unscheduled"`` (an enable-time or command-line fetch, or a job
    payload with no slot). ``round_date`` and ``round_try`` are set for a
    round try only. ``started_at`` is the aware UTC clock reading taken when
    the try started.
    """

    slot: str
    round_date: date | None
    round_try: int | None
    started_at: datetime


@dataclass(frozen=True)
class FeedFetchTarget:
    site_id: int
    feed_id: int
    lat: float
    lon: float
    source: str
    model: str
    max_lead_hours: int


@dataclass(frozen=True)
class FetchFeedSuccess:
    inserted: int
    usable: int


@dataclass(frozen=True)
class FetchFeedNoOp:
    inserted: int = 0
    usable: int = 0


@dataclass(frozen=True)
class BudgetExhausted:
    next_window: str


@dataclass(frozen=True)
class BackoffActive:
    next_attempt: str


@dataclass(frozen=True)
class Ineligible:
    reason: str


@dataclass(frozen=True)
class Unavailable:
    target: FeedFetchTarget
    error: str


@dataclass(frozen=True)
class Refused:
    """The provider answered, but the answer was refused
    (``ProviderAnswerRefused``). The feed's state and the try's ``refused``
    download row are already written; ``error`` is the refusal's message."""

    target: FeedFetchTarget
    error: str


FetchFeedOutcome = (
    FetchFeedSuccess
    | FetchFeedNoOp
    | BudgetExhausted
    | BackoffActive
    | Ineligible
    | Unavailable
    | Refused
)


async def fetch_feed_once(
    db: Database,
    site_id: int,
    feed_id: int,
    *,
    writer: FencedWriter | None = None,
    adapter_builder: AdapterBuilder | None = None,
    slot: DownloadSlot,
) -> FetchFeedOutcome:
    """Fetch one feed's forecast and persist it.

    Every write here goes through ``writer`` (the claimed job's, see
    ``worker.processor.run_claimed_job``). ``writer=None`` (the CLI entry
    point's case) runs plain ``db.write`` calls.

    ``slot`` is the try's place in the feed's schedule. A reply that is
    persisted writes its ``stored`` or ``no_usable`` download row with it,
    in the samples' transaction (daily-close P4 plan 4.2). A refused answer
    (``ProviderAnswerRefused``) writes the feed's state and its ``refused``
    row here, saves nothing of the reply and returns ``Refused``. Every
    other outcome is the caller's to record.

    A reply that carries model runs (``FetchResult.runs``, meteoblue) logs
    one line per run (``worker.run_log``) before it is saved, so each line's
    ``prev`` is the run stored before this reply.
    """

    async def _write(fn: Callable[[sqlite3.Connection], T]) -> T:
        if writer is not None:
            return await writer.write(fn)
        return await db.write(fn)

    adapter_builder = adapter_builder or build_adapter
    logger.debug("fetch_feed_once site=%s feed=%s", site_id, feed_id)
    target = await db.read(lambda conn: feed_fetch_target(conn, site_id, feed_id))
    if target is None:
        logger.debug("fetch_feed_once ineligible site=%s feed=%s", site_id, feed_id)
        return Ineligible("site/feed is not eligible for fetch")
    req = ForecastRequest(
        lat=target.lat,
        lon=target.lon,
        model=target.model,
        variables=FORECAST_VARIABLES + DISPLAY_VARIABLES,
        max_lead_hours=target.max_lead_hours,
    )
    async with httpx.AsyncClient() as client:
        try:
            adapter = adapter_builder(target.source, client)
        except Exception as exc:
            return Unavailable(target=target, error=sanitized_exception(exc))
        cost = adapter.estimate_cost(req)
        logger.debug(
            "fetch budget estimate source=%s calls=%s credits=%s",
            target.source,
            cost.calls,
            cost.credits,
        )
        reserve_outcome = await _write(
            lambda conn, tgt=target, estimate=cost: _reserve_feed_call(
                conn, tgt, estimate
            )
        )
        if not isinstance(reserve_outcome, Reservation):
            if isinstance(reserve_outcome, BudgetExhausted | BackoffActive):
                logger.warning(
                    "fetch skipped source=%s reason=%s",
                    target.source,
                    type(reserve_outcome).__name__,
                )
            else:
                logger.debug(
                    "fetch budget gate source=%s outcome=%s",
                    target.source,
                    type(reserve_outcome).__name__,
                )
            return reserve_outcome
        reservation = reserve_outcome
        logger.debug(
            "fetch reserved source=%s calls=%s credits=%s",
            reservation.source,
            reservation.calls,
            reservation.credits,
        )
        try:
            result = await adapter.fetch_forecast(req)
        except httpx.HTTPStatusError as exc:
            error = sanitized_exception(exc)
            response = exc.response
            next_attempt_at = await write_after_reservation(
                db,
                writer,
                lambda conn, err=error, resp=response: mark_feed_error_and_backoff(
                    conn, target, err, resp
                ),
            )
            logger.debug(
                "fetch http error site=%s feed=%s source=%s backoff=%s: %s",
                site_id,
                feed_id,
                target.source,
                next_attempt_at is not None,
                error,
            )
            if next_attempt_at is not None:
                return BackoffActive(next_attempt_at)
            raise
        except ProviderAnswerRefused as exc:
            # Deterministic: the same request gets the same answer, so it is
            # recorded once and not raised into the retry ladder. No refund:
            # the provider counted the call.
            error = sanitized_exception(exc)
            await write_after_reservation(
                db,
                writer,
                lambda conn, err=error: _record_refusal(conn, target, err, slot),
            )
            return Refused(target=target, error=error)
        except Exception as exc:
            error = sanitized_exception(exc)
            refund = reservation if is_refundable_transport_error(exc) else None
            await write_after_reservation(
                db,
                writer,
                lambda conn, err=error, res=refund: _mark_feed_error_and_refund(
                    conn, target, err, res
                ),
            )
            logger.debug(
                "fetch transport error site=%s feed=%s source=%s refund=%s: %s",
                site_id,
                feed_id,
                target.source,
                refund is not None,
                error,
            )
            raise
    # Before the save, so each line's prev is the run stored before this
    # reply; after the call, so it costs the call nothing.
    if result.runs:
        call_at = isoformat_utc(timeutil.utc_now().replace(microsecond=0))
        await log_model_runs(
            db,
            source=target.source,
            site_id=site_id,
            runs=result.runs,
            call_at=call_at,
        )
    persist_outcome = await write_after_reservation(
        db,
        writer,
        lambda conn, fetch_result=result: _persist_fetch_success(
            conn, target, fetch_result, slot=slot
        ),
    )
    if persist_outcome.usable_sample_count == 0:
        logger.warning(
            "fetch no usable samples site=%s feed=%s source=%s",
            site_id,
            feed_id,
            target.source,
        )
        return FetchFeedNoOp()
    logger.debug(
        "fetch persisted site=%s feed=%s inserted=%s usable=%s",
        site_id,
        feed_id,
        persist_outcome.inserted_count,
        persist_outcome.usable_sample_count,
    )
    return FetchFeedSuccess(
        inserted=persist_outcome.inserted_count,
        usable=persist_outcome.usable_sample_count,
    )


def feed_fetch_target(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> FeedFetchTarget | None:
    row = conn.execute(
        """
        SELECT s.id AS site_id, s.forecast_lat, s.forecast_lon,
               f.id AS feed_id, f.source, f.model, f.max_lead_hours,
               f.enabled AS feed_enabled, f.is_virtual,
               COALESCE(sfs.enabled, f.default_subscribed) AS subscribed
        FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE s.id = ?
          AND f.id = ?
          AND s.enabled = 1
        """,
        (site_id, feed_id),
    ).fetchone()
    if row is None:
        return None
    source = str(row["source"])
    model = str(row["model"])
    if (
        not bool(row["feed_enabled"])
        or bool(row["is_virtual"])
        or not bool(row["subscribed"])
        or (source == "meteoblue" and model != "multimodel")
    ):
        return None
    return FeedFetchTarget(
        site_id=int(row["site_id"]),
        feed_id=int(row["feed_id"]),
        lat=float(row["forecast_lat"]),
        lon=float(row["forecast_lon"]),
        source=source,
        model=model,
        max_lead_hours=int(row["max_lead_hours"]),
    )


def _payload_feed_id(payload: dict[str, object]) -> int | None:
    value = payload.get("feed_id")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


_RETRY_FLOOR_FALLBACK_SECONDS = 3600


def fetch_feed_retry_floor_seconds(conn: sqlite3.Connection, job: Job) -> int | None:
    """Minimum automatic-retry delay for a fetch_feed job, derived from the
    feed's own configured fetch cadence so a hard-failing feed cannot retry
    faster than it would ever have been polled -- important for a metered
    provider, where each retry spends part of a fixed daily call budget.
    Returns None for every other job type, and for the first automatic retry
    of a fetch_feed job (job.retry_count == 0 at claim time): a single
    transient failure should still recover on the order of seconds, so the
    floor only starts bounding retry spacing from the second retry onward.
    Note job.retry_count == 0 means no RECORDED failure yet, not no prior
    attempt: reclaim_all_stale re-pends a running job without touching
    retry_count, so a job abandoned mid-attempt (process kill, a fatal
    permission path, or a stop) can reach this function again with
    retry_count still 0 and skip the floor a second time -- bounded and
    pre-existing.
    Any lookup or parse miss, or an interval too small or too large to be a
    real cadence, fails closed to a fixed fallback ceiling regardless of
    retry_count, rather than let a missing/foreign value fall through to the
    short generic retry ladder.
    """
    if job.type != "fetch_feed":
        return None
    try:
        return _fetch_feed_retry_floor_seconds(conn, job)
    except Exception:
        # Broad on purpose: this runs INSIDE queue.fail()'s failure-recording
        # write. Anything escaping here aborts that write before retry_count
        # advances, so reclaim_all_stale re-pends the job at the same count and
        # it crash-loops with max_retries structurally unable to end it --
        # strictly worse than any wrong-but-bounded floor. CancelledError is
        # BaseException, so cancellation still propagates.
        logger.warning(
            "fetch_feed retry floor: unexpected error job=%s; using fallback",
            job.id,
            exc_info=True,
        )
        return _RETRY_FLOOR_FALLBACK_SECONDS


def _fetch_feed_retry_floor_seconds(conn: sqlite3.Connection, job: Job) -> int | None:
    feed_id = _payload_feed_id(job.payload)
    if feed_id is None:
        logger.warning(
            "fetch_feed retry floor: missing/invalid feed_id in payload job=%s",
            job.id,
        )
        return _RETRY_FLOOR_FALLBACK_SECONDS
    feed_row = conn.execute(
        "SELECT fetch_interval_minutes FROM feeds WHERE id = ?", (feed_id,)
    ).fetchone()
    if feed_row is None:
        logger.warning(
            "fetch_feed retry floor: feed row missing feed_id=%s job=%s",
            feed_id,
            job.id,
        )
        return _RETRY_FLOOR_FALLBACK_SECONDS
    interval_minutes = parse_fetch_interval_minutes(
        feed_row["fetch_interval_minutes"],
        context=f"fetch_feed retry floor feed_id={feed_id} job={job.id}",
    )
    if interval_minutes is None:
        return _RETRY_FLOOR_FALLBACK_SECONDS
    interval_seconds = interval_minutes * 60
    if job.retry_count == 0:
        return None
    return interval_seconds


def mark_feed_error(
    conn: sqlite3.Connection, target: FeedFetchTarget, error: str
) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_error, error_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_error=excluded.last_error,
            error_count=site_feed_state.error_count + 1
        """,
        (target.site_id, target.feed_id, error),
    )


def mark_feed_unavailable(
    conn: sqlite3.Connection, target: FeedFetchTarget, error: str
) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_error, error_count)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=excluded.last_run_at,
            last_error=excluded.last_error,
            error_count=site_feed_state.error_count + 1
        """,
        (target.site_id, target.feed_id, isoformat_utc(), error),
    )


def _mark_feed_error_and_refund(
    conn: sqlite3.Connection,
    target: FeedFetchTarget,
    error: str,
    reservation: Reservation | None,
) -> None:
    """Record the fetch failure and, atomically, refund a phantom reservation."""
    mark_feed_error(conn, target, error)
    if reservation is not None:
        refund_budget(conn, reservation)


def _record_refusal(
    conn: sqlite3.Connection,
    target: FeedFetchTarget,
    error: str,
    slot: DownloadSlot,
) -> None:
    """Record a refused answer in one transaction: the feed's state as
    ``mark_feed_unavailable`` stamps it (last run, ``error``, one more
    error), and the try's ``refused`` download row with ``error`` as its
    reason."""
    mark_feed_unavailable(conn, target, error)
    record_download_try(
        conn,
        site_id=target.site_id,
        feed_id=target.feed_id,
        slot=slot,
        outcome="refused",
        ended_at=timeutil.utc_now(),
        reason=error,
    )


def mark_feed_error_and_backoff(
    conn: sqlite3.Connection,
    target: FeedFetchTarget,
    error: str,
    response: httpx.Response,
) -> str | None:
    mark_feed_error(conn, target, error)
    return record_http_backoff(conn, response)


def _reserve_feed_call(
    conn: sqlite3.Connection, target: FeedFetchTarget, cost: CostEstimate
) -> BudgetExhausted | BackoffActive | Ineligible | Reservation:
    if feed_fetch_target(conn, target.site_id, target.feed_id) is None:
        return Ineligible("site/feed became ineligible before fetch")
    try:
        check_domain_backoff(conn, source_domain(target.source))
    except JobDeferred as exc:
        return BackoffActive(exc.next_attempt_at)
    try:
        return reserve_budget(conn, target.source, cost.calls, cost.credits)
    except JobDeferred as exc:
        return BudgetExhausted(exc.next_attempt_at)


def _persist_fetch_success(
    conn: sqlite3.Connection,
    target: FeedFetchTarget,
    result: FetchResult,
    *,
    slot: DownloadSlot | None = None,
) -> PersistOutcome:
    """Persist a reply, its download row, its scored values and its value
    changes in one transaction.

    The ``stored`` or ``no_usable`` row of ``forecast_downloads`` is written
    with the samples, so a stored download always has its row, and a
    ``stored`` download logs its value changes (daily-close P4 plan 4.2,
    5.2). For each feed the reply carried that has no earlier stored
    download on the try's local date S (``first_recorded_download``, asked
    before this try's row exists), the download is S's first, and its
    valid samples become that feed's scored values (rain release, Part B,
    B2). A failure anywhere rolls all of it back with the transaction.
    ``slot=None`` records the try as ``unscheduled``, started at its end.
    """
    clear_domain_backoff(conn, source_domain(target.source))
    # One clock reading ends the try: the samples' fetched_at keeps its
    # microseconds, the row's ended_at is whole-second (plan 15.2). Read
    # through the module attribute, the clock isoformat_utc() reads, as
    # db/queue.py's claim does.
    ended_at = timeutil.utc_now()
    fetched_at = isoformat_utc(ended_at)
    # The try's local date S, as record_download_try dates its row.
    aware_end = (
        ended_at if ended_at.tzinfo is not None else ended_at.replace(tzinfo=UTC)
    )
    download_date = _download_local_date(
        conn, target.site_id, aware_end.astimezone(UTC).replace(microsecond=0)
    )
    # Read before the samples are written, so each hour is compared with
    # the value this reply replaces (B3).
    comparisons = _value_change_comparisons(conn, target, result)
    outcome = persist_fetch_result(
        conn,
        site_id=target.site_id,
        source=target.source,
        fetch_feed_id=target.feed_id,
        result=result,
        fetched_at=fetched_at,
    )
    stored = outcome.usable_sample_count > 0
    # B2: decided before this try's download row is written, which would
    # otherwise be S's first stored download itself.
    first_feeds = (
        []
        if download_date is None or not stored
        else [
            feed_id
            for feed_id, feed_labels in sorted(outcome.labels.items())
            if feed_labels
            and first_recorded_download(
                conn,
                site_id=target.site_id,
                feed_id=feed_id,
                local_date=download_date,
            )
            is None
        ]
    )
    download_id = record_download_try(
        conn,
        site_id=target.site_id,
        feed_id=target.feed_id,
        slot=(
            slot
            if slot is not None
            else DownloadSlot(
                slot="unscheduled", round_date=None, round_try=None, started_at=ended_at
            )
        ),
        outcome="stored" if stored else "no_usable",
        ended_at=ended_at,
        fetched_at=fetched_at,
        usable=outcome.usable_sample_count,
        inserted=outcome.inserted_count,
        labels=outcome.labels,
    )
    if download_id is not None and download_date is not None:
        for feed_id in first_feeds:
            scored = write_scored_values(
                conn,
                site_id=target.site_id,
                feed_id=feed_id,
                samples=outcome.samples.get(feed_id, ()),
                download_id=download_id,
                download_date=download_date,
                fetched_at=fetched_at,
            )
            logger.debug(
                "fetch scored values site=%s feed=%s date=%s download=%s rows=%d",
                target.site_id,
                feed_id,
                download_date.isoformat(),
                download_id,
                scored,
            )
    if stored and download_id is not None:
        _record_value_changes(
            conn, comparisons, download_id=download_id, labels=outcome.labels
        )
    return outcome


def record_download_try(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    slot: DownloadSlot,
    outcome: DownloadOutcome,
    ended_at: datetime,
    fetched_at: str | None = None,
    reason: str | None = None,
    usable: int = 0,
    inserted: int = 0,
    labels: Mapping[int, Sequence[str]] | None = None,
) -> int | None:
    """Write one ``forecast_downloads`` row for a forward download try (plan 4.2).

    ``started_at`` (``slot.started_at``) and ``ended_at`` are stored
    whole-second, ``isoformat_utc(t.replace(microsecond=0))``, the only form
    import accepts for them (plan 15.2, 15.6); ``fetched_at`` is stored as
    given. ``local_date`` is the local date of the end: the daily-close
    record holding it, else in ``sites.timezone``.
    Returns the row id, or ``None`` with nothing written when the site or
    the feed no longer exists.
    """
    # A naive reading is UTC, as isoformat_utc reads one.
    aware_end = (
        ended_at if ended_at.tzinfo is not None else ended_at.replace(tzinfo=UTC)
    )
    ended = aware_end.astimezone(UTC).replace(microsecond=0)
    local_date = _download_local_date(conn, site_id, ended)
    if local_date is None:
        return None
    labels_json = json.dumps(
        {str(fid): list(runs) for fid, runs in sorted((labels or {}).items())},
        separators=(",", ":"),
    )
    cur = conn.execute(
        """
        INSERT INTO forecast_downloads
            (site_id, feed_id, local_date, slot, round_date, round_try,
             started_at, ended_at, fetched_at, outcome, reason, usable,
             inserted, labels)
        SELECT ?, id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        FROM feeds
        WHERE id = ?
        """,
        (
            site_id,
            local_date.isoformat(),
            slot.slot,
            None if slot.round_date is None else slot.round_date.isoformat(),
            slot.round_try,
            isoformat_utc(slot.started_at.replace(microsecond=0)),
            isoformat_utc(ended),
            fetched_at,
            outcome,
            reason,
            usable,
            inserted,
            labels_json,
            feed_id,
        ),
    )
    if cur.rowcount != 1:
        return None
    return cur.lastrowid


def _download_local_date(
    conn: sqlite3.Connection, site_id: int, ended: datetime
) -> date | None:
    """The local date of a try's end (plan 4.2); ``None`` for a site that
    does not exist."""
    record = day_for_instant(conn, site_id, isoformat_utc(ended))
    if record is not None:
        return record.local_date
    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None:
        return None
    try:
        zone = ZoneInfo(str(row["timezone"]))
    except (ZoneInfoNotFoundError, ValueError):
        # Dating the try in UTC keeps the download's samples, which share
        # this transaction; the site's other readers report the zone.
        logger.warning(
            "download record: site=%s timezone does not load; dated in UTC", site_id
        )
        return ended.date()
    return ended.astimezone(zone).date()


@dataclass(frozen=True)
class _ValueComparison:
    """One carried feed's side of the value-change log (plan 5.2): the
    reply's usable values, the previous stored download, and the stored
    newest values over the reply's hours, which the reply replaces (rain
    release, Part B, B3)."""

    cells: dict[str, dict[str, float]]
    previous: ScoredDownload
    previous_id: int
    version: dict[tuple[str, str], StoredSample]


def _value_change_comparisons(
    conn: sqlite3.Connection, target: FeedFetchTarget, result: FetchResult
) -> dict[int, _ValueComparison]:
    """For each feed the reply carried that has a previous recorded stored
    download, the reply's values and the feed's stored newest values over
    the reply's valid hours (plan 5.2; rain release, Part B, B3).

    Called before the reply's samples are written, so the latest stored
    download is the previous one and each stored value is the one this
    reply replaces (``newest_values``), not the previous download's scored
    version. A model not registered as a feed yet has no previous download.
    """
    comparisons: dict[int, _ValueComparison] = {}
    cells = _reply_cells(result, _reply_feed_ids(conn, target, result))
    for feed_id, variables in cells.items():
        previous = latest_stored_download(conn, site_id=target.site_id, feed_id=feed_id)
        if previous is None or previous.download_id is None:
            continue
        instants: list[datetime] = []
        for variable_cells in variables.values():
            for valid_at in variable_cells:
                try:
                    instants.append(parse_utc(valid_at))
                except ValueError:
                    continue
        version = (
            newest_values(
                conn,
                site_id=target.site_id,
                feed_id=feed_id,
                start_utc=min(instants),
                end_utc=max(instants) + timedelta(seconds=1),
            )
            if instants
            else {}
        )
        comparisons[feed_id] = _ValueComparison(
            cells=variables,
            previous=previous,
            previous_id=previous.download_id,
            version=version,
        )
    return comparisons


def _record_value_changes(
    conn: sqlite3.Connection,
    comparisons: Mapping[int, _ValueComparison],
    *,
    download_id: int,
    labels: Mapping[int, Sequence[str]],
) -> None:
    """Write the value-change rows of a ``stored`` download (plan 5.2).

    One row per compared feed and variable: the hours present in both the
    reply and the stored values it replaces, those whose value differs by more than
    ``VALUE_CHANGE_TOLERANCE``, the largest absolute difference, the first
    changed hour, and whether the two downloads' labels for the feed differ.
    """
    for feed_id, comparison in sorted(comparisons.items()):
        version = comparison.version
        labels_changed = int(
            list(labels.get(feed_id, ()))
            != comparison.previous.labels.get(str(feed_id), [])
        )
        for variable, variable_cells in sorted(comparison.cells.items()):
            compared = 0
            changed = 0
            max_abs_change: float | None = None
            first_changed_valid_at: str | None = None
            for valid_at, value in sorted(variable_cells.items()):
                stored = version.get((variable, valid_at))
                if stored is None:
                    continue
                compared += 1
                change = abs(value - stored.value)
                max_abs_change = (
                    change if max_abs_change is None else max(max_abs_change, change)
                )
                if change > VALUE_CHANGE_TOLERANCE:
                    changed += 1
                    if first_changed_valid_at is None:
                        first_changed_valid_at = valid_at
            conn.execute(
                """
                INSERT INTO forecast_value_changes
                    (download_id, previous_download_id, feed_id, variable,
                     hours_compared, hours_changed, max_abs_change,
                     first_changed_valid_at, labels_changed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    download_id,
                    comparison.previous_id,
                    feed_id,
                    variable,
                    compared,
                    changed,
                    max_abs_change,
                    first_changed_valid_at,
                    labels_changed,
                ),
            )


def _reply_feed_ids(
    conn: sqlite3.Connection, target: FeedFetchTarget, result: FetchResult
) -> dict[str, int]:
    """Each model the reply carried mapped to its feed id, as
    ``persist_fetch_result`` registered it: the fetch feed for its own model,
    else the source's feed row of that model (a meteoblue member)."""
    feed_ids: dict[str, int] = {}
    for model in {sample.model for sample in result.samples}:
        if model == target.model:
            feed_ids[model] = target.feed_id
            continue
        row = conn.execute(
            "SELECT id FROM feeds WHERE source = ? AND model = ?",
            (target.source, model),
        ).fetchone()
        if row is not None:
            feed_ids[model] = int(row["id"])
    return feed_ids


def _reply_cells(
    result: FetchResult, feed_ids: Mapping[str, int]
) -> dict[int, dict[str, dict[str, float]]]:
    """The reply's usable values as ``{feed_id: {variable: {valid_at: value}}}``.

    A usable sample is of the scored roster and has a lead of at least 1
    hour, as ``persist_fetch_result`` counts it; a display sample (card 7)
    and a value that is not finite are left out. Where two labels give the
    same hour, the newer label's value, as ``persist_fetch_result`` writes
    it.
    """
    newest: dict[tuple[int, str, str], tuple[str, float]] = {}
    for sample in result.samples:
        feed_id = feed_ids.get(sample.model)
        if (
            feed_id is None
            or sample.variable not in FORECAST_VARIABLES
            or sample.lead_hours < 1
            or not math.isfinite(sample.value)
        ):
            continue
        key = (feed_id, sample.variable, sample.valid_at)
        prior = newest.get(key)
        if prior is None or sample.issued_at >= prior[0]:
            newest[key] = (sample.issued_at, sample.value)
    cells: dict[int, dict[str, dict[str, float]]] = {}
    for (feed_id, variable, valid_at), (_issued_at, value) in newest.items():
        cells.setdefault(feed_id, {}).setdefault(variable, {})[valid_at] = value
    return cells

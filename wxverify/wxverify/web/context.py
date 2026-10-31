"""Read-side context builders for the server-rendered UI."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify import close_monitor
from wxverify.collection.budget import (
    current_billing_day,
    effective_daily_call_limit,
)
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.collection.wind_quota import (
    WIND_BACKFILL_CALLS_KEY,
    WIND_LIVE_CALLS_KEY,
    backfill_headroom,
    read_lane_counter,
)
from wxverify.core.lead import parse_day_ahead
from wxverify.core.secrets import key_status
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.core.units import ms_to_kmh
from wxverify.db.connection import pinned_read_snapshot
from wxverify.db.queue import ACTIVE_JOB_SQL
from wxverify.db.site_days import (
    DayRecord,
    closed_through,
    current_day,
    daily_close_on,
    day_record,
    zone_flagged,
)
from wxverify.db.tz_generations import (
    correction_job_key,
    correction_state_key,
    generation_status,
    published_generation_clause,
)
from wxverify.db.wind_basis import read_auth_holds
from wxverify.forecast.wind_blend import load_wind_serving
from wxverify.scoring.composite import composite_in_snapshot
from wxverify.scoring.effective import MAX_DAY_AHEAD, active_feed_cte
from wxverify.scoring.leaderboard import leaderboard_in_snapshot
from wxverify.scoring.publication import (
    Coverage,
    Publication,
    active_publication,
    pending_ranking_settings,
    published_coverage,
)
from wxverify.scoring.winrate import winrate as winrate_query
from wxverify.settings.keys import get_number_setting
from wxverify.worker.access_hold import (
    ACCESS_KIND_LABELS,
    ACCESS_KINDS,
    read_access_record,
)
from wxverify.worker.close_chain import (
    CloseStatus,
    CollectionTry,
    PublicationRecord,
    close_status,
    collection_tries,
    stage_log,
)


@dataclass(frozen=True)
class FeedToggle:
    id: int
    source: str
    model: str
    label: str
    description: str
    enabled: bool
    default_subscribed: bool
    override_enabled: bool | None
    effective_enabled: bool
    disabled_reason: str | None


@dataclass(frozen=True)
class StationView:
    id: int
    pws_station_id: str
    lat: float
    lon: float
    dem_elevation_m: float
    enabled: bool
    observation_age: str | None


@dataclass(frozen=True)
class SiteView:
    id: int
    name: str
    forecast_lat: float
    forecast_lon: float
    elevation_m: float
    timezone: str
    enabled: bool
    rain_threshold_mm: float
    stations: list[StationView]
    feeds: list[FeedToggle]

    @property
    def enabled_station_count(self) -> int:
        return sum(1 for station in self.stations if station.enabled)


@dataclass(frozen=True)
class LeaderboardItem:
    feed_id: int
    label: str
    n: int
    skill_score: float | None
    badge: int | None
    below_baseline: bool
    confident: bool
    bias: float | None
    mae: float | None
    rmse: float | None


@dataclass(frozen=True)
class Verdict:
    state: str  # "ok" | "tie" | "insufficient" | "empty"
    winner: LeaderboardItem | None
    runner_up: LeaderboardItem | None
    margin: float | None


@dataclass(frozen=True)
class CoverageCell:
    """One feed's coverage at one day ahead, as the dashboard shows it (9.4).

    ``text`` is "<scored> of <expected>", or "none yet" when nothing is
    expected, with "; <n> earlier days" when the window holds days from
    before the daily close.
    """

    day_ahead: int
    scored: int
    expected: int
    text: str


@dataclass(frozen=True)
class CoverageRow:
    """One feed's row of the coverage table: a cell per day ahead (9.4)."""

    feed_id: int
    label: str
    cells: tuple[CoverageCell, ...]


@dataclass(frozen=True)
class CoverageTable:
    """The coverage next to the rankings in view (9.4).

    ``rows`` follow the Leaderboard's order; ``days`` are the columns, D+0
    to D+7. ``selected`` holds each Leaderboard feed's cell text at the
    selected day ahead, for the Leaderboard's "Scored" column.
    """

    days: tuple[int, ...]
    rows: tuple[CoverageRow, ...]
    selected: dict[int, str]


VARIABLE_LABELS: dict[str, str] = {
    "temperature": "Temperature",
    "precip": "Precipitation",
    "wind": "Wind",
}

LEAD_OPTIONS: list[dict[str, str]] = [
    {"value": "D+0", "word": "Today"},
    {"value": "D+1", "word": "Tomorrow"},
    *[{"value": f"D+{d}", "word": f"+{d} days"} for d in range(2, 8)],
]


def variable_label_for(variable: str) -> str:
    """Return the display label for a variable, humanizing unknown values.

    Total by construction: the dashboard route accepts ``variable`` as an
    unrestricted string, so an unknown value must resolve to a humanized
    fallback (``"foo"`` -> ``"Foo"``) rather than raising a ``KeyError`` and
    regressing today's graceful 200 to a 500.
    """
    return VARIABLE_LABELS.get(variable, variable.replace("_", " ").title())


def _skill_or_zero(value: float | None) -> float:
    return value if value is not None else 0.0


def compute_verdict(
    items: list[LeaderboardItem], *, tie_epsilon: float = 0.01
) -> Verdict:
    """Derive the "best feed" verdict from an already-built leaderboard.

    Candidates are the eligible rows — the single predicate shared with the
    curve and leaderboard sort: ``confident`` (``n >= min_n`` and
    ``skill_score`` non-None) — sorted skill-descending. States: ``empty``
    (no rows), ``insufficient`` (rows but none eligible), ``tie`` (top two
    within ``tie_epsilon``), ``ok`` otherwise. A single eligible candidate is
    ``ok`` with ``runner_up=None``.
    """
    if not items:
        return Verdict(state="empty", winner=None, runner_up=None, margin=None)
    candidates = sorted(
        (item for item in items if item.confident),
        key=lambda item: (-_skill_or_zero(item.skill_score), item.label),
    )
    if not candidates:
        return Verdict(state="insufficient", winner=None, runner_up=None, margin=None)
    winner = candidates[0]
    if len(candidates) == 1:
        return Verdict(state="ok", winner=winner, runner_up=None, margin=None)
    runner_up = candidates[1]
    winner_skill = winner.skill_score
    runner_skill = runner_up.skill_score
    # confident guarantees non-None skill; the guard keeps pyright honest and
    # degrades safely to a single-winner ok rather than raising.
    if winner_skill is None or runner_skill is None:
        return Verdict(state="ok", winner=winner, runner_up=None, margin=None)
    margin = winner_skill - runner_skill
    state = "tie" if margin <= tie_epsilon else "ok"
    return Verdict(state=state, winner=winner, runner_up=runner_up, margin=margin)


@dataclass(frozen=True)
class FeedHealthRow:
    site_id: int
    site_name: str
    feed_id: int
    label: str
    subscribed: bool
    status: str
    disabled_reason: str | None
    last_run_at: str | None
    last_error: str | None
    error_count: int
    feed_enabled: bool
    site_enabled: bool
    has_samples: bool


@dataclass(frozen=True)
class BudgetGauge:
    source: str
    #: The effective cap every reservation defers at (plan §8.6).
    daily_call_limit: int
    #: The stored ``sources.daily_call_limit``; differs where the cap clamps it.
    configured_call_limit: int
    daily_credit_limit: int | None
    calls: int
    credits: int


@dataclass(frozen=True)
class BackfillRow:
    site_id: int
    site_name: str
    status: str | None
    through: str | None


@dataclass(frozen=True)
class KeyStatusRow:
    provider: str
    present: bool


@dataclass(frozen=True)
class AccessHoldView:
    """One download kind weather.com refused, as the Key Status panel shows it."""

    #: The ``AccessKind``, e.g. "hourly".
    kind: str
    #: The whole line, e.g. "weather.com access refused (HTTP 401): hourly
    #: readings, since <t>, held until <t>".
    text: str


@dataclass(frozen=True)
class ObservationHealthRow:
    site_id: int
    site_name: str
    status: str
    last_obs_at: str | None
    enabled_station_count: int
    #: With the daily close on, the site's ``closed_through`` date (plan
    #: §10.1); None with the close off, and while no day is done.
    closed_through: str | None = None


@dataclass(frozen=True)
class CloseDayRow:
    """One day on the close status panel (plan §10.2)."""

    local_date: str
    #: P1's state in words, e.g. "closing, attempt 1 of 2".
    state: str
    #: P1's ``attempts``, 0 to 2.
    attempts: int
    #: P1's ``retry_at``, in UTC.
    retry_at: str | None
    #: P1's ``last_failure``.
    failure: str | None
    #: The blocking stage (R3.4) through ``stage_text``, for a day not done.
    stage: str | None
    #: Since when the stage runs or waits, or a failed stage's start, queue
    #: wait and end (R3.5).
    stage_line: str | None
    #: A closed day's publication record (R3.7) in words, with its time.
    publication: str | None
    #: "zone changeover, not scored", "zone changes on <date>: that day is
    #: not scored" or "scored under its own zone, <zone>".
    marks: tuple[str, ...]


@dataclass(frozen=True)
class ClosePanelRow:
    """One site on the close status panel (plan §10.2)."""

    site_id: int
    site_name: str
    enabled: bool
    #: Newest first.
    days: tuple[CloseDayRow, ...]
    #: The site's newest collection try (R3.6) in words.
    latest_try: str | None
    #: "no day closed yet", "site disabled", or the zone note.
    note: str | None


@dataclass(frozen=True)
class ClosePanel:
    """The close status panel, shown on Ops while the close is on (§10.2)."""

    #: One per site, in ``load_backfill``'s order.
    rows: tuple[ClosePanelRow, ...]
    #: "<id>: <detail>" per tripped close alert, alert 7's "recovered" line,
    #: and alert 6's next try and full list of days affected.
    alert_lines: tuple[str, ...]
    #: "Close status unavailable: <Type>: <msg>" when the read raised
    #: something other than ``sqlite3.Error``; the panel then shows only it.
    unavailable: str | None = None


@dataclass(frozen=True)
class StationTrustRow:
    site_name: str
    station: str
    variable: str
    n: int
    mean_delta: float


@dataclass(frozen=True)
class TimezoneCorrectionRow:
    """One site's row in the Ops retrospective-timezone-correction panel.

    ``applicable`` and ``blocked_reason`` are resolved here rather than in the
    template so they can be pinned against the route's own refusals. There is
    deliberately no ``reconciled`` field: ``examined == changed + unchanged +
    excluded`` holds in every state the runtime can produce (it is true at
    0/0/0/0 and the flip refuses to publish without it), so a badge derived
    from it could never discriminate. The counts are carried raw.
    """

    site_id: int
    site_name: str
    current_timezone: str
    published_generation_id: int | None
    building_generation_id: int | None
    building_timezone: str | None
    failed_generation_id: int | None
    failed_timezone: str | None
    cleanup_stalled_generation_id: int | None
    examined: int | None
    changed: int | None
    unchanged: int | None
    excluded: int | None
    last_published_at: str | None
    applicable: bool
    blocked_reason: str | None


#: The ops-page and banner title of a weather.com auth hold (plan §8.8).
WIND_HOLD_TITLES: Final[dict[str, str]] = {
    "history_all": (
        "This weather.com key cannot download wind history for past days,"
        " a known limit of the key"
    ),
}

WIND_DAY_STATUSES: Final = ("pending", "partial", "fetched", "unavailable", "failed")


@dataclass(frozen=True)
class WindHoldView:
    """One weather.com endpoint the wind lane is paused on (plan §8.8)."""

    endpoint: str
    #: "held" or "probing" (a check is queued for the next wind fetch).
    status: str
    title: str
    since: str | None
    #: The hold's last sanitized error, shown beside "Try again".
    error: str | None


@dataclass(frozen=True)
class WindErrorRow:
    station: str
    local_date: str
    status: str
    error: str


@dataclass(frozen=True)
class WindSiteHistory:
    """One enabled site's row in the ops Wind History panel (plan §10)."""

    site_id: int
    site_name: str
    #: Station-day counts over enabled stations, one per status.
    status_counts: list[tuple[str, int]]
    recent_errors: list[WindErrorRow]


@dataclass(frozen=True)
class WindHistoryPanel:
    """The ops Wind History panel: the shared budget and holds, then per site.

    Headroom, the lane counters and the auth holds are global (every site
    shares one weather.com key and budget), so they are shown once.
    """

    headroom: int | None
    backfill_calls_today: int | None
    live_calls_today: int | None
    holds: list[WindHoldView]
    sites: list[WindSiteHistory]


@dataclass(frozen=True)
class WindBanner:
    """The dashboard's wind banner: any weather.com auth holds."""

    holds: list[str]


@dataclass(frozen=True)
class WindWeightRow:
    """One feed's row in the dashboard wind weights panel (plan §12.2)."""

    feed_id: int
    label: str
    mae_kmh: float
    days: int
    weight_pct: float


def load_sites(
    conn: sqlite3.Connection, *, include_disabled: bool = True
) -> list[SiteView]:
    where = "" if include_disabled else "WHERE enabled=1"
    rows = conn.execute(
        f"""
        SELECT *
        FROM sites
        {where}
        ORDER BY enabled DESC, name COLLATE NOCASE
        """
    ).fetchall()
    return [_site_from_row(conn, row) for row in rows]


def load_site(conn: sqlite3.Connection, site_id: int) -> SiteView | None:
    row = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
    return None if row is None else _site_from_row(conn, row)


def load_dashboard(
    conn: sqlite3.Connection,
    *,
    site_id: int | None,
    variable: str,
    window: str,
    lead: str,
) -> dict[str, object]:
    """The dashboard page's context, read in one snapshot (9.3).

    Opens a ``pinned_read_snapshot`` unless the caller already holds a
    transaction, so every part of the page -- with the daily close on, every
    ranking and the wind weights -- comes from one publication version.
    With the close on, ``rolling_days`` and ``min_n`` are the active
    publication's, so the "Last N days" tab names the published window;
    ``evidence_through`` is its cutoff and ``pending_settings`` the ranking
    settings saved since (13.1). With the close off they are today's
    settings, None and ``{}``.
    """
    if conn.in_transaction:
        return _load_dashboard(
            conn, site_id=site_id, variable=variable, window=window, lead=lead
        )
    with pinned_read_snapshot(conn, label="dashboard"):
        return _load_dashboard(
            conn, site_id=site_id, variable=variable, window=window, lead=lead
        )


def _load_dashboard(
    conn: sqlite3.Connection,
    *,
    site_id: int | None,
    variable: str,
    window: str,
    lead: str,
) -> dict[str, object]:
    sites = load_sites(conn, include_disabled=False)
    site = (
        load_site(conn, site_id)
        if site_id is not None
        else (sites[0] if sites else None)
    )
    # One switch per read (9.1). The publication is the one the page's
    # rankings read, in the same snapshot; none for a disabled site, as the
    # leaderboard's own close-on read.
    publication = (
        active_publication(conn, site_id=site.id, at_utc=None)
        if site is not None and site.enabled and daily_close_on(conn)
        else None
    )
    if publication is not None:
        rolling_days = publication.window_days
        min_n = publication.min_n
    else:
        rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
        min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    evidence_through = None if publication is None else publication.cutoff
    # 9.5: a cutoff before the site's first record is a starting-history day,
    # which the label must not call closed.
    evidence_before_close = (
        publication is not None and publication.cutoff < publication.first_record_date
    )
    pending_settings = pending_ranking_settings(conn, pub=publication)
    if site is None:
        return {
            "sites": sites,
            "site": None,
            "variable": variable,
            "selected_variable_label": variable_label_for(variable),
            "window": window,
            "lead": lead,
            "lead_options": LEAD_OPTIONS,
            "rolling_days": rolling_days,
            "min_n": min_n,
            "evidence_through": evidence_through,
            "evidence_before_close": evidence_before_close,
            "pending_settings": pending_settings,
            "coverage": None,
            "leaderboard": [],
            "verdict": compute_verdict([]),
            "winrate": [],
            "composite": [],
            "composite_status": "empty",
            "wind_banner": None,
            "wind_weights": None,
        }
    day_ahead = _lead_to_day(lead)
    leaderboard_result = leaderboard_in_snapshot(
        conn,
        site_id=site.id,
        variable=variable,
        day_ahead=day_ahead,
        window=window,
    )
    leaderboard = [
        LeaderboardItem(
            feed_id=row.feed_id,
            label=feed_label(row.source, row.model),
            n=row.n,
            skill_score=row.skill_score,
            badge=row.badge,
            below_baseline=row.below_baseline,
            confident=row.confident,
            bias=row.bias,
            mae=row.mae,
            rmse=row.rmse,
        )
        for row in leaderboard_result.rows
    ]
    # Presentation-layer sort: eligible rows first (skill DESC, then label),
    # withheld rows after (label order). Never interleave by raw skill across
    # the eligibility boundary — a withheld row with a high numeric skill still
    # sorts below every eligible row. The scoring-query ORDER BY is untouched
    # (it is load-bearing on the cache-equivalence path).
    leaderboard.sort(
        key=lambda item: (
            0 if item.confident else 1,
            -_skill_or_zero(item.skill_score) if item.confident else 0.0,
            item.label,
        )
    )
    verdict = compute_verdict(leaderboard)
    # Pure read: the status is surfaced for the route to act on AFTER the read
    # connection closes (dashboard_page enqueues the rescore); never write here.
    composite_result = composite_in_snapshot(conn, site_id=site.id, window=window)
    return {
        "sites": sites,
        "site": site,
        "variable": variable,
        "selected_variable_label": variable_label_for(variable),
        "window": window,
        "lead": lead,
        "lead_options": LEAD_OPTIONS,
        "rolling_days": rolling_days,
        "min_n": min_n,
        "evidence_through": evidence_through,
        "evidence_before_close": evidence_before_close,
        "pending_settings": pending_settings,
        "coverage": _coverage_table(
            conn,
            publication=publication,
            variable=variable,
            window_key=leaderboard_result.window_key,
            leaderboard=leaderboard,
            day_ahead=day_ahead,
        ),
        "leaderboard": leaderboard,
        "verdict": verdict,
        "winrate": winrate_query(
            conn,
            site_id=site.id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        ),
        "composite": composite_result.rows,
        "composite_status": composite_result.status,
        "wind_banner": load_wind_banner(conn, site.id),
        "wind_weights": (
            load_wind_weights(conn, site, day_ahead) if variable == "wind" else None
        ),
    }


def _coverage_text(coverage: Coverage | None) -> str:
    """A coverage cell as the dashboard writes it (9.4).

    "<scored> of <expected>", or "none yet" when nothing is expected (a
    cell P4 stored no row for expects nothing); then "; <n> earlier days"
    when the window holds days from before the daily close.
    """
    if coverage is None or coverage.expected == 0:
        text = "none yet"
    else:
        text = f"{coverage.scored} of {coverage.expected}"
    if coverage is not None and coverage.starting_days > 0:
        days = "day" if coverage.starting_days == 1 else "days"
        text += f"; {coverage.starting_days} earlier {days}"
    return text


def _coverage_table(
    conn: sqlite3.Connection,
    *,
    publication: Publication | None,
    variable: str,
    window_key: str,
    leaderboard: list[LeaderboardItem],
    day_ahead: int,
) -> CoverageTable | None:
    """The coverage table for the rankings in view, or None (9.4).

    P4's stored numbers for the active publication the rankings read, under
    the Leaderboard's own window key, so the table and the rankings name one
    window: ``w:{N}`` for the "Last N days" tab, N the publication's
    ``window_days``, and ``w:all`` for "All time". None with the close off
    or no active publication (``publication`` None), and when the
    Leaderboard has no row: the table has a row per Leaderboard feed, in the
    Leaderboard's order.
    """
    if publication is None or not leaderboard:
        return None
    if not window_key.startswith("w:"):
        # A typed `window=<N>d` view (`live:{N}d`), for which P4 stores no
        # coverage: no table and no line in its place. The owner left this
        # view without coverage, and without a line in its place
        # (2026-10-31).
        return None
    stored = published_coverage(
        conn, pub=publication, variable=variable, window_key=window_key
    )
    days = tuple(range(MAX_DAY_AHEAD + 1))
    rows = tuple(
        CoverageRow(
            feed_id=item.feed_id,
            label=item.label,
            cells=tuple(
                _coverage_cell(stored.get((item.feed_id, day)), day) for day in days
            ),
        )
        for item in leaderboard
    )
    return CoverageTable(
        days=days,
        rows=rows,
        selected={
            item.feed_id: _coverage_text(stored.get((item.feed_id, day_ahead)))
            for item in leaderboard
        },
    )


def _coverage_cell(coverage: Coverage | None, day_ahead: int) -> CoverageCell:
    """One coverage table cell; a cell with no stored row expects nothing."""
    return CoverageCell(
        day_ahead=day_ahead,
        scored=0 if coverage is None else coverage.scored,
        expected=0 if coverage is None else coverage.expected,
        text=_coverage_text(coverage),
    )


def load_ops(conn: sqlite3.Connection) -> dict[str, object]:
    """The Ops page's context (plan §10).

    Reads ``now`` and the daily close's switch once, at its start, and
    passes the answer to every panel that has two views, so the page never
    mixes them. With the close on, the time-zone correction panel is not
    loaded (§10.3) and ``close_panel`` holds the close status panel; with it
    off the panels are today's and ``close_panel`` is None.
    """
    from wxverify.verification.publish_hold import read_publish_hold

    now = utc_now()
    close_on = daily_close_on(conn)
    sites = load_sites(conn)
    return {
        "sites": sites,
        "feed_status": load_feed_health(conn),
        "budgets": load_budgets(conn),
        "backfill": load_backfill(conn),
        "keys": [
            KeyStatusRow(provider=provider, present=present)
            for provider, present in sorted(key_status().items())
        ],
        "access_holds": load_access_holds(conn),
        "observation_health": load_observation_health(conn, close_on=close_on),
        "station_trust": load_station_trust(conn),
        "wind_history": load_wind_history(conn),
        "publish_hold": read_publish_hold(conn),
        "timezone_correction": (
            [] if close_on else load_timezone_correction(conn, sites)
        ),
        "close_on": close_on,
        "close_panel": load_close_status(conn, now, close_on=close_on),
    }


def load_backfill_panel(conn: sqlite3.Connection) -> dict[str, object]:
    """The backfill panel alone, in the same view as the page (plan §10.2).

    What ``render_backfill`` renders after a button press: ``rows``
    (``load_backfill``), ``close_on`` and ``close_panel``, with ``now`` and
    the switch read once, as ``load_ops`` reads them.
    """
    now = utc_now()
    close_on = daily_close_on(conn)
    return {
        "rows": load_backfill(conn),
        "close_on": close_on,
        "close_panel": load_close_status(conn, now, close_on=close_on),
    }


def load_overlay(
    conn: sqlite3.Connection,
    *,
    site_id: int | None,
    variable: str,
    feed_id: int | None,
) -> dict[str, object]:
    sites = load_sites(conn, include_disabled=False)
    site = (
        load_site(conn, site_id)
        if site_id is not None
        else (sites[0] if sites else None)
    )
    # One switch per read (9.2): it picks the feed list and the chart hint.
    close_on = daily_close_on(conn)
    feeds = _scoring_feeds(
        conn, site.id if site is not None else None, variable, close_on=close_on
    )
    selected_feed_id = (
        feed_id if feed_id is not None else (feeds[0].id if feeds else None)
    )
    return {
        "sites": sites,
        "site": site,
        "variable": variable,
        "feeds": feeds,
        "feed_id": selected_feed_id,
        "close_on": close_on,
    }


FEED_HEALTH_SQL = """
    WITH feed_rollup AS (
        SELECT m.id AS src_feed_id,
               CASE
                 WHEN m.source='meteoblue' AND m.model!='multimodel'
                 THEN pkg.id
                 ELSE m.id
               END AS display_feed_id
        FROM feeds m
        LEFT JOIN feeds pkg
          ON pkg.source='meteoblue' AND pkg.model='multimodel'
    )
    SELECT s.id AS site_id, s.name AS site_name, f.id AS feed_id,
           s.enabled AS site_enabled,
           f.source, f.model, f.enabled AS feed_enabled,
           f.default_subscribed, f.disabled_reason,
           sfs.enabled AS override_enabled, sfs.last_run_at, sfs.last_error,
           sfs.error_count,
           EXISTS (
               SELECT 1
               FROM feed_rollup r
               -- CROSS JOIN is load-bearing: it pins feed_rollup as the outer
               -- loop so the probe binds BOTH (site_id, feed_id) and SQLite
               -- can seek a covering index for that pair -- which index it
               -- picks is the planner's choice and is not guaranteed, so no
               -- index name is pinned here. With a plain JOIN the planner
               -- drives from forecast_samples, binds site_id only, and the
               -- seek degrades to an index scan -- measured 5x SLOWER than
               -- the full-table aggregate this replaced.
               CROSS JOIN forecast_samples fs
                 ON fs.site_id = s.id AND fs.feed_id = r.src_feed_id
               WHERE r.display_feed_id = f.id
           ) AS has_samples
    FROM sites s
    JOIN feeds f
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = s.id AND sfs.feed_id = f.id
    WHERE f.is_virtual = 0
      AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
    ORDER BY s.name COLLATE NOCASE, f.source, f.model
    """


def load_feed_health(conn: sqlite3.Connection) -> list[FeedHealthRow]:
    rows = conn.execute(FEED_HEALTH_SQL).fetchall()
    out: list[FeedHealthRow] = []
    for row in rows:
        subscribed = bool(
            row["override_enabled"]
            if row["override_enabled"] is not None
            else row["default_subscribed"]
        )
        if not bool(row["site_enabled"]):
            status = "site disabled"
        elif not bool(row["feed_enabled"]):
            status = "disabled"
        elif not subscribed:
            status = "not subscribed / available"
        # last_error must be tested before last_run_at is None: a failed
        # fetch (mark_feed_error) never sets last_run_at, so an all-failures
        # feed would otherwise be misreported as never having run. Every
        # surface deriving feed status from these fields must keep this order.
        elif row["last_error"] == NO_USABLE_SAMPLES_SENTINEL:
            status = "fetched, 0 usable"
        elif row["last_error"] is not None:
            status = "error"
        elif row["last_run_at"] is None:
            status = "never run / due"
        elif not bool(row["has_samples"]):
            status = "ran / no usable data"
        else:
            status = "ok"
        out.append(
            FeedHealthRow(
                site_id=int(row["site_id"]),
                site_name=str(row["site_name"]),
                feed_id=int(row["feed_id"]),
                label=feed_label(str(row["source"]), str(row["model"])),
                subscribed=subscribed,
                status=status,
                disabled_reason=None
                if row["disabled_reason"] is None
                else str(row["disabled_reason"]),
                last_run_at=None
                if row["last_run_at"] is None
                else str(row["last_run_at"]),
                last_error=None
                if row["last_error"] is None
                else str(row["last_error"]),
                error_count=int(row["error_count"] or 0),
                feed_enabled=bool(row["feed_enabled"]),
                site_enabled=bool(row["site_enabled"]),
                has_samples=bool(row["has_samples"]),
            )
        )
    return out


def load_budgets(conn: sqlite3.Connection) -> list[BudgetGauge]:
    rows = conn.execute(
        """
        SELECT source, daily_call_limit, daily_credit_limit, billing_tz
        FROM sources s
        ORDER BY s.source
        """
    ).fetchall()
    out: list[BudgetGauge] = []
    for row in rows:
        source = str(row["source"])
        budget = conn.execute(
            """
            SELECT calls, credits
            FROM api_budget
            WHERE source = ? AND billing_day = ?
            """,
            (source, current_billing_day(str(row["billing_tz"]))),
        ).fetchone()
        configured = int(row["daily_call_limit"])
        out.append(
            BudgetGauge(
                source=source,
                daily_call_limit=effective_daily_call_limit(source, configured),
                configured_call_limit=configured,
                daily_credit_limit=None
                if row["daily_credit_limit"] is None
                else int(row["daily_credit_limit"]),
                calls=0 if budget is None else int(budget["calls"]),
                credits=0 if budget is None else int(budget["credits"]),
            )
        )
    return out


def load_backfill(conn: sqlite3.Connection) -> list[BackfillRow]:
    return [
        BackfillRow(
            site_id=int(row["id"]),
            site_name=str(row["name"]),
            status=None
            if row["backfill_status"] is None
            else str(row["backfill_status"]),
            through=None
            if row["backfill_through"] is None
            else str(row["backfill_through"]),
        )
        for row in conn.execute(
            """
            SELECT id, name, backfill_status, backfill_through
            FROM sites
            ORDER BY name COLLATE NOCASE
            """
        )
    ]


def load_observation_health(
    conn: sqlite3.Connection, *, close_on: bool
) -> list[ObservationHealthRow]:
    """Each site's Observation Status row (plan §10.1).

    With the daily close off: today's query, ``sites.last_obs_at`` and the
    status "never run / due" while it is NULL. With it on: the same query
    without ``s.last_obs_at``, the site's ``closed_through`` in its place
    and the status "no day closed yet" while it is None. "site disabled"
    and "no enabled stations" come first in both views.
    """
    last_obs_column = "" if close_on else " s.last_obs_at,"
    rows = conn.execute(
        f"""
        SELECT s.id, s.name, s.enabled,{last_obs_column}
               COALESCE(station_counts.n, 0) AS enabled_station_count
        FROM sites s
        LEFT JOIN (
            SELECT site_id, COUNT(*) AS n
            FROM stations
            WHERE enabled = 1
            GROUP BY site_id
        ) station_counts
          ON station_counts.site_id = s.id
        ORDER BY s.name COLLATE NOCASE
        """
    ).fetchall()
    out: list[ObservationHealthRow] = []
    for row in rows:
        site_id = int(row["id"])
        enabled_station_count = int(row["enabled_station_count"])
        last_obs_at: str | None = None
        through: date | None = None
        if close_on:
            through = closed_through(conn, site_id)
        elif row["last_obs_at"] is not None:
            last_obs_at = str(row["last_obs_at"])
        if not bool(row["enabled"]):
            status = "site disabled"
        elif enabled_station_count == 0:
            status = "no enabled stations"
        elif close_on:
            status = "no day closed yet" if through is None else "ok"
        elif last_obs_at is None:
            status = "never run / due"
        else:
            status = "ok"
        out.append(
            ObservationHealthRow(
                site_id=site_id,
                site_name=str(row["name"]),
                status=status,
                last_obs_at=last_obs_at,
                enabled_station_count=enabled_station_count,
                closed_through=None if through is None else through.isoformat(),
            )
        )
    return out


# A done day shows on the close status panel for this long after its
# ``done_at``, as alerts 1 and 2 show it (plan §5.1, §10.2).
_CLOSE_PANEL_DONE_WINDOW: Final = timedelta(days=close_monitor.CLOSE_ALERT_DAYS)

# Alert 6's id: the budget group's condition, whose close part the panel
# shows (plan §6.4, §10.2).
_ACCESS_REFUSED_ID: Final = "weathercom_access_refused"


def _parse_stamp(value: str | None) -> datetime | None:
    """``parse_utc(value)``, or None for NULL or a value that does not read."""
    if value is None:
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _utc_text(value: datetime | None) -> str:
    """``value`` as stored, in UTC, or "unknown" when it is None."""
    return "unknown" if value is None else isoformat_utc(value)


def _wait_text(seconds: float) -> str:
    """A queue wait in whole seconds, as "5 s", "2 min 5 s" or "1 h 2 min"."""
    total = max(0, round(seconds))
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    parts = [f"{hours} h"] if hours else []
    if minutes:
        parts.append(f"{minutes} min")
    if secs or not parts:
        parts.append(f"{secs} s")
    return " ".join(parts)


def _missed_why(record: DayRecord, status: CloseStatus | None) -> str:
    """Why a day was missed, worded as alert 1 words it (plan §5.2, §10.2).

    A changeover day, which alert 1 never names, reads "zone changeover".
    """
    failure = (
        "no reason recorded" if record.last_failure is None else record.last_failure
    )
    reason = record.missed_reason
    if reason == "never_attempted":
        return "the close never ran (the add-on was not running when it was due)"
    if reason == "retry_not_run":
        return (
            "the retry fell due while the add-on was not running;"
            f" attempt 1 failed: {failure}"
        )
    if reason == "retry_failed":
        stage = None if status is None else status.blocking_stage
        return (
            f"both attempts failed, the retry at {close_monitor.stage_text(stage)}:"
            f" {failure}"
        )
    if reason == "zone_changeover":
        return "zone changeover"
    return "unreadable missed_reason"


def _close_day_state(
    record: DayRecord, status: CloseStatus | None, now: datetime
) -> str:
    """P1's state of the day in words (plan §10.2).

    An ``open`` day reads "collecting", "waiting for the close" once its
    end has passed, and "not started" before its start (a changeover day
    still to come).
    """
    if record.state == "open":
        if now < record.day_start_utc:
            return "not started"
        if now < record.day_end_utc:
            return "collecting"
        return "waiting for the close"
    if record.state == "closing":
        return f"closing, attempt {record.attempts} of 2"
    if record.state == "retry_due":
        return f"attempt 1 failed; retry due {_utc_text(record.retry_at)}"
    if record.state == "closed":
        return "closed"
    return f"missed: {_missed_why(record, status)}"


def _close_day_stage(
    conn: sqlite3.Connection,
    record: DayRecord,
    status: CloseStatus | None,
    now: datetime,
) -> tuple[str | None, str | None]:
    """The blocking stage and the stage line of a day not done (§10.2).

    None for a done day and for a day still collecting. A stage running or
    waiting to run (``open`` after its end, ``closing``): "stage <n> <name>
    since <t>", <t> the current attempt's ``last_progress_at``, or the
    day's end when no attempt has begun. A failed stage (``retry_due``):
    its stage-log row, "stage <n> <name>, started <t>, waited <w> in the
    queue, ended <e>"; no line when the row is not found.
    """
    if record.state in ("closed", "missed") or (
        record.state == "open" and now < record.day_end_utc
    ):
        return None, None
    stage = None if status is None else status.blocking_stage
    text = close_monitor.stage_text(stage)
    if record.state == "retry_due":
        run = next(
            (
                row
                for row in reversed(
                    stage_log(
                        conn, site_id=record.site_id, local_date=record.local_date
                    )
                )
                if row.stage == stage
                and row.attempt == record.attempts
                and row.outcome == "failed"
            ),
            None,
        )
        if run is None:
            return text, None
        return text, (
            f"{text}, started {isoformat_utc(run.started_at)}, waited"
            f" {_wait_text(run.queue_wait_s)} in the queue, ended"
            f" {isoformat_utc(run.ended_at)}"
        )
    current = (
        None
        if status is None
        else next(
            (row for row in status.attempt_records if row.attempt == status.attempts),
            None,
        )
    )
    since = record.day_end_utc if current is None else current.last_progress_at
    return text, f"{text} since {isoformat_utc(since)}"


def _publication_text(publication: PublicationRecord | None) -> str | None:
    """A closed day's publication record (R3.7) in words (plan §10.2)."""
    if publication is None:
        return None
    reason = (
        "no reason recorded"
        if publication.last_failure is None
        else publication.last_failure
    )
    if publication.state == "pending":
        return f"publication queued since {isoformat_utc(publication.opened_at)}"
    if publication.state == "building":
        started = publication.started_at or publication.opened_at
        return f"publishing since {isoformat_utc(started)}"
    if publication.state == "published":
        return f"published {_utc_text(publication.finished_at)}"
    if publication.state == "retry_due":
        return (
            f"publication attempt 1 failed: {reason}; retry due"
            f" {_utc_text(publication.retry_at)}"
        )
    if publication.state == "failed":
        return f"publication failed {_utc_text(publication.finished_at)}: {reason}"
    return "included in a later publication"


def _close_day_marks(
    conn: sqlite3.Connection, record: DayRecord, now: datetime
) -> tuple[str, ...]:
    """The day's zone marks (plan §10.2; P1 6.4, 6.5)."""
    marks: list[str] = []
    if record.zone_changeover:
        if now < record.day_start_utc:
            marks.append(
                f"zone changes on {record.local_date.isoformat()}: that day is not"
                " scored"
            )
        else:
            marks.append("zone changeover, not scored")
    if record.state == "closed" and zone_flagged(
        conn, record.site_id, record.local_date
    ):
        marks.append(f"scored under its own zone, {record.timezone}")
    return tuple(marks)


def _close_day_row(
    conn: sqlite3.Connection, record: DayRecord, now: datetime
) -> CloseDayRow:
    """One day on the close status panel (plan §10.2)."""
    status = close_status(
        conn, site_id=record.site_id, local_date=record.local_date, now_utc=now
    )
    stage, stage_line = _close_day_stage(conn, record, status, now)
    return CloseDayRow(
        local_date=record.local_date.isoformat(),
        state=_close_day_state(record, status, now),
        attempts=record.attempts,
        retry_at=None if record.retry_at is None else isoformat_utc(record.retry_at),
        failure=record.last_failure,
        stage=stage,
        stage_line=stage_line,
        publication=(
            _publication_text(status.publication)
            if status is not None and record.state == "closed"
            else None
        ),
        marks=_close_day_marks(conn, record, now),
    )


def _close_panel_days(
    conn: sqlite3.Connection,
    site_id: int,
    through: date | None,
    current: DayRecord | None,
    now: datetime,
) -> list[DayRecord]:
    """The days the panel shows for one enabled site, newest first (§10.2).

    The day that holds ``now``, every day not done that has ended, every
    done day whose ``done_at`` is within ``CLOSE_ALERT_DAYS`` (one whose
    ``done_at`` does not read is kept), and the next day when it is a
    changeover day. Walks P1's ``day_record`` date by date, as records
    follow one another one date apart: down from ``closed_through`` for the
    done days, then up from the first day not done; with no day done, that
    is the site's first record, found by walking down from the current day,
    and with no current day either there is none.
    """
    one_day = timedelta(days=1)
    found: dict[date, DayRecord] = {}
    start: date | None = None
    if through is not None:
        local_date = through
        while (record := day_record(conn, site_id, local_date)) is not None:
            done_at = _parse_stamp(record.done_at)
            if done_at is not None and now - done_at > _CLOSE_PANEL_DONE_WINDOW:
                break
            found[record.local_date] = record
            local_date -= one_day
        start = through + one_day
    elif current is not None:
        start = current.local_date
        while day_record(conn, site_id, start - one_day) is not None:
            start -= one_day
    if start is not None:
        local_date = start
        while (record := day_record(conn, site_id, local_date)) is not None:
            if record.day_start_utc > now:
                break
            found[record.local_date] = record
            local_date += one_day
    if current is not None:
        following = day_record(conn, site_id, current.local_date + one_day)
        if (
            following is not None
            and following.zone_changeover
            and now < following.day_start_utc
        ):
            found[following.local_date] = following
    return [found[key] for key in sorted(found, reverse=True)]


def _latest_try_text(tries: list[CollectionTry]) -> str | None:
    """The newest collection try (R3.6) in words (plan §10.2)."""
    if not tries:
        return None
    latest = max(tries, key=lambda attempt: attempt.tried_at)
    when = isoformat_utc(latest.tried_at)
    if latest.slot == "gap_fill":
        return f"last gap-fill try {when}: {latest.outcome}"
    return (
        f"last 5-minute try {when}: {latest.outcome}, {latest.readings_kept} readings"
    )


def _close_panel_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    now: datetime,
) -> ClosePanelRow:
    """One site's part of the close status panel (plan §10.2)."""
    site_id = int(row["id"])
    site_name = str(row["name"])
    if not bool(row["enabled"]):
        return ClosePanelRow(
            site_id=site_id,
            site_name=site_name,
            enabled=False,
            days=(),
            latest_try=None,
            note="site disabled",
        )
    through = closed_through(conn, site_id)
    current = current_day(conn, site_id, now_utc=now)
    notes: list[str] = []
    if through is None:
        notes.append(
            "no day closed yet"
            if current is not None
            else "no day closed yet; the first day has not started"
        )
    try:
        ZoneInfo(str(row["timezone"]))
    except (ZoneInfoNotFoundError, ValueError):
        notes.append("the site's zone cannot be read; new days cannot be created")
    records = _close_panel_days(conn, site_id, through, current, now)
    tries = [
        attempt
        for record in records
        for attempt in collection_tries(
            conn, site_id=site_id, local_date=record.local_date
        )
    ]
    return ClosePanelRow(
        site_id=site_id,
        site_name=site_name,
        enabled=True,
        days=tuple(_close_day_row(conn, record, now) for record in records),
        latest_try=_latest_try_text(tries),
        note="; ".join(notes) if notes else None,
    )


def _close_alert_lines(conn: sqlite3.Connection, now: datetime) -> tuple[str, ...]:
    """The panel's alert lines, read in the caller's read (plan §10.2).

    "<id>: <detail>" for each tripped alert of ``close_alerts`` and
    ``station_silent``, alert 7's "recovered" line while it shows, and
    alert 6's close part from ``access_days_affected``: its next try and
    its full list of days affected. The monitor's start-up grace and its
    toggles do not apply.
    """
    lines: list[str] = []
    alerts = [
        *close_monitor.close_alerts(conn, now),
        close_monitor.station_silent(conn, now, close_on=True),
    ]
    for alert in alerts:
        if alert.tripped:
            lines.append(
                alert.id if alert.detail is None else f"{alert.id}: {alert.detail}"
            )
        elif alert.recovered_at is not None and alert.detail is not None:
            lines.append(f"{alert.id}: {alert.detail}")
    access = close_monitor.access_days_affected(conn, now)
    parts: list[str] = []
    if access.next_try:
        parts.append(f"next try: {', '.join(access.next_try)}")
    if access.days:
        parts.append(f"days affected: {', '.join(access.days)}")
    if parts:
        lines.append(f"{_ACCESS_REFUSED_ID}: {'; '.join(parts)}")
    return tuple(lines)


def load_close_status(
    conn: sqlite3.Connection, now: datetime, *, close_on: bool
) -> ClosePanel | None:
    """The close status panel (plan §10.2), or None with the close off.

    One row per site, in ``load_backfill``'s order: a disabled site reads
    "site disabled" and nothing else; an enabled site has its recent days
    (``_close_panel_days``), its newest collection try and its notes. Then
    the alert lines. Day states come only from P1's reads and the close's
    from P3's. The whole read is wrapped: an ``Exception`` other than
    ``sqlite3.Error`` gives a panel that shows only "Close status
    unavailable: <Type>: <msg>", so the rest of Ops renders; a
    ``sqlite3.Error`` propagates as every other panel's does.
    """
    if not close_on:
        return None
    try:
        sites = conn.execute(
            """
            SELECT id, name, enabled, timezone
            FROM sites
            ORDER BY name COLLATE NOCASE
            """
        ).fetchall()
        rows = tuple(_close_panel_row(conn, row, now) for row in sites)
        return ClosePanel(rows=rows, alert_lines=_close_alert_lines(conn, now))
    except sqlite3.Error:
        raise
    except Exception as exc:
        return ClosePanel(
            rows=(),
            alert_lines=(),
            unavailable=f"Close status unavailable: {type(exc).__name__}: {exc}",
        )


def _station_trust_limits(
    conn: sqlite3.Connection, now: datetime
) -> tuple[list[tuple[int, str]], list[tuple[int, str, str]]]:
    """Per site, where Station Trust stops and the missed days it skips (9.3).

    The stop is the end of the site's latest done day (``closed_through``)
    or, with no day done, the start of its first record, so only the
    starting history is read; a site with neither a done day nor a record
    holding ``now`` has no record started and gets no stop. The missed days
    are the site's ``missed`` records up to its latest done day, each as
    its stored bounds. Both are found by walking down by date with P1's
    ``day_record``: records follow one another one date apart.
    """
    stops: list[tuple[int, str]] = []
    missed: list[tuple[int, str, str]] = []
    for site_row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall():
        site_id = int(site_row[0])
        through = closed_through(conn, site_id)
        record = None if through is None else day_record(conn, site_id, through)
        if record is None:
            first = current_day(conn, site_id, now_utc=now)
            if first is None:
                continue
            earlier = day_record(conn, site_id, first.local_date - timedelta(days=1))
            while earlier is not None:
                first = earlier
                earlier = day_record(
                    conn, site_id, first.local_date - timedelta(days=1)
                )
            stops.append((site_id, isoformat_utc(first.day_start_utc)))
            continue
        stops.append((site_id, isoformat_utc(record.day_end_utc)))
        while record is not None:
            if record.state == "missed":
                missed.append(
                    (
                        site_id,
                        isoformat_utc(record.day_start_utc),
                        isoformat_utc(record.day_end_utc),
                    )
                )
            record = day_record(conn, site_id, record.local_date - timedelta(days=1))
    return stops, missed


def load_station_trust(conn: sqlite3.Connection) -> list[StationTrustRow]:
    """Each station's ``ok`` values against its site's, worst mean gap first.

    With the daily close off, over all time. With it on (9.3), over each
    site's closed days and starting history only: rows at or after the
    site's stop and rows on its missed days (``_station_trust_limits``) are
    left out, each list passed as a ``VALUES`` table; a list with no row
    leaves its clause out, as an empty ``VALUES`` list is not valid SQL.
    The ``observations`` join is on the same ``valid_at``, so both sides end
    there. Never reads the provisional line.
    """
    ctes: list[str] = []
    clauses = ["so.qc_flag = 'ok'"]
    params: list[object] = []
    if daily_close_on(conn):
        stops, missed = _station_trust_limits(conn, utc_now())
        if stops:
            ctes.append(
                "trust_stops(site_id, valid_before) AS (VALUES "
                + ", ".join("(?, ?)" for _ in stops)
                + ")"
            )
            params.extend(value for stop in stops for value in stop)
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM trust_stops t"
                " WHERE t.site_id = stations.site_id"
                " AND so.valid_at >= t.valid_before)"
            )
        if missed:
            ctes.append(
                "trust_missed(site_id, start_utc, end_utc) AS (VALUES "
                + ", ".join("(?, ?, ?)" for _ in missed)
                + ")"
            )
            params.extend(value for day in missed for value in day)
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM trust_missed m"
                " WHERE m.site_id = stations.site_id"
                " AND so.valid_at >= m.start_utc AND so.valid_at < m.end_utc)"
            )
    with_clause = f"WITH {', '.join(ctes)}" if ctes else ""
    rows = conn.execute(
        f"""
        {with_clause}
        SELECT sites.name AS site_name, stations.pws_station_id, so.variable,
               COUNT(*) AS n,
               AVG(so.value - observations.value) AS mean_delta
        FROM station_observations so
        JOIN stations ON stations.id = so.station_id
        JOIN sites ON sites.id = stations.site_id
        JOIN observations
          ON observations.site_id = stations.site_id
         AND observations.variable = so.variable
         AND observations.valid_at = so.valid_at
        WHERE {" AND ".join(clauses)}
        GROUP BY stations.id, so.variable
        HAVING n > 0
        ORDER BY ABS(mean_delta) DESC, sites.name COLLATE NOCASE
        LIMIT 25
        """,
        params,
    ).fetchall()
    return [
        StationTrustRow(
            site_name=str(row["site_name"]),
            station=str(row["pws_station_id"]),
            variable=str(row["variable"]),
            n=int(row["n"]),
            mean_delta=float(row["mean_delta"]),
        )
        for row in rows
    ]


def _wind_holds(conn: sqlite3.Connection) -> list[WindHoldView]:
    return [
        WindHoldView(
            endpoint=endpoint,
            status=hold.status,
            title=WIND_HOLD_TITLES[endpoint],
            since=hold.since,
            error=hold.error,
        )
        for endpoint, hold in read_auth_holds(conn).items()
    ]


def load_access_holds(conn: sqlite3.Connection) -> list[AccessHoldView]:
    """One line per download kind with a refused-access row, in kind order.

    Every row is shown however long ago its hold ended (no 24 h cut, unlike
    the monitor): the row stays until a granted reply of its kind deletes it.
    Times are the stored UTC stamps. An unreadable row is named
    ``<label> (unreadable hold)`` and shows no time. Read-only.
    """
    now = utc_now()
    views: list[AccessHoldView] = []
    for kind in ACCESS_KINDS:
        record = read_access_record(conn, kind)
        if record is None:
            continue
        head = "weather.com access refused"
        if record.status is not None:
            head += f" (HTTP {record.status})"
        text = f"{head}: {ACCESS_KIND_LABELS[kind]}"
        if record.until is None:
            text += " (unreadable hold), waiting for the next try"
        else:
            if record.since is not None:
                text += f", since {record.since}"
            if parse_utc(record.until) > now:
                text += f", held until {record.until}"
            else:
                text += ", waiting for the next try"
        views.append(AccessHoldView(kind=kind, text=text))
    return views


def _lane_calls_today(conn: sqlite3.Connection) -> tuple[int | None, int | None]:
    """The lane's backfill and live calls on today's weather.com billing day."""
    row = conn.execute(
        "SELECT billing_tz FROM sources WHERE source = 'weathercom'"
    ).fetchone()
    if row is None:
        return None, None
    try:
        today = current_billing_day(str(row["billing_tz"]))
    except (ZoneInfoNotFoundError, ValueError):
        return None, None
    return (
        read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY).get(today, 0),
        read_lane_counter(conn, WIND_LIVE_CALLS_KEY).get(today, 0),
    )


def _wind_site_history(
    conn: sqlite3.Connection, site_id: int, site_name: str
) -> WindSiteHistory:
    counts = {
        str(row["status"]): int(row["n"])
        for row in conn.execute(
            """
            SELECT d.status, COUNT(*) AS n
            FROM station_wind_days d JOIN stations st ON st.id = d.station_id
            WHERE st.site_id = ? AND st.enabled = 1
            GROUP BY d.status
            """,
            (site_id,),
        )
    }
    errors = [
        WindErrorRow(
            station=str(row["pws_station_id"]),
            local_date=str(row["local_date"]),
            status=str(row["status"]),
            error=str(row["last_error"]),
        )
        for row in conn.execute(
            """
            SELECT st.pws_station_id, d.local_date, d.status, d.last_error
            FROM station_wind_days d JOIN stations st ON st.id = d.station_id
            WHERE st.site_id = ? AND d.last_error IS NOT NULL
            ORDER BY d.updated_at DESC, st.id, d.local_date DESC
            LIMIT 10
            """,
            (site_id,),
        )
    ]
    return WindSiteHistory(
        site_id=site_id,
        site_name=site_name,
        status_counts=[(status, counts.get(status, 0)) for status in WIND_DAY_STATUSES],
        recent_errors=errors,
    )


def load_wind_history(conn: sqlite3.Connection) -> WindHistoryPanel:
    """The ops Wind History panel (plan §10). Read-only."""
    backfill_calls, live_calls = _lane_calls_today(conn)
    sites = [
        _wind_site_history(conn, int(row["id"]), str(row["name"]))
        for row in conn.execute(
            "SELECT id, name FROM sites WHERE enabled = 1 ORDER BY name COLLATE NOCASE"
        ).fetchall()
    ]
    return WindHistoryPanel(
        headroom=backfill_headroom(conn, now=utc_now()),
        backfill_calls_today=backfill_calls,
        live_calls_today=live_calls,
        holds=_wind_holds(conn),
        sites=sites,
    )


def load_wind_banner(conn: sqlite3.Connection, site_id: int) -> WindBanner | None:
    """The dashboard wind banner while a weather.com auth hold exists, else None."""
    holds = [hold.title for hold in _wind_holds(conn)]
    if not holds:
        return None
    return WindBanner(holds=holds)


def load_wind_weights(
    conn: sqlite3.Connection, site: SiteView, day_ahead: int
) -> list[WindWeightRow] | None:
    """The dashboard wind weights for one lead; None when the site's time
    zone cannot be loaded.

    Uses the same :func:`load_wind_serving` call as the Forecast page. The
    weight (%) is each feed's share among the feeds with a track record at
    this lead; a day's served blend renormalizes over the feeds covering it.
    """
    try:
        today = utc_now().astimezone(ZoneInfo(site.timezone)).date()
    except (ZoneInfoNotFoundError, ValueError):
        return None
    serving = load_wind_serving(
        conn, site_id=site.id, timezone=site.timezone, today=today, as_of=None
    )
    at_lead = {
        feed_id: weight
        for (feed_id, k), weight in serving.weights.items()
        if k == day_ahead
    }
    total = sum(weight.weight for weight in at_lead.values())
    labels = {
        int(row["id"]): feed_label(str(row["source"]), str(row["model"]))
        for row in conn.execute("SELECT id, source, model FROM feeds")
    }
    rows = [
        WindWeightRow(
            feed_id=feed_id,
            label=labels.get(feed_id, f"feed {feed_id}"),
            mae_kmh=ms_to_kmh(weight.mae_ms),
            days=weight.days,
            weight_pct=0.0 if total <= 0 else 100.0 * weight.weight / total,
        )
        for feed_id, weight in at_lead.items()
    ]
    rows.sort(key=lambda row: (-row.weight_pct, row.feed_id))
    return rows


def _generation_int(row: dict[str, object], key: str) -> int:
    return int(str(row[key]))


def _generation_optional_int(row: dict[str, object], key: str) -> int | None:
    value = row[key]
    return None if value is None else int(str(value))


def _generation_optional_str(row: dict[str, object], key: str) -> str | None:
    value = row[key]
    return None if value is None else str(value)


def _newest_failed_generation(
    rows: list[dict[str, object]], published_generation_id: int | None
) -> dict[str, object] | None:
    """The site's newest failed generation that is newer than the published one.

    The "newer than published" clause is what stops a long-dead failure from
    haunting a site that has since been corrected successfully. Rows arrive
    ordered by generation id, so the last match is the newest.
    """
    failed = [row for row in rows if str(row["state"]) == "failed"]
    if published_generation_id is not None:
        failed = [
            row
            for row in failed
            if _generation_int(row, "generation_id") > published_generation_id
        ]
    return failed[-1] if failed else None


# Both halves of the stalled-cleanup predicate in ONE statement, so an
# autocommit reader evaluates them against a single snapshot. The job half is
# composed from ACTIVE_JOB_SQL rather than retyped: "active" must keep meaning
# exactly what enqueue_if_absent's dedupe means, and a second copy would drift.
# Params: (state key, job type, job key, site id).
STALLED_CLEANUP_SQL = f"""
    SELECT 1 FROM runtime_state
    WHERE key = ?
      AND NOT EXISTS ({ACTIVE_JOB_SQL})
    LIMIT 1
    """


def load_timezone_correction(
    conn: sqlite3.Connection, sites: list[SiteView]
) -> list[TimezoneCorrectionRow]:
    """Per-site applicability and progress for the timezone-correction panel.

    One ``generation_status`` SELECT covers every site; the per-site work is
    an indexed ``jobs`` point lookup for the verification-chain check, plus at
    most one ``STALLED_CLEANUP_SQL`` statement (an indexed ``runtime_state``
    seek and an indexed ``jobs`` seek) for the stalled-cleanup check.
    ``applicable`` mirrors the route's refusals: a BUILDING correction or an
    active verification chain blocks; a FAILED generation does not, because
    the domain refuses only on a building row.
    """
    from wxverify.worker.verification_run import verification_chain_active

    by_site: dict[int, list[dict[str, object]]] = {}
    for row in generation_status(conn):
        by_site.setdefault(_generation_int(row, "site_id"), []).append(row)

    out: list[TimezoneCorrectionRow] = []
    for site in sites:
        rows = by_site.get(site.id, [])
        published = next((row for row in rows if bool(row["published_pointer"])), None)
        building = next((row for row in rows if str(row["state"]) == "building"), None)
        published_id = (
            None if published is None else _generation_int(published, "generation_id")
        )
        failed = _newest_failed_generation(rows, published_id)
        # Counts belong to the generation the row is about: the one being
        # built while a correction runs, otherwise the published one (which
        # after a completed correction carries that correction's tally).
        counts = building if building is not None else published
        chain_active = verification_chain_active(conn, site.id)
        building_id = (
            None if building is None else _generation_int(building, "generation_id")
        )
        if building_id is not None:
            blocked_reason = (
                f"a correction is already building (generation {building_id})"
            )
        elif chain_active:
            blocked_reason = "a verification run is active for this site"
        else:
            blocked_reason = None
        # A stall is only alarming for the CURRENTLY PUBLISHED correction: a
        # retired generation's residual blob is a historical stall whose rows
        # a later correction's cleanup has already swept. The predicate is
        # exact rather than a timing guess only when both halves observe ONE
        # snapshot: no committed state has the blob present with no job
        # pending -- a chunk completes and enqueues its continuation in one
        # transaction, and the final cleanup drops the blob inside the txn of
        # a job that is still running -- but that blob delete and the job's
        # completion commit SEPARATELY, so two statements on an autocommit
        # reader can straddle the gap, see the blob before the delete and the
        # chain after the completion, and report a stall for a cleanup that
        # finished. Hence one statement, one implicit transaction.
        cleanup_stalled_id: int | None = None
        if (
            published is not None
            and published_id is not None
            and str(published["mode"]) == "retrospective_correction"
            and conn.execute(
                STALLED_CLEANUP_SQL,
                (
                    correction_state_key(published_id),
                    "timezone_correction",
                    correction_job_key(published_id),
                    site.id,
                ),
            ).fetchone()
            is not None
        ):
            cleanup_stalled_id = published_id
        out.append(
            TimezoneCorrectionRow(
                site_id=site.id,
                site_name=site.name,
                current_timezone=site.timezone,
                published_generation_id=published_id,
                building_generation_id=building_id,
                building_timezone=(
                    None if building is None else str(building["timezone"])
                ),
                failed_generation_id=(
                    None if failed is None else _generation_int(failed, "generation_id")
                ),
                failed_timezone=None if failed is None else str(failed["timezone"]),
                cleanup_stalled_generation_id=cleanup_stalled_id,
                examined=(
                    None
                    if counts is None
                    else _generation_optional_int(counts, "examined")
                ),
                changed=(
                    None
                    if counts is None
                    else _generation_optional_int(counts, "changed")
                ),
                unchanged=(
                    None
                    if counts is None
                    else _generation_optional_int(counts, "unchanged")
                ),
                excluded=(
                    None
                    if counts is None
                    else _generation_optional_int(counts, "excluded")
                ),
                last_published_at=(
                    None
                    if published is None
                    else _generation_optional_str(published, "published_at")
                ),
                applicable=building_id is None and not chain_active,
                blocked_reason=blocked_reason,
            )
        )
    return out


def feed_label(source: str, model: str) -> str:
    if source == "meteoblue" and model == "multimodel":
        return "Meteoblue multimodel package"
    if source == "virtual":
        return model.removeprefix("_").replace("_", " ").title()
    return f"{source} / {model}"


FEED_DESCRIPTIONS: dict[tuple[str, str], str] = {
    ("open-meteo", "ecmwf_ifs"): "ECMWF Integrated Forecasting System global model.",
    ("open-meteo", "gfs_global"): "NOAA GFS global model.",
    ("open-meteo", "icon_global"): "DWD ICON global model.",
    ("open-meteo", "gem_global"): "Environment Canada GEM global model.",
    (
        "open-meteo",
        "meteofrance_arpege_world",
    ): "Meteo-France ARPEGE global model.",
    ("open-meteo", "jma_gsm"): "Japan Meteorological Agency GSM global model.",
    (
        "open-meteo",
        "ukmo_global_deterministic_10km",
    ): "UK Met Office global deterministic model.",
    ("open-meteo", "icon_eu"): "DWD ICON-EU regional model for Europe.",
    (
        "meteoblue",
        "multimodel",
    ): "Multimodel package via one API call.",
    ("meteoblue", "AIFS025"): "ECMWF AIFS machine-learning global model.",
    ("meteoblue", "GEM15"): "Environment Canada GEM global model.",
    ("meteoblue", "GFS05"): "NOAA GFS global model.",
    ("meteoblue", "ICON"): "DWD ICON global model.",
    ("meteoblue", "IFS025"): "ECMWF IFS global model.",
    ("meteoblue", "IFSHRES"): "ECMWF IFS high-resolution global model.",
    ("meteoblue", "MFGLOBAL"): "Meteo-France global model.",
    ("meteoblue", "NEMS12"): "Meteoblue NEMS regional model.",
    ("meteoblue", "NEMS12_E"): "Meteoblue NEMS ensemble regional model.",
    ("meteoblue", "NEMS4"): "Meteoblue NEMS high-resolution regional model.",
    ("meteoblue", "NEMSGLOBAL"): "Meteoblue NEMS global model.",
    ("meteoblue", "NEMSGLOBAL_E"): "Meteoblue NEMS global ensemble model.",
    ("meteoblue", "NMM22"): "Meteoblue NMM regional model.",
    ("meteoblue", "UMGLOBAL10"): "UK Met Office global model.",
}


def feed_description(source: str, model: str) -> str:
    return FEED_DESCRIPTIONS.get((source, model), "")


def _site_from_row(conn: sqlite3.Connection, row: sqlite3.Row) -> SiteView:
    site_id = int(row["id"])
    return SiteView(
        id=site_id,
        name=str(row["name"]),
        forecast_lat=float(row["forecast_lat"]),
        forecast_lon=float(row["forecast_lon"]),
        elevation_m=float(row["elevation_m"]),
        timezone=str(row["timezone"]),
        enabled=bool(row["enabled"]),
        rain_threshold_mm=float(row["rain_threshold_mm"]),
        stations=_load_stations(conn, site_id),
        feeds=_fetch_unit_feeds(conn, site_id),
    )


def observation_age_text(last_obstime: str | None, *, now: datetime) -> str | None:
    """Say how old a station's latest kept reading is, or ``None`` to show nothing.

    ``last_obstime`` is the stored stamp of that reading, ``None`` when the
    station has none; ``now`` is passed in, so this reads no clock. A reading
    at most 60 minutes old shows nothing: the Home Assistant integration treats
    a reading as current up to one hour, so the age starts showing where that
    stops. A reading dated in the future has a negative age and shows nothing
    either. Older readings read ``N h old`` below 24 hours and ``N d old`` from
    24 hours, in whole hours or whole days rounded down -- the units of
    ``forecast.service.relative_ago`` from one hour up. A stamp that
    ``parse_utc`` rejects reads ``age unknown``.
    """
    if last_obstime is None:
        return None
    try:
        observed = parse_utc(last_obstime)
    except ValueError:
        return "age unknown"
    age = now - observed
    if age <= timedelta(minutes=60):
        return None
    if age < timedelta(hours=24):
        return f"{age // timedelta(hours=1)} h old"
    return f"{age // timedelta(days=1)} d old"


def _load_stations(conn: sqlite3.Connection, site_id: int) -> list[StationView]:
    rows = conn.execute(
        """
        SELECT st.id, st.pws_station_id, st.lat, st.lon, st.dem_elevation_m,
               st.enabled, sps.last_obstime AS last_obstime
        FROM stations st
        LEFT JOIN station_poll_state sps ON sps.station_id = st.id
        WHERE st.site_id = ? AND st.retired_at IS NULL
        ORDER BY st.enabled DESC, st.pws_station_id COLLATE NOCASE
        """,
        (site_id,),
    ).fetchall()
    now = utc_now()
    return [
        StationView(
            id=int(row["id"]),
            pws_station_id=str(row["pws_station_id"]),
            lat=float(row["lat"]),
            lon=float(row["lon"]),
            dem_elevation_m=float(row["dem_elevation_m"]),
            enabled=bool(row["enabled"]),
            observation_age=observation_age_text(row["last_obstime"], now=now),
        )
        for row in rows
    ]


def _fetch_unit_feeds(
    conn: sqlite3.Connection, site_id: int | None
) -> list[FeedToggle]:
    if site_id is None:
        rows = conn.execute(
            """
            SELECT f.*, NULL AS override_enabled
            FROM feeds f
            WHERE f.is_virtual = 0
              AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
            ORDER BY f.source, f.model
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT f.*, sfs.enabled AS override_enabled
            FROM feeds f
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = ? AND sfs.feed_id = f.id
            WHERE f.is_virtual = 0
              AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
            ORDER BY f.source, f.model
            """,
            (site_id,),
        ).fetchall()
    out: list[FeedToggle] = []
    for row in rows:
        override = (
            None if row["override_enabled"] is None else bool(row["override_enabled"])
        )
        default = bool(row["default_subscribed"])
        out.append(
            FeedToggle(
                id=int(row["id"]),
                source=str(row["source"]),
                model=str(row["model"]),
                label=feed_label(str(row["source"]), str(row["model"])),
                description=feed_description(str(row["source"]), str(row["model"])),
                enabled=bool(row["enabled"]),
                default_subscribed=default,
                override_enabled=override,
                effective_enabled=override if override is not None else default,
                disabled_reason=None
                if row["disabled_reason"] is None
                else str(row["disabled_reason"]),
            )
        )
    return out


def _scoring_feeds(
    conn: sqlite3.Connection, site_id: int | None, variable: str, *, close_on: bool
) -> list[FeedToggle]:
    """The Overlay's feed list for a site, ordered by source and model.

    With the daily close off, the site's active feeds that have a stored
    pair of ``variable``. With it on (9.2), every active feed: closed days'
    pairs are P4's, so ``forecast_pairs`` no longer says which feed has data.
    """
    if site_id is None:
        return []
    if close_on:
        rows = conn.execute(
            f"""
            WITH {active_feed_cte()}
            SELECT f.*, sfs.enabled AS override_enabled
            FROM active_feeds a
            JOIN feeds f ON f.id = a.feed_id
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = ? AND sfs.feed_id = f.id
            ORDER BY f.source, f.model
            """,
            (site_id, site_id, site_id),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            WITH {active_feed_cte()}
            SELECT f.*, sfs.enabled AS override_enabled
            FROM active_feeds a
            JOIN feeds f ON f.id = a.feed_id
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = ? AND sfs.feed_id = f.id
            WHERE EXISTS (
                SELECT 1 FROM forecast_pairs fp
                WHERE fp.site_id = ? AND fp.feed_id = a.feed_id AND fp.variable = ?
                  AND {published_generation_clause("fp")}
            )
            ORDER BY f.source, f.model
            """,
            (site_id, site_id, site_id, site_id, variable),
        ).fetchall()
    out: list[FeedToggle] = []
    for row in rows:
        override = (
            None if row["override_enabled"] is None else bool(row["override_enabled"])
        )
        default = bool(row["default_subscribed"])
        out.append(
            FeedToggle(
                id=int(row["id"]),
                source=str(row["source"]),
                model=str(row["model"]),
                label=feed_label(str(row["source"]), str(row["model"])),
                description=feed_description(str(row["source"]), str(row["model"])),
                enabled=bool(row["enabled"]),
                default_subscribed=default,
                override_enabled=override,
                effective_enabled=True,
                disabled_reason=None
                if row["disabled_reason"] is None
                else str(row["disabled_reason"]),
            )
        )
    return out


def _lead_to_day(lead: str) -> int:
    return parse_day_ahead(lead)

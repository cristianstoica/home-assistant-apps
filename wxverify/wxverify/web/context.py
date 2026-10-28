"""Read-side context builders for the server-rendered UI."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
from wxverify.core.timeutil import parse_utc, utc_now
from wxverify.core.units import ms_to_kmh
from wxverify.db.queue import ACTIVE_JOB_SQL
from wxverify.db.tz_generations import (
    correction_job_key,
    correction_state_key,
    generation_status,
    published_generation_clause,
)
from wxverify.db.wind_basis import read_auth_holds
from wxverify.forecast.wind_blend import load_wind_serving
from wxverify.scoring.composite import composite_with_status
from wxverify.scoring.effective import active_feed_cte
from wxverify.scoring.leaderboard import leaderboard as leaderboard_query
from wxverify.scoring.winrate import winrate as winrate_query
from wxverify.settings.keys import get_number_setting
from wxverify.worker.access_hold import (
    ACCESS_KIND_LABELS,
    ACCESS_KINDS,
    read_access_record,
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
    sites = load_sites(conn, include_disabled=False)
    site = (
        load_site(conn, site_id)
        if site_id is not None
        else (sites[0] if sites else None)
    )
    rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
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
            "leaderboard": [],
            "verdict": compute_verdict([]),
            "winrate": [],
            "composite": [],
            "composite_status": "empty",
            "wind_banner": None,
            "wind_weights": None,
        }
    day_ahead = _lead_to_day(lead)
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
        for row in leaderboard_query(
            conn,
            site_id=site.id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
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
    composite_result = composite_with_status(conn, site_id=site.id, window=window)
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


def load_ops(conn: sqlite3.Connection) -> dict[str, object]:
    from wxverify.verification.publish_hold import read_publish_hold

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
        "observation_health": load_observation_health(conn),
        "station_trust": load_station_trust(conn),
        "wind_history": load_wind_history(conn),
        "publish_hold": read_publish_hold(conn),
        "timezone_correction": load_timezone_correction(conn, sites),
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
    feeds = _scoring_feeds(conn, site.id if site is not None else None, variable)
    selected_feed_id = (
        feed_id if feed_id is not None else (feeds[0].id if feeds else None)
    )
    return {
        "sites": sites,
        "site": site,
        "variable": variable,
        "feeds": feeds,
        "feed_id": selected_feed_id,
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


def load_observation_health(conn: sqlite3.Connection) -> list[ObservationHealthRow]:
    rows = conn.execute(
        """
        SELECT s.id, s.name, s.enabled, s.last_obs_at,
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
        enabled_station_count = int(row["enabled_station_count"])
        if not bool(row["enabled"]):
            status = "site disabled"
        elif enabled_station_count == 0:
            status = "no enabled stations"
        elif row["last_obs_at"] is None:
            status = "never run / due"
        else:
            status = "ok"
        out.append(
            ObservationHealthRow(
                site_id=int(row["id"]),
                site_name=str(row["name"]),
                status=status,
                last_obs_at=None
                if row["last_obs_at"] is None
                else str(row["last_obs_at"]),
                enabled_station_count=enabled_station_count,
            )
        )
    return out


def load_station_trust(conn: sqlite3.Connection) -> list[StationTrustRow]:
    rows = conn.execute(
        """
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
        WHERE so.qc_flag = 'ok'
        GROUP BY stations.id, so.variable
        HAVING n > 0
        ORDER BY ABS(mean_delta) DESC, sites.name COLLATE NOCASE
        LIMIT 25
        """
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
        WHERE st.site_id = ?
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
    conn: sqlite3.Connection, site_id: int | None, variable: str
) -> list[FeedToggle]:
    if site_id is None:
        return []
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

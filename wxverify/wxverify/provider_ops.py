"""Shared provider operations for CLI and API surfaces."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

from wxverify.collection.budget import current_billing_day
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.collection.forecast_validation import (
    FORECAST_VARIABLES,
    invalid_forecast_sample_sql,
)
from wxverify.core.options import SECRET_ENV
from wxverify.core.secrets import key_status
from wxverify.db.migrations import seed_default_feeds, seed_default_sources
from wxverify.db.queue import enqueue_if_absent
from wxverify.scoring.engine import pair_and_score
from wxverify.worker.feed_fetch import feed_fetch_target

NEW_PROVIDER_SOURCES: tuple[str, ...] = (
    "visualcrossing",
    "openweathermap",
    "weatherapi",
    "meteosource",
    "google",
)


class ProviderOpsError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass(frozen=True)
class FeedRef:
    feed_id: int
    source: str
    model: str
    enabled: bool
    is_virtual: bool


@dataclass(frozen=True)
class FeedSelection:
    feeds: tuple[FeedRef, ...]
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class FetchQueueResult:
    feed_id: int
    enqueued: bool
    created: bool
    job_id: int | None
    reason: str | None = None


@dataclass(frozen=True)
class SubscriptionResult:
    site_id: int
    feed_id: int
    enabled: bool
    fetch: FetchQueueResult | None = None


@dataclass(frozen=True)
class ReconcileResult:
    sources_inserted: int
    feeds_inserted: int


@dataclass(frozen=True)
class SampleMetrics:
    sample_count: int
    variables: tuple[str, ...]
    model_run_count: int
    latest_issued_at: str | None
    valid_from: str | None
    valid_to: str | None
    bad_sample_count: int


@dataclass(frozen=True)
class SmokeStoredCheck:
    ok: bool
    reasons: tuple[str, ...]
    metrics: SampleMetrics


def reconcile_catalog(conn: sqlite3.Connection) -> ReconcileResult:
    source_count_before = _table_count(conn, "sources")
    feed_count_before = _table_count(conn, "feeds")
    seed_default_sources(conn)
    seed_default_feeds(conn)
    return ReconcileResult(
        sources_inserted=_table_count(conn, "sources") - source_count_before,
        feeds_inserted=_table_count(conn, "feeds") - feed_count_before,
    )


def select_feeds(
    conn: sqlite3.Connection,
    *,
    sources: Sequence[str] = (),
    feed_ids: Sequence[int] = (),
    all_new: bool = False,
    all_forecast: bool = False,
) -> FeedSelection:
    if feed_ids:
        return _select_feed_ids(conn, feed_ids)
    selected_sources: tuple[str, ...]
    if all_new:
        selected_sources = NEW_PROVIDER_SOURCES
    elif sources:
        selected_sources = tuple(dict.fromkeys(sources))
    else:
        selected_sources = ()

    where = ["f.is_virtual = 0", "NOT (f.source='meteoblue' AND f.model!='multimodel')"]
    params: list[object] = []
    if all_forecast:
        pass
    elif selected_sources:
        placeholders = ", ".join("?" for _ in selected_sources)
        where.append(f"f.source IN ({placeholders})")
        params.extend(selected_sources)
    else:
        return FeedSelection(())
    rows = conn.execute(
        f"""
        SELECT f.id, f.source, f.model, f.enabled, f.is_virtual
        FROM feeds f
        WHERE {" AND ".join(where)}
        ORDER BY f.source, f.model
        """,
        params,
    ).fetchall()
    refs = tuple(_feed_ref(row) for row in rows)
    missing_sources = ()
    if selected_sources:
        found = {ref.source for ref in refs}
        missing_sources = tuple(
            f"source {source} has no selectable forecast feed"
            for source in selected_sources
            if source not in found
        )
    return FeedSelection(refs, missing_sources)


def set_site_subscription(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    *,
    enabled: bool,
    enqueue_on_enable: bool = False,
) -> SubscriptionResult:
    if conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone() is None:
        raise ProviderOpsError(404, "site not found")
    feed = conn.execute("SELECT * FROM feeds WHERE id=?", (feed_id,)).fetchone()
    if feed is None:
        raise ProviderOpsError(404, "feed not found")
    if bool(feed["is_virtual"]):
        raise ProviderOpsError(400, "virtual feeds are subscription-exempt")
    if str(feed["source"]) == "meteoblue" and str(feed["model"]) != "multimodel":
        raise ProviderOpsError(
            400, "meteoblue members resolve through the package feed"
        )
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)
        VALUES (?, ?, ?, 0)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET enabled=excluded.enabled
        """,
        (site_id, feed_id, 1 if enabled else 0),
    )
    rebuild_mean_for_site(conn, site_id)
    fetch_result = (
        enqueue_fetch_for_feed(conn, site_id, feed_id)
        if enabled and enqueue_on_enable
        else None
    )
    return SubscriptionResult(
        site_id=site_id, feed_id=feed_id, enabled=enabled, fetch=fetch_result
    )


def enqueue_fetch_for_feed(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> FetchQueueResult:
    if feed_fetch_target(conn, site_id, feed_id) is None:
        return FetchQueueResult(
            feed_id=feed_id,
            enqueued=False,
            created=False,
            job_id=None,
            reason=fetch_ineligibility_reason(conn, site_id, feed_id),
        )
    result = enqueue_if_absent(
        conn,
        "fetch_feed",
        site_id,
        f"fetch:{feed_id}",
        {"feed_id": feed_id},
    )
    return FetchQueueResult(
        feed_id=feed_id,
        enqueued=True,
        created=result.created,
        job_id=result.job_id,
    )


def fetch_ineligibility_reason(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> str:
    row = conn.execute(
        """
        SELECT s.id AS site_id, s.enabled AS site_enabled,
               f.id AS feed_id, f.enabled AS feed_enabled, f.is_virtual,
               f.source, f.model,
               COALESCE(sfs.enabled, f.default_subscribed) AS subscribed
        FROM sites s
        CROSS JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id=s.id AND sfs.feed_id=f.id
        WHERE s.id=? AND f.id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    if row is None:
        site = conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
        feed = conn.execute("SELECT 1 FROM feeds WHERE id=?", (feed_id,)).fetchone()
        if site is None:
            return "site not found"
        if feed is None:
            return "feed not found"
        return "site/feed not found"
    source = str(row["source"])
    model = str(row["model"])
    if not bool(row["site_enabled"]):
        return "site disabled"
    if not bool(row["feed_enabled"]):
        return "feed disabled"
    if bool(row["is_virtual"]):
        return "virtual feed"
    if source == "meteoblue" and model != "multimodel":
        return "meteoblue member feed"
    if not bool(row["subscribed"]):
        return "not subscribed"
    return "not eligible"


def provider_health(
    conn: sqlite3.Connection,
    *,
    site_id: int | None = None,
    sources: Sequence[str] = (),
) -> list[dict[str, object]]:
    source_filter = tuple(dict.fromkeys(sources))
    where = ["f.is_virtual = 0", "NOT (f.source='meteoblue' AND f.model!='multimodel')"]
    params: list[object] = []
    if site_id is not None:
        where.append("s.id = ?")
        params.append(site_id)
    if source_filter:
        placeholders = ", ".join("?" for _ in source_filter)
        where.append(f"f.source IN ({placeholders})")
        params.extend(source_filter)
    rows = conn.execute(
        f"""
        SELECT s.id AS site_id, s.name AS site_name, s.enabled AS site_enabled,
               f.id AS feed_id, f.source, f.model, f.enabled AS feed_enabled,
               f.default_subscribed, f.is_virtual,
               sfs.enabled AS override_enabled, sfs.last_run_at, sfs.last_error,
               sfs.error_count
        FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE {" AND ".join(where)}
        ORDER BY f.source, s.name, f.model
        """,
        params,
    ).fetchall()
    keys = key_status()
    groups: dict[str, dict[str, object]] = {}
    for row in rows:
        source = str(row["source"])
        group = groups.get(source)
        if group is None:
            group = _provider_group(conn, source, keys)
            groups[source] = group
        subscribed = bool(
            row["override_enabled"]
            if row["override_enabled"] is not None
            else row["default_subscribed"]
        )
        applicable = _feed_applicable(row)
        metrics = sample_metrics(conn, int(row["site_id"]), int(row["feed_id"]))
        last_error = None if row["last_error"] is None else str(row["last_error"])
        feed: dict[str, object] = {
            "site_id": int(row["site_id"]),
            "site_name": str(row["site_name"]),
            "feed_id": int(row["feed_id"]),
            "model": str(row["model"]),
            "feed_enabled": bool(row["feed_enabled"]),
            "site_enabled": bool(row["site_enabled"]),
            "subscribed": subscribed,
            "applicable": applicable,
            "status": _provider_status(
                site_enabled=bool(row["site_enabled"]),
                applicable=applicable,
                subscribed=subscribed,
                last_run_at=None
                if row["last_run_at"] is None
                else str(row["last_run_at"]),
                last_error=last_error,
                sample_count=metrics.sample_count,
            ),
            "last_run_at": row["last_run_at"],
            "last_error": last_error,
            "error_count": int(row["error_count"] or 0),
            "sample_count": metrics.sample_count,
            "variables": list(metrics.variables),
            "model_run_count": metrics.model_run_count,
            "latest_issued_at": metrics.latest_issued_at,
            "valid_from": metrics.valid_from,
            "valid_to": metrics.valid_to,
            "bad_sample_count": metrics.bad_sample_count,
        }
        feeds = cast(list[dict[str, object]], group["feeds"])
        feeds.append(feed)
    for source in source_filter:
        groups.setdefault(source, _provider_group(conn, source, keys))
    return [groups[source] for source in sorted(groups)]


def sample_metrics(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> SampleMetrics:
    sample_feed_ids = sample_feed_ids_for_metrics(conn, feed_id)
    placeholders = _placeholders(sample_feed_ids)
    count_row = conn.execute(
        f"""
        SELECT COUNT(*) AS sample_count,
               COUNT(DISTINCT CASE
                 WHEN TRIM(model_run_id) != '' THEN model_run_id
               END) AS model_run_count,
               MAX(issued_at) AS latest_issued_at,
               MIN(valid_at) AS valid_from,
               MAX(valid_at) AS valid_to
        FROM forecast_samples
        WHERE site_id=? AND feed_id IN ({placeholders})
        """,
        (site_id, *sample_feed_ids),
    ).fetchone()
    variable_rows = conn.execute(
        f"""
        SELECT DISTINCT variable
        FROM forecast_samples
        WHERE site_id=? AND feed_id IN ({placeholders})
        ORDER BY variable
        """,
        (site_id, *sample_feed_ids),
    ).fetchall()
    bad_count = bad_sample_count(conn, site_id, feed_id)
    if count_row is None:
        return SampleMetrics(0, (), 0, None, None, None, bad_count)
    return SampleMetrics(
        sample_count=int(count_row["sample_count"]),
        variables=tuple(str(row["variable"]) for row in variable_rows),
        model_run_count=int(count_row["model_run_count"] or 0),
        latest_issued_at=None
        if count_row["latest_issued_at"] is None
        else str(count_row["latest_issued_at"]),
        valid_from=None
        if count_row["valid_from"] is None
        else str(count_row["valid_from"]),
        valid_to=None if count_row["valid_to"] is None else str(count_row["valid_to"]),
        bad_sample_count=bad_count,
    )


def bad_sample_count(conn: sqlite3.Connection, site_id: int, feed_id: int) -> int:
    sample_feed_ids = sample_feed_ids_for_metrics(conn, feed_id)
    placeholders = _placeholders(sample_feed_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM forecast_samples fs
        WHERE fs.site_id=? AND fs.feed_id IN ({placeholders})
          AND {invalid_forecast_sample_sql("fs")}
        """,
        (site_id, *sample_feed_ids),
    ).fetchone()
    return 0 if row is None else int(row["n"])


def sample_feed_ids_for_metrics(
    conn: sqlite3.Connection, feed_id: int
) -> tuple[int, ...]:
    row = conn.execute(
        "SELECT source, model FROM feeds WHERE id=?", (feed_id,)
    ).fetchone()
    if row is None:
        return (feed_id,)
    source = str(row["source"])
    model = str(row["model"])
    if source == "meteoblue" and model == "multimodel":
        rows = conn.execute(
            """
            SELECT id
            FROM feeds
            WHERE source='meteoblue' AND (model!='multimodel' OR id=?)
            ORDER BY id
            """,
            (feed_id,),
        ).fetchall()
        return tuple(int(member["id"]) for member in rows) or (feed_id,)
    return (feed_id,)


def smoke_stored_sample_check(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> SmokeStoredCheck:
    metrics = sample_metrics(conn, site_id, feed_id)
    sample_feed_ids = sample_feed_ids_for_metrics(conn, feed_id)
    placeholders = _placeholders(sample_feed_ids)
    reasons: list[str] = []
    if metrics.sample_count == 0:
        reasons.append("no stored samples")
    missing_variables = tuple(
        variable for variable in FORECAST_VARIABLES if variable not in metrics.variables
    )
    if missing_variables:
        reasons.append(f"missing variables: {', '.join(missing_variables)}")
    if metrics.bad_sample_count:
        reasons.append(f"bad samples: {metrics.bad_sample_count}")
    empty_row = conn.execute(
        f"""
        SELECT
          SUM(CASE WHEN TRIM(model_run_id) = '' THEN 1 ELSE 0 END) AS empty_runs,
          SUM(CASE WHEN TRIM(source_raw) = '' THEN 1 ELSE 0 END) AS empty_raw
        FROM forecast_samples
        WHERE site_id=? AND feed_id IN ({placeholders})
        """,
        (site_id, *sample_feed_ids),
    ).fetchone()
    if empty_row is not None and int(empty_row["empty_runs"] or 0) > 0:
        reasons.append("empty model_run_id")
    if empty_row is not None and int(empty_row["empty_raw"] or 0) > 0:
        reasons.append("empty source_raw")
    state = conn.execute(
        """
        SELECT last_error
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    if state is not None and state["last_error"] is not None:
        reasons.append("last_error not cleared")
    return SmokeStoredCheck(ok=not reasons, reasons=tuple(reasons), metrics=metrics)


def rebuild_mean_for_site(conn: sqlite3.Connection, site_id: int) -> None:
    _invalidate_mean_for_site(conn, site_id)
    pair_and_score(conn, site_id)


def provider_doctor_failures(health: Sequence[dict[str, object]]) -> tuple[str, ...]:
    failures: list[str] = []
    for group in health:
        source = str(group["source"])
        if bool(group["key_required"]) and not bool(group["key_present"]):
            failures.append(f"{source}: missing key")
        feeds_obj = group["feeds"]
        if not isinstance(feeds_obj, list):
            continue
        feeds = cast(list[dict[str, object]], feeds_obj)
        for feed in feeds:
            prefix = f"{source} site={feed['site_id']} feed={feed['feed_id']}"
            if not bool(feed["subscribed"]):
                failures.append(f"{prefix}: unsubscribed")
            last_error_value = feed["last_error"]
            last_error = last_error_value if isinstance(last_error_value, str) else None
            if last_error is not None and last_error != NO_USABLE_SAMPLES_SENTINEL:
                failures.append(f"{prefix}: hard error")
            bad_sample_value = feed["bad_sample_count"]
            bad_samples = bad_sample_value if isinstance(bad_sample_value, int) else 0
            if bad_samples > 0:
                failures.append(f"{prefix}: invalid stored samples")
    return tuple(failures)


def _provider_group(
    conn: sqlite3.Connection, source: str, keys: dict[str, bool]
) -> dict[str, object]:
    source_row = conn.execute(
        """
        SELECT daily_call_limit, daily_credit_limit, billing_tz
        FROM sources
        WHERE source=?
        """,
        (source,),
    ).fetchone()
    key_required = source in SECRET_ENV and source != "weathercom"
    if source_row is None:
        budget = {
            "calls": 0,
            "credits": 0,
            "daily_call_limit": None,
            "daily_credit_limit": None,
        }
        source_seeded = False
    else:
        budget_row = conn.execute(
            """
            SELECT calls, credits
            FROM api_budget
            WHERE source=? AND billing_day=?
            """,
            (source, current_billing_day(str(source_row["billing_tz"]))),
        ).fetchone()
        budget = {
            "calls": 0 if budget_row is None else int(budget_row["calls"]),
            "credits": 0 if budget_row is None else int(budget_row["credits"]),
            "daily_call_limit": int(source_row["daily_call_limit"]),
            "daily_credit_limit": source_row["daily_credit_limit"],
        }
        source_seeded = True
    return {
        "source": source,
        "key_required": key_required,
        "key_present": bool(keys.get(source)) if key_required else True,
        "source_seeded": source_seeded,
        "budget": budget,
        "feeds": [],
    }


def _provider_status(
    *,
    site_enabled: bool,
    applicable: bool,
    subscribed: bool,
    last_run_at: str | None,
    last_error: str | None,
    sample_count: int,
) -> str:
    if not site_enabled:
        return "site disabled"
    if not applicable:
        return "disabled"
    if not subscribed:
        return "not subscribed / available"
    if last_error == NO_USABLE_SAMPLES_SENTINEL:
        return "fetched, 0 usable"
    if last_error is not None:
        return "error"
    if last_run_at is None:
        return "never run / due"
    if sample_count == 0:
        return "ran / no usable data"
    return "ok"


def _feed_applicable(row: sqlite3.Row) -> bool:
    source = str(row["source"])
    model = str(row["model"])
    return (
        bool(row["feed_enabled"])
        and not bool(row["is_virtual"])
        and not (source == "meteoblue" and model != "multimodel")
    )


def _select_feed_ids(
    conn: sqlite3.Connection, feed_ids: Sequence[int]
) -> FeedSelection:
    refs: list[FeedRef] = []
    errors: list[str] = []
    for feed_id in dict.fromkeys(feed_ids):
        row = conn.execute(
            """
            SELECT id, source, model, enabled, is_virtual
            FROM feeds
            WHERE id=?
            """,
            (feed_id,),
        ).fetchone()
        if row is None:
            errors.append(f"feed {feed_id} not found")
        else:
            refs.append(_feed_ref(row))
    return FeedSelection(tuple(refs), tuple(errors))


def _feed_ref(row: sqlite3.Row) -> FeedRef:
    return FeedRef(
        feed_id=int(row["id"]),
        source=str(row["source"]),
        model=str(row["model"]),
        enabled=bool(row["enabled"]),
        is_virtual=bool(row["is_virtual"]),
    )


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
    return 0 if row is None else int(row["n"])


def _mean_feed_id(conn: sqlite3.Connection) -> int | None:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    return None if row is None else int(row["id"])


def _invalidate_mean_for_site(conn: sqlite3.Connection, site_id: int) -> None:
    feed_id = _mean_feed_id(conn)
    if feed_id is None:
        return
    conn.execute(
        "DELETE FROM forecast_pairs WHERE site_id=? AND feed_id=?", (site_id, feed_id)
    )
    conn.execute(
        "DELETE FROM score_cache WHERE site_id=? AND feed_id=?", (site_id, feed_id)
    )


def _placeholders(values: Iterable[object]) -> str:
    count = len(tuple(values))
    if count == 0:
        raise ValueError("at least one value is required")
    return ", ".join("?" for _ in range(count))

"""wxverify CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
from typing import NoReturn, cast

import uvicorn

from wxverify import __version__, config
from wxverify.api.app import create_app
from wxverify.collection.budget import set_source_cap
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.options import load_runtime_config
from wxverify.db.connection import get_db, init_db
from wxverify.db.queue import (
    count_failed_jobs_older_than,
    enqueue_if_absent,
    purge_failed_jobs_older_than,
)
from wxverify.provider_ops import (
    NEW_PROVIDER_SOURCES,
    FeedRef,
    ProviderOpsError,
    enqueue_fetch_for_feed,
    provider_doctor_failures,
    provider_health,
    reconcile_catalog,
    select_feeds,
    set_site_subscription,
    smoke_stored_sample_check,
)
from wxverify.scoring.engine import pair_and_score
from wxverify.settings.keys import get_setting, set_setting
from wxverify.settings.service import set_rolling_window_days_sync
from wxverify.worker.feed_fetch import (
    BackoffActive,
    BudgetExhausted,
    FetchFeedNoOp,
    FetchFeedOutcome,
    FetchFeedSuccess,
    Ineligible,
    fetch_feed_once,
)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        return _serve(args)
    init_db(args.db)
    if args.command == "score":
        return _score(args)
    if args.command == "fetch":
        return _fetch(args)
    if args.command == "backfill":
        return _backfill(args)
    if args.command == "catchup":
        return _catchup(args)
    if args.command == "settings":
        return _settings(args)
    if args.command == "sources":
        return _sources(args)
    if args.command == "providers":
        return _providers(args)
    if args.command == "jobs":
        return _jobs(args)
    _die("unknown command")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wxverify")
    parser.add_argument(
        "--version", action="version", version=f"wxverify {__version__}"
    )
    parser.add_argument("--db", default=config.db_path)
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve")
    serve.add_argument("--options", default=config.options_path)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8099)
    serve.add_argument("--root-path", default="")

    fetch = sub.add_parser("fetch")
    fetch.add_argument("site_id", type=int)
    fetch.add_argument("feed_id", type=int)

    score = sub.add_parser("score")
    score.add_argument("--site-id", type=int)

    backfill = sub.add_parser("backfill")
    backfill.add_argument("site_id", type=int)

    sub.add_parser("catchup")

    settings = sub.add_parser("settings")
    settings_sub = settings.add_subparsers(dest="settings_command", required=True)
    settings_sub.add_parser("list")
    get_parser = settings_sub.add_parser("get")
    get_parser.add_argument("key")
    set_parser = settings_sub.add_parser("set")
    set_parser.add_argument("key")
    set_parser.add_argument("value")

    sources = sub.add_parser("sources")
    sources_sub = sources.add_subparsers(dest="sources_command", required=True)
    cap = sources_sub.add_parser("set-cap")
    cap.add_argument("source")
    cap.add_argument("--daily-call-limit", type=int)
    credit = cap.add_mutually_exclusive_group()
    credit.add_argument("--daily-credit-limit", type=int)
    credit.add_argument("--no-credit-limit", action="store_true")

    providers = sub.add_parser("providers")
    providers_sub = providers.add_subparsers(dest="providers_command", required=True)
    doctor = providers_sub.add_parser("doctor")
    doctor.add_argument("--site-id", type=int)
    doctor.add_argument("--source", action="append", default=[])
    doctor.add_argument("--all-new", action="store_true")
    doctor.add_argument("--json", action="store_true")

    reconcile = providers_sub.add_parser("reconcile")
    reconcile.add_argument("--json", action="store_true")

    enable = providers_sub.add_parser("enable")
    enable.add_argument("--site-id", type=int, required=True)
    _add_provider_selection_args(enable)
    enable.add_argument("--json", action="store_true")

    disable = providers_sub.add_parser("disable")
    disable.add_argument("--site-id", type=int, required=True)
    _add_provider_selection_args(disable)
    disable.add_argument("--json", action="store_true")

    provider_fetch = providers_sub.add_parser("fetch")
    provider_fetch.add_argument("--site-id", type=int, required=True)
    _add_provider_selection_args(provider_fetch)
    provider_fetch.add_argument("--run-now", action="store_true")
    provider_fetch.add_argument("--json", action="store_true")

    smoke = providers_sub.add_parser("smoke")
    smoke.add_argument("--site-id", type=int, required=True)
    _add_provider_selection_args(smoke)
    smoke.add_argument("--json", action="store_true")

    jobs = sub.add_parser("jobs")
    jobs_sub = jobs.add_subparsers(dest="jobs_command", required=True)
    purge = jobs_sub.add_parser("purge")
    purge.add_argument("--failed-older-than-hours", type=int, default=168)
    purge.add_argument("--dry-run", action="store_true")
    purge.add_argument("--json", action="store_true")
    return parser


def _add_provider_selection_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--source", action="append", default=[])
    group.add_argument("--feed-id", action="append", type=int, default=[])
    group.add_argument("--all-new", action="store_true")
    group.add_argument("--all-forecast", action="store_true")


def _serve(args: argparse.Namespace) -> int:
    config.db_path = args.db
    config.options_path = args.options
    _configure_logging()
    app = create_app(root_path=args.root_path or "")
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _configure_logging() -> None:
    level_name = load_runtime_config().log_level or "info"
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=level, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _score(args: argparse.Namespace) -> int:
    get_db().write_sync(lambda conn: pair_and_score(conn, args.site_id))
    return 0


def _fetch(args: argparse.Namespace) -> int:
    result = get_db().write_sync(
        lambda conn: enqueue_fetch_for_feed(conn, args.site_id, args.feed_id)
    )
    if result.enqueued:
        print(f"queued feed_id={args.feed_id} created={result.created}")
        return 0
    print(f"skipped feed_id={args.feed_id} reason={result.reason}")
    return 1


def _backfill(args: argparse.Namespace) -> int:
    get_db().write_sync(
        lambda conn: enqueue_if_absent(
            conn,
            "backfill_site",
            args.site_id,
            f"backfill:{args.site_id}",
            {"site_id": args.site_id},
        )
    )
    return 0


def _catchup(args: argparse.Namespace) -> int:
    get_db().write_sync(
        lambda conn: enqueue_if_absent(conn, "catchup", None, "catchup", {})
    )
    return 0


def _settings(args: argparse.Namespace) -> int:
    if args.settings_command == "list":
        rows = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT key, value FROM settings ORDER BY key"
            ).fetchall()
        )
        for row in rows:
            print(f"{row['key']}={row['value']}")
        return 0
    if args.settings_command == "get":
        value = get_db().read_sync(lambda conn: get_setting(conn, args.key))
        if value is None:
            return 1
        print(value)
        return 0
    if args.settings_command == "set":

        def _write(conn: sqlite3.Connection) -> None:
            if args.key == "rolling_window_days":
                set_rolling_window_days_sync(conn, int(args.value))
            else:
                set_setting(conn, args.key, args.value)

        get_db().write_sync(_write)
        return 0
    _die("unknown settings command")


def _sources(args: argparse.Namespace) -> int:
    if args.sources_command == "set-cap":
        get_db().write_sync(
            lambda conn: set_source_cap(
                conn,
                args.source,
                daily_call_limit=args.daily_call_limit,
                daily_credit_limit=args.daily_credit_limit,
                no_credit_limit=args.no_credit_limit,
            )
        )
        return 0
    _die("unknown sources command")


def _providers(args: argparse.Namespace) -> int:
    if args.providers_command == "doctor":
        return _providers_doctor(args)
    if args.providers_command == "reconcile":
        return _providers_reconcile(args)
    if args.providers_command == "enable":
        return _providers_subscription(args, enabled=True)
    if args.providers_command == "disable":
        return _providers_subscription(args, enabled=False)
    if args.providers_command == "fetch":
        return _providers_fetch(args)
    if args.providers_command == "smoke":
        return _providers_smoke(args)
    _die("unknown providers command")


def _jobs(args: argparse.Namespace) -> int:
    if args.jobs_command == "purge":
        return _jobs_purge(args)
    _die("unknown jobs command")


def _jobs_purge(args: argparse.Namespace) -> int:
    hours = int(args.failed_older_than_hours)
    if hours < 1:
        _die("--failed-older-than-hours must be >= 1")
    if args.dry_run:
        count = get_db().read_sync(
            lambda conn: count_failed_jobs_older_than(conn, hours)
        )
    else:
        count = get_db().write_sync(
            lambda conn: purge_failed_jobs_older_than(conn, hours)
        )
    payload = {
        "failed_older_than_hours": hours,
        "dry_run": bool(args.dry_run),
        "jobs": count,
    }
    if args.json:
        _print_json(payload)
    else:
        action = "would_purge" if args.dry_run else "purged"
        print(f"{action} failed_jobs={count} older_than_hours={hours}")
    return 0


def _providers_doctor(args: argparse.Namespace) -> int:
    sources = NEW_PROVIDER_SOURCES if args.all_new else tuple(args.source)
    health = get_db().read_sync(
        lambda conn: provider_health(conn, site_id=args.site_id, sources=tuple(sources))
    )
    failures = list(provider_doctor_failures(health))
    if sources and not health:
        failures.append("no provider feeds matched selection")
    if args.json:
        _print_json({"providers": health, "failures": failures})
    else:
        for group in health:
            key = "present" if group["key_present"] else "missing"
            seeded = "yes" if group["source_seeded"] else "no"
            print(f"{group['source']} key: {key} source_seeded: {seeded}")
            feeds_obj = group["feeds"]
            if isinstance(feeds_obj, list):
                feeds = cast(list[dict[str, object]], feeds_obj)
                for feed in feeds:
                    print(
                        "  "
                        f"site={feed['site_id']} feed={feed['feed_id']} "
                        f"model={feed['model']} status={feed['status']} "
                        f"samples={feed['sample_count']} "
                        f"bad={feed['bad_sample_count']}"
                    )
        for failure in failures:
            print(f"FAIL {failure}")
    return 1 if failures else 0


def _providers_reconcile(args: argparse.Namespace) -> int:
    result = get_db().write_sync(reconcile_catalog)
    payload = {
        "sources_inserted": result.sources_inserted,
        "feeds_inserted": result.feeds_inserted,
    }
    if args.json:
        _print_json(payload)
    else:
        print(
            "reconciled "
            f"sources_inserted={result.sources_inserted} "
            f"feeds_inserted={result.feeds_inserted}"
        )
    return 0


def _providers_subscription(args: argparse.Namespace, *, enabled: bool) -> int:
    def _write(conn: sqlite3.Connection) -> list[dict[str, object]]:
        selection = select_feeds(
            conn,
            sources=tuple(args.source),
            feed_ids=tuple(args.feed_id),
            all_new=bool(args.all_new),
            all_forecast=bool(args.all_forecast),
        )
        rows: list[dict[str, object]] = [
            {"status": "error", "message": message} for message in selection.errors
        ]
        for feed in selection.feeds:
            try:
                result = set_site_subscription(
                    conn,
                    args.site_id,
                    feed.feed_id,
                    enabled=enabled,
                    enqueue_on_enable=enabled,
                )
            except ProviderOpsError as exc:
                rows.append(
                    {
                        "feed_id": feed.feed_id,
                        "source": feed.source,
                        "model": feed.model,
                        "status": "error",
                        "message": exc.message,
                    }
                )
                continue
            fetch = result.fetch
            status = "enabled" if enabled else "disabled"
            row: dict[str, object] = {
                "feed_id": feed.feed_id,
                "source": feed.source,
                "model": feed.model,
                "status": status,
            }
            if fetch is not None:
                row["fetch_enqueued"] = fetch.enqueued
                row["fetch_created"] = fetch.created
                row["reason"] = fetch.reason
                if not fetch.enqueued:
                    row["status"] = "enabled_fetch_skipped"
            rows.append(row)
        if not selection.feeds and not selection.errors:
            rows.append({"status": "error", "message": "no feeds matched selection"})
        return rows

    rows = get_db().write_sync(_write)
    _print_rows(rows, json_output=bool(args.json))
    return 1 if _has_errors(rows) else 0


def _providers_fetch(args: argparse.Namespace) -> int:
    if args.run_now:
        return asyncio.run(_providers_run_now(args, smoke=False))

    def _write(conn: sqlite3.Connection) -> list[dict[str, object]]:
        selection = select_feeds(
            conn,
            sources=tuple(args.source),
            feed_ids=tuple(args.feed_id),
            all_new=bool(args.all_new),
            all_forecast=bool(args.all_forecast),
        )
        rows: list[dict[str, object]] = [
            {"status": "error", "message": message} for message in selection.errors
        ]
        for feed in selection.feeds:
            result = enqueue_fetch_for_feed(conn, args.site_id, feed.feed_id)
            rows.append(
                {
                    "feed_id": feed.feed_id,
                    "source": feed.source,
                    "model": feed.model,
                    "status": "queued" if result.enqueued else "skipped",
                    "created": result.created,
                    "reason": result.reason,
                }
            )
        if not selection.feeds and not selection.errors:
            rows.append({"status": "error", "message": "no feeds matched selection"})
        return rows

    rows = get_db().write_sync(_write)
    _print_rows(rows, json_output=bool(args.json))
    return 1 if _has_errors(rows) or not _has_success(rows) else 0


def _providers_smoke(args: argparse.Namespace) -> int:
    return asyncio.run(_providers_run_now(args, smoke=True))


async def _providers_run_now(args: argparse.Namespace, *, smoke: bool) -> int:
    selection = get_db().read_sync(
        lambda conn: select_feeds(
            conn,
            sources=tuple(args.source),
            feed_ids=tuple(args.feed_id),
            all_new=bool(args.all_new),
            all_forecast=bool(args.all_forecast),
        )
    )
    rows: list[dict[str, object]] = [
        {"status": "error", "message": message} for message in selection.errors
    ]
    for feed in selection.feeds:
        try:
            outcome = await fetch_feed_once(get_db(), args.site_id, feed.feed_id)
        except Exception as exc:
            rows.append(
                {
                    "feed_id": feed.feed_id,
                    "source": feed.source,
                    "model": feed.model,
                    "status": "error",
                    "message": sanitized_exception(exc),
                }
            )
            continue
        row = await _provider_outcome_row(args.site_id, feed, outcome, smoke=smoke)
        rows.append(row)
    if not selection.feeds and not selection.errors:
        rows.append({"status": "error", "message": "no feeds matched selection"})
    _print_rows(rows, json_output=bool(args.json))
    return 1 if _has_errors(rows) or (smoke and not _has_success(rows)) else 0


async def _provider_outcome_row(
    site_id: int,
    feed: FeedRef,
    outcome: FetchFeedOutcome,
    *,
    smoke: bool,
) -> dict[str, object]:
    feed_id = feed.feed_id
    source = feed.source
    model = feed.model
    base: dict[str, object] = {"feed_id": feed_id, "source": source, "model": model}
    if isinstance(outcome, FetchFeedSuccess):
        base.update(
            {
                "status": "success",
                "inserted": outcome.inserted,
                "usable": outcome.usable,
            }
        )
        if smoke:
            check = await get_db().read(
                lambda conn: smoke_stored_sample_check(conn, site_id, feed_id)
            )
            base["stored_sample_count"] = check.metrics.sample_count
            base["bad_sample_count"] = check.metrics.bad_sample_count
            if not check.ok:
                base["status"] = "failed"
                base["message"] = "; ".join(check.reasons)
        return base
    if isinstance(outcome, FetchFeedNoOp):
        base.update(
            {
                "status": "failed" if smoke else "no_op",
                "inserted": outcome.inserted,
                "usable": outcome.usable,
                "message": NO_USABLE_SAMPLES_SENTINEL,
            }
        )
        return base
    if isinstance(outcome, BudgetExhausted):
        base.update({"status": "budget_exhausted", "next_window": outcome.next_window})
        return base
    if isinstance(outcome, BackoffActive):
        base.update({"status": "backoff_active", "next_attempt": outcome.next_attempt})
        return base
    if isinstance(outcome, Ineligible):
        base.update({"status": "ineligible", "message": outcome.reason})
        return base
    base.update({"status": "unavailable", "message": outcome.error})
    return base


def _print_rows(rows: list[dict[str, object]], *, json_output: bool) -> None:
    if json_output:
        _print_json(rows)
        return
    for row in rows:
        status = row.get("status", "unknown")
        feed_id = row.get("feed_id")
        source = row.get("source")
        model = row.get("model")
        details = " ".join(
            f"{key}={value}"
            for key, value in row.items()
            if key not in {"status", "feed_id", "source", "model"} and value is not None
        )
        prefix = f"{status}"
        if feed_id is not None:
            prefix += f" feed_id={feed_id}"
        if source is not None:
            prefix += f" source={source}"
        if model is not None:
            prefix += f" model={model}"
        print(f"{prefix} {details}".rstrip())


def _has_errors(rows: list[dict[str, object]]) -> bool:
    failing = {
        "error",
        "skipped",
        "failed",
        "budget_exhausted",
        "backoff_active",
        "ineligible",
        "unavailable",
    }
    return any(str(row.get("status")) in failing for row in rows)


def _has_success(rows: list[dict[str, object]]) -> bool:
    return any(str(row.get("status")) in {"queued", "success", "no_op"} for row in rows)


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _die(message: str) -> NoReturn:
    raise SystemExit(message)


if __name__ == "__main__":
    raise SystemExit(main())

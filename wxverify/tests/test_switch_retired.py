"""Retired routes, commands and code (test IDs RE1-RE7).

Synthetic data only: no real station id, coordinate, site or zone name.
"""

from __future__ import annotations

import ast
import asyncio
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_make_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, init_db
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.obs.pws_adapter import PwsStation

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "wxverify"


# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_station_lifecycle_routes.py, tests/test_cutover.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path, name: str) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / f"{name}.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    csrf = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": csrf}


def _jobs(conn: sqlite3.Connection) -> list[tuple[str, str | None]]:
    return [
        (str(r[0]), r[1])
        for r in conn.execute("SELECT type, job_key FROM jobs").fetchall()
    ]


# ---------------------------------------------------------------------------
# RE1: POST /api/catchup, POST /api/sites/{id}/backfill -- 410, key absent
# then set.
# ---------------------------------------------------------------------------


def test_re1_catchup_and_backfill_routes_answer_410_whatever_the_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.site_days import RETIRED_CONTROL_MESSAGE

    _init_tmp_db(tmp_path, "re1")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # The lifespan closes and reopens the shared database on startup, so
        # any connection captured before `with TestClient(...)` is stale here
        # -- reach the database through a connection opened after startup.
        from wxverify.db.connection import get_db

        site_id = get_db().write_sync(lambda conn: asof_make_site(conn, "re1-site"))

        headers = _csrf_headers(client)

        for key_state in ("absent", "set"):
            if key_state == "set":
                get_db().write_sync(
                    lambda conn: set_runtime_state(
                        conn, DAILY_CLOSE_ON_KEY, "2025-01-01T00:00:00Z"
                    )
                )

            resp = client.post("/api/catchup", headers=headers)
            assert resp.status_code == 410, (
                f"M39 -> at this assertion (key {key_state}): correct = 410, "
                f"mutant = {resp.status_code} (POST /api/catchup still enqueues)"
            )
            assert resp.json() == {"error": RETIRED_CONTROL_MESSAGE}, (
                f"M39 -> at this assertion (key {key_state}): correct = "
                f"{{'error': RETIRED_CONTROL_MESSAGE}}, mutant = {resp.json()!r}"
            )

            resp = client.post(f"/api/sites/{site_id}/backfill", headers=headers)
            assert resp.status_code == 410, (
                f"M39 -> at this assertion (key {key_state}, known site): "
                f"correct = 410, mutant = {resp.status_code}"
            )
            assert resp.json() == {"error": RETIRED_CONTROL_MESSAGE}

            resp = client.post("/api/sites/999/backfill", headers=headers)
            assert resp.status_code == 410, (
                f"M40 -> at this assertion (key {key_state}, unknown site): "
                f"correct = 410 (the site lookup never runs before the "
                f"retirement check), mutant = {resp.status_code} (looked the "
                f"site up first, so an unknown site gets 404 instead)"
            )
            assert resp.json() == {"error": RETIRED_CONTROL_MESSAGE}

        jobs = get_db().read_sync(_jobs)
        assert jobs == [], (
            "the file must be unchanged: no 'catchup' or 'backfill_site' row "
            f"from any of the calls above, found {jobs!r}"
        )


# ---------------------------------------------------------------------------
# RE2: the CLI commands answer with the message and exit 1, before the
# database file is opened.
# ---------------------------------------------------------------------------


def test_re2_cli_backfill_catchup_timezone_correct_and_status_exit_1(
    tmp_path: Path,
) -> None:
    from wxverify.__main__ import main

    for argv in (
        ["backfill", "1"],
        ["catchup"],
        ["timezone", "correct", "--site-id", "1", "--timezone", "UTC"],
        ["timezone", "status"],
    ):
        db_path = tmp_path / f"re2-{argv[0]}-{argv[-1]}.db"
        assert not db_path.exists()

        with pytest.raises(SystemExit) as exc_info:
            main(["--db", str(db_path), *argv])

        assert exc_info.value.code is not None, (
            f"M41 -> at this assertion ({argv}): correct = a SystemExit "
            f"carrying the retirement message, mutant = {exc_info.value.code!r}"
        )
        assert not db_path.exists(), (
            f"M41 -> at this assertion ({argv}): correct = no file at path "
            f"(the retirement check runs before init_db), mutant = the file "
            f"was created (init_db ran first)"
        )

    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.suppress(SystemExit), contextlib.redirect_stdout(buf):
        main(["--help"])
    text = buf.getvalue()
    for name in ("backfill", "catchup", "timezone"):
        assert name in text, (
            f"M42 -> at this assertion: correct = '{name}' listed in --help "
            f"(the subparsers stay so a script calling them gets the message, "
            f"not a usage error), mutant = '{name}' missing from: {text!r}"
        )


# ---------------------------------------------------------------------------
# RE3: create_station no longer enqueues fetch_obs, key absent then set.
# ---------------------------------------------------------------------------


async def _fake_validate_station(station_id: str, api_key: str) -> PwsStation:
    return PwsStation(station_id=station_id, lat=40.1, lon=-104.9)


async def _fake_lookup_elevation_m(lat: float, lon: float) -> float:
    return 910.0


def _re3_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> Any:
    close_db()
    config.db_path = str(tmp_path / f"{name}.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", _fake_validate_station
    )
    monkeypatch.setattr(
        "wxverify.api.routes.stations.lookup_elevation_m", _fake_lookup_elevation_m
    )
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def test_re3_create_station_enqueues_nothing_key_absent_then_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for i, key_state in enumerate(("absent", "set")):
        app = _re3_app(tmp_path, monkeypatch, f"re3-{key_state}")
        with TestClient(app) as client:
            headers = _csrf_headers(client)
            site_id = client.post(
                "/api/sites",
                json={
                    "name": f"re3-site-{i}",
                    "forecast_lat": 40.0,
                    "forecast_lon": -105.0,
                    "elevation_m": 900.0,
                    "timezone": "UTC",
                },
                headers=headers,
            ).json()["id"]

            from wxverify.db.connection import get_db
            from wxverify.db.runtime_state import set_runtime_state as _set

            if key_state == "set":
                get_db().write_sync(
                    lambda conn: _set(conn, DAILY_CLOSE_ON_KEY, "2025-01-01T00:00:00Z")
                )

            resp = client.post(
                f"/api/sites/{site_id}/stations",
                json={"pws_station_id": f"ISTATION0{i + 1}"},
                headers=headers,
            )
            assert resp.status_code == 200, resp.text

            def _site_jobs(conn: sqlite3.Connection, sid: int = site_id) -> list[str]:
                return [
                    str(r[0])
                    for r in conn.execute(
                        "SELECT type FROM jobs WHERE site_id = ?", (sid,)
                    ).fetchall()
                ]

            jobs = get_db().read_sync(_site_jobs)
            assert jobs == [], (
                f"M43 -> at this assertion (key {key_state}): correct = no "
                f"job row (create_station's fetch_obs enqueue is removed), "
                f"mutant = {jobs!r}"
            )


# ---------------------------------------------------------------------------
# RE4: the retired controls' markup is gone from the templates and app.js.
# ---------------------------------------------------------------------------


def test_re4_retired_controls_markup_removed_from_templates_and_app_js() -> None:
    templates = ROOT / "wxverify" / "web" / "templates"
    app_js = (ROOT / "wxverify" / "web" / "static" / "app.js").read_text(
        encoding="utf-8"
    )

    backfill_html = (templates / "ops" / "_backfill.html").read_text(encoding="utf-8")
    wind_history_html = (templates / "ops" / "_wind_history.html").read_text(
        encoding="utf-8"
    )
    ops_show_html = (templates / "ops" / "show.html").read_text(encoding="utf-8")
    dashboard_show_html = (templates / "dashboard" / "show.html").read_text(
        encoding="utf-8"
    )

    assert "/api/catchup" not in backfill_html, (
        "M44 -> at this assertion: correct = no hx-post to /api/catchup in "
        "_backfill.html, mutant = the Catch up button left in"
    )
    assert "/backfill" not in backfill_html, (
        "M44 -> at this assertion: correct = no hx-post to a site's /backfill "
        "in _backfill.html, mutant = the per-site Backfill button left in"
    )
    assert "data-wind-auth-hold-url" not in wind_history_html, (
        "the wind-history auth-hold block must be gone from _wind_history.html"
    )
    assert "data-wind-auth-hold-url" not in app_js, (
        'M45 -> at this assertion: correct = no "Try again" handler in '
        "app.js, mutant = the handler left in"
    )
    assert "_timezone_correction.html" not in ops_show_html, (
        "the time-zone correction panel include must be gone from ops/show.html"
    )
    assert "_wind_banner.html" not in dashboard_show_html, (
        "the dashboard wind banner include must be gone from dashboard/show.html"
    )


# ---------------------------------------------------------------------------
# RE5: whole-word search of the package for every retired name.
# ---------------------------------------------------------------------------

_RE5_NAMES = [
    "_enqueue_due_obs",
    "_fetch_obs",
    "_reserve_obs_call",
    "_retention_hours",
    "_HISTORY_RETENTION_MAX_HOURS",
    "_persist_station_observations",
    "_complete_obs_cycle",
    "_park_station_history",
    "_history_parked",
    "_reconcile",
    "_finish_waiting_days",
    "_finish_day",
    "_derive_days",
    "_invalidate_unparseable",
    "fetch_history_all",
    "HISTORY_ALL_PATH",
    "AUTH_HOLD_ENDPOINTS",
    "AuthHoldEndpoint",
    "AuthHoldStatus",
    "AuthHold",
    "auth_hold_key",
    "read_auth_hold",
    "write_auth_hold",
    "read_auth_holds",
    "clear_auth_hold",
    "_auth_refused",
    "_probe_failed",
    "WindAuthHoldIn",
    "retry_wind_auth_hold",
    "WIND_HOLD_TITLES",
    "WindHoldView",
    "_wind_holds",
    "load_wind_banner",
    "WindBanner",
    "count_missed_final",
    "WIND_FINAL_LEAD",
    "run_catchup",
    "run_backfill_site",
    "fetch_station_history_window",
    "_fetch_missing_station_history",
    "_fetch_historical_forecasts",
    "_fetch_due_open_meteo",
    "supports_historical",
    "fetch_historical",
    "estimate_historical_cost",
    "fetch_hourly_history_range",
    "_enqueue_due_verification_runs",
    "_enqueue_due_score_refresh",
    "run_verification_chunk",
    "decide_admission",
    "materialize_missing_truth_days",
    "start_retrospective_correction",
    "apply_prospective_change",
    "published_generation_clause",
    "ensure_published_generation",
    "repair_published_pointers",
    "load_timezone_correction",
    "TimezoneCorrectionIn",
    "verification_state_key",
    "verification_heartbeat_key",
    "any_verification_chain_active",
    "_neutralize_imported_verification_chains",
    "IMPORT_SUPPRESSED_REASON",
    "_VERIFICATION_PROGRESS_SQL",
    "_stuck_verification_chunks",
]

_RE5_KEPT = [
    "fetch_hourly_history",
    "fetch_hourly_history_payload",
    "fetch_all_1day",
]

_RE5_REMOVED_MODULES = [
    PACKAGE_ROOT / "worker" / "catchup.py",
    PACKAGE_ROOT / "worker" / "backfill.py",
    PACKAGE_ROOT / "worker" / "verification_run.py",
    PACKAGE_ROOT / "worker" / "tz_correction.py",
    PACKAGE_ROOT / "scoring" / "tz_rebuild.py",
    PACKAGE_ROOT / "db" / "tz_generations.py",
]


def _iter_source_files() -> list[Path]:
    exts = ("*.py", "*.html", "*.js")
    files: list[Path] = []
    for ext in exts:
        files.extend(p for p in PACKAGE_ROOT.rglob(ext) if "__pycache__" not in p.parts)
    return sorted(files)


def test_re5_whole_word_search_finds_none_of_the_retired_names() -> None:
    files = _iter_source_files()
    texts = {p: p.read_text(encoding="utf-8", errors="replace") for p in files}

    hits: dict[str, list[str]] = {}
    for name in _RE5_NAMES:
        pattern = re.compile(r"(?<![\w])" + re.escape(name) + r"(?![\w])")
        found = [
            str(p.relative_to(PACKAGE_ROOT))
            for p, text in texts.items()
            if pattern.search(text)
        ]
        if found:
            hits[name] = found

    assert "_retention_hours" not in hits, (
        f"M46 -> at this assertion: correct = `_retention_hours` not defined "
        f"anywhere, mutant = still defined/used in {hits.get('_retention_hours')!r}"
    )
    assert not hits, (
        f"none of RE5's retired names may remain in the package; found {hits!r}"
    )

    for module in _RE5_REMOVED_MODULES:
        assert not module.exists(), (
            f"M47 -> at this assertion: correct = {module.relative_to(PACKAGE_ROOT)} "
            f"does not exist, mutant = `worker/catchup.py` (or a sibling "
            f"retired module) left in: {module} exists"
        )

    for path in files:
        if path.suffix == ".py":
            assert "/history/hourly" not in texts[path]
            assert "/history/all" not in texts[path]

    assert any(name in texts[p] for p in files for name in _RE5_KEPT), (
        "expected at least one of the kept names "
        f"{_RE5_KEPT} to still be found -- an empty result means this scan "
        "found nothing to check, not that the rule holds"
    )


def _imports_verification_run(path: Path) -> bool:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return False
    target = "wxverify.worker.verification_run"
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == target for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom) and node.module == target:
            return True
    return False


def test_re5_no_file_imports_verification_run_module_or_function() -> None:
    py_files = [p for p in PACKAGE_ROOT.rglob("*.py") if "__pycache__" not in p.parts]
    offenders = [p for p in py_files if _imports_verification_run(p)]
    offender_names = sorted(str(p.relative_to(PACKAGE_ROOT)) for p in offenders)
    assert not offenders, (
        "no file may import `wxverify.worker.verification_run`, at module "
        f"level or inside a function body; found {offender_names!r}"
    )


# ---------------------------------------------------------------------------
# RE6: ast scan of every enqueue_if_absent / enqueue_if_absent_with_cooldown
# call -- each job type is a literal of a kept kind.
# ---------------------------------------------------------------------------

_KEPT_KINDS = {
    "fetch_feed",
    "fetch_current_obs",
    "fetch_obs",
    "day_close",
    "day_publish",
}
# pair_and_score: the kind and its close-off enqueues stay in 0.16.14:
# enqueue_scoring_run, enqueue_score_rescore.
_RE6_ALLOWED_KINDS = _KEPT_KINDS | {"pair_and_score"}
# RE6 names its one forwarder by file, enclosing function and argument,
# never by line. Compared as a sorted list, so a second non-literal call in
# the same function still fails.
_RE6_KNOWN_FORWARDERS = sorted(
    [
        ("db/queue.py", "enqueue_if_absent_with_cooldown", "job_type"),
    ]
)
_WIND_DAYS_KEY_FORMS = {"'wind-days'", "WIND_DAYS_JOB_KEY"}
# backfill_site is retired too, and reused the same way: the Open-Meteo
# archive fill (worker/om_archive_fill.py, enqueue_om_archive) keys it
# 'om-archive'; any other key is an offender, exactly as fetch_obs/wind-days.
_OM_ARCHIVE_KEY_FORMS = {"'om-archive'", "OM_ARCHIVE_JOB_KEY"}
_INTO_JOBS = re.compile(r"INTO\s+jobs\b")

# keep _literal_str and _call_target_name as they are


def _literal_str(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _call_target_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _calls_with_function(tree: ast.AST) -> list[tuple[ast.Call, str]]:
    """Every call, with the name of its innermost enclosing function."""
    found: list[tuple[ast.Call, str]] = []

    def visit(node: ast.AST, func: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = (
                child.name
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                else func
            )
            if isinstance(child, ast.Call):
                found.append((child, func))
            visit(child, inner)

    visit(tree, "<module>")
    return found


def _arg(node: ast.Call, position: int, keyword: str) -> ast.expr | None:
    if len(node.args) > position:
        return node.args[position]
    return next((kw.value for kw in node.keywords if kw.arg == keyword), None)


def test_re6_every_enqueue_and_continuation_names_a_kept_kind() -> None:
    offenders: list[str] = []
    forwarders: list[tuple[str, str, str]] = []
    fetch_obs_sites: list[tuple[str, str]] = []
    om_archive_sites: list[tuple[str, str]] = []
    into_jobs: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(PACKAGE_ROOT).as_posix()
        source = path.read_text(encoding="utf-8")
        if _INTO_JOBS.search(source):
            into_jobs.append(rel)
        for node, func in _calls_with_function(ast.parse(source, filename=str(path))):
            name = _call_target_name(node)
            if name in ("enqueue_if_absent", "enqueue_if_absent_with_cooldown"):
                type_arg, key_arg = _arg(node, 1, "job_type"), _arg(node, 3, "job_key")
            else:
                continue
            where = f"{rel}:{node.lineno} ({func}) {name}()"
            literal = _literal_str(type_arg)
            if literal is None:
                shown = None if type_arg is None else ast.unparse(type_arg)
                if shown is not None:
                    forwarders.append((rel, func, shown))
                else:
                    offenders.append(f"{where}: non-literal type {shown!r}")
                continue
            # backfill_site is retired, like fetch_obs's old routine download,
            # but the Open-Meteo archive fill reuses it keyed 'om-archive' --
            # so it is allowed ONLY under that key, never just by its kind.
            if literal not in _RE6_ALLOWED_KINDS and literal != "backfill_site":
                offenders.append(f"{where}: retired kind {literal!r}")
            if literal == "fetch_obs":
                fetch_obs_sites.append((rel, func))
                key = None if key_arg is None else ast.unparse(key_arg)
                if key not in _WIND_DAYS_KEY_FORMS:
                    offenders.append(
                        f"{where}: fetch_obs keyed {key!r}, not the wind lane's"
                    )
            if literal == "backfill_site":
                om_archive_sites.append((rel, func))
                key = None if key_arg is None else ast.unparse(key_arg)
                if key not in _OM_ARCHIVE_KEY_FORMS:
                    offenders.append(
                        f"{where}: backfill_site keyed {key!r}, not om-archive's"
                    )
    assert not offenders, (
        "M48 -> at this assertion: correct = every enqueue/continuation names a"
        " kept kind, pair_and_score, or backfill_site keyed om-archive, and"
        " every fetch_obs enqueue the wind lane's key, mutant ="
        f" {offenders!r} (e.g. `_enqueue_due_obs` kept under another name"
        " still queues fetch_obs keyed 'obs')"
    )
    assert sorted(forwarders) == _RE6_KNOWN_FORWARDERS, (
        "the non-literal job types must be exactly RE6's two forwarders --"
        f" correct = {_RE6_KNOWN_FORWARDERS!r}, found = {sorted(forwarders)!r}"
    )
    assert fetch_obs_sites, (
        "the scan must reach the wind lane's fetch_obs enqueue"
        " (worker/scheduler.py, _enqueue_due_wind_days); none found"
    )
    assert om_archive_sites, (
        "the scan must reach the Open-Meteo archive fill's enqueue"
        " (worker/om_archive_fill.py, enqueue_om_archive); none found"
    )
    assert into_jobs == ["db/queue.py"], (
        f"INTO jobs must appear only in db/queue.py; found {into_jobs!r}"
    )


# ---------------------------------------------------------------------------
# RE7: the two removed routes answer 404.
# ---------------------------------------------------------------------------


def test_re7_auth_hold_and_timezone_correction_routes_are_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path, "re7")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # The lifespan closes and reopens the shared database on startup, so
        # any connection captured before `with TestClient(...)` is stale here
        # -- reach the database through a connection opened after startup.
        from wxverify.db.connection import get_db

        site_id = get_db().write_sync(lambda conn: asof_make_site(conn, "re7-site"))

        headers = _csrf_headers(client)

        resp = client.put(
            "/api/wind-history/auth-hold",
            json={"endpoint": "history_all", "confirm": True},
            headers=headers,
        )
        assert resp.status_code == 404, (
            f"M49 -> at this assertion: correct = 404 (the auth-hold route "
            f"is removed, not just suppressed), mutant = {resp.status_code} "
            "(the route kept)"
        )

        resp = client.post(
            f"/api/sites/{site_id}/timezone-correction",
            json={},
            headers=headers,
        )
        assert resp.status_code == 404, (
            f"M49 -> at this assertion: correct = 404, mutant = "
            f"{resp.status_code} (the time-zone correction route kept)"
        )

        jobs = get_db().read_sync(_jobs)
        assert jobs == [], "the file must be unchanged by either call"

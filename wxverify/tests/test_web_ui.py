"""Behavioral suite for the wxverify web-UI overhaul (plan 2026-07-17 §10).

Covers the pure verdict logic, the presentation-layer leaderboard sort, the
rendered verdict/leaderboard DOM states, the neutral-copy metric-boundary
regressions, ``LEAD_OPTIONS`` round-tripping, the ``/api/curve`` payload
contract, the unknown-variable graceful fallback, render smoke (standalone +
Ingress with the active-nav oracle), and the two empty-state oracles.

Isolation: every test builds its own tmp DB via ``_init_tmp_db`` (mirrors the
``tests/test_static_ingress.py`` harness) and, for HTTP tests, an idle-worker
app + ``TestClient``. State is controlled by seeding the ``score_cache`` table
directly under the ``w:all`` window (``is_cache_fresh`` is unconditional for
``w:all`` so skill values are exact and deterministic), except the two
metric-boundary tests which seed real ``forecast_pairs`` and exercise the live
metric strategies so the boundary (skill None) is produced by production code,
not asserted around.

Synthetic fixtures only — fake site names/coords, no real keys or station IDs.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.lead import parse_day_ahead
from wxverify.db.connection import close_db, init_db
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import set_setting
from wxverify.web.context import (
    LEAD_OPTIONS,
    VARIABLE_LABELS,
    LeaderboardItem,
    Verdict,
    compute_verdict,
    feed_label,
    load_dashboard,
    variable_label_for,
)

# ---------------------------------------------------------------------------
# Synthetic Ingress constants (RFC-5737 range for the non-Supervisor client).
# ---------------------------------------------------------------------------
_INGRESS_TOKEN = "abc123synthetic"
_INGRESS_PREFIX = f"/api/hassio_ingress/{_INGRESS_TOKEN}"
_SUPERVISOR_IP = "172.30.32.2"

_COMPUTED_AT = "2035-01-02T00:00:00Z"


# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_static_ingress.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    """Drop-in run_worker shim that idles without touching the scheduler."""
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    # Write a REAL (empty-object) options file, not a missing path. When the
    # file is absent, the lifespan's ``load_runtime_options`` falls back to
    # ``_from_env`` and inherits ambient ``WXV_*`` vars (the dev shell exports
    # ``WXV_MIN_N=30``), and ``apply_plain_settings`` then clobbers any DB-seeded
    # ``min_n`` on startup — a false-negative that renders every seeded confident
    # row as withheld. An empty object routes options through the file loader
    # (env ignored) with ``min_n``/``rolling_window_days`` = ``None``, so
    # ``apply_plain_settings`` leaves the test's explicit DB settings untouched.
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Return a fully-configured FastAPI app with an idle worker.

    The tmp DB must already be initialised (via ``_init_tmp_db``) so the app's
    ``get_db()`` opens the same file and reads the committed seed rows.
    """
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


# ---------------------------------------------------------------------------
# Seeding helpers.
# ---------------------------------------------------------------------------


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 47.0, 25.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, model: str, source: str = "open-meteo") -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
        ).fetchone()["id"]
    )


def _seed_cell(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    skill: float | None,
    n: int,
) -> None:
    """Seed one leaderboard cell: a forecast_pairs anchor + a score_cache row.

    The forecast_pairs anchor establishes the "expected active feed" set the
    cached-leaderboard path validates against; the score_cache row (window_key
    ``w:all`` -> always fresh) supplies the exact ``skill``/``n`` so the cache
    path — not the live strategies — serves the read, giving deterministic
    control of eligibility and skill.
    """
    valid_at = f"2035-01-{2 + day_ahead:02d}T00:00:00Z"
    lead_hours = 24 * (day_ahead + 1)
    # Real error/category values so the live composite/win-rate panels (which
    # aggregate forecast_pairs directly, independent of score_cache) don't hit
    # a NULL AVG — the leaderboard skill still comes from the cache row below.
    conn.execute(
        """
        INSERT OR IGNORE INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg)
        VALUES (?, ?, ?, '2035-01-01T00:00:00Z', ?, ?, ?, 12.0, 10.0,
                2.0, 2.0, 4.0, 1, 0, 0, 0)
        """,
        (site_id, feed_id, variable, valid_at, lead_hours, day_ahead),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, skill_score,
             computed_at)
        VALUES (?, ?, ?, ?, 'w:all', ?, ?, ?)
        """,
        (site_id, feed_id, variable, day_ahead, n, skill, _COMPUTED_AT),
    )


# ---------------------------------------------------------------------------
# LeaderboardItem factory for the pure compute_verdict unit tests.
# ---------------------------------------------------------------------------


def _item(
    *,
    feed_id: int,
    label: str,
    skill: float | None,
    confident: bool,
    n: int = 100,
) -> LeaderboardItem:
    return LeaderboardItem(
        feed_id=feed_id,
        label=label,
        n=n,
        skill_score=skill,
        badge=None if skill is None else round(max(0.0, skill) * 100),
        below_baseline=skill is not None and skill < 0,
        confident=confident,
        bias=None,
        mae=None,
        rmse=None,
    )


# ===========================================================================
# §10.1 — compute_verdict unit (states + counterexamples)
# ===========================================================================


def test_compute_verdict_empty() -> None:
    verdict = compute_verdict([])
    assert verdict == Verdict(state="empty", winner=None, runner_up=None, margin=None)


def test_compute_verdict_insufficient_when_rows_present_but_none_eligible() -> None:
    items = [
        _item(feed_id=1, label="a", skill=0.9, confident=False),
        _item(feed_id=2, label="b", skill=0.8, confident=False),
    ]
    verdict = compute_verdict(items)
    assert verdict.state == "insufficient"
    assert verdict.winner is None
    assert verdict.runner_up is None


def test_compute_verdict_tie_within_epsilon() -> None:
    items = [
        _item(feed_id=1, label="a", skill=0.50, confident=True),
        _item(feed_id=2, label="b", skill=0.495, confident=True),
    ]
    verdict = compute_verdict(items, tie_epsilon=0.01)
    assert verdict.state == "tie"
    assert verdict.winner is not None
    assert verdict.runner_up is not None
    assert verdict.margin is not None
    assert verdict.margin <= 0.01


def test_compute_verdict_tie_at_exact_epsilon_boundary() -> None:
    # margin == tie_epsilon must be a tie (gate is `<=`). Use float-exact
    # values (1.0 - 0.0 == 1.0 exactly) so the boundary is unambiguous.
    items = [
        _item(feed_id=1, label="a", skill=1.0, confident=True),
        _item(feed_id=2, label="b", skill=0.0, confident=True),
    ]
    assert compute_verdict(items, tie_epsilon=1.0).state == "tie"
    # Just over the epsilon -> ok, not tie.
    assert compute_verdict(items, tie_epsilon=0.999).state == "ok"


def test_compute_verdict_ok_beyond_epsilon() -> None:
    items = [
        _item(feed_id=1, label="winner", skill=0.60, confident=True),
        _item(feed_id=2, label="second", skill=0.30, confident=True),
    ]
    verdict = compute_verdict(items, tie_epsilon=0.01)
    assert verdict.state == "ok"
    assert verdict.winner is not None
    assert verdict.winner.feed_id == 1
    assert verdict.runner_up is not None
    assert verdict.runner_up.feed_id == 2


def test_compute_verdict_high_numeric_skill_but_not_eligible_never_wins() -> None:
    # Counterexample: a non-confident row with the highest numeric skill must
    # not become the winner; the confident (lower) row wins instead.
    items = [
        _item(feed_id=99, label="loud", skill=0.99, confident=False),
        _item(feed_id=1, label="quiet", skill=0.20, confident=True),
    ]
    verdict = compute_verdict(items)
    assert verdict.state == "ok"
    assert verdict.winner is not None
    assert verdict.winner.feed_id == 1
    assert verdict.runner_up is None


def test_compute_verdict_single_eligible_candidate_ok_no_runner_up() -> None:
    items = [
        _item(feed_id=1, label="only", skill=0.40, confident=True),
        _item(feed_id=2, label="withheld", skill=None, confident=False),
    ]
    verdict = compute_verdict(items)
    assert verdict.state == "ok"
    assert verdict.winner is not None
    assert verdict.winner.feed_id == 1
    assert verdict.runner_up is None
    assert verdict.margin is None


def test_compute_verdict_high_n_but_skill_none_is_withheld_not_winner() -> None:
    # n >= min_n but skill None -> confident False -> withheld, not rankable.
    items = [_item(feed_id=1, label="a", skill=None, confident=False, n=500)]
    verdict = compute_verdict(items)
    assert verdict.state == "insufficient"
    assert verdict.winner is None


# ===========================================================================
# §10.2 — Leaderboard sort (presentation layer, via load_dashboard)
# ===========================================================================


def test_leaderboard_sort_eligible_first_then_withheld_high_skill_last(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Sort Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gem = _feed_id(conn, "gem_global")
    gfs = _feed_id(conn, "gfs_global")
    icon = _feed_id(conn, "icon_global")
    # Two eligible rows share skill 0.5 -> label breaks the tie (ecmwf < gem).
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.50,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gem,
        variable="temperature",
        day_ahead=1,
        skill=0.50,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.30,
        n=5,
    )
    # Withheld (n < min_n) yet carries the highest numeric skill 0.99 -> must
    # still sort BELOW every eligible row.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=icon,
        variable="temperature",
        day_ahead=1,
        skill=0.99,
        n=1,
    )
    conn.commit()

    ctx = load_dashboard(
        conn, site_id=site_id, variable="temperature", window="all", lead="D+1"
    )
    order = [row.feed_id for row in ctx["leaderboard"]]  # type: ignore[union-attr]
    assert order == [ecmwf, gem, gfs, icon]


# ===========================================================================
# §10.3 — Rendered verdict / leaderboard DOM states
# ===========================================================================


def _get_dashboard(
    client: TestClient,
    site_id: int,
    *,
    variable: str = "temperature",
    lead: str = "D+1",
) -> str:
    # Pass params via the mapping so httpx percent-encodes the lead: a literal
    # ``+`` in a raw query string decodes to a SPACE server-side (``D+2`` ->
    # ``"D 2"``), silently mis-resolving the lead day. The default ``D+1`` only
    # survives that mangling by coincidence.
    resp = client.get(
        "/dashboard",
        params={
            "site": str(site_id),
            "variable": variable,
            "window": "all",
            "lead": lead,
        },
    )
    assert resp.status_code == 200
    return resp.text


def test_render_insufficient_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "5")
    site_id = _make_site(conn, "Insufficient Site")
    # n >= min_n but skill None -> withheld with the neutral (non-sample-count)
    # reason; the whole board is withheld.
    for model in ("ecmwf_ifs", "gfs_global"):
        _seed_cell(
            conn,
            site_id=site_id,
            feed_id=_feed_id(conn, model),
            variable="temperature",
            day_ahead=1,
            skill=None,
            n=10,
        )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "No feed has a rankable skill score yet" in html
    assert "persistence comparison unavailable for these samples" in html
    # Neutral, not the sample-count opener.
    assert "needs 5+ samples" not in html
    # All rows withheld -> rank cells are em dashes, no highlight, no rank digit.
    assert "<td>—</td>" in html
    assert "is-top" not in html
    # STRENGTHEN §10 oracle: every withheld row carries an em-dash rank — not
    # just ≥1 row.  Two feeds were seeded, so exactly 2 em-dash rank cells must
    # appear.  The rank cell is uniquely `<td>—</td>` (the skill cell for
    # withheld rows contains both `—` and a `<span>`, never the bare pattern).
    _n_withheld_feeds = 2
    _n_dash_cells = html.count("<td>—</td>")
    assert _n_dash_cells == _n_withheld_feeds, (
        f"Expected {_n_withheld_feeds} em-dash rank cells (one per withheld feed); "
        f"got {_n_dash_cells}. A row may carry a numeric rank "
        "when it should be withheld."
    )
    # No rank-column cell must carry a digit — `<td>1</td>` is rank #1.
    assert "<td>1</td>" not in html, (
        "Rank cell '<td>1</td>' found — a withheld row was incorrectly assigned "
        "a numeric rank."
    )


def test_render_tie_state_both_rows_highlighted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Tie Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.500,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.495,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-tie" in html
    assert "Too close to call" in html
    # BOTH named candidates carry the highlight — never exactly one.
    assert html.count('class="is-top"') == 2


def test_render_below_baseline_ok_uses_signed_not_clamped_figures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Below Baseline Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # Two confident feeds, both negative, separated beyond tie_epsilon -> ok.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=-0.10,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=-0.50,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-ok" in html
    # Signed raw percentages, never the badge-clamped "0 vs 0".
    assert "-10 vs -50" in html
    assert "0 vs 0" not in html
    # State-independent at-or-below-baseline caveat renders.
    assert "beats a naive 'same as yesterday' forecast" in html


def test_render_negative_tie_still_renders_baseline_caveat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Negative Tie Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # Both below baseline, within epsilon -> tie AND caveat.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=-0.300,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=-0.305,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-tie" in html
    caveat = "beats a naive 'same as yesterday' forecast"
    assert caveat in html
    # Caveat renders AFTER the tie body (state-independent gate must not be
    # suppressed by the tie branch).
    assert html.index("Too close to call") < html.index(caveat)


def test_render_exactly_zero_best_ok_renders_caveat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Zero Best OK Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # Best eligible skill is exactly 0 (ties persistence); second clearly lower
    # -> ok. below_baseline is False (strict <0) but the `<= 0` gate fires.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.0,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=-0.30,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-ok" in html
    assert "beats a naive 'same as yesterday' forecast" in html


def test_render_exactly_zero_best_tie_renders_caveat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Zero Best Tie Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # Two feeds at ~0 within epsilon -> tie; winner skill 0.0 <= 0 -> caveat.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.0,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=-0.005,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-tie" in html
    assert "beats a naive 'same as yesterday' forecast" in html


def test_render_precip_d2_ok_uses_display_labels_and_callable_global(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Precip Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="precip",
        day_ahead=2,
        skill=0.60,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="precip",
        day_ahead=2,
        skill=0.30,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id, variable="precip", lead="D+2")
    # Verdict head: display label + lead word, no raw identifier, no possessive.
    assert "Best Precipitation forecast for +2 days" in html
    assert "precip's" not in html
    assert "Precipitation's" not in html
    # Toolbar rendered "Precipitation" via the callable variable_label_for
    # global — a 200 here proves the global is not shadowed by a same-named
    # context string (shadowing would 500 on calling a str).
    # Variable label must not be followed by a <small> lead-code tag.
    assert ">Precipitation <small" not in html
    assert html.count("Precipitation") >= 2


def test_render_single_eligible_candidate_no_runner_up_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Oracle §10.3: one confident feed → runner-up line absent from rendered HTML.

    The precondition is injected (one confident, one withheld — n < min_n),
    not ambient.  A paired positive follows to make the negative non-vacuous.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Single Eligible Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # One confident candidate (n >= min_n, skill non-None).
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.40,
        n=5,
    )
    # Withheld: n=1 < min_n=2 → NOT a confident candidate.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.90,
        n=1,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-ok" in html
    assert feed_label("open-meteo", "ecmwf_ifs") in html
    # compute_verdict returns runner_up=None for a single eligible candidate;
    # the template's {% if verdict.runner_up is not none %} guard must suppress
    # the runner-up paragraph.
    assert "Runner-up:" not in html, (
        "Runner-up line rendered for a single-eligible verdict. "
        "compute_verdict must return runner_up=None when only one confident "
        "candidate exists, and the template must not render the paragraph."
    )


def test_render_two_eligible_candidates_runner_up_line_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the single-eligible no-runner-up oracle.

    With ≥2 confident feeds the runner-up line MUST render; without this
    positive the absence test is vacuously green if the line never rendered.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Two Eligible Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.60,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.30,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-ok" in html
    assert "Runner-up:" in html, (
        "Runner-up line absent with ≥2 eligible candidates. "
        "The template's {% if verdict.runner_up is not none %} guard may be "
        "broken, or compute_verdict returned runner_up=None incorrectly."
    )


def test_render_high_skill_withheld_row_never_gets_is_top(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Oracle §10.3: a withheld feed (high skill but n < min_n) must never
    receive the ``is-top`` leaderboard highlight.

    ``is-top`` follows the verdict winner; a withheld feed cannot be the
    winner because ``compute_verdict`` considers only confident rows.  Seed
    exactly: high-skill withheld feed + lower-skill confident winner → assert
    the confident winner has ``is-top`` and the withheld feed does not.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Withheld High Skill Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # High skill, but n=1 < min_n=2 → withheld (non-confident).
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.90,
        n=1,
    )
    # Lower skill, n=5 >= min_n=2 → confident → the only possible winner.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.20,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "verdict-ok" in html
    # Exactly one row should be highlighted — the confident winner.
    n_is_top = html.count('class="is-top"')
    assert n_is_top == 1, (
        f"Expected exactly 1 is-top row (the confident winner); got {n_is_top}. "
        "A withheld feed may be incorrectly highlighted."
    )
    # The is-top row must contain the confident winner, not the withheld feed.
    is_top_match = re.search(r'<tr class="is-top">(.*?)</tr>', html, re.DOTALL)
    assert is_top_match is not None, "is-top <tr> not found in the rendered leaderboard"
    is_top_content = is_top_match.group(1)
    winner_label = feed_label("open-meteo", "gfs_global")
    withheld_label = feed_label("open-meteo", "ecmwf_ifs")
    assert winner_label in is_top_content, (
        f"Confident winner '{winner_label}' not found in the is-top row. "
        "The wrong row may be highlighted."
    )
    assert withheld_label not in is_top_content, (
        f"Withheld feed '{withheld_label}' appears in the is-top row — "
        "a feed with n < min_n must never receive the winner highlight."
    )


# ===========================================================================
# §10.4 — Neutral-copy metric-boundary regressions (live strategies)
# ===========================================================================


def test_metric_boundary_continuous_persistence_mse_zero_is_neutral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Persistence Zero Site")
    feed = _feed_id(conn, "ecmwf_ifs")
    persistence = _feed_id(conn, "_persistence", source="virtual")
    # Persistence forecasts are perfect (sq_error 0) at every matched valid_at
    # -> persistence_mse == 0 -> _paired_skill returns None despite a baseline
    # existing. The feed itself has plenty of samples (n=3 >= min_n=2).
    for day in range(3):
        valid_at = f"2035-02-{10 + day:02d}T00:00:00Z"
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-02-01T00:00:00Z', ?, 24, 1,
                    12.0, 10.0, 2.0, 2.0, 4.0)
            """,
            (site_id, feed, valid_at),
        )
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-02-01T00:00:00Z', ?, 24, 1,
                    10.0, 10.0, 0.0, 0.0, 0.0)
            """,
            (site_id, persistence, valid_at),
        )
    conn.commit()

    # Direct strategy assertion: skill None, withheld, at n >= min_n.
    result = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=2,
    )
    assert result.n >= 2
    assert result.skill_score is None
    assert result.confident is False

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id)
    assert "persistence comparison unavailable for these samples" in html
    # Never a causal claim.
    assert "no persistence baseline" not in html
    assert "too few" not in html


def test_metric_boundary_precip_all_dry_all_correct_is_neutral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "All Dry Site")
    feed = _feed_id(conn, "ecmwf_ifs")
    # All samples dry and correctly forecast dry: h=f=m=0, cn=N -> ETS
    # denominator empty -> skill None despite ample samples.
    for day in range(4):
        valid_at = f"2035-03-{10 + day:02d}T00:00:00Z"
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg)
            VALUES (?, ?, 'precip', '2035-03-01T00:00:00Z', ?, 24, 1,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0, 1)
            """,
            (site_id, feed, valid_at),
        )
    conn.commit()

    result = strategy_for("precip").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed,
        variable="precip",
        day_ahead=1,
        window_cutoff=None,
        min_n=2,
    )
    assert result.n >= 2
    assert result.skill_score is None
    assert result.confident is False

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, site_id, variable="precip")
    assert "ETS unavailable for this sample mix" in html
    assert "too few rain" not in html
    assert "no persistence baseline" not in html


# ===========================================================================
# §10.5 — LEAD_OPTIONS
# ===========================================================================


def test_lead_options_shape_words_and_roundtrip() -> None:
    assert len(LEAD_OPTIONS) == 8
    assert LEAD_OPTIONS[0] == {"value": "D+0", "word": "Today"}
    assert LEAD_OPTIONS[1] == {"value": "D+1", "word": "Tomorrow"}
    assert LEAD_OPTIONS[2]["word"] == "+2 days"
    assert LEAD_OPTIONS[7] == {"value": "D+7", "word": "+7 days"}
    for idx, opt in enumerate(LEAD_OPTIONS):
        assert parse_day_ahead(opt["value"]) == idx


# ===========================================================================
# §10.6 — /api/curve contract
# ===========================================================================


def test_curve_empty_db_returns_empty_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/curve?site=1&window=all")
    assert resp.status_code == 200
    payload: dict[str, Any] = resp.json()
    assert payload["leads"] == [0, 1, 2, 3, 4, 5, 6, 7]
    assert payload["series"] == []


def test_curve_populated_union_universe_null_gap_order_flip_and_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Curve Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    # A (ecmwf): eligible at leads 0,1,2. B (gfs): eligible at 0,2 (MISSING 1).
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=0,
        skill=0.50,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.40,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=2,
        skill=0.20,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=0,
        skill=0.30,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=2,
        skill=0.80,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        at0: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B0"
        ).json()
        at2: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B2"
        ).json()

    by_id0 = {s["feed_id"]: s for s in at0["series"]}
    # Union universe: both feeds present even though B lacks a lead.
    assert set(by_id0) == {ecmwf, gfs}
    # B carries null at the lead index it lacks (1), values where eligible.
    assert by_id0[gfs]["skill"][1] is None
    assert by_id0[gfs]["skill"][0] == 0.30
    assert by_id0[gfs]["skill"][2] == 0.80
    # Labels come from feed_label.
    assert by_id0[ecmwf]["label"] == feed_label("open-meteo", "ecmwf_ifs")
    assert by_id0[gfs]["label"] == feed_label("open-meteo", "gfs_global")
    # Order flips with the selected lead: A wins D+0 (0.5 > 0.3), B wins D+2
    # (0.8 > 0.2).
    assert [s["feed_id"] for s in at0["series"]] == [ecmwf, gfs]
    assert [s["feed_id"] for s in at2["series"]] == [gfs, ecmwf]


def test_curve_top_clamp_at_both_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Top Clamp Site")
    models = [
        "ecmwf_ifs",
        "gfs_global",
        "icon_global",
        "gem_global",
        "meteofrance_arpege_world",
        "jma_gsm",
        "ukmo_global_deterministic_10km",
    ]
    for rank, model in enumerate(models):
        _seed_cell(
            conn,
            site_id=site_id,
            feed_id=_feed_id(conn, model),
            variable="temperature",
            day_ahead=1,
            skill=0.9 - rank * 0.1,
            n=5,
        )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        for top in (0, -1, 6, 10):
            payload: dict[str, Any] = client.get(
                f"/api/curve?site={site_id}&window=all&lead=D%2B1&top={top}"
            ).json()
            n = len(payload["series"])
            assert 1 <= n <= 6, f"top={top} produced {n} series"
        lower: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B1&top=0"
        ).json()
        assert len(lower["series"]) == 1
        upper: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B1&top=10"
        ).json()
        assert len(upper["series"]) == 6


def test_curve_lead_guard_unparseable_and_out_of_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Oracle §10.6: invalid/garbage lead → D+1 ordering; D+99 → deterministic
    nulls-last / label-ASC order distinct from D+1 skill-ranked order.

    (a) ``lead=foo`` (unparseable) coerces to D+1 via ``_coerce_lead_day``'s
        ValueError catch; the series ordering must equal an explicit
        ``lead=D+1`` request — not just be a non-empty list.
    (b) ``lead=D+99`` (out-of-range — not in ``_CURVE_LEADS`` 0-7) produces
        ``selected_index=None`` → all feeds have ``selected=None`` → sort key
        ``(1, 0.0, label)`` → alphabetical label-ASC; two identical D+99
        requests must yield identical ordering (determinism), and that order
        must differ from the D+1 skill-ranked order (oracle has teeth).

    Three feeds with distinct D+1 skills give a D+1 skill order of
    gem > ecmwf > gfs, while D+99 label-ASC gives ecmwf < gem < gfs — the
    two orderings differ, making the D+99 pin non-vacuous.

    ``params=`` is used throughout so httpx percent-encodes ``D+1`` →
    ``D%2B1``; a literal ``+`` in a raw query string decodes as a space
    server-side (``D+1`` → ``"D 1"``).
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Lead Guard Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    gfs = _feed_id(conn, "gfs_global")
    gem = _feed_id(conn, "gem_global")
    # Three feeds with distinct D+1 skills so the D+1 and D+99 orderings differ:
    #   D+1 skill order:  gem (0.70) > ecmwf (0.50) > gfs (0.30) → [gem, ecmwf, gfs]
    #   D+99 label order: ecmwf < gem < gfs (all "open-meteo / …") → [ecmwf, gem, gfs]
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gem,
        variable="temperature",
        day_ahead=1,
        skill=0.70,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.50,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.30,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # Baseline: explicit D+1 order (skill DESC → gem, ecmwf, gfs).
        d1 = client.get(
            "/api/curve",
            params={"site": site_id, "window": "all", "lead": "D+1"},
        ).json()
        # (a) Unparseable lead → D+1 fallback.
        bad = client.get(
            "/api/curve",
            params={"site": site_id, "window": "all", "lead": "foo"},
        ).json()
        # (b) Out-of-range lead → nulls-last / label-ASC; request twice for
        #     determinism check.
        oob_1 = client.get(
            "/api/curve",
            params={"site": site_id, "window": "all", "lead": "D+99"},
        ).json()
        oob_2 = client.get(
            "/api/curve",
            params={"site": site_id, "window": "all", "lead": "D+99"},
        ).json()

    # Shape checks: all responses must be 200 with a non-None series list.
    assert isinstance(d1["series"], list)
    assert isinstance(bad["series"], list)
    assert isinstance(oob_1["series"], list)

    d1_order = [s["feed_id"] for s in d1["series"]]
    bad_order = [s["feed_id"] for s in bad["series"]]
    oob_1_order = [s["feed_id"] for s in oob_1["series"]]
    oob_2_order = [s["feed_id"] for s in oob_2["series"]]

    # (a) Invalid lead coerces to D+1: ordering must exactly match explicit D+1.
    assert bad_order == d1_order, (
        f"lead=foo must fall back to D+1 ordering; "
        f"got {bad_order!r}, expected {d1_order!r}. "
        "_coerce_lead_day must return 1 (D+1 default) for an unparseable string."
    )

    # (b) D+99 is deterministic: two identical requests yield identical ordering.
    assert oob_1_order == oob_2_order, (
        f"D+99 ordering is non-deterministic: "
        f"first={oob_1_order!r}, second={oob_2_order!r}"
    )

    # Pin D+99 order: selected_index=None (99 ∉ _CURVE_LEADS=[0…7]) → every
    # feed's sort key is (1, 0.0, label) → alphabetical label-ASC.
    # "open-meteo / ecmwf_ifs" < "open-meteo / gem_global" < "open-meteo / gfs_global"
    expected_oob_order = [ecmwf, gem, gfs]
    assert oob_1_order == expected_oob_order, (
        f"D+99 (out-of-range, nulls-last / label-ASC) order wrong: "
        f"got {oob_1_order!r}, expected {expected_oob_order!r} "
        "(ecmwf < gem < gfs alphabetically). "
        "D+99 must not clamp to 7 or use skill-ranked ordering."
    )

    # Sanity: D+99 order must differ from D+1 order, so the D+99 pin has teeth.
    assert oob_1_order != d1_order, (
        "D+99 and D+1 orderings are identical — the D+99 oracle cannot detect "
        "a regression in out-of-range lead handling."
    )


def test_curve_exactly_zero_series_survives_exclusion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Zero Series Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")
    # Only eligible point is exactly 0.0 -> must NOT be dropped by the
    # is-not-None gate (a truthiness gate would drop it).
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=0.0,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        payload: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B1"
        ).json()
    series = payload["series"]
    assert len(series) == 1
    assert series[0]["feed_id"] == ecmwf
    assert series[0]["skill"][1] == 0.0
    assert series[0]["skill"][1] is not None


def test_curve_all_withheld_dataset_yields_empty_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "All Withheld Site")
    # Rows exist at multiple leads but none is confident (skill None) -> the
    # server-side "entirely null" exclusion must yield series: [], never a
    # nonempty list carrying no drawable point.
    for model in ("ecmwf_ifs", "gfs_global"):
        fid = _feed_id(conn, model)
        _seed_cell(
            conn,
            site_id=site_id,
            feed_id=fid,
            variable="temperature",
            day_ahead=0,
            skill=None,
            n=10,
        )
        _seed_cell(
            conn,
            site_id=site_id,
            feed_id=fid,
            variable="temperature",
            day_ahead=1,
            skill=None,
            n=10,
        )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        payload: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B1"
        ).json()
    assert payload["series"] == []


def test_curve_all_null_feed_does_not_displace_drawable_feeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Displacement Site")
    ecmwf = _feed_id(conn, "ecmwf_ifs")  # alphabetically earliest
    gfs = _feed_id(conn, "gfs_global")
    icon = _feed_id(conn, "icon_global")
    gem = _feed_id(conn, "gem_global")
    jma = _feed_id(conn, "jma_gsm")
    # ecmwf: entirely null (withheld at every lead it appears) -> must be
    # dropped BEFORE ordering/top, never displacing a drawable feed.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=0,
        skill=None,
        n=10,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=ecmwf,
        variable="temperature",
        day_ahead=1,
        skill=None,
        n=10,
    )
    # Three drawable feeds eligible at the selected lead D+1.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gfs,
        variable="temperature",
        day_ahead=1,
        skill=0.50,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=icon,
        variable="temperature",
        day_ahead=1,
        skill=0.40,
        n=5,
    )
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=gem,
        variable="temperature",
        day_ahead=1,
        skill=0.30,
        n=5,
    )
    # jma: null at selected lead D+1 but eligible at D+0 -> RETAINED.
    _seed_cell(
        conn,
        site_id=site_id,
        feed_id=jma,
        variable="temperature",
        day_ahead=0,
        skill=0.60,
        n=5,
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        payload: dict[str, Any] = client.get(
            f"/api/curve?site={site_id}&window=all&lead=D%2B1"
        ).json()
    by_id = {s["feed_id"]: s for s in payload["series"]}
    assert ecmwf not in by_id  # all-null feed excluded
    for drawable in (gfs, icon, gem):
        assert drawable in by_id  # drawable feeds keep their slot
    # Feed null-at-selected-lead but eligible elsewhere is present.
    assert jma in by_id
    assert by_id[jma]["skill"][1] is None
    assert by_id[jma]["skill"][0] == 0.60


# ===========================================================================
# §10.7 — Unknown-variable route graceful fallback
# ===========================================================================


def test_unknown_variable_renders_humanized_fallback_not_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Unknown Var Site")
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get(f"/dashboard?site={site_id}&variable=foo")
    assert resp.status_code == 200
    # Humanized fallback in the heading — never a KeyError 500.
    assert "Foo" in resp.text


def test_variable_label_for_totality() -> None:
    assert variable_label_for("precip") == "Precipitation"
    assert variable_label_for("temperature") == "Temperature"
    assert variable_label_for("wind") == "Wind"
    assert variable_label_for("foo") == "Foo"
    assert variable_label_for("air_quality") == "Air Quality"
    assert set(VARIABLE_LABELS) == {"temperature", "precip", "wind"}


# ===========================================================================
# §10.8 — Render smoke: standalone + Ingress with active-nav oracle
# ===========================================================================

_PAGES: list[tuple[str, str]] = [
    ("/sites", "Sites"),
    ("/dashboard", "Dashboard"),
    ("/ops", "Ops"),
    ("/overlay", "Overlay"),
]


def test_render_smoke_all_pages_standalone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        for path, label in _PAGES:
            resp = client.get(path)
            assert resp.status_code == 200, f"{path} standalone -> {resp.status_code}"
            # Active nav oracle: standalone root_path is empty.
            assert f'<a class="active" href="{path}">{label}</a>' in resp.text, (
                f"{path} did not carry the active nav class standalone"
            )


def test_render_smoke_all_pages_under_ingress_with_active_nav(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(
        app, client=(_SUPERVISOR_IP, 4321), follow_redirects=False
    ) as client:
        for path, label in _PAGES:
            resp = client.get(path, headers={"X-Ingress-Path": _INGRESS_PREFIX})
            assert resp.status_code == 200, f"{path} ingress -> {resp.status_code}"
            active = f'<a class="active" href="{_INGRESS_PREFIX}{path}">{label}</a>'
            assert active in resp.text, (
                f"{path} did not carry the root-path-stripped active class "
                "under Ingress"
            )
            # Paired negative: no OTHER tab may claim the active class.
            for other_path, other_label in _PAGES:
                if other_path == path:
                    continue
                assert (
                    f'<a class="active" href="{_INGRESS_PREFIX}{other_path}">'
                    f"{other_label}</a>" not in resp.text
                ), f"{other_path} wrongly active on {path} under Ingress"


# ===========================================================================
# §10.9 — Empty states, two distinct oracles
# ===========================================================================


def test_empty_db_no_site_hides_verdict_card(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/dashboard")
    assert resp.status_code == 200
    assert "No sites configured" in resp.text
    # The verdict card lives inside the `{% if site %}` block — it must not
    # render at all when there is no site.
    assert "panel wide verdict verdict-" not in resp.text
    assert "No scored data yet for this selection" not in resp.text


def test_enabled_site_zero_pairs_renders_empty_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Scoreless Site")
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        html = _get_dashboard(client, 1)
    # Verdict `empty` state copy renders with a seeded enabled site that has no
    # scored pairs.
    assert "verdict-empty" in html
    assert "No scored data yet for this selection" in html
    assert "No scored pairs." in html

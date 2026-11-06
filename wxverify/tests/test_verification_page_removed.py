"""W6 ("Part B"): the Verification page, its API and its run chain are
removed, along with the publish hold and close-chain stage 8 (the
worksheet cleanup) the hold and page depend on. Written before the
removal lands, so every test here fails today for the stated reason and
passes once W6's deletions are made.

Isolation: HTTP-route oracles drive a real app over ``TestClient`` against
a file-backed ``tmp_path`` database with an idle worker stand-in,
mirroring ``tests/test_ops_page_copy.py``'s ``_boot``. The close-chain
test reuses the real end-to-end night harness of
``tests/test_close_chain.py`` (``tests.helpers.standard_night``/
``run_due_jobs``/``install_round_state_standin``/``install_p4_standins``),
never a mocked database.

Synthetic data only (public repo): no real station, device or site
identifier appears below.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    clear_close_chain_log_map,
    disable_other_enqueuers,
    install_p4_standins,
    install_round_state_standin,
    patch_close_clock,
    run_due_jobs,
    standard_night,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db
from wxverify.db.site_days import day_record

R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """Autoused only inside this one file (``tests.helpers``'s own
    convention, see ``test_close_chain.py``) -- never across the suite."""
    clear_close_chain_log_map()


# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_ops_page_copy.py's ``_boot``).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _boot(tmp_path: Path, name: str, monkeypatch: pytest.MonkeyPatch) -> object:
    close_db()
    config.db_path = str(tmp_path / name)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


# ---------------------------------------------------------------------------
# Test 1 -- GET /verification is gone.
# ---------------------------------------------------------------------------


def test_verification_page_route_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fails now: ``web/routes.py`` still registers ``GET /verification``
    (``web/verification.py``'s ``load_verification``), so the page renders
    200. Passes once W6 deletes the route and its template: FastAPI 404s
    any path it has no route for."""
    app = _boot(tmp_path, "page.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/verification")
    assert resp.status_code == 404, (
        f"/verification must 404 once the page is removed, got "
        f"{resp.status_code}: {resp.text}"
    )
    close_db()


# ---------------------------------------------------------------------------
# Test 2 -- every route api/routes/verification.py registers is gone, with
# a paired positive proving the client itself still reaches a kept route.
# ---------------------------------------------------------------------------

# Every path FastAPI registers in wxverify/api/routes/verification.py
# today, read directly from that file (prefix "/api/verification"), with
# made-up ids/site numbers filling path and required query parameters.
_VERIFICATION_ROUTES: list[tuple[str, str]] = [
    ("GET", "/api/verification/status"),
    ("GET", "/api/verification/status?site=1"),
    ("PUT", "/api/verification/publish-hold"),
    ("GET", "/api/verification/runs"),
    ("GET", "/api/verification/latest?site=1"),
    ("GET", "/api/verification/runs/999"),
    ("GET", "/api/verification/runs/999/verdicts"),
    ("GET", "/api/verification/runs/999/evidence"),
    ("GET", "/api/verification/runs/999/diagnostics"),
    ("GET", "/api/verification/runs/999/methodology"),
]


def _csrf_headers(client: TestClient) -> dict[str, str]:
    """Reused verbatim from ``tests/test_publish_hold_control.py:71-73``:
    a valid CSRF token plus a same-origin ``Origin`` header, so a mutating
    request clears ``MutationGuard`` and reaches routing instead of dying
    at the guard with a 403 that would never turn into the removal's 404."""
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


@pytest.mark.parametrize("method,path", _VERIFICATION_ROUTES)
def test_every_verification_api_route_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str, path: str
) -> None:
    """Fails now for each case, but never against the framework's own
    no-route 404 (``{"detail": "Not Found"}``) -- the app registers no
    custom handler for unmatched routes (``wxverify/api/errors.py`` only
    handles ``ApiError``, ``JobDeferred`` and ``RequestValidationError``),
    so that body is Starlette's alone and is the one and only thing that
    proves the route is gone.

    Today each case reaches the live route and gets one of:
    - GET status/latest/runs/999 (+ verdicts/evidence/diagnostics/
      methodology): the route's own ``ApiError(404, ...)`` ("site not
      found" / "no published verification run" / "verification run not
      found"), body ``{"error": ...}`` -- a 404 status but the WRONG body,
      so today's assert on the body fails even though the status matches.
    - GET /api/verification/status, /runs: 200, body a JSON object/array.
    - PUT /api/verification/publish-hold (with a valid CSRF token so it
      clears ``MutationGuard`` and reaches the route): 422 from the
      ``held``/``confirm`` schema validation, since no body is sent.

    Passes once W6 deletes ``api/routes/verification.py`` whole and its
    ``app.py`` registration: FastAPI then has no route at any of these
    paths and Starlette's own no-route handler answers 404 with
    ``{"detail": "Not Found"}`` regardless of method, headers or body."""
    app = _boot(tmp_path, "api.db", monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client) if method != "GET" else {}
        resp = client.request(method, path, headers=headers)
    assert resp.status_code == 404, (
        f"{method} {path} must 404 once the verification API is removed, "
        f"got {resp.status_code}: {resp.text}"
    )
    assert resp.json() == {"detail": "Not Found"}, (
        f"{method} {path} must get the FRAMEWORK's no-route 404 body, not "
        f"an ApiError-shaped 404 the route itself can raise today -- got "
        f"{resp.text}"
    )
    close_db()


def test_a_kept_route_stays_200_proving_the_client_is_not_simply_broken(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the 404 checks above: the SAME client, against
    the SAME app, still gets 200 from a route untouched by W6
    (``GET /api/health/keys``). This must stay green before and after the
    removal -- if it ever went red, the 404s above would be meaningless
    (a broken client/app 404s everything)."""
    app = _boot(tmp_path, "kept.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/health/keys")
    assert resp.status_code == 200, resp.text
    close_db()


# ---------------------------------------------------------------------------
# Test 3 -- the nav has no /verification link, paired with a kept link.
# ---------------------------------------------------------------------------


def test_nav_has_no_verification_link_but_keeps_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fails now: ``base.html`` renders
    ``<a ... href="/verification">Verification</a>`` in every page's nav,
    so the dashboard's rendered HTML carries that href. Passes once W6
    deletes the nav link (``web/templates/base.html``): the href string is
    gone from the nav while the Ops tab's own link stays, proving the
    negative isn't just a broken nav template."""
    app = _boot(tmp_path, "nav.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/dashboard")
    assert resp.status_code == 200, resp.text
    html = resp.text
    assert 'href="/verification"' not in html, (
        "the nav must carry no link to the removed /verification page"
    )
    assert 'href="/ops"' in html, (
        "a kept nav tab (Ops) must still link to its own page -- a "
        "negative on a page with no nav at all would prove nothing"
    )
    close_db()


# ---------------------------------------------------------------------------
# Test 4 -- a close chain ends at stage 7; stage 8 never exists.
# ---------------------------------------------------------------------------


async def _day_close_job_keys(db, site_id):  # type: ignore[no-untyped-def]
    """Every job key the standard night's chain ever created for the site,
    in creation order (mirrors ``test_close_chain.py``'s own helper)."""
    rows = await db.read(
        lambda c: c.execute(
            "SELECT job_key FROM jobs WHERE type IN ('day_close', 'day_publish')"
            " AND site_id = ? ORDER BY id",
            (site_id,),
        ).fetchall()
    )
    return [row[0] for row in rows]


async def _publication_row(db, site_id, cutoff):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT state, publication_id FROM close_publications"
            " WHERE site_id = ? AND cutoff = ?",
            (site_id, cutoff.isoformat()),
        ).fetchone()
    )


def test_close_chain_ends_at_stage_7_with_published_and_no_stage_8(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fails now: today's chain enqueues a stage-8 ``day_publish`` job
    (``publish:{site}:8``, the worksheet cleanup) after stage 7 completes,
    so ``publish:{site}:8`` is present in the job-key list below. Passes
    once W6 makes stage 7 (publish) the chain's last stage: no stage-8 job
    is ever enqueued, while the stage-7 publish (the positive half of this
    assertion) still runs and the publication still reaches ``published``
    -- a chain that broke publishing entirely would fail this test too,
    for a different and equally real reason."""

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        keys = await _day_close_job_keys(db, site_id)
        assert f"publish:{site_id}:7" in keys, (
            "stage 7 (publish) must still run and be enqueued as it does today"
        )
        assert f"publish:{site_id}:8" not in keys, (
            "stage 8 (the worksheet cleanup) must no longer exist once stage 7"
            " is the chain's last stage"
        )

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert record.state == "closed", "the standard night must still close D"

        publication = await _publication_row(db, site_id, D)
        assert publication is not None
        assert publication["state"] == "published", (
            "the publication must still reach 'published' with stage 8 removed"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 5 -- the Ops page renders with no publish-hold panel, paired with a
# kept Ops panel.
# ---------------------------------------------------------------------------


def test_ops_page_renders_without_publish_hold_panel_but_keeps_budget_gauges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fails now: ``web/templates/ops/show.html`` includes
    ``ops/_publish_hold.html``, whose panel carries
    ``id="publish-hold-panel"`` and the heading "Nightly Verification Run
    Hold" (``tests/test_ops_page_copy.py``'s O44). Passes once W6 removes
    the Ops panel entry (``web/context.py``) and its template: both
    distinctive markers are gone from the rendered page, while the Budget
    Gauges panel (untouched by W6) still renders -- proving the page did
    not simply fail to render at all."""
    app = _boot(tmp_path, "ops.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = resp.text
    assert 'id="publish-hold-panel"' not in html, (
        "the publish-hold panel must be gone from the rendered Ops page"
    )
    assert "Nightly Verification Run Hold" not in html, (
        "the publish-hold panel's heading must be gone too"
    )
    assert "Budget Gauges" in html, (
        "a kept Ops panel (Budget Gauges) must still render -- an Ops page"
        " that failed to render at all would pass the two checks above for"
        " the wrong reason"
    )
    close_db()

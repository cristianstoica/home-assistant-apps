"""§17 families 7 and 10 — ingress-safe absolute URLs and an additive wire.

Family 7 (ingress): every absolute URL the verification surface emits must
be built through the ingress prefix. The only absolute URL the API emits is
``/api/verification/latest``'s 307 ``Location``; W2 rebuilt it through
``ingress_url``. The pre-existing coverage drives that redirect with an
EMPTY root path only, where a hand-concatenated path and a prefixed one are
byte-identical — a blind assertion that cannot see the bug. These oracles
drive it under both ingress modes wxverify supports (a static ``--root-path``
and the Supervisor's ``X-Ingress-Path`` header) and keep the unprefixed
drive as the paired negative.

Family 10 (wire-contract additivity): 0.11.1 adds three keys to the
verification API (``trigger`` on a status site, ``ranking_redesign_indicated``
on a verdict, ``observed_wet_precip_mae`` on diagnostics) and holds the
schema constant. That is legitimate only if it is purely additive. The
pinned key/type map below was hand-derived by reading the
0.11.0 route module, not by recording what the new code prints, so it is an
independent statement of the old contract: any key the release quietly
renames, drops, or retypes fails here, and any key it adds beyond the three
declared additions fails too.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.test_phase7_surface import (
    _idle_worker,
    _init_tmp_db,
    _make_site,
    _seed_published_run,
)
from wxverify.api.app import create_app
from wxverify.config import SUPERVISOR_INGRESS_CLIENT

_INGRESS_PREFIX = "/api/hassio_ingress/EXAMPLETOKEN"


def _app(monkeypatch: pytest.MonkeyPatch, *, root_path: str = "") -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path=root_path)


# ---------------------------------------------------------------------------
# Family 7 — the /latest redirect must stay inside the ingress mount.
# ---------------------------------------------------------------------------


def test_the_latest_redirect_is_built_through_the_ingress_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Location carries the ingress prefix under both ingress modes.

    Kills: the 0.11.0 implementation, ``RedirectResponse(url=
    f"/api/verification/runs/{run_id}")``. Under ingress that Location is
    ``/api/verification/runs/<id>`` — outside the mount, so the client
    leaves the add-on and the browser 404s. It also kills a prefix built
    from ``config.ingress_root_path`` instead of the request scope, which
    would miss the header-driven mode where root_path is per-request.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Ingress Town")
    run_id = _seed_published_run(conn, site_id)
    conn.commit()
    expected = f"{_INGRESS_PREFIX}/api/verification/runs/{run_id}"

    # Mode 1: static --root-path (a reverse proxy that strips the prefix).
    app = _app(monkeypatch, root_path=_INGRESS_PREFIX)
    with TestClient(app) as client:
        resp = client.get(
            f"/api/verification/latest?site={site_id}", follow_redirects=False
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == expected
        # Non-vacuity: the target the prefix points at is really there.
        followed = client.get(f"/api/verification/runs/{run_id}")
        assert followed.status_code == 200
        assert followed.json()["run"]["run_id"] == run_id

    # Mode 2: the Supervisor proxy, which advertises the prefix per request.
    app = _app(monkeypatch)
    with TestClient(app, client=(SUPERVISOR_INGRESS_CLIENT, 4321)) as client:
        resp = client.get(
            f"/api/verification/latest?site={site_id}",
            headers={"X-Ingress-Path": _INGRESS_PREFIX},
            follow_redirects=False,
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == expected


def test_the_latest_redirect_stays_bare_without_ingress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired negative: standalone serving is unchanged by the prefixing.

    Kills: an unconditional prefix (e.g. one hard-coded to the add-on mount,
    or ``root_path`` used without the empty-string case), which would send a
    direct/reverse-proxy client to a path that does not exist here.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Standalone Town")
    run_id = _seed_published_run(conn, site_id)
    conn.commit()

    app = _app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get(
            f"/api/verification/latest?site={site_id}", follow_redirects=False
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == f"/api/verification/runs/{run_id}"


# ---------------------------------------------------------------------------
# Family 10 — the 0.11.0 wire contract, hand-transcribed from the 0.11.0
# route module. Values are the JSON types each key carried there.
# ---------------------------------------------------------------------------

_RUN_OUT_0_11_0: dict[str, tuple[type, ...]] = {
    "run_id": (int,),
    "site_id": (int,),
    "state": (str,),
    "attempt": (int,),
    "methodology_version": (int,),
    "app_version": (str,),
    "tz_generation_id": (int,),
    "period_start": (str, type(None)),
    "period_end": (str, type(None)),
    "settled_through": (str, type(None)),
    "bootstrap_seed": (int,),
    "bootstrap_resamples": (int,),
    "input_fingerprint": (str,),
    "created_at": (str,),
    "published_at": (str, type(None)),
    "error": (str, type(None)),
}
_RUN_OUT_WITH_SNAPSHOT_0_11_0: dict[str, tuple[type, ...]] = {
    **_RUN_OUT_0_11_0,
    "config_snapshot": (dict, list, str, int, float, bool, type(None)),
}
_VERDICT_0_11_0: dict[str, tuple[type, ...]] = {
    "variable": (str,),
    "outcome": (str,),
    "recommended_depth": (int, type(None)),
    "incumbent_depth": (int,),
    "tested_family": (dict, list, str, int, float, bool, type(None)),
}
_DAY_CONTEXT_0_11_0: dict[str, tuple[type, ...]] = {
    "snapshot_local_date": (str,),
    "snapshot_utc": (str,),
    "knowability_exclusions": (dict, list, str, int, float, bool, type(None)),
    "null_availability_samples": (int,),
}
_STATUS_SITE_0_11_0: dict[str, tuple[type, ...]] = {
    "site_id": (int,),
    "published_run": (dict, type(None)),
    "warnings": (dict,),
}
_WARNINGS_0_11_0: dict[str, tuple[type, ...]] = {
    "no_publishable_run": (bool,),
    "stale_inputs": (bool,),
    "failed_newer_attempt": (bool,),
}

# The keys later releases are allowed to add, and nothing else.
_DECLARED_ADDITIONS = {
    "status_site": {"trigger"},
    "verdict": {"ranking_redesign_indicated"},
    "diagnostics": {"observed_wet_precip_mae"},
    # 0.13.2 (Fix 2): the per-run methodology-version refusal reason,
    # `str | None` -- `None` on the version-matching path, a `str` naming
    # both the run's version and the build's on any other.
    "methodology": {"contract_unavailable_reason"},
}


def _check(
    payload: dict[str, object],
    pinned: dict[str, tuple[type, ...]],
    *,
    added: set[str],
    where: str,
) -> None:
    for key, types in pinned.items():
        assert key in payload, f"{where}: 0.11.0 key {key!r} disappeared"
        assert isinstance(payload[key], types), (
            f"{where}: {key!r} retyped to {type(payload[key]).__name__}"
        )
    surplus = set(payload) - set(pinned)
    assert surplus == added, f"{where}: undeclared key change {surplus!r}"


def test_every_0_11_0_payload_key_survives_with_its_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 0.11.1 API is a strict superset of the 0.11.0 one.

    Kills: any rename of a 0.11.0 key (the additive claim that lets the
    schema constant stand is then false and every existing consumer breaks
    silently), any drop, and any addition beyond the three W-items declare.

    The key/type map alone does NOT catch a null-to-zero retype: a nullable
    key is pinned ``(int, None)``, so ``0`` where the contract says ``null``
    satisfies it. The route module's own contract ("insufficient /
    not-applicable / failed values are null, never numeric zero") is
    therefore pinned separately below, on the exact fixture values, for the
    two nullable keys ``_seed_published_run`` exercises as null.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Additive Town")
    run_id = _seed_published_run(conn, site_id)
    conn.commit()

    app = _app(monkeypatch)
    with TestClient(app) as client:
        status = client.get(f"/api/verification/status?site={site_id}").json()
        assert set(status) == {"verification_schema", "contract", "sites"}
        assert status["verification_schema"] == 2
        site = status["sites"][0]
        _check(
            site,
            _STATUS_SITE_0_11_0,
            added=_DECLARED_ADDITIONS["status_site"],
            where="status.sites[]",
        )
        _check(site["warnings"], _WARNINGS_0_11_0, added=set(), where="status.warnings")
        assert site["published_run"] is not None
        _check(
            site["published_run"],
            _RUN_OUT_0_11_0,
            added=set(),
            where="status.published_run",
        )

        runs = client.get(f"/api/verification/runs?site={site_id}").json()
        assert set(runs) == {"verification_schema", "limit", "offset", "runs"}
        _check(runs["runs"][0], _RUN_OUT_0_11_0, added=set(), where="runs[]")

        run = client.get(f"/api/verification/runs/{run_id}").json()
        assert set(run) == {"verification_schema", "run"}
        _check(
            run["run"],
            _RUN_OUT_WITH_SNAPSHOT_0_11_0,
            added=set(),
            where="run",
        )
        # Null discipline, half 1: the fixture inserts no ``error`` column, so
        # a run that has not failed reports null -- never "" and never 0.
        assert run["run"]["error"] is None

        verdicts = client.get(f"/api/verification/runs/{run_id}/verdicts").json()
        assert set(verdicts) == {"verification_schema", "run_id", "verdicts"}
        assert verdicts["verdicts"], "fixture must publish at least one verdict"
        for row in verdicts["verdicts"]:
            _check(
                row,
                _VERDICT_0_11_0,
                added=_DECLARED_ADDITIONS["verdict"],
                where="verdicts[]",
            )
        # Null discipline, half 2, with its paired positive in the same map:
        # ``_seed_published_run`` writes recommended_depth 3 for temperature
        # and SQL NULL for wind (retain_incumbent) and precip (skipped). A
        # route that coerced not-applicable to 0 would satisfy the (int, None)
        # type pin above; this exact map is what refuses it, and the
        # temperature entry keeps a blanket ``is None`` from passing too.
        assert {
            str(row["variable"]): row["recommended_depth"]
            for row in verdicts["verdicts"]
        } == {"temperature": 3, "wind": None, "precip": None}

        evidence = client.get(f"/api/verification/runs/{run_id}/evidence").json()
        assert set(evidence) == {
            "verification_schema",
            "run_id",
            "limit",
            "offset",
            "evidence",
        }

        diagnostics = client.get(f"/api/verification/runs/{run_id}/diagnostics").json()
        assert (
            set(diagnostics)
            == {
                "verification_schema",
                "run_id",
                "limit",
                "offset",
                "results",
                "day_context",
            }
            | _DECLARED_ADDITIONS["diagnostics"]
        )
        assert diagnostics["day_context"], "fixture must record day context"
        for day in diagnostics["day_context"]:
            _check(day, _DAY_CONTEXT_0_11_0, added=set(), where="day_context[]")

        methodology = client.get(f"/api/verification/runs/{run_id}/methodology").json()
        assert (
            set(methodology)
            == {
                "verification_schema",
                "run_id",
                "contract",
                "constants",
                "provenance",
            }
            | _DECLARED_ADDITIONS["methodology"]
        )
        # This fixture's run (methodology_version 1) does not match the
        # build's own version, so the refusal fires: `contract_unavailable_
        # reason` is a populated `str`, not the `None` it holds on the
        # matching-version path (test_phase7_surface.py's O-V3 v2 arm pins
        # that side).
        assert isinstance(methodology["contract_unavailable_reason"], str)
        _check(
            methodology["provenance"],
            _RUN_OUT_WITH_SNAPSHOT_0_11_0,
            added=set(),
            where="methodology.provenance",
        )


def test_the_evidence_row_shape_is_the_unchanged_results_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Evidence and diagnostics rows are ``dict(row)`` over two tables that
    0.11.1 does not migrate, so their key sets are the table columns.

    Kills: a release that adds a verification column (or projects a derived
    field into these rows) while leaving the schema constant unchanged — the
    same undeclared-widening failure the map above catches for hand-built
    payloads, on the two payloads that are schema-shaped.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Column Town")
    run_id = _seed_published_run(conn, site_id)
    conn.commit()
    ev_cols = {
        str(r[1]) for r in conn.execute("PRAGMA table_info(verification_evidence)")
    }
    res_cols = {
        str(r[1]) for r in conn.execute("PRAGMA table_info(verification_results)")
    }
    assert ev_cols and res_cols

    app = _app(monkeypatch)
    with TestClient(app) as client:
        evidence = client.get(f"/api/verification/runs/{run_id}/evidence").json()
        assert evidence["evidence"], "fixture must carry evidence rows"
        for row in evidence["evidence"]:
            assert set(row) == ev_cols
        diagnostics = client.get(f"/api/verification/runs/{run_id}/diagnostics").json()
        assert diagnostics["results"], "fixture must carry result rows"
        for row in diagnostics["results"]:
            assert set(row) == res_cols

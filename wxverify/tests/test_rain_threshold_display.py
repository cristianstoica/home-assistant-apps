"""Rain-threshold display precision (0.16.7).

Covers the four-file display fix: the Sites card and the /verification
page's snapshot facts-list and prose (both share the same
``snapshot.rain_threshold_mm if ... is not none else '—'`` expression)
now print the stored value verbatim instead of rounding to one decimal, the
verification page parses a foreign/imported snapshot's threshold the same
way the run itself parsed it (``_as_threshold``, mirroring
``run_config_from_row``'s ``float(str(...))`` rule), and the create-site
form's threshold input accepts any step, not just tenths.

Synthetic fixtures only: a fabricated site name/coords, UTC timezone.

Most route-level cases carry CONTROL or KILL in their id: a CONTROL renders
identically on the pre-fix code (proving the fixture does not manufacture a
spurious difference); a KILL is asserted to diverge from the pre-fix
renderer, and the mutation run in the QA report names the exact divergence
at this test's assertion for each. The `_as_threshold` unit cases and
`test_app_written_snapshot_matches_sites_card_for_the_same_site` have no
tag; the `_as_threshold` unit tests fail on the pre-fix code at import
because the helper does not exist there. Four string-valued KILL cases fail
on the pre-fix code with a 500, not a text difference.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
)

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py and tests/test_phase8_verification_v16.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, *, rain_threshold_mm: float = 0.2) -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled,
                 rain_threshold_mm)
            VALUES ('Rain Threshold Town', 40.0, -105.0, 900.0, 'UTC', 1, ?)
            """,
            (rain_threshold_mm,),
        ).lastrowid
    )
    ensure_published_generation(conn, site_id)
    return site_id


_MISSING = object()


def _seed_snapshot_run(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    rain_threshold_mm: object,
) -> int:
    """A minimal published run whose ``config_snapshot.rain_threshold_mm`` is
    the exact (possibly foreign/malformed) value under test.

    Everything else is copied from the site's own real, well-formed
    snapshot, so the rest of the page (roster, blend depths, timezone) still
    renders normally and only the threshold field is under test. Passing
    ``_MISSING`` drops the key entirely, characterizing the field-absent
    path distinctly from a present-but-``None`` value.
    """
    generation_id = ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    if rain_threshold_mm is _MISSING:
        del snapshot["rain_threshold_mm"]
    else:
        snapshot["rain_threshold_mm"] = rain_threshold_mm
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            VALUES (?, ?, 2, '0.16.7-test', 'running', 1, ?, '2026-05-01',
                    '2026-05-30', '2026-05-30', 4242, 10000, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint),
        ).lastrowid
    )
    publish_run(conn, site_id, run_id)
    conn.commit()
    return run_id


# The Sites card and the verification page's §16.1 facts list share the
# identical wrapping markup (only the Jinja expression inside <dd> differs
# between the two templates), so one extractor serves both.
_DT_DD = re.compile(r"<dt>Rain threshold</dt><dd>([^<]*)</dd>")
_PROSE = re.compile(r"rain threshold (\S+) mm &middot;")


def _card_text(html: str) -> str:
    match = _DT_DD.search(html)
    assert match is not None, "no 'Rain threshold' <dt>/<dd> row in the page"
    return match.group(1)


def _prose_text(html: str) -> str:
    match = _PROSE.search(html)
    assert match is not None, "no 'rain threshold ... mm' prose in the page"
    return match.group(1)


# ---------------------------------------------------------------------------
# 1. `_as_threshold` unit tests (imported lazily inside each test so an
#    ImportError on the pre-fix code -- which has no such symbol -- fails
#    only that test at run time, not the whole module at collection time).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(1, 1.0, id="int-one"),
        pytest.param("1", 1.0, id="numeric-string-one"),
        pytest.param("0.25", 0.25, id="numeric-string-quarter"),
        pytest.param(0.00001, 1e-05, id="very-small-float"),
    ],
)
def test_as_threshold_parses_numeric_and_numeric_string(
    raw: object, expected: float
) -> None:
    from wxverify.web.verification import _as_threshold  # noqa: PLC0415

    assert _as_threshold(raw) == expected


def test_as_threshold_parses_nan_and_inf_strings_via_the_runs_own_rule() -> None:
    """Pins current behaviour: these are NOT rejected by ``_as_threshold``,
    because ``run_config_from_row`` applies the identical ``float(str(...))``
    rule to a run's own pinned snapshot -- rejecting here would silently
    diverge the display from the value the run's methodology actually used.
    """
    from wxverify.web.verification import _as_threshold  # noqa: PLC0415

    nan_value = _as_threshold("nan")
    assert nan_value is not None
    assert math.isnan(nan_value)
    assert _as_threshold("inf") == math.inf


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(None, id="none"),
        pytest.param(True, id="bool"),
        pytest.param(["0.2"], id="list"),
        pytest.param("abc", id="non-numeric-string"),
    ],
)
def test_as_threshold_degrades_unparseable_values_to_none(raw: object) -> None:
    from wxverify.web.verification import _as_threshold  # noqa: PLC0415

    assert _as_threshold(raw) is None


# ---------------------------------------------------------------------------
# 2. Sites card precision.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stored", "expected_text"),
    [
        pytest.param(0.25, "0.25 mm", id="KILL-quarter-mm-precision"),
        pytest.param(0.2, "0.2 mm", id="CONTROL-two-tenths-unchanged"),
        pytest.param(1.0, "1.0 mm", id="CONTROL-one-point-oh-unchanged"),
    ],
)
def test_sites_card_renders_stored_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stored: float,
    expected_text: str,
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, rain_threshold_mm=stored)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    assert _card_text(response.text) == expected_text


# ---------------------------------------------------------------------------
# 3. Verification page snapshot rendering -- §16.1 facts list.
# ---------------------------------------------------------------------------

# NB: every case here must leave `float(str(rain_threshold_mm))` PARSEABLE.
# `load_verification` also calls `cached_observed_wet_precip_mae`, which (via
# `run_config_from_row`, an unrelated pre-existing function untouched by this
# diff) re-parses the SAME snapshot field with the identical `float(str(...))`
# rule but with NO fallback -- an unparseable value raises `ValueError`
# uncaught, 500ing the whole page before the template ever runs. This is a
# real, pre-existing bug (see the QA report's "production bug found" section)
# that blocks proving the display fix's malformed-value tolerance end to end
# via this route; those cases are exercised directly against `_snapshot_view`
# below instead (`test_snapshot_view_degrades_unparseable_values_to_none`).
_VERIFICATION_CASES: list[tuple[object, str, str]] = [
    # (raw stored value, expected rendered text, id)
    (0.25, "0.25", "KILL-float-quarter-precision"),
    (1, "1.0", "CONTROL-raw-int-formats-the-same-both-ways"),
    ("1", "1.0", "KILL-numeric-string-int"),
    ("0.25", "0.25", "KILL-numeric-string-quarter"),
    (0.00001, "1e-05", "KILL-very-small-value-precision"),
    ("nan", "nan", "KILL-bonus-nan-string-pin"),
    ("inf", "inf", "KILL-bonus-inf-string-pin"),
]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [pytest.param(raw, expected, id=id_) for raw, expected, id_ in _VERIFICATION_CASES],
)
def test_verification_facts_list_renders_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    raw: object,
    expected: str,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    _seed_snapshot_run(conn, site_id, rain_threshold_mm=raw)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    assert _card_text(response.text) == f"{expected} mm"


# ---------------------------------------------------------------------------
# 4. Verification page snapshot rendering -- §16.5 prose, parity with §16.1.
#
# Both locations render the identical Jinja expression against the same
# `snapshot.rain_threshold_mm`; this checks a representative subset (a
# present float needing precision, and a raw int that is unchanged both ways)
# renders byte-identically at BOTH observation points in the SAME response,
# rather than re-running the full matrix twice. Restricted to crash-free
# values for the same reason as `_VERIFICATION_CASES` above.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(0.25, "0.25", id="KILL-float-quarter-both-locations"),
        pytest.param(1, "1.0", id="CONTROL-raw-int-both-locations"),
    ],
)
def test_verification_facts_and_prose_agree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    raw: object,
    expected: str,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    _seed_snapshot_run(conn, site_id, rain_threshold_mm=raw)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    assert _card_text(response.text) == f"{expected} mm"
    assert _prose_text(response.text) == expected


# ---------------------------------------------------------------------------
# 4b. `_snapshot_view` directly, for the values that cannot reach the real
#     /verification route (see the note on `_VERIFICATION_CASES` above): the
#     production bug in `run_config_from_row` 500s the page before the
#     template renders, for ANY `rain_threshold_mm` that isn't parseable by
#     `float(str(...))`. This calls the real function the page's context
#     builder calls (`_snapshot_view`, not a reimplementation), pinning that
#     the display layer ITSELF still degrades these to a renderable `None`
#     -- the one assertion still reachable for this input class.
# ---------------------------------------------------------------------------


def _base_snapshot() -> dict[str, object]:
    """A well-formed snapshot shape, minus the field under test."""
    return {
        "timezone": "UTC",
        "wall_clock": "2026-05-01T00:00:00Z",
        "blend_depth": 2,
        "blend_depths": {},
        "blend_depth_sources": {},
        "min_n": 30,
        "window_days": 30,
        "tz_generation_id": 1,
        "roster": [],
    }


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(None, id="CONTROL-none-degrades-to-none"),
        pytest.param(_MISSING, id="CONTROL-missing-key-degrades-to-none"),
        pytest.param(True, id="KILL-bool-degrades-to-none"),
        pytest.param(["0.2"], id="KILL-list-degrades-to-none"),
        pytest.param("abc", id="KILL-non-numeric-string-degrades-to-none"),
    ],
)
def test_snapshot_view_degrades_unparseable_values_to_none(raw: object) -> None:
    from wxverify.web.verification import _snapshot_view  # noqa: PLC0415

    snapshot = _base_snapshot()
    if raw is not _MISSING:
        snapshot["rain_threshold_mm"] = raw
    view = _snapshot_view(snapshot)
    assert view is not None
    assert view["rain_threshold_mm"] is None


# ---------------------------------------------------------------------------
# 5. Parity between an app-written run's snapshot and the Sites card, for
#    the SAME stored threshold -- proves the two surfaces never disagree
#    about a threshold the site itself owns (as opposed to a hand-crafted
#    foreign snapshot, covered above).
# ---------------------------------------------------------------------------


def test_app_written_snapshot_matches_sites_card_for_the_same_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, rain_threshold_mm=0.25)
    generation_id = ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["rain_threshold_mm"] == 0.25
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            VALUES (?, ?, 2, '0.16.7-test', 'running', 1, ?, '2026-05-01',
                    '2026-05-30', '2026-05-30', 4242, 10000, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint),
        ).lastrowid
    )
    publish_run(conn, site_id, run_id)
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        sites_response = client.get("/sites")
        verification_response = client.get(f"/verification?site={site_id}")
    assert sites_response.status_code == 200
    assert verification_response.status_code == 200
    assert _card_text(sites_response.text) == "0.25 mm"
    assert _card_text(verification_response.text) == "0.25 mm"


# ---------------------------------------------------------------------------
# 6. Create-site form.
# ---------------------------------------------------------------------------


def test_create_site_form_input_is_step_any(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    match = re.search(r'<input name="rain_threshold_mm"[^>]*>', response.text)
    assert match is not None, "rain_threshold_mm input not found"
    input_tag = match.group(0)
    assert 'step="any"' in input_tag  # KILL: pre-fix was step="0.1"
    assert 'min="0"' in input_tag  # unaffected by this diff; still true


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def test_create_site_accepts_a_string_quarter_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CONTROL: unaffected by this diff -- `SiteCreate`/`StrictModel` (no
    `strict=True`) already coerced a JSON string field to float before this
    fix; kept as a paired positive beside the -0.1 rejection below.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json={
                "name": "String Threshold Site",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
                "rain_threshold_mm": "0.25",
            },
            headers=headers,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["rain_threshold_mm"] == 0.25
        row = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT rain_threshold_mm FROM sites WHERE id = ?", (body["id"],)
            ).fetchone()
        )
    assert row["rain_threshold_mm"] == 0.25


def test_create_site_rejects_a_negative_string_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CONTROL: unaffected by this diff -- `Field(ge=0)` already rejected a
    negative threshold before this fix; paired with the acceptance case
    above so the ge=0 gate itself is proven live, not merely assumed.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json={
                "name": "Negative Threshold Site",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
                "rain_threshold_mm": "-0.1",
            },
            headers=headers,
        )
        assert response.status_code == 422
        count = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT COUNT(*) AS n FROM sites WHERE name = 'Negative Threshold Site'"
            ).fetchone()["n"]
        )
    assert count == 0

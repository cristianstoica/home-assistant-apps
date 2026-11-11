"""HTTP/screen-level tests for the rebuilt Forecast page:
``?day=`` selection and clamping, the auto-poll region, the day headers'
warning badges and the rain-only-feed route regression (the page test's
fixture, driven through the routes this time).

Isolation: a real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_forecast_routes.py`` and
``tests/test_forecast_tiles_poll.py``'s own harnesses -- duplicated here on
purpose, per this suite's own-helpers convention, rather than imported from
either sibling test module).

HTML is parsed with the stdlib ``html.parser`` (via ``tests/helpers.py``'s
``collect_tags``, plus one small parser of this file's own for button
nesting), never by regex over the markup, so nesting and attribute
placement are checked structurally.

Synthetic fixtures only (public repo): fake site names, synthetic feed
sources, a made-up Ingress token.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import collect_tags, seed_rain_ranks
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import floor_hour, isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.forecast.data import forecast_fingerprint
from wxverify.forecast.page import (
    FETCH_UNKNOWN_TEXT,
    LOW_CONFIDENCE_TEXT,
    PARTIAL_TEXT,
    RANKING_UPDATING_TEXT,
    STALE_TEXT,
    SUBSTITUTE_TEXT,
    day_warnings,
    hour_columns,
)
from wxverify.forecast.service import (
    CellMeta,
    DayTile,
    PrecipCell,
    TempCell,
    WindCell,
)
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# Harness (duplicated from tests/test_forecast_routes.py /
# tests/test_forecast_tiles_poll.py on purpose -- own-helpers convention).
# ---------------------------------------------------------------------------

_SUPERVISOR_IP = "172.30.32.2"
_INGRESS_TOKEN = "screen7synthetic"
_INGRESS_PREFIX = f"/api/hassio_ingress/{_INGRESS_TOKEN}"


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


def _make_site(conn: sqlite3.Connection, name: str = "Screen Test Site") -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
) -> None:
    conn.execute(
        "DELETE FROM forecast_samples WHERE site_id = ? AND feed_id = ?"
        " AND variable = ? AND valid_at = ?",
        (site_id, feed_id, variable, valid_at),
    )
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value, issued_at),
    )


def _current_fingerprint(site_id: int) -> str:
    return get_db().read_sync(lambda conn: forecast_fingerprint(conn, site_id=site_id))


def _seed_non_empty_site(conn: sqlite3.Connection, name: str = "Non Empty Site") -> int:
    """A site with exactly one far-future sample: enough to flip
    ``ForecastView.empty`` to False and render all 8 day headers, following
    the same pattern as ``test_forecast_tiles_poll.py``'s
    ``test_tiles_renders_fragment_when_fingerprint_changed``. None of the 8
    display days actually cover 2030, so every tile renders as
    ``not_available`` -- irrelevant here, since these tests pin markup
    (button attributes/classes/counts), not per-day values."""
    site_id = _make_site(conn, name)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2030-01-01T00:00:00Z",
        valid_at="2030-01-01T00:00:00Z",
        lead_hours=1,
        value=10.0,
    )
    return site_id


def _dh_buttons(html: str) -> list[dict[str, str]]:
    """Every ``<button class="dh ...">`` in document order, in day-header
    order (the template emits one per ``page.headers``, in ``day_index``
    order)."""
    buttons = collect_tags(html, "button")
    return [b for b in buttons if "dh" in b.get("class", "").split()]


class _ButtonNestingParser(HTMLParser):
    """Records, for every ``<button>``, whether it opened while another
    button carrying the ``dh`` class was still open -- the warnings test's
    nesting check (no ``.day-warn`` sits inside a ``.dh``)."""

    def __init__(self) -> None:
        super().__init__()
        self.buttons: list[dict[str, Any]] = []
        self._dh_open: list[bool] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "button":
            return
        attr_map = {key: value or "" for key, value in attrs}
        classes = attr_map.get("class", "").split()
        nested_in_dh = any(self._dh_open)
        self.buttons.append(
            {"classes": classes, "attrs": attr_map, "nested_in_dh": nested_in_dh}
        )
        self._dh_open.append("dh" in classes)

    def handle_endtag(self, tag: str) -> None:
        if tag == "button" and self._dh_open:
            self._dh_open.pop()


# ---------------------------------------------------------------------------
# ?day= selection and clamping.
# ---------------------------------------------------------------------------


def test_day_3_selects_the_fourth_day_header_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``?day=3`` selects day 3 (the 4th header, 0-indexed): exactly one
    ``.dh`` button carries ``aria-pressed="true"``, and it is day_index 3.
    Catches: no clamp, or a raw path."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}&day=3")
    assert response.status_code == 200
    buttons = _dh_buttons(response.text)
    assert len(buttons) == 8
    pressed = [b["aria-pressed"] for b in buttons]
    assert pressed == ["true" if i == 3 else "false" for i in range(8)]


@pytest.mark.parametrize(
    ("requested_day", "expected_selected"),
    [
        pytest.param(9, 7, id="above-range-clamps-to-7"),
        pytest.param(-2, 0, id="below-range-clamps-to-0"),
    ],
)
def test_out_of_range_day_clamps_into_0_7(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    requested_day: int,
    expected_selected: int,
) -> None:
    """9 clamps to 7 and -2 clamps to 0. Catches: no clamp."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}&day={requested_day}")
    assert response.status_code == 200
    buttons = _dh_buttons(response.text)
    pressed_indices = [i for i, b in enumerate(buttons) if b["aria-pressed"] == "true"]
    assert pressed_indices == [expected_selected]


def test_non_integer_day_yields_422(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``?day=x`` -- FastAPI's own query-type validation, not the route's
    clamp. Catches: a raw path that reaches the handler at all."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}&day=x")
    assert response.status_code == 422


def test_in_range_day_is_not_coerced_to_an_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the two clamp tests above: an in-range day (3)
    is left alone, not forced to 0 or 7 by an unconditional override."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}&day=3")
    assert response.status_code == 200
    buttons = _dh_buttons(response.text)
    pressed_indices = [i for i, b in enumerate(buttons) if b["aria-pressed"] == "true"]
    assert pressed_indices == [3]


def test_under_ingress_every_day_button_hx_get_and_replace_url_carry_the_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Under HA Ingress, every one of the 8 day buttons' ``hx-get`` and
    ``hx-replace-url`` carry the Ingress prefix (``url()`` resolving against
    ``request.scope['root_path']``). Catches: a raw, unprefixed path that
    would 404 once the browser is behind the Ingress proxy."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(
        app, client=(_SUPERVISOR_IP, 4321), follow_redirects=False
    ) as client:
        response = client.get(
            f"/forecast?site={site_id}&day=2",
            headers={"X-Ingress-Path": _INGRESS_PREFIX},
        )
    assert response.status_code == 200
    buttons = _dh_buttons(response.text)
    assert len(buttons) == 8
    for button in buttons:
        assert button["hx-get"].startswith(_INGRESS_PREFIX), button
        assert button["hx-replace-url"].startswith(_INGRESS_PREFIX), button


# ---------------------------------------------------------------------------
# Poll.
# ---------------------------------------------------------------------------


def test_region_hx_get_carries_the_selected_day_on_the_page_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The region's own ``hx-get`` (its auto-poll URL) carries ``day=3``
    when day 3 is selected. Catches: a poll that drops the day."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}&day=3")
    assert response.status_code == 200
    tiles_divs = [
        d for d in collect_tags(response.text, "div") if d.get("id") == "forecast-tiles"
    ]
    assert len(tiles_divs) == 1
    assert "day=3" in tiles_divs[0]["hx-get"]


def test_changed_fingerprint_poll_returns_200_with_the_same_day_selected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A changed fingerprint gets 200 with the SAME day selected as the
    poll requested. Catches: a poll that drops the day."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast/tiles?site={site_id}&fingerprint=&day=3")
    assert response.status_code == 200
    buttons = _dh_buttons(response.text)
    assert len(buttons) == 8
    pressed_indices = [i for i, b in enumerate(buttons) if b["aria-pressed"] == "true"]
    assert pressed_indices == [3]
    tiles_divs = [
        d for d in collect_tags(response.text, "div") if d.get("id") == "forecast-tiles"
    ]
    assert "day=3" in tiles_divs[0]["hx-get"]


def test_unknown_site_poll_with_a_day_param_still_has_no_hx_get(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unknown site polled with an explicit ``day`` still answers 200
    "Site not found." with no ``hx-get`` at all -- the day param must not
    make the stopped region start polling again. Catches: a URL push (or a
    resumed poll) from a region that should have stopped."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast/tiles?site=999999&fingerprint=&day=3")
    assert response.status_code == 200
    assert "Site not found." in response.text
    assert "hx-get" not in response.text


def test_empty_state_changed_fingerprint_poll_carries_its_own_fingerprint_and_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A site with no samples, polled with a changed (empty-string)
    fingerprint and ``day=2``, answers 200 with the empty-state copy --
    and its region's own ``hx-get`` still carries the CURRENT fingerprint
    and the clamped day, so the next poll compares against the right
    token. Catches: an empty page without its fingerprint."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Empty Poll Site")  # zero samples
    app = _make_app(monkeypatch)
    current_fp = _current_fingerprint(site_id)
    with TestClient(app) as client:
        response = client.get(f"/forecast/tiles?site={site_id}&fingerprint=&day=2")
    assert response.status_code == 200
    assert "Still collecting forecasts" in response.text
    tiles_divs = [
        d for d in collect_tags(response.text, "div") if d.get("id") == "forecast-tiles"
    ]
    assert len(tiles_divs) == 1
    hx_get = tiles_divs[0]["hx-get"]
    assert f"fingerprint={current_fp}" in hx_get
    assert "day=2" in hx_get


def test_hx_replace_url_appears_exactly_8_times_all_on_dh_buttons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``hx-replace-url`` appears exactly 8 times on the rendered page, and
    every occurrence is on a ``.dh`` button -- never on the day-warning
    button or anywhere else. Catches: a URL push from the poll (or from a
    control that should not push one)."""
    conn = _init_tmp_db(tmp_path)
    site_id = _seed_non_empty_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}")
    assert response.status_code == 200
    parser = _ButtonNestingParser()
    parser.feed(response.text)
    replace_url_buttons = [b for b in parser.buttons if "hx-replace-url" in b["attrs"]]
    assert len(replace_url_buttons) == 8
    for button in replace_url_buttons:
        assert "dh" in button["classes"], button
    assert response.text.count("hx-replace-url") == 8


# ---------------------------------------------------------------------------
# Warnings -- pure day_warnings() order.
# ---------------------------------------------------------------------------


def _meta(*, extrema_state: str = "normal") -> CellMeta:
    return CellMeta(
        state="normal",
        feeds=[],
        partial=False,
        stale=False,
        extrema_unavailable=False,
        extrema_state=extrema_state,
        contributor_ids=(),
        fetch_unknown=False,
        substitute=False,
    )


def _tile(
    *,
    confidence_state: str = "normal",
    rain_extrema_state: str = "normal",
    stale: bool = False,
    substitute: bool = False,
    fetch_unknown: bool = False,
    partial: bool = False,
) -> DayTile:
    temp = TempCell(meta=_meta(), high_c=10.0, low_c=5.0)
    wind = WindCell(meta=_meta(), max_kmh=10.0, note=None)
    precip = PrecipCell(
        meta=_meta(extrema_state=rain_extrema_state),
        total_mm=1.0,
        wet_hours=1,
        icon=None,
        agreement_pct=None,
        feed_label=None,
    )
    return DayTile(
        day_index=0,
        label="Today",
        date_iso="2026-01-01",
        temp=temp,
        wind=wind,
        precip=precip,
        state="normal",
        confidence_state=confidence_state,
        stale=stale,
        partial=partial,
        fetch_unknown=fetch_unknown,
        substitute=substitute,
    )


def test_all_six_conditions_renders_five_texts_without_fetch_unknown() -> None:
    """A tile with all six badge conditions true renders exactly five
    warning texts, in day_warnings' order, with "Fetch time unknown"
    suppressed because the tile is also stale. Catches: a missing reason,
    or the "not stale" rule dropped."""
    tile = _tile(
        confidence_state="low_confidence",
        rain_extrema_state="rebuilding",
        stale=True,
        substitute=True,
        fetch_unknown=True,
        partial=True,
    )
    assert day_warnings(tile) == (
        LOW_CONFIDENCE_TEXT,
        RANKING_UPDATING_TEXT,
        STALE_TEXT,
        SUBSTITUTE_TEXT,
        PARTIAL_TEXT,
    )
    assert FETCH_UNKNOWN_TEXT not in day_warnings(tile)


def test_fetch_unknown_not_stale_renders_fetch_unknown_text() -> None:
    """Paired positive for the suppression above: with ``stale`` False,
    ``fetch_unknown`` alone renders "Fetch time unknown". Catches: the "not
    stale" rule dropped (it would have to drop this case too, not just
    suppress it unconditionally)."""
    tile = _tile(fetch_unknown=True, stale=False)
    assert day_warnings(tile) == (FETCH_UNKNOWN_TEXT,)


def test_no_conditions_renders_no_warnings() -> None:
    """None of the six conditions true renders an empty tuple -- the
    template's ``{% if header.warnings %}`` guard then never emits the ⚠
    button at all."""
    tile = _tile()
    assert day_warnings(tile) == ()


# ---------------------------------------------------------------------------
# Warnings -- the day-warn button is never nested inside the .dh button
# (HTTP-level, needs a real warning on the page).
# ---------------------------------------------------------------------------


def test_day_warn_button_is_never_nested_inside_a_dh_button(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real "Low confidence" day header (no publication seeded, the
    degrade-gracefully fixture of ``tests/test_forecast_routes.py``) has
    exactly one ``.day-warn`` button, and it is a SIBLING of the ``.dh``
    button, never a descendant of it. Catches: a nested button (invalid
    markup a browser would silently re-parent, hiding the bug)."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Warn Nesting Site")
    set_setting(conn, "min_n", "1")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    now = utc_now()
    tomorrow = now.date() + timedelta(days=1)
    issued_at = isoformat_utc(floor_hour(now))
    for h in range(24):
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            issued_at=issued_at,
            valid_at=f"{tomorrow.isoformat()}T{h:02d}:00:00Z",
            lead_hours=h + 1,
            value=11.0,
        )
    # Deliberately no publication seeded -- confidence_state="low_confidence".

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}")
    assert response.status_code == 200
    assert "Low confidence" in " ".join(response.text.split())

    parser = _ButtonNestingParser()
    parser.feed(response.text)
    day_warn_buttons = [b for b in parser.buttons if "day-warn" in b["classes"]]
    assert len(day_warn_buttons) >= 1
    for button in day_warn_buttons:
        assert button["nested_in_dh"] is False, button


# ---------------------------------------------------------------------------
# Rain-only feed, at the route level (the page test's fixture -- the exact
# scenario of tests/test_forecast_page.py's
# test_rain_only_feed_stray_temperature_hour_adds_no_blend_value, driven
# through /forecast and /forecast/day instead of build_forecast_page
# directly).
# ---------------------------------------------------------------------------


def _insert_feed(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 1, 360, 168, 0)",
        (f"synthetic-screen7-{name.lower()}", name.lower()),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_rain_only_feed_stray_temperature_hour_is_200_on_both_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rain-only-feed fixture (a precip-only feed R that also
    carries one stray temperature sample for an hour two temperature feeds
    already cover), driven through the HTTP routes rather than
    ``build_forecast_page`` directly: both ``GET /forecast`` and
    ``GET /forecast/day`` must answer 200. Catches: a per-feed series built
    in ``hourly_day`` that breaks the route, not just the function."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Rain Only Feed Site")
    t1 = _insert_feed(conn, "T1")
    t2 = _insert_feed(conn, "T2")
    r = _insert_feed(conn, "R")

    now = utc_now()
    today = now.date()
    columns = hour_columns(today, "UTC")
    for column in columns:
        for feed_id in (t1, t2):
            _insert_sample(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                issued_at=column.key,
                valid_at=column.key,
                lead_hours=1,
                value=10.0,
            )
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=r,
            variable="precip",
            issued_at=column.key,
            valid_at=column.key,
            lead_hours=1,
            value=0.0,
        )

    # R's one stray temperature sample, hour 5 -- an hour both T1 and T2
    # also cover. 69.0 stays inside the valid temperature range (-90..70)
    # while being unmistakably different from the 10.0 baseline.
    stray_column = columns[5]
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=r,
        variable="temperature",
        issued_at=stray_column.key,
        valid_at=stray_column.key,
        lead_hours=1,
        value=69.0,
    )
    seed_rain_ranks(
        conn,
        site_id=site_id,
        published_at=now - timedelta(minutes=5),
        ranked={0: [r]},
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={site_id}&day=0")
        day = client.get(f"/forecast/day?site={site_id}&day=0")
    assert page.status_code == 200
    assert day.status_code == 200

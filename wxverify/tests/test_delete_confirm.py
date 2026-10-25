"""Tests for the site-delete and station-remove confirmations.

Both buttons carry `hx-confirm` naming what is lost, and `hx-disabled-elt`
so a double click cannot fire the request twice. The attributes are added
only to `_card.html`'s Delete button and `_station_cluster.html`'s Remove
button -- nowhere else, so htmx's attribute inheritance cannot leak a
confirmation onto an ancestor and prompt for an unrelated action too.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from html.parser import HTMLParser
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES (?, 40, -105, 900, 'UTC')
            """,
            (name,),
        ).lastrowid
    )


def _insert_station(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
            VALUES (?, ?, 40, -105, 900, 1)
            """,
            (site_id, pws_station_id),
        ).lastrowid
    )


class _AttrParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, {key: value or "" for key, value in attrs}))


_SITE_DELETE_HX_DELETE_RE = re.compile(r"/api/sites/\d+$")


def _is_site_delete_button(tag: str, attrs: dict[str, str]) -> bool:
    return tag == "button" and bool(
        _SITE_DELETE_HX_DELETE_RE.search(attrs.get("hx-delete", ""))
    )


def _is_station_remove_button(tag: str, attrs: dict[str, str]) -> bool:
    return (
        tag == "button" and "hx-delete" in attrs and "/stations/" in attrs["hx-delete"]
    )


def _render_sites_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(lambda conn: _insert_site(conn, 'Example "Site" <2>'))
        db.write_sync(lambda conn: _insert_station(conn, site_id, "EXAMPLE1"))
        page = client.get("/sites")
    assert page.status_code == 200
    return page.text


# --------------------------------------------------------------------------
# T12 - site Delete button
# --------------------------------------------------------------------------


def test_t12_site_delete_hx_confirm_and_disabled_elt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the Delete button missing `hx-confirm` or `hx-disabled-elt`;
    a confirmation attached to the wrong element, or leaked onto an
    ancestor so an unrelated click (for example a feed toggle) also
    prompts; the site's name left out of the confirmation text."""
    page_text = _render_sites_page(tmp_path, monkeypatch)
    parser = _AttrParser()
    parser.feed(page_text)

    for tag, attrs in parser.tags:
        if "hx-confirm" not in attrs:
            continue
        assert _is_site_delete_button(tag, attrs) or _is_station_remove_button(
            tag, attrs
        ), (
            "only a site Delete button or a station Remove button may "
            f"carry hx-confirm; found <{tag}> {attrs}"
        )

    delete_buttons = [
        attrs for tag, attrs in parser.tags if _is_site_delete_button(tag, attrs)
    ]
    assert len(delete_buttons) == 1
    delete_attrs = delete_buttons[0]
    assert delete_attrs.get("hx-disabled-elt") == "this"
    confirm_text = delete_attrs.get("hx-confirm", "")
    assert 'Delete Example "Site" <2>?' in confirm_text
    assert "cannot be undone" in confirm_text

    assert "Delete Example &#34;Site&#34; &lt;2&gt;?" in page_text


# --------------------------------------------------------------------------
# T13 - station Remove button, same shape as T12
# --------------------------------------------------------------------------


def test_t13_station_remove_button_has_hx_confirm_and_hx_disabled_elt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the station Remove button left without a confirmation
    entirely; its confirmation missing the station's own id, which
    would leave an operator unable to tell which of several stations a
    confirm dialog is about."""
    page_text = _render_sites_page(tmp_path, monkeypatch)
    parser = _AttrParser()
    parser.feed(page_text)

    remove_buttons = [
        attrs for tag, attrs in parser.tags if _is_station_remove_button(tag, attrs)
    ]
    assert len(remove_buttons) == 1
    remove_attrs = remove_buttons[0]
    assert remove_attrs.get("hx-disabled-elt") == "this"
    confirm_text = remove_attrs.get("hx-confirm", "")
    assert "EXAMPLE1" in confirm_text
    assert "cannot be undone" in confirm_text

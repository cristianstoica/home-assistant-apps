"""The starting history (test IDs SH1-SH2).

SH1 (``wxverify.db.switch.first_record_date``) and SH2 (the dashboard/
overlay hint) check the starting-history date shown to the user. Zone is
``UTC`` throughout. Stations and feeds are synthetic
(``ISTATION01``/``ISTATION02``; feed ``X``).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final

import pytest
from fastapi.testclient import TestClient

from tests.helpers import day_close_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import Database, close_db, get_db, init_db

# C, the cutover day, is chosen well before any real clock reaches it.
C: Final = date(2024, 6, 10)


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    return init_db(str(db_path))


# ---------------------------------------------------------------------------
# SH1
# ---------------------------------------------------------------------------


def test_sh1_first_record_date_is_the_minimum_local_date(tmp_path: Path) -> None:
    """SH1: ``first_record_date`` for a site with no record,
    and for one with records F, F + 1.

    M67 -> at the ``with_records`` assertion: correct = F (the minimum
    ``site_days.local_date``), mutant = F + 1 (M67 returns the latest
    record instead of the first).
    """
    from wxverify.db.switch import first_record_date

    conn = _init_tmp_db(tmp_path)._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone="UTC", with_records=False)
    assert first_record_date(conn, site_id) is None

    f = C
    f_start = datetime(f.year, f.month, f.day, tzinfo=UTC)
    from wxverify.db.site_days import start_site_days

    start_site_days(conn, site_id, f, now_utc=f_start)
    assert first_record_date(conn, site_id) == f


# ---------------------------------------------------------------------------
# SH2
# ---------------------------------------------------------------------------


_HINT_TEXT: Final = (
    "Days before {f} are from before the daily close: they keep the"
    " scores they had and were never closed."
)
# The invariant tail of the hint (no F in it), for the absence checks below
# -- those must hold whatever F turns out to be, since no record exists yet.
_HINT_INVARIANT_TAIL: Final = (
    "from before the daily close: they keep the scores they had and were never closed."
)


async def _sh2_idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _sh2_patch_clock(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    """Pin ``utc_now`` on the page read path (mirrors
    ``tests/test_close_pages.py``'s ``_patch_clock``, built locally here)."""
    for target in (
        "wxverify.web.context.utc_now",
        "wxverify.api.routes.timeseries.utc_now",
        "wxverify.worker.access_hold.utc_now",
    ):
        monkeypatch.setattr(target, lambda: now, raising=False)


def test_sh2_the_dashboard_and_overlay_hint_tracks_close_and_record_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SH2: the dashboard and Overlay pages, read through
    ``TestClient`` (``tests/test_close_pages.py``'s ``_make_app``/
    ``_patch_clock`` pattern, built locally here, since that file holds no
    reusable helper): no record yet; then a record. The hint text is
    absent, then present on both pages.

    M68 -> at the ``with_record`` assertions below: correct = the hint
    text present on both pages (a record dated F), mutant = absent (M68
    never shows the hint, so the one state that should carry it never
    gets it).
    """
    conn = _init_tmp_db(tmp_path)._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone="UTC", with_records=False)
    conn.commit()

    f = C
    f_start = datetime(f.year, f.month, f.day, tzinfo=UTC)
    now = f_start + timedelta(hours=1)
    _sh2_patch_clock(monkeypatch, now)
    monkeypatch.setattr("wxverify.api.app.run_worker", _sh2_idle_worker)
    app = create_app(root_path="")

    def _pages(client: TestClient) -> tuple[str, str]:
        dashboard = client.get("/dashboard", params={"site": site_id})
        overlay = client.get("/overlay", params={"site": site_id})
        assert dashboard.status_code == 200
        assert overlay.status_code == 200
        return dashboard.text, overlay.text

    from wxverify.db.site_days import start_site_days

    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001
        dashboard_no_record, overlay_no_record = _pages(client)

        start_site_days(conn, site_id, f, now_utc=f_start)
        conn.commit()
        dashboard_with_record, overlay_with_record = _pages(client)

    assert _HINT_INVARIANT_TAIL not in dashboard_no_record, (
        "the hint must be withheld with no record yet"
    )
    assert _HINT_INVARIANT_TAIL not in overlay_no_record, (
        "the hint must be withheld with no record yet"
    )

    expected = _HINT_TEXT.format(f=f.isoformat())
    assert expected in dashboard_with_record, (
        "M68 -> at this assertion: correct = the hint text present on the"
        f" dashboard page (a record at {f.isoformat()}), mutant = absent"
        " (M68 never shows the hint)"
    )
    assert expected in overlay_with_record, (
        "M68 -> at this assertion: correct = the hint text present on the"
        " Overlay page, mutant = absent"
    )

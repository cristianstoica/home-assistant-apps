"""The rain tile, service and template: the DB/service half of C2, and C3
to C7. The pure ranking-rule half of C2 is in ``tests/test_rain_pick.py``;
the upgrade fill is in ``tests/test_rain_history_fill.py``.

Real SQLite throughout; nothing in the code under test is
mocked. Synthetic data only: feed sources ``synthetic-rt-*``, station ids
``ISTATION-A``..``ISTATION-F``, zone UTC.

C2's incumbent-memory sub-case seeds ``publication_rain_ranks``/``rain_picks``
directly via plain INSERT (the convention ``tests/test_publication_build.py``
already uses for ``publications`` and its child tables), rather than driving
60+ nightly publish cycles, since the incumbent lookup
(``scoring.publication._build``) only needs one real ``publish()`` call at
the cutoff under test plus a hand-planted "60 (or 61) cutoffs ago" row.

C3-C6 seed ``publication_rain_ranks`` directly too (bypassing
``rank_rain``/``_build``/``_swap`` entirely) and drive
``forecast.service.build_forecast`` straight against a fresh
``:memory:`` DB -- the pattern ``tests/test_forecast_service.py`` already
uses for the rest of the tile.

C7 drives ``scoring.close_scores.score_day`` on a hand-built ``DayRecord``
in the ``closing`` state (the pattern ``tests/test_close_scores.py`` uses),
seeding ``station_observations`` directly, to isolate the new ``rain_days``
write without needing a real feed or any scored forecasts.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.helpers import NOW, D, asof_conn, asof_make_site, day_close_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import (
    DAILY_CLOSE_ON_KEY,
    begin_close,
    day_record,
    ensure_site_days,
    finish_close,
    start_site_days,
)
from wxverify.db.switch import ensure_site_generation
from wxverify.forecast.service import build_forecast
from wxverify.scoring.close_scores import score_day
from wxverify.scoring.publication import publish

# ---------------------------------------------------------------------------
# Shared tiny helpers (own-helpers rule: nothing here touches tests/helpers.py
# or another test file -- only the pattern of test_publication_build.py and
# test_close_scores.py is followed, not their code).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _insert_feed(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 1, 360, 168, 0)",
        (f"synthetic-rt-{name.lower()}", name.lower()),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )


# ---------------------------------------------------------------------------
# C2 (DB half): the incumbent remembered across a 60-cutoff gap, lost at 61.
# ---------------------------------------------------------------------------


def _make_night_db(tmp_path: Path) -> tuple[Database, FencedWriter, sqlite3.Connection]:
    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- tests build the fixture on the real writer conn
    return db, FencedWriter(db), conn


def _seed_two_point_lead(
    conn: sqlite3.Connection, *, site_id: int, cutoff: date, best: int, worse: int
) -> None:
    """One counted day (``cutoff`` - 1) at day_ahead 0 where ``best`` agrees
    with a rain truth and ``worse`` misses it once (score 2*1 = 2): BEST's
    score lead over WORSE is exactly 2 points.
    """
    truth_day = cutoff - timedelta(days=1)
    conn.execute(
        "INSERT INTO rain_days (site_id, local_date, outcome, stations_full,"
        " stations_partial) VALUES (?, ?, 'rain', 2, 0)",
        (site_id, truth_day.isoformat()),
    )
    conn.execute(
        "INSERT INTO rain_forecast_days"
        " (site_id, local_date, feed_id, day_ahead, total_mm)"
        " VALUES (?, ?, ?, 0, 1.0)",
        (site_id, truth_day.isoformat(), best),
    )
    conn.execute(
        "INSERT INTO rain_forecast_days"
        " (site_id, local_date, feed_id, day_ahead, total_mm)"
        " VALUES (?, ?, ?, 0, 0.0)",
        (site_id, truth_day.isoformat(), worse),
    )


def _run_publish_with_old_rain_pick(
    tmp_path: Path, *, gap_days: int
) -> tuple[sqlite3.Connection, int, int, int]:
    """Build a site with one closed cutoff, a 2-point lead between two
    feeds, and a ``rain_picks`` row planted ``gap_days`` before the cutoff
    naming the WORSE feed as a prior incumbent. Returns ``(conn, site_id,
    best_feed_id, worse_feed_id)`` after ``publish()`` has run.
    """
    db, writer, conn = _make_night_db(tmp_path)
    site_id = day_close_site(conn, timezone="UTC", with_records=True)
    cutoff = D - timedelta(days=3)  # the only date day_close_site closes
    best = _insert_feed(conn, "Best")
    worse = _insert_feed(conn, "Worse")
    _subscribe(conn, site_id, best)
    _subscribe(conn, site_id, worse)
    _seed_two_point_lead(conn, site_id=site_id, cutoff=cutoff, best=best, worse=worse)
    old_cutoff = cutoff - timedelta(days=gap_days)
    conn.execute(
        "INSERT INTO rain_picks (site_id, cutoff, day_ahead, feed_id)"
        " VALUES (?, ?, 0, ?)",
        (site_id, old_cutoff.isoformat(), worse),
    )
    conn.commit()

    async def _run() -> None:
        await publish(
            db, writer, site_id=site_id, cutoff=cutoff, attempt=1, now_utc=NOW
        )

    asyncio.run(_run())
    pub_row = conn.execute(
        "SELECT id FROM publications WHERE site_id = ? AND cutoff = ?",
        (site_id, cutoff.isoformat()),
    ).fetchone()
    assert pub_row is not None, "publish() must have written a publications row"
    return conn, int(pub_row["id"]), best, worse


def _selected_feed(conn: sqlite3.Connection, publication_id: int) -> int | None:
    row = conn.execute(
        "SELECT feed_id FROM publication_rain_ranks"
        " WHERE publication_id = ? AND day_ahead = 0 AND selected = 1",
        (publication_id,),
    ).fetchone()
    return None if row is None else int(row["feed_id"])


def test_incumbent_remembered_across_a_60_cutoff_gap(tmp_path: Path) -> None:
    """A ``rain_picks`` row 60 cutoffs before the publish cutoff names
    WORSE as the prior incumbent. With only a 2-point lead for BEST, the
    incumbent-stays rule must keep WORSE selected, proving the
    60-cutoff-old row was found and honored.
    """
    conn, publication_id, _best, worse = _run_publish_with_old_rain_pick(
        tmp_path, gap_days=60
    )
    assert _selected_feed(conn, publication_id) == worse


def test_incumbent_lost_at_61_cutoffs(tmp_path: Path) -> None:
    """The identical 2-point-lead setup, but the planted ``rain_picks`` row
    is 61 cutoffs back -- one cutoff outside the remembered window. With no
    incumbent found, the best-score model (BEST) must be selected outright,
    even though a 2-point lead alone would not normally switch an incumbent.
    """
    conn, publication_id, best, _worse = _run_publish_with_old_rain_pick(
        tmp_path, gap_days=61
    )
    assert _selected_feed(conn, publication_id) == best


# ---------------------------------------------------------------------------
# C3-C6: the tile (forecast.service.build_forecast against a fresh
# :memory: DB, the pattern of tests/test_forecast_service.py).
# ---------------------------------------------------------------------------

_FIXED_NOW = datetime(2024, 1, 10, 0, 30, tzinfo=UTC)
_DAY0 = _FIXED_NOW.date()
_DAY1 = _DAY0 + timedelta(days=1)


def _make_forecast_db() -> tuple[sqlite3.Connection, int]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES ('Rain Tile Site', 40.0, -105.0, 900.0, 'UTC')"
        ).lastrowid
    )
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(_FIXED_NOW))
    return conn, site_id


def _seed_publication(conn: sqlite3.Connection, *, site_id: int, cutoff: date) -> int:
    published_at = isoformat_utc(_FIXED_NOW)
    cur = conn.execute(
        "INSERT INTO publications"
        " (site_id, cutoff, first_record_date, window_days, window_start_date,"
        "  wind_start_date, settings, methodology_version, scoring_rule,"
        "  wind_method, wind_constants, build_started_at, published_at)"
        " VALUES (?, ?, ?, 30, ?, ?, '{}', 1, 'test', 'test', '{}', ?, ?)",
        (
            site_id,
            cutoff.isoformat(),
            cutoff.isoformat(),
            cutoff.isoformat(),
            cutoff.isoformat(),
            published_at,
            published_at,
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_rain_rank(
    conn: sqlite3.Connection,
    publication_id: int,
    *,
    day_ahead: int,
    rank: int,
    feed_id: int,
    misses: int = 0,
    false_alarms: int = 0,
    common_days: int = 20,
    counted_days: int = 20,
    provisional: bool = False,
    selected: bool = False,
    in_group: bool = True,
) -> None:
    conn.execute(
        "INSERT INTO publication_rain_ranks"
        " (publication_id, day_ahead, rank, feed_id, misses, false_alarms,"
        "  common_days, counted_days, provisional, selected, in_group)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            publication_id,
            day_ahead,
            rank,
            feed_id,
            misses,
            false_alarms,
            common_days,
            counted_days,
            int(provisional),
            int(selected),
            int(in_group),
        ),
    )


def _insert_hourly(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    day: date,
    hourly: Sequence[float],
) -> None:
    for h, value in enumerate(hourly):
        valid_at = isoformat_utc(
            datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(hours=h)
        )
        conn.execute(
            "INSERT INTO forecast_samples"
            " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
            "  value, source_raw, model_run_id, fetched_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)",
            (site_id, feed_id, variable, valid_at, valid_at, h + 1, value, valid_at),
        )


def _full_precip_day(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, day: date, hourly
) -> None:
    _insert_hourly(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="precip",
        day=day,
        hourly=hourly,
    )


def _build(conn: sqlite3.Connection, site_id: int):
    return build_forecast(
        conn, site_id=site_id, timezone="UTC", rain_threshold_mm=0.2, now=_FIXED_NOW
    )


# --- C3: the tile follows the winner ---------------------------------------


@pytest.mark.parametrize(
    ("wettest", "expected_icon"),
    [
        pytest.param(1.0, "light", id="wettest-hour-1-0-mm-per-h-light"),
        pytest.param(1.01, "heavy", id="wettest-hour-1-01-mm-per-h-heavy"),
    ],
)
def test_tile_follows_winner_f_icon_from_its_wettest_hour(
    wettest: float, expected_icon: str
) -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    g = _insert_feed(conn, "G")
    h = _insert_feed(conn, "H")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=2, feed_id=g)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=3, feed_id=h)

    f_hourly = [0.0] * 24
    f_hourly[5] = wettest
    f_hourly[10] = 1.0
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=f_hourly)
    g_hourly = [10.0 / 24.0] * 24  # much wetter overall than F
    _full_precip_day(conn, site_id=site_id, feed_id=g, day=_DAY0, hourly=g_hourly)
    h_hourly = [0.0] * 24  # drier than F
    _full_precip_day(conn, site_id=site_id, feed_id=h, day=_DAY0, hourly=h_hourly)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.total_mm == pytest.approx(wettest + 1.0)  # F's own total, not G/H's
    assert precip.wet_hours == 2
    assert precip.icon == expected_icon


def test_total_under_0_2mm_shows_the_amount_with_no_icon() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)

    hourly = [0.0] * 24
    hourly[3] = 0.19
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=hourly)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.total_mm == pytest.approx(0.19)
    assert precip.icon is None


def test_winner_with_no_full_day_hands_over_to_the_next_ranked_model() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    g = _insert_feed(conn, "G")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=2, feed_id=g)

    # F: only 10 hours -- does not cover the local day exactly.
    _insert_hourly(
        conn,
        site_id=site_id,
        feed_id=f,
        variable="precip",
        day=_DAY0,
        hourly=[1.0] * 10,
    )
    g_hourly = [0.0] * 24
    g_hourly[0] = 3.0
    _full_precip_day(conn, site_id=site_id, feed_id=g, day=_DAY0, hourly=g_hourly)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.total_mm == pytest.approx(3.0)  # G's total, not F's


# --- C4: Agreement % --------------------------------------------------------


def test_agreement_pct_two_of_three_group_members_wet() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    g = _insert_feed(conn, "G")
    h = _insert_feed(conn, "H")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=2, feed_id=g)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=3, feed_id=h)

    f_hourly = [0.0] * 24
    f_hourly[0] = 1.0  # wet
    g_hourly = [0.0] * 24
    g_hourly[0] = 1.0  # wet
    h_hourly = [0.0] * 24  # dry
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=f_hourly)
    _full_precip_day(conn, site_id=site_id, feed_id=g, day=_DAY0, hourly=g_hourly)
    _full_precip_day(conn, site_id=site_id, feed_id=h, day=_DAY0, hourly=h_hourly)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.agreement_pct == 67


def test_single_ranked_feed_shows_no_agreement_pct() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=[0.0] * 24)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.agreement_pct is None


def test_member_with_no_full_day_forecast_counts_in_neither_k_nor_q() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    g = _insert_feed(conn, "G")
    h = _insert_feed(conn, "H")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=2, feed_id=g)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=3, feed_id=h)

    f_hourly = [0.0] * 24
    f_hourly[0] = 1.0  # wet, full day
    g_hourly = [0.0] * 24  # dry, full day
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=f_hourly)
    _full_precip_day(conn, site_id=site_id, feed_id=g, day=_DAY0, hourly=g_hourly)
    # H: only a partial day -- must be excluded from both k and q.
    _insert_hourly(
        conn,
        site_id=site_id,
        feed_id=h,
        variable="precip",
        day=_DAY0,
        hourly=[5.0] * 6,
    )

    precip = _build(conn, site_id).tiles[0].precip
    # Effective denominator 2 (F, G), 1 wet (F): 50%, never 33% (H wrongly
    # in q) or 67% (H wrongly counted as wet in k).
    assert precip.agreement_pct == 50


def _precip_day_text(response_text: str) -> str:
    """The Precipitation grid's top-row day-column text (``page.rain_text``)
    out of a rendered ``/forecast`` page -- the replacement for the old
    tile row's "Rain" ``<strong>``. Scoped to the Precipitation ``<table>``
    (the selected day's only), so it never picks up another grid's day cell.
    """
    section = response_text.split("Precipitation <span>mm/h</span>")[1]
    section = section.split("</table>")[0]
    start = section.index('<td class="day">') + len('<td class="day">')
    end = section.index("</td>", start)
    # The cell's visible text only -- the rain winner's markup wraps it
    # in a <small> and, with an agreement percentage, a
    # <span title="Agreement">: strip every tag, keep what a reader sees.
    return re.sub(r"<[^>]+>", "", section[start:end]).strip()


def test_forecast_page_rain_tile_shows_the_agreement_tooltip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The template half of C4: the rendered ``/forecast`` page's
    Precipitation row shows the agreement percentage the fixture computes
    (67%, two of three ranked members wet), inside the
    ``<span title="Agreement">`` tooltip."""
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001
    site_id = int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES ('Rain Tooltip Site', 40.0, -105.0, 900.0, 'UTC')"
        ).lastrowid
    )
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(_FIXED_NOW))
    f = _insert_feed(conn, "F")
    g = _insert_feed(conn, "G")
    h = _insert_feed(conn, "H")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=2, feed_id=g)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=3, feed_id=h)
    for day in (_DAY0, _DAY1):
        _insert_hourly(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="temperature",
            day=day,
            hourly=[10.0] * 24,
        )
        _insert_hourly(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="wind",
            day=day,
            hourly=[5.0] * 24,
        )
    f_hourly = [0.0] * 24
    f_hourly[0] = 1.0  # wet
    g_hourly = [0.0] * 24
    g_hourly[0] = 1.0  # wet
    h_hourly = [0.0] * 24  # dry
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=f_hourly)
    _full_precip_day(conn, site_id=site_id, feed_id=g, day=_DAY0, hourly=g_hourly)
    _full_precip_day(conn, site_id=site_id, feed_id=h, day=_DAY0, hourly=h_hourly)
    conn.commit()

    monkeypatch.setattr("wxverify.forecast.service.utc_now", lambda: _FIXED_NOW)
    # build_forecast_page's own `now or utc_now()` reads page.py's own import
    # of utc_now, a separate binding from service.utc_now above -- both must
    # be pinned to the same instant or the page renders against the real
    # wall-clock date instead of _FIXED_NOW.
    monkeypatch.setattr("wxverify.forecast.page.utc_now", lambda: _FIXED_NOW)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        # The page shows only the SELECTED day's grids (day=1, tomorrow, by
        # default) -- day_ahead 0, where this fixture's ranking lives, needs
        # an explicit day=0.
        response = client.get(f"/forecast?site={site_id}&day=0")
    assert response.status_code == 200

    row_text = _precip_day_text(response.text)
    assert "67%" in row_text
    assert '<span title="Agreement">67%</span>' in response.text


def test_forecast_page_rain_tile_with_no_agreement_shows_no_percent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired negative: with only one ranked feed, ``agreement_pct`` is
    ``None`` (``test_single_ranked_feed_shows_no_agreement_pct``), so
    the rendered Precipitation row's day text must carry no percentage --
    today and after the change alike.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001
    site_id = int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES ('Rain No Agreement Site', 40.0, -105.0, 900.0, 'UTC')"
        ).lastrowid
    )
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(_FIXED_NOW))
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    for day in (_DAY0, _DAY1):
        _insert_hourly(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="temperature",
            day=day,
            hourly=[10.0] * 24,
        )
        _insert_hourly(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="wind",
            day=day,
            hourly=[5.0] * 24,
        )
    hourly = [0.0] * 24
    hourly[0] = 1.0
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=hourly)
    conn.commit()

    monkeypatch.setattr("wxverify.forecast.service.utc_now", lambda: _FIXED_NOW)
    # build_forecast_page's own `now or utc_now()` reads page.py's own import
    # of utc_now, a separate binding from service.utc_now above -- both must
    # be pinned to the same instant or the page renders against the real
    # wall-clock date instead of _FIXED_NOW.
    monkeypatch.setattr("wxverify.forecast.page.utc_now", lambda: _FIXED_NOW)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}&day=0")
    assert response.status_code == 200

    row_text = _precip_day_text(response.text)
    assert "%" not in row_text


# --- C5: a missing forecast never shows as dry ------------------------------


def test_no_ranks_for_a_day_ahead_shows_no_rain_forecast_never_zero() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    # Real precip samples exist, but no publication/ranking at all: the
    # view must not be "empty" globally, yet this day's rain must still be
    # unknown, not a winner picked from bare samples with no ranking.
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=[0.0] * 24)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.total_mm is None
    assert precip.total_mm != 0.0


def test_no_ranked_model_with_a_full_day_shows_no_rain_forecast() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(conn, pub_id, day_ahead=0, rank=1, feed_id=f, selected=True)
    # Only a partial day: the ranked model never reaches a full day.
    _insert_hourly(
        conn, site_id=site_id, feed_id=f, variable="precip", day=_DAY0, hourly=[1.0] * 5
    )

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.total_mm is None
    assert precip.total_mm != 0.0


# --- C6: before any model has 20 days ---------------------------------------


def test_provisional_ranking_sets_the_rebuilding_extrema_state() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(
        conn,
        pub_id,
        day_ahead=0,
        rank=1,
        feed_id=f,
        selected=True,
        provisional=True,
        counted_days=10,
    )
    hourly = [0.0] * 24
    hourly[0] = 1.0
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=hourly)

    precip = _build(conn, site_id).tiles[0].precip
    assert precip.meta.extrema_state == "rebuilding"
    assert precip.total_mm == pytest.approx(1.0)  # still shows the provisional winner


def test_a_day_ahead_with_no_counted_day_shows_no_rain_forecast() -> None:
    conn, site_id = _make_forecast_db()
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    # day_ahead 0 has a provisional ranked winner; day_ahead 1 has none at
    # all -- each day ahead's tile must reflect only its own ranking.
    _seed_rain_rank(
        conn,
        pub_id,
        day_ahead=0,
        rank=1,
        feed_id=f,
        selected=True,
        provisional=True,
        counted_days=3,
    )
    for day in (_DAY0, _DAY1):
        hourly = [0.0] * 24
        hourly[0] = 1.0
        _full_precip_day(conn, site_id=site_id, feed_id=f, day=day, hourly=hourly)

    tiles = _build(conn, site_id).tiles
    assert tiles[0].precip.total_mm is not None
    assert tiles[1].precip.total_mm is None


def test_forecast_page_shows_ranking_updating_badge_and_no_rain_forecast_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The template half of C6: a real ``/forecast`` page shows the
    "ranking updating" badge on a provisional day and "No rain forecast" on
    a day with no ranking at all, in the same response.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001
    site_id = int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone)"
            " VALUES ('Rain Page Site', 40.0, -105.0, 900.0, 'UTC')"
        ).lastrowid
    )
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(_FIXED_NOW))
    f = _insert_feed(conn, "F")
    pub_id = _seed_publication(conn, site_id=site_id, cutoff=_DAY0)
    _seed_rain_rank(
        conn,
        pub_id,
        day_ahead=0,
        rank=1,
        feed_id=f,
        selected=True,
        provisional=True,
        counted_days=10,
    )
    for day in (_DAY0, _DAY1):
        temp_hourly = [10.0] * 24
        wind_hourly = [5.0] * 24
        _insert_hourly(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="temperature",
            day=day,
            hourly=temp_hourly,
        )
        _insert_hourly(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="wind",
            day=day,
            hourly=wind_hourly,
        )
    hourly = [0.0] * 24
    hourly[0] = 1.0
    _full_precip_day(conn, site_id=site_id, feed_id=f, day=_DAY0, hourly=hourly)
    conn.commit()

    monkeypatch.setattr("wxverify.forecast.service.utc_now", lambda: _FIXED_NOW)
    # build_forecast_page's own `now or utc_now()` reads page.py's own import
    # of utc_now, a separate binding from service.utc_now above -- both must
    # be pinned to the same instant or the page renders against the real
    # wall-clock date instead of _FIXED_NOW.
    monkeypatch.setattr("wxverify.forecast.page.utc_now", lambda: _FIXED_NOW)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}")
    assert response.status_code == 200
    # The day-warning text is sentence-cased ("Ranking updating"), not
    # the old lowercase CSS-badge wording.
    assert "Ranking updating" in response.text
    assert "No rain forecast" in response.text


# ---------------------------------------------------------------------------
# C7: rain nearby (scoring.close_scores.score_day writing rain_days).
# ---------------------------------------------------------------------------

_C7_TARGET = date(2024, 7, 1)
_C7_STATIONS_ADDED = _C7_TARGET - timedelta(days=30)


def _add_stations(
    conn: sqlite3.Connection, site_id: int, names: Iterable[str]
) -> dict[str, int]:
    changed_at = isoformat_utc_micro(
        datetime(
            _C7_STATIONS_ADDED.year,
            _C7_STATIONS_ADDED.month,
            _C7_STATIONS_ADDED.day,
            tzinfo=UTC,
        )
    )
    ids: dict[str, int] = {}
    for name in names:
        cur = conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
            (site_id, f"ISTATION-{name}"),
        )
        station_id = int(cur.lastrowid)
        ids[name] = station_id
        conn.execute(
            "INSERT INTO station_changes"
            " (site_id, station_id, change, effective_date, reason, changed_at)"
            " VALUES (?, ?, 'added', ?, 'test_seed', ?)",
            (site_id, station_id, _C7_STATIONS_ADDED.isoformat(), changed_at),
        )
    return ids


def _close_through_target(conn: sqlite3.Connection, site_id: int, *, target: date):
    first_date = target - timedelta(days=5)
    start_site_days(
        conn,
        site_id,
        first_date,
        now_utc=datetime(first_date.year, first_date.month, first_date.day, tzinfo=UTC),
    )
    far_future = datetime(
        target.year, target.month, target.day, tzinfo=UTC
    ) + timedelta(days=2)
    ensure_site_days(conn, site_id, now_utc=far_future)
    d = first_date
    while d < target:
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d.isoformat()),
        ).fetchone()
        assert row is not None
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=5)
        begin_close(conn, site_id, d, now_utc=t)
        finish_close(conn, site_id, d, now_utc=t)
        d += timedelta(days=1)
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, target.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, target, now_utc=t)
    record = day_record(conn, site_id, target)
    assert record is not None and record.state == "closing"
    return record, t


def _seed_station_rain(
    conn: sqlite3.Connection, station_id: int, day: date, *, hourly: Sequence[float]
) -> None:
    for h, amount in enumerate(hourly):
        valid_at = isoformat_utc(
            datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(hours=h)
        )
        conn.execute(
            "INSERT INTO station_observations"
            " (station_id, variable, valid_at, value, qc_flag, source_raw)"
            " VALUES (?, 'precip', ?, ?, 'ok', 'test')",
            (station_id, valid_at, amount),
        )


def _seed_daily_truth(
    conn: sqlite3.Connection, site_id: int, local_date: date, *, generation_id: int
) -> None:
    start = f"{local_date.isoformat()}T00:00:00Z"
    end = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
        + timedelta(days=1)
    )
    rows = [
        ("temperature_high", 30.0, 24, None, None),
        ("temperature_low", 10.0, 24, None, None),
        ("wind_max", 14.5, 24, None, None),
        ("precip_total", 0.0, 24, 0, 24),
        ("precip_occurrence", 0.0, 24, 0, 24),
    ]
    for quantity, value, covered_hours, wet, dry in rows:
        conn.execute(
            "INSERT INTO daily_truth"
            " (site_id, local_date, quantity, value, eligible, covered_hours,"
            "  expected_slots, wet_hours, dry_hours, rain_threshold_mm,"
            "  day_start_utc, day_end_utc, timezone, tz_generation_id)"
            " VALUES (?, ?, ?, ?, 1, ?, 24, ?, ?, 0.2, ?, ?, 'UTC', ?)",
            (
                site_id,
                local_date.isoformat(),
                quantity,
                value,
                covered_hours,
                wet,
                dry,
                start,
                end,
                generation_id,
            ),
        )


def _rain_day_outcome(
    station_totals: Mapping[str, tuple[float, int]],
    *,
    excluded: frozenset[str] = frozenset(),
) -> str:
    """Build a fresh site with one station per key of ``station_totals``
    (name -> (day total mm, covered hours)), close it through
    :data:`_C7_TARGET`, and return the ``rain_days.outcome`` ``score_day``
    writes for that date. A name in ``excluded`` gets ``rain_counted = 0``
    on the target day's list row, so its rain is set aside.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "rain-nearby-site")
    generation_id = ensure_site_generation(conn, site_id)
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    station_ids = _add_stations(conn, site_id, station_totals.keys())
    record, now_utc = _close_through_target(conn, site_id, target=_C7_TARGET)
    for name, (total, hours) in station_totals.items():
        hourly = [total] + [0.0] * (hours - 1)
        _seed_station_rain(conn, station_ids[name], _C7_TARGET, hourly=hourly)
    for name in excluded:
        conn.execute(
            "UPDATE site_day_stations SET rain_counted = 0"
            " WHERE site_id = ? AND local_date = ? AND station_id = ?",
            (site_id, _C7_TARGET.isoformat(), station_ids[name]),
        )
    _seed_daily_truth(conn, site_id, _C7_TARGET, generation_id=generation_id)
    conn.commit()

    score_day(conn, record=record, attempt=1, truth_version="v1", now_utc=now_utc)

    row = conn.execute(
        "SELECT outcome FROM rain_days WHERE site_id = ? AND local_date = ?",
        (site_id, _C7_TARGET.isoformat()),
    ).fetchone()
    assert row is not None, "score_day did not write a rain_days row"
    return str(row["outcome"])


def test_rain_when_two_stations_reach_0_2mm() -> None:
    outcome = _rain_day_outcome({"A": (0.2, 24), "B": (0.2, 24), "C": (0.1, 24)})
    assert outcome == "rain"


def test_rain_when_one_station_reaches_0_5mm() -> None:
    outcome = _rain_day_outcome({"A": (0.5, 24), "B": (0.1, 24)})
    assert outcome == "rain"


def test_dry_when_the_lone_wet_station_is_0_49mm() -> None:
    outcome = _rain_day_outcome({"A": (0.49, 24), "B": (0.0, 24)})
    assert outcome == "dry"


def test_hundredths_rounding_counts_0_19999999999999996_as_0_2() -> None:
    value = 0.19999999999999996
    outcome = _rain_day_outcome({"A": (value, 24), "B": (value, 24)})
    assert outcome == "rain"


def test_set_aside_station_is_excluded_from_rain_nearby() -> None:
    """Paired positive/negative: with A counted, its lone 0.5 mm is enough
    for 'rain'. Setting A aside (rain_counted = 0, the injected precondition)
    removes that station from the count entirely -- not merely drops its
    value to 0 -- so the remaining counted station (0.1 mm) is 'dry'.
    """
    positive = _rain_day_outcome({"A": (0.5, 24), "B": (0.1, 24)})
    assert positive == "rain"

    negative = _rain_day_outcome(
        {"A": (0.5, 24), "B": (0.1, 24)}, excluded=frozenset({"A"})
    )
    assert negative == "dry"


def test_unknown_when_no_station_reaches_22_covered_hours() -> None:
    """Neither station has 22+ covered hours and neither rain threshold is
    met: the outcome must be 'unknown', never 'dry' (an unknown truth day
    is never counted as dry).
    """
    outcome = _rain_day_outcome({"A": (0.1, 10), "B": (0.1, 10)})
    assert outcome == "unknown"
    assert outcome != "dry"

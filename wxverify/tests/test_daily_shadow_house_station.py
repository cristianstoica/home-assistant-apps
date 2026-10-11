"""House-station report tests (forecast-method plan §15.3, batch 2b-3).

Fixture (synthetic only -- no real station ID, place name, coordinates or
time zone anywhere): one site, one feed, four enabled stations SYN-1..SYN-4,
all at the site's own elevation (100.0 m, matching ``_insert_station``'s
hardcoded ``dem_elevation_m`` -- so the LAPSE adjustment in
``compute_consensus`` is always zero; see consensus.py:132). SYN-1 and
SYN-3 ramp 0.25 C per hour from a 10.0 C midnight base
(00:00=10.0, ..., 23:00=15.75); SYN-2 ramps the same shape from a 12.0 C
base -- exactly +2.0 C above SYN-1/SYN-3 at every hour, as plan §15.3
requires (``_HOUR_STEP_C``, used inside ``_write_station_hours``). The
ramp resets at the next local midnight, so the largest hour-to-hour step
is the 5.75 C drop from 23:00 to 00:00, confirmed under the temperature
QC spike limit of 15.0 C (obs/qc.py:22-27, applied per station against
the immediately preceding ``valid_at`` row in
``write_station_observation``, consensus.py:231-244). SYN-4 gets no
readings at all. Six ``forecast_of_record`` rows
target one day (``_TARGET``) at leads 0-5, built with the app's own
``build_forecast_record`` (via the ``_build_case_row`` helper copied from
``test_daily_shadow_changed_header.py``, since that name is private to that
test module and the house rule forbids importing a leading-underscore name
from another test module). The group's published daily truth for
``_TARGET`` is materialized with ``materialize_daily_truth`` (the app's own
ungated writer, per §10.1's leak-test precedent) rather than
``regenerate_marked_truth`` (which can only rebuild an *existing* stale row,
never create a first one) or a raw INSERT (which would bypass the app's own
truth-shape logic entirely).

Capability check (confirmed, not assumed, via ``_median_mad``'s own body
at consensus.py:75-87, read via the public ``compute_consensus`` wrapper
at consensus.py:132): for three readings with
one MAD floor of 0.5 C (``MAD_FLOORS["temperature"]``), the inlier band is
median +/- 3 * 1.4826 * 0.5 = +/- 2.2239 C. At every hour h, SYN-1/SYN-2/
SYN-3's values (10.0 + 0.25h, 12.0 + 0.25h, 10.0 + 0.25h) median to
SYN-1's value, and SYN-2 sits 2.0 C away, inside that band (|2.0| < 2.2239
-- the band does not depend on the shared hourly shift), so no station is
rejected and the group value is SYN-1's (=SYN-3's) reading every hour --
the value the house station is compared against.
``test_median_mad_keeps_group_value_at_syn1_syn3_despite_syn2_offset``
below proves this directly against the fixture's own numbers (its 00:00
values, 10.0, 12.0, 10.0) before any other test relies on it.

Mutant M40 (named here, not run by this file): "the house station's
values used as the verdict's truth. Applicable, but a patch of several
lines, not a one-line edit: the note's house station is loaded and its
``HouseDaily`` built before ``temperature_lines`` runs, and
``temperature_lines``'s ``observed = published.value`` is replaced by that
station's value for the quantity and target from ``HouseDaily.outcomes``".
Under the correct code, ``temperature_lines`` only ever reads
``ctx.published`` (the group's daily truth) -- ``load_house_station`` is
called in ``_temperature_heldout`` strictly *after* ``lines`` is already
built (daily_shadow.py:6599-6601) -- so
``header.json``/``lines.jsonl`` can never depend on which station is named
house. Under M40, the house station's own 24-hour reading would be used as
each line's truth instead, and SYN-2's and SYN-3's readings were built
exactly 2.0 C apart at every hour, for exactly this reason: a note naming
SYN-2 would produce different ``truth`` values (hence different
``lines.jsonl`` bytes) than one naming SYN-3, where the correct code
produces byte-identical output.
``test_house_report_syn2_vs_syn3_share_header_and_lines``'s ``lines.jsonl``
byte-identity assertion is the one that must fail under M40 (the header
comparison there is a separate, partial check -- see below -- that excludes
only the wall-clock ``replay_time`` block and runs after it); its
docstring repeats the exact divergence.

Matched-line condition checked (``temperature_lines``, daily_shadow.py:2600-2721):
a line is unmatched only when the row is missing, when
``control_value`` (daily_shadow.py:549-589) returns ``Unmatched`` (row not
``recorded``, an unparseable app version, or a null daily quantity), when
F1 fails, or when ``ctx.published`` has no eligible non-stale truth for
(quantity, target). ``TempTraining.truth`` (daily_shadow.py:2415-2436)
reads the training window from ``ctx.published`` and returns a partial
(possibly empty) dict for days with no published truth rather than
raising, and that result never gates whether the *current* line is
matched -- so this fixture does not need daily truth on any day besides
``_TARGET``, only the 6 case rows' own target day. Every case row's
``build_forecast_record`` call gets a complete 24-hour sample set, meeting
``control_value``'s requirements through the proven ``_build_case_row``
pattern.
"""

from __future__ import annotations

import contextlib
import io
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring import consensus as consensus_mod
from wxverify.settings.keys import set_setting
from wxverify.verification import daily_shadow
from wxverify.verification import truth as truth_mod
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc

_TIMEZONE = "UTC"
_TARGET = date(2036, 2, 20)
_TRAINING_DAYS = 35
# The per-hour ramp inside `_write_station_hours`: each station's 00:00
# value is its base below, rising by this much per hour and resetting at
# the next local midnight, so the day's high and low differ -- max/min
# over 24 hourly values, coverage.py:155-166's `_temperature_outcome`.
_HOUR_STEP_C = 0.25
_SYN1_C = 10.0
_SYN2_C = 12.0  # exactly +2.0 C above SYN-1/SYN-3 at every hour, per §15.3
_SYN3_C = 10.0
# The day's 23:00 (highest) and 00:00 (lowest) ramp values for SYN-1/SYN-3's
# base; the group consensus tracks this base every hour (median/MAD keeps
# SYN-1's/SYN-3's value, per the capability check below), so this is also
# the published daily truth under the correct code.
_RAMP_HIGH_C = _SYN1_C + _HOUR_STEP_C * 23
_RAMP_LOW_C = _SYN1_C
_FETCHED_AT = "2030-01-01T00:00:00+00:00"
_COMMIT_SHA = "a" * 40

# Wall-clock replay durations (daily_shadow.py:6596, replay_time_stats at
# :4923) can never be byte-identical between two separate runs. Precedent:
# test_daily_shadow_published_comparison.py:798-800's
# _HEADER_KEYS_ALLOWED_TO_DIFFER.
_HEADER_KEYS_ALLOWED_TO_DIFFER = frozenset({"replay_time"})


# --- Helpers copied from test_daily_shadow_changed_header.py (private to
# that module; copied rather than imported per the house rule in the
# module docstring) -------------------------------------------------------


def _connect(path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, "
        "timezone) VALUES (?, 0.0, 0.0, 100.0, ?)",
        (name, _TIMEZONE),
    )
    return cast(int, cur.lastrowid)


def _make_site(conn: sqlite3.Connection, name: str) -> daily_shadow.Site:
    site_id = _insert_site(conn, name)
    ensure_published_generation(conn, site_id)
    set_setting(conn, "forecast_blend_depth", "1")
    return daily_shadow.load_site(conn, site_id=site_id)


def _insert_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds (source, model, default_subscribed, "
        "fetch_interval_minutes) VALUES ('SYN', ?, 1, 60)",
        (model,),
    )
    return cast(int, cur.lastrowid)


def _insert_station(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    pws_station_id: str,
    enabled: bool = True,
    created_at: str = "2030-01-01T00:00:00.000Z",
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, "
        "dem_elevation_m, enabled, created_at) VALUES (?, ?, 0.0, 0.0, "
        "100.0, ?, ?)",
        (site_id, pws_station_id, enabled, created_at),
    )
    return cast(int, cur.lastrowid)


def _insert_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    value: float,
) -> None:
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    for hour in range(24):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            "INSERT INTO forecast_samples (site_id, feed_id, variable, "
            "issued_at, valid_at, lead_hours, value, source_raw, "
            "model_run_id, fetched_at) VALUES (?, ?, 'temperature', ?, ?, "
            "?, ?, '{}', 'run-ch', ?)",
            (
                site_id,
                feed_id,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


def _build_case_row(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    snapshot_date: date,
    target_date: date,
    value: float,
) -> None:
    issued_day = snapshot_date - timedelta(days=1)
    issued = f"{issued_day.isoformat()}T06:00:00Z"
    fetched = f"{issued_day.isoformat()}T06:05:00Z"
    _insert_day_samples(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=target_date,
        issued_at=issued,
        fetched_at=fetched,
        value=value,
    )
    now = resolve_snapshot_utc(_TIMEZONE, snapshot_date, "07:00")
    build_forecast_record(conn, site_id, snapshot_date.isoformat(), now=now)


def _note(*, site_id: int, house_station: str | None) -> daily_shadow.FreezeNote:
    return daily_shadow.FreezeNote(
        site_id=site_id,
        first_h_date=_TARGET,
        excluded_h_dates=(),
        station_set=(),
        db_import=None,
        manifest={},
        constants={},
        house_station=house_station,
        td_k=daily_shadow.TD_K_DEFAULT,
    )


def _run_heldout(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    note: daily_shadow.FreezeNote,
) -> dict[str, bytes]:
    return daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="heldout",
        first=_TARGET,
        last=_TARGET,
        h_first=_TARGET,
        last_complete=_TARGET,
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        # `main` never passes a non-None house_station for --heldout: it is
        # rejected outside --freeze-prepare (daily_shadow.py:6977-6981), so
        # `main` always calls `run` with house_station=None for --heldout
        # (daily_shadow.py:7072), and `run` reads the house station from
        # `note.house_station` instead (daily_shadow.py:6351-6361). Passing
        # anything else here would exercise a call `main` never makes.
        house_station=None,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": _COMMIT_SHA},
    )


# --- New helpers for this file --------------------------------------------


def _write_station_hours(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    first_day: date,
    last_day: date,
    value: float,
) -> None:
    """Writes one station's hourly readings for every day in [first_day,
    last_day] through the app's own writer, ``insert_station_observation``
    (consensus.py), which both upserts the row and re-materializes that
    hour's group consensus. Mirrors ``test_daily_shadow_leak.py``'s
    ``_write_station_hours`` pattern (copied, not imported, since that name
    is private to that test module); the fetched-at clock must already be
    faked by the caller before this runs.

    ``value`` is the day's 00:00 reading; each later hour adds
    ``_HOUR_STEP_C`` (so the 23:00 reading is ``value + 5.75``), resetting to
    ``value`` at the next local midnight -- a ramp, not a flat day, so the
    day's high and low (the ``max``/``min`` reduction over its hourly
    values, coverage.py:155-166) differ. The largest hour-to-hour step is
    the 5.75 C drop from 23:00 to the next day's 00:00, which stays under
    the temperature QC spike limit, 15.0 C (obs/qc.py:22-27), checked per
    station against the immediately preceding
    ``valid_at`` row (``write_station_observation``, consensus.py:231-244).
    """
    day = first_day
    while day <= last_day:
        for hour in range(24):
            valid_at = isoformat_utc(
                datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
            )
            consensus_mod.insert_station_observation(
                conn,
                station_id=station_id,
                variable="temperature",
                valid_at=valid_at,
                value=value + _HOUR_STEP_C * hour,
                source_raw="synthetic",
            )
        day += timedelta(days=1)


def _populate_fixture(
    conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> tuple[daily_shadow.Site, int]:
    """Builds the §15.3 fixture: site + feed + SYN-1..4 + 36 days of
    station readings for SYN-1/2/3 (none for SYN-4) + 6 case rows at leads
    0-5 targeting ``_TARGET`` + the group's published daily truth for
    ``_TARGET``, all through the app's own writers.
    """

    def _fake_isoformat_utc(value: datetime | None = None) -> str:
        del value
        return _FETCHED_AT

    monkeypatch.setattr(consensus_mod, "isoformat_utc", _fake_isoformat_utc)

    site = _make_site(conn, "house-station-site")
    feed_id = _insert_real_feed(conn, "SYN-house")
    station_ids = {
        pws_station_id: _insert_station(
            conn, site_id=site.site_id, pws_station_id=pws_station_id
        )
        for pws_station_id in ("SYN-1", "SYN-2", "SYN-3", "SYN-4")
    }

    first_day = _TARGET - timedelta(days=_TRAINING_DAYS)
    for pws_station_id, value in (
        ("SYN-1", _SYN1_C),
        ("SYN-2", _SYN2_C),
        ("SYN-3", _SYN3_C),
    ):
        _write_station_hours(
            conn,
            station_id=station_ids[pws_station_id],
            first_day=first_day,
            last_day=_TARGET,
            value=value,
        )
    # SYN-4 (station_ids["SYN-4"]) is left with zero readings on purpose.

    for lead in range(6):
        snapshot_date = _TARGET - timedelta(days=lead)
        _build_case_row(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            snapshot_date=snapshot_date,
            target_date=_TARGET,
            value=_SYN1_C,
        )

    truth_mod.materialize_daily_truth(
        conn, site_id=site.site_id, local_date=_TARGET.isoformat()
    )
    conn.commit()
    return site, feed_id


@pytest.fixture
def house_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[sqlite3.Connection, daily_shadow.Site]:
    conn = _connect()
    site, _feed_id = _populate_fixture(conn, monkeypatch)
    return conn, site


# --- Capability check (confirms the fixture's own numbers before the rest
# of this file depends on them) --------------------------------------------


def test_median_mad_keeps_group_value_at_syn1_syn3_despite_syn2_offset() -> None:
    """From ``_median_mad``'s own formula (consensus.py:75-87), read via the
    public ``compute_consensus`` (consensus.py:132): with SYN-1=10.0,
    SYN-2=12.0, SYN-3=10.0, the median is 10.0 and the inlier
    band is median +/- 3 * 1.4826 * 0.5 = +/- 2.2239 C; |12.0 - 10.0| = 2.0
    is inside that band, so all three stations stay in and the group value
    is 10.0 -- SYN-1's (=SYN-3's) value, not SYN-2's.
    """
    readings = [
        consensus_mod.StationReading(
            station_id=1,
            dem_elevation_m=100.0,
            variable="temperature",
            valid_at="2036-01-01T00:00:00+00:00",
            value=_SYN1_C,
        ),
        consensus_mod.StationReading(
            station_id=2,
            dem_elevation_m=100.0,
            variable="temperature",
            valid_at="2036-01-01T00:00:00+00:00",
            value=_SYN2_C,
        ),
        consensus_mod.StationReading(
            station_id=3,
            dem_elevation_m=100.0,
            variable="temperature",
            valid_at="2036-01-01T00:00:00+00:00",
            value=_SYN3_C,
        ),
    ]
    result = consensus_mod.compute_consensus(
        readings,
        variable="temperature",
        site_elevation_m=100.0,
        mad_floor=consensus_mod.MAD_FLOORS["temperature"],
    )
    assert result is not None
    assert result.value == _SYN1_C
    assert result.n_stations == 3
    assert result.rejected_stations == 0


# --- (a) SYN-2 vs SYN-3: house_station changes the report, never the
# group-truth-derived header/lines -------------------------------------------


def test_house_report_syn2_vs_syn3_share_header_and_lines(
    house_fixture: tuple[sqlite3.Connection, daily_shadow.Site],
) -> None:
    """M40 -> at ``assert files_syn2["lines.jsonl"] == files_syn3["lines.jsonl"]``:
    correct = True (``temperature_lines`` never reads the house station, so
    its output cannot depend on which note names it); mutant = False (under
    M40 each line's truth becomes the house station's own reading --
    SYN-2's fixture-built ramp vs SYN-3's fixture-built ramp, 2.0 C apart
    at every hour -- so the two runs' ``lines.jsonl`` would diverge). The
    header is checked equal
    except the wall-clock ``replay_time`` block (daily_shadow.py:6596,
    ``replay_time_stats`` at :4923, which measures real replay durations
    and can never be byte-identical between two separate runs); M40 is
    killed at the ``lines.jsonl`` assertion, not the header one.
    """
    conn, site = house_fixture
    note_syn2 = _note(site_id=site.site_id, house_station="SYN-2")
    note_syn3 = _note(site_id=site.site_id, house_station="SYN-3")
    files_syn2 = _run_heldout(conn, site=site, note=note_syn2)
    files_syn3 = _run_heldout(conn, site=site, note=note_syn3)

    # The core M40-killing assertion.
    assert files_syn2["lines.jsonl"] == files_syn3["lines.jsonl"]

    # Non-vacuity: parse lines.jsonl (temperature_line_json,
    # daily_shadow.py:3324-3355, keys include quantity/unmatched/truth) and
    # confirm, for each quantity, at least one line is matched (unmatched
    # is null) with a non-null truth -- and that every such truth equals
    # SYN-1's (=SYN-3's) ramp extreme for that quantity: the day's 23:00
    # reading, 15.75, for the high, and its 00:00 reading, 10.0, for the
    # low (the ``max``/``min`` reduction over the group's hourly consensus,
    # coverage.py:155-166's ``_temperature_outcome``, read the same way for
    # truth as for the house station -- truth.py:138-173's ``_evaluate_day``
    # and daily_shadow.py:5350-5400's ``HouseDaily.outcomes``/``.value`` both
    # call ``evaluate_variable``). This is the check that published truth is
    # SYN-1's, not SYN-2's or the house station's. The parsed bytes checked
    # here are identical between the two runs per the assertion above.
    parsed_lines = [
        json.loads(raw) for raw in files_syn2["lines.jsonl"].decode().splitlines()
    ]
    truth_by_quantity = {
        "temperature_high": _RAMP_HIGH_C,
        "temperature_low": _RAMP_LOW_C,
    }
    for quantity in ("temperature_high", "temperature_low"):
        matched = [
            line
            for line in parsed_lines
            if line["quantity"] == quantity
            and line["unmatched"] is None
            and line["truth"] is not None
        ]
        assert len(matched) > 0
        for line in matched:
            assert line["truth"] == truth_by_quantity[quantity]

    # Header equal except the wall-clock replay_time block (daily_shadow.py:
    # 6596, replay_time_stats at :4923 -- real replay durations, which
    # cannot be byte-identical between two separately-invoked runs).
    # Precedent: test_daily_shadow_published_comparison.py:798-800's
    # _HEADER_KEYS_ALLOWED_TO_DIFFER.
    header_syn2 = cast(dict[str, Any], json.loads(files_syn2["header.json"]))
    header_syn3 = cast(dict[str, Any], json.loads(files_syn3["header.json"]))
    assert "replay_time" in header_syn2
    assert "replay_time" in header_syn3
    stripped_header_syn2 = {
        k: v for k, v in header_syn2.items() if k not in _HEADER_KEYS_ALLOWED_TO_DIFFER
    }
    stripped_header_syn3 = {
        k: v for k, v in header_syn3.items() if k not in _HEADER_KEYS_ALLOWED_TO_DIFFER
    }
    assert stripped_header_syn2 == stripped_header_syn3

    house2 = json.loads(files_syn2["house_station.json"])
    house3 = json.loads(files_syn3["house_station.json"])
    assert house2["house_station"] == "SYN-2"
    assert house3["house_station"] == "SYN-3"
    for quantity in ("temperature_high", "temperature_low"):
        # SYN-2 is usable: it is in the station set, has readings, and has
        # one eligible day (_TARGET), so house_unusable returns None.
        assert house2["unusable"][quantity] is None
        # SYN-2's ramp sits exactly _SYN2_C - _SYN1_C (+2.0 C) above SYN-1's
        # (=SYN-3's) ramp at every hour -- same step, different base -- so
        # this difference is the ramp's own shift and holds for both the
        # high (23:00) and the low (00:00): SYN-2's own reading minus the
        # group truth (per the capability check above, the group value
        # tracks SYN-1's/SYN-3's) is +2.0 C, over exactly one eligible date
        # (_TARGET is the only target among the 6 case rows).
        assert house2["agreement"][quantity] == {
            "dates": 1,
            "mean_difference": _SYN2_C - _SYN1_C,
            "days_apart": 1,
        }
        assert house3["unusable"][quantity] is None
        # SYN-3's ramp is identical to SYN-1's (same base, same step), so
        # its own reading minus the group truth is 0.0 at both quantities,
        # and no date is more than 1.0 C apart.
        assert house3["agreement"][quantity] == {
            "dates": 1,
            "mean_difference": _SYN3_C - _SYN1_C,
            "days_apart": 0,
        }


# --- (b) SYN-4: no readings -> unusable, header/lines still shared --------


def test_house_report_syn4_has_no_ok_readings_but_shares_header_and_lines(
    house_fixture: tuple[sqlite3.Connection, daily_shadow.Site],
) -> None:
    """SYN-4 has no readings at all, yet ``header.json``/``lines.jsonl``
    are unaffected (same reasoning as the SYN-2-vs-SYN-3 test above: the
    house station is loaded strictly after ``lines`` is built). The header
    is checked equal except the wall-clock ``replay_time`` block, for the
    same reason as above; ``lines.jsonl`` stays a full byte-identity check.
    """
    conn, site = house_fixture
    note_syn2 = _note(site_id=site.site_id, house_station="SYN-2")
    note_syn4 = _note(site_id=site.site_id, house_station="SYN-4")
    files_syn2 = _run_heldout(conn, site=site, note=note_syn2)
    files_syn4 = _run_heldout(conn, site=site, note=note_syn4)

    header_syn2 = cast(dict[str, Any], json.loads(files_syn2["header.json"]))
    header_syn4 = cast(dict[str, Any], json.loads(files_syn4["header.json"]))
    assert "replay_time" in header_syn2
    assert "replay_time" in header_syn4
    stripped_header_syn2 = {
        k: v for k, v in header_syn2.items() if k not in _HEADER_KEYS_ALLOWED_TO_DIFFER
    }
    stripped_header_syn4 = {
        k: v for k, v in header_syn4.items() if k not in _HEADER_KEYS_ALLOWED_TO_DIFFER
    }
    assert stripped_header_syn2 == stripped_header_syn4
    assert files_syn2["lines.jsonl"] == files_syn4["lines.jsonl"]

    house4 = json.loads(files_syn4["house_station.json"])
    assert house4["house_station"] == "SYN-4"
    for quantity in ("temperature_high", "temperature_low"):
        # SYN-4 was inserted into "stations" (so it is in the station set
        # and found) but never given a single observation: readings=0, and
        # there is exactly one target date among the 6 case rows.
        assert house4["unusable"][quantity] == {
            "reason": "no_ok_readings",
            "readings": 0,
            "days": 1,
        }


# --- (c) a note naming no house station --------------------------------


def _run_main(
    *,
    copy: Path,
    out: Path,
    note_path: Path,
    site_id: int,
    expect_sha256: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[int, str, str, int]:
    """Drives ``daily_shadow.main()`` for real, counting real
    ``open_copy_readonly`` calls so a refusal (zero opens) can be told
    apart from a run that reached the copy (at least one open).
    ``check_commit`` is stubbed to the package's own manifest, ignoring
    ``commit_sha``, since this test's copy is a throwaway fixture, not a
    real git commit -- the same substitution
    ``test_daily_shadow_batch2_mutants.py`` uses for its own
    ``main()``-driven tests (written locally here, not imported, since
    that module's helpers are private to it).
    """

    def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
        del commit_sha
        return daily_shadow.package_manifest(package_dir)

    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))

    open_calls = 0
    real_open = daily_shadow.open_copy_readonly

    def _counting_open(path: Path) -> sqlite3.Connection:
        nonlocal open_calls
        open_calls += 1
        return real_open(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", _counting_open)

    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
        "--variable",
        "temperature",
        "--commit-sha",
        _COMMIT_SHA,
        "--heldout",
        "--freeze-note",
        str(note_path),
        "--expect-sha256",
        expect_sha256,
    ]
    out_buf, err_buf = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
        exit_code = daily_shadow.main(argv)
    return exit_code, out_buf.getvalue(), err_buf.getvalue(), open_calls


def _build_note_file(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    house_station: str | None,
    note_path: Path,
) -> None:
    """Builds a schema-valid freeze-note file by round-tripping through the
    app's own freeze-prepare writer (``daily_shadow.run(mode="freeze-prepare", ...)``),
    then overriding only ``house_station``. This avoids hand-constructing
    the note JSON schema (a private implementation detail of
    ``load_freeze_note``) -- the note's "manifest" is seeded with the real
    package manifest so ``check_note_matches_package`` will accept it.
    """
    real_manifest = daily_shadow.package_manifest(daily_shadow.package_root())
    files = daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="freeze-prepare",
        first=_TARGET,
        last=_TARGET,
        h_first=_TARGET,
        last_complete=_TARGET,
        excluded_h_dates=(),
        t2_last=None,
        note=None,
        house_station=house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": real_manifest, "commit_sha": _COMMIT_SHA},
    )
    draft: dict[str, Any] = json.loads(files["freeze_note.draft.json"])
    draft.pop("draft", None)
    draft["house_station"] = house_station
    note_path.write_text(json.dumps(draft))


def _build_file_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, daily_shadow.Site]:
    copy = tmp_path / "copy.db"
    conn = _connect(str(copy))
    site, _feed_id = _populate_fixture(conn, monkeypatch)
    conn.close()
    return copy, site


def test_main_refuses_heldout_whose_note_names_no_house_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site = _build_file_fixture(tmp_path, monkeypatch)
    before_sha = daily_shadow.sha256_file(copy)

    conn = _connect(str(copy))
    note_path = tmp_path / "note.json"
    _build_note_file(conn, site=site, house_station=None, note_path=note_path)
    conn.close()

    out = tmp_path / "out"
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy,
        out=out,
        note_path=note_path,
        site_id=site.site_id,
        expect_sha256=before_sha,
        monkeypatch=monkeypatch,
    )

    assert exit_code == 2
    assert err_text.strip() == "refused: the freeze note names no house station"
    assert out_text == ""
    # The refusal fires before any copy is ever opened.
    assert open_calls == 0
    assert daily_shadow.sha256_file(copy) == before_sha


def test_main_paired_positive_runs_when_house_station_is_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the refusal above: the identical fixture and
    manifest, differing only in that the note names a real house station,
    must NOT be refused for lacking one, and must actually reach the copy.
    """
    copy, site = _build_file_fixture(tmp_path, monkeypatch)
    before_sha = daily_shadow.sha256_file(copy)

    conn = _connect(str(copy))
    note_path = tmp_path / "note.json"
    _build_note_file(conn, site=site, house_station="SYN-2", note_path=note_path)
    conn.close()

    out = tmp_path / "out"
    exit_code, _out_text, err_text, open_calls = _run_main(
        copy=copy,
        out=out,
        note_path=note_path,
        site_id=site.site_id,
        expect_sha256=before_sha,
        monkeypatch=monkeypatch,
    )

    assert "names no house station" not in err_text
    assert open_calls >= 1
    del exit_code


# --- (c, continued) --counts-only never writes house_station.json ---------


def test_house_report_counts_only_writes_no_house_station_file(
    house_fixture: tuple[sqlite3.Connection, daily_shadow.Site],
) -> None:
    conn, site = house_fixture
    note_syn2 = _note(site_id=site.site_id, house_station="SYN-2")
    files = daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="counts-only",
        first=_TARGET,
        last=_TARGET,
        h_first=_TARGET,
        last_complete=_TARGET,
        excluded_h_dates=(),
        t2_last=None,
        note=note_syn2,
        # As in `_run_heldout`: `main` passes house_station=None outside
        # --freeze-prepare (daily_shadow.py:6977-6981, :7072); the note
        # still names SYN-2.
        house_station=None,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": _COMMIT_SHA},
    )
    assert set(files) == {"header.json"}

"""Persistence's shared pure function (storage-release plan 2026-11-02,
3.5). ``wxverify.scoring.history`` holds the core of ``_persistence_pairs``
(``wxverify/scoring/close_scores.py:676-712``) as a pure function, so
stage 5 (``score_day``) and the later readers (the win rate, the history
chart) share one definition. Behaviour is unchanged.

Test HP3 additionally imports it to cross-check ``score_day``'s stored rows
against the shared function's own output.

The API exposed by ``wxverify.scoring.history``, chosen from what
``_persistence_pairs`` needs (``close_scores.py:676-712``) and from 3.5's
"shortest/longest lead" readers:

    @dataclass(frozen=True, slots=True)
    class PersistenceCell:
        '''One persistence cell: the value ``lead_hours`` before target
        hour ``instant`` stands in for a forecast of ``observed``, at day
        ahead ``day_ahead``.'''
        instant: datetime        # the target hour h, as a parsed instant
        valid_at: str             # h's own stored text, found in ``rows``
        source_valid_at: str      # the source hour's stored text
        lead_hours: int           # L, 1..max_lead_hours
        day_ahead: int            # local date(h) - local date(h - L), in `zone`
        forecast: float           # the value at h - L
        observed: float           # the value at h

    def persistence_cells(
        rows: Iterable[tuple[str, float]],
        *,
        target_valid_at: Iterable[str],
        zone: ZoneInfo,
        max_lead_hours: int = 168,
    ) -> list[PersistenceCell]:
        '''Pure; no I/O, no database. ``rows`` is one hourly variable's
        (``valid_at`` text parseable by ``wxverify.core.timeutil.parse_utc``,
        value) pairs, in any order, any text form, at most one per instant
        in effect (a duplicate instant may overwrite an earlier one). For
        each requested target text in ``target_valid_at`` whose parsed
        instant h has a row, and each lead L from 1 to ``max_lead_hours``
        whose instant h - L also has a row: a cell at day ahead = local
        date(h) in ``zone`` minus local date(h - L) in ``zone``, kept only
        when day ahead is 0..7. Rows and targets are matched by their
        *parsed* instant, never by their literal text -- two text forms of
        one instant (``...Z`` vs ``...+00:00``) are one row, and
        ``PersistenceCell.valid_at``/``source_valid_at`` always carry the
        text found in ``rows``, not the (possibly different) text given in
        ``target_valid_at``. A target with no row at its own instant, or a
        lead with no row at h - L, yields no cell for it -- never an error.
        Returns one ``PersistenceCell`` per (target, lead) kept, in no
        particular order.'''

    def shortest_lead_cells(
        cells: Iterable[PersistenceCell],
    ) -> dict[tuple[datetime, int], PersistenceCell]:
        '''Per (target instant, day ahead), the cell with the smallest
        ``lead_hours`` -- the win rate's pick (3.5). Pure.'''

    def longest_lead_cells(
        cells: Iterable[PersistenceCell],
    ) -> dict[tuple[datetime, int], PersistenceCell]:
        '''Per (target instant, day ahead), the cell with the largest
        ``lead_hours`` -- the history chart's pick (3.5). Pure.'''

Real SQLite throughout for HP3 (``tests.helpers.asof_conn``, real
migrations); nothing in the code under test is mocked. Synthetic data only:
site "history-persistence-site", coordinates 0.0/0.0, variable
``temperature``, zone ``America/New_York`` (already used elsewhere in this
suite for a DST scenario; never a new real-place zone name), transition
date 2026-03-08 (the US spring-forward Sunday --
America/New_York is 23 real hours long that day, 02:00 EST jumps to
03:00 EDT).
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from tests.helpers import asof_conn
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import (
    DAILY_CLOSE_ON_KEY,
    begin_close,
    day_record,
    start_site_days,
)
from wxverify.db.switch import ensure_site_generation
from wxverify.scoring.close_scores import score_day

# ---------------------------------------------------------------------------
# HP1: the pure function -- matched by instant, a DST change, a missing
# source, a lead crossing local midnight.
# ---------------------------------------------------------------------------


def test_hp1_persistence_cells_matched_by_instant_crosses_dst() -> None:
    """HP1: two target hours each get the cells their definition predicts.

    h1 = 2026-03-09T02:00Z: its only source in range is 2026-03-08T22:00Z
    (lead 4). In UTC those are two different calendar dates (8th, 9th), so
    a mutant that diffs UTC dates would say day ahead 1; the zone-correct
    local dates are both 2026-03-08 (America/New_York's spring-forward
    transition is 2026-03-08T07:00Z, 02:00 EST -> 03:00 EDT -- 02:00Z the
    9th is post-transition, offset -4, local 22:00 the 8th; 22:00Z the 8th
    is also post-transition, offset -4, local 18:00 the 8th), so day ahead
    is 0 -- the assertion below pins the zone-correct value, not the UTC
    one.

    h2 = 2026-03-20T04:00Z is exactly local midnight of the 20th (an
    ordinary EDT day, offset -4 throughout, more than 168 hours after h1 so
    neither target's rows leak into the other's lead search): every one of
    its three in-range sources (leads 1, 19, 23) is local date the 19th, so
    day ahead is 1 for each -- a lead that crosses local midnight. Lead 5
    (2026-03-19T23:00Z) has no row: no cell for it.

    The source for h1 (22:00Z the 8th) is given in ``+00:00`` text, and h1
    itself is requested via ``target_valid_at`` in ``+00:00`` text while its
    own row is stored in ``Z`` text -- both must still match by parsed
    instant, and the returned cell's ``valid_at`` must carry the row's own
    ``Z`` text, not the ``+00:00`` lookup text.
    """
    from wxverify.scoring.history import persistence_cells

    rows = [
        ("2026-03-08T22:00:00+00:00", 12.0),  # source of h1, lead 4
        ("2026-03-09T02:00:00Z", 15.0),  # h1
        ("2026-03-19T05:00:00Z", 12.0),  # source of h2, lead 23
        ("2026-03-19T09:00:00Z", 15.0),  # source of h2, lead 19
        ("2026-03-20T03:00:00Z", 18.0),  # source of h2, lead 1
        ("2026-03-20T04:00:00Z", 20.0),  # h2
        # 2026-03-19T23:00:00Z (h2's lead 5) is deliberately absent.
    ]
    target_valid_at = ["2026-03-09T02:00:00+00:00", "2026-03-20T04:00:00Z"]

    cells = persistence_cells(
        rows,
        target_valid_at=target_valid_at,
        zone=ZoneInfo("America/New_York"),
        max_lead_hours=168,
    )

    h1 = parse_utc("2026-03-09T02:00:00Z")
    h2 = parse_utc("2026-03-20T04:00:00Z")
    by_key = {(cell.instant, cell.lead_hours): cell for cell in cells}
    assert len(cells) == 4, (
        f"expected exactly 4 cells (h1 lead 4; h2 leads 1, 19, 23) -- h1's"
        f" only row within 168h is its own lead-4 source, and h2's rows are"
        f" more than 168h after h1's, so neither leaks into the other's"
        f" search; got {len(cells)}: {[(c.instant, c.lead_hours) for c in cells]}"
    )

    h1_cell = by_key[(h1, 4)]
    assert h1_cell.day_ahead == 0, (
        "zone-correct local dates for 2026-03-09T02:00Z and"
        " 2026-03-08T22:00Z are both 2026-03-08 in America/New_York; a"
        " mutant diffing UTC calendar dates (8th vs 9th) would give"
        " day_ahead=1 here"
    )
    assert h1_cell.forecast == 12.0
    assert h1_cell.observed == 15.0
    assert h1_cell.valid_at == "2026-03-09T02:00:00Z", (
        "valid_at must come from the row's own text, not the +00:00 text"
        " used to request this target"
    )
    assert h1_cell.source_valid_at == "2026-03-08T22:00:00+00:00"

    for lead, forecast in ((1, 18.0), (19, 15.0), (23, 12.0)):
        cell = by_key[(h2, lead)]
        assert cell.day_ahead == 1, (
            f"h2 (2026-03-20T04:00Z) is local midnight of the 20th in"
            f" America/New_York; its lead-{lead} source is local date the"
            f" 19th, so day ahead must be 1, not {cell.day_ahead}"
        )
        assert cell.forecast == forecast
        assert cell.observed == 20.0

    assert (h2, 5) not in by_key, (
        "no row exists at h2 - 5h (2026-03-19T23:00Z); persistence_cells"
        " must not invent a cell for a missing source"
    )


def test_hp1b_target_or_lead_with_no_row_yields_no_cells_paired_positive() -> None:
    """HP1 paired positive: a target hour that itself has no row gets no
    cells at all (not an error), while an otherwise-identical target hour
    that DOES have a row, fed the same source rows, gets its cell -- so the
    absence above traces to the missing target row, not to the function
    refusing to produce cells in general.
    """
    from wxverify.scoring.history import persistence_cells

    rows = [
        ("2026-06-01T10:00:00Z", 7.0),  # source, lead 2, for both targets
        ("2026-06-01T12:00:00Z", 9.0),  # the target WITH a row
        # 2026-06-01T12:00:00Z's twin at hour 13 has no row of its own.
    ]
    zone = ZoneInfo("America/New_York")

    with_row = persistence_cells(
        rows, target_valid_at=["2026-06-01T12:00:00Z"], zone=zone
    )
    assert len(with_row) == 1
    assert with_row[0].forecast == 7.0
    assert with_row[0].observed == 9.0

    without_row = persistence_cells(
        rows, target_valid_at=["2026-06-01T13:00:00Z"], zone=zone
    )
    assert without_row == [], (
        "a target with no row at its own instant must yield no cells --"
        " the paired positive above shows the function does produce a cell"
        " once a row exists, so this absence is not just the function"
        " refusing everything"
    )


# ---------------------------------------------------------------------------
# HP2: the shortest/longest lead picks per (target instant, day ahead).
# ---------------------------------------------------------------------------


def test_hp2_shortest_and_longest_lead_cells_grouped_by_day_ahead() -> None:
    """HP2: one target hour with three in-range sources -- leads 1, 23 and
    25 -- where leads 23 and 25 share day ahead 1 (lead 1 alone is day
    ahead 0). The win rate's pick (shortest lead) must choose lead 23 over
    25 for day ahead 1; the history chart's pick (longest lead) must choose
    25 -- and both must agree on day ahead 0, where lead 1 is the only
    candidate.
    """
    from wxverify.scoring.history import (
        longest_lead_cells,
        persistence_cells,
        shortest_lead_cells,
    )

    rows = [
        ("2026-01-09T11:00:00Z", 9.0),  # lead 25 source
        ("2026-01-09T13:00:00Z", 7.0),  # lead 23 source
        ("2026-01-10T11:00:00Z", 5.0),  # lead 1 source
        ("2026-01-10T12:00:00Z", 10.0),  # the target hour h
    ]
    zone = ZoneInfo("America/New_York")  # -5:00, no transition in January

    cells = persistence_cells(rows, target_valid_at=["2026-01-10T12:00:00Z"], zone=zone)
    h = parse_utc("2026-01-10T12:00:00Z")
    assert {cell.lead_hours for cell in cells} == {1, 23, 25}
    assert {cell.day_ahead for cell in cells if cell.lead_hours == 1} == {0}
    assert {cell.day_ahead for cell in cells if cell.lead_hours in (23, 25)} == {1}

    shortest = shortest_lead_cells(cells)
    longest = longest_lead_cells(cells)

    assert shortest[(h, 0)].lead_hours == 1
    assert shortest[(h, 0)].forecast == 5.0
    assert shortest[(h, 1)].lead_hours == 23, (
        "the win rate's pick: of leads 23 and 25 (both day ahead 1), the shortest is 23"
    )
    assert shortest[(h, 1)].forecast == 7.0

    assert longest[(h, 0)].lead_hours == 1
    assert longest[(h, 0)].forecast == 5.0
    assert longest[(h, 1)].lead_hours == 25, (
        "the history chart's pick: of leads 23 and 25 (both day ahead 1),"
        " the longest is 25"
    )
    assert longest[(h, 1)].forecast == 9.0

    assert shortest[(h, 1)].lead_hours != longest[(h, 1)].lead_hours, (
        "paired contrast: the two picks must actually differ here, or this"
        " test could not tell shortest from longest apart"
    )


# ---------------------------------------------------------------------------
# HP3: stage 5 (score_day) unchanged -- persistence's day_scores row, hand
# computed from the definition, across a real DST change.
# ---------------------------------------------------------------------------


def _make_history_site(conn) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('history-persistence-site', 0.0, 0.0, 100.0, 'America/New_York')"
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_history_truth(conn, site_id: int, record, *, generation_id: int) -> None:
    start = isoformat_utc(record.day_start_utc)
    end = isoformat_utc(record.day_end_utc)
    # Temperature only: wind and rain both ineligible, so `variables` from
    # `scored_variables` is exactly {"temperature"} and the only persistence
    # pairs score_day can make are temperature ones (TEMP_TRUTH_MIN_HOURS=18).
    rows = [
        ("temperature_high", 20.0, True, 24, None, None),
        ("temperature_low", 10.0, True, 24, None, None),
        ("wind_max", 14.5, False, 24, None, None),
        ("precip_total", 0.0, False, 24, 0, 24),
        ("precip_occurrence", 0.0, False, 24, 0, 24),
    ]
    for quantity, value, eligible, covered_hours, wet, dry in rows:
        conn.execute(
            "INSERT INTO daily_truth"
            " (site_id, local_date, quantity, value, eligible, covered_hours,"
            "  expected_slots, wet_hours, dry_hours, rain_threshold_mm,"
            "  day_start_utc, day_end_utc, timezone, tz_generation_id)"
            " VALUES (?, ?, ?, ?, ?, ?, 24, ?, ?, 0.25, ?, ?, 'America/New_York', ?)",
            (
                site_id,
                record.local_date.isoformat(),
                quantity,
                value,
                1 if eligible else 0,
                covered_hours,
                wet,
                dry,
                start,
                end,
                generation_id,
            ),
        )


def _seed_history_observation(conn, site_id: int, valid_at: str, value: float) -> None:
    conn.execute(
        "INSERT INTO observations"
        " (site_id, variable, valid_at, value, n_stations, computed_at)"
        " VALUES (?, 'temperature', ?, ?, 2, ?)",
        (site_id, valid_at, value, valid_at),
    )


def test_hp3_stage5_persistence_day_score_matches_hand_computed_definition() -> None:
    """HP3: ``score_day`` over America/New_York's 2026-03-08 spring-forward
    day (the same transition day as HP1's h1), with only two temperature
    observations seeded (22:00Z the day before, value 12.0; 02:00Z the next
    day, value 15.0, which is D's only scored hour). The persistence
    feed's only possible pair is lead 4, day ahead 0 (HP1's own h1 case) --
    the resulting ``day_scores`` row is compared against values computed
    here by hand from the definition (3.5), not against anything the code
    under test produced.

    Also cross-checks the shared pure function directly against the same
    two rows: stage 5 and the function must agree, because stage 5 calls
    it.
    """
    from wxverify.scoring.history import persistence_cells

    conn = asof_conn()
    site_id = _make_history_site(conn)
    generation_id = ensure_site_generation(conn, site_id)
    close_on_at = isoformat_utc(datetime(2026, 1, 1, tzinfo=UTC))
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, close_on_at)

    target_date = date(2026, 3, 8)
    start_site_days(
        conn, site_id, target_date, now_utc=datetime(2026, 3, 8, 7, 0, tzinfo=UTC)
    )
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, target_date.isoformat()),
    ).fetchone()
    assert row is not None
    end_of_day = parse_utc(row["day_end_utc"])
    # Hand computed: America/New_York's 2026-03-08 is 23 real hours (02:00
    # EST jumps to 03:00 EDT at 2026-03-08T07:00Z). Local midnight of the
    # 8th is 00:00 EST = 05:00Z the 8th (day_start_utc below); local
    # midnight of the 9th is 00:00 EDT = 04:00Z the 9th (offset -4, one
    # hour less than the 05:00Z start), so the day ends at 04:00Z the 9th.
    assert end_of_day == datetime(2026, 3, 9, 4, 0, tzinfo=UTC), (
        "fixture guard: America/New_York's 2026-03-08 must end at 04:00Z"
        " the 9th (23 real hours, 02:00 EST jumps to 03:00 EDT) for this"
        " test's hand computation to hold"
    )
    now_utc = end_of_day + timedelta(minutes=30)
    begin_close(conn, site_id, target_date, now_utc=now_utc)
    record = day_record(conn, site_id, target_date)
    assert record is not None and record.state == "closing"
    assert record.day_start_utc == datetime(2026, 3, 8, 5, 0, tzinfo=UTC)

    _seed_history_truth(conn, site_id, record, generation_id=generation_id)
    _seed_history_observation(conn, site_id, "2026-03-08T22:00:00Z", 12.0)
    _seed_history_observation(conn, site_id, "2026-03-09T02:00:00Z", 15.0)

    persistence_id_row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_persistence'"
    ).fetchone()
    assert persistence_id_row is not None, "fixture bug: no seeded persistence feed"
    persistence_id = int(persistence_id_row["id"])

    score_day(conn, record=record, attempt=1, truth_version="hp3-1", now_utc=now_utc)

    day_score_row = conn.execute(
        "SELECT * FROM day_scores WHERE site_id = ? AND local_date = ?"
        " AND feed_id = ? AND variable = 'temperature' AND day_ahead = 0",
        (site_id, target_date.isoformat(), persistence_id),
    ).fetchone()
    assert day_score_row is not None, (
        "fixture bug: expected a persistence/temperature/day_ahead=0 day_scores row"
    )

    # Hand computed from 3.5's definition: the only pair is lead 4,
    # forecast=12.0 (the 22:00Z-the-day-before value), observed=15.0 (D's
    # 02:00Z value); persistence's own ref_sq_error equals its own
    # sq_error, so the skill sums equal the error sums here.
    assert day_score_row["status"] == "scored"
    assert day_score_row["n"] == 1
    assert day_score_row["err_n"] == 1
    assert day_score_row["err_sum"] == -3.0
    assert day_score_row["abs_sum"] == 3.0
    assert day_score_row["sq_sum"] == 9.0
    assert day_score_row["skill_n"] == 1
    assert day_score_row["skill_sq_sum"] == 9.0
    assert day_score_row["skill_ref_sq_sum"] == 9.0

    # forecast_history never holds a persistence row: the cross-check
    # against the shared function's own output is done here directly on
    # the same two observation rows rather than on a stored pair.
    fh_persistence = conn.execute(
        "SELECT 1 FROM forecast_history WHERE site_id = ? AND feed_id = ?",
        (site_id, persistence_id),
    ).fetchone()
    assert fh_persistence is None, (
        "M-fh-persist -> at this assertion: correct = None (persistence is"
        " never written to forecast_history), mutant (persistence pairs"
        " stored like a real feed) = a row present"
    )
    cells = persistence_cells(
        [("2026-03-08T22:00:00Z", 12.0), ("2026-03-09T02:00:00Z", 15.0)],
        target_valid_at=["2026-03-09T02:00:00Z"],
        zone=ZoneInfo("America/New_York"),
    )
    assert len(cells) == 1
    cell = cells[0]
    assert cell.day_ahead == 0, (
        "zone-correct local dates for 2026-03-09T02:00Z and"
        " 2026-03-08T22:00Z are both 2026-03-08 in America/New_York, so"
        " lead 4 is day ahead 0 -- the same cell this test's docstring and"
        " HP1's h1 describe"
    )
    assert cell.lead_hours == 4
    assert cell.forecast == 12.0
    assert cell.observed == 15.0


# ---------------------------------------------------------------------------
# W11: persistence_picks_at (storage-release plan 2026-11-02-storage-release,
# W11) -- held equal, cell for cell, to longest_lead_cells/shortest_lead_cells
# applied to persistence_cells filtered to one day ahead (the rule, plan
# line 351).
# ---------------------------------------------------------------------------


def _w11_reference_picks(rows, target_valid_at, zone, day_ahead, *, longest):
    """The rule's own definition of the correct pick: ``persistence_cells``
    filtered to ``day_ahead``, then ``longest_lead_cells`` /
    ``shortest_lead_cells``, sorted by instant -- what
    ``persistence_picks_at`` must equal for every input (plan line 351)."""
    from wxverify.scoring.history import (
        longest_lead_cells,
        persistence_cells,
        shortest_lead_cells,
    )

    cells = [
        cell
        for cell in persistence_cells(rows, target_valid_at=target_valid_at, zone=zone)
        if cell.day_ahead == day_ahead
    ]
    picked = longest_lead_cells(cells) if longest else shortest_lead_cells(cells)
    return sorted(picked.values(), key=lambda cell: cell.instant)


def test_w11_off_the_hour_target_and_source_matched_by_whole_hour_lead() -> None:
    """Off-the-hour instants, both as the target and as its source: h =
    2026-01-20T12:17:00Z (local 07:17 EST, date the 20th); its only
    whole-hour-separated source at lead 24 is 2026-01-19T12:17:00Z (local
    07:17 EST, date the 19th) -- also off the hour. Day ahead is 1."""
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-01-19T12:17:00Z", 11.0),  # source, lead 24
        ("2026-01-20T12:17:00Z", 14.0),  # target h
    ]
    targets = ["2026-01-20T12:17:00Z"]

    for longest in (False, True):
        expected = _w11_reference_picks(rows, targets, zone, 1, longest=longest)
        actual = persistence_picks_at(
            rows, target_valid_at=targets, zone=zone, day_ahead=1, longest=longest
        )
        assert actual == expected
        assert len(actual) == 1
        assert actual[0].lead_hours == 24
        assert actual[0].forecast == 11.0
        assert actual[0].observed == 14.0


def test_w11_non_whole_hour_candidate_never_picked_over_whole_hour_one() -> None:
    """A row 3h30 before the target is not whole hours away and must never
    be picked, even though the shortest-lead walk reaches it before the
    whole-hour lead-4 source -- the whole-hour test (``lead % hour == 0``)
    must reject it.

    M2 -> at the shortest-lead assertion below: correct = lead_hours == 4
    (the whole-hour candidate at lead 4), mutant (the whole-hour test
    removed) = lead_hours == 3 (``lead // hour`` of the 3h30 decoy), because
    the shortest-lead walk visits the decoy (lead 3h30, the nearer
    candidate) before the lead-4 source, and without the ``continue`` the
    decoy is accepted immediately instead of being skipped.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-01-20T08:30:00Z", 99.0),  # decoy, lead 3h30 -- not whole hours
        ("2026-01-20T08:00:00Z", 7.0),  # the correct source, lead 4
        ("2026-01-20T12:00:00Z", 10.0),  # target h
    ]
    targets = ["2026-01-20T12:00:00Z"]

    expected_shortest = _w11_reference_picks(rows, targets, zone, 0, longest=False)
    actual_shortest = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    assert actual_shortest == expected_shortest
    assert len(actual_shortest) == 1
    assert actual_shortest[0].lead_hours == 4, (
        "the 3h30 decoy must never be picked -- only a whole-hour lead is"
        f" eligible, got lead_hours={actual_shortest[0].lead_hours}"
    )
    assert actual_shortest[0].forecast == 7.0

    expected_longest = _w11_reference_picks(rows, targets, zone, 0, longest=True)
    actual_longest = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=True
    )
    assert actual_longest == expected_longest
    assert len(actual_longest) == 1
    assert actual_longest[0].lead_hours == 4
    assert actual_longest[0].forecast == 7.0


def test_w11_later_row_at_duplicate_instant_text_wins() -> None:
    """Two text forms of one source instant: the later row in ``rows``
    (here the ``+00:00`` form, appended after the ``Z`` form) wins -- its
    value and its own text are what the pick carries, not the earlier
    row's.

    M3 -> at the final two assertions: correct = forecast == 99.0 and
    source_valid_at == "2026-02-02T08:00:00+00:00" (the second, later row
    wins -- a plain dict assignment overwrites), mutant (``setdefault``
    instead of assignment) = forecast == 5.0 and source_valid_at ==
    "2026-02-02T08:00:00Z" (the FIRST row wins instead), diverging because
    this fixture puts two different values on the two duplicate rows.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-02-02T08:00:00Z", 5.0),  # source, earlier text form
        ("2026-02-02T08:00:00+00:00", 99.0),  # source, later text form -- wins
        ("2026-02-02T10:00:00Z", 50.0),  # target h
    ]
    targets = ["2026-02-02T10:00:00Z"]

    expected = _w11_reference_picks(rows, targets, zone, 0, longest=False)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    assert actual == expected
    assert len(actual) == 1
    assert actual[0].forecast == 99.0
    assert actual[0].source_valid_at == "2026-02-02T08:00:00+00:00"


def test_w11_unparseable_row_text_dropped_without_affecting_other_cells() -> None:
    """A row whose text does not parse is silently left out -- it neither
    crashes the function nor shadows a real cell at a nearby instant."""
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("not a timestamp at all", 123.0),
        ("2026-02-05T08:00:00Z", 6.0),  # source, lead 2
        ("2026-02-05T10:00:00Z", 9.0),  # target h
    ]
    targets = ["2026-02-05T10:00:00Z"]

    expected = _w11_reference_picks(rows, targets, zone, 0, longest=False)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    assert actual == expected
    assert len(actual) == 1
    assert actual[0].lead_hours == 2
    assert actual[0].forecast == 6.0


def test_w11_unparseable_target_text_yields_no_cell_paired_positive() -> None:
    """A target whose own text does not parse is simply skipped -- no cell,
    no error -- while a second, well-formed target asked in the SAME call,
    fed the same rows, still gets its cell: the absence traces to the
    target's unparseable text, not to the function refusing everything.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-02-06T08:00:00Z", 4.0),  # source, lead 2
        ("2026-02-06T10:00:00Z", 11.0),  # the good target's row
    ]
    targets = ["also not a timestamp", "2026-02-06T10:00:00Z"]

    expected = _w11_reference_picks(rows, targets, zone, 0, longest=False)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    assert actual == expected
    assert len(actual) == 1, (
        "the unparseable target text must yield no cell, while the"
        " well-formed target in the same call still gets its own -- a"
        " paired positive, not just an absence"
    )
    assert actual[0].forecast == 4.0


def test_w11_target_with_no_row_yields_no_cell_paired_positive() -> None:
    """A target hour with no row of its own gets no cell, while a second
    target hour, one hour later, that DOES have a row, fed the same
    source, gets its own cell in the SAME call -- the absence traces to
    the missing target row, not to the function refusing everything.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-02-07T08:00:00Z", 3.0),  # source, lead 2 for the good target
        ("2026-02-07T10:00:00Z", 12.0),  # the good target's row
        # 2026-02-07T11:00:00Z (the target with no row) is deliberately absent.
    ]
    targets = ["2026-02-07T11:00:00Z", "2026-02-07T10:00:00Z"]

    expected = _w11_reference_picks(rows, targets, zone, 0, longest=False)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    assert actual == expected
    assert len(actual) == 1, (
        "the target with no row of its own must yield no cell, while the"
        " target that DOES have a row still gets its own in the same call"
    )
    assert actual[0].forecast == 3.0


def test_w11_target_asked_twice_is_counted_once() -> None:
    """One target instant given twice in ``target_valid_at`` -- once in
    ``Z`` text, once in ``+00:00`` text -- yields exactly one cell, not
    two."""
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-02-10T07:00:00Z", 2.0),  # source, lead 2
        ("2026-02-10T09:00:00Z", 8.0),  # target h
    ]
    targets = ["2026-02-10T09:00:00Z", "2026-02-10T09:00:00+00:00"]

    expected = _w11_reference_picks(rows, targets, zone, 0, longest=False)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    assert actual == expected
    assert len(actual) == 1, (
        "two text forms of one target instant must be counted once, not twice"
    )
    assert actual[0].forecast == 2.0


def test_w11_dst_spring_forward_23_hour_local_day_lead() -> None:
    """America/New_York's 2026-03-08 is 23 real hours (spring-forward,
    02:00 EST jumps to 03:00 EDT at 2026-03-08T07:00Z). Local midnight of
    the 9th (2026-03-09T04:00Z) and local midnight of the 8th
    (2026-03-08T05:00Z) are a whole-hour lead of exactly 23 -- a bug that
    assumes a 24-hour local day would look for a source at lead 24 and
    miss this one entirely.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-03-08T05:00:00Z", 13.0),  # local midnight the 8th, lead 23
        ("2026-03-09T04:00:00Z", 17.0),  # local midnight the 9th, the target
    ]
    targets = ["2026-03-09T04:00:00Z"]

    expected = _w11_reference_picks(rows, targets, zone, 1, longest=False)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=1, longest=False
    )
    assert actual == expected
    assert len(actual) == 1
    assert actual[0].lead_hours == 23, (
        "the spring-forward day is 23 real hours -- a 24-hour-day"
        f" assumption would miss this source; got lead_hours="
        f"{actual[0].lead_hours}"
    )
    assert actual[0].forecast == 13.0


def test_w11_dst_fall_back_25_hour_local_day_lead() -> None:
    """America/New_York's 2026-11-01 is 25 real hours (fall-back, 02:00
    EDT becomes 01:00 EST at 2026-11-01T06:00Z). Local midnight of the 2nd
    (2026-11-02T05:00Z) and local midnight of the 1st (2026-11-01T04:00Z)
    are a whole-hour lead of exactly 25 -- the mirror of the
    spring-forward case above, in the other direction.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-11-01T04:00:00Z", 21.0),  # local midnight the 1st, lead 25
        ("2026-11-02T05:00:00Z", 24.0),  # local midnight the 2nd, the target
    ]
    targets = ["2026-11-02T05:00:00Z"]

    expected = _w11_reference_picks(rows, targets, zone, 1, longest=True)
    actual = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=1, longest=True
    )
    assert actual == expected
    assert len(actual) == 1
    assert actual[0].lead_hours == 25, (
        "the fall-back day is 25 real hours -- a 24-hour-day assumption"
        f" would miss this source; got lead_hours={actual[0].lead_hours}"
    )
    assert actual[0].forecast == 21.0


def test_w11_lead_exactly_1_and_168_picked_lead_169_never_picked() -> None:
    """Leads of exactly 1 and 168 are picked at their own day ahead (0 and
    7 respectively); a row at lead 169 alone, with no row at lead 168, gets
    no cell at all even though its day ahead (7) would otherwise be valid
    -- the lead cutoff is a hard bound on the lead itself, never inferred
    from the day-ahead range. Checked for both ``longest`` values, since
    the 168 boundary and the 169 cutoff must hold for the longest walk
    too, not just the shortest-lead default.

    M1 -> at the lead-169-alone assertion: correct = [] (lead 169 exceeds
    ``PERSISTENCE_MAX_LEAD_HOURS``), mutant (``max_lead`` widened by one
    hour) = one cell with lead_hours == 169, because this fixture's ONLY
    candidate at day ahead 7 is the lead-169 row -- there is no lead-168
    row in this second row set for the mutant to pick instead.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    target_text = "2026-01-15T12:00:00Z"
    lead1_text = "2026-01-15T11:00:00Z"
    lead168_text = "2026-01-08T12:00:00Z"
    lead169_text = "2026-01-08T11:00:00Z"

    rows_with_168 = [
        (lead1_text, 1.0),
        (lead168_text, 7.0),
        (target_text, 9.0),
    ]
    targets = [target_text]

    for longest in (False, True):
        for day_ahead, lead_hours, value in ((0, 1, 1.0), (7, 168, 7.0)):
            expected = _w11_reference_picks(
                rows_with_168, targets, zone, day_ahead, longest=longest
            )
            actual = persistence_picks_at(
                rows_with_168,
                target_valid_at=targets,
                zone=zone,
                day_ahead=day_ahead,
                longest=longest,
            )
            assert actual == expected
            assert len(actual) == 1
            assert actual[0].lead_hours == lead_hours
            assert actual[0].forecast == value

        rows_only_169 = [
            (lead169_text, 99.0),
            (target_text, 9.0),
        ]
        expected_169 = _w11_reference_picks(
            rows_only_169, targets, zone, 7, longest=longest
        )
        actual_169 = persistence_picks_at(
            rows_only_169,
            target_valid_at=targets,
            zone=zone,
            day_ahead=7,
            longest=longest,
        )
        assert actual_169 == expected_169
        assert actual_169 == [], (
            "a row at lead 169 alone (no lead-168 row present) must yield no"
            " cell, even though day ahead 7 is otherwise valid"
        )


def test_w11_day_ahead_minus_one_and_eight_are_always_empty_paired_positive() -> None:
    """Day ahead -1 and 8 both lie outside the kept range (0-7) and must
    always give an empty list, fed the SAME rows and target that give a
    real cell at day ahead 0 and at day ahead 7 -- so the absence traces
    to the day-ahead bound, not to this fixture producing no cells at
    all.
    """
    from wxverify.scoring.history import persistence_picks_at

    zone = ZoneInfo("America/New_York")
    rows = [
        ("2026-01-15T11:00:00Z", 1.0),  # lead 1, day ahead 0
        ("2026-01-08T12:00:00Z", 7.0),  # lead 168, day ahead 7
        ("2026-01-15T12:00:00Z", 9.0),  # target h
    ]
    targets = ["2026-01-15T12:00:00Z"]

    for day_ahead, expect_empty in ((-1, True), (0, False), (7, False), (8, True)):
        expected = _w11_reference_picks(rows, targets, zone, day_ahead, longest=False)
        actual = persistence_picks_at(
            rows, target_valid_at=targets, zone=zone, day_ahead=day_ahead, longest=False
        )
        assert actual == expected
        assert (actual == []) == expect_empty, (
            f"day ahead {day_ahead}: expected empty={expect_empty}, got {actual}"
        )

    pick_0 = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=0, longest=False
    )
    pick_7 = persistence_picks_at(
        rows, target_valid_at=targets, zone=zone, day_ahead=7, longest=False
    )
    assert pick_0[0].lead_hours == 1
    assert pick_7[0].lead_hours == 168


# ---------------------------------------------------------------------------
# W11 seeded sweep: ~100 made-up row sets around each 2026 DST change,
# checked at every day ahead -1 to 8 with both picks (plan line 354).
# ---------------------------------------------------------------------------

_SWEEP_ZONE = ZoneInfo("America/New_York")
_SWEEP_TRANSITIONS = (
    datetime(2026, 3, 8, 7, 0, tzinfo=UTC),  # spring-forward
    datetime(2026, 11, 1, 6, 0, tzinfo=UTC),  # fall-back
)


def _sweep_text(rng: random.Random, instant: datetime) -> str:
    text = isoformat_utc(instant)
    return text if rng.random() < 0.5 else text.replace("Z", "+00:00")


def _sweep_case(
    rng: random.Random, transition: datetime
) -> tuple[list[tuple[str, float]], list[str]]:
    """One made-up row set anchored near a DST ``transition``: rows from at
    least 8 days before the first target, with gaps, ``:30`` rows,
    duplicate ``Z``/``+00:00`` instants and text that does not parse (plan
    line 354)."""
    first_target = (transition + timedelta(hours=rng.randint(-24, 24))).replace(
        minute=0, second=0, microsecond=0
    )
    days_before = rng.randint(8, 11)
    start = first_target - timedelta(days=days_before)
    span_hours = days_before * 24 + rng.randint(24, 72)

    rows: list[tuple[str, float]] = []
    value = rng.uniform(-10.0, 30.0)
    for offset in range(span_hours):
        hour = start + timedelta(hours=offset)
        value += rng.uniform(-2.0, 2.0)
        if rng.random() < 0.12:  # a gap -- no row at this hour
            continue
        rows.append((_sweep_text(rng, hour), round(value, 2)))
        if rng.random() < 0.08:  # a :30 row
            rows.append(
                (
                    _sweep_text(rng, hour + timedelta(minutes=30)),
                    round(value + rng.uniform(-1, 1), 2),
                )
            )
        if rng.random() < 0.1:  # a duplicate instant, the other text form
            rows.append((_sweep_text(rng, hour), round(value + rng.uniform(-1, 1), 2)))
    if rng.random() < 0.3:  # text that does not parse, as a row
        rows.append((f"not a real timestamp {rng.random()}", 0.0))

    targets: list[str] = [isoformat_utc(first_target)]
    for _ in range(rng.randint(1, 3)):
        offset_hours = rng.randint(0, span_hours - 1)
        minute = 30 if rng.random() < 0.2 else 0
        targets.append(
            _sweep_text(rng, start + timedelta(hours=offset_hours, minutes=minute))
        )
    if rng.random() < 0.3:  # text that does not parse, as a target
        targets.append(f"also not a real timestamp {rng.random()}")
    return rows, targets


def test_w11_sweep_matches_reference_across_dst_changes() -> None:
    """About 100 made-up row sets, half anchored near each 2026 DST change,
    compared against the rule's reference pick at every day ahead -1 to 8,
    both the shortest and longest pick, cell for cell, in instant order
    (plan line 354). Also checks that the sweep's own generator exercises
    a non-empty pick at every day ahead 0-7, for both picks -- otherwise
    the day-ahead comparisons above could pass vacuously on an all-empty
    sweep.
    """
    from wxverify.scoring.history import persistence_picks_at

    rng = random.Random(20261102)
    zone = _SWEEP_ZONE
    missing = {
        (day_ahead, longest) for day_ahead in range(0, 8) for longest in (False, True)
    }

    for case_index in range(100):
        transition = _SWEEP_TRANSITIONS[case_index % 2]
        rows, targets = _sweep_case(rng, transition)
        for day_ahead in range(-1, 9):
            for longest in (False, True):
                expected = _w11_reference_picks(
                    rows, targets, zone, day_ahead, longest=longest
                )
                actual = persistence_picks_at(
                    rows,
                    target_valid_at=targets,
                    zone=zone,
                    day_ahead=day_ahead,
                    longest=longest,
                )
                assert actual == expected, (
                    f"sweep case {case_index}, day_ahead={day_ahead},"
                    f" longest={longest}: persistence_picks_at must equal"
                    f" longest_lead_cells/shortest_lead_cells of"
                    f" persistence_cells filtered to this day ahead, cell"
                    f" for cell, in instant order"
                )
                if actual:
                    missing.discard((day_ahead, longest))

    assert not missing, (
        "the sweep must hit a non-empty pick at every day ahead 0-7 for"
        f" both the shortest and longest pick; never hit: {sorted(missing)}"
    )

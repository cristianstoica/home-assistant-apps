"""Item D1 -- ``scripts/icon_eu_post_release_check.py`` (plan §8.6/§4.8).

Owner-run, read-only coverage check for the ``icon_eu`` feed, run manually
7 days after release. No network: it only reads ``forecast_samples`` joined
to ``feeds`` from a Database Export copy. Covers the grouping/aging rules
(``_is_forward``, ``qualifying_fetches``), the per-fetch completeness rule
(``_is_full``), the side-window predicate (``in_side_window``), the pooled
verdict (``verdict``), and ``main`` end-to-end against a real on-disk
SQLite file (never ``:memory:`` -- ``_read_rows`` opens the path by URI).

Synthetic data only: the site below uses a synthetic mid-latitude
coordinate, never a real station location.
"""

from __future__ import annotations

import ast
import hashlib
import os
import re
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import scripts.icon_eu_coverage_check as _coverage_check
from scripts.icon_eu_post_release_check import (
    MIN_HOURS,
    MIN_IN_WINDOW_FETCHES,
    Fetch,
    Row,
    _is_forward,
    _is_full,
    in_side_window,
    main,
    qualifying_fetches,
    verdict,
)
from tests.helpers import asof_make_site
from wxverify.db.migrations import run_migrations

# ---------------------------------------------------------------------------
# Shared fixture plumbing.
# ---------------------------------------------------------------------------

_SITE_ID = 1
_MARKER_DIR_NAME = "d1-t7b-marker-wfpq"

_OUTPUT_TEMPLATES = [
    re.compile(p)
    for p in (
        r"post-release fetch: site \d+ fetched \d{4}-\d{2}-\d{2}T\d{2}:\d{2}"
        r" label \d{4}-\d{2}-\d{2}T\d{2}:\d{2} window (yes|no) full (yes|no)"
        r" hours \d+ max lead \d+",
        r"post-release qualifying fetches: \d+",
        r"post-release in-window fetches: \d+",
        r"post-release excluded labels: \d+",
        r"post-release verdict: (PASS|FAIL|CAN'T TELL)"
        r" \((coverage ok|short coverage|too few samples)\)",
        r"error: database unreadable",
    )
]


def _assert_allowed_output(text: str) -> None:
    for line in text.splitlines():
        if not line:
            continue
        assert any(template.fullmatch(line) for template in _OUTPUT_TEMPLATES), line


def _assert_no_leak(text: str, db_path: Path) -> None:
    assert "://" not in text
    assert _MARKER_DIR_NAME not in text
    assert str(db_path) not in text


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _marker_db(tmp_path: Path) -> Path:
    marker_dir = tmp_path / _MARKER_DIR_NAME
    marker_dir.mkdir(exist_ok=True)
    return marker_dir / "export.db"


def _stamp(dt: datetime) -> str:
    """Exactly the shape ``invalid_forecast_sample_sql`` accepts."""
    return (
        f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
        f"T{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}Z"
    )


#: Production stores leads 1..120 for icon_eu (``OPEN_METEO_MAX_LEAD_HOURS``).
#: A literal, not ``MIN_HOURS``-derived, so a fixture built from it reaches
#: a max lead >= 100 -- the only way a "full if max lead >= 100" mutant (P2b)
#: can diverge from the real, pure count/whole-hour/no-gap rule.
_FULL_HOURS = 120


def _full_rows(
    *,
    label: datetime,
    fetched: datetime,
    hours: int = _FULL_HOURS,
    skip_hour: int | None = None,
    skip_variable: str = "temperature",
    omit_variable: str | None = None,
) -> list[Row]:
    """Rows for one fetch, full coverage of ``hours`` contiguous whole hours
    for every variable, unless ``skip_hour`` (1-based) is omitted for
    ``skip_variable`` -- used to build a short-coverage fixture -- or
    ``omit_variable`` is dropped from the fetch entirely."""
    rows: list[Row] = []
    for variable in ("temperature", "wind", "precip"):
        if variable == omit_variable:
            continue
        for hour in range(1, hours + 1):
            if variable == skip_variable and hour == skip_hour:
                continue
            valid = label + timedelta(hours=hour)
            rows.append(
                (
                    _SITE_ID,
                    _stamp(label),
                    _stamp(valid),
                    variable,
                    hour,
                    _stamp(fetched),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# `in_side_window` -- exact minute boundaries, every window.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        (1, 0, True),  # 01:00 -- window start, inclusive
        (2, 29, True),  # 90-minute window: still inside at +89m
        (2, 30, False),  # 02:30 -- window end, exclusive
        (0, 59, False),
        (7, 0, True),
        (8, 30, False),
        (13, 0, True),
        (14, 30, False),
        (19, 0, True),
        (20, 30, False),
        (12, 0, False),  # clearly outside every window
    ],
)
def test_in_side_window_boundaries(hour: int, minute: int, expected: bool) -> None:
    now = datetime(2026, 6, 1, hour, minute, 0, tzinfo=UTC)
    assert in_side_window(now) is expected


# ---------------------------------------------------------------------------
# `_is_forward` -- age gate, with a paired positive so the gate's absence
# branch isn't ambiently true.
# ---------------------------------------------------------------------------


def test_is_forward_true_just_under_the_forward_age() -> None:
    issued = _stamp(datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC))
    fetched = _stamp(datetime(2026, 6, 1, 11, 59, 0, tzinfo=UTC))  # 11h59m < 12h
    row: Row = (_SITE_ID, issued, "2026-06-01T06:00:00Z", "temperature", 6, fetched)
    assert _is_forward(row) is True


def test_is_forward_false_at_or_past_the_forward_age() -> None:
    # Paired negative: the same construction, aged past the 12h line, is a
    # backfill/catch-up row and must be excluded from forward grouping.
    issued = _stamp(datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC))
    fetched = _stamp(datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))  # exactly 12h
    row: Row = (_SITE_ID, issued, "2026-06-01T06:00:00Z", "temperature", 6, fetched)
    assert _is_forward(row) is False


def test_is_forward_true_when_either_stamp_is_unparsable() -> None:
    # Kept (not dropped silently) so an unparsable group surfaces later as
    # an excluded label, rather than vanishing from the count entirely.
    row: Row = (
        _SITE_ID,
        "not-a-date",
        "2026-06-01T06:00:00Z",
        "temperature",
        6,
        "also-bad",
    )
    assert _is_forward(row) is True


# ---------------------------------------------------------------------------
# `_is_full` -- count, whole-hour, and no-gap requirements, each pinned
# with a paired positive that would fail if the check were loosened.
# ---------------------------------------------------------------------------


def _hourly(start: datetime, count: int) -> list[datetime]:
    return [start + timedelta(hours=h) for h in range(count)]


def test_is_full_true_at_exactly_the_minimum_hour_count() -> None:
    stamps = _hourly(datetime(2026, 6, 1, tzinfo=UTC), MIN_HOURS)
    assert _is_full(stamps) is True


def test_is_full_false_one_short_of_the_minimum() -> None:
    stamps = _hourly(datetime(2026, 6, 1, tzinfo=UTC), MIN_HOURS - 1)
    assert _is_full(stamps) is False


def test_is_full_false_on_a_single_internal_gap() -> None:
    # Same count as the passing case, but one hour skipped and one hour
    # added past the far end -- count matches, span does not.
    stamps = _hourly(datetime(2026, 6, 1, tzinfo=UTC), MIN_HOURS + 1)
    del stamps[50]
    assert len(stamps) == MIN_HOURS
    assert _is_full(stamps) is False


def test_is_full_true_with_no_gap_at_the_same_count() -> None:
    # Paired positive for the gap check: identical count, contiguous span.
    stamps = _hourly(datetime(2026, 6, 1, tzinfo=UTC), MIN_HOURS)
    assert _is_full(stamps) is True


def test_is_full_false_on_a_non_whole_hour_stamp() -> None:
    stamps = _hourly(datetime(2026, 6, 1, tzinfo=UTC), MIN_HOURS)
    stamps[0] = stamps[0].replace(minute=30)
    assert _is_full(stamps) is False


# ---------------------------------------------------------------------------
# `qualifying_fetches` -- grouping, exclusion, and the forward-age drop.
# ---------------------------------------------------------------------------


def test_qualifying_fetches_groups_one_fetch_from_its_rows() -> None:
    label = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    fetched = datetime(2026, 6, 1, 1, 0, 0, tzinfo=UTC)
    rows = _full_rows(label=label, fetched=fetched)
    fetches, excluded = qualifying_fetches(rows)
    assert excluded == 0
    assert len(fetches) == 1
    assert fetches[0].full is True
    assert fetches[0].site_id == _SITE_ID


def test_qualifying_fetches_excludes_a_label_with_two_distinct_fetch_times() -> None:
    label = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    fetched_a = datetime(2026, 6, 1, 1, 0, 0, tzinfo=UTC)
    fetched_b = datetime(2026, 6, 1, 1, 5, 0, tzinfo=UTC)
    rows = _full_rows(label=label, fetched=fetched_a, hours=2)
    rows += [
        (
            _SITE_ID,
            _stamp(label),
            _stamp(label + timedelta(hours=3)),
            "temperature",
            3,
            _stamp(fetched_b),
        ),
    ]
    fetches, excluded = qualifying_fetches(rows)
    assert fetches == []
    assert excluded == 1


def test_qualifying_fetches_does_not_exclude_a_single_fetch_time_label() -> None:
    # Paired positive: the same shape, one fetch time, is not excluded.
    label = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    fetched = datetime(2026, 6, 1, 1, 0, 0, tzinfo=UTC)
    rows = _full_rows(label=label, fetched=fetched, hours=2)
    fetches, excluded = qualifying_fetches(rows)
    assert excluded == 0
    assert len(fetches) == 1


def test_qualifying_fetches_drops_an_aged_group_without_counting_it_excluded() -> None:
    # A backfill row (fetched >= 12h after issued) is not forward, so its
    # group never forms at all -- distinct from "excluded" (which counts
    # groups that DID form but couldn't be resolved).
    label = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    fetched = label + timedelta(hours=24)
    rows = _full_rows(label=label, fetched=fetched, hours=2)
    fetches, excluded = qualifying_fetches(rows)
    assert fetches == []
    assert excluded == 0


def test_qualifying_fetches_keeps_a_fresh_group_at_the_same_label() -> None:
    # Paired positive for the age-drop above: identical label, forward age.
    label = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    fetched = label + timedelta(hours=1)
    rows = _full_rows(label=label, fetched=fetched, hours=2)
    fetches, _excluded = qualifying_fetches(rows)
    assert len(fetches) == 1


def test_qualifying_fetches_orders_by_fetch_time_then_site() -> None:
    later = datetime(2026, 6, 2, 1, 0, 0, tzinfo=UTC)
    earlier = datetime(2026, 6, 1, 1, 0, 0, tzinfo=UTC)
    rows = _full_rows(
        label=datetime(2026, 6, 2, 0, 0, 0, tzinfo=UTC), fetched=later, hours=2
    )
    rows += _full_rows(
        label=datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC), fetched=earlier, hours=2
    )
    fetches, _excluded = qualifying_fetches(rows)
    assert [fetch.fetched for fetch in fetches] == sorted(
        fetch.fetched for fetch in fetches
    )


# ---------------------------------------------------------------------------
# `verdict` -- the pooled, priority-ordered rule.
# ---------------------------------------------------------------------------


def _fetch(*, full: bool, in_window: bool) -> Fetch:
    return Fetch(
        site_id=_SITE_ID,
        label=datetime(2026, 6, 1, tzinfo=UTC),
        fetched=datetime(2026, 6, 1, 1, 0, tzinfo=UTC),
        in_window=in_window,
        full=full,
        hours=MIN_HOURS,
        max_lead=MIN_HOURS,
    )


def test_verdict_fail_outranks_too_few_in_window() -> None:
    # One short fetch among plenty of in-window full ones still FAILs --
    # short coverage is checked before the in-window count at all.
    fetches = [_fetch(full=True, in_window=True) for _ in range(10)]
    fetches.append(_fetch(full=False, in_window=True))
    assert verdict(fetches) == ("FAIL", "short coverage", 1)


def test_verdict_pass_at_exactly_the_in_window_minimum() -> None:
    fetches = [_fetch(full=True, in_window=True) for _ in range(MIN_IN_WINDOW_FETCHES)]
    assert verdict(fetches) == ("PASS", "coverage ok", 0)


def test_verdict_cant_tell_one_short_of_the_in_window_minimum() -> None:
    # Paired with the PASS case above at the exact boundary: one fewer
    # in-window fetch, same full coverage, flips PASS to CAN'T TELL.
    fetches = [
        _fetch(full=True, in_window=True) for _ in range(MIN_IN_WINDOW_FETCHES - 1)
    ]
    assert verdict(fetches) == ("CAN'T TELL", "too few samples", 3)


def test_verdict_cant_tell_on_no_fetches_at_all() -> None:
    assert verdict([]) == ("CAN'T TELL", "too few samples", 3)


# ---------------------------------------------------------------------------
# `main` end-to-end -- a real on-disk SQLite file, opened read-only by URI.
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path, *, db_path: Path | None = None) -> tuple[Path, int, int]:
    """A real schema-migrated on-disk database with a site and the
    already-seeded icon_eu feed row. Returns (path, site_id, feed_id)."""
    db_path = db_path if db_path is not None else tmp_path / "export.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "Icon-EU Post-Release Site")
    feed_row = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND model='icon_eu'"
    ).fetchone()
    assert feed_row is not None
    feed_id = int(feed_row["id"])
    conn.commit()
    conn.close()
    return db_path, site_id, feed_id


def _insert_rows(db_path: Path, site_id: int, feed_id: int, rows: list[Row]) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.executemany(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, 1.0, '{}', 'run-x', ?)
        """,
        [(site_id, feed_id, row[3], row[1], row[2], row[4], row[5]) for row in rows],
    )
    conn.commit()
    conn.close()


def _in_window_fetches(count: int, *, site_id: int) -> list[Row]:
    rows: list[Row] = []
    for i in range(count):
        label = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(days=i)
        # 01:30 -- 1.5 h after the label (plan §8.6's realistic F-L gap),
        # still inside the first window ([01:00, 02:30)).
        fetched = label + timedelta(hours=1, minutes=30)
        for variable in ("temperature", "wind", "precip"):
            for hour in range(1, _FULL_HOURS + 1):
                valid = label + timedelta(hours=hour)
                rows.append(
                    (
                        site_id,
                        _stamp(label),
                        _stamp(valid),
                        variable,
                        hour,
                        _stamp(fetched),
                    )
                )
    return rows


def test_main_pass_prints_the_pass_verdict_and_exits_0(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Plan §8.6 line 783: the 8 full in-window fetches are spread over TWO
    # sites, sharing the same labels and the same fetched_at values. A
    # script that groups fetches by label alone (dropping site_id from the
    # key), or that counts in-window fetches per site instead of pooling
    # across sites, still passes a single-site fixture -- this one does not.
    db_path = _marker_db(tmp_path)
    db_path, site_a, feed_id = _make_db(tmp_path, db_path=db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        site_b = asof_make_site(conn, "Icon-EU Post-Release Site B")
        conn.commit()
    finally:
        conn.close()
    per_site = MIN_IN_WINDOW_FETCHES // 2
    rows_a = _in_window_fetches(per_site, site_id=site_a)
    rows_b = _in_window_fetches(per_site, site_id=site_b)
    _insert_rows(db_path, site_a, feed_id, rows_a)
    _insert_rows(db_path, site_b, feed_id, rows_b)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 0
    assert "post-release qualifying fetches: 8" in out
    assert "post-release in-window fetches: 8" in out
    assert "post-release excluded labels: 0" in out
    assert "post-release verdict: PASS (coverage ok)" in out
    assert out.count(f"post-release fetch: site {site_a} ") == per_site
    assert out.count(f"post-release fetch: site {site_b} ") == per_site


def test_main_cant_tell_one_fetch_short_of_the_in_window_minimum(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Paired with the PASS case: identical fixture shape, one fewer
    # qualifying in-window fetch.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES - 1, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 3
    assert "post-release verdict: CAN'T TELL (too few samples)" in out


def test_main_fail_on_an_interior_gap_at_full_120h_scale(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The PASS set, plus one otherwise-full 120 h fetch missing precip hour
    # 60 -- the gap this leaves behind must still FAIL even though every
    # fetch, including the short one, reaches a max lead of 120 (>= 100).
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES, site_id=site_id)
    short_label = datetime(2026, 7, 1, tzinfo=UTC)
    short_fetched = short_label + timedelta(hours=1, minutes=30)
    rows += _full_rows(
        label=short_label,
        fetched=short_fetched,
        skip_variable="precip",
        skip_hour=60,
    )
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 1
    assert "post-release verdict: FAIL (short coverage)" in out


def test_main_literal_96_vs_95_hour_fetches_not_min_hours_derived(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Two hand-built fetches sized by LITERAL row counts (96 and 95), never
    # by importing MIN_HOURS itself: a mutant that resizes MIN_HOURS would
    # also resize a fixture built FROM that constant, hiding the mutant.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows: list[Row] = []
    full_label = datetime(2026, 6, 1, tzinfo=UTC)
    full_fetched = full_label + timedelta(hours=1)
    for variable in ("temperature", "wind", "precip"):
        for hour in range(1, 97):  # literal 96 rows
            rows.append(
                (
                    site_id,
                    _stamp(full_label),
                    _stamp(full_label + timedelta(hours=hour)),
                    variable,
                    hour,
                    _stamp(full_fetched),
                )
            )
    short_label = datetime(2026, 6, 2, tzinfo=UTC)
    short_fetched = short_label + timedelta(hours=1)
    for variable in ("temperature", "wind", "precip"):
        for hour in range(1, 96):  # literal 95 rows
            rows.append(
                (
                    site_id,
                    _stamp(short_label),
                    _stamp(short_label + timedelta(hours=hour)),
                    variable,
                    hour,
                    _stamp(short_fetched),
                )
            )
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 1
    assert "full yes hours 95 max lead 96" in out
    assert "full no hours 94 max lead 95" in out


def test_main_unreadable_db_exits_2_with_an_error_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "does-not-exist.db"
    code = main(["--db", str(missing)])
    out, err = capsys.readouterr()
    assert (code, out, err) == (2, "", "error: database unreadable\n")


def test_main_prints_exactly_one_fetch_line_per_qualifying_fetch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(3, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    _code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert out.count("post-release fetch:") == 3
    assert "post-release qualifying fetches: 3" in out
    assert "post-release in-window fetches: 3" in out
    assert "post-release excluded labels: 0" in out


def test_main_never_prints_the_db_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The docstring promises no fixed line carries the database path, a
    # coordinate, or a station name -- only the stable summary fields do.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(1, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    _code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert str(db_path) not in out


# ---------------------------------------------------------------------------
# Window parity (plan §8.6): this script's own `in_side_window` must agree
# with the coverage script's, for both an aware-UTC and a naive datetime
# under a non-UTC system zone. The post-release script does not import the
# coverage script (that would load httpx) -- only this TEST file imports
# both.
# ---------------------------------------------------------------------------


def test_in_side_window_matches_the_coverage_scripts_copy_aware_and_naive() -> None:
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Etc/GMT+7"
    time.tzset()
    try:
        for minute_of_day in range(24 * 60):
            hour, minute = divmod(minute_of_day, 60)
            aware = datetime(2026, 6, 2, hour, minute, tzinfo=UTC)
            naive = datetime(2026, 6, 2, hour, minute)
            post_release_aware = in_side_window(aware)
            coverage_aware = _coverage_check.in_side_window(aware)
            assert post_release_aware == coverage_aware, (hour, minute)
            post_release_naive = in_side_window(naive)
            coverage_naive = _coverage_check.in_side_window(naive)
            assert post_release_naive == coverage_naive == post_release_aware, (
                hour,
                minute,
            )
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time.tzset()


# ---------------------------------------------------------------------------
# `_is_full` -- the plan's exact "whole hours" microsecond/second-boundary
# unit test, in addition to the (differently-constructed) minute-shift case
# above. `main` can never reach a fractional-second `valid_at` in practice --
# `invalid_forecast_sample_sql`'s FORECAST_TIMESTAMP_LIKE shape check filters
# it at the SQL layer first -- so this is a direct unit test of the helper,
# not an end-to-end case.
# ---------------------------------------------------------------------------


def test_is_full_whole_hours_microsecond_and_second_shifts() -> None:
    base = _hourly(datetime(2026, 6, 1, tzinfo=UTC), MIN_HOURS)
    assert _is_full(base) is True

    shifted_micro = [stamp.replace(microsecond=500000) for stamp in base]
    assert _is_full(shifted_micro) is False

    shifted_second = [stamp.replace(second=1) for stamp in base]
    assert _is_full(shifted_second) is False


# ---------------------------------------------------------------------------
# Import allowlist (§4.9): stdlib-only, plus `wxverify.collection
# .forecast_validation` and `wxverify.core.timeutil`. `httpx` must not be
# imported at all -- the post-release script's defining property, stronger
# than the coverage script's §4.7 rule.
# ---------------------------------------------------------------------------

_ALLOWED_WXVERIFY_MODULES = {
    "wxverify.collection.forecast_validation",
    "wxverify.core.timeutil",
}


def test_import_allowlist_static() -> None:
    import sys

    source = Path("scripts/icon_eu_post_release_check.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wxverify_imports: set[str] = set()
    other_top_level: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith("wxverify"):
                wxverify_imports.add(node.module)
            else:
                other_top_level.add(node.module.split(".")[0])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("wxverify"):
                    wxverify_imports.add(alias.name)
                else:
                    other_top_level.add(alias.name.split(".")[0])
    assert wxverify_imports == _ALLOWED_WXVERIFY_MODULES
    assert other_top_level <= set(sys.stdlib_module_names)
    assert "httpx" not in other_top_level


#: §4.9's complete transitive closure: the two allowed modules plus
#: everything they are documented to pull in. As in the coverage script's
#: test, a baseline-subtraction diff cannot see a module one of the
#: ALLOWED modules itself transitively loads; this subset check can.
_ALLOWED_TRANSITIVE_MODULES = {
    "wxverify",
    "wxverify.core",
    "wxverify.core.timeutil",
    "wxverify.collection",
    "wxverify.collection.forecast_validation",
    "wxverify.settings",
    "wxverify.settings.depth",
    "wxverify.settings.keys",
}

_FORBIDDEN_SUBTREES = (
    "wxverify.worker",
    "wxverify.db",
    "wxverify.collection.budget",
    "wxverify.api",
)


def test_import_allowlist_runtime_never_loads_httpx(tmp_path: Path) -> None:
    import ast as ast_module
    import subprocess
    import sys

    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys\n"
        "import scripts.icon_eu_post_release_check  # noqa: F401\n"
        "loaded = sorted(\n"
        "    m for m in sys.modules\n"
        "    if m == 'wxverify' or m.startswith('wxverify.')\n"
        ")\n"
        "print(loaded)\n"
        "print('httpx' in sys.modules)\n",
        encoding="utf-8",
    )
    env = dict(os.environ, PYTHONPATH=str(Path.cwd()))
    result = subprocess.run(
        [sys.executable, str(probe)],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    lines = result.stdout.strip().splitlines()
    loaded = set(ast_module.literal_eval(lines[0]))
    httpx_loaded = lines[1] == "True"
    # Everything the script's import brings into sys.modules must be inside
    # the §4.9 transitive closure.
    assert loaded <= _ALLOWED_TRANSITIVE_MODULES
    for module in loaded:
        for forbidden in _FORBIDDEN_SUBTREES:
            assert module != forbidden
            assert not module.startswith(forbidden + ".")
    assert httpx_loaded is False


# ---------------------------------------------------------------------------
# The remaining §8.6 end-to-end cases, each with the per-case
# SHA-256-of-main-db-file-unchanged proof and the output-allowlist/no-leak
# checks.
# ---------------------------------------------------------------------------


def _run_main_verified(
    db_path: Path, argv: list[str], capsys: pytest.CaptureFixture[str]
) -> tuple[int, str, str]:
    before = _hash_file(db_path)
    code = main(argv)
    out, err = capsys.readouterr()
    assert _hash_file(db_path) == before
    _assert_allowed_output(out)
    _assert_allowed_output(err)
    _assert_no_leak(out, db_path)
    _assert_no_leak(err, db_path)
    return code, out, err


def test_main_fail_on_a_fetch_missing_the_wind_variable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The PASS set, plus one extra full-length (120 h) fetch missing wind
    # entirely. Both fetches reach a max lead of 120 (>= 100): under a
    # mutant that reports full whenever max(leads) >= 100 (P2b), this one
    # fetch would be wrongly treated as full even with wind absent, hiding
    # the gap the real, pure count/whole-hour/no-gap rule correctly catches.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES, site_id=site_id)
    no_wind_label = datetime(2026, 7, 1, tzinfo=UTC)
    no_wind_fetched = no_wind_label + timedelta(hours=1, minutes=30)
    rows += _full_rows(
        label=no_wind_label, fetched=no_wind_fetched, omit_variable="wind"
    )
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 1
    assert "post-release verdict: FAIL (short coverage)" in out


def test_main_pass_with_repeated_labels_under_two_fetch_times_excluded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Plan §8.6 line 787: the PASS set plus one (site, label) holding a
    # full run under one fetched_at and, under a SECOND fetched_at, three
    # precip rows starting two hours after that run's last hour. The whole
    # label is excluded (two distinct fetched_at values under one label);
    # qualifying fetches stays at the base 8 and the verdict is PASS.
    # Splitting the group by fetched_at (P3a) would keep the 3-row precip
    # fetch as its own, far-too-short fetch -> FAIL. Merging the two,
    # keeping the latest fetched_at (P3c), would keep all members under one
    # label -> precip then has leads 1..120 plus 122..124, a gap at 121 ->
    # not full -> FAIL. Either mutant diverges from the correct PASS here.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES, site_id=site_id)
    tainted_label = datetime(2026, 9, 1, tzinfo=UTC)
    first_fetched = tainted_label + timedelta(hours=1, minutes=30)
    rows += _full_rows(label=tainted_label, fetched=first_fetched)
    second_fetched = tainted_label + timedelta(hours=5)
    for offset in (122, 123, 124):  # two hours after the full run's last hour
        rows.append(
            (
                site_id,
                _stamp(tainted_label),
                _stamp(tainted_label + timedelta(hours=offset)),
                "precip",
                offset,
                _stamp(second_fetched),
            )
        )
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 0
    assert "post-release excluded labels: 1" in out
    assert "post-release qualifying fetches: 8" in out
    assert "post-release verdict: PASS (coverage ok)" in out


def test_main_cant_tell_with_in_window_fetches_plus_outside_window_ones(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # One short of the in-window minimum (7), plus one full fetch outside
    # any side window: it counts toward qualifying fetches but not toward
    # the in-window total, so the pool stays one short -- CAN'T TELL, not
    # PASS.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES - 1, site_id=site_id)
    outside_label = datetime(2026, 8, 1, tzinfo=UTC)
    outside_fetched = outside_label + timedelta(hours=5)  # 05:00, no window
    rows += _full_rows(label=outside_label, fetched=outside_fetched)
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 3
    assert "post-release qualifying fetches: 8" in out
    assert "post-release in-window fetches: 7" in out
    assert "post-release verdict: CAN'T TELL (too few samples)" in out


def test_main_cant_tell_with_short_historical_rows_ignored(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES - 1, site_id=site_id)
    # A backfill/history fetch (fetched >= 24h after issued) is short-fetched
    # on purpose -- it must never even form a group, so it cannot help reach
    # the 8-fetch minimum.
    history_label = datetime(2026, 9, 1, tzinfo=UTC)
    history_fetched = history_label + timedelta(hours=25)
    rows += _full_rows(label=history_label, fetched=history_fetched, hours=10)
    _insert_rows(db_path, site_id, feed_id, rows)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 3
    assert "post-release qualifying fetches: 7" in out


def test_main_cant_tell_with_short_rows_from_another_feed_ignored(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES - 1, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    # Rows under a DIFFERENT feed (same site) -- the QUERY join on
    # f.model='icon_eu' AND f.source='open-meteo' must exclude these.
    conn = sqlite3.connect(str(db_path))
    other_feed_id = conn.execute(
        "SELECT id FROM feeds WHERE source != 'open-meteo' OR model != 'icon_eu'"
        " LIMIT 1"
    ).fetchone()
    assert other_feed_id is not None
    other_feed_id = int(other_feed_id[0])
    other_label = datetime(2026, 10, 1, tzinfo=UTC)
    other_fetched = other_label + timedelta(hours=1)
    other_rows = _full_rows(label=other_label, fetched=other_fetched)
    conn.executemany(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, 1.0, '{}', 'run-x', ?)
        """,
        [
            (site_id, other_feed_id, row[3], row[1], row[2], row[4], row[5])
            for row in other_rows
        ],
    )
    conn.commit()
    conn.close()
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 3
    assert "post-release qualifying fetches: 7" in out


def test_main_fail_on_an_implausible_value_removed_mid_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The forecast_validation row filter removes a temperature=71 row from
    # the middle of a run -- the gap this leaves behind must still FAIL.
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES, site_id=site_id)
    bad_label = datetime(2026, 11, 1, tzinfo=UTC)
    bad_fetched = bad_label + timedelta(hours=1)
    bad_rows = _full_rows(label=bad_label, fetched=bad_fetched)
    _insert_rows(db_path, site_id, feed_id, rows)
    conn = sqlite3.connect(str(db_path))
    for row in bad_rows:
        value = 71.0 if row[3] == "temperature" and row[4] == 50 else 1.0
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (site_id, feed_id, row[3], row[1], row[2], row[4], value, row[5]),
        )
    conn.commit()
    conn.close()
    code, _out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 1


def test_main_cant_tell_with_no_rows_at_all(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, _site_id, _feed_id = _make_db(tmp_path, db_path=db_path)
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 3
    assert "post-release qualifying fetches: 0" in out


def test_main_fail_on_a_short_fetch_outside_the_window(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES, site_id=site_id)
    short_label = datetime(2026, 12, 1, tzinfo=UTC)
    short_fetched = short_label + timedelta(hours=5)  # outside any window
    rows += _full_rows(label=short_label, fetched=short_fetched, skip_hour=50)
    _insert_rows(db_path, site_id, feed_id, rows)
    code, _out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 1


def test_main_cant_tell_with_null_fetched_at_rows_ignored(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES - 1, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    conn = sqlite3.connect(str(db_path))
    null_label = datetime(2027, 1, 1, tzinfo=UTC)
    null_rows = _full_rows(label=null_label, fetched=null_label + timedelta(hours=1))
    conn.executemany(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, 1.0, '{}', 'run-x', NULL)
        """,
        [(site_id, feed_id, row[3], row[1], row[2], row[4]) for row in null_rows],
    )
    conn.commit()
    conn.close()
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 3
    assert "post-release qualifying fetches: 7" in out


def test_main_pass_with_an_unparsable_fetched_at_label_excluded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(MIN_IN_WINDOW_FETCHES, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    # Well-formed issued_at/valid_at/lead_hours/value so the SQL row filter
    # keeps these rows, but fetched_at is the literal text "not a time" --
    # qualifying_fetches must exclude this label, not crash.
    conn = sqlite3.connect(str(db_path))
    bad_label = datetime(2027, 2, 1, tzinfo=UTC)
    bad_rows = _full_rows(label=bad_label, fetched=bad_label + timedelta(hours=1))
    conn.executemany(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, 1.0, '{}', 'run-x', 'not a time')
        """,
        [(site_id, feed_id, row[3], row[1], row[2], row[4]) for row in bad_rows],
    )
    conn.commit()
    conn.close()
    code, out, _err = _run_main_verified(db_path, ["--db", str(db_path)], capsys)
    assert code == 0
    assert "post-release excluded labels: 1" in out
    assert "post-release qualifying fetches: 8" in out
    assert out.count("post-release fetch:") == 8


def test_main_db_path_with_embedded_nul_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker_dir = tmp_path / _MARKER_DIR_NAME
    marker_dir.mkdir(exist_ok=True)
    bad_path = str(marker_dir) + "\x00x"
    code = main(["--db", bad_path])
    assert code == 2
    err = capsys.readouterr().err
    assert "error: database unreadable" in err
    _assert_allowed_output(err)


def test_main_path_resolve_oserror_exits_2(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _marker_db(tmp_path)
    db_path, site_id, feed_id = _make_db(tmp_path, db_path=db_path)
    rows = _in_window_fetches(1, site_id=site_id)
    _insert_rows(db_path, site_id, feed_id, rows)
    before = _hash_file(db_path)

    class _BoomPath(type(Path())):  # type: ignore[misc]
        def resolve(self, strict: bool = False) -> _BoomPath:  # noqa: ARG002
            raise OSError("synthetic resolve failure")

    monkeypatch.setattr("scripts.icon_eu_post_release_check.Path", _BoomPath)
    code = main(["--db", str(db_path)])
    assert code == 2
    assert _hash_file(db_path) == before
    err = capsys.readouterr().err
    assert "error: database unreadable" in err
    _assert_allowed_output(err)

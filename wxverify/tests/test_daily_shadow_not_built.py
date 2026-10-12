"""§15.3 "What stage one lacks is named" (plan :1399).

Stage one only: temperature's ``--freeze-prepare`` header names, in
``not_built``, at least A3, A7, A12, F2, A11's temporary-file size and D2
option C, and holds a ``not_built`` naming D2 option C in
``tradeoffs.tables["low=C"]``; rain's ``--freeze-prepare`` header names at
least A6 at the offered values of w_amount and w_r, A7, A16, A17, F2 and
A11's temporary-file size, each by its A-number or ``F2``. Rain's header
also names the interval hold-out by ``rain_holdout``, standing for it,
``exclusion_table``, ``holdout_comparison`` and ``holdout_partial_days``.
Temperature's ``--heldout`` half -- its header's A11/D2-option-C
``not_built`` entries and the ``not_built`` in ``sensitivities["low=C"]``
-- is pinned separately, in
``test_daily_shadow_heldout_wiring.py``'s
``test_heldout_not_built_names_a11_and_d2_option_c``. Stage two replaces
this test with one that runs the same modes, finds no ``not_built`` key at
any depth, and finds rain's four hold-out keys present.
"""

from __future__ import annotations

import contextlib
import io
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.aggregate import displayed_daily
from wxverify.verification import daily_shadow
from wxverify.verification.record import resolve_snapshot_utc

COMMIT_SHA = "a" * 40
_DAY = date(2035, 6, 10)


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(tmp_path: Path, *, name: str) -> Path:
    """A real, fully-migrated copy holding one site, no feeds or stations.

    Mirrors ``test_daily_shadow_output_folder.py``'s ``_make_copy``: an
    empty-but-migrated copy is enough for a temperature ``--freeze-prepare``
    run to complete (no training history, no stations). Rain needs one
    matched record row (``_add_one_matched_rain_row`` below), since
    ``rain_s0`` requires at least one rain record row to exist.
    """
    copy = tmp_path / f"{name}.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, f"not-built-{name}-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    return copy


def _add_one_matched_rain_row(copy: Path) -> None:
    """One lead-0 rain row, mapped (0.16.7) and matched, per :1300/T4."""
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=1)
        feed_id = asof_make_real_feed(conn, "model-a")
        day_before = _DAY - timedelta(days=1)
        issued_at = f"{day_before.isoformat()}T06:00:00Z"
        fetched_at = f"{day_before.isoformat()}T06:05:00Z"
        values = [0.5] * 6 + [0.0] * 18
        issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
        for hour, value in enumerate(values):
            valid = datetime(_DAY.year, _DAY.month, _DAY.day, hour, tzinfo=UTC)
            lead = max(1, int((valid - issued).total_seconds() // 3600))
            conn.execute(
                """
                INSERT INTO forecast_samples
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     value, source_raw, model_run_id, fetched_at)
                VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-x', ?)
                """,
                (
                    site.site_id,
                    feed_id,
                    issued_at,
                    isoformat_utc(valid),
                    lead,
                    value,
                    fetched_at,
                ),
            )
        displayed = displayed_daily("precip", [values], rain_threshold_mm=0.2)
        assert displayed["total_mm"] is not None
        assert displayed["wet_hours"] is not None
        snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", _DAY, "07:00"))
        daily_quantities = json.dumps(
            {
                "displayed": {
                    "total_mm": displayed["total_mm"],
                    "wet_hours": displayed["wet_hours"],
                    "extrema_feed_ids": [feed_id],
                }
            }
        )
        conn.execute(
            """
            INSERT INTO forecast_of_record
                (site_id, tz_generation_id, timezone, snapshot_local_date,
                 snapshot_local_time, snapshot_utc, target_local_date, variable,
                 display_lead, status, write_path, policy, methodology_version,
                 app_version, candidates, daily_quantities)
            VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                    'on_time', '{"rain_threshold_mm": 0.2}', 1, '0.16.7', NULL, ?)
            """,
            (
                site.site_id,
                site.generation_id,
                _DAY.isoformat(),
                snapshot_utc,
                _DAY.isoformat(),
                daily_quantities,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _run_main(
    *, copy: Path, out: Path, variable: str, monkeypatch: pytest.MonkeyPatch
) -> tuple[int, str, str]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        "1",
        "--out",
        str(out),
        "--variable",
        variable,
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _header_from_run(
    *, copy: Path, out: Path, variable: str, monkeypatch: pytest.MonkeyPatch
) -> dict[str, object]:
    exit_code, out_text, err_text = _run_main(
        copy=copy, out=out, variable=variable, monkeypatch=monkeypatch
    )
    assert exit_code == 0, err_text
    lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(lines) == 1
    payload = json.loads(lines[0][len(daily_shadow.RESULT_PREFIX) :])
    run_dir = out / cast(str, payload["run_dir"])
    return cast("dict[str, object]", json.loads((run_dir / "header.json").read_text()))


def test_temperature_freeze_prepare_not_built_names_the_missing_a_numbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Temperature's ``not_built`` names A3, A7, A12, F2, A11's file size and
    D2 option C, and names D2 option C again in ``tradeoffs.tables["low=C"]``.

    Each assertion fails if that entry is dropped from ``not_built`` --
    there is no way for this test to pass with any one of A3, A7, A12, F2,
    A11's temporary-file-size entry or D2 option C missing from the
    top-level list, or with D2 option C missing from the tradeoffs table.
    """
    copy = _make_copy(tmp_path, name="temperature")
    out = tmp_path / "out"
    header = _header_from_run(
        copy=copy, out=out, variable="temperature", monkeypatch=monkeypatch
    )
    not_built_raw = header["not_built"]
    assert isinstance(not_built_raw, list)
    not_built = cast("list[object]", not_built_raw)
    joined = "|".join(str(entry) for entry in not_built)
    assert "A3" in joined
    assert "A7" in joined
    assert "A12" in joined
    assert "F2" in joined
    assert "D2 option C" in joined
    a11_entries = [entry for entry in not_built if "A11" in str(entry)]
    assert a11_entries, not_built
    assert "SQLITE_TMPDIR" in str(a11_entries[0])

    tradeoffs_raw = header["tradeoffs"]
    assert isinstance(tradeoffs_raw, dict)
    tradeoffs = cast("dict[str, object]", tradeoffs_raw)
    tables_raw = tradeoffs["tables"]
    assert isinstance(tables_raw, dict)
    tables = cast("dict[str, object]", tables_raw)
    low_c_raw = tables["low=C"]
    assert isinstance(low_c_raw, dict)
    low_c = cast("dict[str, object]", low_c_raw)
    assert "D2 option C" in str(low_c["not_built"])


def test_rain_freeze_prepare_not_built_names_the_missing_a_numbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rain's ``not_built`` names A6/A7/A16/A17/F2/A11 and rain_holdout.

    Each assertion fails if that entry is dropped from ``not_built`` -- in
    particular a mutant that drops A17 alone, or A6's w_amount/w_r wording
    alone, fails this test where a looser "some entries present" check would
    not. The same holds for the ``rain_holdout`` entry, which stands for
    ``exclusion_table``, ``holdout_comparison`` and
    ``holdout_partial_days`` (plan :1399) and also names ``--rain-holdout``
    and ``interval_holdout`` (plan :952).
    """
    copy = _make_copy(tmp_path, name="precip")
    _add_one_matched_rain_row(copy)
    out = tmp_path / "out"
    header = _header_from_run(
        copy=copy, out=out, variable="precip", monkeypatch=monkeypatch
    )
    not_built_raw = header["not_built"]
    assert isinstance(not_built_raw, list)
    not_built = cast("list[object]", not_built_raw)
    joined = "|".join(str(entry) for entry in not_built)
    assert "A7" in joined
    assert "A16" in joined
    assert "A17" in joined
    assert "F2" in joined
    a6_entries = [entry for entry in not_built if "A6" in str(entry)]
    assert a6_entries, not_built
    assert "w_amount" in str(a6_entries[0])
    assert "w_r" in str(a6_entries[0])
    a11_entries = [entry for entry in not_built if "A11" in str(entry)]
    assert a11_entries, not_built
    assert "SQLITE_TMPDIR" in str(a11_entries[0])
    holdout_entries = [entry for entry in not_built if "rain_holdout" in str(entry)]
    assert holdout_entries, not_built
    assert "exclusion_table" in str(holdout_entries[0])
    assert "holdout_comparison" in str(holdout_entries[0])
    assert "holdout_partial_days" in str(holdout_entries[0])
    assert "--rain-holdout" in str(holdout_entries[0])
    assert "interval_holdout" in str(holdout_entries[0])


@pytest.mark.skip(
    reason="deferred to stage two (plan §15.3): replaces the stage-one "
    "not_built tests (here and test_heldout_not_built_names_a11_and_d2_"
    "option_c); runs temperature's --freeze-prepare and --heldout and "
    "rain's --freeze-prepare, walks every header's nested objects and "
    "lists, finds no not_built key at any depth, and finds each named "
    "report present (D2 option C's table and sensitivity included) with "
    "rain's four hold-out keys (rain_holdout, exclusion_table, "
    "holdout_comparison, holdout_partial_days)"
)
def test_no_not_built_key_at_any_depth_deferred_to_stage_two() -> None:
    raise NotImplementedError("Stage-two acceptance test not implemented")


@pytest.mark.skip(
    reason="deferred to stage two (plan §13.3, §15.3): tests "
    "sqlite_tmp_peak.py's run function against small child processes "
    "(measured/not_measured verdicts, lsof field parsing, exit codes); "
    "A11's temporary-file size, which that tool yields, stays named in "
    "not_built until then"
)
def test_sqlite_tmp_peak_measurement_deferred_to_stage_two() -> None:
    raise NotImplementedError("Stage-two acceptance test not implemented")

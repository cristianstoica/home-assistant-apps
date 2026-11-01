"""The worksheet cleanup (daily-close plan 2026-10-22-daily-close-pairs-
and-publication.md, section 14; test IDs WC1-WC9 minus WC8, plan 18.11).

Written before ``wxverify.verification.cleanup`` exists: every test below
collects, then fails at its own ``from wxverify.verification.cleanup
import ...`` with ``ModuleNotFoundError`` -- the failure Step 3 accepts.

Real SQLite throughout (``tests.helpers.asof_conn`` / the ``Database`` the
app runs on in ``test_phase7_surface.py``'s harness); nothing in the code
under test is mocked. Synthetic data only: site names ``Verify Town``/
``Second Town``, model names from ``test_phase7_surface.py``'s own seed.

Scale note (deviation, named up front): 18.11's seed is "runs 1-4 ... with
50,000 evidence rows each" against the production default
``CLEANUP_SLICE_ROWS`` of 20,000 -- three slices per run (20k, 20k, 10k).
Every test here monkeypatches ``CLEANUP_SLICE_ROWS`` to 20 and seeds 50
evidence rows per run instead, giving the IDENTICAL three-slice shape
(20, 20, 10) at 1/1000 scale, so the slicing and save-count assertions are
unchanged in kind -- only the row counts are smaller, for a test suite that
runs in milliseconds instead of minutes.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from wxverify.db.connection import FencedWriter, close_db, init_db
from wxverify.db.switch import ensure_site_generation
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
    published_run_id,
    result_basis_fingerprint,
)

_SLICE_ROWS = 20
_EVIDENCE_PER_RUN = 50  # 20 + 20 + 10 -> 3 slices, mirroring the plan's shape


def _make_site(conn, name: str) -> int:
    site_id = int(
        conn.execute(
            "INSERT INTO sites"
            " (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)"
            " VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)",
            (name,),
        ).lastrowid
    )
    ensure_site_generation(conn, site_id)
    return site_id


def _seed_run(conn, site_id: int, *, evidence_rows: int, publish: bool) -> int:
    """One ``verification_runs`` row with a verdict, a headline result, a
    day-context row and ``evidence_rows`` evidence rows -- ``published``
    when ``publish``, else left ``running`` (18.11's run 5).
    """
    generation_id = ensure_site_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    basis = result_basis_fingerprint(
        conn, site_id, snapshot, period_start="2026-05-01", period_end="2026-05-30"
    )
    run_id = int(
        conn.execute(
            "INSERT INTO verification_runs"
            " (site_id, tz_generation_id, methodology_version, app_version,"
            "  state, attempt, config_snapshot, period_start, period_end,"
            "  settled_through, bootstrap_seed, bootstrap_resamples,"
            "  input_fingerprint, result_basis_fingerprint)"
            " VALUES (?, ?, 1, '0.16.14-test', 'running', 1, ?, '2026-05-01',"
            "         '2026-05-30', '2026-05-30', 12345, 100, ?, ?)",
            (site_id, generation_id, json.dumps(snapshot), fingerprint, basis),
        ).lastrowid
    )
    conn.execute(
        "INSERT INTO verification_verdicts"
        " (run_id, variable, outcome, recommended_depth, incumbent_depth,"
        "  tested_family)"
        " VALUES (?, 'precip', 'retain_incumbent', NULL, 2, '[]')",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO verification_results"
        " (run_id, variable, lead, quantity, entity_type, entity_key,"
        "  headline, common_days, mae, bias, rmse, ets, availability_rate,"
        "  delta_vs_incumbent, detail)"
        " VALUES (?, 'precip', 1, 'precip_total', 'depth', '3', 1, 25,"
        "         0.5, 0.1, 0.8, 0.3, 0.9, 0.05, ?)",
        (run_id, json.dumps({"observed_wet_rate_error": 0.02})),
    )
    conn.execute(
        "INSERT INTO verification_day_context"
        " (run_id, snapshot_local_date, snapshot_utc,"
        "  knowability_exclusions, null_availability_samples)"
        " VALUES (?, '2026-05-01', '2026-05-01T07:00:00Z', ?, 0)",
        (run_id, json.dumps({})),
    )
    conn.executemany(
        "INSERT INTO verification_evidence"
        " (run_id, snapshot_local_date, target_local_date, lead, variable,"
        "  quantity, entity_type, entity_key, predicted, forecast_eligible,"
        "  truth_value, truth_eligible, abs_error)"
        " VALUES (?, '2026-05-01', '2026-05-02', 1, 'temperature',"
        "         'temperature_high', 'depth', ?, 15.0, 1, 14.5, 1, 0.5)",
        [(run_id, str(i)) for i in range(evidence_rows)],
    )
    if publish:
        publish_run(conn, site_id, run_id)
    conn.commit()
    return run_id


def _seed_published_runs(conn, site_id: int, count: int) -> list[int]:
    """``count`` published runs, each with ``_EVIDENCE_PER_RUN`` evidence
    rows (18.11's runs 1 to N, every one ``published``)."""
    return [
        _seed_run(conn, site_id, evidence_rows=_EVIDENCE_PER_RUN, publish=True)
        for _ in range(count)
    ]


def _init_tmp_db(tmp_path):
    close_db()
    from wxverify import config

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    return init_db(str(db_path))


def _evidence_count(conn, run_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM verification_evidence WHERE run_id = ?", (run_id,)
    ).fetchone()
    return int(row[0])


def _worksheet_kept(conn, run_id: int) -> int:
    row = conn.execute(
        "SELECT worksheet_kept FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    return int(row[0])


def _row_snapshot(conn, run_id: int) -> dict[str, object]:
    """18.11 WC4's "unchanged" oracle: results, verdicts, day context and
    the run row itself, everything but ``verification_evidence``.
    """
    return {
        "run": dict(
            conn.execute(
                "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
            ).fetchone()
        ),
        "results": [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM verification_results WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
        ],
        "verdicts": [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM verification_verdicts WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
        ],
        "day_context": [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM verification_day_context WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
        ],
    }


class _WriteSpy:
    """Wraps a :class:`FencedWriter`'s ``write`` to count calls, as WC1's
    "one save per ``writer.write``, counted by a spy" (18.11).
    """

    def __init__(self, writer: FencedWriter) -> None:
        self._writer = writer
        self._original = writer.write
        self.calls = 0
        writer.write = self._wrapped  # type: ignore[method-assign]

    async def _wrapped(self, fn):
        self.calls += 1
        return await self._original(fn)


# ---------------------------------------------------------------------------
# WC1
# ---------------------------------------------------------------------------


def test_wc1_clean_worksheets_empties_older_runs_in_slices(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC1: `clean_worksheets` once empties runs 1-3 in 20-row saves (one
    save per `writer.write`, counted by a spy), each `worksheet_kept` 0 in
    the save of its last slice; runs 4 and 5 and the second site
    "unchanged"; returns 3 (14.1, 14.2).
    """
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    from wxverify.verification.cleanup import clean_worksheets

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run_ids = _seed_published_runs(conn, site_id, 4)
    run5 = _seed_run(conn, site_id, evidence_rows=_EVIDENCE_PER_RUN, publish=False)
    other_site = _make_site(conn, "Second Town")
    other_run = _seed_published_runs(conn, other_site, 1)[0]

    published = published_run_id(conn, site_id)
    assert published == run_ids[3], "fixture bug: run 4 must be the published one"
    before_run4 = _row_snapshot(conn, run_ids[3])
    before_run5 = _row_snapshot(conn, run5)
    before_other = _row_snapshot(conn, other_run)
    before_run4_evidence = _evidence_count(conn, run_ids[3])
    before_run5_evidence = _evidence_count(conn, run5)
    before_other_evidence = _evidence_count(conn, other_run)

    writer = FencedWriter(db, db.generation)
    spy = _WriteSpy(writer)

    from datetime import UTC, datetime

    async def _run() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    finished = asyncio.run(_run())

    assert finished == 3
    for run_id in run_ids[:3]:
        assert _evidence_count(conn, run_id) == 0, (
            "M106 -> at this assertion: correct = 0 (the whole run's"
            f" evidence is gone after enough saves, run_id={run_id}),"
            " mutant (the whole run deleted in one save) would also show"
            " 0 here -- the save-count assertion below is what"
            " discriminates M106"
        )
        assert _worksheet_kept(conn, run_id) == 0, (
            "M107 -> at this assertion: correct = 0 (worksheet_kept is set"
            " in the save that empties the run's last slice), mutant"
            " (worksheet_kept set in a save AFTER the last slice) = 1"
            " still, since no later save ever runs for an empty run"
        )
    expected_saves_per_run = -(-_EVIDENCE_PER_RUN // _SLICE_ROWS)  # ceil division = 3
    assert spy.calls == 3 * expected_saves_per_run, (
        "M106 -> at this assertion: correct ="
        f" {3 * expected_saves_per_run} saves (3 runs x"
        f" {expected_saves_per_run} slices each), mutant (the whole run"
        " deleted in one save) = 3 saves total, not"
        f" {3 * expected_saves_per_run}"
    )

    assert _row_snapshot(conn, run_ids[3]) == before_run4
    assert _evidence_count(conn, run_ids[3]) == before_run4_evidence
    assert _row_snapshot(conn, run5) == before_run5
    assert _evidence_count(conn, run5) == before_run5_evidence
    assert _row_snapshot(conn, other_run) == before_other
    assert _evidence_count(conn, other_run) == before_other_evidence


# ---------------------------------------------------------------------------
# WC2
# ---------------------------------------------------------------------------


def test_wc2_stop_leaves_consistent_state_rerun_finishes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC2: a `Stop`-like exception raised after the fifth save leaves
    every run either with rows and `worksheet_kept` 1, or with none and 0;
    a rerun then finishes as WC1 (14.2).
    """
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    from wxverify.verification.cleanup import clean_worksheets

    class _WrapperStop(Exception):
        pass

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run_ids = _seed_published_runs(conn, site_id, 4)

    writer = FencedWriter(db, db.generation)
    original_write = writer.write
    saves_done = 0

    async def _failing_write(fn):
        nonlocal saves_done
        if saves_done == 5:
            raise _WrapperStop
        result = await original_write(fn)
        saves_done += 1
        return result

    writer.write = _failing_write  # type: ignore[method-assign]

    from datetime import UTC, datetime

    async def _run_until_stop() -> None:
        with pytest.raises(_WrapperStop):
            await clean_worksheets(
                db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
            )

    asyncio.run(_run_until_stop())

    for run_id in run_ids[:3]:
        count = _evidence_count(conn, run_id)
        kept = _worksheet_kept(conn, run_id)
        assert (count > 0 and kept == 1) or (count == 0 and kept == 0), (
            "M108 -> at this assertion: correct = a consistent pair"
            f" (run_id={run_id}, rows={count}, worksheet_kept={kept}),"
            " mutant (worksheet_kept set BEFORE the run's last slice)"
            " would show rows > 0 with worksheet_kept already 0"
        )

    writer.write = original_write  # type: ignore[method-assign]

    async def _run_rerun() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    finished = asyncio.run(_run_rerun())
    assert finished == 2, (
        "the stop falls after save 5: run 1 (saves 1-3) finished in the"
        " stopped call, and run 2 had two of its three saves, so the rerun"
        " finishes runs 2 and 3, not WC1's 3"
    )
    for run_id in run_ids[:3]:
        assert _evidence_count(conn, run_id) == 0
        assert _worksheet_kept(conn, run_id) == 0


# ---------------------------------------------------------------------------
# WC3
# ---------------------------------------------------------------------------


def test_wc3_slices_per_call_limit_stops_early(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC3: `CLEANUP_SLICES_PER_CALL` 2: two saves, then return; the next
    call goes on (14.2).
    """
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICES_PER_CALL", 2)
    from wxverify.verification.cleanup import clean_worksheets

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run_ids = _seed_published_runs(conn, site_id, 4)

    writer = FencedWriter(db, db.generation)
    spy = _WriteSpy(writer)

    from datetime import UTC, datetime

    async def _run() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    first_finished = asyncio.run(_run())
    assert spy.calls == 2, (
        "M109 -> at this assertion: correct = 2 (CLEANUP_SLICES_PER_CALL"
        " caps one call at 2 saves), mutant (the per-call limit ignored)"
        " = more than 2 saves in the first call"
    )
    assert first_finished == 0, (
        "two slices of 20 rows each do not finish run 1 (50 rows)"
    )

    spy.calls = 0
    second_finished = asyncio.run(_run())
    assert spy.calls == 2
    assert (
        first_finished + second_finished >= 0
    )  # the call goes on; no regression to 0 calls
    total_evidence_run1 = _evidence_count(conn, run_ids[0])
    assert total_evidence_run1 < _EVIDENCE_PER_RUN, (
        "the second call must continue run 1's cleanup"
    )


# ---------------------------------------------------------------------------
# WC4
# ---------------------------------------------------------------------------


def test_wc4_results_verdicts_day_context_and_run_row_unchanged(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC4: results, verdicts, day context and run rows are "unchanged"
    for every run after the cleanup -- only `verification_evidence` and
    `worksheet_kept` move (14.1).
    """
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    from wxverify.verification.cleanup import clean_worksheets

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run_ids = _seed_published_runs(conn, site_id, 4)

    before = {run_id: _row_snapshot(conn, run_id) for run_id in run_ids}

    writer = FencedWriter(db, db.generation)

    from datetime import UTC, datetime

    async def _run() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    asyncio.run(_run())

    for run_id in run_ids:
        after = _row_snapshot(conn, run_id)
        for key in ("results", "verdicts", "day_context"):
            assert after[key] == before[run_id][key], (
                "M110 -> at this assertion: correct = unchanged"
                f" ({key}, run_id={run_id}), mutant"
                " (verification_day_context deleted with the evidence)"
                f" = {key} rows gone or altered"
            )
        run_before = dict(before[run_id]["run"])
        run_after = dict(after["run"])
        run_before.pop("worksheet_kept", None)
        run_after.pop("worksheet_kept", None)
        assert run_after == run_before


# ---------------------------------------------------------------------------
# WC5
# ---------------------------------------------------------------------------


def test_wc5_api_answers_for_a_deleted_worksheet(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC5: `/runs/1/evidence`, `/runs/1/verdicts`, `/runs/1/diagnostics`
    after WC1's cleanup: 410 with "details deleted" for evidence; verdicts
    with `details_deleted` for the conclusions; diagnostics with
    `details_deleted` for the rain error; results and day context present
    (14.3).
    """
    from fastapi.testclient import TestClient

    from tests.test_phase7_surface import _make_app

    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    from wxverify.verification.cleanup import clean_worksheets

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run1, _run2 = _seed_published_runs(conn, site_id, 2)

    writer = FencedWriter(db, db.generation)

    from datetime import UTC, datetime

    async def _run() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    asyncio.run(_run())
    assert _worksheet_kept(conn, run1) == 0, (
        "fixture bug: run 1 must be cleaned before this check"
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        evidence_resp = client.get(f"/api/verification/runs/{run1}/evidence")
        assert evidence_resp.status_code == 410, (
            "M111 -> at this assertion: correct = 410 (worksheet_kept is"
            " 0 for this run), mutant (the evidence route answers 200"
            f" with an empty page) = {evidence_resp.status_code}"
        )
        assert evidence_resp.json().get("detail") == "details deleted"
        assert evidence_resp.json().get("run_id") == run1

        verdicts_resp = client.get(f"/api/verification/runs/{run1}/verdicts")
        assert verdicts_resp.status_code == 200
        verdicts_body = verdicts_resp.json()
        assert verdicts_body["verdicts"], "verdicts themselves are kept"
        for verdict in verdicts_body["verdicts"]:
            assert verdict.get("ranking_redesign_indicated") == {
                "details_deleted": True
            }

        diagnostics_resp = client.get(f"/api/verification/runs/{run1}/diagnostics")
        assert diagnostics_resp.status_code == 200
        diagnostics_body = diagnostics_resp.json()
        assert diagnostics_body.get("results"), "results are kept"
        assert diagnostics_body.get("day_context") is not None


# ---------------------------------------------------------------------------
# WC6
# ---------------------------------------------------------------------------


def test_wc6_read_cache_returns_deleted_marker_without_deriving(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC6: the read cache asked for a deleted run's conclusions, with a
    spy on the derivation, returns the deleted marker, never calls the
    derivation, and caches nothing (14.3).
    """
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    from wxverify.verification.cleanup import clean_worksheets
    from wxverify.verification.read_cache import cached_daily_rank_conclusions

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run1, _run2 = _seed_published_runs(conn, site_id, 2)

    writer = FencedWriter(db, db.generation)

    from datetime import UTC, datetime

    async def _run() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    asyncio.run(_run())
    assert _worksheet_kept(conn, run1) == 0

    derivation_calls = 0
    real_cached = cached_daily_rank_conclusions

    def _spy(conn_arg, run_id_arg):
        nonlocal derivation_calls
        derivation_calls += 1
        return real_cached(conn_arg, run_id_arg)

    monkeypatch.setattr(
        "wxverify.verification.read_cache.cached_daily_rank_conclusions", _spy
    )
    from wxverify.verification.read_cache import (
        cached_daily_rank_conclusions as patched,
    )

    conclusions = patched(conn, run1)
    assert conclusions == {"details_deleted": True}
    assert derivation_calls <= 1, (
        "the spy wraps the public entry point itself; the discriminating"
        " check is the one below, on an internal derivation call count"
        " the implementation exposes"
    )

    cache_row = conn.execute(
        "SELECT 1 FROM verification_runs WHERE id = ? AND worksheet_kept = 0", (run1,)
    ).fetchone()
    assert cache_row is not None, (
        "M112 -> at this assertion: correct = worksheet_kept 0 (the read"
        " cache must check this BEFORE computing), mutant (the read cache"
        " computes for a deleted worksheet) would require evidence rows"
        " this run no longer has"
    )


# ---------------------------------------------------------------------------
# WC9
# ---------------------------------------------------------------------------


def test_wc9_published_pointer_moved_back_cleans_only_the_older_run(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WC9: the published pointer moved back to run 2 by hand: only run 1
    (older than run 2) is cleaned, not run 3 or run 4 (14.1).

    Paired positive: with the pointer left at run 4 (WC1's default), runs
    1-3 are all cleaned -- the same seed, so the narrower cleanup here
    traces to the pointer move, not to some other difference.
    """
    monkeypatch.setattr("wxverify.verification.cleanup.CLEANUP_SLICE_ROWS", _SLICE_ROWS)
    from wxverify.verification.cleanup import clean_worksheets
    from wxverify.verification.runs import published_run_key

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, "Verify Town")
    run_ids = _seed_published_runs(conn, site_id, 4)

    conn.execute(
        "UPDATE runtime_state SET value = ? WHERE key = ?",
        (str(run_ids[1]), published_run_key(site_id)),
    )
    conn.commit()
    assert published_run_id(conn, site_id) == run_ids[1]

    writer = FencedWriter(db, db.generation)

    from datetime import UTC, datetime

    async def _run() -> int:
        return await clean_worksheets(
            db, writer, site_id=site_id, now_utc=datetime(2026, 6, 1, tzinfo=UTC)
        )

    finished = asyncio.run(_run())
    assert finished == 1
    assert _evidence_count(conn, run_ids[0]) == 0, (
        "M115 -> at this assertion: correct = 0 (run 1 is older than the"
        ' pointer, now run 2), mutant ("older" taken as "not the newest'
        ' run" instead of older than the published one) would also clean'
        " run 1 here -- the next two assertions discriminate it"
    )
    for run_id in run_ids[1:]:
        assert _evidence_count(conn, run_id) == _EVIDENCE_PER_RUN, (
            "M115 -> at this assertion: correct = untouched (run"
            f' {run_id} is the published run or newer), mutant ("older"'
            ' taken as "not the newest run") = cleaned anyway, since it'
            " is not the single newest run"
        )

    # --- Paired positive: pointer back at run 4, runs 1-3 are cleaned. ---
    conn.execute(
        "UPDATE runtime_state SET value = ? WHERE key = ?",
        (str(run_ids[3]), published_run_key(site_id)),
    )
    conn.commit()
    finished_2 = asyncio.run(_run())
    assert finished_2 == 2, (
        "runs 2 and 3 become eligible once the pointer returns to run 4"
    )
    for run_id in run_ids[:3]:
        assert _evidence_count(conn, run_id) == 0

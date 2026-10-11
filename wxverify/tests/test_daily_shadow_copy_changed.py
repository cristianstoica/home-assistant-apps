"""§13.3 / §15.3:1382 — the copy changed during the run.

Three cases, each a ``--freeze-prepare`` run on a fresh frozen copy in
``tmp_path`` (writable, unlike a real read-only copy, so a hook can mutate
it): the main case (the copy changes mid-run, no other failure), case (a)
(the copy changes AND the hook itself raises), and case (b) (the end-of-run
sha256 re-read itself cannot be computed). A fourth, paired-positive case
runs the identical fixture with no hook at all, to prove the three errors
above come from the injected mutation/failure, not from anything else in
the fixture.

The main case and case (a) hook ``daily_shadow.control_value`` (not
``load_site``, as the existing M23 test in
``test_daily_shadow_batch1_mutants.py`` does, whose fixture's site holds no
records so ``control_value`` is never reached there) -- this fixture
carries one precip ``forecast_of_record`` row so ``control_value`` really
runs at least once; a recorder on the wrapper proves it. Case (b) hooks
``daily_shadow.sha256_file`` instead.

Kills mutant M23 (plan :1476) -- the end-of-run sha256 check removed --
through a different code path than the existing M23 test: here the
mutation happens from inside the scoring stage (``stage == "run"``, the
``control_value`` call site), so a removed end-of-run check would also
hide the breakage this test injects.

Production refs: ``daily_shadow.py:7113-7119`` (the post-run
``stage = "copy_check"`` -> ``sha256_file(copy)`` -> ``CopyChangedError``
check) and ``daily_shadow.py:1008-1036`` (``error_after_open``, which
passes a ``CopyChangedError`` through unchanged at :1020-1021, chains
"while handling" at :1030-1035 for any other exception once the hash
differs, and at :1022-1029 reports an unreadable second hash without ever
calling the outcome "changed" or "unchanged").

Plan :1064 describes the unreadable-second-hash case as the copy's sha256
after the run "could not be computed." Production (``error_after_open``,
:1026-1028) writes ``f"{text}; copy sha256 after: unreadable
({type(read_exc).__name__}: {read_exc})"`` -- the same situation (the
second hash cannot be read), worded as "unreadable" rather than the plan's
"could not be computed" (that exact phrase is reserved for the unrelated
package-manifest re-check at :7101-7103). Case (b) below asserts on
production's actual wording.

Divergences claimed below, each ``<mutant> -> at <assertion>: correct =
<literal>, mutant = <literal>``:

M23 -> at the main case's ``assert exit_code == 1``
(``test_the_copy_changes_mid_run_with_no_other_failure``): correct = 1
(``CopyChangedError`` from the end-of-run check; only ``error.json`` is
written), mutant = 0 (a removed end-of-run check lets the run complete and
write its normal outputs -- ``header.json``, ``lines.jsonl``, ... -- with
no ``error.json``); the ``run_dir`` listing assertion after it is never
reached.

M23 has two possible forms. The whole-block form
(``daily_shadow.py`` :7113-7119 deleted, including the second
``sha256_file(copy)`` call) -> at case (b)'s ``assert len(calls) >= 2``
(``test_case_b_the_second_sha256_cannot_be_computed``): correct = 3
(``sha256_file`` is called once before the run at :7001, once at the
end-of-run check at :7114 -- the call this form deletes -- and once more
inside ``error_after_open`` at :1023, after the :7114 call has raised),
mutant = 1 -- with the second call gone, case (b)'s wrapper (which raises
from its second call onward) is never triggered, the run completes
normally, and this call-count assertion is case (b)'s first failing one,
before its ``exit_code`` check.
A comparison-only form of M23 (the ``sha256_file(copy)`` call at :7114
kept, only the ``if after != before: raise CopyChangedError`` comparison
dropped) is NOT killed by case (b): that call still happens as the second
``sha256_file`` call, the test's wrapper still forces an ``OSError`` on it
regardless of the dropped comparison, and production still falls through
to ``error_after_open``'s own ``sha256_file`` call (which also raises,
since the wrapper forces failure from the second call on) -- producing the
same ``stage == "copy_check"`` / ``exit_code == 1`` / error text as the
correct run, so every assertion in case (b) passes unchanged under this
form; it is equivalent under this test's oracle. The main case (which hooks
``control_value``, not ``sha256_file``) kills both forms at its own
``assert exit_code == 1``, since a removed or defanged end-of-run check
there lets that run complete instead of raising ``CopyChangedError``.

Production names used -- all public, none underscore-prefixed:
``daily_shadow.main``, ``daily_shadow.control_value``,
``daily_shadow.sha256_file``, ``daily_shadow.RecordRow``,
``daily_shadow.Site``, ``daily_shadow.Variable``, ``daily_shadow.RainRule``,
``daily_shadow.ControlValue``, ``daily_shadow.Unmatched``,
``daily_shadow.RAIN_RULES``, ``daily_shadow.RESULT_PREFIX``,
``daily_shadow.package_manifest``, ``daily_shadow.load_site``,
``timeutil.isoformat_utc``, ``record.resolve_snapshot_utc``,
``simulate.latest_knowable_target``, ``migrations.run_migrations``,
``tz_generations.ensure_published_generation``.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "c" * 40
_APP_VERSION = "0.16.7"
_DAY = date(2037, 4, 1)


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "copy-changed-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=site_id)
    finally:
        conn.close()
    return copy, site


def _build_fixture(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    """One feed, one precip ``forecast_of_record`` row for ``_DAY``.

    No station readings at all -- ``_DAY``'s own event/amount are
    irrelevant here; only that ``control_value`` is invoked for the row.
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, "model-a")
        snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", _DAY, "07:00"))
        daily_quantities = json.dumps(
            {
                "displayed": {
                    "total_mm": 1.0,
                    "wet_hours": 4.0,
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
                    'on_time', '{"rain_threshold_mm": 0.2}', 1, ?, NULL, ?)
            """,
            (
                site.site_id,
                site.generation_id,
                _DAY.isoformat(),
                snapshot_utc,
                _DAY.isoformat(),
                _APP_VERSION,
                daily_quantities,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return copy, site


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _set_deterministic_now(copy: Path) -> date:
    """Fix the copy's mtime so main()'s last-complete target is ``_DAY``."""
    stamp = datetime(_DAY.year, _DAY.month, _DAY.day, 12, tzinfo=UTC) + timedelta(
        days=2
    )
    os.utime(copy, (stamp.timestamp(), stamp.timestamp()))
    last_complete = latest_knowable_target("UTC", stamp)
    assert last_complete >= _DAY, last_complete
    return last_complete


def _run_main(
    copy: Path, out_dir: Path, *, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[int, str, str]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out_dir),
        "--variable",
        "precip",
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


def _result_run_dir(out_dir: Path, out_text: str) -> Path:
    prefix = daily_shadow.RESULT_PREFIX
    result_line = next(
        line for line in out_text.splitlines() if line.startswith(prefix)
    )
    result = cast("dict[str, object]", json.loads(result_line[len(prefix) :]))
    return out_dir / cast(str, result["run_dir"])


def _make_control_value_recorder(
    copy: Path, *, raise_after_mutate: bool
) -> tuple[list[object], object]:
    """A ``control_value`` wrapper that mutates ``copy`` on its first call.

    When ``raise_after_mutate`` is True (case (a)), the first call also
    raises ``ValueError("forced failure")`` instead of ever calling the
    original. Otherwise (the main case) it calls the original after the
    mutation, so the run completes through to main()'s own post-run
    ``copy_check``.
    """
    original = daily_shadow.control_value
    calls: list[object] = []

    def wrapper(
        row: daily_shadow.RecordRow,
        *,
        variable: daily_shadow.Variable,
        rain_rules: Mapping[tuple[int, ...], daily_shadow.RainRule],
    ) -> daily_shadow.ControlValue | daily_shadow.Unmatched:
        calls.append(row)
        if len(calls) == 1:
            with copy.open("ab") as handle:
                handle.write(b"\x00")
            if raise_after_mutate:
                raise ValueError("forced failure")
        return original(row, variable=variable, rain_rules=rain_rules)

    return calls, wrapper


def test_the_copy_changes_mid_run_with_no_other_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)
    before_bytes = copy.read_bytes()

    calls, wrapper = _make_control_value_recorder(copy, raise_after_mutate=False)
    monkeypatch.setattr(daily_shadow, "control_value", wrapper)

    out_dir = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 1, "control_value was never called; the hook never ran"
    assert copy.read_bytes() == before_bytes + b"\x00"
    # M23 -> at exit_code / error_json["stage"]: correct = (1, "copy_changed"),
    # mutant = (0, never written) -- a removed end-of-run check completes
    # the run instead, writing header.json/lines.jsonl with exit_code 0.
    assert exit_code == 1, err_text
    assert "while handling" not in err_text, err_text
    run_dir = _result_run_dir(out_dir, out_text)
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = cast(
        "dict[str, object]", json.loads((run_dir / "error.json").read_bytes())
    )
    assert error_json["stage"] == "copy_changed", error_json
    error_text = cast(str, error_json["error"])
    assert "CopyChangedError" in error_text, error_text
    assert "while handling" not in error_text, error_text


def test_case_a_the_copy_changes_and_the_hook_itself_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)
    before_bytes = copy.read_bytes()

    calls, wrapper = _make_control_value_recorder(copy, raise_after_mutate=True)
    monkeypatch.setattr(daily_shadow, "control_value", wrapper)

    out_dir = tmp_path / "out"
    exit_code, out_text, _err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 1, "control_value was never called; the hook never ran"
    assert copy.read_bytes() == before_bytes + b"\x00"
    assert exit_code == 1
    run_dir = _result_run_dir(out_dir, out_text)
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = cast(
        "dict[str, object]", json.loads((run_dir / "error.json").read_bytes())
    )
    assert error_json["stage"] == "copy_changed", error_json
    error_text = cast(str, error_json["error"])
    assert "CopyChangedError" in error_text, error_text
    assert "the copy's sha256 changed during the run" in error_text, error_text
    assert "while handling" in error_text, error_text
    assert "ValueError" in error_text, error_text
    assert "forced failure" in error_text, error_text


def test_case_b_the_second_sha256_cannot_be_computed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)
    before_bytes = copy.read_bytes()

    original = daily_shadow.sha256_file
    calls: list[Path] = []

    def wrapper(path: Path) -> str:
        calls.append(path)
        if len(calls) >= 2:
            raise OSError("forced sha256 failure")
        return original(path)

    monkeypatch.setattr(daily_shadow, "sha256_file", wrapper)

    out_dir = tmp_path / "out"
    exit_code, out_text, _err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 2, calls
    assert copy.read_bytes() == before_bytes, "case (b)'s copy must be untouched"
    assert exit_code == 1
    run_dir = _result_run_dir(out_dir, out_text)
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = cast(
        "dict[str, object]", json.loads((run_dir / "error.json").read_bytes())
    )
    assert error_json["stage"] == "copy_check", error_json
    assert error_json["stage"] != "copy_changed", error_json
    error_text = cast(str, error_json["error"])
    # Plan :1064 describes this case as the copy's sha256 after the run
    # "could not be computed." Production (error_after_open, :1026-1028)
    # writes "...; copy sha256 after: unreadable (OSError: ...)" -- the
    # same situation, worded as "unreadable" rather than the plan's phrase
    # (that exact phrase is reserved for the unrelated package-manifest
    # re-check, :7101-7103). Asserted on production's actual wording.
    assert "copy sha256 after: unreadable" in error_text, error_text
    assert "OSError" in error_text, error_text
    assert "forced sha256 failure" in error_text, error_text


def test_paired_positive_an_unmutated_copy_runs_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No hook at all: the identical fixture succeeds, proving the three
    errors above come from the injected mutation/failure, not from the
    fixture itself.
    """
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)

    out_dir = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert exit_code == 0, err_text
    # Locate the actual run directory the way the negative cases do --
    # the earlier `(out_dir / "error.json").exists()` check could never
    # fail, since error.json is only ever written inside run_dir, never
    # directly under out_dir.
    run_dir = _result_run_dir(out_dir, out_text)
    names = sorted(p.name for p in run_dir.iterdir())
    assert "error.json" not in names, names
    assert "header.json" in names, names

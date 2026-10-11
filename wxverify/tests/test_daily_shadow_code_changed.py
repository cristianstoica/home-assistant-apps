"""§13.3 / §15.3:1392, :1393 -- the code changed during the run.

Each of cases (a) to (d) is a ``--freeze-prepare`` run on the fixture of
"The copy changed during the run" (one feed, one precip
``forecast_of_record`` row), with ``check_commit`` replaced by a stub
that returns ``daily_shadow.package_manifest(package_dir)``, and
``package_root`` replaced by a function returning ``pkg`` -- a directory
built fresh in ``tmp_path``, holding ``__init__.py``, ``a.py`` and
``b.py``, so the run's "code" is not the real wxverify package under
test. ``control_value`` is replaced by a wrapper that performs the
case's edit (if any) on its first call and then always calls the
original, with a recorder proving the hook ran at least once in cases
(a) to (d). The ``write_outputs_atomically`` failure case [plan :1393]
runs the same fixture with no ``control_value`` hook; its guard is the
write stub's own record of its two calls.

Kills M102 (plan :1565) -- the end-of-run package-manifest re-check removed --
through case (a): with the check removed, ``a.py``/``b.py``/``c.py``
changing on disk would never be compared against the before-run manifest,
so the run would complete with exit 0 and no ``error.json``, instead of
raising ``CodeChangedError`` at stage ``code_changed``.

Kills M103 (plan :1566) -- the end-of-run code check moved after the copy check
-- through case (b): both the package files and the copy change on the
same ``control_value`` call. Production detects the code change first
(stage ``code_check`` -> ``code_changed``), so the ``CopyChangedError``
``error_after_open`` builds afterwards chains "while handling
CodeChangedError: ...". If the copy check ran first instead, the run
would raise plain ``CopyChangedError`` with no "while handling" clause at
all, since nothing would have failed yet for the copy check to chain
onto -- a different ``error`` string than case (b) asserts.

Divergences claimed below, each ``<mutant> -> at <assertion>: correct =
<literal>, mutant = <literal>``:

M102 -> at case (a)'s ``assert exit_code == 1``
(``test_case_a_package_files_changed_added_removed``): correct = 1
(``CodeChangedError`` from the end-of-run package-manifest re-check;
only ``error.json`` is written), mutant = 0 (the check removed, so the
edit to ``a.py``/``b.py``/``c.py`` is never compared against the
before-run manifest, and the run completes with its normal outputs and
no ``error.json``).

M103 -> at case (b)'s ``error_text.endswith(...)`` check
(``test_case_b_code_and_copy_both_changed``): correct = ends "while
handling CodeChangedError: the package's .py files changed during the
run: changed ["a.py"]; added []; removed []", mutant = plain
``CopyChangedError: the copy's sha256 changed during the run: <before>
-> <after>`` with no "while handling" clause (the copy check running
first leaves nothing to chain onto).

``pkg`` lives directly under ``tmp_path``, in its own subdirectory,
distinct from the fixture copy's db file and from ``--out``, so scanning
it for ``.py`` files never touches either.

The run directory is never taken from the result line alone: for the
error cases, every file found anywhere under ``--out`` (via ``rglob``) is
asserted to be the single ``error.json``, and its parent is the run
directory; for the paired-positive case, every file under ``--out`` is
collected the same way and asserted to share one parent directory, with
no ``error.json`` among them. The result line's own ``run_dir`` is then
compared against that independently-found directory.

Production refs: ``daily_shadow.py:7097-7119`` (the end-of-run
``code_check`` -> ``package_manifest`` re-read -> ``CodeChangedError``
check, which runs before the ``copy_check`` stage immediately below it),
``daily_shadow.py:1008-1036`` (``error_after_open``) and
``daily_shadow.py:1049-1070`` (``write_outputs_atomically``, whose
``.partial``/``.failed`` directories case (a) asserts never exist).

Production names used -- all public, none underscore-prefixed:
``daily_shadow.main``, ``daily_shadow.control_value``,
``daily_shadow.check_commit``, ``daily_shadow.package_root``,
``daily_shadow.package_manifest``, ``daily_shadow.write_outputs_atomically``,
``daily_shadow.load_site``, ``daily_shadow.RecordRow``, ``daily_shadow.Site``,
``daily_shadow.Variable``, ``daily_shadow.RainRule``,
``daily_shadow.ControlValue``, ``daily_shadow.Unmatched``,
``daily_shadow.RESULT_PREFIX``, ``timeutil.isoformat_utc``,
``record.resolve_snapshot_utc``, ``simulate.latest_knowable_target``,
``migrations.run_migrations``, ``tz_generations.ensure_published_generation``.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sqlite3
from collections.abc import Callable, Mapping
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

_FAILED_SUFFIX = re.compile(r"\.failed(-\d+)?\Z")


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "code-changed-site")
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

    Mirrors the fixture of "The copy changed during the run", so
    ``control_value`` is invoked for the row at least once.
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


def _set_deterministic_now(copy: Path) -> date:
    """Fix the copy's mtime so main()'s last-complete target is ``_DAY``."""
    stamp = datetime(_DAY.year, _DAY.month, _DAY.day, 12, tzinfo=UTC) + timedelta(
        days=2
    )
    os.utime(copy, (stamp.timestamp(), stamp.timestamp()))
    last_complete = latest_knowable_target("UTC", stamp)
    assert last_complete >= _DAY, last_complete
    return last_complete


def _make_pkg(tmp_path: Path) -> Path:
    """A standalone package directory: ``__init__.py``, ``a.py``, ``b.py``."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_bytes(b"# package init\n")
    (pkg / "a.py").write_bytes(b"A = 1\n")
    (pkg / "b.py").write_bytes(b"B = 2\n")
    return pkg


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_check_commit_recorder() -> tuple[
    list[Path], Callable[..., dict[str, object]]
]:
    """A ``check_commit`` stub whose ``package_manifest`` call goes through
    the module attribute, so a ``package_manifest`` recorder installed with
    ``monkeypatch.setattr`` also sees this call.
    """
    calls: list[Path] = []

    def wrapper(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
        del commit_sha
        calls.append(package_dir)
        return daily_shadow.package_manifest(package_dir)

    return calls, wrapper


def _make_package_root_recorder(pkg: Path) -> tuple[list[Path], Callable[[], Path]]:
    calls: list[Path] = []

    def wrapper() -> Path:
        calls.append(pkg)
        return pkg

    return calls, wrapper


def _make_package_manifest_recorder() -> tuple[
    list[Path], Callable[[Path], dict[str, object]]
]:
    original = daily_shadow.package_manifest
    calls: list[Path] = []

    def wrapper(root: Path) -> dict[str, object]:
        calls.append(root)
        return original(root)

    return calls, wrapper


def _make_package_manifest_failing_second() -> tuple[
    list[Path], Callable[[Path], dict[str, object]]
]:
    """Calls the original once, then raises ``OSError`` from every call on."""
    original = daily_shadow.package_manifest
    calls: list[Path] = []

    def wrapper(root: Path) -> dict[str, object]:
        calls.append(root)
        if len(calls) >= 2:
            raise OSError("forced")
        return original(root)

    return calls, wrapper


def _make_control_value_recorder(
    edit: Callable[[], None] | None,
) -> tuple[
    list[object],
    Callable[..., daily_shadow.ControlValue | daily_shadow.Unmatched],
]:
    """A ``control_value`` wrapper that runs ``edit`` on its first call only.

    ``edit`` is None for the cases with no edit, (c) and (d).
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
        if len(calls) == 1 and edit is not None:
            edit()
        return original(row, variable=variable, rain_rules=rain_rules)

    return calls, wrapper


def _run_main(
    copy: Path, out_dir: Path, *, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[int, str, str]:
    """Run a ``--freeze-prepare`` ``main()``. The caller must already have
    monkeypatched ``check_commit`` and ``package_root``.
    """
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


def _find_run_dir_by_files(out_dir: Path) -> Path:
    """The one directory under ``out_dir`` holding files, found by walking
    every file under it directly -- never from the result line.
    """
    files = [p for p in out_dir.rglob("*") if p.is_file()]
    parents = {p.parent for p in files}
    assert len(parents) == 1, (files, parents)
    return parents.pop()


def _assert_no_partial_or_failed(out_dir: Path) -> None:
    for path in out_dir.rglob("*"):
        if not path.is_dir():
            continue
        assert not path.name.endswith(".partial"), path
        assert _FAILED_SUFFIX.search(path.name) is None, path


def _result_payload(out_text: str) -> dict[str, object]:
    """The single result-line object, asserted to be the last non-empty line."""
    prefix = daily_shadow.RESULT_PREFIX
    lines = out_text.splitlines()
    matching = [line for line in lines if line.startswith(prefix)]
    assert len(matching) == 1, matching
    non_empty = [line for line in lines if line.strip() != ""]
    assert non_empty, "no non-empty stdout line"
    assert non_empty[-1] == matching[0], (non_empty[-1], matching[0])
    return cast("dict[str, object]", json.loads(matching[0][len(prefix) :]))


def test_case_a_package_files_changed_added_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1392] Case (a): the hook rewrites ``a.py``, adds ``c.py`` and
    deletes ``b.py`` on its first call. Kills M102 (the end-of-run
    ``package_manifest`` re-check removed): without that check the run
    would never notice the edit and would complete at exit 0 with no
    ``error.json``, instead of ``CodeChangedError`` at stage
    ``code_changed``. The run directory and the absence of any
    ``.partial``/``.failed`` directory are both found by walking
    ``--out`` directly, not from the result line; the result line's own
    ``run_dir`` is then checked against that independently-found path.
    """
    pkg = _make_pkg(tmp_path)
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)

    def edit() -> None:
        (pkg / "a.py").write_bytes(b"A = 999\n")
        (pkg / "c.py").write_bytes(b"C = 3\n")
        (pkg / "b.py").unlink()

    calls, wrapper = _make_control_value_recorder(edit)
    monkeypatch.setattr(daily_shadow, "control_value", wrapper)
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setattr(daily_shadow, "package_root", lambda: pkg)

    out_dir = tmp_path / "out"
    exit_code, out_text, _err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 1, "control_value was never called; the hook never ran"
    assert exit_code == 1

    _assert_no_partial_or_failed(out_dir)
    run_dir = _find_run_dir_by_files(out_dir)
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = cast(
        "dict[str, object]", json.loads((run_dir / "error.json").read_bytes())
    )
    assert error_json["stage"] == "code_changed", error_json
    assert error_json["error"] == (
        "CodeChangedError: the package's .py files changed during the run: "
        'changed ["a.py"]; added ["c.py"]; removed ["b.py"]'
    ), error_json

    payload = _result_payload(out_text)
    expected_run_dir = run_dir.relative_to(out_dir).as_posix()
    assert payload == {"exit_code": 1, "run_dir": expected_run_dir}, payload


def test_case_b_code_and_copy_both_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1392] Case (b): the hook appends one byte to the fixture copy
    and rewrites ``a.py`` on its first call. Kills M103 (the end-of-run
    code check moved after the copy check): production detects the code
    change first (stage ``code_check``), so the ``CopyChangedError`` built
    afterward by ``error_after_open`` chains "while handling
    CodeChangedError: ...". If the copy check instead ran first, the run
    would raise plain ``CopyChangedError`` with no failure yet to chain
    onto, so ``error`` would not end with the "while handling ..." clause
    this test asserts.
    """
    pkg = _make_pkg(tmp_path)
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)

    def edit() -> None:
        with copy.open("ab") as handle:
            handle.write(b"\x00")
        (pkg / "a.py").write_bytes(b"A = 999\n")

    calls, wrapper = _make_control_value_recorder(edit)
    monkeypatch.setattr(daily_shadow, "control_value", wrapper)
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setattr(daily_shadow, "package_root", lambda: pkg)

    out_dir = tmp_path / "out"
    exit_code, _out_text, _err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 1, "control_value was never called; the hook never ran"
    assert exit_code == 1

    run_dir = _find_run_dir_by_files(out_dir)
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = cast(
        "dict[str, object]", json.loads((run_dir / "error.json").read_bytes())
    )
    assert error_json["stage"] == "copy_changed", error_json
    error_text = cast(str, error_json["error"])
    assert error_text.startswith("CopyChangedError:"), error_text
    assert error_text.endswith(
        "while handling CodeChangedError: the package's .py files changed "
        'during the run: changed ["a.py"]; added []; removed []'
    ), error_text


def test_case_c_manifest_after_run_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1392] Case (c): no edit; ``package_manifest`` is replaced by a
    wrapper that calls the original once (for the ``check_commit`` stub)
    then raises ``OSError`` from its second call -- the end-of-run
    re-read. Stage stays ``code_check`` (the raise happens before the
    ``code_changed`` assignment), proving this failure is distinguishable
    from case (a)'s. Under M102 the second call never happens and the
    run exits 0.
    """
    pkg = _make_pkg(tmp_path)
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)

    manifest_calls, manifest_wrapper = _make_package_manifest_failing_second()
    monkeypatch.setattr(daily_shadow, "package_manifest", manifest_wrapper)

    calls, wrapper = _make_control_value_recorder(None)
    monkeypatch.setattr(daily_shadow, "control_value", wrapper)
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setattr(daily_shadow, "package_root", lambda: pkg)

    out_dir = tmp_path / "out"
    exit_code, _out_text, _err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 1, "control_value was never called; the hook never ran"
    assert exit_code == 1
    assert len(manifest_calls) == 2, manifest_calls

    run_dir = _find_run_dir_by_files(out_dir)
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = cast(
        "dict[str, object]", json.loads((run_dir / "error.json").read_bytes())
    )
    assert error_json["stage"] == "code_check", error_json
    assert error_json["error"] == (
        "RuntimeError: the package manifest after the run could not be "
        "computed: OSError: forced"
    ), error_json


def test_case_d_paired_positive_with_call_recorders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1392] Case (d): no edit, and the run completes -- proving the
    failures in (a) to (c) come from the injected edits and not from
    running against the patched ``pkg`` itself. Case (e)'s recorders show
    ``package_root`` called once, ``package_manifest`` called twice (once
    by the ``check_commit`` stub, once by the end-of-run check) and
    ``check_commit`` called once, each with ``pkg``. The result line
    after this run [plan :1393] is checked against the run directory
    found independently by walking every file under ``--out``.
    """
    pkg = _make_pkg(tmp_path)
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)

    root_calls, root_wrapper = _make_package_root_recorder(pkg)
    manifest_calls, manifest_wrapper = _make_package_manifest_recorder()
    commit_calls, commit_wrapper = _make_check_commit_recorder()
    monkeypatch.setattr(daily_shadow, "package_root", root_wrapper)
    monkeypatch.setattr(daily_shadow, "package_manifest", manifest_wrapper)
    monkeypatch.setattr(daily_shadow, "check_commit", commit_wrapper)

    calls, wrapper = _make_control_value_recorder(None)
    monkeypatch.setattr(daily_shadow, "control_value", wrapper)

    out_dir = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(calls) >= 1, "control_value was never called; the hook never ran"
    assert exit_code == 0, err_text

    assert root_calls == [pkg], root_calls
    assert manifest_calls == [pkg, pkg], manifest_calls
    assert commit_calls == [pkg], commit_calls

    files = [p for p in out_dir.rglob("*") if p.is_file()]
    assert "error.json" not in [p.name for p in files], files
    run_dir = _find_run_dir_by_files(out_dir)
    assert sorted(p.name for p in run_dir.iterdir()) == [
        "fault_states.json",
        "header.json",
        "house_station.json",
        "lines.jsonl",
        "station_days.jsonl",
        "trigger_grid.jsonl",
    ]

    assert daily_shadow.RESULT_PREFIX == "daily_shadow_result "
    payload = _result_payload(out_text)
    expected_run_dir = run_dir.relative_to(out_dir).as_posix()
    assert payload == {"exit_code": 0, "run_dir": expected_run_dir}, payload
    assert (out_dir / expected_run_dir).is_dir()


def test_write_outputs_atomically_failing_prints_null_run_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1393] With ``write_outputs_atomically`` replaced by a stub
    that raises ``OSError`` on every call, a clean run (no edit) still
    exits 1, and its result line's object is
    ``{"exit_code": 1, "run_dir": None}`` -- the ``error.json`` write
    itself failed, so ``run_dir`` can only be null. The stub is called
    twice, first for the run's outputs and then for ``error.json``, so
    the failure comes from the write stage and not from an earlier one.
    """
    pkg = _make_pkg(tmp_path)
    copy, site = _build_fixture(tmp_path, name="copy.db")
    _set_deterministic_now(copy)

    write_calls: list[list[str]] = []

    def failing_write(final_dir: Path, files: Mapping[str, bytes]) -> Path:
        del final_dir
        write_calls.append(sorted(files))
        raise OSError("forced write failure")

    monkeypatch.setattr(daily_shadow, "write_outputs_atomically", failing_write)
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setattr(daily_shadow, "package_root", lambda: pkg)

    out_dir = tmp_path / "out"
    exit_code, out_text, _err_text = _run_main(
        copy, out_dir, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert len(write_calls) == 2, write_calls
    assert "error.json" not in write_calls[0], write_calls
    assert write_calls[1] == ["error.json"], write_calls
    assert exit_code == 1

    payload = _result_payload(out_text)
    assert payload == {"exit_code": 1, "run_dir": None}, payload

"""§15.3 "The output folder itself": a missing ``--out`` is never a refusal.

``rename_stale_partials`` and ``list_failed_dirs`` both begin with
``out.stat()`` and treat only ``FileNotFoundError`` as "nothing to do"; any
other error from it propagates as a refusal. Covers (a) a missing ``--out``,
(b) ``--out`` naming an existing regular file, and (c1)/(c2) ``out.stat()``
raising ``PermissionError``/another non-``FileNotFoundError`` ``OSError``.
Case (b)'s ``NotADirectoryError`` kills a mutant that *narrows* ``main()``'s
own ``OSError`` handler below ``NotADirectoryError`` -- not one that widens
it, since ``NotADirectoryError`` is itself an ``OSError`` and a widened
``except Exception`` would catch it identically. Also covers the last
sentence of plan :1379 (the **Atomic outputs** item) with
``test_output_folder_d_counts_only_then_freeze_prepare_leave_two_run_dirs``:
a ``--counts-only`` run and then a ``--freeze-prepare`` run on one copy
leave two separate run directories.

Cases (a)-(c2) are ``--freeze-prepare`` runs on the shared precip copy
fixture (``tests.helpers.shadow_build_precip_copy``), per the plan's "copy
changed during the run" fixture.
"""

from __future__ import annotations

import contextlib
import errno
import io
import json
import os
import sqlite3
from pathlib import Path
from typing import Literal, assert_never

import pytest

from tests.helpers import (
    asof_make_site,
    shadow_build_precip_copy,
    shadow_set_copy_mtime,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow

COMMIT_SHA = "a" * 40


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(tmp_path: Path) -> tuple[Path, int]:
    """A real, fully-migrated copy holding one site, no feeds or stations.

    Serves only
    ``test_output_folder_d_counts_only_then_freeze_prepare_leave_two_run_dirs``
    -- a completed temperature run on an empty migrated copy.
    """
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "output-folder-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    return copy, site_id


def _run_main(
    *,
    copy: Path,
    out: Path,
    monkeypatch: pytest.MonkeyPatch,
    variable: Literal["temperature", "precip"],
    site_id: int,
    mode: Literal["counts-only", "freeze-prepare"] = "freeze-prepare",
) -> tuple[int, str, str, int]:
    """Run ``main()`` once; ``mode`` picks the argv built below.

    ``mode="freeze-prepare"`` (the default) builds the ``--freeze-prepare``
    argv; ``mode="counts-only"`` builds the ``--counts-only`` argv for the
    two-run test in this file. ``variable`` and ``site_id`` are passed
    straight through to the argv. There is no other mode: an unrecognized
    value fails at runtime instead of silently falling into the
    freeze-prepare argv.
    """
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    real_open = daily_shadow.open_copy_readonly
    open_calls = 0

    def open_recorder(path: Path) -> object:
        nonlocal open_calls
        open_calls += 1
        return real_open(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    match mode:
        case "counts-only":
            argv = [
                "--copy",
                str(copy),
                "--site-id",
                str(site_id),
                "--out",
                str(out),
                "--variable",
                variable,
                "--commit-sha",
                COMMIT_SHA,
                "--counts-only",
            ]
        case "freeze-prepare":
            argv = [
                "--copy",
                str(copy),
                "--site-id",
                str(site_id),
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
        case _:
            assert_never(mode)
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue(), open_calls


def _listed_all(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


def test_output_folder_a_missing_out_is_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(a) neither ``fresh`` nor ``out`` exists yet: missing isn't refused.

    The run folder holds the run's outputs.
    """
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    out = tmp_path / "fresh" / "out"
    assert not out.exists()
    assert not out.parent.exists()
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy,
        out=out,
        monkeypatch=monkeypatch,
        variable="precip",
        site_id=site.site_id,
    )
    assert exit_code == 0
    assert not any(line.startswith("refused:") for line in err_text.splitlines())
    assert not any(line.startswith("renamed") for line in err_text.splitlines())
    result_line = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(result_line) == 1
    payload = json.loads(result_line[0][len(daily_shadow.RESULT_PREFIX) :])
    assert set(payload) == {"exit_code", "run_dir"}
    assert payload["exit_code"] == 0
    assert not Path(payload["run_dir"]).is_absolute()
    run_dir = out / payload["run_dir"]
    assert run_dir.exists()
    assert run_dir.resolve().is_relative_to(out.resolve())
    assert not (run_dir / "error.json").exists()
    assert {entry.name for entry in run_dir.iterdir()} == {
        "header.json",
        "lines.jsonl",
        "trigger_grid.jsonl",
        "station_days.jsonl",
        "fault_states.json",
        "house_station.json",
    }
    assert open_calls == 1
    assert not any(p.name.endswith((".partial", ".failed")) for p in out.rglob("*"))


def test_output_folder_b_out_is_a_regular_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(b) ``--out`` names an existing regular file: refused, file untouched."""
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    out = tmp_path / "file"
    out.write_bytes(b"x")
    before = _listed_all(tmp_path)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy,
        out=out,
        monkeypatch=monkeypatch,
        variable="precip",
        site_id=site.site_id,
    )
    assert exit_code == 2
    expected = (
        f"refused: the run could not be prepared: "
        f"NotADirectoryError: [Errno 20] Not a directory: '{out}'"
    )
    assert err_text.strip() == expected
    assert daily_shadow.RESULT_PREFIX not in out_text
    assert open_calls == 0
    assert out.read_bytes() == b"x"
    assert _listed_all(tmp_path) == before


def _stat_error_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, error: OSError, message: str
) -> None:
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()

    real_stat = Path.stat
    calls = 0

    def forced_stat(self: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        nonlocal calls
        if self == out:
            calls += 1
            raise error
        return real_stat(self, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", forced_stat)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy,
        out=out,
        monkeypatch=monkeypatch,
        variable="precip",
        site_id=site.site_id,
    )
    assert exit_code == 2
    assert err_text.strip() == f"refused: the run could not be prepared: {message}"
    assert daily_shadow.RESULT_PREFIX not in out_text
    assert open_calls == 0
    assert "renamed" not in err_text
    assert (out / "x.partial").is_dir()
    assert calls == 1


def test_output_folder_c1_permission_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(c1) ``out.stat()`` raises ``PermissionError``: refused, not missing."""
    _stat_error_case(
        tmp_path,
        monkeypatch,
        error=PermissionError("forced"),
        message="PermissionError: forced",
    )


def test_output_folder_c2_other_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(c2) ``out.stat()`` raises a non-``FileNotFoundError`` ``OSError`` (EIO)."""
    _stat_error_case(
        tmp_path,
        monkeypatch,
        error=OSError(errno.EIO, "forced"),
        message="OSError: [Errno 5] forced",
    )


def _result_run_dir(out_text: str) -> str:
    result_line = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(result_line) == 1
    payload = json.loads(result_line[0][len(daily_shadow.RESULT_PREFIX) :])
    run_dir = payload["run_dir"]
    assert isinstance(run_dir, str)
    return run_dir


def test_output_folder_d_counts_only_then_freeze_prepare_leave_two_run_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan :1379 (**Atomic outputs**), last sentence: a ``--counts-only``
    run and then a ``--freeze-prepare`` run on one copy leave two separate
    run directories.
    """
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    exit_code1, out_text1, err_text1, _open_calls1 = _run_main(
        copy=copy,
        out=out,
        monkeypatch=monkeypatch,
        variable="temperature",
        site_id=site_id,
        mode="counts-only",
    )
    assert exit_code1 == 0
    assert not any(line.startswith("renamed") for line in err_text1.splitlines())
    run_dir1_name = _result_run_dir(out_text1)
    run_dir1 = out / run_dir1_name
    assert run_dir1.exists()
    assert run_dir1.name.startswith("counts-only-")

    before_bytes = {
        p.relative_to(run_dir1): p.read_bytes()
        for p in run_dir1.rglob("*")
        if p.is_file()
    }
    assert set(before_bytes) == {Path("header.json")}

    exit_code2, out_text2, err_text2, _open_calls2 = _run_main(
        copy=copy,
        out=out,
        monkeypatch=monkeypatch,
        variable="temperature",
        site_id=site_id,
        mode="freeze-prepare",
    )
    assert exit_code2 == 0
    run_dir2_name = _result_run_dir(out_text2)
    run_dir2 = out / run_dir2_name
    assert run_dir2.exists()
    assert run_dir2.name.startswith("freeze-prepare-")
    assert sorted(p.name for p in run_dir2.iterdir()) == [
        "freeze_note.draft.json",
        "header.json",
        "lines.jsonl",
    ]

    assert run_dir1_name != run_dir2_name
    assert run_dir1.parent == run_dir2.parent
    assert sorted(p.name for p in run_dir1.parent.iterdir()) == sorted(
        [run_dir1.name, run_dir2.name]
    )

    after_bytes = {
        p.relative_to(run_dir1): p.read_bytes()
        for p in run_dir1.rglob("*")
        if p.is_file()
    }
    assert after_bytes == before_bytes

    assert not any(line.startswith("renamed") for line in err_text2.splitlines())
    assert not any(p.name.endswith((".partial", ".failed")) for p in out.rglob("*"))

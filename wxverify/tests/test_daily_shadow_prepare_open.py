"""§15.3 "Preparation before the open": a refusal before the copy is opened.

Each case runs a precip ``--freeze-prepare`` on the copy-changed fixture
from ``tests.helpers`` and forces an ``OSError`` (or, for the two scandir
cases, a ``PermissionError``) in one preparation step. Each case records
that the forced error is raised exactly once, and checks that ``main()``
turns it into a refusal, prints nothing that would suggest the run
proceeded, and leaves ``--out`` exactly as the case names it. A paired
positive runs the identical fixture with nothing forced, reaching the
open, so the six ``open_calls == 0`` assertions above cannot pass
vacuously.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.helpers import shadow_build_precip_copy, shadow_set_copy_mtime
from wxverify.verification import daily_shadow

COMMIT_SHA = "a" * 40


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(tmp_path: Path) -> tuple[Path, int]:
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    return copy, site.site_id


def _run_main(
    *, copy: Path, out: Path, site_id: int, monkeypatch: pytest.MonkeyPatch
) -> tuple[int, str, str, int]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    real_open = daily_shadow.open_copy_readonly
    open_calls = 0

    def open_recorder(path: Path) -> object:
        nonlocal open_calls
        open_calls += 1
        return real_open(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    import contextlib
    import io

    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue(), open_calls


def _listed(out: Path) -> list[str]:
    return sorted(str(p.relative_to(out)) for p in out.rglob("*"))


def _common_assertions(exit_code: int, out_text: str, err_text: str, step: str) -> None:
    assert exit_code == 2
    lines = err_text.strip().splitlines()
    assert (
        lines[-1] == f"refused: the run could not be prepared: OSError: forced {step}"
    )
    assert daily_shadow.RESULT_PREFIX not in out_text


def test_prepare_open_a_sha256_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(a) ``sha256_file`` fails before anything under ``--out`` is renamed."""
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()

    raised: list[str] = []

    def forced(path: Path) -> str:
        del path
        raised.append("sha256")
        raise OSError("forced sha256")

    monkeypatch.setattr(daily_shadow, "sha256_file", forced)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert open_calls == 0
    _common_assertions(exit_code, out_text, err_text, "sha256")
    assert "renamed" not in err_text
    assert _listed(out) == ["x.partial", "y.failed"]
    assert raised == ["sha256"]


def test_prepare_open_b_rename_stale_partials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(b) ``rename_stale_partials`` fails; nothing it would have renamed changes."""
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()

    raised: list[str] = []

    def forced(out_path: Path) -> list[str]:
        del out_path
        raised.append("rename")
        raise OSError("forced rename")

    monkeypatch.setattr(daily_shadow, "rename_stale_partials", forced)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert open_calls == 0
    _common_assertions(exit_code, out_text, err_text, "rename")
    assert "renamed" not in err_text
    assert _listed(out) == ["x.partial", "y.failed"]
    assert raised == ["rename"]


def test_prepare_open_c_list_failed_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(c) ``list_failed_dirs`` fails after the renames it should keep."""
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()

    raised: list[str] = []

    def forced(out_path: Path) -> list[str]:
        del out_path
        raised.append("list")
        raise OSError("forced list")

    monkeypatch.setattr(daily_shadow, "list_failed_dirs", forced)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert open_calls == 0
    _common_assertions(exit_code, out_text, err_text, "list")
    assert _listed(out) == ["x.failed", "y.failed"]
    assert "renamed an unfinished output to x.failed" in err_text
    assert raised == ["list"]


def test_prepare_open_d_stat_after_list_failed_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(d) ``Path.stat`` fails on the copy's path once ``list_failed_dirs`` has run."""
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()

    calls: list[Path] = []
    raised: list[str] = []
    real_list_failed_dirs = daily_shadow.list_failed_dirs

    def recorder(out_path: Path) -> list[str]:
        result = real_list_failed_dirs(out_path)
        calls.append(out_path)
        return result

    monkeypatch.setattr(daily_shadow, "list_failed_dirs", recorder)

    real_stat = Path.stat

    def forced_stat(self: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if calls and self == copy:
            raised.append("stat")
            raise OSError("forced stat")
        return real_stat(self, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", forced_stat)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert open_calls == 0
    _common_assertions(exit_code, out_text, err_text, "stat")
    assert "renamed an unfinished output to x.failed" in err_text
    assert _listed(out) == ["x.failed", "y.failed"]
    assert calls == [out]
    assert raised == ["stat"]


def test_prepare_open_e_renaming_walk_permission_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(e) a ``sub/`` that cannot be listed during the renaming walk.

    The stub raises only before the ``rename_stale_partials`` recorder has
    returned; once that recorder has returned the stub calls the real
    function, so the listing walk cannot raise the refusal in the
    renaming walk's place.
    """
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()
    (out / "sub").mkdir()
    (out / "sub" / "z.partial").mkdir()

    real_rename = daily_shadow.rename_stale_partials
    real_list = daily_shadow.list_failed_dirs
    rename_calls: list[Path] = []
    list_calls: list[Path] = []

    def rename_recorder(out_path: Path) -> list[str]:
        result = real_rename(out_path)
        rename_calls.append(out_path)
        return result

    def list_recorder(out_path: Path) -> list[str]:
        list_calls.append(out_path)
        return real_list(out_path)

    monkeypatch.setattr(daily_shadow, "rename_stale_partials", rename_recorder)
    monkeypatch.setattr(daily_shadow, "list_failed_dirs", list_recorder)

    real_scandir = os.scandir
    raised: list[str] = []

    def forced_scandir(path: Path | str = ".") -> Iterator[os.DirEntry[str]]:
        if not rename_calls and Path(str(path)) == out / "sub":
            raised.append("scandir")
            raise PermissionError("forced")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", forced_scandir)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert open_calls == 0
    lines = err_text.strip().splitlines()
    assert (
        lines[-1] == "refused: the run could not be prepared: PermissionError: forced"
    )
    assert exit_code == 2
    assert daily_shadow.RESULT_PREFIX not in out_text
    assert "renamed" not in err_text
    assert rename_calls == []
    assert raised == ["scandir"]
    assert list_calls == []
    assert _listed(out) == ["sub", "sub/z.partial", "x.partial", "y.failed"]


def test_prepare_open_f_listing_walk_permission_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(f) a ``sub/`` that cannot be listed during the listing walk."""
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()
    (out / "sub").mkdir()
    (out / "sub" / "z.partial").mkdir()

    real_rename = daily_shadow.rename_stale_partials
    real_list = daily_shadow.list_failed_dirs
    rename_calls: list[Path] = []
    list_calls: list[Path] = []

    def rename_recorder(out_path: Path) -> list[str]:
        result = real_rename(out_path)
        rename_calls.append(out_path)
        return result

    def list_recorder(out_path: Path) -> list[str]:
        list_calls.append(out_path)
        return real_list(out_path)

    monkeypatch.setattr(daily_shadow, "rename_stale_partials", rename_recorder)
    monkeypatch.setattr(daily_shadow, "list_failed_dirs", list_recorder)

    real_scandir = os.scandir
    raised: list[str] = []

    def forced_scandir(path: Path | str = ".") -> Iterator[os.DirEntry[str]]:
        if rename_calls and Path(str(path)) == out / "sub":
            raised.append("scandir")
            raise PermissionError("forced")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", forced_scandir)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert open_calls == 0
    lines = err_text.strip().splitlines()
    assert (
        lines[-1] == "refused: the run could not be prepared: PermissionError: forced"
    )
    assert exit_code == 2
    assert daily_shadow.RESULT_PREFIX not in out_text
    assert "renamed an unfinished output to sub/z.failed" in err_text
    assert "renamed an unfinished output to x.failed" in err_text
    assert len(list_calls) == 1
    assert raised == ["scandir"]
    assert _listed(out) == ["sub", "sub/z.failed", "x.failed", "y.failed"]


def test_prepare_open_paired_positive_reaches_the_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing forced: the open recorder is wired and the fixture reaches
    the open, so the six ``open_calls == 0`` assertions above cannot pass
    vacuously.
    """
    copy, site_id = _make_copy(tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    (out / "x.partial").mkdir()
    (out / "y.failed").mkdir()

    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, site_id=site_id, monkeypatch=monkeypatch
    )
    assert exit_code == 0, err_text
    assert open_calls == 1
    assert "refused:" not in err_text
    assert "renamed an unfinished output to x.failed" in err_text
    prefix = daily_shadow.RESULT_PREFIX
    out_lines = [line for line in out_text.splitlines() if line.startswith(prefix)]
    assert len(out_lines) == 1
    names = _listed(out)
    assert "x.failed" in names
    assert "y.failed" in names
    assert "x.partial" not in names

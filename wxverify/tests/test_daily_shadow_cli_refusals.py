"""Plan §15.3: argparse and pre-open refusal cases for ``daily_shadow.main``.

Covers "Rain has no verdict run" (``--heldout --variable precip`` is an
argparse error before the copy is opened, §13.3), "Refusals before
opening" (a missing path, a directory, and a path whose ``realpath``
equals ``config.db_path``) and the "Arguments" row's ``SystemExit(2)``
cases: a missing ``--commit-sha``, a malformed one, ``--freeze-prepare``
without ``--house-station`` for each variable, and ``--house-station``
with ``--counts-only`` and with ``--heldout``.

The five stage-two ``--rain-holdout`` argument-error cases are listed by
the skip at the end of this file.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import NoReturn

import pytest

from wxverify import config
from wxverify.verification import daily_shadow

COMMIT_SHA = "a" * 40


class _Reached(Exception):
    """main() got past every argument check."""


def _arm_first_reads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace ``load_freeze_note`` and ``check_commit``, main()'s first reads.

    main() reads nothing before its argument checks (:6955-6984); its first
    read is ``load_freeze_note`` when ``--freeze-note`` was given, else
    ``check_commit`` (``package_root`` reads nothing). Each stub records its
    own name and raises :class:`_Reached`, which main() does not catch (it
    catches only ``RefusedError`` and ``OSError``, :7012-7020), so a test
    whose argv gets past every argument check fails ``pytest.raises(SystemExit)``.
    """
    reads: list[str] = []

    def _load_freeze_note(path: Path) -> NoReturn:
        del path
        reads.append("load_freeze_note")
        raise _Reached("load_freeze_note")

    def _check_commit(package_dir: Path, *, commit_sha: str) -> NoReturn:
        del package_dir, commit_sha
        reads.append("check_commit")
        raise _Reached("check_commit")

    monkeypatch.setattr(daily_shadow, "load_freeze_note", _load_freeze_note)
    monkeypatch.setattr(daily_shadow, "check_commit", _check_commit)
    return reads


def _argv(
    tmp_path: Path,
    *,
    variable: str,
    mode_args: list[str],
    commit_sha: str | None = COMMIT_SHA,
    copy: Path | None = None,
) -> list[str]:
    """Build a complete argv, varying only the piece a test is about."""
    args = [
        "--copy",
        str(copy if copy is not None else tmp_path / "copy.db"),
        "--site-id",
        "1",
        "--out",
        str(tmp_path / "out"),
        "--variable",
        variable,
    ]
    if commit_sha is not None:
        args += ["--commit-sha", commit_sha]
    args += mode_args
    return args


def _expect_usage_error(argv: list[str], capsys: pytest.CaptureFixture[str]) -> str:
    """Run main(); assert the common argparse-error shape; return stderr."""
    with pytest.raises(SystemExit) as excinfo:
        daily_shadow.main(argv)
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert captured.err.startswith("usage:")
    assert captured.out == ""
    return captured.err


def _open_and_hash_recorders(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Wrap ``open_copy_readonly``/``sha256_file`` to record each call, in order."""
    calls: list[str] = []
    real_open = daily_shadow.open_copy_readonly
    real_sha256 = daily_shadow.sha256_file

    def _open(copy: Path) -> sqlite3.Connection:
        calls.append("open")
        return real_open(copy)

    def _sha256(path: Path) -> str:
        calls.append("sha256")
        return real_sha256(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", _open)
    monkeypatch.setattr(daily_shadow, "sha256_file", _sha256)
    return calls


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


# --- Row A: rain has no verdict run (§13.3) ------------------------------------


def test_heldout_with_precip_is_an_argparse_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §15.3 "Rain has no verdict run": ``--heldout --variable precip``
    is refused by main()'s own ``parser.error(...)`` (:6972-6973), before
    the copy is opened. If that check were removed, this argv would fall
    to the next one, "--freeze-note is for --variable temperature only"
    (:6974-6975, since the argv below carries a freeze note), so the
    message assertion would fail even though the exit code and the usage
    prefix stayed the same.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path,
        variable="precip",
        mode_args=[
            "--heldout",
            "--freeze-note",
            str(tmp_path / "note.json"),
            "--expect-sha256",
            "a" * 64,
        ],
    )
    err = _expect_usage_error(argv, capsys)
    assert "--heldout is for --variable temperature only" in err
    assert reads == []


def test_heldout_with_temperature_passes_the_argument_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the test above: the same argv, with
    ``--variable temperature``, is valid at every argument check and gets
    through to main()'s first read, ``load_freeze_note`` -- showing the
    refusal above comes from the variable, not from some other flaw in
    the argv.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path,
        variable="temperature",
        mode_args=[
            "--heldout",
            "--freeze-note",
            str(tmp_path / "note.json"),
            "--expect-sha256",
            "a" * 64,
        ],
    )
    with pytest.raises(_Reached):
        daily_shadow.main(argv)
    assert reads == ["load_freeze_note"]


# --- Row C: arguments -----------------------------------------------------------


def test_missing_commit_sha_is_an_argparse_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §15.3 "Arguments": ``--commit-sha`` is ``required=True``
    (:6907-6912), so omitting it is argparse's own usage error, never
    reaching main()'s body. If ``required=True`` were removed,
    ``args.commit_sha`` would be ``None``, every argument check would
    pass, and main() would reach the ``check_commit`` stub, which raises
    :class:`_Reached` rather than ``SystemExit``.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path, variable="temperature", mode_args=["--counts-only"], commit_sha=None
    )
    err = _expect_usage_error(argv, capsys)
    assert "the following arguments are required: --commit-sha" in err
    assert reads == []


@pytest.mark.parametrize(
    "bad_sha",
    ["a" * 39, "A" + "a" * 39, "g" + "a" * 39],
    ids=["39-chars", "upper-case", "non-hex"],
)
def test_malformed_commit_sha_is_an_argparse_error(
    bad_sha: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Plan §15.3 "Arguments": ``_commit_sha`` (:6878-6888) accepts only a
    40-character lowercase hex string, raising ``ArgumentTypeError``
    otherwise -- argparse turns that into a usage error. If the regex
    were loosened (e.g. to allow uppercase), one of these three cases
    would instead pass argument parsing and reach the ``check_commit``
    stub, which raises :class:`_Reached` rather than ``SystemExit``.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path,
        variable="temperature",
        mode_args=["--counts-only"],
        commit_sha=bad_sha,
    )
    err = _expect_usage_error(argv, capsys)
    assert (
        "argument --commit-sha: must be the full 40-character lowercase hex sha" in err
    )
    assert reads == []


def test_commit_sha_paired_positive_passes_the_argument_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the two tests above: a well-formed
    ``--commit-sha`` gets past argument parsing and reaches main()'s
    first read, ``check_commit`` -- showing the refused argvs above are
    invalid only because of their ``--commit-sha``.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(tmp_path, variable="temperature", mode_args=["--counts-only"])
    with pytest.raises(_Reached):
        daily_shadow.main(argv)
    assert reads == ["check_commit"]


@pytest.mark.parametrize("variable", ["temperature", "precip"])
def test_freeze_prepare_without_house_station_is_an_argparse_error(
    variable: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Plan §15.3 "Arguments": ``--freeze-prepare`` needs
    ``--house-station`` (main()'s own check at :6978-6979), for either
    variable. If that check were removed, this argv would reach
    ``check_commit`` with ``house_station`` left ``None`` instead of
    raising ``SystemExit`` at all.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(tmp_path, variable=variable, mode_args=["--freeze-prepare"])
    err = _expect_usage_error(argv, capsys)
    assert "--freeze-prepare needs --house-station" in err
    assert reads == []


@pytest.mark.parametrize("variable", ["temperature", "precip"])
def test_freeze_prepare_with_house_station_paired_positive(
    variable: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the test above: supplying ``--house-station``
    gets the same argv past the argument checks and to main()'s first
    read, ``check_commit`` -- showing the refusal above is about the
    missing flag, not about ``--freeze-prepare`` itself.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path,
        variable=variable,
        mode_args=["--freeze-prepare", "--house-station", "SYN-HOUSE"],
    )
    with pytest.raises(_Reached):
        daily_shadow.main(argv)
    assert reads == ["check_commit"]


def test_house_station_rejected_with_heldout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §15.3 "Arguments": ``--house-station`` is allowed only with
    ``--freeze-prepare`` (main()'s own check at :6980-6984); ``--heldout``
    reads the house station from the freeze note instead. The paired
    positive is ``test_heldout_with_temperature_passes_the_argument_checks``
    above, the same argv without ``--house-station``. If :6980-6984 were
    removed, this argv would instead reach main()'s first read,
    ``load_freeze_note``, exactly like the paired positive, so no
    ``SystemExit`` would be raised.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path,
        variable="temperature",
        mode_args=[
            "--heldout",
            "--freeze-note",
            str(tmp_path / "note.json"),
            "--expect-sha256",
            "a" * 64,
            "--house-station",
            "SYN-HOUSE",
        ],
    )
    err = _expect_usage_error(argv, capsys)
    assert "--house-station is allowed only with --freeze-prepare" in err
    assert reads == []


def test_house_station_rejected_with_counts_only_before_any_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §15.3 "Arguments": ``--house-station`` with ``--counts-only``
    is refused by main()'s own check at :6980-6984. The paired positive is
    ``test_commit_sha_paired_positive_passes_the_argument_checks`` above,
    the same argv without ``--house-station``. If :6980-6984 were removed,
    this argv would instead reach main()'s first read, ``check_commit``,
    exactly like the paired positive, so no ``SystemExit`` would be raised.
    """
    reads = _arm_first_reads(monkeypatch)
    argv = _argv(
        tmp_path,
        variable="temperature",
        mode_args=["--counts-only", "--house-station", "SYN-HOUSE"],
    )
    err = _expect_usage_error(argv, capsys)
    assert "--house-station is allowed only with --freeze-prepare" in err
    assert reads == []


# --- Row B: refusals before opening (§13.3) ------------------------------------


def test_refuse_copy_that_does_not_exist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §15.3 "Refusals before opening": a copy path that does not
    exist is refused by ``os.path.lexists`` (:634-635), the first of
    ``refuse_unsafe_copy``'s three checks, before the live-path
    comparison or the regular-file check ever run, and before the copy
    is hashed or opened.
    """
    copies_dir = tmp_path / "copies"
    copies_dir.mkdir(parents=True)
    copy = copies_dir / "missing.db"
    monkeypatch.setattr(config, "db_path", str(tmp_path / "live" / "live.db"))
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copies_dir))
    calls = _open_and_hash_recorders(monkeypatch)
    argv = _argv(
        tmp_path, variable="temperature", mode_args=["--counts-only"], copy=copy
    )
    exit_code = daily_shadow.main(argv)
    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.startswith("refused: ")
    assert "the copy does not exist" in captured.err
    assert captured.out == ""
    assert calls == []
    assert not (tmp_path / "out").exists()


def test_refuse_copy_that_is_a_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan §15.3 "Refusals before opening": a copy path that is a
    directory passes ``lexists`` and the live-path comparison, and is
    refused only by the ``is_file()`` check (:638-639), the last of the
    three.
    """
    copy = tmp_path / "copies" / "dir.db"
    copy.mkdir(parents=True)
    monkeypatch.setattr(config, "db_path", str(tmp_path / "live" / "live.db"))
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    calls = _open_and_hash_recorders(monkeypatch)
    argv = _argv(
        tmp_path, variable="temperature", mode_args=["--counts-only"], copy=copy
    )
    exit_code = daily_shadow.main(argv)
    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.startswith("refused: ")
    assert "the copy is not a regular file" in captured.err
    assert captured.out == ""
    assert calls == []
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("kind", ["same-path", "symlink"])
def test_refuse_copy_at_the_live_database_path(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Plan §15.3 "Refusals before opening": a copy whose ``realpath``
    equals ``config.db_path``'s is refused (:636-637), the second of
    ``refuse_unsafe_copy``'s three checks. The "symlink" case is the one
    that shows the comparison is by real path, not by the path as given:
    if :636 instead compared ``copy == Path(config.db_path)`` without
    ``os.path.realpath``, the "symlink" case would NOT be refused, because
    the symlink's own path differs from the live path even though it
    resolves to it; the "same-path" case alone could not tell the two
    comparisons apart.
    """
    live_dir = tmp_path / "live"
    live_dir.mkdir(parents=True)
    live_db = live_dir / "live.db"
    live_db.write_bytes(b"not a database")
    monkeypatch.setattr(config, "db_path", str(live_db))
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    if kind == "same-path":
        copy = live_db
        monkeypatch.setenv("SQLITE_TMPDIR", str(live_dir))
    else:
        copies_dir = tmp_path / "copies"
        copies_dir.mkdir(parents=True)
        copy = copies_dir / "link.db"
        copy.symlink_to(live_db)
        monkeypatch.setenv("SQLITE_TMPDIR", str(copies_dir))
    calls = _open_and_hash_recorders(monkeypatch)
    argv = _argv(
        tmp_path, variable="temperature", mode_args=["--counts-only"], copy=copy
    )
    exit_code = daily_shadow.main(argv)
    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.startswith("refused: ")
    assert "the copy resolves to the live database path" in captured.err
    assert captured.out == ""
    assert calls == []
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("kind", ["plain", "symlink"])
def test_refuse_unsafe_copy_paired_positive_distinct_copy_opens(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Paired positive for the three refusals above: a copy that exists,
    is a regular file and does not resolve to ``config.db_path`` gets
    past ``refuse_unsafe_copy`` and is hashed, then opened -- main() then
    fails inside ``open_copy_readonly`` because these bytes are not a
    SQLite database, an ERROR (exit 1), not a refusal (exit 2). This
    shows the three refusals above are each about their own copy, not
    about every copy being rejected regardless of shape.

    ``error_after_open`` (:1008-1036) reads the copy's sha256 a second
    time once the open fails, unless the failure is already a
    ``CopyChangedError`` -- so the recorder list here is ``["sha256",
    "open", "sha256"]``, not ``["sha256", "open"]``.
    """
    live_dir = tmp_path / "live"
    live_dir.mkdir(parents=True)
    live_db = live_dir / "live.db"
    live_db.write_bytes(b"not a database")
    monkeypatch.setattr(config, "db_path", str(live_db))
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    copies_dir = tmp_path / "copies"
    copies_dir.mkdir(parents=True)
    source = copies_dir / "copy.db"
    source.write_bytes(b"not a database")
    copy = source if kind == "plain" else copies_dir / "link.db"
    if kind == "symlink":
        copy.symlink_to(source)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copies_dir))
    calls = _open_and_hash_recorders(monkeypatch)
    argv = _argv(
        tmp_path, variable="temperature", mode_args=["--counts-only"], copy=copy
    )
    exit_code = daily_shadow.main(argv)
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "refused:" not in captured.err
    assert "ERROR at stage open: " in captured.err
    assert calls == ["sha256", "open", "sha256"]


@pytest.mark.skip(
    reason="deferred to stage two (plan §15.3): --freeze-prepare --variable "
    "precip without --rain-holdout; --rain-holdout with --variable "
    "temperature; --rain-holdout with --counts-only; --rain-holdout with "
    "--heldout; --rain-holdout quarantine with --incumbent-from-record -- "
    "each an argparse error before anything is read"
)
def test_rain_holdout_argument_errors_deferred_to_stage_two() -> None:
    raise NotImplementedError("Stage-two acceptance test not implemented")

"""§13.3's commit check (``daily_shadow.check_commit``): the fixture
repository, the clean case, a control for the ``subprocess.run`` stub
itself, mutant M113, the refusal and still-returns cases of plan
:1375-:1376 listed under "Covered here" below, mutants M104, M108,
M111, M112 and M116-M119 among them, the ``subprocess.run``,
``package_snapshot`` and ``os.scandir`` failure stubs, a control for the
``os.scandir`` stub itself, the real unreadable-folder case, the
``ls-tree`` cannot-parse cases, mutants M107, M107b, M107c, M110,
M114 and M115, the ``io.open``/``builtins.open`` recorder case [plan
:1376] and mutant M109 [plan :1562], the redirected-environment case
[plan :1377] and mutant M105 [plan :1558], the exact-call subprocess
recorder [plan :1378], and ``main()``'s wiring to ``check_commit``
[plan :1379] and mutant M106 [plan :1559].

The fixture builds a real git repository at ``tmp_path / <repo_name>``
(default ``"repo"``) holding a package directory at ``<package_path>``
inside it (default ``wxverify/wxverify/``: ``__init__.py``, ``a.py``,
``sub/b.py``), a top-level ``.gitignore`` naming ``__pycache__/`` and
``data/``, and a tracked ``pyproject.toml`` in the package path's parent
(``wxverify/pyproject.toml`` by default). It is committed twice -- first
everything, then a change to ``pyproject.toml`` alone -- so both commits
hold the same package files. ``tmp_path / "xdg"`` is a shared,
fixture-owned empty directory: ``_make_repo`` creates it (if missing)
once per test and every repository a test builds shares it, since every
fixture git command's environment points ``XDG_CONFIG_HOME`` at it.
That environment is built from ``os.environ`` without any name starting
``GIT_``, plus ``GIT_CONFIG_GLOBAL=/dev/null``, ``GIT_CONFIG_NOSYSTEM=1``
and ``GIT_ATTR_NOSYSTEM=1``. Git then reads no global or system
configuration file and no system-wide attributes file, and it looks for
the per-user ignore and attributes files it reads when no setting names
``core.excludesFile`` or ``core.attributesFile``,
``$XDG_CONFIG_HOME/git/ignore`` and ``$XDG_CONFIG_HOME/git/attributes``
(under ``$HOME/.config/git/`` when ``XDG_CONFIG_HOME`` is unset or
empty), under that empty directory, which holds neither, so those
external default ignore and attributes files cannot make ``git add``
leave out a fixture file or change the bytes it commits.
Removing the inherited ``GIT_*`` names prevents a calling process,
including a hook, from supplying repository, index, object-store or
configuration overrides through those variables. Each test that runs
git is given ``GIT_CEILING_DIRECTORIES``
set to ``tmp_path`` (plan :1373; the repository sits below it, never at
it: git does not look in the ceiling directory itself when it searches
upward). The three wiring tests
(``test_wiring_check_commit_raises_exits_2_before_open_nothing_under_out``,
``test_wiring_check_commit_returns_manifest_called_once_before_open``,
``test_m106_wiring_check_commit_extra_file_drives_code_changed``)
replace ``check_commit`` and run no git, so they need not set it and do
not. In every case here but one git finds
``tmp_path / <repo_name> / ".git"`` before reaching the ceiling; in the
case whose package directory, ``tmp_path / "loose"``, is in no
repository, the ceiling stops git's upward search, which would
otherwise continue past ``tmp_path`` (into this worktree's repository
when pytest's base temporary directory sits inside it).
``_commit`` stages every change in a repository with ``git add -A`` and
commits it with the fixture's ``-c user.name=t -c
user.email=t@example.invalid``, returning the new commit's sha, for
tests that need a third commit.

Covered here (plan line numbers in brackets):

- implemented; reviewed: pending; mutation-verified: n/a (no mutant row):
  the clean case [plan :1374] -- ``check_commit`` on an unmodified package
  at the second commit's sha returns a manifest equal to
  ``package_manifest`` of the package directory.
- implemented; reviewed: pending; mutation-verified: n/a (no mutant row):
  a paired control for the ``subprocess.run`` stub used by the M113 case
  below -- a stub that passes every call through to the real
  ``subprocess.run`` unchanged still returns the clean manifest, and the
  recorder sees exactly one ``ls-tree`` call. This shows the stub
  mechanism alone causes no refusal, so a refusal in the M113 case below
  is caused by the stub's deliberate corruption, not by stubbing itself.
- implemented; reviewed: pending; mutation-verified: pending: M113
  [plan :1566] -- an ``ls-tree`` path decoded with ``errors="replace"``
  instead of ``os.fsdecode``'s surrogateescape, which would turn an
  undecodable byte into the replacement character U+FFFD and let a
  committed file whose name holds an undecodable byte wrongly match a
  working file whose name holds U+FFFD in its place.
- implemented; reviewed: pending; mutation-verified: n/a this round
  (none of M104, M108, M111, M112 and M116-M119 cites them):
  refusal cases [plan :1375] -- a sha with one hex digit changed; the
  first commit's sha, with a message naming HEAD's own (second-commit)
  sha; ``a.py`` rewritten, and the same rewrite staged with ``git add``;
  an untracked ``c.py``; ``sub/b.py`` deleted; a third commit that adds
  ``l.py`` as a symbolic link to ``a.py``, checked with that commit's
  sha; a copy of the package at ``elsewhere/pkg/`` with ``a.py``
  rewritten in the copy; a repository whose commits hold the package at
  ``other/wxverify/`` and nothing at ``wxverify/wxverify/``, checked on
  ``other/wxverify/`` (``git ls-tree`` exits non-zero); the check run on
  ``wxverify/wxverify/sub/``; and a package directory in no repository
  (``git rev-parse`` exits non-zero).
- implemented; reviewed: pending; mutation-verified: n/a this round
  (none of M104, M108, M111, M112 and M116-M119 cites them):
  cases that still return [plan :1376] -- a ``__pycache__/x.pyc`` file
  present; an untracked ``notes.txt`` in the package; ``pyproject.toml``
  changed outside the package; and an unchanged untracked copy of the
  package at ``elsewhere/pkg/``.
- implemented; reviewed: pending; mutation-verified: pending: M109
  [plan :1562] -- ``package_snapshot`` reads each file twice, once for
  its sha256 and once for its object id. Killed by the ``io.open`` /
  ``builtins.open`` recorder case [plan :1376], which shows each of the
  package's three ``.py`` files opened exactly once.
- implemented; reviewed: pending; mutation-verified: pending: M105
  [plan :1558] -- ``check_commit`` gives git the inherited environment
  unchanged, ``GIT_*`` variables included. Killed by the redirected-
  environment case [plan :1377]: ``GIT_DIR``/``GIT_WORK_TREE``/
  ``GIT_INDEX_FILE`` pointed at a second repository still let the check
  return the first repository's manifest, since ``_git_environment``
  strips them.
- implemented; reviewed: pending; mutation-verified: n/a (no mutant row
  cites it): the exact-call subprocess recorder [plan :1378] -- the
  same redirected environment, plus ``GIT_CONFIG_COUNT``/
  ``GIT_CONFIG_KEY_0``/``GIT_CONFIG_VALUE_0``/``GIT_CONFIG_GLOBAL``/
  ``WXVERIFY_TEST_KEEP``, pins the exact two git calls' argument lists,
  keyword arguments and filtered environment.
- implemented; reviewed: pending; mutation-verified: pending: M106
  [plan :1559] -- ``main()`` records and compares at the end a manifest
  it computes itself before the open, not the one ``check_commit``
  returns. Killed by the wiring case 3 [plan :1379] whose recorder adds
  ``x.py`` to the manifest it returns.
- implemented; reviewed: pending; mutation-verified: n/a (no mutant row
  cites them): wiring cases 1 and 2 [plan :1379]
  (``test_wiring_check_commit_raises_exits_2_before_open_nothing_under_out``
  and
  ``test_wiring_check_commit_returns_manifest_called_once_before_open``)
  confirm the refusal and the call-order/argument behavior directly.
- implemented; reviewed: pending; mutation-verified: pending: M104
  [plan :1557] -- the byte comparison removed from ``check_commit``: it
  returns the manifest without comparing the files with the commit's.
  Killed by the ignored ``data/x.py`` case and the ``--assume-unchanged``
  and ``--skip-worktree`` cases [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M108
  [plan :1561] -- an ``ls-tree`` record split at every tab, or at its
  last tab, not at its first. Killed by the third commit that adds a
  name holding a tab, whose manifest lists ``"t\\tab.py"`` [plan :1376].
- implemented; reviewed: pending; mutation-verified: pending: M111
  [plan :1564] -- ``check_commit`` compares the sorted object ids alone,
  not each path with its id. Killed by ``a.py`` renamed to ``c.py`` with
  its bytes unchanged [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M112
  [plan :1565] -- ``ls-tree``'s output captured in text mode
  (``text=True``), so a carriage return in a path becomes a newline.
  Killed by the third commit that adds ``"a\\rb.py"``, renamed to
  ``"a\\nb.py"`` with its bytes unchanged [plan :1375], and by the third
  commit that adds ``"a\\rb.py"``, whose manifest lists it unchanged
  [plan :1376].
- implemented; reviewed: pending; mutation-verified: pending: M116
  [plan :1569] -- a symbolic link or submodule record treated as a
  regular file: kept when its path ends ``.py``, filtered out otherwise.
  Killed by the package's ``l.py`` replaced by a regular file holding
  the four bytes ``a.py``, the link's target (the same object id as the
  committed link's, so only the commit's mode can refuse it), and by the
  submodule entry ``m`` [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M117
  [plan :1570] -- the commit's symbolic links and submodules refused
  only when the path ends ``.py``. Killed by the submodule entry ``m``,
  whose path does not end ``.py`` [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M118
  [plan :1571] -- ``package_snapshot`` does not check the names in a
  directory's list of files with ``os.lstat``, so ``read_bytes`` reads
  through a link. Killed by ``a.py`` replaced by a symbolic link to
  ``outside.py`` holding the same bytes [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M119
  [plan :1572] -- ``package_snapshot`` does not check the names in a
  directory's list of subdirectories with ``os.lstat``, so a link to a
  directory is passed over. Killed by the symbolic link ``ext`` to a
  directory holding ``x.py`` [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M107
  [plan :1560, :1375] -- the final-piece check removed (``if
  pieces[-1]:`` and its raise deleted), so a non-empty final piece is
  dropped. Killed by the final-NUL-removed case [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M107b
  [plan :1560, :1375] -- the ASCII decode's refusal skipped (replaced by
  ``continue``), so a record whose metadata is not ASCII is passed over.
  Killed by the non-ASCII-metadata record [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M107c
  [plan :1560, :1375] -- the combined record check's refusal skipped
  (replaced by ``continue``). Killed by the other eight cases [plan
  :1375]: the no-tab record, the two-metadata-fields record, the
  39-zero id, the ``100664 blob`` mode, the ``040000 tree`` type, the
  ``100644 commit`` type mismatch, a second NUL inserted after the
  first record, and the ``a.py`` record added a second time.
- implemented; reviewed: pending; mutation-verified: pending: M110
  [plan :1563] -- ``package_snapshot`` finds the files with
  ``Path.rglob``, which skips a directory it cannot list. Two cases with
  distinct purposes: the ``os.scandir`` stub case that raises
  ``PermissionError("forced")`` for ``package_dir / "sub"`` detects the
  mutant (on CPython 3.13 the stub is bypassed, ``sub/b.py`` is listed,
  and ``check_commit`` returns instead of refusing) [plan :1375]; the
  real unreadable-folder case exercises the skip itself (the mutant
  passes over the ``PermissionError`` and refuses with ``differ`` instead
  of the permission message) [plan :1375] -- if that case is skipped on a
  machine, its coverage there is unavailable.
- implemented; reviewed: pending; mutation-verified: pending: M114
  [plan :1567] -- the mode and type of an ``ls-tree`` record not checked:
  any three fields with a 40-digit id are accepted. Killed by the
  changed-output cases with ``100664 blob``, ``040000 tree`` and
  ``100644 commit`` [plan :1375].
- implemented; reviewed: pending; mutation-verified: pending: M115
  [plan :1568] -- the mode and the type each checked against its own
  list, not as one of the four pairs. Killed by the changed-output case
  with ``100644 commit`` [plan :1375].
- implemented; reviewed: pending; mutation-verified: n/a this round
  (none of M107, M107b, M107c, M110, M114 and M115 cites them):
  ``subprocess.run`` and failure-stub cases [plan :1375] -- a stub that
  raises ``FileNotFoundError`` on every call; a stub that raises
  ``subprocess.TimeoutExpired`` on every call; a stub that returns a
  ``CompletedProcess`` with return code 128 on every call; and a
  ``package_snapshot`` stub that raises ``OSError("forced")``.
- implemented; reviewed: pending; mutation-verified: n/a (no mutant row):
  a paired control for the ``os.scandir`` stub used by M110's stub case above
  -- the same stub, aimed at a path the snapshot never scans
  (``tmp_path / "unused"``), records exactly ``package_dir`` and
  ``package_dir / "sub"`` and leaves the clean manifest returned, showing
  the stub mechanism alone causes no refusal.
- implemented; reviewed: pending; mutation-verified: n/a (no mutant row
  cites it): audit (plan :1380, no test -- a check of the other test files):
  every other test in ``tests/`` that drives ``daily_shadow.main`` past
  argument parsing replaces ``check_commit`` with a stub shaped
  ``def _stub_check_commit(package_dir: Path, *, commit_sha: str) ->
  dict[str, object]: return daily_shadow.package_manifest(package_dir)``
  -- the ``package_dir`` it is given, with ``package_manifest`` looked
  up on the ``daily_shadow`` module at call time. Confirmed conforming,
  one module-level helper each: ``test_daily_shadow_leak_rain.py``,
  ``test_daily_shadow_not_built.py``, ``test_daily_shadow_copy_changed.py``,
  ``test_daily_shadow_code_changed.py``,
  ``test_daily_shadow_span_check.py``, ``test_daily_shadow_prepare_open.py``,
  ``test_daily_shadow_batch2_mutants.py``, ``test_daily_shadow_rain_rules.py``,
  ``test_daily_shadow_published_comparison.py``,
  ``test_daily_shadow_output_folder.py`` and
  ``test_daily_shadow_amount_known_days.py``.
  ``test_daily_shadow_batch1_mutants.py`` drives ``main()`` only through
  ``test_daily_shadow_output_folder._run_main``, so it is covered by
  that helper's stub. No non-conforming case found.
"""

from __future__ import annotations

import builtins
import contextlib
import io
import json
import os
import shutil
import sqlite3
import stat
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import cast

import pytest

import wxverify
from tests.helpers import asof_make_site
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow

_WIRING_COMMIT_SHA = "f" * 40

_INIT_BYTES = b"# package init\n"
_A_BYTES = b"VALUE_A = 1\n"
_B_BYTES = b"VALUE_B = 2\n"
_PYPROJECT_BYTES_1 = b'[project]\nname = "wxverify"\nversion = "0.0.1"\n'
_PYPROJECT_BYTES_2 = b'[project]\nname = "wxverify"\nversion = "0.0.2"\n'


def _fixture_git_env(xdg_config_home: Path) -> dict[str, str]:
    """The environment every fixture-building git command runs with.

    ``os.environ`` without any name starting ``GIT_``, plus
    ``GIT_CONFIG_GLOBAL=/dev/null``, ``GIT_CONFIG_NOSYSTEM=1`` and
    ``GIT_ATTR_NOSYSTEM=1``, and with ``XDG_CONFIG_HOME`` set to
    ``xdg_config_home``, an empty directory outside the repository (plan
    :1373). Git then reads no global or system configuration file and no
    system-wide attributes file, and it looks for its default per-user
    ignore and attributes files, ``$XDG_CONFIG_HOME/git/ignore`` and
    ``$XDG_CONFIG_HOME/git/attributes`` (read when no setting names
    ``core.excludesFile`` or ``core.attributesFile``), under
    ``xdg_config_home``, which holds neither.
    Removing the inherited ``GIT_*`` names prevents a calling process,
    including a hook, from supplying repository, index, object-store or
    configuration overrides through those variables.
    """
    env = {
        name: value for name, value in os.environ.items() if not name.startswith("GIT_")
    }
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_ATTR_NOSYSTEM"] = "1"
    env["XDG_CONFIG_HOME"] = str(xdg_config_home)
    return env


def _git(repo: Path, xdg_config_home: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        env=_fixture_git_env(xdg_config_home),
        capture_output=True,
        check=True,
        timeout=30,
    )
    return result.stdout


def _commit(repo: Path, xdg_config_home: Path, message: str) -> str:
    """Stage every change with ``git add -A`` and commit it; return the new sha."""
    _git(repo, xdg_config_home, "add", "-A")
    _git(
        repo,
        xdg_config_home,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "commit",
        "-m",
        message,
    )
    return _git(repo, xdg_config_home, "rev-parse", "HEAD").decode("ascii").strip()


def _make_repo(
    tmp_path: Path,
    *,
    repo_name: str = "repo",
    package_path: str = "wxverify/wxverify",
    a_bytes: bytes = _A_BYTES,
) -> tuple[Path, str]:
    """Build the fixture repository; return (package_dir, second commit sha).

    ``repo_name`` names the repository's folder directly under
    ``tmp_path`` (default ``"repo"``); ``package_path`` is the package
    directory's path inside that repository (default
    ``"wxverify/wxverify"``), with ``pyproject.toml`` committed in its
    parent; ``a_bytes`` is ``a.py``'s content. ``tmp_path / "xdg"`` is
    created, if it does not already exist, as the shared
    ``XDG_CONFIG_HOME`` every repository a test builds uses.
    """
    xdg_config_home = tmp_path / "xdg"
    xdg_config_home.mkdir(exist_ok=True)
    repo = tmp_path / repo_name
    repo.mkdir()
    (repo / ".gitignore").write_bytes(b"__pycache__/\ndata/\n")
    package_dir = repo / package_path
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_bytes(_INIT_BYTES)
    (package_dir / "a.py").write_bytes(a_bytes)
    (package_dir / "sub").mkdir()
    (package_dir / "sub" / "b.py").write_bytes(_B_BYTES)
    pyproject = package_dir.parent / "pyproject.toml"
    pyproject.write_bytes(_PYPROJECT_BYTES_1)

    _git(repo, xdg_config_home, "init", "--initial-branch=main")
    _git(repo, xdg_config_home, "add", "-A")
    _git(
        repo,
        xdg_config_home,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "commit",
        "-m",
        "first",
    )

    pyproject.write_bytes(_PYPROJECT_BYTES_2)
    _git(repo, xdg_config_home, "add", "-A")
    _git(
        repo,
        xdg_config_home,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "commit",
        "-m",
        "second",
    )
    second_sha = (
        _git(repo, xdg_config_home, "rev-parse", "HEAD").decode("ascii").strip()
    )
    return package_dir, second_sha


def test_commit_check_clean_package_returns_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clean case [plan :1374]: a manifest equal to ``package_manifest``."""
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)


def test_commit_check_passthrough_stub_control_still_returns_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired control: a stub that passes every call through unchanged
    still returns the clean manifest, and sees exactly one ``ls-tree``
    call -- the stub mechanism alone causes no refusal.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    real_run = subprocess.run
    ls_tree_calls: list[list[str]] = []

    def passthrough_stub(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        if "ls-tree" in args:
            ls_tree_calls.append(list(args))
        return real_run(
            args, capture_output=capture_output, check=check, timeout=timeout, env=env
        )

    monkeypatch.setattr(daily_shadow.subprocess, "run", passthrough_stub)
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)
    assert len(ls_tree_calls) == 1


def test_m113_committed_path_with_undecodable_byte_never_matches_replacement_char(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M113 [plan :1566]: an ``ls-tree`` path decoded with
    ``errors="replace"`` turns an undecodable byte into U+FFFD, which then
    wrongly matches a working file whose name holds U+FFFD in its place.
    This test corrupts the ``ls-tree`` record for ``a.py`` to
    ``b"a\\xff.py"`` (an undecodable byte) and renames the working
    ``a.py`` to hold U+FFFD in its place; the real decoder
    (``os.fsdecode``, surrogateescape) keeps the two names distinct and
    refuses, while the mutant's ``errors="replace"`` decode would make
    them equal and let ``check_commit`` return.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    real_run = subprocess.run
    replaced: list[bool] = []

    def corrupting_stub(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        result = real_run(
            args, capture_output=capture_output, check=check, timeout=timeout, env=env
        )
        if "ls-tree" in args:
            assert result.stdout.count(b"\ta.py\0") == 1
            corrupted_stdout = result.stdout.replace(b"\ta.py\0", b"\ta\xff.py\0")
            replaced.append(True)
            result = subprocess.CompletedProcess(
                result.args, result.returncode, corrupted_stdout, result.stderr
            )
        return result

    monkeypatch.setattr(daily_shadow.subprocess, "run", corrupting_stub)

    renamed_a = package_dir / ("a" + chr(0xFFFD) + ".py")
    (package_dir / "a.py").rename(renamed_a)

    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    expected_tail = (
        "changed []; added "
        + json.dumps(["a" + chr(0xFFFD) + ".py"])
        + "; removed "
        + json.dumps(["a" + chr(0xDCFF) + ".py"])
    )
    assert message.endswith(expected_tail)
    assert replaced == [True]


def test_commit_check_refuses_sha_with_one_hex_digit_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 1 [plan :1375]: a sha with one hex digit changed refuses,
    naming the wrong sha it was asked to match.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    flipped_digit = "1" if second_sha[0] == "0" else "0"
    wrong_sha = flipped_digit + second_sha[1:]
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=wrong_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith(f"not --commit-sha {wrong_sha}")


def test_commit_check_refuses_first_commit_sha_naming_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 2 [plan :1375]: the first commit's sha, whose package files
    equal the second's, so only the HEAD check can refuse it, with a
    message naming HEAD's (the second commit's) own sha.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    first_sha = (
        _git(repo, xdg_config_home, "rev-parse", "HEAD~1").decode("ascii").strip()
    )
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=first_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert repr(second_sha[:64]) in message
    assert message.endswith(f"not --commit-sha {first_sha}")


def test_commit_check_refuses_a_py_rewritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 3 [plan :1375]: ``a.py`` rewritten on disk."""
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    (package_dir / "a.py").write_bytes(b"VALUE_A = 999\n")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed ["a.py"]; added []; removed []')


def test_commit_check_refuses_a_py_rewritten_and_staged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 4 [plan :1375]: the same rewrite, staged with ``git add``,
    refuses the same way -- no command reads the index.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    (package_dir / "a.py").write_bytes(b"VALUE_A = 999\n")
    _git(repo, xdg_config_home, "add", "wxverify/wxverify/a.py")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed ["a.py"]; added []; removed []')


def test_commit_check_refuses_untracked_c_py(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 5 [plan :1375]: an untracked ``c.py`` in the package."""
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    (package_dir / "c.py").write_bytes(b"VALUE_C = 1\n")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed []; added ["c.py"]; removed []')


def test_commit_check_refuses_sub_b_py_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 6 [plan :1375]: ``sub/b.py`` deleted from disk."""
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    (package_dir / "sub" / "b.py").unlink()
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed []; added []; removed ["sub/b.py"]')


def test_m104_ignored_data_py_under_package_still_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 7, M104 [plan :1557, :1375]: an ignored ``data/x.py`` under
    the package still refuses -- the byte comparison is not skipped just
    because ``.gitignore`` hides the file from git.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    (package_dir / "data").mkdir()
    (package_dir / "data" / "x.py").write_bytes(b"VALUE_X = 1\n")
    # check-ignore exits 1 (and _git raises) unless .gitignore ignores it.
    _git(
        tmp_path / "repo",
        tmp_path / "xdg",
        "check-ignore",
        "--quiet",
        "wxverify/wxverify/data/x.py",
    )
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed []; added ["data/x.py"]; removed []')


@pytest.mark.parametrize(
    "flag",
    ["--assume-unchanged", "--skip-worktree"],
    ids=["assume-unchanged", "skip-worktree"],
)
def test_m104_a_py_rewritten_after_index_flag_still_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    """Cases 8/9, M104 [plan :1557, :1375]: ``a.py`` rewritten after
    ``git update-index --assume-unchanged`` or ``--skip-worktree`` still
    refuses, after first confirming the fixture's own ``git status
    --porcelain`` prints nothing -- a change ``git status`` cannot see.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    _git(repo, xdg_config_home, "update-index", flag, "wxverify/wxverify/a.py")
    (package_dir / "a.py").write_bytes(b"VALUE_A = 999\n")
    status = _git(repo, xdg_config_home, "status", "--porcelain")
    assert status == b""
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed ["a.py"]; added []; removed []')


def test_m111_a_py_renamed_to_c_py_bytes_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 10, M111 [plan :1564, :1375]: ``a.py`` renamed to ``c.py``
    with its bytes unchanged -- comparing sorted object ids alone (the
    mutant) would not catch this, since the id set is the same; comparing
    each path with its id (the real code) refuses.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    (package_dir / "a.py").rename(package_dir / "c.py")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed []; added ["c.py"]; removed ["a.py"]')


def test_m112_committed_cr_name_renamed_to_lf_name_bytes_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 11, M112 [plan :1565, :1375]: a third commit adds
    ``"a\\rb.py"`` (a carriage return in the name); that file is then
    renamed to ``"a\\nb.py"`` (a newline) with its bytes unchanged. Text
    mode (the mutant) would turn the committed record's ``\\r`` into
    ``\\n`` on read, making the two names equal; bytes mode (the real
    code) keeps them distinct and refuses.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, _ = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    cr_name = "a\rb.py"
    (package_dir / cr_name).write_bytes(b"VALUE_CR = 1\n")
    third_sha = _commit(repo, xdg_config_home, "add cr name")
    (package_dir / cr_name).rename(package_dir / "a\nb.py")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=third_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    expected_tail = (
        "changed []; added "
        + json.dumps(["a\nb.py"])
        + "; removed "
        + json.dumps(["a\rb.py"])
    )
    assert message.endswith(expected_tail)


def test_commit_check_refuses_committed_symlink_l_py(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 12 [plan :1375]: a third commit adds ``l.py`` as a symbolic
    link to ``a.py``; checked with that commit's sha, it refuses by the
    commit's mode.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, _ = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    os.symlink("a.py", package_dir / "l.py")
    third_sha = _commit(repo, xdg_config_home, "add symlink l.py")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=third_sha)
    assert str(excinfo.value) == (
        f"commit check: {third_sha} holds {json.dumps('l.py')} as a symbolic link"
    )


def test_m116_committed_symlink_refuses_even_when_disk_object_id_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 13, M116 [plan :1569, :1375]: the same third commit, then the
    package's ``l.py`` replaced by a regular file holding the four bytes
    ``a.py`` -- the committed link's own object id -- so only the
    commit's mode can refuse it. A symbolic link or submodule record
    treated as a regular file (the mutant) would match it and return.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, _ = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    os.symlink("a.py", package_dir / "l.py")
    third_sha = _commit(repo, xdg_config_home, "add symlink l.py")
    (package_dir / "l.py").unlink()
    (package_dir / "l.py").write_bytes(b"a.py")
    assert not (package_dir / "l.py").is_symlink()
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=third_sha)
    assert str(excinfo.value) == (
        f"commit check: {third_sha} holds {json.dumps('l.py')} as a symbolic link"
    )


def test_m117_committed_submodule_entry_refuses_despite_no_py_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 14, M116/M117 [plan :1569, :1570, :1375]: a third commit adds
    a submodule entry ``m`` (via ``git update-index --add
    --cacheinfo``), though the package directory holds nothing named
    ``m``. It refuses by the commit's mode alone, even though ``m`` does
    not end ``.py`` -- a mutant that refuses a symbolic link or submodule
    only when its path ends ``.py`` would not catch this.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, _ = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    first_sha = (
        _git(repo, xdg_config_home, "rev-parse", "HEAD~1").decode("ascii").strip()
    )
    _git(
        repo,
        xdg_config_home,
        "update-index",
        "--add",
        "--cacheinfo",
        f"160000,{first_sha},wxverify/wxverify/m",
    )
    _git(
        repo,
        xdg_config_home,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "commit",
        "-m",
        "add submodule m",
    )
    third_sha = _git(repo, xdg_config_home, "rev-parse", "HEAD").decode("ascii").strip()
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=third_sha)
    message = str(excinfo.value)
    expected_message = (
        f"commit check: {third_sha} holds {json.dumps('m')} as a submodule"
    )
    assert message == expected_message


def test_m118_a_py_replaced_by_symlink_to_outside_file_refuses_as_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 15, M118 [plan :1571, :1375]: ``a.py`` replaced by a symbolic
    link to ``outside.py``, which holds ``a.py``'s own bytes. It refuses
    because the link is detected (``os.lstat``), not because the bytes
    differ -- a mutant that reads through the link would see matching
    bytes and return normally.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    outside = tmp_path / "outside.py"
    outside.write_bytes(_A_BYTES)
    (package_dir / "a.py").unlink()
    os.symlink(outside, package_dir / "a.py")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert '"a.py" is a symbolic link' in message
    assert "differ" not in message


def test_m119_symlinked_directory_in_package_refuses_as_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 16, M119 [plan :1572, :1375]: a symbolic link ``ext`` in the
    package to a directory ``outside_dir`` that holds ``x.py``. It
    refuses because the link is detected (``os.lstat``) on the
    subdirectory listing, not because the bytes differ -- a mutant that
    skips that check would pass the link's target directory over.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    outside_dir = tmp_path / "outside_dir"
    outside_dir.mkdir()
    (outside_dir / "x.py").write_bytes(b"VALUE_X = 1\n")
    os.symlink(outside_dir, package_dir / "ext")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert '"ext" is a symbolic link' in message
    assert "differ" not in message


def test_commit_check_refuses_rewritten_copy_elsewhere_in_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 17 [plan :1375]: an untracked copy of the package at
    ``elsewhere/pkg/`` in the same repository, with ``a.py`` rewritten in
    the copy, checked on the copy -- it refuses because its bytes differ,
    not because of where it sits.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    elsewhere = repo / "elsewhere" / "pkg"
    shutil.copytree(package_dir, elsewhere)
    (elsewhere / "a.py").write_bytes(b"VALUE_A = 999\n")
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(elsewhere, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith('changed ["a.py"]; added []; removed []')


def test_commit_check_refuses_when_commit_has_no_wxverify_wxverify_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 18 [plan :1375]: a repository whose commits hold the package
    files at ``other/wxverify/`` and nothing at ``wxverify/wxverify/``,
    checked on ``other/wxverify/`` -- ``ls-tree``'s tree path is the fixed
    string ``wxverify/wxverify``, so it exits non-zero regardless of the
    package directory the check was given.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(
        tmp_path, repo_name="other_repo", package_path="other/wxverify"
    )
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith(
        f"commit check: git ls-tree -r -z --full-tree {second_sha}:wxverify/wxverify"
        " exited "
    )


def test_commit_check_refuses_when_run_on_subdirectory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 19 [plan :1375]: the check run on ``wxverify/wxverify/sub/``
    instead of the package directory -- ``ls-tree``'s tree path stays
    ``wxverify/wxverify``, so the commit's full file list is compared
    against the subdirectory's single file.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir / "sub", commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert message.endswith(
        'changed []; added ["b.py"]; removed ["__init__.py", "a.py", "sub/b.py"]'
    )


def test_commit_check_refuses_package_dir_in_no_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 20 [plan :1375]: a package directory in no repository --
    ``git rev-parse`` exits non-zero. ``GIT_CEILING_DIRECTORIES`` is set
    to ``tmp_path`` so git's upward search stops there instead of
    reaching this worktree's own repository.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    loose = tmp_path / "loose"
    loose.mkdir()
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(loose, commit_sha="0" * 40)
    message = str(excinfo.value)
    assert message.startswith(
        "commit check: git rev-parse --verify HEAD^{commit} exited "
    )


def test_commit_check_returns_manifest_with_pycache_pyc_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 21 [plan :1376]: a ``__pycache__/x.pyc`` file present is
    ignored, so the check still returns.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    pycache = package_dir / "__pycache__"
    pycache.mkdir()
    (pycache / "x.pyc").write_bytes(b"not python bytecode, just bytes")
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)


def test_commit_check_returns_manifest_with_untracked_notes_txt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 22 [plan :1376]: an untracked ``notes.txt`` in the package is
    outside the ``.py`` scope, so the check still returns.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    (package_dir / "notes.txt").write_bytes(b"not a python file\n")
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)


def test_commit_check_returns_manifest_with_pyproject_toml_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 23 [plan :1376]: ``pyproject.toml``, outside the package
    directory, changed -- the check still returns; refusals above are not
    caused by the fixture.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    pyproject = package_dir.parent / "pyproject.toml"
    pyproject.write_bytes(b'[project]\nname = "wxverify"\nversion = "9.9.9"\n')
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)


def test_commit_check_returns_manifest_for_unchanged_copy_elsewhere(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 24 [plan :1376]: an unchanged untracked copy of the package
    at ``elsewhere/pkg/`` in the same repository -- where the package
    sits does not matter when its bytes have not changed.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    elsewhere = repo / "elsewhere" / "pkg"
    shutil.copytree(package_dir, elsewhere)
    manifest = daily_shadow.check_commit(elsewhere, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(elsewhere)


def test_m108_committed_tab_name_is_split_at_first_tab(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 25, M108 [plan :1561, :1376]: a third commit adds
    ``"t\\tab.py"`` (a tab in the name); the check still returns, and its
    manifest lists the name split at its first tab only. A mutant that
    splits at every tab, or at the last tab, would produce a different
    path.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, _ = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    tab_name = "t\tab.py"
    (package_dir / tab_name).write_bytes(b"VALUE_TAB = 1\n")
    third_sha = _commit(repo, xdg_config_home, "add tab name")
    manifest = daily_shadow.check_commit(package_dir, commit_sha=third_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)
    files = cast("list[list[str]]", manifest["files"])
    assert any(path == tab_name for path, _digest in files)


def test_m112_committed_cr_name_manifest_lists_it_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 26, M112 [plan :1565, :1376]: a third commit adds
    ``"a\\rb.py"`` (a carriage return in the name); the check still
    returns, and its manifest lists the name with the carriage return
    kept. Text-mode capture of ``ls-tree``'s output (the mutant) would
    turn it into a newline instead.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, _ = _make_repo(tmp_path)
    repo = tmp_path / "repo"
    xdg_config_home = tmp_path / "xdg"
    cr_name = "a\rb.py"
    (package_dir / cr_name).write_bytes(b"VALUE_CR = 1\n")
    third_sha = _commit(repo, xdg_config_home, "add cr name")
    manifest = daily_shadow.check_commit(package_dir, commit_sha=third_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)
    files = cast("list[list[str]]", manifest["files"])
    assert any(path == cr_name for path, _digest in files)


def test_m109_open_recorder_sees_each_py_file_opened_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 27, M109 [plan :1562, :1376]: ``io.open`` and ``builtins.open``
    both replaced by one recorder that calls the real ``open`` -- the
    check still returns the clean manifest, and the recorder shows each
    of the package's three ``.py`` files (``__init__.py``, ``a.py``,
    ``sub/b.py``) opened exactly once, and no other file under the
    package directory opened. ``pathlib/_local.py``'s ``Path.open``
    (CPython 3.13) calls ``io.open(self, mode, buffering, encoding,
    errors, newline)`` directly, never ``builtins.open``, so
    ``package_snapshot``'s one ``Path.read_bytes()`` per file reaches the
    recorder through ``io.open`` alone; ``builtins.open`` is replaced too,
    per the plan, but the recorder is never expected to see a call there.
    M109 (``package_snapshot`` reads each file twice, once for its
    sha256 and once for its object id) would make the recorder see each
    of the three files opened twice.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    expected_manifest = daily_shadow.package_manifest(package_dir)
    real_open = cast("Callable[..., object]", io.open)
    opened: list[Path] = []

    def recorder(*args: object, **kwargs: object) -> object:
        if args:
            first = args[0]
            if isinstance(first, (str, os.PathLike)):
                opened.append(Path(cast("str | os.PathLike[str]", first)))
        return real_open(*args, **kwargs)

    monkeypatch.setattr(io, "open", recorder)
    monkeypatch.setattr(builtins, "open", recorder)
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == expected_manifest
    under_package = [path for path in opened if package_dir in path.parents]
    counts: dict[Path, int] = {}
    for path in under_package:
        counts[path] = counts.get(path, 0) + 1
    expected = {
        package_dir / "__init__.py": 1,
        package_dir / "a.py": 1,
        package_dir / "sub" / "b.py": 1,
    }
    assert counts == expected


def test_m105_redirected_git_env_vars_do_not_divert_the_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1377], M105 [plan :1558]: a second repository is built the
    same way at ``tmp_path / "other"``, with other bytes in ``a.py``.
    After both are built, ``GIT_DIR``, ``GIT_WORK_TREE`` and
    ``GIT_INDEX_FILE`` are set naming the second repository's ``.git``
    directory, its top and its index. The check on the first
    repository's package, with the first repository's HEAD sha, still
    returns the first package's manifest: ``_git_environment`` strips
    every inherited ``GIT_*`` name except ``GIT_CEILING_DIRECTORIES``, so
    these three names never reach git. M105 (the inherited environment
    passed unchanged) would instead let ``GIT_DIR``/``GIT_WORK_TREE``
    point git at the second repository, so ``git rev-parse --verify
    HEAD^{commit}`` would resolve the second repository's HEAD and
    compare it with the first repository's sha, refusing.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    _make_repo(tmp_path, repo_name="other", a_bytes=b"VALUE_A = 777\n")
    other_git_dir = tmp_path / "other" / ".git"
    monkeypatch.setenv("GIT_DIR", str(other_git_dir))
    monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path / "other"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(other_git_dir / "index"))
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)


def test_subprocess_recorder_sees_exact_two_calls_with_filtered_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1378]: the same redirected environment as above, with
    ``GIT_CONFIG_COUNT=1``, ``GIT_CONFIG_KEY_0=user.name``,
    ``GIT_CONFIG_VALUE_0=t``, ``GIT_CONFIG_GLOBAL`` naming a file in
    ``tmp_path`` and ``WXVERIFY_TEST_KEEP=1`` also set. A recorder that
    wraps the real ``subprocess.run``, installed after the fixture is
    built, shows exactly two calls: the exact argument lists for
    ``rev-parse --verify HEAD^{commit}`` and ``ls-tree -r -z --full-tree
    <sha>:wxverify/wxverify``, no ``shell=True``, ``timeout=30``, none
    of ``text``/``encoding``/``errors``/``universal_newlines``, and an
    environment whose ``GIT_``-prefixed names are exactly the four
    ``_git_environment`` sets, keeping ``WXVERIFY_TEST_KEEP`` and
    ``PATH`` as the test's environment has them. Plan :1378 requires
    only the call shape and the absence of ``shell=True``; the
    ``capture_output is True`` and ``check is False`` assertions below
    go beyond it. No mutant in this round's ledger is cited against this
    test; it pins the exact call shape ``check_commit`` makes; the M105
    case above relies on the environment filtering it pins.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    _make_repo(tmp_path, repo_name="other", a_bytes=b"VALUE_A = 777\n")
    other_git_dir = tmp_path / "other" / ".git"
    monkeypatch.setenv("GIT_DIR", str(other_git_dir))
    monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path / "other"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(other_git_dir / "index"))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "user.name")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "t")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "hostile-gitconfig"))
    monkeypatch.setenv("WXVERIFY_TEST_KEEP", "1")
    expected_path = os.environ["PATH"]

    real_run = cast("Callable[..., subprocess.CompletedProcess[bytes]]", subprocess.run)
    calls: list[tuple[list[str], dict[str, object]]] = []

    def recorder(
        args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append((list(args), dict(kwargs)))
        return real_run(args, **kwargs)

    monkeypatch.setattr(daily_shadow.subprocess, "run", recorder)
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert manifest == daily_shadow.package_manifest(package_dir)
    assert len(calls) == 2
    expected_args = [
        ["git", "-C", str(package_dir), "rev-parse", "--verify", "HEAD^{commit}"],
        [
            "git",
            "-C",
            str(package_dir),
            "ls-tree",
            "-r",
            "-z",
            "--full-tree",
            f"{second_sha}:wxverify/wxverify",
        ],
    ]
    assert [args for args, _kwargs in calls] == expected_args
    for _args, kwargs in calls:
        assert not kwargs.get("shell", False)
        assert kwargs["capture_output"] is True
        assert kwargs["check"] is False
        assert kwargs["timeout"] == 30
        for forbidden in ("text", "encoding", "errors", "universal_newlines"):
            assert forbidden not in kwargs
        env = cast("dict[str, str]", kwargs["env"])
        git_names = {
            name: value for name, value in env.items() if name.startswith("GIT_")
        }
        assert git_names == {
            "GIT_CEILING_DIRECTORIES": str(tmp_path),
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
        }
        assert env.get("WXVERIFY_TEST_KEEP") == "1"
        assert env.get("PATH") == expected_path


def _make_valid_copy(tmp_path: Path) -> tuple[Path, int]:
    """A real, fully-migrated copy holding one site, no feeds or stations.

    Enough for a ``--counts-only`` run to complete past the commit check
    and ``open_copy_readonly`` (plan :1379's wiring cases). Returns
    (copy, site_id).
    """
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "commit-check-wiring-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    return copy, site_id


def _run_main_counts_only(
    *,
    copy: Path,
    out: Path,
    commit_sha: str,
    site_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[int, str, str]:
    """Run ``main()`` once with a ``--counts-only --variable temperature``
    argv; return (exit_code, stdout, stderr). ``check_commit`` is left for
    the caller to monkeypatch.
    """
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
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
        commit_sha,
        "--counts-only",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def test_wiring_check_commit_raises_exits_2_before_open_nothing_under_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wiring case 1 [plan :1379]: ``main()`` with ``check_commit``
    replaced by a recorder that raises ``RefusedError`` is called exactly
    once, exits 2, prints a single ``refused:`` line on standard error
    carrying the recorder's own message, never calls
    ``open_copy_readonly``, and leaves nothing under ``--out`` -- the
    raise happens before ``out`` is ever touched. It also prints no
    result line [plan :1393].
    """
    copy, site_id = _make_valid_copy(tmp_path)
    out = tmp_path / "out"
    open_calls = 0
    check_commit_calls = 0
    real_open_copy_readonly = daily_shadow.open_copy_readonly

    def open_recorder(path: Path) -> object:
        nonlocal open_calls
        open_calls += 1
        return real_open_copy_readonly(path)

    def raising_check_commit(
        package_dir: Path, *, commit_sha: str
    ) -> dict[str, object]:
        del package_dir, commit_sha
        nonlocal check_commit_calls
        check_commit_calls += 1
        raise daily_shadow.RefusedError("refused for test")

    monkeypatch.setattr(daily_shadow, "check_commit", raising_check_commit)
    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    exit_code, out_text, err_text = _run_main_counts_only(
        copy=copy,
        out=out,
        commit_sha=_WIRING_COMMIT_SHA,
        site_id=site_id,
        monkeypatch=monkeypatch,
    )
    assert exit_code == 2
    refused_lines = [
        line for line in err_text.splitlines() if line.startswith("refused:")
    ]
    assert len(refused_lines) == 1
    assert "refused for test" in refused_lines[0]
    assert check_commit_calls == 1
    assert open_calls == 0
    assert not out.exists()
    assert not any(
        line.startswith(daily_shadow.RESULT_PREFIX) for line in out_text.splitlines()
    )


def test_wiring_check_commit_returns_manifest_called_once_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wiring case 2 [plan :1379]: ``main()`` with ``check_commit``
    replaced by a recorder that returns
    ``daily_shadow.package_manifest(package_dir)`` is called once,
    before ``open_copy_readonly``, with the value ``package_root()``
    returns (``Path(wxverify.__file__).parent``) and the given sha; the
    counts-only header's ``commit_sha`` is that sha and its ``manifest``
    is what the recorder returned.
    """
    copy, site_id = _make_valid_copy(tmp_path)
    out = tmp_path / "out"
    events: list[tuple[str, ...]] = []
    check_commit_dirs: list[Path] = []
    returned_manifests: list[dict[str, object]] = []

    def recording_check_commit(
        package_dir: Path, *, commit_sha: str
    ) -> dict[str, object]:
        events.append(("check_commit", str(package_dir), commit_sha))
        check_commit_dirs.append(package_dir)
        manifest = daily_shadow.package_manifest(package_dir)
        returned_manifests.append(manifest)
        return manifest

    real_open_copy_readonly = daily_shadow.open_copy_readonly

    def open_recorder(path: Path) -> object:
        events.append(("open_copy_readonly",))
        return real_open_copy_readonly(path)

    monkeypatch.setattr(daily_shadow, "check_commit", recording_check_commit)
    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    exit_code, out_text, err_text = _run_main_counts_only(
        copy=copy,
        out=out,
        commit_sha=_WIRING_COMMIT_SHA,
        site_id=site_id,
        monkeypatch=monkeypatch,
    )
    assert exit_code == 0, err_text
    assert daily_shadow.package_root() == Path(wxverify.__file__).parent
    assert [event for event in events if event[0] == "check_commit"] == [
        ("check_commit", str(daily_shadow.package_root()), _WIRING_COMMIT_SHA)
    ]
    assert check_commit_dirs == [daily_shadow.package_root()]
    assert len(returned_manifests) == 1
    check_index = next(
        i for i, event in enumerate(events) if event[0] == "check_commit"
    )
    open_events = [
        i for i, event in enumerate(events) if event[0] == "open_copy_readonly"
    ]
    assert len(open_events) == 1
    open_index = open_events[0]
    assert check_index < open_index
    result_lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(result_lines) == 1
    payload = json.loads(result_lines[0][len(daily_shadow.RESULT_PREFIX) :])
    run_dir = out / payload["run_dir"]
    header = json.loads((run_dir / "header.json").read_bytes())
    assert header["commit_sha"] == _WIRING_COMMIT_SHA
    assert header["manifest"] == returned_manifests[0]


def test_m106_wiring_check_commit_extra_file_drives_code_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wiring case 3, M106 [plan :1559, :1379]: ``main()`` with
    ``check_commit`` replaced by a recorder that returns the clean
    manifest plus one more ``files`` entry, ``["x.py", "0" * 64]`` -- a
    path the package does not hold -- exits 1; the run directory holds
    only ``error.json``, whose ``stage`` is ``code_changed``, whose
    ``manifest`` equals what the recorder returned (``x.py`` included),
    and whose ``error`` ends ``changed []; added []; removed ["x.py"]``,
    so the manifest ``main()`` records and compares at the end is the
    one ``check_commit`` returned. M106 (``main()`` records and compares
    at the end a manifest it computes itself before the open, not the
    one ``check_commit`` returns) would instead compare a manifest it
    computed itself -- also without ``x.py`` -- against the real
    post-run manifest, which match, so no ``CodeChangedError`` would be
    raised and the run would exit 0 with no ``error.json`` at all.
    """
    copy, site_id = _make_valid_copy(tmp_path)
    out = tmp_path / "out"
    returned_manifests: list[dict[str, object]] = []

    def extra_file_check_commit(
        package_dir: Path, *, commit_sha: str
    ) -> dict[str, object]:
        del commit_sha
        manifest = daily_shadow.package_manifest(package_dir)
        files = cast("list[list[str]]", manifest["files"])
        extra_manifest = {**manifest, "files": [*files, ["x.py", "0" * 64]]}
        returned_manifests.append(extra_manifest)
        return extra_manifest

    monkeypatch.setattr(daily_shadow, "check_commit", extra_file_check_commit)
    exit_code, out_text, _err_text = _run_main_counts_only(
        copy=copy,
        out=out,
        commit_sha=_WIRING_COMMIT_SHA,
        site_id=site_id,
        monkeypatch=monkeypatch,
    )
    assert exit_code == 1
    result_lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(result_lines) == 1
    payload = json.loads(result_lines[0][len(daily_shadow.RESULT_PREFIX) :])
    run_dir = out / payload["run_dir"]
    assert sorted(path.name for path in run_dir.iterdir()) == ["error.json"]
    error = json.loads((run_dir / "error.json").read_bytes())
    assert len(returned_manifests) == 1
    assert error["stage"] == "code_changed"
    assert error["manifest"] == returned_manifests[0]
    assert error["error"].endswith('changed []; added []; removed ["x.py"]')


def test_commit_check_refuses_when_subprocess_run_cannot_find_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1375]: ``subprocess.run`` replaced by a stub that raises
    ``FileNotFoundError`` on every call refuses. The stub fires on every
    git call, so it hits the first one, ``git rev-parse --verify
    HEAD^{commit}``.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)

    def raising_stub(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        raise FileNotFoundError("no such file: git")

    monkeypatch.setattr(daily_shadow.subprocess, "run", raising_stub)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith(
        "commit check: git rev-parse --verify HEAD^{commit} could not run: "
    )


def test_commit_check_refuses_when_subprocess_run_times_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1375]: ``subprocess.run`` replaced by a stub that raises
    ``subprocess.TimeoutExpired`` on every call refuses. The stub fires on
    every git call, so it hits the first one, ``git rev-parse --verify
    HEAD^{commit}``.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)

    def raising_stub(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    monkeypatch.setattr(daily_shadow.subprocess, "run", raising_stub)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith(
        "commit check: git rev-parse --verify HEAD^{commit} could not run: "
    )


def test_commit_check_refuses_when_subprocess_run_returns_exit_128(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1375]: ``subprocess.run`` replaced by a stub that returns a
    ``CompletedProcess`` with return code 128 on every call refuses. The
    stub fires on every git call, so it hits the first one, ``git
    rev-parse --verify HEAD^{commit}``; empty stderr makes the reason
    ``"no message"``.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)

    def returning_stub(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(args, 128, b"", b"")

    monkeypatch.setattr(daily_shadow.subprocess, "run", returning_stub)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message == (
        "commit check: git rev-parse --verify HEAD^{commit} exited 128: no message"
    )


def test_commit_check_refuses_when_package_snapshot_raises_os_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[plan :1375]: ``package_snapshot`` replaced by a stub that raises
    ``OSError("forced")`` refuses with a message naming the forced error.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)

    def raising_stub(root: Path) -> tuple[dict[str, object], dict[str, str]]:
        raise OSError("forced")

    monkeypatch.setattr(daily_shadow, "package_snapshot", raising_stub)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert "forced" in message


def _scandir_raising_for(
    target: Path,
) -> tuple[Callable[[str], Iterator[os.DirEntry[str]]], list[Path]]:
    """An ``os.scandir`` stub that raises ``PermissionError("forced")`` for
    ``target`` and calls the real function for every other path; returns
    the stub and the paths it was called with.
    """
    real_scandir = os.scandir
    seen: list[Path] = []

    def stub(path: str) -> Iterator[os.DirEntry[str]]:
        seen.append(Path(path))
        if Path(path) == target:
            raise PermissionError("forced")
        return real_scandir(path)

    return stub, seen


def test_commit_check_refuses_when_os_scandir_raises_for_sub_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M110 [plan :1563, :1375]: ``os.scandir`` replaced by a stub that
    raises ``PermissionError("forced")`` for ``package_dir / "sub"`` only
    (the real function runs for every other path) refuses with a message
    naming the forced error, not a byte difference -- a directory that
    cannot be listed refuses rather than dropping ``sub/b.py`` from a
    snapshot that is then compared. Under M110 (``Path.rglob``) this case
    fails too, but on Python 3.13 not through the skip M110 names:
    ``rglob`` lists directories through ``glob._StringGlobber.scandir``,
    bound to the real ``os.scandir`` when ``glob`` is imported, so the
    stub never fires, ``sub/b.py`` is found and the check returns the
    manifest.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    stub, seen = _scandir_raising_for(package_dir / "sub")
    monkeypatch.setattr(os, "scandir", stub)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert "forced" in message
    assert "differ" not in message
    assert package_dir / "sub" in seen


def test_commit_check_os_scandir_stub_aimed_elsewhere_still_returns_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired control for the ``os.scandir`` stub above: the identical
    stub, aimed at a path the snapshot never scans
    (``tmp_path / "unused"``), is called for exactly ``package_dir`` and
    ``package_dir / "sub"`` and leaves the clean manifest returned -- the
    stub sits in the walk's path, and the refusal above is caused by the
    stub firing on the targeted directory, not by stubbing ``os.scandir``
    itself.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    stub, seen = _scandir_raising_for(tmp_path / "unused")
    monkeypatch.setattr(os, "scandir", stub)
    manifest = daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    assert set(seen) == {package_dir, package_dir / "sub"}
    assert manifest == daily_shadow.package_manifest(package_dir)


def test_commit_check_refuses_when_sub_directory_cannot_be_listed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M110 [plan :1563, :1375]: ``package_dir / "sub"`` made a directory
    that really cannot be listed, by removing every permission from it,
    refuses with a message equal to ``"commit check: the package's .py
    files could not be read: PermissionError: " + str(probe)``, where
    ``probe`` is the ``PermissionError`` the test's own full listing of
    ``sub`` raised first; that equality is the whole assertion. The case
    exercises the skip itself: under ``Path.rglob`` (the mutant) the
    ``PermissionError`` the real operating system raises is passed over,
    ``sub/b.py`` goes missing from the snapshot, and the check refuses
    with ``differ`` and ``changed []; added []; removed ["sub/b.py"]``
    instead of naming the permission error. A ``finally`` restores the
    saved mode. Skipped where the directory can still be listed despite
    the removed permission (a root user, or a file system that does not
    enforce the mode).
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    sub = package_dir / "sub"
    saved_mode = stat.S_IMODE(os.stat(sub).st_mode)
    os.chmod(sub, 0)
    try:
        probe: PermissionError | None = None
        try:
            with os.scandir(str(sub)) as entries:
                list(entries)
        except PermissionError as error:
            probe = error
        if probe is None:
            pytest.skip(
                "sub/ can still be listed with every permission removed "
                "(root user, or a file system that does not enforce the mode)"
            )
        with pytest.raises(daily_shadow.RefusedError) as excinfo:
            daily_shadow.check_commit(package_dir, commit_sha=second_sha)
        assert str(excinfo.value) == (
            "commit check: the package's .py files could not be read: "
            f"PermissionError: {probe}"
        )
    finally:
        os.chmod(sub, saved_mode)


def _append_record(record: bytes) -> Callable[[bytes], bytes]:
    """A corruption that appends ``record`` (with its own NUL) at the end."""

    def corrupt(original: bytes) -> bytes:
        return original + record + b"\0"

    return corrupt


def _insert_second_nul(original: bytes) -> bytes:
    """A corruption that inserts a second NUL right after the first record's."""
    first_nul = original.index(b"\0")
    return original[: first_nul + 1] + b"\0" + original[first_nul + 1 :]


def _remove_final_nul(original: bytes) -> bytes:
    """A corruption that removes the trailing NUL, so the last piece is non-empty."""
    assert original.endswith(b"\0")
    return original[:-1]


def _duplicate_a_py_record(original: bytes) -> bytes:
    """A corruption that appends the ``a.py`` record (with its own NUL) again."""
    records = original.split(b"\0")
    assert records[-1] == b""
    matches = [record for record in records[:-1] if record.endswith(b"\ta.py")]
    assert len(matches) == 1
    return original + matches[0] + b"\0"


def _ls_tree_corrupting_stub(
    corrupt: Callable[[bytes], bytes],
) -> tuple[Callable[..., subprocess.CompletedProcess[bytes]], list[bool]]:
    """Build a passthrough ``subprocess.run`` stub that corrupts only the
    ``ls-tree`` call's stdout, via ``corrupt``; returns the stub and a list
    that gains one ``True`` per ``ls-tree`` call it corrupted, so a test
    can confirm the stub actually altered an ``ls-tree`` call.
    """
    real_run = subprocess.run
    corrupted_calls: list[bool] = []

    def stub(
        args: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        result = real_run(
            args, capture_output=capture_output, check=check, timeout=timeout, env=env
        )
        if "ls-tree" in args:
            corrupted_stdout = corrupt(result.stdout)
            corrupted_calls.append(True)
            result = subprocess.CompletedProcess(
                result.args, result.returncode, corrupted_stdout, result.stderr
            )
        return result

    return stub, corrupted_calls


_LS_TREE_CANNOT_PARSE_CASES: list[tuple[str, Callable[[bytes], bytes]]] = [
    ("no-tab", _append_record(b"100644 blob " + b"0" * 40)),
    ("two-metadata-fields", _append_record(b"blob " + b"0" * 40 + b"\tz.txt")),
    ("39-zero-id", _append_record(b"100644 blob " + b"0" * 39 + b"\tz.txt")),
    (
        "non-ascii-metadata",
        _append_record(b"10064\xff blob " + b"0" * 40 + b"\tz.txt"),
    ),
    ("bad-mode", _append_record(b"100664 blob " + b"0" * 40 + b"\tz.txt")),
    ("tree-type", _append_record(b"040000 tree " + b"0" * 40 + b"\tz")),
    (
        "commit-type-mismatch",
        _append_record(b"100644 commit " + b"0" * 40 + b"\tz.txt"),
    ),
    ("second-nul-inserted", _insert_second_nul),
    ("final-nul-removed", _remove_final_nul),
    ("a-py-record-duplicated", _duplicate_a_py_record),
]


@pytest.mark.parametrize(
    "corrupt",
    [corrupt for _case_id, corrupt in _LS_TREE_CANNOT_PARSE_CASES],
    ids=[case_id for case_id, _corrupt in _LS_TREE_CANNOT_PARSE_CASES],
)
def test_commit_check_refuses_ls_tree_output_it_cannot_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corrupt: Callable[[bytes], bytes]
) -> None:
    """M107, M107b, M107c, M114 and M115 [plan :1560, :1567, :1568, :1375]:
    ``subprocess.run`` replaced by a stub that passes each call through
    unchanged except ``ls-tree``'s, whose ``stdout`` it corrupts one way
    per case -- a malformed, mistyped or duplicated record refuses with a
    message naming the parse failure, not a byte difference, and the
    recorder confirms the stub actually altered an ``ls-tree`` call (so no
    case passes because the stub never fired). M107 (the final-piece check
    removed) is killed by the final-NUL-removed case; M107b (the ASCII
    decode's refusal skipped) is killed by the non-ASCII-metadata case;
    M107c (the combined record check's refusal skipped) is killed by the
    other eight cases. M114 (mode and type not checked) is killed by the
    bad-mode, tree-type and commit-type-mismatch cases; M115 (mode and
    type checked against separate lists) by the commit-type-mismatch case.
    """
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    package_dir, second_sha = _make_repo(tmp_path)
    stub, corrupted_calls = _ls_tree_corrupting_stub(corrupt)
    monkeypatch.setattr(daily_shadow.subprocess, "run", stub)
    with pytest.raises(daily_shadow.RefusedError) as excinfo:
        daily_shadow.check_commit(package_dir, commit_sha=second_sha)
    message = str(excinfo.value)
    assert message.startswith("commit check:")
    assert "cannot parse" in message
    assert "differ" not in message
    assert corrupted_calls == [True]

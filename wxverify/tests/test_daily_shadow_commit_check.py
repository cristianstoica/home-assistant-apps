"""§13.3's commit check (``daily_shadow.check_commit``): the fixture
repository, the clean case, a control for the ``subprocess.run`` stub
itself, and mutant M113.

The fixture builds a real git repository at ``tmp_path / "repo"`` holding
a package directory ``wxverify/wxverify/`` (``__init__.py``, ``a.py``,
``sub/b.py``), a top-level ``.gitignore`` naming ``__pycache__/`` and
``data/``, and a tracked ``wxverify/pyproject.toml`` outside the package.
It is committed twice -- first everything, then a change to
``pyproject.toml`` alone -- so both commits hold the same package files.
Every fixture git command runs with an environment built from
``os.environ`` without any name starting ``GIT_``, plus
``GIT_CONFIG_GLOBAL=/dev/null``, ``GIT_CONFIG_NOSYSTEM=1`` and
``GIT_ATTR_NOSYSTEM=1``, and with ``XDG_CONFIG_HOME`` set to an empty
directory the fixture creates outside the repository. Git then reads no
global or system configuration file and no system-wide attributes file,
and it looks for the per-user ignore and attributes files it reads when
no setting names ``core.excludesFile`` or ``core.attributesFile``,
``$XDG_CONFIG_HOME/git/ignore`` and ``$XDG_CONFIG_HOME/git/attributes``
(under ``$HOME/.config/git/`` when ``XDG_CONFIG_HOME`` is unset or
empty), under that empty directory, which holds neither, so those
external default ignore and attributes files cannot make ``git add``
leave out a fixture file or change the bytes it commits.
Removing the inherited ``GIT_*`` names prevents a calling process,
including a hook, from supplying repository, index, object-store or
configuration overrides through those variables. Each test's own
environment is given ``GIT_CEILING_DIRECTORIES``
set to ``tmp_path`` (plan :1373; the repository sits below it, never at
it: git does not look in the ceiling directory itself when it searches
upward). In the cases here git finds ``tmp_path / "repo" / ".git"``
before reaching the ceiling; the ceiling matters for a case whose
package directory is in no repository, where git's upward search would
otherwise continue past ``tmp_path`` (into this worktree's repository
when pytest's base temporary directory sits inside it).

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
- still owed (a later round): every other commit-check case of plan
  :1375-:1380, and mutants M104-M112 and M114-M119.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from wxverify.verification import daily_shadow

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


def _make_repo(tmp_path: Path) -> tuple[Path, str]:
    """Build the fixture repository; return (package_dir, second commit sha)."""
    xdg_config_home = tmp_path / "xdg"
    xdg_config_home.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".gitignore").write_bytes(b"__pycache__/\ndata/\n")
    package_dir = repo / "wxverify" / "wxverify"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_bytes(_INIT_BYTES)
    (package_dir / "a.py").write_bytes(_A_BYTES)
    (package_dir / "sub").mkdir()
    (package_dir / "sub" / "b.py").write_bytes(_B_BYTES)
    pyproject = repo / "wxverify" / "pyproject.toml"
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

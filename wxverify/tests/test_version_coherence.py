"""The three version sources must agree (config.yaml is release canon)."""

import re
import tomllib
from pathlib import Path

import wxverify

ROOT = Path(__file__).resolve().parents[1]


def _config_yaml_version() -> str:
    text = (ROOT / "config.yaml").read_text(encoding="utf-8")
    match = re.search(r'^version:\s*"?([^"\s]+)"?\s*$', text, re.MULTILINE)
    assert match is not None, "config.yaml has no version line"
    return match.group(1)


def _pyproject_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)["project"]["version"]


def _uv_lock_version() -> str:
    with (ROOT / "uv.lock").open("rb") as fh:
        lock = tomllib.load(fh)
    for package in lock["package"]:
        if package["name"] == "wxverify":
            return package["version"]
    raise AssertionError('uv.lock has no [[package]] entry named "wxverify"')


def test_versions_agree() -> None:
    assert wxverify.__version__ == _config_yaml_version() == _pyproject_version()


def test_uv_lock_version_agrees() -> None:
    assert _uv_lock_version() == _pyproject_version()

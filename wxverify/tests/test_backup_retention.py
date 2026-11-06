"""Tests for `backup_exclude` static config content.

The database import, its `.bak` producer and the `.bak` retention sweep
(`sweep_bak_files`, `_looks_like_valid_sqlite`) were removed in the
storage-release plan (5.5). `config.yaml`'s `backup_exclude` key is kept
regardless: a `.bak` left in `/data` by an earlier (pre-upgrade) import
stays on disk, and the add-on no longer touches it, so Home Assistant's
backups must still skip it.
"""

from __future__ import annotations

import re
from pathlib import Path


def test_config_yaml_excludes_bak_files() -> None:
    """`config.yaml` must declare `backup_exclude: ["*.db.bak"]` verbatim.

    Catches: the key being added with a wrong/looser glob (e.g. ``"*.bak"``)
    or silently dropped/typo'd (``backup_excludes``, wrong key name).
    """
    repo = Path(__file__).resolve().parents[1]
    config_text = (repo / "config.yaml").read_text(encoding="utf-8")
    match = re.search(r"^backup_exclude:\n((?:  - .*\n)+)", config_text, re.MULTILINE)
    assert match is not None, "backup_exclude key with a YAML list must be present"
    entries = [line.strip()[2:] for line in match.group(1).splitlines()]
    assert '"*.db.bak"' in entries, (
        f'expected "*.db.bak" in backup_exclude; got {entries}'
    )

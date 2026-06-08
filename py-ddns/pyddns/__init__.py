# pyright: strict
"""py-ddns — a stdlib-only multi-provider dynamic-DNS updater as an HA add-on.

The importable package is the de-hyphenated form of the add-on slug
(``py-ddns`` cannot be a Python module name). Run with ``python3 -m pyddns``.

`__version__` is kept lock-step with ``config.yaml`` ``version:`` (py-syslog let
the two drift 1.4.0 vs 1.3.0 — deliberately not repeated here).
"""

from __future__ import annotations

__version__ = "2.2.0"

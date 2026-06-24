# pyright: strict
"""py-weather — a stdlib-only adaptive Weather.com PWS poller as an HA add-on.

The importable package is the de-hyphenated form of the add-on slug
(``py-weather`` cannot be a Python module name). Run with ``python3 -m pyweather``.

`__version__` is kept lock-step with ``config.yaml`` ``version:`` (the py-ddns
convention; py-syslog let the two drift, deliberately not repeated here). The
exact value is gitops's at first publish; ``0.1.0`` is the initial pre-live-validation release.
"""

from __future__ import annotations

__version__ = "0.3.0"

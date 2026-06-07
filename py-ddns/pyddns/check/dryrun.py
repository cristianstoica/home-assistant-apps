# pyright: strict
"""``--check --dry-run``: print the redacted planned action, touch no network.

Loads + validates the options for the configured provider (an explicit
``--options`` path, else the built-in example payload off-HAOS), then asks the
selected provider to describe its planned action via `plan_provider`. The plan is
secret-free by construction (the providers' ``plan`` methods redact), and the
provider is **not** invoked — `plan_provider` builds the provider and calls only
its no-network ``plan`` method.
"""

from __future__ import annotations

import sys
from pathlib import Path

from .. import config
from ..config import ConfigError
from ..httpclient import UrllibHttpClient
from ..providers import plan_provider
from ..runtime import monotonic


def _resolve_config(options_path: str) -> config.Config | None:
    """Load the options for `options_path`, or the built-in example off-HAOS.

    An explicit path that is missing/invalid still errors (naming the cause). The
    default ``/data/options.json`` being absent (off-HAOS) falls back to the
    built-in azure example so ``--check --dry-run`` runs without a file.
    """
    from .. import fixtures

    try:
        if Path(options_path).exists():
            return config.load(options_path)
        if options_path == config.DEFAULT_OPTIONS_PATH:
            return config.validate(fixtures.example_azure_options())
        return config.load(options_path)  # explicit missing path -> name the error
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return None


def run_dry_run(options_path: str) -> int:
    """Print the redacted planned action for the configured provider; no network.

    Returns 0 on a successful (network-free) plan render, 1 on a config error.
    The detected IP is reported as ``<not detected (dry-run)>`` — ``--dry-run``
    never contacts an IP source either, so the plan describes the action shape,
    not a live value.
    """
    cfg = _resolve_config(options_path)
    if cfg is None:
        return 1
    print(
        f"resolved config: provider={cfg.provider.value} name={cfg.name} "
        f"interval={cfg.interval_seconds}s drift={cfg.drift_reconcile_seconds}s "
        f"log_level={cfg.log_level}",
        file=sys.stderr,
    )
    plan = plan_provider(cfg, UrllibHttpClient(), monotonic, None)
    print("DRY-RUN PLAN (no network, secrets redacted):", file=sys.stderr)
    print(f"  {plan}", file=sys.stderr)
    return 0

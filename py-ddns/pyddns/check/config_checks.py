# pyright: strict
"""Config-surface checks: invalid-options rejection and the name↔zone contract."""

from __future__ import annotations

import json
import logging
import sys
import tempfile
from ipaddress import IPv4Address
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .. import config, fixtures
from ..config import (
    ConfigError,
    derive_record_label,
    warn_azure_ignored,
)
from ..models import Provider
from ..providers import build_provider
from ..runtime import monotonic
from .fakes import FakeHttp, ok_response, with_recording_handler
from .report import report


def _canonical_azure_ignored_message() -> str:
    """The exact 'Azure options ignored' string, captured from the emitter.

    Driving `warn_azure_ignored` through the recording handler (rather than
    hand-copying the literal) keeps the assertion anchored to the single
    production source — a reworded emitter is caught here, not silently passed.
    """
    captured: list[str] = with_recording_handler(
        lambda _h: warn_azure_ignored(logging.getLogger("pyddns"))
    )
    return captured[0]


def check_invalid_options() -> bool:
    """Assert every `INVALID_OPTIONS` payload is rejected, naming the field.

    Two layers, mirroring py-syslog:

    1. **Field validation** — each payload through `config.validate` raises a
       `ConfigError` whose message contains the expected field token (inferred
       provider gates, HTTPS-only contract on both ``url.endpoint`` and
       ``azure.ip_sources``, name↔zone apex/wrong-zone, range/enum checks).
    2. **File loading** — `config.load` rejects malformed JSON, a non-object
       top-level value, and a missing path, each naming the cause.
    """
    checks: list[tuple[str, bool]] = []
    for fixture in fixtures.INVALID_OPTIONS:
        try:
            config.validate(fixture.options)
        except ConfigError as exc:
            passed = fixture.field in str(exc)
            checks.append(
                (f"[{fixture.name}] rejected naming {fixture.field!r}", passed)
            )
            if not passed:
                print(
                    f"  (got {str(exc)!r}, expected to name {fixture.field!r})",
                    file=sys.stderr,
                )
        else:
            checks.append((f"[{fixture.name}] raised ConfigError", False))
    ok = report("INVALID-OPTIONS", "invalid-options", checks)
    return _check_load_negatives() and ok


def _check_load_negatives() -> bool:
    """Assert `config.load` rejects bad files with a cause-naming `ConfigError`."""
    checks: list[tuple[str, bool]] = []

    def _assert_load_error(name: str, content: str, cause: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            opts = Path(tmp) / "options.json"
            opts.write_text(content, encoding="utf-8")
            try:
                config.load(str(opts))
            except ConfigError as exc:
                checks.append((f"load [{name}] names {cause!r}", cause in str(exc)))
            else:
                checks.append((f"load [{name}] raised ConfigError", False))

    _assert_load_error("malformed JSON", "{ not json", "invalid JSON")
    _assert_load_error(
        "top-level array", '["a", "b"]', "top-level value must be an object"
    )
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does-not-exist.json"
        try:
            config.load(str(missing))
        except ConfigError as exc:
            checks.append(
                ("load [missing path] names 'cannot read'", "cannot read" in str(exc))
            )
        else:
            checks.append(("load [missing path] raised ConfigError", False))
    return report("LOAD-NEGATIVES", "load-negative", checks)


def check_name_zone() -> bool:
    """Assert the name↔zone derivation: accepts derive the label, rejects name it.

    Drives `derive_record_label` directly (the contract chokepoint) against the
    `NAME_ZONE_CASES` corpus: a valid sub-record derives the relative label
    (case- and trailing-dot-insensitive); the zone apex, a wrong-zone name, and
    an empty name are each rejected with the expected substring.
    """
    checks: list[tuple[str, bool]] = []
    for case in fixtures.NAME_ZONE_CASES:
        try:
            label = derive_record_label(case.name, case.zone)
        except ConfigError as exc:
            if case.expected_reject is not None:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} rejected ({case.expected_reject!r})",
                        case.expected_reject in str(exc),
                    )
                )
            else:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} should derive {case.expected_label!r}",
                        False,
                    )
                )
                print(f"  (unexpected reject: {exc})", file=sys.stderr)
        else:
            if case.expected_label is not None:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} -> label {case.expected_label!r}",
                        label == case.expected_label,
                    )
                )
            else:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} should reject ({case.expected_reject!r})",
                        False,
                    )
                )
                print(f"  (unexpectedly derived label {label!r})", file=sys.stderr)
    return report("NAME-ZONE", "name-zone", checks)


def _has_azure_ignored_line(messages: list[str], canonical: str) -> bool:
    """True iff any recorded message is the canonical 'Azure options ignored' line."""
    return any(canonical in message for message in messages)


def check_callback_precedence() -> bool:  # noqa: C901 - one cohesive positive surface
    """Positive checks for inferred-provider precedence + the warning seam.

    None of these raise (they are correct-behavior cases), so they live here, NOT
    in `INVALID_OPTIONS` (where a non-raising payload would record a misleading
    FAIL). Covered:

    * **both-filled** (partial Azure group — ``client_id`` only, then ``zone``
      only): URL wins → ``azure_options_ignored is True``, resolved
      ``provider is Provider.URL``, ``config.azure is None`` (ignored group not
      parsed), and the recorded warning matches the shared emitter's string;
    * **single-section** azure-only and URL-only: ``azure_options_ignored is
      False`` and **no** 'Azure options ignored' line recorded (a flag-on-
      azure_selected bug would emit a spurious warning on every normal startup);
    * a **whitespace-only** ``url.endpoint`` does NOT select URL mode when Azure
      credentials are present;
    * a **mixed comma/whitespace** ``azure.ip_sources`` splits correctly;
    * **URL-mode production path**: a URL payload loads with
      ``ip_source_urls == _DEFAULT_IP_SOURCES`` and a real `UrlProvider` (via
      `build_provider`) fires a GET carrying ``myip=<detected>`` when
      ``send_myip=True``;
    * the **dry-run path** surfaces the same warning at the INFO-configured logger
      (not just the DEBUG recorder) on a both-filled payload.
    """
    canonical = _canonical_azure_ignored_message()
    checks: list[tuple[str, bool]] = []

    # --- both-filled, partial azure (client_id only): URL wins -----------------
    both_filled = fixtures.example_url_options(
        azure={"client_id": "00000000-0000-0000-0000-000000000003"}
    )
    selection_both = config.validate(both_filled)
    captured_both = with_recording_handler(
        lambda h: _emit_if_ignored(selection_both, h)
    )
    checks += [
        (
            "both-filled: azure_options_ignored is True",
            selection_both.azure_options_ignored is True,
        ),
        (
            "both-filled: provider inferred as URL",
            selection_both.config.provider is Provider.URL,
        ),
        (
            "both-filled: ignored azure group not parsed (config.azure is None)",
            selection_both.config.azure is None,
        ),
        (
            "both-filled: warning recorded matches the shared emitter string",
            _has_azure_ignored_line(captured_both, canonical),
        ),
    ]

    # --- both-filled, zone-only azure (the camelCase==snake_case field) --------
    both_zone_only = fixtures.example_url_options(azure={"zone": "example.com"})
    selection_zone = config.validate(both_zone_only)
    checks += [
        (
            "both-filled (zone-only azure): URL still wins",
            selection_zone.config.provider is Provider.URL,
        ),
        (
            "both-filled (zone-only azure): azure_options_ignored is True (zone in the set)",
            selection_zone.azure_options_ignored is True,
        ),
    ]

    # --- single-section azure-only: no flag, no spurious warning ---------------
    sel_azure = config.validate(fixtures.example_azure_options())
    azure_msgs = with_recording_handler(lambda h: _emit_if_ignored(sel_azure, h))
    checks += [
        (
            "azure-only: azure_options_ignored is False",
            sel_azure.azure_options_ignored is False,
        ),
        (
            "azure-only: provider inferred as AZURE",
            sel_azure.config.provider is Provider.AZURE,
        ),
        (
            "azure-only: no 'Azure options ignored' line recorded",
            not _has_azure_ignored_line(azure_msgs, canonical),
        ),
    ]

    # --- single-section URL-only: no flag, no spurious warning -----------------
    sel_url = config.validate(fixtures.example_url_options())
    url_msgs = with_recording_handler(lambda h: _emit_if_ignored(sel_url, h))
    checks += [
        (
            "url-only: azure_options_ignored is False",
            sel_url.azure_options_ignored is False,
        ),
        ("url-only: provider inferred as URL", sel_url.config.provider is Provider.URL),
        (
            "url-only: no 'Azure options ignored' line recorded",
            not _has_azure_ignored_line(url_msgs, canonical),
        ),
    ]

    # --- whitespace-only url.endpoint does NOT select URL when azure present ----
    ws_endpoint = fixtures.example_azure_options(url={"endpoint": "  "})
    sel_ws = config.validate(ws_endpoint)
    checks += [
        (
            "whitespace url.endpoint: azure still selected",
            sel_ws.config.provider is Provider.AZURE,
        ),
        (
            "whitespace url.endpoint: azure_options_ignored is False",
            sel_ws.azure_options_ignored is False,
        ),
    ]

    # --- mixed comma/whitespace ip_sources splits + validates ------------------
    mixed = fixtures.example_azure_options(
        azure=fixtures.example_azure_group(
            ip_sources="https://api.ipify.org,  https://icanhazip.com"
        )
    )
    sel_mixed = config.validate(mixed)
    checks.append(
        (
            "azure.ip_sources mixed comma/whitespace splits to both sources",
            sel_mixed.config.ip_source_urls
            == ("https://api.ipify.org", "https://icanhazip.com"),
        )
    )

    # --- URL-mode production path: defaults inherited + myip reaches the GET ----
    checks += _check_url_production_path()

    # --- dry-run path surfaces the warning at the INFO-configured logger --------
    checks.append(_check_both_filled_dry_run(both_filled, canonical))

    # --- insecure_skip_verify: parse + default (§10(a)) ------------------------
    checks += _check_insecure_skip_verify_parse()

    return report("CALLBACK-PRECEDENCE", "precedence", checks)


def _check_insecure_skip_verify_parse() -> list[tuple[str, bool]]:
    """Assert `url.insecure_skip_verify` parses to the right `Config` bool.

    Default url config → ``False``; ``insecure_skip_verify: True`` → ``True``;
    an azure-mode config (no url group) → ``False`` (it never carries the flag).
    """
    default_url = config.validate(fixtures.example_url_options()).config
    flag_url = config.validate(
        fixtures.example_url_options(
            url={
                "endpoint": fixtures.EXAMPLE_URL_ENDPOINT,
                "insecure_skip_verify": True,
            }
        )
    ).config
    azure = config.validate(fixtures.example_azure_options()).config
    return [
        (
            "insecure_skip_verify: defaults False on a url config",
            default_url.url_insecure_skip_verify is False,
        ),
        (
            "insecure_skip_verify: parses True when set on a url config",
            flag_url.url_insecure_skip_verify is True,
        ),
        (
            "insecure_skip_verify: False on an azure config (never carries the flag)",
            azure.url_insecure_skip_verify is False,
        ),
    ]


def _emit_if_ignored(selection: config.ConfigSelection, _handler: object) -> None:
    """Mirror the imperative shell: emit the warning iff the flag is set.

    Used inside `with_recording_handler` so a check can assert what production
    *would* log for a given selection — the recorder reads `pyddns` log output.
    The handler is supplied by `with_recording_handler` but unused (the recorder
    captures the emit by being attached to the logger, not via this arg).
    """
    if selection.azure_options_ignored:
        warn_azure_ignored(logging.getLogger("pyddns"))


def _check_url_production_path() -> list[tuple[str, bool]]:
    """Load a URL payload, build the real `UrlProvider`, assert myip on the GET."""
    detected = IPv4Address(fixtures.EXAMPLE_GLOBAL_IPV4)
    url_send = fixtures.example_url_options(
        url={"endpoint": fixtures.EXAMPLE_URL_ENDPOINT, "send_myip": True}
    )
    selection = config.validate(url_send)
    cfg = selection.config
    checks: list[tuple[str, bool]] = [
        (
            "url-mode inherits the built-in default IP sources",
            cfg.ip_source_urls == config._DEFAULT_IP_SOURCES,  # pyright: ignore[reportPrivateUsage]
        ),
    ]
    http = FakeHttp(ok_response(""))
    provider = build_provider(cfg, http, monotonic)
    provider.apply(detected)
    fired = http.calls[-1][1] if http.calls else ""
    myip = parse_qs(urlsplit(fired).query).get("myip")
    checks.append(
        (
            "real UrlProvider GET carries myip=<detected> through the loader wiring",
            myip == [str(detected)],
        )
    )
    return checks


def _check_both_filled_dry_run(
    both_filled: dict[str, object], canonical: str
) -> tuple[str, bool]:
    """Drive a both-filled payload through the dry-run path at the INFO logger.

    This exercises the **real** dispatch (``run_dry_run`` reached through a
    written options file) under the same INFO logger configuration ``main()``
    applies before dispatching. The capturing handler is pinned at INFO — a
    ``debug``-level emit would be filtered out and silently vanish here, which is
    exactly the regression the warning-level contract in `warn_azure_ignored`
    guards against. A DEBUG-only recorder (like ``with_recording_handler``) would
    not catch that, so this assertion deliberately uses an INFO threshold.
    """
    from .dryrun import run_dry_run

    pyddns_logger = logging.getLogger("pyddns")
    handler = _InfoListHandler()
    prev_level = pyddns_logger.level
    pyddns_logger.addHandler(handler)
    pyddns_logger.setLevel(logging.INFO)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = Path(tmp) / "options.json"
            opts.write_text(json.dumps(both_filled), encoding="utf-8")
            run_dry_run(str(opts))
    finally:
        pyddns_logger.removeHandler(handler)
        pyddns_logger.setLevel(prev_level)
    return (
        "dry-run surfaces the warning at the INFO-configured logger (not DEBUG-only)",
        _has_azure_ignored_line(handler.records, canonical),
    )


class _InfoListHandler(logging.Handler):
    """A handler that records messages only at INFO and above (DEBUG is dropped)."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record.getMessage())

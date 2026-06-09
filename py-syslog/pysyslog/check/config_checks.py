# pyright: strict
"""Config-surface checks: listen-host, size-guard, invalid-options, reject-unknown."""

from __future__ import annotations

import sys

from .. import config, fixtures
from ..config import ConfigError
from ..models import SourceMapping
from ..resolver import Resolver
from ..server import Counters, process_datagram
from .fakes import CaptureWriter, DebugRecordingHandler
from .options import default_check_options, pinned_clock


def check_listen_host() -> bool:
    """Assert a configured ``listen_host`` round-trips into `Config.listen_host`.

    The rejection side (missing / empty / non-string) is asserted by the
    `INVALID_OPTIONS` corpus via `check_invalid_options`; this pins the positive
    contract: a valid bind address supplied in the options payload reaches
    ``Config.listen_host`` unchanged, so the live ``_bind`` binds the configured
    interface rather than a hardcoded address. (``--check`` is offline and never
    binds a real socket, so this asserts the value plumbing, not the bind call.)
    """
    bind_host = "192.0.2.20"
    options = {**default_check_options(), "listen_host": bind_host}
    try:
        cfg = config.validate(options)
    except ConfigError as exc:
        print(
            f"FAIL  listen-host: valid host rejected -> {exc}",
            file=sys.stderr,
        )
        return False
    ok = cfg.listen_host == bind_host
    if ok:
        print(
            f"PASS  listen-host: configured host round-trips ({bind_host})",
            file=sys.stderr,
        )
    else:
        print(
            f"FAIL  listen-host: expected {bind_host!r}, got {cfg.listen_host!r}",
            file=sys.stderr,
        )

    # The shipped production default (config.yaml: ``listen_host: 0.0.0.0``) must
    # stay accepted by `_require_ipv4`. The no-bind-all-in-seed invariant keeps
    # ``0.0.0.0`` out of every fixture seed, so no corpus assertion drives it
    # through `validate`; this is the one place the invariant permits the literal.
    # Without this guard a future IPv4 tightening could silently reject the
    # default while the oracle stayed green.
    bind_all = "0.0.0.0"
    bind_all_options = {**default_check_options(), "listen_host": bind_all}
    try:
        bind_all_cfg = config.validate(bind_all_options)
    except ConfigError as exc:
        ok = False
        print(
            f"FAIL  listen-host: shipped default {bind_all!r} rejected -> {exc}",
            file=sys.stderr,
        )
    else:
        if bind_all_cfg.listen_host == bind_all:
            print(
                f"PASS  listen-host: shipped default round-trips ({bind_all})",
                file=sys.stderr,
            )
        else:
            ok = False
            print(
                f"FAIL  listen-host: expected {bind_all!r}, "
                f"got {bind_all_cfg.listen_host!r}",
                file=sys.stderr,
            )

    print(f"LISTEN-HOST CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok


def check_size_guard_config() -> bool:
    """Assert valid size-guard knobs round-trip into the `Config` unchanged.

    The rejection side (out-of-range percents/MB, and the coherence gate) is
    asserted by the `INVALID_OPTIONS` corpus via `check_invalid_options`; this
    pins the positive contract, mirroring `check_listen_host`: a coherent guard
    config (both percents set, segment rotation enabled) reaches
    ``Config.min_free_percent`` / ``max_log_percent`` / ``max_segment_mb``
    unchanged, so the live Writer is wired with the operator's values.
    """
    options = {
        **default_check_options(),
        "min_free_percent": 10,
        "max_log_percent": 25,
        "max_segment_mb": 64,
    }
    try:
        cfg = config.validate(options)
    except ConfigError as exc:
        print(
            f"FAIL  size-guard-config: valid guard rejected -> {exc}",
            file=sys.stderr,
        )
        print("SIZE-GUARD-CONFIG CHECK FAILED", file=sys.stderr)
        return False
    ok = (cfg.min_free_percent, cfg.max_log_percent, cfg.max_segment_mb) == (
        10,
        25,
        64,
    )
    if ok:
        print(
            "PASS  size-guard-config: coherent guard round-trips "
            "(min_free=10 max_log=25 segment=64MB)",
            file=sys.stderr,
        )
    else:
        print(
            "FAIL  size-guard-config: expected (10, 25, 64), got "
            f"({cfg.min_free_percent}, {cfg.max_log_percent}, {cfg.max_segment_mb})",
            file=sys.stderr,
        )
    print(
        f"SIZE-GUARD-CONFIG CHECK {'PASSED' if ok else 'FAILED'}",
        file=sys.stderr,
    )
    return ok


def check_invalid_options() -> bool:
    """Assert invalid options are rejected with a cause-naming `ConfigError`.

    Two layers:

    1. **Field validation** — each `INVALID_OPTIONS` payload through
       `config.validate` raises a `ConfigError` naming the offending field.
    2. **File-level loading** — `config.load` against a tempfile rejects
       malformed JSON, a non-object top-level value, and a non-existent path,
       each with a `ConfigError` whose message names the cause.
    """
    ok = True
    for fixture in fixtures.INVALID_OPTIONS:
        try:
            config.validate(fixture.options)
        except ConfigError as exc:
            if fixture.field in str(exc):
                print(
                    f"PASS  invalid-options [{fixture.name}] -> {exc}",
                    file=sys.stderr,
                )
            else:
                ok = False
                print(
                    f"FAIL  invalid-options [{fixture.name}]: message "
                    f"{str(exc)!r} does not name field {fixture.field!r}",
                    file=sys.stderr,
                )
        else:
            ok = False
            print(
                f"FAIL  invalid-options [{fixture.name}]: expected ConfigError, "
                "none raised",
                file=sys.stderr,
            )
    return _check_load_negatives() and ok


def _check_load_negatives() -> bool:
    """Assert `config.load` rejects bad files with a cause-naming `ConfigError`.

    Covers the file-level branches `config.validate` (payload-only) can never
    reach: unreadable JSON, a top-level JSON array (not an object), and a
    missing file. Each must raise a `ConfigError` whose message contains the
    expected cause substring.
    """
    import tempfile
    from pathlib import Path

    ok = True

    def _assert_load_error(name: str, content: str, cause: str) -> bool:
        with tempfile.TemporaryDirectory() as tmp:
            opts = Path(tmp) / "options.json"
            opts.write_text(content, encoding="utf-8")
            try:
                config.load(str(opts))
            except ConfigError as exc:
                if cause in str(exc):
                    print(f"PASS  load-negative [{name}] -> {exc}", file=sys.stderr)
                    return True
                print(
                    f"FAIL  load-negative [{name}]: message {str(exc)!r} does "
                    f"not name cause {cause!r}",
                    file=sys.stderr,
                )
                return False
            else:
                print(
                    f"FAIL  load-negative [{name}]: expected ConfigError, none raised",
                    file=sys.stderr,
                )
                return False

    ok = _assert_load_error("malformed JSON", "{ not json", "invalid JSON") and ok
    ok = (
        _assert_load_error(
            "top-level array", '["a", "b"]', "top-level value must be an object"
        )
        and ok
    )

    # Non-existent path: load must name the unreadable cause.
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does-not-exist.json"
        try:
            config.load(str(missing))
        except ConfigError as exc:
            if "cannot read" in str(exc):
                print(f"PASS  load-negative [missing path] -> {exc}", file=sys.stderr)
            else:
                ok = False
                print(
                    f"FAIL  load-negative [missing path]: message {str(exc)!r} "
                    "does not name cause 'cannot read'",
                    file=sys.stderr,
                )
        else:
            ok = False
            print(
                "FAIL  load-negative [missing path]: expected ConfigError, none raised",
                file=sys.stderr,
            )
    return ok


def check_reject_unknown_sources() -> bool:
    """Assert reject_unknown_sources: default-off WRITES an unknown source (still
    warning "-> stamped unknown/<ip>"), on DROPS it and counts rejected_sources
    without echo and with a warn-once "rejected ... enabled" WARNING (NOT a
    "stamped unknown" claim), a CONFIGURED source still writes regardless of the
    flag, and the flag round-trips through config.validate() (explicit True stays
    True; omitted defaults to False)."""
    import io
    import contextlib
    import logging

    sources = {
        entry["ip"]: SourceMapping(
            ip=entry["ip"], site=entry["site"], host=entry["host"]
        )
        for entry in fixtures.CHECK_SOURCES
    }
    unknown_ip = "203.0.113.9"
    unknown_raw = b"<14>Jun  3 12:00:01 otherhost prog: hello from elsewhere"
    configured_raw = fixtures.DATAGRAMS[0].raw  # known SOURCE_IP datagram
    ok = True

    pkg_log = logging.getLogger("pysyslog")
    rec = DebugRecordingHandler()
    prev_level = pkg_log.level
    pkg_log.addHandler(rec)
    pkg_log.setLevel(logging.DEBUG)
    try:
        off_counters = Counters()
        off_writer = CaptureWriter()
        off_echo = io.StringIO()
        rec.messages.clear()
        with contextlib.redirect_stdout(off_echo):
            process_datagram(
                unknown_raw,
                unknown_ip,
                resolver=Resolver(sources),
                writer=off_writer,
                counters=off_counters,
                reject_unknown_sources=False,
                clock=pinned_clock,
            )
        off_msgs = list(rec.messages)
        checks: list[tuple[str, bool]] = [
            ("OFF: unknown source written", off_counters.written == 1),
            ("OFF: rejected_sources stayed 0", off_counters.rejected_sources == 0),
            ("OFF: unknown_source counted", off_counters.unknown_source == 1),
            ("OFF: line echoed to stdout", off_echo.getvalue() != ""),
            (
                "OFF: resolve() still warns '-> stamped unknown/<ip>'",
                any("stamped unknown" in m for m in off_msgs),
            ),
            (
                "OFF: no 'rejected ... enabled' WARNING",
                not any("reject_unknown_sources enabled" in m for m in off_msgs),
            ),
        ]

        on_resolver = Resolver(sources)
        on_counters = Counters()
        on_writer = CaptureWriter()
        on_echo = io.StringIO()
        rec.messages.clear()
        with contextlib.redirect_stdout(on_echo):
            process_datagram(
                unknown_raw,
                unknown_ip,
                resolver=on_resolver,
                writer=on_writer,
                counters=on_counters,
                reject_unknown_sources=True,
                clock=pinned_clock,
            )
        on_msgs_first = list(rec.messages)
        reject_warns_first = [
            m for m in on_msgs_first if "reject_unknown_sources enabled" in m
        ]
        rejected_traces = [m for m in on_msgs_first if "write=rejected" in m]
        with contextlib.redirect_stdout(io.StringIO()):
            process_datagram(
                unknown_raw,
                unknown_ip,
                resolver=on_resolver,
                writer=on_writer,
                counters=on_counters,
                reject_unknown_sources=True,
                clock=pinned_clock,
            )
        reject_warns_total = [
            m for m in rec.messages if "reject_unknown_sources enabled" in m
        ]
        checks += [
            ("ON: unknown source NOT written", on_writer.lines == []),
            (
                "ON: rejected_sources incremented to 1 on first",
                reject_warns_first != [] and on_counters.rejected_sources >= 1,
            ),
            ("ON: unknown_source still counted", on_counters.unknown_source >= 1),
            ("ON: no stdout echo for rejected datagram", on_echo.getvalue() == ""),
            (
                "ON: exactly one 'rejected ... enabled' WARNING on first drop",
                len(reject_warns_first) == 1,
            ),
            (
                "ON: drop did NOT claim 'stamped unknown'",
                not any("stamped unknown" in m for m in on_msgs_first),
            ),
            (
                "ON: one write=rejected trace resolving unknown/<ip>",
                len(rejected_traces) == 1
                and any(f"unknown/{unknown_ip}" in m for m in rejected_traces),
            ),
            (
                "ON: second same-IP drop counted (rejected_sources==2)",
                on_counters.rejected_sources == 2,
            ),
            (
                "ON: warn-once held — no additional WARNING on second drop",
                len(reject_warns_total) == 1,
            ),
        ]
    finally:
        pkg_log.removeHandler(rec)
        pkg_log.setLevel(prev_level)

    cfg_counters = Counters()
    cfg_writer = CaptureWriter()
    with contextlib.redirect_stdout(io.StringIO()):
        process_datagram(
            configured_raw,
            fixtures.SOURCE_IP,
            resolver=Resolver(sources),
            writer=cfg_writer,
            counters=cfg_counters,
            reject_unknown_sources=True,
            clock=pinned_clock,
        )
    checks += [
        ("ON: configured source still written", cfg_counters.written == 1),
        ("ON: configured source not rejected", cfg_counters.rejected_sources == 0),
    ]

    labelled_ip = "198.51.100.7"
    labelled_sources = {
        labelled_ip: SourceMapping(
            ip=labelled_ip, site="unknown", host="labelled-unknown"
        )
    }
    lbl_counters = Counters()
    lbl_writer = CaptureWriter()
    with contextlib.redirect_stdout(io.StringIO()):
        process_datagram(
            unknown_raw,
            labelled_ip,
            resolver=Resolver(labelled_sources),
            writer=lbl_writer,
            counters=lbl_counters,
            reject_unknown_sources=True,
            clock=pinned_clock,
        )
    checks += [
        (
            'ON: configured source labelled "unknown" still written',
            lbl_counters.written == 1,
        ),
        (
            'ON: labelled-"unknown" source not rejected',
            lbl_counters.rejected_sources == 0,
        ),
        (
            'ON: labelled-"unknown" source not counted as miss',
            lbl_counters.unknown_source == 0,
        ),
    ]

    cfg_on = config.validate(
        {**default_check_options(), "reject_unknown_sources": True}
    )
    cfg_default = config.validate(default_check_options())
    checks += [
        ("CONFIG: explicit True round-trips", cfg_on.reject_unknown_sources is True),
        ("CONFIG: default is False", cfg_default.reject_unknown_sources is False),
    ]

    for label, passed in checks:
        print(
            f"{'PASS' if passed else 'FAIL'}  reject-unknown: {label}", file=sys.stderr
        )
        ok = ok and passed
    print(f"REJECT-UNKNOWN CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok

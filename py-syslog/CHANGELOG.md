# Changelog

## 1.3.0 — disk-space guard (size-bounded ring buffer)

- New opt-in disk-space guard turns the on-disk log directory into a
  size-bounded ring buffer that composes with the existing time-based
  `retention_days`. Three options govern it and all default to `0` (disabled),
  so existing deployments are byte-identical until someone opts in —
  backward-compatible:
  - `min_free_percent` (`0..99`): floor on free space on the volume backing
    `/data/log`. Pruning targets the floor, keeping newest segments and
    dropping oldest first.
  - `max_log_percent` (`0..99`): cap on the share of the volume the log
    directory may occupy. Pruning targets the cap with the same
    keep-newest / drop-oldest policy.
  - `max_segment_mb` (`0..4096`): intra-day size rotation threshold. The
    current `syslog.log` is closed and renamed to a sequenced segment
    `syslog.log.<UTC-date>.<NNN>.gz` (zero-padded `NNN`, monotonic per UTC
    day) once it crosses the threshold, then atomically gzip-compressed
    alongside the existing daily-rotation pipeline. Required for either
    percentage guard to have anything to prune within a single day; both
    percentage guards are no-ops when `max_segment_mb: 0`.
- Pruning is two-dimensional (floor + cap evaluated together each cycle) and
  the active `syslog.log` is never a deletion candidate, so a single oversize
  current segment cannot be silently truncated and receiving continues across
  the guard tick.
- Stats line gains two gauges (`disk_free_pct`, `log_dir_mb`) and counters
  for size-rotations performed and segments pruned, so operators can see the
  guard working without enabling `log_level: debug`.
- `translations/en.yaml` labels the three new options on the HA Configuration
  tab and explains the `max_segment_mb > 0` precondition for the percentage
  guards.
- README documents the ring-buffer semantics, the segment naming scheme, and
  the composition rule with `retention_days` (time-based deletion still wins
  when both apply to the same segment).

## 1.2.0 — configurable `listen_host` (closes bind-all CodeQL finding)

- New `listen_host` option selects the local interface/address the collector
  binds. The default is `0.0.0.0` (binds all interfaces), so existing
  deployments are byte-identical until someone overrides it — backward-
  compatible. Set `listen_host` to a specific host IP to accept datagrams on
  only that interface.
- Closes the CodeQL finding `py/bind-socket-all-network-interfaces`
  (CVE-2018-1281) on the previously-hardcoded `0.0.0.0` bind. The bind-all
  string literal no longer exists anywhere on a code path that can reach
  `socket.bind`: `_bind` reads `self._config.listen_host`, the default lives
  exclusively in the HA schema (`config.yaml`), and `validate()` requires
  `listen_host` (rejects missing / non-string / empty / whitespace-only) so
  the dev/`--options` path must supply it explicitly. `--check` and the
  invalid-options fixture base use the RFC 5737 documentation address
  `192.0.2.10` so no Python source carries a bind-all literal.
- README gains a "Threat model and hardening" section: bind-interface
  restriction, UDP syslog being unauthenticated and source-spoofable, the
  restrict-AND-firewall guidance for exposure beyond a trusted LAN, and a
  roadmap caveat that driving HA automations from collected logs would need
  sender-trust first. The `listen_host` row is added to the Options table.
- `translations/en.yaml` adds a `listen_host` entry so the HA Configuration
  tab labels it "Listen address" with help text that names the bind-all
  default and points operators at the restrict-AND-firewall hardening.
- `--check` adds a positive `listen-host` assertion that a configured bind
  address round-trips into `Config.listen_host` unchanged, and the
  invalid-options corpus pins all four rejection arms (missing / empty /
  non-string / whitespace-only).

## 1.1.0 — meaningful `log_level: debug` and config-page translations

- `log_level: debug` is now meaningful: each received datagram emits a single
  consolidated DEBUG trace on stderr surfacing its parse and resolution
  decision (protocol, priority, program, sender_ts, resolved site/host,
  malformed flag, and write outcome). Default stays `info`, so existing
  deployments are byte-identical until someone opts into `debug` —
  backward-compatible.
- The trace renders sender-controlled `program` / `sender_ts` through
  `repr()`, so embedded line breaks and C1 controls are escaped and the
  diagnostics line is guaranteed a single physical line (same one-physical-line
  contract the stored-line path enforces).
- New `translations/en.yaml` gives the four Configuration-tab options friendly
  labels and help text in the HA UI (e.g. `log_level` → "Diagnostics
  verbosity", with help clarifying it does not filter the syslog collected from
  configured sources).
- The `--check` self-tests now assert the DEBUG trace contract directly:
  info-level emits zero records, DEBUG emits one trace per datagram, every
  trace is one physical line and reports `write=written`, and a direct
  `trace_datagram` call with a hostile `program` / `sender_ts` is captured
  as a single line — pinning the `repr()` neutralization the parser cannot
  reach. The `--check --write-error` mode additionally asserts the trace
  fires exactly once and reports `write=error` on the failure branch.

## 1.0.1 — escape C1 controls and Unicode line separators

- `_escape` now also escapes C1 control characters (U+0080–U+009F, including
  NEL U+0085) as `\xNN` and the Unicode line/paragraph separators U+2028 and
  U+2029 as `\uNNNN`. Previously these validly-decoded code points passed
  through and could split a stored log line into multiple physical lines,
  violating the one-datagram → one-physical-line contract (a log-line-injection
  vector).
- Added two regression fixtures: an isolated U+2028 / U+2029 / C1-edge fixture,
  and a combined "all escape classes + legit UTF-8" fixture exercising the
  `\\` self-escape and DEL (`\x7f`) arms alongside multi-byte UTF-8 that must
  pass through verbatim.
- `--check` now asserts that every rendered line is exactly one physical line
  (one trailing newline, none embedded), pinning the contract directly so a
  future expected_line that itself wrongly embedded a newline cannot pass.

## 1.0.0 — initial public release

- Durable UDP syslog collector for Home Assistant (RFC 3164 / 5424).
- Resolves each sender IP to a configurable `(site, host)` stamped on every line.
- Daily UTC rotation with atomic gzip compression and time-bounded retention.
- Counted, throttled-warned `WriteError` handling; receiving continues on
  storage failure.
- Built-in `--check` self-validation (`--check`, `--check --storage`,
  `--check --write-error`).
- Packaged on `ghcr.io/home-assistant/base-python:3.13-alpine3.23` with the
  s6-overlay supervision tree, distributed as a prebuilt multi-arch image
  (`amd64`, `aarch64`) at `ghcr.io/cristianstoica/py-syslog:1.0.0`, built and
  published by the `home-assistant/builder` GitHub Actions composite set
  (`prepare-multi-arch-matrix` → `build-image` → `publish-multi-arch-manifest`).
- Add-on installs from the collection repository
  `https://github.com/cristianstoica/home-assistant-apps` (slug subdirectory
  `py-syslog/`); `config.yaml` declares `image:` so Supervisor pulls the
  prebuilt image instead of building locally.

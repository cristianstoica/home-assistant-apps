# Changelog

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

# Changelog

## 1.0.0 — initial public release

- Durable UDP syslog collector for Home Assistant (RFC 3164 / 5424).
- Resolves each sender IP to a configurable `(site, host)` stamped on every line.
- Daily UTC rotation with atomic gzip compression and time-bounded retention.
- Counted, throttled-warned `WriteError` handling; receiving continues on
  storage failure.
- Built-in `--check` self-validation (`--check`, `--check --storage`,
  `--check --write-error`).

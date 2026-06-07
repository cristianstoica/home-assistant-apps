# Changelog

## 1.0.0 — initial public release

- Generic, stdlib-only **dynamic-DNS updater** for Home Assistant: keeps one
  DNS **A record** pointed at the box's current egress IPv4. IPv4/A only —
  AAAA / IPv6 are out of scope for the initial release.
- Two provider archetypes behind a single `provider` switch:
  - **`azure` (API archetype)** — detects egress IPv4 from an HTTPS echo
    source, then create-or-replaces the A record via the Azure DNS management
    API (`GET` then `PUT`, pinned to the GA `2018-05-01` api-version) with a
    least-privilege **DNS Zone Contributor** service principal scoped to a
    single zone. The SP credential blob's `zone` is authoritative; the
    configured `name` must be a sub-record of it (the zone apex is rejected).
  - **`url` (callback archetype)** — fires a secret cPanel-style callback
    endpoint (HTTPS only) and the remote server reads the request's source IP
    and sets the record. Opt-in `url_send_myip` appends the locally-detected
    IP as `?myip=`; otherwise the server detects it.
- Each cycle reconciles on an interval, applies **bounded interruptible
  backoff** to transient failures (3 attempts, exponential with ±20% jitter,
  honoring HTTP `Retry-After`), and **confirms the change in DNS** before
  considering it done (post-fire resolve, with `test_ns` overriding the local
  resolver path for cycle-time confirmation when the cache TTL is high).
  Inconclusive confirmations hold last-known rather than clearing state; a
  drift cycle (`drift_reconcile_seconds`) re-asserts authoritative state even
  on a steady IP.
- **Secret-safe by construction.** The Azure SP `clientSecret`, bearer tokens,
  and the full callback URL are never logged: diagnostics show the host with
  the secret path redacted, error messages from `urllib`/`URLError` are
  scrubbed before they reach the log, and a `--check` no-secret-leakage suite
  pins every error path against the live secret string. `redact_url` falls
  back to a safe constant on unparseable / hostless inputs so a malformed
  config cannot leak through the redactor.
- Built-in `--check` self-validation oracle covers config schema (invalid
  options, load-negatives, name/zone gating), URL shaping (Azure API URL
  pinned to `management.azure.com` + GA api-version + full-replace body; URL
  callback param merge preserving the secret path), IP parsing (clean /
  trailing newline / non-global rejection), DNS resolver (build_query
  contract, A/NXDOMAIN/transient/timeout/id-mismatch arms, `test_ns` UDP and
  getaddrinfo paths), HTTP status handling (`Retry-After` parsing,
  401-after-fresh vs cached-401 token re-acquire, terminal vs transient
  classification), backoff (capped sleep sequence, jitter band, stop
  mid-backoff, terminal-error propagation), callback-confirm semantics
  (confirmed / unconfirmed / inconclusive / drift re-fire), API reconcile
  (self-heal / steady / change / skip-no-IP / drift), the `run_once` contract
  (swallows unexpected exceptions, holds last-good), startup self-heal, and a
  harness backstop that proves an escaped check exception is folded to FAIL
  rather than propagated.
- Packaged on `ghcr.io/home-assistant/base-python:3.13-alpine3.23` with the
  s6-overlay supervision tree (`startup: services`, outbound-only client),
  distributed as a prebuilt multi-arch image (`amd64`, `aarch64`) at
  `ghcr.io/cristianstoica/py-ddns:1.0.0`, built and published by the
  `home-assistant/builder` GitHub Actions composite set
  (`prepare-multi-arch-matrix` → `build-image` → `publish-multi-arch-manifest`).
- Add-on installs from the collection repository
  `https://github.com/cristianstoica/home-assistant-apps` (slug subdirectory
  `py-ddns/`); `config.yaml` declares `image:` so Supervisor pulls the
  prebuilt image instead of building locally.

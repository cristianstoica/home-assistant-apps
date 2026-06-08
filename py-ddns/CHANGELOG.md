# Changelog

## 2.0.0 — BREAKING: nested `url:` / `azure:` config groups, provider inferred

**Breaking config change.** The flat options `provider`, `azure_token`,
`url_endpoint`, `ip_source_urls`, and `ttl` are **removed**. Configuration is
now split into two nested groups and the provider is inferred from whichever
section is filled — there is no `provider` switch anymore. The add-on's
`config.yaml` carries `breaking_versions: ["2.0.0"]` so Supervisor will warn
before crossing this update even with auto-update enabled.

The new shape:

- **`url:` group** (callback archetype) — fill `endpoint` to select URL mode.
  - `url.endpoint` — the secret HTTPS callback URL (previously `url_endpoint`).
  - `url.send_myip` — append the locally-detected IP as `?myip=` (previously
    `url_send_myip`, behavior unchanged).
- **`azure:` group** (API archetype) — fill the credential fields to select
  Azure mode. The fields that previously lived inside the `azure_token` JSON
  blob are now first-class options on the Configuration tab:
  - `azure.client_id`, `azure.client_secret`, `azure.tenant_id`,
    `azure.subscription_id`, `azure.resource_group`, `azure.zone`
  - `azure.ip_sources` — comma/space-separated HTTPS echo endpoints (blank =
    built-in defaults; previously `ip_source_urls`).
  - `azure.ttl` — record TTL (previously top-level `ttl`).
- **Shared top-level fields** retain their names: `name`, `interval_seconds`,
  `drift_reconcile_seconds`, `test_ns`, `log_level`.

**Provider selection** is now inferred: if `url.endpoint` is set, URL mode
wins; otherwise if any `azure.*` credential field is set, Azure mode is used.
If both sections are filled the URL section wins and the populated `azure:`
fields are ignored with a warning logged.

**How to migrate.** Open the add-on Configuration tab after updating and
re-enter your old values into the new fields:

- Old `url_endpoint` → new `url.endpoint`
- Old `url_send_myip` → new `url.send_myip`
- Old `azure_token` JSON blob — copy each value out of the blob into the
  matching `azure.*` field (`clientId` → `azure.client_id`,
  `clientSecret` → `azure.client_secret`, `tenantId` → `azure.tenant_id`,
  `subscriptionId` → `azure.subscription_id`,
  `resourceGroup` → `azure.resource_group`, `zone` → `azure.zone`).
- Old `ip_source_urls` → new `azure.ip_sources`
- Old `ttl` → new `azure.ttl`

The `provider:` option no longer exists and should not be set — provider is
inferred from whichever section is populated.

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

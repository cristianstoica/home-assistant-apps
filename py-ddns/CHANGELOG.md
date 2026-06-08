# Changelog

## 2.2.0 — opt-in `url.insecure_skip_verify` for unverifiable callback certs

**Additive, non-breaking.** No config-shape change beyond the new optional
field; default off means existing installs behave byte-for-byte as 2.1.1. The
prior `breaking_versions: ["2.0.0"]` stays as-is — there is no new breaking
entry.

- **New optional `url.insecure_skip_verify` (default `false`).** When set on
  the callback (URL) archetype, the callback request is sent over HTTPS with
  TLS **certificate verification disabled** — the channel is still encrypted,
  but endpoint authentication is dropped. The intended (and only sensible)
  use case is a cPanel-style shared-host callback whose served certificate
  cannot be validated against a public CA. HTTPS remains mandatory; an
  `http://` callback is still rejected.
- **Scope is deliberately narrow.** The skip applies **only** to the URL
  provider's callback request. The Azure DNS management API and the
  IP-source echo endpoints **always** verify TLS, regardless of this flag.
- **Loud while enabled.** Every callback cycle (not just at boot) emits a
  `WARNING` line naming the host and stating that certificate verification
  is disabled — the operator cannot lose track that the install is running
  in the weakened mode.
- **`--check` gains a `TLS-SCOPE` section** that asserts the verification
  posture per built client: with the flag off, every client verifies; with
  the flag on, only the URL client skips and azure/ip-source still verify.
  Backed by a new `verifies_tls` predicate on the HTTP client so the oracle
  doesn't reach into private SSL-context state.
- **Tradeoff, explicit.** Disabling cert verification accepts an
  active-MITM risk on the callback path in exchange for tolerating an
  otherwise-unverifiable cert. Leave it off unless you specifically need it.
- **README** gains a "TLS certificate verification (advanced)" section
  documenting the use case, the encrypted-but-unauthenticated tradeoff, the
  callback-only scope, and the per-cycle WARNING.

## 2.1.1 — docs: clarify `az` output keys for Azure credential fields

**Docs-only.** No code, config schema, or runtime behavior change.

- **README** gains a "Where each value comes from" note immediately after the
  Azure DNS configuration mapping table, naming the exact `az`-CLI output key
  for each add-on field — **Client ID** = `appId`, **Client secret** =
  `password` (Microsoft Graph and the Azure portal call the same value
  `secretText`), **Tenant ID** = `tenantId`, **Subscription ID** = the bare
  **`id`** field of `az account show` (there is no `subscriptionId` key).
  Eliminates a common copy-paste foot-gun where the wrong key is grabbed and
  the add-on fails authentication on first cycle.

## 2.1.0 — meaningful `log_level: debug` (secret-safe per-cycle trace)

**Additive, non-breaking.** No config schema change; `error`/`warning`/`info`
behavior is unchanged. Existing installs upgrade in place — there is no new
`breaking_versions` entry (the prior `["2.0.0"]` stays as-is).

- **`log_level: debug` now traces each cycle.** The level previously emitted
  nothing beyond `info`; it now adds a per-cycle trace at the three lifecycle
  points the README promises — **IP detection** (the detected egress IPv4, or
  the deferred-to-server note on the callback path), the **update decision**
  branch (authoritative / steady / suppress / fire), and the **DNS
  confirmation** outcome (the apply action or the post-fire resolve status).
  Covers both archetypes (Azure API and callback URL).
- **Secret-safe by construction.** The trace lines never carry the callback
  endpoint or the Azure `client_secret`; a new `--check` section
  `DEBUG-TRACE` pins this against the live fixture secrets (and proves the
  trace is silenced at the `info` threshold rather than always emitted).
- **README** gains a "Log levels" section documenting what each level
  surfaces (`error` / `warning` / `info` / `debug`).
- **Config UI cleanup.** The verbose provider-inference prose was trimmed
  from the `Host name` field's description in the Configuration tab
  (`translations/en.yaml`) and from the matching `config.yaml` comment; the
  full explanation remains in the README.

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

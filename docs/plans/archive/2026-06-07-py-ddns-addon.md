# Plan: Generic multi-provider dynamic-DNS Home Assistant add-on (`py-ddns`)

## Context

**The need.** Keep a DNS **A** record pointed at a host's current public IPv4, updating
automatically when it changes. Many home/edge connections have a **dynamic** public IPv4
(no static lease, no CGNAT), so any service that needs a stable, resolvable endpoint hostname
behind such a connection needs a dynamic-DNS updater.

**Scope.** A **generic, reusable, multi-provider dynamic-DNS add-on** (`py-ddns`) for the
`home-assistant-apps/` repo (alongside `py-syslog`), with **two providers in v1** behind a
provider **seam** so others are additive:

- `azure` — **Azure DNS** as the first API-archetype provider.
- `url` — a generic **callback-DNS** provider (a "dynamic DNS update URL", as offered by many
  DNS/hosting control panels).

**IPv4/A only** — IPv6/AAAA is deliberately out of scope. An AAAA would publish the host's own
/128 (a different value from the WAN IPv4 most dynamic-endpoint use cases care about) and would
need its own provider scope; it is a clean additive feature if ever wanted.

**Chosen approach.** A single add-on on a Home Assistant host. On a timer it determines the
public IPv4 and asserts the target A record through the configured provider. **Everything is
add-on configuration entered after install** — provider, hostname, credentials, intervals —
nothing is pre-seeded; the same image serves any host/zone/provider.

## Architecture

Two provider **archetypes** behind one seam (`DnsProvider`), selected by a `provider:` option:

```
                         ┌─ provider = azure (API archetype) ───────────────────────────┐
HA host                  │  client discovers its own egress IPv4, then writes via ARM    │
  py-ddns add-on         │  ──GET──▶ <ip-source>             (egress IPv4 = WAN; no CGNAT)│
   │                     │  ──POST─▶ login.microsoftonline.com/{tenant}/oauth2/v2.0/token│
   │  on a timer:        │            (client_credentials; scope mgmt.azure.com/.default) │
   │   1. determine IP   │  ──PUT──▶ management.azure.com/.../dnszones/{zone}/A/{record}  │
   │   2. assert record  │            ?api-version=2018-05-01  (create-or-replace)        │
   │   3. resolve+log    └──────────────────────────────────────────────────────────────┘
   │                     ┌─ provider = url (callback archetype) ─────────────────────────┐
   │                     │  SERVER determines the IP; client just fires a secret URL      │
   │                     │  ──GET──▶ https://<your-provider>/<secret>  [?myip=<ip>]       │
   │                     │            (server reads request source IP, sets the A record)  │
   │                     └──────────────────────────────────────────────────────────────┘
   └─ status readout (both): resolve `name` via `test_ns` (stdlib UDP query) else getaddrinfo → log vs IP
```

- **API archetype (Azure):** the _client_ does the work — discover egress IP (`ipsource`),
  authenticate, write the record. Owns TTL, idempotency, authoritative drift-reconcile.
- **Callback archetype (URL):** the _server_ does the work — the host just `GET`s a secret URL
  and the callback provider reads the request source IP. No IP discovery, no auth blob, no
  zone/record needed client-side (the URL encodes record + secret). More robust in one way: no
  dependency on a "what's my IP" service.
- **Runtime API version (Azure):** the add-on pins the **GA** `2018-05-01` record-set surface,
  not a `*-preview` version. A long-lived unattended client must not depend on a preview version
  Azure can retire; GA `2018-05-01` fully supports the A record-set GET + create-or-replace.
- **Stdlib only:** token, IP fetch, record GET/PUT, URL fire, and DNS-resolve are plain HTTPS /
  `socket` via `urllib.request` + `json` + `ipaddress` + `socket`; no third-party deps — mirrors
  `py-syslog`.
- **Idempotent:** persist last-known IP under `/data`; act only on change. Azure PUT is a full
  record-set replace (self-healing); the URL fire is naturally idempotent server-side.

## Provider model

- **Seam (covers both archetypes explicitly):** `DnsProvider` exposes `read_current() ->
IPv4Address | None` (the provider's authoritative current value) and
  `apply(detected_ip: IPv4Address | None) -> ApplyResult`. The **API archetype** (`azure`)
  implements `read_current()` via a management GET and **requires** a known `detected_ip` to
  `apply()` (a `None` IP is a no-op — nothing valid to write). The **callback archetype** (`url`)
  returns `None` from `read_current()` (the server owns the value — drift is judged by
  DNS-resolving `name` instead) and **fires regardless** in `apply()`, even when `detected_ip`
  is `None` (server-side detection is the whole point). `ApplyResult` distinguishes
  _wrote-known-IP_ / _fired-server-detected_ / _skipped-no-IP_ / _failed_, so the updater and
  logs never claim a "match" they can't substantiate. **Confirmation/steady-state seam:** for the
  callback archetype the updater confirms a fire by DNS-resolving `name` afterwards and persisting
  the **resolved value** as last-known — _not_ a client `detected_ip`, which in default
  server-side-detection mode is `None`. The resolved value is what makes default `url` mode reach
  steady state (a server-detected fire still has a confirmable post-fire value), so confirmation
  never depends on a client IP being known. A third provider is additive (drop in
  `providers/<name>.py`, pick an archetype).
- **`azure`** (API archetype) — SP **client-credentials** (app-only). `name` (FQDN) drives
  `{zone}`/`{record}`; `azure_token` is a pasted SP credential blob. Custom **A-only** RBAC role
  at **zone scope**.
- **`url`** (callback archetype) — `url_endpoint` (the secret callback URL); fires **independent
  of the IP source** — if `url_send_myip` and a detected IP exists it appends `?myip=<detected-ip>`,
  otherwise it relies on server-side detection (so an ipsource outage never blocks the update).
  `name` is kept (shared) for the DNS-resolve status/verification readout, even though it does not
  drive the update.

## Files to create — `home-assistant-apps/py-ddns/`

Mirror `py-syslog/` (dir slug `py-ddns`, Python package `pyddns`, image
`ghcr.io/cristianstoica/py-ddns`):

| File                                           | Mirrors                 | Notes                                                                                                                                                                                               |
| ---------------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `config.yaml`                                  | `py-syslog/config.yaml` | manifest + `options:`/`schema:` (below). `image: ghcr.io/cristianstoica/py-ddns`, `arch: [aarch64, amd64]`, `startup: application`, `boot: auto`, `init: false`. No `host_network` (outbound only). |
| `Dockerfile`                                   | `py-syslog/Dockerfile`  | `BUILD_FROM=ghcr.io/home-assistant/base-python:3.13-alpine3.23`, `PYTHONUNBUFFERED=1`, `PYTHONPATH=/app`, `COPY rootfs /`, `COPY pyddns/ /app/pyddns/`, OCI labels.                                 |
| `rootfs/etc/services.d/py-ddns/run` + `finish` | py-syslog s6 scripts    | `exec python3 -m pyddns`; identical finish/exit-code handling.                                                                                                                                      |
| `translations/en.yaml`                         | py-syslog               | one entry per option key.                                                                                                                                                                           |
| `README.md`, `CHANGELOG.md`, `icon.png`        | py-syslog               | docs incl. per-provider setup (Azure SP bootstrap + security; callback URL); CHANGELOG starts `1.0.0`.                                                                                              |
| `pyddns/` package                              | `pysyslog/` package     | see module layout below.                                                                                                                                                                            |

**Python package `pyddns/` (`# pyright: strict`, ruff clean, stdlib only):**

- `__init__.py` — `__version__` (lock-step with `config.yaml` `version:`; keep them in sync — do
  **not** let the package version drift from the manifest version).
- `__main__.py` — argparse: the **live updater loop** (default), or the offline oracle **`--check`**
  (with sub-flags per mode) and **`--check --dry-run [--options PATH]`**, which prints the redacted
  planned action for the configured provider (defaults to `/data/options.json`) and exits without
  touching the network.
- `config.py` — read+validate `/data/options.json` → typed `Config`; **per-provider** required-
  field validation; `ConfigError` names the offending field (py-syslog `config.py:load` pattern).
  **Azure name↔zone contract:** the `azure_token` blob's `zone` is authoritative; normalize case
  and trailing dots, **require a strict sub-record** (`name.endswith("." + zone)` **and**
  `name != zone`), derive the relative record label by stripping the zone suffix, and reject
  wrong-zone / empty / malformed labels — so `name` and the token can never silently disagree.
  **Reject the zone apex** (`name == zone`, relative label `@`): a host-record DDNS updater must
  never repoint a zone apex. On a shared zone the apex `A` is typically the zone's primary website
  record — a typo (`name: example.com`) or wrong config would otherwise aim the host's dynamic IP
  at the whole domain. The intended targets are **sub-records** (e.g. `home.example.com`), so apex
  support is **not** needed; refusing it costs nothing and removes the footgun. (If a future use
  case genuinely needs apex DDNS, the right answer is a _dedicated_ zone delegated for it — see the
  child-zone note in Security — not relaxing this guard on a shared zone.) **HTTPS-only URL
  contract:** `url_endpoint` and **every** `ip_source_urls` entry must be an absolute `https://`
  URL with a host and no userinfo or fragment (validated via `urllib.parse.urlsplit`); reject
  `http:` / `file:` / schemeless / userinfo (`user:pass@`) / fragment forms with a `ConfigError`
  naming the field. A plaintext callback would leak the record-repointing secret in transit; a
  plaintext/spoofable `ip_source` could make the add-on publish an attacker-chosen A record. HTTPS
  is the default and only contract in v1 (no insecure opt-in).
- `models.py` — `Config`, provider-config + result types.
- `ipsource.py` — fetch egress **IPv4** via a configurable primary+fallback provider list; **strip
  surrounding whitespace** before parsing (some echo services return a trailing newline) but
  **reject internal whitespace / multiple tokens**, then parse with `ipaddress.IPv4Address`;
  **reject non-global-unicast** (RFC1918, CGNAT `100.64/10`, loopback, link-local, `0.0.0.0`). On
  rejection/all-fail, return nothing → **hold last-good** and log loudly. (For `url`, ipsource is
  an optional _change-trigger_; if it fails, still fire the URL since the server detects the real IP.)
- `resolver.py` — resolve `name` for the status/verification log line and the `url` drift signal,
  behind an injectable seam (so the `--check` oracle can fake it). **Honors the `test_ns` option:**
  if set, send a small **stdlib UDP DNS A-query** straight at that nameserver
  (`socket` + `struct`; RD=1 so it works against both a recursive resolver _and_ the zone's
  authoritative NS; if `test_ns` is a hostname it's resolved via `getaddrinfo` first, then queried
  by IP). Querying the zone's **authoritative NS** yields the authoritative, cache-free view;
  querying a **public recursive resolver** (e.g. `8.8.8.8`) yields _that_ resolver's view — it
  bypasses the host's local/system resolver but may still be cached by the chosen resolver. If
  `test_ns` is blank, fall back to `socket.getaddrinfo` (the system resolver's recursive/cached A
  view). **The seam distinguishes three outcomes, never collapsing them to one `None`:** _resolved =
  X_ (a concrete value), _resolved = no such record_ (authoritative NXDOMAIN / empty answer), and
  _query failed/transient_ (timeout, NS unreachable, UDP loss, the ~5s musl bound tripping). A
  transient query failure must be reported distinctly from "the record holds value X / no record",
  because the callback confirmation gate (updater) reasons on it: a transient resolve failure must
  **not** be misclassified as "stale" and must not mark a correct record unconfirmed. Each outcome is
  logged, never fatal.
- `providers/` — `__init__.py` (registry/selector), `azure.py` (token: lazy/cached
  client-credentials, refreshed ~5 min pre-expiry / on 401; GET+PUT via `urllib` behind an
  injectable HTTP seam; parses the SP credential blob), `url.py` (fire the secret endpoint;
  when `url_send_myip` adds `myip`, compose the query via `urllib.parse` —
  `urlsplit` → `parse_qsl(keep_blank_values=True)` → set/replace `myip` → `urlencode` → `urlunsplit`
  — so an endpoint that **already carries a query string** keeps its existing params and the secret
  path is never mangled by naive `?`/`&` concatenation). Both behind the `DnsProvider` protocol in
  `models.py`. **Secret-safe by construction:**
  never log `clientSecret`, bearer tokens, or the full `url_endpoint`; log only method + host +
  record label, and **sanitize exception strings** so a `urllib` error can't surface a secret URL.
- `updater.py` — archetype-aware reconcile loop `(config, *, ip_source, provider, resolver, state,
clock, stop)`: the **first cycle on start is authoritative** — read the provider's real current
  value (API: `read_current()`; callback: DNS-resolve `name`) and act if it is missing/stale,
  **never trusting local state to skip a startup self-heal**. Thereafter every `interval_seconds` →
  determine egress IP, then decide whether to act, then `provider.apply(detected_ip)`. The **API
  archetype** skips when no valid IP and writes only on change vs last-good/`read_current()`. The
  **callback archetype fires only when it has reason to** — first cycle, drift cycle, a detected-IP
  change, or a previously-unconfirmed fire — but **suppresses a fire while `name` already resolves to
  the persisted last-known value** (this is what makes the default server-side-detection mode reach
  steady state; see confirmation below). `myip` is appended only when a detected IP is known. Every
  `drift_reconcile_seconds` re-assert authoritatively (`0` = off). **Last-known-IP is persisted only
  on _confirmed_ success, and both what is persisted and how it is confirmed differ by archetype:**
  - **API archetype** — a `2xx` PUT _is_ the record (authoritative replace) → persist the
    **detected IP** on success + known IP.
  - **Callback archetype** — an HTTP `2xx` proves only that the URL fired, not that DNS changed, so
    the fire is confirmed by a **post-fire resolver check** (using `test_ns` when set for a cache-free
    read, else `getaddrinfo`) and the **resolved value** is what gets persisted as last-known — _not_
    a client `detected_ip`, which in default mode is `None`. Confirmation gates persistence on the
    resolver's three-way outcome:
    - _resolved = a concrete global value_ → **confirmed**: persist that resolved value as last-known.
      A subsequent cycle that resolves the same value suppresses the fire — so the default
      server-detected mode reaches steady state and **stops firing while the IP is unchanged**, with
      no dependence on a client-detected IP.
    - _resolved = stale/no-record_ (the value did not move) → **unconfirmed**: do **not** persist,
      keep firing next `interval_seconds`, log a distinct _unconfirmed — fired, DNS not yet updated_
      diagnostic, so a callback that 2xx's without moving the record can never silently suppress
      retries (even with `drift_reconcile_seconds = 0`).
    - _resolve failed/transient_ (timeout, NS unreachable) → **inconclusive, not stale**: retry the
      post-fire resolve within the per-cycle budget; if still inconclusive, **hold last-known** (do
      not clear it) and log a distinct _post-fire confirmation inconclusive_ line — so a transient
      resolver hiccup never marks a correct record unconfirmed nor forces a needless refire.

  This keeps a crash mid-update or a 2xx-without-effect from desyncing local state. Sleeps
  are **interruptible** via the injected `stop` signal (SIGTERM/SIGINT abort immediately).

- `state.py` — persist last-known IP under `/data`.
- `check/` — self-validation oracle with fakes for the HTTP/DNS seams, mirroring `pysyslog/check/`:
  per-provider config rejection; **Azure name↔zone normalization** — `home.example.com` sub-record
  and multi-label sub-records → accept (correct relative label); trailing-dot normalization; **apex
  (`name == zone`) → reject**, plus wrong-zone / empty / malformed label → reject; Azure
  URL/body/token shaping; **URL endpoint shaping incl. an endpoint that already carries a query
  string** (`myip` merged via `urllib.parse`, existing params preserved, secret path intact) with
  `url_send_myip` true and false; **HTTPS-only URL rejection** — `http:` / `file:` / schemeless /
  userinfo / fragment `url_endpoint` **and** `ip_source_urls` entries each rejected by `config.py`
  with the field named; **IP parse/guard incl. newline-padded valid IPv4, multi-token/malformed,
  non-global**; resolver `test_ns` UDP-query packet shaping + answer parse + blank-`test_ns`
  fallback, and the seam's **three-way outcome** (resolved value / no-record / transient-failure)
  reported distinctly, never collapsed to one `None`; **URL callback fires on ipsource failure**,
  persisting last-known only after post-fire confirmation. The callback confirmation cases **assert
  on the state seam (last-known), not the diagnostic log line**, so a broken impl that persists on
  the `2xx` itself fails the case:
  - **URL `2xx` + post-fire resolver returns the fired value** → confirmed: assert last-known **is**
    persisted to the resolved value, and the **next cycle suppresses the fire** (drives the
    default-server-detection steady state — no `detected_ip` needed; proves the "no churn while
    unchanged" guarantee for the shipped default).
  - **URL `2xx` + post-fire resolver still old/missing** → unconfirmed: assert last-known is **not**
    persisted, next cycle **refires**, and the unconfirmed diagnostic is logged.
  - **URL `2xx` + post-fire resolver FAILS (transient)** → inconclusive: assert last-known is **held
    unchanged** (not cleared, not advanced), the post-fire resolve is retried within budget, and a
    correct prior record is **not** marked unconfirmed nor refired needlessly.

  **per-request status handling** — Azure record `GET` `404` → `None`/create path;
  token-endpoint auth-fail → terminal; cached-token management `401` → re-acquire once then terminal;
  URL `4xx`≠`429` → terminal; `429`/`5xx`/network/**socket timeout _and_ bounded `getaddrinfo`
  resolution failure on each operation** → transient → bounded, interruptible backoff driven by a
  **fully synchronous injected clock/stop (never a real `Timer` or thread)**. Because production
  already holds a `threading.Event`, a bare `active_count()` snapshot cannot isolate a leaked backoff
  thread; the backoff case instead asserts the **behavioral contract**: the clock/stop seam is driven
  exactly N times with the **bounded delay sequence** (`2→4→8` capped at `30`, ±20% jitter,
  `Retry-After` capped at `60`), `threading.active_count()` is **unchanged across the backoff**
  (no new thread spawned), and `stop.set()` mid-backoff **returns control before the next attempt**
  (no further attempt or sleep runs). **startup self-heal** (stale local state + missing/wrong
  external record forces an assert); and a **no-secret-leakage** assertion proving `clientSecret` /
  bearer tokens / full `url_endpoint` never appear in stdout/stderr/logged errors (incl. dry-run
  output and sanitized exceptions).

**`config.yaml` options/schema** (per-provider fields validated in `config.py`):

```yaml
options:
  provider: azure # azure | url
  name: "" # FQDN, e.g. home.example.com  (azure: drives zone+record; url: status/verification)
  test_ns: "" # nameserver (IP or host) to query for the verification readout; blank = system resolver
  azure_token: "" # SP credential blob (tenantId/subscriptionId/resourceGroup/zone/clientId/clientSecret)
  url_endpoint: "" # secret callback URL (https only), e.g. https://<your-provider>/<secret>
  url_send_myip: false # url only: merge ?myip=<detected-ip> instead of server-side detection
  ttl: 60 # azure only (the callback server owns TTL for url)
  interval_seconds: 120
  drift_reconcile_seconds: 3600 # authoritative re-assert cadence; 0 = disabled
  ip_source_urls: # IPv4 egress-echo providers (https only), tried in order
    - "https://api.ipify.org"
    - "https://icanhazip.com"
  log_level: info
schema:
  provider: list(azure|url)
  name: str
  test_ns: str?
  azure_token: password? # masked; required when provider=azure (config.py enforces)
  url_endpoint: password? # masked; required when provider=url
  url_send_myip: bool?
  ttl: int(30,86400)?
  interval_seconds: int(60,86400)
  drift_reconcile_seconds: int(0,86400)
  ip_source_urls:
    - str
  log_level: list(debug|info|warning|error)
```

**Single-target (v1).** HA add-ons are singletons (one instance per host), so the schema is
flat — one provider/name per host. A multi-target `targets:` list (partial-failure semantics,
per-target creds) is a clean additive future enhancement, deliberately deferred (YAGNI — one
record per add-on instance in v1).

**CI (gitops owns; two real changes, not auto-magic):**

- `lint.yaml` — the `find` + `frenck` add-on-linter jobs auto-discover `py-ddns`, but the Python
  gates are a **hardcoded job** (`pysyslog-gates`, `lint.yaml:47-78`). Add a parallel
  **`pyddns-gates`** job mirroring it: `PYTHONPATH=py-ddns python -m pyddns --check` (+ each
  `--check` mode), `pyright py-ddns/pyddns`, `ruff check`/`ruff format --check py-ddns`.
- `builder.yaml` — the change-filter `MONITORED_FILES` (`builder.yaml:4`) is
  `config.json config.yaml config.yml Dockerfile rootfs`. A change touching only the
  `py-ddns/pyddns/` Python sources would therefore lint clean but never trigger a rebuild/publish
  (a latent gap for `py-syslog` too). Fix: rebuild an app when any file under its directory changes,
  or add the package source directory to the monitored set. `build-app.yaml` (the reusable
  multi-arch → ghcr build) needs no change.

## Azure provider setup (first provider)

The `azure` provider authenticates as a **service principal (SP)** using **client-credentials**
(app-only). The operator bootstraps, out of band, a least-privilege SP plus a custom **A-only**
role **scoped to one DNS zone** (see the per-provider setup docs in the add-on `README.md`):

- An **app registration** with a **client secret** (rotatable remotely via the add-on options,
  so no on-site visit is needed for rotation).
- A **custom RBAC role** that can read/write **A record sets in one zone only** — tighter than the
  built-in `DNS Zone Contributor` (which grants CRUD on every record type). The custom role's
  `Actions` are just `Microsoft.Network/dnszones/read`,
  `Microsoft.Network/dnszones/A/read`, `Microsoft.Network/dnszones/A/write`. The `A/write` action
  covers create-or-replace, so the updater needs no `A/delete`; add that action later only if a
  delete path is ever built.
- The role **assigned to the SP at the DNS zone resource scope** — this is what narrows effective
  access to the one zone (Azure custom-role `AssignableScopes` may only be a management-group /
  subscription / resource-group scope, so least privilege is enforced by the _assignment_ scope,
  not by `AssignableScopes`).
- **No placeholder record is pre-created** — the add-on creates the record itself on first run
  (zone-scoped RBAC makes this possible; the record name comes from config, not bootstrap).

The operator then assembles the **`azure_token` blob** (`tenantId`, `subscriptionId`,
`resourceGroup`, `zone`, `clientId`, `clientSecret`) and enters it in the add-on options (never
committed to the repo). Note that runtime auth uses the application (client) ID, whereas the
role assignment is keyed on the SP **object ID** — different values for the same app.

**Out-of-band record + Incremental-mode safety.** When the zone is otherwise managed as
infrastructure-as-code (e.g. Azure Bicep), the dynamically-managed A record is created
imperatively by the add-on and is deliberately **not** declared in the IaC. This is safe under
**Incremental** deployment mode (it never deletes undeclared resources). Document the out-of-band
record so a future IaC author does not add a conflicting static record. Because the add-on
**rejects apex targets in config**, it never writes the zone apex — only its configured sub-record.

## Security notes

- **Azure least privilege:** custom A-only role assigned at **one zone** scope (tightest RBAC
  granularity is record-type-within-zone). Two distinct apex-clobber vectors, handled separately:
  - **Typo / bad-config vector — _closed_:** `config.py` **rejects** any apex target
    (`name == zone`), so the add-on cannot be configured (by typo or otherwise) to repoint a zone
    apex `A`. Apex/other records in the zone stay owned by whoever manages the zone.
  - **Compromised-secret vector — _documented residual_:** the SP still holds zone-scoped
    `A/write`, so a _leaked `clientSecret`_ could rewrite any `A` in the zone, including the apex.
    This is the one real residual (mitigated by secret hygiene + loud auth-fail logging). It is
    **eliminable**, not unavoidable: delegate a **dedicated child zone** (the DDNS sub-record as its
    own Azure DNS zone, NS-delegated from the parent) and scope the SP to the child zone — then the
    SP has _zero_ permission on the parent apex even with a leaked secret. Deferred for v1 (the
    sub-record + apex-reject guard keeps the typo vector closed and avoids restructuring an
    IaC-owned parent zone); reconsider the child-zone upgrade on its merits if the residual is
    judged unacceptable for a given deployment (e.g. when the apex is a live production website).
- **Secret hygiene:** use the longest practical client-secret lifetime + a rotation reminder; the
  add-on logs auth failures **loudly/distinctly** (auth-fail ≠ transient) and alerts on the
  `AADSTS7000222` expired-secret signal so a lapsed secret is visible, not silent. (A client
  **certificate** is a more-robust deferred variant — multi-year, no shared secret.)
- **URL endpoint is a secret:** anyone holding `url_endpoint` can repoint the record — stored
  `password`-typed, in `/data/options.json` only; public image + repo stay credential-free.
- **HTTPS-only transport (enforced in `config.py`):** `url_endpoint` and every `ip_source_urls`
  entry must be `https://` (no `http:`/`file:`/userinfo/fragment) — a plaintext callback would leak
  the record-repointing secret in transit, and a plaintext/spoofable `ip_source` could make the
  add-on publish an attacker-chosen A record. No insecure opt-in in v1.
- **No secret ever reaches a log:** `clientSecret`, OAuth bearer tokens, and the full
  `url_endpoint` are never printed — anywhere. Logs and dry-run output carry only method + host +
  record label + redacted bodies; `urllib`/exception strings are sanitized before logging (a raw
  `HTTPError`/`URLError` can echo the requested URL, which for `url` _is_ the secret). The `--check`
  oracle asserts no secret appears in stdout/stderr/logged errors.
- **Egress-IP correctness:** the published value is the host's egress IP (echoed by the ip-source
  service, or seen by the callback server), equal to the WAN IP only while the host's default route
  stays on the upstream link. The global-unicast guard is the backstop against publishing a
  private/CGNAT/link-local value.

## Failure handling & observability

- **Token (azure):** lazy acquire (only when calling Azure), cache until ~5 min pre-expiry.
- **Status handling (per request type)** — each call classifies its response into _terminal_,
  _retry-once_, _missing_, or _transient_:
  - **Azure token endpoint** (`login.microsoftonline.com`): any auth failure (`invalid_client`,
    `AADSTS7000222` expired secret, `400`/`401`) is **terminal** — no retry, loud persistent
    auth-failure log, hold last-good. A bad secret never self-heals by retrying.
  - **Azure management `GET`/`PUT`** `401` **with a cached token**: treat the token as stale —
    invalidate it, re-acquire **once**, retry the request once. A `401` after a _freshly minted_
    token is **terminal** (real authz problem; same loud path as above). `403` is always terminal
    (role not assigned / wrong scope).
  - **Azure record-set `GET` `404`:** **missing record, not a failure** → `read_current()` returns
    `None`, which drives the first-run create path (PUT). Never logged as an error.
  - **URL provider `4xx` (except `429`):** **terminal/config-loud** (bad/disabled callback URL or
    wrong secret won't fix itself) — hold, don't spin.
  - **`429` / `5xx` / network / timeout (any request):** **transient** → bounded retry (below).
- **Retry bounds (transient only):** at most **3 attempts per cycle**, exponential delay
  `2s → 4s → 8s` **capped at 30s**, ±20% jitter; `Retry-After` honored but **capped at 60s**. Every
  sleep runs through the injected `clock`/`stop` so it is **interruptible** — shutdown never blocks
  on a backoff. On exhaustion, give up this cycle and wait for the next `interval_seconds`. Each
  branch above gets a dedicated fake-`check` case.
- **Per-operation timeouts (no unbounded blocking):** every network call sets an explicit timeout so
  a hung socket becomes a _transient failure_ (feeding the retry bounds above), never a stuck loop
  or an un-interruptible shutdown. Concrete v1 values, all overridable only in code (not config):
  IP-source `GET` **5s**; Azure token `POST` **10s**; Azure record `GET`/`PUT` **10s**; URL fire
  `GET` **10s**; DNS UDP query **3s** (per nameserver, with one retry inside the 3-attempt budget).
  `urllib` calls pass `timeout=`; the raw DNS UDP socket uses `settimeout()`.
- **Name-resolution contract (honest about the `getaddrinfo` gap):** a `urllib` `timeout=` bounds
  connect/read **but not the underlying `getaddrinfo`**, and `socket.getaddrinfo` takes no timeout
  argument. Rather than hand-roll a pinned-IP HTTPS connection (which would break TLS SNI /
  certificate-hostname validation) or a thread-per-resolve seam (which leaks a thread per stall),
  v1 **relies on the base image's resolver being internally bounded** and documents the residual:
  the add-on runs on **Alpine/musl** (`base-python:3.13-alpine3.23`), and musl's resolver queries
  all nameservers **in parallel** with an **absolute total cap** of the `resolv.conf` `timeout`
  (default **5s**) — `res_msend.c`: `timeout = 1000*conf->timeout; for (; t2-t0 < timeout; …)`. So a
  dead/stalled resolver makes `getaddrinfo` **fail in ≤~5s** (→ transient, fed to the retry bounds),
  **never an indefinite hang** — no daemon threads, no thread-leak, no custom TLS seam. The one
  acknowledged residual: this resolution phase is bounded by the **OS resolver (~5s)**, _separately
  from_ and _before_ our per-op `timeout=`, so a cycle's worst case adds that ~5s ahead of the
  connect timeout. SIGTERM/SIGINT still completes promptly: the loop starts no new attempt once
  `stop` is set, an in-flight resolve/connect runs at most to its bound, and s6 SIGKILL backstops a
  pathological case. _(An IP-literal `test_ns` skips resolution entirely — the strictly-`settimeout`-
  bound path; a hostname `test_ns` inherits the ~5s musl bound.)_

  **Verifying the bound (it lives in the base image, not in our code — the offline oracle can't see
  it).** The ~5s cap is a property of musl's `resolv.conf` defaults, so the injected-fake `--check`
  never exercises it and a base-image bump (e.g. `base-python` moving to glibc, or a musl
  `resolv.conf` with a raised `timeout`/`attempts`) could silently remove it. So the assumption is
  gated, not assumed: (a) a **runtime smoke on the real Alpine image** — point a hostname `test_ns`
  at an unroutable nameserver (e.g. `192.0.2.1`, RFC 5737) and assert the resolve **fails transient
  in ≤~6s**, never hangs; runnable by the operator at install and addable to CI against the built
  image. (b) This smoke is a **mandatory re-run check on every `BUILD_FROM` bump** (whenever the
  `Dockerfile` base image changes), since the bound is a base-image guarantee — a bump that changes
  the resolver invalidates it. Document the assumption + this gate in `README.md` so a future base
  bump re-asserts the bound rather than inheriting an unverified one.

- A timeout (socket connect/read **or** the bounded `getaddrinfo` failing) raises → caught →
  classified transient → bounded, interruptible retry; a `stop` signal during any timeout/backoff
  aborts before the next attempt. Backoff runs through the injected synchronous `clock`/`stop`
  **only — never a `threading.Timer`, `concurrent.futures` pool, or thread-per-resolve guard**
  (these are the tempting-but-banned ways to bound `getaddrinfo`/sleep, each of which leaks a thread
  on the long-lived host). Covered by `check` cases (via the injected HTTP/resolver fakes, no real
  sockets/threads) that force (a) a socket connect/read timeout and (b) a **resolution failure** on
  each operation, asserting each degrades to a transient failure. Because production already holds a
  `threading.Event`, the thread guarantee is pinned **behaviorally, not by a bare count snapshot**:
  the clock/stop seam is driven exactly N times with the bounded delay sequence (`2→4→8` cap `30`,
  jitter, `Retry-After` cap `60`); `threading.active_count()` is **unchanged across the backoff**;
  and `stop.set()` mid-backoff **returns control before the next attempt** (no further attempt or
  sleep). The synchronous fake clock is mandatory — a real `Timer` would make the case nondeterministic
  and is the failure mode the assertion exists to catch.
- **Crash vs continue:** config errors exit 1 (s6 `finish` halts → HA shows the add-on stopped,
  unmissable); runtime transient errors are caught and the loop continues.
- **Observability — logs (v1):** each cycle resolve `name` and log the current value. When a
  client-detected IP exists, the line compares the two (e.g. `home.example.com → 198.51.100.7
(matches ✓)`); when `url` relied on **server-side detection** (no client IP), it instead reads
  `home.example.com → 198.51.100.7 (server-detected)` — never a "match" it can't substantiate. When a
  `url` fire returned `2xx` but the post-fire resolve still shows the **old/missing** value, the line
  reads `home.example.com → 203.0.113.9 (unconfirmed — fired, DNS not yet updated)` and the cycle stays
  unconfirmed (refires next interval) — so a success-without-effect is visible, not masked. Either
  way it's the end-to-end proof the update took, for **both** providers. The query is directed by
  the **`test_ns`** option: set it to the zone's **authoritative NS** (e.g. `ns1-NN.azure-dns.com`)
  for a cache-free authoritative readout — the true "ns query"; set it to a **public resolver**
  (`8.8.8.8`) for that resolver's view (bypasses the host's resolver but may still be cached there);
  leave it blank for the system resolver's recursive/cached view (simplest, but can lag up to the
  record TTL right after a change).
- **The config page carries an _input_, not a readout:** the `test_ns` field lives on the static
  options form (an input is fine), but the **resolved value itself is logged, not shown on the
  page** — an add-on's options form cannot display a live/computed runtime value. Live state lives
  in the **Log tab** (v1); an **ingress status page** (`ingress: true`) and/or an **HA sensor**
  (MQTT/Supervisor API) are deferred enhancements.

## Verification

- **Static/CI:** `pyright` strict + `ruff check`/`format` clean; `py-ddns --check` oracle modes
  pass locally and in `lint.yaml` (per-provider config rejection, Azure URL/body/token shaping,
  URL endpoint shaping incl. a pre-existing query string, IP parse/guard, resolver `test_ns`
  UDP-query shaping + getaddrinfo fallback, **per-request status handling** (`404`→create,
  token-fail terminal, cached-token `401` re-acquire-once, URL `4xx` terminal), and **per-operation
  timeouts degrade to interruptible transient retries** — all via fakes/fake-clock, no network).
- **Dry run:** `python -m pyddns --check --dry-run --options <opts.json>` prints the planned action
  **redacted** — method, host, record label, redacted bodies only (Azure: PUT target + record/IP;
  URL: scheme+host with the secret path masked) — never token-request material, bearer token,
  `clientSecret`, or full `url_endpoint`. No network.
- **Build:** `build-app.yaml` produces a multi-arch image; confirm tokenless public pull of
  `ghcr.io/cristianstoica/py-ddns:latest` (as verified for py-syslog).
- **End-to-end (operator-executed):** install on the HA host; for `azure`, configure provider +
  `name` + `azure_token`, start, watch the Log tab for a successful PUT and the resolve line; for
  `url`, configure provider + `name` + `url_endpoint`, start, watch the fire + resolve line; then
  from anywhere `dig {name}` and confirm it resolves to the host's current public IP. Optionally set
  `test_ns` to the zone's authoritative NS for a cache-free resolve line, and cross-check it against
  `dig @{test_ns} {name}`.
- **Idempotency (both archetypes, including default `url` mode):** no churn while the IP is
  unchanged, then an update within `interval_seconds` after a real/simulated IP change. For the
  **API archetype** this is "no PUT while `read_current()`/last-good matches". For the **callback
  archetype** — including the **default `url_send_myip: false` server-side-detection mode where there
  is no client `detected_ip`** — steady state is reached by persisting the **post-fire resolved
  value** and suppressing the next fire while `name` still resolves to it; verify the default-mode
  case explicitly fires once, confirms, then **stops firing** on subsequent unchanged cycles (it must
  not fire every `interval_seconds` forever). Covered by the callback steady-state `--check` case.
- **Startup self-heal:** with local state claiming the current IP but the external record
  missing/stale, the first cycle still asserts (does not wait for `drift_reconcile_seconds`, and
  heals even when drift reconcile is `0`) — covered by a fake-provider `--check` case.
- **No secret leakage:** `python -m pyddns --check --dry-run --options <opts.json> 2>&1 | grep -Ei
'<clientSecret>|bearer |<url-secret-path>'` returns empty; the `--check` oracle additionally
  forces an `HTTPError`/`URLError` and asserts the secret is absent from the logged message.

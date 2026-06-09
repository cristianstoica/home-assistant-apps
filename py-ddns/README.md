# Py-DDNS

A generic, stdlib-only **dynamic-DNS updater**, packaged as a Home Assistant
add-on. It keeps one DNS **A record** pointed at the box's current egress IPv4
through one of two provider archetypes. There is **no `provider` option** — the
provider is **inferred from whichever config section you fill** (the **Azure DNS**
section selects Azure; the **Callback URL** section selects the callback):

- **Azure (API archetype)** — fill the **Azure DNS** section. This add-on detects
  the egress IPv4 from an HTTPS echo source, then create-or-replaces the A record
  via the **Azure DNS** management API (`GET` then `PUT`, pinned to the GA
  `2018-05-01` api-version).
- **Callback URL (callback archetype)** — fill the **Callback URL** section. This
  add-on fires a **secret callback URL** (cPanel-style) and the remote server
  reads the request's source IP and sets the record. The client does not need to
  detect the IP; the server does.

If you fill **both** sections, the **Callback URL wins** — the Azure DNS section
is ignored (a warning is logged to the Log tab).

Each cycle reconciles on an interval, applies **bounded interruptible backoff** to
transient failures (3 attempts, exponential with jitter, honoring `Retry-After`),
and **confirms the change in DNS** before considering it done. It is **secret-safe
by construction**: the Azure `clientSecret`, bearer tokens, and the full callback
URL are never logged — diagnostics show the host with the secret path redacted.

## Installation

This is a **custom add-on repository**, so installation is two steps — add the
repository, then install the add-on from it. It requires a Home Assistant install
with the **Supervisor** (HA OS or Supervised); HA Container/Core have no add-on
store.

1. In Home Assistant, open **Settings → Add-ons → Add-on Store**.
2. From the top-right **⋮** menu choose **Repositories**, paste
   `https://github.com/cristianstoica/home-assistant-apps`, click **Add**, then
   **Close**.
3. The store refreshes — find the **Py-DDNS** card, open it, and click
   **Install**.
4. On the **Configuration** tab, fill **one** of the two sections — **Azure DNS**
   or **Callback URL** (see below); the provider is inferred from which section
   you fill. Then **Start**.

## Configuring the Azure DNS section

The Azure path drives the Azure DNS management API with a **service principal**
(SP) whose **role assignment is scoped to a single DNS zone** (not the
subscription or resource group). You fill the six credential fields plus the
zone-identifying values into the **Azure DNS** section; there is no JSON blob.

The six identifying values map to the Azure resource path of your zone, the
**`<SUB>/<RG>/<ZONE>` triplet**:

- **`<SUB>`** — the **subscription ID** (a GUID) that owns the zone.
- **`<RG>`** — the **resource group** that contains the zone.
- **`<ZONE>`** — the DNS **zone name**, e.g. `example.com`.

Together they form the zone resource ID
`/subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.Network/dnszones/<ZONE>`,
which is the scope the SP's role assignment is pinned to.

### Prerequisites — sign in and discover the triplet

Run these with the Azure CLI before either avenue below:

1. **Sign in** and select the subscription that owns the zone (at the ≥2.61
   picker, choose the target subscription **by number** — pressing Enter keeps a
   possibly-wrong default):

   ```sh
   az login
   az account show   # confirm id (= <SUB>) and tenantId
   ```

2. **Find `<RG>` and `<ZONE>`** by listing the DNS zones in the subscription:

   ```sh
   az network dns zone list --output table
   ```

   The output's `ResourceGroup` column is `<RG>` and the `Name` column is
   `<ZONE>`. Optionally inspect the live records first (the apex usually belongs
   to your website — the add-on must never touch it):

   ```sh
   az network dns record-set list \
     --resource-group "<RG>" --zone-name "<ZONE>" --output table
   ```

### Avenue 1 — discover an existing SP

If you **already have** a DNS zone _and_ a service principal whose role
assignment is scoped to that zone, you only need to gather its identifiers:

- **`tenant_id`** — from `az account show` (`tenantId`).
- **`client_id`** — the SP's **application (client) ID** (a GUID, the `appId`).
- **`client_secret`** — an existing/rotated SP secret (Azure never reveals an old
  one, so you may need to create a new credential for the SP).
- **`subscription_id` / `resource_group` / `zone`** — the `<SUB>/<RG>/<ZONE>`
  triplet from the prerequisites.

Confirm the SP's assignment is single-zone scoped before relying on it:

```sh
az role assignment list --assignee "<appId>" --all --output table
```

The `Scope` must end `.../dnszones/<ZONE>` (one zone — not a subscription- or
resource-group-wide scope). `--all` is required so a zone-scoped assignment is
not omitted.

### Avenue 2 — provision a new single-zone SP

If you do **not** yet have a service principal, create one whose role assignment
is scoped to **only** this zone:

1. Get the zone resource ID (the `--scopes` value):

   ```sh
   az network dns zone show \
     --resource-group "<RG>" --name "<ZONE>" \
     --query id --output tsv
   ```

2. Create the SP with a **single-zone-scoped** role assignment (the assignment
   targets one zone, not the subscription or resource group):

   ```sh
   az ad sp create-for-rbac \
     --name "py-ddns" \
     --role "DNS Zone Contributor" \
     --scopes "<paste the zone resource ID from step 1>"
   ```

   This prints `appId` (→ **Client ID**), `password` (→ **Client secret**) and
   `tenant` (→ **Tenant ID**). The role is **`DNS Zone Contributor`**, scoped to
   one zone — a leaked credential can then only change records _in that zone_. It
   is **not** least-privilege: `DNS Zone Contributor` grants CRUD on _every_
   record type in the zone, not just the A record. For a tighter grant (an
   A-record-only custom role), an Owner/User-Access-Administrator can define a
   custom role granting only the actions py-ddns needs and pass it as `--role`
   instead — see Azure's
   [Protect DNS zones and records](https://learn.microsoft.com/en-us/azure/dns/dns-protect-zones-recordsets#custom-roles)
   for a record-type-scoped custom-role example.

### Fill the Azure DNS section, then set the host name

Enter the gathered values into the **Azure DNS** section on the Configuration
tab — leave the **Callback URL** section blank (that is how the add-on infers the
Azure path):

| Azure DNS field     | Value                             |
| ------------------- | --------------------------------- |
| **Client ID**       | `appId` (a GUID)                  |
| **Client secret**   | `password` (masked; never logged) |
| **Tenant ID**       | `tenant` / `tenantId`             |
| **Subscription ID** | `<SUB>`                           |
| **Resource group**  | `<RG>`                            |
| **DNS zone**        | `<ZONE>`, e.g. `example.com`      |

> **Where each value comes from** (grab the right key — the names differ across layers):
> **Client ID** = `appId`; **Client secret** = `password` in `az` output (Microsoft Graph and
> the Azure portal call the same value `secretText`); **Tenant ID** = `tenantId` and
> **Subscription ID** = the **`id`** field — both from `az account show` (there is no
> `subscriptionId` key).

Then set the top-level **Host name** to the FQDN to keep updated, e.g.
`home.example.com`. It **must be a sub-record of the `DNS zone`** — the **zone
apex is rejected** (a host updater must never repoint a zone apex, which on a
shared zone is the live site's record). The **DNS zone** field is authoritative
for this check. No manual DNS record is needed: the first cycle does
`GET → 404 → create` for the host name and self-heals drift on every boot.

**Security notes for the Azure path:**

- Scope the SP's role assignment to **one zone**, not the subscription or
  resource group — a leaked credential can then only repoint records in that
  zone.
- An expired/rotated client secret is a **terminal** error (it never self-heals);
  the add-on surfaces the AAD error code (e.g. `AADSTS7000222`) in the Log tab
  **without** echoing the secret, so rotate the secret and restart.
- The api-version is pinned to GA `2018-05-01` so a long-lived unattended client
  never depends on a preview version Azure can retire.

## Configuring the Callback URL section

The callback path fires a **secret callback URL** — the credential is encoded in
the URL's path/query (the usual cPanel "dynamic DNS" update URL). Fill the
**Callback URL** section (leave the **Azure DNS** section blank):

- **Callback URL** to the full secret endpoint. It **must be `https://`** — a
  plaintext callback would leak the record-repointing secret in transit; `http`,
  hostless, `user:pass@` and `#fragment` URLs are rejected at startup.
- **Send detected IP (myip)** — leave **disabled** (the usual cPanel behaviour) to
  let the server detect the record from the request's own source IP. Enable it
  only if your endpoint expects the IP as a `?myip=` parameter; the add-on then
  appends/replaces `myip` while preserving any other query parameters.

The secret never appears in the Log tab: diagnostics render the callback as
`https://<host>/<redacted>`.

### Skipping TLS certificate verification (advanced, insecure)

`url.insecure_skip_verify` (default **off**) is an opt-out for one specific case:
your callback endpoint is served over `https://` but presents a certificate that
**fails verification** (a self-signed cert, or a hostname mismatch on a shared
cPanel host) **and** your provider only gives you that own-domain URL — there is
no clean-cert "provider hostname" URL to use instead. With the flag off, every
cycle fails at request time with a transport error and the add-on cannot run.

When you enable it, the add-on disables TLS **certificate verification** on the
callback path only. Understand the tradeoff before you do:

- The channel **stays encrypted** — a passive eavesdropper still cannot read the
  secret callback URL.
- But **authentication is lost**: an _active_ man-in-the-middle could impersonate
  the endpoint, terminate the TLS session, and capture the capability URL — which
  **is** the DDNS update credential. This is _encrypted-but-unauthenticated_:
  materially safer than plaintext (which the add-on always rejects), strictly
  weaker than verified TLS.

Scope and guardrails:

- **HTTPS is still mandatory.** The flag only changes which certificate check
  runs on an already-`https://` URL; `http://`, hostless, `user:pass@` and
  `#fragment` URLs are still rejected at startup.
- **Azure and the IP sources always verify** — the skip never reaches them.
- A **WARNING is logged every callback cycle** while the flag is on (including
  steady cycles where no update fires), so the downgrade is never silent.

**Recommendation:** leave this off and fix the certificate (or obtain a
verifiable-cert callback URL) wherever possible. Enable it only when a
verifiable-cert URL is genuinely unobtainable.

## Options

There is **no `provider` option**. The provider is **inferred** from which
section you fill: a non-blank **Callback URL** `endpoint` selects the callback
path; otherwise any non-blank **Azure DNS** credential field selects the Azure
path; if **neither** is filled the add-on errors at startup. If you fill **both**
sections, the **Callback URL wins** — the Azure DNS group is **ignored** (not
parsed) and a warning is logged.

Configuration is two always-visible nested groups (`url:` and `azure:`) plus the
shared top-level fields below.

**Shared (top-level):**

| Option                    | Type                                | Default | Meaning                                                                                                                                                                                    |
| ------------------------- | ----------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`                    | `str?`                              | `""`    | **Host name** — the FQDN to keep updated. Required for both paths. For Azure it must be a sub-record of `azure.zone` (apex rejected); for the callback it is the DNS verification readout. |
| `interval_seconds`        | `int(60,86400)`                     | `120`   | How often a reconcile cycle runs.                                                                                                                                                          |
| `drift_reconcile_seconds` | `int(0,86400)`                      | `3600`  | Force an authoritative live re-check to heal out-of-band drift; `0` disables the periodic drift check.                                                                                     |
| `test_ns`                 | `str?`                              | `""`    | Optional nameserver IP to query directly when confirming a record value; blank uses the system resolver.                                                                                   |
| `log_level`               | `list(debug\|info\|warning\|error)` | `info`  | Verbosity of Py-DDNS's **own** diagnostics on stderr. Secrets are never logged at any level.                                                                                               |

**Callback URL section (`url:`) — fill `url.endpoint` to select the callback path:**

| Option                     | Type        | Default | Meaning                                                                                                                                                                                                                                                                                    |
| -------------------------- | ----------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `url.endpoint`             | `password?` | `""`    | The secret HTTPS callback endpoint. Must be `https://` (http, hostless, `user:pass@`, `#fragment` rejected). Masked; never logged (redacted).                                                                                                                                              |
| `url.send_myip`            | `bool?`     | `false` | Append the detected IP as `?myip=`; leave off to let the server detect the source IP.                                                                                                                                                                                                      |
| `url.insecure_skip_verify` | `bool?`     | `false` | **Advanced, insecure.** Skip TLS _certificate_ verification on the callback path only (HTTPS still required; azure/ip-source always verify). Encrypted but unauthenticated; logs a WARNING every cycle. See [the section above](#skipping-tls-certificate-verification-advanced-insecure). |

**Azure DNS section (`azure:`) — fill the credential fields to select the Azure path:**

| Option                  | Type             | Default                          | Meaning                                                                                                                                                                            |
| ----------------------- | ---------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `azure.client_id`       | `str?`           | `""`                             | SP application (client) ID — a GUID (the `appId`).                                                                                                                                 |
| `azure.client_secret`   | `password?`      | `""`                             | SP secret. Masked; never logged.                                                                                                                                                   |
| `azure.tenant_id`       | `str?`           | `""`                             | The Azure AD tenant ID the SP belongs to.                                                                                                                                          |
| `azure.subscription_id` | `str?`           | `""`                             | The subscription ID (`<SUB>`) that owns the zone.                                                                                                                                  |
| `azure.resource_group`  | `str?`           | `""`                             | The resource group (`<RG>`) that contains the zone.                                                                                                                                |
| `azure.zone`            | `str?`           | `""`                             | The DNS zone (`<ZONE>`), e.g. `example.com`. Authoritative for the name↔zone check.                                                                                                |
| `azure.ip_sources`      | `str?`           | `api.ipify.org`, `icanhazip.com` | Comma/space-separated HTTPS echo endpoints; first global-unicast answer wins. **Blank → built-in defaults.** Non-global answers and http/hostless/userinfo/fragment URLs rejected. |
| `azure.ttl`             | `int(30,86400)?` | `60`                             | TTL written on the A record-set.                                                                                                                                                   |

The six credential fields (`client_id`, `client_secret`, `tenant_id`,
`subscription_id`, `resource_group`, `zone`) are what select the Azure path —
`ttl`, `ip_sources` and `send_myip` carry defaults and do **not** select a
provider on their own. `ip_sources` and `ttl` live under the `azure:` group for
UI placement only; the built-in default IP sources are used on the callback path
too.

`state_path` (`/data/last_known_ip`) is a **development override** key recognized
by the loader but deliberately **absent from the HA schema**, so a deployed add-on
never sets it; it exists only for `--check`/test harnesses.

## How reconciliation works

- **First cycle is authoritative.** On startup the add-on reads the **real**
  current record (`azure`: a `GET`; `url`: a DNS confirmation) regardless of local
  state, so it self-heals drift on boot **even with `drift_reconcile_seconds: 0`**
  and even if the persisted last-known value still matches.
- **Steady state suppresses needless work.** Once the record is confirmed at the
  current value, subsequent cycles **suppress** the update until the IP changes or
  the drift cadence elapses.
- **Callback confirmation gate (`url`).** After firing the callback, the add-on
  re-resolves the name and only **persists** the value (and suppresses future
  fires) once DNS confirms it. A `NO_RECORD` result is treated as _unconfirmed_
  and **re-fires** next cycle; a transient resolve failure is _inconclusive_ and
  **holds** the last-known value unchanged rather than clearing it.
- **Bounded backoff, no threads.** Transient failures (`429`/`5xx`/network) retry
  up to 3 attempts with exponential delay (2→4→8s, capped 30s, ±20% jitter,
  `Retry-After` honored and capped at 60s). The interval sleep and the backoff are
  **interruptible** — a `SIGTERM`/`SIGINT` aborts promptly via a single
  `threading.Event`; there is no `Timer`/worker thread.

> **DNS confirmation latency on the Alpine/musl base image.** When `test_ns` is
> blank, confirmation uses `socket.getaddrinfo`, which takes no timeout argument;
> on the musl resolver in the base image a failing lookup is internally bounded to
> **~5s**. Set `test_ns` to the zone's authoritative nameserver IP to confirm via
> a direct **3s** UDP query that bypasses the local resolver cache (useful when
> the cache would otherwise mask a just-written change).

## Log levels

`log_level` sets how much of Py-DDNS's **own** diagnostics reach the Log tab (all
on stderr). Secrets are never logged at any level — the callback URL renders as
`https://<host>/<redacted>` and the Azure `client_secret` is scrubbed. Each level
adds to the ones below it:

- **`error`** — a failure that won't self-heal this cycle; the last-good IP is
  **held** (a terminal provider error, or an unexpected error caught by the
  never-raises backstop).
- **`warning`** — recoverable/transient conditions: transient-failure retries and
  retry exhaustion, IP-source failures/non-global-answer fallbacks, the
  callback's _unconfirmed_ and _inconclusive_ post-fire confirmation outcomes,
  and the both-sections-filled "Azure ignored" notice.
- **`info`** _(default)_ — the normal lifecycle and per-cycle outcomes: the
  startup readout, `matches ✓` / `unchanged` / `wrote A record` / steady-suppress
  decisions, and the stop-signalled exit line.
- **`debug`** — adds a per-cycle trace at the three lifecycle points: **IP
  detection** (the detected egress IPv4, or the deferred-to-server note for the
  callback path), the **update decision** (authoritative/steady/suppress/fire
  branch), and the **DNS confirmation** outcome (the apply action or the post-fire
  resolve status). Useful for answering "why didn't my record update?".

## Watchdog

No `watchdog:` is configured. HA's watchdog is HTTP/TCP-only and **N/A** for an
outbound updater — there is no TCP/HTTP endpoint to probe. Liveness is observed
via the per-cycle status lines on stderr (Log tab).

## Verifying the updater

**Offline self-check (no network, no real sockets/threads):**

```sh
# from the repo root
PYTHONPATH=py-ddns python3 -m pyddns --check
```

This drives the built-in fixture corpus through the real production seams and
asserts every decision the updater makes: per-provider options rejection (the
name↔zone apex/wrong-zone contract, the HTTPS-only contract on the callback and
every ip-source, and the range/enum checks), the Azure URL/body/token shaping and
its per-request status handling (`GET 404`→create path, token-auth-failure→terminal,
`429`/`5xx`→transient with `Retry-After`, `403`→terminal, cached-`401`→re-acquire
once then retry, `401`-after-fresh→terminal), the URL `myip`-merge shaping, the
IP-source parse/global-unicast guard, the three-way DNS resolver outcome
(`RESOLVED`/`NO_RECORD`/`FAILED`, never a bare `None`), the bounded interruptible
backoff (including a `threading.active_count()` invariant proving **no thread is
spawned**), the callback confirmation gate on the state seam, the startup
self-heal, and a **no-secret-leakage** assertion that forces every secret-bearing
failure path and proves the secret appears in none of the captured output. It
exits non-zero on any mismatch.

**Dry-run the planned action (no network, secret redacted):**

```sh
# uses the built-in azure example off-HAOS:
PYTHONPATH=py-ddns python3 -m pyddns --check --dry-run
# or point at a local options.json for either provider:
PYTHONPATH=py-ddns python3 -m pyddns --check --dry-run --options /path/to/options.json
```

This loads + validates the options and prints the **redacted** planned action
(method/host/record label/redacted body) without touching the network.

## Maintenance: re-run `--check` after a base-image bump

The add-on pins `BUILD_FROM=ghcr.io/home-assistant/base-python:3.13-alpine3.23`.
After bumping that base image (Python or Alpine version), **re-run
`python3 -m pyddns --check`** before publishing: the musl resolver behavior and
the stdlib `ipaddress`/`socket`/`ssl` surfaces the updater leans on can shift
across base-image versions, and the offline oracle is the gate that catches a
regression before it ships.

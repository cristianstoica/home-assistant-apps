# Py-DDNS

A generic, stdlib-only **dynamic-DNS updater**, packaged as a Home Assistant
add-on. It keeps one DNS **A record** pointed at the box's current egress IPv4
through one of two provider archetypes, behind a single `provider` switch:

- **`azure` (API archetype)** — this add-on detects the egress IPv4 from an HTTPS
  echo source, then create-or-replaces the A record via the **Azure DNS**
  management API (`GET` then `PUT`, pinned to the GA `2018-05-01` api-version).
- **`url` (callback archetype)** — this add-on fires a **secret callback URL**
  (cPanel-style) and the remote server reads the request's source IP and sets the
  record. The client does not need to detect the IP; the server does.

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
4. On the **Configuration** tab, set `provider` and that provider's fields (see
   below), then **Start**.

## Configuring the `azure` provider

The `azure` provider drives the Azure DNS management API with a **service
principal** (SP) scoped to exactly one DNS zone. Bootstrap it once with the Azure
CLI, then paste the credential blob into the add-on.

1. Create a least-privilege SP scoped to **only** your DNS zone, as **DNS Zone
   Contributor** (not a subscription-wide role):

   ```sh
   az ad sp create-for-rbac \
     --name "py-ddns-<zone>" \
     --role "DNS Zone Contributor" \
     --scopes "/subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.Network/dnszones/<ZONE>"
   ```

2. Assemble the credential blob from the CLI output plus your subscription /
   resource-group / zone, and paste it into the **Azure SP credential (JSON)**
   field as a single JSON object:

   ```json
   {
     "tenantId": "<tenant>",
     "subscriptionId": "<SUB>",
     "resourceGroup": "<RG>",
     "zone": "example.com",
     "clientId": "<appId>",
     "clientSecret": "<password>"
   }
   ```

3. Set **Record name** to the FQDN to keep updated, e.g. `home.example.com`. It
   **must be a sub-record of the blob's `zone`** — the **zone apex is rejected**
   (a host updater must never repoint a zone apex, which on a shared zone is the
   live site's record). The blob's `zone` is authoritative for this check.

**Security notes for `azure`:**

- Scope the SP to **one zone**, not the subscription — a leaked credential can
  then only repoint records in that zone.
- An expired/rotated client secret is a **terminal** error (it never self-heals);
  the add-on surfaces the AAD error code (e.g. `AADSTS7000222`) in the Log tab
  **without** echoing the secret, so rotate the secret and restart.
- The api-version is pinned to GA `2018-05-01` so a long-lived unattended client
  never depends on a preview version Azure can retire.

## Configuring the `url` provider

The `url` provider fires a **secret callback URL** — the credential is encoded in
the URL's path/query (the usual cPanel "dynamic DNS" update URL). Set:

- **Callback URL** to the full secret endpoint. It **must be `https://`** — a
  plaintext callback would leak the record-repointing secret in transit; `http`,
  hostless, `user:pass@` and `#fragment` URLs are rejected at startup.
- **Send detected IP (myip)** — leave **disabled** (the usual cPanel behaviour) to
  let the server detect the record from the request's own source IP. Enable it
  only if your endpoint expects the IP as a `?myip=` parameter; the add-on then
  appends/replaces `myip` while preserving any other query parameters.

The secret never appears in the Log tab: diagnostics render the callback as
`https://<host>/<redacted>`.

## Options

| Option                    | Type                                | Default                          | Meaning                                                                                                           |
| ------------------------- | ----------------------------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `provider`                | `list(azure\|url)`                  | `azure`                          | Which DNS backend to drive.                                                                                       |
| `name`                    | `str?`                              | `""`                             | The FQDN to keep updated. For `azure`, a sub-record of the blob's zone (apex rejected); for `url`, informational. |
| `azure_token`             | `str?`                              | `""`                             | **`azure` only.** SP credential blob (JSON). Required when `provider=azure`. `clientSecret` is never logged.      |
| `ip_source_urls`          | list of `url?`                      | `api.ipify.org`, `icanhazip.com` | **`azure` only.** Ordered HTTPS echo endpoints; first global-unicast answer wins. Non-global answers rejected.    |
| `ttl`                     | `int(30,86400)`                     | `60`                             | **`azure` only.** TTL written on the A record-set.                                                                |
| `url_endpoint`            | `url?`                              | `""`                             | **`url` only.** The secret HTTPS callback endpoint. Required when `provider=url`. Never logged (redacted).        |
| `url_send_myip`           | `bool`                              | `false`                          | **`url` only.** Append the detected IP as `?myip=`; leave off to let the server detect the source IP.             |
| `interval_seconds`        | `int(60,86400)`                     | `120`                            | How often a reconcile cycle runs.                                                                                 |
| `drift_reconcile_seconds` | `int(0,86400)`                      | `3600`                           | Force an authoritative live re-check to heal out-of-band drift; `0` disables the periodic drift check.            |
| `test_ns`                 | `str?`                              | `""`                             | Optional nameserver IP to query directly when confirming a record value; blank uses the system resolver.          |
| `log_level`               | `list(debug\|info\|warning\|error)` | `info`                           | Verbosity of Py-DDNS's **own** diagnostics on stderr. Secrets are never logged at any level.                      |

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

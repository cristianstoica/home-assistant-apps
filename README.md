# Home Assistant Apps

A small collection of Home Assistant add-ons (apps) packaged for the
Supervisor add-on store, with prebuilt multi-architecture images published to
`ghcr.io`.

[![Open your Home Assistant instance and show the add-on store with this repository pre-filled.](https://my.home-assistant.io/badges/supervisor_store.svg)](https://my.home-assistant.io/redirect/supervisor_store/?repository_url=https%3A%2F%2Fgithub.com%2Fcristianstoica%2Fhome-assistant-apps)

## Apps

### [Py-Syslog](./py-syslog)

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

A durable, stdlib-only **UDP syslog collector** for Home Assistant. It receives
RFC 3164 / 5424 datagrams, resolves each sender IP to a `site`/`host`, and
writes one daily-rotated, gzip-compressed, retained file under `/data/log` —
with each stored line echoed live to the add-on **Log tab**. Collector only:
no search engine, no sensors. Storage failures are counted and surfaced, never
silently swallowed.

### [Py-DDNS](./py-ddns)

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

A generic, stdlib-only **dynamic-DNS updater** for Home Assistant. It keeps one
DNS A record pointed at the box's current egress IPv4 through one of two
provider archetypes, with the provider **inferred from whichever config section
(Callback URL / Azure DNS) you fill**: **Azure** (API archetype —
create-or-replace via the Azure DNS management API with a service principal
whose role assignment is scoped to a single DNS zone), or **callback URL**
(fires a secret cPanel-style endpoint and the server detects the source IP).
Each cycle reconciles on an interval with bounded backoff and post-update DNS
confirmation. Secret-safe by construction: SP secrets, bearer tokens, and the
callback URL path are never logged.

### [Py-Weather](./py-weather)

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

An adaptive, stdlib-only **poller for Weather.com PWS REST sensors** for Home
Assistant. It forces a refresh of one representative sensor per station via
`homeassistant.update_entity` through the Supervisor Core-API proxy, judges each
station's freshness and health from `/states`, and **adapts the cadence**: a
randomized fast interval when confirmed, exponential backoff when transient, and a
slow hold (no tight retry loop) on a terminal token/target fault. Holds no
Weather.com credentials — the REST integration owns external access — and never
logs the Supervisor bearer.

## Install

> **Requires Home Assistant with the Supervisor** (HA OS or Supervised). HA
> Container/Core have no add-on store.

1. In Home Assistant: **Settings → Add-ons → Add-on Store → ⋮ → Repositories**.
2. Add `https://github.com/cristianstoica/home-assistant-apps` and close the
   dialog.
3. Find the app you want (e.g. **Py-Syslog**) in the store and click
   **Install**.
4. On the **Configuration** tab, set options, then **Start**.

## Publishing

Release images are built by `.github/workflows/builder.yaml` and published to
GHCR as per-architecture images plus a multi-architecture manifest. For
`py-weather`, the workflow must be able to write these packages:

- `ghcr.io/cristianstoica/amd64-py-weather`
- `ghcr.io/cristianstoica/aarch64-py-weather`
- `ghcr.io/cristianstoica/py-weather`

If GHCR rejects a push with `permission_denied: write_package`, grant the
`cristianstoica/home-assistant-apps` repository write access under each package's
**Package settings -> Manage Actions access**. The workflow can also use a
repository secret named `GHCR_TOKEN` with `write:packages` scope, and optionally
`GHCR_USERNAME`, if package access cannot be repaired for `GITHUB_TOKEN`.

## License

[MIT](LICENSE).

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg

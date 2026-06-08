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

## Install

> **Requires Home Assistant with the Supervisor** (HA OS or Supervised). HA
> Container/Core have no add-on store.

1. In Home Assistant: **Settings → Add-ons → Add-on Store → ⋮ → Repositories**.
2. Add `https://github.com/cristianstoica/home-assistant-apps` and close the
   dialog.
3. Find the app you want (e.g. **Py-Syslog**) in the store and click
   **Install**.
4. On the **Configuration** tab, set options, then **Start**.

## License

[MIT](LICENSE).

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg

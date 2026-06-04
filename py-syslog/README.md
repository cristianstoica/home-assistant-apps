# Py-Syslog

A durable, stdlib-only UDP **syslog collector**, packaged as a Home Assistant
add-on. It receives RFC 3164 / 5424 datagrams, resolves each sender IP to a
`site`/`host`, and writes **one** daily-rotated, gzip-compressed, retained file
under `/data/log`, with the site/host stamped on every line. Each stored line is
also echoed to stdout so it shows up live in the HA add-on **Log tab**.

It is a **collector only** — no search engine, no Home Assistant sensors or
Logbook events (the stdout echo is just the container log stream). Storage
failures are **propagated and counted**, never silently swallowed.

## Installation

This is a **custom add-on repository**, so installation is two steps — add the
repository, then install the add-on from it. It requires a Home Assistant
install with the **Supervisor** (HA OS or Supervised); HA Container/Core have no
add-on store.

1. In Home Assistant, open **Settings → Add-ons → Add-on Store**.
2. From the top-right **⋮** menu choose **Repositories**, paste
   `https://github.com/cristianstoica/home-assistant-apps`, click **Add**, then
   **Close**.
3. The store refreshes — find the **Py-Syslog** card, open it, and click
   **Install**.
4. On the **Configuration** tab, add your sender mappings (see below), then
   **Start**.
5. Point each device's syslog forwarding at the Home Assistant host on UDP
   **5514** (the default `listen_port`).

## Configuring your sources

The collector is **multi-source by design**: each sender IP maps to a
`(site, host)` pair stamped on every line it produces. Configure the mapping in
the add-on **Configuration** tab. Example:

```yaml
sources:
  - { ip: 192.0.2.1, site: home, host: router1 }
```

A sender that is **not** listed in `sources` is still **received and written**,
stamped `unknown`/`<ip>`, with a one-time WARNING on first sight. The default
ships with an empty `sources` list, so add at least one row to get friendly
site/host labels.

Point your devices' syslog forwarding at the Home Assistant host on
`listen_port` (default UDP **5514**).

## Options

| Option           | Type                                | Default | Meaning                                                           |
| ---------------- | ----------------------------------- | ------- | ----------------------------------------------------------------- |
| `listen_port`    | `port`                              | `5514`  | UDP port to bind on the host network.                             |
| `retention_days` | `int(1,3650)`                       | `30`    | Days of gzipped archives to keep; older ones are pruned.          |
| `log_level`      | `list(debug\|info\|warning\|error)` | `info`  | Diagnostic verbosity on stderr (does not affect stored data).     |
| `sources`        | list of `{ip, site, host}`          | `[]`    | IP → (site, host) resolution table. A duplicate `ip` is rejected. |

`log_dir` (`/data/log`) and `log_file` (`syslog.log`) are **development
override** keys recognized by the loader but deliberately **absent from the HA
schema**, so a deployed add-on can never misconfigure the storage path. They
exist only for `--check`/test harnesses.

## Networking and port

`host_network: true` is **required**, not cosmetic. On default bridge
networking a UDP collector breaks two ways: with Docker's userland-proxy on, the
container sees the bridge gateway as the source IP (collapsing every sender to
one identity and defeating resolution); with the proxy off, inbound UDP is
silently dropped. On the host network the process sees real source IPs and binds
the host port directly. With host networking there is no container→host port
mapping to desync, so `listen_port` is the single authoritative port. Add-ons
run as root, so a privileged port (≤1024) binds fine.

The authoritative "is the port free?" check is the add-on's own first start: it
binds `listen_port` or exits fast with a clear `cannot bind` message — set a
free port and restart.

## Viewing collected logs — two ways

1. **Full history (the rotated file):** `/data/log/syslog.log` is the current
   day; closed days are `syslog.log.<YYYY-MM-DD>.gz`. From the host:
   `tail -n 50 <addon-data>/log/syslog.log` (or `zcat` an archive).
2. **Live stream (the HA Log tab):** each stored line is echoed to stdout, which
   the add-on **Log tab** shows in real time. Diagnostics (stats, warnings) are
   on stderr; both streams surface in the Log tab, the split just keeps the
   collected stream separable.

> **"Failed to get … logs, Failed to fetch" in the Log tab.** Because this
> collector is intentionally quiet, its Log-tab stream can sit idle for minutes.
> Over a proxy or remote access (e.g. Home Assistant Cloud), an idle add-on log
> stream is dropped by the proxy's read-timeout after a few minutes — a known
> Home Assistant behavior ([home-assistant/addons#4149](https://github.com/home-assistant/addons/issues/4149))
> that affects any add-on's Log tab (built-in ones included) and does **not**
> occur on direct local access. It shows a one-off "Failed to fetch" toast;
> **reload the page to re-subscribe.** It is a cosmetic quirk of the HA log
> viewer, not a py-syslog failure — collection and the on-disk `/data/log` files
> are unaffected.

## Rotation, retention, and failure behavior

- **Daily UTC rotation.** At the first write after a UTC-day boundary the active
  file is gzipped atomically (`*.gz.tmp` → fsync → `os.replace` → fsync dir →
  unlink source → fsync dir) into `syslog.log.<date>.gz`.
- **Retention is time-bounded, not byte-capped.** Archives are pruned by the
  `<date>` **embedded in the filename** (not mtime), keeping `retention_days` of
  history. There is **no** size ceiling on the active file or the window.
- **Residual disk-fill risk.** Because retention is time-only, a misbehaving
  sender or a UDP flood could fill `/data` within a single day. Storage then
  degrades through the counted/throttled-warned `WriteError` path (receiving
  continues), never a silent crash — but it is still a fill. Operationally,
  watch it:

  ```sh
  df -h /data
  du -sh <addon-data>/log
  ```

  Byte-capping is revisited only if a high-volume sender is added.

- **Write failures are propagated.** A failed `write()`/rotation raises a domain
  `WriteError`; the server counts it as `write_errors` (never as `written`),
  emits one throttled WARNING, and keeps receiving. A failed startup
  `log_dir` creation is **fatal** (clear message, exit 1) rather than binding
  and silently dropping every datagram.

## Watchdog

No `watchdog:` is configured. HA's watchdog is HTTP/TCP-only and **N/A** for a
UDP listener — there is no TCP/HTTP endpoint to probe. Liveness is observed via
the periodic stats line on stderr (Log tab) and the storage file advancing.

## Verifying the collector

**Offline self-check (no socket, no real datagram):**

```sh
# from the repo root
PYTHONPATH=py-syslog python3 -m pysyslog --check
```

This drives the built-in fixture corpus through the real processing seam and
asserts every rendered line, protocol tag, `sender_ts`, resolved site/host, and
the aggregate counters — including an example `192.0.2.1 → home/router1`
mapping and the one-line escaping contract — plus the invalid-options rejection
(duplicate `ip`, empty fields, out-of-range retention). It exits non-zero on any
mismatch. `--check --storage` exercises the real rotation/gzip/prune/
reconciliation state machine; `--check --write-error` asserts the `WriteError`
contract.

**Live send (any LAN host → resolves to `unknown`/`<that host's IP>` unless it is
in `sources`):**

```sh
logger --udp --server <ha-host-ip> --port 5514 --rfc3164 --tag test --priority user.info "hi"
logger --udp --server <ha-host-ip> --port 5514 --rfc5424 --tag test "structured"
# confirm: tail -n 5 <addon-data>/log/syslog.log
# after a UTC-day boundary: ls -la shows syslog.log.<date>.gz (<= retention_days)
```

A `logger` datagram carries that host's source IP. If that IP is configured in
`sources` it is stamped with your `site`/`host`; otherwise it lands stamped
`unknown`/`<ip>` — a valid exercise of the unknown-source path. Configured
mappings are proven by `--check`, not by `logger`.

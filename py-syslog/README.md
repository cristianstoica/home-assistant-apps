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
site/host labels. Set `reject_unknown_sources: true` to **drop** (not store)
datagrams from senders absent from `sources`; they are counted in the
`rejected_sources` stat. This is a **noise filter, not authentication** — UDP
source IPs are spoofable, so a forged datagram claiming a configured IP is still
accepted.

Point your devices' syslog forwarding at the Home Assistant host on
`listen_port` (default UDP **5514**).

## Options

| Option                   | Type                                | Default   | Meaning                                                                                                                                                          |
| ------------------------ | ----------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `listen_port`            | `port`                              | `5514`    | UDP port to bind on the host network.                                                                                                                            |
| `listen_host`            | `str`                               | `0.0.0.0` | Interface/address to bind. `0.0.0.0` binds **all** interfaces; set a host IP to restrict (see Threat model below).                                               |
| `retention_days`         | `int(1,3650)`                       | `30`      | Days of gzipped archives to keep; older ones are pruned.                                                                                                         |
| `min_free_percent`       | `int(0,99)`                         | `0`       | **Size guard.** Free-space floor: prune oldest segments to keep ≥ this % of the volume free. `0` disables (see Size guard below).                                |
| `max_log_percent`        | `int(0,99)`                         | `0`       | **Size guard.** Log-dir cap: prune oldest segments so the log dir occupies ≤ this % of the volume. `0` disables.                                                 |
| `max_segment_mb`         | `int(0,4096)`                       | `0`       | **Size guard.** Size-rotation trigger: roll the active file to a `.gz` segment at this many MB. `0` disables; **must be > 0** to enable either percentage guard. |
| `log_level`              | `list(debug\|info\|warning\|error)` | `info`    | Verbosity of py-syslog's **own** diagnostics on stderr; does **not** filter ingested logs by severity (see note below).                                          |
| `sources`                | list of `{ip, site, host}`          | `[]`      | IP → (site, host) resolution table. A duplicate `ip` is rejected.                                                                                                |
| `reject_unknown_sources` | `bool`                              | `false`   | Drop (don't store) datagrams from senders not in `sources`; counted in `rejected_sources`. Noise filter, NOT authentication.                                     |

> **`log_level` controls py-syslog's own logging, not the logs it collects.** It
> sets the verbosity of py-syslog's _own_ operational diagnostics on stderr — the
> periodic stats line, the unknown-source warning, and throttled write-error
> warnings (the sense in which Python's `logging` uses "level"). It has **no
> effect on the syslog you ingest**: every received and non-rejected datagram is
> written to `/data/log` and echoed to the Log tab in full, regardless of its own
> syslog severity. For example, `log_level: error` quiets py-syslog's own chatter
> but you will still receive and store every non-rejected `debug`-severity line a
> sender emits.
> py-syslog does **not** currently filter, drop, or route ingested logs by
> severity.

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

### Threat model and hardening

- **Bind interface is operator-restrictable.** The default `listen_host`
  (`0.0.0.0`) binds **all** interfaces, which is convenient on a trusted LAN.
  Set `listen_host` to a specific host IP (e.g. `192.0.2.5`) to accept datagrams
  on only that interface.
- **UDP syslog is unauthenticated and source-spoofable.** There is no handshake
  and no sender authentication; the source IP on a UDP datagram can be forged, so
  any host that can reach the port can inject lines stamped as any sender —
  including one mapped in `sources`. Stamped `site`/`host` reflect the **claimed**
  source IP, not a verified identity. `reject_unknown_sources` reduces noise from
  unconfigured senders but does **not** authenticate — a spoofed source IP
  matching a configured entry bypasses it.
- **When exposing beyond a trusted LAN, restrict and firewall.** If you
  port-forward or otherwise expose the port past a trusted network, restrict the
  bind interface via `listen_host` **and** firewall the source (allow only known
  sender addresses on the collector port, e.g. `203.0.113.0/24`). Do both:
  restricting the bind interface alone does not authenticate senders.
- **Roadmap caveat.** Driving Home Assistant automations or events from collected
  logs would require a sender-trust mechanism first, since senders are currently
  unauthenticated — a spoofed datagram must never be able to trigger an action.

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
- **Retention is time-bounded by default.** Archives are pruned by the `<date>`
  **embedded in the filename** (not mtime), keeping `retention_days` of history.
  Time-retention alone has **no** size ceiling on the active file or the window.
- **Residual disk-fill risk — mitigated when the size guard is enabled.**
  Because time-retention is time-only, a misbehaving sender or a UDP flood could
  fill `/data` within a single day. The optional **size guard** (below) closes
  this gap: set `max_segment_mb` plus a percentage limit to byte-cap the log set
  as a ring buffer. When the guard is **disabled** (the default — all three
  knobs `0`), behavior is unchanged from 1.2.0: storage degrades through the
  counted/throttled-warned `WriteError` path (receiving continues), never a
  silent crash, but it is still a fill — so watch it:

  ```sh
  df -h /data
  du -sh <addon-data>/log
  ```

  The stats line also surfaces live `disk_free_pct` and `log_dir_mb` gauges
  (rendered whether or not the guard is enabled), so the manual `df`/`du` above
  is a fallback, not the only signal.

- **Write failures are propagated.** A failed `write()`/rotation raises a domain
  `WriteError`; the server counts it as `write_errors` (never as `written`),
  emits one throttled WARNING, and keeps receiving. A failed startup
  `log_dir` creation is **fatal** (clear message, exit 1) rather than binding
  and silently dropping every datagram.

### Size guard (ring buffer)

The size guard is an **optional, two-dimensional, byte-bounded ring buffer**
layered on top of time-retention. It is **disabled by default** — all three
knobs (`min_free_percent`, `max_log_percent`, `max_segment_mb`) default to `0`,
so a 1.2.0 → 1.3.0 upgrade changes nothing until you configure it.

- **Two limits, one volume basis.** `min_free_percent` is a **free-space
  floor** (prune until ≥ this % of the volume is free); `max_log_percent` is a
  **log-dir cap** (prune until the log dir occupies ≤ this % of the volume).
  Both derive from one `statvfs` read of the storage volume, so the cap
  auto-adapts if the volume is resized. Either or both may be set; pruning
  satisfies both at once (each delete raises free **and** shrinks the log dir).
- **`max_segment_mb` is a separate granularity knob.** It rolls the active file
  to a numbered `.gz` segment (`syslog.log.<date>.<NNN>.gz`) once it reaches that
  many MB, giving the guard intra-day segments to prune. The size check is
  **write-driven** (checked after each write+flush), so flood response is bounded
  by one segment's worth of writes, not the periodic stats tick. It **must be > 0**
  to enable either percentage guard — without intra-day segments a flood in the
  single active file would silently defeat the cap, so that combination is
  rejected at startup with a clear error.
- **Data-loss semantics: keep-newest, drop-oldest.** Under sustained pressure
  the guard deletes the **oldest archived segments first** (the bare daily
  archive prunes before same-day numbered segments). The newest data is always
  preserved; the **active, still-being-written file is never a prune victim**.
- **Terminal cases (never thrash, never lose live data).** If pruning every
  rotated segment still can't satisfy the floor (e.g. the disk is full of
  non-log data), the guard **stops** with one throttled WARNING and lets the
  counted `WriteError` path take over — it never deletes or truncates the active
  file to make room. At 999 segments in one UTC day it stops rolling (one
  WARNING) and keeps appending to the active file; the daily rollover resets the
  sequence. A `statvfs`/scandir **measurement** failure degrades safe (throttled
  WARNING, no prune, retry next tick) — deliberately, the guard is a monitoring
  concern, not a durability one.

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
(duplicate `ip`, empty fields, out-of-range retention, the size-guard ranges,
and the `max_segment_mb`-required coherence gate). It exits non-zero on any
mismatch. `--check --storage` exercises the real rotation/gzip/prune/
reconciliation state machine **and the size guard** (numbered-segment
size-rotation, `(date, seq)` keep-newest prune ordering, the two-dimensional
floor+cap ring buffer, the only-active-file / seq-overflow terminals, and
degrade-safe measurement failure); `--check --write-error` asserts the
`WriteError` contract. `--check --bind` binds a real `AF_INET`/`SOCK_DGRAM`
loopback socket through the production bind path to prove the listen-socket setup
(configured host, family/type, recv timeout, and an OS-ephemeral port).

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

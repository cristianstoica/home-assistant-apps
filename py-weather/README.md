# Py-Weather

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

An adaptive, stdlib-only **poller for Weather.com PWS REST sensors**, packaged as
a Home Assistant add-on. The Weather.com REST sensors stay in your Home Assistant
config as the source of truth for the values; Py-Weather becomes their **adaptive
scheduler**, forcing a refresh of one representative sensor per station through
the Supervisor Core-API proxy and adapting each station's poll cadence to how that
station is actually behaving.

For each station it calls `homeassistant.update_entity` on the configured
representative sensor (recommended `sensor.wu_obstimeutc_<key>`), waits a short
settle window, reads `/states`, and judges whether the station is **online** —
i.e. whether Weather.com has fresh observation data behind the REST resource. The
cadence then adapts:

- **Online** (the `obstimeutc` sensor is present and its state parses as a
  timestamp) → schedule the next poll at the **learned cadence** with jitter,
  tracking the station's real upload rhythm.
- **Offline** (the `obstimeutc` sensor is absent, unavailable, or unparseable — a
  Weather.com `204` collapses the whole REST resource) → re-probe once a day
  (`OFFLINE_REPROBE`, 86400s), accepting up to ~24h before a recovered station is
  noticed again.
- **Transient** (a timeout, a `5xx`, a `429`, a malformed `/states` body, or a
  stop signal interrupting the settle wait) → retry soon, at `min_interval_seconds`.
- **Terminal** (a `401`/`403`, or a non-`429` `4xx` on `update_entity` or
  `/states` — a bad token or a misconfigured target) → logged at `error` and
  **held at `max_backoff_seconds`**, never spun in a tight retry loop, so it
  self-heals on the next slow poll if corrected out of band.

A `429` is always **transient**, even on the `update_entity` POST — a rate limit
is not a terminal misconfiguration.

Py-Weather holds **no Weather.com credentials**: the REST integration owns
external Weather.com access. The only secret it touches is the Supervisor bearer
token, which it reads from the environment and **never logs**.

## Installation

This is a **custom add-on repository**, so installation is two steps — add the
repository, then install the add-on from it. It requires a Home Assistant install
with the **Supervisor** (HA OS or Supervised); HA Container/Core have no add-on
store.

1. In Home Assistant, open **Settings → Add-ons → Add-on Store**.
2. From the top-right **⋮** menu choose **Repositories**, paste
   `https://github.com/cristianstoica/home-assistant-apps`, click **Add**, then
   **Close**.
3. The store refreshes — find the **Py-Weather** card, open it, and click
   **Install**.
4. On the **Configuration** tab, review the cadence options. If your
   `sensor.wu_obstimeutc_*` entities already exist in Home Assistant you can leave
   **Stations** empty — the add-on will discover them automatically on first
   start (see [Auto-populate](#auto-populate-stations)). Otherwise add one row
   per station manually, then **Start**.

## Prerequisite: the REST sensors and the obstime macros

> **⚠️ Before upgrading to v0.3.0:** the `sensor.wu_obstimeutc_<key>` REST sensors
> **and** the `wu_has_obstime` / `wu_obstime` Jinja macros must already exist in
> your Home Assistant config (`rest.yaml` / `weathercom.jinja`), and Home Assistant
> must be **reloaded**, _before_ you start Py-Weather v0.3.0. Py-Weather reads each
> station's online/offline state from `sensor.wu_obstimeutc_<key>` — if that sensor
> and its backing macros are not in place and loaded, **every station reads
> OFFLINE** and is re-probed only once a day. Adding those sensors/macros to
> `rest.yaml` / `weathercom.jinja` is an **operator change to your own HA config**;
> Py-Weather never edits that config and does **not** define any sensors — it only
> refreshes existing ones.

Each station's representative sensor must already exist in your Home Assistant
config as `sensor.wu_obstimeutc_<key>` (the `sensor.wu_` namespace, **not** the
registry `sensor.rest_wu_*` `unique_id` form) before Py-Weather can refresh it.
For back-compat, a `sensor.wu_temp_<key>` representative is **still accepted** by
the validator (its metric segment is generic), but new configs should use
`sensor.wu_obstimeutc_<key>` so the representative is also the sensor Py-Weather
reads for health.

## Safe rollout

Raise the REST resources' built-in `scan_interval` **last**, only after Py-Weather
is confirmed driving the sensors — otherwise the REST platform's own timer stops
refreshing them while nothing has taken over, and they can go stale for up to the
long interval (e.g. 24h):

1. Confirm the REST sensors exist and are refreshing on their normal built-in
   `scan_interval` (e.g. `300`).
2. Install and configure Py-Weather with your stations.
3. Start Py-Weather and confirm from its logs that it is polling and earning a
   learned cadence on online stations — i.e. it is actually driving the sensors.
4. **Only then** raise the REST resources' built-in `scan_interval` to a long
   value (e.g. `86400`) so Py-Weather, not the REST platform's fixed timer, drives
   the cadence.
5. **Rollback:** if you stop or remove Py-Weather, lower `scan_interval` back to a
   normal value so the REST platform resumes refreshing on its own; otherwise
   those sensors go stale (up to the long interval, e.g. 24h) with nothing driving
   them.

## Auto-populate stations

When `stations:` is left empty the add-on discovers its fleet automatically at
startup by reading `/states` from the Supervisor Core-API proxy and matching
every entity whose id has the form `sensor.wu_obstimeutc_<key>`, where `<key>` is
bare lowercase-alphanumeric. This covers the typical case where the REST
sensors already exist in Home Assistant and you simply want the add-on to start
polling without a manual configuration step.

**What happens at startup:**

1. The add-on reads `/states` (up to 5 attempts, spaced by `settle_seconds`,
   to allow for a brief host-boot lag while REST sensors finish loading).
2. It performs a confirmation re-read and takes the per-key maximum
   `expected_sensors` across both reads so a still-loading sibling set is not
   snapshotted short.
3. It writes the discovered list back to its own add-on options via the
   Supervisor API so subsequent restarts find an explicit `stations:` and skip
   discovery. If that write fails, a paste-ready `stations:` YAML block is
   logged at WARNING so you can copy it into the Configuration tab manually.
4. The session then runs off the discovered list exactly as if you had typed
   it by hand.

**Non-conforming entities** — a `sensor.wu_obstimeutc_*` whose id suffix contains
an underscore or uppercase character (e.g. `sensor.wu_obstimeutc_back_yard`) is
excluded from auto-discovery with a WARNING that names the entity and the expected
rename target (`sensor.wu_obstimeutc_backyard`). Rename the underlying sensor's
entity id if you want it included.

**If no entities are found** the add-on exits with an error and a hint to check
`rest.yaml` or populate `stations:` manually.

Once `stations:` is populated (whether by auto-populate or by hand) the
discovery path is skipped entirely on subsequent restarts and the add-on runs
in plain manual mode.

## Configuration

| Option                    | Default | Meaning                                                                                                                                             |
| ------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_backoff_seconds`     | `86400` | Terminal holding cadence (the slow hold for a non-retryable fault).                                                                                 |
| `settle_seconds`          | `15`    | Wait before each station's `/states` read, and the spacing between the startup discovery attempts (1-300).                                          |
| `startup_stagger_seconds` | `10`    | Delay between each station's first poll at launch (1-300).                                                                                          |
| `request_timeout_seconds` | `30`    | Per-call Core-API proxy timeout (1-300).                                                                                                            |
| `log_level`               | `info`  | `debug` / `info` / `warning` / `error`.                                                                                                             |
| `stations`                | `[]`    | Stations to poll: `key`, `update_entity`, `expected_sensors` per row. Leave empty to auto-populate from existing `sensor.wu_obstimeutc_*` entities. |

Each station row:

- **`key`** — the lowercase-alphanumeric station id (`^[a-z0-9]+$`), e.g.
  `istation01`. It is the entity-id suffix interpolated into both the
  `update_entity` check and the sensor discovery.
- **`update_entity`** — the representative sensor for this row's `key`.
  Recommended: `sensor.wu_obstimeutc_<key>` (the sensor Py-Weather reads for the
  station's online/offline state). A `sensor.wu_temp_<key>` representative is
  **still accepted** for back-compat — the validator's metric segment is generic,
  so existing rows need not change — but new rows should use `obstimeutc`. The
  registry `sensor.rest_wu_*` form and a wrong-key copy-paste are both rejected at
  validation time.
- **`expected_sensors`** — a positive integer; the station's normal sensor count.
  A discovered count below it is **logged as a soft signal**, never on its own a
  reason to mark the station unhealthy.

## Health model

A station is **online** or **offline** — the model is binary, keyed on whether
Weather.com has fresh observation data behind the REST resource, not on whether
any one sensor's value changed. The single signal is the representative
`sensor.wu_obstimeutc_<key>`: **online** when that sensor is present and its state
parses as an ISO-8601 timestamp; **offline** when it is absent, `unavailable`,
`unknown`, `none`, the empty string, or an unparseable value. A Weather.com `204`
collapses the entire REST resource, so the presence of a parseable `obsTimeUtc`
alone captures online-vs-offline. An individually-unavailable other metric (e.g.
`uv` going `unavailable` overnight) or a discovered count short of
`expected_sensors` is **non-fatal** — it never makes a station offline.

**Why `obsTimeUtc`, not freshness of the refresh.** `homeassistant.update_entity`
forces the REST platform to re-fetch, but its return cannot prove Weather.com
actually accepted a new upload — the call succeeds even when the station is dead
and the resource returns stale or empty data. The state's own timestamps
(`last_reported`/`last_updated`) advance on every HA-side write regardless, so
they cannot distinguish a live upload from a no-op refresh. The `obsTimeUtc` value
**carried in the data** is the only field that moves when, and only when, the
station genuinely uploaded a new observation — so it is the real online/offline
signal. (A present-but-frozen `obsTimeUtc` still reads online; it just stops
advancing the learned cadence — see below.)

### Learned cadence

Each station's poll interval is **learned per-station** from its own observed
upload rhythm. On every online poll the read `obsTimeUtc` is recorded (only when
it differs from the last recorded one, so a frozen value does not pollute the
window). From the recorded events the gaps between successive observations are
computed; the next interval is `clamp(median(gaps) × 0.8, min_interval_seconds,
1800)` with **±15% jitter** applied, so a fleet does not resynchronize into a
thundering herd. The `× 0.8` factor polls a little faster than the observed period
so a fresh upload is rarely missed; the **floor** is the `min_interval_seconds`
knob (default `300`) and the **ceiling** is a fixed `1800s`.

**Cold start:** until a station has at least **two** recorded observations there is
no measurable gap, so it is polled at the floor (`min_interval_seconds`) until it
has earned a real cadence.

**`min_interval_seconds`** (default `300`, range `60–1800`) is the only cadence
tuning knob: it is the floor for the learned interval, bounding WU API load even
for a fast (e.g. 1-minute) uploader. The v0.2 knobs `healthy_interval_min`,
`healthy_interval_max`, and `initial_backoff_seconds` are **removed in v0.3.0** —
superseded by the learned cadence and `min_interval_seconds`.

### Offline re-probe

An offline station is re-probed once a day (`OFFLINE_REPROBE`, `86400s`), not at
the learned cadence — a dead station should not be hammered. The cost of this is a
recovery latency: a station that comes back online is noticed on its next daily
re-probe, so recovery can take **up to ~24h**. This is an accepted trade-off
against re-probing dead stations every few minutes.

### Cadence persistence

The per-station learned windows are persisted to `/data`
(`/data/pyweather-cadence.json`) and **survive add-on restarts**, so a fresh boot
resumes each station's learned cadence instead of cold-starting the whole fleet.
The save is best-effort and debounced (an `OSError` — disk-full, permission,
`/data` unavailable — is logged and swallowed, never crashing a poll). The load is
**tolerant**: any corruption (bad JSON, wrong shape, unknown version) degrades to
an empty state rather than raising, so a clobbered `/data` file can never crash
boot — the fleet simply cold-starts.

## Operations

Py-Weather keeps **no diagnostic entities** — it logs to the add-on **Log tab**.
It persists only its learned per-station cadence windows to `/data` (see
[Cadence persistence](#cadence-persistence)); a corrupt or missing file degrades
to a clean cold start. Log levels:

- **`info`** — startup, station registration, recovery, and major interval
  transitions.
- **`warning`** — a station offline, cadence changes, and response-parse failures
  (a non-JSON or schema-invalid `/states` body).
- **`error`** — terminal config/token faults (these hold at `max_backoff_seconds`
  and do not retry tightly).
- **`debug`** — routine online polls.

## License

[MIT](../LICENSE).

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg

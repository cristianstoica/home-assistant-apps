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
representative sensor (`sensor.wu_temp_<key>`), waits a short settle window, reads
`/states`, and judges the station's **freshness** and **health**. The cadence then
adapts:

- **Confirmed** (the representative sensor's `last_reported`, or a degrade-safe
  `last_updated`/`last_changed`, advanced past the pre-refresh instant) → schedule
  the next poll at a **random fast interval** in the healthy window.
- **Inconclusive** (the poll succeeded and the required-core sensors are usable,
  but on an older Core the fallback timestamp did not advance) → **accept** the
  poll and **hold** the slow cadence — never rewarded with the fast interval, so a
  masked outage cannot masquerade as healthy.
- **Transient unhealthy** (a missing/unusable required-core sensor, a stale
  primary timestamp, a timeout, a `5xx`, a `429`, or a malformed `/states` body) →
  **exponential backoff**, doubling from the initial backoff up to the cap.
- **Terminal** (a `401`/`403`, or a non-`429` `4xx` on `update_entity` or
  `/states` — a bad token or a misconfigured target) → logged at `error` and
  **held at the maximum backoff**, never spun in a tight retry loop, so it
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
   `sensor.wu_temp_*` entities already exist in Home Assistant you can leave
   **Stations** empty — the add-on will discover them automatically on first
   start (see [Auto-populate](#auto-populate-stations)). Otherwise add one row
   per station manually, then **Start**.

## Prerequisite: the REST sensors

Py-Weather does **not** define any sensors — it refreshes existing ones. Each
station's representative sensor must already exist in your Home Assistant config
as `sensor.wu_temp_<key>` (the `sensor.wu_` namespace, **not** the registry
`sensor.rest_wu_*` `unique_id` form) before Py-Weather can refresh it.

## Safe rollout

Raise the REST resources' built-in `scan_interval` **last**, only after Py-Weather
is confirmed driving the sensors — otherwise the REST platform's own timer stops
refreshing them while nothing has taken over, and they can go stale for up to the
long interval (e.g. 24h):

1. Confirm the REST sensors exist and are refreshing on their normal built-in
   `scan_interval` (e.g. `300`).
2. Install and configure Py-Weather with your stations.
3. Start Py-Weather and confirm from its logs that it is polling and earning a
   healthy/confirmed cadence — i.e. it is actually driving the sensors.
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
every entity whose id has the form `sensor.wu_temp_<key>`, where `<key>` is
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

**Non-conforming entities** — a `sensor.wu_temp_*` whose id suffix contains an
underscore or uppercase character (e.g. `sensor.wu_temp_back_yard`) is excluded
from auto-discovery with a WARNING that names the entity and the expected rename
target (`sensor.wu_temp_backyard`). Rename the underlying sensor's entity id if
you want it included.

**If no entities are found** the add-on exits with an error and a hint to check
`rest.yaml` or populate `stations:` manually.

Once `stations:` is populated (whether by auto-populate or by hand) the
discovery path is skipped entirely on subsequent restarts and the add-on runs
in plain manual mode.

## Configuration

| Option                    | Default | Meaning                                                                                                                                       |
| ------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `healthy_interval_min`    | `300`   | Lower bound of the fast cadence window (seconds, 60-86400).                                                                                   |
| `healthy_interval_max`    | `400`   | Upper bound of the fast cadence window; must be `>= min`.                                                                                     |
| `initial_backoff_seconds` | `300`   | Cold-start / inconclusive holding cadence and the base of backoff (first retry is `* 2`).                                                     |
| `max_backoff_seconds`     | `86400` | Backoff cap and the terminal holding cadence; must be `>= initial`.                                                                           |
| `settle_seconds`          | `15`    | Wait before the first `/states` read and the spacing between freshness re-reads (1-300).                                                      |
| `startup_stagger_seconds` | `10`    | Delay between each station's first poll at launch (1-300).                                                                                    |
| `request_timeout_seconds` | `30`    | Per-call Core-API proxy timeout (1-300).                                                                                                      |
| `log_level`               | `info`  | `debug` / `info` / `warning` / `error`.                                                                                                       |
| `stations`                | `[]`    | Stations to poll: `key`, `update_entity`, `expected_sensors` per row. Leave empty to auto-populate from existing `sensor.wu_temp_*` entities. |

Each station row:

- **`key`** — the lowercase-alphanumeric station id (`^[a-z0-9]+$`), e.g.
  `istation01`. It is the entity-id suffix interpolated into both the
  `update_entity` check and the sensor discovery.
- **`update_entity`** — the representative sensor, which must be
  `sensor.wu_temp_<key>` for this row's `key`. The registry `sensor.rest_wu_*`
  form and a wrong-key copy-paste are both rejected at validation time.
- **`expected_sensors`** — a positive integer; the station's normal sensor count.
  A discovered count below it is **logged as a soft signal**, never on its own a
  reason to mark the station unhealthy.

## Health model

A station is healthy only when the **required-core** subset — `temp`, `humidity`,
`pressure` — is each present and usable **and** the freshness check passes. An
individually-unavailable **optional** metric (e.g. `uv` going `unavailable`
overnight) or a discovered count short of `expected_sensors` is **non-fatal**:
only a missing/unusable required-core sensor or a failed freshness check makes a
station unhealthy. Unusable states are `unavailable`, `unknown`, `none`, and the
empty string.

Freshness prefers the representative sensor's `last_reported` (HA 2024.8+), which
advances on **every** state write — so an unchanged temperature (normal between
polls) still counts as a successful refresh. When `last_reported` is absent or
`null` (older Core, or a serialization quirk), it degrades safely to
`last_updated`/`last_changed`; an unchanged fallback timestamp on an
otherwise-successful poll is accepted as inconclusive rather than misread as an
outage.

## Operations

Py-Weather keeps **no diagnostic entities** and **no persisted state** — it logs
to the add-on **Log tab** and starts cold on every restart (a crash loop cannot
re-hammer stations that were correctly held slow; a healthy station re-earns its
fast cadence on its first confirmed poll). Log levels:

- **`info`** — startup, station registration, recovery, and major interval
  transitions.
- **`warning`** — a station unavailable, backoff changes, and response-parse
  failures (a non-JSON or schema-invalid `/states` body).
- **`error`** — terminal config/token faults (these hold at the slow cadence and
  do not enter backoff).
- **`debug`** — routine healthy polls.

## License

[MIT](../LICENSE).

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg

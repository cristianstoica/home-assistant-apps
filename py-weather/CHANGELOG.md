# Changelog

## 0.4.1 — republish 0.4.x image tag and guard release version drift

- **Bump the add-on manifest to `0.4.1`** so the builder publishes a fresh
  versioned GHCR image tag. The previous `0.4.0` manifest was present in the
  repository, but the corresponding `ghcr.io/cristianstoica/py-weather:0.4.0`
  image tag was not published, causing Supervisor updates to fail at image
  resolution time.
- **Synchronize `pyweather.__version__` with `config.yaml`.** It was still
  `0.3.0` after the `0.4.0` manifest bump.
- **Add a release-version self-check** so `python3 -m pyweather --check` fails if
  the Python package version and add-on manifest version drift again.

## 0.4.0 — remove expected_sensors/discovered; tighten schema and logs

- **Removed `expected_sensors` config key.** The per-station `expected_sensors`
  field introduced in v0.2.0 for discovery counting is no longer used by the
  v0.3.0 obstime-only health model and has been deleted end-to-end (schema,
  options, code). A v0.3.0 config blob that still carries the key validates
  clean — unknown keys are ignored by the schema validator.
- **Removed `discovered` health-result field.** The internal `discovered` flag
  on health results was a discovery-era artefact with no role in the current
  binary online/offline model; removed end-to-end.
- **Deleted non-fatal scheduler shortfall log.** A log line emitted when a
  poll cycle ran long was noise in normal operation and has been removed.
- **Station-merge simplified to key-union dedup.** The merge helper now
  deduplicates station lists by key union only, dropping the `expected_sensors`
  count-comparison logic that no longer applies.
- **`translations/en.yaml` rewritten to v0.4.0 schema.** The UI translation
  file now reflects the actual configuration options; the `expected_sensors`
  entry and other stale fields have been removed.
- **No behavior change.** Health, cadence, and persistence logic are unaffected.
  This is a schema and log hygiene release.

## 0.3.0 — data-presence health check + auto-learned poll cadence

> **Prerequisite before upgrading:** add a `sensor.wu_obstimeutc_<key>` REST
> sensor to Home Assistant for every station and reload the REST integration
> **before** installing this version. Without it every station reads as offline
> immediately on startup.

- **Binary online/offline health.** Replaced the previous freshness-based
  health check (which compared sensor timestamps to a pre-poll snapshot) with
  a direct read of each station's `sensor.wu_obstimeutc_<key>` entity. A
  station is online when that sensor holds a recent UTC timestamp; offline when
  the value is absent or stale. The old model falsely backed off stations whose
  Weather.com uploader was slow to publish even when the station itself was
  working.
- **Auto-learned poll cadence.** Each station's poll interval is derived from
  the gap between consecutive `obsTimeUtc` values, so stations that upload
  every 5 minutes are polled more frequently than stations that upload every
  30 minutes. The learned interval is clamped to
  `[min_interval_seconds, 1800]` with ±15 % jitter to spread load.
- **Cadence persisted across restarts.** The learned interval for each station
  is written to `/data/cadence.json` and reloaded on startup, so the add-on
  converges immediately rather than re-learning from cold on every restart.
- **New `min_interval_seconds` option** (default `300`, valid `60`–`1800`).
  Sets the floor for the learned poll interval. Operators with fast-uploading
  stations can lower this; those on metered API plans can raise it.
- **Offline daily re-probe unchanged.** A station that reads offline is
  re-checked once a day (governed by `max_backoff_seconds`, default `86400`).
  Terminal config/token faults hold at that same interval.

## 0.2.0 — auto-populate stations from existing `sensor.wu_temp_*` entities

- **Auto-discover station fleet.** When `stations:` is left empty at startup,
  the add-on reads `/states` from the Supervisor Core-API proxy, matches every
  `sensor.wu_temp_<key>` entity whose suffix is bare lowercase-alphanumeric, and
  builds the station list from those matches. Discovery is bounded (up to 5
  attempts with a settle wait between each) so a brief host-boot lag — where the
  REST sensors have not finished loading — does not produce a false empty fleet.
- **Count stabilisation.** After the first successful scan the add-on performs a
  confirmation re-read and takes the per-key maximum `expected_sensors` across
  both reads, so a sibling set that was still loading at first-scan time does not
  get snapshotted short.
- **Best-effort persistence.** The discovered list is written back to the add-on's
  own options via `POST /addons/self/options` so subsequent restarts find an
  explicit `stations:` and skip discovery. If the persist call fails the add-on
  logs a paste-ready `stations:` YAML block at WARNING so the operator can copy it
  into the Configuration tab manually, then continues the session off the
  discovered list.
- **Non-conforming entities skipped, not rejected.** A `sensor.wu_temp_*` whose
  id suffix contains an underscore or uppercase character (e.g.
  `sensor.wu_temp_back_yard`) is excluded from auto-discovery with a WARNING
  naming the entity and the expected rename target; it does not abort the run.
- **Manual mode unchanged.** When `stations:` is non-empty, the auto-discover and
  persist paths are skipped entirely; the add-on runs as before.
- **New manifest grant.** `hassio_api: true` is now required in the manifest (it
  was not present in earlier releases). This grants write access to
  `POST /addons/self/options` for the persistence step; it does NOT grant any
  elevated Supervisor role.
- **Expanded `--check` coverage.** The offline oracle now covers the full
  discovery and persistence paths: the pure `discover_stations`/`merge_station_counts`
  transform, the `SupervisorSelfClient.set_options` body and header contract, the
  persist-failure paste-block fallback, the cap-exhaustion error variants, and the
  SIGTERM-during-settle shutdown path.

## 0.1.1 — clarify scan_interval rollout ordering; add icon; expand config-validation tests

- **Safe rollout doc.** Clarified the correct `scan_interval` migration order in the
  README: raise the REST sensor `scan_interval` to a long hold value only _after_
  py-weather is confirmed driving the sensors, not before. Added a rollback note
  covering how to return control to the REST integration if needed.
- **Add-on icon.** Added `icon.png` so the add-on displays with the shared family
  icon in the HA store UI.
- **Expanded config-validation test coverage.** The offline `--check` oracle now
  covers additional `config.validate` rejection cases: `max_backoff_seconds` below 60
  with `initial_backoff_seconds` in range, and `max_backoff_seconds` alone below 60,
  both pinning the exact field the validator names.

## 0.1.0 — initial release: adaptive Weather.com PWS poller

First release of **Py-Weather**, a stdlib-only Home Assistant add-on that
adaptively polls Weather.com PWS REST sensors.

- **Adaptive per-station scheduler.** For each configured station, forces a
  refresh of one representative sensor (`sensor.wu_temp_<key>`) via
  `homeassistant.update_entity` through the Supervisor Core-API proxy
  (`http://supervisor/core/api/...`, `homeassistant_api: true`), then judges the
  station's freshness and health from `GET /states` and adapts its cadence.
- **Reward / backoff split.** A positively-confirmed poll earns a randomized fast
  interval (`healthy_interval_min`..`healthy_interval_max`) and resets backoff; an
  inconclusive-but-accepted poll holds the slow cadence (never the fast reward); a
  transient-unhealthy poll backs off exponentially (`initial_backoff_seconds * 2`,
  doubling to `max_backoff_seconds`); a terminal config/token fault holds at
  `max_backoff_seconds` without entering the doubling loop.
- **Freshness contract.** Prefers the representative sensor's `last_reported`
  advancing past a pre-refresh UTC instant (so an identical-value write still
  counts fresh), degrading safely to `last_updated`/`last_changed` on older Core;
  bounded best-effort re-reads (up to 2, spaced through the single stop-aware
  sleeper) let a slow render settle before the station is judged.
- **Health predicate.** The required-core subset (`temp`, `humidity`, `pressure`)
  must be present and usable; an individually-unavailable optional metric (e.g.
  `uv`) or a discovered count short of `expected_sensors` is logged but non-fatal.
- **Error taxonomy.** `401`/`403` and non-`429` `4xx` on either call are terminal;
  transport failures, timeouts, `5xx`, `429` (on every call, including the
  `update_entity` POST), and malformed `/states` bodies are transient. A `429`
  takes precedence over the `4xx`-on-`update_entity` terminal rule.
- **Restart-safe by design.** No persisted state — every restart is a cold start
  at the slow holding cadence, so a crash loop cannot re-hammer stations; a
  healthy station re-earns its fast cadence on its first confirmed poll. All sleeps
  are interruptible, so SIGTERM exits promptly.
- **Secret-safe.** Holds no Weather.com credentials (the REST integration owns
  external access); the only secret is the Supervisor bearer token, which is read
  from the environment and never logged.
- **Offline self-test.** `python3 -m pyweather --check` runs an all-pass oracle
  over the validator, request shaping, health/freshness evaluation, the
  reward/backoff split, the error taxonomy, and the interruptible waits — no
  network and no live token.

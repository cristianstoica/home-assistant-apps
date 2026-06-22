# Changelog

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

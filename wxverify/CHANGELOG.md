# Changelog

## 0.8.0

- Fixed: importing a large database no longer fails at HA ingress's 16 MiB
  request-size cap. The add-on now streams the upload instead of buffering
  the whole request in memory, so an offline-edited database can be
  re-imported up to the add-on's existing 256 MiB import limit.
- Changed: Ops → Database Export now prepares the snapshot and then streams
  it (begin → poll status → download) instead of building and sending it in
  a single blocking request. First-attempt exports of a large database no
  longer time out waiting for the snapshot to finish. The previous
  `GET /api/export/db` endpoint has been removed.

## 0.7.1

- Fixed: far-horizon forecast tiles (6–7 days out) could show an identical
  daily high and low — a single collapsed value — with only one point in the
  hourly view, when the top-ranked feed supplied just one sample at that
  range. Feed selection now prefers feeds with enough hourly coverage (≥12
  hours) to form a real daily high/low, falling back gracefully at the very
  edge of the forecast range.

## 0.7.0

- Changed: wind consensus now uses a 90th-percentile estimator across all
  reporting stations instead of median + outlier filtering. Exposed or gusty
  stations that read genuinely higher wind speeds were previously discarded
  as statistical outliers; they are now counted toward the consensus value.
- Fixed: the Meteoblue feed now explicitly requests km/h, °C, and mm units
  from the provider instead of relying on its (locale-dependent) default,
  preventing a unit mismatch in wind readings.
- Temperature and precipitation consensus are unchanged in this release.

## 0.6.0

- Added: Ops → Database Export downloads a consistent snapshot of the live
  database (`VACUUM INTO`), safe to take while the worker is running.
- Added: Ops → Database Import uploads a previously exported `.db` file and
  fully replaces the live database with it, so an operator can edit values
  offline and re-import instead of waiting on an in-app data migration. The
  upload is validated (integrity check, schema version, required tables)
  before anything is swapped, the current database is automatically backed
  up to `/data/wxverify-<timestamp>Z.db.bak` first, and the swap happens
  in-process (WAL-safe reopen) with no add-on restart. After a successful
  import, consensus observations, forecast pairs, and cached scores are
  rebuilt in the background.
- Both endpoints are operator-only, reached the same way as the rest of the
  add-on UI (HA ingress auth), and the existing same-origin/CSRF mutation
  guard applies to the import upload as it does to every other write.

## 0.5.0

- Added: a new Forecast landing page (replaces the previous dashboard
  redirect at `/`) showing 8 day tiles, Today through +7. Each tile blends
  the top-N best-verifying feeds per weather variable per day, computed from
  existing forecast data with no new fetching.
- Added: `forecast_blend_depth` option (default 2, range 1-6) controlling how
  many top feeds are blended per variable/day.
- Added: hourly HTMX drill-down per day tile, with a per-feed spread toggle.
- Added: a minimum-coverage guard plus stale/partial badges shown when feed
  data for a tile is incomplete.
- Added: the forecast page auto-polls roughly every 5 minutes and leaves any
  open day detail untouched when nothing has changed.
- Changed: navigation now puts Forecast first, ahead of Dashboard/Sites/Ops.

## 0.4.2

- Fixed: dashboard loads no longer stall behind a slow Composite recompute.
  Composite scoring for the rolling/all-time windows is now served from the
  persisted score cache (stale-while-revalidate) instead of being recomputed
  live on every request. The previous live recompute could take up to ~16s
  and, because it ran on the single serialized database reader connection,
  blocked every other dashboard read for that same span. A stale cache entry
  is now served immediately while a rescore is enqueued in the background;
  custom day-window queries are unaffected and continue to compute live.
- No config or API response-shape changes: the `/api/composite` response
  contract (a bare JSON list) is unchanged.

## 0.4.1

- Fixed: the skill chart and dark theme no longer revert to old styles after an
  add-on upgrade. Static assets are now served from a versioned path
  (`/static/0.4.1/…`) so Home Assistant fetches fresh files on each release
  instead of serving the cached copy from the previous version.
- Changed: lead-day labels now show plain words only ("Today", "Tomorrow",
  "+2 days", …). The redundant `D+N` codes alongside each label have been
  removed.
- Changed: the weather-data attribution in the page footer is now a single
  combined line instead of one `<span>` per provider.
- Fixed: horizontal tables now scroll smoothly on iOS (momentum scrolling
  re-enabled via `-webkit-overflow-scrolling: touch`).

## 0.4.0

- Web UI overhaul aimed at non-expert operators. The dashboard now leads with a
  plain-language verdict, and the station leaderboard is ranked with everyday
  explainers instead of raw metrics.
- The skill curve is labelled in words and its curve API was restructured to
  serve the labelled representation.
- HA-native theming: the interface now follows Home Assistant's light/dark theme
  automatically.
- Responsive layouts for phone-width screens, with navigation corrected for the
  Ingress-served path.

## 0.3.2

- Add `backup: cold` — the Supervisor now stops the add-on while taking a
  backup, so the WAL SQLite database is snapshotted consistently (a hot
  backup could capture a mid-commit db/-wal pair that fails at restore).
- Fix: `python -m wxverify --version` now reports the add-on version (was
  frozen at 0.1.0). The package, project, and add-on versions are synced
  and a regression test pins them together.
- Docs: README Monitoring section now documents the actual supervision
  model — the Watchdog toggle gates both crash-restart and Docker-HEALTHCHECK-
  unhealthy restart; turning it off disables all Supervisor restarts
  including crash recovery.

## 0.3.1

- Fix: request decimal precision (`numericPrecision=decimal`) on current-observation
  and hourly/7-day observation fetches from the Weather.com PWS API — previously
  temperature, dew point, and wind values were integer-rounded (history-range fetch
  already had this parameter).
- Add: best-effort Supervisor discovery publish at startup — the add-on posts its
  host and port to the HA Supervisor discovery endpoint so a companion integration
  can locate it without manual configuration. Fail-open: any HTTP error, unexpected
  status, or absent `SUPERVISOR_TOKEN` (standalone/dev) is logged and startup
  continues normally.

## 0.3.0

- New: wxverify now runs its own adaptive per-station current-observation poller
  for Weather.com PWS stations, absorbing the upload-cadence tracking and the
  online / offline / transient / terminal state machine formerly provided by the
  separate **py-weather** add-on. wxverify is now the sole caller for these
  stations; py-weather is deprecated (see its README for migration notes).
- New route: `GET /api/observations/current` — returns the latest stored
  observation per station in native units, together with per-station poll
  diagnostics (state, last-seen timestamp, learned cadence). A companion Home
  Assistant integration can surface these as HA entities using this route.
- New config options: `min_interval_seconds` (floor for the per-station learned
  poll interval, default `300`), `max_backoff_seconds` (terminal holding cadence,
  default `86400`), `request_timeout_seconds` (per-request Weather.com timeout,
  default `30`), and `weathercom_daily_call_limit` (daily Weather.com API call
  cap, default `3000`, sized above natural poll volume for typical fleets; the
  existing budget guard remains the hard backstop).
- Schema upgraded to v3: adds per-station poll-state and latest-observation
  tables; migration runs automatically on start with no operator action required.
- Unchanged: the native hourly-history stream and scoring continue to consume
  Weather.com's native hourly aggregates; current-observation samples are stored
  separately and never feed scoring.

## 0.2.1

- Revised logging levels throughout with a documented four-level policy
  (ERROR/WARNING/INFO/DEBUG). DEBUG now traces all feed fetches, DB, queue,
  scheduler, backfill, and catchup operations, and scoring; per-job INFO cycle
  lines, service start/stop, and scoring-run milestones are emitted at INFO.
  API keys in request URLs are redacted from log output at every level. The
  `## Logging` section in the README documents the policy. No change to weather
  data, scoring, monitoring, or the HTTP API.

## 0.2.0

- New: `GET /api/health/monitor` verdict endpoint — returns a structured JSON
  envelope grouping health conditions into four categories: `pipeline`
  (observation staleness, worker liveness), `budget` (per-provider API call
  reservations), `db` (SQLite readability), and `monitor` (monitor-subsystem
  self-check). The overall status is `ok`, `degraded`, or `unknown`; individual
  condition groups carry `ok`/`degraded`/`unknown` verdicts with structured
  detail payloads.
- New: three operator toggles in add-on options — `monitor_pipeline` (bool,
  default true), `monitor_budget` (bool, default true), `monitor_db` (bool,
  default true) — allow selectively disabling condition groups that are not
  relevant to a given deployment.
- New: HA monitoring package documented in README — a ready-made `rest`
  sensor polling `/api/health/monitor` plus a `degraded` binary sensor, and
  two automations (notify on degraded, auto-clear on recovery) — so operators
  can surface add-on health inside Home Assistant without custom scripting.

## 0.1.2

- Fixed: static assets (CSS/JS) returning 404 under Home Assistant Ingress —
  `IngressPathMiddleware` now restores the ASGI `root_path`/`path` invariant so
  `StaticFiles` resolves correctly behind the Supervisor ingress proxy; the
  dashboard was unstyled in 0.1.1.

## 0.1.1

- Fixed: Web UI now loads correctly through Home Assistant Ingress — the URL
  prefix is taken from the Supervisor proxy's `X-Ingress-Path` header on every
  request instead of a boot-time value that could be empty; a stale CSRF cookie
  left by 0.1.0 is cleaned up automatically on upgrade.
- Fixed: the add-on is no longer restarted by the Supervisor watchdog during
  long scoring passes — the container healthcheck tolerates busy periods,
  scoring runs in smaller per-phase transactions, and the persistence baseline
  is rebuilt incrementally (~30× faster) with a new database index speeding up
  score aggregation.
- Fixed: worker failures now appear in the add-on log (warnings on retried
  jobs, errors on permanent failures, notices for deferrals and provider
  backoffs), with API keys — including OpenWeatherMap's `appid` — redacted from
  all messages.
- Fixed: connection-level fetch failures (DNS failure, connection refused,
  connect timeout) no longer consume API call budgets — the reservation is
  refunded when the request provably never reached the provider.

## 0.1.0

First public release as a Home Assistant add-on.

- De-nested from standalone private repo into the public add-on repository.
- Ingress-only FastAPI app with in-process async worker (single process, D9).
- SQLite state at `/data/wxverify.db`; options from `/data/options.json`.
- Forecast providers: Meteoblue, Weather.com, VisualCrossing, OpenWeatherMap,
  WeatherAPI, Meteosource, Google — all optional, activated by API key.
- JSON API: `/api/sites`, `/api/composite`, `/api/leaderboard`,
  `/api/worker/status`, `/api/health/*`.
- New endpoint: `GET /api/health/backoffs` — active domain-backoff diagnostics.
- Supervisor watchdog on `GET /api/sites` for automatic restart on failure.
- CI: `wxverify-gates` lint job (ruff + pyright strict + pytest).
- Supply-chain: SHA-pinned `find-addons` and `changed-files` CI actions.

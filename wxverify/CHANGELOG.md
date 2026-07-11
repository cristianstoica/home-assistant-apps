# Changelog

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

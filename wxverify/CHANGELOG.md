# Changelog

## 0.9.4

### Fixed

- Timestamps that could not be parsed in the job queue or station poll
  state previously caused the affected work to stall silently forever.
  Such values are now repaired when the database is opened, including
  in imported or hand-swapped databases.
- Feed fetch intervals are now validated through one shared check and
  fail closed on an unusable value, instead of risking a paid provider
  call being scheduled at an invented cadence.
- Charts now carry an accessible name, and their status regions exist
  in the page before their content updates, so screen readers announce
  them correctly.
- The version-coherence test now also checks the lockfile, so the
  package metadata, the module version, and the lockfile can no longer
  drift apart unnoticed.
- The health endpoint now reports the most recent database import
  swap.
- Fixed a version mismatch between the package metadata and the module
  that shipped in 0.9.3.

## 0.9.3

### Fixed

- A restored database that contains a value the app itself would never
  write — for example a corrupted timestamp, station cadence, or site
  reference — could previously crash the background worker on every
  scheduling pass, with no way out short of editing the database
  directly. Such rows are now read defensively and either skipped or
  treated safely by default, with a warning logged instead of a crash.
- Interrupting a database operation — for instance while shutting down
  mid-job — could previously let a second operation start against the
  same connection before the first had actually finished. This most
  visibly showed up as a "shutdown reclaim failed" warning that
  deferred job recovery to the next restart instead of completing it
  immediately. Database operations now always finish before their
  connection or lock is released, closing that race entirely.
- Database reads — page loads, the worker's own background checks, and
  chart data requests — previously queued behind one another one at a
  time. They now run concurrently against a small pool of connections,
  which should reduce wait times when several things are happening at
  once.
- Restoring a database while a worker job or a new-station request was
  still in progress could let that job's write land in the
  newly-restored database against rows that no longer meant the same
  thing, corrupting unrelated data. Such writes are now detected and
  rejected, and the affected job or request is safely abandoned
  instead.
- After restoring a database, the app rebuilds its derived scoring
  tables in the background. Previously, restarting the app while that
  rebuild was still running could leave it permanently half-finished,
  while the restored file's own leftover completion marker made the
  app trust incomplete data. The rebuild's progress is now tracked
  durably and automatically resumes on the next start if it was
  interrupted.
- Downloading a database export previously had to fit entirely in the
  browser tab's memory before any of it reached disk, which could
  crash the tab when exporting a large database on a
  memory-constrained device. In browsers that support it, the download
  now streams directly to the file you choose as it arrives; other
  browsers keep the previous behavior unchanged.
- The dashboard skill-curve chart, the overlay chart, and the
  forecast's hourly chart previously rendered only as a graphic, with
  nothing for a screen reader to read once the page loaded. Each chart
  now also shows a plain-language summary and a data table with the
  same numbers, visible to every visitor.
- A forecast feed whose endpoint kept failing could retry on the
  standard backoff schedule fast enough to burn through a metered
  provider's small daily call budget well before that feed's own poll
  interval would normally allow. Retries for a persistently-failing
  feed are now spaced no closer together than the feed's own poll
  interval, after an initial fast retry that still recovers quickly
  from a one-off blip.

## 0.9.2

### Fixed

- A `jobs` row that could not be read back — a corrupt JSON payload, a
  payload that parsed but was not a JSON object, or a BLOB stored in
  `retry_count`/`max_retries` — was previously raised straight out of the
  claim path with nothing to catch it, stopping the worker process. Such a
  row is now dispositioned to `failed` in the same transaction as the claim,
  so an unreadable row costs one job instead of the worker.
- A job that keeps failing was re-enqueued on every scheduler tick, and a
  failing scoring rescore was re-enqueued on every subsequent read of a
  stale cache. Scheduled fetch jobs (feed, observation, and
  current-observation polling) and post-read rescores now back off for a
  cooldown after a terminal failure — one hour for scheduled fetches,
  fifteen minutes for rescore — before being enqueued again automatically.
  Operator-triggered retries (Ops → retry, backfill, and the CLI) are
  unchanged: those still enqueue immediately, so a manual retry is never
  silently dropped by a stale cooldown.
- Polling a current-observation station reserved a Weather.com API call
  before the request was made but never refunded it on a connection
  failure, even when the request never reached the provider. The
  reservation is now refunded for that case. A malformed body on an
  otherwise successful response could also escape the poller's failure
  handling before the usual retry floor was recorded, allowing an immediate
  re-poll of a station that had just failed; it now follows the same
  delayed-retry path as a transport failure.
- Importing a database whose `variable` column holds a BLOB instead of text
  — a state `PRAGMA integrity_check` does not catch, since SQLite's type
  affinity lets a BLOB survive an insert into a TEXT column — is now
  rejected. Import validation also now runs a foreign-key check and rejects
  an upload that fails it.
- `/api/worker/status` now reports `import_rebuild_done_at`, the timestamp
  of the last post-import rebuild. The value has been recorded since import
  rebuilds were added, but the status endpoint's query listed the reported
  keys by name and never included it.

## 0.9.1

### Fixed

- The provider-health API and the ops dashboard's provider panel returned a
  server error whenever any stored forecast sample had a non-text variable
  name. A restored database can legitimately carry such a row, since the
  import validator checks integrity and table presence but not storage
  classes, and a single one of these values broke the entire request. Those
  values are now rendered in a hex-quoted form instead of failing the whole
  response.

## 0.9.0

### Added

- `/api/worker/status` now reports cumulative per-callback database read timing
  (`read_timing`, `read_timing_since`): lock wait, executor dispatch and query
  execution, with call counts, error counts and maxima. Reads taking 250 ms or
  more in any phase now log a warning.
- `/api/worker/status?counts=exact` reports exact `forecast_samples` and
  `forecast_pairs` row counts. Opt-in, so the default five-minutely poll does
  not pay for them.
- A progress bar appears at the top of the page when a full-page navigation
  takes longer than 150 ms, and respects `prefers-reduced-motion`.

### Changed

- Provider health collects per-feed sample metrics in three index-covered
  statements instead of one query per feed, removing a long read-lock hold that
  delayed other API reads.
- Win-rate scoring now selects the latest forecast per feed and valid time in SQL instead of
  fetching every reissue and reducing in Python, and resolves the active-feed set once per query
  instead of once per row. Locally measured 1.6–2.2x faster depending on forecast lead time, ~2x for
  the common case, on a 444k-row database.
- New indexes are built on first start after this update, which adds a few
  seconds to that one startup and roughly 48 MB of database size at 444k
  forecast pairs.

## 0.8.13

- Fixed: the ops dashboard reported "never run / due" for a feed whose every
  fetch attempt had failed, instead of "error" — the two health API
  endpoints already agreed on "error" for the same state, but the ops
  dashboard tested `last_run_at is None` before checking `last_error`.
  Marking a feed's fetch as failed records the error without ever setting
  `last_run_at`, so a feed that has never once completed successfully kept
  reading as "never run / due" indefinitely instead of surfacing the error.
  All three surfaces now derive status in the same order.

## 0.8.12

- Fixed: the ops dashboard and the health-feeds API endpoint each ran
  a full `COUNT(*)` over the entire forecast-samples history just to
  report whether a feed had recent data. Both now use a bound-seek
  existence probe instead, while the health endpoint continues to
  report the exact same sample count as before. The health endpoint
  is polled every five minutes by the Home Assistant integration, so
  this removes the last unbounded read on that path.

## 0.8.11

- Fixed: six hot read-path queries each bound only a prefix of their
  covering index, leaving a middle column unbound, so SQLite could
  never reach the selective range predicate that follows it in index
  order and fell back to scanning. Each query now enumerates the
  bounded candidate grid and issues one fully-bound index seek per
  candidate. Measured warm, best-of-3, on real data, with every
  rewrite asserted set-equal to the original output first:
  feed-freshness lookup ~980x, scoring-feed lookup ~1172x, composite
  ~16x, leaderboard ~8x, accuracy curve ~7x, forecast build ~4x,
  dashboard ~3x faster; the forecast tiles poll's unchanged-data
  response dropped from 1135ms to 15ms.
- Fixed: a shared site-resolution helper introduced during the above
  work caused full forecast-page builds to load the enabled-site list
  twice. It now returns early for an explicit site id and reuses the
  caller's already-loaded list otherwise; behavior is unchanged.

## 0.8.10

- Fixed: a second DB import started while one was already in progress
  could race the first, corrupting derived state. The import endpoint
  now rejects a concurrent import with `409 Conflict`; the admission
  lock is held through the background rebuild and backup sweep, not
  just until the handler returns, since two overlapping sweeps could
  otherwise delete each other's backups.
- Fixed: two fast sequential imports within the same wall-clock second
  could produce colliding backup filenames. Backup filenames now
  include a random token (`wxverify-<timestamp>-<token>Z.db.bak`); the
  sweep regex still matches the older bare-timestamp filename shape so
  backups written by earlier versions remain sweepable after upgrade.
- Added: a 900s timeout on stalled import uploads, returning `408`.

## 0.8.9

- Fixed: a job claimed by the worker when Home Assistant stops the
  add-on was left stranded in `running` status forever. The worker now
  reclaims its own in-flight job on cancellation, returning it to
  `pending` so the next start picks it back up.
- Fixed: `wxverify-<timestamp>Z.db.bak` backup files accumulated
  indefinitely with nothing ever deleting them. A new sweep now keeps
  exactly the newest valid backup and removes the rest, running at
  startup and after each import; it never prunes when the newest file
  fails a validity check, and never deletes the backup the current
  import just created. Backups are also excluded from Supervisor
  snapshot archives (`backup_exclude`).

## 0.8.8

- Fixed: leaderboard and composite reads could time out during a scoring
  rebuild. Scoring previously held one long write transaction for the
  whole rebuild, blocking reads for the duration; it is now serialized
  into short batched write transactions (24 cells per transaction), so
  reads interleave between batches instead of queuing behind a single
  long lock hold.
- Changed: route-triggered rescores are now fire-and-forget — a stale
  cached read returns immediately and the rescore runs in the
  background, instead of the request waiting on the rescore to enqueue.
- Changed: worker and scoring milestones now log at INFO instead of
  DEBUG, and all logs go to stdout, so rebuild progress is visible in
  the add-on log without raising the log level.
- Added: `scripts/bench_route_during_rebuild.py`, a bench script that
  measures route latency while a scoring rebuild is running.

## 0.8.7

- Fixed: today's forecast now covers the full local calendar day (the
  "forecast of record"). Daily aggregation for the Today tile previously
  started its window at the current hour rather than local midnight, so
  the tile under-reported the daily max and degraded to "partial" as the
  day went on even though earlier hours had already been forecast. The
  window now starts at local midnight, and elapsed hours resolve to the
  freshest forecast run that covers them.

## 0.8.6

- Fixed: leaderboard and skill-curve reads no longer stall behind a slow
  live rescore. Cache-backed leaderboard windows are now served
  stale-while-revalidate, the same as composite scoring: a stale cached
  snapshot is served immediately while a rescore is enqueued in the
  background, instead of recomputing live inside the request. A site or
  variable with no applicable input returns an empty result without
  enqueueing any work.
- Changed: the leaderboard, curve, composite, and dashboard-page rescore
  enqueues now share one cooldown-guarded helper, so a persistently
  failing scoring job cannot be re-enqueued on every poll from any of
  those routes. An enqueue failure never fails a read that already has
  rows to serve.
- No config or API response-shape changes: the leaderboard, curve, and
  composite JSON contracts are unchanged.

## 0.8.5

- Fixed: the database export's first download attempt could still be cut off
  after about 30 seconds through Home Assistant's ingress and Nabu Casa. The
  ingress proxy cancels any single long streaming response, regardless of how
  the download is started — a page fetch was cut off just the same as a
  navigation download — so the 0.8.4 fetch approach did not help. The export
  is now downloaded in sequential 4 MB chunks (roughly 17 requests for a
  64 MB export), so no single response lives long enough to be cut. Each chunk
  is validated and retried on failure, and the reassembled file's size is
  checked against the export before it is saved. If any chunk cannot be
  fetched, a direct browser-native download link is shown against the retained
  export file, preserving the resume-and-retry behaviour from 0.8.3.

## 0.8.4

- Fixed: the database export's first download attempt could still be cut off
  after about 30 seconds when opened through Home Assistant's ingress and Nabu
  Casa — a bare download link routed the transfer onto the browser's legacy
  navigation-download channel, which Cloudflare/ingress cancels at 30 seconds
  on large files. The export is now downloaded on the browser's normal fetch
  channel and saved as a local file, bypassing the legacy channel entirely,
  and the status line shows bytes received as it downloads. If the fetch fails
  for any reason, a direct browser-native download link is shown against the
  retained export file, preserving the resume-and-retry behaviour added in
  0.8.3.

## 0.8.3

- Fixed: a database export interrupted partway through download — for example
  over Home Assistant's ingress — could not be resumed. The browser's
  automatic retry failed with an "unknown export id" error, and the download
  had to be restarted from scratch, which hit the same interruption again. The
  prepared export snapshot was being deleted the moment the response finished,
  even when the download had been aborted, so the retry had nothing left to
  resume from. The prepared export is now retained, letting an interrupted
  download resume where it left off or be re-requested, and a periodic sweeper
  reclaims old export files after about an hour instead of relying on
  delete-on-download.

## 0.8.2

- Fixed: exporting a large database through Home Assistant's ingress failed
  on the first download attempt — the stream dropped partway with a
  "Connection lost" error, and a browser retry was needed to complete it.
  Export snapshots are now gzip-compressed before download, so the transfer
  is far smaller and completes on the first attempt.
- Changed: Ops → Database Import now automatically detects and decompresses
  a gzipped export, while still accepting an uncompressed `.db` file. No
  action is needed — older uncompressed exports keep importing as before.

## 0.8.1

- Fixed: Ops → Database Export failed to start with a 415 error when the
  add-on was opened through Home Assistant's ingress. The same failure
  affected other bodyless actions — catch-up, backfill, and delete — which
  ingress forwards without a declared content type. The request guard now
  checks its content-type allowlist only when a Content-Type is actually
  declared, so these bodyless requests are no longer rejected. Same-origin
  and CSRF protection are unchanged.

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

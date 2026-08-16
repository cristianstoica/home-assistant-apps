# Changelog

## 0.13.0

Adds an Ops-panel control to start a retrospective timezone correction for a
site, closing the gap for operators who have no route to a container shell.

### Added

- Ops → Timezone Correction: a panel that starts a retrospective timezone
  correction for a site directly from the browser. The panel states, per
  site, whether the action currently applies, when it does not and why,
  whether the last correction failed before it published, and whether a
  published correction's background cleanup stopped before finishing. The
  action is refused (with the domain's own message, not a generic error)
  when it does not apply, and a double-click cannot start two corrections
  for the same site.

### Fixed

- The Timezone Correction panel could report a finished cleanup as stalled:
  the check read the cleanup's two internal records separately, so it could
  catch one before the worker's last commit and the other after, and
  mistake that gap for a stall. The check now reads both records together,
  so it always sees a state the worker actually committed.

## 0.12.0

Raises the Google feed's forecast horizon from 24 hours to the add-on's
full 7-day (168-hour) horizon. The 24-hour value was a local mis-seed, not
a limit of the Google API.

### Changed

- The Google feed's configured maximum forecast lead is now 168 hours
  (7 days), matching every other feed. Existing installations are
  corrected automatically and idempotently on startup; no manual action is
  needed. The database schema is unchanged.
- The Google adapter now paginates to cover the full horizon instead of
  fetching a single 24-hour window. **This raises the Google feed's
  API cost from 1 call per fetch to 7.** If you run Google against a
  metered key, account for the higher call volume before upgrading.
- The roster and composite dashboards now disclose each feed's forecast
  horizon alongside its lead counts, so a feed's coverage is visible
  without cross-referencing configuration.

## 0.11.3

Adds an operator-facing kill switch for the nightly verification chain and
closes a gap where an imported database could carry an active verification
run past the hold.

### Added

- Ops → Nightly Verification Publishing: a control that arms or releases the
  verification publish hold. Arming is never refused, including while a
  chain is running. Releasing is refused with `409` while any site has a
  queued or running verification chain, and the state is left unchanged.
  Upgrading an existing pre-0.11.3 installation arms the hold once during
  startup, so newly scheduled verification runs are blocked until you
  review and release it; a fresh install is not held.

### Changed

- Importing a database now discards the donor's in-flight verification run
  instead of carrying it across the import. Any `verification_run` job
  still `pending` or `running` is failed with
  `suppressed: imported active verification chain`, any run left in a
  non-`published` state is failed with the same reason, and the partial
  evidence of every unpublished run is deleted. Published runs and every
  other job type are untouched.

## 0.11.2

A fix release remediating an external implementation audit of 0.11.1. No
schema change, no migration, and no wire-contract change; the methodology
version is unchanged because the fix makes the code conform to the
methodology it already declared, rather than changing it.

### Fixed

- The condition-4 baseline gate compared each required baseline against its
  own independently derived set of adequate-lead days instead of against
  the candidate's own headline days, contradicting the methodology the code
  documented. This could recommend a blend-depth change for a candidate
  that, restricted to its own headline leads, actually loses to plain
  persistence. The candidate's headline core is now threaded through and
  used for every baseline comparison in the gate.
- The published-fingerprint and attempt-cap checks are now re-evaluated
  immediately before a run starts, not only earlier in the decide phase, so
  a state change between decide and start can no longer slip past them.
- The per-lead observed-wet MAE disclosure grid was computed but never
  rendered on the Verification page; it now displays.

## 0.11.1

A remediation release for the Verification page introduced in 0.11.0. It
corrects how the backtest scores the add-on's own forecast, closes gaps in
the daily forecast-of-record log, and publishes evidence 0.11.0 described
but never produced. Nothing about how the add-on selects or displays a
forecast changes.

### Added

- A `verification_publish_hold` setting that stops the nightly
  verification chain from starting new runs. Set it with
  `wxverify settings set verification_publish_hold 1`; the hold is in
  effect only for the exact value `1`, so an absent or any other value
  leaves the chain running and a fresh install is never silently
  non-verifying. A run already under way is left to finish. Clear the hold
  by setting the same key to `0`.
- The Verification page and `GET /api/verification/status` now report what
  the nightly trigger actually did for the current cycle — started a run,
  skipped it and why, or recorded no decision at all — along with any
  starts that were superseded that night and whether the publish hold is
  on. Until now, "no published verification run for this site yet" was the
  only signal an operator got, whatever the cause.
- The per-feed diagnostics table gains a **Vs recommended** column. A feed
  below the availability floor is now compared against the blend the run
  recommends, over the days the two share, instead of being listed with no
  comparison at all; where no comparison is possible, the table gives the
  reason.
- The system health monitor gains a `record_gap_scan_degraded` condition,
  counting sites whose daily record gap scan could not assess one or more
  dates.

### Fixed

- Under Home Assistant ingress, `GET /api/verification/latest` redirected
  to a path outside the add-on's own mount, so the redirect failed. It is
  now built through the ingress prefix like every other absolute URL in
  the app. Standalone installs were unaffected.
- The all-feed average the backtest compares against was built from every
  feed on the site's pinned roster, including feeds that supplied nothing
  for the day and variable being scored. It is now built from the feeds
  actually resolved for that comparison, in a second pass after the roster
  is settled, so it is a real average of the available feeds rather than a
  diluted one.
- The backtest scored a blend of every feed that had samples, while the
  Forecast page displays a blend of only the feeds that clear its
  selection. Both now use the same set, so the score describes the
  forecast the add-on actually publishes, and the covered hours and
  contributor count reported beside each score describe that same set. The
  daily quantities kept in the forecast-of-record carried the same
  mismatch and are corrected the same way.
- A blend depth could be recommended without having been checked against
  every comparison baseline it is required to beat. The check now works
  from a fixed required list: a baseline that is absent, or that has too
  little history at a given forecast lead, fails the check and is named as
  insufficient rather than quietly skipped. For precipitation, the
  amount check and the did-it-rain check now both always run, instead of
  only whichever one happened to show an improvement.
- A forecast lead could count toward the "enough leads agree" requirement
  at leads where a required baseline had no usable history. Those leads
  are now dropped before the count, and each dropped lead records which of
  three reasons applied — too few days, too few wet or dry days, or a
  missing baseline — so a thin lead and a baseline-less lead are no longer
  reported as the same thing.
- A day's forecast-of-record counted as finished as soon as a single row
  existed for it, so a day interrupted part-way through was sealed
  incomplete and never revisited. A day is now finished only when every
  variable and every lead is present, and an incomplete day is retried
  about once an hour while its late-write window is open. A cell for which
  nothing was knowable at recording time now writes no row at all, instead
  of an empty one that would seal the day.
- The gap scan that closes missing record days only looked forward from
  the newest day already recorded, so a hole left earlier in the log was
  never revisited. It now walks the log itself, back as far as 30 days,
  and visits every day that is not provably complete.
- When the gap scan closes a day whose late-write window has passed, each
  missing entry now records why it is missing: the write was lost, or
  there was nothing to record in the first place. Every entry previously
  got the same reason, which asserted a lost write even where no forecast
  had ever been available.
- A date the gap scan could not assess used to abandon the rest of the
  scan with no trace — the job still reported success and nothing anywhere
  said a date had been skipped. Such a date is now rolled back on its own,
  the rest of the scan still commits, and the failure is recorded durably
  and counted by the system health monitor.
- A verification run could be pinned to inputs that had already moved on,
  because its fingerprint was taken when the nightly trigger decided to
  run rather than when the run started against a snapshot. The run now
  derives its fingerprint from the snapshot it stores. Where the two
  differ, the night's decision is taken again once — re-running every gate
  — and the superseded attempt is recorded, so the trail shows what
  happened.
- Two steps of the verification chain rebuilt their progress record from
  scratch instead of updating it, losing the run they belonged to. A run
  reaching either step would have cancelled itself silently, and gone on
  doing so every night after.
- 0.11.0 described a precipitation-amount error measured only on days it
  actually rained, and always displayed, but nothing computed it. The
  Verification page and the run diagnostics endpoint now show it, for the
  precipitation blend depth the run was scored under, with the number of
  days behind it and a caution when that sample is thin.
- 0.11.0 obliged the release to say so when ranking feeds day by day beats
  every blend depth in use. Nothing made that comparison. Each variable
  now carries that conclusion on the Verification page and in the verdicts
  endpoint. It is diagnostic only — a ranking basis is not something the
  add-on can act on, so the recommendation is unchanged by it.
- An empty diagnostics section on the Verification page now says why it is
  empty and which condition this run failed to meet, instead of leaving an
  operator to guess whether the section was empty or simply not built yet.
- The provenance kept with each recorded forecast cell listed only the
  feeds that supplied samples. It now lists every feed that could have
  taken part — including those configured but silent, and those that can
  never be loaded — and how each one participated, so a feed that
  contributed nothing is distinguishable from one that was never
  considered.
- Two methodology constants were published for the audit trail but wired
  to nothing. They have been removed, and every remaining published
  constant must now be either used or explicitly declared as unused.

### Notes

- **Before deploying this version**, hold the nightly verification chain:
  set `verification_publish_hold` to exactly `1`, and confirm no
  verification run is in progress. Release the hold only after you have
  confirmed the upgrade is healthy.
- Verification results from this version are not comparable with those
  from 0.11.0. The scored blend and the all-feed average have both
  changed, so scores, baselines and recommendations will move even where
  nothing about the forecasts themselves did. Runs published by 0.11.0 are
  left in place; read them as a separate series.
- A verification run now has three more phases and does more work per
  night, so nightly runs take longer than they did in 0.11.0.

## 0.11.0

### Added

- A **Verification** page, with matching read-only `/api/verification/*`
  endpoints, that backtests the add-on's own forecast product. For each past
  day it reconstructs the forecast the add-on would actually have published,
  using only what was known at that morning's decision time, then scores it
  against every individual feed, against a day-before persistence baseline,
  and against the equal-weight average of all feeds — all on the identical set
  of days, so no comparison is won on an easier sample. Daily high and daily
  low are scored separately, as are daily maximum wind, whether precipitation
  occurred, and how much fell. Where a feed or a lead has too little history
  to support a conclusion, the page says so rather than publishing a number.
  Recommendations are advisory: nothing on this page changes the add-on's
  forecast selection on its own.
- Forecast blend depth can now be set per variable, through three new
  options — `forecast_blend_depth_temperature`, `forecast_blend_depth_wind`
  and `forecast_blend_depth_precip`. Each is optional and falls back to the
  global `forecast_blend_depth` when left unset. The Forecast page shows the
  depth in effect for each variable and whether it came from the per-variable
  setting or the global one. Clearing one of these options removes the
  override and restores the global value.
- Three site-timezone commands — `wxverify timezone status`,
  `wxverify timezone correct` and `wxverify timezone change`. Correcting a
  timezone that was recorded wrongly rebuilds history under the corrected
  zone; changing a timezone because a site genuinely moved preserves the
  earlier history and applies the new zone only from an effective date
  forward. Neither happens as a side effect of an ordinary edit, so a routine
  change can no longer silently rewrite history.
- Verification runs and their results are carried through database export and
  restore.

### Changed

- **Correcting a site's timezone shifts historical leaderboard values.** This
  is expected: local days and forecast leads that had been bucketed under the
  wrong timezone are being reclassified, so the scores and rankings derived
  from them change. The corrected history is built alongside the live data and
  only replaces it once its row counts reconcile; while it runs, the site
  reports as recalculating and forecast selection keeps using the previous
  results.
- The Forecast page's precipitation percentage is now labelled **predicted
  wet-hour share** instead of chance of rain. The number never was a
  probability — it is the share of the day's forecast hours that are wet — and
  the new label says what it actually measures.
- The first start after upgrading rebuilds two database tables, including the
  forecast-pair table, and recreates their indexes. This happens once and is
  slower than a normal start, more noticeably on SD or eMMC card storage.

### Notes

- The Verification page names the diagnostics it does not produce rather than
  omitting them silently. **Wet-hour-share verification is deferred to
  methodology version 2** — version 1 declares neither the bin edges nor the
  rule reconciling the predicted share against the observed one, so no such
  evidence is published. **Split-half results have been withdrawn** from the
  page altogether: at the history depth available they could not have carried
  information.

## 0.10.1

### Fixed

- An observation timestamp near the limits of the supported date range,
  carried in a restored or imported database, could interrupt a station's
  observation poll instead of being skipped as unreadable, leaving that
  station's polling schedule stuck.
- A provider backoff counter carried at an extreme value from a restored
  or imported database could stall or fail the retry-delay calculation
  instead of holding at the one-hour maximum.
- A background job whose attempt counter was carried at an extreme value
  from a restored or imported database could stop the add-on outright when
  that job next failed, and start it stopping again after every restart;
  the counter is now brought back into range and the job simply retries or
  gives up as usual.
- A provider backoff record carrying an unreadable retry time or attempt
  count, again only from a restored or imported database, could block
  every future request to that provider with no way to recover, could fail
  the add-station request outright, could make the backoff diagnostic fail
  for every provider at once, and could be reported on the system health
  page as an active backoff the add-on was not in fact applying; such a
  record is now ignored and replaced on the next provider response, the
  diagnostic lists it with an empty attempt count instead of failing, and
  the system health count no longer includes it.

## 0.10.0

### Changed

- `GET /api/health/providers` (and `providers doctor`) now report a 7-day
  recent window per feed instead of a lifetime total: `sample_count`,
  `variables`, `model_run_count`, `latest_issued_at`, `valid_from` and
  `valid_to` are renamed to `recent_sample_count`, `recent_variables`,
  `recent_model_run_count`, `recent_latest_issued_at`, `recent_valid_from`
  and `recent_valid_to`, and each feed now also carries a
  `metrics_window_start` timestamp and a `metrics_schema` marker (currently
  `2`). `bad_sample_count`, `status`, and every other field, including
  `/api/health/feeds`' own lifetime `sample_count`, are unchanged. Any
  integration reading the six renamed fields must switch to the `recent_`
  names; a consumer that needs a true lifetime per-feed count should read
  `/api/health/feeds` instead. The first boot after upgrading builds a new
  database index and is slower than usual — seconds on fast storage,
  possibly longer on SD or eMMC card storage — and happens only once; an
  INFO log line reports when the build finishes.

### Fixed

- A timestamp near the limits of the supported date range could raise an
  unexpected error instead of being reported as unreadable, which could
  blank the monitor page, interrupt a scheduling pass, or, if it reached
  the scheduler, stop the add-on outright on every start until the stored
  value was corrected by hand.
- A job whose retry counter had reached the maximum storable value could
  not be marked failed and would be retried forever, restarting the
  worker each time.
- The stuck-job monitor now recognizes a wider range of malformed
  scheduling timestamps, so a job that cannot be scheduled is surfaced
  instead of going uncounted.

## 0.9.5

### Fixed

- Feeds that had never successfully run bypassed the fetch-interval
  validation added in 0.9.4, so a corrupt or imported interval could
  still schedule provider calls at an invented rate. The interval is
  now validated before the due decision in both the scheduler and the
  catchup path; a feed whose interval cannot be read is skipped with a
  warning and left for the operator to repair.
- Updating a feed through the API now rejects an interval above the
  30-day maximum the scheduler enforces, instead of accepting it and
  silently never applying it.
- The feed list and forecast freshness endpoints no longer fail when a
  stored interval cannot be read; they report the feed as having no
  usable interval, and forecast freshness marks it stale.
- A stored fetch interval with a fractional part is now rejected instead of
  being truncated to a whole minute, so a corrupt or imported value such as
  `1.9` is no longer silently scheduled as a 1-minute cadence. Whole-number
  intervals are unaffected.

### API

- `fetch_interval_minutes` in the `/api/feeds` response can now be
  `null` for a feed whose stored interval is unreadable, where earlier
  versions returned an error. Existing stored values above the maximum
  are left untouched — there is no migration or clamp.

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

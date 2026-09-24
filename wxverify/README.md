# Weather Verify (`wxverify`)

Weather Verify is a local Home Assistant add-on and standalone FastAPI app for
checking forecast-model skill against a cluster of Weather.com PWS stations.

It stores all state in SQLite, builds an observation consensus from enabled PWS
stations, pairs forecasts against that consensus, and serves both a JSON API and
a small HTMX/uPlot web UI.

Weather Verify is self-contained. Its web panel, HTTP API, and command-line
interface are the whole product; it registers no entities, devices, or services
with Home Assistant, so no `sensor.wxverify_*` entity appears in your entity
list unless you add one yourself. To bring its data into Home Assistant as an
entity, define a REST sensor in your own configuration, as shown under
[Home Assistant package (REST sensor + automations)](#home-assistant-package-rest-sensor--automations).

## What Operators Need

- Python 3.13 for local standalone use.
- `uv` for local dependency management.
- A Weather.com PWS API key for station validation and observation refresh.
- Optional Meteoblue API key if that forecast provider is enabled.
- One or more PWS station IDs near each verification site.
- A writable data directory for the SQLite database and options file.

Temperature values are stored and displayed in Celsius, wind in m/s, and
precipitation in mm.

## Local Standalone Start

Standalone start for local development:

```sh
# from the wxverify/ add-on directory
uv sync --frozen
uv run python -m wxverify --db .local/wxverify.db serve
```

Open:

```text
http://127.0.0.1:8099/dashboard
```

The first start creates the SQLite database automatically.

Optional overrides:

```sh
export WXV_DB_PATH="$PWD/.local/wxverify.db"
export WXV_HOST="127.0.0.1"
export WXV_PORT="8099"
```

## Local Environment Variables

Local standalone mode reads environment variables when no Home Assistant
`/data/options.json` file exists. Export these in the shell environment that
starts the app:

```sh
export WXV_WEATHERCOM_KEY="YOUR_WEATHERCOM_KEY"
export WXV_METEOBLUE_KEY=""
export WXV_VISUALCROSSING_KEY=""
export WXV_OPENWEATHERMAP_KEY=""
export WXV_WEATHERAPI_KEY=""
export WXV_METEOSOURCE_KEY=""
export WXV_GOOGLE_KEY=""
export WXV_ROLLING_WINDOW_DAYS=30
export WXV_MIN_N=30
export WXV_OBS_INTERVAL_MINUTES=180
export WXV_OBS_JITTER_MINUTES=20
export WXV_LOG_LEVEL="info"
```

Blank provider keys are treated as missing.

To verify the current shell has the variables loaded without printing secrets:

```sh
print ${+WXV_WEATHERCOM_KEY}
```

`1` means the key is set in the current shell.

## Home Assistant Add-on Configuration

When run as a Home Assistant add-on, runtime options come from
`/data/options.json` and state is stored at:

```text
/data/wxverify.db
```

The add-on config exposes these options:

| Option                 | Required         | Default | Purpose                                          |
| ---------------------- | ---------------- | ------- | ------------------------------------------------ |
| `weathercom_key`       | Yes for stations | empty   | Weather.com PWS validation and observation pulls |
| `meteoblue_key`        | No               | empty   | Meteoblue forecast provider                      |
| `visualcrossing_key`   | No               | empty   | Visual Crossing Timeline forecast provider       |
| `openweathermap_key`   | No               | empty   | OpenWeatherMap One Call forecast provider        |
| `weatherapi_key`       | No               | empty   | WeatherAPI.com forecast provider                 |
| `meteosource_key`      | No               | empty   | Meteosource forecast provider                    |
| `google_key`           | No               | empty   | Google Weather forecast provider                 |
| `rolling_window_days`  | No               | `30`    | Default rolling score window                     |
| `min_n`                | No               | `30`    | Minimum paired samples for confident scores      |
| `obs_interval_minutes` | No               | `180`   | Base observation refresh cadence                 |
| `obs_jitter_minutes`   | No               | `20`    | Per-cycle bounded jitter for PWS refreshes       |
| `log_level`            | No               | `info`  | Runtime logging level                            |

The add-on serves through Home Assistant Ingress. Do not pass an extra root path
to uvicorn; the app owns `root_path` internally.

### Forecast blend depth is owned by the options, not the database

Blend depth — how many forecast days the blend averages over — has one global
value plus an optional override per variable:

| Key                                 | Range | Effect                                     |
| ----------------------------------- | ----- | ------------------------------------------ |
| `forecast_blend_depth`              | 1–6   | Global depth, used wherever no override set |
| `forecast_blend_depth_temperature`  | 1–6   | Overrides the global depth for temperature |
| `forecast_blend_depth_wind`         | 1–6   | Overrides the global depth for wind        |
| `forecast_blend_depth_precip`       | 1–6   | Overrides the global depth for precip      |

**The three per-variable overrides belong to the add-on options, not to the
database.** Every startup re-applies the options over the stored settings: an
override present in the options is written into the `settings` table, and an
override **absent from the options is deleted from it**, dropping that variable
back to the global depth. So a per-variable depth set straight into the
database — with `settings set`, with a script, or by restoring someone else's
database — lasts only until the next restart, then silently disappears. The
options are the only place a per-variable depth survives.

That has one consequence worth spelling out, because it is easy to hit and
leaves no error behind:

> **Set the options BEFORE importing a database, not after.** Importing a
> database that carries per-variable depth overrides does not carry the options
> with it. If the destination's options do not already list those overrides, the
> next startup clears them and every variable quietly reverts to the global
> depth — scores then continue to be computed at a depth nobody chose. Put the
> overrides in the add-on configuration (or the matching `WXV_FORECAST_BLEND_DEPTH_*`
> environment variables for a standalone run) and only then run the import.

### Parse-cap probe (optional, temporary)

`parse_cap_probe` is an optional switch, off unless you set it to `true`. A
standalone run uses the environment variable `WXV_PARSE_CAP_PROBE` instead,
where `1`, `true`, `yes` or `on` switch it on.

When it is on, each fetch from Visual Crossing, Meteoblue or Meteosource writes
one log line per model, starting with `parse_cap_probe`. The line says how many
forecast hours the response held, how many of them the add-on keeps, and how
far ahead they reach.

Nothing new is stored, shown, scored or requested: the probe reads only the
response the add-on has already fetched. Its lines hold no API key, location or
forecast value.

The lines are written at `info` level, so they appear when `log_level` is
`info` or `debug`, and not when it is `warning` or `error`.

The option is temporary and will be removed in a later release.

## First-run Workflow

1. Start the app.
2. Open the Sites page.
3. Create a site with:
   - name
   - forecast latitude and longitude
   - reference elevation in meters
   - IANA timezone, for example `America/Denver`
   - rain threshold in mm
4. Add at least one Weather.com PWS station to the site.
5. Confirm provider key status on the Ops page.
6. Let the worker run. It refreshes PWS observations, materializes consensus,
   and queues scoring work when observations change.

Site creation does not require a Weather.com key. Adding a station does require
the key because the station ID is validated synchronously.

### Site Field Notes

`rain_threshold_mm` defines what counts as a rain event for precipitation
scoring.

Precipitation is scored two ways:

- as an amount error: forecast mm vs observed mm
- as an event classification: did it rain or not?

The threshold is used for the event part:

```text
rain event = hourly precip >= rain_threshold_mm
```

That lets wxverify compute metrics like POD, FAR, CSI, ETS, and HSS without
treating tiny trace or noisy values as real rain.

The default is `0.2 mm`, which is a reasonable "trace rain counts as dry" floor.
You usually do not need to change it unless your station reports noisy tiny
precip amounts or you want a stricter definition such as `1.0 mm`.

It only affects precipitation: event scoring and the Forecast page's wet-hour
count. Temperature and wind are unaffected. Changing it later recomputes precip
pairs and cached scores for that site.

`elevation_m` is the reference elevation for the verification location. wxverify
uses it when building the temperature consensus: station temperatures are
lapse-normalized to the site elevation before taking the median. That keeps a
station higher or lower than the target location from biasing the ground-truth
temperature.

Use meters above sea level. If you do not know the exact value, use a reasonable
estimate from a map, GPS, or elevation lookup. It does not need centimeter
precision, but it should not be empty or wildly wrong.

For a simple source of latitude, longitude, and elevation, use FreeMapTools
Elevation Finder:

```text
https://www.freemaptools.com/elevation-finder.htm
```

Search for the location or click the map point, then copy the latitude,
longitude, and estimated elevation in meters.

## How Verification Works

wxverify does not score forecasts against each PWS station separately. For each
site, hour, and variable, it first fuses the enabled station cluster into one
consensus observation. That single consensus value is the ground truth used for
all model scoring.

For temperature, each station reading is lapse-normalized to the site's
`elevation_m` before aggregation, using `0.0065 C/m` (`6.5 C/km`). For example,
a station 200 m higher than the site is adjusted upward by about `1.3 C` before
the cluster median is computed. Wind and precipitation are not elevation
adjusted.

After basic station QC, the consensus step rejects outliers fresh for each
`(site, variable, hour)` using median absolute deviation, then stores the median
of the surviving readings in `observations`. Rejection is per-hour and
per-variable; a station rejected for one bad hour can still contribute normally
on the next hour. The stored `n_stations` and `rejected_stations` values are
diagnostics for that consensus row.

The Ops page has a station-trust diagnostic that compares each station with the
consensus over time. It is informational only and does not change scoring.

Forecasts use one configured query point per site: `forecast_lat` and
`forecast_lon`. Every forecast feed uses that same point so model comparisons
are fair. The app still makes separate provider calls per effective feed:
Open-Meteo feeds are fetched per model, and Meteoblue is fetched as one
multimodel package that expands into member-model samples. Observation refreshes
fetch each enabled PWS station independently.

Scoring pairs each model forecast with the one consensus observation for the
same site, variable, and valid hour. Metrics are grouped by site, feed, variable,
day-ahead lead bucket, and rolling window. A model can therefore rank well for
day-1 temperature and poorly for day-5 precipitation; those are separate score
cells.

## Forecast Horizon

Two limits apply here, and they are not the same number.

**Request horizon — per feed.** Every feed row carries its own
`max_lead_hours`, and that value is what the fetcher asks the provider for. For
Open-Meteo, `config.OPEN_METEO_MAX_LEAD_HOURS` maps each model to its horizon:
`ecmwf_ifs`, `gfs_global`, `gem_global` and `jma_gsm` are requested to the
ceiling `config.DISPLAY_REQUEST_HOURS` (`217`); `icon_global` to `180` and
`ukmo_global_deterministic_10km` to `168`, each model's longest-run maximum;
and `meteofrance_arpege_world` stays at `168` to preserve its existing scoring
eligibility. The request length counts from the hour of the fetch, but a stored
lead counts from the estimated issue time, so the model's advertised duration
of about four days does not by itself justify lowering its `max_lead_hours`,
which also bounds scoring. No feed's horizon is lowered in 0.16.0. Adding
a model means adding an entry to that mapping — the fresh-database seed and
the one-shot correction applied to existing databases both read it, so the two
cannot drift apart. Meteoblue is unchanged: its seed stays at `168` and its
package data is filtered to that feed's `max_lead_hours`. Open-Meteo historical
backfill still stores previous-run day-ahead leads from day 1 through day 7.

`DISPLAY_REQUEST_HOURS` is `217`, and it is a display figure, not a scoring
one. The product displays `forecast.service.DAY_COUNT` days; the request has to
cover one day beyond that, because the displayed span rolls over at local
midnight while each feed refreshes only on its own fetch interval, and one hour
beyond that, because a fall-back daylight-saving transition adds an hour to a
local day.

**Scoring ceiling — calendar-day buckets.** Pairing does not cut at an hour
count. It admits issuance-relative local calendar-day buckets `0` through `7`:
the local day a forecast was issued, plus the next seven. Within that, a sample
still has to fall inside its own feed's `max_lead_hours`. The ceiling is stated
in buckets and never in hours on purpose, because a bucket is a difference of
local calendar dates: eight local days span 191 to 193 elapsed hours, depending
on whether a daylight-saving transition falls inside the window. Any fixed hour
count would be wrong on one side or the other.

Raising a feed's `max_lead_hours` extends both its request window and its
scoring admission bound, but scoring still stops at the calendar-day buckets
described just above. For a feed now at `217`, scoring can admit additional
leads above `168`, up to that bound and subject to those buckets, and the
display can fill its last day as far as the model actually returns usable
hours: a longer request improves last-day coverage but does not guarantee a
complete day. `icon_global`, raised to `180`, gains at most leads 169-180 for
scoring, and its last displayed day can still be incomplete, because its
request can end before that day ends. Both feeds kept at `168` are unchanged.

Actual stored coverage can still be shorter than a feed's own request horizon,
when the provider or a member model returns less — some regional Meteoblue
member models stop at 72, 96, 120, or 144 hours. There is no single global
limit to compare them against: each feed's `max_lead_hours` is the reference.

## Web UI

Main pages:

```text
/dashboard
/sites
/ops
/overlay
/verification
```

The UI uses CSRF-protected HTMX JSON actions. Mutating actions are not plain HTML
forms; if a session token expires after a restart, reload the page and retry.

### Dashboard Guide

The Dashboard answers: which forecast feed is best for this site, variable,
time window, and lead time?

Top controls:

- Site pills choose the verification site.
- `Last N days` / `All time` chooses the scoring window. `Last N days` uses the
  `rolling_window_days` setting.
- `Temperature` / `Precipitation` / `Wind` chooses the variable.
- The lead control chooses the day-ahead bucket, labelled by word — `Today`,
  `Tomorrow`, then `+2 days` through `+7 days` — with the `D+n` code shown small
  beside each. `Today` is same-day, `Tomorrow` is next-day, and so on through 7
  days ahead.

Dashboard panels:

- `Best forecast` (top card) names, in plain words, the single best feed for the
  current site, variable, window, and lead, with its runner-up and how many
  verified forecasts back it. When the top two are too close to separate it says
  so instead, and when no feed beats its baseline it adds that caveat.
- `Leaderboard` is the main ranking for the selected site, variable, window,
  and lead. `Samples` is the number of matched forecast-vs-observation pairs.
  `Skill` is a `0-100` badge: temperature and wind use skill against the
  persistence baseline, while precipitation uses ETS for rain-event
  classification; a warn-coloured badge means the feed scored at or below its
  baseline. `MAE` and `RMSE` are error magnitudes where lower is better. A feed
  is ranked — and given a rank number — only once it has both enough verified
  pairs (`n >= min_n`, default `30`) and a skill score that can actually be
  computed; sample count alone is not enough, and feeds that fall short are
  withheld with the reason shown in place of a score. The best-scoring ranked
  feed is the one to trust; use `MAE`, `RMSE`, and `Samples` to judge how large
  the errors were and how much data supports the score.
- `Skill Curve` plots a separate line per feed across the day-ahead buckets
  (`Today` through `+7 days`), with the lead axis labelled in words. Use it to
  see how each feed's skill changes as the forecast lead gets longer.
- `Win Rate` counts comparable valid-hour cells where a feed was closest to the
  consensus. `Covered` is how many cells the feed covered. `Rate` is the share
  of comparable cells it won; ties are split fractionally. This is different
  from skill because a model can win many hours but still have worse RMSE if its
  misses are large.
- `Composite` is an overall read-side score across available variables and lead
  buckets for the selected site/window. Negative skill components are floored at
  `0`, then available components are averaged and ranked.

Virtual feeds can appear beside provider feeds:

- `Persistence` is the baseline feed. For each lead, it predicts that the
  future hour will equal the observed consensus from the same lead time ago. For
  example, a 6-hour persistence forecast for 12:00 uses the observed value from
  06:00. Temperature and wind skill are measured against this baseline; a model
  worse than persistence is shown as `0 below baseline`.
- `Multimodel Mean` is a synthetic competitor built by averaging active real
  model forecasts for the same site, variable, issued time, valid time, and
  lead. It is created only when at least two active real models contribute. It
  is not an external provider call.

### Forecast page: daily high, low and rainfall

A Forecast tile shows a temperature `High / Low` only when at least one of the
feeds behind it covers every hour of that local day, from local midnight to the
next local midnight — 24 hourly values, or 23 or 25 on a daylight-saving change
day. Each feed is checked on its own. Two feeds that each cover part of the day
do not add up to a covered day, because a feed's high and low only describe the
hours that feed supplied. Among the feeds that qualify, the usual skill ranking
and blend depth decide which ones are blended.

When no feed covers the whole day, the row reads
`Daily high/low unavailable — partial coverage` instead of a number. A high and
low worked out from part of a day is never shown under the `High / Low` label.
Everything else stays as it was:

- The day's hourly chart still plots every selected feed, including feeds that
  cover only part of the day.
- The `partial` badge keeps its meaning: for temperature, wind or rain, none
  of the selected feeds covers at least 18 hours of that day. It can appear
  with or without the unavailable label, because it answers a different
  question.
- `Wind max` is not affected by this rule. `Rain` has a stricter rule of its
  own, described below.

The `Today` tile normally shows a high and low too. Adding a site's station
starts a setup backfill, which fetches earlier forecast runs of every
subscribed Open-Meteo feed up to the start of the hour the backfill began, and
regular fetches supply the rest of the day from the hour they run. So once the
backfill has finished, today's elapsed hours are already there. A feed can
lack some of today's hours, and then cannot cover `Today`, when:

- the setup backfill has not finished yet;
- the feed comes from a provider other than Open-Meteo and was first fetched
  today: those providers have no earlier runs to fetch, so the feed may lack
  today's earliest hours (a feed already fetched before today normally has
  them);
- it is an Open-Meteo feed subscribed after the backfill finished, which the
  backfill does not go back for (a catch-up, from the `Catch up` button on the
  Ops page or the `catchup` command, may fetch its history);
- the provider returned an hour without a value, or an hour fell after the
  backfill's end and before the feed's first regular fetch.

The label appears only when none of the feeds with forecasts for that day
covers it, so a feed added later never blanks a day another feed already
covers. Regular fetches never fill the hours before their own forecast run: a
fetch keeps only hours at least an hour after its forecast run was issued, and
Open-Meteo's regular fetches start at the hour they run. So when no feed covers
today, `Today` shows the label for the rest of the date, unless the backfill or
a catch-up fills the gap, or a later fetch returns a value for an hour an
earlier one left without one. At local midnight the next date becomes `Today`,
and it shows a high and low only if a feed covers that whole date: the change
of day fills in no missing hours.

The `Rain` row has a stricter rule, because a daily total and a count of wet
hours are only right if each hour is counted exactly once. A feed counts
towards them only when it supplies exactly one value for every hour of the
local day — 24, or 23 or 25 on a daylight-saving change day. A missing hour,
an hour given twice, or a value that falls between two hours rules the feed
out. Each feed is checked on its own, and among the feeds that qualify, the
usual skill ranking and blend depth decide which ones are used. The reasons
above why a feed can lack some of `Today`'s hours apply to rain too.

The row then shows the day's rainfall and its number of wet hours, each
averaged across those feeds, for example `3.1 mm · 5 h`. The rainfall is shown
to one decimal place and the wet hours are rounded to a whole number. An hour
is wet when its forecast rain is at or above the site's `rain_threshold_mm`;
exactly at the threshold counts, and rain need not fall for the whole hour. A
rain-cloud symbol follows when the row shows `6 h` or more. The rain bars on
the day's hourly chart come from the same feeds, so they add up to the tile's
total before it is rounded for display; `Show individual feeds` still shows
every selected feed.

When feeds have rain forecasts for the day but none qualifies, the row reads
`Daily rainfall unavailable — partial coverage` and the hourly chart has no
rain bars. A `—` in the row means there are no rain forecasts for that day at
all.

A tile's `low confidence`, `ranking updating` and `stale` badges also cover the
feeds its daily high and low come from and the feeds its daily rain figures
come from, which can differ from the selected feeds its hourly chart plots.

The daily forecast record stores what the tile showed: when the label appears,
the recorded `high_c` and `low_c` are empty (`null`) and `extrema_coverage` is
`insufficient`, next to the `extrema_feed_ids` that were used (an empty list in
that case) and `extrema_low_confidence` (also `null`, because there is no set of
feeds to judge). For rain it records `total_mm` and `wet_hours` (the average
before rounding) with the same three fields. When the rainfall label appears,
both are `null`, never `0`, and the three fields read `insufficient`, an empty
list and `null`. For rain, the record's `hourly_values` are the chart's rain
bars: for each time, the average of the feeds the daily figures come from,
`null` where one of them has no value, and `null` throughout when no feed
qualifies. Rain records written before this rule have no `extrema_feed_ids`:
they store `chance` (a 0-to-1 share of wet hours) instead of `wet_hours`, and
their `hourly_values` average whichever selected feeds had a value at each
time. Verification scoring is unchanged — this decides what is displayed, not
what is scored.

## CLI

All CLI commands use the same SQLite database path:

```sh
python -m wxverify --db /path/to/wxverify.db <command>
```

Available commands:

```sh
python -m wxverify --db /path/to/wxverify.db serve --options /path/to/options.json
python -m wxverify --db /path/to/wxverify.db fetch <site_id> <feed_id>
python -m wxverify --db /path/to/wxverify.db score [--site-id <site_id>]
python -m wxverify --db /path/to/wxverify.db backfill <site_id>
python -m wxverify --db /path/to/wxverify.db catchup
python -m wxverify --db /path/to/wxverify.db settings list
python -m wxverify --db /path/to/wxverify.db settings get <key>
python -m wxverify --db /path/to/wxverify.db settings set <key> <value>
python -m wxverify --db /path/to/wxverify.db sources set-cap <source> --daily-call-limit <n>
python -m wxverify --db /path/to/wxverify.db providers doctor --site-id <site_id>
python -m wxverify --db /path/to/wxverify.db providers reconcile
python -m wxverify --db /path/to/wxverify.db providers enable --site-id <site_id> --all-new
python -m wxverify --db /path/to/wxverify.db providers fetch --site-id <site_id> --source visualcrossing
python -m wxverify --db /path/to/wxverify.db providers smoke --site-id <site_id> --all-new
python -m wxverify --db /path/to/wxverify.db timezone status [--site-id <site_id>] [--json]
python -m wxverify --db /path/to/wxverify.db timezone correct --site-id <site_id> --timezone <IANA> [--json]
python -m wxverify --db /path/to/wxverify.db timezone change --site-id <site_id> --timezone <IANA> --effective-from <ISO-8601 UTC> [--json]
```

Changing `rolling_window_days` through the settings path invalidates old cached
scores. Other settings are plain runtime knobs.

Provider operations are local admin commands. `doctor`, `reconcile`,
`enable`, `disable`, and enqueue-only `providers fetch` do not call external
forecast APIs. `providers fetch --run-now` and `providers smoke` perform live
provider calls and consume provider budget.

`providers reconcile` is safe to run against a live database when seed catalog
rows are missing; it inserts missing `sources` and `feeds` rows without
overwriting edited caps or feed settings. New or changed Python adapter code
still requires restarting the app process so the running interpreter loads that
code.

## Timezone corrections

A site's timezone is **not** an ordinary editable field. `PUT /api/sites/{id}`
has no timezone attribute and the Sites page cannot change one, on purpose: the
timezone decides which observations fall on which local day, so changing it
changes every daily truth row, every daily verification result, and every
leaderboard value derived from them. A normal edit must never silently rewrite
history, so the two ways to change a timezone are separate, deliberately
obvious commands.

Every change produces a numbered **generation**. Exactly one generation per
site is *published* at a time, and readers keep serving the published one until
a new generation is complete.

```sh
# Read-only: every generation for every site (or one site), with its
# reconciliation counts.
python -m wxverify --db /data/wxverify.db timezone status --site-id 7
```

```text
site=7 generation=1 published=yes timezone=UTC mode=initial state=published
  effective_from=- effective_to=- published_at=2026-08-01T09:12:04Z
  examined=- changed=- unchanged=- excluded=-
site=7 generation=2 published=no timezone=America/Denver mode=retrospective_correction state=building
  effective_from=- effective_to=- published_at=-
  examined=612 changed=118 unchanged=494 excluded=0
```

`--json` prints the same rows as a JSON document for scripting. A correction is
reconciled when `examined == changed + unchanged + excluded`.

### Which command

**`timezone correct` — the stored timezone was always wrong.**

```sh
python -m wxverify --db /data/wxverify.db timezone correct \
  --site-id 7 --timezone America/Denver
```

This starts a **retrospective correction**: a new generation is built
*alongside* the live one, re-bucketing and rescoring the entire timezone-derived
history, and only flips to published when the rebuild finishes. Until then
readers keep serving the previous generation, so nothing goes blank mid-rebuild.

> **A retrospective correction rewrites history.** Published leaderboard and
> verification values for past days will change once the rebuilt generation is
> published — a day that scored one way under the old zone can score
> differently under the new one. That is the intended effect of fixing a wrong
> timezone; it is not a display glitch, and it is not reversible except by
> running another correction back.

**`timezone change` — the site genuinely moved to a different zone on a date.**

```sh
python -m wxverify --db /data/wxverify.db timezone change \
  --site-id 7 --timezone America/Denver --effective-from 2026-09-01T00:00:00Z
```

This applies a **prospective change**: the previous generation is closed at that
instant and the new one takes over from it. History before the instant keeps its
own generation and is not rebuilt.

Both commands exit non-zero with a one-line `error …` message (no traceback) if
the site is unknown, the timezone is not a valid IANA name, the instant does not
parse, or a correction is already building for that site.

### Correction runbook

Order matters. Run these steps in sequence:

1. **Ship** the release that contains the timezone-aware code and migration,
   and let the add-on start normally.
2. **Correct** the site: `timezone correct --site-id 7 --timezone America/Denver`.
3. **Verify the counts** with `timezone status --site-id 7`, repeating until the
   building generation reports `examined == changed + unchanged + excluded`.
   Do not move on while the numbers are still climbing.
4. **The generation activates** on its own: when the rebuild is reconciled the
   published pointer flips to it, and `status` then shows `published=yes`
   against the new generation. This is the moment historical values change.
5. **The forecast-of-record log begins** after the flip, so every appended entry
   is stamped against the corrected generation from its first row.

## Operational Notes

- SQLite runs in WAL mode.
- All writes are serialized through one writer connection.
- The observation refresh window is six hours in steady state and widens
  automatically, up to the provider's seven-day limit, to cover a gap in stored
  coverage. It is not a setting.
- A station for which the provider returns no data, or no reading from the last
  six hours, is retried on a growing delay, from one hour doubling to
  twenty-four hours, while the site's other stations keep being fetched. The
  health monitor (`/api/health/monitor`) flags the station until it reports a
  recent reading again. Older hours it does return are still stored and used.
- Weather.com PWS calls are budgeted per enabled station.
- A site with no enabled stations is not observation-due and advances neither
  `last_obs_cycle_at` (cycle completion) nor `last_obs_at` (data freshness).
- Forecast and observation provider keys are never stored in the database.
- `/api/health/keys` reports only present or absent, never secret values.
- Audit queries against `verification_trigger_decisions` must select
  `MAX(id)` per `(site_id, trigger_date)`: a retried trigger appends a new
  decision row for the same site and day rather than updating the old one,
  so the highest `id` is the decision that actually stood.

## Verification Publish Hold

The publish hold is the kill switch for the nightly verification chain. While it
is held, no **new** verification run is started. It does not stop a run that is
already queued or under way — that chain continues and may publish — so read the
active-chain badge before you rely on the hold.

Ops → Nightly Verification Run Hold is the control. It shows whether the hold
is held or released, whether a chain is currently active, and when the state last
changed and from where (Ops or Bootstrap). The button arms or releases the hold:
it asks for confirmation in the browser, then sends
`PUT /api/verification/publish-hold` and rewrites the badges, the button, and the
last-changed line from what that call returns — no manual reload. Arming reloads
the page once, so the banner is written by the server rather than restated in the
browser. While the hold is on, `/ops` and `/verification` also carry a banner
saying so.

- **Arming is never refused.** You can hold publishing at any time, including
  while a chain is running.
- **Releasing is refused with `409` while any site has a queued or running
  verification chain**, and the state is left unchanged. Wait for the chain to
  finish, then click again.

Upgrading an existing pre-0.11.3 installation arms the hold once during startup.
This blocks newly scheduled verification runs until you review and release it.
A run already queued or under way may still publish, which is why deployment
requires confirming that no job is pending or running first. A fresh install is
not held. Once released, this 0.11.3 bootstrap decision is preserved across
subsequent 0.11.3 restarts.

### Emergency fallback: the CLI

For when the Ops control itself is unreachable — a broken toggle, a template or
route regression, or a downgrade to a version that honours the hold but has no
control for it. Releasing is the direction this fallback exists for: a
downgraded 0.11.2 honours an armed hold and has no control to clear it. The
command writes the same `settings` row the Ops control writes, but it bypasses
the last-transition record, which will keep describing the older transition.

On 0.11.3 the scheduler picks the change up on its next tick, with no restart.
**On 0.11.2 it does not:** the skip decision already recorded for that day stops
the scheduler before it re-reads the key, so a release there only takes effect
at the next nightly trigger (02:00 local time) — up to about a day later. Plan
the window accordingly if you are releasing on a downgraded instance.

The command runs **inside the add-on's own container**, against that container's
own `/data/wxverify.db`:

```sh
python3 -m wxverify --db /data/wxverify.db settings set verification_publish_hold 0
```

`0` releases the hold, `1` arms it. There is no `wxverify` executable in the
image — `python3 -m wxverify` is the invocation the service itself uses — and
`--db` is a global option that must come before the `settings` subcommand.

Getting a shell there is the part to plan in advance. The *Advanced SSH & Web
Terminal* add-on is a **different** container: the wxverify package is not
importable in it and its `/data` is a different volume. From there, find the
wxverify container first, then run the command inside it:

```sh
docker ps --filter name=wxverify --format '{{.Names}}'
docker exec <name-from-the-line-above> \
    python3 -m wxverify --db /data/wxverify.db settings set verification_publish_hold 0
```

Two links in that chain cannot be answered from this repository. Confirm both
once, in advance, rather than during an incident:

- **The container name.** The Supervisor composes it from the repository the
  add-on was installed from plus the slug — `addon_local_wxverify` for a local
  install, `addon_<repository-id>_wxverify` for one added by URL. Only the slug,
  `wxverify`, is fixed here, which is why the `docker ps` line is a required
  discovery step and not decoration.
- **Whether the SSH add-on can reach Docker at all.** That generally requires its
  *Protection mode* to be off. Check that `docker ps` returns output before you
  need this procedure.

If the wxverify container is restart-looping, `docker exec` may not land at all.
The fallback then is the add-on *Restore from backup* path, or reinstalling a
known-good version.

## Database Export and Import

Ops → Database Export downloads a consistent snapshot of the add-on database
as a timestamped `.db` file. The snapshot is taken with `VACUUM INTO`, so it is
a standalone, checkpointed copy — safe to take while the worker is running.

Ops → Database Import uploads a previously exported `.db` file and **fully
replaces** the live database with it. Any data collected since that export is
lost. The upload is validated first (integrity check, wxverify schema version,
required tables, that each of the add-on's own tables present in the file is
an ordinary table, and that every forecast and observation time is in the
add-on's own UTC form, `YYYY-MM-DDTHH:MM:SSZ`), and the current database is
automatically backed up to `/data/wxverify-<timestamp>-<id>Z.db.bak` before
the swap. Only the newest `.bak` file is kept; older ones are swept
automatically after each import and on every add-on startup, so the operator
never needs to remove them by hand. After a successful import the add-on
rebuilds consensus observations, forecast pairs, and cached scores in the
background — no restart is needed. Import needs SQLite 3.37 or newer. The
add-on image ships a newer SQLite, so this matters only for local standalone
use. There, on an older SQLite, every import is refused before the add-on
starts processing the uploaded file (reading, checking or swapping it in), the
current database is left untouched, and the message names the version it
found.

**An import discards any verification run that was in progress in the donor.**
Its `verification_run` job is failed with `suppressed: imported active
verification chain`, any run still marked running is failed with the same
reason, and the partial evidence of every unpublished run is deleted. Published
runs, and every other job type, are untouched.

**Before importing, set the per-variable forecast blend depths in the
destination's options** — an imported database's overrides are cleared at the
next startup if the destination's options do not list them. See
[Forecast blend depth is owned by the options, not the database](#forecast-blend-depth-is-owned-by-the-options-not-the-database).

To restore a backup manually: stop the add-on, copy the chosen `.bak` file
over `/data/wxverify.db`, delete any `wxverify.db-wal` or `wxverify.db-shm`
file beside it, and start the add-on again.

## Logging

The add-on writes structured log lines to the add-on log (Settings → Add-ons → Weather
Verify → Log). All log output goes to stdout, so the add-on log pane (or `docker logs`)
sees everything the add-on emits. Each line is prefixed with a timestamp, level, and the
component that emitted it, e.g.:

```text
2026-07-10T14:03:11+0000 INFO wxverify.worker.processor cycle: job=42 type=fetch_feed site=1 outcome=completed elapsed=0.4s
```

The four levels, loudest to quietest:

- **ERROR — act now.** Something failed and will not fix itself: the worker crashed, a job
  gave up after exhausting its retries, a forecast or observation fetch failed permanently,
  or the add-on could not write to its database. If you see ERROR, the add-on needs you.
- **WARNING — notice, but it handled itself.** The add-on hit a snag and recovered or is
  degrading gracefully: a provider asked it to back off (rate limit), a job failed once and
  will retry, a feed provider is temporarily unavailable, a fetch was skipped because the
  daily API budget was used up, or a fetch came back with no usable samples. Nothing to do
  unless WARNINGs are constant.
- **INFO — it's working, here's the heartbeat.** The default level. On startup, a single
  `logging configured level=… stream=stdout` line confirms the active level. From then on
  you'll see the worker start and stop, a `job claimed …` line when the worker picks up a
  job, one `cycle: …` line each time it finishes a unit of work (naming the job, its
  outcome — completed, deferred, retry, or failed — and how long it took, `elapsed=…`),
  the scoring milestones (`score phase=…`, `score discovery …`, `score window=…`, and
  `score sweep …`, each with its own elapsed time), and one `scoring run complete …` line
  per scoring run. If these keep ticking over, the add-on is alive and doing its job.
  Finer-grained per-batch and per-request detail stays at DEBUG.
- **DEBUG — show me literally everything.** The full firehose: every forecast fetch, every
  observation fetch, every scoring phase, every queue and worker transition, every backfill
  and catch-up step, every database transaction, and the raw HTTP requests and responses.
  Use this when you're diagnosing a specific problem; it is very verbose.

### Setting the level

Set `log_level` in the add-on configuration (Settings → Add-ons → Weather Verify →
Configuration) to one of `error`, `warning`, `info` (default), or `debug`, then restart the
add-on. The chosen level applies to the running service **and** to any one-shot command you
run inside the add-on container (for example the CLI `fetch`, `score`, `backfill`, and
`catchup` commands) — so `log_level: debug` gives you the full trace for a manual command
too, not just the background worker.

### API keys are never logged

Forecast and observation providers are called with your API keys in the request URL. Every
log line that could contain a URL — including the raw HTTP request lines at `debug` — has
its secret query parameters stripped before it is written, so keys are replaced with a
redaction marker — which appears in the log as `%2A%2A%2A`, the URL-encoded form of `***`:

```text
2026-07-10T14:03:11+0000 DEBUG httpx HTTP Request: GET https://api.example.com/v1/forecast?key=%2A%2A%2A "HTTP/1.1 200 OK"
```

This means a `debug` log is safe to copy into a bug report or share for support without
leaking credentials.

## Monitoring

As a Home Assistant add-on, wxverify's **process supervision** is the
Supervisor's Watchdog, gated by the add-on's **Watchdog toggle** in the HA UI.
With the toggle on, the Supervisor restarts the add-on on either of two
signals: a clean crash (the worker exits and the container halts), or the
Docker `HEALTHCHECK` (in `Dockerfile`, probing `/api/sites`) reporting the
container unhealthy — a deliberately lax envelope (60 s interval × 10 retries,
so ~10-11 minutes to trip). With the toggle off, neither triggers a restart: a
crashed worker stays halted and data collection stops silently.

The generous healthcheck envelope is deliberate: the five-minute start period
and the ten consecutive failed probes it takes to mark the container unhealthy
are meant to tolerate temporary response delays. The cost is the ~10-11 minute
detection window for an app that is genuinely wedged. Turning the Watchdog toggle off is an emergency stopgap
only — it disables all Supervisor restarts, including crash recovery.

**Proactive alerting** is HA-native. The add-on exposes a read-only verdict
endpoint, `GET /api/health/monitor`. It runs pipeline (group 1), budget
(group 2), and DB-integrity (group 4) threshold checks against its own database,
and one always-on `process` group that queries nothing at all — it reports facts
about the running add-on itself, currently just whether the background export
cleanup has stopped. It returns a structured verdict (`overall` = `ok` /
`warning` / `critical`, plus per-condition detail), and always responds `200`
with a verdict body — even on a database read error, which surfaces as
`db_readable:false` / `overall:critical` rather than an HTTP failure. Each of the
three database groups can be turned off via the `monitor_pipeline`,
`monitor_budget`, and `monitor_db` options; a disabled group runs no queries and
its conditions report `skipped`. The `process` group has no toggle — it runs no
queries, so switching it off could only hide a fault, never save any work.

Home Assistant owns the poll loop and delivery: a **REST sensor** polls
`/api/health/monitor` on the internal add-on network, and two **automations**
send a persistent notification plus a mobile push when the verdict degrades and
clear the notification on recovery. If the add-on is down, the REST sensor goes
`unavailable` — that is the "add-on not responding" signal. It covers a crash
with the Watchdog toggle off (the add-on stays halted), startup/migration
failures that abort before any request is served, and the window while the
Supervisor's Watchdog restarts the add-on after a trip. A brief `unavailable`
that clears on its own is consistent with a Watchdog-triggered restart —
confirm in the Supervisor log, which shows
a `Watchdog found app Weather Verify ...` line.
The runtime health routes `/api/health/*` and
`/api/worker/status` remain available for ad-hoc inspection.
`GET /api/health/feeds` returns one row per site and provider feed, each with
its `status`; Meteoblue's member models are reported under its package row. By
default each row carries `sample_count`, an exact count of every sample stored
for that site and feed, which gets slower to compute as history grows. Add
`?include_sample_count=false` for a lighter check: each row then carries a
boolean `has_samples` in place of `sample_count`, and every other field, every
`status` value and the row order are the same as in the default response. The
default response is unchanged from 0.15.0. `/api/worker/status`
also carries `read_cache_warm` — the read cache's own report of its most recent
warm (`state`, `at`, `detail`, `derivations_failed`), or `null` before any warm
has run. Read `state` together with `at`, never on its own: one slot is shared by
the warm at startup and the warm after every publish, so a `running` whose `at`
predates the last publish is a warm that never finished, while a recent
`running` is simply one still in progress.

### Home Assistant package (REST sensor + automations)

Paste these into your HA configuration to poll the verdict endpoint and alert
when it degrades.

**Resolve the add-on host.** The add-on is reachable from HA core over the
internal Docker network at `http://<repo>-wxverify:8099/api/health/monitor`,
where `<repo>` depends on the install method: a repo hash for a store install
from `github.com/cristianstoica/home-assistant-apps` (e.g.
`http://3283fh-wxverify:8099/...`), or `http://local-wxverify:8099/...` for a
local/dev install. The literal `<repo>` prefix cannot be derived from the repo
alone — confirm it once from an HA terminal: the sensor must return `200` with a
verdict body at the resolved host.

**REST sensor:**

```yaml
rest:
  - resource: http://<repo>-wxverify:8099/api/health/monitor
    scan_interval: 300  # seconds — primary load dial; raise to poll less often
    sensor:
      - name: "wxverify monitor"
        unique_id: wxverify_monitor
        value_template: "{{ value_json.overall }}"
        json_attributes:
          - conditions
          - grace_active
          - generated_at
```

**Automation — degraded** (sends a persistent notification plus a mobile push;
repoint `notify.mobile_app_<your_device>` to your app's entity):

```yaml
automation:
  - alias: "wxverify degraded"
    trigger:
      - platform: state
        entity_id: sensor.wxverify_monitor
    condition:
      - condition: template
        value_template: >
          {{ states('sensor.wxverify_monitor') not in ('ok', 'unavailable', 'unknown') }}
    action:
      - variables:
          tripped: >
            {{ state_attr('sensor.wxverify_monitor', 'conditions')
               | selectattr('ok', 'equalto', false)
               | selectattr('skipped', 'equalto', false)
               | map(attribute='id') | list | join(', ') }}
      - service: persistent_notification.create
        data:
          notification_id: wxverify_monitor
          title: "wxverify: {{ states('sensor.wxverify_monitor') }}"
          message: "Tripped: {{ tripped }}"
      - service: notify.mobile_app_<your_device>
        data:
          title: "wxverify: {{ states('sensor.wxverify_monitor') }}"
          message: "Tripped: {{ tripped }}"
```

**Automation — recovered** (clears the notification on return to `ok`):

```yaml
  - alias: "wxverify recovered"
    trigger:
      - platform: state
        entity_id: sensor.wxverify_monitor
        to: "ok"
    action:
      - service: persistent_notification.dismiss
        data:
          notification_id: wxverify_monitor
```

## API Call Budget

Steady-state provider usage depends on the number of enabled sites, enabled
stations, enabled forecast feeds, and fetch cadence.

With the default cadences, each enabled site makes:

- Weather.com PWS: one call per enabled station per observation cycle. With the
  default `obs_interval_minutes=180` and up to `obs_jitter_minutes=20`, that is
  roughly `7.2` to `8` cycles per day.
- Open-Meteo forecasts: each enabled Open-Meteo model is polled every `360`
  minutes (`4` calls per day), except `gem_global`, polled every `720` minutes
  (`2` calls per day). Polling follows each model's run cadence but is not
  aligned to publication, so fetching every published run is not guaranteed.
- Meteoblue: one multimodel package call every `360` minutes, or `4` calls per
  enabled site per day. The current package costs `16000` credits per call.

The run a forward Open-Meteo forecast is labelled with, and the time it is
recorded as issued, are estimated from the time of the fetch: the add-on
subtracts a flat `90`-minute availability lag and rounds the result down to the
model's run cadence in UTC, to `00`, `06`, `12` or `18` UTC, or to `00` or `12`
UTC for `gem_global`. No run identifier is read from the provider, and nothing
confirms which provider run a stored forecast came from. `gem_global`'s
estimated labels are now twelve-hourly, matching the model's twelve-hour run
schedule; forward `gem_global` forecasts stored before 0.16.0 keep their
six-hourly estimated labels, and no stored row is relabelled.

For an example deployment with one enabled site, 8 enabled stations, 7 enabled
Open-Meteo models, and the Meteoblue package enabled, the expected steady-state
use is:

| Provider        |          Expected steady-state use |
| --------------- | ---------------------------------: |
| Weather.com PWS |            about `56-64` calls/day |
| Open-Meteo      |                     `26` calls/day |
| Meteoblue       | `4` calls/day, `64000` credits/day |

The default wxverify caps are:

| Provider    |    Call cap |  Credit cap |
| ----------- | ----------: | ----------: |
| Weather.com |  `1000/day` |        none |
| Open-Meteo  | `10000/day` |        none |
| Meteoblue   |     `5/day` | `65000/day` |

For Meteoblue, one package request counts as `1` API call and currently costs
`16000` credits. The credit cap is therefore the binding limit: with the default
`65000` credit cap, at most `4` Meteoblue package calls fit in one billing day
because a fifth would require `80000` credits. The `5/day` call cap is a
secondary safety limit; the credit cap remains the practical Meteoblue
constraint.

One-time setup and recovery work can add temporary extra calls. Adding a station
uses one Weather.com validation call and one Open-Meteo elevation call. Backfill
and catchup can add Weather.com and Open-Meteo calls while filling missing
history.

## Local Checks

These are the same gates CI runs. Run them from the `wxverify/` add-on
directory, in this order:

```sh
uv lock --check
uv sync --locked
uv run --locked ruff check
uv run --locked ruff format --check
uv run --locked pyright
WXV_WEATHERCOM_KEY=ci-placeholder uv run --locked pytest
uv run --locked python -m wxverify --help
```

Run each gate unscoped, exactly as written: narrowing one to a path can make it
pass without checking anything. pyright is configured with `exclude = ["tests"]`,
so `uv run --locked pyright tests/test_daily_truth_oracles.py` prints
`0 errors, 0 warnings, 0 informations` while analyzing zero files
(`filesAnalyzed: 0` under `--outputjson`) — it type-checks nothing and reports
success. `ruff format --check` covers 227 files from the add-on directory but
only 225 when scoped to `wxverify tests`, which drops `scripts/`.

CI additionally runs the query-plan tests inside the add-on's base image with
`WXV_EQP_SHIPPING=1`, so they check the SQLite build that actually ships rather
than the local one. A green local run does not cover them. The same job also
runs the import checks that rely on SQLite's own table classification
(`PRAGMA table_list`), so they are checked against that build as well.

Cite code by its owning symbol, never by `file.py:<line>`: a line number
drifts the moment the file changes. The pytest suite includes a guard
(`tests/test_citation_anchor_guard.py`) that fails on any `file.py:<line>`
citation in a production docstring, comment, or Jinja template comment, so
write "see `_decide_precip` in `decision.py`" rather than a line reference.

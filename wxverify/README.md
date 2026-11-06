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
| `obs_jitter_minutes`   | No               | `20`    | Not used. Kept so existing configurations still load |
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
| `forecast_blend_depth_wind`         | 1–6   | Not used: wind is weighted by past accuracy. Kept so existing configurations still load |
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

> **Set the options BEFORE restoring a database, not after.** Restoring a
> database that carries per-variable depth overrides does not carry the options
> with it. If the destination's options do not already list those overrides, the
> next startup clears them and every variable quietly reverts to the global
> depth — scores then continue to be computed at a depth nobody chose. Put the
> overrides in the add-on configuration (or the matching `WXV_FORECAST_BLEND_DEPTH_*`
> environment variables for a standalone run) and only then restore the file.

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
6. Let the worker run. It collects each station's readings through the day.
   A new site's first scored day is the first full local day after it was
   created; each day is closed and scored once, after it ends.

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
count. Temperature and wind are unaffected. Changing it later affects the
scoring of the days not yet closed; a closed day keeps the scores it was
closed with.

A rain value for an hour is the rain that falls in the hour that starts at that
time. Open-Meteo and meteoblue label rain by the hour's end, so wxverify moves
the rain it fetches from them back one hour; their rain fetched before this
change is no longer scored or served.
Google labels it by the hour's start. Visual Crossing, OpenWeatherMap, WeatherAPI
and Meteosource do not say which hour they mean, and their values are stored as
received.

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

### Station wind

Each station's hourly wind is a figure named `max_adjacent_pair_mean_wind`.
wxverify reads the station's native 5-minute records from weather.com and pairs
each record with the one before it when they are more than 0 and at most 20
minutes apart. A pair's value is the mean of the two `windspeedAvg` readings,
and the pair counts in the hour of its later record. The station's wind for an
hour is the largest pair value in that hour; a reading with no partner is
skipped, and no other wind field is used. For example, readings of 15, 10 and 5
at 2:55, 3:00 and 3:05 PM give 12.5 for the 3 PM hour. This is wxverify's own
statistic; it makes no claim to match a WMO definition. The site's hourly wind
is still the 90th percentile of its stations' values, and the daily reference is
the day's highest site hourly value.

A background wind lane downloads today's 5-minute records for every enabled
station: from the start of the local day every `obs_interval_minutes` (default
`180`) until 23:00, then at fixed evening slots from 23:00 to 23:59 local time.
In the evening a failed try moves to the next slot, and a good one skips ahead
to the 23:56 slot, which always runs. Past days are not downloaded again. A
day's hourly pair-max figures are written when the daily close closes that
day, after it ends.

The Ops page's `Wind History` panel shows the station-day counts, today's call
headroom and the lane's calls, and the latest errors.

**The served wind forecast.** The forecast wind uses every feed with a track
record, weighted by past accuracy. For each day-ahead (today through 7 days
ahead), a feed's weight is `1 / MAE` of its daily wind high over the 30 complete
local days before today, with the MAE floored at `0.1 m/s`. An observed day
counts when it has at least 22 hours; a feed's day counts when its latest run at
that day-ahead covers at least 20 hours of it; and a feed needs at least 20 such
days. Today, and any day whose station history is not settled yet, are left out
of the training. The daily wind is the weighted mean of the feeds' daily highs,
and the hourly line is the weighted mean at each hour, over the feeds present at
that hour. With fewer than two eligible feeds, wind shows "—" with the note "Not
enough feeds with a wind track record yet"; it is never a silent single-feed
value. There is no setting for this. Rain and temperature are unchanged.

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
ceiling `config.DISPLAY_REQUEST_HOURS` (`217`); `icon_global` to `180`,
`ukmo_global_deterministic_10km` to `168` and `icon_eu` to `120`, each model's
longest-run maximum; and `meteofrance_arpege_world` stays at `168` to preserve
its existing scoring eligibility. The request length counts from the hour of
the fetch, but a stored lead counts from the estimated issue time, so the
model's advertised duration of about four days does not by itself justify
lowering its `max_lead_hours`, which also bounds scoring. No feed's horizon is
lowered in 0.16.0. Adding a model means editing all three mappings
(`feeds.open_meteo.RUN_CADENCE_HOURS`, `config.OPEN_METEO_MAX_LEAD_HOURS` and
`config.OPEN_METEO_FETCH_INTERVAL_MINUTES`) plus the tests: the seed, which
runs on every start, inserts the new model's row, and the 0.16.0 one-shot
corrections leave a model added later as it was seeded. Meteoblue is
unchanged: its seed stays at `168` and its package data is filtered to that
feed's `max_lead_hours`.

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
`icon_eu`, requested to `120`, covers about the first five displayed days.

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
  so instead, and when no feed beats its baseline it adds that caveat. For wind,
  this card shows the wind weights instead: each feed's MAE (km/h), the number
  of days behind it and its share of the weight for the selected lead. The
  `Leaderboard` stays as information for wind and highlights no winner.
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
- The `partial` badge keeps its meaning: for temperature or rain, none
  of the selected feeds covers at least 18 hours of that day. It can appear
  with or without the unavailable label, because it answers a different
  question.
- `Wind max` is not affected by this rule. `Wind max` is the weighted mean of
  the feeds' daily maxima (see [Station wind](#station-wind)), and the `partial`
  badge is never set for wind. `Rain` has a stricter rule of its own, described
  below.

The `Today` tile normally shows a high and low too: a feed fetched before
today began normally has all of today's hours. Earlier forecast runs are not
fetched, and regular fetches supply hours from the hour they run. A feed can
lack some of today's hours, and then cannot cover `Today`, when:

- it was first fetched today, as on a new site or for a feed subscribed today,
  and so may lack today's hours before that fetch;
- the provider returned an hour without a value.

The label appears only when none of the feeds with forecasts for that day
covers it, so a feed added later never blanks a day another feed already
covers. Regular fetches never fill the hours before their own forecast run: a
fetch keeps only hours at least an hour after its forecast run was issued, and
Open-Meteo's regular fetches start at the hour they run. So when no feed covers
today, `Today` shows the label for the rest of the date, unless a later fetch
returns a value for an hour an earlier one left without one. At local midnight
the next date becomes `Today`, and it shows a high and low only if a feed
covers that whole date: the change of day fills in no missing hours.

The `Rain` row shows one model's rain: the model with the best rain record for
that day ahead. Each closed day records whether it rained nearby (at least
0.2 mm at two of the site's counted stations, or at least 0.5 mm at one) and
each model's forecast daily total for every day ahead. Over the last 60 closed
days, each model is scored per day ahead on the days where both are known: a
missed rainy day costs 2 points and a false alarm 1, and the lowest score
wins. Once two models have 20 counted days, only models with 20 or more take
part, and the shown model changes only when another one beats it by 3 points
or more. Until then the ranking is provisional and the tile shows `ranking
updating`. The rain ranking needs the daily close.

A station's gauge stops counting for rain when it reads exactly zero while the
other stations report rain, on 3 days spread over 2 wet spells within 30 days.
It is set aside from the next day, and the Sites page shows `rain not counted
since <date>`. It counts again after 3 days, in 2 wet spells, on which it
agrees with the others. Temperature and wind from that station are unaffected.

The winner's rain counts only when it supplies exactly one value for every hour
of the local day — 24, or 23 or 25 on a daylight-saving change day. A missing
hour, an hour given twice, or a value that falls between two hours rules it out
for that date, and the next ranked model is shown instead. The reasons above
why a feed can lack some of `Today`'s hours apply to rain too.

The row then shows the winner's rainfall and wet hours, for example
`3.1 mm · 5 h`. The rainfall is shown to one decimal place. An hour is wet when
its forecast rain is at or above the site's `rain_threshold_mm`. At 0.2 mm or
more a rain symbol follows: a rain cloud when the wettest hour is at most
1 mm, an umbrella above that. `Agreement N%` follows when two or more models
are ranked: the share of the top-ranked group (about three-quarters of the
ranked models) whose own daily total is 0.2 mm or more. Hovering over the row
names the model. The rain bars on the day's hourly chart are the winner's.

`No rain forecast` means no ranked model has a full day of rain forecasts for
that date; it never means a dry forecast.

A tile's `low confidence`, `ranking updating` and `stale` badges also cover the
feeds its daily high and low come from, and the rain winner and its agreement
group, which can differ from the selected feeds its hourly chart plots.
`ranking updating` also shows whenever the rain ranking is provisional, even
beside `low confidence`.

A feed counts as `stale` when the app has not completed a successful forecast
download for it within twice its fetch interval. Only a download that returned
usable forecast values counts; a download that returned values already stored
counts too. `fetch time unknown` means no such download has been recorded yet,
for example right after an upgrade.

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
python -m wxverify --db /path/to/wxverify.db settings list
python -m wxverify --db /path/to/wxverify.db settings get <key>
python -m wxverify --db /path/to/wxverify.db settings set <key> <value>
python -m wxverify --db /path/to/wxverify.db sources set-cap <source> --daily-call-limit <n>
python -m wxverify --db /path/to/wxverify.db providers doctor --site-id <site_id>
python -m wxverify --db /path/to/wxverify.db providers reconcile
python -m wxverify --db /path/to/wxverify.db providers enable --site-id <site_id> --all-new
python -m wxverify --db /path/to/wxverify.db providers fetch --site-id <site_id> --source visualcrossing
python -m wxverify --db /path/to/wxverify.db providers smoke --site-id <site_id> --all-new
python -m wxverify --db /path/to/wxverify.db timezone change --site-id <site_id> --timezone <IANA zone> [--json]
python -m wxverify --db /path/to/wxverify.db wind-history prune --dry-run [--json]
python -m wxverify --db /path/to/wxverify.db wind-history prune --restored-backup <backup_name> [--json]
```

For `weathercom`, the cap in force is the lower of the value stored with
`sources set-cap` and a provisional provider allowance of 1500 calls a day; the
Ops page's Budget Gauges panel shows both.

Changing `rolling_window_days` takes effect at the next publication, which
follows each day's close. Other settings are plain runtime knobs.

Provider operations are local admin commands. `doctor`, `reconcile`,
`enable`, `disable`, and enqueue-only `providers fetch` do not call external
forecast APIs. `providers fetch --run-now` and `providers smoke` perform live
provider calls and consume provider budget.

`providers reconcile` is safe to run against a live database when seed catalog
rows are missing; it inserts missing `sources` and `feeds` rows without
overwriting edited caps or feed settings. New or changed Python adapter code
still requires restarting the app process so the running interpreter loads that
code.

`wind-history prune` deletes, once, the 5-minute wind readings stored before
0.16.14 that are dated more than 20 minutes before their site's first closed
day. Run it with `--dry-run` first: that only reads, and prints each station's
row count and date range, then every reason the prune would refuse. A run that
deletes needs `--restored-backup`, naming a backup from before 0.16.14 whose
restore you have checked. It refuses until the daily close is on and the first
closed day of every site that still has such readings is closed or missed. It
deletes one station at a time, prints the rows left, and exits `0` only when
none is left, so it is safe to run again after a stop.

## Changing a site's time zone

A site's time zone decides which readings fall on which local day, and each day is closed and scored once under the zone it was created with. A zone is therefore not an ordinary editable field: `PUT /api/sites/{id}` has no time-zone attribute and the Sites page cannot change one.

If a site's zone is wrong, or the site moves to another zone, enter the change with:

```sh
python -m wxverify --db /data/wxverify.db timezone change --site-id <site_id> --timezone <IANA zone>
```

The change takes effect from the next full local day. The command prints the first full day under the new zone, and the changeover day before it when there is one.

- The **changeover day** runs from the end of the last day under the old zone to the start of the first day under the new one, so it is shorter or longer than a normal day. It is flagged and not scored, like a missed day. Its readings are still stored.
- **Days already closed keep the zone they were scored under.** They are not rebuilt, and the pages flag them as scored under their own zone.
- Past days cannot be re-bucketed into a different zone. The earlier retrospective correction was retired when the daily close turned on.

The command exits non-zero with a one-line message, for example when the site is unknown, the zone is not a valid IANA name, the site already runs under that zone from the next day, or the daily close is not yet on.

## Operational Notes

- SQLite runs in WAL mode.
- All writes are serialized through one writer connection.
- Weather.com PWS calls are budgeted per enabled station.
- Forecast and observation provider keys are never stored in the database.
- `/api/health/keys` reports only present or absent, never secret values.

## Database version 17: the first start after the upgrade

The first start of a release with database version 17 converts the database
once, before the add-on serves anything:

- Each forecast is stored once, in `forecast_history`, in place of the old
  pair tables. The tables of the retired verification runs are dropped, and so
  are queued jobs of kinds the add-on no longer runs.
- The file is then compacted once (`VACUUM`), giving the freed space back to
  the disk. That needs free space on `/data` of about 2.2 times the data the
  file holds. With less, the compaction is skipped with a WARNING and tried
  again at the next start; the add-on runs normally meanwhile.

On a large database this first start takes minutes, not seconds. Every start
after it is quick. A stop during the conversion is safe: the file keeps its
data, and the next start finishes the conversion.

The conversion accepts only a file at version 16 with the daily close on, which
is what 0.16.15 writes once the daily close is on. The add-on refuses to start
on any other file: an older version is told to install 0.16.15 first, and a
version 16 file without the daily close is told that this release needs it. A
version 17 file cannot be opened by an older release: to go back, restore an
export taken before the upgrade.

## Database Export

Ops → Database Export downloads a consistent snapshot of the add-on database
as a timestamped, gzip-compressed `.db.gz` file. The snapshot is taken with
`VACUUM INTO`, so it is a standalone, checkpointed copy — safe to take while
the worker is running.

There is no import. To restore an export: stop the add-on, unpack the `.db.gz`,
copy the `.db` over `/data/wxverify.db`, delete any `wxverify.db-wal` or
`wxverify.db-shm` file beside it, and start the add-on again. The file is
checked and upgraded when the add-on opens it, as above. **Set the
per-variable forecast blend depths in the options first** — see
[Forecast blend depth is owned by the options, not the database](#forecast-blend-depth-is-owned-by-the-options-not-the-database).

Earlier releases kept one `wxverify-<timestamp>-<id>Z.db.bak` in `/data` after
an import. Nothing creates or removes these files now; delete any that is left
to free its space.

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
  outcome — completed, deferred, retry, failed, or dropped (its outcome was not recorded,
  usually because its site was deleted while it ran) — and how long it took, `elapsed=…`),
  the scoring milestones (`score phase=…`, `score discovery …`, `score window=…`, and
  `score sweep …`, each with its own elapsed time), and one `scoring run complete …` line
  per scoring run. If these keep ticking over, the add-on is alive and doing its job.
  Finer-grained per-batch and per-request detail stays at DEBUG.
- **DEBUG — show me literally everything.** The full firehose: every forecast fetch, every
  observation fetch, every scoring phase, every queue and worker transition, every database
  transaction, and the raw HTTP requests and responses.
  Use this when you're diagnosing a specific problem; it is very verbose.

### Setting the level

Set `log_level` in the add-on configuration (Settings → Add-ons → Weather Verify →
Configuration) to one of `error`, `warning`, `info` (default), or `debug`, then restart the
add-on. The chosen level applies to the running service **and** to any one-shot command you
run inside the add-on container (for example the CLI `fetch` and `score` commands) — so
`log_level: debug` gives you the full trace for a manual command too, not just the
background worker.

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

The budget group includes a `weathercom_access_refused` warning. It trips when
weather.com refuses (HTTP 401 or 403) one of the three download kinds: current
conditions, hourly readings or 5-minute readings. Its `count` is the number of
kinds refused, and its `detail` names them, the HTTP status, since when, and
until when they are held, or that the hold has ended and the next try is
awaited. It clears once each refused kind is granted again, or 24 hours after a
kind's hold ended if that kind is not tried again.

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
default response is unchanged from 0.15.0.

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

- Weather.com PWS: per enabled station, the current-conditions poll, the
  5-minute downloads described under [Station wind](#station-wind), and, once
  a day has ended, an hourly download for a station whose day has gaps. The
  Ops page's Budget Gauges panel shows the day's calls against the cap in
  force.
- Open-Meteo forecasts: each enabled Open-Meteo model is polled every `360`
  minutes (`4` calls per day), except `gem_global`, whose interval is `720`
  minutes. The add-on reports a feed as stale after `12` hours, so `gem_global`
  is polled `30` minutes early, about every `690` minutes (just over `2` calls
  per day). Except for `icon_eu`, polling follows each model's run
  cadence; it is not aligned to publication, so fetching every published run is
  not guaranteed. `icon_eu` publishes every 3 hours and is polled every `360`
  minutes, which is six-hourly polling that samples roughly alternate ICON-EU
  updates. Which updates it samples depends on when the add-on started and
  shifts over time. ICON-EU covers Europe only and is on by default because the
  add-on's defaults are set for Europe; for a site outside Europe, switch it off
  for that site on the Sites page, under Feed Subscriptions.
- Meteoblue: one multimodel package call every `360` minutes, or `4` calls per
  enabled site per day. The current package costs `16000` credits per call.

The run a forward Open-Meteo forecast is labelled with, and the time it is
recorded as issued, are estimated from the time of the fetch: the add-on
subtracts a flat `90`-minute availability lag and rounds the result down to the
model's run cadence in UTC, to `00`, `06`, `12` or `18` UTC, to `00` or `12`
UTC for `gem_global`, or to any multiple of 3 hours (`00`, `03`, … `21` UTC)
for `icon_eu`. No run identifier is read from the provider, and nothing
confirms which provider run a stored forecast came from. `gem_global`'s
estimated labels are now twelve-hourly, matching the model's twelve-hour run
schedule; forward `gem_global` forecasts stored before 0.16.0 keep their
six-hourly estimated labels, and no stored row is relabelled. Because the lag
is a flat `90` minutes and ICON-EU is usually published later than that, an
`icon_eu` label can name a run newer than the one that supplied the data, so a
stored lead can be shorter than the data's true age.

For an example deployment with one enabled site, 8 enabled stations, 8 enabled
Open-Meteo models, and the Meteoblue package enabled, the expected steady-state
use is:

| Provider        |          Expected steady-state use |
| --------------- | ---------------------------------: |
| Open-Meteo      |                     `30` calls/day |
| Meteoblue       | `4` calls/day, `64000` credits/day |

The default wxverify caps are:

| Provider    |    Call cap |  Credit cap |
| ----------- | ----------: | ----------: |
| Weather.com |  `1500/day` |        none |
| Open-Meteo  | `10000/day` |        none |
| Meteoblue   |     `5/day` | `65000/day` |

For Weather.com, the add-on option `weathercom_daily_call_limit` (default
`3000`) is the configured cap, but the cap in force is the lower of that value
and a provisional provider allowance of `1500` calls a day, so the default
configuration runs at `1500/day`. A configured value below `1500` lowers the cap
in force; a higher one has no effect until a release raises the allowance. The
Ops page's Budget Gauges panel draws Weather.com use against the cap in force
and, where the two differ, notes the configured value.

For Meteoblue, one package request counts as `1` API call and currently costs
`16000` credits. The credit cap is therefore the binding limit: with the default
`65000` credit cap, at most `4` Meteoblue package calls fit in one billing day
because a fifth would require `80000` credits. The `5/day` call cap is a
secondary safety limit; the credit cap remains the practical Meteoblue
constraint.

One-time setup work can add a few calls: adding a station uses one Weather.com
validation call and one Open-Meteo elevation call.

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
than the local one. A green local run does not cover them.

Cite code by its owning symbol, never by `file.py:<line>`: a line number
drifts the moment the file changes. The pytest suite includes a guard
(`tests/test_citation_anchor_guard.py`) that fails on any `file.py:<line>`
citation in a production docstring, comment, or Jinja template comment, so
write "see `compact_storage` in `compaction.py`" rather than a line reference.

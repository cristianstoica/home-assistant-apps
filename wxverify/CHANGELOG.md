# Changelog

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

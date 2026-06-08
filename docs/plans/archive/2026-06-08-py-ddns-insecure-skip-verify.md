# py-ddns 2.2.0 — `url.insecure_skip_verify` (opt-in TLS certificate-verification skip on the callback path)

Authored by py-architect (owns the design, first-hand-verified against `main` / released 2.1.1);
transcribed by the main thread. Canonical destination on implementation:
`home-assistant-apps/docs/plans/2026-06-08-py-ddns-insecure-skip-verify.md`.

## Context

Public py-ddns add-on users whose cPanel / shared-hosting **callback** endpoint presents a
certificate that fails verification (hostname mismatch, or self-signed) — and who cannot obtain
the clean-cert "provider-hostname" URL because their provider only hands them the "standard"
own-domain URL — currently cannot use the add-on at all. `url.endpoint` is accepted only if it is
`https://`, but a valid-scheme URL with an unverifiable cert fails at request time on every cycle
with a transport error, and there is no escape hatch.

This adds the standard DDNS-client opt-out (cf. ddclient `ssl_verify=no`, wget
`--no-check-certificate`): a single nested boolean `url.insecure_skip_verify` that, **only on the
callback path and only when explicitly enabled**, disables TLS _certificate verification_ (not
encryption). Default OFF. Intended outcome: those users can run the add-on, with a loud,
documented, narrowly-scoped downgrade — while Azure ARM and the IP-source lookups keep verifying
unconditionally.

### The tradeoff this feature deliberately accepts (drives the README copy and the WARNING)

Skipping certificate verification keeps the channel **encrypted** (a passive eavesdropper still
cannot read the secret callback URL) but drops **authentication**: an _active_ man-in-the-middle
could impersonate the endpoint, terminate the TLS session, and capture the capability URL — which
**is** the DDNS update credential. So this is _encrypted-but-unauthenticated_. Materially safer than
plaintext (which the design keeps rejecting) but strictly weaker than verified TLS. Enable only when
a clean-cert URL is genuinely unobtainable.

## Locked guardrails (each verified against the real code)

| #   | Guardrail                                           | How the design holds it (verified)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| --- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Default OFF, explicit opt-in, `bool?` default false | `url.insecure_skip_verify` parsed via existing `_require_bool(url_group, "insecure_skip_verify", False, ...)`; schema `bool?`; `config.yaml` default `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 2   | HTTPS still mandatory; no transport downgrade       | Flag is read **after** and **independently of** `_validate_https_url(url_endpoint.strip(), "url.endpoint")` (`config.py:404`). That call is unchanged and still rejects `http://`/hostless/userinfo/fragment. The flag only selects which SSL _context_ the request uses on an already-https URL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 3   | Loud every callback cycle at WARNING                | `_log.warning("%s -> ...", self._config.name)` in `Updater._cycle_callback` (the URL-only per-cycle driver; `def` at `updater.py:301`), inserted **before line 309 (`detected = self._ip_source.detect()`), after the `_cycle_callback` docstring** (not at 301, which would demote the docstring), guarded by `if self._config.url_insecure_skip_verify:`. Emitted once per callback cycle **including suppressed steady cycles** (it sits **above** the steady-state suppression `return` at `updater.py:342`) and **outside** `_fire_and_confirm`'s `RetryRunner` loop (so a retried fire does **not** multiply it) → exactly once per cycle. WARNING level so it survives the `info` production threshold (the `warn_azure_ignored` precedent; a `debug` emit would pass `--check`'s DEBUG recorder but vanish at `info`). Secret-safe: the only interpolated scalar is `name` (the FQDN — non-secret, already logged on every updater line); the endpoint never appears. |
| 4   | Scope = callback path ONLY; never azure / ip-source | **Verified leak surface:** `__main__.py:_run_loop` builds ONE `http = UrllibHttpClient()` and injects the _same instance_ into `build_provider` (→ AzureProvider + UrlProvider) **and** into `IpSourceClient`. So a flag on the shared client would leak. The design instead constructs a **second, separate** client only for the URL provider when the flag is set; azure + ip-source keep the default-verifying client untouched.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 5   | stdlib `ssl` only, no new dep                       | `ssl.create_default_context()` then `check_hostname=False` + `verify_mode=ssl.CERT_NONE`, passed to `urllib.request.urlopen(..., context=ctx)`. Zero new dependencies.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 6   | Modifier, not a selector                            | `url.insecure_skip_verify` follows `url.send_myip` exactly: read + threaded onto `Config`, **never** referenced in `url_selected` (`config.py:380`) or `_azure_selected`. Setting only the flag (no `endpoint`) leaves `url_selected` false → no spurious URL selection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

## Design forks (resolved, not deferred)

**A. Where does the unverified context attach — per-request kwarg, per-client constructor flag, or injected `ssl.SSLContext`?**
**Resolved: per-client constructor flag** (`UrllibHttpClient(*, insecure_skip_verify=False)`). It is
the minimal seam that keeps the skip provably scoped: the default-verifying client is the only client
azure + ip-source ever see, and the URL provider gets a distinct insecure client _only when the flag
is set_. It needs **no** change to the `HttpClient` Protocol, so `--check`'s `FakeHttp` and the other
call sites stay as-is. (A per-request `verify` kwarg would touch the Protocol + every call site;
injecting the `ssl.SSLContext` leaks an `ssl` type across the seam for no current need.) Build the
insecure context **once** in `__init__` and store it; pass it as `context=`. A verifying client stores
`None` and passes nothing — today's behavior byte-for-byte.

**B. Where is the second `UrllibHttpClient` constructed?**
**Resolved: inside `build_provider`'s URL branch** (via a small named local `_url_http(config, http)`),
not in `__main__`. The selector is the natural home for "construct the concrete transport a given
provider needs," already lives one import away from `UrllibHttpClient`, and keeps the entire scope
decision in one auditable place — the `--check` can drive `build_provider` directly and assert the URL
provider got an insecure client while azure got the shared one.

## Files & changes

### 1. `pyddns/httpclient.py` — the SSL-context seam

- `import ssl`.
- Add `UrllibHttpClient.__init__(self, *, insecure_skip_verify: bool = False) -> None:`. When true, build
  `ctx = ssl.create_default_context()`, then **set `ctx.check_hostname = False` BEFORE
  `ctx.verify_mode = ssl.CERT_NONE`** (ssl raises if `verify_mode=CERT_NONE` while `check_hostname` is
  still True), store `self._ssl_context: ssl.SSLContext | None = ctx`. Else `self._ssl_context = None`.
  Keyword-only so a positional caller can't accidentally enable it.
- In `request`, pass `context=self._ssl_context` to `urlopen` (keep the existing `# noqa: S310`).
  `context=None` is identical to today's no-`context` call → verifying path unchanged.
- Extend the module docstring: the client may be constructed with cert verification disabled (used
  **only** by the URL provider when `url.insecure_skip_verify` is set); the default constructor verifies.

### 2. `pyddns/models.py` — carry the flag on `Config`

- Add `url_insecure_skip_verify: bool` to the `Config` NamedTuple, immediately after `url_send_myip`.
- Extend the docstring's url-carries clause to mention it.
- **No field default** (the NamedTuple has none): every construction site must set it explicitly —
  pyright then flags every omission. Known sites: production `validate` (`config.py`), and the
  test-config builders `_url_config()`/`_azure_config()` in **both** `check/debug_trace.py` **and**
  `check/confirm.py` (each has its own pair). Azure-mode configs set it `False`. Final guard:
  `rg "Config\(" py-ddns/pyddns` + a clean `pyright py-ddns/pyddns` to confirm no site was missed.

### 3. `pyddns/config.py` — parse + thread (no validator change)

- After `url_send_myip = _require_bool(url_group, "send_myip", False, label="url.send_myip")` (line 378):
  `url_insecure_skip_verify = _require_bool(url_group, "insecure_skip_verify", False, label="url.insecure_skip_verify")`
  — read **unconditionally** (so a wrong-type value is named even when the URL path isn't selected),
  mirroring `send_myip`.
- Add `url_insecure_skip_verify=url_insecure_skip_verify` to the `Config(...)` construction (lines 425-439).
- **Do NOT** touch `url_selected`, `_azure_selected`, `_AZURE_TOKEN_FIELDS`, or `_validate_https_url`.
- Docstring: append to the "HTTPS-only URL contract" paragraph that the flag skips cert _verification_ on
  the callback path only — never relaxes the https/host/userinfo/fragment rejection, never affects azure
  or ip-source TLS.

### 4. `pyddns/updater.py` — the per-cycle WARNING (once per callback cycle, survives suppression + retry)

> `pyddns/providers/url.py` is **UNCHANGED**. `UrlProvider` takes its `http` by injection and `apply`
> calls `self._http.request("GET", ...)`, so the cert-skipping client (built in §5's `_url_http`) reaches
> the GET with zero change to the provider. The WARNING cannot live in `apply`: the steady-state
> suppression branch returns before `apply` is called (under-emission) and the `RetryRunner` retries
> `apply` within one cycle (over-emission). It belongs at the cycle driver instead.

- In `_cycle_callback` (the URL-only per-cycle driver — `run_once` sends azure → `_cycle_api`, else →
  `_cycle_callback`), emit the WARNING **at the top of the method, before `detected = self._ip_source.detect()`**
  (`updater.py:309`), guarded by `if self._config.url_insecure_skip_verify:`. This sits **above** the
  steady-state suppression `return` (`updater.py:342`) → fires on suppressed steady cycles too; and
  **outside** `_fire_and_confirm`'s `RetryRunner.run` loop → exactly once per cycle even when a fire
  retries:
  ```python
  if self._config.url_insecure_skip_verify:
      _log.warning(
          "%s -> TLS certificate verification is DISABLED (url.insecure_skip_verify=true): "
          "the channel stays encrypted but an active MITM could impersonate the endpoint "
          "and capture the secret callback URL",
          self._config.name,
      )
  ```
  WARNING level (survives the `info` production threshold). Secret-safe: the only interpolated scalar is
  `self._config.name` (the FQDN — non-secret, already logged on every updater line, e.g. `updater.py:251`);
  the endpoint never appears. Correctly callback-scoped: `_cycle_callback` is only entered for non-azure
  configs, and an azure `Config` carries the flag `False` regardless.

### 5. `pyddns/providers/__init__.py` — wire the insecure client to the URL provider only (scope chokepoint)

- Add a small named local `_url_http(config, http)` returning a **new** `UrllibHttpClient(insecure_skip_verify=True)`
  when `config.url_insecure_skip_verify` is set, else the passed-in `http` unchanged.
- `build_provider`'s `Provider.URL` branch passes `_url_http(config, http)` to `UrlProvider` (its existing
  3-arg signature `endpoint, send_myip, http` is **unchanged** — the flag is not threaded into the provider);
  the `Provider.AZURE` branch is **untouched** (always the shared verifying `http`).
- `plan_provider`'s URL branch: build the provider with `_url_http(config, http)`, take the string
  `UrlProvider(...).plan(detected_ip)` returns, and **append** `"; TLS cert verification DISABLED"` to it
  when `config.url_insecure_skip_verify` (the suffix lives here at the selector, secret-free, for
  `--check --dry-run` operator visibility — `UrlProvider.plan` itself is unchanged).
- `_url_http` imports the concrete `UrllibHttpClient` here (the selector already imports `HttpClient` from
  the same module).

### 6. `pyddns/__main__.py` — no functional change required

- The insecure client is built inside `build_provider`, so the shared `http` `__main__` passes to
  `IpSourceClient` + azure stays verifying. Confirm during implementation that
  `IpSourceClient(cfg.ip_source_urls, http)` still receives the original `http` (no reassignment).

### 7. `config.yaml` + `translations/en.yaml` — schema, default, UI copy

- `config.yaml`: under `options.url` add `insecure_skip_verify: false` (after `send_myip`); under
  `schema.url` add `insecure_skip_verify: bool?` (after `send_myip`); one-line comment update; bump
  `version: "2.2.0"`; `breaking_versions:` stays `["2.0.0"]`.
- `translations/en.yaml`: under `configuration.url.fields`, add an `insecure_skip_verify` entry —
  name "Skip TLS certificate verification (insecure)", description stating: advanced/default off;
  disables cert verification on the callback path only; connection stays encrypted but authentication
  is lost (active MITM could impersonate and capture the secret callback URL = the DDNS credential);
  HTTPS still required (plaintext always rejected); Azure / IP-source always verify; a WARNING is logged
  every callback cycle while on (including steady cycles); enable only if a verifiable-cert URL is
  unobtainable.

### 8. `pyddns/__init__.py` — version bump

- `__version__ = "2.2.0"` (lock-step with `config.yaml`).

### 9. `README.md` — document the flag + the tradeoff

- New subsection under the Callback URL / URL-archetype docs covering: what it is (opt-in, default-off,
  cert-verification skip on the callback only); when to use it (cPanel/shared-host cert that fails
  verification with no clean-cert URL); the explicit encrypted-but-unauthenticated / active-MITM /
  credential-capture tradeoff; scope (HTTPS still mandatory; azure + ip-source always verify; a WARNING
  is logged every callback cycle while enabled, including steady/suppressed cycles); recommendation
  (enable only when a verifiable-cert URL is unobtainable; prefer fixing the cert). CHANGELOG.md is
  **not** edited here — gitops owns it at commit time.

### 10. `pyddns/check/*` — the `--check` oracle additions (this repo's only test surface)

> No pytest in this repo; qa-engineer does coverage **review** only. Each assertion names its seam.

- **(a) parse/default** — `check/config_checks.py::check_callback_precedence`: default url config →
  `url_insecure_skip_verify is False`; with `insecure_skip_verify: True` → `True`; azure-mode config →
  `False`.
- **(b) http:// still rejected with flag true + wrong-type rejected** — new `pyddns/fixtures.py::INVALID_OPTIONS`
  entries (package-root `pyddns/fixtures.py`, **not** `check/fixtures.py`; `check/*` reach it via `from .. import fixtures`):
  `http://` endpoint + `insecure_skip_verify: True` rejected naming `url.endpoint`;
  `insecure_skip_verify: 1` rejected naming `url.insecure_skip_verify` (run by existing
  `check_invalid_options`).
- **(c) unverified context used ONLY on callback + ONLY when set** — drive the real
  `build_provider(cfg, shared_http, monotonic)` for: URL+flag-true (UrlProvider's client
  `_ssl_context is not None` & `verify_mode == ssl.CERT_NONE` & `check_hostname is False`);
  URL+flag-false (shared verifying client, `_ssl_context is None`); **any azure** config (shared verifying
  client, `_ssl_context is None`). Plus a standalone `UrllibHttpClient(insecure_skip_verify=True)` →
  `CERT_NONE`, and default `UrllibHttpClient()` → `_ssl_context is None`. (New `check/tls_scope.py` run from
  `check/__init__.py`, or folded into `config_checks.py`.) See **Open decision** below for how the SSL
  context is read.
- **(d) per-cycle WARNING — once per cycle, survives suppression + retry, visible at info** — drive the real
  `Updater.run_once()` (**not** `UrlProvider.apply`, which cannot catch the under/over-emission this fix is
  for) via the callback-`Updater` harness already in `check/confirm.py` (`_make_updater` + `FakeState` /
  `FakeResolver` / `FakeProvider` / `FakeClock` / `FakeSleeper`, `mark_started()`, `run_once()`) plus
  `capture_at_level(logging.INFO, run)` (`check/fakes.py`). Three assertions:
  - **(d.a) suppressed steady cycle, flag true** — state holds last-known X, `name` resolves to X,
    `detected == X`, `mark_started()` (non-first cycle → hits the suppression `return` at `updater.py:342`).
    `capture_at_level(logging.INFO, lambda _h: u.run_once())` → **exactly one** record containing
    "verification is DISABLED", its `levelno == logging.WARNING`, **and** `provider.apply_calls == []`
    (proves the warning fires even though no fire happened, at the info production threshold).
  - **(d.b) same shape, flag false** → **zero** records containing "verification is DISABLED".
  - **(d.c) firing cycle on the retry path, flag true** — first cycle (empty state → fires); a local
    raise-once provider double whose `apply` raises `TransientError` on the first call then returns a
    fired result (the stock `FakeProvider` scripts one fixed `apply_result`, so it **cannot** raise-once —
    specify the local double, recording `apply_calls`); default `FakeSleeper()` (never stops) so
    `RetryRunner` retries; resolver scripted RESOLVED→value for the post-fire confirm. Run under
    `capture_at_level(logging.INFO, ...)` → **exactly one** "verification is DISABLED" record even though
    `len(provider.apply_calls) == 2` (proves the emit is outside the `RetryRunner` loop).
  - Harness note: `_make_updater` hardcodes `_url_config()`; thread the new flag through it — extend
    `_url_config()` with `insecure_skip_verify: bool = False` and a `config=`/flag override on
    `_make_updater`, or add a sibling `_insecure_url_config()`. (Belongs in `check/confirm.py` — the harness
    lives there — or a new `check/insecure_warning.py` registered in `check/__init__.py`.)
- **(e) no-secret-leakage on the skip path** — `check/secrets_leak.py::check_no_secret_leakage`: replace the
  direct `UrlProvider.apply` drive with a **flag-set `Updater.run_once`** drive (one suppressed steady cycle
  - one firing cycle) whose `Config.url_endpoint == fixtures.EXAMPLE_URL_ENDPOINT` (real secret in scope),
    through `with_recording_handler`; assert nothing (incl. the new WARNING) leaks via `_leaks`/`_SECRETS`.
    **Also** add the flag-set `plan_provider(...)` output (with the appended `"; TLS cert verification DISABLED"`
    suffix — the surface that actually changed) to the captured set via `_record(...)` and assert `not _leaks`.
    Enumerate the import delta: `secrets_leak.py` currently imports `UrlProvider`/`compose_fire_url`; the rewrite
    adds `plan_provider` and the harness builders.

## Open decision (fold into implementation)

The §10(c) scope assertions must read the client's SSL context (white-box). The repo's existing idiom is
`# pyright: ignore[reportPrivateUsage]` in check code (used for `config._DEFAULT_IP_SOURCES`, `http.calls`).
**Recommendation (per the standing "public names over pyright suppression" preference):** expose a small
public read-only predicate `UrllibHttpClient.verifies_tls -> bool` — a genuine domain concept, not API
pollution — and assert on that instead of the suppression comment. Implementer's call; the assertion is
what matters.

## CI gate (the canonical verification — matches `.github/workflows/lint.yaml`, `pyddns-gates`)

Run from the repo root `home-assistant-apps/` (the commands are cwd-relative), with the pinned tooling
`ruff==0.15.13`, `pyright==1.1.409`, Python 3.13:

```bash
PYTHONPATH=py-ddns python -m pyddns --check      # the --check oracle
pyright py-ddns/pyddns                            # strict type gate
ruff check --target-version py313 py-ddns         # lint
ruff format --check py-ddns                        # format (do NOT skip — CI runs it)
```

Throughout the build sequence, "`--check`" means `PYTHONPATH=py-ddns python -m pyddns --check` run from
`home-assistant-apps/`. Per-file `pyright`/`ruff` runs are fine mid-sequence, but the **final** gate must
be the exact four commands above (note `ruff format --check` — the prior draft omitted it).

## Build sequence (each step ends green against the CI-gate subset for the files it touched)

1. **`httpclient.py` seam** — `import ssl`, keyword-only `__init__`, stored `_ssl_context`, `context=` pass-through.
   → verify: pyright `httpclient.py` 0/0; ruff; `--check` exit 0 (default client unchanged; no consumer yet).
2. **`models.py` field** — add `url_insecure_skip_verify: bool` to `Config`. → verify: pyright now reports
   missing-arg at every `Config(...)` site (expected; fixed next).
3. **`config.py` parse + thread** (and the `check/debug_trace.py` **and** `check/confirm.py` Config-builder
   sites, so `--check` can import). → verify: pyright `config.py`/`debug_trace.py`/`confirm.py` 0/0; ruff;
   `--check` exit 0.
4. **`updater.py` per-cycle WARNING** — guarded `_log.warning("%s -> ...", self._config.name)` at the top of
   `_cycle_callback` (before `updater.py:309`). `providers/url.py` is **NOT touched** (no `UrlProvider(...)`
   signature change). → verify: pyright `updater.py` 0/0; ruff; `--check` exit 0 (flag reads False everywhere
   until configs set it — no behavior change yet).
5. **`providers/__init__.py` wiring** — `_url_http` routes the insecure client into `build_provider`'s URL
   branch; `plan_provider` appends the `"; TLS cert verification DISABLED"` suffix. `UrlProvider`'s 3-arg
   signature is unchanged. → verify: pyright `providers/__init__.py` 0/0; ruff; `--check` exit 0.
6. **`check/*` assertions** — add §10 (a)-(e) + the INVALID_OPTIONS fixtures + the flag-aware `Updater`
   harness builder + the §10(d.c) raise-once provider double. → verify: `--check` exit 0 with the new PASS
   lines; pyright on touched `check/*.py` 0/0; ruff.
7. **`config.yaml` + `en.yaml` + `__init__.py` + README** — schema/default/UI copy, version bump 2.2.0
   (config.yaml + `__init__.py`), `breaking_versions` unchanged, README tradeoff subsection. → verify:
   `config.yaml` parses; `__version__` == config `version` == `2.2.0`;
   `PYTHONPATH=py-ddns python -m pyddns --version` prints `py-ddns 2.2.0`; `--check` exit 0; pyright (touched)
   0/0; ruff.
8. **Full CI gate sweep** (from `home-assistant-apps/`): `PYTHONPATH=py-ddns python -m pyddns --check`;
   `pyright py-ddns/pyddns`; `ruff check --target-version py313 py-ddns`; `ruff format --check py-ddns`.
   Then `PYTHONPATH=py-ddns python -m pyddns --check --dry-run --options <url-flag-set file>` to eyeball the
   dry-run plan line shows "TLS cert verification DISABLED" and leaks no secret. → verify: all four gate
   commands green; dry-run plan secret-free.

## Risks

- **R1 — Flag leaks onto azure / ip-source (headline risk).** Mitigation: insecure client built in **one**
  place (`build_provider` URL branch via `_url_http`); azure gets the shared verifying client; ip-source is
  wired separately in `__main__` with the verifying client. §10(c) pins all three. Residual: a future
  refactor reusing one client for both archetypes re-introduces the leak — the §10(c) azure assertion is the
  tripwire.
- **R2 — WARNING under-emits (steady cycles) / over-emits (retries) / is silent at `info`.** The headline
  audit finding: in `apply` the warning never fires on suppressed steady cycles (`updater.py:342` returns
  first) and fires once per retry attempt. Mitigation: emit at the top of `Updater._cycle_callback` — above
  the suppression return, outside the `RetryRunner` loop — at WARNING level; pinned by §10(d.a) (suppressed
  cycle → exactly one, `apply_calls == []`), §10(d.c) (retry cycle → exactly one, `apply_calls == 2`), and
  the `capture_at_level(logging.INFO, ...)` visibility assertion.
- **R3 — Secret in the new WARNING / appended plan suffix.** Mitigation: the WARNING's only interpolated
  scalar is `self._config.name` (the FQDN — non-secret, already logged throughout the updater); the endpoint
  never appears, and the plan suffix is a constant string. Both surfaces are run through the `_leaks`/`_SECRETS`
  corpus in §10(e) (the WARNING via a flag-set `Updater.run_once`; the suffix via the flag-set `plan_provider`).
- **R4 — Flag weakening the https contract.** Mitigation: `_validate_https_url` untouched; §10(b) proves
  `http://` + flag true still rejected naming `url.endpoint`.
- **R5 — Operator over-use / footgun.** Mitigation: default off, opt-in only; loud per-cycle WARNING; README
  - en.yaml state the tradeoff and recommend a clean cert first. Accepted, documented residual — the
    feature's reason for existing.
- **R6 — `Config` field-add breaks an un-updated construction site.** Mitigation: deliberately **no default**
  → pyright flags every site; steps 2-3 fix them in dependency order; `--check` won't import until fixed.
- **R7 — `CERT_NONE` + `check_hostname` ordering.** Mitigation: set `check_hostname = False` **before**
  `verify_mode = CERT_NONE` (§1).

## Out of scope

- A global (all-paths) verification skip — rejected (guardrail #4: azure / ipify / icanhazip always present
  valid TLS).
- Custom CA bundle / pinning support — separate, larger feature; not requested.
- Any change to plaintext rejection, the provider-inference gates, or CHANGELOG (gitops owns the changelog).

## Verification (end-to-end)

- **CI gate (canonical, from `home-assistant-apps/`):** `PYTHONPATH=py-ddns python -m pyddns --check`;
  `pyright py-ddns/pyddns`; `ruff check --target-version py313 py-ddns`; `ruff format --check py-ddns` — all
  green. The `--check` run shows the new §10 (a)-(e) PASS lines and `_leaks` over all captured strings stays
  false.
- **Dry-run eyeball:** `PYTHONPATH=py-ddns python -m pyddns --check --dry-run --options <url config with insecure_skip_verify: true>`
  → plan line shows "TLS cert verification DISABLED", no secret in output.
- **Version:** `PYTHONPATH=py-ddns python -m pyddns --version` prints `py-ddns 2.2.0`; `config.yaml`
  `version` == `__version__` == `2.2.0`; `breaking_versions` == `["2.0.0"]`.
- **Live (operator, post-merge, optional):** on Panaci, set `url.insecure_skip_verify: true` against a
  callback whose cert previously failed verification; confirm the add-on log shows the per-cycle WARNING at
  the default `info` level and the record updates. (Pace add-on restarts per the Supervisor-hang runbook.)

## Implementation base (decided 2026-06-08: rebuild fresh from `main`)

The canonical implementation follows the constructor-flag (option B) design above, built from a clean
checkout of current `main` (released 2.1.1).

The existing `feat/py-ddns-insecure-skip-verify` worktree is **not** the base. Ground-truth on
2026-06-08 found it is **not a stub**: it holds a _complete but entirely uncommitted_ prior implementation
— 13 modified tracked files (`config.py`, `models.py`, `httpclient.py`, `providers/url.py`,
`providers/__init__.py`, `fixtures.py`, the three `check/` modules, `config.yaml`, `en.yaml`, `README.md`)
plus a new untracked `check/insecure_skip.py` — using the **rejected option A** (per-request `ssl_context`
threaded through the `HttpClient` Protocol; per-cycle WARNING with a _redacted_ endpoint; `FakeHttp` records
per-call `ssl_contexts`). Nothing is committed (branch is 0 ahead / 1 behind `main`).

That work is **superseded** by this plan, not extended. Because it is uncommitted and therefore loss-prone
(uncommitted work is silently wiped by `git reset`/`clean`), the **first** gitops step is to commit it to
its branch as-is — a recoverable reference, in case any detail of option A is worth consulting — and only
then provision a **fresh worktree on a new branch off `main`** for the canonical option-B implementation.
Do **not** discard the existing worktree before that commit.

## Post-plan chain (after approval; not this turn)

gitops preserves the existing worktree (commit-to-branch) + provisions the fresh worktree → python-dev
implements per the build sequence → qa-engineer coverage review → py-architect signoff → gitops (commit,
2.2.0 bump, CHANGELOG, merge). The plan promotes to
`home-assistant-apps/docs/plans/2026-06-08-py-ddns-insecure-skip-verify.md` (committed with the
implementation; archived to `docs/plans/archive/` on merge).

## Critical files (absolute)

- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/httpclient.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/models.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/config.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/updater.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/providers/__init__.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/__init__.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/config.yaml`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/translations/en.yaml`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/README.md`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/fixtures.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/check/config_checks.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/check/secrets_leak.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/check/debug_trace.py`
- `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/check/confirm.py`
- (possibly new) `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/check/tls_scope.py` + registration in `pyddns/check/__init__.py`
- Verified-unchanged: `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/__main__.py`,
  `/Users/chris/Documents/Projects/IT/home-assistant-apps/py-ddns/pyddns/providers/url.py` (the cert-skipping
  client reaches the GET by injection; no provider change)

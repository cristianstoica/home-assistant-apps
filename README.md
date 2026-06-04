# Py-Syslog — Home Assistant add-on

A durable, stdlib-only **UDP syslog collector** for Home Assistant. It receives
RFC 3164 / 5424 datagrams, resolves each sender IP to a `site`/`host`, and
writes one daily-rotated, gzip-compressed, retained file under `/data/log` —
with each stored line echoed live to the add-on **Log tab**. Collector only:
no search engine, no sensors. Storage failures are counted and surfaced, never
silently swallowed.

## Install

> **Requires Home Assistant with the Supervisor** (HA OS or Supervised). HA
> Container/Core have no add-on store.

1. In Home Assistant: **Settings → Add-ons → Add-on Store → ⋮ → Repositories**.
2. Add `https://github.com/cristianstoica/py-syslog` and close the dialog.
3. Find **Py-Syslog** in the store and click **Install**.
4. On the **Configuration** tab, add your sender mappings, then **Start**.

## Configure

Map each sender IP to a `(site, host)` stamped on every collected line:

```yaml
sources:
  - { ip: 192.0.2.1, site: home, host: router1 }
```

Point your devices' syslog forwarding at the Home Assistant host on the
configured `listen_port` (default UDP **5514**). A sender not listed in
`sources` is still received and written, stamped `unknown`/`<ip>`.

## More

See the [add-on README](py-syslog/README.md) for options, networking rationale,
rotation/retention/failure behavior, and the built-in `--check` self-validation.

## License

[MIT](LICENSE).

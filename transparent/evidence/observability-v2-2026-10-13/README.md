# Transparent monitoring moved to `/v2`, 2026-10-13

pir-apm and pir-monitor now watch the v12 history service and the one-query txid
display. Both are served under `/v2` on `https://transparent-pir.valargroup.dev`.
v1 (schema v11 history and `txid-2k`) is no longer monitored. Every Transparent
alert family runs in shadow. No shard server, publisher, display controller,
router or load was changed, and Enhance and Status alerting stayed active.
Roman approved the rollout in this session.

## What changed

**pir-monitor** (`wallet-pir-monitor-01`, about 03:15 UTC). Its probe family was
already in shadow.
- `pir-monitor-quality` was rebuilt from `1868e5a6` and now accepts a `txid`
  probe. The old binary is kept as `pir-monitor-quality.before-1868e5a6`.
- The `transparent` probe runs `quality-canary` from `1868e5a6` against the v12
  fixture below. The `txid` probe runs `txid-quality-canary` from `1868e5a6`
  against a pinned sealed entry,
  [`txid-quality-fixture-4k8.json`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/monitor/txid-quality-fixture-4k8.json).
  That fixture is case 0 of the
  [txid-4k8 lookups](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/txid-display-4k8-2026-10-13/raw/lookups), built by
  `txid-inventory fixture` from the publisher's files, never from a server answer.
- Probe configuration [before](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/monitor/root/obs-1868e5a6-backup/service-probes.json)
  and [after](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/monitor/etc/pir-monitor/service-probes.json).

**v12 canary fixture.**
- `activity-query-fixture.py` (now v12-aware, `1a1cae3e`) exported 233 sealed
  tables from the live v12 publication at height 3,515,907
  ([candidate](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/source-candidate.txt)). The export covers recent
  and archive geometries, `directory` and `pages-<j>`.
- `transparent-canary-pins.py` checked every table against that map. It derived
  the anchor, block 3,499,356 `000000000083e912…8591`, and the node returned that
  hash at that height.
- Files: [`pins.json`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/pins.json) and the fixture
  ([`fixture-v12.json`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/fixture-v12.json), SHA-256 `5a4e1fe7…5f5b`).

**pir-apm and the host sampler** (coordinator, under the production lock, after
the txid-4k8 verify released it at about 04:00 UTC).
- pir-apm from `1868e5a6`, then from `5ad1ef61`, through later-sorting ExecStart
  drop-ins in [`pir-apm.service.d/`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/pir-apm.service.d).
- [Switch script](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/switch-apm.sh) and
  [follow-up](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/apm-5ad1ef61.sh).
- The Transparent config moved to the services list:
  [before](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/backup/transparent-production.json),
  [after](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/transparent-production.json).
  - `transparent`: v12 history. Publisher `10.142.0.3:8096`, the v12 roster,
    edge metrics from the v12 router's Caddy, `min_recent_replicas` 1.
  - `txid`: controller `127.0.0.1:8098` and worker `10.142.0.18:8095`.
- The host sampler moved to the `1868e5a6` script with explicit hosts and
  per-unit memory: [before](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/backup/hosts.json),
  [after](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/hosts.json). It covers the coordinator, the v12 host
  (both workers and Caddy), the public router and the txid site.
  [Dry run before the switch](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/dry/hosts.json).
- `5ad1ef61` added `min_recent_replicas`. Under the `1868e5a6` binary, the
  inherited "fewer than two recent replicas" warning fired permanently, because
  the v12 fleet runs one recent worker by design. It was in shadow and never
  notified.

## Verified

- The canaries were run by hand before the switch:

  | Canary | Where | Result |
  |---|---|---|
  | Txid | build host, twice | passed (0.06 and 0.04 s) |
  | Txid, wrong expected entry | build host | `answer_mismatch`, exit 1 |
  | v12 history, with node anchor check | monitor host | passed (1.27 s) |
  | Txid | monitor host | passed (1.07 s) |

- [Monitor status](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/monitor/monitor-status.json) after the switch:
  `transparent` and `txid` both passing, beside `status` and `receiver`.
- The APM ([transparent](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/apm-quality-transparent.json.gz),
  [txid](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/raw/coordinator/apm-quality-txid.json.gz)) shows 5 of 5 and 2 of 2
  sources fresh.
  - Node and published heights agree, with a lag of 0 blocks.
  - The public origin agrees on `/v2/shards/init` and `/v2/txid/init`.
  - The v12 publication cycle is p50 19.8 s and p95 29.0 s over 44 cycles.
  - `/apm/capacity/` lists every host and unit with its size, limit and peak
    memory.
- After the `5ad1ef61` restart, no incident was active or firing. Slack's last
  successful delivery stayed at 1791699297, two days earlier, so neither restart
  posted anything.

## Not yet covered

- **Worker preparation timings.**
  - v12 workers run `ff27d649`, which has no preparation histogram, so the v12
    prepare rows stay empty until their next rollout.
  - The txid worker already exports it.
  - Txid rebuild-stage rows fill as the APM sees cycles after its restart.
- **Capacity forecasts** need three days of daily peaks.
- **Activation.** No family is active. Activation needs 24 hours of shadow, then
  one forced fire-and-recover per family through the policy file
  ([remaining work](../../docs/remaining-work.md#transparent-monitoring-on-v2)).

## Incident

The first monitor binary broke the Enhance canary for nine hours; see
[incident-enhance-canary.md](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/observability-v2-2026-10-13/incident-enhance-canary.md).

## Rollback

- **APM:** remove `zzzzzzzzzzzzzzzzzzzzz-apm-obs-1868e5a6.conf` and
  `zzzzzzzzzzzzzzzzzzzzzz-apm-5ad1ef61.conf` from `pir-apm.service.d`. Restore
  `/root/obs-1868e5a6/backup/transparent-production.json` to `/etc/pir-quality/`.
- **Sampler:** remove `pir-quality-hosts.service.d/zz-obs-1868e5a6.conf` and
  restore `/root/obs-1868e5a6/backup/hosts.json`.
- **Coordinator:** run `daemon-reload` and restart both units.
- **Monitor:** restore `/root/obs-1868e5a6-backup/service-probes.json` and
  `pir-monitor-quality.before-1868e5a6`, then restart `pir-monitor`.

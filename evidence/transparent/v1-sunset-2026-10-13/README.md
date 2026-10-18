# Wallet API v1 retired, 2026-10-13

`/v1/*` on `https://transparent-pir.valargroup.dev` answers 410 since 17:13:50 UTC.
The v11 history fleet and the `txid-2k` display site no longer serve. `/v2/` (schema
v12 history and the one-query `txid-4k8` site) is the only wallet API. Roman approved
the sunset, the journal handover and each step in this session.

The cutover took `/v2` history metadata down for about 11 minutes (below).

## Why it was safe to retire

- **Wallet.** chainapsis/vizor-wallet#977 is merged into Vizor `main`. Vizor main pins
  zakura-core/wallet-libraries `391d0b02`, which uses the wallet-pir transparent crates
  0.1.0-rc.3. Those crates request only `/v2/shards/…`, `/v2/filters/shards/…` and
  `/v2/txid/…`. The `/v1/filters/*` client in `transparent-filter` is not used by the
  wallet.
- **Traffic.** Over 30 minutes (15:15:03–15:45:07 UTC), the three v11 workers counted
  4,505 directory queries and 4,507 page queries. Over the same window, the 5 QPS load's
  own log recorded 4,505 and 4,507. No setup downloads arrived; a wallet would have
  needed them before querying. The router keeps no access log, so the counts come from
  each worker's `pir_http_arrivals_total`, read through the router
  ([start](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/traffic/g0-start.txt), [end](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/traffic/g0-end.txt),
  [delta](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/traffic/g0-delta.txt), [load log count](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/traffic/g0-load.txt),
  [script](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/traffic/g0.sh)). The `txid-2k` site's own traffic was not counted.

## What changed

**Router (`transparent-pir-router-01`, 17:13:50).** The v11 fleet's adapter no
longer owns it. `transparent-public-router.py` from wallet-pir `c9262e84` installed a
shell that inlines one route file per version
([deployment](../../../docs/transparent/deployment.md#public-router-and-api-versions)):
- `/v1/*` answers 410 with `{"error":"api_version_retired","retired":"/v1","use":"/v2"}`;
- `/v2/*` routes are byte-identical to the previous `api-v2` import.

Files: [rendered router](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v1-sunset-20261013/router-shell.caddy),
[previous router](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v1-sunset-20261013/router-Caddyfile), and its
[config](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/opt/transparent-public-router/router.json).

**Coordinator (17:13:47–17:13:49).** These units were stopped and disabled
([log](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v1-sunset-20261013.log),
[state before](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v1-sunset-20261013/units-before.txt)):
- `transparent-5qps-continuous` (the v11 load);
- `transparent-fleet-actuator.timer`;
- `transparent-fleet-scaler`;
- `transparent-replica-reconciler`;
- `transparent-control-sessions`;
- `transparent-publish-controller` (v11);
- `transparent-txid-display-controller` (`txid-2k`).

**Journal writer.** The v11 publish controller was also the writer of the shared
journal `/srv/transparent-activity/full-v3/journal`, which the v12 controller only
followed. The v12 controller now writes it:
- `follow_journal` is false in `/opt/transparent-publisher/v12/controller.json`;
- the drop-in [journal-writer.conf](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/etc/systemd/system/transparent-v12-publish-controller.service.d/journal-writer.conf)
  makes the journal writable under the unit's `ProtectSystem=strict`.

The `txid-4k8` controller writes its own journal (`journal-v3`) and was not touched.

**v1 workers (17:26:18–17:26:24).** `transparent-shard-server` was stopped and disabled
on recent-01 (10.142.0.10), recent-02 (10.142.0.8) and archive-03 (10.142.0.7), and
`transparent-txid-display-worker` on txid-display-01 (10.142.0.6)
([log](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v1-sunset-20261013/workers-stopped.txt)). The droplets are kept
until 48 hours have passed, for rollback.

## Incident: `/v2` history metadata down 17:13:49–about 17:24:45

The v12 unit had been written for a follower. It has `ProtectSystem=strict`, with the
journal under `ReadOnlyPaths`. Restarted as writer, the controller exited at once with
`Read-only file system`, and systemd restarted it 201 times. While it was down, the
publisher returned no metadata, so `/v2/shards/init` and `/v2/filters/shards` answered
502. Shard setup and query paths and `/v2/txid` kept serving.

- **Detection.** The cutover script waited up to 7.5 minutes for v12 to report
  `serving`. When the wait ran out, it logged nothing and exited 1 at 17:21:30, on a
  `fuser` that found no writer. The outage was found at about 17:24 by the external
  check (`/v2/shards/init` 502).
- **Recovery.** v12 went back to follower (the saved `controller.json`), and
  `/v2/shards/init` answered 200 at about 17:24:45. With no writer, the journal stayed
  at 3,516,585. The drop-in was then added, and v12 restarted as writer at 17:25:15
  ([script](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v12-writer.sh), [log](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v12-writer.log)). It
  caught up to the node (3,516,595) and published a new map. It published 3,516,596
  on its own by 17:27.
- **What would have caught it.** Before giving a follower the writer role, check the
  unit's sandbox, not only its configuration. And after any publisher restart, check the
  public `/vN/shards/init`, not only the process.

APM was in shadow for every Transparent family, so no alert fired.

## `/v2` load gate and continuous load (18:59–19:10 UTC)

**Continuous load.** `transparent-v2-5qps-continuous` replaces the v11 load
([unit](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/load/etc/systemd/system/transparent-v2-5qps-continuous.service)). It runs
`transparent-quality-load.py` from `c9262e84` with these settings:
- `--api v2` against the public origin;
- the v12 fleet and publisher (`10.142.0.3:8096`);
- both v12 workers pinned to binary `b89ebae3…` ([pins](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/load/srv/transparent-activity/canonical-load/v12/pins.json)).

Its fixture ([fixture.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/load/srv/transparent-activity/canonical-load/v12/fixture.json),
sha256 `2136cf13…`) was exported by `activity-query-fixture.py` from `c9262e84`. It holds
233 sealed tables from the 3,516,596 publication: 21 recent directory, 26 recent page,
12 archive directory and 174 archive page tables. `rate-query` was built from
`c9262e84` (`release-fast`, `x86-64-v3 +pclmulqdq`; sha256 `2d9f04dd…`) on
`roman-ipir-bench-8vcpu`.

**Gate.** It had the same shape as the accepted 2026-09-29 gates: three 7 QPS
`rate-query` clients for 600 s on the coordinator, on top of the continuous 5 QPS load
([script](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/load/root/v2-gate.sh), [clients](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/load/srv/transparent-activity/v2-gate-20261013-run2),
[summary](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/load/summary.txt)).

| | Queries | Rate | p50 | p99 | max | Errors / missed |
|---|---|---|---|---|---|---|
| Gate clients | 12,573, all exact | 20.99 QPS | 11 ms | 38 ms | 0.72 s | 0 / 0 |
| Recent tier | 10,059 | | 10 ms | 34 ms | | |
| Archive tier | 2,514 | | 15 ms | 48 ms | | |
| Continuous load, last minute | 300 exact | 5.0 QPS | 7 ms | 22 ms | | 0 / 0 |

The criterion (p99 < 2 s, p50 < 700 ms) passes. Times are whole-query client times,
including encryption and HTTPS through the public router.

An earlier attempt at 18:51 is not counted. It ran one 20 QPS client under
`CPUQuota=400%` with a permit file that was never refreshed. The client paused and
missed 257 slots ("client admission delayed beyond one interval"), while server time
stayed under 40 ms at p99. Its raw output was not retained.

## v1 droplets destroyed (2026-10-14, about 15:50 UTC)

After about 22 hours of clean `/v2` serving, Roman ended the 48-hour rollback window
early and approved the saved plan.
- **Applied:** 4 droplets destroyed (recent-01, recent-02, archive-03, txid-display-01),
  plus txid-display-01's project entry; the wallet project was updated
  ([plan](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/destroy/plan.txt), [apply](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/destroy/apply.txt)).
- **Check after apply:** a full plan showed "No changes"
  ([after](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/destroy/plan-after.txt)). `/v2` served throughout.
- **Rejected first plan.** The first plan also removed the shared worker firewall's 8095
  rule, which still admits the public router to the `/v2` txid host. That host's own
  firewall admits 8095 only from the coordinator. The plan was discarded unapplied and
  `transparent_txid_display_port_enabled` stays `true`
  ([rejected plan](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/destroy/plan-rejected-8095.txt)).
- **Variables now set:** `transparent_recent_count = 0` and `transparent_archive_names = []`
  in `/etc/enhance-pir/production.tfvars`, and
  `transparent_txid_display_archive_count = 0`. The originals are in
  `/root/v1-destroy-20261014/`.

There is no rollback to v1 any more.

## Rollback (until 2026-10-14; no longer possible)

Within 48 hours, [v1-sunset-rollback.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v1-sunset-2026-10-13/raw/coordinator/v1-sunset-rollback.sh)
restores:
- the previous router file;
- v12 as follower (remove the drop-in as well);
- the v11 coordinator units.

The workers must then be re-enabled on the four hosts. After the droplets are
destroyed, there is no rollback.

## v1 state deleted on the coordinator (2026-10-14, about 17:05 UTC)

Before deletion, no open file and no enabled unit used these paths; the v12, `txid-4k8`
and `/v2` load processes use none of them. Roman deleted:
- `/srv/txid-display-genesis/journal-v2x` and `/srv/txid-display-genesis/v2` (the
  `txid-2k` journal and root);
- `/srv/transparent-activity/canonical-load/v11*` (the v11 load fixtures);
- `/opt/transparent-publisher/v11*` and `/opt/transparent-txid-display`.

Afterwards the `/srv/txid-display-genesis` volume had 66 GB free and `/` had 44 GB free.
The eight retired unit files and 18 displaced copies of the v11 scaler and reconciler
units were moved, not deleted, to `/root/v1-destroy-20261014/retired-units/`. The
finished one-shot jobs that pointed at the deleted programs were cleared. `/v2` served
throughout, and the v12 controller kept publishing (3,517,715 at 17:05:54).

## Not done here

- Deleting the v1-only tooling: the pre-v11 `/v1` paths in `transparent-Caddyfile` and
  `deploy-transparent-shard.sh`.
- The v12 unit still orders itself after the retired `transparent-publish-controller`;
  the line has no effect.

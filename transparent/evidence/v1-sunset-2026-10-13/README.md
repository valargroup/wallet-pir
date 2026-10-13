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
  ([start](raw/traffic/g0-start.txt), [end](raw/traffic/g0-end.txt),
  [delta](raw/traffic/g0-delta.txt), [load log count](raw/traffic/g0-load.txt),
  [script](raw/traffic/g0.sh)). The `txid-2k` site's own traffic was not counted.

## What changed

**Router (`transparent-pir-router-01`, 17:13:50).** The v11 fleet's adapter no
longer owns it. `transparent-public-router.py` from wallet-pir `c9262e84` installed a
shell that inlines one route file per version
([deployment](../../docs/deployment.md#public-router-and-api-versions)):
- `/v1/*` answers 410 with `{"error":"api_version_retired","retired":"/v1","use":"/v2"}`;
- `/v2/*` routes are byte-identical to the previous `api-v2` import.

Files: [rendered router](raw/coordinator/v1-sunset-20261013/router-shell.caddy),
[previous router](raw/coordinator/v1-sunset-20261013/router-Caddyfile), and its
[config](raw/coordinator/opt/transparent-public-router/router.json).

**Coordinator (17:13:47–17:13:49).** These units were stopped and disabled
([log](raw/coordinator/v1-sunset-20261013.log),
[state before](raw/coordinator/v1-sunset-20261013/units-before.txt)):
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
- the drop-in [journal-writer.conf](raw/coordinator/etc/systemd/system/transparent-v12-publish-controller.service.d/journal-writer.conf)
  makes the journal writable under the unit's `ProtectSystem=strict`.

The `txid-4k8` controller writes its own journal (`journal-v3`) and was not touched.

**v1 workers (17:26:18–17:26:24).** `transparent-shard-server` was stopped and disabled
on recent-01 (10.142.0.10), recent-02 (10.142.0.8) and archive-03 (10.142.0.7), and
`transparent-txid-display-worker` on txid-display-01 (10.142.0.6)
([log](raw/coordinator/v1-sunset-20261013/workers-stopped.txt)). The droplets are kept
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
  ([script](raw/coordinator/v12-writer.sh), [log](raw/coordinator/v12-writer.log)). It
  caught up to the node (3,516,595) and published a new map. It published 3,516,596
  on its own by 17:27.
- **What would have caught it.** Before giving a follower the writer role, check the
  unit's sandbox, not only its configuration. And after any publisher restart, check the
  public `/vN/shards/init`, not only the process.

APM was in shadow for every Transparent family, so no alert fired.

## Rollback

Within 48 hours, [v1-sunset-rollback.sh](raw/coordinator/v1-sunset-rollback.sh)
restores:
- the previous router file;
- v12 as follower (remove the drop-in as well);
- the v11 coordinator units.

The workers must then be re-enabled on the four hosts. After the droplets are
destroyed, there is no rollback.

## Not done here

- `/v2` load gate and the continuous load on `/v2`: see
  [remaining work](../../docs/remaining-work.md#wallet-api-versions-on-one-origin).
- Destroying the four droplets, deleting v11 state on the coordinator, and deleting
  the v1-only tooling.

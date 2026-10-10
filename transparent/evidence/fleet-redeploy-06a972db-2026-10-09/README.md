# Transparent history and txid display on `06a972db`, 2026-10-09

The history workers and the txid display service were moved to the full-CI release of
`main` at `06a972db` (run 37974719227). Their servers now accept a 44-bit dithered
query beside the 49-bit one and advertise it in init (`directory_scheme_dq44`,
`pages_scheme_dq44`; display `scheme_dq44`), so clients built from this source switch
to 44 bits. Roman chose to certify every served segment at both widths first; all 608
passed ([certificates](../served-segment-certificates-2026-10-09/README.md)).

## What changed

| Component | Before | After |
|---|---|---|
| history `transparent-shard-server` (recent-01, recent-02, archive-03) | `34ba7ebb…` (`a317455e`) | `2490c09d…` |
| `shard-control`, `shard-assign`, `transparent-publish-controller` | — | byte-identical in the new bundle; not reinstalled |
| txid display workers (`transparent-txid-server`, archive txid-display-01 and recent-01) | `3ba0ab47…` (`d730ed6b`) | `4194b7af…` |
| txid display controller | `96bf18d4…` | `6bfec14e…` |

History used the morning's [manual v11 procedure](../fleet-redeploy-2026-10-09/README.md)
from `/root/deploy-06a972db` on the coordinator (step scripts and numbered logs under
`raw/coordinator/`); the operations scripts it calls are byte-identical between
`a317455e` and `06a972db`. Txid display used `wallet-pir-deploy.py txid-display-*` with a
new request (`raw/txid/request-v3.json`, digest `505e6e20…`, plan `67f42ea7…`).

| UTC | Step | Result |
|---|---|---|
| 23:48 | Health baseline | membership fresh, 3 serving, warm 18/18/164; controller at the tip, freshness 11 s; load running |
| 23:49–23:53 | Stage and `--verify-only` on each history worker, no restart | recent 8 s each, archive-03 216 s |
| 23:53–23:54 | 5 QPS load paused; recent-01 then recent-02 rolled (other-serving floor 1, as decision D2 of the morning roll); load re-pinned and restarted | 25 s and 23 s; warm 18/18; no recent outage |
| 23:54–23:57 | Load on mixed versions | 860 exact, 0 errors, p99 ≤ 28 ms |
| 23:57–00:07 | archive-03: load paused, verify-only again, restart from the disk runtime cache, re-pin, load restarted | install 551 s; archive shards 0–81 out of service 00:01:20–00:06:58 (**338 s**): shard set 223 s, 164/164 runtimes restored in 112 s, 0 failures |
| 00:07–00:10 | Load on the new fleet | 879 exact, 0 errors, p99 ≤ 45 ms, freshness 9–10 s |
| 00:09 | txid display `stage` | release staged on coordinator, archive and recent; every executable runs |
| 00:16–00:43 | txid display `stop`, `workers`, `route`, `controller` | `/v1/txid/` 503 from about 00:19 to 00:37 (**about 18 min**); archive worker warm 425/425, recent 1/1; controller back at the history tip |

## Validation

- **44-bit history queries.** `rate-query` built at `06a972db` (`dd285190…`) ran 180 s at
  5 QPS from `roman-ipir-bench-8vcpu` against `https://transparent-pir.valargroup.dev`
  with the v11 load fixture (`705770a6…`), 00:44:29–00:47:30. 896/896 exact, no retries.
  Every request had the 44-bit size: archive pages 388,104 B (49-bit would be
  429,064), archive directory 207,880, recent directory 50,184, recent pages 72,712.
  p50 9–19 ms, p99 23–29 ms by class ([summary](raw/dq44-check/summary.txt)).
- **44-bit txid lookup.** `txid_live_lookup` at `06a972db` passed before the upgrade
  with 80,400 B up (49-bit fallback) and after it with 77,840 B up (44-bit), both an
  archive-tier lookup of the pinned transaction (`raw/txid/txid-live-*.log`).
- **49-bit clients.** The 5 QPS load (v11 canonical-load binary, 49-bit) stayed exact
  throughout, as above.
- **Identity.** All three history workers report `2490c09d…` on `/v1/ready`, both txid
  workers `4194b7af…`, and the controller process is `6bfec14e…`.

## Operational notes

- On this run the deploy tool's lock SSH sessions were twice left open on the
  coordinator after the tool exited (after the txid `workers` phase and after the
  Enhance deploy recorded in
  [the Enhance record](../../../enhance/evidence/production-deploy-06a972db-2026-10-09/README.md)),
  holding `/run/lock/wallet-pir-production.lock` until ended by hand. The later phases
  ran through `raw/txid/txid-phase.sh`, which ends such a session from the same client
  address after the phase exits.
- The 5 QPS load must be paused around history worker restarts and re-pinned after
  (`raw/coordinator/pins.json.before` holds the previous pins).

## Rollback

- History workers: on each worker `/opt/transparent-publisher/rollback/roll-06a972db/<worker>/`
  holds the previous binary, `shard-control` and unit; `steps/p7-rollback.sh` restores
  archive-03 (another ~6 min archive outage). Re-pin the load to `34ba7ebb…` afterwards.
- Txid display: `txid-display-rollback --transaction <id>` newest first (controller,
  route, workers, stop), with the ids printed in `raw/txid/`; the `d730ed6b` release
  is still staged.

## Not established

Not a soak or a 20 QPS gate; three minutes of 44-bit history queries at 5 QPS and one
txid lookup. No wallet regression or Vizor sync was run on the 44-bit path.

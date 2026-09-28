# Schema v9 production cutover, 2026-09-28

The transparent fleet moved from the `transparent-shard-v7` publication to a new
`transparent-shard-v9` publication: 14-byte salted script tags, 87-byte events,
the `zcash-transparent-range-v2` filter profile, `recent-4k-8k` for the recent
tier and directory choice tables on every shard. CI was skipped at the
operator's request; binaries were built on the coordinator and every step ran
over SSH with the workflows' scripts and inputs. Commands, binaries and hashes
are in [manifest.json](manifest.json).

Public metadata was withdrawn from **15:30:53 to 16:31:20 UTC (about 60
minutes)**. Wallets built before v9 cannot read the new publication.

## Sequence

1. Publication from the version-2 journal
   ([journal evidence](../journal-v2-conversion-2026-09-28/README.md)) through
   3,499,274: 139 shards (126 `archive-wide`, 13 `recent-4k-8k`), 48 GiB, 18 min
   23 s, 3.06 GB peak RSS. The v7 set had 174 shards.
2. `shard-verify` against the publication record, with 8 shards rebuilt from the
   journal: every check passed and every rebuilt filter, directory and page table
   was identical ([verify.log](verify.log)).
3. `fleet-preflight` shipped each worker its subset and verified it with the
   staged v9 binary; nothing was activated ([log](fleet-preflight.log)).
4. The v7 controller and reconciler were stopped (window start), then
   `fleet-deploy` in schema-cutover mode.
   - [Attempt 1](fleet-deploy-attempt1.log) stopped before activation on an
     unset `HOME` under systemd-run. Nothing changed.
   - [Attempt 2](fleet-deploy-attempt2.log) activated both archive owners and
     two recent replicas on v9, then an undeferred public probe after the first
     replica pair failed because metadata was withdrawn by design. The script
     rolled every worker back to v7, and all six returned warm. Fixed in
     `ab0e824e`.
   - [Attempt 3](fleet-deploy.log) completed: all workers warm on v9, router
     switched.
5. The publisher deployment in shadow restored publication control on every
   worker and verified a candidate ([log](publisher-shadow.log)); activation
   served it and both public origins returned the same map
   ([log](publisher-activate.log)).
6. The public regression ran with a fixture re-exported for this publication:
   **11/11 cases, 68/68 checkpoints exact**
   ([report](regression-report.json)).

## Measurements

| Step | Result ([deploy-timings.tsv](deploy-timings.tsv)) |
|---|---|
| Cold build, archive owners in parallel (attempt 2) | 866 s and 1,187 s for 126 runtimes each |
| Cold build, recent replicas (attempt 2) | 124–196 s for 26 runtimes |
| Activation from the persisted runtime cache (attempt 3) | archive 243–255 s; recent 23–124 s; all workers 437 s |
| Preflight verification of prepared subsets | 154–161 s |

The cold-rebuild figures come from the production rollout, not from a bench
rehearsal. They are one run each.

## Regression fixture

The frozen fixture pins the served set's sealed entries, so the v7 fixture
refused the v9 publication as drift. `regression-export` re-exported the same
case specification from the version-2 journal against the served v9 map.
`compare-regression-fixtures.py` found no blocking or review differences: every
shared checkpoint reduces identically, and cases and events are byte-identical;
only the map binding, accepted headers and source changed
([comparison](fixture-compare.json)).

## Rollback and limits

- Retained for rollback: `/srv/zakura/transparent-shards-v7-full`, the v7
  publication root at `/srv/zakura/transparent-publications-v7`, the v1 journal,
  the v7 filter-server binary at
  `/opt/transparent-publisher/rollback/transparent-filter-server.v7`, each
  worker's `active.json.v7`, the fleet transaction backups and the v7 runtime
  caches (pruning was deferred).
- At activation the controller reported one ready recent replica, as under v7
  before the cutover; the reconciler admits the others as they catch up.
- A [pinned-anchor load run](load/README.md) repeated the v7 baseline after the cutover: 45/45 exact. No capacity series has been run against v9.
- wallet-libraries was ported on `m3/wallet-correctness` (`2be3d343`). It was not
  run against this deployment.

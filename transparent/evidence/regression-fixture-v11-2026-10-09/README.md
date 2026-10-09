# Regression fixture re-cut for schema v11, 2026-10-09

The public wallet regression had stopped at preflight since the v11 cutover on
2026-10-03: its fixture pinned the v10 map, and its expected events lacked the
transaction metadata v11 events carry. This run re-cut the case specification,
exported a new fixture from the live v11 journal against the map both public
origins served, compared it with the v10 fixture, and ran the regression against
production. **It passed 11/11 cases and 79/79 syncs** with no failed or retried
request. [Manifest](manifest.json) holds every hash, time and measurement.

Nothing was published, deployed, restarted or reconfigured, and the production
lock was not taken. The coordinator work started after the `a317455e` fleet
redeploy had released the lock and its processes had exited.

## What ran

| UTC | Step | Result |
|---|---|---|
| before 15:10 | `recut-regression-cases.py --self-check --anchor 3511700 --cutoff 3289805` on the Mac | self-check reproduced the v10 specification; 11 cases, 57 checkpoints ([log](raw/recut.log)) |
| 15:10–15:11 | `git archive` of `main` `66b0b9fb` to a scratch tree on the coordinator | first extraction truncated (below); re-extracted tree equals a fresh archive file by file |
| 15:11:57–15:14:18 | Build `regression-export`, `release-fast`, under 12 GiB, no swap, 200% CPU, nice 19, idle I/O | executable `2448cd1e…` ([log](raw/coordinator/build.log)) |
| 15:14:22 | Served map from both origins | identical bytes `2a8709b2…`, 91 shards, tail shard 90 through 3,511,887, no `recuts` ([map](raw/served-map/shards-3511887.json)) |
| 15:14:32–15:18:05 | Read-only replay of `/srv/transparent-activity/full-v3/journal` through 3,511,887, same limits | exit 0, 3 min 33 s, 33% CPU, 325 MiB peak RSS; fixture `a6115f1a…` ([stderr](raw/coordinator/export.stderr)) |
| after 15:18 | Comparison with the v10 fixture | 0 blocking with `--event-metadata`; 46/46 v10 checkpoints reproduce from v11 events ([log](compare/fixture-compare.log)) |
| 15:33:32–15:40:42 | `transparent-regression` from roman-dev-2 against both public origins | 11/11 cases, 79/79 syncs, 2,674 requests, 0 failed, 0 retried ([report](regression/report.json)) |

The coordinator wrapper is [run-on-coordinator.sh](raw/coordinator/run-on-coordinator.sh),
with [build.sh](raw/coordinator/build.sh) and [run-export.sh](raw/coordinator/run-export.sh).
Memory and disk before and after the replay are in `raw/coordinator/headroom-*.txt`:
about 53.7 GB available memory throughout, root at 76–77%, the journal filesystem at
41%. The build tree was removed afterwards.

## The re-cut

The script set of every case is unchanged. What moved:

- **Anchor** 3,473,686 → 3,511,700, about 190 blocks below the served tail at export.
  It lies in unsealed shard 90, so the terminal syncs exercise the provisional rule.
- **Tier probes** 3,262,749 → 3,289,805, the first height of the served map's
  `recent-4k-8k` shard 82. `recent-birthday`'s birthday moved with them; its first
  activity is at 3,290,754, so the exporter's omission check passed.
- **Sampled checkpoint** 3,492,693, added to every case from the span published since
  the last cut. It lies inside shard 89, which continuous publication sealed at
  revision 901.

`recut-regression-cases.py` now records this cut (previous anchor 3,511,700, cutoff
3,289,805, the sampled height as an absolute checkpoint), and its `--self-check`
reproduces the frozen specification exactly.

## Comparison with the v10 fixture

v11 events append a flags byte and metadata varints to the 87-byte v10 record;
all 4,096 final events here are 89–94 bytes. A plain comparison therefore reports
18 blocking "different expected state" findings at shared checkpoints ([log](compare/fixture-compare-without-metadata-mode.log)).
The new `--event-metadata` mode compares events by that 87-byte prefix and
everything else exactly: **0 blocking** ([findings](compare/fixture-compare.json)).
Two cases share no height with v10, because all their checkpoints follow the cutoff
or anchor, so [a separate check](compare/v10-checkpoints-from-v11-events.py) filtered
each case's v11 events to every v10 checkpoint height: **all 46 matched** v10's
expected events ([result](compare/v10-checkpoints-from-v11-events.json)).

Review findings, each accepted:

| Case | Change at the anchor | Decision |
|---|---|---|
| small-active | 1 → 2 events, balance 0.754 ZEC → 0 | spent its output; receive checkpoints 3,423,034–036 unchanged |
| active-p2sh | 1 → 2 events, balance 0.01 ZEC → 0 | spent its output; still an active P2SH script |
| reused-pages | 106 → 647 events, 2 → 15 UTXOs | kept transacting; heavier but bounded |
| coinbase | 1,643 → 2,937 events, 3 → 27 UTXOs | the same miner kept mining; checkpoints stay on real coinbase heights |
| multi-script-self-transfer, recent-birthday | no shared checkpoint height | covered by the 46-checkpoint event check |

The unused cases still have no activity at 3,511,700.

## Production regression

The runner, `b6880524…`, was built on roman-dev-2 from the same `66b0b9fb` archive
and given the frozen fixture. Both origins served the same map at preflight
(`ef05eb81…`, tail revision 1078 through 3,511,907, zero skew). The tail was
republished four times during the run, up to revision 1082 through 3,511,911; every
sync re-pinned 90 sealed entries and the tail's continuity.

| Case | Syncs | Requests | Sync seconds |
|---|---:|---:|---:|
| unused-p2pkh | 7 | 211 | 18.8 |
| unused-p2sh | 6 | 202 | 14.3 |
| small-active | 7 | 225 | 19.1 |
| zero-balance | 7 | 223 | 21.8 |
| old-receive-recent-spend | 9 | 229 | 24.7 |
| offline-receive-spend | 9 | 221 | 22.1 |
| active-p2sh | 7 | 223 | 18.8 |
| reused-pages | 7 | 108 | 22.6 |
| multi-script-self-transfer | 7 | 561 | 61.3 |
| recent-birthday | 7 | 77 | 12.4 |
| coinbase | 6 | 392 | 53.7 |

All 2,672 case requests and 2 preflight requests succeeded at their first attempt.
34 syncs ended at a provisional anchor (in shard 90); their repeats read only the
tail. Sync time totals 289.7 s; requests from the hub took roughly 0.45–0.9 s each, so
these times are client round trips, not service capacity. The 22 SQLite stores (479 MB) stay on
roman-dev-2 with [checksums](regression/sqlite-sha256.txt) and [sizes](regression/sqlite-sizes.txt).

## Failed attempts

- **Truncated source transfer, 15:10.** Two copies of the wrapper, one started by
  Roman and one by the agent, wrote the same local archive at once. One extracted a
  truncated archive (3,459 of 4,856 files, gzip unexpected EOF); the other stopped at
  the existing-directory check. The tree was re-extracted from the complete archive,
  and its per-file digest `a7bf79cf…` equals a fresh `git archive` of `66b0b9fb`.
- **Mac runner never started, 15:24.** It waited in `dyld` behind `syspolicyd` with zero
  CPU for about nine minutes, was stopped by PID before sending any request, and the
  run moved to roman-dev-2. The SSH session that launched that run later timed out at
  the client; the remote log records exit 0 at 15:40:42.

## Limits

- Expectations and the publication share journal and ingest provenance. This detects
  publication, retrieval and wallet regressions, not ingest errors.
- Cases are synthetic groupings of public scripts, not wallets or population weights.
- One run of the reference HTTP client and SQLite store; not capacity, application or
  lifecycle qualification.
- A future declared re-cut of the publication will renumber pinned entries and needs a
  new export.

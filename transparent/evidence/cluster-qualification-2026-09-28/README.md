# Cluster qualification baseline, 2026-09-28

Plan step A1: exact qualification of the deployed cluster with this
repository's clients. This is the baseline for the currently served v7
publication: workers on `9d47b05b`, directory choice tables on new tail and
sealed revisions.

| Field | Value |
|---|---|
| Target | `https://transparent-pir.valargroup.dev` (shards), `https://enhance-pir.valargroup.dev` (filters); the live production cluster |
| Client host | `roman-ipir-bench-8vcpu`, over the public internet |
| Clients | `transparent-regression` and `transparent-loadtest` built from the worktree at `46a82a62` plus the pinned-anchor change (landed in `ad71a4fe`), x86-64-v3 |

## Regression suite

`transparent-regression --fixture transparent/tools/transparent-regression/fixtures/mainnet.json --shard-url … --filter-url …`

- **Result:** all 11 cases and 68 checkpoints passed (`regression-live-1/report.json`, `passed: true`).
- **What it checks:** the fixture's independent outpoint reducer, at exact anchors up to 3,473,686. The sealed prefix covered 173 shards.
- **Cases:** unused P2PKH and P2SH, small active, zero balance, cross-tier old receive with recent spend, offline receive and spend, active P2SH, reused pages, a 40-script self-transfer, recent birthday, and coinbase (1,643 events).
- **Kept here:** per-case results and the log. The 1.1 GB of HTTP transcripts and wallet stores stayed on the host.

## Pinned-anchor load run

`transparent-loadtest --sample workload-sample-2026-09-08/sample.json --steps 4 …`, with the served tip past the sample's anchor. Every wallet synced to the sample's anchor (3,473,686), so the sample's expected digests apply exactly.

| Run | HTTP attempts | Syncs | Completed | Exact | Failed |
|---|---:|---:|---:|---:|---:|
| `pinned-live-1.json` (tip 3,498,940) | 1 | 66 | 59 | 59 | 7 |
| `pinned-live-2.json` | 3 | 47 | 45 | 45 | 0 |

- **Coverage:** all nine sample classes, archive restores included.
- **Run 1 failures:** "connection closed before message completed" on private queries during long heavy syncs, a stale keep-alive race. With the transport's transient retries (run 2) none recurred.
- **Run 2 incompletes:** two reused-tail syncs hit the 600 s per-sync timeout. The reused-tail p50 is 30 s, so this is expected for that class.

No completed sync was inexact in either run.

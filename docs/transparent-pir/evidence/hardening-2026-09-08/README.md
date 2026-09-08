# Worker hardening and wallet adapter — 2026-09-08

The implementation is committed; the broader deployment gate is **not complete**.
The single recent canary is running the new binary. Both public origins matched
and node, journal and publication reached 3,476,721 at 23:04:30 UTC. All workers
reported warm, but recent-02 and recent-03 remained on older publications. Only
recent-01 has the hardened worker binary; the other five retain their previous
binary. `after.json` records each identity rather than inferring rollout from
source.

## Changes and validation

Worker changes add shared admission for transient allocations, retain reservations
through cancellation of blocking work, trim free allocator pages on retirement,
remove failed cache slots safely, and collect disk runtime files only after their
active/candidate/retained readers are released. Verification budgets current warm
assignments. The reconciler stages lagging recent workers independently of the
publication quorum, then rechecks desired publication, withdrawal and canonical
endpoint under routing/worker locks before advertising a warm revision. It is
scoped to recent-01 during this canary.

The Zakura adapter (`bca43b343`, upstream client `22e6bec`) captures an independently
accepted scan target, persists both target and publication source anchors, retains
validated page progress, rolls back to an exact ancestor, and exposes amount and
coverage from one database snapshot through regenerated Rust/Dart bindings. The
balance card qualifies zero while coverage is behind or incomplete. Layout 4
rejects layout 3 without erasing the database; no migration was requested.

- 103 worker tests passed before disk collection changes; the affected 13-test
  revision/cache suite and two operator payload tests passed after those changes.
  The extended disk churn test also passed separately.
- 23 worker library tests passed on Linux with portable x86-64-v3 release flags.
- 33 operator tests passed, including catch-up withdrawal/reorg races, canary
  scoping, upgrade identity and late-arrival rejection by the monitor.
- 282 wallet Rust tests passed (five opt-in live tests skipped); the additional
  layout-3 rejection test passed separately.
- 49 wallet UI tests passed; generated bindings analyzed without issues; the
  macOS debug example app built.
- Worker strict Clippy passed. Wallet affected-package Clippy passed with the
  existing unrelated `large_enum_variant` lint explicitly allowed.
- Actual wallet-adapter HTTPS private queries decoded a recent row in 0.962 s
  and an archive row in 1.178 s. Their logs include setup/upload/response bytes.
- An independent-node-anchor fresh wallet recovery reached 3,476,726 in
  3.665 seconds; a repeat completed in 2.142 seconds, both complete with zero
  unresolved spends or pending pages. The test fixture expects no funds.
- The 60-second two-client canary smoke completed 662 exact private queries and
  observed a new public block after 14.686 seconds. This is a short smoke result,
  not the sustained gate.

## Failed attempts preserved

The first manual release inherited `target-cpu=native` from the coordinator and
failed the worker preflight with an invalid-opcode trap. Portable flags fixed
that. Subsequent preflights refused a full 10 GiB disk runtime cache. Pruning
unreferenced derived runtime files, while preserving control-reported revisions,
the active set and the fallback set, reclaimed 6,172,511,104 bytes. These preflight
failures did not replace the running service.

After installation, admission stopped warming at 17/28 runtimes because cgroup
charge included about 3.26 GB of file cache. A 5 GiB MemoryHigh let all 28 runtimes
warm in 13.9 seconds. Sustained query/build overlap then exceeded that soft limit
with anonymous memory alone and stalled prewarm for 89 seconds. The first soak
was explicitly stopped; its samples do not establish acceptance. MemoryHigh was
raised to 5.5 GiB, with the 7 GiB MemoryMax and allocation guards unchanged. The
second soak started at 23:03:30 UTC. It failed after 505.876 seconds: the public
service remained timely (maximum 19.363 seconds), but canary catch-up exceeded
30 seconds at block 3,476,727. It completed 5,971 exact queries with no OOM or
restart. The reconciler had treated a newly requested candidate as superseding
the still-current public authority, starving a completed canary revision during
a burst. Commit `7b76b39` makes catch-up follow the durable active publication
until activation actually replaces it; tests retain withdrawal/canonical and
stale-after-activation rejection. The third soak started at 23:15:22 UTC. Its
earliest possible completion is 05:15:22 UTC on September 9, and it must also
observe 300 new blocks. Neither earlier soak is a passing acceptance result.

The monitor checks exact private queries, both public origins, canonical public
and canary endpoints, visibility within 30 seconds, unchanged process identity
and OOM/restart counters, and at least 20% host memory headroom. It restarts its
block count on a detected reorg. A failed monitor cannot advance rollout. The
read-only unit is `transparent-hardening-canary-soak-3.service`; full samples and
the eventual result remain at the coordinator path in `manifest.json`. Wider
rollout and 24-hour monitoring remain in [remaining work](../../remaining-work.md).

## Reproduction

Worker tests: `cargo nextest run -p transparent-shard-server`; affected cache tests:
`cargo test -p transparent-shard-server --test revisions_and_cache --test operator_payloads`.
Operator tests: `python3 -m unittest discover -s ops/tests`.
Wallet tests: `cargo nextest run -p zakura-wallet-store -p zakura-wallet-sync -p zakura-wallet-facade -p zakura_wallet_bridge -p zakura-wallet-transparent --all-features --test-threads 4`.
The monitor command is `python3 ops/scripts/observe-transparent-hardening.py
--worker transparent-pir-recent-01 --binary-sha256 <manifest binary digest>
--query-binary <release soak-query> --out <fresh evidence directory>` on the
coordinator. Defaults require six hours, 300 blocks and two query clients.

The complete baseline and post-install capture include OS/CPU/RAM/disk facts,
network genesis, journal metadata, publication geometry/cutoff/policy and every
worker's metrics. These are dated measurements of one rollout, not forecasts of
wallet capacity or confirmation of any user's funds.

A partial snapshot at 23:17:01 UTC (`soak-3-progress.json`) records 1,380 exact
queries, one new block visible publicly in 18.780 seconds and on the canary in
22.915 seconds, peak sampled cgroup charge 5,904,982,016 bytes, and unchanged
process/OOM/restart counters. This is progress only; the full gate is still open.

Raw logs and NDJSON streams are stored as lossless `.gz` files; use `gzip -dc`
to inspect them. Compression preserves their original whitespace and bytes.

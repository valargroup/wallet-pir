# Transparent PIR status

Source inspection: 2026-09-09 through worker/qualification correction `f2f351c` and fleet burst-progress correction `7754d6c`. Live state is observed separately below; [remaining work](remaining-work.md) owns the outstanding release and capacity gates.

## M3 wallet correctness, fixture scope: source-verified 2026-09-10

The [M3 evidence](evidence/productionize-m3-2026-09-10/README.md) records the
wallet branch `m3/wallet-correctness` (from `0ce6d158f`, final
`7937d48df`). Every wallet history the plan names — receives, spends, a
coinbase, a self-transfer, a paged multi-script history, an old receive spent
across the tier boundary, an offline receive and spend, a zero-balance
history, unused and imported scripts, gap-limit discovery — is recovered
privately against the real shard service from a chain expressed as blocks and
compared exactly, event by event and through the wallet's own balance, outputs
and history, with a reducer that never sees an event or the ledger. Six
scripts from 1,152 real mainnet blocks recover the same way. Seven forks and
reorgs, twenty-two interruption rows, three process kills and one kill of the
real bridge library mid-query against loopback fixture services all leave
nothing committed lost, nothing called complete and the next run exact. A
shadow profile is read without being touched and compared by counts and
digests. Imported scripts live in their own address scope, the count of
scripts the tables cannot index is persisted and shown, a wallet reopened
mid-run reads `interrupted`, and discovery keeps widening while a spend is
unresolved. An independent reviewer's finding is part of the record.

**M3 is not closed.** The deployed accepted-anchor regression suite and the
application-level comparison on public paths are unexecuted because M1 is
open; no private query was sent to the public services. Opt-in PIR balances
remain non-authoritative and labelled so.

## M2 recovery application: source-verified 2026-09-09

The [M2 evidence](evidence/productionize-m2-2026-09-09/README.md) records the
wallet branch `m2/macos-recovery-beta` (from `b6aa1f97f`, final
`0ce6d158f`). The wallet's own store now appends a coverage
checkpoint by inserting one row: the matched baseline probe changed 2,002 SQL
rows after 1,000 checkpoints and the corrected store changes two. Incomplete
reasons are never silenced near the tip; only `scan-ahead` and
`publication-behind` may read as current within ten blocks. A wallet that has
read nothing reports no coverage rather than the block before its birthday.
The bridge is built without its `send` feature, so `send` and `quote` answer
`SendDisabled` (code 20) before a phrase is read, and the facade refuses to
open a recovery wallet without both transparent services over TLS on separate
hosts (`Configuration`, code 21). The application requires an explicit
`ZAKURA_MODE` of `demo`, `shadow` or `recovery`, refuses to run on native-load
failure, checks the light server's chain before opening, keeps each beta mode's
wallet under its own bundle container and profile marker, and refuses another
build's layout before touching its files.

The macOS release build `ZakuraRecoveryBeta.app` was produced in recovery mode
with real native bindings (`executable SHA-256 `851df2642730f402…`, ad-hoc signed`). A live restore, sync, stop, reopen
and resume ran through the same library against `us.zec.stardust.rest` and the
public transparent services with one fresh synthetic wallet
(`birthday 3,477,808, complete accepted target 3,477,838, 5.0 s first sync and 3.5 s resume with identical coverage after reopen`). This does not make PIR balances authoritative; M3 owns
that gate. M2 closes with the limits listed in the evidence.

## M1 unpublished-reorg correction: qualified source, 2026-09-11

The controller and fleet now distinguish invalidating a candidate above served
coverage from withdrawing an orphaned served publication. Retention requires an
explicit fleet acknowledgment after independent canonical identity and routed
worker checks under the routing lock. Worker invalidation always cancels the
preparation epoch, including when every served revision remains canonical.

[Regression and qualification evidence](evidence/productionize-m1-unpublished-reorg-2026-09-11/README.md)
records the failure against prior code, successful concurrent preparation and
deep-reorg tests, all 591 Rust tests and 101 operations tests. The Linux controller
build is in progress. This source is not deployed; the failed canary remains
stopped. M1 remains open, including the separate freshness margin concern.

## M1 unpublished-candidate withdrawal: observed 2026-09-11, 03:32 UTC

The incremental-writeback canary failed after 1,112.910 seconds and 21 blocks
when public routing was withdrawn; 16,101 queries were exact with six retries.
The supervisor is terminal failed and the other five workers were not promoted.
A one-block reorg invalidated a candidate at height 3,479,237; the last served
endpoint at 3,479,236 matches the subsequent canonical-hash check. Source
invalidation unconditionally withdraws routing even for journal-only forks.
[Evidence and regression plan](evidence/productionize-m1-incremental-writeback-2026-09-11/README.md)
separate this issue from the earlier fsync stall. Service recovered, but M1
remains open and no prior observation time counts toward its next attempt.

## M1 incremental-writeback canary: observed 2026-09-11, 03:14 UTC

Source `7621c34` passed local and Linux tests and all three Amsterdam burst
qualifications: maximum worker visibility 8.876 seconds and minimum corrected
modeled host headroom 25.963%. The [evidence](evidence/productionize-m1-incremental-writeback-2026-09-11/README.md)
preserves raw runs and explains the stricter host-overhead recalculation.

Recent-01 now runs binary `b2da68f289e4eebd3eafbc5b763963fa87c3daf347b8b2d312e1ac64ea97a38c`.
A fresh loaded canary began 03:13:32 UTC at node height 3,479,215 under
`transparent-m1-incremental-writeback-rollout.service` (PID 2836142 confirmed
active). Routing withdrawal baseline is 8 after planned upgrade maintenance.
Initial queries were exact and service warm; no sustained pass is claimed.
The other five workers remain gated on both six hours and 300 new blocks,
followed by the separate 24-hour all-worker observation. M1 remains open.

## M1 durable-write attribution: observed 2026-09-11, 02:49 UTC

The [completed traces](evidence/productionize-m1-stage-timing-2026-09-11/README.md)
attribute 7.717 seconds of a 7.732-second recent-01 activation to its active-record
file and directory fsync calls. Cache persistence was slow concurrently; its
causal contribution still requires a controlled comparison. The ten-minute
activation diagnostic passed but does not establish sustained acceptance.
An incremental cache-writeback source candidate is under qualification. It
preserves durability and is not deployed. M1 remains open with unchanged gates.

## M1 activation-delay investigation: observed 2026-09-11, 02:32 UTC

The [stage-timing comparison](evidence/productionize-m1-stage-timing-2026-09-11/README.md)
shows connection reuse reduced recent-01 hardlink staging to about 0.45–0.52
seconds, but candidate `9e674d3` still failed its diagnostic after 434.583
seconds: block 3,479,181 appeared publicly at 32.882 seconds. It returned
6,582 exact queries with seven retries. No full rollout started.

One activation-phase SSH operation took 9.782 seconds after preparation.
Recent-01 remained responsive on its old map and switched around 02:32:14,
after preparation completed around 02:32:03. The worker synchronizes its durable
record before switching, so persistence latency is being traced. This cause is
not yet proven; activation/attestation and router timing identify the exact
operation on subsequent cycles. **M1 remains open**, and no failed-run time
counts toward the full acceptance gates.

## M1 headless canary failure: observed 2026-09-11, 02:02 UTC

The [complete canary and failure evidence](evidence/productionize-m1-headless-2026-09-11/README.md)
records operations `f383b01`, worker `043c051`, and persistent headless
installation. The run began at 00:59:10 UTC and failed at 02:01:57 UTC after
3,767.108 seconds, 44 accepted new blocks and 54,932 exact queries. There were
39 retries and no mismatches or new routing withdrawals. The supervisor is
terminal failed; no fleet promotion or replacement gate has started.

Block 3,479,158 was observed publicly at 30.001 seconds, beyond the 30-second
limit. The controller recorded its activation at 28.613 seconds. Closely spaced
blocks accumulated delay through sequential publication; staging commands also
took multiple seconds. These measurements require further attribution, not a
threshold waiver. Local HTTP/Unix probes stayed fast through the failure with
no errors, so this is not evidence of a recurrent worker-local readiness stall.

**M1 remains open.** Attribute queueing, host staging and external observation
latency, qualify the correction, then pass a fresh six-hour/300-block canary,
matching six-worker rollout and 24-hour observation. The first-hour checkpoint
is historical evidence only; no failed-run samples count toward acceptance.

## M1 control-path failure: observed 2026-09-11, 00:08 UTC

The [publication-I/O canary failed](evidence/productionize-m1-control-path-failure-2026-09-11/README.md)
after 421.021 seconds, nine new blocks and 5,705 exact queries. Two coordinator
status timeouts removed recent-01 at 00:08:24; membership recovered at 00:08:28.
The durable audit correctly rejected this withdrawal. Worker-local probes
throughout that window stayed below 1.7 ms for Unix status and 7 ms for HTTP,
with no errors. This incident is narrowed to the coordinator/control path;
it does not reproduce the earlier worker-wide pause.

That supervisor is terminal failed. Subsequent independent status/HTTP probes
and a bounded reconciler fsync trace investigated the cause; the trace did not
establish synchronous reconciler persistence as the cause. Subsequent diagnostics caught local and
remote HTTP timeouts together while local Unix status stayed responsive, so SSH
alone does not explain all delays. Completed TCP-stage timing and CPU
sampling-gap probes are recorded in the same evidence; virtual-GPU stack
findings led to the replacement canary above.
**M1 remains open.**

## M1 publication I/O canary: observed 2026-09-11, 00:02 UTC

The [new deployment and start evidence](evidence/productionize-m1-publication-io-canary-2026-09-11/README.md)
records three successful Amsterdam qualifications (174 exact queries, maximum
visibility 8.502 seconds, minimum modeled headroom 25.694%). The durable routing
audit is now deployed. Recent-01 runs worker `043c051`, binary
`6b97509b20dc21e67e39512676dbc896963411b00a7df7e76727d1305a1d705d`;
warm and exact-query upgrade verification passed. The other five workers have
not been promoted.

The fresh loaded canary began at **00:01:23 UTC** under
`transparent-m1-publication-io-rollout.service` and was verified active at
00:02:55 UTC. Its durable withdrawal counter is unchanged from the baseline.
Local status/readiness probes run alongside it. **M1 remains open** pending
both six hours and 300 new blocks, the gated fleet batch and full 24-hour
observation. The earlier multi-second timeout has not yet been conclusively
explained; no previous run counts toward this gate.

## M1 publication I/O correction: source work, 2026-09-10, 23:48 UTC

The [completed diagnostic trace](evidence/productionize-m1-publication-io-2026-09-10/README.md)
caught a 379.788 ms fsync on a Tokio executor thread. Activation and invalidation
still performed publication persistence synchronously; both now use blocking
jobs, retaining publication ordering and cancellation serialization. Invalidation
releases serving locks before persistence after marking revoked history in memory.
Both new blocked-write regressions fail on the previous source and pass with the
correction. Full `make check` passed (590 Rust tests, zero failures, two ignored),
and the Linux build and targeted regressions passed. No deployment has occurred.

The ten-minute diagnostic load completed 9,157 exact queries with one retry.
The trace did not reproduce the earlier multi-second timeout and does not settle
its full cause. Neither this diagnostic nor the source correction closes M1;
no full acceptance gate is running.

## M1 post-reorg observation invalidated: observed 2026-09-10, 23:17 UTC

The [23:14 timeout evidence](evidence/productionize-m1-postreorg-2026-09-10/status-timeout-2314/README.md)
confirms two failed control attempts and removal of the only current recent
replica from 23:14:08 to 23:14:10. The router installs HTTP 503 when that quorum
is absent. Periodic observer checks missed the interval; an active supervisor
was therefore insufficient acceptance evidence. The run was stopped before
promotion and is not accepted. No full gate is currently running.

A source correction adds durable routing withdrawal evidence and rejects a
withdrawal followed by recovery between observer polls. Targeted operations
tests and full repository checks pass; this correction is not deployed. The underlying multi-second
worker scheduling delay remains unresolved. **M1 remains open.**

## M1 reorg-related withdrawal: observed 2026-09-10, 22:51 UTC

The [failure and fresh-gate evidence](evidence/productionize-m1-postreorg-2026-09-10/README.md)
records the collection/forwarding canary's HTTP 503 failure at 22:46:54 UTC
after 65.7 minutes, 51 blocks and 57,670 exact queries. Local endpoints stayed
below 8 ms during that window. The controller rejected a noncanonical candidate;
its block hash differs from the node's subsequent canonical hash. Both public
origins recovered to matching coverage. This supports a reorg-related withdrawal,
not the earlier local status pauses, as this failure's trigger.

A fresh full gate started at **22:50:58 UTC** under
`transparent-m1-collection-postreorg-rollout.service`, verified active at 22:51 UTC.
The installed worker and operations configuration are unchanged. No old samples
count, no fleet promotion occurred, and no acceptance thresholds changed.
**M1 remains open**, including the separate scheduling-pause investigation.

## M1 collection and forwarded-status canary: observed 2026-09-10, 21:53 UTC

The [deployment and start evidence](evidence/productionize-m1-collection-forward-canary-2026-09-10/README.md)
records worker `d8f5217` on recent-01 and operations `eb116e9` with owned sessions
and forwarded status enabled. The collection fix prevents disk cleanup holding
serving locks; forwarding removes remote helper startup from status requests.
Three Amsterdam qualification runs and full local checks passed before deployment.

The fresh `transparent-m1-collection-forward-rollout.service` entered canary
observation at **21:41:13 UTC** and was rechecked active at 21:53 UTC: 13 new
blocks, about 10,700 exact queries, 11 retries, no worker restart or OOM.
**M1 remains open.** These partial samples do not establish sustained acceptance;
the matching full gates remain in [remaining work](remaining-work.md).

The preceding persistent-session canary failed after 96.8 minutes and 76 blocks,
with 82,406 exact queries, on status timeout and HTTP 503. The subsequent bounded
diagnostic run also failed with HTTP 503 at 21:35:50 UTC before operator cleanup.
Its final memory-backed probes captured a genuine 3.9-second local status pause,
superseding the earlier partial maximum. The full cause of that pause remains
unproven. The new candidate has a separate bounded diagnostic probe; acceptance
budgets have not changed and no previous samples count toward this run.

## M1 owned control sessions: deployed 2026-09-10, 18:44 UTC

Operations `85d76e8` is deployed with `control_sessions: true` and the dedicated
`transparent-control-sessions.service`. All six connections attested warm, valid
status before enabling; both public metadata endpoints returned HTTP 200 after
the switch. Worker recent-01 remains `f2f351c`; no fleet promotion has occurred.
The [status-channel investigation](evidence/productionize-m1-status-channel-2026-09-10/README.md)
preserves the preceding failed 25-minute/18-block canary, direct-versus-persistent
measurements and the live cancellation/reconnect tests. Full `make check` passed
(587 Rust tests, two ignored), as did all 81 targeted operations tests.

A fresh loaded gate started at **18:45:04 UTC** under
`transparent-m1-sessions-rollout.service`, observed active in canary observation.
Its outputs are `/opt/transparent-publisher-build/sessions-20260910/rollout` on
the coordinator. Starting this run is not acceptance: the full duration and block
count, gated rollout and all-worker observation remain required. Prior samples
do not count. The supervisor stops on failure.

## M1 corrected control transport: 2026-09-10, 04:22 UTC

Operations commit `7fedd79` isolates control SSH connections and retries a
transport-failed read-only status once inside the existing membership timeout.
No stale status is accepted; mutations are not blindly retried. The
[503 investigation and correction](evidence/productionize-m1-publication-503-2026-09-10/README.md)
records the failure chain, failing baseline regression, 75 passing operations
tests and the full check (587 Rust tests, zero failures, two ignored benchmarks).

The corrected script was installed at 04:22:10 UTC; only the reconciler restarted.
Both public origins matched at height 3,478,132 and recent-01 remained warm 28/28
on worker `f2f351c`. The 04:22:46 gate passed its first publication, then stopped
at 04:24:01 on a real same-height reorg and protective public withdrawal. Canonical
coverage recovered at 04:24:37; the observer was not weakened to accept that run.
After canonical/warm verification, a new strict gate began at **04:26:57 UTC**
under `transparent-m1-control-postreorg-rollout.service`. Its output is
`/opt/transparent-publisher-build/control-20260910/postreorg-rollout` on the
coordinator, active at the start capture. No previous samples count. M1 still
requires the complete canary, fleet rollout and 24-hour observation. A reorg can
still interrupt the strict gate; this operations correction does not change that
policy or establish a reorg recovery SLO.

## M1 public availability failure: observed 2026-09-10

The `f2f351c` canary stopped at 2026-09-09 22:03:05 UTC after 67 minutes and 51
blocks on HTTP 503 from the public shard map. No fleet rollout occurred. The
[publication-control investigation](evidence/productionize-m1-publication-503-2026-09-10/README.md)
traces membership withdrawal after a failed control status exchange, followed by
an activation SSH failure and re-admission about three seconds after withdrawal.
The worker remained query-correct: 57,386 exact queries, 28 retryable responses,
no recorded restart/OOM. The prior ETA is invalid; M1 requires a fresh gate.

## M1 preceding canary start: 2026-09-09, 20:56 UTC

Source `f2f351c` makes verified runtimes servable before optional snapshot writes
finish and gives admitted queries a cancellable memory wait of at most 250 ms,
bounded by their remaining deadline. The memory ceiling is unchanged. The
[qualification and handoff evidence](evidence/productionize-m1-query-wait-2026-09-09/README.md)
records three passing two-slot portable-backend runs: 8.856–9.721 s visibility,
233 exact queries, two retries and 1.172–1.236 s maximum completion gaps. Memory
and persistence-drain checks passed. `make check`: 587 Rust tests passed, two
manual benchmarks ignored; 71 transparent operations tests passed.

Recent-01 runs binary
`c6f169a3bdbd0cc5f110309ac70ee089b9336b507f46493b8fe80aaa51861bea`.
Its upgrade restored 28/28 runtimes in 10.296 s and verified 43 exact queries
before reopening public service. A fresh six-hour/300-block canary started at
20:56:02 UTC under `transparent-m1-querywait-rollout.service`. At the handoff
capture it was active in `canary_observation`. The other five binaries remain
unchanged. The supervisor advances to the fleet batch and 24-hour observation
only after passing the matching gate. **M1 is not complete.** The saved canary
files are a dated start capture, not a final acceptance result.

## M1 preceding candidate and failed loaded canary: 2026-09-09

The [query-tail qualification](evidence/productionize-m1-query-tails-2026-09-09/README.md)
passes all three two-slot coupled screens: worker visibility 10.923–11.298 s,
retry fractions 0.9–5.4%, maximum completion gaps 1.189–1.281 s. All 730 queries
across six runs were exact, initial readiness was complete, and memory checks
passed without high/max/OOM events. Source `d778c62` batches snapshot reads and
adds query phase diagnostics; 585 Rust tests and 70 operations tests passed.

At this preceding capture, recent-01 ran binary `c9d88ca7d73014c8217c4240c4af858921fbfaa2848da5a1d49f9fbb7727308f`.
Its upgrade warmed 28/28 runtimes in 18.014 s and passed 43 exact maintenance
queries before reopening public service. The desired roster selects two build
slots for recent replicas; the other five workers retain their preceding binaries
until the gated batch. The loaded canary started at 20:00:53 UTC and failed after 78.576 s: block
3,477,724 reached public visibility in 30.228 s (observer exceeded 30 s first).
The two clients completed 1,052 exact queries without retries. No fleet batch ran;
`transparent-m1-qualified-rollout.service` is stopped in `failed`.

The generator's Xeon 8280 selects the AVX-512 backend; recent-01's older
`DO-Regular` CPU selects the portable split backend. Live worker preparation took
8.911 and 10.572 s in the failed burst. CPU-count/memory matching did not establish
hardware equivalence. M1 now requires fallback-path and actual-target timing
qualification before restarting the full loaded gate. **M1 remains open.**

## M1 memory phase qualification: 2026-09-09

The [memory phase experiment](evidence/productionize-m1-memory-phases-2026-09-09/README.md)
releases completed construction accounting before snapshot persistence and advises
Linux to release consumed file cache. Under unchanged limits, two-slot visibility
fell to 10.765–11.566 s and retry fractions to 5.6–7.5%. All 687 queries across six
final runs were exact, all 28 initial runtimes were warm, and no cgroup memory-limit
or OOM events occurred. Modeled host headroom was 26.5–27.4%.

Only one of three two-slot runs passed the coupled screen: two query completion
gaps (2.371 and 2.182 s) exceeded the 2.089 s reference. One slot avoided retries
but missed the 14 s worker screen. The benchmark now drains external clients before
closing HTTP; unsuccessful earlier shutdown attempts are retained separately.
584 Rust tests and seven runner tests passed. No live configuration changed.
The subsequent query-tail qualification above resolves this isolated screen;
its loaded canary subsequently failed, as recorded above.

## M1 construction optimization: 2026-09-09

The [construction experiment](evidence/productionize-m1-construction-2026-09-09/README.md)
reuses fixed public packing images, parallelizes independent collapse halves,
and batches snapshot writes. Packing preprocessing fell about 38%; one-slot
worker visibility improved from 20.592 s to 15.307–15.870 s. Two slots met the
provisional 14-second timing screen in all three repetitions (12.774–13.009 s),
but query retries rose to 37.9–43.8% of attempts and completion gaps reached
4.747 s. All 696 final queries were exact; full warm readiness and modeled memory
checks passed. No configuration passes both latency and query availability.

The source pins `ipir-sp` `61dc83e`; 579 application Rust tests and the upstream
equivalence tests passed. No live binary or configuration changed. The subsequent memory phase experiment above tests allocation lifetimes and
file-cache pressure under those same limits.

## M1 admission diagnosis and correction: 2026-09-09

The [instrumented comparison](evidence/productionize-m1-admission-2026-09-09/README.md)
separated clients from the worker's cgroup and CPUs. It found that both baseline
two-slot startups warmed only 25 of 28 runtimes. The benchmark had omitted an
initial readiness check, so candidate preparation repaired missing startup work.
Those baseline timings cannot qualify a fully warm publication comparison.
The earlier full-residency run likewise lacks proof of complete initial readiness.

The source correction queues cold-build memory admission fairly without reducing
reservations or changing memory limits, and the benchmark now rejects incomplete
initial readiness. Corrected two-slot repeats completed startup but produced
13.037 s and 16.842 s worst worker visibility; only one met the provisional
14-second screen derived from earlier fleet overhead. Query overload retries also
remain observable. This rejects promotion based on the build-slot change alone;
no live worker setting or canary soak has changed. M1 remains open.

## M1 full-residency qualification: observed 2026-09-09, 08:18 UTC

The [full recent-assignment comparison](evidence/productionize-m1-full-residency-2026-09-09/README.md)
loaded all 14 recent shards with disk caching and production memory limits on the
Amsterdam generator. All 864 queries were exact and modeled headroom exceeded
20%. Two slots reported shorter cold startup, whose completeness was not checked,
and produced worst update visibility of
29.97–30.18 s, versus 20.72–26.45 s with one slot. The supervisor correctly failed
the comparison when two slots exceeded the worker-stage 30-second budget.
The earlier single-shard result does not support promotion. No live setting or
canary soak changed. The later admission investigation above supersedes the
interpretation of the cold-start speedup and supplies the missing diagnostics.

## M1 burst experiment: observed 2026-09-09, 07:39 UTC

The [isolated Amsterdam comparison](evidence/productionize-m1-burst-2026-09-09/README.md)
completed four fresh worker processes with two exact-query clients and real
recent-8k tables. One build slot produced worst worker visibility of 20.85–25.17 s;
two slots produced 10.27–13.99 s. All 592 queries decoded exactly. This supports
qualifying two slots with full residency; the single-shard experiment does not
establish live freshness or 8 GiB host headroom. No deployed worker setting was
changed and no replacement canary soak was started. M1 remains open.

## M1 investigation: observed 2026-09-09, 07:18 UTC

[M1 evidence](evidence/productionize-m1-2026-09-09/README.md) resolves the M0
private-access unknown. The managed rollout-3 canary failed after 2,090.910 seconds
and 23 blocks: block 3,476,836 exceeded public freshness (observer 30.719 s;
controller activation 30.842 s). Two blocks arriving about a second apart incurred
serial publication cycles of 17.300 and 14.509 seconds. Recent-worker preparation
was the principal observed cost; unchanged archive preparation was negligible.
No fleet promotion occurred. All six workers currently report warm readiness;
only recent-01 runs the corrected binary. The failed run and logs are preserved.
M1 remains open pending a burst-latency correction and fresh complete acceptance.
No replacement soak was started. The older active-run description below is historical.

## Productionization baseline: observed 2026-09-09, 07:06 UTC

[M0 evidence](evidence/productionize-m0-2026-09-09/README.md) captures the wallet
integration at `2c7cee52c` plus an exact working patch and untracked height screen.
Wallet HEAD advanced independently to `8267008b4` during the inventory; captured
source contents still matched. The client is pinned to `22e6bec`, and the wallet's
own layout-4 store retains quadratic coverage rewrites: appending after 1,000
checkpoints changed 2,002 SQL rows. A Dart probe also reproduced suppression of
incomplete reasons near tip without changing the model's false completion state.
Targeted native/database and Dart/Flutter checks passed; release and user-wallet
validation remain separate gates.

The two public map samples agreed at height 3,477,098 and were corroborated by
`us.zec.stardust.rest:443` at that height/hash. This establishes sampled
independent-server agreement, not index completeness. Private SSH timed out:
current worker readiness, binary/configuration identity and the supervised
rollout result were not verified. No infrastructure was changed. M0 is complete
with those explicit unknowns; [M1 and M2](remaining-work.md#macos-recovery-beta-milestones)
own their resolution. The earlier deployment observations below remain dated
historical records rather than assertions about the current supervisor phase.

## Archive parent artifacts: observed 2026-09-08

The [production parent-filter rollout](evidence/parent-filters-production-2026-09-08/README.md) published the archive bundle on the existing public filter origin. All 20 bodies were verified over HTTPS and all 160 archive descriptors matched both production origins. The updated reference client (`5577b34`) completed 5/5 exact recoveries with parents enabled and 5/5 with direct filters; the three recent profiles downloaded identical bytes. This was a small deployment canary, not the unfinished paired performance study. Wallet applications require explicit opt-in; no application release or PIR worker rollout was performed.

## Source-verified implementation

| Capability | Observed state |
|---|---|
| Shard schema | `transparent-shard-v7` in `pir/transparent-shard/src/manifest.rs` |
| Registry | `recent-8k` 8192/8192; `recent-4k` 4096/4096; `archive-32k` 32768/32768; `archive-wide` 32768/65536 |
| Desired optional recent pairing | 4096/8192 absent; add a new name, never reinterpret `recent-4k` |
| Census ranges | `--start-height`, `--end-height`, geometry overrides, `--placement`, `--per-shard`, exact script matches exist in `shard-census.rs` |
| Two-tier publisher | `--recent-geometry`, `--archive-geometry`, `--recent-from` exist in `shard-publish.rs` |
| Workflow exposure | `publish-transparent-shards.yml` exposes commit, journal, output directory, anchor, `recent_from` (re-derived and checked) and both geometries; the backfill workflow's `inventory` action records journal identity, cutoff and an independent event spot-check |
| Loading/cache | `ShardSet::open_with` loads the whole set or an assignment's subset: every shard's manifest and filter, tables only for assigned ids, global ids and manifest chain intact; bounded runtime cache and file-backed plaintext sources |
| Continuous publication | Controller, incremental suffix publisher, worker prepare/activate/invalidate control, and shadow/activate deployment workflow implemented and deployed; see the dated rollout below. RPC supplies non-finalized tip blocks; the RocksDB secondary remains for historical backfill. |
| Revision handling | Revision-addressed setup/query, 409 refresh, retryable cache pressure, bounded wallet refresh exist |
| Retention | Three superseded revisions per shard beyond current, with optional byte bound (`--retain-bytes`); live collection preserves the newest three retired snapshots and individually held readers, collects other idle snapshots, and trims unpinned retired runtimes before preparation. Current-only prewarm preserves build headroom |
| Observability | Every metric series labelled with map and assignment digests, worker id and role; cold-build histogram; process RSS and cgroup memory gauges; queue depth and body bytes in flight; unassigned refusals; prewarm progress |
| Query validation | The query route refuses a body whose declared length is not the selected table's exact length before buffering, queueing or building; a missing length is 411; the runtime re-checks length and binding as defence in depth; the global HTTP cap remains the ceiling for the widest geometry |
| Admission | `admission.rs` bounds waiting requests (`--query-waiters`), buffered body bytes (`--body-bytes`), upload time (`--upload-deadline-secs`) and total wait (`--query-deadline-secs`); a full queue, exhausted body budget or expired deadline is 503 with `retry-after`, a stalled upload 408; a dropped connection releases its place and is counted |
| Deployment acceleration | Persistent public runtime snapshots, stable per-worker assignment identity, binary identity in readiness, selective staging/restarts, healthy replica pairs and transaction-scoped rollback implemented; cache-enabled recent and archive canaries deployed. Broader timing acceptance remains in [remaining work](remaining-work.md). |
| Readiness | `/v1/ready` reports mode, map and assignment digests, warm and target runtime counts; loaded-only mode (`--pilot-cold`, whole-set default) is ready once loaded, warm mode (assignment default) only once every assigned runtime is prewarmed |
| Fleet assignment/router | `transparent-assignment-v1` planned by `shard-assign` from a roster; router is Caddy rendered from the assignment, routing on the shard id in the path only; fleet deploy modes prepare, verify with the staged binary, activate owners then replicas, switch the router, verify publicly and over the VPC, prune, and roll back on failure; filter service has its own deploy workflow. Exercised on the fleet; failure and capacity rehearsals remain separately tracked |
| Wallet manifest binding | `GET /v1/shards/:id/revisions/:digest/manifest` serves canonical manifest bytes; before any private request on a matched shard the wallet recomputes the digest, checks every field against the map entry, the previous entry's digest and the registry, and takes the geometry from the verified manifest |
| Accepted-anchor recovery | `sync_into` and the facade require a wallet-accepted height/hash. Partial-shard coverage retains a distinct source endpoint, events above the target are discarded after validation, and rollback accepts an exact ancestor. SQLite schema 2 migrates legacy progress conservatively. See [tests](testing.md); deployed regression remains a separate gate. |
| Persisted continuation | `sync_into` over a `WalletStore` (reference `MemoryStore`; SQLite in `pir/transparent-wallet-store`): per-script per-shard coverage with the block hash it rests on, settled or provisional; atomic idempotent shard commits; reorg rollback to the accepted ancestor via the wallet's `ChainView`; provisional tail truncation and promotion; durable pending page work; scripts added by the wallet's rules discovered over their required range; a budget or outage ends a call incomplete with no anchor. `sync()` is a wrapper over a fresh memory store. `TransparentSync` facade takes plain data for a host binding. The store contract suite is exported under the `testing` feature and `parse_init` is available without `reqwest` (`5758ffd`); the Zakura wallet (`valargroup/wallet-libraries`) implements the store over its own database and passes it |

Relevant sources: [store](../../pir/transparent-wallet/src/store.rs), [publisher](../../server/transparent-filter-server/src/bin/shard-publish.rs), [census](../../server/transparent-filter-server/src/bin/shard-census.rs), [service](../../server/transparent-shard-server/src/service.rs), [runtime](../../server/transparent-shard-server/src/runtime.rs), [loader](../../server/transparent-shard-server/src/shardset.rs), [wallet](../../pir/transparent-wallet/src/sync.rs).

## Hardening and adapter: observed 2026-09-09, 00:55 UTC

[Managed-preparation evidence](evidence/managed-preparation-2026-09-09/README.md)
records the new canary deployment and failed predecessors. Recent-01 runs worker
`d5fea93`, binary `817b621cbffc5743750ed0d91280dc041ed4040f55764aadf1230ce6cc718eee`.
It warmed all 28 runtimes without failures, passed 41 exact directory/page
queries during maintenance verification, and reopened with canonical authority.
Both public origins matched the node at height 3,476,801 in the 00:45:54 capture.
The other five workers retain their previous binaries. Foreground publication
and the reconciler now share `fleet.json`; only recent-01 uses persistent
managed preparation. No archive soft-limit change is yet deployed.

The Zakura adapter is committed to `zakura-core/wallet-libraries` main at
`bca43b343`, pinned to client `22e6bec`: fixed accepted scan target, exact rollback
anchor, distinct coverage source anchor, durable validated page progress,
explicit incomplete reasons, and atomic balance/coverage reads through the app.
Its new layout 4 refuses older layouts without deleting wallet data. The 282-test
wallet suite, schema rejection test, 49 UI tests, generated binding analysis and
macOS debug build passed. Live recent/archive adapter queries decoded correctly;
a fresh recovery using independent node hashes also reached its accepted target of 3,476,726 and completed on repeat. These test fixtures do not prove a particular user's balance.

The final code passed 106 worker tests, 61 operations tests, strict worker
Clippy and 26 portable Linux library tests. The prior third soak failed its
catch-up gate; the first managed deployment then exposed permanently incomplete
prewarm after transient admission refusal and premature public reopening before
the controller listener was ready. Both are corrected and their raw failures
are preserved. The second managed run failed after 141.740 seconds: public visibility stayed
within budget, but a superseded prepared candidate was discarded instead of
advancing the unrouted replica. Correction `7754d6c` preserves that verified
canonical private progress while keeping exact-current public membership checks.
The fresh loaded gate began at 00:55:04 UTC under
`transparent-managed-hardening-rollout-3.service`. Its supervisor advances to the
approved six-worker maintenance batch only after a matching successful gate,
then monitors all six workers for 24 hours. **The six-hour/300-block canary,
wider rollout and 24-hour monitoring are not yet accepted.** See
[remaining work](remaining-work.md).

## Live deployment: observed 2026-09-08, continuous publication

The current rollout is recorded in [continuous-publication evidence](evidence/continuous-publication-2026-09-08/README.md). The post-fix 20-block service acceptance passed; the subsequent wallet adapter changes and worker canary are recorded below. Earlier [dataset inventory](evidence/inventory-2026-09-08/README.md), [full-chain publication](evidence/publication-2026-09-08/README.md), [warm fleet measurements](evidence/runs/fleet-warm-2026-09-08-c/README.md) and [cache canaries](evidence/deployment-runtime-live-2026-09-08/README.md) remain historical evidence at their recorded sources and workloads.

| Field | Observed |
|---|---|
| Source | Controller `c676fb6`; fleet hook `08e2261`; all six workers at `6e0c65c` |
| Handoff at 16:02:05 UTC | Node, journal and public coverage matched at 3,476,386; both origins agreed, all workers were warm, and no controller publication error was present |
| Chain and geometry | Mainnet journal starts at 0; 174 shards, comprising 160 `archive-wide` and 14 `recent-8k`; verified cutoff remains 3,262,749 |
| Public routing | `transparent-pir.valargroup.dev` resolves to router 209.38.42.220; both public map URLs and filters use the coordinator's active authority. The old pilot forwards wallet paths to the router during cached-DNS overlap |
| Fleet | Four recent replicas (10.142.0.10/.8/.7/.12), two disjoint archive owners (10.142.0.6/.9), one router (10.142.0.11), all ams3; runtime and memory budgets unchanged |
| Publication | RPC ingest follows non-finalized best-chain height/hash. Immutable suffix preparation and warm activation replace the old morning-only static tail. Publication preparation resumed after all six workers were verified warm; the temporary repair pause is removed |
| Memory correction | Live churn exposed runtime headroom and startup-snapshot retention bugs; both corrected. The final private canary completed 12 activations with at most three retired snapshots after collection and no OOM events or restarts |
| Reorg | An actual one-block journal reorg at 3,476,290 triggered public withdrawal during the first repair pause and recovered after resumption. It was above then-public coverage. Published sealed-history replacement and cross-tier wallet rewind are deterministic test evidence, not an injected mainnet reorg |
| Block visibility | Twenty consecutive new blocks, 3,476,357–3,476,376, observed 15:28:55–15:48:55 UTC: p50 17.562 s, p95 24.356 s, maximum 26.687 s; no HTTP errors, origin mismatches or orphaned endpoint samples |
| Memory during that window | Recent worker sampled RSS 4.604–5.858 GiB; owners 45.955–46.526 GiB. At most four retired snapshots between activation and collection; zero new OOM events or restarts on all six workers |
| Active quorum | Both archive owners and one recent replica per accepted generation. Other replicas were retried but excluded from that generation's routing; this is not four-replica capacity evidence |
| Wallet adapter | Existing `5758ffd` pin passed fresh empty-wallet recovery through 3,476,342 and recent/archive HTTPS private queries. A later recovery raced map/filter activation and failed safely. Reference-library fix `87226c3` passes race/corruption and broader recovery tests; adopting it in the adapter requires accepted-anchor store/API migration, as recorded in remaining work |

## Decision and evidence state

The user accepted the two-tier target on 2026-09-07. [Deployment](deployment.md) owns its settings. Full-chain uniform geometry and single-c8 evaluation/residency evidence exist; mixed publication evidence and limited target-host HTTP measurements are recorded; sustained capacity, cross-host scaling and mobile wallet latency remain open. [Remaining work](remaining-work.md) is the authoritative checklist.

## Deployment runtime follow-up

[Further live validation](evidence/deployment-runtime-followup-2026-09-08/README.md)
confirmed four-slot startup prewarm at 17.644 seconds on recent-01 and 125.416
seconds on archive-01 from current-process journals. Three new encrypted public
row probes passed. Four workers still lack disk-cache configuration; recent-01
has exhausted its disk-cache budget and rebuilds new tails without persisting
them. A full-fleet deployment under ten minutes remains unverified.

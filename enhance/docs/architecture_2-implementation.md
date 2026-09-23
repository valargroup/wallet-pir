# Architecture 2 implementation status

The v4 implementation is an opt-in path alongside the existing schema-9 service.
It is **not production-qualified**. The existing deployment workflow and service
defaults continue to select the legacy binaries. The architecture specification
remains [architecture 2](architecture_2.md).
The [conformance audit](architecture_2-conformance.md) separates mandatory release
gates, available evidence, and optional planner/recovery improvements.

## Implemented path

- Schema 10 / `ironwood-enhance-pir-v4`, atomic generation manifests, lazy shard
  sessions, digest-bound mutable-unit identities and generation/shard/epoch framing.
- Fixed 32K query domains, 4K suffix loans and atomic return, stable identities,
  complete-block coverage, adaptive 2K/4K/8K units and immutable runtime reuse.
- Separate coordinator and private worker HTTP processes, per-shard evaluation,
  replica fallback, five retained generations and explicit HTTP 410 expiry.
- Rust client and CLI with protocol selection, lazy session caching, bounded
  refresh and row deduplication within one shard session.
- Durable controller/worker state, process fencing, reservation-before-preparation,
  commit recovery, retained routes and a separate draining-operation set.
- Commit notifications are persisted atomically with the publication decision.
  Each participant is retried independently; a pending worker is excluded from
  new candidates and retention updates while healthy peers continue ordinary
  publications. Restart recovery can apply an old decision after its generation
  has expired, synchronize retention, and then catch up. Lost responses require
  the exact published manifest digest before clearing the pending notification.
  A peer that missed ordinary publications can prepare the current assignment
  without intermediate snapshots.
- Durable abort notifications preserve unacknowledged reservations while excluding
  those workers from new candidates and retention changes. Healthy peers can
  continue ordinary publication after an abort decision. Successfully published
  operations also queue aborts for excluded participants, fencing reservations
  whose success responses were lost. Both notification types survive restart.
- Retention-based worker and coordinator artifact cleanup. Coordinator cleanup
  waits for publication recovery and validates all retained snapshot references
  before removing owned, unreferenced files.
- Conservative worker admission for future growth, retained runtimes, transitions
  and query pins; serialized preparation and two admitted evaluations.
- Explicit group assignments, role limits and deterministic sealed-shard
  consolidation into older groups. Append-only online inventory registration
  requires both new replicas to be distinct, idle v4 processes with fresh state.
- Durable growth observations and expansion demand. The count/role forecast
  previews successive loan/return placements, requests one additional pair, and
  retains its request identity across retries, reorgs, and coordinator restarts.
- Advisory admission for elective consolidation on both destination replicas.
  Known admission failure preserves the ordinary canonical-update placement;
  the subsequent reservation still rechecks actual memory and quorum.
- Separate checksummed `enhance-pir-v4-candidate` artifact configuration in full
  CI. Candidate metadata explicitly remains unqualified and records source dirty
  state. A local native bundle has been verified and run outside a Git checkout.

- Isolated v4 Terraform root with stable replica-pair addresses, additive project
  membership, restricted ingress, and deletion/replacement guards. An append-only
  plan validator checks recorded worker identities and rejects unrelated changes;
  mocked provider and guard tests run in full CI.
- A separately locked infrastructure journal consumes coordinator demand, freezes
  operation inputs, preserves resource identities, and fences ambiguous apply
  retries until adapter reconciliation. A provisioning adapter validates pinned
  source/backend/account/project/VPC, binds state to provider IDs and inventory,
  validates and applies a saved additive plan, and recovers a fully created fleet
  without reapplying. Live execution, automatic partial/orphan recovery,
  qualification verification, and registration remain outstanding.
- Candidate bundles include a host-local Linux worker bootstrap installer with
  clean-source/checksum/ELF verification, measured host-limit checks, a dedicated
  non-root service, immutable binaries, effective cgroup checks, and restart-safe
  receipts explicitly marked unqualified. The generated service passes Linux
  parser validation; actual service startup remains unverified.
- Pair bootstrap orchestration rechecks live infrastructure identity, pins SSH
  host keys, verifies transferred files before execution, and durably records each
  replica receipt. It resumes a partial pair without treating bootstrap as
  qualification. Live SSH execution remains unverified.
- Bundled read-only hardware sampling and an off-host trace recorder preserve
  cgroup/process memory, peaks, pressure, swap, service identities, retention and
  candidate observations. Errors and missed deadlines remain explicit. These
  tools record evidence only; qualification assessment remains unfinished.
- An isolated synthetic workload command drives real publications and concurrent
  PIR queries, checks boundary/retained answers and expiry refresh, and records
  durable publication traces. Active-five and sealed-six schedules have planning
  tests; the separate-process smoke profile passes. Full-size six-hour execution
  on the intended hardware remains outstanding.

- Coordinator metrics now expose shard/unit geometry, loan state, group roles,
  placement/operation state, expansion forecasts, pending decisions and lag against
  the latest observed publication target. Private worker metrics distinguish live
  database buffers, retained/candidate/query references and the existing admission
  model's growth, transition and overhead reservations. Unavailable samples are
  explicit; these model values do not qualify actual RSS or cgroup consumption.
- Hardware sample version 2 integrates continuous worker runtime metrics with
  host/cgroup observations. It brackets scrapes with worker identity, preserves
  kernel evidence on runtime-scrape failure, distinguishes busy-engine samples
  from zero usage, and counts inconsistent observations separately. The matching
  off-host observer rejects older successful samples without this evidence.

## Run isolated processes

Full CI is configured to build and upload `enhance-pir-v4-candidate-<sha>` alongside
the legacy release bundles. This candidate contains the v4 service binary, client,
load driver, disposable runner, and isolated launch configuration. The production
deploy workflow continues to select the separate legacy artifact. Candidate
checksums and successful software CI do not constitute hardware qualification.

After building the required binaries under `target/release`, assemble and verify
only the candidate with:

```sh
python3 tools/ci/release.py assemble --sha "$(git rev-parse HEAD)" \
  --kind enhance-pir-v4-candidate --output /tmp/enhance-v4-bundles
python3 tools/ci/release.py extract --sha "$(git rev-parse HEAD)" \
  --kind enhance-pir-v4-candidate \
  --archive /tmp/enhance-v4-bundles/enhance-pir-v4-candidate.tar.gz \
  --output /tmp/enhance-v4-verified
```

The bundle includes [isolated launch instructions](../ops/deploy/v4-candidate.md).
The output directories must be new. Build native binaries for the intended host;
the local macOS validation is not a Linux release. Dirty checkout metadata is
retained and cannot establish exact-commit qualification provenance.

Build optimized binaries:

```sh
cargo build --locked --release -p enhance-pir-server --bin enhance-pir-v4
cargo build --locked --release -p enhance-pir-load-test
cargo build --locked --release -p enhance-pir --features cli --bin enhance-pir-cli
```

Start each worker with a separate empty directory. Bind its private origin to a
private interface and allow only the coordinator to reach it:

```sh
enhance-pir-v4 worker --listen 127.0.0.1:8091 --data-dir /tmp/enhance-v4-worker-a
enhance-pir-v4 worker --listen 127.0.0.1:8092 --data-dir /tmp/enhance-v4-worker-b
```

The coordinator inventory uses the existing `groups/name/replicas/name/url`
shape, with exactly two distinct replicas per group. For an isolated synthetic
fixture, start the coordinator with:

```sh
enhance-pir-v4 coordinator --listen 127.0.0.1:8080 \
  --data-dir /tmp/enhance-v4-coordinator --worker-config /path/to/workers.json \
--isolated-fixture --fixture-records 67 --fixture-append-records 1
```

The coordinator reloads `--worker-config` on every poll. To register additional
capacity, atomically replace that file with the existing groups unchanged followed
by the new replica pair. Registration probes both new workers, then durably adds
the complete pair and increments the placement revision. Replays do not duplicate
capacity. Removing, reordering, or changing registered groups/endpoints is rejected.
Restart accepts the same inventory or a strict extension; extensions are checked
online before registration. Readiness here is a process check, not hardware
qualification. The infrastructure controller must still provision and qualify
workers before supplying them. Existing publications continue if an addition fails.

Capacity observation runs before publication and also when a poll finds no new
anchor. `--capacity-fallback-rows-per-second` defaults to 1 and must be positive;
`--capacity-readiness-seconds` defaults to 21,600 and cannot be lower;
`--capacity-burst-rows` defaults to 4,096. The effective rate is the greater of the
fallback and the observed peak since state creation. The persisted peak
does not decay on quiet polls or reorgs, which can deliberately request capacity
early after a burst. These defaults are planning assumptions, not measured SLOs.
The durable controller JSON and health response expose `capacity`, including the
remaining rows, outstanding request, registered outcomes, and fleet ceiling.
The infrastructure journal and guarded provisioning adapter consume these requests;
live execution and the qualification-to-registration handoff remain unverified.
This forecast checks placement counts and roles; it does not reserve memory or
prove future admission under retained runtime and query-pin pressure.

Before adding elective consolidation to a publication, the coordinator checks
the destination's complete proposed assignment through `/internal/v4/admit` on
both replicas. This private endpoint runs worker role/memory admission without
changing reservations, epochs, or durable state. If either check fails, that
publication keeps its ordinary assignment. These checks are advisory: changing
query pins or a later replica failure can still reject the authoritative reserve
step, leaving the previous generation available for recovery.
If an elective consolidation fails during reservation, before preparation, the
coordinator persists abort notifications for that attempt and retries the same
canonical generation once with its ordinary placement and a fresh attempt ID.
Unacknowledged workers keep their reservations and are excluded from the retry;
healthy peers may satisfy ordinary publication quorum. New groups and elective
moves still require both replicas. The coordinator never discards an unacknowledged
reservation or replaces its older pending decision with a later cancellation.
Canonical validation remains mandatory, and a committed generation is never
rolled back by this fallback. Worker aborts fence delayed reservations even when
the worker did not receive the original reserve command.

Health exposes `published_replica_counts` by group, and metrics expose
`enhance_v4_shard_published_ready_replicas` by shard. These count the ready routes
recorded for the current generation, making one-replica publication observable;
they do not claim that every recorded replica remains live after publication.

Canonical ingestion instead requires `--zakura-cookie` and optionally
`--zakura-rpc-url`. Fixture and canonical modes cannot share a data directory.
Canonical publication rechecks the block hash immediately before its durable
commit. Published artifacts have a separate v4 namespace; old journals and
serving artifact roots must not be repurposed for this experiment.

Query the new origin explicitly:

```sh
enhance-pir-cli --v4 --server http://127.0.0.1:8080 metadata
enhance-pir-cli --v4 --server http://127.0.0.1:8080 query 33
enhance-pir-cli --v4 --server http://127.0.0.1:8080 --shard 0 dummy
```

Public requests and responses begin with `EPQ4`, a little-endian u64 generation,
a little-endian u64 shard ID and an eight-byte parameter epoch. Existing PIR key,
query and response encodings follow this 28-byte header. Real and dummy queries
use the same framing. Shard selection is public; record authentication remains
the wallet's responsibility.

## Validation and load tests

Run the model, runtime and distributed tests:

```sh
cargo test --locked --profile release-fast -p enhance-pir --lib v4
cargo test --locked --profile release-fast -p enhance-pir-server --lib v4
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http \
  full_shard_loan -- --ignored
```

The last test allocates a full 32K domain and runs two worker listeners. Use a
host with at least 16 GiB RAM; it is deliberately excluded from ordinary shared
CI runs. It verifies real encrypted queries through loan and return transitions,
including old sessions. It also tests a rejected canonical anchor after candidate
preparation, undoing return and split transitions, and reapplying a split with
different record content while retained sessions preserve their original answers.
It does not simulate a worker cgroup or qualify hardware.

The larger multi-group test exercises a failed consolidation reservation, a
successful canonical retry, a later six-sealed-shard placement, and both retained
and current queries. Use at least 32 GiB RAM and 64 GiB of free disk:

```sh
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http \
  consolidation_reservation -- --ignored
```

The disposable process runner starts two workers and a coordinator, appends
fixture records during traffic, runs the requested concurrency sweep and cleans
up its own processes/data. Evidence is retained at the selected output path:

```sh
python3 enhance/ops/scripts/test-v4-local.py --bin-dir target/release \
  --out /tmp/enhance-v4-evidence --seconds 30 --concurrency 1,2,4,8
```

Add `--expand-inventory` to exercise online registration of a second replica pair
using four disposable worker processes, followed by the selected load sweep.
Add `--require-publications 2` to require at least two completed generations
between the start and end of the load campaign. Increase `--seconds` for slow
hosts or emulators; starting preparation alone does not satisfy this gate.

The v4 load driver requires an exact-answer oracle. `--fixture-oracle` is only
for the positional synthetic fixture above. `--oracle` accepts an independently
extracted JSON array of `{ "position": 33, "record_hex": "..." }` entries:

```sh
enhance-pir-load-test --v4 --fixture-oracle --server http://127.0.0.1:8080 \
  --parallelism 2 --duration 10m --warmup 2m --max-error-rate 0 \
  --slo-p99-ms 5000 --json-out /tmp/v4-load.json
```

Use `--rate` for aggregate offered QPS. The report includes scheduled-to-completion
latency and unstarted arrivals; a slow generator cannot silently discard missed
arrivals. Wrong answers always fail the run. Higher-concurrency overload runs
report HTTP 429 separately; permissive error thresholds used for characterization
do not pass the zero-error qualification gate.

## Remaining release gates

The implementation and deployment plan is not complete. Required outstanding
work includes:

1. Connect durable forecast-driven expansion requests to the infrastructure
   controller, with idempotent resource recovery and qualification receipts.
   Forecasting and online append-only inventory reconciliation are implemented;
   the journal and provisioning adapter now connect demand to guarded Terraform
   execution. Validate live execution and complete partial/orphan recovery,
   live pair bootstrap, qualification verification, and inventory registration.
   The generated service passes Linux parser checks; actual Linux installation
   remains unverified. Explicit reservation memory refusals now persist earlier
   capacity limits and immediately request the next pair. Published shards can now
   move after explicit memory refusal when a single whole-shard move admits both
   destination replicas and the residual source assignment. The architecture
   permits blocking when no admitted placement fits; a general multi-shard solver
   is an optional optimization. New shards now try another role-valid group when the preferred group
   fails preflight; both alternative replicas must admit and reserve the assignment. Reservation
   failure fallback is implemented for elective moves. Durable abort delivery
   excludes unavailable participants while healthy peers continue ordinary work;
   deployed durable-row repair and the broader fault campaign still require
   validation. Automatic peer transfer is an optional recovery improvement. Offline peer-archive row repair
   is implemented and tested over HTTP. Lost compiled caches can rebuild on restart
   from verified durable rows without changing committed metadata.
2. Complete per-phase failure injection, multi-group expansion/consolidation,
   delayed-reclamation and reorg campaigns. Local HTTP coverage exercises rollback
   across return/split boundaries and a peer rejoining after three missed ordinary
   publications. The committed-participant campaign now covers coordinator and
   worker restart, five publications while a participant remains offline, expiry
   before notification delivery, a later aborted attempt, lost commit responses
   and conflicting acknowledgement digests. The broader distributed crash/reorg
   campaign and deployed recovery when durable worker rows are also lost still
   require completion; explicit offline peer-archive repair is covered. The
   abort campaign now covers pre-commit preparation failure, durable cancellation
   through coordinator/worker restart, continued exact-answer publication, lost
   abort responses, delayed reservation replay, and ambiguous accepted reserves.
3. Qualify five-total active and six-sealed assignments on actual 8 GiB workers
   for six hours each, with at least 300 active publications, off-host harnesses,
   real transition overlap, cgroup/swap/reclaim measurements and the 512 MiB
   resident guard. The 0.71 GiB overhead allowance is still an estimate.
4. Run the full capacity/open-loop/soak matrix and independent canonical-data
   oracle; validate authenticated incoming/outgoing wallet recovery and refresh
   correctness certificates for the actual snapshot/setup/geometry.
5. Integrate continuous fleet scraping and qualification assessment with the
   parallel-origin installer and deployment workflow. Runtime metrics are now
   exposed, but host-limit measurements and passing qualification receipts remain
   outstanding. Candidate packaging is implemented and locally tested;
   execute and verify the Linux full-CI artifact path before hardware campaigns.
   Qualify the host hard cap rather than assuming 7.5 GiB fits every nominal 8 GB
   machine.
6. Confirm downstream wallet conformance, rehearse rollback, complete the
   production cutover and observe it for 24 hours before enabling expansion.

Do not point the production origin or existing autoscaler at v4 before these
gates pass. The [final core validation report](../evidence/architecture-v4-core-2026-09-22/README.md)
records source and binary hashes, correctness tests, and the local concurrency
and open-loop sweeps. Earlier smoke runs are retained in the
[initial report](../evidence/architecture-v4-local-2026-09-22/README.md) and
[intermediate report](../evidence/architecture-v4-local-final-2026-09-22/README.md).
The subsequent [retention and reorg report](../evidence/architecture-v4-recovery-2026-09-22/README.md)
records cleanup and alternate-branch HTTP validation for the later source changes.
The [online inventory report](../evidence/architecture-v4-inventory-2026-09-22/README.md)
records append-only pair registration, restart persistence, and a four-worker
process run with continued exact-answer traffic.

The [capacity-demand report](../evidence/architecture-v4-capacity-2026-09-22/README.md)
records forecast, durable request, and demand-to-registration validation.

The [consolidation-admission report](../evidence/architecture-v4-admission-2026-09-22/README.md)
records advisory memory admission and deferral of elective moves.

The [reservation-fallback report](../evidence/architecture-v4-fallback-2026-09-22/README.md)
records the full-size multi-group retry and six-sealed-shard consolidation test.

The [degraded-replication report](../evidence/architecture-v4-degraded-2026-09-22/README.md)
records commit recovery with an excluded peer, catch-up after missed publications,
and published-replica health/metric checks.

The [candidate-bundle report](../evidence/architecture-v4-bundle-2026-09-22/README.md)
records package verification and an extracted-bundle process/load run outside Git.

The [infrastructure foundation report](../evidence/architecture-v4-infra-2026-09-22/README.md)
records isolated Terraform schema/mock tests and append-only plan validation.

The [durable expansion journal report](../evidence/architecture-v4-journal-2026-09-23/README.md)
records crash-boundary tests and live coordinator demand consumption across
journal process restarts, followed by fixture expansion and exact-answer load.

The [provisioning adapter report](../evidence/architecture-v4-provision-2026-09-23/README.md)
records pinned-target checks, saved-plan execution, and restart recovery tests
using synthetic provider responses. Live provider execution remains unverified.

The [worker bootstrap report](../evidence/architecture-v4-bootstrap-2026-09-23/README.md)
records candidate verification, measured-limit checks, restart-safe installation
contracts, and packaging compatibility. Actual Linux service execution remains
unverified; the local suite skips the Linux-only unit-parser check.

The [pair bootstrap and Linux report](../evidence/architecture-v4-bootstrap-pair-2026-09-23/README.md)
records durable per-replica recovery tests and all 47 v4 operations tests passing
in Ubuntu, including service parsing. Live SSH and service execution are still
unverified.

The [hardware observation report](../evidence/architecture-v4-observation-2026-09-23/README.md)
records read-only sampling, explicit failure/deadline traces, and all 57 v4
operations tests passing on Linux. These tools do not grant hardware qualification.

The [workload-driver report](../evidence/architecture-v4-exercise-2026-09-23/README.md)
records six real publications under concurrent exact-answer traffic, retained
sessions and expiry refresh. It includes the initial HTTP 429 failure and its
concurrency-budget fix. This local smoke does not establish hardware capacity.

The [commit-notification report](../evidence/architecture-v4-commit-notifications-2026-09-23/README.md)
records durable notification recovery across an offline participant, generation
expiry and worker restart. Ordinary publications can continue on the healthy peer;
new groups and elective moves still require both replicas.

The [abort-notification report](../evidence/architecture-v4-abort-notifications-2026-09-23/README.md)
records cancellation recovery with unreachable workers and cleanup of accepted
reservations whose responses were lost. Pending aborts preserve worker charges
and remain separate from committed notification retries.

The [runtime-metrics report](../evidence/architecture-v4-metrics-2026-09-23/README.md)
records geometry/role and publication-target checks, nonblocking worker memory
sampling, and final metric snapshots from the separate-process workload.

The [continuous-metrics report](../evidence/architecture-v4-continuous-metrics-2026-09-23/README.md)
records sampler/parser and observer tests, Linux validation, and live native
worker RPC checks through reservation and abort. Actual hardware observation
and qualification remain outstanding.


The [campaign-assessment report](../evidence/architecture-v4-campaign-assessment-2026-09-23/README.md)
records frozen bootstrap/artifact binding, streamed trace verification, profile
and observation checks, and rejection of the real short smoke as qualification
evidence. The assessor always leaves qualification unproven; actual hardware
campaigns and the remaining release gates above are still required.


Workload evidence now records the Linux generator host identity; the assessor
rejects missing identities and load generation on an observed worker. This
closes the basic off-worker identity check, while identity provenance and
cross-host clock alignment remain outstanding.


Worker startup now reconstructs missing or rejected compiled PIR caches from
content-addressed durable rows. Recovery validates the entire padded unit length
and digest before building; absent/corrupt rows prevent readiness. It preserves
the durable epoch, revision, retained publications and committed manifest. The offline `repair-rows` command can restore journal-referenced rows from a
local peer archive while the worker is stopped, with length/digest validation and
no metadata changes. Automated peer/coordinator transfer remains outstanding.


The HTTP recovery campaign now deletes a worker's compiled caches and durable
rows after two publications, verifies exact answers through the surviving peer,
runs the real offline repair CLI twice, then restarts the affected worker at its
original address. With the peer stopped, both retained and current encrypted
queries must succeed through the recovered worker. The repair preserves the
publication journal byte-for-byte. This covers local peer-archive recovery;
automatic remote orchestration and deployed failure campaigns remain open.

See the [HTTP recovery evidence](../evidence/architecture-v4-row-repair-http-2026-09-23/README.md) for the executed sequence and its limits.


Provisioning can now reconcile an interruption where all workers exist and only
project memberships remain unfinished. It records a retryable resolution without
applying, then requires a fresh membership-only/no-op plan on the next invocation.
Missing worker pairs, orphans and unfinished firewall/tag resources remain fenced.
See the [membership-recovery evidence](../evidence/architecture-v4-membership-recovery-2026-09-23/README.md).

The [updated candidate load report](../evidence/architecture-v4-latest-load-2026-09-23/README.md) records ten concurrent publications and a full local concurrency/open-loop sweep, including overload errors and a warmup-reporting fix. It does not establish hardware or zero-error open-loop qualification.

The [new-shard placement evidence](../evidence/architecture-v4-new-placement-2026-09-23/README.md) records real admission endpoints and exact encrypted queries through an alternative replica pair. This bounded fallback does not relocate retained published data.


Admission preflight now excludes groups containing a replica with a pending commit
or abort notification. New-shard alternatives and elective consolidation use the
same exclusion as reservation, so a memory-only preflight cannot select a
quarantined destination and unnecessarily block publication. Ordinary peer-only
publication retains its existing reservation-quorum behavior.


Explicit worker memory-budget refusal is now HTTP 507 on private admission and
reservation routes. The coordinator records reservation refusals as durable
`capacity.memory_limit` observations and immediately emits at most one outstanding
next-pair request. Busy/stale/invalid requests keep their existing status and do
not trigger this signal. Limits apply to the observed fleet size; registering a
larger fleet releases that old limit while keeping its request history. This
uses the existing unqualified admission model, not measured hardware capacity.

The [live demand handoff test](../evidence/architecture-v4-demand-journal-http-2026-09-23/README.md) invokes the actual Python journal CLI against Rust coordinator health and verifies memory-demand deduplication before and after publication recovery. Terraform/provider execution remains unverified.

The [integrated regression report](../evidence/architecture-v4-integrated-regression-2026-09-23/README.md) records 25 v4 server-library tests, three protocol tests and all eight HTTP campaigns passing against the combined changes, including the normally ignored full-size cases. Hardware qualification and deployment gates remain open.


The [authenticated-record report](../evidence/architecture-v4-note-recovery-2026-09-23/README.md)
records incoming decryption and outgoing recovery of a synthetic Ironwood V3
record retrieved through real v4 HTTP PIR, plus wrong-domain/key/commitment and
ciphertext-tampering rejection. It explicitly distinguishes authenticated note
fields from trusted indexer metadata. Independent canonical oracles and actual
wallet conformance remain required.

The [canonical transaction-byte test](../evidence/architecture-v4-canonical-record-2026-09-23/README.md) verifies two public transaction records against frozen raw-byte oracles through current and retained PIR sessions. It does not establish snapshot-wide or downstream-wallet conformance.


The [Linux build and emulator diagnosis](../evidence/architecture-v4-linux-build-2026-09-23/README.md)
records successful compilation of all three x86-64-v3 candidate binaries. Default
local emulation crashes in Clap help generation; the unchanged server help passes
under QEMU 10.2.3. Runtime campaign status is recorded separately in that report.
Neither emulated execution nor a dirty-source build grants hardware qualification.


The [Linux publication-under-load campaign](../evidence/architecture-v4-linux-publications-2026-09-23/README.md)
completed three publications while running two 90-second load steps through
QEMU 10.2.3. All 1,336 measured answers were correct, with zero request errors.
This establishes completed publication under emulated functional load; native
hardware qualification, full-size campaigns and deployment remain outstanding.


Count-based reorg placement now requires both destination replicas whenever an
already-published shard changes groups, including moves into a group that already
served other shards. Previously that case could inherit ordinary single-replica
quorum. The [reorg replication report](../evidence/architecture-v4-reorg-replication-2026-09-23/README.md)
records the planner regression and degraded-publication validation. This does not
implement general memory-pressure relocation. Earlier Linux binary evidence
predates this correction and does not validate these updated Rust sources.


The [memory relocation report](../evidence/architecture-v4-memory-relocation-2026-09-23/README.md)
records relocation of a published active shard after explicit memory refusal,
complete destination replication, failed preflight/reservation preservation, and
exact current/retained queries. The planner tries one whole-shard move per pressured
group; if no such move fits, normal reservation blocks publication and records
capacity demand. Multi-shard rearrangements and measured hardware admission remain
outstanding. These Rust changes also require a fresh Linux artifact build.


The [updated placement full-size report](../evidence/architecture-v4-placement-fullsize-2026-09-23/README.md)
records both normally ignored HTTP campaigns passing after the replication and
memory-relocation changes. Fresh Linux x86-64-v3 binaries also passed an emulated
publication/load campaign: three completed publications and 1,265 correct answers
with no request errors. This supersedes the earlier source-version caveats for
those two fixes; clean release and native hardware qualification remain open.


The [reorg/memory composition regression](../evidence/architecture-v4-reorg-memory-placement-2026-09-23/README.md)
checks rerouting a shard already moved by the count planner when its proposed
destination cannot meet the required pair admission quorum. The fallback now uses
the same quorum policy as publication and can try a complete spare pair. The prior
full-size/Linux report predates this correction; hardware qualification and a
current clean release remain outstanding.


The [canonical RPC integration report](../evidence/architecture-v4-canonical-rpc-2026-09-23/README.md)
records the actual coordinator CLI ingesting through a local RPC server, replaying
a same-height reorg, preserving retained queries and restarting without duplicate
records. The bounded v4 integration suites are now explicitly selected by full CI;
remote CI execution, real-chain validation and wallet conformance remain open.


### Live target discovery and Spaces locking (2026-09-23)

The user confirmed new isolated workers in the existing wallet-pir project,
matching production hardware. Read-only DigitalOcean inventory verified project
`85639967-fecb-4c8d-88be-c0e3dee3f86c`, VPC
`c5bd6679-aa32-48fc-9d5d-51d422fb3468` in ams3, and two production c-4 workers
with four vCPUs, 8192 MiB RAM and 50 GB root disks. Roman's local public-key
fingerprint matches registered key 56343657. No resources were created.

The existing production backend documents that Spaces did not enforce
conditional lock writes. The v4 provisioner now rejects native S3 locking on
Spaces and supports a root-owned OS lock on a policy-pinned Linux machine.
Provisioning and pair bootstrap hold it for their operations; Terraform inherits
the descriptor so it remains locked if the controller exits before its child.
All manual state writers must use the same host/lock. Dedicated state-writer
credential ownership and the initial-pair procedure remain deployment work.

Validation: all 18 provisioning tests passed in an isolated Linux container,
including real contention and inherited-lock lifetime; on macOS 17 passed and
one root-only test skipped. All seven pair-bootstrap tests passed. The cloud
provider and Terraform apply paths were not exercised by this test run.

The qualification coordinator choice remains pending. The 64 GiB bootstrap
free-space check also exceeds the production c-4 root disk capacity and must be
resolved before host creation/bootstrap. Neither fact changes the hardware
qualification or wallet-conformance acceptance gates.


### Worker storage measured against production c-4 (2026-09-23)

The two current full-size HTTP integration tests passed in 394.20 seconds while
sampling each worker's files every second. Consolidation peaked at 14.392 GiB per
worker; loan/return peaked at 5.425 GiB. Evidence is under
`enhance/evidence/architecture-v4-worker-disk-2026-09-23/` with raw samples,
binary/source hashes and test logs. Coordinator storage was measured separately.

The initial worker bootstrap floor is now 32 GiB free on its data filesystem.
The earlier 64 GiB floor was inappropriate for a single production-equivalent
c-4 worker with a 50 GB root disk; 64 GiB remains the local multi-worker test-host
recommendation. Storage probing now respects the worker directory's filesystem
rather than always probing `/srv`. The 20-test bootstrap/pair suite passed with
one environment-specific skip. Actual cloud-init free space and six-hour growth
and reclamation still require native verification. No cloud resources were created.

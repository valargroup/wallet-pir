# Enhance v4 candidate bundle

This bundle is **unqualified** and does not authorize production cutover. It is
separate from the legacy Enhance deployment artifact. `candidate.json` identifies
schema 10, protocol `ironwood-enhance-pir-v4`, the source revision, and whether the
source checkout was dirty. `SHA256SUMS` covers the binaries, launch example,
runner, host bootstrap and sampling scripts, metadata, and this document. Checksums establish consistency; trusted CI
provenance and hardware qualification are separate requirements.

Use the repository's `tools/ci/release.py extract --kind enhance-pir-v4-candidate`
with the full source SHA to verify and extract the archive. Do not use the legacy
`deploy-enhance-pir` workflow for this bundle.

## Disposable local validation

From the verified extracted bundle directory:

```sh
python3 test-v4-local.py --bin-dir . --out /tmp/enhance-v4-evidence \
  --seconds 30 --concurrency 1,2,4,8 --open-loop --expand-inventory
```

The output directory must not already exist. The runner starts separate workers
and a coordinator, checks exact synthetic records, records binary hashes and
reports, then terminates its own processes and removes its temporary data. The
inventory option runs four workers and tests demand-to-registration with a
configured forecast. All run on the same host; this is not off-host or per-worker
memory qualification. The load sweep allows overload errors for characterization;
inspect error counts rather than treating exit zero as release acceptance.

## Explicit isolated launch

For manual local investigation, use separate new data directories and separate
terminals. The example inventory matches these private worker origins:

```sh
./enhance-pir-v4 worker --listen 127.0.0.1:8291 --data-dir /tmp/enhance-v4-worker-1
./enhance-pir-v4 worker --listen 127.0.0.1:8292 --data-dir /tmp/enhance-v4-worker-2
./enhance-pir-v4 coordinator --listen 127.0.0.1:8280 \
  --data-dir /tmp/enhance-v4-coordinator --worker-config ./workers-v4.example.json \
  --isolated-fixture --fixture-records 67 --fixture-append-records 1
./enhance-pir-cli --v4 --server http://127.0.0.1:8280 query 33
```

For separate hosts, configure worker private addresses and firewall them to the
coordinator. Canonical ingestion uses `--zakura-cookie` and `--zakura-rpc-url`
instead of `--isolated-fixture`; the cookie value must stay outside this bundle.
Never mix fixture and canonical data directories or repurpose legacy serving
state. Use a parallel origin and dedicated service names for qualification.

## Promotion requirements

Production deployment remains gated on exact-release correctness and wallet
conformance, the active and sealed six-hour campaigns on actual 8 GiB workers,
publication-delay and load SLOs, measured host memory/cgroup limits, failure and
reorg recovery, rollback rehearsal, and the planned observation period. This
bundle contains no passing qualification receipt. Its worker bootstrap script
installs only an isolated unqualified candidate; production rollout remains gated. A dirty-source candidate cannot establish exact-commit release
provenance. The infrastructure request journal and guarded provisioning consumer
are implemented in the repository. They require an established initial pair and
isolated remote state; live execution and the qualification-to-registration
handoff remain unverified. The promotion workflow remains separate work.

## Dedicated Linux worker bootstrap

The bundle includes `bootstrap-v4-worker.py`. It requires a clean-source Linux
x86-64 candidate, the exact revision, and an independently trusted SHA-256 digest
of `SHA256SUMS` from release verification. A locally dirty or native macOS bundle
is rejected for this installation path. The existing deployment scripts do not
invoke this installer.

On the dedicated worker, `python3 bootstrap-v4-worker.py inspect` reports usable
RAM, available RAM, CPU count, swap, free disk, cgroup-v2 memory support, hostname,
and boot identity. Select an explicit JSON limits file with integer byte values
for `memory_high_bytes`, `memory_max_bytes`, `memory_swap_max_bytes`, and
`host_reserve_bytes`. The high limit must be 7 GiB; the maximum must exceed it but
remain at or below 7.5 GiB and fit measured host memory. The external host reserve
must be at least 512 MiB and also cover measured idle usage plus a 128 MiB margin.
Swap is bounded by the configured host swap and 2 GiB. These checks establish a
candidate configuration, not a qualified memory profile.

The install command requires root and all of `--bundle`, `--revision`,
`--manifest-sha256`, `--worker-name`, `--private-ipv4`, and `--limits`. The hostname
must match the requested replica, with four CPUs, nominal 8 GB RAM, and at least
32 GiB free on the worker data filesystem before initial installation. This is a
candidate floor for one worker; the 64 GiB recommendation for the local multi-worker
integration test host is separate. Full-size consolidation measurements peaked at
14.392 GiB per worker on macOS, leaving more than twice that sampled footprint
within the initial 32 GiB floor. Sampling can miss transient peaks and does not
establish six-hour disk stability: retain continuous disk-free measurements during
the native hardware campaigns and review growth/reclamation before qualification.
The c-4 profile has a 50 GB root disk; verify actual free space after OS packages
and swap creation. The private address must be the provider-verified VPC address.

Installation uses the non-root `enhance-pir-v4` identity, immutable binaries under
`/opt/enhance-pir-v4/releases/<binary-sha256>`, private data under
`/srv/enhance-pir-v4/worker`, and `enhance-pir-v4-worker.service`. It binds only the
specified VPC address on port 8291 and verifies both systemd settings and effective
cgroup limits. A private `bootstrap.json` records source/binary identity, host
measurements, limits, and fresh-worker readiness with `qualification: unqualified`.

Interrupted installation can resume only with identical inputs. After success,
a repeat verifies the existing service without restarting or stopping it; changed
inputs and assigned worker state require a separate procedure. Initial service
startup failure triggers a stop attempt and records whether it succeeded. No
inventory is registered or qualification claimed. The repository
pair-bootstrap driver orchestrates remote transfer and durable receipts. Live SSH
execution, actual Linux service startup, hardware campaigns, and promotion remain
outstanding; the generated service passes Linux parser validation.

## Hardware observation

The bundled `sample-v4-worker.py` produces one read-only sample of the dedicated
service. It records actual binary identity, boot/process identities, cgroup
current/peak memory, swap, memory events/statistics, CPU counters, pressure, host
memory and swap counters, disk space, accurate per-process `smaps_rollup` RSS/PSS,
and the kernel-reported RSS high-water mark. It checks effective cgroup limits
against the bootstrap receipt and rejects process or membership changes during
collection. Worker health contributes generation retention, candidate presence,
and runtime database accounting without recording per-query positions.

The repository's `observe-v4-pair.py` runs the sampler over pinned-key SSH from
the controller. Supply `--state-dir`, `--bootstrap-policy`, `--bundle`, `--ssh-key`,
`--known-hosts`, and a new `--out` directory. The default is six hours with a
five-second interval; shorter runs are useful for checking instrumentation but
cannot satisfy qualification. Targets and artifact identity must match the
persisted bootstrap operation. The observer does not launch a fixture or load
traffic and does not change operation state or inventory.

Every scheduled slot is retained. SSH/decoding failures, inactive services,
collection failures, and missed deadlines appear as explicit error records;
they are never replaced by zero memory. Traces include controller send/receive
and scheduled times as well as worker monotonic timestamps. An available Linux
controller machine-ID digest is compared to worker identity, and a match is
recorded as an off-host violation. A missing controller identity does not prove
that the harness ran off-host.

Counters and peaks are never reset. Compute deltas for cumulative events and
host swap-page counters, using the recorded page size; do not subtract memory
peaks or treat them as interval counters. Cgroup peak memory may include work
before observation. `smaps_rollup` reports resident/proportional memory at the
sample time; `/proc` status RSS/high-water fields have kernel accounting limits
and are not by themselves a proof that short peaks preserved the resident guard.
Keep cgroup charge, file cache, RSS, and PSS distinct when assessing the profile.
These interfaces follow the Linux [cgroup-v2](https://www.kernel.org/doc/html/v6.12/admin-guide/cgroup-v2.html)
and [procfs](https://docs.kernel.org/filesystems/proc.html) documentation.

A completed observation has `status: recorded`, a trace digest, an error count,
and `qualification: unqualified`. It is not a passing receipt. The full-size
active/sealed workloads, transition/recovery exercises, load/oracle results,
publication timing, and qualification verifier still need to be integrated and
run on the actual worker hardware. The evidence assessor below performs a subset
of the checks; it does not issue qualification.


## Synthetic workload driver

The candidate binary exposes `exercise` for fresh, isolated workers. Run it on
an off-worker controller with sufficient disk for the synthetic journal and
snapshot artifacts. Supply a new data directory and an inventory containing one
replica pair for `active`, or two pairs for `sealed`. Both replicas must be fresh
and idle; the command rejects existing published/candidate state and duplicate
worker identities. It writes a synthetic source marker and leaves its remote
fixture assignments in place when finished.

```sh
enhance-pir-v4 exercise --isolated-workers --profile active \
  --data-dir /srv/enhance-v4-active-run --worker-config /path/to/active-workers.json \
  --seconds 21600 --min-publications 300 --publication-interval 60 --concurrency 2
```

The active schedule keeps five assigned shards and cycles through loan growth,
return, owned-tail growth and rewind across return. The sealed schedule holds six
sealed shards on the oldest group while publishing appends on the second group.
Both require two published-ready replicas per shard after every publication.
Readiness at publication is not continuous replica health. Each publication
checks current unit/loan boundaries, retained tails and expired-session refresh.
The rewind changes branch hashes but preserves the positional fixture record
contents; it does not replace the alternate-ciphertext reorg oracle tests.

Background queries and validation probes share the configured concurrency budget.
The default two fits the coordinator query limit. Background p99 includes permit
waiting and failed attempts; it is closed-loop latency, not an open-loop arrival
or capacity result. Legitimate expired reorg-tail target reselections are counted
separately. Any wrong answer fails the run; background transport/query errors are
counted in the report and must be assessed even when recording completes.

Start hardware observation before workload initialization, and keep it running
through the complete workload. Duration starts after seed publications; the
command runs until both duration and publication minimum are met, so total time
can exceed six hours. Capture both pairs for the sealed profile. Retain
`exercise.json`, `publications.jsonl`, worker observations, binary/source identity
and controller provenance. The report includes a trace digest, publication timing,
exact-answer counts and refresh counts. `six_hour_publication_gate` only states
that duration/publication counts were reached; all reports remain unqualified.
Full hardware assessment, controlled outages, canonical independent wallet oracles
and registration/promotion gates remain separate requirements.

For a disposable local driver check from the repository:

```sh
cargo build --locked --profile release-fast -p enhance-pir-server --bin enhance-pir-v4
python3 enhance/ops/scripts/test-v4-exercise.py --out /tmp/v4-exercise-smoke
```

This helper runs two worker processes with a small fixture, cleans its temporary
worker state, and saves the workload report and trace. Its result cannot establish
full-size active/sealed capacity or satisfy the six-hour requirement.


## Committed participant recovery

A publication succeeds once its decision is durable and visible. Worker commit
notifications may remain pending; they are persisted with the decision and
retried independently by reconciliation and subsequent publications. Monitor
`pending_commit_notifications` in coordinator health and
`enhance_v4_pending_commit_notifications` in metrics. These counts are separate
from published replica readiness and do not prove current worker health.

A pending worker keeps its original committed candidate and retained runtimes.
The coordinator sends neither a new candidate nor a newer retention set to it,
and aborting a later publication does not abort its older committed candidate.
The healthy peer may continue ordinary publications. Operations requiring both
replicas remain subject to that requirement. Once the worker returns with its
durable artifacts, the coordinator reconstructs preparation, applies the original
decision, advances retention, and permits catch-up on a subsequent publication.
If that generation has already expired, it is reclaimed after applying the
decision. A lost commit response is resolved only by the matching published
manifest digest. Missing/corrupt worker artifacts keep the notification pending
and require repair; they do not authorize discarding the committed decision.


## Interrupted preparation and abort recovery

Before abandoning an uncommitted attempt, the coordinator atomically records
abort notifications and releases its candidate slot. It retries each worker
independently. An unreachable or busy worker keeps its candidate and reservations
and is excluded from new reservations and retention updates until its abort is
acknowledged. Healthy peers may continue ordinary publication; requirements for
two replicas on new groups and elective moves still apply. Cancelling a later
attempt preserves any earlier pending decision for an already excluded worker.

A successful publication also persists abort notifications for affected workers
that did not acknowledge activation. This covers a reservation accepted by a
worker whose response was lost. The worker's abort acknowledgement durably fences
that attempt, including delayed reserve requests. Lost abort responses leave the
notification pending and are resolved through an idempotent retry. A conflicting
worker candidate keeps recovery pending rather than authorizing deletion.

Monitor `pending_abort_notifications` in coordinator health and
`enhance_v4_pending_abort_notifications` in metrics alongside pending commit
notifications. These are outstanding work counts, not memory-release receipts.
Reconciliation and subsequent publications retry delivery. No manual state
clearing is needed for a worker that returns with its durable state intact.


## Runtime metrics

Scrape the coordinator's `/metrics` and each worker's private
`/internal/v4/metrics`. Attach `group` and `replica` target labels to worker scrapes
from the validated inventory. The worker endpoint stays on its existing private
listener; it does not create a public port. The coordinator exporter performs no
network probes during a scrape. Prometheus `up` for each worker job supplies scrape
reachability; published readiness and pending decisions are separate signals.

For the isolated local inventory, the scrape configuration is:

```yaml
scrape_configs:
  - job_name: enhance-v4-coordinator
    scrape_interval: 5s
    scrape_timeout: 3s
    static_configs:
      - targets: ["127.0.0.1:8280"]
  - job_name: enhance-v4-workers
    scrape_interval: 5s
    scrape_timeout: 3s
    metrics_path: /internal/v4/metrics
    static_configs:
      - targets: ["127.0.0.1:8291"]
        labels: {group: group-1, replica: replica-1}
      - targets: ["127.0.0.1:8292"]
        labels: {group: group-1, replica: replica-2}
```

Use actual validated private endpoints and the permitted off-worker collector
for deployment. Continuous scraping has not yet been deployed or qualified.

| Metric family (all use `enhance_v4_`) | Meaning |
|---|---|
| `shard_logical_rows`, `shard_used_rows`, `shard_state`, `shard_borrowing`, `unit_allocated_rows`, `unit_used_rows` | Current published geometry; unit labels use local unit boundaries, never a query's selected position. |
| `loan_active`, `rows_before_loan_return`, `rows_before_next_carveout` | Published lifecycle and remaining growth to its next logical boundary. |
| `group_role`, `group_assigned_shards`, `group_sealed_shards`, `group_pending_replicas` | Role and occupancy separately from replica availability. A pending replica is excluded from new attempts, not necessarily offline. |
| `placement_revision`, `controller_epoch`, `operation_phase`, `draining_operations`, `pending_*_notifications` | Durable placement and outstanding work. Free-text blocked reasons remain in health rather than metric labels. |
| `growth_rows_per_second`, `peak_growth_rows_per_second`, `capacity_remaining_rows`, `capacity_forecast_seconds` | Count/role capacity forecast; not proof that a future memory admission will succeed. |
| `expansion_readiness_budget_seconds`, `expansion_burst_budget_rows`, `expansion_requests_pending`, `expansion_target_groups` | Configured planning budgets and registration demand; no claim of measured readiness or hardware qualification. |
| `consolidation_planner_moves` | Moves selected by the pure placement preview; replica admission may defer them. |
| `publication_target_current`, `publication_target_*_delta`, `publication_pending_seconds` | Lag against the latest target actually attempted by this coordinator process. Signed deltas preserve rollback direction. Matching height/record counts with a different block hash is still pending. This is not chain-tip sync lag. |
| `last_publication_attempt_seconds`, `last_publication_attempt_succeeded` | End-to-end accepted attempt duration/result, including retry and notification work; not the timestamp at which the manifest became visible. |
| `worker_live_database_bytes`, `worker_published_database_bytes`, `worker_latest_database_bytes`, `worker_candidate_database_bytes` | Deduplicated database buffer bytes, with reference-specific subsets that may overlap. Do not sum those subsets. These exclude process overhead and are not RSS. |
| `worker_query_only_database_bytes` | Live buffers referenced by neither a published assignment nor candidate plans; includes query pins after expiry. |
| `worker_source_reclamation_database_bytes` | Live buffers outside the worker's latest publication and candidate, including older retained and query-only references. Such references must drain before reclamation. |
| `worker_model_*_bytes`, `worker_model_within_limit` | The same unqualified model used for admission: distinct planned/retained/live database buffers plus future growth, transition and overhead reservations. These are not measurements of cgroup memory or a qualification receipt. |

Availability gauges distinguish missing observations from zero. During engine
preparation, `worker_memory_sample_available` and `worker_memory_model_available`
become zero and their byte samples are omitted. Scrapes do not wait for the engine
lock. Publication pending time measures the age of the current target's first
attempt; it resets when that target changes. Publication-target observations reset
on coordinator restart and remain unavailable until the next attempt. Capacity policy fields from older journals
remain absent until another observation records the policy. Clock regression is
reported separately from capacity-observation age.

The workload command saves checksummed final coordinator/worker scrapes under
`metrics/`, indexed by `final_metrics` in `exercise.json`. These snapshots verify
final state only. Keep continuous runtime metrics and the existing independent
cgroup/RSS/PSS/swap observer running throughout initialization, transitions and
the full hardware campaign. Final snapshots cannot establish peaks or the resident
guard, and the 728 MiB model overhead remains an estimate.


## Continuous runtime and host samples

The bundled hardware sampler now emits **sample version 2**. Each successful
sample includes `runtime_metrics.values` from the private worker exporter,
a digest of the raw exposition, and start/finish monotonic timestamps for that
scrape. The byte counters retain their units and remain separate from cgroup,
RSS, PSS, swap and kernel high-water measurements. The parser accepts the
worker's bounded scalar contract and rejects duplicates, labels, unknown fields,
nonfinite/fractional values, integers beyond exact float64 range, and values
inconsistent with the exporter availability flags. It bypasses HTTP proxies and
does not follow redirects away from the selected worker endpoint.

Worker health brackets each runtime scrape. `state_consistent` compares process
incarnation, controller epoch, worker revision, retained generations and candidate
identity, and matches the applicable runtime counters. A candidate replacement
with unchanged publication revision is still detected. A normal concurrent
publication can make this flag false; both health observations and the metric
values remain in the trace. This checks publication/attempt identity, not an
atomic memory snapshot. The host-memory collection end time and runtime scrape
window are separate, so do not treat their byte values as simultaneous.

When the engine is busy, valid samples contain
`memory_sample_available: 0` and omit memory bytes rather than reporting zero.
A failed or invalid runtime scrape sets `error: runtime_metrics_failed` while
preserving host/kernel measurements already collected. Existing process-identity
and cgroup-membership checks still run afterward.

The off-host observer requires version 2 for successful records. Its manifest
records `sample_version`, `unavailable_memory_samples` and
`inconsistent_runtime_samples` separately from collection/transport errors and
missed deadlines. Older successful version-1 samples are rejected as incomplete
for this contract. Use the matching newly built candidate bundle and observer;
previous bundles are not evidence of continuous runtime-metric coverage.

These counters expose evidence gaps for assessment. Neither a recorded trace nor
zero collection errors is a passing qualification receipt. Continuous coordinator
scraping remains a separate collector job; this SSH sampler records worker and
host evidence only.


## Campaign evidence assessment

Run the repository's assessor after the workload and all observations finish:

```bash
python3 enhance/ops/scripts/assess-v4-campaign.py \
  --workload /campaign/workload \
  --bundle /artifacts/verified-linux-candidate \
  --pair /campaign/pair-1/journal /campaign/pair-1/bootstrap.json /campaign/pair-1/observation \
  --out /campaign/assessment.json
```

Use one `--pair` for the active profile and two for the sealed profile. The output
must be new. The assessor checks each pair against its frozen provisioning
journal, bootstrap policy, receipts and clean candidate bundle. Successful worker
samples must match the selected artifact, revision, bootstrap manifest, boot,
hostname, private address, measured host RAM and effective cgroup limits; matching
only the worker's local receipt is insufficient. The observer also enforces this
binding while collecting evidence.

The audit hashes trace bytes as it parses them, rejects duplicate JSON keys,
checks six-hour duration and at least 300 publications, required profile stages,
role limits, retention, replica readiness, query errors, observation slots and
coverage. It detects worker restarts, OOM counter increases, counter resets,
identity conflicts, missing runtime observations and sampled resident-guard
violations. It records maximum sampled RSS plus kernel memory separately from
cgroup peak charge. A short smoke cannot satisfy these gates.

Exit zero and `status: evidence_checks_passed` mean only those automated checks
passed. Every result retains `qualification: unqualified`. The report lists the
remaining gates: resident peaks between samples, overhead calibration, host hard
cap and reclaim acceptance, cross-host clock alignment and host identity provenance, latency
and open-loop capacity, and protocol/wallet conformance. A synthetic fixture that
passes this audit establishes parser/check behavior only. The assessor neither
registers workers nor permits promotion, and is not a signed attestation of the
evidence source.


The workload report includes `workload_host_id_sha256`, derived from the raw
Linux `/etc/machine-id` bytes using the same digest convention as the sampler.
An unavailable or malformed identity is recorded as null, so a local macOS
smoke remains usable but cannot pass the hardware evidence audit. The assessor
requires a valid digest distinct from every observed worker. The load generator
may share the observer host. This check detects accidental co-location; it does
not authenticate machine IDs or prove cross-host clock synchronization.


## Local compiled-cache recovery

On restart the worker loads retained compiled units, rebuilding missing or
rejected cache artifacts from its durable `rows/` files when available. Every
rebuild checks the padded row-file length and content digest against the retained
unit identity. Startup completes only after retained evaluations are available;
it does not clear or rewrite the committed publication journal to bypass an
error. Normal candidate preparation uses the same verified source reader.

If both the compiled cache and its durable rows are missing or corrupt, startup
fails. Automated peer/coordinator row restoration remains an implementation gate.
Retain the worker directory for repair; deleting its committed state is not a
recovery procedure. Rebuild time and peak memory still need hardware measurement.


For offline row repair, stage a peer's content-addressed row files in a local
archive directory, stop the affected worker service, then run as the worker's
service user:

```bash
enhance-pir-v4 repair-rows \
  --data-dir /var/lib/enhance-pir-v4 \
  --source-rows /path/to/peer-row-archive
```

The worker lock prevents repair while a worker process holds the directory.
The local journal must exist and its referenced plans must validate. Both
retained publications and the durable candidate contribute unit identities.
Valid destination files are kept, and only missing or invalid referenced files
are replaced. Source files must match the complete padded length and digest;
unreferenced files and peer metadata are never copied. A failure may follow
successful repairs of earlier units; rerunning is safe because each replacement
is atomic and synced. A successful command reports `rows_verified` and the number
of restored units, not worker readiness or qualification.

Restart the worker, verify its retained generations and manifest digests against
the coordinator, and perform exact-answer retained/current queries before returning
it to service. This command does not transfer peer files, register a worker,
repair a lost journal, or reconstruct a unit that no available source retains.
Keep the source archive outside directories subject to concurrent garbage
collection. Automated remote repair and deployed recovery campaigns remain open.


## Interrupted project-membership recovery

When an interrupted apply has created every requested worker, provisioning
reconciles the recorded IDs against Terraform state and the provider before
examining a new full plan. If the only remaining creates are project-membership
records for these verified workers, it persists reconciliation evidence and
marks the attempt retryable. That invocation performs no apply. Run provisioning
again to obtain a fresh validated plan and finish membership; the fresh plan may
not widen recovery to create droplets, tags or firewalls. A no-op recovery plan
marks the existing attempt provisioned directly.

Incomplete worker pairs, provider orphans, changed identities and unfinished
firewall/tag resources remain fenced for explicit recovery. This behavior has
mocked provider/Terraform coverage; live interrupted-apply validation remains
outstanding.


Load reports include `warmup_errors`, `warmup_correct_answers` and
`warmup_incorrect_answers`. Warmup transport failures and HTTP 429/503 responses
are counted and retried within the warmup period rather than terminating an
overload characterization before measurement. Other client/protocol failures
remain fatal. Every successful warmup answer is checked against the selected
oracle, and any incorrect answer in warmup or measurement fails the run.
Warmup counters are separate from measured latency, throughput and the measured
error-rate threshold; review both phases for qualification. An overload run with
`--max-error-rate 1` is characterization, not zero-error acceptance evidence.


## Memory-triggered demand

Private worker admission/reservation returns HTTP 507 specifically when the
full growth/transition memory budget exceeds the resident model limit. Candidate
contention, invalid plans and stale commands retain their existing failure status.
A coordinator reservation receiving 507 persists `capacity.memory_limit` with
records, fleet group count and observation time, then requests the next pair
immediately if none is pending. Four groups sets the capacity-ceiling flag without
requesting a fifth. Repeated failures do not create duplicate pair requests.

The earlier record boundary remains part of forecasting for that fleet size and
survives restart. Once a larger inventory is registered, forecasting stops using
the old fleet's limit. Demand alone does not provision or register workers. The
memory model remains unqualified until the hardware campaigns establish its
allowances; this signal is not proof of measured exhaustion or qualification.

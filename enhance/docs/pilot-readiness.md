# Enhance limited production rollout

This is the release gate for an opt-in pilot at **2 aggregate queries/second**,
qualified at **4 queries/second**. It is not a record of completed acceptance.
The source, load driver, wallet, configuration, and binary hashes must identify
one candidate. A source or deployment change invalidates affected results.

## Release and wallet gates

1. Build from a clean, pinned checkout. Require CI full on that exact revision
   and run `make check`; full-size tests need optimized code and sufficient RAM.
   CI deliberately excludes some full-size fixtures and is not hardware acceptance.
2. Verify release checksums on every host. Record actual coordinator **and worker**
   revisions; a mixed fleet is not evidence for a uniform candidate.
3. Complete the q48 matrix on ARM64 and native x86 Linux: 432 cases and 55,296
   queries per platform, zero decoding failures, the accepted 78-bit per-query
   sufficient bound, and matching deterministic evidence. Bind acceptance to
   trusted extraction, exact dependencies and deployed snapshot/public setup.
   Earlier matrix success cannot silently qualify a different dependency pin.
4. Run both independent wallet harnesses:

   ```sh
   RAYON_NUM_THREADS=2 cargo run --locked --release \
     --manifest-path enhance/tools/q48-interop/Cargo.toml
   python3 enhance/tools/schema11-interop/run.py /absolute/path/to/pinned-wallet
   ```

   The required wallet revision is `9b190657d129d08e964623d0ecc1d8e4ffb31b1d`
   (PR #29, v6/q48). The first harness covers all 12 supported allocations;
   the second covers real loopback HTTP, fresh generation acceptance, scanned
   SQLite wallet recovery and atomic rejection of corrupt records.
5. Validate the consuming wallet release through public HTTPS using independently
   reconstructed chain records. Include restore, resume, boundaries, retained and
   expired generations, bounded retry, cancellation and controlled reorgs.
   Neither loopback tests nor a CLI/journal oracle establish this gate. No false
   completion, incorrect balance, lost committed state or plaintext fallback is
   accepted. Reject v5/q46 clients and state without modifying stored data.

## Sustained load and physical workers

Use isolated state on representative physical workers. Run active and six-sealed
profiles for at least six measured hours and 300 publications each. Every replica
needs uninterrupted hardware samples spanning initialization and measurement.
Exercise retention, reclamation, growth and placement transitions. Seven sealed
shards remain disabled. Use `assess-campaign.py`, then explicitly review its
unproven gates; it intentionally never emits a qualification certificate.

Start worker observation before the public load window as well. Bootstrap-managed
workers use `sample-loop.py` with their bootstrap receipt. Directly deployed v6
workers use `--direct-policy /etc/enhance-pir-v6/sampling.json`: the root-only
policy pins the worker name, release revision, binary and checksum-manifest hashes,
private address and port, and the three effective memory limits. The sampler
checks those against the running process, cgroup, private health and metrics on
every sample. Keep the raw worker-local traces and summarize immutable copies with
`summarize-samples.py`; a sampler started after a stage began cannot cover that
stage. On the coordinator, `observe-freshness.py` records the local node tip and
published anchor without writing RPC credentials to its trace. Retain its raw
trace to assess the five-minute freshness gate over the same window.

The direct sampling policy has this nonsecret shape; use observed values from
the verified release and effective `systemctl show` limits, then install it
mode 0600 on its matching worker:

```json
{
  "kind": "enhance-direct-worker-sampling-v1",
  "worker_name": "enhance-pir-worker-01",
  "revision": "<40-character source revision>",
  "binary_sha256": "<server binary SHA-256>",
  "manifest_sha256": "<SHA256SUMS file SHA-256>",
  "private_ipv4": "<worker private IPv4>",
  "private_port": 8091,
  "limits": {
    "memory_high_bytes": 7516192768,
    "memory_max_bytes": 7609516032,
    "memory_swap_max_bytes": 2147479552
  }
}
```

Run the public workload from a client outside the coordinator/worker hosts:

```sh
cargo build --locked --release -p enhance-pir-load-test
python3 enhance/ops/scripts/qualify-public.py \
  --binary target/release/enhance-pir-load-test \
  --server https://enhance-pir.valargroup.dev \
  --oracle /absolute/path/to/independent-oracle.json \
  --source-revision LOAD_DRIVER_FULL_GIT_SHA \
  --server-revision VERIFIED_SERVER_FULL_GIT_SHA \
  --out /absolute/path/to/new-run-directory
```

The runner executes 30 minutes each at 1, 2 and 4 offered QPS, a six-hour soak at
4 QPS, then a five-minute eight-client burst. It preserves each command, log,
report and digest and stops on a failed stage. Use duration overrides for smoke
checks only; their result is `short_run_only`. A journal-derived oracle can
validate retrieval, but independent chain extraction is still required for
end-to-end correctness. The runner records claimed revisions; operators must
verify those against the actual binaries and keep worker observations separately.

| Gate | Required result |
|---|---|
| Correctness | Zero incorrect answers, including warmup |
| Normal load | At least 99.9% success, including errors and unstarted arrivals |
| Latency | Successful-query and scheduled-to-completion p99 at most 1 second |
| Freshness | Published coverage within 5 minutes of upstream availability |
| Memory/disk | Existing deployment guards satisfied, no OOM, worker swap or growing backlog |
| Observation | Complete samples from every physical worker; no missing intervals |
| Single replica loss | Sustain the 2 QPS pilot envelope |
| Restart/rollback | Correct service restored within 10 minutes |

The load report retains the original all-attempt `p50_ms`, `p95_ms`, `p99_ms` and
`scheduled_p99_ms` fields. New `successful_*` fields include only exact correct
answers and are null when none completed. `--slo-p99-ms` checks both populations
so fast rejections cannot hide slow successful queries. Warmup errors are retained
and included in the public runner's availability assessment. Burst rejection is
recorded separately and cannot justify raising the pilot envelope.

## Recovery and operations

Rehearse one fault at a time on isolated infrastructure: worker loss, replica
repair/rebuild, coordinator restart, interrupted publication, delayed reclamation,
controlled reorg and failed deployment. Verify exact answers and restored
replication after each fault. Preserve canonical serving data during synthetic
campaigns. Record rollback timings with compatible wallet behavior and separate
old release/data directories; never open new state with an incompatible binary.

Verify worker port 8091 is unreachable from the public internet and private
coordinator services bind only to their intended interfaces. Check TLS, request
limits and deadlines. Exercise alerts for stale publication, missing replicas,
errors/rejections, resource pressure and missing scrapes; name incident and
release owners. Preserve rollback data for the entire qualification and pilot
window, including checking any scheduled cleanup timers.

## Admission decision

Only after all gates pass, admit opt-in wallets within the 2 QPS aggregate budget
and observe for 24 hours. Bound client concurrency and retries; cohort size alone
does not bound restore bursts. Stop expansion on any wrong answer, false completion,
state loss, OOM, persistent freshness failure or unexplained recurring 5xx.
Release-affecting fixes restart the observation window and affected gates.

Store source/configuration identities, commands, raw failures, complete metrics,
recovery timings, supported workload and the signed-off acceptance decision in
one evidence bundle. `load_gates_passed` and `evidence_checks_passed` are partial
results; neither authorizes rollout by itself.

# Status PIR production qualification

The deployed `status-pir-v3-native-two-mask-m29` service must pass the complete
live-source gate for release qualification. One unified publisher owns source
capture and publication; separate query worker and router processes serve
wallets through the existing ingress and autonomous HTTPS publication synchronization. Loopback evaluation and
wallet forwarding remain separate from publisher distribution.

The database uses 8,192 rows, 256 slots per row, 40 bytes per slot, 12,288 padded
row bytes, 6,144 u16 columns and a 96 MiB database. Its 75% admission ceiling
remains 1,572,864 entries. The wallet and server must use the same native contract
and 20-second freshness limit. Block hashes remain internal source/publication
metadata; wallet replies are status observations, not inclusion proofs.
Historical v1/v2 timing and resource captures cannot qualify the native service.
Preserve their raw files and remeasure every gate with the deployed protocol.

## Residual risks

Passing the gates below does not close these; they are accepted with the
release decision. (1) Mempool bucket grinding against the public salt can force
publication `Capacity` failures and is not mitigated in code; it degrades
availability (stale, then 503), not privacy. (2) `NotFound` and `observed_ms`
are server assertions, not proofs; see "Residual risks" and the trust boundary
in `architecture_status.md`. (3) Qualification requires a wallet client that implements the deployed native
protocol; results from an older client cannot establish native wallet compatibility.

## Capture contract

Run the production candidate at full 8,192-row geometry on the intended host.
Schedule 20 open-loop complete lookups per second for at least 21,600 seconds.
Each lookup includes manifest initialization, public-material download, client
setup, encrypted query, decode, and comparison with an independent live-source
oracle. The client retains local coverage evidence for negative answers.

Retain four JSONL files:

- `summary.jsonl`: one `phase: "load"` object with `protocol: "status-pir-v3-native-two-mask-m29"`,
  `rows: 8192`, `slots: 256`, `slot_bytes: 40`, `columns: 6144`,
  `row_bytes: 12288`, `database_bytes: 100663296`, `source: "live"`,
  `oracle_source: "independent"`, `seconds`, `offered`, `run_started_ms`, and
  `run_ended_ms`, and independently captured `source_observations` total.
- `requests.jsonl`: one object per scheduled arrival, numbered from zero, with
  `arrival`, nominal `scheduled_ms`, actual `completed_ms`, `result` (`correct`,
  `error`, or `unstarted`), `duration_ms`, and `observation_age_ms`. Omit txids,
  encrypted queries, and decrypted outcomes.
- `publications.jsonl`: one terminal record per completed source observation,
  with unique increasing `observation_id`, `kind` (`block`, `mempool`, or
  `unchanged`), `source_observed_ms`, and `disposition`. Published records require
  `queryable_ms`, independent `oracle_match`, `canonical_continuity`, and
  `complete_mempool` confirmations. Superseded records identify `superseded_by`.
  Every supersession chain must terminate in a verified publication within
  twenty seconds of the **original** observation; repeated supersession cannot
  reset this deadline. Missing successors, unexplained drops, and publication
  evidence gaps greater than twenty seconds fail the gate. Only actual published
  block/mempool records establish the two required update kinds.
- `resources.jsonl`: at least one sample per minute with `sampled_ms`, cumulative
  `oom_events`, and current `swap_bytes`; retain CPU, RAM, VRAM, disk, and
  process-incarnation fields alongside these required fields.

Capture normal block and mempool updates and recorded bursts while the load is
running. Keep process restarts and downtime in the arrival record. Run fault
injection separately so expected unavailability does not contaminate the
healthy-load gate. Record source and binary hashes, workload inputs, hardware,
and raw timelines under the [evidence policy](../../evidence/README.md).

## Automated gate

```sh
python3 tools/status/status_qualification.py \
  --summary summary.jsonl --requests requests.jsonl \
  --publications publications.jsonl --resources resources.jsonl
```

The read-only `tools/scripts/wallet-pir-status-capture.py` collector independently
encodes rows from the node's canonical block JSON and complete mempool. Run it
for 21,615 seconds beside the native live-load client: it measures six hours of
source observations, then drains their verification for fifteen seconds. Keep
its raw observations and resource samples. Align the assessor's observation IDs
and resource samples to the load client's actual timestamps; missing or failed
captures must remain failures. Require the collector to exit successfully and its
`result.json` to report `passed: true`; the assessor alone does not consume
collector errors. The collector verifies retained block contents against the node,
starts at the retained coverage boundary, and permanently prunes capacity-evicted
blocks so reduced mempool pressure cannot restore discarded history.
If block reads overlap a reorganization, the collector retries from its last
validated history; an incomplete or disconnected read never replaces that state.

Freeze retained fork inputs before the cutover and provide the frozen file with
`--retained-source` and `--retained-source-sha256`. Old fork blocks may no longer
be available from the node: those records use the unchanged pre-cutover raw
inputs as their expected values, with an independent check that their block is
noncanonical. This establishes preservation of historical fork inputs, not a
new independent reconstruction of discarded blocks. Canonical blocks and the
mempool are always read independently from the node; the collector does not
trust a publisher's rows or digest.

The assessor accepts only the deployed native Status protocol. It retains the
same geometry, six-hour arrival coverage, independent source/publication oracle,
freshness and resource gates. Historical compact-v2 captures cannot qualify the
current service or a wallet without a native client.

The assessor exits nonzero for missing evidence, any failed or skipped arrival,
stale answer, publication taking more than twenty seconds, incorrect publication,
one-second p99 breach, OOM, or swap use. It also requires both block and
mempool publication evidence. Passing this automated gate does not replace the
protocol/privacy review, restart and reorg exercises, incremental-versus-full
rebuild comparison, or tested rollback.

The synthetic probe supports long-run evidence collection with
`--seconds 21600 --qps 20 --concurrency 16 --report-jsonl requests.jsonl
--max-p99-ms 1000`; its summary is labeled `source: "synthetic_fixture"` and
`production_qualified: false`. It is useful for backend soak testing but is
ineligible for the production assessor.

The distributed controller's operational journal is not a complete independent
publication capture. The `probe-live-load` command supplies a canonical-mined
request oracle and arrival records only. Neither output alone can pass this
assessor; independent publication and resource evidence is mandatory.

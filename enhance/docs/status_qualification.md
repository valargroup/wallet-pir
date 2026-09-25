# Status PIR production qualification

The public Status API is blocked until a live-source release candidate passes
this gate. The `status-pir serve-live` entry point connects observation to
isolated loopback serving with a durable source cache and publication counter.
Its roles still share one process and it uses the synthetic protocol identifier.
The one-minute 20-QPS fixture result and isolated fault checks do not qualify production.
The recorded one-row update took about 13.8 seconds. That isolated preparation
fits the revised twenty-second budget, but excludes live source collection,
activation, request overlap, and publication under load.

Before collecting release evidence, integrate Status with the deployed shared
coordinator and HTTPS origin, implement authenticated publication and revocation
acknowledgments across separate role processes, qualify preparation during live
traffic, and review a compatible release protocol. The Vizor client currently
targets `status-pir-v1-q48` while this backend advertises
`status-pir-v1-synthetic-q48`; their pinned PIR library revisions also differ.
The pinned wallet protocol also uses a ten-second freshness limit, below the
agreed twenty-second service gate. Align and test these interfaces together.
The wallet release gate remains disabled until those interfaces are reconciled.
The [September 25 SSH trial](../evidence/status-live-trial-2026-09-25/README.md)
validates the isolated P4000 synthetic service and a separate live-source CPU
trial; it does not meet this production gate.

## Capture contract

Run the production candidate at full 8,192-row geometry on the intended host.
Schedule 20 open-loop complete lookups per second for at least 21,600 seconds.
Each lookup includes manifest initialization, public-material download, client
setup, encrypted query, decode, and comparison with an independent live-source
oracle. The client retains local coverage evidence for negative answers.

Retain four JSONL files:

- `summary.jsonl`: one `phase: "load"` object with `source: "live"`,
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
python3 enhance/tools/status_qualification.py \
  --summary summary.jsonl --requests requests.jsonl \
  --publications publications.jsonl --resources resources.jsonl
```

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

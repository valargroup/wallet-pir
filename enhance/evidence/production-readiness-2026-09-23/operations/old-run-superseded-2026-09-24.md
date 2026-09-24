# Superseded public run and post-load correctness check

Status: **diagnostic, incomplete, unqualified**. The public run used source and
deployed server `527048217f8cb83d27c838e94dbaac21ac25837d`. Once the
journal crash-recovery fixes produced a new server binary, this run could no
longer qualify the release. The operator sent SIGTERM to its 4 QPS soak child
at 2026-09-24 03:03 UTC after about 4½ measured hours. The parent exited and
recorded the expected `missing_report` and `load_process_failed` findings in
its [unaltered manifest](../public-run-superseded-manifest.json). There is no
six-hour soak report or eight-client burst for this run. The interruption was
an operator decision, not an observed request or server failure.

The three completed public HTTPS stages remain useful diagnostics. Their
[1 QPS](../public-full-observed-5270482/rate-1.json),
[2 QPS](../public-full-observed-5270482/rate-2.json) and
[4 QPS](../public-full-observed-5270482/rate-4.json) reports record 1,800,
3,600 and 7,200 exact answers, respectively, with zero wrong answers, request
errors or unstarted arrivals. Scheduled p99 was 252.671, 393.983 and
233.983 ms. Their source and serving binary were the earlier candidate.

The copied coordinator trace covers the soak through the operator stop and a
five-minute tail. Its SHA-256 is
`a7e34b89f4f65b7360b56c608cadaa834f54c5910a995ae60293a23c25e74869`
at `/tmp/wallet-pir-readiness/public-records/old-5270482-full/freshness/samples.jsonl`.
The [assessment](../soak-to-operator-stop-freshness.json) bounded all 198
sampled tip advances within 70.001 seconds and found no collection gap or
ambiguous height advance. It remains `evidence_incomplete` because the
observer recorded the brief same-height reorg publication block. Node RPC
after the load returned canonical hash
`000000000034b1c2e2c9b9f0b94fc6ad2403733516fdb63e6003590ea3af2abd`
at height 3,494,062, matching the replacement block in the
[incident log](soak-reorg-2026-09-24.md).

The two copied worker traces each have 22,250 valid samples and zero sample
errors or findings across all measured public stages and the partial soak.
Their exact digests are `03aff813cf57f9bbcc60a70b71e4e75f4721343be27a92d7bff15da04f4cf0cc`
and `6dc1c5d8eaeab397ed980ecbe71a317e72f4de001a03fc6b602526126075d877`.
Both final sampler manifests match those digests and counts. Maximum gaps were
1.019 and 1.003 seconds; cgroup peaks were 4.828 and 4.947 GB, with zero
worker swap, memory-high events or OOM events. Runtime-specific memory samples
were unavailable in 679 and 696 of the 22,250 samples; cgroup and process
memory observations remained available and the assessor found no gap. The
immutable raw traces and summaries are under
`/tmp/wallet-pir-readiness/public-records/old-5270482-full/` on the operator
machine; they are too large for Git.

The older freshness observer did not finalize its manifest on SIGTERM, so its
on-host manifest still says `running`. The copied trace is frozen and its
hash and sample count are recorded separately; this is an evidence limitation,
not a claim that the process remained live. The observer was inactive after
`systemctl stop`. The script now finalizes an interrupted manifest for future
runs.

With measured load stopped, the [chain oracle](../chain-oracle-old-5270482/manifest.json)
reconstructed 16 positions from canonical raw blocks without reading the PIR
journal. The first external client attempt safely rejected a moved public
generation. A fresh extraction then matched generation 429, published anchor
3,494,115 and oracle SHA-256
`c1b254842ed00b432bf59e9b53eb85aa44d840a97e50b2aa3cd986e15be943c9`.
The pinned Linux wallet client (SHA-256
`97a51f5fc6a10b41c5f962871631db8308775a93e5c9e71f8729a733e5fdc179`)
returned [16 exact public HTTPS answers](../chain-oracle-old-5270482/public-client.json).
This checks positional correctness on the old fleet after the reorg; it does
not exercise an actual consuming wallet release or qualify the corrected
server binary.

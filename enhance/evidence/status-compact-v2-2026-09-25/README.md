# Compact Status v2 — production measurements, 2026-09-25

**Public Status remains blocked and disabled.** Compact v2 is deployed to the
private production qualification topology. A six-hour diagnostic load run is
active; its completion and full qualification are not established by this report.

## Contract and deployment

- `status-pir-v2-q48`, `SPQ2`; separate v2 bucket/setup/manifest domains.
- 8,192 rows × 256 slots × 40 logical bytes; 12,288 padded row bytes,
  6,144 u16 columns, 100,663,296 database bytes (96 MiB).
- Admission ceiling: 1,572,864 entries. Freshness ceiling: 20,000 ms.
- Source checkpoints retain canonical/parent/fork hashes in unchanged `STSOBS01`
  bytes. Compact replies are observations, not transaction-inclusion proofs.
- Independent wallet revision: `e153ad393ac4bfb12ac1e06a57a31f631fb9ce3e`.
  Vizor's related library pins were updated together and `cargo check` passed;
  its mixed working tree remains uncommitted and release enablement is disabled.
- P4000 executable SHA-256:
  `42518d35e0a6b604ce6ad5657bbcf72022a55e35477894f975b9c80ea4d4ae9a`.
- Coordinator executable SHA-256:
  `ef6bf345d346ac6fbe411d75f4d6e3dbece8b39c3510691ab37863003c1cc52b`.
- Separate `status-worker`, `status-router`, and
  `status-controller-qualification` services use the existing authenticated SSH
  tunnel. Durable source/authority/fence directories were preserved. The deployed
  Enhance coordinator executable was not replaced.

## Measured gates

| Gate | Result | Evidence / limitation |
| --- | --- | --- |
| Codec, v1 rejection, canonical source, durable recovery, sparse arithmetic | Pass | 5 local protocol tests and 27 local server Status tests |
| Independent wallet contract | Pass | 14 wallet tests; release CPU interoperability at maximum admitted occupancy |
| Independent wallet / CUDA interoperability | Pass | All four encrypted results at 1,572,864 entries; wrong session and v1 envelope rejected |
| Separate process CUDA path | Pass | Maximum occupancy, three processes, all four outcomes and remote fencing |
| Full preparation at maximum occupancy | 5,994 ms | GPU hint 2,917 ms; packing 3,078 ms; index construction separately 5,445 ms |
| One-row update | 2,282 ms | GPU hint 614 ms; packing 1,668 ms; index construction separately 5,107 ms; measured without 20-QPS load |
| Update/retry/failure scenarios | Pass | Mempool-to-mined transition, full-hint differential, 409/410 retry, worker loss, epoch fencing; source reorg/forks covered by local tests |
| Private live 20-QPS smoke | **Fail** | 1,185 correct, 5 HTTP 429 failures, 10 unstarted out of 1,200 offered in 60 seconds |
| Successful-request p99 | 927.524 ms | Failed and unstarted requests are excluded from this latency statistic; availability still fails |
| Live publication timing sample | 28 publications, maximum 14,049 ms | Includes cold start; median 891 ms; epoch 65; not complete observation/supersession evidence |
| Deployed worker restart | Recovered | Observed 503 interval to first new-epoch 200: 15,545 ms; epoch 65 → 66; successful manifests stayed under 20 seconds |
| Resource snapshots | No OOM or swap | P4000 GPU 551 MiB; worker ~1.19 GB and router ~1.30 GB cgroup memory after recovery; not soak peaks |
| Public routing isolation | Pass | Public Status init 404; public Enhance init 200 |
| Full repository `make check` | Pass | Ops/tools/docs, formatting, strict all-feature Clippy, workspace release tests and doc tests; opt-in hardware fixtures remain ignored |
| Six-hour joint load/publication run | Running | Started 2026-09-25 15:16:33 UTC, expected finish about 21:16:33 UTC |

V1 comparison: the prior short campaign recorded 867/1,200 correct lookups.
This rollout also contains the previous checkpoint's undeployed recovery fixes,
so the change in success rate cannot be attributed solely to compact geometry.

## Remaining public-release blockers

1. The short joint availability gate failed (429s and unstarted arrivals).
2. Six-hour results have not been collected and assessed. The current load oracle
   checks canonical mined answers only. Publication journals do not establish
   complete independent block/mempool correctness or account for every
   superseded observation; a successful soak alone cannot pass the full gate.
3. The deployed shared Enhance coordinator cutover, restricted HTTPS Status
   rehearsal, and live Status APM integration remain unqualified.
4. Independent protocol review and the full admitted update envelope under joint
   load remain open. The one-row fixture is not a dense-update qualification.

## Running soak and evidence locations

On coordinator `167.99.42.60`:

- `status-compact-v2-soak.service` offers 20 complete encrypted lookups/sec for
  21,600 seconds. Output: `/var/lib/status-pir-controller/compact-v2-soak/`.
- `status-compact-v2-publications.service` captures controller journal records in
  `/var/lib/status-pir-controller/compact-v2-soak-journal.jsonl`.
- `status-compact-v2-resources.service` samples every five seconds to
  `/var/lib/status-pir-controller/compact-v2-soak-resources.jsonl`.

On `status-pir-p4000-ams1`, `status-compact-v2-resources.service` samples worker,
router, GPU memory, swap and cgroup OOM counters to
`/var/lib/status-pir-worker/compact-v2-soak-resources.jsonl`.

The transient units have bounded runtimes and do not enable public routes. Load
failures are recorded per arrival and cause a nonzero final load exit. The
qualifier rejects missing v2 geometry and missing production evidence. Do not
convert these operational publication records into `oracle_match: true` or
invent supersession records to make the gate pass.

`status-compact-v2-report.service` will write
`compact-v2-soak/measurement-report.json` after the load exits, recomputing
arrival counts and successful-request p99. It never grants production
qualification. Initial extended-run samples already include HTTP 429 failures.

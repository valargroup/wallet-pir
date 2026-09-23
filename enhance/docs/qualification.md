# Qualification and current status

Latest rollout: [architecture update 2](../evidence/architecture2-2026-09-24/README.md)
is deployed directly over SSH with dedicated packing, query ingress and two-worker
pool placement. Its physical 8-GiB router and initial public-wallet checks passed;
see that evidence for measured limits and sustained-run status. The historical
v7 qualification below remains a separate record.

This page records dated evidence, not a live fleet report. Protocol v7 was
implemented and deployed directly over SSH on September 24, 2026 from `2a83c21`.
The [v7 report](../evidence/immutable-v7-2026-09-24/README.md) binds source, binary,
wallet PR, raw tests and samples. Public exact answers, wallet interoperability,
rollback and restart passed. **Hardware qualification remains incomplete:** the
31-minute larger-dataset run had zero query errors but failed its strict memory
gate because of reclaim pressure and swap growth. Short public load runs also
exceeded the proposed p99 regression gate. No capacity increase is authorized by
these results. The full six-hour qualification remains outstanding.

| Evidence | Recorded result | Limit |
|---|---|---|
| [V7 SSH deployment and focused validation](../evidence/immutable-v7-2026-09-24/README.md) | Correct public wallet/native answers; 35 publications and 24,506 correct background answers; rollback and restart | Memory/swap and short-run p99 gates failed; six-hour qualification outstanding |
| [Protocol-v6 production cutover](../evidence/protocol-v6-production-2026-09-23/README.md) | Two ready replicas and exact-answer public checks after cutover | No full CI or sustained qualification; eight-way load hit the two-query admission limit |
| [Schema-11 interoperability](../evidence/schema11-suffix-2026-09-23/README.md) | Suffix record and wallet checks | Local; no fleet qualification |
| [Production deployment](../evidence/architecture-v4-production-2026-09-23/README.md) | Exact answers, replicated publication, failover, restart, repair, and rollback rehearsal | Previous candidate; short load and a worker sampling gap |
| [Active campaign](../evidence/architecture-v4-production-2026-09-23/README.md) | Six hours, 320 publications, 225,939 correct background answers | Worker-1 sample gap prevents uninterrupted hardware qualification |
| [Sealed preparation failure and retry](../evidence/architecture-v4-sealed-deadline-2026-09-23/README.md) | Failure retained; bounded preparation timeout deployed; retry began measurement | No completed sealed qualification claimed by this record |
| [Full-size HTTP tests](../evidence/architecture-v4-worker-disk-2026-09-23/README.md) | Consolidation, loan/return, retained queries, reorgs, and disk sampling | Local host, not production memory qualification |
| [Capacity baseline](../evidence/schema9-worker-capacity-2026-09-22/REPORT.md) | Historical sealed-shard measurements | Different schema and host; does not qualify seven sealed shards |

Open gates are worker memory/swap and admission calibration, latency during
publication and routing refresh, the supported canonical burst envelope, and
uninterrupted active and sealed hardware traces. Seven sealed shards remain
opt-in and unqualified. The automated assessors never issue a qualification
certificate. Historical results below apply only to their recorded binaries,
hardware and workloads.

The [worker cache-reclaim deployment](../evidence/worker-cache-reclaim-2026-09-24/README.md)
shows zero worker pressure and swap in an operator-stopped 23-publication,
approximately 26-minute observed window on the same 8 GiB hosts. It is partial
evidence: the requested 30-minute/30-publication focused gate was not completed,
so this page does not mark worker memory qualification passed.

The [architecture](architecture.md) states the placement contract and the
[evidence index](../evidence/README.md) owns historical run navigation.

## Limited production rollout

The [pilot readiness gates](pilot-readiness.md) define the 2 QPS opt-in envelope,
4 QPS qualification workload, wallet checks, recovery budgets and observation
window. They require new evidence for the selected release and every worker.

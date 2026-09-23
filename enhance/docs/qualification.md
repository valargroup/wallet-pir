# Qualification and current status

This page records what the repository's dated evidence supports. It is not a
live fleet report. Protocol v6/q48 was deployed directly over SSH from commit
`afdb4b6` on September 23, 2026. That binary predates the later `ipir-sp` rc.2
pin on `main`. Each result applies only to its recorded source, binary,
hardware, and workload.

| Evidence | Recorded result | Limit |
|---|---|---|
| [Protocol-v6 production cutover](../evidence/protocol-v6-production-2026-09-23/README.md) | Two ready replicas and exact-answer public checks after cutover | No full CI or sustained qualification; eight-way load hit the two-query admission limit |
| [Schema-11 interoperability](../evidence/schema11-suffix-2026-09-23/README.md) | Suffix record and wallet checks | Local; no fleet qualification |
| [Production deployment](../evidence/architecture-v4-production-2026-09-23/README.md) | Exact answers, replicated publication, failover, restart, repair, and rollback rehearsal | Previous candidate; short load and a worker sampling gap |
| [Active campaign](../evidence/architecture-v4-production-2026-09-23/README.md) | Six hours, 320 publications, 225,939 correct background answers | Worker-1 sample gap prevents uninterrupted hardware qualification |
| [Sealed preparation failure and retry](../evidence/architecture-v4-sealed-deadline-2026-09-23/README.md) | Failure retained; bounded preparation timeout deployed; retry began measurement | No completed sealed qualification claimed by this record |
| [Full-size HTTP tests](../evidence/architecture-v4-worker-disk-2026-09-23/README.md) | Consolidation, loan/return, retained queries, reorgs, and disk sampling | Local host, not production memory qualification |
| [Capacity baseline](../evidence/schema9-worker-capacity-2026-09-22/REPORT.md) | Historical sealed-shard measurements | Different schema and host; does not qualify seven sealed shards |

Open production gates are uninterrupted active and sealed hardware traces on
physical workers; full-size delayed reclamation, replica recovery, and overload
characterization; admission and memory-model calibration; and v6 wallet
interoperability confirmation. Seven sealed shards remain opt-in and unqualified until measured
on the intended hardware. The automated campaign assessor never issues a
qualification certificate. Inspect raw failures and unstarted work as well as
successful query counts.

The [architecture](architecture.md) states the placement contract and the
[evidence index](../evidence/README.md) owns historical run navigation.

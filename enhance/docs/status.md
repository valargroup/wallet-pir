# Status

## September 20, 2026: schema-8 implementation, not deployed

The 29-record layout is implemented and tested locally at schema 8; **nothing
has been deployed**. The public origin still served schema 7 when last read:
737-byte records, nine records per row, 6,633-byte rows, 65,536 logical rows,
467,255 Ironwood positions, seven shards, anchor height 3,489,681. That reading
is retained in `enhance/ops/fixtures/enhance/init-schema7-nine-record.json`,
where it also serves as the negative case for the deployment layout gate.

Direct production access from the implementing session was refused by a local
permission gate, so no host inventory, unit configuration, memory limit,
preparation run or cutover was performed or observed. No schema-8 figure in this
repository is a production measurement; the sizes in
[performance](performance.md#derived-schema-8-sizes) and the residency table in
[capacity expansion](capacity-expansion.md) are derived from the pinned encoder
and the runtime shapes, and the September 20 layout benchmark was taken on an
isolated four-vCPU AMD host, not on this fleet.

Two facts dated here because they expire. The schema-8 query holds its 196.0 KiB
size only until 475,136 positions, after which it is 282.0 KiB; at 467,522
positions that is 7,614 positions away. Do not convert that into a time from the
samples taken here: two intervals the same morning gave 35.4 and 10.7 positions
per block, so the honest statement is hours to days, and the count must be read
again before the figure is quoted. The second fact is that the c-4 worker memory
budget is reached at roughly eight schema-8 shards, about 1.9 million positions,
which the group/autoscale design cannot expand past without one of the decisions
listed in [capacity expansion](capacity-expansion.md).

## September 21, 2026: current-worker shard ceiling decision

Keep the existing `MemoryHigh=6G` and `MemoryMax=7G` limits and treat three
schema-8 shards (712,704 positions) as the maximum supported placement on the
current `c-4` workers. Do not activate a fourth shard on that hardware. The
completed three-shard fixture peaked at 5,760 MiB with no memory events; the
four-shard attempt reached the 6,144-MiB soft limit and recorded 2,073 reclaim
events before the colocated harness was killed. Four shards require a larger
worker and a new off-host qualification. This is an operational decision based
on isolated evidence, not a claim that schema 8 has been deployed.

## September 14, 2026: public origin

A read-only observation of `https://enhance-pir.valargroup.dev` found schema 7,
protocol `ironwood-enhance-pir-v2`, five shards in `shard-group-01`, and two
configured workers. Public health reported serving with eight retained
generations. The initialization response advertised 737-byte records, nine
records per row, 8,192 rows per shard and 65,536 logical rows.

The [bounded public baseline](../evidence/public-baseline-2026-09-14/README.md)
contains captured health/init responses and query measurements. This establishes
that the public origin serves schema 7. It does not establish worker SKU, binary
revision, release receipts, wallet adoption, autoscaler state or full hardware
qualification; those were not inspected through private host access.

## September 13, 2026: retained operator report

The earlier record said the versioned Spaces state migration and creation of a
new c-4 pair were complete, with a no-change Terraform plan afterward. It said
the old pair still served production, the autoscaler was disabled, and the c-4
expansion target was not yet qualified or activated. These are historical
observations, not a claim that the fleet remains in that state today.

That report also recorded restored coordinator SSH access after removal of its
DigitalOcean firewall and disabling UFW at boot, and use of systemd encrypted
runtime credentials with Infisical as the source for rotation and recovery.

The [isolated preflight](../evidence/preflight-2026-09-13/README.md) reported exact
answers at a named revision. Its raw bundle was not committed. It does not satisfy
full hardware, failover, expansion or production-observation acceptance.

## Unresolved deployment evidence

The public schema observation supersedes the older uncertainty about whether
schema 7 is served at this origin. It does not resolve older conflicting claims
about Vizor migration or whether any wallet depended on the former API. Obtain
a dated client conformance/adoption record before relying on either claim.

Reconcile installed release, current inventory and qualification or operator
acceptance receipts before a future migration. Do not repeat historical state
moves based solely on the September 13 note. The [remaining work](remaining-work.md)
tracks evidence still needed; [deployment](deployment.md) owns release procedures.

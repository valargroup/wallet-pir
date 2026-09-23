# Reorg and memory-placement composition — September 23, 2026

The memory fallback previously tested ordinary single-replica admission quorum
even when the count planner had already moved a published shard and therefore
required both destination replicas. It also only considered shards whose planned
group matched their published group. Together these conditions could block a
reorg despite a complete spare pair being available.

The fallback now derives admission quorum from the same required-destination-pair
policy used by reservation/publication. It can reroute any already-published shard
assigned to a pressured group, including one the count planner has moved. Ordinary
unchanged assignments still use a surviving admitting replica; HTTP 503 alone
still does not trigger a memory relocation.

The focused regression passes using actual lifecycle coverage and count placement:
a rollback turns the sixth sealed shard into an active shard, initially assigned
to an existing second group. One replica there admits and the other returns 507;
the fallback selects a third complete pair and leaves all other shards unchanged.
It also checks that 503 alone preserves the proposed placement and an incomplete
spare pair cannot receive the shard. This test uses controlled HTTP admission
responses and full-size plan metadata, not materialized full-size runtimes.

The existing real encrypted HTTP relocation regression passed in 18.70 seconds,
including ordinary peer availability, failed destination reservation, and exact
current/retained queries. Server-library/test Clippy passed with warnings denied. This correction requires a fresh
Linux rebuild; the preceding full-size/Linux report predates it. Multi-shard
rearrangement, hardware qualification and deployment remain outstanding.

# Reorg destination replication — September 23, 2026

The count-based placement planner can relocate a published shard when a reorg
turns a six-sealed group into a workload requiring an active slot. If the
destination group already appeared in the previous assignment, publication
previously did not necessarily require both destination replicas: strict quorum
was derived from newly used groups, new-shard fallback and elective consolidation.

`required_destination_pairs` now includes every destination of an existing shard
whose group changes, as well as newly used groups. Coordinator reservation and
readiness checks consume that strict set. Unchanged assignments retain the
ordinary surviving-replica publication path. A retry without elective consolidation
recomputes this requirement, so it cannot downgrade a required reorg move.

The regression uses actual lifecycle coverage and `assign`: a six-sealed group
and a second active group, followed by a rollback that makes the sixth shard
active. It verifies relocation into the existing second group and strict pair
quorum; unchanged assignments and newly used groups are checked too. All eight
control tests pass. Server-library Clippy passes with warnings denied.

The existing real HTTP offline-participant/expiry/recovery campaign passed in
70.42 seconds, preserving ordinary publication and recovery with an offline peer. The planner regression uses metadata,
not six materialized full-size shard runtimes. General memory-pressure relocation,
native Linux rebuild, hardware qualification and deployment remain outstanding.

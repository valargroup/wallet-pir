# Published-shard memory relocation — September 23, 2026

The coordinator now tries a whole-shard relocation when source preflight includes
an explicit HTTP 507 memory refusal and no eligible replica admits the assignment.
HTTP 503, network failure, or a pending decision alone does not trigger movement.
A surviving admitting source peer preserves ordinary publication placement.

Destinations are considered in stable group order, must satisfy role limits, and
must admit their entire candidate assignment on both replicas. Pending-decision
and settling destinations are excluded. A nonempty residual source assignment
must also admit on both replicas. Empty sources retain their old snapshots and
query routes; failed source reservations cannot remove retained service. Actual
reservation repeats admission and the published-shard move requires both
destination replicas regardless of whether that group was previously used.
Atomic publication and existing retention/reclamation rules apply to the move.

The planner tries one whole-shard move per pressured group. It does not search
multi-shard rearrangements; if no candidate fits, the existing reservation failure
path preserves published service and emits capacity demand. No capacity estimate
or hardware qualification rule is weakened.

The real four-worker HTTP regression passed in 18.60 seconds. It starts with a
published active shard, verifies busy-only refusal does not move it, verifies one
healthy source replica keeps placement, rejects an unavailable destination peer,
and rejects a destination reservation failure after successful preflight. The
final admitted move publishes both destination replicas; exact current queries
including appended records and an old-generation query pass afterward. Refusals
are injected at actual private HTTP endpoints, not caused by measured host memory.

All 26 v4 server-library tests pass. Server library/tests Clippy passes with
warnings denied. The broader default HTTP regression passed: seven tests, zero failures, and
two explicitly ignored full-size campaigns, in 148.10 seconds. The full-size
campaigns were not rerun for this change. Full-size multi-shard rearrangement, six-hour 8 GiB
qualification, fresh Linux artifacts and live deployment remain outstanding.

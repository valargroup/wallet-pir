# Pending-decision placement exclusion — September 23, 2026

Both new-shard alternatives and elective consolidation now exclude groups with
replicas awaiting durable commit/abort notification. The shared preflight helper
checks the coordinator's pending-replica set before issuing memory admission
requests. Reservation already enforced this exclusion; aligning preflight avoids
selecting a known unusable destination and unnecessarily blocking publication.

Validation:

- [Admission test](admission-tests.log): real worker endpoints plus persisted
  pending-abort records verify that otherwise-admissible groups are not selected
  for new shards or consolidation; clearing the pending decision restores the
  normal admission result. Existing two-replica, role and state checks pass.
- [Abort-recovery HTTP campaign](http-tests.log): passed in 57.94 seconds, covering
  healthy-peer progress, ambiguous accepted reservations, restarts, lost abort
  responses and replay fencing.
- [Clippy](clippy.log): library passed with warnings denied. Formatting and diff
  checks passed.

These local checks do not establish deployed fault tolerance, memory qualification
or automatic infrastructure recovery. The overall release gates remain open.

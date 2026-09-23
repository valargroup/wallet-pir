# Admission-aware placement for new shards — September 23, 2026

Before reservation, the coordinator checks the preferred group's complete
candidate assignment. For a newly introduced shard whose preferred group does
not admit it on both replicas, it tries other registered, non-settling groups in
sequence order. The alternative must satisfy role limits and admit the complete
assignment on both replicas. Its actual reservation still requires both replicas
and repeats memory admission. Preflight itself does not reserve or mutate state.

Existing published shards are not moved by this fallback. Failure to find an
alternative leaves placement unchanged for ordinary reservation/quorum handling.
This is a greedy per-new-shard search, not a general solver for published-data
relocation or coordinated multi-shard rearrangements.

Validation:

- [Admission checks](admission-tests.log): real worker admission endpoints with
  full-profile metadata verify alternative selection, unchanged published shard
  placement, two-replica requirement, settling exclusion and no persistent worker
  mutations. Metadata fixtures do not materialize full-size databases.
- [HTTP publication](http-tests.log): with four real worker listeners and one
  preferred replica refusing admission, the coordinator publishes on the alternate
  pair. Health reports two ready replicas there and no published assignment on
  the refused group; encrypted queries at positions 0, 32, 33 and 66 return exact
  fixture records. The test passed in 7.48 seconds.
- [Clippy](clippy.log): library and HTTP integration target passed with warnings
  denied. Formatting and diff checks passed.

Memory-triggered expansion, retained-data relocation, full hardware qualification
and deployed recovery remain outstanding. No infrastructure changes occurred.
